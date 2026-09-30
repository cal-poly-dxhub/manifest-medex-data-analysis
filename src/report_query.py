"""Shared OpenSearch count-query construction and a dry-run query-test service.

The batch report runner and the interactive query-test endpoint must count a stored
report row the same way, so this module owns the single copy of that logic: it injects an
optional partition (facility) ``term`` and an optional half-open time-window ``range``
into a deep copy of the row query -- the stored query is never mutated -- and issues a
``size: 0``, ``track_total_hits: true`` search. :func:`build_count_body` is reused by the
runner's ``_msearch`` batches and by :class:`QueryTester`'s single ``_search`` request,
and :func:`read_total_hits` is the one parser of a total-hits count, so a dry run and a
real run share byte-identical query semantics with no duplicated behavior.

:class:`QueryTester` validates a candidate row through the definition core (with synthetic
label metadata so a bare query reuses the same guardrails), confirms an optional time
window is a well-formed, ordered, timezone-aware ISO range, and confirms an optional
facility is one the :class:`~src.report_facilities.FacilityDirectory` actually returns
rather than arbitrary free text. It returns only a count; neither the query body nor the
clinical response is ever logged.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import quote

from src.report_definition import (
    PARTITION_FIELD,
    TIME_FIELD,
    ReportDefinitionError,
    ReportRow,
    load_report_row,
)
from src.search_store import SearchTransport

MAX_ISO_LENGTH = 64

# Short, caller-safe codes for expected request failures.
INVALID_QUERY = "invalid_query"
INVALID_TIME = "invalid_time"
INVALID_TIME_RANGE = "invalid_time_range"
INVALID_FACILITY = "invalid_facility"
UNKNOWN_FACILITY = "unknown_facility"
# A placeholder row carries a null query and has no implemented count, so a dry run of it
# is rejected up front, before any transport request is issued.
PLACEHOLDER_QUERY = "placeholder_query_not_implemented"

COUNT_RESPONSE_INVALID = "OpenSearch search response was malformed"
QUERY_TEST_FAILED = "Query test request failed"

# Synthetic metadata lets a bare query reuse the row validator without a real definition.
_SYNTHETIC_SEQ = 1
_SYNTHETIC_LABEL = "query-test"
_SYNTHETIC_DESCRIPTION = "query-test dry run"


class QueryCountError(RuntimeError):
    """Sanitized failure reading a total-hits count from a search response."""


class QueryTestError(RuntimeError):
    """Sanitized query-test failure that never includes the backend response."""


class QueryTestRequestError(QueryTestError):
    """Expected query-test failure safe to return to the authenticated caller."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


class _FacilityLister(Protocol):
    def list_facilities(self) -> list[str]: ...


def build_count_body(
    stored_query: Mapping[str, Any],
    *,
    partition_field: str | None = None,
    partition_value: str | None = None,
    time_field: str | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
) -> dict[str, Any]:
    """Return a size-0, total-tracking search body with optional partition/time filters.

    A deep copy of the stored query is wrapped in a ``bool`` clause when it is not already
    one so the injected partition ``term`` and time-window ``range`` always apply as
    filters. The partition filter is added only when both a field and value are given, and
    the time filter only when a field and both bounds are given; the stored query is never
    mutated.
    """
    query: dict[str, Any] = copy.deepcopy(dict(stored_query))
    bool_clause = query.get("bool")
    if not isinstance(bool_clause, dict):
        # Wrap any non-bool query so the partition and time constraints always apply.
        query = {"bool": {"filter": [query]}}
        bool_clause = query["bool"]

    existing = bool_clause.get("filter")
    if isinstance(existing, list):
        filters = list(existing)
    elif existing is None:
        filters = []
    else:
        filters = [existing]

    if partition_field is not None and partition_value is not None:
        filters.append({"term": {partition_field: partition_value}})
    if time_field is not None and from_time is not None and to_time is not None:
        filters.append({"range": {time_field: {"gte": from_time, "lt": to_time}}})
    bool_clause["filter"] = filters

    return {"size": 0, "track_total_hits": True, "query": query}


def read_total_hits(payload: Mapping[str, Any]) -> int:
    """Extract a non-negative total-hits count from a search or ``_msearch`` response body."""
    hits = payload.get("hits")
    if not isinstance(hits, dict):
        raise QueryCountError(COUNT_RESPONSE_INVALID)
    total = hits.get("total")
    if isinstance(total, bool):
        raise QueryCountError(COUNT_RESPONSE_INVALID)
    if isinstance(total, int):
        value: Any = total
    elif isinstance(total, dict):
        value = total.get("value")
    else:
        raise QueryCountError(COUNT_RESPONSE_INVALID)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise QueryCountError(COUNT_RESPONSE_INVALID)
    return value


class QueryTester:
    """Count a candidate report row against one index as a dry run, persisting nothing."""

    def __init__(self, transport: SearchTransport, facilities: _FacilityLister) -> None:
        self._transport = transport
        self._facilities = facilities

    def count(
        self,
        *,
        index: Any,
        query: Any,
        facility: Any = None,
        from_time: Any = None,
        to_time: Any = None,
    ) -> dict[str, int]:
        """Validate the row, inject optional facility/time filters, and return ``{count}``.

        The candidate query is validated through the same guardrails as a stored row, the
        optional time window must be a well-formed, ordered, timezone-aware ISO range, and
        the optional facility must be one the directory actually returns. The request uses
        the shared size-0, total-tracking injection and a signed ``POST index/_search``.
        """
        validated = _validate_candidate_row(index, query)
        from_time_value, to_time_value = _validate_window(from_time, to_time)
        partition_value = self._validate_facility(facility)
        # A placeholder (null) query is rejected in _validate_candidate_row before this
        # point, so a validated row here always carries a concrete query to count.
        stored_query = validated.query
        if stored_query is None:  # pragma: no cover - guarded above
            raise QueryTestRequestError(400, PLACEHOLDER_QUERY)
        body = build_count_body(
            stored_query,
            partition_field=PARTITION_FIELD,
            partition_value=partition_value,
            time_field=TIME_FIELD,
            from_time=from_time_value,
            to_time=to_time_value,
        )
        path = f"/{quote(validated.index, safe='')}/_search"
        payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        try:
            status, response = self._transport.request("POST", path, payload)
        except Exception:
            raise QueryTestError(QUERY_TEST_FAILED) from None
        if status < 200 or status >= 300:
            raise QueryTestError(QUERY_TEST_FAILED)
        try:
            count = read_total_hits(response)
        except QueryCountError:
            raise QueryTestError(QUERY_TEST_FAILED) from None
        return {"count": count}

    def _validate_facility(self, facility: Any) -> str | None:
        if facility is None:
            return None
        if not isinstance(facility, str) or not facility:
            raise QueryTestRequestError(400, INVALID_FACILITY)
        # A facility must be an exact keyword identifier the directory returns, never
        # arbitrary free text a caller could use to probe outside known partitions.
        if facility not in self._facilities.list_facilities():
            raise QueryTestRequestError(400, UNKNOWN_FACILITY)
        return facility


def _validate_candidate_row(index: Any, query: Any) -> ReportRow:
    # A placeholder (null) query has no implemented count; reject it with a clear code
    # before any transport request rather than letting it fall through as invalid.
    if query is None:
        raise QueryTestRequestError(400, PLACEHOLDER_QUERY)
    if not isinstance(index, str) or not isinstance(query, Mapping):
        raise QueryTestRequestError(400, INVALID_QUERY)
    candidate = {
        "seq": _SYNTHETIC_SEQ,
        "label": _SYNTHETIC_LABEL,
        "description": _SYNTHETIC_DESCRIPTION,
        "index": index,
        "query": dict(query),
    }
    try:
        return load_report_row(candidate)
    except ReportDefinitionError:
        raise QueryTestRequestError(400, INVALID_QUERY) from None


def _validate_window(from_time: Any, to_time: Any) -> tuple[str | None, str | None]:
    if from_time is None and to_time is None:
        return None, None
    if from_time is None or to_time is None:
        raise QueryTestRequestError(400, INVALID_TIME_RANGE)
    from_parsed = _parse_iso(from_time)
    to_parsed = _parse_iso(to_time)
    if from_parsed >= to_parsed:
        raise QueryTestRequestError(400, INVALID_TIME_RANGE)
    return from_time, to_time


def _parse_iso(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > MAX_ISO_LENGTH:
        raise QueryTestRequestError(400, INVALID_TIME)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise QueryTestRequestError(400, INVALID_TIME) from None
    if parsed.tzinfo is None:
        raise QueryTestRequestError(400, INVALID_TIME)
    return parsed
