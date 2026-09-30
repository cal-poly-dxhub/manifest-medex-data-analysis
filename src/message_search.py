"""Metadata-only attribute search over the parsed HL7 and CCDA OpenSearch indexes.

This module owns the read side of the clinical search stack. It never accepts raw
OpenSearch DSL: a caller supplies a bounded list of typed attribute filters
(``equals``/``exists``/``contains``/``prefix``), an optional exact facility term, an
optional half-open timezone-aware time window, a page ``limit``, and an opaque cursor.
Every field a caller names is validated for syntax and for exact presence in the index's
``_field_caps`` response before any ``_search`` request is issued, so a search can only
ever touch fields the index actually maps.

Responses are reduced to a fixed metadata allowlist -- no clinical ``_source`` value is
ever copied through -- and a single INFO audit line records only the caller, the index,
the distinct field names touched, the event, and a timestamp. No values, facility,
dates, or result content are ever logged.

The module depends only on the standard library and the :class:`SearchTransport` contract
from :mod:`src.search_store`.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from re import compile as re_compile
from typing import Any
from urllib.parse import quote

from src.search_store import SearchTransport

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# Sanitized failure messages. These never carry backend or clinical content.
SEARCH_FAILED = "Message search request failed"
FIELD_CAPS_FAILED = "OpenSearch field capabilities request failed"
FIELD_CAPS_INVALID = "OpenSearch field capabilities response was malformed"

# Short, caller-safe request codes.
INVALID_INDEX = "invalid_index"
INVALID_CALLER = "invalid_caller"
TOO_MANY_FILTERS = "too_many_filters"
INVALID_FILTER = "invalid_filter"
INVALID_FIELD = "invalid_field"
UNKNOWN_FIELD = "unknown_field"
INVALID_OPERATOR = "invalid_operator"
INVALID_VALUE = "invalid_value"
VALUE_TOO_SHORT = "value_too_short"
VALUE_UNSAFE = "value_unsafe"
INVALID_FACILITY = "invalid_facility"
INVALID_TIME = "invalid_time"
INVALID_TIME_RANGE = "invalid_time_range"
INVALID_LIMIT = "invalid_limit"
INVALID_CURSOR = "invalid_cursor"

SEARCH_EVENT = "message_search_executed"

ALLOWED_OPERATORS = frozenset({"equals", "exists", "contains", "prefix"})
MAX_FILTERS = 20
CONTAINS_MIN_LENGTH = 3
MAX_VALUE_LENGTH = 256
DEFAULT_LIMIT = 25
MAX_LIMIT = 100
TRACK_TOTAL_HITS = 10_000
MAX_CURSOR_LENGTH = 2048
MAX_ISO_LENGTH = 64
MAX_CALLER_LENGTH = 128
CURSOR_VERSION = 1
_SORT_ARITY = 2

# Field names are validated for syntax before any exact field-caps lookup. The gate keeps
# obviously hostile input (spaces, wildcards, quotes, control characters) out of the caps
# membership check; semantic correctness is enforced by exact caps presence.
FIELD_NAME_PATTERN = re_compile(r"[A-Za-z0-9_][A-Za-z0-9_.\-]{0,254}")

_WILDCARD_ESCAPE_CHARS = frozenset("\\*?")


@dataclass(frozen=True)
class IndexConfig:
    """Static per-index configuration for a searchable clinical index."""

    source_format: str
    time_field: str


# Exactly two indexes are searchable; nothing else is addressable through this module.
INDEX_CONFIGS: dict[str, IndexConfig] = {
    "hl7-messages-v1": IndexConfig(source_format="hl7-v2", time_field="messageTime"),
    "ccda-documents-v1": IndexConfig(source_format="ccda", time_field="documentTime"),
}

# Metadata-only projection returned to callers. No clinical narrative is ever included.
_BASE_SOURCE_FIELDS = (
    "documentId",
    "sourceFormat",
    "sourceFacilityId",
    "messageType",
    "triggerEvent",
    "ingestTime",
)


class SearchError(RuntimeError):
    """Sanitized search failure that never includes a backend response or clinical value.

    Carries non-clinical diagnostics only: the backend HTTP status (when the failure was
    a non-2xx response) and a failure kind distinguishing transport errors from status
    errors, mirroring the ``IndexingError`` telemetry pattern in ``search_store``.
    """

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        failure_kind: str | None = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.failure_kind = failure_kind


class SearchRequestError(SearchError):
    """Expected request failure that is safe to return to the authenticated caller."""

    def __init__(self, status: int, code: str) -> None:
        super().__init__(code)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class FieldCatalog:
    """Parsed, searchable leaf fields for one index with text/keyword relationships."""

    present: frozenset[str]
    text_fields: frozenset[str]
    keyword_subfields: frozenset[str]

    @property
    def sorted_fields(self) -> list[str]:
        """Return every searchable non-object leaf field in deterministic order."""
        return sorted(self.present)

    def has(self, field: str) -> bool:
        """Report whether a field is present in the index capabilities exactly."""
        return field in self.present

    def resolve_equals_field(self, field: str) -> str:
        """Map a text base field to its exact keyword subfield when one is searchable.

        HL7 native fields are keywords already and are never in ``text_fields``, so they
        are returned unchanged. A CCDA free-text field is matched exactly through its
        ``.keyword`` subfield when that subfield exists and is searchable.
        """
        if field in self.text_fields:
            candidate = f"{field}.keyword"
            if candidate in self.keyword_subfields:
                return candidate
        return field


class SearchService:
    """Execute bounded, metadata-only attribute searches against one OpenSearch cluster.

    Field capabilities are fetched once per index and cached for the lifetime of the
    instance, so a warm execution environment pays the ``_field_caps`` cost only on the
    first search of each index.
    """

    def __init__(self, transport: SearchTransport) -> None:
        self._transport = transport
        self._caps_cache: dict[str, FieldCatalog] = {}

    def field_catalog(self, index: str) -> FieldCatalog:
        """Return the cached, parsed field capabilities for one allowed index."""
        _resolve_index(index)
        return self._load_field_caps(index)

    def search(
        self,
        *,
        index: Any,
        caller_sub: Any,
        filters: Any = None,
        facility: Any = None,
        from_time: Any = None,
        to_time: Any = None,
        limit: Any = None,
        cursor: Any = None,
    ) -> dict[str, Any]:
        """Validate a request, run one ``_search``, and return metadata-only hits.

        The returned mapping carries ``items`` (each a metadata allowlist projection),
        ``total`` (a bounded total-hits count), and ``nextCursor`` (an opaque cursor bound
        to this index, or ``None`` when the page was not full).
        """
        caller = _validate_caller(caller_sub)
        config = _resolve_index(index)
        page_size = _resolve_limit(limit)
        search_after = _decode_cursor(cursor, index)
        raw_filters = _normalize_filters(filters)
        if raw_filters:
            catalog = self._load_field_caps(index)
            filter_clauses, field_names = self._build_attribute_filters(catalog, raw_filters)
        else:
            filter_clauses, field_names = [], []
        facility_clause = _facility_filter(facility)
        if facility_clause is not None:
            filter_clauses.append(facility_clause)
        time_clause = _time_filter(config, from_time, to_time)
        if time_clause is not None:
            filter_clauses.append(time_clause)

        source_includes = _source_includes(config)
        body = _build_body(config, filter_clauses, page_size, source_includes, search_after)
        response = self._execute(index, body)

        items = _parse_hits(response, source_includes)
        total = _read_total(response)
        next_cursor = _next_cursor(index, response, page_size, len(items))

        LOGGER.info(
            json.dumps(
                {
                    "callerSub": caller,
                    "event": SEARCH_EVENT,
                    "fields": sorted(set(field_names)),
                    "index": index,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return {"items": items, "total": total, "nextCursor": next_cursor}

    def _load_field_caps(self, index: str) -> FieldCatalog:
        cached = self._caps_cache.get(index)
        if cached is not None:
            return cached
        # GET with a query string is the only field-caps form the OpenSearch Serverless
        # permission model documents; the transport canonicalizes the query string so
        # the SigV4 signature matches the wire bytes (a raw "*" previously mismatched).
        path = f"/{quote(index, safe='')}/_field_caps?fields=*"
        try:
            status, response = self._transport.request("GET", path)
        except Exception:
            raise SearchError(FIELD_CAPS_FAILED, failure_kind="transport") from None
        if status < 200 or status >= 300:
            raise SearchError(FIELD_CAPS_FAILED, http_status=status, failure_kind="status")
        catalog = _parse_field_caps(response)
        self._caps_cache[index] = catalog
        return catalog

    def _build_attribute_filters(
        self,
        catalog: FieldCatalog,
        filters: Any,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        raw_filters = _normalize_filters(filters)
        clauses: list[dict[str, Any]] = []
        field_names: list[str] = []
        for raw_filter in raw_filters:
            clause, field = _build_filter(catalog, raw_filter)
            clauses.append(clause)
            field_names.append(field)
        return clauses, field_names

    def _execute(self, index: str, body: dict[str, Any]) -> Mapping[str, Any]:
        path = f"/{quote(index, safe='')}/_search"
        payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        try:
            status, response = self._transport.request("POST", path, payload)
        except Exception:
            raise SearchError(SEARCH_FAILED, failure_kind="transport") from None
        if status < 200 or status >= 300:
            raise SearchError(SEARCH_FAILED, http_status=status, failure_kind="status")
        if not isinstance(response, Mapping):
            raise SearchError(SEARCH_FAILED)
        return response


def _resolve_index(index: Any) -> IndexConfig:
    config = INDEX_CONFIGS.get(index) if isinstance(index, str) else None
    if config is None:
        raise SearchRequestError(400, INVALID_INDEX)
    return config


def _validate_caller(caller_sub: Any) -> str:
    if not isinstance(caller_sub, str) or not caller_sub or len(caller_sub) > MAX_CALLER_LENGTH:
        raise SearchRequestError(400, INVALID_CALLER)
    return caller_sub


def _resolve_limit(limit: Any) -> int:
    if limit is None:
        return DEFAULT_LIMIT
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise SearchRequestError(400, INVALID_LIMIT)
    if limit < 1 or limit > MAX_LIMIT:
        raise SearchRequestError(400, INVALID_LIMIT)
    return limit


def _normalize_filters(filters: Any) -> list[Any]:
    if filters is None:
        return []
    if not isinstance(filters, list):
        raise SearchRequestError(400, INVALID_FILTER)
    if len(filters) > MAX_FILTERS:
        raise SearchRequestError(400, TOO_MANY_FILTERS)
    return filters


def _source_includes(config: IndexConfig) -> tuple[str, ...]:
    # The configured time field is always projected so a caller can order and page results
    # without ever receiving a clinical narrative value.
    return (*_BASE_SOURCE_FIELDS[:-1], config.time_field, _BASE_SOURCE_FIELDS[-1])


def _build_body(
    config: IndexConfig,
    filter_clauses: list[dict[str, Any]],
    page_size: int,
    source_includes: tuple[str, ...],
    search_after: list[Any] | None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "size": page_size,
        "track_total_hits": TRACK_TOTAL_HITS,
        # Deterministic total order: newest first, then a stable keyword tie-breaker. No
        # ``from`` offset is ever used; deep pagination is cursor-only.
        "sort": [
            {config.time_field: {"order": "desc"}},
            {"documentId": {"order": "desc"}},
        ],
        "_source": {"includes": list(source_includes)},
        "query": ({"bool": {"filter": filter_clauses}} if filter_clauses else {"match_all": {}}),
    }
    if search_after is not None:
        body["search_after"] = search_after
    return body


def _build_filter(catalog: FieldCatalog, raw_filter: Any) -> tuple[dict[str, Any], str]:
    if not isinstance(raw_filter, Mapping):
        raise SearchRequestError(400, INVALID_FILTER)
    if set(raw_filter) - {"field", "operator", "value"}:
        raise SearchRequestError(400, INVALID_FILTER)
    field = raw_filter.get("field")
    operator = raw_filter.get("operator")
    value = raw_filter.get("value")
    if not isinstance(field, str) or not _valid_field_syntax(field):
        raise SearchRequestError(400, INVALID_FIELD)
    # Exact capabilities presence is enforced before any search request is issued.
    if not catalog.has(field):
        raise SearchRequestError(400, UNKNOWN_FIELD)
    if operator not in ALLOWED_OPERATORS:
        raise SearchRequestError(400, INVALID_OPERATOR)
    return _operator_clause(catalog, field, operator, value), field


def _operator_clause(
    catalog: FieldCatalog,
    field: str,
    operator: str,
    value: Any,
) -> dict[str, Any]:
    if operator == "exists":
        if value is not None:
            raise SearchRequestError(400, INVALID_VALUE)
        return {"exists": {"field": field}}
    if operator == "contains":
        if not isinstance(value, str):
            raise SearchRequestError(400, INVALID_VALUE)
        return {"wildcard": {field: {"value": _wildcard_pattern(value)}}}
    text = _require_text_value(value)
    if operator == "equals":
        # A CCDA free-text base field is matched exactly through its keyword subfield; an
        # HL7 native keyword field is used unchanged.
        return {"term": {catalog.resolve_equals_field(field): text}}
    return {"prefix": {field: {"value": text}}}


def _require_text_value(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_VALUE_LENGTH:
        raise SearchRequestError(400, INVALID_VALUE)
    return value


def _wildcard_pattern(raw: str) -> str:
    trimmed = raw.strip()
    if len(trimmed) < CONTAINS_MIN_LENGTH:
        raise SearchRequestError(400, VALUE_TOO_SHORT)
    # Reject values that are only wildcard characters (a cluster-scanning pattern) and any
    # oversized value that could drive a pathological wildcard scan.
    if not trimmed.strip("*?") or len(trimmed) > MAX_VALUE_LENGTH:
        raise SearchRequestError(400, VALUE_UNSAFE)
    return f"*{_escape_wildcard(trimmed)}*"


def _escape_wildcard(value: str) -> str:
    escaped: list[str] = []
    for character in value:
        if character in _WILDCARD_ESCAPE_CHARS:
            escaped.append("\\")
        escaped.append(character)
    return "".join(escaped)


def _valid_field_syntax(field: str) -> bool:
    return FIELD_NAME_PATTERN.fullmatch(field) is not None


def _facility_filter(facility: Any) -> dict[str, Any] | None:
    if facility is None:
        return None
    if not isinstance(facility, str) or not facility or len(facility) > MAX_VALUE_LENGTH:
        raise SearchRequestError(400, INVALID_FACILITY)
    return {"term": {"sourceFacilityId": facility}}


def _time_filter(config: IndexConfig, from_time: Any, to_time: Any) -> dict[str, Any] | None:
    if from_time is None and to_time is None:
        return None
    if from_time is None or to_time is None:
        raise SearchRequestError(400, INVALID_TIME_RANGE)
    start = _parse_iso(from_time)
    end = _parse_iso(to_time)
    if start >= end:
        raise SearchRequestError(400, INVALID_TIME_RANGE)
    # Half-open [from, to) on the index's configured time field.
    return {"range": {config.time_field: {"gte": from_time, "lt": to_time}}}


def _parse_iso(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > MAX_ISO_LENGTH:
        raise SearchRequestError(400, INVALID_TIME)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise SearchRequestError(400, INVALID_TIME) from None
    if parsed.tzinfo is None:
        raise SearchRequestError(400, INVALID_TIME)
    return parsed


def _parse_field_caps(response: Any) -> FieldCatalog:
    if not isinstance(response, Mapping):
        raise SearchError(FIELD_CAPS_INVALID)
    fields = response.get("fields")
    if not isinstance(fields, Mapping):
        raise SearchError(FIELD_CAPS_INVALID)
    present: set[str] = set()
    text_fields: set[str] = set()
    keyword_subfields: set[str] = set()
    for name, type_map in fields.items():
        if not isinstance(name, str) or not isinstance(type_map, Mapping):
            continue
        searchable, is_object, has_text, has_keyword = _summarize_field_types(type_map)
        # Object and nested containers are not directly searchable leaves; skip them along
        # with any leaf no type reports as searchable.
        if is_object or not searchable:
            continue
        present.add(name)
        if has_text:
            text_fields.add(name)
        if has_keyword and name.endswith(".keyword"):
            keyword_subfields.add(name)
    return FieldCatalog(
        present=frozenset(present),
        text_fields=frozenset(text_fields),
        keyword_subfields=frozenset(keyword_subfields),
    )


def _summarize_field_types(type_map: Mapping[str, Any]) -> tuple[bool, bool, bool, bool]:
    searchable = False
    is_object = False
    has_text = False
    has_keyword = False
    for type_name, caps in type_map.items():
        if not isinstance(caps, Mapping):
            continue
        if type_name in {"object", "nested"}:
            is_object = True
        if caps.get("searchable") is True:
            searchable = True
            if type_name == "text":
                has_text = True
            elif type_name == "keyword":
                has_keyword = True
    return searchable, is_object, has_text, has_keyword


def _parse_hits(
    response: Mapping[str, Any],
    source_includes: tuple[str, ...],
) -> list[dict[str, Any]]:
    hits = response.get("hits")
    if not isinstance(hits, Mapping):
        raise SearchError(SEARCH_FAILED)
    raw_hits = hits.get("hits")
    if not isinstance(raw_hits, list):
        raise SearchError(SEARCH_FAILED)
    items: list[dict[str, Any]] = []
    for hit in raw_hits:
        if not isinstance(hit, Mapping):
            raise SearchError(SEARCH_FAILED)
        source = hit.get("_source")
        if not isinstance(source, Mapping):
            raise SearchError(SEARCH_FAILED)
        # Copy only allowlisted metadata keys; arbitrary _source keys are never surfaced.
        items.append({key: source[key] for key in source_includes if key in source})
    return items


def _read_total(response: Mapping[str, Any]) -> int:
    hits = response.get("hits")
    if not isinstance(hits, Mapping):
        raise SearchError(SEARCH_FAILED)
    total = hits.get("total")
    if isinstance(total, bool):
        raise SearchError(SEARCH_FAILED)
    if isinstance(total, int):
        value: Any = total
    elif isinstance(total, Mapping):
        value = total.get("value")
    else:
        raise SearchError(SEARCH_FAILED)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SearchError(SEARCH_FAILED)
    return value


def _next_cursor(
    index: str,
    response: Mapping[str, Any],
    page_size: int,
    item_count: int,
) -> str | None:
    # Only a full page can have a successor; a short page is terminal.
    if item_count < page_size:
        return None
    hits = response.get("hits")
    if not isinstance(hits, Mapping):
        return None
    raw_hits = hits.get("hits")
    if not isinstance(raw_hits, list) or not raw_hits:
        return None
    sort_values = _hit_sort(raw_hits[-1])
    if sort_values is None:
        return None
    return _encode_cursor(index, sort_values)


def _hit_sort(hit: Any) -> list[Any] | None:
    if not isinstance(hit, Mapping):
        return None
    sort = hit.get("sort")
    if not isinstance(sort, list) or len(sort) != _SORT_ARITY:
        return None
    for element in sort:
        if isinstance(element, bool) or not isinstance(element, str | int | float):
            return None
    return list(sort)


def _encode_cursor(index: str, sort_values: list[Any]) -> str:
    payload = json.dumps(
        {"index": index, "searchAfter": sort_values, "version": CURSOR_VERSION},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(value: Any, index: str) -> list[Any] | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > MAX_CURSOR_LENGTH:
        raise SearchRequestError(400, INVALID_CURSOR)
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        payload = json.loads(decoded)
        return _validate_cursor_payload(payload, index)
    except (
        binascii.Error,
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ):
        raise SearchRequestError(400, INVALID_CURSOR) from None


def _validate_cursor_payload(payload: Any, index: str) -> list[Any]:
    if not isinstance(payload, Mapping):
        raise TypeError
    if set(payload) != {"index", "searchAfter", "version"}:
        raise ValueError
    if payload["version"] != CURSOR_VERSION or payload["index"] != index:
        raise ValueError
    search_after = payload["searchAfter"]
    if not isinstance(search_after, list) or len(search_after) != _SORT_ARITY:
        raise ValueError
    for element in search_after:
        if isinstance(element, bool) or not isinstance(element, str | int | float):
            raise TypeError
    return search_after
