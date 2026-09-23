"""Enumerate source facility identifiers from the clinical search indexes.

Report scoping needs the set of facilities that have sent data without exposing any
clinical content. This service issues a single ``size: 0`` terms aggregation over both
the HL7 and CCDA indexes on the reserved partition field (``sourceFacilityId``) using the
same signed ``SearchTransport`` protocol as the indexing path. Because the partition
field is a keyword, the aggregation returns exact identifiers rather than analyzed free
text; the results are still validated to be non-empty strings, deduplicated across both
indexes, and returned in sorted order. Failures surface as a sanitized error that never
echoes the backend response.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import quote

from src.search_store import SearchTransport

DEFAULT_MAX_TERMS = 10_000
FACILITY_FIELD = "sourceFacilityId"

# Keyword field paths only; this deliberately excludes analyzed/free-text style names.
FIELD_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]{0,255}$")
# A conservative subset of the OpenSearch index naming rules, safe as a URL path segment.
INDEX_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,254}$")

INVALID_FACILITY_CONFIG = "Facility directory configuration is invalid"
AGGREGATION_FAILED = "Facility directory lookup failed"

_AGGREGATION_NAME = "facilities"


class FacilityDirectoryError(RuntimeError):
    """Sanitized facility lookup failure that never includes the backend response."""


class FacilityDirectory:
    """Return the deduplicated, sorted set of known source facility identifiers."""

    def __init__(
        self,
        transport: SearchTransport,
        *,
        hl7_index: str,
        ccda_index: str,
        field: str = FACILITY_FIELD,
        max_terms: int = DEFAULT_MAX_TERMS,
    ) -> None:
        if not INDEX_NAME_PATTERN.fullmatch(hl7_index) or not INDEX_NAME_PATTERN.fullmatch(
            ccda_index
        ):
            raise ValueError(INVALID_FACILITY_CONFIG)
        if not FIELD_PATTERN.fullmatch(field) or max_terms < 1:
            raise ValueError(INVALID_FACILITY_CONFIG)
        self._transport = transport
        self._field = field
        self._max_terms = max_terms
        # A single multi-index target aggregates across both indexes in one request.
        self._path = f"/{quote(hl7_index, safe='')},{quote(ccda_index, safe='')}/_search"

    def list_facilities(self) -> list[str]:
        """Aggregate distinct facility identifiers across both clinical indexes."""
        body = json.dumps(
            {
                "size": 0,
                "aggs": {
                    _AGGREGATION_NAME: {"terms": {"field": self._field, "size": self._max_terms}}
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        try:
            status, response = self._transport.request("POST", self._path, body)
        except Exception:
            raise FacilityDirectoryError(AGGREGATION_FAILED) from None
        if status < 200 or status >= 300:
            raise FacilityDirectoryError(AGGREGATION_FAILED)
        return _facility_values(response)


def _facility_values(response: dict[str, Any]) -> list[str]:
    aggregations = response.get("aggregations")
    if not isinstance(aggregations, dict):
        raise FacilityDirectoryError(AGGREGATION_FAILED)
    facilities = aggregations.get(_AGGREGATION_NAME)
    if not isinstance(facilities, dict):
        raise FacilityDirectoryError(AGGREGATION_FAILED)
    buckets = facilities.get("buckets")
    if not isinstance(buckets, list):
        raise FacilityDirectoryError(AGGREGATION_FAILED)
    values: set[str] = set()
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise FacilityDirectoryError(AGGREGATION_FAILED)
        key = bucket.get("key")
        # Only exact keyword identifiers are surfaced; non-string keys are never returned.
        if isinstance(key, str) and key:
            values.add(key)
    return sorted(values)
