"""Validate Reports definitions against the checked-in schema and query guardrails.

The JSON Schema at schema/report_definition.schema.json captures the document shape
derived from seed/p4p-prototype.json. This module enforces that same shape plus the
cross-item invariants JSON Schema cannot express: unique positive seq values, the fixed
partition/time fields, the allowed index set, and the guardrail that row queries never
reference the reserved partition (sourceFacilityId) or time (messageTime) fields those
queries are combined with at run time.

The definition objects are plain immutable dataclasses backed only by the standard
library. Each exposes a :meth:`model_dump` method so downstream code can serialize a
definition to JSON-compatible primitives without a third-party modeling dependency.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PARTITION_FIELD = "sourceFacilityId"
TIME_FIELD = "messageTime"
RESERVED_QUERY_FIELDS = frozenset({PARTITION_FIELD, TIME_FIELD})
ALLOWED_INDEXES = frozenset({"hl7-messages-v1", "ccda-documents-v1"})

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "report_definition.schema.json"

BLANK_LABEL = "row label must not be blank"
BLANK_SECTION_NAME = "section name must not be blank"
DISALLOWED_INDEX = "row index must be one of the allowed OpenSearch indexes"
FIXED_PARTITION_FIELD = "partition_field must be the reserved source facility field"
FIXED_TIME_FIELD = "time_field must be the reserved message time field"
DUPLICATE_SECTION_SEQ = "section seq values must be unique"
DUPLICATE_ROW_SEQ = "row seq values must be unique within a section"
RESERVED_QUERY_FIELD = "row query must not reference the reserved partition or time field"
INVALID_DEFINITION = "report definition is invalid"

_ROW_KEYS = ("seq", "label", "description", "index", "query")
_SECTION_KEYS = ("seq", "name", "rows")
_DEFINITION_KEYS = (
    "report_id",
    "name",
    "description",
    "partition_field",
    "time_field",
    "sections",
)


class ReportDefinitionError(ValueError):
    """A report definition failed schema or guardrail validation."""


def row_count_key(section_seq: int, row_seq: int) -> str:
    """Return the stable per-row identity key used for run counts.

    A run keys every row's count by its section seq and row seq -- rendered as
    ``"S<section seq>:R<row seq>"`` -- rather than by its label. This lets two sections
    reuse the same row label without one row's count masking another's, because the
    identity is derived from the definition's own ordering keys (which are already unique:
    section seq is unique across the definition and row seq is unique within a section).
    The CSV formatter, the runner's aggregate counts and persisted ``rowCounts``, and the
    grid UI all resolve counts through this same key.
    """
    return f"S{section_seq}:R{row_seq}"


def _references_reserved_field(node: Any) -> bool:
    """Return True if a reserved partition/time field appears anywhere in a query clause."""
    if isinstance(node, dict):
        return any(
            _token_is_reserved(key) or _references_reserved_field(value)
            for key, value in node.items()
        )
    if isinstance(node, list):
        return any(_references_reserved_field(item) for item in node)
    if isinstance(node, str):
        return _token_is_reserved(node)
    return False


def _token_is_reserved(text: str) -> bool:
    # Field references appear as dotted paths (for example ROOT.PID...), so inspect each part
    # as well as the whole token to reject any use of the reserved fields.
    if text in RESERVED_QUERY_FIELDS:
        return True
    return any(part in RESERVED_QUERY_FIELDS for part in text.split("."))


def _require_keys(data: Mapping[str, Any], allowed: tuple[str, ...]) -> None:
    """Reject mappings with unknown or missing keys, mirroring extra="forbid" + required."""
    keys = set(data.keys())
    allowed_set = set(allowed)
    if keys - allowed_set or allowed_set - keys:
        raise ReportDefinitionError(INVALID_DEFINITION)


def _require_str(value: Any, *, min_length: int = 0) -> str:
    if not isinstance(value, str) or len(value) < min_length:
        raise ReportDefinitionError(INVALID_DEFINITION)
    return value


def _require_positive_int(value: Any) -> int:
    # bool is an int subclass; reject it so only genuine integers pass the seq contract.
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ReportDefinitionError(INVALID_DEFINITION)
    return value


def _validate_query(value: Any) -> dict[str, Any] | None:
    """Return a deep copy of a non-empty query object, or ``None`` for a placeholder row.

    ``None`` is accepted verbatim as a placeholder query. A non-null query must be a
    non-empty object that does not reference the reserved partition or time field; the
    reserved-field guardrail therefore applies only when a query is present.
    """
    if value is None:
        return None
    if not isinstance(value, dict) or not value:
        raise ReportDefinitionError(INVALID_DEFINITION)
    if _references_reserved_field(value):
        raise ReportDefinitionError(RESERVED_QUERY_FIELD)
    return copy.deepcopy(value)


@dataclass(frozen=True)
class ReportRow:
    """One counted row within a section, backed by a single index query.

    A row whose ``query`` is ``None`` is a placeholder: it holds a fixed position in the
    report grid (label and grid coordinates preserved) but has no query yet, so the runner
    never counts it and its count renders blank. The reserved partition/time guardrail is
    applied only to a non-null query; a placeholder carries no query to guard.
    """

    seq: int
    label: str
    description: str
    index: str
    query: dict[str, Any] | None

    @classmethod
    def _from_mapping(cls, data: Mapping[str, Any]) -> ReportRow:
        _require_keys(data, _ROW_KEYS)
        seq = _require_positive_int(data["seq"])
        label = _require_str(data["label"], min_length=1)
        if not label.strip():
            raise ReportDefinitionError(BLANK_LABEL)
        description = _require_str(data["description"])
        index = _require_str(data["index"])
        if index not in ALLOWED_INDEXES:
            raise ReportDefinitionError(DISALLOWED_INDEX)
        query = _validate_query(data["query"])
        return cls(
            seq=seq,
            label=label,
            description=description,
            index=index,
            query=query,
        )

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:  # noqa: ARG002
        """Return a JSON-compatible mapping of this row's fields, preserving a null query."""
        return {
            "seq": self.seq,
            "label": self.label,
            "description": self.description,
            "index": self.index,
            "query": copy.deepcopy(self.query),
        }


@dataclass(frozen=True)
class ReportSection:
    """A named, ordered group of rows rendered as one CSV column pair."""

    seq: int
    name: str
    rows: tuple[ReportRow, ...]

    @classmethod
    def _from_mapping(cls, data: Mapping[str, Any]) -> ReportSection:
        _require_keys(data, _SECTION_KEYS)
        seq = _require_positive_int(data["seq"])
        name = _require_str(data["name"], min_length=1)
        if not name.strip():
            raise ReportDefinitionError(BLANK_SECTION_NAME)
        raw_rows = data["rows"]
        if not isinstance(raw_rows, list) or not raw_rows:
            raise ReportDefinitionError(INVALID_DEFINITION)
        rows = tuple(ReportRow._from_mapping(_as_mapping(row)) for row in raw_rows)
        row_seqs = [row.seq for row in rows]
        if len(set(row_seqs)) != len(row_seqs):
            raise ReportDefinitionError(DUPLICATE_ROW_SEQ)
        return cls(seq=seq, name=name, rows=rows)

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """Return a JSON-compatible mapping of this section and its rows."""
        return {
            "seq": self.seq,
            "name": self.name,
            "rows": [row.model_dump(mode=mode) for row in self.rows],
        }


@dataclass(frozen=True)
class ReportDefinition:
    """A complete Reports definition with fixed partition/time fields and ordered sections."""

    report_id: str
    name: str
    description: str
    partition_field: str
    time_field: str
    sections: tuple[ReportSection, ...]

    @classmethod
    def _from_mapping(cls, data: Mapping[str, Any]) -> ReportDefinition:
        _require_keys(data, _DEFINITION_KEYS)
        report_id = _require_str(data["report_id"], min_length=1)
        name = _require_str(data["name"], min_length=1)
        description = _require_str(data["description"])
        partition_field = _require_str(data["partition_field"])
        if partition_field != PARTITION_FIELD:
            raise ReportDefinitionError(FIXED_PARTITION_FIELD)
        time_field = _require_str(data["time_field"])
        if time_field != TIME_FIELD:
            raise ReportDefinitionError(FIXED_TIME_FIELD)
        raw_sections = data["sections"]
        if not isinstance(raw_sections, list) or not raw_sections:
            raise ReportDefinitionError(INVALID_DEFINITION)
        sections = tuple(
            ReportSection._from_mapping(_as_mapping(section)) for section in raw_sections
        )
        section_seqs = [section.seq for section in sections]
        if len(set(section_seqs)) != len(section_seqs):
            raise ReportDefinitionError(DUPLICATE_SECTION_SEQ)
        # Row labels are intentionally NOT required to be unique across sections. Runs key
        # each row's count by a stable section/row identity (see :func:`row_count_key`), so
        # two sections may reuse the same label without one row's count masking another's.
        return cls(
            report_id=report_id,
            name=name,
            description=description,
            partition_field=partition_field,
            time_field=time_field,
            sections=sections,
        )

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        """Return a JSON-compatible mapping of the whole definition."""
        return {
            "report_id": self.report_id,
            "name": self.name,
            "description": self.description,
            "partition_field": self.partition_field,
            "time_field": self.time_field,
            "sections": [section.model_dump(mode=mode) for section in self.sections],
        }


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReportDefinitionError(INVALID_DEFINITION)
    return value


def load_report_definition(data: Mapping[str, Any]) -> ReportDefinition:
    """Validate a mapping into a ReportDefinition, raising ReportDefinitionError on failure."""
    if not isinstance(data, Mapping):
        raise ReportDefinitionError(INVALID_DEFINITION)
    return ReportDefinition._from_mapping(data)


def load_report_row(data: Mapping[str, Any]) -> ReportRow:
    """Validate a single row mapping into a :class:`ReportRow`.

    The row must carry exactly the row schema (``seq``, ``label``, ``description``,
    ``index``, ``query``) and its query must not reference the reserved partition or time
    field. This is the row-granular counterpart to :func:`load_report_definition`, used by
    the catalog to validate one row on an add or update without re-reading a whole
    definition. It raises :class:`ReportDefinitionError` on any schema or guardrail
    violation.
    """
    if not isinstance(data, Mapping):
        raise ReportDefinitionError(INVALID_DEFINITION)
    return ReportRow._from_mapping(data)


def load_report_definition_json(text: str) -> ReportDefinition:
    """Parse and validate a report definition from a JSON document string."""
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as error:
        raise ReportDefinitionError(INVALID_DEFINITION) from error
    if not isinstance(parsed, Mapping):
        raise ReportDefinitionError(INVALID_DEFINITION)
    return load_report_definition(parsed)


def load_schema() -> dict[str, Any]:
    """Load the checked-in JSON Schema for report definitions."""
    schema: dict[str, Any] = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return schema
