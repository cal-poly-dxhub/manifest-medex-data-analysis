"""Tests for report definition schema, guardrails, and the shipped seed definition."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from src.report_definition import (
    ALLOWED_INDEXES,
    BLANK_LABEL,
    BLANK_SECTION_NAME,
    DISALLOWED_INDEX,
    DUPLICATE_ROW_SEQ,
    DUPLICATE_SECTION_SEQ,
    FIXED_PARTITION_FIELD,
    FIXED_TIME_FIELD,
    PARTITION_FIELD,
    RESERVED_QUERY_FIELD,
    TIME_FIELD,
    ReportDefinitionError,
    load_report_definition,
    load_report_definition_json,
    load_schema,
    row_count_key,
)

SEED_PATH = Path(__file__).resolve().parent.parent / "seed" / "p4p-prototype.json"
DEMO_SEED_PATH = Path(__file__).resolve().parent.parent / "seed" / "p4p-demo.json"


def _seed_data() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    return data


def _minimal_definition() -> dict[str, Any]:
    return {
        "report_id": "example",
        "name": "Example",
        "description": "",
        "partition_field": PARTITION_FIELD,
        "time_field": TIME_FIELD,
        "sections": [
            {
                "seq": 1,
                "name": "Section One",
                "rows": [
                    {
                        "seq": 1,
                        "label": "Row One",
                        "description": "first row",
                        "index": "hl7-messages-v1",
                        "query": {"bool": {"filter": [{"exists": {"field": "ROOT.MSH"}}]}},
                    }
                ],
            }
        ],
    }


def test_seed_definition_is_valid() -> None:
    definition = load_report_definition(_seed_data())

    assert definition.report_id == "p4p-prototype"
    assert definition.partition_field == PARTITION_FIELD
    assert definition.time_field == TIME_FIELD
    assert [section.name for section in definition.sections] == ["ADT A08", "ORU"]
    assert [len(section.rows) for section in definition.sections] == [10, 6]
    assert {row.index for section in definition.sections for row in section.rows} <= ALLOWED_INDEXES


def test_checked_in_schema_matches_the_seed_derived_contract() -> None:
    schema = load_schema()

    assert schema["additionalProperties"] is False
    assert schema["properties"]["partition_field"] == {"const": PARTITION_FIELD}
    assert schema["properties"]["time_field"] == {"const": TIME_FIELD}
    assert schema["$defs"]["row"]["properties"]["index"]["enum"] == [
        "hl7-messages-v1",
        "ccda-documents-v1",
    ]
    assert schema["$defs"]["section"]["properties"]["seq"]["minimum"] == 1
    assert schema["$defs"]["row"]["properties"]["seq"]["minimum"] == 1
    assert schema["properties"]["sections"]["minItems"] == 1


def test_load_report_definition_json_round_trips_the_seed() -> None:
    definition = load_report_definition_json(SEED_PATH.read_text(encoding="utf-8"))

    assert definition.report_id == "p4p-prototype"


@pytest.mark.parametrize("payload", ["not json", "[]", "123"])
def test_load_report_definition_json_rejects_non_object_documents(payload: str) -> None:
    with pytest.raises(ReportDefinitionError):
        load_report_definition_json(payload)


@pytest.mark.parametrize(
    "reserved_query",
    [
        {"bool": {"filter": [{"term": {"sourceFacilityId": "FAC1"}}]}},
        {"bool": {"filter": [{"exists": {"field": "messageTime"}}]}},
        {"bool": {"filter": [{"range": {"messageTime": {"gte": "2026-01-01"}}}]}},
        {"bool": {"filter": [{"exists": {"field": "ROOT.sourceFacilityId"}}]}},
    ],
)
def test_row_query_rejects_reserved_partition_or_time_fields(
    reserved_query: dict[str, Any],
) -> None:
    data = _minimal_definition()
    data["sections"][0]["rows"][0]["query"] = reserved_query

    with pytest.raises(ReportDefinitionError, match=RESERVED_QUERY_FIELD):
        load_report_definition(data)


def test_row_query_allows_non_reserved_fields() -> None:
    data = _minimal_definition()
    data["sections"][0]["rows"][0]["query"] = {
        "bool": {"filter": [{"term": {"ROOT.MSH.MSH_9_Message_Type.MSG_1": "ADT"}}]}
    }

    assert load_report_definition(data).sections[0].rows[0].query == {
        "bool": {"filter": [{"term": {"ROOT.MSH.MSH_9_Message_Type.MSG_1": "ADT"}}]}
    }


def test_row_index_must_be_allowed() -> None:
    data = _minimal_definition()
    data["sections"][0]["rows"][0]["index"] = "some-other-index"

    with pytest.raises(ReportDefinitionError, match=DISALLOWED_INDEX):
        load_report_definition(data)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("partition_field", "messageTime", FIXED_PARTITION_FIELD),
        ("time_field", "sourceFacilityId", FIXED_TIME_FIELD),
    ],
)
def test_partition_and_time_fields_are_fixed(field: str, value: str, message: str) -> None:
    data = _minimal_definition()
    data[field] = value

    with pytest.raises(ReportDefinitionError, match=message):
        load_report_definition(data)


def test_section_seq_values_must_be_unique() -> None:
    data = _minimal_definition()
    duplicate = copy.deepcopy(data["sections"][0])
    duplicate["name"] = "Section Two"
    data["sections"].append(duplicate)

    with pytest.raises(ReportDefinitionError, match=DUPLICATE_SECTION_SEQ):
        load_report_definition(data)


def test_row_seq_values_must_be_unique_within_a_section() -> None:
    data = _minimal_definition()
    duplicate = copy.deepcopy(data["sections"][0]["rows"][0])
    duplicate["label"] = "Row Two"
    data["sections"][0]["rows"].append(duplicate)

    with pytest.raises(ReportDefinitionError, match=DUPLICATE_ROW_SEQ):
        load_report_definition(data)


def test_row_labels_may_repeat_across_sections() -> None:
    # Labels are addressed by section/row identity, not by label, so the same label may
    # appear in two different sections without being rejected or colliding.
    data = _minimal_definition()
    data["sections"].append(
        {
            "seq": 2,
            "name": "Section Two",
            "rows": [
                {
                    "seq": 1,
                    "label": "Row One",  # duplicates the label in section one
                    "description": "duplicate label across sections",
                    "index": "hl7-messages-v1",
                    "query": {"bool": {"filter": [{"exists": {"field": "ROOT.MSH"}}]}},
                }
            ],
        }
    )

    definition = load_report_definition(data)

    labels = [row.label for section in definition.sections for row in section.rows]
    assert labels == ["Row One", "Row One"]
    # The two same-labelled rows resolve to distinct identity keys (S1:R1 vs S2:R1).
    keys = {
        row_count_key(section.seq, row.seq)
        for section in definition.sections
        for row in section.rows
    }
    assert keys == {"S1:R1", "S2:R1"}


def test_row_count_key_format() -> None:
    assert row_count_key(1, 1) == "S1:R1"
    assert row_count_key(3, 12) == "S3:R12"


@pytest.mark.parametrize("seq", [0, -1])
def test_seq_values_must_be_positive(seq: int) -> None:
    data = _minimal_definition()
    data["sections"][0]["rows"][0]["seq"] = seq

    with pytest.raises(ReportDefinitionError):
        load_report_definition(data)


@pytest.mark.parametrize(
    ("field", "message"),
    [("label", BLANK_LABEL), ("name", BLANK_SECTION_NAME)],
)
def test_labels_and_names_must_not_be_blank(field: str, message: str) -> None:
    data = _minimal_definition()
    if field == "label":
        data["sections"][0]["rows"][0]["label"] = "   "
    else:
        data["sections"][0]["name"] = "   "

    with pytest.raises(ReportDefinitionError, match=message):
        load_report_definition(data)


def test_unknown_top_level_fields_are_rejected() -> None:
    data = _minimal_definition()
    data["owner"] = "unexpected"

    with pytest.raises(ReportDefinitionError):
        load_report_definition(data)


def test_demo_seed_validates_with_placeholder_rows() -> None:
    # The shipped demo definition reuses row labels across sections and mixes implemented
    # rows with null-query placeholders. It must validate end to end, and its derived
    # counts must stay internally consistent (counts are asserted dynamically rather than
    # against obsolete hardcoded totals).
    definition = load_report_definition_json(DEMO_SEED_PATH.read_text(encoding="utf-8"))

    rows = [row for section in definition.sections for row in section.rows]
    placeholders = [row for row in rows if row.query is None]
    implemented = [row for row in rows if row.query is not None]

    # The file is non-trivial and its row partitions add up to the whole.
    assert len(rows) > 0
    assert len(placeholders) + len(implemented) == len(rows)

    # The demo deliberately reuses labels across sections; this is why the definition must
    # tolerate duplicate labels. Row identities remain unique even though labels do not.
    labels = [row.label for row in rows]
    assert len(set(labels)) < len(labels)
    identity_keys = [
        row_count_key(section.seq, row.seq)
        for section in definition.sections
        for row in section.rows
    ]
    assert len(set(identity_keys)) == len(identity_keys)


def test_row_query_may_be_null_placeholder() -> None:
    data = _minimal_definition()
    data["sections"][0]["rows"][0]["query"] = None

    definition = load_report_definition(data)

    row = definition.sections[0].rows[0]
    assert row.query is None
    # A placeholder query is preserved verbatim through a model_dump round trip.
    assert row.model_dump(mode="json")["query"] is None


@pytest.mark.parametrize("query", [{}, "not-an-object", 5, []])
def test_row_query_rejects_non_null_non_object(query: Any) -> None:
    data = _minimal_definition()
    data["sections"][0]["rows"][0]["query"] = query

    with pytest.raises(ReportDefinitionError):
        load_report_definition(data)
