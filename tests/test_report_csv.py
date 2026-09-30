"""Tests for the side-by-side report CSV formatter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from src.report_csv import (
    CRLF,
    MISSING_COUNT,
    build_report_rows,
    format_report_csv,
    format_report_csv_bytes,
)
from src.report_definition import ReportDefinition, load_report_definition

SEED_PATH = Path(__file__).resolve().parent.parent / "seed" / "p4p-prototype.json"

# Deterministic counts for every seed label. TotalA08 is zero to prove 0 renders as "0".
SEED_COUNTS: dict[str, int] = {
    "PID-3.1": 100,
    "PID-5.1": 95,
    "PID-7.1": 90,
    "PID-8": 85,
    "PV1-2": 80,
    "PV1-PROVIDERS": 70,
    "NK1-3.1": 60,
    "TotalNK1": 55,
    "TotalPV2": 40,
    "TotalA08": 0,
    "OBRSegment": 200,
    "LabDocument": 150,
    "RadiologyDocument": 30,
    "PathologyDocument": 10,
    "OBX-11(F/C)": 180,
    "TotalORU": 220,
}


def _seed_definition() -> ReportDefinition:
    data: dict[str, Any] = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    return load_report_definition(data)


def test_header_row_holds_section_names_with_blank_count_headings() -> None:
    grid = build_report_rows(_seed_definition(), SEED_COUNTS)

    assert grid[0] == ["ADT A08", "", "ORU", ""]


def test_grid_places_labels_and_counts_in_expected_columns() -> None:
    grid = build_report_rows(_seed_definition(), SEED_COUNTS)

    # 1 header + 10 rows (the longer ADT A08 section drives the row count).
    assert len(grid) == 11
    assert all(len(line) == 4 for line in grid)

    # First data row: both sections still have rows, side by side.
    assert grid[1] == ["PID-3.1", "100", "OBRSegment", "200"]

    # Label columns are 0 and 2; count columns are 1 and 3.
    assert [line[0] for line in grid[1:]] == [
        "PID-3.1",
        "PID-5.1",
        "PID-7.1",
        "PID-8",
        "PV1-2",
        "PV1-PROVIDERS",
        "NK1-3.1",
        "TotalNK1",
        "TotalPV2",
        "TotalA08",
    ]
    assert [line[2] for line in grid[1:]] == [
        "OBRSegment",
        "LabDocument",
        "RadiologyDocument",
        "PathologyDocument",
        "OBX-11(F/C)",
        "TotalORU",
        "",
        "",
        "",
        "",
    ]


def test_shorter_section_is_padded_with_two_empty_cells() -> None:
    grid = build_report_rows(_seed_definition(), SEED_COUNTS)

    # ORU has 6 rows; positions 7-10 (grid rows 7-10) are padded with two empty cells.
    assert grid[6] == ["PV1-PROVIDERS", "70", "TotalORU", "220"]
    assert grid[7] == ["NK1-3.1", "60", "", ""]
    assert grid[10] == ["TotalA08", "0", "", ""]


def test_zero_count_is_rendered_as_zero() -> None:
    grid = build_report_rows(_seed_definition(), SEED_COUNTS)

    total_a08 = next(line for line in grid if line[0] == "TotalA08")
    assert total_a08[1] == "0"


def test_csv_uses_commas_and_crlf_line_endings_with_golden_first_lines() -> None:
    text = format_report_csv(_seed_definition(), SEED_COUNTS)
    lines = text.split(CRLF)

    assert lines[0] == "ADT A08,,ORU,"
    assert lines[1] == "PID-3.1,100,OBRSegment,200"
    assert lines[7] == "NK1-3.1,60,,"
    assert lines[10] == "TotalA08,0,,"
    # csv writer terminates the final record, producing a trailing CRLF.
    assert text.endswith(CRLF)
    assert lines[-1] == ""


def test_csv_bytes_are_utf8_encoded() -> None:
    text = format_report_csv(_seed_definition(), SEED_COUNTS)

    assert format_report_csv_bytes(_seed_definition(), SEED_COUNTS) == text.encode("utf-8")


def _placeholder_definition() -> ReportDefinition:
    """A definition mixing an implemented row, a zero-count row, and a placeholder row."""
    return load_report_definition(
        {
            "report_id": "placeholders",
            "name": "Placeholders",
            "description": "",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": 1,
                    "name": "SecA",
                    "rows": [
                        {
                            "seq": 1,
                            "label": "Implemented",
                            "description": "counted",
                            "index": "hl7-messages-v1",
                            "query": {"bool": {"filter": [{"exists": {"field": "ROOT.MSH"}}]}},
                        },
                        {
                            "seq": 2,
                            "label": "ZeroCount",
                            "description": "counted but zero",
                            "index": "hl7-messages-v1",
                            "query": {"bool": {"filter": [{"exists": {"field": "ROOT.PID"}}]}},
                        },
                        {
                            "seq": 3,
                            "label": "Placeholder",
                            "description": "not yet implemented",
                            "index": "hl7-messages-v1",
                            "query": None,
                        },
                    ],
                }
            ],
        }
    )


def test_placeholder_renders_blank_while_zero_renders_zero_and_label_is_kept() -> None:
    definition = _placeholder_definition()
    # The placeholder label maps to None; the zero-count label maps to a real 0.
    counts: dict[str, int | None] = {
        "Implemented": 42,
        "ZeroCount": 0,
        "Placeholder": None,
    }

    grid = build_report_rows(definition, counts)

    # Implemented row shows its count; the label and grid position are always preserved.
    assert grid[1] == ["Implemented", "42"]
    # A real zero still renders as "0", never blank.
    assert grid[2] == ["ZeroCount", "0"]
    # A placeholder (query-null) row keeps its label but renders a blank count cell.
    assert grid[3] == ["Placeholder", ""]

    text = format_report_csv(definition, counts)
    lines = text.split(CRLF)
    assert lines[2] == "ZeroCount,0"
    assert lines[3] == "Placeholder,"


def test_missing_label_count_is_rejected() -> None:
    incomplete = dict(SEED_COUNTS)
    del incomplete["TotalORU"]

    with pytest.raises(ValueError, match=MISSING_COUNT):
        build_report_rows(_seed_definition(), incomplete)


def _duplicate_label_definition() -> ReportDefinition:
    """Two sections that reuse the same label ("Total") in each section."""
    return load_report_definition(
        {
            "report_id": "dupes",
            "name": "Duplicate labels",
            "description": "",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": 1,
                    "name": "SecA",
                    "rows": [
                        {
                            "seq": 1,
                            "label": "Total",
                            "description": "section A total",
                            "index": "hl7-messages-v1",
                            "query": {"bool": {"filter": [{"exists": {"field": "ROOT.MSH"}}]}},
                        }
                    ],
                },
                {
                    "seq": 2,
                    "name": "SecB",
                    "rows": [
                        {
                            "seq": 1,
                            "label": "Total",
                            "description": "section B total",
                            "index": "hl7-messages-v1",
                            "query": {"bool": {"filter": [{"exists": {"field": "ROOT.PID"}}]}},
                        }
                    ],
                },
            ],
        }
    )


def test_duplicate_labels_resolve_by_identity_without_collision() -> None:
    definition = _duplicate_label_definition()
    # Same label in two sections, but distinct identity keys carry distinct counts.
    counts: dict[str, int | None] = {"S1:R1": 11, "S2:R1": 22}

    grid = build_report_rows(definition, counts)

    assert grid[0] == ["SecA", "", "SecB", ""]
    # The reused label renders in both columns, each with its own section's count.
    assert grid[1] == ["Total", "11", "Total", "22"]

    text = format_report_csv(definition, counts)
    lines = text.split(CRLF)
    assert lines[1] == "Total,11,Total,22"


def test_duplicate_labels_reject_legacy_label_keys() -> None:
    # A label key is ambiguous when labels repeat, so only identity keys are accepted.
    definition = _duplicate_label_definition()

    with pytest.raises(ValueError, match=MISSING_COUNT):
        build_report_rows(definition, {"Total": 5})
