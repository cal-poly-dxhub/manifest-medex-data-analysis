"""Render a report definition and its per-row counts as a side-by-side CSV table.

Each section becomes an adjacent pair of columns: a label column and a count column.
Sections are ordered by seq and, within a section, rows are ordered by seq. The first
row carries the section names (with a blank count heading); each following row carries a
row label and its count. Sections with fewer rows are padded with two empty cells so the
columns stay aligned. A count of zero is rendered as ``0`` rather than left blank, while a
placeholder row (one whose query is ``None``, surfaced as a ``None`` row count) renders a
blank count cell but keeps its label and grid position.

Counts are keyed by each row's stable identity (:func:`row_count_key`), so two sections may
reuse the same row label without one row's count masking another's. For backward
compatibility with pre-identity runs and prototype tests, a legacy label-keyed mapping is
also accepted, but only when every row label in the definition is unique; otherwise a label
key would be ambiguous and is rejected.

The output is comma-delimited CSV with CRLF line endings. The returned text is UTF-8
encodable; ``format_report_csv_bytes`` returns the UTF-8 encoded document directly.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Mapping

from src.report_definition import ReportDefinition, ReportSection, row_count_key

CRLF = "\r\n"
MISSING_COUNT = "label count is missing for a report row"


def _labels_are_unique(definition: ReportDefinition) -> bool:
    labels = [row.label for section in definition.sections for row in section.rows]
    return len(set(labels)) == len(labels)


def build_report_rows(
    definition: ReportDefinition,
    row_counts: Mapping[str, int | None],
) -> list[list[str]]:
    """Build the CSV grid (including the header row) for a definition and its counts.

    ``row_counts`` is keyed by row identity (:func:`row_count_key`). A legacy label-keyed
    mapping is also accepted, but only when every row label in the definition is unique. A
    key mapped to ``None`` is a placeholder row: its count cell renders blank while its label
    and grid position are preserved. A real count of ``0`` still renders as ``0``.
    """
    allow_label_keys = _labels_are_unique(definition)
    sections = sorted(definition.sections, key=lambda section: section.seq)
    ordered_rows = [sorted(section.rows, key=lambda row: row.seq) for section in sections]

    grid: list[list[str]] = [_header_row(sections)]
    row_count = max((len(rows) for rows in ordered_rows), default=0)
    for position in range(row_count):
        line: list[str] = []
        for section, rows in zip(sections, ordered_rows, strict=True):
            if position < len(rows):
                row = rows[position]
                identity = row_count_key(section.seq, row.seq)
                if identity in row_counts:
                    count = row_counts[identity]
                elif allow_label_keys and row.label in row_counts:
                    count = row_counts[row.label]
                else:
                    raise ValueError(MISSING_COUNT)
                line.extend([row.label, "" if count is None else str(count)])
            else:
                line.extend(["", ""])
        grid.append(line)
    return grid


def format_report_csv(
    definition: ReportDefinition,
    row_counts: Mapping[str, int | None],
) -> str:
    """Render the report as comma-delimited CSV text with CRLF line endings."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator=CRLF)
    writer.writerows(build_report_rows(definition, row_counts))
    return buffer.getvalue()


def format_report_csv_bytes(
    definition: ReportDefinition,
    row_counts: Mapping[str, int | None],
) -> bytes:
    """Render the report as a UTF-8 encoded CSV document."""
    return format_report_csv(definition, row_counts).encode("utf-8")


def _header_row(sections: list[ReportSection]) -> list[str]:
    header: list[str] = []
    for section in sections:
        header.extend([section.name, ""])
    return header
