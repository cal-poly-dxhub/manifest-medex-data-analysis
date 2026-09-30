#!/usr/bin/env python3
"""Independent, deterministic audit of the HL7 field catalog against the Kibana export.

Development-only utility. It is never imported or executed by runtime code; loading this
module has no side effects. Run it manually to confirm that every label the catalog emits
exists verbatim as a ROOT.<segment>.<label> path in the customer's 2026-08-24 Kibana
export, and that the deprecated-but-observed fields the catalog now includes (DG1-8
Diagnostic Related Group and PR1-11 Surgeon) are present with their expected paths.

The export defaults to the sibling ``customer_delivery_aug_24`` delivery next to the
repository, or an explicit path may be supplied with ``--export``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.customer_field_catalog import HL7_FIELD_CATALOG

DEFAULT_EXPORT = (
    Path(__file__).resolve().parents[2] / "customer_delivery_aug_24" / "hl7 fields in Kibana.txt"
)

# Deprecated-but-observed legacy fields whose exact export paths must be present. These are
# the fields Task 4 adds, asserted explicitly so a future catalog change cannot drop them
# silently without this audit failing.
EXPECTED_PATHS: tuple[str, ...] = (
    "ROOT.DG1.DG1_8_Diagnostic_Related_Group",
    "ROOT.DG1.DG1_8_Diagnostic_Related_Group.CNE_1",
    "ROOT.DG1.DG1_8_Diagnostic_Related_Group.CNE_2",
    "ROOT.DG1.DG1_8_Diagnostic_Related_Group.CNE_3",
    "ROOT.PR1.PR1_11_Surgeon",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--export",
        type=Path,
        default=DEFAULT_EXPORT,
        help="Path to the customer 'hl7 fields in Kibana.txt' export",
    )
    return parser.parse_args()


def load_export_fields(export_path: Path) -> set[str]:
    """Return the set of dotted field paths declared in the customer export."""
    data = json.loads(export_path.read_text())
    return set(data["fields"].keys())


def missing_expected_paths(fields: set[str]) -> list[str]:
    """Return the explicitly expected DG1-8/PR1-11 paths absent from the export."""
    return [path for path in EXPECTED_PATHS if path not in fields]


def missing_catalog_labels(fields: set[str]) -> list[str]:
    """Return every catalog label whose ROOT.<segment>.<label> path is absent from export."""
    missing: list[str] = []
    for segment, entries in HL7_FIELD_CATALOG.items():
        for number, (label, _datatype) in entries.items():
            path = f"ROOT.{segment}.{label}"
            if path not in fields:
                missing.append(f"{segment}-{number}: {path}")
    return missing


def audit(export_path: Path) -> int:
    if not export_path.exists():
        print(f"export not found: {export_path}", file=sys.stderr)
        return 2

    fields = load_export_fields(export_path)

    expected_gaps = missing_expected_paths(fields)
    label_gaps = missing_catalog_labels(fields)

    print(f"export fields: {len(fields)}")
    print(f"catalog labels: {sum(len(v) for v in HL7_FIELD_CATALOG.values())}")

    print(
        f"expected DG1-8/PR1-11 paths present: {len(EXPECTED_PATHS) - len(expected_gaps)}"
        f"/{len(EXPECTED_PATHS)}"
    )
    for path in expected_gaps:
        print("  MISSING EXPECTED", path)

    print(
        f"catalog labels verified in export: "
        f"{sum(len(v) for v in HL7_FIELD_CATALOG.values()) - len(label_gaps)}"
    )
    for entry in label_gaps:
        print("  MISSING LABEL", entry)

    if expected_gaps or label_gaps:
        return 1
    print("OK: catalog is consistent with the customer export")
    return 0


def main() -> int:
    args = parse_args()
    return audit(args.export.expanduser().resolve())


if __name__ == "__main__":
    raise SystemExit(main())
