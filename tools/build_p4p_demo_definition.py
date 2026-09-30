"""Build the full-layout P4P demo definition from the customer workbook layout.

seed/import_data_layout.json is extracted from the 'Import Data' tab of the customer's
'sample Participant_Report_v2a_2026 no macros.xlsx' and is the authoritative section and
row layout (10 sections, ~610 rows). This script generates seed/p4p-demo.json from it:

- Labels of the form SEG-N or SEG-N.C get exists-queries with paths resolved through
  src.customer_field_catalog (guaranteed to match parser output).
- IE-suffixed labels additionally scope to PV1-2 in {I, E} (per the customer's ADT
  Detailed Report notes).
- Total<SEG> labels get segment-presence queries; a small table handles the other known
  specials (TotalA08, TotalInPatientVisit, PV1-PROVIDERS, ...).
- Everything else becomes a placeholder (query: null) rendering as a blank count.

Section filters are derived from the section name (message type / trigger event).
Duplicate labels within a section (the customer sheet repeats PV1-36/PV1-37.1 in A08)
keep only their first occurrence.

Run: uv run python tools/build_p4p_demo_definition.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.customer_field_catalog import HL7_DATATYPE_CATALOG, HL7_FIELD_CATALOG

ROOT = Path(__file__).resolve().parents[1]
LAYOUT = ROOT / "seed" / "import_data_layout.json"
OUT = ROOT / "seed" / "p4p-demo.json"
INDEX = "hl7-messages-v1"

MSG1 = "ROOT.MSH.MSH_9_Message_Type.MSG_1"
MSG2 = "ROOT.MSH.MSH_9_Message_Type.MSG_2"
PV1_2 = "ROOT.PV1.PV1_2_Patient_Class"
PV1_45 = "ROOT.PV1.PV1_45_Discharge_Date-Time"
MSH_1 = "ROOT.MSH.MSH_1_Field_Separator"

# Section name -> base filters. Trigger-event scoping mirrors the customer's section
# semantics (admits A01/A04/A06, discharges A03, per the ADT Detailed Report notes);
# ADT (OTHER) = ADT excluding the trigger events owned by the other ADT sections.
ADT_ADMIT_TRIGGERS = ["A01", "A04", "A06"]
SECTION_FILTERS: dict[str, list[dict[str, Any]]] = {
    "ADT (OTHER)": [
        {"term": {MSG1: "ADT"}},
        {"bool": {"must_not": [{"terms": {MSG2: ["A08", "A03", *ADT_ADMIT_TRIGGERS]}}]}},
    ],
    "ADT A08": [{"term": {MSG1: "ADT"}}, {"term": {MSG2: "A08"}}],
    "ADT ADMIT Inpatient": [
        {"term": {MSG1: "ADT"}},
        {"terms": {MSG2: ADT_ADMIT_TRIGGERS}},
        {"term": {PV1_2: "I"}},
    ],
    "ADT ADMIT OTHER": [
        {"term": {MSG1: "ADT"}},
        {"terms": {MSG2: ADT_ADMIT_TRIGGERS}},
        {"bool": {"must_not": [{"term": {PV1_2: "I"}}]}},
    ],
    "ADT DISCHARGE": [{"term": {MSG1: "ADT"}}, {"term": {MSG2: "A03"}}],
    "ORU": [{"term": {MSG1: "ORU"}}],
    "RDE": [{"term": {MSG1: "RDE"}}],
    "ORU Chart Notes": [{"term": {MSG1: "ORU"}}],
    "VXU": [{"term": {MSG1: "VXU"}}],
    "MDM": [{"term": {MSG1: "MDM"}}],
}

IE = {"terms": {PV1_2: ["I", "E"]}}
PLACEHOLDER_NOTE = "Prism logic not yet provided - placeholder pending customer row-query export"
FIELD_LABEL = re.compile(r"^([A-Z][A-Z0-9]{2})-(\d+)(?:\.(\d+))?(IE)?$")

warnings: list[str] = []


def field_path(seg: str, num: int, comp: int | None) -> str | None:
    entry = HL7_FIELD_CATALOG.get(seg, {}).get(num)
    if entry is None:
        return None
    label, datatype = entry
    base = f"ROOT.{seg}.{label}"
    if comp is None:
        return base
    components = HL7_DATATYPE_CATALOG.get(datatype or "", ())
    for key, _nested in components:
        if key.endswith(f"_{comp}"):
            return f"{base}.{key}"
    if comp == 1 and datatype is None:
        return base  # plain field; ".1" of a leaf is the leaf itself
    warnings.append(f"{seg}-{num}.{comp}: component not in catalog -> parent exists")
    return base


def q(filters: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    return {"bool": {"filter": filters, **extra}}


def exists(path: str) -> dict[str, Any]:
    return {"exists": {"field": path}}


def providers_row(base: list[dict[str, Any]]) -> dict[str, Any]:
    should = [
        exists(field_path("PV1", n, 1) or "")
        for n in (7, 8, 9, 17)
    ]
    return q(base, should=should, minimum_should_match=1)


def special_query(label: str, base: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Known non-field labels. Returns None when the logic is unknown (placeholder)."""
    if label.startswith("Total"):
        rest = label[5:]
        if rest in HL7_FIELD_CATALOG:
            return q([*base, exists(f"ROOT.{rest}")])
        table: dict[str, dict[str, Any] | None] = {
            "InPatientVisit": q([*base, {"term": {PV1_2: "I"}}]),
            "EDVisit": q([*base, {"term": {PV1_2: "E"}}]),
            "A08": q([*base, exists(MSH_1)]),
            "A08-DischargeDate": q([*base, exists(PV1_45)]),
            "PV1-45IE": q([*base, IE, exists(PV1_45)]),
        }
        if rest in table:
            return table[rest]
        # TotalADT, TotalORU, TotalRDE, TotalVXU, TotalMDM style denominators
        if rest in ("ADT", "ORU", "RDE", "VXU", "MDM", "InpatientVisit"):
            return q([*base, exists(MSH_1)])
        return None
    if label == "PV1-PROVIDERS":
        return providers_row(base)
    return None


def build_row(label: str, base: list[dict[str, Any]]) -> dict[str, Any]:
    row: dict[str, Any] = {"label": label, "index": INDEX}
    match = FIELD_LABEL.match(label)
    if match:
        seg, num, comp, ie_flag = (
            match.group(1),
            int(match.group(2)),
            int(match.group(3)) if match.group(3) else None,
            match.group(4),
        )
        path = field_path(seg, num, comp)
        if path is not None:
            filters = [*base, *( [IE] if ie_flag else [] ), exists(path)]
            row["description"] = f"Messages where {path} is populated" + (
                ", scoped to inpatient/emergency (PV1-2 in I,E)" if ie_flag else ""
            )
            row["query"] = q(filters)
            return row
        warnings.append(f"{label}: {seg}-{num} not in catalog -> placeholder")
    else:
        special = special_query(label, base)
        if special is not None:
            row["description"] = f"Derived row '{label}' per customer spreadsheet semantics"
            row["query"] = special
            return row
    row["description"] = PLACEHOLDER_NOTE
    row["query"] = None
    return row


def main() -> None:
    layout: dict[str, list[str]] = json.loads(LAYOUT.read_text())
    order = list(layout.keys())
    sections = []
    for section_index, name in enumerate(order, start=1):
        base = SECTION_FILTERS[name]
        seen: set[str] = set()
        rows = []
        for label in layout[name]:
            if label in seen:
                warnings.append(f"{name}: duplicate row label {label!r} dropped")
                continue
            seen.add(label)
            rows.append(build_row(label, base))
        sections.append(
            {
                "seq": section_index,
                "name": name,
                "rows": [{"seq": (i + 1) * 10, **row} for i, row in enumerate(rows)],
            }
        )
    definition = {
        "report_id": "p4p-demo",
        "name": "P4P Demo (full customer layout)",
        "description": (
            "Full Import Data layout from 'sample Participant_Report_v2a_2026'. "
            "Rows without confirmed Prism logic are placeholders (blank counts) "
            "pending the customer's row-query export. Section trigger-event scoping "
            "per the ADT Detailed Report notes."
        ),
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": sections,
    }
    OUT.write_text(json.dumps(definition, indent=2) + "\n", encoding="utf-8")
    implemented = sum(1 for s in sections for r in s["rows"] if r["query"])
    blank = sum(1 for s in sections for r in s["rows"] if not r["query"])
    print(f"wrote {OUT.name}: {len(sections)} sections, {implemented} implemented, {blank} placeholders")
    for w in sorted(set(warnings)):
        print("  note:", w)


if __name__ == "__main__":
    main()
