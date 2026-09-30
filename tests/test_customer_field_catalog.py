import re

from src.customer_field_catalog import (
    CCDA_SECTION_TITLE_CATALOG,
    CUSTOMER_CCDA_EXPORT_FIELD_COUNT,
    CUSTOMER_CCDA_EXPORT_SHA256,
    CUSTOMER_HL7_EXPORT_FIELD_COUNT,
    CUSTOMER_HL7_EXPORT_SHA256,
    HL7_DATATYPE_CATALOG,
    HL7_FIELD_CATALOG,
)


def test_customer_catalog_records_exact_export_provenance() -> None:
    assert CUSTOMER_HL7_EXPORT_FIELD_COUNT == 3_513
    assert CUSTOMER_CCDA_EXPORT_FIELD_COUNT == 33_509
    assert re.fullmatch(r"[0-9a-f]{64}", CUSTOMER_HL7_EXPORT_SHA256)
    assert re.fullmatch(r"[0-9a-f]{64}", CUSTOMER_CCDA_EXPORT_SHA256)


def test_hl7_catalog_contains_only_unambiguous_standard_customer_fields() -> None:
    assert len(HL7_FIELD_CATALOG) == 30
    assert sum(len(fields) for fields in HL7_FIELD_CATALOG.values()) == 623
    assert len(HL7_DATATYPE_CATALOG) == 44

    assert HL7_FIELD_CATALOG["MSH"][7][0] == "MSH_7_Date-Time_of_Message"
    assert HL7_FIELD_CATALOG["AL1"][3][0] == "AL1_3_Allergen_Code-Mnemonic-Description"
    assert HL7_FIELD_CATALOG["DG1"][3][0] == "DG1_3_Diagnosis_Code_-_DG1"
    assert HL7_FIELD_CATALOG["DG1"][8] == ("DG1_8_Diagnostic_Related_Group", "CNE")
    assert HL7_FIELD_CATALOG["PR1"][11] == ("PR1_11_Surgeon", "XCN")
    assert HL7_FIELD_CATALOG["ORC"][12][0] == "ORC_12_Ordering_Provider"

    # The datatypes backing the newly added deprecated-but-observed fields must be defined
    # with at least their first three components so the parser can project them.
    for component in ("CNE_1", "CNE_2", "CNE_3"):
        assert component in {name for name, _ in HL7_DATATYPE_CATALOG["CNE"]}
    assert HL7_DATATYPE_CATALOG["XCN"][0][0] == "XCN_1"

    excluded_suffixes = ("_resolution", "_text", "_string", "_large")
    for segment, fields in HL7_FIELD_CATALOG.items():
        assert re.fullmatch(r"[A-Z][A-Z0-9]{2}", segment)
        for number, (label, _datatype) in fields.items():
            assert number >= 1
            assert label.startswith(f"{segment}_{number}_")
            assert not label.endswith(excluded_suffixes)


def test_ccda_catalog_preserves_observed_section_aliases_without_es_fields() -> None:
    assert len(CCDA_SECTION_TITLE_CATALOG) == 108
    assert CCDA_SECTION_TITLE_CATALOG["allergies"] == "allergies-section"
    assert CCDA_SECTION_TITLE_CATALOG["socialhistory"] == "socialHistory-section"
    assert CCDA_SECTION_TITLE_CATALOG["hospitaldischargemedications"] == (
        "hospitalDischargeMedications-section"
    )
    assert all(value.endswith("-section") for value in CCDA_SECTION_TITLE_CATALOG.values())
    assert all(".keyword" not in value for value in CCDA_SECTION_TITLE_CATALOG.values())
