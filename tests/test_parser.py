import hashlib

import pytest
from src.parser import ParseError, SourceReference, parse_hl7_file, split_hl7_messages

SYNTHETIC_BATCH = (
    "MSH|^~\\&|SYNTH|FACILITY_A|RECEIVER|DEST|20260810123045-0700||ORU^R01^ORU_R01|MSG-1|P|2.5.1\r"
    "PID|1||PATIENT-1^^^FACILITY_A^MR||Example^Synthetic||20000101|F\r"
    "PV1|1|O|||||1234567890^Doctor^One|2345678901^Doctor^Two|3456789012^Doctor^Three||||||||4567890123^Doctor^Four|||||||||||||||||||||||||||20260810110000|20260810120000\r"
    "PV2|||TEST^Synthetic reason^L\r"
    "OBR|1|||1234-5^Synthetic test^LN||||||||||||||||||20260810124000||LAB|F\r"
    "OBX|1|NM|1234-5^Synthetic result^LN||42|mg/dL|||||F\r"
    "OBX|2|ST|6789-0^Synthetic text^LOINC||present||||||C\r"
    "MSH|^~\\&|SYNTH|FACILITY_A|RECEIVER|DEST|20260810130000||ADT^A01|MSG-2|P|2.5.1\r"
    "PID|1||PATIENT-2^^^FACILITY_A^MR||Example^Second||19991231|M\r"
)


def test_split_and_parse_batch_with_legacy_query_paths() -> None:
    documents = parse_hl7_file(
        SYNTHETIC_BATCH.encode(),
        SourceReference(
            bucket="synthetic-raw",
            key="participant=FACILITY_A/type=ORU/synthetic.hl7",
            version_id="version-1",
        ),
        ingested_at="2026-08-10T19:30:00Z",
    )

    assert len(documents) == 2
    oru = documents[0]
    assert oru["messageType"] == "ORU"
    assert oru["triggerEvent"] == "R01"
    assert oru["sourceFacilityId"] == "FACILITY_A"
    assert oru["participantId"] == "FACILITY_A"
    assert oru["messageTime"] == "2026-08-10T19:30:45Z"
    assert oru["segmentCounts"]["OBX"] == 2
    assert oru["ROOT"]["MSH"]["MSH_9_Message_Type"] == {
        "MSG_1": "ORU",
        "MSG_2": "R01",
        "MSG_3": "ORU_R01",
    }
    assert oru["ROOT"]["PID"]["PID_3_Patient_Identifier_List"]["CX_1"] == "PATIENT-1"
    assert oru["ROOT"]["PV1"]["PV1_7_Attending_Doctor"]["XCN_1"] == "1234567890"
    assert oru["ROOT"]["PV2"]["PV2_3_Admit_Reason"]["CWE_2"] == "Synthetic reason"
    assert oru["ROOT"]["OBR"]["OBR_24_Diagnostic_Serv_Sect_ID"] == "LAB"
    assert oru["ROOT"]["OBX"]["OBX_11_Observation_Result_Status"] == ["F", "C"]
    assert oru["ROOT"]["OBX"]["OBX_3_Observation_Identifier"]["CWE_3"] == [
        "LN",
        "LOINC",
    ]

    adt = documents[1]
    assert adt["messageType"] == "ADT"
    assert adt["messageOrdinal"] == 1
    assert adt["documentId"] != oru["documentId"]


def test_document_id_uses_bucket_key_and_normalized_message_content() -> None:
    first_source = SourceReference(bucket="raw", key="synthetic.hl7", version_id="v1")
    second_source = SourceReference(bucket="raw", key="synthetic.hl7", version_id="v2")

    first = parse_hl7_file(SYNTHETIC_BATCH.encode(), first_source)
    second = parse_hl7_file(SYNTHETIC_BATCH.encode(), second_source)
    message = split_hl7_messages(SYNTHETIC_BATCH)[0]
    message_checksum = hashlib.sha256(message.encode()).hexdigest()
    expected = hashlib.sha256(f"raw\0synthetic.hl7\0{message_checksum}".encode()).hexdigest()

    assert first[0]["documentId"] == expected
    assert [item["documentId"] for item in first] == [item["documentId"] for item in second]
    different_key = parse_hl7_file(
        SYNTHETIC_BATCH.encode(),
        SourceReference(bucket="raw", key="copy.hl7", version_id="v2"),
    )
    assert first[0]["documentId"] != different_key[0]["documentId"]


def test_identical_messages_under_one_key_collapse_to_the_first_occurrence() -> None:
    message = split_hl7_messages(SYNTHETIC_BATCH)[0]
    documents = parse_hl7_file(
        f"{message}\r{message}".encode(),
        SourceReference(bucket="raw", key="duplicates.hl7", version_id="v1"),
    )

    assert len(documents) == 1
    assert documents[0]["messageOrdinal"] == 0


def test_reordering_distinct_messages_does_not_change_their_document_ids() -> None:
    messages = split_hl7_messages(SYNTHETIC_BATCH)
    source = SourceReference(bucket="raw", key="reordered.hl7", version_id="v1")

    original = parse_hl7_file("\r".join(messages).encode(), source)
    reordered = parse_hl7_file("\r".join(reversed(messages)).encode(), source)

    original_ids = {item["messageControlId"]: item["documentId"] for item in original}
    reordered_ids = {item["messageControlId"]: item["documentId"] for item in reordered}
    assert original_ids == reordered_ids


def test_changing_one_message_changes_only_that_messages_document_id() -> None:
    source = SourceReference(bucket="raw", key="changed.hl7", version_id="v1")
    original = parse_hl7_file(SYNTHETIC_BATCH.encode(), source)
    changed = parse_hl7_file(
        SYNTHETIC_BATCH.replace("PATIENT-2", "PATIENT-CHANGED").encode(), source
    )

    original_ids = {item["messageControlId"]: item["documentId"] for item in original}
    changed_ids = {item["messageControlId"]: item["documentId"] for item in changed}
    assert original_ids["MSG-1"] == changed_ids["MSG-1"]
    assert original_ids["MSG-2"] != changed_ids["MSG-2"]


def test_split_tolerates_mllp_and_newline_delimiters() -> None:
    payload = "\x0bMSH|^~\\&|S|F|||||ADT^A01|1|P|2.5\nPID|1\x1c\n"

    messages = split_hl7_messages(payload)

    assert messages == ["MSH|^~\\&|S|F|||||ADT^A01|1|P|2.5\rPID|1"]


def test_invalid_input_is_rejected_without_echoing_payload() -> None:
    with pytest.raises(ParseError, match="Content appeared before the first MSH segment"):
        parse_hl7_file(b"not an hl7 message", SourceReference(bucket="raw", key="bad.hl7"))


def test_split_preserves_trailing_field_whitespace() -> None:
    message = "MSH|^~\\&|S|F|||||ADT^A01|1|P|2.5\rPID|1|value  \r"

    assert split_hl7_messages(message)[0].endswith("PID|1|value  ")


def test_library_parsing_handles_custom_delimiters_and_unknown_segments() -> None:
    payload = (
        "MSH*$%!?*APP*FACILITY$OID***20260810120000**ORU$R01*ID-1*P*2.5\r"
        "PID*1**FIRST%SECOND**Family!S!Suffix$Given\r"
        "Z99*accepted\r"
    )

    document = parse_hl7_file(payload.encode(), SourceReference(bucket="raw", key="custom.hl7"))[0]

    assert document["parserVersion"] == "1.0.0"
    assert document["sourceFacilityId"] == "FACILITY"
    assert document["messageType"] == "ORU"
    assert document["triggerEvent"] == "R01"
    assert document["ROOT"]["PID"]["PID_3_Patient_Identifier_List"] == [
        {"CX_1": "FIRST"},
        {"CX_1": "SECOND"},
    ]
    assert document["ROOT"]["PID"]["PID_5_Patient_Name"]["XPN_1"]["FN_1"] == ("Family$Suffix")
    assert document["segmentCounts"]["Z99"] == 1


def test_library_parsing_preserves_spaces_in_the_final_projected_field() -> None:
    payload = "MSH|^~\\&|S|F|||||ORU^R01|1|P|2.5\rOBX|1|ST|CODE||value||||||F  \r"

    document = parse_hl7_file(payload.encode(), SourceReference(bucket="raw", key="spaces.hl7"))[0]

    assert document["ROOT"]["OBX"]["OBX_11_Observation_Result_Status"] == "F  "


def test_report_query_fields_absent_from_the_dashboard_queries_are_projected() -> None:
    """PV1-2 and NK1-3 appear only in the supplied report query rows, not the dashboards."""
    payload = (
        "MSH|^~\\&|SYNTH|FACILITY_A|RECEIVER|DEST|20260810123045||ADT^A08|MSG-1|P|2.5\r"
        "PID|1||PATIENT-1^^^FACILITY_A^MR||Example^Synthetic||20000101|F\r"
        "NK1|1|Contact^First|SPO^Spouse^HL70063|1 TEST ST^^TOWN^CA^90001|555-0001\r"
        "PV1|1|I|3W^301^A||||1234567890^Doctor^One\r"
    )

    document = parse_hl7_file(payload.encode(), SourceReference(bucket="raw", key="a08.hl7"))[0]

    assert document["ROOT"]["PV1"]["PV1_2_Patient_Class"] == "I"
    assert document["ROOT"]["NK1"]["NK1_3_Relationship"] == {
        "CWE_1": "SPO",
        "CWE_2": "Spouse",
        "CWE_3": "HL70063",
    }


def test_customer_catalog_projects_additional_standard_segments_and_fields() -> None:
    payload = (
        "MSH|^~\\&|SYNTH|FACILITY|RECEIVER|DEST|20260810123045||ADT^A08|MSG-1|P|2.6\r"
        "EVN|A08|20260810123100|||1234567890^Operator^One|20260810123000|FACILITY^OID^ISO\r"
        "PID|1|LEGACY-ID|PATIENT-1^^^FACILITY^MR||Example^Synthetic||20000101|F||"
        "2106-3^White^HL70005|1 TEST ST^^TOWN^CA^90001\r"
        "AL1|1|DA|PEN^Penicillin^RXNORM|SV^Severe^HL70128|Hives|20260101\r"
        "DG1|1|ICD10|A00^Cholera^I10|Example diagnosis|20260102|F\r"
        "ORC|NW|PLACER^FACILITY|FILLER^FACILITY||SC||||20260810123200|||1234567890^Doctor^One\r"
    )

    document = parse_hl7_file(
        payload.encode(),
        SourceReference(bucket="raw", key="customer-fields.hl7"),
    )[0]

    root = document["ROOT"]
    assert root["MSH"]["MSH_12_Version_ID"]["VID_1"] == "2.6"
    assert root["EVN"]["EVN_2_Recorded_Date-Time"] == "20260810123100"
    assert root["PID"]["PID_10_Race"]["CWE_1"] == "2106-3"
    assert root["PID"]["PID_11_Patient_Address"]["XAD_3"] == "TOWN"
    assert root["AL1"]["AL1_3_Allergen_Code-Mnemonic-Description"]["CWE_2"] == ("Penicillin")
    assert root["DG1"]["DG1_3_Diagnosis_Code_-_DG1"]["CWE_1"] == "A00"
    assert root["ORC"]["ORC_2_Placer_Order_Number"]["EI_1"] == "PLACER"
    assert root["ORC"]["ORC_12_Ordering_Provider"]["XCN_1"] == "1234567890"


def test_customer_catalog_projects_deprecated_dg1_8_and_pr1_11_fields() -> None:
    """DG1-8 (CNE) and PR1-11 (XCN) are deprecated-but-observed customer export fields."""
    payload = (
        "MSH|^~\\&|SYNTH|FACILITY|RECEIVER|DEST|20260810123045||ADT^A08|MSG-1|P|2.6\r"
        "DG1|1|ICD10|A00^Cholera^I10|Example diagnosis|20260102|F||"
        "470^Major Joint Replacement^MS-DRG\r"
        "PR1|1|C4|47.0^Appendectomy^I9|Appendectomy|20260103||||||"
        "9876543210^Surgeon^Jane\r"
    )

    document = parse_hl7_file(
        payload.encode(),
        SourceReference(bucket="raw", key="dg1-8-pr1-11.hl7"),
    )[0]

    root = document["ROOT"]
    assert root["DG1"]["DG1_8_Diagnostic_Related_Group"] == {
        "CNE_1": "470",
        "CNE_2": "Major Joint Replacement",
        "CNE_3": "MS-DRG",
    }
    assert root["PR1"]["PR1_11_Surgeon"]["XCN_1"] == "9876543210"
    assert root["PR1"]["PR1_11_Surgeon"]["XCN_2"] == {"FN_1": "Surgeon"}
    assert root["PR1"]["PR1_11_Surgeon"]["XCN_3"] == "Jane"


def test_dynamic_segment_projection_is_not_emitted() -> None:
    """The customer-backed ROOT projection is emitted without a duplicate SEGMENTS tree."""
    document = parse_hl7_file(
        SYNTHETIC_BATCH.encode(),
        SourceReference(bucket="raw", key="synthetic.hl7"),
    )[0]

    assert "SEGMENTS" not in document
    assert set(document["ROOT"]) <= {"MSH", "PID", "PV1", "PV2", "OBR", "OBX", "NK1"}


def test_library_parse_failures_are_sanitized() -> None:
    with pytest.raises(ParseError, match="HL7 message has an invalid MSH segment"):
        parse_hl7_file(
            b"MSH|broken",
            SourceReference(bucket="raw", key="malformed.hl7"),
        )
