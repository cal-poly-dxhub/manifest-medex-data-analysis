# ruff: noqa: E501

import hashlib
import json

import pytest
from src import ccda_parser
from src.ccda_parser import parse_ccda_document
from src.parser import ParseError, SourceReference

SYNTHETIC_CCDA = b"""<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <effectiveTime value="20260810123045-0700"/>
  <recordTarget><patientRole>
    <id root="patient-root" extension="patient-extension"/>
    <addr><streetAddressLine>Example Street</streetAddressLine><city>Example City</city><state>CA</state><postalCode>00000</postalCode></addr>
    <telecom value="tel:+10000000000"/>
    <patient><name><given>Example</given><family>Patient</family></name><administrativeGenderCode code="U"/><birthTime value="20000101"/><raceCode code="unknown"/><ethnicGroupCode code="unknown"/></patient>
  </patientRole></recordTarget>
  <custodian><assignedCustodian><representedCustodianOrganization><id root="facility-root"/><name>Example Facility</name></representedCustodianOrganization></assignedCustodian></custodian>
  <component><structuredBody>
    <component><section><code code="10160-0" codeSystemName="LOINC"/><text>EXCLUDED-NARRATIVE</text><entry><substanceAdministration>
      <id root="medication-id"/><statusCode code="COMPLETED"/><effectiveTime><low value="20260101"/><high value="20260201"/></effectiveTime><doseQuantity value="1" unit="tablet"/><routeCode code="oral" codeSystem="route-system" codeSystemName="Route" displayName="Oral"/>
      <consumable><manufacturedProduct><manufacturedMaterial><code code="med-code" codeSystem="rxnorm" codeSystemName="RXNORM"><originalText><reference value="#med"/></originalText></code></manufacturedMaterial></manufacturedProduct></consumable>
      <performer><assignedEntity><id root="npi-root" extension="npi-value"/><assignedPerson><name><given>Example</given><family>Clinician</family></name></assignedPerson></assignedEntity></performer>
      <entryRelationship typeCode="REFR"><observation><code code="status" codeSystem="snomed" codeSystemName="SNOMED"/></observation></entryRelationship>
    </substanceAdministration></entry></section></component>
    <component><section><code code="11369-6"/><entry><substanceAdministration><id root="immunization-id"/><statusCode code="COMPLETED"/><effectiveTime value="20260301"/><doseQuantity value="1" unit="dose"/><consumable><manufacturedProduct><manufacturedMaterial><code code="cvx-code" codeSystem="cvx" codeSystemName="CVX"/></manufacturedMaterial></manufacturedProduct></consumable></substanceAdministration></entry></section></component>
    <component><section><code code="11450-4"/><entry><act><entryRelationship typeCode="SUBJ"><observation><value code="problem-code" codeSystem="snomed" codeSystemName="SNOMED"/><effectiveTime><low value="20260101"/></effectiveTime><entryRelationship><observation><code code="33999-4" codeSystem="loinc" codeSystemName="LOINC" displayName="Active"/></observation></entryRelationship></observation></entryRelationship></act></entry></section></component>
    <component><section><code code="47519-4"/><entry><procedure><code code="procedure-code" codeSystem="cpt" codeSystemName="CPT" displayName="Example Procedure"><translation code="28570-0"/></code><effectiveTime value="20260401"/><performer><assignedEntity><id root="performer-root" extension="performer-extension"/><assignedPerson><name><given>Example</given><family>Performer</family></name></assignedPerson></assignedEntity></performer></procedure></entry></section></component>
    <component><section><code code="30954-2"/><entry><organizer><id root="result-id"/><code code="panel-code" codeSystem="loinc" codeSystemName="LOINC"/><statusCode code="FINAL"/><component><observation><code code="test-code" codeSystem="loinc" codeSystemName="LOINC"><originalText>Example result<reference value="#result"/></originalText></code><text><reference value="#result-text"/></text><statusCode code="COMPLETED"/><effectiveTime value="20260501"/><value value="1" unit="unit"/><interpretationCode code="N" codeSystem="interpretation"/><referenceRange><observationRange><value><low value="0" unit="unit"/><high value="2" unit="unit"/></value></observationRange></referenceRange></observation></component></organizer></entry></section></component>
    <component><section><code code="29762-2"/><entry><observation><code code="social-code" codeSystem="social-system" codeSystemName="Social" displayName="Example Social"/><statusCode code="COMPLETED"/><effectiveTime><low value="20260101"/></effectiveTime></observation></entry></section></component>
    <component><section><code code="8716-3" codeSystemName="LOINC" displayName="Vital Signs"/><entry><organizer><component><observation><code code="vital-code" codeSystem="loinc"/><effectiveTime value="20260601"/><value value="1" unit="unit"/></observation></component></organizer></entry></section></component>
  </structuredBody></component>
</ClinicalDocument>"""


def test_ccda_parser_projects_supplied_dashboard_paths_without_narrative() -> None:
    document = parse_ccda_document(
        SYNTHETIC_CCDA,
        SourceReference(
            bucket="raw",
            key="participant=FACILITY/ccda/document.xml",
            version_id="version-1",
        ),
        ingested_at="2026-08-10T20:00:00Z",
    )

    assert document["sourceFormat"] == "ccda"
    assert document["participantId"] == "FACILITY"
    assert document["documentTime"] == "2026-08-10T19:30:45Z"
    assert document["CD"]["recordTarget"]["patientRole"]["id"]["root"] == "patient-root"
    assert (
        document["CD"]["custodian"]["assignedCustodian"]["representedCustodianOrganization"]["name"]
        == "Example Facility"
    )

    body = document["CD"]["body"]
    medication = body["medications-section"]["entry"]["substanceAdministration"]
    assert medication["statusCode"]["code"] == "COMPLETED"
    assert medication["effectiveTime"]["low"]["value"] == "20260101"
    assert (
        medication["consumable"]["manufacturedProduct"]["manufacturedMaterial"]["code"]["code"]
        == "med-code"
    )
    assert medication["performer"]["assignedEntity"]["id"]["extension"] == "npi-value"

    assert (
        body["immunizations-section"]["entry"]["substanceAdministration"]["effectiveTime"]["value"]
        == "20260301"
    )
    problem = body["problem-section"]["entry"]["act"]["entryRelationship"]["observation"]
    assert problem["value"]["code"] == "problem-code"
    procedure = body["procedures-section"]["entry"]["procedure"]
    assert procedure["code"]["translation"]["code"] == "28570-0"
    assert (
        procedure["performer"]["assignedEntity"]["assignedPerson"]["name"]["given"]["content"]
        == "Example"
    )

    result = body["results-section"]["entry"]["organizer"]["component"]["observation"]
    assert result["value"] == {"value": "1", "unit": "unit"}
    assert result["referenceRange"]["observationRange"]["value"]["high"] == {
        "value": "2",
        "unit": "unit",
    }
    assert body["socialHistory-section"]["entry"]["act"]["code"]["code"] == "social-code"
    assert (
        body["vitalSigns-section"]["entry"]["organizer"]["component"]["observation"]["code"]["code"]
        == "vital-code"
    )
    assert "EXCLUDED-NARRATIVE" not in json.dumps(document)


def test_ccda_document_id_uses_bucket_key_and_raw_xml_content() -> None:
    first = parse_ccda_document(
        SYNTHETIC_CCDA,
        SourceReference(bucket="raw", key="document.xml", version_id="v1", etag="etag-1"),
    )
    second = parse_ccda_document(
        SYNTHETIC_CCDA,
        SourceReference(bucket="raw", key="document.xml", version_id="v2", etag="etag-2"),
    )
    checksum = hashlib.sha256(SYNTHETIC_CCDA).hexdigest()
    expected = hashlib.sha256(f"raw\0document.xml\0{checksum}".encode()).hexdigest()

    assert first["parserVersion"] == "0.2.0"
    assert first["documentId"] == expected
    assert second["documentId"] == expected
    assert first["rawObject"]["sha256"] == checksum


def test_ccda_document_id_changes_for_a_different_source_key() -> None:
    first = parse_ccda_document(
        SYNTHETIC_CCDA,
        SourceReference(bucket="raw", key="document.xml"),
    )
    second = parse_ccda_document(
        SYNTHETIC_CCDA,
        SourceReference(bucket="raw", key="copy.xml"),
    )

    assert first["documentId"] != second["documentId"]


def test_ccda_document_id_changes_when_raw_xml_bytes_change() -> None:
    source = SourceReference(bucket="raw", key="document.xml")
    original = parse_ccda_document(SYNTHETIC_CCDA, source)
    formatting_only_change = parse_ccda_document(SYNTHETIC_CCDA + b"\n", source)

    assert original["CD"] == formatting_only_change["CD"]
    assert original["documentId"] != formatting_only_change["documentId"]
    assert original["rawObject"]["sha256"] != formatting_only_change["rawObject"]["sha256"]


def test_ccda_parser_rejects_dtd_entities_and_wrong_root() -> None:
    unsafe = b'<!DOCTYPE x [<!ENTITY e "unsafe">]><ClinicalDocument>&e;</ClinicalDocument>'
    with pytest.raises(ParseError, match="safe, well-formed XML"):
        parse_ccda_document(unsafe, SourceReference(bucket="raw", key="unsafe.xml"))

    with pytest.raises(ParseError, match="root is not an HL7 ClinicalDocument"):
        parse_ccda_document(b"<NotClinicalDocument/>", SourceReference(bucket="raw", key="bad.xml"))


def test_ccda_parser_rejects_excessive_markup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ccda_parser, "MAX_MARKUP_TOKENS", 1)

    with pytest.raises(ParseError, match="structural complexity"):
        parse_ccda_document(SYNTHETIC_CCDA, SourceReference(bucket="raw", key="large.xml"))


def test_ccda_parser_handles_sparse_document_and_unknown_section() -> None:
    sparse = b"""<ClinicalDocument xmlns="urn:hl7-org:v3">
      <effectiveTime value="not-a-timestamp"/>
      <component><structuredBody><component><section>
        <code code="unknown-section"/>
      </section></component></structuredBody></component>
    </ClinicalDocument>"""

    document = parse_ccda_document(
        sparse,
        SourceReference(bucket="raw", key="sparse.xml"),
    )

    assert document["documentTime"] is None
    assert document["participantId"] is None
    assert document["CD"] == {}
    assert document["sectionCounts"] == {"unknown-section": 1}
