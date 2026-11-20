import json
import logging
from io import BytesIO
from typing import Any

import pytest
from src.ccda_handler import handler as ccda_handler
from src.document_handler import DocumentProcessor
from src.hl7_handler import handler as hl7_handler
from src.metadata_store import MetadataRecord, MetadataStoreError

SYNTHETIC_HL7 = (
    b"MSH|^~\\&|SYNTH|FACILITY|DEST|DEST|20260810120000||ORU^R01|MSG-1|P|2.5.1\r"
    b"PID|1||SYNTHETIC-ID||Example^Patient||20000101|U\r"
    b"OBR|1|||1234-5^Synthetic test^LN||||||||||||||||||20260810121000||LAB|F\r"
    b"OBX|1|NM|1234-5^Synthetic result^LN||42||||||F\r"
)
SECOND_HL7 = (
    b"MSH|^~\\&|SYNTH|FACILITY|DEST|DEST|20260810130000||ADT^A01|MSG-2|P|2.5.1\r"
    b"PID|1||SECOND-ID||Second^Patient||20000202|U\r"
)
SYNTHETIC_CCDA = b"""<ClinicalDocument xmlns="urn:hl7-org:v3">
<effectiveTime value="20260810120000"/>
<recordTarget><patientRole><id root="synthetic"/></patientRole></recordTarget>
<component><structuredBody><component><section><code code="8716-3"/>
<entry><organizer><component><observation><code code="synthetic-vital"/>
<value value="1" unit="unit"/></observation></component></organizer></entry>
</section></component></structuredBody></component>
</ClinicalDocument>"""
ERROR_WRITE_FAILURE_DETAIL = "sensitive error-write detail"


class FakeS3:
    def __init__(
        self,
        payload: bytes | Exception,
        operations: list[str] | None = None,
        *,
        fail_error_put: bool = False,
    ) -> None:
        self.payload = payload
        self.operations = operations if operations is not None else []
        self.fail_error_put = fail_error_put
        self.get_calls: list[dict[str, Any]] = []
        self.put_calls: list[dict[str, Any]] = []
        self.parsed_version = 0

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.operations.append("s3:get")
        self.get_calls.append(kwargs)
        if isinstance(self.payload, Exception):
            raise self.payload
        return {
            "Body": BytesIO(self.payload),
            "ContentLength": len(self.payload),
            "VersionId": "raw-response-version",
            "ETag": '"raw-etag"',
        }

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if kwargs["Bucket"] == "error-bucket":
            self.operations.append("s3:error")
            if self.fail_error_put:
                raise RuntimeError(ERROR_WRITE_FAILURE_DETAIL)
            return {"VersionId": "error-version"}
        self.operations.append("s3:parsed")
        self.parsed_version += 1
        # Derived from the key so the version stays paired with its document even when
        # parsed writes run concurrently.
        return {"VersionId": "parsed-version-" + kwargs["Key"].rsplit("/", 1)[-1].split(".")[0]}


class FakeTransport:
    def __init__(
        self,
        responses: list[tuple[int, dict[str, Any]]],
        operations: list[str] | None = None,
    ) -> None:
        self.responses = responses
        self.operations = operations if operations is not None else []
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.operations.append(f"search:{method}")
        self.calls.append((method, path, body))
        return self.responses.pop(0)


class FakeMetadataStore:
    def __init__(
        self,
        operations: list[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.operations = operations if operations is not None else []
        self.error = error
        self.calls: list[list[MetadataRecord]] = []

    def upsert(self, records: list[MetadataRecord]) -> None:
        self.operations.append("metadata:upsert")
        self.calls.append(records)
        if self.error is not None:
            raise self.error


def _event(key: str, *, version_id: str | None = "raw-event-version") -> dict[str, Any]:
    object_detail: dict[str, Any] = {"key": key, "etag": "raw-event-etag"}
    if version_id is not None:
        object_detail["version-id"] = version_id
    return {
        "Records": [
            {
                "messageId": "sqs-message-1",
                "body": json.dumps(
                    {
                        "source": "aws.s3",
                        "detail-type": "Object Created",
                        "time": "2026-08-10T19:00:00Z",
                        "detail": {
                            "bucket": {"name": "raw-bucket"},
                            "object": object_detail,
                        },
                    }
                ),
            }
        ]
    }


def _processor(
    source_format: str,
    s3: FakeS3,
    transport: FakeTransport,
    metadata: FakeMetadataStore,
) -> DocumentProcessor:
    return DocumentProcessor(
        expected_format=source_format,
        s3_client=s3,
        search_transport=transport,
        metadata_store=metadata,
    )


def _successful_transport(document_count: int, operations: list[str]) -> FakeTransport:
    return FakeTransport(
        [
            (404, {}),
            (201, {"acknowledged": True}),
            (
                200,
                {
                    "errors": False,
                    "items": [{"index": {"status": 201}} for _ in range(document_count)],
                },
            ),
        ],
        operations,
    )


def test_hl7_combined_processor_completes_all_stages_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    operations: list[str] = []
    s3 = FakeS3(SYNTHETIC_HL7 + SECOND_HL7, operations)
    transport = _successful_transport(2, operations)
    metadata = FakeMetadataStore(operations)

    result = _processor("hl7-v2", s3, transport, metadata).process_batch(
        _event("incoming/hl7/batch.hl7")
    )

    assert result == {"batchItemFailures": []}
    # Aurora is written before indexing so search never holds a document without metadata.
    assert operations == [
        "s3:get",
        "s3:parsed",
        "s3:parsed",
        "metadata:upsert",
        "search:HEAD",
        "search:PUT",
        "search:POST",
    ]
    assert len(metadata.calls) == 1
    records = metadata.calls[0]
    assert len(records) == 2
    assert len({record.document_id for record in records}) == 2
    assert all(record.source_format == "hl7-v2" for record in records)
    assert [record.document_time for record in records] == [
        "2026-08-10T12:00:00Z",
        "2026-08-10T13:00:00Z",
    ]
    assert all(record.ingested_time == "2026-08-10T19:00:00Z" for record in records)
    assert all(record.raw_s3_uri == "s3://raw-bucket/incoming/hl7/batch.hl7" for record in records)
    assert all(record.raw_version_id == "raw-event-version" for record in records)
    assert all(
        record.parsed_version_id == f"parsed-version-{record.document_id}" for record in records
    )
    assert all(
        record.parsed_s3_uri == f"s3://parsed-bucket/hl7/{record.document_id}.json"
        for record in records
    )
    assert all(call["Key"].startswith("hl7/") for call in s3.put_calls)
    bulk_body = transport.calls[-1][2]
    assert bulk_body is not None
    assert bulk_body.count(b'"_id"') == 2


def test_hl7_duplicate_content_reaches_each_destination_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    operations: list[str] = []
    s3 = FakeS3(SYNTHETIC_HL7 + SYNTHETIC_HL7, operations)
    transport = _successful_transport(1, operations)
    metadata = FakeMetadataStore(operations)

    result = _processor("hl7-v2", s3, transport, metadata).process_batch(
        _event("incoming/hl7/duplicates.hl7")
    )

    assert result == {"batchItemFailures": []}
    assert operations.count("s3:parsed") == 1
    assert len(metadata.calls) == 1
    assert len(metadata.calls[0]) == 1
    bulk_body = transport.calls[-1][2]
    assert bulk_body is not None
    assert bulk_body.count(b'"_id"') == 1


def test_ccda_combined_processor_persists_one_document(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    operations: list[str] = []
    s3 = FakeS3(SYNTHETIC_CCDA, operations)
    transport = _successful_transport(1, operations)
    metadata = FakeMetadataStore(operations)

    result = _processor("ccda", s3, transport, metadata).process_batch(
        _event("incoming/ccda/document.xml", version_id=None)
    )

    assert result == {"batchItemFailures": []}
    record = metadata.calls[0][0]
    assert record.source_format == "ccda"
    assert record.document_time == "2026-08-10T12:00:00Z"
    assert record.raw_version_id == "raw-response-version"
    assert record.parsed_version_id == f"parsed-version-{record.document_id}"
    assert record.parsed_s3_uri.startswith("s3://parsed-bucket/ccda/")
    assert s3.get_calls == [{"Bucket": "raw-bucket", "Key": "incoming/ccda/document.xml"}]
    assert transport.calls[0][:2] == ("HEAD", "/ccda-documents-v1")


def test_permanent_ccda_index_rejection_retries_after_aurora(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    sensitive_key = "incoming/ccda/sensitive-patient-name.xml"
    sensitive_reason = "rejected clinical value patient-sensitive-backend-response"
    s3 = FakeS3(SYNTHETIC_CCDA)
    transport = FakeTransport(
        [
            (200, {}),
            (
                200,
                {
                    "errors": True,
                    "items": [
                        {
                            "index": {
                                "status": 400,
                                "error": {
                                    "type": "mapper_parsing_exception",
                                    "reason": sensitive_reason,
                                },
                            }
                        }
                    ],
                },
            ),
        ]
    )
    metadata = FakeMetadataStore()

    with caplog.at_level(logging.ERROR, logger="src.document_handler"):
        result = _processor("ccda", s3, transport, metadata).process_batch(_event(sensitive_key))

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    # Aurora already holds the row (explorer lists it); the retry completes indexing.
    assert len(metadata.calls) == 1
    error_record = json.loads(s3.put_calls[-1]["Body"])
    assert error_record["stage"] == "indexing"
    assert error_record["failureCategory"] == "document_indexing_failed"
    assert error_record["httpStatus"] == 400
    assert error_record["backendErrorType"] == "mapper_parsing_exception"
    assert error_record["retryable"] is True
    diagnostics = caplog.text + json.dumps(error_record) + str(s3.put_calls[-1]["Key"])
    assert sensitive_key not in diagnostics
    assert sensitive_reason not in diagnostics
    parsed = json.loads(s3.put_calls[0]["Body"])
    assert parsed["documentId"] not in diagnostics


def test_oversized_ccda_is_retried_for_dlq_redrive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    monkeypatch.setenv("MAX_OBJECT_BYTES", "1")
    s3 = FakeS3(SYNTHETIC_CCDA)
    metadata = FakeMetadataStore()
    transport = FakeTransport([])

    result = _processor("ccda", s3, transport, metadata).process_batch(
        _event("incoming/ccda/large.xml")
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    assert transport.calls == []
    assert metadata.calls == []
    error_record = json.loads(s3.put_calls[0]["Body"])
    assert error_record["failureCategory"] == "object_too_large"
    assert error_record["objectBytes"] == len(SYNTHETIC_CCDA)
    assert error_record["maxObjectBytes"] == 1
    assert error_record["stage"] == "source_read"
    assert error_record["retryable"] is True
    assert error_record["final"] is False


def test_aurora_failure_retries_before_anything_is_indexed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    operations: list[str] = []
    s3 = FakeS3(SYNTHETIC_HL7, operations)
    transport = _successful_transport(1, operations)
    metadata = FakeMetadataStore(
        operations,
        MetadataStoreError("patient-sensitive SQL parameters"),
    )

    result = _processor("hl7-v2", s3, transport, metadata).process_batch(
        _event("incoming/hl7/message.txt")
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    assert operations[-2:] == ["metadata:upsert", "s3:error"]
    # Nothing reached the search index, so a failed Aurora write leaves no orphan.
    assert not any(operation.startswith("search:") for operation in operations)
    error_record = json.loads(s3.put_calls[-1]["Body"])
    assert error_record["stage"] == "metadata"
    assert error_record["failureCategory"] == "metadata_persistence_failed"
    assert "patient-sensitive" not in json.dumps(error_record)


@pytest.mark.parametrize(
    ("source_format", "key"),
    [
        ("hl7-v2", "incoming/hl7/document.xml"),
        ("hl7-v2", "other/hl7/document.hl7"),
        ("ccda", "incoming/ccda/document.txt"),
        ("ccda", "other/ccda/document.xml"),
    ],
)
def test_format_handlers_reject_wrong_prefixes_and_extensions_for_retry(
    monkeypatch: pytest.MonkeyPatch,
    source_format: str,
    key: str,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = FakeS3(SYNTHETIC_HL7)

    result = _processor(
        source_format,
        s3,
        FakeTransport([]),
        FakeMetadataStore(),
    ).process_batch(_event(key))

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    assert s3.get_calls == []
    assert json.loads(s3.put_calls[0]["Body"])["failureCategory"] == "invalid_source_route"


def test_malformed_event_and_error_write_failure_still_return_partial_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = FakeS3(SYNTHETIC_HL7, fail_error_put=True)
    event = {"Records": [{"messageId": "malformed", "body": "{"}]}

    with caplog.at_level(logging.ERROR, logger="src.document_handler"):
        result = _processor("hl7-v2", s3, FakeTransport([]), FakeMetadataStore()).process_batch(
            event
        )

    assert result == {"batchItemFailures": [{"itemIdentifier": "malformed"}]}
    assert "error_record_write_failed" in caplog.text
    assert "sensitive error-write detail" not in caplog.text


def test_source_read_failure_does_not_log_sensitive_exception(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    sensitive = "temporary S3 failure for patient-sensitive-value"
    s3 = FakeS3(RuntimeError(sensitive))

    with caplog.at_level(logging.ERROR, logger="src.document_handler"):
        result = _processor("hl7-v2", s3, FakeTransport([]), FakeMetadataStore()).process_batch(
            _event("incoming/hl7/message.hl7")
        )

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    assert sensitive not in caplog.text
    assert sensitive not in s3.put_calls[0]["Body"].decode()


def test_thin_handlers_select_their_fixed_format(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[dict[str, Any], object, str]] = []

    def fake_runtime(
        event: dict[str, Any], context: object, *, expected_format: str
    ) -> dict[str, list[dict[str, str]]]:
        calls.append((event, context, expected_format))
        return {"batchItemFailures": []}

    monkeypatch.setattr("src.hl7_handler.runtime_handler", fake_runtime)
    monkeypatch.setattr("src.ccda_handler.runtime_handler", fake_runtime)
    context = object()

    assert hl7_handler({}, context) == {"batchItemFailures": []}
    assert ccda_handler({}, context) == {"batchItemFailures": []}
    assert [call[2] for call in calls] == ["hl7-v2", "ccda"]


def test_s3_console_folder_marker_is_acknowledged_without_retry(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = FakeS3(b"")
    metadata = FakeMetadataStore()
    transport = FakeTransport([])

    with caplog.at_level("INFO"):
        result = _processor("hl7-v2", s3, transport, metadata).process_batch(
            _event("incoming/hl7/")
        )

    assert result == {"batchItemFailures": []}
    assert s3.get_calls == []
    assert s3.put_calls == []
    assert transport.calls == []
    assert metadata.calls == []
    assert '"reason":"folder_marker"' in caplog.text


def test_many_messages_are_written_concurrently_and_stay_paired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    messages = b"".join(
        SYNTHETIC_HL7.replace(b"MSG-1", f"MSG-{index}".encode()) for index in range(25)
    )
    operations: list[str] = []
    s3 = FakeS3(messages, operations)
    metadata = FakeMetadataStore(operations)
    transport = _successful_transport(25, operations)

    result = _processor("hl7-v2", s3, transport, metadata).process_batch(
        _event("incoming/hl7/many.hl7")
    )

    assert result == {"batchItemFailures": []}
    assert operations.count("s3:parsed") == 25
    # Every parsed write finishes before indexing starts.
    assert operations.index("search:HEAD") > max(
        index for index, op in enumerate(operations) if op == "s3:parsed"
    )
    records = metadata.calls[0]
    assert len({record.document_id for record in records}) == 25
    assert all(
        record.parsed_s3_uri == f"s3://parsed-bucket/hl7/{record.document_id}.json"
        and record.parsed_version_id == f"parsed-version-{record.document_id}"
        for record in records
    )


class EnvelopeS3(FakeS3):
    """Serves a raw object written by the intake API plus its envelope sidecar."""

    def __init__(self, payload: bytes, envelope: dict[str, Any] | Exception, **kwargs: Any) -> None:
        super().__init__(payload, **kwargs)
        self.envelope = envelope

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs["Key"].startswith("envelopes/"):
            self.get_calls.append(kwargs)
            if isinstance(self.envelope, Exception):
                raise self.envelope
            body = json.dumps(self.envelope).encode()
            return {"Body": BytesIO(body), "ContentLength": len(body)}
        response = super().get_object(**kwargs)
        response["Metadata"] = {"intake-sender": "rhapsody", "envelope-version-id": "env-v1"}
        return response


INTAKE_KEY = "incoming/hl7/participant=ARR_CA_ARMC_H/2026/11/18/MSG-1.hl7"
ENVELOPE = {
    "sourceMessageId": "MSG-1",
    "messageDate": "2026-11-18T09:30:00-08:00",
    "clinicalMessageType": "HL7V2",
    "eventType": "ADT_A08",
    "sourceFacilityId": "ARR_CA_ARMC_H",
    "sourceFacility": "Synthetic Facility",
    "patientId": "12345",
    "patientIdAa": "KHS",
    "patientMpiId": "MPI-1",
    "patientMpiIdsOld": "MPI-0",
    "sendingApplication": "Rhapsody",
    "otherAttributes": {"isPartTwo": True, "testFlag": 1},
    "contents": [{"processedLevel": "PROCESSED", "otherAttributes": {"lane": "a"}}],
    "selectedContentIndex": 0,
    "intakeReceivedAt": "2026-11-18T17:00:00Z",
}


def test_intake_envelope_fields_are_merged_and_identity_is_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = EnvelopeS3(SYNTHETIC_HL7, ENVELOPE)
    transport = _successful_transport(1, [])
    metadata = FakeMetadataStore()

    result = _processor("hl7-v2", s3, transport, metadata).process_batch(_event(INTAKE_KEY))

    assert result == {"batchItemFailures": []}
    envelope_get = next(call for call in s3.get_calls if call["Key"].startswith("envelopes/"))
    assert envelope_get == {
        "Bucket": "raw-bucket",
        "Key": "envelopes/hl7/participant=ARR_CA_ARMC_H/2026/11/18/MSG-1.json",
        "VersionId": "env-v1",
    }
    parsed = json.loads(next(c for c in s3.put_calls if c["Bucket"] == "parsed-bucket")["Body"])
    assert parsed["patientId"] == "12345"
    assert parsed["patientIdAa"] == "KHS"
    assert parsed["patientMpiId"] == "MPI-1"
    assert parsed["patientMpiId_old"] == "MPI-0"
    assert parsed["messageEventType"] == "ADT_A08"
    assert parsed["sourceFacility"] == "Synthetic Facility"
    assert parsed["sourceFacilityId"] == "ARR_CA_ARMC_H"
    assert parsed["messageReceivedDate"] == "2026-11-18T17:00:00Z"
    assert parsed["messageProcessedLevel"] == "PROCESSED"
    assert parsed["otherAttributes"] == {"isPartTwo": True, "testFlag": 1}
    assert parsed["contentOtherAttributes"] == {"lane": "a"}
    # Envelope values win over the ones read from the message (PID-3, MSH-3, MSH-9).
    assert parsed["sendingApplication"] == "Rhapsody"
    assert parsed["patientFullId"] == "12345^KHS"
    assert parsed["sourceMessageType"] == "HL7V2"
    # messageReceivedDate comes from the envelope, createdTime from ingestion.
    assert parsed["createdTime"] == parsed["ingestTime"] != parsed["messageReceivedDate"]

    # A corrected re-send (same key, different content) keeps the same document identity.
    corrected = EnvelopeS3(SYNTHETIC_HL7.replace(b"MSG-1", b"MSG-1-FIX"), ENVELOPE)
    _processor(
        "hl7-v2", corrected, _successful_transport(1, []), FakeMetadataStore()
    ).process_batch(_event(INTAKE_KEY))
    corrected_doc = json.loads(
        next(c for c in corrected.put_calls if c["Bucket"] == "parsed-bucket")["Body"]
    )
    assert corrected_doc["documentId"] == parsed["documentId"]


def test_envelope_overrides_message_facility(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = EnvelopeS3(SYNTHETIC_HL7, {**ENVELOPE, "sourceFacilityId": "PRISM_FACILITY"})
    _processor("hl7-v2", s3, _successful_transport(1, []), FakeMetadataStore()).process_batch(
        _event(INTAKE_KEY)
    )
    parsed = json.loads(next(c for c in s3.put_calls if c["Bucket"] == "parsed-bucket")["Body"])
    assert parsed["sourceFacilityId"] == "PRISM_FACILITY"


def test_oversized_other_attributes_are_dropped_not_indexed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    flood = {f"k{index}": index for index in range(500)}
    s3 = EnvelopeS3(SYNTHETIC_HL7, {**ENVELOPE, "otherAttributes": flood})
    _processor("hl7-v2", s3, _successful_transport(1, []), FakeMetadataStore()).process_batch(
        _event(INTAKE_KEY)
    )
    parsed = json.loads(next(c for c in s3.put_calls if c["Bucket"] == "parsed-bucket")["Body"])
    assert "otherAttributes" not in parsed
    assert parsed["patientId"] == "12345"


def test_unreadable_envelope_fails_the_record_for_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = EnvelopeS3(SYNTHETIC_HL7, ConnectionError("sidecar unavailable"))
    metadata = FakeMetadataStore()

    result = _processor("hl7-v2", s3, FakeTransport([]), metadata).process_batch(_event(INTAKE_KEY))

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    assert metadata.calls == []
    error_record = json.loads(
        next(c for c in s3.put_calls if c["Bucket"] == "error-bucket")["Body"]
    )
    assert error_record["failureCategory"] == "envelope_read_failed"


def test_files_uploaded_directly_never_look_for_an_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = FakeS3(SYNTHETIC_HL7)
    _processor("hl7-v2", s3, _successful_transport(1, []), FakeMetadataStore()).process_batch(
        _event("incoming/hl7/direct.hl7")
    )
    assert all(not call["Key"].startswith("envelopes/") for call in s3.get_calls)
    parsed = json.loads(next(c for c in s3.put_calls if c["Bucket"] == "parsed-bucket")["Body"])
    # Without an envelope, Prism fields come from the message itself; envelope-only
    # fields (MPI ID, facility name, processed level) stay absent.
    assert parsed["patientId"] == "SYNTHETIC-ID"
    assert parsed["patientFullId"] == "SYNTHETIC-ID"
    assert parsed["sourceMessageId"] == "MSG-1"
    assert parsed["sendingApplication"] == "SYNTH"
    assert parsed["messageEventType"] == "ORU_R01"
    assert parsed["sourceMessageType"] == "HL7V2"
    assert parsed["messageStatus"] == "PROCESSED"
    assert parsed["createdTime"] == parsed["updatedTime"] == parsed["ingestTime"]
    assert parsed["messageReceivedDate"] == parsed["ingestTime"]
    assert parsed["clinicalTime"] == parsed["messageTime"]
    assert len(parsed["sha256"]) == 64
    for envelope_only in ("patientMpiId", "sourceFacility", "messageProcessedLevel"):
        assert envelope_only not in parsed


def test_envelope_key_mapping() -> None:
    from src.document_handler import envelope_key_for

    assert (
        envelope_key_for(INTAKE_KEY)
        == "envelopes/hl7/participant=ARR_CA_ARMC_H/2026/11/18/MSG-1.json"
    )
    assert (
        envelope_key_for("incoming/ccda/participant=X/a.b.xml")
        == "envelopes/ccda/participant=X/a.b.json"
    )
    assert envelope_key_for("other/x.hl7") is None
    assert envelope_key_for("incoming/hl7/noextension") is None


def _hl7_batch(count: int, *, broken_last: bool = False) -> bytes:
    messages = [SYNTHETIC_HL7.replace(b"MSG-1", f"MSG-{index}".encode()) for index in range(count)]
    if broken_last:
        messages[-1] = b"MSH|bad\r"
    return b"".join(messages)


def test_large_hl7_file_is_streamed_in_chunks_with_identical_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import document_handler

    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    payload = _hl7_batch(12)
    # Small-file path for reference output.
    reference_metadata = FakeMetadataStore()
    _processor(
        "hl7-v2", FakeS3(payload), _successful_transport(12, []), reference_metadata
    ).process_batch(_event("incoming/hl7/big.hl7"))
    reference_ids = [record.document_id for record in reference_metadata.calls[0]]

    monkeypatch.setattr(document_handler, "HL7_STREAM_THRESHOLD_BYTES", 10)
    monkeypatch.setattr(document_handler, "HL7_CHUNK_MESSAGES", 5)
    monkeypatch.setattr(document_handler, "STREAM_READ_BYTES", 37)
    operations: list[str] = []
    s3 = FakeS3(payload, operations)
    metadata = FakeMetadataStore(operations)
    transport = FakeTransport(
        [
            (404, {}),
            (201, {"acknowledged": True}),
            (200, {"errors": False, "items": [{"index": {"status": 201}}] * 5}),
            (200, {}),
            (200, {"errors": False, "items": [{"index": {"status": 201}}] * 5}),
            (200, {}),
            (200, {"errors": False, "items": [{"index": {"status": 201}}] * 2}),
        ],
        operations,
    )

    result = _processor("hl7-v2", s3, transport, metadata).process_batch(
        _event("incoming/hl7/big.hl7")
    )

    assert result == {"batchItemFailures": []}
    # Two reads of the same pinned version: checksum/validation pass, then parsing pass.
    assert [call.get("VersionId") for call in s3.get_calls] == ["raw-event-version"] * 2
    assert [len(call) for call in metadata.calls] == [5, 5, 2]
    assert [record.document_id for call in metadata.calls for record in call] == reference_ids
    # Each chunk is stored, then recorded in Aurora, then indexed, before the next begins.
    upserts = [index for index, op in enumerate(operations) if op == "metadata:upsert"]
    bulks = [index for index, op in enumerate(operations) if op == "search:POST"]
    assert all(upsert < bulk for upsert, bulk in zip(upserts, bulks, strict=True))
    assert upserts[1] > bulks[0]


def test_bad_message_late_in_large_file_fails_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import document_handler

    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    monkeypatch.setattr(document_handler, "HL7_STREAM_THRESHOLD_BYTES", 10)
    s3 = FakeS3(_hl7_batch(8, broken_last=True))
    metadata = FakeMetadataStore()

    result = _processor("hl7-v2", s3, FakeTransport([]), metadata).process_batch(
        _event("incoming/hl7/big.hl7")
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "sqs-message-1"}]}
    assert metadata.calls == []
    assert [call["Bucket"] for call in s3.put_calls] == ["error-bucket"]


def test_multi_document_ccda_file_stores_each_document(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    second = SYNTHETIC_CCDA.replace(b"synthetic-vital", b"synthetic-vital-2")
    s3 = FakeS3(SYNTHETIC_CCDA + b"\n" + second)
    metadata = FakeMetadataStore()
    transport = FakeTransport(
        [
            (404, {}),
            (201, {"acknowledged": True}),
            (200, {"errors": False, "items": [{"index": {"status": 201}}]}),
            (200, {}),
            (200, {"errors": False, "items": [{"index": {"status": 201}}]}),
        ]
    )

    result = _processor("ccda", s3, transport, metadata).process_batch(
        _event("incoming/ccda/batch.xml")
    )

    assert result == {"batchItemFailures": []}
    records = [record for call in metadata.calls for record in call]
    assert len(records) == 2
    assert len({record.document_id for record in records}) == 2
