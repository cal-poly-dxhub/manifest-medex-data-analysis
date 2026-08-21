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
        return {"VersionId": f"parsed-version-{self.parsed_version}"}


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
    assert operations == [
        "s3:get",
        "s3:parsed",
        "s3:parsed",
        "search:HEAD",
        "search:PUT",
        "search:POST",
        "metadata:upsert",
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
    assert [record.parsed_version_id for record in records] == [
        "parsed-version-1",
        "parsed-version-2",
    ]
    assert all(record.parsed_s3_uri.startswith("s3://parsed-bucket/hl7/") for record in records)
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
    assert record.parsed_version_id == "parsed-version-1"
    assert record.parsed_s3_uri.startswith("s3://parsed-bucket/ccda/")
    assert s3.get_calls == [{"Bucket": "raw-bucket", "Key": "incoming/ccda/document.xml"}]
    assert transport.calls[0][:2] == ("HEAD", "/ccda-documents-v1")


def test_permanent_ccda_index_rejection_retries_without_aurora(
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
    assert metadata.calls == []
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
    assert error_record["stage"] == "source_read"
    assert error_record["retryable"] is True
    assert error_record["final"] is False


def test_aurora_failure_retries_after_successful_s3_and_index(
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
