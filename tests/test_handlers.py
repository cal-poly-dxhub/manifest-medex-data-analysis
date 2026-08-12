import hashlib
import json
import logging
import sys
import urllib.error
from io import BytesIO
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from src.index_handler import SignedOpenSearchTransport
from src.index_handler import process_batch as process_index_batch
from src.parse_handler import process_batch as process_parse_batch

SYNTHETIC_HL7 = (
    b"MSH|^~\\&|SYNTH|FACILITY|DEST|DEST|20260810120000||ORU^R01|MSG-1|P|2.5.1\r"
    b"PID|1||SYNTHETIC-ID||Example^Patient||20000101|U\r"
    b"OBR|1|||1234-5^Synthetic test^LN||||||||||||||||||20260810121000||LAB|F\r"
    b"OBX|1|NM|1234-5^Synthetic result^LN||42||||||F\r"
)


SYNTHETIC_CCDA = b"""<ClinicalDocument xmlns="urn:hl7-org:v3">
<effectiveTime value="20260810120000"/>
<recordTarget><patientRole><id root="synthetic"/></patientRole></recordTarget>
<component><structuredBody><component><section><code code="8716-3"/>
<entry><organizer><component><observation><code code="synthetic-vital"/>
<value value="1" unit="unit"/></observation></component></organizer></entry>
</section></component></structuredBody></component>
</ClinicalDocument>"""


class FakeS3:
    def __init__(self, payload: bytes | Exception) -> None:
        self.payload = payload
        self.put_calls: list[dict[str, Any]] = []

    def get_object(self, **_kwargs: Any) -> dict[str, Any]:
        if isinstance(self.payload, Exception):
            raise self.payload
        return {
            "Body": BytesIO(self.payload),
            "ContentLength": len(self.payload),
            "VersionId": "version-1",
            "ETag": '"etag-1"',
        }

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        return {}


class FakeSqs:
    def __init__(self) -> None:
        self.send_calls: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.send_calls.append(kwargs)
        return {}


class FakeTransport:
    def __init__(self, responses: list[tuple[int, dict[str, Any]]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        return self.responses.pop(0)


def _parse_event(key: str = "participant=FACILITY/type=ORU/synthetic.hl7") -> dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": "parse-message-1",
                "body": json.dumps(
                    {
                        "source": "aws.s3",
                        "detail-type": "Object Created",
                        "time": "2026-08-10T19:00:00Z",
                        "detail": {
                            "bucket": {"name": "raw-bucket"},
                            "object": {
                                "key": key,
                                "version-id": "version-1",
                                "etag": "etag-1",
                            },
                        },
                    }
                ),
            }
        ]
    }


def test_parse_handler_writes_deterministic_document_and_index_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    monkeypatch.setenv("INDEX_QUEUE_URL", "index-queue-url")
    s3 = FakeS3(SYNTHETIC_HL7)
    sqs = FakeSqs()

    result = process_parse_batch(_parse_event(), s3, sqs)

    assert result == {"batchItemFailures": []}
    assert len(s3.put_calls) == 1
    parsed = json.loads(s3.put_calls[0]["Body"])
    assert parsed["messageType"] == "ORU"
    assert s3.put_calls[0]["Key"] == f"hl7/{parsed['documentId']}.json"
    assert len(sqs.send_calls) == 1
    pointer = json.loads(sqs.send_calls[0]["MessageBody"])
    assert pointer == {
        "bucket": "parsed-bucket",
        "key": f"hl7/{parsed['documentId']}.json",
        "documentId": parsed["documentId"],
        "sourceFormat": "hl7-v2",
    }


def test_parse_handler_writes_ccda_document_and_index_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    monkeypatch.setenv("INDEX_QUEUE_URL", "index-queue-url")
    s3 = FakeS3(SYNTHETIC_CCDA)
    sqs = FakeSqs()

    result = process_parse_batch(_parse_event("participant=FACILITY/document.xml"), s3, sqs)

    assert result == {"batchItemFailures": []}
    assert len(s3.put_calls) == 1
    parsed = json.loads(s3.put_calls[0]["Body"])
    assert parsed["sourceFormat"] == "ccda"
    assert s3.put_calls[0]["Key"] == f"ccda/{parsed['documentId']}.json"
    assert json.loads(sqs.send_calls[0]["MessageBody"]) == {
        "bucket": "parsed-bucket",
        "key": f"ccda/{parsed['documentId']}.json",
        "documentId": parsed["documentId"],
        "sourceFormat": "ccda",
    }


def test_permanent_parse_error_is_persisted_without_retry(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    monkeypatch.setenv("INDEX_QUEUE_URL", "index-queue-url")
    s3 = FakeS3(SYNTHETIC_HL7)

    with caplog.at_level(logging.ERROR, logger="src.parse_handler"):
        result = process_parse_batch(_parse_event("sensitive-patient-name.pdf"), s3, FakeSqs())

    assert result == {"batchItemFailures": []}
    error_record = json.loads(s3.put_calls[0]["Body"])
    assert error_record["stage"] == "parsing"
    assert error_record["failureCategory"] == "unsupported_extension"
    assert error_record["retryable"] is False
    assert error_record["final"] is True
    log_event = json.loads(caplog.records[-1].message)
    assert log_event == {
        "errorType": "ParseError",
        "event": "record_processing_failed",
        "failureCategory": "unsupported_extension",
        "retryable": False,
        "stage": "parsing",
    }
    assert "sensitive-patient-name" not in caplog.text
    assert "synthetic routing smoke test" not in error_record["message"]


def test_transient_parse_failure_is_reported_for_retry(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    sensitive_exception = "temporary S3 failure for patient-sensitive-value"
    s3 = FakeS3(RuntimeError(sensitive_exception))

    with caplog.at_level(logging.ERROR, logger="src.parse_handler"):
        result = process_parse_batch(_parse_event(), s3, FakeSqs())

    assert result == {"batchItemFailures": [{"itemIdentifier": "parse-message-1"}]}
    error_record = json.loads(s3.put_calls[0]["Body"])
    assert error_record["failureCategory"] == "unexpected_failure"
    assert error_record["retryable"] is True
    assert error_record["final"] is False
    assert sensitive_exception not in json.dumps(error_record)
    assert sensitive_exception not in caplog.text


def test_index_handler_creates_index_and_bulk_indexes_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {"documentId": "doc-1", "messageType": "ORU", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport(
        [
            (404, {}),
            (200, {"acknowledged": True}),
            (200, {"errors": False, "items": [{"index": {"status": 201}}]}),
        ]
    )
    event = {
        "Records": [
            {
                "messageId": "index-message-1",
                "body": json.dumps(
                    {"bucket": "parsed-bucket", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            }
        ]
    }

    result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": []}
    assert [call[:2] for call in transport.calls] == [
        ("HEAD", "/hl7-messages-v1"),
        ("PUT", "/hl7-messages-v1"),
        ("POST", "/_bulk"),
    ]
    mapping_body = transport.calls[1][2]
    assert mapping_body is not None
    assert "settings" not in json.loads(mapping_body)
    bulk_body = transport.calls[-1][2]
    assert bulk_body is not None
    assert '"_id":"doc-1"' in bulk_body.decode()


def test_index_handler_reports_rejected_bulk_item(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {"documentId": "doc-1", "messageType": "ORU", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
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
                                "status": 429,
                                "error": {
                                    "type": "rejected_execution_exception",
                                    "reason": "patient-sensitive-backend-response",
                                },
                            }
                        }
                    ],
                },
            ),
        ]
    )
    event = {
        "Records": [
            {
                "messageId": "index-message-1",
                "body": json.dumps(
                    {"bucket": "parsed-bucket", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            }
        ]
    }

    with caplog.at_level(logging.ERROR, logger="src.index_handler"):
        result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": [{"itemIdentifier": "index-message-1"}]}
    error_record = json.loads(s3.put_calls[0]["Body"])
    assert error_record["stage"] == "indexing"
    assert error_record["failureCategory"] == "document_rejected"
    assert error_record["httpStatus"] == 429
    assert error_record["backendErrorType"] == "rejected_execution_exception"
    assert error_record["retryable"] is True
    log_event = json.loads(caplog.records[-1].message)
    assert log_event["failureCategory"] == "document_rejected"
    assert log_event["httpStatus"] == 429
    assert log_event["backendErrorType"] == "rejected_execution_exception"
    assert log_event["retryable"] is True
    assert "backendErrorMessage" not in log_event
    assert "backendErrorMessage" not in error_record
    assert "patient-sensitive-backend-response" not in caplog.text
    assert "patient-sensitive-backend-response" not in json.dumps(error_record)


def test_parse_handler_rejects_oversized_object_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    monkeypatch.setenv("MAX_OBJECT_BYTES", "1")
    s3 = FakeS3(SYNTHETIC_HL7)

    result = process_parse_batch(_parse_event(), s3, FakeSqs())

    assert result == {"batchItemFailures": []}
    assert json.loads(s3.put_calls[0]["Body"])["retryable"] is False


def test_parse_handler_persists_malformed_event_without_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    s3 = FakeS3(SYNTHETIC_HL7)
    event = {
        "Records": [
            {
                "messageId": "malformed-event",
                "body": "{",
            }
        ]
    }

    result = process_parse_batch(event, s3, FakeSqs())

    assert result == {"batchItemFailures": []}
    error_record = json.loads(s3.put_calls[0]["Body"])
    assert error_record["source"] is None


def test_index_handler_retries_malformed_pointer_and_index_lookup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {"documentId": "doc-1", "messageType": "ORU", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport([(503, {})])
    event = {
        "Records": [
            {"messageId": "bad-pointer", "body": "{}"},
            {
                "messageId": "lookup-failure",
                "body": json.dumps(
                    {"bucket": "parsed", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            },
        ]
    }

    result = process_index_batch(event, s3, transport)

    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "bad-pointer"},
            {"itemIdentifier": "lookup-failure"},
        ]
    }
    assert len(s3.put_calls) == 2


def test_index_handler_accepts_concurrent_index_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ERROR_BUCKET", raising=False)
    document = {"documentId": "doc-1", "messageType": "ORU", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport(
        [
            (404, {}),
            (400, {"error": {"type": "resource_already_exists_exception"}}),
            (200, {"items": [{"index": {"status": 200}}]}),
        ]
    )
    event = {
        "Records": [
            {
                "messageId": "index-message-1",
                "body": json.dumps(
                    {"bucket": "parsed", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            }
        ]
    }

    result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": []}
    assert s3.put_calls == []


def test_index_handler_retries_failed_bulk_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {"documentId": "doc-1", "messageType": "ORU", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport([(200, {}), (503, {})])
    event = {
        "Records": [
            {
                "messageId": "index-message-1",
                "body": json.dumps(
                    {"bucket": "parsed", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            }
        ]
    }

    result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": [{"itemIdentifier": "index-message-1"}]}
    assert json.loads(s3.put_calls[0]["Body"])["retryable"] is True


class _FakeCredentials:
    def get_frozen_credentials(self) -> object:
        return object()


class _FakeSession:
    def get_credentials(self) -> _FakeCredentials:
        return _FakeCredentials()


class _FakeAwsRequest:
    def __init__(
        self,
        *,
        method: str,
        url: str,
        data: bytes | None,
        headers: dict[str, str],
    ) -> None:
        self.method = method
        self.url = url
        self.data = data
        self.headers = headers

    def prepare(self) -> SimpleNamespace:
        return SimpleNamespace(headers=self.headers)


class _FakeSigV4Auth:
    invocations: ClassVar[list[tuple[str, str]]] = []
    signed_headers: ClassVar[list[dict[str, str]]] = []

    def __init__(self, _credentials: object, service: str, region: str) -> None:
        self.invocations.append((service, region))

    def add_auth(self, request: _FakeAwsRequest) -> None:
        self.signed_headers.append(dict(request.headers))
        request.headers["Authorization"] = "synthetic-signature"


class _FakeHttpResponse:
    status = 200

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"acknowledged":true}'


def _install_fake_botocore(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeSigV4Auth.invocations.clear()
    _FakeSigV4Auth.signed_headers.clear()
    package = ModuleType("botocore")
    package.__path__ = []
    auth = ModuleType("botocore.auth")
    auth.SigV4Auth = _FakeSigV4Auth  # type: ignore[attr-defined]
    awsrequest = ModuleType("botocore.awsrequest")
    awsrequest.AWSRequest = _FakeAwsRequest  # type: ignore[attr-defined]
    session = ModuleType("botocore.session")
    session.Session = _FakeSession  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "botocore", package)
    monkeypatch.setitem(sys.modules, "botocore.auth", auth)
    monkeypatch.setitem(sys.modules, "botocore.awsrequest", awsrequest)
    monkeypatch.setitem(sys.modules, "botocore.session", session)


def test_index_create_failure_logs_safe_category_without_backend_response(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {"documentId": "doc-1", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport(
        [
            (404, {}),
            (
                400,
                {
                    "error": {
                        "type": "illegal_argument_exception",
                        "reason": "schema-only index setting was rejected",
                    }
                },
            ),
        ]
    )
    event = {
        "Records": [
            {
                "messageId": "index-message-1",
                "body": json.dumps(
                    {"bucket": "parsed", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            }
        ]
    }

    with caplog.at_level(logging.ERROR, logger="src.index_handler"):
        result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": [{"itemIdentifier": "index-message-1"}]}
    error_record = json.loads(s3.put_calls[0]["Body"])
    assert error_record["failureCategory"] == "index_create_failed"
    assert error_record["httpStatus"] == 400
    assert error_record["backendErrorType"] == "illegal_argument_exception"
    assert error_record["backendErrorMessage"] == "schema-only index setting was rejected"
    log_event = json.loads(caplog.records[-1].message)
    assert log_event == {
        "backendErrorMessage": "schema-only index setting was rejected",
        "backendErrorType": "illegal_argument_exception",
        "errorType": "IndexingError",
        "event": "record_processing_failed",
        "failureCategory": "index_create_failed",
        "httpStatus": 400,
        "retryable": True,
        "stage": "indexing",
    }
    assert "schema-only index setting was rejected" in caplog.text


def test_index_create_failure_redacts_unknown_backend_error_type(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {"documentId": "doc-1", "sourceFormat": "hl7-v2"}
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport(
        [
            (404, {}),
            (
                400,
                {
                    "error": {
                        "type": "patient-sensitive-custom-type",
                        "reason": "x" * 1200,
                    }
                },
            ),
        ]
    )
    event = {
        "Records": [
            {
                "messageId": "index-message-1",
                "body": json.dumps(
                    {"bucket": "parsed", "key": "hl7/doc-1.json", "documentId": "doc-1"}
                ),
            }
        ]
    }

    with caplog.at_level(logging.ERROR, logger="src.index_handler"):
        result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": [{"itemIdentifier": "index-message-1"}]}
    error_record = json.loads(s3.put_calls[0]["Body"])
    log_event = json.loads(caplog.records[-1].message)
    assert error_record["backendErrorType"] == "other"
    assert log_event["backendErrorType"] == "other"
    assert error_record["backendErrorMessage"] == "x" * 1024
    assert log_event["backendErrorMessage"] == "x" * 1024
    assert "patient-sensitive-custom-type" not in caplog.text
    assert "patient-sensitive-custom-type" not in json.dumps(error_record)


def test_signed_transport_returns_successful_json_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_botocore(monkeypatch)
    monkeypatch.setattr(
        "src.index_handler.urllib.request.urlopen", lambda *_args, **_kwargs: _FakeHttpResponse()
    )
    transport = SignedOpenSearchTransport(
        endpoint="search.example", region="us-west-2", service="aoss"
    )

    status, response = transport.request("POST", "/_bulk", b"{}\n")

    assert status == 200
    assert response == {"acknowledged": True}
    assert transport.endpoint == "https://search.example"
    assert _FakeSigV4Auth.invocations == [("aoss", "us-west-2")]
    assert _FakeSigV4Auth.signed_headers == [
        {
            "Content-Type": "application/x-ndjson",
            "x-amz-content-sha256": hashlib.sha256(b"{}\n").hexdigest(),
        }
    ]


def test_signed_transport_rejects_non_https_endpoint() -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        SignedOpenSearchTransport(
            endpoint="http://search.example", region="us-west-2", service="aoss"
        )


def test_signed_transport_returns_http_error_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_botocore(monkeypatch)
    http_error = urllib.error.HTTPError(
        "https://search.example/index",
        429,
        "rejected",
        hdrs=cast(Any, None),
        fp=BytesIO(b'{"error":"rejected"}'),
    )

    def raise_http_error(*_args: object, **_kwargs: object) -> _FakeHttpResponse:
        raise http_error

    monkeypatch.setattr("src.index_handler.urllib.request.urlopen", raise_http_error)
    transport = SignedOpenSearchTransport(
        endpoint="https://search.example/", region="us-west-2", service="aoss"
    )

    status, response = transport.request("GET", "/index")

    assert status == 429
    assert response == {"error": "rejected"}
    assert (
        _FakeSigV4Auth.signed_headers[0]["x-amz-content-sha256"] == hashlib.sha256(b"").hexdigest()
    )


def test_index_handler_routes_ccda_to_separate_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ERROR_BUCKET", "error-bucket")
    document = {
        "documentId": "ccda-doc-1",
        "sourceFormat": "ccda",
        "documentTime": "2026-08-10T12:00:00Z",
        "CD": {"body": {"vitalSigns-section": {"_present": True}}},
    }
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport(
        [
            (404, {}),
            (201, {"acknowledged": True}),
            (200, {"items": [{"index": {"status": 201}}]}),
        ]
    )
    event = {
        "Records": [
            {
                "messageId": "ccda-index-message",
                "body": json.dumps(
                    {
                        "bucket": "parsed",
                        "key": "ccda/ccda-doc-1.json",
                        "documentId": "ccda-doc-1",
                        "sourceFormat": "ccda",
                    }
                ),
            }
        ]
    }

    result = process_index_batch(event, s3, transport)

    assert result == {"batchItemFailures": []}
    assert [call[:2] for call in transport.calls] == [
        ("HEAD", "/ccda-documents-v1"),
        ("PUT", "/ccda-documents-v1"),
        ("POST", "/_bulk"),
    ]
    mapping_body = transport.calls[1][2]
    assert mapping_body is not None
    mapping_document = json.loads(mapping_body)
    assert "settings" not in mapping_document
    mapping = mapping_document["mappings"]
    assert mapping["properties"]["documentTime"] == {"type": "date"}
    string_mapping = mapping["dynamic_templates"][0]["strings_with_keyword"]["mapping"]
    assert string_mapping["type"] == "text"
    assert string_mapping["fields"]["keyword"] == {
        "type": "keyword",
        "ignore_above": 2048,
    }
