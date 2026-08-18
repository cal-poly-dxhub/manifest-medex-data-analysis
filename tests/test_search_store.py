import hashlib
import json
import sys
import urllib.error
from io import BytesIO
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from src.search_store import (
    BULK_COUNT_MISMATCH,
    BULK_REQUEST_FAILED,
    DOCUMENT_REJECTED,
    INDEX_CREATE_FAILED,
    INDEX_LOOKUP_FAILED,
    UNSUPPORTED_SOURCE_FORMAT,
    IndexingError,
    SignedOpenSearchTransport,
    index_documents,
)


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


def _document(document_id: str = "doc-1", source_format: str = "hl7-v2") -> dict[str, Any]:
    return {
        "documentId": document_id,
        "sourceFormat": source_format,
        "messageType": "ORU",
    }


def test_index_documents_creates_index_and_uses_deterministic_ids() -> None:
    transport = FakeTransport(
        [
            (404, {}),
            (201, {"acknowledged": True}),
            (200, {"items": [{"index": {"status": 201}}]}),
        ]
    )

    index_documents([_document()], transport)

    assert [call[:2] for call in transport.calls] == [
        ("HEAD", "/hl7-messages-v1"),
        ("PUT", "/hl7-messages-v1"),
        ("POST", "/_bulk"),
    ]
    mapping_body = transport.calls[1][2]
    assert mapping_body is not None
    mapping = json.loads(mapping_body)
    assert "settings" not in mapping
    assert mapping["mappings"]["properties"]["messageTime"] == {"type": "date"}
    bulk_body = transport.calls[2][2]
    assert bulk_body is not None
    assert '"_id":"doc-1"' in bulk_body.decode()


def test_ccda_index_has_text_and_keyword_mapping() -> None:
    transport = FakeTransport(
        [
            (404, {}),
            (200, {}),
            (200, {"items": [{"index": {"status": 200}}]}),
        ]
    )

    index_documents([_document("ccda-1", "ccda")], transport)

    assert transport.calls[0][:2] == ("HEAD", "/ccda-documents-v1")
    body = transport.calls[1][2]
    assert body is not None
    mapping = json.loads(body)["mappings"]
    dynamic = mapping["dynamic_templates"][0]["strings_with_keyword"]["mapping"]
    assert dynamic["type"] == "text"
    assert dynamic["fields"]["keyword"] == {"type": "keyword", "ignore_above": 2048}


def test_concurrent_index_creation_is_accepted() -> None:
    transport = FakeTransport(
        [
            (404, {}),
            (400, {"error": {"type": "resource_already_exists_exception"}}),
            (200, {"items": [{"index": {"status": 200}}]}),
        ]
    )

    index_documents([_document()], transport)

    assert len(transport.calls) == 3


@pytest.mark.parametrize(
    ("responses", "message", "status", "backend_type"),
    [
        (
            [(503, {"error": {"type": "cluster_block_exception"}})],
            INDEX_LOOKUP_FAILED,
            503,
            "cluster_block_exception",
        ),
        (
            [
                (404, {}),
                (400, {"error": {"type": "patient-sensitive-custom-type"}}),
            ],
            INDEX_CREATE_FAILED,
            400,
            "other",
        ),
        (
            [(200, {}), (413, {"error": {"type": "validation_exception"}})],
            BULK_REQUEST_FAILED,
            413,
            "validation_exception",
        ),
    ],
)
def test_index_failures_expose_only_bounded_operational_telemetry(
    responses: list[tuple[int, dict[str, Any]]],
    message: str,
    status: int,
    backend_type: str,
) -> None:
    with pytest.raises(IndexingError, match=message) as captured:
        index_documents([_document()], FakeTransport(responses))

    assert captured.value.http_status == status
    assert captured.value.backend_error_type == backend_type
    assert not hasattr(captured.value, "backend_error_message")


def test_bulk_item_rejection_raises_even_for_permanent_400() -> None:
    transport = FakeTransport(
        [
            (200, {}),
            (
                200,
                {
                    "items": [
                        {
                            "index": {
                                "status": 400,
                                "error": {"type": "mapper_parsing_exception"},
                            }
                        }
                    ]
                },
            ),
        ]
    )

    with pytest.raises(IndexingError, match=DOCUMENT_REJECTED) as captured:
        index_documents([_document("ccda-1", "ccda")], transport)

    assert captured.value.http_status == 400
    assert captured.value.backend_error_type == "mapper_parsing_exception"


def test_bulk_count_mismatch_fails_the_source_object() -> None:
    transport = FakeTransport([(200, {}), (200, {"items": []})])

    with pytest.raises(IndexingError, match=BULK_COUNT_MISMATCH):
        index_documents([_document()], transport)


@pytest.mark.parametrize(
    "documents",
    [
        [],
        [{"documentId": "doc", "sourceFormat": "unsupported"}],
        [{"documentId": "", "sourceFormat": "hl7-v2"}],
        [
            {"documentId": "doc-1", "sourceFormat": "hl7-v2"},
            {"documentId": "doc-2", "sourceFormat": "ccda"},
        ],
    ],
)
def test_invalid_document_sets_are_rejected(documents: list[dict[str, Any]]) -> None:
    expected = (
        UNSUPPORTED_SOURCE_FORMAT
        if documents and documents[0].get("sourceFormat") == "unsupported"
        else None
    )

    with pytest.raises(IndexingError, match=expected):
        index_documents(documents, FakeTransport([]))


def test_bulk_chunking_sends_oversized_documents_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_BULK_BYTES", "1")
    transport = FakeTransport(
        [
            (200, {}),
            (200, {"items": [{"index": {"status": 200}}]}),
            (200, {"items": [{"index": {"status": 200}}]}),
        ]
    )

    index_documents([_document("doc-1"), _document("doc-2")], transport)

    assert [call[:2] for call in transport.calls] == [
        ("HEAD", "/hl7-messages-v1"),
        ("POST", "/_bulk"),
        ("POST", "/_bulk"),
    ]


class _FakeCredentials:
    def get_frozen_credentials(self) -> object:
        return object()


class _FakeSession:
    credentials: ClassVar[_FakeCredentials | None] = _FakeCredentials()

    def get_credentials(self) -> _FakeCredentials | None:
        return self.credentials


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
    _FakeSession.credentials = _FakeCredentials()
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


def test_signed_transport_signs_payload_hash_and_returns_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_botocore(monkeypatch)
    monkeypatch.setattr(
        "src.search_store.urllib.request.urlopen", lambda *_args, **_kwargs: _FakeHttpResponse()
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


def test_signed_transport_returns_http_error_without_logging_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_botocore(monkeypatch)
    http_error = urllib.error.HTTPError(
        "https://search.example/index",
        429,
        "rejected",
        hdrs=cast(Any, None),
        fp=BytesIO(b'{"error":{"type":"too_many_requests_exception"}}'),
    )

    def raise_http_error(*_args: object, **_kwargs: object) -> _FakeHttpResponse:
        raise http_error

    monkeypatch.setattr("src.search_store.urllib.request.urlopen", raise_http_error)
    transport = SignedOpenSearchTransport(
        endpoint="https://search.example/", region="us-west-2", service="aoss"
    )

    status, response = transport.request("GET", "/index")

    assert status == 429
    assert response == {"error": {"type": "too_many_requests_exception"}}
    assert (
        _FakeSigV4Auth.signed_headers[0]["x-amz-content-sha256"] == hashlib.sha256(b"").hexdigest()
    )


def test_signed_transport_rejects_http_and_missing_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="must use HTTPS"):
        SignedOpenSearchTransport(
            endpoint="http://search.example", region="us-west-2", service="aoss"
        )

    _install_fake_botocore(monkeypatch)
    _FakeSession.credentials = None
    transport = SignedOpenSearchTransport(
        endpoint="search.example", region="us-west-2", service="aoss"
    )
    with pytest.raises(RuntimeError, match="credentials are unavailable"):
        transport.request("GET", "/index")
