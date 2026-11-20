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
    INDEX_MISSING,
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


def test_canonicalize_path_encodes_query_values_for_sigv4() -> None:
    from src.search_store import _canonicalize_path

    # "*" must become %2A so the signed bytes match the service-side canonical form.
    assert (
        _canonicalize_path("/hl7-messages-v1/_field_caps?fields=*")
        == "/hl7-messages-v1/_field_caps?fields=%2A"
    )
    # Paths without query strings pass through untouched.
    assert _canonicalize_path("/_bulk") == "/_bulk"
    # Already-encoded values stay stable (idempotent).
    assert _canonicalize_path("/i/_field_caps?fields=%2A") == "/i/_field_caps?fields=%2A"
    # Multiple parameters are each encoded.
    assert _canonicalize_path("/i/_x?a=1&b=*") == "/i/_x?a=1&b=%2A"


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
    assert mapping["settings"] == {"index.mapping.total_fields.limit": 10_000}
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
    payload = json.loads(body)
    assert payload["settings"] == {"index.mapping.total_fields.limit": 10_000}
    mapping = payload["mappings"]
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


def test_create_index_false_never_issues_put_and_indexes_when_present() -> None:
    transport = FakeTransport(
        [
            (200, {}),  # HEAD: index exists
            (200, {"items": [{"index": {"status": 201}}]}),
        ]
    )

    index_documents([_document()], transport, create_index=False)

    methods = [call[0] for call in transport.calls]
    assert methods == ["HEAD", "POST"]
    assert "PUT" not in methods


def test_create_index_false_fails_clearly_when_index_is_missing() -> None:
    transport = FakeTransport([(404, {})])

    with pytest.raises(IndexingError) as captured:
        index_documents([_document()], transport, create_index=False)

    assert str(captured.value) == INDEX_MISSING
    assert captured.value.http_status == 404
    # No creation attempt and no bulk write were made.
    assert [call[0] for call in transport.calls] == ["HEAD"]


# ---- Prism field mappings ------------------------------------------------------------------

# Top-level fields from the customer's Prism export that this platform produces (Prism-internal
# *Keys, documentKey/messageKey/objectKey, entityType, messageIteration, createdBy/updatedBy,
# and prism.* are intentionally absent).
PRISM_HL7_FIELDS = {
    "clinicalTime": "date",
    "createdTime": "date",
    "updatedTime": "date",
    "messageReceivedDate": "date",
    "sourceLastUpdatedTime": "date",
    "messageTime": "date",
    "messageEventType": "text",
    "patientFullId": "text",
    **dict.fromkeys(
        (
            "messageProcessedLevel",
            "messageStatus",
            "patientId",
            "patientIdAa",
            "patientMpiId",
            "patientMpiId_old",
            "resultType",
            "sendingApplication",
            "sha256",
            "sourceFacility",
            "sourceFacilityId",
            "sourceMessageId",
            "sourceMessageType",
        ),
        "keyword",
    ),
}
PRISM_CCDA_FIELDS = {
    **PRISM_HL7_FIELDS,
    "timeZoneId": "keyword",
    "chiefComplaint": "keyword",
    "reasonForVisit": "keyword",
}


@pytest.mark.parametrize(
    ("mapping_name", "expected"),
    [("HL7_INDEX_MAPPING", PRISM_HL7_FIELDS), ("CCDA_INDEX_MAPPING", PRISM_CCDA_FIELDS)],
)
def test_new_indexes_declare_prism_field_types(mapping_name: str, expected: dict[str, str]) -> None:
    from src import search_store

    properties = getattr(search_store, mapping_name)["mappings"]["properties"]
    assert {name: properties[name]["type"] for name in expected} == expected


def test_ccda_author_mapping_matches_prism_shape() -> None:
    from src.search_store import CCDA_INDEX_MAPPING

    properties = CCDA_INDEX_MAPPING["mappings"]["properties"]
    for name in ("author", "authorOther"):
        author = properties[name]["properties"]
        assert author["name"]["properties"]["fullName"]["type"] == "keyword"
        assert author["nameOther"]["properties"]["count"]["type"] == "long"
        assert author["effectiveDate"]["properties"]["value"]["type"] == "date"
        assert author["code"]["properties"]["code"]["type"] == "text"
        assert author["code"]["properties"]["description"]["type"] == "keyword"
        organization = author["organization"]["properties"]
        assert organization["telecom"]["properties"]["phoneNumber"]["type"] == "keyword"
        assert organization["identifierOther"]["properties"]["entry"]["properties"]["id"]


def _existing_index_transport(existing: dict[str, Any], put_status: int = 200) -> FakeTransport:
    return FakeTransport(
        [
            (200, {}),  # HEAD: index exists
            (200, {"hl7-messages-v1": {"mappings": {"properties": existing}}}),  # GET _mapping
            (put_status, {"acknowledged": True}),  # PUT _mapping
            (200, {"items": [{"index": {"status": 201}}]}),  # bulk
        ]
    )


def test_existing_index_gains_only_the_missing_declared_fields() -> None:
    from src import search_store

    search_store._MAPPINGS_CHECKED.clear()
    # An older index: patientId was dynamically mapped earlier and must not be redeclared.
    transport = _existing_index_transport({"documentId": {}, "patientId": {"type": "keyword"}})

    index_documents([_document()], transport)

    methods = [(method, path) for method, path, _ in transport.calls]
    assert methods == [
        ("HEAD", "/hl7-messages-v1"),
        ("GET", "/hl7-messages-v1/_mapping"),
        ("PUT", "/hl7-messages-v1/_mapping"),
        ("POST", "/_bulk"),
    ]
    added = json.loads(cast(bytes, transport.calls[2][2]))["properties"]
    assert "patientId" not in added
    assert "documentId" not in added
    assert added["sha256"]["type"] == "keyword"
    assert added["createdTime"]["type"] == "date"

    # Checked once per process: the next batch goes straight to bulk.
    transport.responses = [(200, {}), (200, {"items": [{"index": {"status": 201}}]})]
    index_documents([_document("doc-2")], transport)
    assert [method for method, _, _ in transport.calls[4:]] == ["HEAD", "POST"]


def test_up_to_date_index_issues_no_mapping_update() -> None:
    from src import search_store
    from src.search_store import HL7_INDEX_MAPPING

    search_store._MAPPINGS_CHECKED.clear()
    current: dict[str, Any] = {name: {} for name in HL7_INDEX_MAPPING["mappings"]["properties"]}
    transport = FakeTransport(
        [
            (200, {}),
            (200, {"hl7-messages-v1": {"mappings": {"properties": current}}}),
            (200, {"items": [{"index": {"status": 201}}]}),
        ]
    )
    index_documents([_document()], transport)
    assert [method for method, _, _ in transport.calls] == ["HEAD", "GET", "POST"]


@pytest.mark.parametrize("failing_step", ["lookup", "update"])
def test_mapping_upgrade_failure_never_blocks_indexing(
    failing_step: str, caplog: pytest.LogCaptureFixture
) -> None:
    from src import search_store

    search_store._MAPPINGS_CHECKED.clear()
    if failing_step == "lookup":
        transport = FakeTransport(
            [
                (200, {}),
                (403, {"error": {"type": "security_exception"}}),
                (200, {"items": [{"index": {"status": 201}}]}),
            ]
        )
    else:
        transport = _existing_index_transport({}, put_status=400)

    with caplog.at_level("WARNING"):
        index_documents([_document()], transport)

    assert transport.calls[-1][0] == "POST"
    event = json.loads(caplog.records[-1].getMessage())
    assert event["event"] == "index_mapping_update_skipped"
    assert event["step"] == failing_step
