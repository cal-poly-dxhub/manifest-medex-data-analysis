from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

BULK_REQUEST_FAILED = "OpenSearch bulk request failed"
BULK_COUNT_MISMATCH = "OpenSearch bulk response item count did not match request"
DOCUMENT_REJECTED = "OpenSearch rejected an indexed document"
INDEX_CREATE_FAILED = "OpenSearch index creation failed"
INDEX_LOOKUP_FAILED = "OpenSearch index lookup failed"
CREDENTIALS_UNAVAILABLE = "AWS credentials are unavailable for OpenSearch signing"
INVALID_ENDPOINT = "OpenSearch endpoint must use HTTPS"
UNSUPPORTED_SOURCE_FORMAT = "Parsed document source format is unsupported"
MAX_BULK_BYTES_DEFAULT = 5 * 1024 * 1024

_ALLOWED_BACKEND_ERROR_TYPES = frozenset(
    {
        "authorization_exception",
        "cluster_block_exception",
        "forbidden_exception",
        "illegal_argument_exception",
        "index_not_found_exception",
        "mapper_parsing_exception",
        "parse_exception",
        "rejected_execution_exception",
        "resource_already_exists_exception",
        "security_exception",
        "too_many_requests_exception",
        "validation_exception",
        "x_content_parse_exception",
    }
)

HL7_INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "dynamic_templates": [
            {
                "strings_as_keywords": {
                    "match_mapping_type": "string",
                    "mapping": {"type": "keyword", "ignore_above": 2048},
                }
            }
        ],
        "properties": {
            "documentId": {"type": "keyword"},
            "parserVersion": {"type": "keyword"},
            "sourceFormat": {"type": "keyword"},
            "sourceFacilityId": {"type": "keyword"},
            "participantId": {"type": "keyword"},
            "messageType": {"type": "keyword"},
            "triggerEvent": {"type": "keyword"},
            "messageControlId": {"type": "keyword"},
            "messageTime": {"type": "date"},
            "ingestTime": {"type": "date"},
            "rawObject": {"type": "object", "enabled": False},
        },
    },
}

CCDA_INDEX_MAPPING: dict[str, Any] = {
    "mappings": {
        "dynamic_templates": [
            {
                "strings_with_keyword": {
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 2048}},
                    },
                }
            }
        ],
        "properties": {
            "documentId": {"type": "keyword"},
            "parserVersion": {"type": "keyword"},
            "sourceFormat": {"type": "keyword"},
            "sourceFacilityId": {"type": "keyword"},
            "participantId": {"type": "keyword"},
            "documentTime": {"type": "date"},
            "ingestTime": {"type": "date"},
            "rawObject": {"type": "object", "enabled": False},
        },
    },
}


class IndexingError(RuntimeError):
    """Sanitized indexing failure with bounded non-clinical telemetry."""

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        backend_error_type: object = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.backend_error_type = _safe_backend_error_type(backend_error_type)


class SearchTransport(Protocol):
    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]: ...


@dataclass(frozen=True)
class _IndexedDocument:
    document_id: str
    document: dict[str, Any]


def index_documents(documents: list[dict[str, Any]], transport: SearchTransport) -> None:
    """Convergently index one source object's documents using deterministic IDs."""
    if not documents:
        raise IndexingError(BULK_COUNT_MISMATCH)
    index_name, mapping = _index_configuration(documents[0])
    indexed = [_indexed_document(document, documents[0]["sourceFormat"]) for document in documents]
    _ensure_index(transport, index_name, mapping)
    max_bulk_bytes = int(os.getenv("MAX_BULK_BYTES", str(MAX_BULK_BYTES_DEFAULT)))
    for chunk in _bulk_chunks(index_name, indexed, max_bulk_bytes):
        status, response = transport.request("POST", "/_bulk", _bulk_body(index_name, chunk))
        items = _validated_bulk_items(status, response, len(chunk))
        for response_item in items:
            result: dict[str, Any] = next(iter(response_item.values()), {})
            item_status = int(result.get("status", 500))
            if item_status < 200 or item_status >= 300:
                raise IndexingError(
                    DOCUMENT_REJECTED,
                    http_status=item_status,
                    backend_error_type=_response_error_type(result),
                )


def _indexed_document(document: dict[str, Any], expected_format: object) -> _IndexedDocument:
    if document.get("sourceFormat") != expected_format:
        raise IndexingError(UNSUPPORTED_SOURCE_FORMAT)
    document_id = document.get("documentId")
    if not isinstance(document_id, str) or not document_id:
        raise IndexingError(BULK_COUNT_MISMATCH)
    return _IndexedDocument(document_id=document_id, document=document)


def _index_configuration(document: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    if document.get("sourceFormat") == "hl7-v2":
        return os.getenv("OPENSEARCH_HL7_INDEX", "hl7-messages-v1"), HL7_INDEX_MAPPING
    if document.get("sourceFormat") == "ccda":
        return os.getenv("OPENSEARCH_CCDA_INDEX", "ccda-documents-v1"), CCDA_INDEX_MAPPING
    raise IndexingError(UNSUPPORTED_SOURCE_FORMAT)


def _ensure_index(
    transport: SearchTransport,
    index_name: str,
    mapping: dict[str, Any],
) -> None:
    index_path = f"/{quote(index_name, safe='')}"
    status, response = transport.request("HEAD", index_path)
    if status == 404:
        create_status, create_response = transport.request(
            "PUT",
            index_path,
            json.dumps(mapping, separators=(",", ":")).encode(),
        )
        error_type = _response_error_type(create_response)
        if create_status not in {200, 201} and error_type != "resource_already_exists_exception":
            raise IndexingError(
                INDEX_CREATE_FAILED,
                http_status=create_status,
                backend_error_type=error_type,
            )
    elif status < 200 or status >= 300:
        raise IndexingError(
            INDEX_LOOKUP_FAILED,
            http_status=status,
            backend_error_type=_response_error_type(response),
        )


def _bulk_chunks(
    index_name: str,
    documents: list[_IndexedDocument],
    max_bytes: int,
) -> list[list[_IndexedDocument]]:
    chunks: list[list[_IndexedDocument]] = []
    current: list[_IndexedDocument] = []
    current_bytes = 0
    for document in documents:
        document_bytes = len(_bulk_body(index_name, [document]))
        if current and current_bytes + document_bytes > max_bytes:
            chunks.append(current)
            current = []
            current_bytes = 0
        current.append(document)
        current_bytes += document_bytes
    if current:
        chunks.append(current)
    return chunks


def _bulk_body(index_name: str, documents: list[_IndexedDocument]) -> bytes:
    lines: list[str] = []
    for item in documents:
        lines.append(
            json.dumps(
                {"index": {"_index": index_name, "_id": item.document_id}},
                separators=(",", ":"),
            )
        )
        lines.append(json.dumps(item.document, separators=(",", ":"), sort_keys=True))
    return ("\n".join(lines) + "\n").encode()


def _validated_bulk_items(
    status: int,
    response: dict[str, Any],
    expected_count: int,
) -> list[dict[str, Any]]:
    if status < 200 or status >= 300:
        raise IndexingError(
            BULK_REQUEST_FAILED,
            http_status=status,
            backend_error_type=_response_error_type(response),
        )
    items = list(response.get("items", []))
    if len(items) != expected_count:
        raise IndexingError(BULK_COUNT_MISMATCH)
    return items


def _response_error_type(response: dict[str, Any]) -> str | None:
    error = response.get("error")
    if not isinstance(error, dict):
        return None
    return _safe_backend_error_type(error.get("type"))


def _safe_backend_error_type(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    return normalized if normalized in _ALLOWED_BACKEND_ERROR_TYPES else "other"


class SignedOpenSearchTransport:
    """Small SigV4 HTTP transport using botocore included in the Lambda runtime."""

    def __init__(self, *, endpoint: str, region: str, service: str) -> None:
        normalized_endpoint = endpoint.strip().rstrip("/")
        if normalized_endpoint.startswith("https://"):
            self.endpoint = normalized_endpoint
        elif "://" in normalized_endpoint:
            raise ValueError(INVALID_ENDPOINT)
        else:
            self.endpoint = f"https://{normalized_endpoint}"
        self.region = region
        self.service = service

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        from botocore.auth import SigV4Auth  # type: ignore[import-not-found]
        from botocore.awsrequest import AWSRequest  # type: ignore[import-not-found]
        from botocore.session import Session  # type: ignore[import-not-found]

        url = f"{self.endpoint}{path}"
        headers = {
            "Content-Type": "application/x-ndjson" if path == "/_bulk" else "application/json",
            "x-amz-content-sha256": hashlib.sha256(body or b"").hexdigest(),
        }
        credentials = Session().get_credentials()
        if credentials is None:
            raise RuntimeError(CREDENTIALS_UNAVAILABLE)
        request = AWSRequest(method=method, url=url, data=body, headers=headers)
        SigV4Auth(credentials.get_frozen_credentials(), self.service, self.region).add_auth(request)
        prepared = request.prepare()
        http_request = urllib.request.Request(  # noqa: S310
            url,
            data=body,
            headers=dict(prepared.headers.items()),
            method=method,
        )
        try:
            with urllib.request.urlopen(http_request, timeout=30) as response:  # noqa: S310
                payload = response.read()
                return response.status, json.loads(payload) if payload else {}
        except urllib.error.HTTPError as error:
            payload = error.read()
            return error.code, json.loads(payload) if payload else {}
