from __future__ import annotations

import hashlib
import json
import logging
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from urllib.parse import quote

DOCUMENT_ID_MISMATCH = "Index pointer document ID does not match parsed document"
BULK_REQUEST_FAILED = "OpenSearch bulk request failed"
BULK_COUNT_MISMATCH = "OpenSearch bulk response item count did not match request"
INDEX_CREATE_FAILED = "OpenSearch index creation failed"
INDEX_LOOKUP_FAILED = "OpenSearch index lookup failed"
BODY_NOT_READABLE = "S3 response body is not readable"
CREDENTIALS_UNAVAILABLE = "AWS credentials are unavailable for OpenSearch signing"
INVALID_ENDPOINT = "OpenSearch endpoint must use HTTPS"
UNSUPPORTED_SOURCE_FORMAT = "Parsed document source format is unsupported"
DOCUMENT_REJECTED = "OpenSearch rejected an indexed document"
MAX_BULK_BYTES_DEFAULT = 5 * 1024 * 1024
MAX_BACKEND_ERROR_MESSAGE_CHARS = 1024

LOGGER = logging.getLogger(__name__)
_INDEX_FAILURE_CATEGORIES = {
    BODY_NOT_READABLE: "parsed_body_not_readable",
    BULK_COUNT_MISMATCH: "bulk_count_mismatch",
    BULK_REQUEST_FAILED: "bulk_request_failed",
    CREDENTIALS_UNAVAILABLE: "credentials_unavailable",
    DOCUMENT_ID_MISMATCH: "document_id_mismatch",
    DOCUMENT_REJECTED: "document_rejected",
    INDEX_CREATE_FAILED: "index_create_failed",
    INDEX_LOOKUP_FAILED: "index_lookup_failed",
    UNSUPPORTED_SOURCE_FORMAT: "unsupported_source_format",
}
_INDEX_ERROR_TYPE_CATEGORIES = {
    "JSONDecodeError": "invalid_index_input",
    "KeyError": "invalid_index_input",
    "TimeoutError": "network_failure",
    "URLError": "network_failure",
}
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
    """Sanitized indexing failure with bounded operational metadata."""

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        backend_error_type: object = None,
        backend_error_message: object = None,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.backend_error_type = _safe_backend_error_type(backend_error_type)
        self.backend_error_message = _bounded_backend_error_message(backend_error_message)


class _ReadableBody(Protocol):
    def read(self) -> bytes: ...


class _S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


class _Transport(Protocol):
    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]: ...


@dataclass(frozen=True)
class _PendingDocument:
    message_id: str
    source_bucket: str
    source_key: str
    document_id: str
    index_name: str
    index_mapping: dict[str, Any]
    document: dict[str, Any]


def handler(event: dict[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
    """Lambda entry point using runtime-provided AWS credentials and boto3."""
    return process_batch(
        event,
        _aws_client("s3"),
        SignedOpenSearchTransport(
            endpoint=os.environ["OPENSEARCH_ENDPOINT"],
            region=os.environ["AWS_REGION"],
            service=os.environ["OPENSEARCH_SERVICE"],
        ),
    )


def process_batch(
    event: dict[str, Any],
    s3_client: _S3Client,
    transport: _Transport,
) -> dict[str, list[dict[str, str]]]:
    """Fetch parsed documents and bulk index them with per-message failure reporting."""
    records = list(event.get("Records", []))
    pending: list[_PendingDocument] = []
    failed_ids: set[str] = set()

    for record in records:
        message_id = str(record.get("messageId", "unknown"))
        try:
            pointer = json.loads(str(record["body"]))
            bucket = str(pointer["bucket"])
            key = str(pointer["key"])
            document_id = str(pointer["documentId"])
            response = s3_client.get_object(Bucket=bucket, Key=key)
            document = json.loads(_read_body(response["Body"]))
            _require_matching_document_id(document, document_id)
            index_name, index_mapping = _index_configuration(document)
            pending.append(
                _PendingDocument(
                    message_id=message_id,
                    source_bucket=bucket,
                    source_key=key,
                    document_id=document_id,
                    index_name=index_name,
                    index_mapping=index_mapping,
                    document=document,
                )
            )
        except Exception as error:
            failed_ids.add(message_id)
            _write_index_error(s3_client, message_id, None, error, retryable=True)

    grouped: dict[str, list[_PendingDocument]] = {}
    for item in pending:
        grouped.setdefault(item.index_name, []).append(item)
    for index_name, documents in grouped.items():
        mapping = documents[0].index_mapping
        try:
            _ensure_index(transport, index_name, mapping)
        except Exception as error:
            _fail_documents(failed_ids, s3_client, documents, error)
            continue
        max_bulk_bytes = int(os.getenv("MAX_BULK_BYTES", str(MAX_BULK_BYTES_DEFAULT)))
        for chunk in _bulk_chunks(index_name, documents, max_bulk_bytes):
            try:
                status, response = transport.request(
                    "POST",
                    "/_bulk",
                    _bulk_body(index_name, chunk),
                )
                items = _validated_bulk_items(status, response, len(chunk))
                for pending_document, response_item in zip(chunk, items, strict=True):
                    result = next(iter(response_item.values()))
                    item_status = int(result.get("status", 500))
                    if item_status >= 300:
                        failed_ids.add(pending_document.message_id)
                        _write_index_error(
                            s3_client,
                            pending_document.message_id,
                            pending_document,
                            IndexingError(
                                DOCUMENT_REJECTED,
                                http_status=item_status,
                                backend_error_type=_response_error_type(result),
                            ),
                            retryable=item_status == 429 or item_status >= 500,
                        )
            except Exception as error:
                _fail_documents(failed_ids, s3_client, chunk, error)

    return {
        "batchItemFailures": [{"itemIdentifier": message_id} for message_id in sorted(failed_ids)]
    }


def _index_configuration(document: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    source_format = document.get("sourceFormat")
    if source_format == "hl7-v2":
        name = (
            os.getenv("OPENSEARCH_HL7_INDEX") or os.getenv("OPENSEARCH_INDEX") or "hl7-messages-v1"
        )
        return name, HL7_INDEX_MAPPING
    if source_format == "ccda":
        return os.getenv("OPENSEARCH_CCDA_INDEX", "ccda-documents-v1"), CCDA_INDEX_MAPPING
    raise ValueError(UNSUPPORTED_SOURCE_FORMAT)


def _fail_documents(
    failed_ids: set[str],
    s3_client: _S3Client,
    documents: list[_PendingDocument],
    error: Exception,
) -> None:
    for document in documents:
        failed_ids.add(document.message_id)
        _write_index_error(
            s3_client,
            document.message_id,
            document,
            error,
            retryable=True,
        )


def _bulk_chunks(
    index_name: str,
    documents: list[_PendingDocument],
    max_bytes: int,
) -> list[list[_PendingDocument]]:
    chunks: list[list[_PendingDocument]] = []
    current: list[_PendingDocument] = []
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


def _require_matching_document_id(document: dict[str, Any], expected: str) -> None:
    if document.get("documentId") != expected:
        raise ValueError(DOCUMENT_ID_MISMATCH)


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
        raise RuntimeError(BULK_COUNT_MISMATCH)
    return items


def _ensure_index(
    transport: _Transport,
    index_name: str,
    mapping: dict[str, Any],
) -> None:
    index_path = f"/{quote(index_name, safe='')}"
    status, response = transport.request("HEAD", index_path)
    if status == 404:
        create_status, response = transport.request(
            "PUT",
            index_path,
            json.dumps(mapping, separators=(",", ":")).encode(),
        )
        error_type = _response_error_type(response)
        if create_status not in {200, 201} and error_type != "resource_already_exists_exception":
            raise IndexingError(
                INDEX_CREATE_FAILED,
                http_status=create_status,
                backend_error_type=error_type,
                backend_error_message=_response_error_message(response),
            )
    elif status < 200 or status >= 300:
        raise IndexingError(
            INDEX_LOOKUP_FAILED,
            http_status=status,
            backend_error_type=_response_error_type(response),
            backend_error_message=_response_error_message(response),
        )


def _bulk_body(index_name: str, documents: list[_PendingDocument]) -> bytes:
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


def _write_index_error(
    s3_client: _S3Client,
    message_id: str,
    pending: _PendingDocument | None,
    error: Exception,
    *,
    retryable: bool,
) -> None:
    failure_category = _index_failure_category(error)
    _log_index_failure(error, failure_category, retryable=retryable)
    error_bucket = os.getenv("ERROR_BUCKET")
    if not error_bucket:
        return
    document_id = pending.document_id if pending else message_id
    record = {
        "errorId": document_id,
        "stage": "indexing",
        "errorCode": type(error).__name__,
        "failureCategory": failure_category,
        "message": "A parsed document could not be indexed.",
        "retryable": retryable,
        "occurredAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source": (
            {"bucket": pending.source_bucket, "key": pending.source_key}
            if pending is not None
            else None
        ),
    }
    record.update(_index_failure_telemetry(error))
    s3_client.put_object(
        Bucket=error_bucket,
        Key=f"indexing/{document_id}.json",
        Body=json.dumps(record, separators=(",", ":"), sort_keys=True).encode(),
        ContentType="application/json",
    )


def _index_failure_category(error: Exception) -> str:
    return _INDEX_FAILURE_CATEGORIES.get(
        str(error),
        _INDEX_ERROR_TYPE_CATEGORIES.get(type(error).__name__, "unexpected_failure"),
    )


def _log_index_failure(
    error: Exception,
    failure_category: str,
    *,
    retryable: bool,
) -> None:
    event: dict[str, Any] = {
        "errorType": type(error).__name__,
        "event": "record_processing_failed",
        "failureCategory": failure_category,
        "retryable": retryable,
        "stage": "indexing",
    }
    event.update(_index_failure_telemetry(error))
    LOGGER.error(json.dumps(event, separators=(",", ":"), sort_keys=True))


def _index_failure_telemetry(error: Exception) -> dict[str, int | str]:
    if not isinstance(error, IndexingError):
        return {}
    telemetry: dict[str, int | str] = {}
    if error.http_status is not None:
        telemetry["httpStatus"] = error.http_status
    if error.backend_error_type is not None:
        telemetry["backendErrorType"] = error.backend_error_type
    if error.backend_error_message is not None:
        telemetry["backendErrorMessage"] = error.backend_error_message
    return telemetry


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


def _response_error_message(response: dict[str, Any]) -> str | None:
    error = response.get("error")
    if isinstance(error, dict):
        for field in ("reason", "message"):
            message = _bounded_backend_error_message(error.get(field))
            if message is not None:
                return message
    else:
        message = _bounded_backend_error_message(error)
        if message is not None:
            return message
    return _bounded_backend_error_message(response.get("message"))


def _bounded_backend_error_message(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    message = value.strip()
    if not message:
        return None
    return message[:MAX_BACKEND_ERROR_MESSAGE_CHARS]


def _read_body(body: object) -> bytes:
    if not hasattr(body, "read"):
        raise TypeError(BODY_NOT_READABLE)
    return cast(_ReadableBody, body).read()


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


def _aws_client(service: str) -> Any:
    import boto3  # type: ignore[import-not-found]

    return boto3.client(service)
