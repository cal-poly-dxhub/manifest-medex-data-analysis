from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from src.ccda_parser import parse_ccda_document
from src.parser import ParseError, SourceReference, parse_hl7_file

MAX_OBJECT_BYTES_DEFAULT = 50 * 1024 * 1024
OBJECT_TOO_LARGE = "Raw object exceeds the configured Lambda parsing limit"
UNSUPPORTED_EXTENSION = "Only HL7 and CCDA source objects are supported by this parser release"
INVALID_EVENT = "SQS record is not an S3 Object Created EventBridge event"
BODY_NOT_READABLE = "S3 response body is not readable"

LOGGER = logging.getLogger(__name__)
_PARSE_FAILURE_CATEGORIES = {
    INVALID_EVENT: "invalid_event",
    OBJECT_TOO_LARGE: "object_too_large",
    UNSUPPORTED_EXTENSION: "unsupported_extension",
    BODY_NOT_READABLE: "source_body_not_readable",
}
_PARSE_ERROR_TYPE_CATEGORIES = {"ParseError": "document_parse_failed"}


class _ReadableBody(Protocol):
    def read(self) -> bytes: ...


class _S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


class _SqsClient(Protocol):
    def send_message(self, **kwargs: Any) -> dict[str, Any]: ...


def handler(event: dict[str, Any], _context: Any) -> dict[str, list[dict[str, str]]]:
    """Lambda entry point using runtime-provided boto3 clients."""
    return process_batch(event, _aws_client("s3"), _aws_client("sqs"))


def process_batch(
    event: dict[str, Any],
    s3_client: _S3Client,
    sqs_client: _SqsClient,
) -> dict[str, list[dict[str, str]]]:
    """Process an SQS batch and report only retryable record failures."""
    failures: list[dict[str, str]] = []
    for record in event.get("Records", []):
        message_id = str(record.get("messageId", "unknown"))
        source: SourceReference | None = None
        try:
            source, ingested_at = _source_from_record(record)
            _process_source(source, ingested_at, s3_client, sqs_client)
        except ParseError as error:
            try:
                _write_error(
                    source,
                    message_id,
                    error,
                    retryable=False,
                    s3_client=s3_client,
                )
            except Exception:
                failures.append({"itemIdentifier": message_id})
        except Exception as error:
            try:
                _write_error(
                    source,
                    message_id,
                    error,
                    retryable=True,
                    s3_client=s3_client,
                )
            except Exception:
                failures.append({"itemIdentifier": message_id})
                continue
            failures.append({"itemIdentifier": message_id})
    return {"batchItemFailures": failures}


def _process_source(
    source: SourceReference,
    ingested_at: str | None,
    s3_client: _S3Client,
    sqs_client: _SqsClient,
) -> None:
    extension = source.key.lower().rsplit(".", maxsplit=1)[-1]
    if extension not in {"hl7", "txt", "xml"}:
        raise ParseError(UNSUPPORTED_EXTENSION)

    request: dict[str, Any] = {"Bucket": source.bucket, "Key": source.key}
    if source.version_id:
        request["VersionId"] = source.version_id
    response = s3_client.get_object(**request)
    content_length = int(response.get("ContentLength", 0))
    max_bytes = int(os.getenv("MAX_OBJECT_BYTES", str(MAX_OBJECT_BYTES_DEFAULT)))
    if content_length > max_bytes:
        raise ParseError(OBJECT_TOO_LARGE)

    body = response["Body"]
    payload = _read_body(body)
    if len(payload) > max_bytes:
        raise ParseError(OBJECT_TOO_LARGE)

    effective_source = SourceReference(
        bucket=source.bucket,
        key=source.key,
        version_id=source.version_id or _optional_string(response.get("VersionId")),
        etag=source.etag or _clean_etag(response.get("ETag")),
    )
    if extension == "xml":
        documents = [parse_ccda_document(payload, effective_source, ingested_at=ingested_at)]
    else:
        documents = parse_hl7_file(payload, effective_source, ingested_at=ingested_at)
    parsed_bucket = os.environ["PARSED_BUCKET"]
    index_queue_url = os.environ["INDEX_QUEUE_URL"]

    for document in documents:
        document_id = str(document["documentId"])
        source_format = str(document["sourceFormat"])
        key_prefix = "ccda" if source_format == "ccda" else "hl7"
        parsed_key = f"{key_prefix}/{document_id}.json"
        s3_client.put_object(
            Bucket=parsed_bucket,
            Key=parsed_key,
            Body=_json_bytes(document),
            ContentType="application/json",
        )
        sqs_client.send_message(
            QueueUrl=index_queue_url,
            MessageBody=json.dumps(
                {
                    "bucket": parsed_bucket,
                    "key": parsed_key,
                    "documentId": document_id,
                    "sourceFormat": source_format,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )


def _source_from_record(record: dict[str, Any]) -> tuple[SourceReference, str | None]:
    # decoding SQS message
    try:
        envelope = json.loads(str(record["body"]))
        event_source = envelope.get("source")
        detail_type = envelope.get("detail-type")
        detail = envelope["detail"]
        object_detail = detail["object"]
        source = SourceReference(
            bucket=str(detail["bucket"]["name"]),
            key=str(object_detail["key"]),
            version_id=_optional_string(object_detail.get("version-id")),
            etag=_clean_etag(object_detail.get("etag")),
        )
        ingested_at = _optional_string(envelope.get("time"))
    except (AttributeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ParseError(INVALID_EVENT) from error
    if event_source != "aws.s3" or detail_type != "Object Created":
        raise ParseError(INVALID_EVENT)
    return source, ingested_at


def _write_error(
    source: SourceReference | None,
    message_id: str,
    error: Exception,
    *,
    retryable: bool,
    s3_client: _S3Client,
) -> None:
    failure_category = _parse_failure_category(error)
    _log_parse_failure(error, failure_category, retryable=retryable)
    error_bucket = os.environ["ERROR_BUCKET"]
    error_id = _error_id(source, message_id)
    error_record = {
        "errorId": error_id,
        "stage": "parsing",
        "errorCode": type(error).__name__,
        "failureCategory": failure_category,
        "message": "The source object could not be parsed.",
        "retryable": retryable,
        "final": not retryable,
        "occurredAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source": (
            {
                "bucket": source.bucket,
                "key": source.key,
                "versionId": source.version_id,
                "etag": source.etag,
            }
            if source is not None
            else None
        ),
    }
    s3_client.put_object(
        Bucket=error_bucket,
        Key=f"parsing/{error_id}.json",
        Body=_json_bytes(error_record),
        ContentType="application/json",
    )


def _parse_failure_category(error: Exception) -> str:
    return _PARSE_FAILURE_CATEGORIES.get(
        str(error),
        _PARSE_ERROR_TYPE_CATEGORIES.get(type(error).__name__, "unexpected_failure"),
    )


def _log_parse_failure(
    error: Exception,
    failure_category: str,
    *,
    retryable: bool,
) -> None:
    LOGGER.error(
        json.dumps(
            {
                "errorType": type(error).__name__,
                "event": "record_processing_failed",
                "failureCategory": failure_category,
                "retryable": retryable,
                "stage": "parsing",
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _error_id(source: SourceReference | None, message_id: str) -> str:
    import hashlib

    identity = (
        f"{source.bucket}\0{source.key}\0{source.version_id or source.etag or ''}"
        if source is not None
        else message_id
    )
    return hashlib.sha256(identity.encode()).hexdigest()


def _read_body(body: object) -> bytes:
    if not hasattr(body, "read"):
        raise TypeError(BODY_NOT_READABLE)
    return cast(_ReadableBody, body).read()  # get the body as bytes


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _clean_etag(value: object) -> str | None:
    text = _optional_string(value)
    return text.strip('"') if text else None


def _optional_string(value: object) -> str | None:
    return str(value) if value not in {None, ""} else None


def _aws_client(service: str) -> Any:
    import boto3  # type: ignore[import-not-found]

    return boto3.client(service)
