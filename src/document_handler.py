from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, cast

from src.ccda_parser import parse_ccda_document
from src.metadata_store import DataApiMetadataStore, MetadataRecord, MetadataStoreError
from src.parser import ParseError, SourceReference, parse_hl7_file
from src.search_store import (
    IndexingError,
    SearchTransport,
    SignedOpenSearchTransport,
    index_documents,
)

MAX_OBJECT_BYTES_DEFAULT = 50 * 1024 * 1024
OBJECT_TOO_LARGE = "Raw object exceeds the configured Lambda parsing limit"
INVALID_EVENT = "SQS record is not an S3 Object Created EventBridge event"
INVALID_INGESTED_TIME = "S3 event time is missing or invalid"
INVALID_SOURCE_ROUTE = "Source object does not match the format-specific ingestion route"
BODY_NOT_READABLE = "S3 response body is not readable"
FORMAT_CONFIGURATION_MISMATCH = "Lambda source format configuration does not match its handler"
LOCATION_COUNT_MISMATCH = "Parsed document location count mismatch"

LOGGER = logging.getLogger(__name__)
_RUNTIME_PROCESSORS: dict[str, DocumentProcessor] = {}


class _ReadableBody(Protocol):
    def read(self) -> bytes: ...


class S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


class MetadataStore(Protocol):
    def upsert(self, records: list[MetadataRecord]) -> None: ...


@dataclass(frozen=True)
class ParsedLocation:
    key: str
    version_id: str | None


@dataclass
class DocumentProcessor:
    """Combined parser, parsed-object writer, indexer, and metadata writer."""

    expected_format: str
    s3_client: S3Client
    search_transport: SearchTransport
    metadata_store: MetadataStore

    def process_batch(self, event: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
        failures: list[dict[str, str]] = []
        for record in event.get("Records", []):
            message_id = str(record.get("messageId", "unknown"))
            source: SourceReference | None = None
            stage = "event_validation"
            try:
                source, ingested_time = _source_from_record(record)
                _validate_route(source, self.expected_format)
                stage = "source_read"
                documents, effective_source = _parse_source(
                    source,
                    ingested_time,
                    self.expected_format,
                    self.s3_client,
                )
                stage = "parsed_storage"
                locations = _write_parsed_documents(
                    documents,
                    self.expected_format,
                    self.s3_client,
                )
                stage = "indexing"
                index_documents(documents, self.search_transport)
                stage = "metadata"
                self.metadata_store.upsert(
                    _metadata_records(
                        documents,
                        locations,
                        effective_source,
                        ingested_time,
                    )
                )
            except Exception as error:
                _record_failure(
                    self.s3_client,
                    source,
                    message_id,
                    self.expected_format,
                    stage,
                    error,
                )
                failures.append({"itemIdentifier": message_id})
        return {"batchItemFailures": failures}


def runtime_handler(
    event: dict[str, Any],
    _context: Any,
    *,
    expected_format: str,
) -> dict[str, list[dict[str, str]]]:
    """Build runtime dependencies once per execution environment and process an SQS batch."""
    configured_format = os.environ["SOURCE_FORMAT"]
    if configured_format != expected_format:
        raise RuntimeError(FORMAT_CONFIGURATION_MISMATCH)
    processor = _RUNTIME_PROCESSORS.get(expected_format)
    if processor is None:
        processor = DocumentProcessor(
            expected_format=expected_format,
            s3_client=_aws_client("s3"),
            search_transport=SignedOpenSearchTransport(
                endpoint=os.environ["OPENSEARCH_ENDPOINT"],
                region=os.environ["AWS_REGION"],
                service=os.environ["OPENSEARCH_SERVICE"],
            ),
            metadata_store=DataApiMetadataStore(
                _aws_client("rds-data"),
                cluster_arn=os.environ["METADATA_CLUSTER_ARN"],
                secret_arn=os.environ["METADATA_SECRET_ARN"],
                database=os.environ["METADATA_DATABASE"],
                table_name=os.environ["METADATA_TABLE"],
            ),
        )
        _RUNTIME_PROCESSORS[expected_format] = processor
    return processor.process_batch(event)


def _source_from_record(record: dict[str, Any]) -> tuple[SourceReference, str]:
    try:
        envelope = json.loads(str(record["body"]))
        detail = envelope["detail"]
        object_detail = detail["object"]
        source = SourceReference(
            bucket=str(detail["bucket"]["name"]),
            key=str(object_detail["key"]),
            version_id=_optional_string(object_detail.get("version-id")),
            etag=_clean_etag(object_detail.get("etag")),
        )
        ingested_time = str(envelope["time"])
    except (AttributeError, json.JSONDecodeError, KeyError, TypeError) as error:
        raise ParseError(INVALID_EVENT) from error
    if envelope.get("source") != "aws.s3" or envelope.get("detail-type") != "Object Created":
        raise ParseError(INVALID_EVENT)
    _validate_ingested_time(ingested_time)
    return source, ingested_time


def _validate_ingested_time(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ParseError(INVALID_INGESTED_TIME) from error
    if parsed.tzinfo is None:
        raise ParseError(INVALID_INGESTED_TIME)


def _validate_route(source: SourceReference, expected_format: str) -> None:
    key = source.key.lower()
    if expected_format == "hl7-v2":
        valid = key.startswith("incoming/hl7/") and key.endswith((".hl7", ".txt"))
    elif expected_format == "ccda":
        valid = key.startswith("incoming/ccda/") and key.endswith(".xml")
    else:
        valid = False
    if not valid:
        raise ParseError(INVALID_SOURCE_ROUTE)


def _parse_source(
    source: SourceReference,
    ingested_time: str,
    expected_format: str,
    s3_client: S3Client,
) -> tuple[list[dict[str, Any]], SourceReference]:
    request: dict[str, Any] = {"Bucket": source.bucket, "Key": source.key}
    if source.version_id:
        request["VersionId"] = source.version_id
    response = s3_client.get_object(**request)
    max_bytes = int(os.getenv("MAX_OBJECT_BYTES", str(MAX_OBJECT_BYTES_DEFAULT)))
    if int(response.get("ContentLength", 0)) > max_bytes:
        raise ParseError(OBJECT_TOO_LARGE)
    payload = _read_body(response["Body"])
    if len(payload) > max_bytes:
        raise ParseError(OBJECT_TOO_LARGE)

    effective_source = SourceReference(
        bucket=source.bucket,
        key=source.key,
        version_id=source.version_id or _optional_string(response.get("VersionId")),
        etag=source.etag or _clean_etag(response.get("ETag")),
    )
    if expected_format == "hl7-v2":
        documents = parse_hl7_file(payload, effective_source, ingested_at=ingested_time)
    else:
        documents = [parse_ccda_document(payload, effective_source, ingested_at=ingested_time)]
    if not documents or any(
        document.get("sourceFormat") != expected_format for document in documents
    ):
        raise ParseError(INVALID_SOURCE_ROUTE)
    return documents, effective_source


def _write_parsed_documents(
    documents: list[dict[str, Any]],
    expected_format: str,
    s3_client: S3Client,
) -> list[ParsedLocation]:
    parsed_bucket = os.environ["PARSED_BUCKET"]
    prefix = "hl7" if expected_format == "hl7-v2" else "ccda"
    locations: list[ParsedLocation] = []
    for document in documents:
        document_id = str(document["documentId"])
        key = f"{prefix}/{document_id}.json"
        response = s3_client.put_object(
            Bucket=parsed_bucket,
            Key=key,
            Body=_json_bytes(document),
            ContentType="application/json",
        )
        locations.append(
            ParsedLocation(key=key, version_id=_optional_string(response.get("VersionId")))
        )
    return locations


def _metadata_records(
    documents: list[dict[str, Any]],
    locations: list[ParsedLocation],
    source: SourceReference,
    ingested_time: str,
) -> list[MetadataRecord]:
    if len(documents) != len(locations):
        raise RuntimeError(LOCATION_COUNT_MISMATCH)
    parsed_bucket = os.environ["PARSED_BUCKET"]
    records: list[MetadataRecord] = []
    for document, location in zip(documents, locations, strict=True):
        source_format = str(document["sourceFormat"])
        document_time_value = (
            document.get("messageTime")
            if source_format == "hl7-v2"
            else document.get("documentTime")
        )
        records.append(
            MetadataRecord(
                document_id=str(document["documentId"]),
                source_format=source_format,
                document_time=_optional_string(document_time_value),
                ingested_time=ingested_time,
                raw_s3_uri=f"s3://{source.bucket}/{source.key}",
                raw_version_id=source.version_id,
                parsed_s3_uri=f"s3://{parsed_bucket}/{location.key}",
                parsed_version_id=location.version_id,
            )
        )
    return records


def _record_failure(
    s3_client: S3Client,
    source: SourceReference | None,
    message_id: str,
    expected_format: str,
    stage: str,
    error: Exception,
) -> None:
    event: dict[str, Any] = {
        "errorType": type(error).__name__,
        "event": "record_processing_failed",
        "failureCategory": _failure_category(error),
        "retryable": True,
        "sourceFormat": expected_format,
        "stage": stage,
    }
    if isinstance(error, IndexingError):
        if error.http_status is not None:
            event["httpStatus"] = error.http_status
        if error.backend_error_type is not None:
            event["backendErrorType"] = error.backend_error_type
    LOGGER.error(json.dumps(event, separators=(",", ":"), sort_keys=True))

    error_bucket = os.getenv("ERROR_BUCKET")
    if not error_bucket:
        return
    error_id = _error_id(source, message_id)
    lane = "hl7" if expected_format == "hl7-v2" else "ccda"
    error_record = {
        **event,
        "errorId": error_id,
        "message": "The source object did not complete every ingestion stage.",
        "final": False,
    }
    try:
        s3_client.put_object(
            Bucket=error_bucket,
            Key=f"processing/{lane}/{error_id}.json",
            Body=_json_bytes(error_record),
            ContentType="application/json",
        )
    except Exception:
        LOGGER.error(  # noqa: TRY400 - traceback could expose a sensitive SDK exception
            json.dumps(
                {
                    "event": "error_record_write_failed",
                    "sourceFormat": expected_format,
                    "stage": stage,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )


def _failure_category(error: Exception) -> str:
    if str(error) == OBJECT_TOO_LARGE:
        return "object_too_large"
    if str(error) == INVALID_EVENT:
        return "invalid_event"
    if str(error) == INVALID_INGESTED_TIME:
        return "invalid_ingested_time"
    if str(error) == INVALID_SOURCE_ROUTE:
        return "invalid_source_route"
    if isinstance(error, ParseError):
        return "document_parse_failed"
    if isinstance(error, IndexingError):
        return "document_indexing_failed"
    if isinstance(error, MetadataStoreError):
        return "metadata_persistence_failed"
    return "unexpected_failure"


def _error_id(source: SourceReference | None, message_id: str) -> str:
    identity = (
        f"{source.bucket}\0{source.key}\0{source.version_id or source.etag or ''}"
        if source is not None
        else message_id
    )
    return hashlib.sha256(identity.encode()).hexdigest()


def _read_body(body: object) -> bytes:
    if not hasattr(body, "read"):
        raise TypeError(BODY_NOT_READABLE)
    payload = cast(_ReadableBody, body).read()
    if not isinstance(payload, bytes):
        raise TypeError(BODY_NOT_READABLE)
    return payload


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
