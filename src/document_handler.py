"""Coordinate read, parse, durable writes, and retry reporting for one SQS lane."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, cast

from src.ccda_parser import iter_ccda_documents
from src.metadata_store import DataApiMetadataStore, MetadataRecord, MetadataStoreError
from src.parser import (
    HL7_CHUNK_MESSAGES,
    ParseError,
    SourceReference,
    iter_hl7_document_chunks,
    parse_hl7_file,
    scan_hl7_stream,
)
from src.search_store import (
    IndexingError,
    SearchTransport,
    SignedOpenSearchTransport,
    index_documents,
)

MAX_OBJECT_BYTES_DEFAULT = 50 * 1024 * 1024
OBJECT_TOO_LARGE = "Raw object exceeds the configured parsing limit"
# A multi-message HL7 file writes one parsed object per message. Writing them concurrently
# removes most of the time large files spend in this stage. Eight threads fit inside the
# default botocore connection pool (10), so no request waits for a connection.
PARSED_WRITE_CONCURRENCY = 8
# HL7 files above this size are streamed twice (checksum/validation, then parsing) and
# processed in chunks, so memory is bounded by the chunk instead of the file.
HL7_STREAM_THRESHOLD_BYTES = 8 * 1024 * 1024
STREAM_READ_BYTES = 1024 * 1024
ENVELOPE_READ_FAILED = "Intake envelope sidecar could not be read"
MAX_ENVELOPE_BYTES = 1024 * 1024
# otherAttributes is free-form sender JSON. Beyond these bounds it is dropped (and counted)
# rather than indexed, so one sender cannot exhaust the index field limit.
MAX_OTHER_ATTRIBUTE_LEAVES = 200
MAX_OTHER_ATTRIBUTE_DEPTH = 5
# Envelope field -> indexed top-level field. Names follow Prism's index where Prism had the
# field (messageEventType, patientMpiId_old, messageReceivedDate), else the envelope name.
ENVELOPE_FIELDS = (
    ("sourceFacilityId", "sourceFacilityId"),
    ("sourceFacility", "sourceFacility"),
    ("patientId", "patientId"),
    ("patientIdAa", "patientIdAa"),
    ("patientMpiId", "patientMpiId"),
    ("patientMpiIdsOld", "patientMpiId_old"),
    ("resultType", "resultType"),
    ("eventType", "messageEventType"),
    ("sendingApplication", "sendingApplication"),
    ("receivingApplication", "receivingApplication"),
    ("sourceMessageId", "sourceMessageId"),
    ("sourceLastUpdatedTime", "sourceLastUpdatedTime"),
    ("messageDate", "messageDate"),
    ("timeZoneId", "timeZoneId"),
    ("clinicalMessageType", "clinicalMessageType"),
    ("intakeReceivedAt", "messageReceivedDate"),
)
INVALID_EVENT = "SQS record is not an S3 Object Created EventBridge event"
INVALID_INGESTED_TIME = "S3 event time is missing or invalid"
INVALID_SOURCE_ROUTE = "Source object does not match the format-specific ingestion route"
BODY_NOT_READABLE = "S3 response body is not readable"
FORMAT_CONFIGURATION_MISMATCH = "Lambda source format configuration does not match its handler"
LOCATION_COUNT_MISMATCH = "Parsed document location count mismatch"

LOGGER = logging.getLogger(__name__)
# Reuse SDK clients and transports across warm Lambda invocations.
_RUNTIME_PROCESSORS: dict[str, DocumentProcessor] = {}


class ObjectTooLargeError(ParseError):
    """Raw object over the parsing limit; carries sizes (never content) for diagnostics."""

    def __init__(self, object_bytes: int, max_bytes: int) -> None:
        super().__init__(OBJECT_TOO_LARGE)
        self.object_bytes = object_bytes
        self.max_bytes = max_bytes


class _ReadableBody(Protocol):
    def read(self) -> bytes: ...


class _StreamingBody(Protocol):
    def read(self, amount: int) -> bytes: ...


class S3Client(Protocol):
    """Minimal S3 operations required by the ingestion workflow."""

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


class MetadataStore(Protocol):
    """Destination contract for idempotent metadata persistence."""

    def upsert(self, records: list[MetadataRecord]) -> None: ...


@dataclass(frozen=True)
class ParsedLocation:
    """Versioned parsed-S3 location paired with one parsed document."""

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
        """Process each SQS record and return the partial-batch failure contract."""
        failures: list[dict[str, str]] = []
        for record in event.get("Records", []):
            message_id = str(record.get("messageId", "unknown"))
            source: SourceReference | None = None
            stage = "event_validation"
            try:
                source, ingested_time = _source_from_record(record)
                if source.key.endswith("/"):
                    # A "folder" created in the S3 console is a zero-byte key ending in "/".
                    # It matches the routing rule but is not a document: acknowledge it so it
                    # never retries into the dead-letter queue.
                    _log_skipped(self.expected_format, "folder_marker")
                    continue
                _validate_route(source, self.expected_format)
                stage = "source_read"
                chunks, effective_source = _open_document_chunks(
                    source,
                    ingested_time,
                    self.expected_format,
                    self.s3_client,
                )
                # Per chunk: parsed S3, then Aurora, then the search index. Aurora is written
                # before indexing so a document is never searchable without its metadata row;
                # if indexing fails the explorer still lists it and the retry completes it.
                # Every write is deterministic, so a retry of the same file converges.
                for documents in chunks:
                    stage = "parsed_storage"
                    locations = _write_parsed_documents(
                        documents, self.expected_format, self.s3_client
                    )
                    stage = "metadata"
                    self.metadata_store.upsert(
                        _metadata_records(documents, locations, effective_source, ingested_time)
                    )
                    stage = "indexing"
                    index_documents(documents, self.search_transport)
                    # Parsing of the next chunk happens while advancing the iterator.
                    stage = "source_read"
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
    # Cache by format so separate handlers cannot accidentally share lane configuration.
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


def _bounded_chunks(body: object, max_bytes: int) -> Iterator[bytes]:
    """Stream an S3 body in fixed reads, enforcing the size limit on actual bytes."""
    if not hasattr(body, "read"):
        raise TypeError(BODY_NOT_READABLE)
    total = 0
    while True:
        chunk = cast(_StreamingBody, body).read(STREAM_READ_BYTES)
        if not chunk:
            return
        if not isinstance(chunk, bytes):
            raise TypeError(BODY_NOT_READABLE)
        total += len(chunk)
        if total > max_bytes:
            raise ObjectTooLargeError(total, max_bytes)
        yield chunk


def _batched(documents: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    for start in range(0, len(documents), size):
        yield documents[start : start + size]


def _open_document_chunks(
    source: SourceReference,
    ingested_time: str,
    expected_format: str,
    s3_client: S3Client,
) -> tuple[Iterator[list[dict[str, Any]]], SourceReference]:
    """Open the source object and return its documents as bounded chunks.

    Small files are read once and parsed whole (unchanged behavior). Large HL7 files are
    streamed: a first pass computes the file checksum and validates encoding and message
    boundaries before anything is written, and a second pass over the same object version
    yields chunks of HL7_CHUNK_MESSAGES documents. Document IDs are identical either way.
    """
    request: dict[str, Any] = {"Bucket": source.bucket, "Key": source.key}
    if source.version_id:
        request["VersionId"] = source.version_id
    # A versioned read binds parsing to the exact object referenced by EventBridge.
    response = s3_client.get_object(**request)
    max_bytes = int(os.getenv("MAX_OBJECT_BYTES", str(MAX_OBJECT_BYTES_DEFAULT)))
    # Check both metadata and actual bytes because ContentLength is an external response value.
    content_length = int(response.get("ContentLength", 0))
    if content_length > max_bytes:
        raise ObjectTooLargeError(content_length, max_bytes)
    effective_source = SourceReference(
        bucket=source.bucket,
        key=source.key,
        version_id=source.version_id or _optional_string(response.get("VersionId")),
        etag=source.etag or _clean_etag(response.get("ETag")),
    )
    envelope = _read_envelope(source, response.get("Metadata") or {}, s3_client)

    raw_chunks: Iterator[list[dict[str, Any]]]
    if expected_format == "hl7-v2" and content_length > HL7_STREAM_THRESHOLD_BYTES:
        raw_checksum, _ = scan_hl7_stream(_bounded_chunks(response["Body"], max_bytes))
        reread = dict(request)
        if effective_source.version_id:
            reread["VersionId"] = effective_source.version_id

        def second_pass() -> Iterator[bytes]:
            yield from _bounded_chunks(s3_client.get_object(**reread)["Body"], max_bytes)

        raw_chunks = iter_hl7_document_chunks(
            second_pass(),
            effective_source,
            raw_checksum=raw_checksum,
            ingested_at=ingested_time,
            chunk_messages=HL7_CHUNK_MESSAGES,
        )
    else:
        payload = _read_body(response["Body"])
        if len(payload) > max_bytes:
            raise ObjectTooLargeError(len(payload), max_bytes)
        if expected_format == "hl7-v2":
            raw_chunks = _batched(
                parse_hl7_file(payload, effective_source, ingested_at=ingested_time),
                HL7_CHUNK_MESSAGES,
            )
        else:
            raw_chunks = (
                [document]
                for document in iter_ccda_documents(
                    payload, effective_source, ingested_at=ingested_time
                )
            )
    return _checked_chunks(raw_chunks, expected_format, envelope, source), effective_source


def _checked_chunks(
    chunks: Iterator[list[dict[str, Any]]],
    expected_format: str,
    envelope: dict[str, Any] | None,
    source: SourceReference,
) -> Iterator[list[dict[str, Any]]]:
    produced = False
    for documents in chunks:
        if any(document.get("sourceFormat") != expected_format for document in documents):
            raise ParseError(INVALID_SOURCE_ROUTE)
        if envelope is not None:
            _apply_envelope(documents, envelope, source)
        _add_derived_fields(documents)
        produced = produced or bool(documents)
        yield documents
    if not produced:
        raise ParseError(INVALID_SOURCE_ROUTE)


def envelope_key_for(raw_key: str) -> str | None:
    """Map incoming/<lane>/...<name>.<ext> to its sidecar envelopes/<lane>/...<name>.json."""
    if not raw_key.startswith("incoming/") or "." not in raw_key.rsplit("/", 1)[-1]:
        return None
    return "envelopes/" + raw_key[len("incoming/") :].rsplit(".", 1)[0] + ".json"


def _read_envelope(
    source: SourceReference, metadata: dict[str, Any], s3_client: S3Client
) -> dict[str, Any] | None:
    """Return the intake envelope for an object delivered through the intake API, else None.

    Only objects the intake service wrote carry these metadata markers, so files uploaded
    straight to S3 cost no extra request and are processed exactly as before.
    """
    version_id = metadata.get("envelope-version-id")
    if version_id is None and metadata.get("envelope-key-only") != "true":
        return None
    key = envelope_key_for(source.key)
    if key is None:
        return None
    request: dict[str, Any] = {"Bucket": source.bucket, "Key": key}
    if version_id:
        request["VersionId"] = version_id
    try:
        response = s3_client.get_object(**request)
        too_large = int(response.get("ContentLength", 0)) > MAX_ENVELOPE_BYTES
        envelope = None if too_large else json.loads(_read_body(response["Body"]))
    except Exception as error:
        raise ParseError(ENVELOPE_READ_FAILED) from error
    if not isinstance(envelope, dict):
        raise ParseError(ENVELOPE_READ_FAILED)
    return envelope


def _attribute_shape(value: Any, depth: int = 0) -> tuple[int, int]:
    """Return (leaf count, max depth) of a JSON value."""
    if isinstance(value, dict):
        shapes = [_attribute_shape(item, depth + 1) for item in value.values()] or [(0, depth)]
    elif isinstance(value, list):
        shapes = [_attribute_shape(item, depth + 1) for item in value] or [(0, depth)]
    else:
        return 1, depth
    return sum(leaves for leaves, _ in shapes), max(level for _, level in shapes)


def _bounded_attributes(value: Any, name: str, expected_format: str) -> Any | None:
    if not isinstance(value, dict) or not value:
        return None
    leaves, depth = _attribute_shape(value)
    if leaves > MAX_OTHER_ATTRIBUTE_LEAVES or depth > MAX_OTHER_ATTRIBUTE_DEPTH:
        _log_skipped(expected_format, f"{name}_too_large")
        return None
    return value


def _apply_envelope(
    documents: list[dict[str, Any]], envelope: dict[str, Any], source: SourceReference
) -> None:
    """Merge intake envelope fields into each document and give it a stable identity.

    Prism treats a re-sent sourceMessageId as an update, so a message delivered through the
    intake API is identified by its stable object key and position in the file, not by its
    content: a corrected message replaces the earlier document instead of adding another.
    """
    fields: dict[str, Any] = {}
    for envelope_name, field_name in ENVELOPE_FIELDS:
        value = envelope.get(envelope_name)
        if value not in (None, "", [], {}):
            fields[field_name] = value
    contents = envelope.get("contents")
    selected = envelope.get("selectedContentIndex")
    if isinstance(contents, list) and isinstance(selected, int) and 0 <= selected < len(contents):
        chosen = contents[selected] if isinstance(contents[selected], dict) else {}
        if chosen.get("processedLevel"):
            fields["messageProcessedLevel"] = chosen["processedLevel"]
        content_attributes = _bounded_attributes(
            chosen.get("otherAttributes"),
            "contentOtherAttributes",
            str(documents[0]["sourceFormat"]),
        )
        if content_attributes is not None:
            fields["contentOtherAttributes"] = content_attributes
    attributes = _bounded_attributes(
        envelope.get("otherAttributes"), "otherAttributes", str(documents[0]["sourceFormat"])
    )
    if attributes is not None:
        fields["otherAttributes"] = attributes
    for document in documents:
        ordinal = document.get("messageOrdinal", 0)
        document["documentId"] = hashlib.sha256(
            f"{source.bucket}\0{source.key}\0intake\0{ordinal}".encode()
        ).hexdigest()
        document.update(fields)


SOURCE_MESSAGE_TYPES = {"hl7-v2": "HL7V2", "ccda": "CCDA"}
PROCESSED_STATUS = "PROCESSED"


def _add_derived_fields(documents: list[dict[str, Any]]) -> None:
    """Add Prism's bookkeeping fields, computed after the envelope so its values win."""
    for document in documents:
        ingested = document.get("ingestTime")
        if ingested:
            document["createdTime"] = ingested
            document["updatedTime"] = ingested
            document.setdefault("messageReceivedDate", ingested)
        patient_id = document.get("patientId")
        if patient_id:
            authority = document.get("patientIdAa")
            document["patientFullId"] = f"{patient_id}^{authority}" if authority else patient_id
        source_type = document.get("clinicalMessageType") or SOURCE_MESSAGE_TYPES.get(
            str(document.get("sourceFormat"))
        )
        if source_type:
            document["sourceMessageType"] = source_type
        document["messageStatus"] = PROCESSED_STATUS


def _write_parsed_documents(
    documents: list[dict[str, Any]],
    expected_format: str,
    s3_client: S3Client,
) -> list[ParsedLocation]:
    parsed_bucket = os.environ["PARSED_BUCKET"]
    prefix = "hl7" if expected_format == "hl7-v2" else "ccda"

    def write(document: dict[str, Any]) -> ParsedLocation:
        # Stable keys overwrite the logical document while bucket versioning preserves history.
        key = f"{prefix}/{document['documentId']}.json"
        response = s3_client.put_object(
            Bucket=parsed_bucket,
            Key=key,
            Body=_json_bytes(document),
            ContentType="application/json",
        )
        return ParsedLocation(key=key, version_id=_optional_string(response.get("VersionId")))

    if len(documents) <= 1:
        return [write(document) for document in documents]
    # map() preserves input order, so locations stay paired with their documents; the first
    # failed write propagates and the whole source object is retried (writes are idempotent).
    with ThreadPoolExecutor(max_workers=min(PARSED_WRITE_CONCURRENCY, len(documents))) as pool:
        return list(pool.map(write, documents))


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
    # Emit only bounded operational categories; source identities and exception text stay out.
    event: dict[str, Any] = {
        "errorType": type(error).__name__,
        "event": "record_processing_failed",
        "failureCategory": _failure_category(error),
        "retryable": True,
        "sourceFormat": expected_format,
        "stage": stage,
    }
    if isinstance(error, ObjectTooLargeError):
        # Sizes are not identifying and tell the operator exactly how far over the limit
        # the file is. The fix is to split the file (HL7: at MSH boundaries); see README.
        event["objectBytes"] = error.object_bytes
        event["maxObjectBytes"] = error.max_bytes
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


def _log_skipped(expected_format: str, reason: str) -> None:
    LOGGER.info(
        json.dumps(
            {"event": "record_skipped", "reason": reason, "sourceFormat": expected_format},
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _failure_category(error: Exception) -> str:
    if str(error) == OBJECT_TOO_LARGE:
        return "object_too_large"
    if str(error) == ENVELOPE_READ_FAILED:
        return "envelope_read_failed"
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
    # Hashing permits correlation and deterministic overwrites without exposing the source key.
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
