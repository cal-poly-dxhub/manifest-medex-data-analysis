"""Re-index already-parsed clinical documents for a bulk reingestion job.

The reindexer reads a previously parsed document from the parsed S3 zone and
writes it back into OpenSearch without re-parsing raw source. It never imports
the format parsers: the parsed object is treated as an opaque, already-valid
document. Every field is indexed as originally produced except ``ingestTime``,
which is set to the reingestion time (customer decision, 2026-09-23); the
original value is retained as ``originalIngestTime``. The S3 parsed object is
never modified.

Job progress is tracked with atomic DynamoDB counters so many concurrent Lambda
invocations converge on a single completion transition.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from src.search_store import (
    IndexingError,
    SearchTransport,
    SignedOpenSearchTransport,
    index_documents,
)

INVALID_MESSAGE = "Reindex message is missing required fields"
INVALID_SOURCE_FORMAT = "Reindex message source format is unsupported"
INVALID_PARSED_URI = "Parsed S3 URI does not reference the parsed bucket"
PARSED_OBJECT_TOO_LARGE = "Parsed object exceeds the configured reindex read limit"
PARSED_NOT_JSON_OBJECT = "Parsed object is not a JSON object"
PARSED_DOCUMENT_MISMATCH = "Parsed document identity does not match the reindex message"
BODY_NOT_READABLE = "S3 response body is not readable"

MAX_PARSED_BYTES_DEFAULT = 10 * 1024 * 1024
MAX_RECEIVE_COUNT_DEFAULT = 5
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"
SUPPORTED_FORMATS = frozenset({"hl7-v2", "ccda"})
_PARSER_VERSION_ENV = {
    "hl7-v2": "HL7_PARSER_VERSION",
    "ccda": "CCDA_PARSER_VERSION",
}

LOGGER = logging.getLogger(__name__)
# Reuse SDK clients and the signed transport across warm Lambda invocations.
_RUNTIME_REINDEXERS: dict[str, Reindexer] = {}


class ReindexMessageError(RuntimeError):
    """Sanitized failure describing why a reindex message could not be processed."""


class _ReadableBody(Protocol):
    def read(self) -> bytes: ...


class S3Client(Protocol):
    """Minimal S3 contract required to read one parsed object."""

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


class DynamoDbClient(Protocol):
    """Low-level DynamoDB contract used for atomic counter updates."""

    def update_item(self, **kwargs: Any) -> dict[str, Any]: ...


class JobStore(Protocol):
    """Atomic job-progress counters and the one-time completion transition."""

    def record_reindexed(self, job_id: str, *, stale: bool) -> dict[str, Any]: ...

    def record_missing(self, job_id: str) -> dict[str, Any]: ...

    def record_failed(self, job_id: str) -> dict[str, Any]: ...

    def finalize(self, job_id: str, *, failed: bool = False) -> None: ...


@dataclass(frozen=True)
class ReindexMessage:
    """Validated reindex request describing one parsed document to re-index."""

    job_id: str
    document_id: str
    parsed_bucket: str
    parsed_key: str
    source_format: str


@dataclass
class DynamoJobStore:
    """DynamoDB-backed job counters keyed solely by ``jobId``."""

    client: DynamoDbClient
    table_name: str

    def record_reindexed(self, job_id: str, *, stale: bool) -> dict[str, Any]:
        additions = {"reindexed": 1}
        if stale:
            # A stale parser version is still a successful reindex; count it separately.
            additions["reindexedStaleParser"] = 1
        return self._add(job_id, additions)

    def record_missing(self, job_id: str) -> dict[str, Any]:
        return self._add(job_id, {"missingParsed": 1})

    def record_failed(self, job_id: str) -> dict[str, Any]:
        return self._add(job_id, {"failed": 1})

    def _add(self, job_id: str, additions: dict[str, int]) -> dict[str, Any]:
        names: dict[str, str] = {}
        values: dict[str, Any] = {}
        assignments: list[str] = []
        for position, (attribute, amount) in enumerate(sorted(additions.items())):
            name_token = f"#c{position}"
            value_token = f":c{position}"
            names[name_token] = attribute
            values[value_token] = {"N": str(amount)}
            assignments.append(f"{name_token} {value_token}")
        response = self.client.update_item(
            TableName=self.table_name,
            Key={"jobId": {"S": job_id}},
            UpdateExpression="ADD " + ", ".join(assignments),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )
        return _decode_counters(response.get("Attributes", {}))

    def finalize(self, job_id: str, *, failed: bool = False) -> None:
        status = STATUS_FAILED if failed else STATUS_COMPLETE
        try:
            self.client.update_item(
                TableName=self.table_name,
                Key={"jobId": {"S": job_id}},
                UpdateExpression="SET #status = :terminal, finishedAt = :finishedAt",
                # Only the first invocation flips the status, so completion is idempotent.
                ConditionExpression=(
                    "attribute_not_exists(#status) OR NOT #status IN (:complete, :failed)"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":terminal": {"S": status},
                    ":complete": {"S": STATUS_COMPLETE},
                    ":failed": {"S": STATUS_FAILED},
                    ":finishedAt": {"S": _utc_now_iso()},
                },
            )
        except Exception as error:
            if _is_conditional_check_failure(error):
                return
            raise


@dataclass
class Reindexer:
    """Read parsed objects, re-index them, and advance job counters."""

    s3_client: S3Client
    transport: SearchTransport
    job_store: JobStore

    def process_batch(self, event: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
        """Process each SQS record and return the partial-batch failure contract."""
        failures: list[dict[str, str]] = []
        for record in event.get("Records", []):
            message_id = str(record.get("messageId", "unknown"))
            try:
                self._process_record(record)
            except Exception as error:
                self._handle_failure(record, error)
                failures.append({"itemIdentifier": message_id})
        return {"batchItemFailures": failures}

    def _process_record(self, record: dict[str, Any]) -> None:
        message = _parse_message(record)
        try:
            payload = self._read_parsed(message)
        except Exception as error:
            if _is_missing_parsed(error):
                # The parsed object is gone; count it and stop without a batch failure.
                counters = self.job_store.record_missing(message.job_id)
                self._finalize(message.job_id, counters)
                _log_event("reindex_missing_parsed", message.source_format)
                return
            raise
        document = _load_document(payload, message)
        stale = _is_stale_parser(document, message.source_format)
        # Customer decision (2026-09-23): a reingested document shows the time it was
        # reingested, not its original arrival. Keep the original for provenance so the
        # S3 parsed object remains the unchanged source of truth.
        original_ingest_time = document.get("ingestTime")
        if original_ingest_time is not None:
            document["originalIngestTime"] = original_ingest_time
        # Match the parsers' Z-suffixed ISO format so the date mapping stays uniform.
        document["ingestTime"] = _utc_now_iso().replace("+00:00", "Z")
        # The reindexer holds only WriteDocument, by design: a parsed document implies the
        # ingestion path already created its index, so never attempt creation here.
        index_documents([document], self.transport, create_index=False)
        counters = self.job_store.record_reindexed(message.job_id, stale=stale)
        self._finalize(message.job_id, counters)
        _log_event("reindex_succeeded", message.source_format, stale=stale)

    def _read_parsed(self, message: ReindexMessage) -> bytes:
        max_bytes = int(os.getenv("MAX_PARSED_BYTES", str(MAX_PARSED_BYTES_DEFAULT)))
        response = self.s3_client.get_object(Bucket=message.parsed_bucket, Key=message.parsed_key)
        # Check metadata and actual bytes because ContentLength is an external response value.
        if int(response.get("ContentLength", 0)) > max_bytes:
            raise ReindexMessageError(PARSED_OBJECT_TOO_LARGE)
        payload = _read_body(response["Body"])
        if len(payload) > max_bytes:
            raise ReindexMessageError(PARSED_OBJECT_TOO_LARGE)
        return payload

    def _handle_failure(self, record: dict[str, Any], error: Exception) -> None:
        receive_count = _receive_count(record)
        # SQS delivers at least once; the terminal attempt records a permanent failure.
        terminal = receive_count >= _max_receive_count()
        if terminal:
            job_id = _extract_job_id(record)
            if job_id is not None:
                # A redelivery past the redrive threshold could double count; documented as
                # acceptable because counters are advisory and completion uses >=.
                counters = self.job_store.record_failed(job_id)
                self._finalize(job_id, counters)
        _log_failure(error, terminal=terminal)

    def _finalize(self, job_id: str, counters: dict[str, Any]) -> None:
        if not _is_complete(counters):
            return
        # A job with any permanently failed document is a failed job, not a complete one;
        # the counters still show how many succeeded.
        any_failed = int(counters.get("failed", 0)) > 0
        try:
            self.job_store.finalize(job_id, failed=any_failed)
        except Exception:
            # Swallowing keeps a rare finalize error from retrying and re-adding a counter.
            LOGGER.error(  # noqa: TRY400 - traceback could expose a sensitive SDK exception
                json.dumps({"event": "reindex_finalize_failed"}, separators=(",", ":"))
            )


def runtime_handler(
    event: dict[str, Any],
    _context: Any,
) -> dict[str, list[dict[str, str]]]:
    """Build runtime dependencies once per execution environment and process a batch."""
    reindexer = _RUNTIME_REINDEXERS.get("reindexer")
    if reindexer is None:
        reindexer = Reindexer(
            s3_client=_aws_client("s3"),
            transport=SignedOpenSearchTransport(
                endpoint=os.environ["OPENSEARCH_ENDPOINT"],
                region=os.environ["AWS_REGION"],
                service=os.environ["OPENSEARCH_SERVICE"],
            ),
            job_store=DynamoJobStore(
                client=_aws_client("dynamodb"),
                table_name=os.environ["JOBS_TABLE"],
            ),
        )
        _RUNTIME_REINDEXERS["reindexer"] = reindexer
    return reindexer.process_batch(event)


def handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    """AWS Lambda entry point for the parsed-zone reindexer."""
    return runtime_handler(event, context)


def _parse_message(record: dict[str, Any]) -> ReindexMessage:
    try:
        body = json.loads(str(record["body"]))
        job_id = body["jobId"]
        document_id = body["documentId"]
        parsed_s3_uri = body["parsedS3Uri"]
        source_format = body["sourceFormat"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise ReindexMessageError(INVALID_MESSAGE) from error
    if not all(
        isinstance(value, str) and value
        for value in (job_id, document_id, parsed_s3_uri, source_format)
    ):
        raise ReindexMessageError(INVALID_MESSAGE)
    if source_format not in SUPPORTED_FORMATS:
        raise ReindexMessageError(INVALID_SOURCE_FORMAT)
    bucket, key = _parsed_location(parsed_s3_uri)
    return ReindexMessage(
        job_id=job_id,
        document_id=document_id,
        parsed_bucket=bucket,
        parsed_key=key,
        source_format=source_format,
    )


def _parsed_location(parsed_s3_uri: str) -> tuple[str, str]:
    expected_bucket = os.environ["PARSED_BUCKET"]
    parsed = urlparse(parsed_s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or bucket != expected_bucket or not key:
        raise ReindexMessageError(INVALID_PARSED_URI)
    return bucket, key


def _load_document(payload: bytes, message: ReindexMessage) -> dict[str, Any]:
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as error:
        raise ReindexMessageError(PARSED_NOT_JSON_OBJECT) from error
    if not isinstance(document, dict):
        raise ReindexMessageError(PARSED_NOT_JSON_OBJECT)
    if (
        document.get("documentId") != message.document_id
        or document.get("sourceFormat") != message.source_format
    ):
        raise ReindexMessageError(PARSED_DOCUMENT_MISMATCH)
    return cast(dict[str, Any], document)


def _is_stale_parser(document: dict[str, Any], source_format: str) -> bool:
    expected = os.getenv(_PARSER_VERSION_ENV[source_format])
    if not expected:
        # Without a configured expectation, staleness cannot be asserted.
        return False
    return document.get("parserVersion") != expected


def _is_complete(counters: dict[str, Any]) -> bool:
    if counters.get("enqueueComplete") is not True:
        return False
    enqueued = counters.get("enqueued")
    if not isinstance(enqueued, int):
        return False
    processed = (
        int(counters.get("reindexed", 0))
        + int(counters.get("missingParsed", 0))
        + int(counters.get("failed", 0))
    )
    return processed >= enqueued


def _receive_count(record: dict[str, Any]) -> int:
    attributes = record.get("attributes") or {}
    try:
        return int(attributes.get("ApproximateReceiveCount", 1))
    except (TypeError, ValueError):
        return 1


def _max_receive_count() -> int:
    try:
        return int(os.getenv("MAX_RECEIVE_COUNT", str(MAX_RECEIVE_COUNT_DEFAULT)))
    except ValueError:
        return MAX_RECEIVE_COUNT_DEFAULT


def _extract_job_id(record: dict[str, Any]) -> str | None:
    try:
        body = json.loads(str(record["body"]))
        job_id = body["jobId"]
    except (json.JSONDecodeError, KeyError, TypeError):
        return None
    return job_id if isinstance(job_id, str) and job_id else None


def _log_event(event: str, source_format: str, *, stale: bool = False) -> None:
    # Emit only bounded operational categories; job and document identities stay out.
    payload: dict[str, Any] = {"event": event, "sourceFormat": source_format}
    if stale:
        payload["staleParser"] = True
    LOGGER.info(json.dumps(payload, separators=(",", ":"), sort_keys=True))


def _log_failure(error: Exception, *, terminal: bool) -> None:
    event: dict[str, Any] = {
        "event": "reindex_record_failed",
        "errorType": type(error).__name__,
        "failureCategory": _failure_category(error),
        "terminal": terminal,
    }
    if isinstance(error, IndexingError):
        # The message is one of the fixed, non-clinical constants in search_store
        # (index lookup vs bulk request vs document rejected); it names the failing stage.
        event["indexingStage"] = str(error)
        if error.http_status is not None:
            event["httpStatus"] = error.http_status
        if error.backend_error_type is not None:
            event["backendErrorType"] = error.backend_error_type
    LOGGER.error(json.dumps(event, separators=(",", ":"), sort_keys=True))


def _failure_category(error: Exception) -> str:
    if isinstance(error, IndexingError):
        return "document_indexing_failed"
    return {
        INVALID_MESSAGE: "invalid_message",
        INVALID_SOURCE_FORMAT: "invalid_source_format",
        INVALID_PARSED_URI: "invalid_parsed_uri",
        PARSED_OBJECT_TOO_LARGE: "parsed_object_too_large",
        PARSED_NOT_JSON_OBJECT: "parsed_not_json_object",
        PARSED_DOCUMENT_MISMATCH: "parsed_document_mismatch",
    }.get(str(error), "unexpected_failure")


def _decode_counters(attributes: dict[str, Any]) -> dict[str, Any]:
    counters: dict[str, Any] = {}
    for key, value in attributes.items():
        if not isinstance(value, dict):
            continue
        if "N" in value:
            try:
                counters[key] = int(value["N"])
            except (TypeError, ValueError):
                counters[key] = 0
        elif "BOOL" in value:
            counters[key] = bool(value["BOOL"])
    return counters


def _is_missing_parsed(error: Exception) -> bool:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return False
    code = response.get("Error", {}).get("Code")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "NoSuchBucket", "NotFound", "404"} or status == 404


def _is_conditional_check_failure(error: Exception) -> bool:
    if type(error).__name__ == "ConditionalCheckFailedException":
        return True
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return False
    return bool(response.get("Error", {}).get("Code") == "ConditionalCheckFailedException")


def _read_body(body: object) -> bytes:
    if not hasattr(body, "read"):
        raise TypeError(BODY_NOT_READABLE)
    payload = cast(_ReadableBody, body).read()
    if not isinstance(payload, bytes):
        raise TypeError(BODY_NOT_READABLE)
    return payload


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _aws_client(service: str) -> Any:
    import boto3  # type: ignore[import-not-found]

    return boto3.client(service)
