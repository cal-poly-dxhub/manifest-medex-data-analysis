"""Resolve a reingestion job's documents and enqueue them for parsed-zone reindexing.

The reingest jobs API dispatches this planner asynchronously with a payload that carries
the job ID, the mode, and either the guarded SELECT or the explicit document-ID list. The
planner resolves each selected document's parsed-zone location from the validated metadata
table and fans it out to the reindex SQS queue, updating the job's counters atomically as
it goes.

A SQL job walks the selection with keyset pagination: an outer ``SELECT DISTINCT`` joins
the guarded selection as a derived table and pages on ``document_id > :cursor`` ordered by
``document_id``. There is no ``OFFSET`` -- the cursor is a bound parameter, so page cost
stays flat and no row is visited twice or skipped. An ID job resolves metadata in bounded,
fully parameterized ``IN`` batches, preserving the caller's unique IDs.

Documents are enqueued with ``SendMessageBatch`` in groups of at most ten, each message
carrying only ``{jobId, documentId, parsedS3Uri, sourceFormat}``. An optional bounded
delay between batches lets an operator throttle the fan-out. The planner marks the job
running before it starts, marks enqueue complete when the walk finishes, and completes the
job immediately when nothing was enqueued or every enqueued document was already processed.
Any failure transitions the job to a sanitized failed state.

SQL identifiers come only from environment configuration and are validated against
:data:`src.metadata_store.SQL_IDENTIFIER_PATTERN`; the guarded selection is re-validated
here as defense in depth. Boto3 clients are constructed lazily so importing this module
never requires AWS credentials, and no parser module is imported.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Protocol

from src.metadata_store import SQL_IDENTIFIER_PATTERN
from src.reingest_jobs import (
    JOB_ID_PATTERN,
    MODE_IDS,
    MODE_SQL,
    JobProgressStore,
    validate_document_ids,
    validate_reingest_sql,
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

DEFAULT_PAGE_SIZE = 500
DEFAULT_ID_BATCH_SIZE = 100
DEFAULT_BATCH_DELAY_SECONDS = 0.01
MAX_SELECTION_DOCUMENTS = 100_000
_SQS_BATCH_SIZE = 10

_SELECTION_TOKEN = "__SELECTION__"  # noqa: S105 - not a secret, a SQL template placeholder
_IDS_TOKEN = "__IDS__"  # noqa: S105 - a SQL template placeholder, not a secret
_TABLE_TOKEN = "document_metadata"  # noqa: S105 - a table name placeholder, not a secret

# The outer keyset page: the guarded selection is a derived table joined to the validated
# metadata, and paging is by an ordered, parameterized cursor with no OFFSET.
_KEYSET_TEMPLATE = (
    "SELECT DISTINCT dm.document_id, dm.parsed_s3_uri, dm.source_format\n"
    "FROM " + _TABLE_TOKEN + " dm\n"
    "JOIN (\n"
    f"{_SELECTION_TOKEN}\n"
    ") selection ON selection.document_id = dm.document_id\n"
    "WHERE dm.document_id > :cursor\n"
    "ORDER BY dm.document_id\n"
    "LIMIT :page_size"
)

# The ID-batch resolution: fully parameterized ``IN`` list against the metadata table.
_IDS_TEMPLATE = (
    "SELECT document_id, parsed_s3_uri, source_format\n"
    "FROM " + _TABLE_TOKEN + "\n"
    f"WHERE document_id IN ({_IDS_TOKEN})"
)

INVALID_PLANNER_CONFIG = "Reingest planner configuration is invalid"
INVALID_PLANNER_EVENT = "Reingest planner event is invalid"
SELECTION_TOO_LARGE = "Reingest selection exceeded the maximum document count"
METADATA_READ_FAILED = "Reingest metadata read failed"
ENQUEUE_FAILED = "Reingest enqueue failed"
PLANNER_FAILED = "Reingest planner failed"


class _DataApiClient(Protocol):
    def execute_statement(self, **kwargs: Any) -> dict[str, Any]: ...


class _SqsClient(Protocol):
    def send_message_batch(self, **kwargs: Any) -> dict[str, Any]: ...


class PlannerError(RuntimeError):
    """Sanitized planner failure that never includes SQL, Data API, or SQS detail."""


class _ResolvedDocument:
    """One reindexable document resolved from the validated metadata table."""

    __slots__ = ("document_id", "parsed_s3_uri", "source_format")

    def __init__(self, document_id: str, parsed_s3_uri: str, source_format: str) -> None:
        self.document_id = document_id
        self.parsed_s3_uri = parsed_s3_uri
        self.source_format = source_format


class ReingestPlanner:
    """Walk a job's selection, enqueue each document, and drive the job's lifecycle."""

    def __init__(
        self,
        data_api: _DataApiClient,
        sqs_client: _SqsClient,
        progress: JobProgressStore,
        *,
        cluster_arn: str,
        secret_arn: str,
        database: str,
        table_name: str,
        queue_url: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        id_batch_size: int = DEFAULT_ID_BATCH_SIZE,
        batch_delay_seconds: float = DEFAULT_BATCH_DELAY_SECONDS,
        max_documents: int = MAX_SELECTION_DOCUMENTS,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        if not SQL_IDENTIFIER_PATTERN.fullmatch(database) or not SQL_IDENTIFIER_PATTERN.fullmatch(
            table_name
        ):
            raise ValueError(INVALID_PLANNER_CONFIG)
        if (
            not queue_url
            or page_size < 1
            or id_batch_size < 1
            or batch_delay_seconds < 0
            or max_documents < 1
        ):
            raise ValueError(INVALID_PLANNER_CONFIG)
        self._data_api = data_api
        self._sqs = sqs_client
        self._progress = progress
        self._request = {
            "resourceArn": cluster_arn,
            "secretArn": secret_arn,
            "database": database,
        }
        self._keyset_sql = _KEYSET_TEMPLATE.replace(_TABLE_TOKEN, table_name)
        self._ids_template = _IDS_TEMPLATE.replace(_TABLE_TOKEN, table_name)
        self._queue_url = queue_url
        self._page_size = page_size
        self._id_batch_size = id_batch_size
        self._batch_delay_seconds = batch_delay_seconds
        self._max_documents = max_documents
        self._sleep = sleep or time.sleep

    def run(self, event: Mapping[str, Any]) -> dict[str, Any]:
        """Process one planner event, driving the job to a terminal state on any failure."""
        job_id, mode, sql, document_ids = _parse_event(event)
        self._progress.mark_running(job_id)
        try:
            enqueued = self._plan(job_id, mode, sql, document_ids)
        except Exception:
            # Never surface Data API, SQS, or clinical detail; record the sanitized failure.
            self._progress.mark_failed(job_id)
            raise PlannerError(PLANNER_FAILED) from None
        self._progress.mark_enqueue_complete(job_id)
        # A job that enqueued nothing, or whose documents were all already processed by a
        # racing reindex worker, completes here rather than waiting for a reindex event.
        self._progress.complete_if_settled(job_id)
        return {"jobId": job_id, "enqueued": enqueued}

    def _plan(
        self,
        job_id: str,
        mode: str,
        sql: str | None,
        document_ids: list[str] | None,
    ) -> int:
        buffer: list[_ResolvedDocument] = []
        total = 0
        source = (
            self._walk_sql(sql)
            if mode == MODE_SQL
            else self._walk_ids(document_ids if document_ids is not None else [])
        )
        for document in source:
            buffer.append(document)
            total += 1
            if total > self._max_documents:
                raise PlannerError(SELECTION_TOO_LARGE)
            if len(buffer) >= _SQS_BATCH_SIZE:
                self._flush(job_id, buffer)
                buffer = []
        if buffer:
            self._flush(job_id, buffer)
        return total

    def _walk_sql(self, sql: str | None) -> Iterator[_ResolvedDocument]:
        # Re-validate the guarded selection even though the API already did; the planner
        # must never trust an event payload as a reason to relax the SELECT-only guard.
        guarded = validate_reingest_sql(sql)
        page_sql = self._keyset_sql.replace(_SELECTION_TOKEN, guarded)
        cursor = ""
        while True:
            parameters = [
                _string_parameter("cursor", cursor),
                _long_parameter("page_size", self._page_size),
            ]
            rows = self._resolve(page_sql, parameters)
            yield from rows
            if len(rows) < self._page_size:
                return
            cursor = rows[-1].document_id

    def _walk_ids(self, document_ids: Sequence[str]) -> Iterator[_ResolvedDocument]:
        for start in range(0, len(document_ids), self._id_batch_size):
            batch = document_ids[start : start + self._id_batch_size]
            placeholders = ", ".join(f":id{index}" for index in range(len(batch)))
            batch_sql = self._ids_template.replace(_IDS_TOKEN, placeholders)
            parameters = [
                _string_parameter(f"id{index}", value) for index, value in enumerate(batch)
            ]
            yield from self._resolve(batch_sql, parameters)

    def _resolve(
        self,
        sql: str,
        parameters: list[dict[str, Any]],
    ) -> list[_ResolvedDocument]:
        try:
            response = self._data_api.execute_statement(
                **self._request,
                sql=sql,
                parameters=parameters,
            )
        except Exception:
            raise PlannerError(METADATA_READ_FAILED) from None
        records = response.get("records", [])
        if not isinstance(records, list):
            raise PlannerError(METADATA_READ_FAILED)
        try:
            return [_resolved_document(record) for record in records]
        except (KeyError, TypeError, ValueError):
            raise PlannerError(METADATA_READ_FAILED) from None

    def _flush(self, job_id: str, documents: list[_ResolvedDocument]) -> None:
        entries = [
            {
                "Id": str(index),
                "MessageBody": json.dumps(
                    {
                        "jobId": job_id,
                        "documentId": document.document_id,
                        "parsedS3Uri": document.parsed_s3_uri,
                        "sourceFormat": document.source_format,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            }
            for index, document in enumerate(documents)
        ]
        try:
            response = self._sqs.send_message_batch(QueueUrl=self._queue_url, Entries=entries)
        except Exception:
            raise PlannerError(ENQUEUE_FAILED) from None
        failed = response.get("Failed")
        if failed:
            raise PlannerError(ENQUEUE_FAILED)
        # Count only messages the queue actually accepted, so the enqueued total the
        # reindexer settles against never overstates what was sent.
        self._progress.add_enqueued(job_id, len(entries))
        if self._batch_delay_seconds > 0:
            self._sleep(self._batch_delay_seconds)


def _parse_event(
    event: Mapping[str, Any],
) -> tuple[str, str, str | None, list[str] | None]:
    if not isinstance(event, Mapping):
        raise PlannerError(INVALID_PLANNER_EVENT)
    job_id = event.get("jobId")
    if not isinstance(job_id, str) or not JOB_ID_PATTERN.fullmatch(job_id):
        raise PlannerError(INVALID_PLANNER_EVENT)
    mode = event.get("mode")
    if mode == MODE_SQL:
        sql = event.get("sql")
        if not isinstance(sql, str):
            raise PlannerError(INVALID_PLANNER_EVENT)
        return job_id, MODE_SQL, sql, None
    if mode == MODE_IDS:
        try:
            document_ids = validate_document_ids(event.get("documentIds"))
        except Exception:
            raise PlannerError(INVALID_PLANNER_EVENT) from None
        return job_id, MODE_IDS, None, document_ids
    raise PlannerError(INVALID_PLANNER_EVENT)


def _resolved_document(record: Any) -> _ResolvedDocument:
    if not isinstance(record, list) or len(record) != 3:
        raise ValueError
    document_id = _string_field(record[0])
    parsed_s3_uri = _string_field(record[1])
    source_format = _string_field(record[2])
    return _ResolvedDocument(document_id, parsed_s3_uri, source_format)


def _string_field(field: Any) -> str:
    if not isinstance(field, dict):
        raise TypeError
    value = field.get("stringValue")
    if not isinstance(value, str) or not value:
        raise ValueError
    return value


def _string_parameter(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": {"stringValue": value}}


def _long_parameter(name: str, value: int) -> dict[str, Any]:
    return {"name": name, "value": {"longValue": value}}


_RUNTIME_PLANNER: ReingestPlanner | None = None


def _runtime_planner() -> ReingestPlanner:
    global _RUNTIME_PLANNER
    if _RUNTIME_PLANNER is None:
        jobs_table = os.environ["JOBS_TABLE"]
        _RUNTIME_PLANNER = ReingestPlanner(
            _aws_client("rds-data"),
            _aws_client("sqs"),
            JobProgressStore(_aws_client("dynamodb"), jobs_table=jobs_table),
            cluster_arn=os.environ["METADATA_CLUSTER_ARN"],
            secret_arn=os.environ["METADATA_SECRET_ARN"],
            database=os.environ["METADATA_DATABASE"],
            table_name=os.environ["METADATA_TABLE"],
            queue_url=os.environ["REINDEX_QUEUE_URL"],
            batch_delay_seconds=float(
                os.getenv("ENQUEUE_DELAY_SECONDS", str(DEFAULT_BATCH_DELAY_SECONDS))
            ),
        )
    return _RUNTIME_PLANNER


def _aws_client(service: str) -> Any:
    import boto3  # type: ignore[import-not-found]

    return boto3.client(service)


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Process one asynchronous reingestion planning event."""
    return _runtime_planner().run(event)
