"""Create, dispatch, and track parsed-zone reingestion jobs.

A reingestion job selects a set of documents either by a strict, caller-supplied
SELECT-only SQL statement or by an explicit list of document IDs, then dispatches an
asynchronous planner Lambda that resolves each document's parsed-zone location and
enqueues it for reindexing.

The interactive ``POST /query`` explorer endpoint stays unrestricted; this service owns an
independent, deliberately strict SQL guard so a reingestion can never run anything but a
single read-only projection. The guard accepts one ``SELECT`` (or ``WITH ... SELECT``)
statement with no comments and no embedded statement separators, rejects every DML, DDL,
``COPY``, ``CALL``, ``DO``, and row-locking construct, and caps the statement length. The
caller's SQL is never bound as a parameter; it is embedded verbatim as a derived table so
Postgres itself guarantees a subquery can only read, and the guard is defense in depth.

The job row lives in a low-level DynamoDB table. Because a ten-thousand-ID job would blow
past the 400 KB item limit, the row never stores the ID list: it stores only ``idCount``
(for an ID job) or the verbatim SQL plus its SHA-256 (for a SQL job). The full ID list, or
the SQL, travels in the planner's asynchronous invocation payload, which the 6 MB Lambda
event budget accommodates comfortably. Counters and the lifecycle status are updated in
place by the planner and the parsed-zone reindexer through :class:`JobProgressStore` using
atomic ``ADD`` updates so concurrent reindex workers never lose an increment.

Every failure surfaces as a typed, sanitized error that carries no SQL, DynamoDB, Lambda,
or clinical detail, and the audit log records only the caller subject, the job mode, the
selected document count, the SQL SHA-256 (never the SQL text), and a timestamp.

The authenticated public projection returned by the JWT-protected list and get routes
includes the verbatim ``sql`` for a SQL job (alongside its ``sqlSha256``) so the UI can
expand a job and show exactly what was submitted; an ID job exposes only ``idCount``. The
SQL text lives only in the job row and this projection: it is never written to any log
line, including the audit log.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

from src.metadata_store import SQL_IDENTIFIER_PATTERN

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# The reingest SQL guard is intentionally far tighter than the interactive query endpoint.
MAX_SQL_CHARACTERS = 20_000
MAX_DOCUMENT_IDS = 10_000
# A reingestion larger than this is rejected before a job row is ever created.
MAX_SELECTION_DOCUMENTS = 100_000
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200

DOCUMENT_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")
JOB_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")

# Lifecycle statuses shared with the planner and the reindexer backend.
STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"

MODE_SQL = "sql"
MODE_IDS = "ids"

# The counters the planner and the parsed-zone reindexer maintain with atomic ADDs.
COUNTER_NAMES = (
    "enqueued",
    "reindexed",
    "reindexedStaleParser",
    "missingParsed",
    "failed",
)

# Placeholder tokens replaced (never formatted) into constant SQL templates so a validated
# identifier and the guarded SELECT are the only variable text that reaches the database.
_SELECTION_TOKEN = "__SELECTION__"  # noqa: S105 - not a secret, a SQL template placeholder
_TABLE_TOKEN = "document_metadata"  # noqa: S105 - a table name placeholder, not a secret

# The preview counts the documents the guarded selection resolves to that also exist in the
# validated metadata table, so a job's expected size reflects only reindexable documents.
_PREVIEW_TEMPLATE = (
    "SELECT COUNT(DISTINCT selection.document_id)\n"
    "FROM (\n"
    f"{_SELECTION_TOKEN}\n"
    ") selection\n"
    f"JOIN {_TABLE_TOKEN} dm ON dm.document_id = selection.document_id"
)

# Reserved words that must never appear in a reingest selection. Matching is whole-word and
# case-insensitive; identifiers containing these as substrings (``offset``, ``do_thing``)
# are unaffected because ``_`` is a word character and the surrounding boundaries differ.
_FORBIDDEN_KEYWORDS = frozenset(
    {
        "insert",
        "update",
        "delete",
        "merge",
        "upsert",
        "drop",
        "create",
        "alter",
        "truncate",
        "rename",
        "grant",
        "revoke",
        "copy",
        "call",
        "do",
        "execute",
        "prepare",
        "deallocate",
        "vacuum",
        "analyze",
        "cluster",
        "reindex",
        "refresh",
        "lock",
        "begin",
        "commit",
        "rollback",
        "savepoint",
        "start",
        "set",
        "reset",
        "discard",
        "listen",
        "notify",
        "load",
        "into",
        "share",
    }
)
_WORD_PATTERN = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")
_LEADING_KEYWORD_PATTERN = re.compile(r"^\(*\s*(select|with)\b", re.IGNORECASE)

# Short, caller-safe request codes.
INVALID_SQL = "invalid_sql"
SQL_TOO_LONG = "sql_too_long"
SQL_NOT_SELECT = "sql_not_select_only"
SQL_HAS_COMMENT = "sql_contains_comment"
SQL_MULTIPLE_STATEMENTS = "sql_multiple_statements"
SQL_FORBIDDEN_KEYWORD = "sql_forbidden_keyword"
INVALID_DOCUMENT_IDS = "invalid_document_ids"
TOO_MANY_DOCUMENT_IDS = "too_many_document_ids"
INVALID_DOCUMENT_ID = "invalid_document_id"
DUPLICATE_DOCUMENT_ID = "duplicate_document_id"
INVALID_JOB_REQUEST = "invalid_job_request"
SELECTION_TOO_LARGE = "selection_too_large"
INVALID_JOB_ID = "invalid_job_id"
INVALID_CALLER = "invalid_caller"
INVALID_LIMIT = "invalid_limit"
JOB_NOT_FOUND = "job_not_found"

# Sanitized internal failures.
INVALID_JOBS_CONFIG = "Reingest jobs configuration is invalid"
PREVIEW_FAILED = "Reingest preview failed"
JOB_CREATE_FAILED = "Reingest job creation failed"
JOB_DISPATCH_FAILED = "Reingest job dispatch failed"
JOB_READ_FAILED = "Reingest job read failed"
JOB_UPDATE_FAILED = "Reingest job update failed"

_ASYNC_ACCEPTED_STATUS = frozenset({200, 202})
_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"


class _DataApiClient(Protocol):
    def execute_statement(self, **kwargs: Any) -> dict[str, Any]: ...


class _DynamoClient(Protocol):
    def put_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def update_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def scan(self, **kwargs: Any) -> dict[str, Any]: ...


class _LambdaClient(Protocol):
    def invoke(self, **kwargs: Any) -> dict[str, Any]: ...


class ReingestError(RuntimeError):
    """Sanitized reingest failure that never includes SQL, DynamoDB, or Lambda detail."""


class ReingestRequestError(ReingestError):
    """Expected reingest failure safe to return to the authenticated caller."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


def validate_reingest_sql(sql: Any) -> str:
    """Return the caller's SQL unchanged once it passes the strict SELECT-only guard.

    The statement must be a single ``SELECT`` or ``WITH ... SELECT``, contain no SQL
    comment markers, carry at most one optional trailing semicolon, and reference none of
    the forbidden DML, DDL, ``COPY``, ``CALL``, ``DO``, or row-locking keywords. The text
    is returned verbatim so the same bytes the guard inspected are the bytes embedded as a
    derived table; surrounding whitespace and one optional trailing semicolon are removed
    so the nested statement remains valid SQL.
    """
    if not isinstance(sql, str):
        raise ReingestRequestError(400, INVALID_SQL)
    stripped = sql.strip()
    if not stripped:
        raise ReingestRequestError(400, INVALID_SQL)
    if len(sql) > MAX_SQL_CHARACTERS:
        raise ReingestRequestError(400, SQL_TOO_LONG)
    if "--" in sql or "/*" in sql or "*/" in sql:
        raise ReingestRequestError(400, SQL_HAS_COMMENT)
    if _SELECTION_TOKEN in sql:
        # The token is how the guarded SQL is spliced into the wrapper template; a caller
        # cannot be allowed to smuggle it in and disturb the substitution.
        raise ReingestRequestError(400, INVALID_SQL)
    # Allow exactly one optional trailing semicolon, then forbid any further separator so a
    # second statement can never ride along.
    body = stripped[:-1].rstrip() if stripped.endswith(";") else stripped
    if ";" in body:
        raise ReingestRequestError(400, SQL_MULTIPLE_STATEMENTS)
    if not _LEADING_KEYWORD_PATTERN.match(body):
        raise ReingestRequestError(400, SQL_NOT_SELECT)
    lowered_words = {match.group(0).lower() for match in _WORD_PATTERN.finditer(body)}
    if lowered_words & _FORBIDDEN_KEYWORDS:
        raise ReingestRequestError(400, SQL_FORBIDDEN_KEYWORD)
    return body


def validate_document_ids(raw: Any) -> list[str]:
    """Return a deduplicated, order-preserving list of validated document IDs.

    Each ID must be a lowercase 64-character hex string, the list may not exceed the job
    cap, and a repeated ID is rejected rather than silently collapsed so a caller learns
    their request was ambiguous.
    """
    if not isinstance(raw, list) or not raw:
        raise ReingestRequestError(400, INVALID_DOCUMENT_IDS)
    if len(raw) > MAX_DOCUMENT_IDS:
        raise ReingestRequestError(400, TOO_MANY_DOCUMENT_IDS)
    seen: set[str] = set()
    ordered: list[str] = []
    for value in raw:
        if not isinstance(value, str) or not DOCUMENT_ID_PATTERN.fullmatch(value):
            raise ReingestRequestError(400, INVALID_DOCUMENT_ID)
        if value in seen:
            raise ReingestRequestError(400, DUPLICATE_DOCUMENT_ID)
        seen.add(value)
        ordered.append(value)
    return ordered


class ReingestJobs:
    """Preview, create, list, and read parsed-zone reingestion jobs."""

    def __init__(
        self,
        data_api: _DataApiClient,
        dynamo_client: _DynamoClient,
        lambda_client: _LambdaClient,
        *,
        cluster_arn: str,
        secret_arn: str,
        database: str,
        table_name: str,
        jobs_table: str,
        planner_function_name: str,
        job_id_factory: Any = None,
    ) -> None:
        if not SQL_IDENTIFIER_PATTERN.fullmatch(database) or not SQL_IDENTIFIER_PATTERN.fullmatch(
            table_name
        ):
            raise ValueError(INVALID_JOBS_CONFIG)
        if not jobs_table or not planner_function_name:
            raise ValueError(INVALID_JOBS_CONFIG)
        self._data_api = data_api
        self._dynamo = dynamo_client
        self._lambda = lambda_client
        self._request = {
            "resourceArn": cluster_arn,
            "secretArn": secret_arn,
            "database": database,
        }
        self._preview_sql = _PREVIEW_TEMPLATE.replace(_TABLE_TOKEN, table_name)
        self._jobs_table = jobs_table
        self._planner = planner_function_name
        self._job_id_factory = job_id_factory or (lambda: uuid.uuid4().hex)

    def preview(self, sql: Any, caller_sub: Any) -> dict[str, int]:
        """Return the exact distinct-document count the guarded selection resolves to."""
        _validate_caller(caller_sub)
        guarded = validate_reingest_sql(sql)
        count = self._preview_count(guarded)
        return {"count": count}

    def create_job(
        self,
        caller_sub: Any,
        *,
        sql: Any = None,
        document_ids: Any = None,
    ) -> dict[str, Any]:
        """Create one queued job from either SQL or an ID list and dispatch the planner.

        Exactly one selection source is allowed. A SQL job is previewed first and rejected
        when it resolves to more than the reingestion cap, so an oversized job never
        reaches DynamoDB or the planner. The job row records only ``idCount`` or the SQL and
        its SHA-256, and the full ID list or SQL travels in the planner payload.
        """
        caller = _validate_caller(caller_sub)
        if (sql is None) == (document_ids is None):
            raise ReingestRequestError(400, INVALID_JOB_REQUEST)

        job_id = self._new_job_id()
        created_at = datetime.now(UTC).isoformat()
        if sql is not None:
            submitted_sql = sql if isinstance(sql, str) else ""
            guarded = validate_reingest_sql(sql)
            expected = self._preview_count(guarded)
            if expected > MAX_SELECTION_DOCUMENTS:
                raise ReingestRequestError(400, SELECTION_TOO_LARGE)
            sql_sha256 = hashlib.sha256(submitted_sql.encode("utf-8")).hexdigest()
            item = self._base_item(job_id, MODE_SQL, caller, created_at, expected)
            item["sql"] = {"S": submitted_sql}
            item["sqlSha256"] = {"S": sql_sha256}
            payload: dict[str, Any] = {
                "jobId": job_id,
                "mode": MODE_SQL,
                "sql": submitted_sql,
            }
            audit_hash: str | None = sql_sha256
        else:
            ids = validate_document_ids(document_ids)
            expected = len(ids)
            item = self._base_item(job_id, MODE_IDS, caller, created_at, expected)
            item["idCount"] = {"N": str(expected)}
            payload = {"jobId": job_id, "mode": MODE_IDS, "documentIds": ids}
            audit_hash = None

        self._put_job(item)
        self._dispatch(payload)
        _audit(caller, job_id, item["mode"]["S"], expected, audit_hash)
        return _public_job(item)

    def list_jobs(self, limit: Any = None) -> dict[str, Any]:
        """Return one newest-first page of jobs, scanning the low-level jobs table."""
        page_size = _parse_limit(limit)
        jobs = [_public_job(item) for item in self._scan_items()]
        # A scan is unordered, so sort newest first on the immutable creation timestamp.
        jobs.sort(key=lambda job: job["createdAt"], reverse=True)
        return {"items": jobs[:page_size]}

    def _scan_items(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        request: dict[str, Any] = {"TableName": self._jobs_table}
        try:
            while True:
                response = self._dynamo.scan(**request)
                items.extend(_scan_page(response))
                start_key = response.get("LastEvaluatedKey")
                if not start_key:
                    break
                request["ExclusiveStartKey"] = start_key
        except ReingestError:
            raise
        except Exception:
            raise ReingestError(JOB_READ_FAILED) from None
        return items

    def get_job(self, job_id: Any) -> dict[str, Any]:
        """Return one job's public projection by job ID."""
        return _public_job(self._job_item(_validate_job_id(job_id)))

    def _preview_count(self, guarded_sql: str) -> int:
        sql = self._preview_sql.replace(_SELECTION_TOKEN, guarded_sql)
        try:
            response = self._data_api.execute_statement(**self._request, sql=sql)
        except Exception:
            raise ReingestRequestError(400, INVALID_SQL) from None
        try:
            return _scalar_count(response)
        except (KeyError, TypeError, ValueError):
            raise ReingestError(PREVIEW_FAILED) from None

    def _base_item(
        self,
        job_id: str,
        mode: str,
        caller: str,
        created_at: str,
        expected: int,
    ) -> dict[str, Any]:
        item: dict[str, Any] = {
            "jobId": {"S": job_id},
            "status": {"S": STATUS_QUEUED},
            "mode": {"S": mode},
            "requestedBy": {"S": caller},
            "createdAt": {"S": created_at},
            "expected": {"N": str(expected)},
            "enqueueComplete": {"BOOL": False},
        }
        for counter in COUNTER_NAMES:
            item[counter] = {"N": "0"}
        return item

    def _put_job(self, item: dict[str, Any]) -> None:
        try:
            self._dynamo.put_item(
                TableName=self._jobs_table,
                Item=item,
                ConditionExpression="attribute_not_exists(jobId)",
            )
        except Exception:
            raise ReingestError(JOB_CREATE_FAILED) from None

    def _dispatch(self, payload: dict[str, Any]) -> None:
        # The payload carries only identifiers, the guarded SQL, or hex document IDs, so it
        # is never logged. The 6 MB asynchronous event budget accommodates the largest job.
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        try:
            response = self._lambda.invoke(
                FunctionName=self._planner,
                InvocationType="Event",
                Payload=encoded,
            )
        except Exception:
            raise ReingestError(JOB_DISPATCH_FAILED) from None
        if response.get("StatusCode") not in _ASYNC_ACCEPTED_STATUS:
            raise ReingestError(JOB_DISPATCH_FAILED)

    def _job_item(self, job_id: str) -> dict[str, Any]:
        try:
            response = self._dynamo.get_item(
                TableName=self._jobs_table,
                Key={"jobId": {"S": job_id}},
            )
        except Exception:
            raise ReingestError(JOB_READ_FAILED) from None
        item = response.get("Item")
        if not isinstance(item, dict) or not item:
            raise ReingestRequestError(404, JOB_NOT_FOUND)
        return item

    def _new_job_id(self) -> str:
        job_id = self._job_id_factory()
        if not isinstance(job_id, str) or not JOB_ID_PATTERN.fullmatch(job_id):
            raise ReingestError(JOB_CREATE_FAILED)
        return job_id


class JobProgressStore:
    """Atomic counter and lifecycle updates shared by the planner and reindexer backend.

    Counter increments use DynamoDB ``ADD`` so two reindex workers recording outcomes for
    the same job never overwrite each other's totals. Completion is guarded by a status
    condition so exactly one caller can transition a running job to a terminal state.
    """

    def __init__(self, dynamo_client: _DynamoClient, *, jobs_table: str) -> None:
        if not jobs_table:
            raise ValueError(INVALID_JOBS_CONFIG)
        self._dynamo = dynamo_client
        self._jobs_table = jobs_table

    def mark_running(self, job_id: str) -> None:
        """Record the job as running and stamp its start time."""
        self._update(
            job_id,
            "SET #status = :running, startedAt = :startedAt",
            {":running": {"S": STATUS_RUNNING}, ":startedAt": {"S": _now_iso()}},
            names={"#status": "status"},
        )

    def add_enqueued(self, job_id: str, delta: int) -> None:
        """Atomically add to the enqueued counter as the planner fans documents out."""
        self._add(job_id, {"enqueued": delta})

    def record_outcomes(
        self,
        job_id: str,
        *,
        reindexed: int = 0,
        reindexed_stale_parser: int = 0,
        missing_parsed: int = 0,
        failed: int = 0,
    ) -> None:
        """Atomically add the parsed-zone reindexer's per-document outcome tallies.

        ``reindexed_stale_parser`` is an informational subset of ``reindexed`` (a document
        reindexed from a parse produced by an older parser version), so it is not counted
        again toward the terminal processed total.
        """
        self._add(
            job_id,
            {
                "reindexed": reindexed,
                "reindexedStaleParser": reindexed_stale_parser,
                "missingParsed": missing_parsed,
                "failed": failed,
            },
        )

    def mark_enqueue_complete(self, job_id: str) -> None:
        """Record that the planner has finished enqueuing every selected document."""
        self._update(
            job_id,
            "SET enqueueComplete = :true",
            {":true": {"BOOL": True}},
        )

    def mark_failed(self, job_id: str) -> None:
        """Transition a running or queued job to the terminal failed state."""
        self._update(
            job_id,
            "SET #status = :failed, finishedAt = :finishedAt",
            {":failed": {"S": STATUS_FAILED}, ":finishedAt": {"S": _now_iso()}},
            names={"#status": "status"},
        )

    def complete_if_settled(self, job_id: str) -> bool:
        """Complete the job when enqueue is done and every enqueued document is processed.

        Returns ``True`` only when this call performed the terminal transition, so a caller
        can rely on a single completion event even under concurrent reindex workers. The
        completion write is conditioned on the status still being ``running`` so a lost race
        is a no-op rather than an error.
        """
        item = self._read(job_id)
        if _string_value(item, "status") != STATUS_RUNNING:
            return False
        if not _bool_value(item, "enqueueComplete"):
            return False
        enqueued = _int_value(item, "enqueued")
        processed = (
            _int_value(item, "reindexed")
            + _int_value(item, "missingParsed")
            + _int_value(item, "failed")
        )
        if processed < enqueued:
            return False
        return self._mark_complete(job_id)

    def _mark_complete(self, job_id: str) -> bool:
        try:
            self._dynamo.update_item(
                TableName=self._jobs_table,
                Key={"jobId": {"S": job_id}},
                UpdateExpression="SET #status = :complete, finishedAt = :finishedAt",
                ConditionExpression="#status = :running",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":complete": {"S": STATUS_COMPLETE},
                    ":finishedAt": {"S": _now_iso()},
                    ":running": {"S": STATUS_RUNNING},
                },
            )
        except Exception as error:
            # A losing completion race is expected and benign; anything else is sanitized.
            if _client_error_code(error) == _CONDITIONAL_CHECK_FAILED:
                return False
            raise ReingestError(JOB_UPDATE_FAILED) from None
        return True

    def _add(self, job_id: str, deltas: Mapping[str, int]) -> None:
        parts: list[str] = []
        values: dict[str, Any] = {}
        for name, delta in deltas.items():
            if delta:
                parts.append(f"{name} :{name}")
                values[f":{name}"] = {"N": str(delta)}
        if not parts:
            return
        self._update(job_id, "ADD " + ", ".join(parts), values)

    def _update(
        self,
        job_id: str,
        expression: str,
        values: dict[str, Any],
        *,
        names: dict[str, str] | None = None,
    ) -> None:
        request: dict[str, Any] = {
            "TableName": self._jobs_table,
            "Key": {"jobId": {"S": job_id}},
            "UpdateExpression": expression,
            "ExpressionAttributeValues": values,
            "ConditionExpression": "attribute_exists(jobId)",
        }
        if names is not None:
            request["ExpressionAttributeNames"] = names
        try:
            self._dynamo.update_item(**request)
        except Exception:
            raise ReingestError(JOB_UPDATE_FAILED) from None

    def _read(self, job_id: str) -> dict[str, Any]:
        try:
            response = self._dynamo.get_item(
                TableName=self._jobs_table,
                Key={"jobId": {"S": job_id}},
            )
        except Exception:
            raise ReingestError(JOB_READ_FAILED) from None
        item = response.get("Item")
        if not isinstance(item, dict) or not item:
            raise ReingestError(JOB_READ_FAILED)
        return item


def _scan_page(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_items = response.get("Items", [])
    if not isinstance(raw_items, list):
        raise ReingestError(JOB_READ_FAILED)
    return [item for item in raw_items if isinstance(item, dict)]


def _audit(caller: str, job_id: str, mode: str, count: int, sql_sha256: str | None) -> None:
    event: dict[str, Any] = {
        "callerSub": caller,
        "count": count,
        "event": "reingest_job_created",
        "jobId": job_id,
        "mode": mode,
        "timestamp": _now_iso(),
    }
    if sql_sha256 is not None:
        event["sqlSha256"] = sql_sha256
    LOGGER.info(json.dumps(event, separators=(",", ":"), sort_keys=True))


def _public_job(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ReingestError(JOB_READ_FAILED)
    job: dict[str, Any] = {
        "jobId": _string_value(item, "jobId"),
        "status": _string_value(item, "status"),
        "mode": _string_value(item, "mode"),
        "requestedBy": _string_value(item, "requestedBy"),
        "createdAt": _string_value(item, "createdAt"),
        "expected": _int_value(item, "expected"),
        "enqueueComplete": _bool_value(item, "enqueueComplete"),
        "counters": {name: _int_value(item, name) for name in COUNTER_NAMES},
    }
    started_at = _optional_string_value(item, "startedAt")
    if started_at is not None:
        job["startedAt"] = started_at
    finished_at = _optional_string_value(item, "finishedAt")
    if finished_at is not None:
        job["finishedAt"] = finished_at
    sql_sha256 = _optional_string_value(item, "sqlSha256")
    if sql_sha256 is not None:
        job["sqlSha256"] = sql_sha256
    # The verbatim SQL is projected for SQL jobs so the authenticated UI can expand a job
    # and show exactly what was submitted. This projection is only ever returned over the
    # JWT-protected reingest routes and is never written to any log line.
    sql = _optional_string_value(item, "sql")
    if sql is not None:
        job["sql"] = sql
    id_count = _optional_int_value(item, "idCount")
    if id_count is not None:
        job["idCount"] = id_count
    return job


def _scalar_count(response: Mapping[str, Any]) -> int:
    records = response.get("records")
    if not isinstance(records, list) or len(records) != 1:
        raise ValueError
    row = records[0]
    if not isinstance(row, list) or len(row) != 1:
        raise ValueError
    field = row[0]
    if not isinstance(field, dict):
        raise TypeError
    value = field.get("longValue")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError
    return value


def _parse_limit(value: Any) -> int:
    if value is None:
        return DEFAULT_LIST_LIMIT
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReingestRequestError(400, INVALID_LIMIT)
    if not 1 <= value <= MAX_LIST_LIMIT:
        raise ReingestRequestError(400, INVALID_LIMIT)
    return value


def _validate_caller(caller_sub: Any) -> str:
    if not isinstance(caller_sub, str) or not caller_sub or len(caller_sub) > 128:
        raise ReingestRequestError(400, INVALID_CALLER)
    return caller_sub


def _validate_job_id(job_id: Any) -> str:
    if not isinstance(job_id, str) or not JOB_ID_PATTERN.fullmatch(job_id):
        raise ReingestRequestError(400, INVALID_JOB_ID)
    return job_id


def _client_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    error_info = response.get("Error")
    if not isinstance(error_info, dict):
        return None
    code = error_info.get("Code")
    return code if isinstance(code, str) else None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _string_value(item: Mapping[str, Any], key: str) -> str:
    value = _optional_string_value(item, key)
    if value is None:
        raise ReingestError(JOB_READ_FAILED)
    return value


def _optional_string_value(item: Mapping[str, Any], key: str) -> str | None:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        return None
    value = attribute.get("S")
    return value if isinstance(value, str) else None


def _int_value(item: Mapping[str, Any], key: str) -> int:
    value = _optional_int_value(item, key)
    if value is None:
        raise ReingestError(JOB_READ_FAILED)
    return value


def _optional_int_value(item: Mapping[str, Any], key: str) -> int | None:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        return None
    raw = attribute.get("N")
    if not isinstance(raw, str):
        return None
    try:
        return int(raw)
    except ValueError:
        raise ReingestError(JOB_READ_FAILED) from None


def _bool_value(item: Mapping[str, Any], key: str) -> bool:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        return False
    return attribute.get("BOOL") is True
