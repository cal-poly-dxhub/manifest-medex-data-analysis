"""Track report runs in DynamoDB, dispatch them asynchronously, and serve their output.

A run is created with a ``running`` status and immediately triggers an asynchronous
(``Event``) Lambda invocation of the worker that computes the report. The dispatched
payload carries the run id and every report parameter under the same snake_case naming
contract the worker validates with :func:`src.report_runner.parse_report_request`, so the
API and the worker can never drift. The worker later records progress and the terminal
outcome directly against this table through
:class:`src.report_runner.DynamoRunProgressStore`.

Run listing works with either a DynamoDB scan or a report-id global secondary index, so
the same code path serves a small development table and an indexed production table.

Output downloads are authenticated at the edge and re-verified here: the run must be
``complete``, the stored archive key is read from within the configured output bucket,
and the body is read under a hard byte bound. Each download emits an audit event
containing only the caller subject, the run id, and a timestamp; report contents, S3
keys, and partition values are never logged. All failures surface as typed, sanitized
errors so an HTTP handler can own routing and response shaping.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from src.report_runner import ReportRequestError, parse_report_request

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

DEFAULT_MAX_OUTPUT_BYTES = 25 * 1024 * 1024
DEFAULT_MAX_RUNS = 500

REPORT_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,127}$")
RUN_ID_PATTERN = re.compile(r"^[0-9A-Za-z_-]{1,128}$")

# The lifecycle statuses shared with the worker's progress store.
STATUS_RUNNING = "running"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"

# Lambda returns 202 for an accepted asynchronous (Event) invocation.
_ASYNC_ACCEPTED_STATUS = frozenset({200, 202})

INVALID_RUNS_CONFIG = "Runs service configuration is invalid"
TABLE_CREATE_FAILED = "Report runs table creation failed"
RUN_CREATE_FAILED = "Report run creation failed"
RUN_DISPATCH_FAILED = "Report run dispatch failed"
RUN_READ_FAILED = "Report run read failed"
RUN_OUTPUT_MISSING = "Report run output is missing"
OUTPUT_RETRIEVAL_FAILED = "Report output retrieval failed"

# Short, caller-safe codes for expected request failures.
INVALID_REPORT_ID = "invalid_report_id"
INVALID_RUN_ID = "invalid_run_id"
INVALID_CALLER = "invalid_caller"
RUN_NOT_FOUND = "run_not_found"
RUN_NOT_READY = "run_output_not_ready"
OUTPUT_TOO_LARGE = "report_output_too_large"

ARCHIVE_CONTENT_TYPE = "application/zip"


class _ReadableBody(Protocol):
    def read(self, amount: int | None = None) -> bytes: ...


class _S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


class _DynamoClient(Protocol):
    def create_table(self, **kwargs: Any) -> dict[str, Any]: ...

    def put_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def query(self, **kwargs: Any) -> dict[str, Any]: ...

    def scan(self, **kwargs: Any) -> dict[str, Any]: ...


class _LambdaClient(Protocol):
    def invoke(self, **kwargs: Any) -> dict[str, Any]: ...


class RunError(RuntimeError):
    """Sanitized run failure that never includes DynamoDB, Lambda, or S3 details."""


class RunRequestError(RunError):
    """Expected run failure safe to return to the authenticated caller."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


@dataclass(frozen=True)
class DownloadResult:
    """Bounded report output returned through API Gateway."""

    content: bytes
    content_type: str


class ReportRuns:
    """Create, dispatch, inspect, and serve the output of report runs."""

    def __init__(
        self,
        dynamo_client: _DynamoClient,
        lambda_client: _LambdaClient,
        s3_client: _S3Client,
        *,
        table_name: str,
        worker_function_name: str,
        output_bucket: str,
        report_index_name: str | None = None,
        max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
        max_runs: int = DEFAULT_MAX_RUNS,
        run_id_factory: Callable[[], str] | None = None,
    ) -> None:
        if (
            not table_name
            or not worker_function_name
            or not output_bucket
            or max_output_bytes < 1
            or max_runs < 1
        ):
            raise ValueError(INVALID_RUNS_CONFIG)
        self._dynamo = dynamo_client
        self._lambda = lambda_client
        self._s3 = s3_client
        self._table = table_name
        self._worker = worker_function_name
        self._output_bucket = output_bucket
        self._report_index_name = report_index_name
        self._max_output_bytes = max_output_bytes
        self._max_runs = max_runs
        self._run_id_factory = run_id_factory or (lambda: uuid.uuid4().hex)

    def ensure_table(self) -> None:
        """Idempotently create the runs table, adding the report index when configured.

        The runtime provisions this table through CDK; this helper exists for local and
        test environments that need a table without the full stack.
        """
        attribute_definitions: list[dict[str, str]] = [
            {"AttributeName": "runId", "AttributeType": "S"},
        ]
        request: dict[str, Any] = {
            "TableName": self._table,
            "KeySchema": [{"AttributeName": "runId", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
        }
        if self._report_index_name is not None:
            attribute_definitions.append({"AttributeName": "reportId", "AttributeType": "S"})
            attribute_definitions.append({"AttributeName": "startedAt", "AttributeType": "S"})
            request["GlobalSecondaryIndexes"] = [
                {
                    "IndexName": self._report_index_name,
                    "KeySchema": [
                        {"AttributeName": "reportId", "KeyType": "HASH"},
                        {"AttributeName": "startedAt", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ]
        request["AttributeDefinitions"] = attribute_definitions
        try:
            self._dynamo.create_table(**request)
        except Exception as error:
            if _client_error_code(error) == "ResourceInUseException":
                return
            raise RunError(TABLE_CREATE_FAILED) from None

    def start_run(
        self,
        report_id: str,
        from_time: str,
        to_time: str,
        partition_values: list[str],
        caller_sub: str,
    ) -> dict[str, Any]:
        """Record a new running run and dispatch the worker asynchronously.

        The half-open time window and one-to-two-hundred partition values are validated
        through the worker's single source of truth (:func:`parse_report_request`) so the
        dispatched payload is guaranteed to be accepted by the worker.
        """
        _validate_report_id(report_id)
        caller = _validate_caller(caller_sub)
        run_id = self._run_id_factory()
        _validate_run_id(run_id)
        # One consistent snake_case payload shared by the API and the worker.
        payload_event: dict[str, Any] = {
            "report_id": report_id,
            "run_id": run_id,
            "from": from_time,
            "to": to_time,
            "partition_values": partition_values,
        }
        try:
            request = parse_report_request(payload_event)
        except ReportRequestError as error:
            raise RunRequestError(400, error.code) from None
        started_at = datetime.now(UTC).isoformat()
        total = len(request.partition_values)
        item = {
            "runId": {"S": run_id},
            "reportId": {"S": report_id},
            "fromTime": {"S": request.from_time},
            "toTime": {"S": request.to_time},
            "partitionValues": {"L": [{"S": value} for value in request.partition_values]},
            "requestedBy": {"S": caller},
            "startedAt": {"S": started_at},
            "status": {"S": STATUS_RUNNING},
            "completedPartitions": {"N": "0"},
            "totalPartitions": {"N": str(total)},
        }
        try:
            self._dynamo.put_item(
                TableName=self._table,
                Item=item,
                ConditionExpression="attribute_not_exists(runId)",
            )
        except Exception:
            raise RunError(RUN_CREATE_FAILED) from None
        # The payload carries only non-clinical identifiers and is never logged.
        payload = json.dumps(payload_event, separators=(",", ":"), sort_keys=True).encode("utf-8")
        try:
            response = self._lambda.invoke(
                FunctionName=self._worker,
                InvocationType="Event",
                Payload=payload,
            )
        except Exception:
            raise RunError(RUN_DISPATCH_FAILED) from None
        if response.get("StatusCode") not in _ASYNC_ACCEPTED_STATUS:
            raise RunError(RUN_DISPATCH_FAILED)
        return _public_run(item)

    def get_run_status(self, run_id: str) -> dict[str, Any]:
        """Return the current public status projection for one run."""
        return _public_run(self._run_item(run_id))

    def list_runs_for_report(self, report_id: str) -> list[dict[str, Any]]:
        """Return runs for a report, newest first, via the GSI or a filtered scan."""
        _validate_report_id(report_id)
        if self._report_index_name is not None:
            return self._query_runs(report_id)
        return self._scan_runs(report_id)

    def download_output(self, run_id: str, caller_sub: str) -> DownloadResult:
        """Return one completed run's bounded ZIP archive and emit an audit event."""
        _validate_run_id(run_id)
        caller = _validate_caller(caller_sub)
        item = self._run_item(run_id)
        if _string_attribute(item, "status") != STATUS_COMPLETE:
            raise RunRequestError(409, RUN_NOT_READY)
        key = _optional_string_attribute(item, "zipS3Key")
        if key is None:
            raise RunError(RUN_OUTPUT_MISSING)
        request: dict[str, Any] = {"Bucket": self._output_bucket, "Key": key}
        version_id = _optional_string_attribute(item, "version")
        if version_id is not None:
            request["VersionId"] = version_id
        try:
            response = self._s3.get_object(**request)
        except Exception:
            raise RunError(OUTPUT_RETRIEVAL_FAILED) from None
        if int(response.get("ContentLength", 0)) > self._max_output_bytes:
            raise RunRequestError(413, OUTPUT_TOO_LARGE)
        content = _read_bounded(response.get("Body"), self._max_output_bytes)
        LOGGER.info(
            json.dumps(
                {
                    "callerSub": caller,
                    "event": "report_output_downloaded",
                    "runId": run_id,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return DownloadResult(content=content, content_type=ARCHIVE_CONTENT_TYPE)

    def _run_item(self, run_id: str) -> dict[str, Any]:
        _validate_run_id(run_id)
        try:
            response = self._dynamo.get_item(
                TableName=self._table,
                Key={"runId": {"S": run_id}},
            )
        except Exception:
            raise RunError(RUN_READ_FAILED) from None
        item = response.get("Item")
        if not isinstance(item, dict) or not item:
            raise RunRequestError(404, RUN_NOT_FOUND)
        return item

    def _query_runs(self, report_id: str) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        request: dict[str, Any] = {
            "TableName": self._table,
            "IndexName": self._report_index_name,
            "KeyConditionExpression": "reportId = :reportId",
            "ExpressionAttributeValues": {":reportId": {"S": report_id}},
            # The GSI range key is startedAt, so descending order is newest first.
            "ScanIndexForward": False,
        }
        try:
            while True:
                response = self._dynamo.query(**request)
                runs.extend(_collect_runs(response))
                start_key = response.get("LastEvaluatedKey")
                if len(runs) >= self._max_runs or not start_key:
                    break
                request["ExclusiveStartKey"] = start_key
        except RunError:
            raise
        except Exception:
            raise RunError(RUN_READ_FAILED) from None
        return runs[: self._max_runs]

    def _scan_runs(self, report_id: str) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        request: dict[str, Any] = {
            "TableName": self._table,
            "FilterExpression": "reportId = :reportId",
            "ExpressionAttributeValues": {":reportId": {"S": report_id}},
        }
        try:
            while True:
                response = self._dynamo.scan(**request)
                runs.extend(_collect_runs(response))
                start_key = response.get("LastEvaluatedKey")
                if len(runs) >= self._max_runs or not start_key:
                    break
                request["ExclusiveStartKey"] = start_key
        except RunError:
            raise
        except Exception:
            raise RunError(RUN_READ_FAILED) from None
        # A scan is unordered, so sort newest first on the start timestamp.
        runs.sort(key=lambda run: run["startedAt"], reverse=True)
        return runs[: self._max_runs]


def _collect_runs(response: dict[str, Any]) -> list[dict[str, Any]]:
    raw_items = response.get("Items", [])
    if not isinstance(raw_items, list):
        raise RunError(RUN_READ_FAILED)
    return [_public_run(item) for item in raw_items]


def _public_run(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise RunError(RUN_READ_FAILED)
    status = _string_attribute(item, "status")
    run: dict[str, Any] = {
        "runId": _string_attribute(item, "runId"),
        "reportId": _string_attribute(item, "reportId"),
        "status": status,
        "requestedBy": _string_attribute(item, "requestedBy"),
        "startedAt": _string_attribute(item, "startedAt"),
        "params": {
            "from": _string_attribute(item, "fromTime"),
            "to": _string_attribute(item, "toTime"),
            "partitionValues": _string_list_attribute(item, "partitionValues"),
        },
        "progress": {
            "completedPartitions": _int_attribute(item, "completedPartitions"),
            "totalPartitions": _int_attribute(item, "totalPartitions"),
        },
        "downloadReady": status == STATUS_COMPLETE,
    }
    finished_at = _optional_string_attribute(item, "finishedAt")
    if finished_at is not None:
        run["finishedAt"] = finished_at
    failing_partition = _optional_string_attribute(item, "failingPartition")
    if failing_partition is not None:
        run["failingPartition"] = failing_partition
    if status == STATUS_COMPLETE:
        # Aggregate row counts (keyed by row identity) are only meaningful once a run is
        # complete, so the projection surfaces them exclusively for complete runs (when
        # persisted).
        row_counts = _optional_int_map_attribute(item, "rowCounts")
        if row_counts is not None:
            run["rowCounts"] = row_counts
        # The execution summary (how many rows carried a real query versus were skipped as
        # placeholders) is likewise surfaced only for complete runs, when persisted.
        summary = _optional_run_summary(item)
        if summary is not None:
            run["summary"] = summary
    return run


def _optional_run_summary(item: dict[str, Any]) -> dict[str, int] | None:
    """Decode the optional per-run execution summary into plain ints.

    Returns ``None`` when neither tally is present (an older run recorded before the
    summary existed). When either is present, both are decoded; a malformed numeric value
    is a stored-shape violation and surfaces as a read failure.
    """
    if "rowsExecuted" not in item and "placeholdersSkipped" not in item:
        return None
    return {
        "rowsExecuted": _int_attribute(item, "rowsExecuted"),
        "placeholdersSkipped": _int_attribute(item, "placeholdersSkipped"),
    }


def _validate_report_id(report_id: str) -> None:
    if not isinstance(report_id, str) or not REPORT_ID_PATTERN.fullmatch(report_id):
        raise RunRequestError(400, INVALID_REPORT_ID)


def _validate_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise RunRequestError(400, INVALID_RUN_ID)


def _validate_caller(caller_sub: str) -> str:
    if not isinstance(caller_sub, str) or not caller_sub or len(caller_sub) > 128:
        raise RunRequestError(400, INVALID_CALLER)
    return caller_sub


def _client_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    error_info = response.get("Error")
    if not isinstance(error_info, dict):
        return None
    code = error_info.get("Code")
    return code if isinstance(code, str) else None


def _read_bounded(body: Any, max_bytes: int) -> bytes:
    if not hasattr(body, "read"):
        raise RunError(OUTPUT_RETRIEVAL_FAILED)
    payload = cast(_ReadableBody, body).read(max_bytes + 1)
    if not isinstance(payload, bytes):
        raise RunError(OUTPUT_RETRIEVAL_FAILED)
    if len(payload) > max_bytes:
        raise RunRequestError(413, OUTPUT_TOO_LARGE)
    return payload


def _string_attribute(item: dict[str, Any], key: str) -> str:
    value = _optional_string_attribute(item, key)
    if value is None:
        raise RunError(RUN_READ_FAILED)
    return value


def _optional_string_attribute(item: dict[str, Any], key: str) -> str | None:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        return None
    value = attribute.get("S")
    return value if isinstance(value, str) else None


def _int_attribute(item: dict[str, Any], key: str) -> int:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        raise RunError(RUN_READ_FAILED)
    value = attribute.get("N")
    if not isinstance(value, str):
        raise RunError(RUN_READ_FAILED)
    try:
        return int(value)
    except ValueError:
        raise RunError(RUN_READ_FAILED) from None


def _string_list_attribute(item: dict[str, Any], key: str) -> list[str]:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        raise RunError(RUN_READ_FAILED)
    raw = attribute.get("L")
    if not isinstance(raw, list):
        raise RunError(RUN_READ_FAILED)
    values: list[str] = []
    for element in raw:
        if not isinstance(element, dict):
            raise RunError(RUN_READ_FAILED)
        value = element.get("S")
        if not isinstance(value, str):
            raise RunError(RUN_READ_FAILED)
        values.append(value)
    return values


def _optional_int_map_attribute(item: dict[str, Any], key: str) -> dict[str, int | None] | None:
    """Decode an optional DynamoDB map of numeric/null values into a plain mapping.

    A missing or non-map attribute yields ``None`` so the projection can omit it. Within the
    map, a numeric (N) value decodes to an ``int`` and a NULL value decodes to ``None`` (a
    placeholder row whose count is blank); any other shape is a stored-shape violation and
    surfaces as a read failure.
    """
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        return None
    raw = attribute.get("M")
    if not isinstance(raw, dict):
        return None
    result: dict[str, int | None] = {}
    for label, value in raw.items():
        if not isinstance(value, dict):
            raise RunError(RUN_READ_FAILED)
        if value.get("NULL") is True:
            result[label] = None
            continue
        number = value.get("N")
        if not isinstance(number, str):
            raise RunError(RUN_READ_FAILED)
        try:
            result[label] = int(number)
        except ValueError:
            raise RunError(RUN_READ_FAILED) from None
    return result
