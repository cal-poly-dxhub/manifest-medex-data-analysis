"""Run a report definition across partitions and publish one bundled CSV archive.

A report run receives an event naming a report definition, a half-open ISO time window,
and one to two hundred partition values (source facility identifiers). For each partition the
runner counts every definition row by injecting a partition ``term`` and a time-window
``range`` into a deep copy of the row query -- the stored query is never mutated -- and
issuing size-0, total-tracking OpenSearch ``_msearch`` requests batched at twenty row
queries (forty NDJSON lines) per request. Per-partition counts are rendered by the pure
formatter into one CSV per sanitized partition filename, all collected into a single
in-memory ZIP. The archive is uploaded only after every partition succeeds; run progress
is reported after each partition and a failure records the failing partition without any
partial upload.

The transport mirrors the SigV4 request style of ``src.search_store`` (no ``opensearch-py``
dependency) and reuses its :class:`~src.search_store.SearchTransport` protocol. Runtime
wiring is lazy so the module imports cleanly outside the Lambda runtime, and all logs are
bounded and sanitized so a fifteen-minute run never emits clinical content.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
import urllib.error
import urllib.request
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import batched
from typing import Any, Protocol

from src.report_catalog import ReportCatalog
from src.report_csv import format_report_csv_bytes
from src.report_definition import (
    ReportDefinition,
    ReportDefinitionError,
    load_report_definition,
    row_count_key,
)
from src.report_query import QueryCountError, build_count_body, read_total_hits
from src.search_store import SearchTransport

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

MSEARCH_PATH = "/_msearch"
MAX_ROW_QUERIES_PER_BATCH = 20

MIN_PARTITION_VALUES = 1
# The runner processes facilities sequentially inside a single 15-minute Lambda,
# building one CSV per facility and packaging them into one bounded ZIP returned
# through a single API request. 200 is a deliberate upper bound that keeps the
# worst-case run within the Lambda timeout, the in-memory ZIP size, and the
# request/response limits. It is intentionally bounded, never unlimited.
MAX_PARTITION_VALUES = 200
MAX_PARTITION_VALUE_LENGTH = 256
MAX_REPORT_ID_LENGTH = 200
MAX_RUN_ID_LENGTH = 128
MAX_ISO_LENGTH = 64

# A run id is an opaque, URL- and S3-key-safe token minted by the Reports API.
RUN_ID_PATTERN = re.compile(r"^[0-9A-Za-z_-]{1,128}$")
MAX_UID_FILENAME_LENGTH = 100
MAX_ARCHIVE_BYTES = 25 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 60

INVALID_ENDPOINT = "OpenSearch endpoint must use HTTPS"
CREDENTIALS_UNAVAILABLE = "AWS credentials are unavailable for OpenSearch signing"
MSEARCH_REQUEST_FAILED = "OpenSearch multi-search request failed"
MSEARCH_RESPONSE_INVALID = "OpenSearch multi-search response was malformed"
MSEARCH_COUNT_MISMATCH = "OpenSearch multi-search response count did not match request"
MSEARCH_QUERY_REJECTED = "OpenSearch rejected a report query"
DEFINITION_LOAD_FAILED = "Report definition could not be loaded"
DEFINITION_MISMATCH = "Report definition does not match the requested report"
COUNT_MISSING = "A report row count was not produced"
ARCHIVE_TOO_LARGE = "Report archive exceeded the maximum allowed size"
REPORT_UPLOAD_FAILED = "Report upload failed"
RUN_PROGRESS_UPDATE_FAILED = "Report run progress update failed"

_ALLOWED_BACKEND_ERROR_TYPES = frozenset(
    {
        "authorization_exception",
        "cluster_block_exception",
        "forbidden_exception",
        "illegal_argument_exception",
        "index_not_found_exception",
        "parse_exception",
        "rejected_execution_exception",
        "search_phase_execution_exception",
        "security_exception",
        "too_many_requests_exception",
        "validation_exception",
        "x_content_parse_exception",
    }
)

_UID_UNSAFE = re.compile(r"[^A-Za-z0-9_-]")


class ReportRequestError(ValueError):
    """An invalid report run event, safe to surface to the caller by stable code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ReportRunError(RuntimeError):
    """Sanitized report run failure carrying only bounded operational telemetry."""

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


@dataclass(frozen=True)
class ReportRequest:
    """A validated report run request."""

    report_id: str
    run_id: str
    from_time: str
    to_time: str
    partition_values: tuple[str, ...]


@dataclass(frozen=True)
class ReportOutput:
    """The result of a successful report run."""

    report_id: str
    run_id: str
    key: str
    version: str
    partition_count: int
    byte_size: int


@dataclass(frozen=True)
class _RowQuery:
    """One row's partition-scoped search, keyed by its section and row identity."""

    section_seq: int
    row_seq: int
    index: str
    body: dict[str, Any]


class ReportSink(Protocol):
    """Durable destination for the completed report archive.

    Implementations upload the archive under ``key`` and return the durable object
    version id so the Reports run record can bind the download to an exact version.
    """

    def put_report(self, key: str, body: bytes) -> str: ...


class RunProgressStore(Protocol):
    """Sink for run lifecycle transitions, keyed by the run id minted by the API."""

    def record_progress(self, run_id: str, completed: int, total: int) -> None: ...

    def record_success(
        self,
        run_id: str,
        zip_s3_key: str,
        version: str,
        row_counts: Mapping[str, int | None],
        rows_executed: int,
        placeholders_skipped: int,
    ) -> None: ...

    def record_failure(self, run_id: str, failing_partition: str) -> None: ...


DefinitionProvider = Callable[[str], ReportDefinition]


def parse_report_request(event: Mapping[str, Any]) -> ReportRequest:
    """Validate a report run event into a :class:`ReportRequest`."""
    if not isinstance(event, Mapping):
        raise ReportRequestError("invalid_event")

    report_id = event.get("report_id")
    if (
        not isinstance(report_id, str)
        or not report_id.strip()
        or len(report_id) > MAX_REPORT_ID_LENGTH
    ):
        raise ReportRequestError("invalid_report_id")

    run_id = event.get("run_id")
    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise ReportRequestError("invalid_run_id")

    from_parsed, from_raw = _parse_iso(event.get("from"), "invalid_from")
    to_parsed, to_raw = _parse_iso(event.get("to"), "invalid_to")
    if from_parsed >= to_parsed:
        raise ReportRequestError("invalid_time_range")

    partition_values = event.get("partition_values")
    if not isinstance(partition_values, list):
        raise ReportRequestError("invalid_partition_values")
    if not MIN_PARTITION_VALUES <= len(partition_values) <= MAX_PARTITION_VALUES:
        raise ReportRequestError("invalid_partition_values")
    cleaned: list[str] = []
    for value in partition_values:
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > MAX_PARTITION_VALUE_LENGTH
        ):
            raise ReportRequestError("invalid_partition_values")
        cleaned.append(value)
    if len(set(cleaned)) != len(cleaned):
        raise ReportRequestError("duplicate_partition_values")

    return ReportRequest(
        report_id=report_id,
        run_id=run_id,
        from_time=from_raw,
        to_time=to_raw,
        partition_values=tuple(cleaned),
    )


def _parse_iso(value: object, code: str) -> tuple[datetime, str]:
    if not isinstance(value, str) or len(value) > MAX_ISO_LENGTH:
        raise ReportRequestError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ReportRequestError(code) from None
    if parsed.tzinfo is None:
        raise ReportRequestError(code)
    return parsed, value


class ReportRunner:
    """Count a definition across partitions and publish one bundled CSV archive."""

    def __init__(
        self,
        transport: SearchTransport,
        sink: ReportSink,
        progress: RunProgressStore,
        definition_provider: DefinitionProvider,
        *,
        key_prefix: str = "outputs",
    ) -> None:
        self._transport = transport
        self._sink = sink
        self._progress = progress
        self._definition_provider = definition_provider
        self._key_prefix = key_prefix.strip("/") or "outputs"

    def run(self, event: Mapping[str, Any]) -> ReportOutput:
        """Validate the event, count every partition, and upload the archive once."""
        request = parse_report_request(event)
        definition = self._load_definition(request.report_id)
        total = len(request.partition_values)
        LOGGER.info(
            _log_event("report_run_started", reportId=request.report_id, partitionCount=total)
        )

        counts_by_partition: dict[str, dict[tuple[int, int], int]] = {}
        for index, partition_value in enumerate(request.partition_values, start=1):
            try:
                counts = self._count_partition(definition, partition_value, request)
            except ReportRunError:
                self._progress.record_failure(request.run_id, partition_value)
                raise
            except Exception:
                self._progress.record_failure(request.run_id, partition_value)
                raise ReportRunError(MSEARCH_REQUEST_FAILED) from None
            counts_by_partition[partition_value] = counts
            self._progress.record_progress(request.run_id, index, total)

        archive, byte_size = _build_archive(definition, request, counts_by_partition)
        key = _archive_key(self._key_prefix, request)
        version = self._sink.put_report(key, archive)
        row_counts = _aggregate_row_counts(definition, counts_by_partition)
        rows_executed, placeholders_skipped = _row_execution_counts(definition)
        self._progress.record_success(
            request.run_id, key, version, row_counts, rows_executed, placeholders_skipped
        )
        LOGGER.info(
            _log_event(
                "report_run_succeeded",
                reportId=request.report_id,
                partitionCount=total,
                byteSize=byte_size,
            )
        )
        return ReportOutput(
            report_id=request.report_id,
            run_id=request.run_id,
            key=key,
            version=version,
            partition_count=total,
            byte_size=byte_size,
        )

    def _load_definition(self, report_id: str) -> ReportDefinition:
        try:
            definition = self._definition_provider(report_id)
        except ReportRunError:
            raise
        except Exception:
            raise ReportRunError(DEFINITION_LOAD_FAILED) from None
        if definition.report_id != report_id:
            raise ReportRunError(DEFINITION_MISMATCH)
        return definition

    def _count_partition(
        self,
        definition: ReportDefinition,
        partition_value: str,
        request: ReportRequest,
    ) -> dict[tuple[int, int], int]:
        row_queries = _row_queries(definition, partition_value, request)
        counts: dict[tuple[int, int], int] = {}
        for batch in batched(row_queries, MAX_ROW_QUERIES_PER_BATCH):
            status, response = self._transport.request("POST", MSEARCH_PATH, _msearch_ndjson(batch))
            responses = _validated_msearch_responses(status, response, len(batch))
            for row_query, item in zip(batch, responses, strict=True):
                counts[(row_query.section_seq, row_query.row_seq)] = _extract_count(item)
        return counts


def _row_queries(
    definition: ReportDefinition,
    partition_value: str,
    request: ReportRequest,
) -> list[_RowQuery]:
    row_queries: list[_RowQuery] = []
    for section in sorted(definition.sections, key=lambda item: item.seq):
        for row in sorted(section.rows, key=lambda item: item.seq):
            # Placeholder rows carry no query: they are never counted and never issued to
            # OpenSearch, so their count renders blank while their grid position is kept.
            if row.query is None:
                continue
            # The partition term and time-window range injection is owned by
            # src.report_query so a real run and the dry-run query test stay identical.
            body = build_count_body(
                row.query,
                partition_field=definition.partition_field,
                partition_value=partition_value,
                time_field=definition.time_field,
                from_time=request.from_time,
                to_time=request.to_time,
            )
            row_queries.append(
                _RowQuery(
                    section_seq=section.seq,
                    row_seq=row.seq,
                    index=row.index,
                    body=body,
                )
            )
    return row_queries


def _msearch_ndjson(batch: tuple[_RowQuery, ...]) -> bytes:
    lines: list[str] = []
    for row_query in batch:
        lines.append(json.dumps({"index": row_query.index}, separators=(",", ":")))
        lines.append(json.dumps(row_query.body, separators=(",", ":"), sort_keys=True))
    return ("\n".join(lines) + "\n").encode()


def _validated_msearch_responses(
    status: int,
    response: dict[str, Any],
    expected_count: int,
) -> list[dict[str, Any]]:
    if status < 200 or status >= 300:
        raise ReportRunError(
            MSEARCH_REQUEST_FAILED,
            http_status=status,
            backend_error_type=_raw_error_type(response),
        )
    responses = response.get("responses")
    if not isinstance(responses, list):
        raise ReportRunError(MSEARCH_RESPONSE_INVALID)
    if len(responses) != expected_count:
        raise ReportRunError(MSEARCH_COUNT_MISMATCH)
    return responses


def _extract_count(item: Any) -> int:
    if not isinstance(item, dict):
        raise ReportRunError(MSEARCH_RESPONSE_INVALID)
    status = item.get("status")
    if isinstance(status, int) and (status < 200 or status >= 300):
        raise ReportRunError(
            MSEARCH_QUERY_REJECTED,
            http_status=status,
            backend_error_type=_raw_error_type(item),
        )
    # The total-hits parsing is shared with the dry-run query test so both read a
    # size-0, total-tracking response identically; only the transport framing differs.
    try:
        return read_total_hits(item)
    except QueryCountError:
        raise ReportRunError(MSEARCH_RESPONSE_INVALID) from None


def _raw_error_type(payload: dict[str, Any]) -> str | None:
    error = payload.get("error")
    if not isinstance(error, dict):
        return None
    error_type = error.get("type")
    return error_type if isinstance(error_type, str) else None


def _safe_backend_error_type(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    # Unknown backend values may embed sensitive content; collapse to a fixed category.
    return normalized if normalized in _ALLOWED_BACKEND_ERROR_TYPES else "other"


def _build_archive(
    definition: ReportDefinition,
    request: ReportRequest,
    counts_by_partition: Mapping[str, dict[tuple[int, int], int]],
) -> tuple[bytes, int]:
    buffer = io.BytesIO()
    used_names: set[str] = set()
    uncompressed_bytes = 0
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        # Preserve request order so partition CSVs appear in the caller-supplied sequence.
        for partition_value in request.partition_values:
            row_counts = _row_identity_counts(definition, counts_by_partition[partition_value])
            csv_bytes = format_report_csv_bytes(definition, row_counts)
            uncompressed_bytes += len(csv_bytes)
            if uncompressed_bytes > MAX_ARCHIVE_BYTES:
                raise ReportRunError(ARCHIVE_TOO_LARGE)
            info = zipfile.ZipInfo(_unique_csv_name(partition_value, used_names))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, csv_bytes)
    data = buffer.getvalue()
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ReportRunError(ARCHIVE_TOO_LARGE)
    return data, len(data)


def _row_identity_counts(
    definition: ReportDefinition,
    counts: Mapping[tuple[int, int], int],
) -> dict[str, int | None]:
    """Map every row's identity key to its count, or ``None`` for a placeholder row.

    Counts are keyed by :func:`row_count_key` (``"S<section seq>:R<row seq>"``) rather than
    by label so two sections can reuse the same label without collision. A placeholder row
    (null query) is never counted, so it maps to ``None``. Every implemented row must have a
    produced count; a missing one is a run integrity failure.
    """
    identity_counts: dict[str, int | None] = {}
    for section in definition.sections:
        for row in section.rows:
            key = row_count_key(section.seq, row.seq)
            if row.query is None:
                identity_counts[key] = None
                continue
            identity = (section.seq, row.seq)
            if identity not in counts:
                raise ReportRunError(COUNT_MISSING)
            identity_counts[key] = counts[identity]
    return identity_counts


def _aggregate_row_counts(
    definition: ReportDefinition,
    counts_by_partition: Mapping[str, dict[tuple[int, int], int]],
) -> dict[str, int | None]:
    """Sum each implemented row across every partition, keyed by row identity.

    Counts are keyed by :func:`row_count_key` so duplicate labels across sections never
    collapse together. A placeholder row (null query) stays ``None`` and is never summed; an
    implemented row that counted zero everywhere aggregates to zero.
    """
    totals: dict[str, int | None] = {}
    for section in definition.sections:
        for row in section.rows:
            key = row_count_key(section.seq, row.seq)
            if row.query is None:
                totals[key] = None
            else:
                totals.setdefault(key, 0)
    for counts in counts_by_partition.values():
        for key, value in _row_identity_counts(definition, counts).items():
            if value is None:
                continue
            current = totals[key]
            totals[key] = (current or 0) + value
    return totals


def _row_execution_counts(definition: ReportDefinition) -> tuple[int, int]:
    """Count unique definition rows that are implemented vs. placeholders.

    Returns ``(rows_executed, placeholders_skipped)`` over the definition's rows -- one count
    per row regardless of partition count -- so a run records how many rows carried a real
    query and how many were skipped as placeholders.
    """
    executed = 0
    placeholders = 0
    for section in definition.sections:
        for row in section.rows:
            if row.query is None:
                placeholders += 1
            else:
                executed += 1
    return executed, placeholders


def _row_count_attribute(count: int | None) -> dict[str, Any]:
    """Encode one aggregate row count as a DynamoDB attribute: NULL for a placeholder."""
    if count is None:
        return {"NULL": True}
    return {"N": str(count)}


def _unique_csv_name(partition_value: str, used_names: set[str]) -> str:
    base = _sanitize_uid(partition_value)
    candidate = f"{base}.csv"
    index = 1
    while candidate in used_names:
        candidate = f"{base}-{index}.csv"
        index += 1
    used_names.add(candidate)
    return candidate


def _sanitize_uid(value: str) -> str:
    cleaned = _UID_UNSAFE.sub("_", value)[:MAX_UID_FILENAME_LENGTH]
    return cleaned or "partition"


def _archive_key(prefix: str, request: ReportRequest) -> str:
    return f"{prefix}/{_sanitize_uid(request.report_id)}/{_sanitize_uid(request.run_id)}.zip"


def _log_event(event: str, **fields: Any) -> str:
    payload = {"event": event, "timestamp": _now_iso(), **fields}
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class SignedMsearchTransport:
    """SigV4 HTTP transport for ``_msearch`` using botocore in the Lambda runtime."""

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
        # _msearch requires NDJSON; other paths use JSON. SigV4 signs the exact bytes sent.
        content_type = "application/x-ndjson" if path.endswith("/_msearch") else "application/json"
        headers = {
            "Content-Type": content_type,
            "x-amz-content-sha256": hashlib.sha256(body or b"").hexdigest(),
        }
        credentials = Session().get_credentials()
        if credentials is None:
            raise ReportRunError(CREDENTIALS_UNAVAILABLE)
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
            with urllib.request.urlopen(  # noqa: S310
                http_request, timeout=REQUEST_TIMEOUT_SECONDS
            ) as response:
                payload = response.read()
                return response.status, json.loads(payload) if payload else {}
        except urllib.error.HTTPError as error:
            payload = error.read()
            return error.code, json.loads(payload) if payload else {}


class S3ReportSink:
    """Upload the completed archive to S3 and return its durable object version id."""

    def __init__(self, client: Any, *, bucket: str) -> None:
        self._s3 = client
        self._bucket = bucket

    def put_report(self, key: str, body: bytes) -> str:
        try:
            response = self._s3.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                ContentType="application/zip",
            )
        except Exception:
            raise ReportRunError(REPORT_UPLOAD_FAILED) from None
        version = response.get("VersionId") if isinstance(response, dict) else None
        return version if isinstance(version, str) else ""


class LoggingRunProgressStore:
    """Emit bounded, sanitized run lifecycle events as structured logs.

    Logs are keyed by run id and never include a facility identifier or storage
    location, so a fifteen-minute run cannot leak partition values or S3 keys.
    """

    def record_progress(self, run_id: str, completed: int, total: int) -> None:
        LOGGER.info(
            _log_event(
                "report_run_progress",
                runId=run_id,
                completedPartitions=completed,
                totalPartitions=total,
            )
        )

    def record_success(
        self,
        run_id: str,
        _zip_s3_key: str,
        _version: str,
        _row_counts: Mapping[str, int | None],
        _rows_executed: int,
        _placeholders_skipped: int,
    ) -> None:
        # The archive key, version, aggregate row counts, and execution/placeholder tallies
        # are all omitted from logs.
        LOGGER.info(_log_event("report_run_status", runId=run_id, status="complete"))

    def record_failure(self, run_id: str, _failing_partition: str) -> None:
        # The failing partition is a facility identifier and is never written to logs.
        LOGGER.error(_log_event("report_run_status", runId=run_id, status="failed"))


class DynamoRunProgressStore:
    """Persist run lifecycle transitions to the Reports runs table, keyed by run id.

    Progress increments ``completedPartitions``; success marks the run ``complete`` with
    the archive key, its object version, the aggregate per-row row counts (keyed by row
    identity), and a finish timestamp; failure marks the run ``failed`` with the failing
    partition and a finish timestamp. The failing partition is stored for operators but is
    never emitted to logs, and the sanitized status logs never include a facility
    identifier or storage location.
    """

    def __init__(self, client: Any, *, table_name: str) -> None:
        self._dynamo = client
        self._table = table_name

    def record_progress(self, run_id: str, completed: int, total: int) -> None:
        self._update(
            run_id,
            "SET completedPartitions = :completed, totalPartitions = :total",
            {":completed": {"N": str(completed)}, ":total": {"N": str(total)}},
        )
        LOGGER.info(
            _log_event(
                "report_run_progress",
                runId=run_id,
                completedPartitions=completed,
                totalPartitions=total,
            )
        )

    def record_success(
        self,
        run_id: str,
        zip_s3_key: str,
        version: str,
        row_counts: Mapping[str, int | None],
        rows_executed: int,
        placeholders_skipped: int,
    ) -> None:
        names = {"#status": "status"}
        values: dict[str, Any] = {
            ":status": {"S": "complete"},
            ":zipS3Key": {"S": zip_s3_key},
            ":finishedAt": {"S": _now_iso()},
            # Aggregate row counts are stored as a DynamoDB map so the Reports grid can read
            # the most-recent run's totals without re-counting. An implemented row stores a
            # numeric (N) value; a placeholder row stores NULL so a blank count round-trips.
            ":rowCounts": {
                "M": {label: _row_count_attribute(count) for label, count in row_counts.items()}
            },
            # Unique-row tallies: how many rows carried a real query versus were skipped as
            # placeholders. These count definition rows once, independent of partition count.
            ":rowsExecuted": {"N": str(rows_executed)},
            ":placeholdersSkipped": {"N": str(placeholders_skipped)},
        }
        set_parts = [
            "#status = :status",
            "zipS3Key = :zipS3Key",
            "finishedAt = :finishedAt",
            "rowCounts = :rowCounts",
            "rowsExecuted = :rowsExecuted",
            "placeholdersSkipped = :placeholdersSkipped",
        ]
        if version:
            # ``version`` is a DynamoDB reserved word, so bind it through a name alias.
            names["#version"] = "version"
            set_parts.append("#version = :version")
            values[":version"] = {"S": version}
        self._update(run_id, "SET " + ", ".join(set_parts), values, names=names)
        LOGGER.info(_log_event("report_run_status", runId=run_id, status="complete"))

    def record_failure(self, run_id: str, failing_partition: str) -> None:
        self._update(
            run_id,
            "SET #status = :status, failingPartition = :failingPartition, finishedAt = :finishedAt",
            {
                ":status": {"S": "failed"},
                ":failingPartition": {"S": failing_partition},
                ":finishedAt": {"S": _now_iso()},
            },
            names={"#status": "status"},
        )
        # The failing partition is persisted above but deliberately kept out of logs.
        LOGGER.error(_log_event("report_run_status", runId=run_id, status="failed"))

    def _update(
        self,
        run_id: str,
        expression: str,
        values: dict[str, Any],
        *,
        names: dict[str, str] | None = None,
    ) -> None:
        request: dict[str, Any] = {
            "TableName": self._table,
            "Key": {"runId": {"S": run_id}},
            "UpdateExpression": expression,
            "ExpressionAttributeValues": values,
            "ConditionExpression": "attribute_exists(runId)",
        }
        if names is not None:
            request["ExpressionAttributeNames"] = names
        try:
            self._dynamo.update_item(**request)
        except Exception:
            raise ReportRunError(RUN_PROGRESS_UPDATE_FAILED) from None


def _catalog_definition_provider(client: Any, *, table_name: str) -> DefinitionProvider:
    """Load definitions from the row-granular Reports catalog table in DynamoDB.

    Each report is assembled by the catalog with a single ``Query`` on its partition key
    and returned as a clean definition mapping, which is validated through the definition
    core. Any read or validation failure is collapsed to a sanitized load error so no
    DynamoDB or clinical detail escapes.
    """
    catalog = ReportCatalog(client, table_name=table_name)

    def load(report_id: str) -> ReportDefinition:
        try:
            definition_mapping = catalog.get_report(report_id)["definition"]
        except Exception:
            raise ReportRunError(DEFINITION_LOAD_FAILED) from None
        try:
            return load_report_definition(definition_mapping)
        except ReportDefinitionError:
            raise ReportRunError(DEFINITION_LOAD_FAILED) from None

    return load


def _aws_client(service: str) -> Any:
    import boto3  # type: ignore[import-not-found]

    return boto3.client(service)


_RUNTIME_RUNNER: ReportRunner | None = None


def _runtime_runner() -> ReportRunner:
    global _RUNTIME_RUNNER
    if _RUNTIME_RUNNER is None:
        region = os.getenv("OPENSEARCH_REGION") or os.environ["AWS_REGION"]
        transport = SignedMsearchTransport(
            endpoint=os.environ["OPENSEARCH_ENDPOINT"],
            region=region,
            service=os.getenv("OPENSEARCH_SERVICE", "aoss"),
        )
        sink = S3ReportSink(_aws_client("s3"), bucket=os.environ["REPORT_BUCKET"])
        dynamodb = _aws_client("dynamodb")
        provider = _catalog_definition_provider(
            dynamodb,
            table_name=os.environ["REPORT_CATALOG_TABLE"],
        )
        progress = DynamoRunProgressStore(
            dynamodb,
            table_name=os.environ["RUNS_TABLE"],
        )
        _RUNTIME_RUNNER = ReportRunner(transport, sink, progress, provider)
    return _RUNTIME_RUNNER


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Run one report asynchronously without exposing sensitive failure detail."""
    runner = _runtime_runner()
    try:
        output = runner.run(event)
    except ReportRequestError as error:
        LOGGER.warning(_log_event("report_request_invalid", code=error.code))
        return {"status": "invalid", "error": error.code}
    except ReportRunError as error:
        LOGGER.error(  # noqa: TRY400 - tracebacks could contain SDK request detail
            _log_event(
                "report_run_failed",
                errorType=type(error).__name__,
                httpStatus=error.http_status,
                backendErrorType=error.backend_error_type,
            )
        )
        return {"status": "failed", "error": "report_run_failed"}
    return {
        "status": "succeeded",
        "reportId": output.report_id,
        "runId": output.run_id,
        "zipS3Key": output.key,
        "version": output.version,
        "partitionCount": output.partition_count,
        "byteSize": output.byte_size,
    }
