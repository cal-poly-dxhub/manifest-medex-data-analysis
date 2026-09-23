"""Integration-style checks that the Reports API and worker share one contract.

These exercise the seam between :mod:`src.report_runs` (the API that mints a run and
dispatches the worker) and :mod:`src.report_runner` (the worker that validates the event
and drives progress). The dispatched payload must be accepted verbatim by the worker's
validator, and the worker's progress transitions must be keyed by the run id.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.report_definition import ReportDefinition, load_report_definition
from src.report_runner import MAX_PARTITION_VALUES, ReportRunner, parse_report_request
from src.report_runs import ReportRuns

FROM_TIME = "2026-01-01T00:00:00+00:00"
TO_TIME = "2026-02-01T00:00:00+00:00"
REPORT_ID = "rpt"
RUN_ID = "run0001"
PARTITIONS = ["facility-a", "facility-b"]


class _FakeDynamo:
    def create_table(self, **_kwargs: Any) -> dict[str, Any]:  # pragma: no cover - unused
        return {}

    def put_item(self, **_kwargs: Any) -> dict[str, Any]:
        return {}

    def get_item(self, **_kwargs: Any) -> dict[str, Any]:  # pragma: no cover - unused
        return {}

    def query(self, **_kwargs: Any) -> dict[str, Any]:  # pragma: no cover - unused
        return {"Items": []}

    def scan(self, **_kwargs: Any) -> dict[str, Any]:  # pragma: no cover - unused
        return {"Items": []}


class _FakeLambda:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"StatusCode": 202}


class _FakeS3:
    def get_object(self, **_kwargs: Any) -> dict[str, Any]:  # pragma: no cover - unused
        raise AssertionError


class _FakeTransport:
    def __init__(self, responses: list[tuple[int, dict[str, Any]]]) -> None:
        self._responses = responses

    def request(
        self, _method: str, _path: str, _body: bytes | None = None
    ) -> tuple[int, dict[str, Any]]:
        return self._responses.pop(0)


class _FakeSink:
    def put_report(self, _key: str, _body: bytes) -> str:
        return "ver-1"


class _FakeProgress:
    def __init__(self) -> None:
        self.progress: list[tuple[str, int, int]] = []
        self.successes: list[tuple[str, str, str, dict[str, int | None]]] = []
        self.summaries: list[tuple[str, int, int]] = []
        self.failures: list[tuple[str, str]] = []

    def record_progress(self, run_id: str, completed: int, total: int) -> None:
        self.progress.append((run_id, completed, total))

    def record_success(
        self,
        run_id: str,
        zip_s3_key: str,
        version: str,
        row_counts: Mapping[str, int | None],
        rows_executed: int,
        placeholders_skipped: int,
    ) -> None:
        self.successes.append((run_id, zip_s3_key, version, dict(row_counts)))
        self.summaries.append((run_id, rows_executed, placeholders_skipped))

    def record_failure(self, run_id: str, failing_partition: str) -> None:
        self.failures.append((run_id, failing_partition))


def _definition() -> ReportDefinition:
    return load_report_definition(
        {
            "report_id": REPORT_ID,
            "name": "Test Report",
            "description": "test",
            "partition_field": "sourceFacilityId",
            "time_field": "messageTime",
            "sections": [
                {
                    "seq": 1,
                    "name": "SecA",
                    "rows": [
                        {
                            "seq": 1,
                            "label": "L1",
                            "description": "row",
                            "index": "hl7-messages-v1",
                            "query": {"bool": {"filter": [{"term": {"ROOT.PID._present": "1"}}]}},
                        }
                    ],
                }
            ],
        }
    )


def _count_response() -> dict[str, Any]:
    return {"responses": [{"status": 200, "hits": {"total": {"value": 3}}}]}


def test_start_run_payload_is_accepted_by_worker_and_progress_uses_run_id() -> None:
    lambda_client = _FakeLambda()
    runs = ReportRuns(
        _FakeDynamo(),
        lambda_client,
        _FakeS3(),
        table_name="runs",
        worker_function_name="worker",
        output_bucket="bucket",
        run_id_factory=lambda: RUN_ID,
    )

    runs.start_run(REPORT_ID, FROM_TIME, TO_TIME, PARTITIONS, "auth0|caller")

    # The API dispatched exactly one payload.
    import json

    payload = json.loads(lambda_client.calls[0]["Payload"])

    # The worker's validator accepts the dispatched payload verbatim, including run_id.
    request = parse_report_request(payload)
    assert request.run_id == RUN_ID
    assert request.report_id == REPORT_ID
    assert request.partition_values == ("facility-a", "facility-b")

    # Running the worker against that same payload drives run-id-keyed progress.
    transport = _FakeTransport([(200, _count_response()), (200, _count_response())])
    progress = _FakeProgress()
    runner = ReportRunner(transport, _FakeSink(), progress, lambda _report_id: _definition())

    output = runner.run(payload)

    assert output.run_id == RUN_ID
    assert output.key == "outputs/rpt/run0001.zip"
    assert progress.progress == [(RUN_ID, 1, 2), (RUN_ID, 2, 2)]
    assert progress.successes == [(RUN_ID, "outputs/rpt/run0001.zip", "ver-1", {"S1:R1": 6})]
    assert progress.failures == []
    assert progress.failures == []


def test_start_run_payload_at_partition_cap_is_accepted_by_worker() -> None:
    """A run at the 200-facility cap dispatches a payload the worker accepts verbatim."""
    lambda_client = _FakeLambda()
    runs = ReportRuns(
        _FakeDynamo(),
        lambda_client,
        _FakeS3(),
        table_name="runs",
        worker_function_name="worker",
        output_bucket="bucket",
        run_id_factory=lambda: RUN_ID,
    )

    partitions = [f"facility-{index:03d}" for index in range(MAX_PARTITION_VALUES)]
    assert len(partitions) == 200

    runs.start_run(REPORT_ID, FROM_TIME, TO_TIME, partitions, "auth0|caller")

    import json

    payload = json.loads(lambda_client.calls[0]["Payload"])

    request = parse_report_request(payload)
    assert len(request.partition_values) == 200
    assert request.partition_values == tuple(partitions)
