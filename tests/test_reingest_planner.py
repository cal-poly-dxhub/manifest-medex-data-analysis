import json
from typing import Any

import pytest
from src.reingest_jobs import JobProgressStore
from src.reingest_planner import (
    PlannerError,
    ReingestPlanner,
)

JOB_ID = "a" * 32
CLUSTER_ARN = "arn:aws:rds:us-west-2:111111111111:cluster:app"
SECRET_ARN = "arn:aws:secretsmanager:us-west-2:111111111111:secret:app"  # noqa: S105
QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/111111111111/reindex"


def _doc(index: int) -> str:
    return f"{index:064x}"


class FakeDataApi:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.error: Exception | None = None
        self.calls: list[dict[str, Any]] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self._responses.pop(0) if self._responses else {"records": []}


class FakeSqs:
    def __init__(self, *, fail_ids: set[str] | None = None, error: Exception | None = None) -> None:
        self._fail_ids = fail_ids or set()
        self._error = error
        self.calls: list[dict[str, Any]] = []

    def send_message_batch(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        failed = [
            {"Id": entry["Id"]} for entry in kwargs["Entries"] if entry["Id"] in self._fail_ids
        ]
        response: dict[str, Any] = {"Successful": []}
        if failed:
            response["Failed"] = failed
        return response


class RecordingProgress(JobProgressStore):
    def __init__(self) -> None:
        self.running: list[str] = []
        self.enqueued: list[tuple[str, int]] = []
        self.enqueue_complete: list[str] = []
        self.failed: list[str] = []
        self.settled: list[str] = []
        self.settle_result = False

    def mark_running(self, job_id: str) -> None:
        self.running.append(job_id)

    def add_enqueued(self, job_id: str, delta: int) -> None:
        self.enqueued.append((job_id, delta))

    def mark_enqueue_complete(self, job_id: str) -> None:
        self.enqueue_complete.append(job_id)

    def mark_failed(self, job_id: str) -> None:
        self.failed.append(job_id)

    def complete_if_settled(self, job_id: str) -> bool:
        self.settled.append(job_id)
        return self.settle_result


def _rows(*docs: str) -> dict[str, Any]:
    return {
        "records": [
            [
                {"stringValue": doc},
                {"stringValue": f"s3://parsed/{doc}.json"},
                {"stringValue": "ccda"},
            ]
            for doc in docs
        ]
    }


def _planner(
    data_api: FakeDataApi,
    sqs: FakeSqs,
    progress: RecordingProgress,
    *,
    page_size: int = 500,
    id_batch_size: int = 100,
    batch_delay_seconds: float = 0.0,
    max_documents: int = 100_000,
    sleeps: list[float] | None = None,
) -> ReingestPlanner:
    def sleep(seconds: float) -> None:
        if sleeps is not None:
            sleeps.append(seconds)

    return ReingestPlanner(
        data_api,
        sqs,
        progress,
        cluster_arn=CLUSTER_ARN,
        secret_arn=SECRET_ARN,
        database="app_db",
        table_name="document_metadata",
        queue_url=QUEUE_URL,
        page_size=page_size,
        id_batch_size=id_batch_size,
        batch_delay_seconds=batch_delay_seconds,
        max_documents=max_documents,
        sleep=sleep,
    )


# --- event parsing ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        {},
        {"jobId": "short", "mode": "sql", "sql": "SELECT document_id FROM document_metadata"},
        {"jobId": JOB_ID, "mode": "unknown"},
        {"jobId": JOB_ID, "mode": "sql"},
        {"jobId": JOB_ID, "mode": "ids", "documentIds": ["bad"]},
    ],
)
def test_run_rejects_invalid_event(event: dict[str, Any]) -> None:
    planner = _planner(FakeDataApi([]), FakeSqs(), RecordingProgress())
    with pytest.raises(PlannerError):
        planner.run(event)


# --- SQL keyset walk -------------------------------------------------------------------


def test_sql_mode_keyset_pages_without_offset() -> None:
    page_one = _rows(_doc(1), _doc(2))
    page_two = _rows(_doc(3))
    data_api = FakeDataApi([page_one, page_two])
    sqs = FakeSqs()
    progress = RecordingProgress()
    planner = _planner(data_api, sqs, progress, page_size=2)

    result = planner.run(
        {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
    )

    assert result == {"jobId": JOB_ID, "enqueued": 3}
    # Page one filled the page size so a second page was fetched; page two was short and
    # ended the walk, so exactly two Data API reads occurred with no OFFSET.
    assert len(data_api.calls) == 2
    for call in data_api.calls:
        assert "OFFSET" not in call["sql"].upper()
        assert "dm.document_id > :cursor" in call["sql"]
        names = {param["name"] for param in call["parameters"]}
        assert names == {"cursor", "page_size"}
    # The cursor advances by keyset to the last document id of the previous page.
    assert _cursor(data_api.calls[0]) == ""
    assert _cursor(data_api.calls[1]) == _doc(2)
    assert progress.running == [JOB_ID]
    assert progress.enqueue_complete == [JOB_ID]
    assert progress.settled == [JOB_ID]


def test_sql_mode_stops_when_page_not_full() -> None:
    data_api = FakeDataApi([_rows(_doc(1))])
    planner = _planner(data_api, FakeSqs(), RecordingProgress(), page_size=10)
    result = planner.run(
        {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
    )
    assert result["enqueued"] == 1
    assert len(data_api.calls) == 1


def test_sql_mode_embeds_guarded_selection() -> None:
    data_api = FakeDataApi([_rows()])
    planner = _planner(data_api, FakeSqs(), RecordingProgress())
    planner.run(
        {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
    )
    sql = data_api.calls[0]["sql"]
    assert "SELECT DISTINCT dm.document_id, dm.parsed_s3_uri, dm.source_format" in sql
    assert "JOIN (" in sql
    assert "selection ON selection.document_id = dm.document_id" in sql


def test_sql_mode_revalidates_selection() -> None:
    planner = _planner(FakeDataApi([]), FakeSqs(), RecordingProgress())
    with pytest.raises(PlannerError):
        planner.run({"jobId": JOB_ID, "mode": "sql", "sql": "DROP TABLE document_metadata"})


# --- ID batch walk ---------------------------------------------------------------------


def test_ids_mode_resolves_parameterized_batches() -> None:
    ids = [_doc(i) for i in range(3)]
    data_api = FakeDataApi([_rows(*ids)])
    sqs = FakeSqs()
    progress = RecordingProgress()
    planner = _planner(data_api, sqs, progress, id_batch_size=100)

    result = planner.run({"jobId": JOB_ID, "mode": "ids", "documentIds": ids})

    assert result["enqueued"] == 3
    call = data_api.calls[0]
    assert "WHERE document_id IN (:id0, :id1, :id2)" in call["sql"]
    params = {param["name"]: param["value"]["stringValue"] for param in call["parameters"]}
    assert params == {"id0": ids[0], "id1": ids[1], "id2": ids[2]}


def test_ids_mode_splits_into_batches() -> None:
    ids = [_doc(i) for i in range(5)]
    data_api = FakeDataApi([_rows(*ids[:2]), _rows(*ids[2:4]), _rows(ids[4])])
    planner = _planner(data_api, FakeSqs(), RecordingProgress(), id_batch_size=2)
    result = planner.run({"jobId": JOB_ID, "mode": "ids", "documentIds": ids})
    assert result["enqueued"] == 5
    assert len(data_api.calls) == 3
    assert "IN (:id0, :id1)" in data_api.calls[0]["sql"]
    assert "IN (:id0)" in data_api.calls[2]["sql"]


# --- enqueue batching / payload / rate -------------------------------------------------


def test_enqueue_batches_of_ten_with_exact_payload() -> None:
    docs = [_doc(i) for i in range(23)]
    data_api = FakeDataApi([_rows(*docs)])
    sqs = FakeSqs()
    progress = RecordingProgress()
    planner = _planner(data_api, sqs, progress, page_size=100)

    planner.run({"jobId": JOB_ID, "mode": "ids", "documentIds": docs})

    # 23 documents fan out as batches of at most ten.
    assert [len(call["Entries"]) for call in sqs.calls] == [10, 10, 3]
    assert progress.enqueued == [(JOB_ID, 10), (JOB_ID, 10), (JOB_ID, 3)]
    first_entry = sqs.calls[0]["Entries"][0]
    body = json.loads(first_entry["MessageBody"])
    assert body == {
        "jobId": JOB_ID,
        "documentId": docs[0],
        "parsedS3Uri": f"s3://parsed/{docs[0]}.json",
        "sourceFormat": "ccda",
    }
    # Entry ids are unique within a batch.
    assert [entry["Id"] for entry in sqs.calls[0]["Entries"]] == [str(i) for i in range(10)]


def test_enqueue_delay_applied_between_batches() -> None:
    docs = [_doc(i) for i in range(15)]
    data_api = FakeDataApi([_rows(*docs)])
    sleeps: list[float] = []
    planner = _planner(
        data_api,
        FakeSqs(),
        RecordingProgress(),
        page_size=100,
        batch_delay_seconds=0.05,
        sleeps=sleeps,
    )
    planner.run({"jobId": JOB_ID, "mode": "ids", "documentIds": docs})
    assert sleeps == [0.05, 0.05]


def test_zero_documents_completes_immediately() -> None:
    data_api = FakeDataApi([_rows()])
    sqs = FakeSqs()
    progress = RecordingProgress()
    planner = _planner(data_api, sqs, progress)
    result = planner.run(
        {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
    )
    assert result["enqueued"] == 0
    assert sqs.calls == []
    assert progress.enqueued == []
    assert progress.enqueue_complete == [JOB_ID]
    assert progress.settled == [JOB_ID]


# --- limits and failures ---------------------------------------------------------------


def test_max_documents_enforced() -> None:
    docs = [_doc(i) for i in range(5)]
    data_api = FakeDataApi([_rows(*docs)])
    progress = RecordingProgress()
    planner = _planner(data_api, FakeSqs(), progress, page_size=100, max_documents=3)
    with pytest.raises(PlannerError):
        planner.run({"jobId": JOB_ID, "mode": "ids", "documentIds": docs})
    assert progress.failed == [JOB_ID]


def test_metadata_read_failure_marks_failed() -> None:
    data_api = FakeDataApi([])
    data_api.error = RuntimeError("boom")
    progress = RecordingProgress()
    planner = _planner(data_api, FakeSqs(), progress)
    with pytest.raises(PlannerError):
        planner.run(
            {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
        )
    assert progress.failed == [JOB_ID]
    assert progress.enqueue_complete == []


def test_sqs_failure_marks_failed() -> None:
    docs = [_doc(1)]
    data_api = FakeDataApi([_rows(*docs)])
    progress = RecordingProgress()
    planner = _planner(data_api, FakeSqs(fail_ids={"0"}), progress)
    with pytest.raises(PlannerError):
        planner.run({"jobId": JOB_ID, "mode": "ids", "documentIds": docs})
    assert progress.failed == [JOB_ID]


def test_malformed_metadata_record_marks_failed() -> None:
    data_api = FakeDataApi([{"records": [[{"stringValue": "only-one-column"}]]}])
    progress = RecordingProgress()
    planner = _planner(data_api, FakeSqs(), progress)
    with pytest.raises(PlannerError):
        planner.run(
            {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
        )
    assert progress.failed == [JOB_ID]


def test_constructor_rejects_bad_identifier() -> None:
    with pytest.raises(ValueError, match="configuration"):
        ReingestPlanner(
            FakeDataApi([]),
            FakeSqs(),
            RecordingProgress(),
            cluster_arn=CLUSTER_ARN,
            secret_arn=SECRET_ARN,
            database="app_db",
            table_name="Bad-Table",
            queue_url=QUEUE_URL,
        )


def _cursor(call: dict[str, Any]) -> str:
    for param in call["parameters"]:
        if param["name"] == "cursor":
            return str(param["value"]["stringValue"])
    pytest.fail("cursor parameter missing")


def test_run_rejects_non_mapping_event() -> None:
    planner = _planner(FakeDataApi([]), FakeSqs(), RecordingProgress())
    with pytest.raises(PlannerError):
        planner.run([])  # type: ignore[arg-type]


def test_sql_mode_probes_after_two_full_pages() -> None:
    # Two page-size-filling pages force a third read that returns empty and ends the walk.
    data_api = FakeDataApi([_rows(_doc(1), _doc(2)), _rows(_doc(3), _doc(4)), _rows()])
    planner = _planner(data_api, FakeSqs(), RecordingProgress(), page_size=2)
    result = planner.run(
        {"jobId": JOB_ID, "mode": "sql", "sql": "SELECT document_id FROM document_metadata"}
    )
    assert result["enqueued"] == 4
    assert len(data_api.calls) == 3
    assert _cursor(data_api.calls[2]) == _doc(4)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"queue_url": ""},
        {"page_size": 0},
        {"id_batch_size": 0},
        {"batch_delay_seconds": -1.0},
        {"max_documents": 0},
    ],
)
def test_constructor_rejects_bad_bounds(kwargs: dict[str, Any]) -> None:
    base: dict[str, Any] = {
        "cluster_arn": CLUSTER_ARN,
        "secret_arn": SECRET_ARN,
        "database": "app_db",
        "table_name": "document_metadata",
        "queue_url": QUEUE_URL,
    }
    base.update(kwargs)
    with pytest.raises(ValueError, match="configuration"):
        ReingestPlanner(FakeDataApi([]), FakeSqs(), RecordingProgress(), **base)
