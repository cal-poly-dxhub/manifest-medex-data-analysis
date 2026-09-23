import json
from io import BytesIO
from typing import Any

import pytest
from src.report_runs import (
    INVALID_CALLER,
    INVALID_REPORT_ID,
    INVALID_RUN_ID,
    OUTPUT_TOO_LARGE,
    RUN_NOT_FOUND,
    RUN_NOT_READY,
    DownloadResult,
    ReportRuns,
    RunError,
    RunRequestError,
)

CALLER = "auth0|run-caller"
REPORT_ID = "p4p-prototype"
RUN_ID = "run0001"
FROM_TIME = "2026-01-01T00:00:00+00:00"
TO_TIME = "2026-02-01T00:00:00+00:00"
PARTITIONS = ["facility-a", "facility-b"]


class FakeClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeDynamo:
    def __init__(
        self,
        *,
        get_item_response: dict[str, Any] | None = None,
        query_pages: list[dict[str, Any]] | None = None,
        scan_pages: list[dict[str, Any]] | None = None,
        create_error: Exception | None = None,
        put_error: Exception | None = None,
        get_error: Exception | None = None,
    ) -> None:
        self._get_item_response = get_item_response or {}
        self._query_pages = query_pages or [{"Items": []}]
        self._scan_pages = scan_pages or [{"Items": []}]
        self._create_error = create_error
        self._put_error = put_error
        self._get_error = get_error
        self.create_calls: list[dict[str, Any]] = []
        self.put_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []
        self.scan_calls: list[dict[str, Any]] = []

    def create_table(self, **kwargs: Any) -> dict[str, Any]:
        self.create_calls.append(kwargs)
        if self._create_error is not None:
            raise self._create_error
        return {}

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if self._put_error is not None:
            raise self._put_error
        return {}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        if self._get_error is not None:
            raise self._get_error
        return self._get_item_response

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls.append(kwargs)
        return self._query_pages[min(len(self.query_calls) - 1, len(self._query_pages) - 1)]

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls.append(kwargs)
        return self._scan_pages[min(len(self.scan_calls) - 1, len(self._scan_pages) - 1)]


class FakeLambda:
    def __init__(
        self,
        *,
        status_code: int = 202,
        error: Exception | None = None,
    ) -> None:
        self._status_code = status_code
        self._error = error
        self.calls: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return {"StatusCode": self._status_code}


class FakeS3:
    def __init__(
        self,
        *,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._response = response
        self._error = error
        self.calls: list[dict[str, Any]] = []

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        assert self._response is not None
        return self._response


def _runs(
    dynamo: FakeDynamo,
    lambda_client: FakeLambda | None = None,
    s3: FakeS3 | None = None,
    **kwargs: Any,
) -> ReportRuns:
    return ReportRuns(
        dynamo,
        lambda_client or FakeLambda(),
        s3 or FakeS3(),
        table_name="report_runs",
        worker_function_name="report-worker",
        output_bucket="report-output",
        run_id_factory=lambda: RUN_ID,
        **kwargs,
    )


def _run_item(
    status: str = "complete",
    *,
    run_id: str = RUN_ID,
    started_at: str = "2026-08-31T10:00:00+00:00",
    completed: str = "2",
    total: str = "2",
    **extra: Any,
) -> dict[str, Any]:
    item = {
        "runId": {"S": run_id},
        "reportId": {"S": REPORT_ID},
        "status": {"S": status},
        "requestedBy": {"S": CALLER},
        "startedAt": {"S": started_at},
        "fromTime": {"S": FROM_TIME},
        "toTime": {"S": TO_TIME},
        "partitionValues": {"L": [{"S": value} for value in PARTITIONS]},
        "completedPartitions": {"N": completed},
        "totalPartitions": {"N": total},
    }
    item.update(extra)
    return item


# --------------------------------------------------------------------------- #
# ensure_table
# --------------------------------------------------------------------------- #


def test_ensure_table_creates_key_schema_only_without_index() -> None:
    dynamo = FakeDynamo()

    _runs(dynamo).ensure_table()

    request = dynamo.create_calls[0]
    assert request["KeySchema"] == [{"AttributeName": "runId", "KeyType": "HASH"}]
    assert "GlobalSecondaryIndexes" not in request
    assert request["AttributeDefinitions"] == [{"AttributeName": "runId", "AttributeType": "S"}]


def test_ensure_table_adds_report_gsi_when_configured() -> None:
    dynamo = FakeDynamo()

    _runs(dynamo, report_index_name="report-index").ensure_table()

    gsi = dynamo.create_calls[0]["GlobalSecondaryIndexes"][0]
    assert gsi["IndexName"] == "report-index"
    assert gsi["KeySchema"] == [
        {"AttributeName": "reportId", "KeyType": "HASH"},
        {"AttributeName": "startedAt", "KeyType": "RANGE"},
    ]


def test_ensure_table_is_idempotent_when_table_exists() -> None:
    dynamo = FakeDynamo(create_error=FakeClientError("ResourceInUseException"))

    _runs(dynamo).ensure_table()  # does not raise


def test_ensure_table_other_error_is_sanitized() -> None:
    dynamo = FakeDynamo(create_error=FakeClientError("LimitExceededException"))

    with pytest.raises(RunError, match="table creation failed"):
        _runs(dynamo).ensure_table()


def test_ensure_table_non_client_error_is_sanitized() -> None:
    dynamo = FakeDynamo(create_error=RuntimeError("no response attribute"))

    with pytest.raises(RunError, match="table creation failed"):
        _runs(dynamo).ensure_table()


def test_ensure_table_error_with_non_string_code_is_sanitized() -> None:
    class WeirdError(Exception):
        def __init__(self) -> None:
            super().__init__("weird")
            self.response = {"Error": {"Code": 123}}

    dynamo = FakeDynamo(create_error=WeirdError())

    with pytest.raises(RunError, match="table creation failed"):
        _runs(dynamo).ensure_table()


def test_ensure_table_error_with_non_dict_error_payload_is_sanitized() -> None:
    class WeirdError(Exception):
        def __init__(self) -> None:
            super().__init__("weird")
            self.response = {"Error": "not-a-dict"}

    dynamo = FakeDynamo(create_error=WeirdError())

    with pytest.raises(RunError, match="table creation failed"):
        _runs(dynamo).ensure_table()


# --------------------------------------------------------------------------- #
# start_run
# --------------------------------------------------------------------------- #


def test_start_run_records_running_and_dispatches_async() -> None:
    dynamo = FakeDynamo()
    lambda_client = FakeLambda()

    result = _runs(dynamo, lambda_client).start_run(
        REPORT_ID, FROM_TIME, TO_TIME, PARTITIONS, CALLER
    )

    assert result["runId"] == RUN_ID
    assert result["reportId"] == REPORT_ID
    assert result["status"] == "running"
    assert result["requestedBy"] == CALLER
    assert result["params"] == {"from": FROM_TIME, "to": TO_TIME, "partitionValues": PARTITIONS}
    assert result["progress"] == {"completedPartitions": 0, "totalPartitions": 2}
    assert result["downloadReady"] is False

    item = dynamo.put_calls[0]["Item"]
    assert item["status"] == {"S": "running"}
    assert item["completedPartitions"] == {"N": "0"}
    assert item["totalPartitions"] == {"N": "2"}
    assert item["requestedBy"] == {"S": CALLER}
    assert item["partitionValues"] == {"L": [{"S": "facility-a"}, {"S": "facility-b"}]}
    assert dynamo.put_calls[0]["ConditionExpression"] == "attribute_not_exists(runId)"

    invoke = lambda_client.calls[0]
    assert invoke["InvocationType"] == "Event"
    assert invoke["FunctionName"] == "report-worker"
    payload = json.loads(invoke["Payload"])
    assert payload == {
        "report_id": REPORT_ID,
        "run_id": RUN_ID,
        "from": FROM_TIME,
        "to": TO_TIME,
        "partition_values": PARTITIONS,
    }


def test_start_run_dispatch_failure_is_sanitized() -> None:
    lambda_client = FakeLambda(error=FakeClientError("ServiceException"))

    with pytest.raises(RunError, match="dispatch failed"):
        _runs(FakeDynamo(), lambda_client).start_run(
            REPORT_ID, FROM_TIME, TO_TIME, PARTITIONS, CALLER
        )


def test_start_run_rejects_non_accepted_invoke_status() -> None:
    lambda_client = FakeLambda(status_code=500)

    with pytest.raises(RunError, match="dispatch failed"):
        _runs(FakeDynamo(), lambda_client).start_run(
            REPORT_ID, FROM_TIME, TO_TIME, PARTITIONS, CALLER
        )


def test_start_run_put_failure_is_sanitized_and_skips_dispatch() -> None:
    dynamo = FakeDynamo(put_error=FakeClientError("ConditionalCheckFailedException"))
    lambda_client = FakeLambda()

    with pytest.raises(RunError, match="creation failed"):
        _runs(dynamo, lambda_client).start_run(REPORT_ID, FROM_TIME, TO_TIME, PARTITIONS, CALLER)

    assert lambda_client.calls == []


@pytest.mark.parametrize("report_id", ["Bad", "", "with space"])
def test_start_run_rejects_invalid_report_id(report_id: str) -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo()).start_run(report_id, FROM_TIME, TO_TIME, PARTITIONS, CALLER)

    assert captured.value.code == INVALID_REPORT_ID


@pytest.mark.parametrize("caller", ["", "x" * 129])
def test_start_run_rejects_invalid_caller(caller: str) -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo()).start_run(REPORT_ID, FROM_TIME, TO_TIME, PARTITIONS, caller)

    assert captured.value.code == INVALID_CALLER


def test_start_run_rejects_naive_from_via_worker_contract() -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo()).start_run(REPORT_ID, "2026-01-01T00:00:00", TO_TIME, PARTITIONS, CALLER)

    assert captured.value.status_code == 400
    assert captured.value.code == "invalid_from"


def test_start_run_rejects_inverted_time_range() -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo()).start_run(REPORT_ID, TO_TIME, FROM_TIME, PARTITIONS, CALLER)

    assert captured.value.code == "invalid_time_range"


@pytest.mark.parametrize(
    ("partitions", "code"),
    [
        ([], "invalid_partition_values"),
        (["a"] * 201, "invalid_partition_values"),
        (["facility-a", "facility-a"], "duplicate_partition_values"),
    ],
)
def test_start_run_rejects_invalid_partitions(partitions: list[str], code: str) -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo()).start_run(REPORT_ID, FROM_TIME, TO_TIME, partitions, CALLER)

    assert captured.value.code == code


def test_start_run_accepts_maximum_partition_count() -> None:
    partitions = [f"facility-{index:03d}" for index in range(200)]
    dynamo = FakeDynamo()

    result = _runs(dynamo).start_run(REPORT_ID, FROM_TIME, TO_TIME, partitions, CALLER)

    assert result["progress"]["totalPartitions"] == 200
    assert dynamo.put_calls[0]["Item"]["totalPartitions"] == {"N": "200"}


# --------------------------------------------------------------------------- #
# get_run_status
# --------------------------------------------------------------------------- #


def test_get_run_status_projects_running_run_without_download() -> None:
    dynamo = FakeDynamo(get_item_response={"Item": _run_item(status="running", completed="1")})

    status = _runs(dynamo).get_run_status(RUN_ID)

    assert status["status"] == "running"
    assert status["progress"] == {"completedPartitions": 1, "totalPartitions": 2}
    assert status["params"] == {"from": FROM_TIME, "to": TO_TIME, "partitionValues": PARTITIONS}
    assert status["downloadReady"] is False
    assert "finishedAt" not in status
    assert "failingPartition" not in status


def test_get_run_status_projects_complete_run_as_downloadable() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    assert status["status"] == "complete"
    assert status["downloadReady"] is True
    assert status["finishedAt"] == "2026-08-31T10:05:00+00:00"


def test_get_run_status_projects_failed_run_with_failing_partition() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="failed",
                completed="1",
                failingPartition={"S": "facility-b"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    assert status["status"] == "failed"
    assert status["failingPartition"] == "facility-b"
    assert status["downloadReady"] is False


def test_get_run_status_complete_run_exposes_row_counts() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
                rowCounts={"M": {"L1": {"N": "6"}, "L2": {"N": "0"}, "L3": {"N": "17"}}},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    # A complete run decodes the stored DynamoDB map of numeric values into plain ints.
    assert status["rowCounts"] == {"L1": 6, "L2": 0, "L3": 17}


def test_get_run_status_complete_run_decodes_null_placeholder_counts() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
                rowCounts={
                    "M": {
                        "L1": {"N": "6"},
                        "L2": {"N": "0"},
                        "Placeholder": {"NULL": True},
                    }
                },
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    # A placeholder row's count decodes to None (blank) while real counts decode to ints,
    # so the projection exposes rowCounts as number|null.
    assert status["rowCounts"] == {"L1": 6, "L2": 0, "Placeholder": None}


def test_get_run_status_complete_run_exposes_summary() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
                rowsExecuted={"N": "80"},
                placeholdersSkipped={"N": "37"},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    # The execution summary decodes the unique-row tallies for a complete run.
    assert status["summary"] == {"rowsExecuted": 80, "placeholdersSkipped": 37}


def test_get_run_status_complete_run_without_summary_omits_it() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    assert "summary" not in status


def test_get_run_status_malformed_summary_is_sanitized() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
                rowsExecuted={"N": "80"},
                placeholdersSkipped={"N": "not-a-number"},
            )
        }
    )

    with pytest.raises(RunError, match="read failed") as captured:
        _runs(dynamo).get_run_status(RUN_ID)

    assert not isinstance(captured.value, RunRequestError)


def test_get_run_status_complete_run_without_row_counts_omits_them() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    assert "rowCounts" not in status


def test_get_run_status_running_run_never_exposes_row_counts() -> None:
    # Even if the attribute were somehow present, it is only projected for complete runs.
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="running",
                completed="1",
                rowCounts={"M": {"L1": {"N": "6"}}},
            )
        }
    )

    status = _runs(dynamo).get_run_status(RUN_ID)

    assert "rowCounts" not in status


def test_get_run_status_malformed_row_counts_is_sanitized() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
                finishedAt={"S": "2026-08-31T10:05:00+00:00"},
                rowCounts={"M": {"L1": {"N": "not-a-number"}}},
            )
        }
    )

    with pytest.raises(RunError, match="read failed") as captured:
        _runs(dynamo).get_run_status(RUN_ID)

    assert not isinstance(captured.value, RunRequestError)


def test_list_runs_exposes_row_counts_for_most_recent_complete_run() -> None:
    recent = _run_item(
        run_id="run0002",
        started_at="2026-08-31T11:00:00+00:00",
        status="complete",
        zipS3Key={"S": "outputs/p4p-prototype/run0002.zip"},
        version={"S": "v9"},
        finishedAt={"S": "2026-08-31T11:05:00+00:00"},
        rowCounts={"M": {"L1": {"N": "6"}, "L2": {"N": "9"}}},
    )
    older = _run_item(
        run_id="run0001",
        started_at="2026-08-31T10:00:00+00:00",
        status="running",
        completed="1",
    )
    dynamo = FakeDynamo(scan_pages=[{"Items": [older, recent]}])

    runs = _runs(dynamo).list_runs_for_report(REPORT_ID)

    # A scan is sorted newest first, so the most-recent run leads the grid.
    assert runs[0]["runId"] == "run0002"
    assert runs[0]["rowCounts"] == {"L1": 6, "L2": 9}
    # The older, still-running run carries no aggregate counts.
    assert "rowCounts" not in runs[1]


def test_get_run_status_missing_run_is_not_found() -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo(get_item_response={})).get_run_status(RUN_ID)

    assert captured.value.status_code == 404
    assert captured.value.code == RUN_NOT_FOUND


def test_get_run_status_read_failure_is_sanitized() -> None:
    dynamo = FakeDynamo(get_error=FakeClientError("ProvisionedThroughputExceededException"))

    with pytest.raises(RunError, match="read failed"):
        _runs(dynamo).get_run_status(RUN_ID)


@pytest.mark.parametrize("run_id", ["", "bad id", "x" * 129])
def test_get_run_status_rejects_invalid_run_id(run_id: str) -> None:
    with pytest.raises(RunRequestError) as captured:
        _runs(FakeDynamo()).get_run_status(run_id)

    assert captured.value.code == INVALID_RUN_ID


def test_get_run_status_missing_required_field_is_sanitized() -> None:
    item = _run_item()
    del item["requestedBy"]
    dynamo = FakeDynamo(get_item_response={"Item": item})

    with pytest.raises(RunError, match="read failed") as captured:
        _runs(dynamo).get_run_status(RUN_ID)

    assert not isinstance(captured.value, RunRequestError)


def test_get_run_status_malformed_numeric_field_is_sanitized() -> None:
    item = _run_item(completed="not-a-number")
    dynamo = FakeDynamo(get_item_response={"Item": item})

    with pytest.raises(RunError, match="read failed"):
        _runs(dynamo).get_run_status(RUN_ID)


# --------------------------------------------------------------------------- #
# list_runs_for_report
# --------------------------------------------------------------------------- #


def test_list_runs_uses_gsi_query_when_index_configured() -> None:
    dynamo = FakeDynamo(
        query_pages=[
            {
                "Items": [_run_item(status="complete")],
                "LastEvaluatedKey": {"runId": {"S": RUN_ID}},
            },
            {"Items": [_run_item(status="running", run_id="run0002")]},
        ]
    )

    runs = _runs(dynamo, report_index_name="report-index").list_runs_for_report(REPORT_ID)

    assert len(runs) == 2
    assert dynamo.query_calls[0]["IndexName"] == "report-index"
    assert dynamo.query_calls[0]["ScanIndexForward"] is False
    assert dynamo.scan_calls == []


def test_list_runs_scans_and_sorts_newest_first_without_index() -> None:
    older = _run_item(status="complete", started_at="2026-08-01T00:00:00+00:00")
    newer = _run_item(status="running", run_id="run0002", started_at="2026-08-31T00:00:00+00:00")
    dynamo = FakeDynamo(scan_pages=[{"Items": [older, newer]}])

    runs = _runs(dynamo).list_runs_for_report(REPORT_ID)

    assert [run["startedAt"] for run in runs] == [
        "2026-08-31T00:00:00+00:00",
        "2026-08-01T00:00:00+00:00",
    ]
    assert dynamo.scan_calls[0]["FilterExpression"] == "reportId = :reportId"


def test_list_runs_is_bounded_by_max_runs() -> None:
    page = {
        "Items": [_run_item() for _ in range(3)],
        "LastEvaluatedKey": {"runId": {"S": RUN_ID}},
    }
    dynamo = FakeDynamo(scan_pages=[page])

    runs = _runs(dynamo, max_runs=2).list_runs_for_report(REPORT_ID)

    assert len(runs) == 2
    assert len(dynamo.scan_calls) == 1


def test_list_runs_scan_follows_pagination_across_pages() -> None:
    dynamo = FakeDynamo(
        scan_pages=[
            {"Items": [_run_item()], "LastEvaluatedKey": {"runId": {"S": RUN_ID}}},
            {"Items": [_run_item(run_id="run0002")]},
        ]
    )

    runs = _runs(dynamo).list_runs_for_report(REPORT_ID)

    assert len(runs) == 2
    assert len(dynamo.scan_calls) == 2
    assert dynamo.scan_calls[1]["ExclusiveStartKey"] == {"runId": {"S": RUN_ID}}


def test_list_runs_backend_error_is_sanitized() -> None:
    class ExplodingDynamo(FakeDynamo):
        def scan(self, **_kwargs: Any) -> dict[str, Any]:
            raise FakeClientError("InternalServerError")

    with pytest.raises(RunError, match="read failed"):
        _runs(ExplodingDynamo()).list_runs_for_report(REPORT_ID)


def test_list_runs_query_backend_error_is_sanitized() -> None:
    class ExplodingQueryDynamo(FakeDynamo):
        def query(self, **_kwargs: Any) -> dict[str, Any]:
            raise FakeClientError("InternalServerError")

    with pytest.raises(RunError, match="read failed"):
        _runs(ExplodingQueryDynamo(), report_index_name="report-index").list_runs_for_report(
            REPORT_ID
        )


@pytest.mark.parametrize("page", [{"Items": "not-a-list"}, {"Items": [123]}])
def test_list_runs_query_rejects_malformed_items(page: dict[str, Any]) -> None:
    dynamo = FakeDynamo(query_pages=[page])

    with pytest.raises(RunError, match="read failed"):
        _runs(dynamo, report_index_name="report-index").list_runs_for_report(REPORT_ID)


@pytest.mark.parametrize("page", [{"Items": "not-a-list"}, {"Items": [123]}])
def test_list_runs_scan_rejects_malformed_items(page: dict[str, Any]) -> None:
    dynamo = FakeDynamo(scan_pages=[page])

    with pytest.raises(RunError, match="read failed"):
        _runs(dynamo).list_runs_for_report(REPORT_ID)


# --------------------------------------------------------------------------- #
# download_output
# --------------------------------------------------------------------------- #


def test_download_output_returns_bounded_zip_and_audits(caplog: pytest.LogCaptureFixture) -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
                version={"S": "v9"},
            )
        }
    )
    s3 = FakeS3(response={"ContentLength": 5, "Body": BytesIO(b"PK\x03\x04\x00")})

    with caplog.at_level("INFO"):
        result = _runs(dynamo, s3=s3).download_output(RUN_ID, CALLER)

    assert isinstance(result, DownloadResult)
    assert result.content == b"PK\x03\x04\x00"
    assert result.content_type == "application/zip"
    assert s3.calls[0] == {
        "Bucket": "report-output",
        "Key": "outputs/p4p-prototype/run0001.zip",
        "VersionId": "v9",
    }
    audit = json.loads(caplog.records[-1].getMessage())
    assert audit == {
        "callerSub": CALLER,
        "event": "report_output_downloaded",
        "runId": RUN_ID,
        "timestamp": audit["timestamp"],
    }


def test_download_output_requires_complete_status() -> None:
    dynamo = FakeDynamo(get_item_response={"Item": _run_item(status="running", completed="1")})

    with pytest.raises(RunRequestError) as captured:
        _runs(dynamo).download_output(RUN_ID, CALLER)

    assert captured.value.status_code == 409
    assert captured.value.code == RUN_NOT_READY


def test_download_output_missing_key_is_sanitized() -> None:
    dynamo = FakeDynamo(get_item_response={"Item": _run_item(status="complete")})

    with pytest.raises(RunError, match="output is missing"):
        _runs(dynamo).download_output(RUN_ID, CALLER)


def test_download_output_enforces_content_length_bound() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
            )
        }
    )
    s3 = FakeS3(response={"ContentLength": 999, "Body": BytesIO(b"x")})

    with pytest.raises(RunRequestError) as captured:
        _runs(dynamo, s3=s3, max_output_bytes=8).download_output(RUN_ID, CALLER)

    assert captured.value.code == OUTPUT_TOO_LARGE


def test_download_output_enforces_streamed_body_bound() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
            )
        }
    )
    # ContentLength understates the true size; the streamed read still enforces the bound.
    s3 = FakeS3(response={"ContentLength": 1, "Body": BytesIO(b"toolong")})

    with pytest.raises(RunRequestError) as captured:
        _runs(dynamo, s3=s3, max_output_bytes=4).download_output(RUN_ID, CALLER)

    assert captured.value.code == OUTPUT_TOO_LARGE


def test_download_output_retrieval_failure_is_sanitized() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
            )
        }
    )
    s3 = FakeS3(error=FakeClientError("AccessDenied"))

    with pytest.raises(RunError, match="output retrieval failed"):
        _runs(dynamo, s3=s3).download_output(RUN_ID, CALLER)


def test_download_output_missing_body_is_sanitized() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
            )
        }
    )
    s3 = FakeS3(response={"ContentLength": 1})

    with pytest.raises(RunError, match="output retrieval failed"):
        _runs(dynamo, s3=s3).download_output(RUN_ID, CALLER)


class _NonBytesBody:
    def read(self, _amount: int | None = None) -> Any:
        return "not-bytes"


def test_download_output_non_bytes_body_is_sanitized() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
            )
        }
    )
    s3 = FakeS3(response={"ContentLength": 1, "Body": _NonBytesBody()})

    with pytest.raises(RunError, match="output retrieval failed"):
        _runs(dynamo, s3=s3).download_output(RUN_ID, CALLER)


def test_download_output_without_version_omits_version_id() -> None:
    dynamo = FakeDynamo(
        get_item_response={
            "Item": _run_item(
                status="complete",
                zipS3Key={"S": "outputs/p4p-prototype/run0001.zip"},
            )
        }
    )
    s3 = FakeS3(response={"ContentLength": 3, "Body": BytesIO(b"zip")})

    _runs(dynamo, s3=s3).download_output(RUN_ID, CALLER)

    assert "VersionId" not in s3.calls[0]


# --------------------------------------------------------------------------- #
# Constructor
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "kwargs",
    [
        {"table_name": ""},
        {"worker_function_name": ""},
        {"output_bucket": ""},
        {"max_output_bytes": 0},
        {"max_runs": 0},
    ],
)
def test_constructor_rejects_invalid_configuration(kwargs: dict[str, Any]) -> None:
    base: dict[str, Any] = {
        "table_name": "t",
        "worker_function_name": "w",
        "output_bucket": "b",
    }
    base.update(kwargs)
    with pytest.raises(ValueError, match="configuration is invalid"):
        ReportRuns(FakeDynamo(), FakeLambda(), FakeS3(), **base)
