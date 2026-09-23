import json
import logging
from typing import Any

import pytest
from src.reingest_jobs import (
    COUNTER_NAMES,
    DUPLICATE_DOCUMENT_ID,
    INVALID_CALLER,
    INVALID_DOCUMENT_ID,
    INVALID_DOCUMENT_IDS,
    INVALID_JOB_ID,
    INVALID_JOB_REQUEST,
    INVALID_LIMIT,
    JOB_NOT_FOUND,
    MAX_DOCUMENT_IDS,
    MAX_SQL_CHARACTERS,
    SELECTION_TOO_LARGE,
    SQL_FORBIDDEN_KEYWORD,
    SQL_HAS_COMMENT,
    SQL_MULTIPLE_STATEMENTS,
    SQL_NOT_SELECT,
    SQL_TOO_LONG,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    JobProgressStore,
    ReingestError,
    ReingestJobs,
    ReingestRequestError,
    validate_document_ids,
    validate_reingest_sql,
)

CALLER = "auth0|reingest-caller"
JOB_ID = "a" * 32
DOC_A = "a" * 64
DOC_B = "b" * 64
DOC_C = "c" * 64
SQL_JOB_SQL = "SELECT document_id FROM document_metadata WHERE source_format = 'ccda'"
SQL_JOB_SHA256 = "f" * 64
CLUSTER_ARN = "arn:aws:rds:us-west-2:111111111111:cluster:app"
SECRET_ARN = "arn:aws:secretsmanager:us-west-2:111111111111:secret:app"  # noqa: S105


class FakeClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeDataApi:
    def __init__(self, responses: list[dict[str, Any]] | None = None) -> None:
        self._responses = list(responses or [])
        self.error: Exception | None = None
        self.calls: list[dict[str, Any]] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self._responses.pop(0) if self._responses else {"records": []}


class FakeDynamo:
    def __init__(
        self,
        *,
        get_item_response: dict[str, Any] | None = None,
        scan_pages: list[dict[str, Any]] | None = None,
    ) -> None:
        self._get_item_response = get_item_response if get_item_response is not None else {}
        self._scan_pages = scan_pages or [{"Items": []}]
        self.put_error: Exception | None = None
        self.update_error: Exception | None = None
        self.get_error: Exception | None = None
        self.scan_error: Exception | None = None
        self.put_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []
        self.scan_calls: list[dict[str, Any]] = []

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        if self.put_error is not None:
            raise self.put_error
        return {}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        if self.get_error is not None:
            raise self.get_error
        return self._get_item_response

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.update_calls.append(kwargs)
        if self.update_error is not None:
            raise self.update_error
        return {}

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls.append(kwargs)
        if self.scan_error is not None:
            raise self.scan_error
        return self._scan_pages[min(len(self.scan_calls) - 1, len(self._scan_pages) - 1)]


class FakeLambda:
    def __init__(self, *, status_code: int = 202, error: Exception | None = None) -> None:
        self._status_code = status_code
        self._error = error
        self.calls: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self._error is not None:
            raise self._error
        return {"StatusCode": self._status_code}


def _count_response(count: int) -> dict[str, Any]:
    return {"records": [[{"longValue": count}]]}


def _jobs(
    *,
    data_api: FakeDataApi | None = None,
    dynamo: FakeDynamo | None = None,
    lambda_client: FakeLambda | None = None,
    table_name: str = "document_metadata",
) -> ReingestJobs:
    return ReingestJobs(
        data_api or FakeDataApi(),
        dynamo or FakeDynamo(),
        lambda_client or FakeLambda(),
        cluster_arn=CLUSTER_ARN,
        secret_arn=SECRET_ARN,
        database="app_db",
        table_name=table_name,
        jobs_table="reingest-jobs",
        planner_function_name="reingest-planner",
        job_id_factory=lambda: JOB_ID,
    )


# --- SQL guard -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "SELECT document_id FROM document_metadata",
            "SELECT document_id FROM document_metadata",
        ),
        (
            "  select document_id from document_metadata where source_format = 'ccda'  ",
            "select document_id from document_metadata where source_format = 'ccda'",
        ),
        (
            "WITH recent AS (SELECT document_id FROM document_metadata) "
            "SELECT document_id FROM recent",
            "WITH recent AS (SELECT document_id FROM document_metadata) "
            "SELECT document_id FROM recent",
        ),
        (
            "SELECT document_id FROM document_metadata;",
            "SELECT document_id FROM document_metadata",
        ),
        (
            "(SELECT document_id FROM document_metadata)",
            "(SELECT document_id FROM document_metadata)",
        ),
    ],
)
def test_validate_reingest_sql_accepts_select(sql: str, expected: str) -> None:
    assert validate_reingest_sql(sql) == expected


def test_validate_reingest_sql_rejects_non_string() -> None:
    with pytest.raises(ReingestRequestError):
        validate_reingest_sql(None)


def test_validate_reingest_sql_rejects_empty() -> None:
    with pytest.raises(ReingestRequestError):
        validate_reingest_sql("   ")


def test_validate_reingest_sql_rejects_over_length() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_reingest_sql("SELECT " + "a" * MAX_SQL_CHARACTERS)
    assert excinfo.value.code == SQL_TOO_LONG


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT document_id FROM document_metadata -- trailing",
        "SELECT document_id FROM document_metadata /* block */",
        "SELECT document_id FROM document_metadata */",
    ],
)
def test_validate_reingest_sql_rejects_comments(sql: str) -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_reingest_sql(sql)
    assert excinfo.value.code == SQL_HAS_COMMENT


def test_validate_reingest_sql_rejects_multiple_statements() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_reingest_sql("SELECT document_id FROM document_metadata; DROP TABLE x")
    assert excinfo.value.code == SQL_MULTIPLE_STATEMENTS


def test_validate_reingest_sql_rejects_non_select_leading() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_reingest_sql("EXPLAIN SELECT document_id FROM document_metadata")
    assert excinfo.value.code == SQL_NOT_SELECT


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT document_id INTO staging FROM document_metadata",
        "INSERT INTO document_metadata VALUES (1)",
        "UPDATE document_metadata SET x = 1",
        "DELETE FROM document_metadata",
        "SELECT document_id FROM document_metadata FOR UPDATE",
        "SELECT document_id FROM document_metadata FOR SHARE",
        "SELECT document_id FROM document_metadata; COPY x TO stdout",
        "CALL do_thing()",
        "DO $$ BEGIN END $$",
    ],
)
def test_validate_reingest_sql_rejects_forbidden(sql: str) -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_reingest_sql(sql)
    assert excinfo.value.code in {
        SQL_FORBIDDEN_KEYWORD,
        SQL_NOT_SELECT,
        SQL_MULTIPLE_STATEMENTS,
    }


def test_validate_reingest_sql_allows_offset_identifier() -> None:
    # ``offset`` shares the ``set`` substring but is a distinct whole word and is allowed.
    sql = "SELECT document_id FROM document_metadata ORDER BY document_id OFFSET 5"
    assert validate_reingest_sql(sql) == sql


def test_validate_reingest_sql_rejects_selection_token() -> None:
    with pytest.raises(ReingestRequestError):
        validate_reingest_sql("SELECT document_id FROM __SELECTION__")


# --- document id guard -----------------------------------------------------------------


def test_validate_document_ids_dedup_preserves_order() -> None:
    assert validate_document_ids([DOC_B, DOC_A]) == [DOC_B, DOC_A]


def test_validate_document_ids_rejects_empty() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_document_ids([])
    assert excinfo.value.code == INVALID_DOCUMENT_IDS


def test_validate_document_ids_rejects_non_list() -> None:
    with pytest.raises(ReingestRequestError):
        validate_document_ids("not-a-list")


def test_validate_document_ids_rejects_too_many() -> None:
    with pytest.raises(ReingestRequestError):
        validate_document_ids([DOC_A] * (MAX_DOCUMENT_IDS + 1))


def test_validate_document_ids_rejects_bad_id() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_document_ids(["not-hex"])
    assert excinfo.value.code == INVALID_DOCUMENT_ID


def test_validate_document_ids_rejects_uppercase() -> None:
    with pytest.raises(ReingestRequestError):
        validate_document_ids(["A" * 64])


def test_validate_document_ids_rejects_duplicate() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        validate_document_ids([DOC_A, DOC_A])
    assert excinfo.value.code == DUPLICATE_DOCUMENT_ID


# --- preview ---------------------------------------------------------------------------


def test_preview_returns_exact_count_and_builds_wrapper() -> None:
    data_api = FakeDataApi([_count_response(42)])
    jobs = _jobs(data_api=data_api, table_name="reindex_docs")
    result = jobs.preview("SELECT document_id FROM reindex_docs", CALLER)
    assert result == {"count": 42}
    sql = data_api.calls[0]["sql"]
    assert "COUNT(DISTINCT selection.document_id)" in sql
    assert "JOIN reindex_docs dm ON dm.document_id = selection.document_id" in sql
    assert "SELECT document_id FROM reindex_docs" in sql
    # The user SQL is embedded verbatim, never bound as a parameter.
    assert "parameters" not in data_api.calls[0]


def test_preview_rejects_bad_caller() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        _jobs().preview("SELECT document_id FROM document_metadata", "")
    assert excinfo.value.code == INVALID_CALLER


def test_preview_maps_data_api_error_to_invalid_sql() -> None:
    data_api = FakeDataApi()
    data_api.error = RuntimeError("boom")
    with pytest.raises(ReingestRequestError) as excinfo:
        _jobs(data_api=data_api).preview("SELECT document_id FROM document_metadata", CALLER)
    assert excinfo.value.code == "invalid_sql"


def test_preview_malformed_response_is_sanitized() -> None:
    data_api = FakeDataApi([{"records": [[{"stringValue": "nope"}]]}])
    with pytest.raises(ReingestError):
        _jobs(data_api=data_api).preview("SELECT document_id FROM document_metadata", CALLER)


# --- create job: SQL -------------------------------------------------------------------


def test_create_job_sql_persists_hash_and_dispatches() -> None:
    data_api = FakeDataApi([_count_response(3)])
    dynamo = FakeDynamo()
    lambda_client = FakeLambda()
    jobs = _jobs(data_api=data_api, dynamo=dynamo, lambda_client=lambda_client)
    user_sql = "  SELECT document_id FROM document_metadata;  "
    result = jobs.create_job(CALLER, sql=user_sql)

    assert result["jobId"] == JOB_ID
    assert result["status"] == STATUS_QUEUED
    assert result["mode"] == "sql"
    assert result["expected"] == 3
    assert result["counters"] == dict.fromkeys(COUNTER_NAMES, 0)
    assert result["enqueueComplete"] is False
    assert "sqlSha256" in result
    # The authenticated projection returns the verbatim SQL for the UI's expandable panel.
    assert result["sql"] == user_sql
    assert "idCount" not in result

    item = dynamo.put_calls[0]["Item"]
    assert item["sql"] == {"S": user_sql}
    assert item["sqlSha256"]["S"] == result["sqlSha256"]
    assert "documentIds" not in item
    assert dynamo.put_calls[0]["ConditionExpression"] == "attribute_not_exists(jobId)"

    invoke = lambda_client.calls[0]
    assert invoke["InvocationType"] == "Event"
    payload = json.loads(invoke["Payload"])
    assert payload == {"jobId": JOB_ID, "mode": "sql", "sql": user_sql}
    preview_sql = data_api.calls[0]["sql"]
    assert "SELECT document_id FROM document_metadata" in preview_sql
    assert ";" not in preview_sql


def test_create_job_sql_rejects_oversized_selection() -> None:
    data_api = FakeDataApi([_count_response(100_001)])
    dynamo = FakeDynamo()
    with pytest.raises(ReingestRequestError) as excinfo:
        _jobs(data_api=data_api, dynamo=dynamo).create_job(
            CALLER, sql="SELECT document_id FROM document_metadata"
        )
    assert excinfo.value.code == SELECTION_TOO_LARGE
    # No job row is written and no planner is dispatched for an oversized selection.
    assert dynamo.put_calls == []


def test_create_job_sql_boundary_count_is_allowed() -> None:
    data_api = FakeDataApi([_count_response(100_000)])
    jobs = _jobs(data_api=data_api)
    result = jobs.create_job(CALLER, sql="SELECT document_id FROM document_metadata")
    assert result["expected"] == 100_000


# --- create job: IDs -------------------------------------------------------------------


def test_create_job_ids_stores_count_not_ids() -> None:
    dynamo = FakeDynamo()
    lambda_client = FakeLambda()
    jobs = _jobs(dynamo=dynamo, lambda_client=lambda_client)
    result = jobs.create_job(CALLER, document_ids=[DOC_A, DOC_B])

    assert result["mode"] == "ids"
    assert result["expected"] == 2
    assert result["idCount"] == 2
    assert "sql" not in result
    assert "sqlSha256" not in result
    item = dynamo.put_calls[0]["Item"]
    assert item["idCount"] == {"N": "2"}
    assert "sql" not in item
    assert "documentIds" not in item

    payload = json.loads(lambda_client.calls[0]["Payload"])
    assert payload == {"jobId": JOB_ID, "mode": "ids", "documentIds": [DOC_A, DOC_B]}


def test_create_job_requires_exactly_one_source() -> None:
    jobs = _jobs()
    with pytest.raises(ReingestRequestError) as neither:
        jobs.create_job(CALLER)
    assert neither.value.code == INVALID_JOB_REQUEST
    with pytest.raises(ReingestRequestError) as both:
        jobs.create_job(
            CALLER, sql="SELECT document_id FROM document_metadata", document_ids=[DOC_A]
        )
    assert both.value.code == INVALID_JOB_REQUEST


def test_create_job_dispatch_invoke_error_is_sanitized() -> None:
    lambda_client = FakeLambda(error=RuntimeError("boom"))
    with pytest.raises(ReingestError):
        _jobs(lambda_client=lambda_client).create_job(CALLER, document_ids=[DOC_A])


def test_create_job_dispatch_failure_is_sanitized() -> None:
    lambda_client = FakeLambda(status_code=500)
    with pytest.raises(ReingestError):
        _jobs(lambda_client=lambda_client).create_job(CALLER, document_ids=[DOC_A])


def test_create_job_put_failure_is_sanitized() -> None:
    dynamo = FakeDynamo()
    dynamo.put_error = FakeClientError("ConditionalCheckFailedException")
    with pytest.raises(ReingestError):
        _jobs(dynamo=dynamo).create_job(CALLER, document_ids=[DOC_A])


def test_create_job_audit_logs_hash_only(caplog: pytest.LogCaptureFixture) -> None:
    data_api = FakeDataApi([_count_response(1)])
    jobs = _jobs(data_api=data_api)
    with caplog.at_level(logging.INFO, logger="src.reingest_jobs"):
        jobs.create_job(CALLER, sql="SELECT document_id FROM document_metadata")
    record = json.loads(caplog.records[-1].message)
    assert record["event"] == "reingest_job_created"
    assert record["callerSub"] == CALLER
    assert record["mode"] == "sql"
    assert record["count"] == 1
    assert "sqlSha256" in record
    # The raw SQL is never logged.
    assert "sql" not in record
    assert "document_metadata" not in caplog.records[-1].message


def test_create_job_ids_audit_omits_hash(caplog: pytest.LogCaptureFixture) -> None:
    jobs = _jobs()
    with caplog.at_level(logging.INFO, logger="src.reingest_jobs"):
        jobs.create_job(CALLER, document_ids=[DOC_A, DOC_B])
    record = json.loads(caplog.records[-1].message)
    assert record["mode"] == "ids"
    assert record["count"] == 2
    assert "sqlSha256" not in record


# --- list / get ------------------------------------------------------------------------


def _item(job_id: str, created_at: str, *, mode: str = "ids") -> dict[str, Any]:
    item = {
        "jobId": {"S": job_id},
        "status": {"S": STATUS_QUEUED},
        "mode": {"S": mode},
        "requestedBy": {"S": CALLER},
        "createdAt": {"S": created_at},
        "expected": {"N": "1"},
        "enqueueComplete": {"BOOL": False},
    }
    for counter in COUNTER_NAMES:
        item[counter] = {"N": "0"}
    if mode == "ids":
        item["idCount"] = {"N": "1"}
    if mode == "sql":
        item["sql"] = {"S": SQL_JOB_SQL}
        item["sqlSha256"] = {"S": SQL_JOB_SHA256}
    return item


def test_list_jobs_sorts_newest_first_and_limits() -> None:
    pages = [
        {
            "Items": [
                _item("a" * 32, "2026-01-01T00:00:00+00:00"),
                _item("b" * 32, "2026-03-01T00:00:00+00:00"),
                _item("c" * 32, "2026-02-01T00:00:00+00:00"),
            ]
        }
    ]
    jobs = _jobs(dynamo=FakeDynamo(scan_pages=pages))
    result = jobs.list_jobs(2)
    ids = [job["jobId"] for job in result["items"]]
    assert ids == ["b" * 32, "c" * 32]


def test_list_jobs_paginates_scan() -> None:
    pages: list[dict[str, Any]] = [
        {"Items": [_item("a" * 32, "2026-01-01T00:00:00+00:00")], "LastEvaluatedKey": {"k": 1}},
        {"Items": [_item("b" * 32, "2026-02-01T00:00:00+00:00")]},
    ]
    dynamo = FakeDynamo(scan_pages=pages)
    result = _jobs(dynamo=dynamo).list_jobs()
    assert len(result["items"]) == 2
    assert len(dynamo.scan_calls) == 2


@pytest.mark.parametrize("limit", [0, 201, True, "5"])
def test_list_jobs_rejects_bad_limit(limit: Any) -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        _jobs().list_jobs(limit)
    assert excinfo.value.code == INVALID_LIMIT


def test_get_job_returns_projection() -> None:
    dynamo = FakeDynamo(get_item_response={"Item": _item(JOB_ID, "2026-01-01T00:00:00+00:00")})
    result = _jobs(dynamo=dynamo).get_job(JOB_ID)
    assert result["jobId"] == JOB_ID
    assert result["idCount"] == 1


def test_get_job_missing_is_404() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        _jobs(dynamo=FakeDynamo(get_item_response={})).get_job(JOB_ID)
    assert excinfo.value.code == JOB_NOT_FOUND


def test_get_job_rejects_bad_id() -> None:
    with pytest.raises(ReingestRequestError) as excinfo:
        _jobs().get_job("short")
    assert excinfo.value.code == INVALID_JOB_ID


def test_get_job_sql_projection_includes_verbatim_sql() -> None:
    item = _item(JOB_ID, "2026-01-01T00:00:00+00:00", mode="sql")
    dynamo = FakeDynamo(get_item_response={"Item": item})
    result = _jobs(dynamo=dynamo).get_job(JOB_ID)
    assert result["mode"] == "sql"
    # The verbatim SQL and its hash are both projected so the UI panel can display them.
    assert result["sql"] == SQL_JOB_SQL
    assert result["sqlSha256"] == SQL_JOB_SHA256
    # A SQL job never exposes an ID count.
    assert "idCount" not in result


def test_get_job_ids_projection_omits_sql() -> None:
    item = _item(JOB_ID, "2026-01-01T00:00:00+00:00", mode="ids")
    dynamo = FakeDynamo(get_item_response={"Item": item})
    result = _jobs(dynamo=dynamo).get_job(JOB_ID)
    assert result["mode"] == "ids"
    assert result["idCount"] == 1
    # An ID job never projects SQL or its hash.
    assert "sql" not in result
    assert "sqlSha256" not in result


def test_list_jobs_projects_sql_and_id_jobs_distinctly() -> None:
    pages = [
        {
            "Items": [
                _item("a" * 32, "2026-01-01T00:00:00+00:00", mode="sql"),
                _item("b" * 32, "2026-02-01T00:00:00+00:00", mode="ids"),
            ]
        }
    ]
    result = _jobs(dynamo=FakeDynamo(scan_pages=pages)).list_jobs()
    by_id = {job["jobId"]: job for job in result["items"]}
    sql_job = by_id["a" * 32]
    assert sql_job["sql"] == SQL_JOB_SQL
    assert sql_job["sqlSha256"] == SQL_JOB_SHA256
    assert "idCount" not in sql_job
    id_job = by_id["b" * 32]
    assert id_job["idCount"] == 1
    assert "sql" not in id_job
    assert "sqlSha256" not in id_job


# --- progress store --------------------------------------------------------------------


def _progress(dynamo: FakeDynamo) -> JobProgressStore:
    return JobProgressStore(dynamo, jobs_table="reingest-jobs")


def test_mark_running_sets_status_and_start() -> None:
    dynamo = FakeDynamo()
    _progress(dynamo).mark_running(JOB_ID)
    call = dynamo.update_calls[0]
    assert ":running" in call["ExpressionAttributeValues"]
    assert call["ExpressionAttributeValues"][":running"] == {"S": STATUS_RUNNING}
    assert "startedAt" in call["UpdateExpression"]
    assert call["ConditionExpression"] == "attribute_exists(jobId)"


def test_add_enqueued_uses_atomic_add() -> None:
    dynamo = FakeDynamo()
    _progress(dynamo).add_enqueued(JOB_ID, 7)
    call = dynamo.update_calls[0]
    assert call["UpdateExpression"] == "ADD enqueued :enqueued"
    assert call["ExpressionAttributeValues"] == {":enqueued": {"N": "7"}}


def test_add_enqueued_zero_is_noop() -> None:
    dynamo = FakeDynamo()
    _progress(dynamo).add_enqueued(JOB_ID, 0)
    assert dynamo.update_calls == []


def test_record_outcomes_atomic_add_multiple() -> None:
    dynamo = FakeDynamo()
    _progress(dynamo).record_outcomes(
        JOB_ID, reindexed=2, reindexed_stale_parser=1, missing_parsed=1, failed=3
    )
    call = dynamo.update_calls[0]
    assert call["UpdateExpression"].startswith("ADD ")
    values = call["ExpressionAttributeValues"]
    assert values[":reindexed"] == {"N": "2"}
    assert values[":reindexedStaleParser"] == {"N": "1"}
    assert values[":missingParsed"] == {"N": "1"}
    assert values[":failed"] == {"N": "3"}


def test_mark_failed_sets_terminal_status() -> None:
    dynamo = FakeDynamo()
    _progress(dynamo).mark_failed(JOB_ID)
    call = dynamo.update_calls[0]
    assert call["ExpressionAttributeValues"][":failed"] == {"S": STATUS_FAILED}


def _running_item(**counters: int) -> dict[str, Any]:
    item = {
        "jobId": {"S": JOB_ID},
        "status": {"S": STATUS_RUNNING},
        "enqueueComplete": {"BOOL": True},
    }
    for name in COUNTER_NAMES:
        item[name] = {"N": str(counters.get(name, 0))}
    return item


def test_complete_if_settled_completes_when_processed() -> None:
    item = _running_item(enqueued=3, reindexed=2, missingParsed=0, failed=1)
    dynamo = FakeDynamo(get_item_response={"Item": item})
    assert _progress(dynamo).complete_if_settled(JOB_ID) is True
    complete_call = dynamo.update_calls[-1]
    assert complete_call["ExpressionAttributeValues"][":complete"] == {"S": STATUS_COMPLETE}
    assert complete_call["ConditionExpression"] == "#status = :running"


def test_complete_if_settled_waits_for_enqueue_complete() -> None:
    item = _running_item(enqueued=1, reindexed=1)
    item["enqueueComplete"] = {"BOOL": False}
    dynamo = FakeDynamo(get_item_response={"Item": item})
    assert _progress(dynamo).complete_if_settled(JOB_ID) is False
    assert dynamo.update_calls == []


def test_complete_if_settled_waits_for_processing() -> None:
    item = _running_item(enqueued=5, reindexed=2, missingParsed=1, failed=0)
    dynamo = FakeDynamo(get_item_response={"Item": item})
    assert _progress(dynamo).complete_if_settled(JOB_ID) is False


def test_complete_if_settled_ignores_non_running() -> None:
    item = _running_item(enqueued=1, reindexed=1)
    item["status"] = {"S": STATUS_COMPLETE}
    dynamo = FakeDynamo(get_item_response={"Item": item})
    assert _progress(dynamo).complete_if_settled(JOB_ID) is False


def test_complete_if_settled_lost_race_returns_false() -> None:
    item = _running_item(enqueued=1, reindexed=1)
    dynamo = FakeDynamo(get_item_response={"Item": item})
    dynamo.update_error = FakeClientError("ConditionalCheckFailedException")
    assert _progress(dynamo).complete_if_settled(JOB_ID) is False


def test_complete_if_settled_other_error_is_sanitized() -> None:
    item = _running_item(enqueued=1, reindexed=1)
    dynamo = FakeDynamo(get_item_response={"Item": item})
    dynamo.update_error = FakeClientError("ProvisionedThroughputExceededException")
    with pytest.raises(ReingestError):
        _progress(dynamo).complete_if_settled(JOB_ID)


# --- configuration and error branches --------------------------------------------------


@pytest.mark.parametrize(
    ("database", "table_name", "jobs_table", "planner"),
    [
        ("Bad-DB", "document_metadata", "jobs", "planner"),
        ("app_db", "Bad-Table", "jobs", "planner"),
        ("app_db", "document_metadata", "", "planner"),
        ("app_db", "document_metadata", "jobs", ""),
    ],
)
def test_constructor_rejects_bad_config(
    database: str, table_name: str, jobs_table: str, planner: str
) -> None:
    with pytest.raises(ValueError, match="configuration"):
        ReingestJobs(
            FakeDataApi(),
            FakeDynamo(),
            FakeLambda(),
            cluster_arn=CLUSTER_ARN,
            secret_arn=SECRET_ARN,
            database=database,
            table_name=table_name,
            jobs_table=jobs_table,
            planner_function_name=planner,
        )


def test_create_job_bad_id_factory_is_sanitized() -> None:
    jobs = ReingestJobs(
        FakeDataApi(),
        FakeDynamo(),
        FakeLambda(),
        cluster_arn=CLUSTER_ARN,
        secret_arn=SECRET_ARN,
        database="app_db",
        table_name="document_metadata",
        jobs_table="jobs",
        planner_function_name="planner",
        job_id_factory=lambda: "not-hex",
    )
    with pytest.raises(ReingestError):
        jobs.create_job(CALLER, document_ids=[DOC_A])


def test_get_job_read_error_is_sanitized() -> None:
    dynamo = FakeDynamo()
    dynamo.get_error = RuntimeError("boom")
    with pytest.raises(ReingestError):
        _jobs(dynamo=dynamo).get_job(JOB_ID)


def test_get_job_malformed_item_is_sanitized() -> None:
    # An item missing a required attribute is a stored-shape violation, not a 404.
    dynamo = FakeDynamo(get_item_response={"Item": {"jobId": {"S": JOB_ID}}})
    with pytest.raises(ReingestError):
        _jobs(dynamo=dynamo).get_job(JOB_ID)


def test_get_job_bad_numeric_attribute_is_sanitized() -> None:
    item = _item(JOB_ID, "2026-01-01T00:00:00+00:00")
    item["expected"] = {"N": "not-a-number"}
    dynamo = FakeDynamo(get_item_response={"Item": item})
    with pytest.raises(ReingestError):
        _jobs(dynamo=dynamo).get_job(JOB_ID)


def test_list_jobs_scan_error_is_sanitized() -> None:
    dynamo = FakeDynamo()
    dynamo.scan_error = RuntimeError("boom")
    with pytest.raises(ReingestError):
        _jobs(dynamo=dynamo).list_jobs()


def test_list_jobs_non_list_items_is_sanitized() -> None:
    dynamo = FakeDynamo(scan_pages=[{"Items": "nope"}])
    with pytest.raises(ReingestError):
        _jobs(dynamo=dynamo).list_jobs()


@pytest.mark.parametrize(
    "response",
    [
        {"records": []},
        {"records": [[{"longValue": -1}]]},
        {"records": [[{"longValue": 1}, {"longValue": 2}]]},
    ],
)
def test_preview_bad_count_shapes_are_sanitized(response: dict[str, Any]) -> None:
    data_api = FakeDataApi([response])
    with pytest.raises(ReingestError):
        _jobs(data_api=data_api).preview("SELECT document_id FROM document_metadata", CALLER)


def test_progress_store_rejects_empty_table() -> None:
    with pytest.raises(ValueError, match="configuration"):
        JobProgressStore(FakeDynamo(), jobs_table="")


def test_progress_read_missing_item_is_sanitized() -> None:
    dynamo = FakeDynamo(get_item_response={})
    with pytest.raises(ReingestError):
        _progress(dynamo).complete_if_settled(JOB_ID)


def test_progress_read_error_is_sanitized() -> None:
    dynamo = FakeDynamo()
    dynamo.get_error = RuntimeError("boom")
    with pytest.raises(ReingestError):
        _progress(dynamo).complete_if_settled(JOB_ID)


def test_progress_update_error_is_sanitized() -> None:
    dynamo = FakeDynamo()
    dynamo.update_error = RuntimeError("boom")
    with pytest.raises(ReingestError):
        _progress(dynamo).mark_running(JOB_ID)
