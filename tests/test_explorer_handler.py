import base64
import json
import logging
from io import BytesIO
from typing import Any

import pytest
from src import explorer_handler
from src.explorer_handler import (
    BodyResult,
    ExplorerError,
    MessageExplorer,
    RequestError,
    handler,
)

DOCUMENT_A = "a" * 64
DOCUMENT_B = "b" * 64
DOCUMENT_C = "c" * 64
SENSITIVE_FAILURE = "sensitive SQL and clinical response"
CREDENTIAL_ARN = "arn:aws:secretsmanager:us-west-2:111122223333:secret:metadata"


class FakeDataApi:
    def __init__(
        self,
        responses: list[dict[str, Any]] | None = None,
        *,
        error: Exception | None = None,
        count: int = 2,
    ) -> None:
        self.responses = list(responses or [])
        self.error = error
        self.count = count
        self.calls: list[dict[str, Any]] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        if str(kwargs.get("sql", "")).startswith("SELECT COUNT(*)"):
            return {"records": [[{"longValue": self.count}]]}
        return self.responses.pop(0) if self.responses else {"records": []}


class FakeBody:
    def __init__(self, payload: bytes | str) -> None:
        self.payload = payload
        self.amounts: list[int | None] = []

    def read(self, amount: int | None = None) -> bytes | str:
        self.amounts.append(amount)
        return self.payload if amount is None else self.payload[:amount]


class FakeS3:
    def __init__(
        self,
        payload: bytes = b"message-body",
        *,
        content_length: int | None = None,
        error: Exception | None = None,
        body: object | None = None,
    ) -> None:
        self.payload = payload
        self.content_length = len(payload) if content_length is None else content_length
        self.error = error
        self.body = body
        self.calls: list[dict[str, Any]] = []

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {
            "Body": self.body if self.body is not None else BytesIO(self.payload),
            "ContentLength": self.content_length,
        }


def _field(value: Any) -> dict[str, Any]:
    if value is None:
        return {"isNull": True}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"longValue": value}
    return {"doubleValue": value}


def _list_record(
    document_id: str,
    ingested_time: str,
    *,
    source_format: str = "hl7-v2",
    document_time: str | None = None,
) -> list[dict[str, Any]]:
    return [
        _field(document_id),
        _field(source_format),
        _field(document_time),
        _field(ingested_time),
    ]


def _detail_record(
    document_id: str = DOCUMENT_A,
    *,
    source_format: str = "hl7-v2",
    raw_uri: str = "s3://raw-bucket/incoming/hl7/message.hl7",
    parsed_uri: str = "s3://parsed-bucket/hl7/document.json",
) -> list[dict[str, Any]]:
    return [
        _field(document_id),
        _field(source_format),
        _field("2026-08-20T10:00:00+00:00"),
        _field("2026-08-20T11:00:00+00:00"),
        _field(raw_uri),
        _field("raw-version"),
        _field(parsed_uri),
        _field("parsed-version"),
    ]


def _explorer(
    data_api: FakeDataApi,
    s3: FakeS3 | None = None,
    *,
    table_name: str = "document_metadata",
    max_body_bytes: int = 1024,
) -> MessageExplorer:
    return MessageExplorer(
        data_api,
        s3 or FakeS3(),
        cluster_arn="cluster",
        secret_arn=CREDENTIAL_ARN,
        database="manifest_medex",
        table_name=table_name,
        raw_bucket="raw-bucket",
        parsed_bucket="parsed-bucket",
        max_body_bytes=max_body_bytes,
    )


def _authorized_event(route_key: str) -> dict[str, Any]:
    return {
        "routeKey": route_key,
        "requestContext": {
            "authorizer": {"jwt": {"claims": {"sub": "caller-subject"}}},
        },
    }


def _parameters(call: dict[str, Any]) -> dict[str, Any]:
    return {parameter["name"]: parameter["value"] for parameter in call["parameters"]}


def test_list_messages_defaults_to_newest_first_and_narrow_page() -> None:
    data_api = FakeDataApi(
        [
            {
                "records": [
                    _list_record(DOCUMENT_A, "2026-08-20T12:00:00+00:00"),
                    _list_record(DOCUMENT_B, "2026-08-20T11:00:00+00:00", source_format="ccda"),
                ]
            }
        ]
    )

    result = _explorer(data_api).list_messages({})

    assert [item["documentId"] for item in result["items"]] == [DOCUMENT_A, DOCUMENT_B]
    assert result["items"][0]["documentTime"] is None
    assert result["nextCursor"] is None
    assert result["totalCount"] == 2
    call = data_api.calls[0]
    assert "SELECT document_id, source_format, document_time, ingested_time" in call["sql"]
    assert "ORDER BY ingested_time DESC, document_id DESC" in call["sql"]
    assert "OFFSET" not in call["sql"]
    assert _parameters(call) == {"page_size": {"longValue": 51}}
    count_call = data_api.calls[1]
    assert count_call["sql"] == "SELECT COUNT(*)\nFROM document_metadata"
    assert count_call["parameters"] == []


def test_list_messages_uses_parameterized_filters_and_keyset_cursor() -> None:
    data_api = FakeDataApi(
        [
            {
                "records": [
                    _list_record(DOCUMENT_A, "2026-08-20T12:00:00+00:00"),
                    _list_record(DOCUMENT_B, "2026-08-20T11:00:00+00:00"),
                    _list_record(DOCUMENT_C, "2026-08-20T10:00:00+00:00"),
                ]
            },
            {"records": []},
        ],
        count=137,
    )
    explorer = _explorer(data_api)
    query = {
        "from": "2026-08-01T00:00:00Z",
        "to": "2026-08-31T23:59:59Z",
        "source_format": "hl7-v2",
        "limit": "2",
    }

    first = explorer.list_messages(query)
    second = explorer.list_messages({**query, "cursor": first["nextCursor"]})

    assert second == {"items": [], "nextCursor": None, "totalCount": 137}
    page_calls = [call for call in data_api.calls if "ORDER BY" in call["sql"]]
    count_calls = [call for call in data_api.calls if call["sql"].startswith("SELECT COUNT(*)")]
    first_call, second_call = page_calls
    assert "source_format = :source_format" in first_call["sql"]
    assert "ingested_time >= CAST(:from_time AS TIMESTAMPTZ)" in first_call["sql"]
    assert "ingested_time < CAST(:to_time AS TIMESTAMPTZ)" in first_call["sql"]
    assert "2026-08" not in first_call["sql"]
    assert "(ingested_time, document_id) <" in second_call["sql"]
    assert _parameters(second_call)["cursor_document_id"] == {"stringValue": DOCUMENT_B}
    assert _parameters(second_call)["cursor_time"] == {"stringValue": "2026-08-20T11:00:00+00:00"}
    assert _parameters(first_call)["page_size"] == {"longValue": 3}
    assert len(count_calls) == 2
    for count_call in count_calls:
        assert "cursor" not in count_call["sql"]
        assert "LIMIT" not in count_call["sql"]
        assert _parameters(count_call) == {
            "from_time": {"stringValue": "2026-08-01T00:00:00Z"},
            "to_time": {"stringValue": "2026-08-31T23:59:59Z"},
            "source_format": {"stringValue": "hl7-v2"},
        }


@pytest.mark.parametrize(
    ("query", "code"),
    [
        ({"limit": "0"}, "invalid_limit"),
        ({"limit": "201"}, "invalid_limit"),
        ({"limit": "01"}, "invalid_limit"),
        ({"limit": "many"}, "invalid_limit"),
        ({"source_format": "fhir"}, "invalid_source_format"),
        ({"from": "2026-08-20"}, "invalid_time"),
        ({"from": "x" * 65}, "invalid_time"),
        (
            {"from": "2026-08-21T00:00:00Z", "to": "2026-08-20T00:00:00Z"},
            "invalid_time_range",
        ),
        ({"cursor": "not-base64!"}, "invalid_cursor"),
    ],
)
def test_list_messages_rejects_invalid_queries(query: dict[str, str], code: str) -> None:
    with pytest.raises(RequestError, match=code):
        _explorer(FakeDataApi()).list_messages(query)


def test_detail_is_parameterized_and_returns_locations() -> None:
    data_api = FakeDataApi([{"records": [_detail_record()]}])

    result = _explorer(data_api).get_message(DOCUMENT_A)

    assert result == {
        "documentId": DOCUMENT_A,
        "sourceFormat": "hl7-v2",
        "documentTime": "2026-08-20T10:00:00+00:00",
        "ingestedTime": "2026-08-20T11:00:00+00:00",
        "rawS3Uri": "s3://raw-bucket/incoming/hl7/message.hl7",
        "rawVersionId": "raw-version",
        "parsedS3Uri": "s3://parsed-bucket/hl7/document.json",
        "parsedVersionId": "parsed-version",
    }
    assert DOCUMENT_A not in data_api.calls[0]["sql"]
    assert _parameters(data_api.calls[0]) == {"document_id": {"stringValue": DOCUMENT_A}}


def test_detail_rejects_invalid_or_missing_document() -> None:
    explorer = _explorer(FakeDataApi([{"records": []}]))

    with pytest.raises(RequestError, match="invalid_document_id"):
        explorer.get_message("not-a-document-id")
    with pytest.raises(RequestError, match="message_not_found"):
        explorer.get_message(DOCUMENT_A)


@pytest.mark.parametrize(
    ("variant", "source_format", "expected_type", "uri_key", "version_id"),
    [
        ("raw", "hl7-v2", "text/plain; charset=utf-8", "incoming/hl7/message.hl7", "raw-version"),
        (
            "raw",
            "ccda",
            "application/xml; charset=utf-8",
            "incoming/hl7/message.hl7",
            "raw-version",
        ),
        ("parsed", "hl7-v2", "application/json", "hl7/document.json", "parsed-version"),
    ],
)
def test_body_fetch_uses_exact_s3_version_and_audits_without_content(
    variant: str,
    source_format: str,
    expected_type: str,
    uri_key: str,
    version_id: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    content = b"clinical-content-must-not-be-logged"
    s3 = FakeS3(content)
    data_api = FakeDataApi([{"records": [_detail_record(source_format=source_format)]}])
    caplog.set_level(logging.INFO, logger="src.explorer_handler")

    result = _explorer(data_api, s3).get_body(DOCUMENT_A, variant, "caller-subject")

    assert result == BodyResult(content=content, content_type=expected_type)
    expected_bucket = "raw-bucket" if variant == "raw" else "parsed-bucket"
    assert s3.calls == [{"Bucket": expected_bucket, "Key": uri_key, "VersionId": version_id}]
    audit = json.loads(caplog.messages[-1])
    assert audit["event"] == "message_body_fetched"
    assert audit["callerSub"] == "caller-subject"
    assert audit["documentId"] == DOCUMENT_A
    assert audit["variant"] == variant
    assert "clinical-content" not in caplog.text


def test_body_fetch_rejects_invalid_variant_storage_reference_and_size() -> None:
    with pytest.raises(RequestError, match="invalid_variant"):
        _explorer(FakeDataApi()).get_body(DOCUMENT_A, "other", "caller")

    wrong_bucket = FakeDataApi(
        [{"records": [_detail_record(raw_uri="s3://other-bucket/message.hl7")]}]
    )
    with pytest.raises(ExplorerError, match="storage reference"):
        _explorer(wrong_bucket).get_body(DOCUMENT_A, "raw", "caller")

    too_large = FakeDataApi([{"records": [_detail_record()]}])
    with pytest.raises(RequestError, match="message_body_too_large"):
        _explorer(too_large, FakeS3(content_length=1025), max_body_bytes=1024).get_body(
            DOCUMENT_A, "raw", "caller"
        )


def test_body_fetch_bounds_stream_and_sanitizes_s3_failures() -> None:
    data_api = FakeDataApi([{"records": [_detail_record()]}])
    body = FakeBody(b"12345")
    with pytest.raises(RequestError, match="message_body_too_large"):
        _explorer(data_api, FakeS3(body=body, content_length=4), max_body_bytes=4).get_body(
            DOCUMENT_A, "raw", "caller"
        )
    assert body.amounts == [5]

    data_api = FakeDataApi([{"records": [_detail_record()]}])
    with pytest.raises(ExplorerError, match="retrieval failed") as captured:
        _explorer(data_api, FakeS3(error=RuntimeError(SENSITIVE_FAILURE))).get_body(
            DOCUMENT_A, "raw", "caller"
        )
    assert captured.value.__cause__ is None
    assert "sensitive" not in str(captured.value)


def test_data_api_and_record_failures_are_sanitized() -> None:
    with pytest.raises(ExplorerError, match="Metadata read failed") as captured:
        _explorer(FakeDataApi(error=RuntimeError(SENSITIVE_FAILURE))).list_messages({})
    assert captured.value.__cause__ is None
    assert "sensitive" not in str(captured.value)

    malformed = _explorer(FakeDataApi([{"records": [[{"blobValue": b"x"}]]}]))
    with pytest.raises(ExplorerError, match="Metadata read failed"):
        malformed.list_messages({})


@pytest.mark.parametrize("database", ["ManifestMedex", "metadata-db", "x;DROP TABLE x"])
def test_explorer_rejects_unsafe_identifiers(database: str) -> None:
    with pytest.raises(ValueError, match="database configuration"):
        MessageExplorer(
            FakeDataApi(),
            FakeS3(),
            cluster_arn="cluster",
            secret_arn=CREDENTIAL_ARN,
            database=database,
            table_name="document_metadata",
            raw_bucket="raw",
            parsed_bucket="parsed",
        )


def test_explorer_rejects_invalid_storage_configuration() -> None:
    with pytest.raises(ValueError, match="storage configuration"):
        MessageExplorer(
            FakeDataApi(),
            FakeS3(),
            cluster_arn="cluster",
            secret_arn=CREDENTIAL_ARN,
            database="manifest_medex",
            table_name="document_metadata",
            raw_bucket="",
            parsed_bucket="parsed",
        )


class StubExplorer:
    def list_messages(self, query: dict[str, str]) -> dict[str, Any]:
        return {"items": [], "nextCursor": query.get("cursor"), "totalCount": 0}

    def execute_sql(self, sql: str, caller_sub: str) -> dict[str, Any]:
        assert caller_sub == "caller-subject"
        return {"columns": ["sql"], "rows": [[sql]], "numberOfRecordsUpdated": 0}

    def get_message(self, document_id: str) -> dict[str, Any]:
        return {"documentId": document_id}

    def get_body(self, document_id: str, variant: str, caller_sub: str) -> BodyResult:
        assert document_id == DOCUMENT_A
        assert caller_sub == "caller-subject"
        return BodyResult(content=variant.encode(), content_type="text/plain")


def test_handler_routes_authenticated_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(explorer_handler, "_RUNTIME_EXPLORER", StubExplorer())

    list_event = _authorized_event("GET /messages")
    list_event["queryStringParameters"] = {"cursor": "opaque"}
    list_response = handler(list_event, None)
    assert json.loads(list_response["body"]) == {
        "items": [],
        "nextCursor": "opaque",
        "totalCount": 0,
    }

    query_event = _authorized_event("POST /query")
    query_event["body"] = json.dumps({"sql": "SELECT 1"})
    query_response = handler(query_event, None)
    assert json.loads(query_response["body"]) == {
        "columns": ["sql"],
        "rows": [["SELECT 1"]],
        "numberOfRecordsUpdated": 0,
    }

    detail_event = _authorized_event("GET /messages/{documentId}")
    detail_event["pathParameters"] = {"documentId": DOCUMENT_A}
    detail_response = handler(detail_event, None)
    assert json.loads(detail_response["body"]) == {"documentId": DOCUMENT_A}

    body_event = _authorized_event("POST /messages/{documentId}/body")
    body_event["pathParameters"] = {"documentId": DOCUMENT_A}
    body_event["body"] = base64.b64encode(b'{"variant":"raw"}').decode()
    body_event["isBase64Encoded"] = True
    body_response = handler(body_event, None)
    assert body_response["isBase64Encoded"] is True
    assert base64.b64decode(body_response["body"]) == b"raw"


def test_handler_rejects_unauthenticated_invalid_and_unknown_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(explorer_handler, "_RUNTIME_EXPLORER", StubExplorer())
    unauthenticated = handler({"routeKey": "GET /messages"}, None)
    assert unauthenticated["statusCode"] == 401

    invalid_query = _authorized_event("GET /messages")
    invalid_query["queryStringParameters"] = "not-an-object"
    assert handler(invalid_query, None)["statusCode"] == 400

    invalid_body = _authorized_event("POST /messages/{documentId}/body")
    invalid_body["pathParameters"] = {"documentId": DOCUMENT_A}
    invalid_body["body"] = "not-json"
    assert handler(invalid_body, None)["statusCode"] == 400

    invalid_sql = _authorized_event("POST /query")
    invalid_sql["body"] = json.dumps({"sql": 123})
    assert handler(invalid_sql, None)["statusCode"] == 400

    unknown = _authorized_event("DELETE /messages")
    assert handler(unknown, None)["statusCode"] == 404


def test_handler_sanitizes_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingExplorer(StubExplorer):
        def list_messages(self, _query: dict[str, str]) -> dict[str, Any]:
            raise RuntimeError(SENSITIVE_FAILURE)

    monkeypatch.setattr(explorer_handler, "_RUNTIME_EXPLORER", FailingExplorer())
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")

    response = handler(_authorized_event("GET /messages"), None)

    assert response["statusCode"] == 500
    assert SENSITIVE_FAILURE not in response["body"]
    assert SENSITIVE_FAILURE not in caplog.text


def test_runtime_explorer_builds_and_caches_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients = {"rds-data": FakeDataApi(), "s3": FakeS3()}
    monkeypatch.setattr(explorer_handler, "_RUNTIME_EXPLORER", None)
    monkeypatch.setattr(explorer_handler, "_aws_client", clients.__getitem__)
    monkeypatch.setenv("METADATA_CLUSTER_ARN", "cluster")
    monkeypatch.setenv("METADATA_SECRET_ARN", "secret")
    monkeypatch.setenv("METADATA_DATABASE", "manifest_medex")
    monkeypatch.setenv("METADATA_TABLE", "document_metadata")
    monkeypatch.setenv("RAW_BUCKET", "raw-bucket")
    monkeypatch.setenv("PARSED_BUCKET", "parsed-bucket")
    monkeypatch.setenv("MAX_BODY_BYTES", "2048")

    first = explorer_handler._runtime_explorer()
    second = explorer_handler._runtime_explorer()

    assert first is second


def test_keyset_cursor_normalizes_offsetless_aurora_timestamp() -> None:
    data_api = FakeDataApi(
        [
            {
                "records": [
                    _list_record(DOCUMENT_A, "2026-08-20 12:00:00"),
                    _list_record(DOCUMENT_B, "2026-08-20 11:00:00"),
                ]
            },
            {"records": []},
        ]
    )
    explorer = _explorer(data_api)

    first = explorer.list_messages({"limit": "1"})
    second = explorer.list_messages({"limit": "1", "cursor": first["nextCursor"]})

    assert second == {"items": [], "nextCursor": None, "totalCount": 2}
    page_calls = [call for call in data_api.calls if "ORDER BY" in call["sql"]]
    call = page_calls[1]
    assert "CAST(:cursor_document_id AS TEXT)" in call["sql"]
    assert _parameters(call)["cursor_time"] == {"stringValue": "2026-08-20T12:00:00+00:00"}
    assert _parameters(call)["cursor_document_id"] == {"stringValue": DOCUMENT_A}


def test_execute_sql_passes_statement_and_returns_json_safe_rows(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sql = "SELECT 1 AS one, true AS enabled, decode('eA==', 'base64') AS payload"
    data_api = FakeDataApi(
        [
            {
                "columnMetadata": [
                    {"name": "one"},
                    {"label": "enabled"},
                    {"name": "payload"},
                    {},
                ],
                "records": [
                    [
                        {"longValue": 1},
                        {"booleanValue": True},
                        {"blobValue": b"x"},
                        {"arrayValue": {"longValues": [2, 3]}},
                    ]
                ],
                "numberOfRecordsUpdated": 0,
            }
        ]
    )
    caplog.set_level(logging.INFO, logger="src.explorer_handler")

    result = _explorer(data_api).execute_sql(sql, "caller-subject")

    assert result == {
        "columns": ["one", "enabled", "payload", "column_4"],
        "rows": [[1, True, "eA==", [2, 3]]],
        "numberOfRecordsUpdated": 0,
    }
    assert data_api.calls == [
        {
            "resourceArn": "cluster",
            "secretArn": CREDENTIAL_ARN,
            "database": "manifest_medex",
            "sql": sql,
            "includeResultMetadata": True,
        }
    ]
    audit = json.loads(caplog.messages[-1])
    assert audit["event"] == "sql_query_executed"
    assert audit["callerSub"] == "caller-subject"
    assert audit["rowCount"] == 1
    assert sql not in caplog.text


def test_execute_sql_supports_updates_and_rejects_invalid_or_failed_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    update_api = FakeDataApi([{"numberOfRecordsUpdated": 2}])
    result = _explorer(update_api).execute_sql("DELETE FROM example", "caller")
    assert result == {"columns": [], "rows": [], "numberOfRecordsUpdated": 2}

    explorer = _explorer(FakeDataApi())
    with pytest.raises(RequestError, match="invalid_sql"):
        explorer.execute_sql("   ", "caller")

    failed = _explorer(FakeDataApi(error=RuntimeError(SENSITIVE_FAILURE)))
    with pytest.raises(RequestError, match="sql_query_failed") as captured:
        failed.execute_sql("SELECT sensitive", "caller")
    assert captured.value.__cause__ is None
    assert "sensitive" not in str(captured.value)

    monkeypatch.setattr(explorer_handler, "MAX_SQL_RESULT_BYTES", 1)
    too_large = _explorer(
        FakeDataApi(
            [
                {
                    "columnMetadata": [{"name": "value"}],
                    "records": [[{"stringValue": "large"}]],
                }
            ]
        )
    )
    with pytest.raises(RequestError, match="sql_result_too_large"):
        too_large.execute_sql("SELECT 'large'", "caller")


# --- Reports routes -------------------------------------------------------------------

from src.message_search import FieldCatalog, SearchError, SearchRequestError  # noqa: E402
from src.report_catalog import CatalogError, CatalogRequestError  # noqa: E402
from src.report_facilities import FacilityDirectoryError  # noqa: E402
from src.report_query import QueryTester, QueryTestError  # noqa: E402
from src.report_runs import DownloadResult, RunError, RunRequestError  # noqa: E402


def _row_body() -> dict[str, Any]:
    return {
        "seq": 3,
        "label": "PID-7.1",
        "description": "date of birth present",
        "index": "hl7-messages-v1",
        "query": {"term": {"ROOT.PID._present": "1"}},
    }


class StubCatalog:
    def __init__(self) -> None:
        self.imported: list[dict[str, Any]] = []
        self.updated_rows: list[dict[str, Any]] = []
        self.added_rows: list[dict[str, Any]] = []
        self.deleted_rows: list[dict[str, Any]] = []
        self.added_sections: list[dict[str, Any]] = []
        self.history_calls: list[dict[str, Any]] = []
        self.deleted_reports: list[dict[str, Any]] = []
        self.replaced_reports: list[dict[str, Any]] = []

    def list_reports(self) -> list[dict[str, Any]]:
        return [{"reportId": "quality", "name": "Quality"}]

    def get_report(self, report_id: str) -> dict[str, Any]:
        return {
            "reportId": report_id,
            "definition": {"report_id": report_id},
            "editor": {"reportId": report_id, "sections": []},
        }

    def export_report(self, report_id: str) -> dict[str, Any]:
        definition = {"report_id": report_id}
        return {
            "reportId": report_id,
            "definition": definition,
            "text": json.dumps(definition, separators=(",", ":"), sort_keys=True),
        }

    def history(self, report_id: str, limit: int) -> list[dict[str, Any]]:
        self.history_calls.append({"reportId": report_id, "limit": limit})
        return [
            {
                "reportId": report_id,
                "sk": "AUDIT#2026-09-09T00:00:00+00:00#import",
                "action": "import",
                "updatedAt": "2026-09-09T00:00:00+00:00",
                "updatedBy": "caller-subject",
                "source": "api",
            }
        ]

    def import_report(
        self, definition_text: str, *, updated_by: str, source: str = "api"
    ) -> dict[str, Any]:
        self.imported.append(
            {"definition_text": definition_text, "updated_by": updated_by, "source": source}
        )
        return {"reportId": "quality", "name": "Quality", "updatedBy": updated_by}

    def update_row(
        self,
        report_id: str,
        section_storage_seq: int,
        row_storage_seq: int,
        row: dict[str, Any],
        *,
        expected_updated_at: str,
        updated_by: str,
    ) -> dict[str, Any]:
        self.updated_rows.append(
            {
                "reportId": report_id,
                "sectionStorageSeq": section_storage_seq,
                "rowStorageSeq": row_storage_seq,
                "row": row,
                "expected_updated_at": expected_updated_at,
                "updated_by": updated_by,
            }
        )
        return {
            "reportId": report_id,
            "sectionStorageSeq": section_storage_seq,
            "rowStorageSeq": row_storage_seq,
            "updatedAt": "2026-09-09T00:00:00+00:00",
        }

    def add_row(
        self,
        report_id: str,
        section_storage_seq: int,
        row: dict[str, Any],
        *,
        after_storage_seq: int | None = None,
        updated_by: str,
    ) -> dict[str, Any]:
        self.added_rows.append(
            {
                "reportId": report_id,
                "sectionStorageSeq": section_storage_seq,
                "row": row,
                "after_storage_seq": after_storage_seq,
                "updated_by": updated_by,
            }
        )
        return {
            "reportId": report_id,
            "sectionStorageSeq": section_storage_seq,
            "rowStorageSeq": 30,
        }

    def delete_row(
        self,
        report_id: str,
        section_storage_seq: int,
        row_storage_seq: int,
        *,
        expected_updated_at: str,
        updated_by: str,
    ) -> None:
        self.deleted_rows.append(
            {
                "reportId": report_id,
                "sectionStorageSeq": section_storage_seq,
                "rowStorageSeq": row_storage_seq,
                "expected_updated_at": expected_updated_at,
                "updated_by": updated_by,
            }
        )

    def add_section(
        self,
        report_id: str,
        section: dict[str, Any],
        *,
        after_storage_seq: int | None = None,
        updated_by: str,
    ) -> dict[str, Any]:
        self.added_sections.append(
            {
                "reportId": report_id,
                "section": section,
                "after_storage_seq": after_storage_seq,
                "updated_by": updated_by,
            }
        )
        return {"reportId": report_id, "sectionStorageSeq": 30}

    def delete_report(self, report_id: str, *, updated_by: str) -> dict[str, Any]:
        self.deleted_reports.append({"reportId": report_id, "updated_by": updated_by})
        return {"reportId": report_id, "deleted": True}

    def replace_report(
        self,
        definition_text: str,
        *,
        expected_updated_at: str,
        updated_by: str,
    ) -> dict[str, Any]:
        self.replaced_reports.append(
            {
                "definition_text": definition_text,
                "expected_updated_at": expected_updated_at,
                "updated_by": updated_by,
            }
        )
        return {
            "reportId": "quality",
            "name": "Quality",
            "description": "",
            "updatedAt": "2026-09-09T00:00:00+00:00",
            "updatedBy": updated_by,
        }


class StubRuns:
    def __init__(self) -> None:
        self.started: list[tuple[str, str, str, list[str], str]] = []

    def start_run(
        self,
        report_id: str,
        from_time: str,
        to_time: str,
        partition_values: list[str],
        caller_sub: str,
    ) -> dict[str, Any]:
        self.started.append((report_id, from_time, to_time, partition_values, caller_sub))
        return {"runId": "run-1", "reportId": report_id, "status": "running"}

    def list_runs_for_report(self, report_id: str) -> list[dict[str, Any]]:
        return [{"runId": "run-1", "reportId": report_id, "status": "running"}]

    def get_run_status(self, run_id: str) -> dict[str, Any]:
        return {"runId": run_id, "status": "complete", "downloadReady": True}

    def download_output(self, run_id: str, caller_sub: str) -> DownloadResult:
        assert run_id == "run-1"
        assert caller_sub == "caller-subject"
        return DownloadResult(content=b"PK\x03\x04archive", content_type="application/zip")


class StubFacilities:
    def list_facilities(self) -> list[str]:
        return ["facility-a", "facility-b"]


class FakeSearchTransport:
    def __init__(
        self,
        *,
        status: int = 200,
        response: dict[str, Any] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.status = status
        self.response = response if response is not None else {"hits": {"total": {"value": 7}}}
        self.error = error
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        if self.error is not None:
            raise self.error
        return self.status, self.response


def _install_report_stubs(
    monkeypatch: pytest.MonkeyPatch,
    *,
    catalog: object | None = None,
    runs: object | None = None,
    facilities: object | None = None,
    query_tester: object | None = None,
) -> tuple[Any, Any, Any, Any]:
    catalog = catalog or StubCatalog()
    runs = runs or StubRuns()
    facilities = facilities or StubFacilities()
    query_tester = query_tester or QueryTester(FakeSearchTransport(), StubFacilities())
    monkeypatch.setattr(explorer_handler, "_RUNTIME_CATALOG", catalog)
    monkeypatch.setattr(explorer_handler, "_RUNTIME_RUNS", runs)
    monkeypatch.setattr(explorer_handler, "_RUNTIME_FACILITIES", facilities)
    monkeypatch.setattr(explorer_handler, "_RUNTIME_QUERY_TESTER", query_tester)
    return catalog, runs, facilities, query_tester


def test_handler_lists_and_reads_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_report_stubs(monkeypatch)

    list_response = handler(_authorized_event("GET /reports"), None)
    assert list_response["statusCode"] == 200
    assert json.loads(list_response["body"]) == {
        "items": [{"reportId": "quality", "name": "Quality"}]
    }

    detail_event = _authorized_event("GET /reports/{id}")
    detail_event["pathParameters"] = {"id": "quality"}
    detail_response = handler(detail_event, None)
    body = json.loads(detail_response["body"])
    assert body["reportId"] == "quality"
    assert body["definition"] == {"report_id": "quality"}
    assert body["editor"]["reportId"] == "quality"


def test_handler_imports_whole_definition_with_caller_sub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    import_event = _authorized_event("POST /reports/import")
    import_event["body"] = json.dumps({"report_id": "quality", "name": "Quality"})

    response = handler(import_event, None)

    assert response["statusCode"] == 201
    assert json.loads(response["body"])["reportId"] == "quality"
    assert catalog.imported[0]["updated_by"] == "caller-subject"
    # The API import path always tags provenance as source='api' rather than the catalog default.
    assert catalog.imported[0]["source"] == "api"
    # The whole definition is forwarded as canonical JSON for whole-definition validation.
    assert json.loads(catalog.imported[0]["definition_text"]) == {
        "name": "Quality",
        "report_id": "quality",
    }


def test_handler_import_maps_invalid_definition(monkeypatch: pytest.MonkeyPatch) -> None:
    class RejectingCatalog(StubCatalog):
        def import_report(
            self,
            _definition_text: str,
            *,
            updated_by: str,  # noqa: ARG002
            source: str = "api",  # noqa: ARG002
        ) -> dict[str, Any]:
            raise CatalogRequestError(400, "invalid_definition")

    _install_report_stubs(monkeypatch, catalog=RejectingCatalog())
    import_event = _authorized_event("POST /reports/import")
    import_event["body"] = json.dumps({"report_id": "quality"})

    response = handler(import_event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_definition"}


def test_handler_exports_clean_definition_json(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_report_stubs(monkeypatch)
    export_event = _authorized_event("GET /reports/{id}/export")
    export_event["pathParameters"] = {"id": "quality"}

    response = handler(export_event, None)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["reportId"] == "quality"
    assert body["definition"] == {"report_id": "quality"}
    assert body["text"] == '{"report_id":"quality"}'


def test_handler_returns_report_history_with_default_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("GET /reports/{id}/history")
    event["pathParameters"] = {"id": "quality"}

    response = handler(event, None)

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert [entry["action"] for entry in body["items"]] == ["import"]
    assert body["items"][0]["source"] == "api"
    # An absent limit falls back to the shared catalog default rather than an ad-hoc value.
    assert catalog.history_calls == [{"reportId": "quality", "limit": 50}]


def test_handler_report_history_forwards_explicit_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("GET /reports/{id}/history")
    event["pathParameters"] = {"id": "quality"}
    event["queryStringParameters"] = {"limit": "10"}

    response = handler(event, None)

    assert response["statusCode"] == 200
    # The scalar query-string limit is converted to an integer before reaching the catalog.
    assert catalog.history_calls == [{"reportId": "quality", "limit": 10}]


@pytest.mark.parametrize("value", ["abc", "1.5", "", "ten"])
def test_handler_report_history_rejects_non_integer_limit(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("GET /reports/{id}/history")
    event["pathParameters"] = {"id": "quality"}
    event["queryStringParameters"] = {"limit": value}

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_limit"}
    # A non-integer scalar never reaches the catalog.
    assert catalog.history_calls == []


def test_handler_report_history_surfaces_catalog_limit_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BoundedCatalog(StubCatalog):
        def history(self, report_id: str, limit: int) -> list[dict[str, Any]]:
            self.history_calls.append({"reportId": report_id, "limit": limit})
            if not 1 <= limit <= 200:
                raise CatalogRequestError(400, "invalid_limit")
            return []

    catalog = BoundedCatalog()
    _install_report_stubs(monkeypatch, catalog=catalog)
    event = _authorized_event("GET /reports/{id}/history")
    event["pathParameters"] = {"id": "quality"}
    event["queryStringParameters"] = {"limit": "999"}

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_limit"}
    # The handler forwards the parsed integer and lets the catalog own the range check.
    assert catalog.history_calls == [{"reportId": "quality", "limit": 999}]


def test_handler_report_history_requires_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = {"routeKey": "GET /reports/{id}/history", "pathParameters": {"id": "quality"}}

    response = handler(event, None)

    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "authentication_required"}
    assert catalog.history_calls == []


def test_handler_report_history_rejects_non_object_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("GET /reports/{id}/history")
    event["pathParameters"] = {"id": "quality"}
    event["queryStringParameters"] = "not-an-object"

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_query"}
    assert catalog.history_calls == []


def test_handler_report_history_maps_catalog_read_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingCatalog(StubCatalog):
        def history(
            self,
            report_id: str,  # noqa: ARG002
            limit: int,  # noqa: ARG002
        ) -> list[dict[str, Any]]:
            raise CatalogError(SENSITIVE_FAILURE)

    _install_report_stubs(monkeypatch, catalog=FailingCatalog())
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")
    event = _authorized_event("GET /reports/{id}/history")
    event["pathParameters"] = {"id": "quality"}

    response = handler(event, None)

    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {"error": "explorer_request_failed"}
    # A sanitized internal catalog failure never leaks its message to the caller or logs.
    assert SENSITIVE_FAILURE not in response["body"]
    assert SENSITIVE_FAILURE not in caplog.text


def test_handler_replaces_whole_report_with_canonical_definition_and_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    put_event = _authorized_event("PUT /reports/{id}")
    put_event["pathParameters"] = {"id": "quality"}
    put_event["body"] = json.dumps(
        {
            "definition": {"report_id": "quality", "name": "Quality"},
            "updated_at": "2026-09-08T00:00:00+00:00",
        }
    )

    response = handler(put_event, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"])["reportId"] == "quality"
    recorded = catalog.replaced_reports[0]
    assert recorded["expected_updated_at"] == "2026-09-08T00:00:00+00:00"
    assert recorded["updated_by"] == "caller-subject"
    # The definition is forwarded as the same canonical JSON the import path uses.
    assert recorded["definition_text"] == '{"name":"Quality","report_id":"quality"}'
    assert json.loads(recorded["definition_text"]) == {
        "name": "Quality",
        "report_id": "quality",
    }


def test_handler_replace_report_rejects_path_definition_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    put_event = _authorized_event("PUT /reports/{id}")
    put_event["pathParameters"] = {"id": "quality"}
    put_event["body"] = json.dumps(
        {
            "definition": {"report_id": "other", "name": "Quality"},
            "updated_at": "2026-09-08T00:00:00+00:00",
        }
    )

    response = handler(put_event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "report_id_mismatch"}
    # A mismatched definition never reaches the catalog.
    assert catalog.replaced_reports == []


@pytest.mark.parametrize(
    "body",
    [
        {"updated_at": "lock"},
        {"definition": "not-an-object", "updated_at": "lock"},
        {"definition": {"report_id": "quality"}, "updated_at": 123},
        {"definition": {"report_id": "quality"}},
    ],
)
def test_handler_replace_report_rejects_malformed_body(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    put_event = _authorized_event("PUT /reports/{id}")
    put_event["pathParameters"] = {"id": "quality"}
    put_event["body"] = json.dumps(body)

    response = handler(put_event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_report_update"}
    assert catalog.replaced_reports == []


def test_handler_replace_report_conflict_maps_to_409(monkeypatch: pytest.MonkeyPatch) -> None:
    class ConflictCatalog(StubCatalog):
        def replace_report(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise CatalogRequestError(409, "edit_conflict")

    _install_report_stubs(monkeypatch, catalog=ConflictCatalog())
    put_event = _authorized_event("PUT /reports/{id}")
    put_event["pathParameters"] = {"id": "quality"}
    put_event["body"] = json.dumps(
        {
            "definition": {"report_id": "quality", "name": "Quality"},
            "updated_at": "stale-lock",
        }
    )

    response = handler(put_event, None)

    assert response["statusCode"] == 409
    assert json.loads(response["body"]) == {"error": "edit_conflict"}


def test_handler_replace_report_maps_invalid_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RejectingCatalog(StubCatalog):
        def replace_report(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise CatalogRequestError(400, "invalid_definition")

    _install_report_stubs(monkeypatch, catalog=RejectingCatalog())
    put_event = _authorized_event("PUT /reports/{id}")
    put_event["pathParameters"] = {"id": "quality"}
    put_event["body"] = json.dumps(
        {
            "definition": {"report_id": "quality", "name": "Quality"},
            "updated_at": "2026-09-08T00:00:00+00:00",
        }
    )

    response = handler(put_event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_definition"}


def test_handler_replace_report_maps_catalog_save_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingCatalog(StubCatalog):
        def replace_report(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise CatalogError(SENSITIVE_FAILURE)

    _install_report_stubs(monkeypatch, catalog=FailingCatalog())
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")
    put_event = _authorized_event("PUT /reports/{id}")
    put_event["pathParameters"] = {"id": "quality"}
    put_event["body"] = json.dumps(
        {
            "definition": {"report_id": "quality", "name": "Quality"},
            "updated_at": "2026-09-08T00:00:00+00:00",
        }
    )

    response = handler(put_event, None)

    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {"error": "explorer_request_failed"}
    # A sanitized internal catalog failure never leaks its message to the caller or logs.
    assert SENSITIVE_FAILURE not in response["body"]
    assert SENSITIVE_FAILURE not in caplog.text


def test_handler_replace_report_requires_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = {
        "routeKey": "PUT /reports/{id}",
        "pathParameters": {"id": "quality"},
        "body": json.dumps({"definition": {"report_id": "quality"}, "updated_at": "lock"}),
    }

    response = handler(event, None)

    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "authentication_required"}
    assert catalog.replaced_reports == []


def test_handler_deletes_whole_report_returns_no_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    delete_event = _authorized_event("DELETE /reports/{id}")
    delete_event["pathParameters"] = {"id": "quality"}

    response = handler(delete_event, None)

    assert response["statusCode"] == 204
    assert response["body"] == ""
    assert response["headers"]["cache-control"] == "no-store"
    assert response["headers"]["x-content-type-options"] == "nosniff"
    assert catalog.deleted_reports == [{"reportId": "quality", "updated_by": "caller-subject"}]


def test_handler_delete_report_is_idempotent_and_still_returns_204(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MissingReportCatalog(StubCatalog):
        def delete_report(self, report_id: str, *, updated_by: str) -> dict[str, Any]:
            self.deleted_reports.append({"reportId": report_id, "updated_by": updated_by})
            # A repeat delete finds no live META and reports deleted=False without error.
            return {"reportId": report_id, "deleted": False}

    catalog = MissingReportCatalog()
    _install_report_stubs(monkeypatch, catalog=catalog)
    delete_event = _authorized_event("DELETE /reports/{id}")
    delete_event["pathParameters"] = {"id": "quality"}

    response = handler(delete_event, None)

    assert response["statusCode"] == 204
    assert response["body"] == ""
    assert catalog.deleted_reports == [{"reportId": "quality", "updated_by": "caller-subject"}]


def test_handler_delete_report_requires_authentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = {"routeKey": "DELETE /reports/{id}", "pathParameters": {"id": "quality"}}

    response = handler(event, None)

    assert response["statusCode"] == 401
    assert json.loads(response["body"]) == {"error": "authentication_required"}
    assert catalog.deleted_reports == []


def test_handler_delete_report_maps_catalog_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingCatalog(StubCatalog):
        def delete_report(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise CatalogError(SENSITIVE_FAILURE)

    _install_report_stubs(monkeypatch, catalog=FailingCatalog())
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")
    delete_event = _authorized_event("DELETE /reports/{id}")
    delete_event["pathParameters"] = {"id": "quality"}

    response = handler(delete_event, None)

    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {"error": "explorer_request_failed"}
    assert SENSITIVE_FAILURE not in response["body"]
    assert SENSITIVE_FAILURE not in caplog.text


def test_handler_updates_row_with_storage_seqs_and_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("PUT /reports/{id}/sections/{sseq}/rows/{rseq}")
    event["pathParameters"] = {"id": "quality", "sseq": "10", "rseq": "20"}
    event["body"] = json.dumps({"row": _row_body(), "updated_at": "2026-09-08T00:00:00+00:00"})

    response = handler(event, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"])["rowStorageSeq"] == 20
    recorded = catalog.updated_rows[0]
    assert recorded["sectionStorageSeq"] == 10
    assert recorded["rowStorageSeq"] == 20
    assert recorded["expected_updated_at"] == "2026-09-08T00:00:00+00:00"
    assert recorded["updated_by"] == "caller-subject"
    assert recorded["row"] == _row_body()


def test_handler_update_row_conflict_maps_to_409(monkeypatch: pytest.MonkeyPatch) -> None:
    class ConflictCatalog(StubCatalog):
        def update_row(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise CatalogRequestError(409, "edit_conflict")

    _install_report_stubs(monkeypatch, catalog=ConflictCatalog())
    event = _authorized_event("PUT /reports/{id}/sections/{sseq}/rows/{rseq}")
    event["pathParameters"] = {"id": "quality", "sseq": "10", "rseq": "20"}
    event["body"] = json.dumps({"row": _row_body(), "updated_at": "lock"})

    response = handler(event, None)

    assert response["statusCode"] == 409
    assert json.loads(response["body"]) == {"error": "edit_conflict"}


@pytest.mark.parametrize(
    "body",
    [
        {"updated_at": "lock"},
        {"row": "not-an-object", "updated_at": "lock"},
        {"row": {}, "updated_at": 123},
    ],
)
def test_handler_update_row_rejects_malformed_body(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    _install_report_stubs(monkeypatch)
    event = _authorized_event("PUT /reports/{id}/sections/{sseq}/rows/{rseq}")
    event["pathParameters"] = {"id": "quality", "sseq": "10", "rseq": "20"}
    event["body"] = json.dumps(body)

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_row_edit"}


@pytest.mark.parametrize("value", ["abc", "0", "-1", "010", ""])
def test_handler_rejects_non_positive_storage_seq_path(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    _install_report_stubs(monkeypatch)
    event = _authorized_event("PUT /reports/{id}/sections/{sseq}/rows/{rseq}")
    event["pathParameters"] = {"id": "quality", "sseq": value, "rseq": "20"}
    event["body"] = json.dumps({"row": _row_body(), "updated_at": "lock"})

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_storage_seq"}


def test_handler_adds_row_with_optional_after_seq(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("POST /reports/{id}/sections/{sseq}/rows")
    event["pathParameters"] = {"id": "quality", "sseq": "10"}
    event["body"] = json.dumps({"row": _row_body(), "after_seq": 10})

    response = handler(event, None)

    assert response["statusCode"] == 201
    assert json.loads(response["body"])["rowStorageSeq"] == 30
    recorded = catalog.added_rows[0]
    assert recorded["sectionStorageSeq"] == 10
    assert recorded["after_storage_seq"] == 10
    assert recorded["updated_by"] == "caller-subject"


def test_handler_add_row_rejects_missing_row_and_bad_after_seq(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_report_stubs(monkeypatch)
    missing = _authorized_event("POST /reports/{id}/sections/{sseq}/rows")
    missing["pathParameters"] = {"id": "quality", "sseq": "10"}
    missing["body"] = json.dumps({"after_seq": 10})
    assert json.loads(handler(missing, None)["body"]) == {"error": "invalid_row_add"}

    bad_after = _authorized_event("POST /reports/{id}/sections/{sseq}/rows")
    bad_after["pathParameters"] = {"id": "quality", "sseq": "10"}
    bad_after["body"] = json.dumps({"row": _row_body(), "after_seq": 0})
    assert json.loads(handler(bad_after, None)["body"]) == {"error": "invalid_storage_seq"}


def test_handler_deletes_row_returns_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("DELETE /reports/{id}/sections/{sseq}/rows/{rseq}")
    event["pathParameters"] = {"id": "quality", "sseq": "10", "rseq": "20"}
    event["body"] = json.dumps({"updated_at": "2026-09-08T00:00:00+00:00"})

    response = handler(event, None)

    assert response["statusCode"] == 204
    assert response["body"] == ""
    recorded = catalog.deleted_rows[0]
    assert recorded["sectionStorageSeq"] == 10
    assert recorded["rowStorageSeq"] == 20
    assert recorded["expected_updated_at"] == "2026-09-08T00:00:00+00:00"
    assert recorded["updated_by"] == "caller-subject"


def test_handler_delete_row_rejects_missing_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_report_stubs(monkeypatch)
    event = _authorized_event("DELETE /reports/{id}/sections/{sseq}/rows/{rseq}")
    event["pathParameters"] = {"id": "quality", "sseq": "10", "rseq": "20"}
    event["body"] = json.dumps({})

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_row_delete"}


def test_handler_adds_section_forwards_name_seq_and_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _runs, _facilities, _tester = _install_report_stubs(monkeypatch)
    event = _authorized_event("POST /reports/{id}/sections")
    event["pathParameters"] = {"id": "quality"}
    event["body"] = json.dumps({"name": "New Section", "seq": 3, "after_seq": 10})

    response = handler(event, None)

    assert response["statusCode"] == 201
    assert json.loads(response["body"])["sectionStorageSeq"] == 30
    recorded = catalog.added_sections[0]
    assert recorded["section"] == {"name": "New Section", "seq": 3}
    assert recorded["after_storage_seq"] == 10
    assert recorded["updated_by"] == "caller-subject"


def test_handler_add_section_defers_shape_validation_to_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RejectingCatalog(StubCatalog):
        def add_section(
            self,
            _report_id: str,
            _section: dict[str, Any],
            *,
            after_storage_seq: int | None = None,  # noqa: ARG002
            updated_by: str,  # noqa: ARG002
        ) -> dict[str, Any]:
            raise CatalogRequestError(400, "invalid_definition")

    _install_report_stubs(monkeypatch, catalog=RejectingCatalog())
    event = _authorized_event("POST /reports/{id}/sections")
    event["pathParameters"] = {"id": "quality"}
    event["body"] = json.dumps({"name": "New Section"})

    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_definition"}


def test_handler_routes_report_run_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    _catalog, runs, _facilities, _tester = _install_report_stubs(monkeypatch)

    start_event = _authorized_event("POST /reports/{id}/runs")
    start_event["pathParameters"] = {"id": "quality"}
    start_event["body"] = json.dumps(
        {
            "from": "2026-08-01T00:00:00Z",
            "to": "2026-08-31T00:00:00Z",
            "partitions": ["facility-a", "facility-b"],
        }
    )
    start_response = handler(start_event, None)
    assert start_response["statusCode"] == 202
    assert json.loads(start_response["body"])["runId"] == "run-1"
    assert runs.started == [
        (
            "quality",
            "2026-08-01T00:00:00Z",
            "2026-08-31T00:00:00Z",
            ["facility-a", "facility-b"],
            "caller-subject",
        )
    ]

    list_event = _authorized_event("GET /reports/{id}/runs")
    list_event["pathParameters"] = {"id": "quality"}
    list_response = handler(list_event, None)
    assert json.loads(list_response["body"]) == {
        "items": [{"runId": "run-1", "reportId": "quality", "status": "running"}]
    }

    status_event = _authorized_event("GET /runs/{runId}")
    status_event["pathParameters"] = {"runId": "run-1"}
    status_response = handler(status_event, None)
    assert json.loads(status_response["body"])["status"] == "complete"


@pytest.mark.parametrize(
    "body",
    [
        {"from": "2026-08-01T00:00:00Z", "to": "2026-08-31T00:00:00Z"},
        {"from": 1, "to": "2026-08-31T00:00:00Z", "partitions": ["a"]},
        {"from": "2026-08-01T00:00:00Z", "to": "2026-08-31T00:00:00Z", "partitions": "a"},
    ],
)
def test_handler_start_run_rejects_malformed_body(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any]
) -> None:
    _install_report_stubs(monkeypatch)
    start_event = _authorized_event("POST /reports/{id}/runs")
    start_event["pathParameters"] = {"id": "quality"}
    start_event["body"] = json.dumps(body)

    response = handler(start_event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_run_request"}


def test_handler_downloads_run_output_as_no_store_zip_attachment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_report_stubs(monkeypatch)
    download_event = _authorized_event("GET /runs/{runId}/download")
    download_event["pathParameters"] = {"runId": "run-1"}

    response = handler(download_event, None)

    assert response["statusCode"] == 200
    assert response["isBase64Encoded"] is True
    assert base64.b64decode(response["body"]) == b"PK\x03\x04archive"
    assert response["headers"]["content-type"] == "application/zip"
    assert response["headers"]["cache-control"] == "no-store"
    assert response["headers"]["content-disposition"] == 'attachment; filename="run-1.zip"'
    assert response["headers"]["x-content-type-options"] == "nosniff"


def test_handler_routes_facilities(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_report_stubs(monkeypatch)

    response = handler(_authorized_event("GET /facilities"), None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"facilities": ["facility-a", "facility-b"]}


def test_handler_query_test_without_injection_uses_size_zero_search(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = FakeSearchTransport(response={"hits": {"total": {"value": 4}}})
    tester = QueryTester(transport, StubFacilities())
    _install_report_stubs(monkeypatch, query_tester=tester)
    caplog.set_level(logging.INFO, logger="src.explorer_handler")

    event = _authorized_event("POST /query-test")
    event["body"] = json.dumps(
        {"index": "hl7-messages-v1", "query": {"term": {"ROOT.PID._present": "1"}}}
    )
    response = handler(event, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"count": 4}
    method, path, body = transport.calls[0]
    assert method == "POST"
    assert path == "/hl7-messages-v1/_search"
    assert body is not None
    sent = json.loads(body.decode())
    assert sent["size"] == 0
    assert sent["track_total_hits"] is True
    filters = sent["query"]["bool"]["filter"]
    assert {"term": {"ROOT.PID._present": "1"}} in filters
    # No facility term and no time range are injected when neither is supplied.
    assert not any("range" in clause for clause in filters)
    assert not any("sourceFacilityId" in clause.get("term", {}) for clause in filters)
    # The clinical query body and count must never be logged.
    assert "ROOT.PID._present" not in caplog.text


def test_handler_query_test_injects_facility_and_time_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FakeSearchTransport(response={"hits": {"total": {"value": 9}}})
    tester = QueryTester(transport, StubFacilities())
    _install_report_stubs(monkeypatch, query_tester=tester)

    event = _authorized_event("POST /query-test")
    event["body"] = json.dumps(
        {
            "index": "hl7-messages-v1",
            "query": {"term": {"ROOT.PID._present": "1"}},
            "facility": "facility-a",
            "from": "2026-08-01T00:00:00Z",
            "to": "2026-08-31T00:00:00Z",
        }
    )
    response = handler(event, None)

    assert json.loads(response["body"]) == {"count": 9}
    _method, _path, body = transport.calls[0]
    assert body is not None
    filters = json.loads(body.decode())["query"]["bool"]["filter"]
    assert {"term": {"ROOT.PID._present": "1"}} in filters
    assert {"term": {"sourceFacilityId": "facility-a"}} in filters
    assert {
        "range": {"messageTime": {"gte": "2026-08-01T00:00:00Z", "lt": "2026-08-31T00:00:00Z"}}
    } in filters


def test_handler_query_test_rejects_unknown_facility(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeSearchTransport()
    tester = QueryTester(transport, StubFacilities())
    _install_report_stubs(monkeypatch, query_tester=tester)

    event = _authorized_event("POST /query-test")
    event["body"] = json.dumps(
        {
            "index": "hl7-messages-v1",
            "query": {"term": {"ROOT.PID._present": "1"}},
            "facility": "arbitrary free text",
        }
    )
    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "unknown_facility"}
    # A rejected facility must never reach the search backend.
    assert transport.calls == []


def test_handler_query_test_rejects_reserved_field_guardrail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = FakeSearchTransport()
    tester = QueryTester(transport, StubFacilities())
    _install_report_stubs(monkeypatch, query_tester=tester)

    event = _authorized_event("POST /query-test")
    event["body"] = json.dumps(
        {"index": "hl7-messages-v1", "query": {"term": {"sourceFacilityId": "facility-a"}}}
    )
    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "invalid_query"}
    assert transport.calls == []


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ({"index": "hl7-messages-v1"}, "placeholder_query_not_implemented"),
        ({"index": "unknown-index", "query": {"term": {"a": "b"}}}, "invalid_query"),
        ({"index": "hl7-messages-v1", "query": {}}, "invalid_query"),
        (
            {
                "index": "hl7-messages-v1",
                "query": {"term": {"a": "b"}},
                "from": "2026-08-01T00:00:00Z",
            },
            "invalid_time_range",
        ),
        (
            {
                "index": "hl7-messages-v1",
                "query": {"term": {"a": "b"}},
                "from": "2026-08-31T00:00:00Z",
                "to": "2026-08-01T00:00:00Z",
            },
            "invalid_time_range",
        ),
        (
            {
                "index": "hl7-messages-v1",
                "query": {"term": {"a": "b"}},
                "from": "not-a-time",
                "to": "2026-08-01T00:00:00Z",
            },
            "invalid_time",
        ),
    ],
)
def test_handler_query_test_rejects_invalid_request(
    monkeypatch: pytest.MonkeyPatch, body: dict[str, Any], code: str
) -> None:
    transport = FakeSearchTransport()
    tester = QueryTester(transport, StubFacilities())
    _install_report_stubs(monkeypatch, query_tester=tester)

    event = _authorized_event("POST /query-test")
    event["body"] = json.dumps(body)
    response = handler(event, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": code}
    assert transport.calls == []


def test_handler_query_test_sanitizes_backend_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = FakeSearchTransport(error=RuntimeError(SENSITIVE_FAILURE))
    tester = QueryTester(transport, StubFacilities())
    _install_report_stubs(monkeypatch, query_tester=tester)
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")

    event = _authorized_event("POST /query-test")
    event["body"] = json.dumps(
        {"index": "hl7-messages-v1", "query": {"term": {"ROOT.PID._present": "1"}}}
    )
    response = handler(event, None)

    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {"error": "explorer_request_failed"}
    assert SENSITIVE_FAILURE not in response["body"]
    assert SENSITIVE_FAILURE not in caplog.text


def test_handler_maps_typed_report_request_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class MissingCatalog(StubCatalog):
        def get_report(self, _report_id: str) -> dict[str, Any]:
            raise CatalogRequestError(404, "report_not_found")

    class ConflictRuns(StubRuns):
        def download_output(self, _run_id: str, _caller_sub: str) -> DownloadResult:
            raise RunRequestError(409, "run_output_not_ready")

    _install_report_stubs(monkeypatch, catalog=MissingCatalog(), runs=ConflictRuns())

    detail_event = _authorized_event("GET /reports/{id}")
    detail_event["pathParameters"] = {"id": "quality"}
    detail_response = handler(detail_event, None)
    assert detail_response["statusCode"] == 404
    assert json.loads(detail_response["body"]) == {"error": "report_not_found"}

    download_event = _authorized_event("GET /runs/{runId}/download")
    download_event["pathParameters"] = {"runId": "run-1"}
    download_response = handler(download_event, None)
    assert download_response["statusCode"] == 409
    assert json.loads(download_response["body"]) == {"error": "run_output_not_ready"}


def test_handler_sanitizes_internal_report_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingCatalog(StubCatalog):
        def list_reports(self) -> list[dict[str, Any]]:
            raise CatalogError(SENSITIVE_FAILURE)

    class FailingRuns(StubRuns):
        def get_run_status(self, _run_id: str) -> dict[str, Any]:
            raise RunError(SENSITIVE_FAILURE)

    class FailingFacilities(StubFacilities):
        def list_facilities(self) -> list[str]:
            raise FacilityDirectoryError(SENSITIVE_FAILURE)

    _install_report_stubs(
        monkeypatch,
        catalog=FailingCatalog(),
        runs=FailingRuns(),
        facilities=FailingFacilities(),
    )
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")

    catalog_response = handler(_authorized_event("GET /reports"), None)
    assert catalog_response["statusCode"] == 500
    assert json.loads(catalog_response["body"]) == {"error": "explorer_request_failed"}

    status_event = _authorized_event("GET /runs/{runId}")
    status_event["pathParameters"] = {"runId": "run-1"}
    assert handler(status_event, None)["statusCode"] == 500

    facilities_response = handler(_authorized_event("GET /facilities"), None)
    assert facilities_response["statusCode"] == 500

    assert SENSITIVE_FAILURE not in caplog.text
    for response in (catalog_response, facilities_response):
        assert SENSITIVE_FAILURE not in response["body"]


def test_handler_query_test_maps_sanitized_error_class() -> None:
    # QueryTestError is a plain sanitized failure that carries no caller-safe code.
    assert not hasattr(QueryTestError("boom"), "status_code")


def test_report_routes_require_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_report_stubs(monkeypatch)
    for route in ("GET /reports", "POST /reports/import", "POST /query-test"):
        assert handler({"routeKey": route}, None)["statusCode"] == 401


def test_runtime_report_services_build_and_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    clients = {
        "s3": FakeS3(),
        "dynamodb": object(),
        "lambda": object(),
    }
    monkeypatch.setattr(explorer_handler, "_RUNTIME_CATALOG", None)
    monkeypatch.setattr(explorer_handler, "_RUNTIME_RUNS", None)
    monkeypatch.setattr(explorer_handler, "_RUNTIME_FACILITIES", None)
    monkeypatch.setattr(explorer_handler, "_RUNTIME_QUERY_TESTER", None)
    monkeypatch.setattr(explorer_handler, "_aws_client", clients.__getitem__)
    monkeypatch.setenv("REPORT_BUCKET", "report-bucket")
    monkeypatch.setenv("REPORT_CATALOG_TABLE", "report-catalog")
    monkeypatch.setenv("REPORT_RUNS_TABLE", "report-runs")
    monkeypatch.setenv("REPORT_RUNNER_FUNCTION", "report-runner")
    monkeypatch.setenv("OPENSEARCH_ENDPOINT", "https://example.aoss.us-west-2.amazonaws.com")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    assert explorer_handler._runtime_catalog() is explorer_handler._runtime_catalog()
    assert explorer_handler._runtime_runs() is explorer_handler._runtime_runs()
    assert explorer_handler._runtime_facilities() is explorer_handler._runtime_facilities()
    assert explorer_handler._runtime_query_tester() is explorer_handler._runtime_query_tester()


# --- Metadata attribute-search routes -------------------------------------------------


class StubMessageSearch:
    def __init__(
        self,
        *,
        result: dict[str, Any] | None = None,
        fields: list[str] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.result = (
            result if result is not None else {"items": [], "total": 0, "nextCursor": None}
        )
        self.fields = fields if fields is not None else ["documentId", "messageType"]
        self.error = error
        self.searches: list[dict[str, Any]] = []
        self.field_lookups: list[str] = []

    def search(
        self,
        *,
        index: Any,
        caller_sub: Any,
        filters: Any = None,
        facility: Any = None,
        from_time: Any = None,
        to_time: Any = None,
        limit: Any = None,
        cursor: Any = None,
    ) -> dict[str, Any]:
        self.searches.append(
            {
                "index": index,
                "caller_sub": caller_sub,
                "filters": filters,
                "facility": facility,
                "from_time": from_time,
                "to_time": to_time,
                "limit": limit,
                "cursor": cursor,
            }
        )
        if self.error is not None:
            raise self.error
        return self.result

    def field_catalog(self, index: str) -> FieldCatalog:
        self.field_lookups.append(index)
        if self.error is not None:
            raise self.error
        return FieldCatalog(
            present=frozenset(self.fields),
            text_fields=frozenset(),
            keyword_subfields=frozenset(),
        )


def test_handler_runs_metadata_search_and_lists_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = StubMessageSearch(
        result={
            "items": [{"documentId": DOCUMENT_A, "messageType": "ORU"}],
            "total": 1,
            "nextCursor": None,
        },
        fields=["messageType", "documentId", "sourceFacilityId"],
    )
    monkeypatch.setattr(explorer_handler, "_RUNTIME_MESSAGE_SEARCH", service)

    search_event = _authorized_event("POST /search")
    search_event["body"] = json.dumps(
        {
            "index": "hl7-messages-v1",
            "filters": [{"field": "messageType", "operator": "equals", "value": "ORU"}],
            "facility": "facility-a",
            "from": "2026-08-01T00:00:00+00:00",
            "to": "2026-09-01T00:00:00+00:00",
            "limit": 25,
            "cursor": "opaque",
        }
    )
    search_response = handler(search_event, None)
    assert search_response["statusCode"] == 200
    assert json.loads(search_response["body"]) == {
        "items": [{"documentId": DOCUMENT_A, "messageType": "ORU"}],
        "total": 1,
        "nextCursor": None,
    }
    # The caller subject is resolved from the JWT claim and forwarded before routing, and the
    # request JSON is unpacked into the service's typed keyword parameters verbatim.
    assert service.searches == [
        {
            "index": "hl7-messages-v1",
            "caller_sub": "caller-subject",
            "filters": [{"field": "messageType", "operator": "equals", "value": "ORU"}],
            "facility": "facility-a",
            "from_time": "2026-08-01T00:00:00+00:00",
            "to_time": "2026-09-01T00:00:00+00:00",
            "limit": 25,
            "cursor": "opaque",
        }
    ]

    fields_event = _authorized_event("GET /search/fields")
    fields_event["queryStringParameters"] = {"index": "ccda-documents-v1"}
    fields_response = handler(fields_event, None)
    assert fields_response["statusCode"] == 200
    # Fields are returned in deterministic sorted order regardless of caps ordering.
    assert json.loads(fields_response["body"]) == {
        "fields": ["documentId", "messageType", "sourceFacilityId"]
    }
    assert service.field_lookups == ["ccda-documents-v1"]


def test_handler_search_requires_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(explorer_handler, "_RUNTIME_MESSAGE_SEARCH", StubMessageSearch())

    assert handler({"routeKey": "POST /search"}, None)["statusCode"] == 401
    assert handler({"routeKey": "GET /search/fields"}, None)["statusCode"] == 401


def test_handler_search_maps_typed_request_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        explorer_handler,
        "_RUNTIME_MESSAGE_SEARCH",
        StubMessageSearch(error=SearchRequestError(400, "unknown_field")),
    )

    # A typed request failure (here an unknown field) surfaces its own safe status and code.
    search_event = _authorized_event("POST /search")
    search_event["body"] = json.dumps(
        {"index": "hl7-messages-v1", "filters": [{"field": "nope", "operator": "exists"}]}
    )
    response = handler(search_event, None)
    assert response["statusCode"] == 400
    assert json.loads(response["body"]) == {"error": "unknown_field"}

    # The fields route requires the index query-string parameter before touching the service.
    missing_index = _authorized_event("GET /search/fields")
    missing_index["queryStringParameters"] = {}
    missing_response = handler(missing_index, None)
    assert missing_response["statusCode"] == 400
    assert json.loads(missing_response["body"]) == {"error": "invalid_index"}

    bad_query = _authorized_event("GET /search/fields")
    bad_query["queryStringParameters"] = "not-an-object"
    assert handler(bad_query, None)["statusCode"] == 400


def test_handler_search_sanitizes_internal_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive = "backend cluster detail and clinical value"
    monkeypatch.setattr(
        explorer_handler,
        "_RUNTIME_MESSAGE_SEARCH",
        StubMessageSearch(error=SearchError(sensitive)),
    )
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")

    search_event = _authorized_event("POST /search")
    search_event["body"] = json.dumps({"index": "hl7-messages-v1"})
    response = handler(search_event, None)

    # A non-request SearchError collapses to a generic sanitized 500 that never echoes the
    # backend detail, and the request body is never logged.
    assert response["statusCode"] == 500
    assert sensitive not in response["body"]
    assert sensitive not in caplog.text
    assert "hl7-messages-v1" not in caplog.text


def test_runtime_message_search_builds_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(explorer_handler, "_RUNTIME_MESSAGE_SEARCH", None)
    monkeypatch.setenv("OPENSEARCH_ENDPOINT", "https://example.aoss.us-west-2.amazonaws.com")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    first = explorer_handler._runtime_message_search()
    second = explorer_handler._runtime_message_search()

    assert first is second


# --- Parsed-zone reingestion routes ---------------------------------------------------

from src.reingest_jobs import (  # noqa: E402
    INVALID_JOB_REQUEST,
    JOB_NOT_FOUND,
    ReingestError,
    ReingestRequestError,
)

JOB_ID = "a" * 32


class StubReingest:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.previews: list[dict[str, Any]] = []
        self.created: list[dict[str, Any]] = []
        self.listed: list[Any] = []
        self.fetched: list[str] = []

    def preview(self, sql: Any, caller_sub: str) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.previews.append({"sql": sql, "caller_sub": caller_sub})
        return {"count": 3}

    def create_job(
        self,
        caller_sub: str,
        *,
        sql: Any = None,
        document_ids: Any = None,
    ) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.created.append({"caller_sub": caller_sub, "sql": sql, "document_ids": document_ids})
        return {
            "jobId": JOB_ID,
            "status": "queued",
            "mode": "sql" if sql is not None else "ids",
        }

    def list_jobs(self, limit: Any = None) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.listed.append(limit)
        return {"items": []}

    def get_job(self, job_id: str) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        self.fetched.append(job_id)
        return {"jobId": job_id, "status": "queued"}


def test_reingest_routes_require_authentication(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", StubReingest())
    for route in (
        "POST /reingest/preview",
        "POST /reingest/jobs",
        "GET /reingest/jobs",
        "GET /reingest/jobs/{jobId}",
    ):
        assert handler({"routeKey": route}, None)["statusCode"] == 401


def test_reingest_preview_forwards_sql_and_caller(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = StubReingest()
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", stub)

    event = _authorized_event("POST /reingest/preview")
    event["body"] = json.dumps({"sql": "SELECT document_id FROM document_metadata"})
    response = handler(event, None)

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"count": 3}
    assert stub.previews == [
        {
            "sql": "SELECT document_id FROM document_metadata",
            "caller_sub": "caller-subject",
        }
    ]


def test_reingest_create_job_from_sql_returns_202(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = StubReingest()
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", stub)

    event = _authorized_event("POST /reingest/jobs")
    event["body"] = json.dumps({"sql": "SELECT document_id FROM document_metadata"})
    response = handler(event, None)

    assert response["statusCode"] == 202
    assert json.loads(response["body"]) == {
        "jobId": JOB_ID,
        "status": "queued",
        "mode": "sql",
    }
    # The handler forwards both selection sources verbatim so the service enforces the
    # exactly-one-of rule from a single source of truth.
    assert stub.created == [
        {
            "caller_sub": "caller-subject",
            "sql": "SELECT document_id FROM document_metadata",
            "document_ids": None,
        }
    ]


def test_reingest_create_job_from_ids_returns_202(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = StubReingest()
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", stub)

    event = _authorized_event("POST /reingest/jobs")
    event["body"] = json.dumps({"documentIds": [DOCUMENT_A, DOCUMENT_B]})
    response = handler(event, None)

    assert response["statusCode"] == 202
    assert json.loads(response["body"])["mode"] == "ids"
    assert stub.created == [
        {
            "caller_sub": "caller-subject",
            "sql": None,
            "document_ids": [DOCUMENT_A, DOCUMENT_B],
        }
    ]


def test_reingest_lists_and_reads_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    stub = StubReingest()
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", stub)

    list_event = _authorized_event("GET /reingest/jobs")
    list_event["queryStringParameters"] = {"limit": "25"}
    list_response = handler(list_event, None)
    assert list_response["statusCode"] == 200
    assert json.loads(list_response["body"]) == {"items": []}
    assert stub.listed == [25]

    # An absent limit is forwarded as None so the service applies its own default.
    default_event = _authorized_event("GET /reingest/jobs")
    handler(default_event, None)
    assert stub.listed == [25, None]

    get_event = _authorized_event("GET /reingest/jobs/{jobId}")
    get_event["pathParameters"] = {"jobId": JOB_ID}
    get_response = handler(get_event, None)
    assert get_response["statusCode"] == 200
    assert json.loads(get_response["body"]) == {"jobId": JOB_ID, "status": "queued"}
    assert stub.fetched == [JOB_ID]


def test_reingest_rejects_invalid_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", StubReingest())

    invalid_json = _authorized_event("POST /reingest/jobs")
    invalid_json["body"] = "not-json"
    assert handler(invalid_json, None)["statusCode"] == 400

    invalid_limit = _authorized_event("GET /reingest/jobs")
    invalid_limit["queryStringParameters"] = {"limit": "not-a-number"}
    limit_response = handler(invalid_limit, None)
    assert limit_response["statusCode"] == 400
    assert json.loads(limit_response["body"]) == {"error": "invalid_limit"}

    missing_job_id = _authorized_event("GET /reingest/jobs/{jobId}")
    missing_job_id["pathParameters"] = {}
    job_response = handler(missing_job_id, None)
    assert job_response["statusCode"] == 400
    assert json.loads(job_response["body"]) == {"error": "invalid_job_id"}


def test_reingest_maps_typed_request_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class ConflictingSelection(StubReingest):
        def create_job(
            self,
            _caller_sub: str,
            *,
            sql: Any = None,  # noqa: ARG002 - signature must match the service contract
            document_ids: Any = None,  # noqa: ARG002 - signature must match the contract
        ) -> dict[str, Any]:
            raise ReingestRequestError(400, INVALID_JOB_REQUEST)

    class MissingJob(StubReingest):
        def get_job(self, _job_id: str) -> dict[str, Any]:
            raise ReingestRequestError(404, JOB_NOT_FOUND)

    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", ConflictingSelection())
    create_event = _authorized_event("POST /reingest/jobs")
    create_event["body"] = json.dumps({"sql": "SELECT 1", "documentIds": [DOCUMENT_A]})
    create_response = handler(create_event, None)
    assert create_response["statusCode"] == 400
    assert json.loads(create_response["body"]) == {"error": INVALID_JOB_REQUEST}

    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", MissingJob())
    get_event = _authorized_event("GET /reingest/jobs/{jobId}")
    get_event["pathParameters"] = {"jobId": JOB_ID}
    get_response = handler(get_event, None)
    assert get_response["statusCode"] == 404
    assert json.loads(get_response["body"]) == {"error": JOB_NOT_FOUND}


def test_reingest_sanitizes_internal_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive = "sensitive SQL text and DynamoDB detail"
    monkeypatch.setattr(
        explorer_handler,
        "_RUNTIME_REINGEST",
        StubReingest(error=ReingestError(sensitive)),
    )
    caplog.set_level(logging.ERROR, logger="src.explorer_handler")

    event = _authorized_event("POST /reingest/preview")
    event["body"] = json.dumps({"sql": "SELECT document_id FROM document_metadata"})
    response = handler(event, None)

    # A non-request ReingestError collapses to a generic sanitized 500 that never echoes the
    # SQL or backend detail, and the request body is never logged.
    assert response["statusCode"] == 500
    assert json.loads(response["body"]) == {"error": "explorer_request_failed"}
    assert sensitive not in response["body"]
    assert sensitive not in caplog.text
    assert "document_metadata" not in caplog.text


def test_runtime_reingest_builds_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    clients = {
        "rds-data": object(),
        "dynamodb": object(),
        "lambda": object(),
    }
    monkeypatch.setattr(explorer_handler, "_RUNTIME_REINGEST", None)
    monkeypatch.setattr(explorer_handler, "_aws_client", clients.__getitem__)
    monkeypatch.setenv("METADATA_CLUSTER_ARN", "cluster")
    monkeypatch.setenv("METADATA_SECRET_ARN", "secret")
    monkeypatch.setenv("METADATA_DATABASE", "manifest_medex")
    monkeypatch.setenv("METADATA_TABLE", "document_metadata")
    monkeypatch.setenv("REINGEST_JOBS_TABLE", "reingest-jobs")
    monkeypatch.setenv("REINGEST_PLANNER_FUNCTION", "reingest-planner")

    first = explorer_handler._runtime_reingest()
    second = explorer_handler._runtime_reingest()

    assert first is second
