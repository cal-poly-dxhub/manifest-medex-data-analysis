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
