"""Tests for the parsed-zone reindexer handler and its DynamoDB job store."""

from __future__ import annotations

import ast
import json
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from src import reindexer_handler
from src.reindexer_handler import (
    STATUS_COMPLETE,
    DynamoJobStore,
    Reindexer,
    runtime_handler,
)

PARSED_BUCKET = "parsed-bucket"
HL7_VERSION = "1.0.0"
CCDA_VERSION = "1.0.0"


class FakeClientError(Exception):
    """Duck-typed stand-in for botocore ClientError raised by S3."""

    def __init__(self, code: str, http_status: int = 400) -> None:
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": http_status},
        }


class ConditionalCheckFailedException(Exception):  # noqa: N818 - mirrors the botocore name
    """Duck-typed stand-in for the DynamoDB conditional-check failure."""


class FakeS3:
    def __init__(
        self,
        payload: bytes | Exception | None = None,
        *,
        by_key: dict[str, bytes | Exception] | None = None,
        content_length: int | None = None,
    ) -> None:
        self.payload = payload
        self.by_key = by_key or {}
        self.content_length = content_length
        self.get_calls: list[dict[str, Any]] = []

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        item = self.by_key.get(kwargs["Key"], self.payload)
        if isinstance(item, Exception):
            raise item
        if item is None:
            msg = "no payload configured"
            raise KeyError(msg)
        length = self.content_length if self.content_length is not None else len(item)
        return {"Body": BytesIO(item), "ContentLength": length}


class FakeTransport:
    def __init__(self, responses: list[tuple[int, dict[str, Any]]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, bytes | None]] = []

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append((method, path, body))
        return self.responses.pop(0)


class FakeJobStore:
    def __init__(
        self,
        counters: dict[str, Any] | None = None,
        *,
        finalize_error: Exception | None = None,
    ) -> None:
        self.counters = dict(counters or {})
        self.finalize_error = finalize_error
        self.reindexed_calls: list[tuple[str, bool]] = []
        self.missing_calls: list[str] = []
        self.failed_calls: list[str] = []
        self.finalized: list[str] = []
        self.finalized_failed: list[tuple[str, bool]] = []

    def record_reindexed(self, job_id: str, *, stale: bool) -> dict[str, Any]:
        self.reindexed_calls.append((job_id, stale))
        self.counters["reindexed"] = int(self.counters.get("reindexed", 0)) + 1
        if stale:
            self.counters["reindexedStaleParser"] = (
                int(self.counters.get("reindexedStaleParser", 0)) + 1
            )
        return dict(self.counters)

    def record_missing(self, job_id: str) -> dict[str, Any]:
        self.missing_calls.append(job_id)
        self.counters["missingParsed"] = int(self.counters.get("missingParsed", 0)) + 1
        return dict(self.counters)

    def record_failed(self, job_id: str) -> dict[str, Any]:
        self.failed_calls.append(job_id)
        self.counters["failed"] = int(self.counters.get("failed", 0)) + 1
        return dict(self.counters)

    def finalize(self, job_id: str, *, failed: bool = False) -> None:
        self.finalized_failed.append((job_id, failed))
        if self.finalize_error is not None:
            raise self.finalize_error
        self.finalized.append(job_id)


class FakeDynamo:
    def __init__(
        self,
        *,
        attributes: dict[str, Any] | None = None,
        finalize_error: Exception | None = None,
    ) -> None:
        self.attributes = attributes or {}
        self.finalize_error = finalize_error
        self.update_calls: list[dict[str, Any]] = []

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.update_calls.append(kwargs)
        if "ConditionExpression" in kwargs:
            if self.finalize_error is not None:
                raise self.finalize_error
            return {}
        return {"Attributes": self.attributes}


def _parsed_doc(
    document_id: str = "doc-1",
    *,
    source_format: str = "hl7-v2",
    parser_version: str = HL7_VERSION,
    ingest_time: str = "2026-08-10T19:00:00Z",
) -> dict[str, Any]:
    return {
        "documentId": document_id,
        "sourceFormat": source_format,
        "parserVersion": parser_version,
        "ingestTime": ingest_time,
        "messageType": "ORU",
        "rawObject": {"segment": "value"},
    }


def _record(
    *,
    job_id: str = "job-1",
    document_id: str = "doc-1",
    source_format: str = "hl7-v2",
    key: str = "hl7/doc-1.json",
    bucket: str = PARSED_BUCKET,
    receive_count: int = 1,
    message_id: str = "m-1",
) -> dict[str, Any]:
    return {
        "messageId": message_id,
        "attributes": {"ApproximateReceiveCount": str(receive_count)},
        "body": json.dumps(
            {
                "jobId": job_id,
                "documentId": document_id,
                "parsedS3Uri": f"s3://{bucket}/{key}",
                "sourceFormat": source_format,
            }
        ),
    }


def _event(*records: dict[str, Any]) -> dict[str, Any]:
    return {"Records": list(records)}


def _ok_transport(count: int = 1) -> FakeTransport:
    # The index already exists (HEAD 200); the reindexer never creates indexes.
    return FakeTransport(
        [
            (200, {}),
            (
                200,
                {
                    "errors": False,
                    "items": [{"index": {"status": 201}} for _ in range(count)],
                },
            ),
        ]
    )


def _indexed_documents(transport: FakeTransport) -> list[dict[str, Any]]:
    bulk_body = transport.calls[-1][2]
    assert bulk_body is not None
    lines = bulk_body.decode().splitlines()
    return [json.loads(line) for line in lines[1::2]]


@pytest.fixture(autouse=True)
def _parsed_bucket_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PARSED_BUCKET", PARSED_BUCKET)
    monkeypatch.setenv("HL7_PARSER_VERSION", HL7_VERSION)
    monkeypatch.setenv("CCDA_PARSER_VERSION", CCDA_VERSION)


def test_happy_path_restamps_ingest_time_and_counts_reindexed() -> None:
    document = _parsed_doc()
    s3 = FakeS3(json.dumps(document).encode())
    transport = _ok_transport(1)
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": []}
    assert s3.get_calls == [{"Bucket": PARSED_BUCKET, "Key": "hl7/doc-1.json"}]
    assert store.reindexed_calls == [("job-1", False)]
    assert store.missing_calls == []
    indexed = _indexed_documents(transport)
    assert len(indexed) == 1
    restored = indexed[0]
    # Customer decision: a reingested document reports the reingestion time as its
    # ingestTime, while the original arrival time is retained for provenance.
    assert restored["originalIngestTime"] == "2026-08-10T19:00:00Z"
    assert restored["ingestTime"] != "2026-08-10T19:00:00Z"
    assert restored["ingestTime"].endswith("Z")
    datetime.fromisoformat(restored["ingestTime"].replace("Z", "+00:00"))
    # Every other field is indexed exactly as stored in the parsed zone.
    for key, value in document.items():
        if key != "ingestTime":
            assert restored[key] == value


def test_reingested_document_without_ingest_time_gets_one() -> None:
    document = _parsed_doc()
    del document["ingestTime"]
    s3 = FakeS3(json.dumps(document).encode())
    transport = _ok_transport(1)

    Reindexer(s3, transport, FakeJobStore()).process_batch(_event(_record()))

    restored = _indexed_documents(transport)[0]
    assert "originalIngestTime" not in restored
    assert restored["ingestTime"].endswith("Z")


def test_stale_parser_version_counts_stale_but_still_indexes() -> None:
    document = _parsed_doc(parser_version="0.4.0")
    s3 = FakeS3(json.dumps(document).encode())
    transport = _ok_transport(1)
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": []}
    assert store.reindexed_calls == [("job-1", True)]
    assert store.counters["reindexedStaleParser"] == 1
    restored = _indexed_documents(transport)[0]
    # Stale documents are still indexed as stored, apart from the ingestTime restamp.
    assert restored["parserVersion"] == "0.4.0"
    assert restored["originalIngestTime"] == document["ingestTime"]
    assert {k: v for k, v in restored.items() if k not in ("ingestTime", "originalIngestTime")} == {
        k: v for k, v in document.items() if k != "ingestTime"
    }


@pytest.mark.parametrize(
    ("code", "http_status"),
    [("NoSuchKey", 404), ("NoSuchBucket", 404), ("SlowDown", 404)],
)
def test_missing_parsed_object_counts_missing_without_batch_failure(
    code: str,
    http_status: int,
) -> None:
    s3 = FakeS3(FakeClientError(code, http_status))
    transport = FakeTransport([])
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": []}
    assert store.missing_calls == ["job-1"]
    assert store.reindexed_calls == []
    assert transport.calls == []
    assert len(s3.get_calls) == 1


def test_partial_batch_failure_isolates_the_failing_record() -> None:
    good = json.dumps(_parsed_doc("doc-1")).encode()
    mismatched = json.dumps(_parsed_doc("wrong-id")).encode()
    s3 = FakeS3(by_key={"hl7/doc-1.json": good, "hl7/doc-2.json": mismatched})
    transport = _ok_transport(1)
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(
        _event(
            _record(message_id="m-1", document_id="doc-1", key="hl7/doc-1.json"),
            _record(message_id="m-2", document_id="doc-2", key="hl7/doc-2.json"),
        )
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-2"}]}
    assert store.reindexed_calls == [("job-1", False)]
    assert store.failed_calls == []


def test_terminal_receive_count_records_failed_and_returns_failure() -> None:
    mismatched = json.dumps(_parsed_doc("wrong-id")).encode()
    s3 = FakeS3(mismatched)
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record(receive_count=5)))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.failed_calls == ["job-1"]


def test_non_terminal_failure_does_not_record_failed() -> None:
    mismatched = json.dumps(_parsed_doc("wrong-id")).encode()
    s3 = FakeS3(mismatched)
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record(receive_count=1)))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.failed_calls == []


def test_mixed_job_accumulates_all_outcome_counters_and_completes() -> None:
    fresh = json.dumps(_parsed_doc("fresh-doc")).encode()
    stale = json.dumps(_parsed_doc("stale-doc", parser_version="0.4.0")).encode()
    bad = json.dumps(_parsed_doc("different-doc")).encode()
    s3 = FakeS3(
        by_key={
            "hl7/fresh-doc.json": fresh,
            "hl7/stale-doc.json": stale,
            "hl7/missing-doc.json": FakeClientError("NoSuchKey", 404),
            "hl7/bad-doc.json": bad,
        }
    )
    transport = FakeTransport([*_ok_transport().responses, *_ok_transport().responses])
    store = FakeJobStore(
        {
            "enqueueComplete": True,
            "enqueued": 4,
            "reindexed": 0,
            "reindexedStaleParser": 0,
            "missingParsed": 0,
            "failed": 0,
        }
    )

    result = Reindexer(s3, transport, store).process_batch(
        _event(
            _record(
                message_id="fresh-message",
                document_id="fresh-doc",
                key="hl7/fresh-doc.json",
            ),
            _record(
                message_id="stale-message",
                document_id="stale-doc",
                key="hl7/stale-doc.json",
            ),
            _record(
                message_id="missing-message",
                document_id="missing-doc",
                key="hl7/missing-doc.json",
            ),
            _record(
                message_id="failed-message",
                document_id="bad-doc",
                key="hl7/bad-doc.json",
                receive_count=5,
            ),
        )
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "failed-message"}]}
    assert store.counters == {
        "enqueueComplete": True,
        "enqueued": 4,
        "reindexed": 2,
        "reindexedStaleParser": 1,
        "missingParsed": 1,
        "failed": 1,
    }
    assert store.finalized[-1] == "job-1"


def test_malformed_message_fails_without_touching_s3_or_index() -> None:
    s3 = FakeS3(b"{}")
    transport = FakeTransport([])
    store = FakeJobStore()
    event = {"Records": [{"messageId": "malformed", "attributes": {}, "body": "{"}]}

    result = Reindexer(s3, transport, store).process_batch(event)

    assert result == {"batchItemFailures": [{"itemIdentifier": "malformed"}]}
    assert s3.get_calls == []
    assert transport.calls == []
    assert store.reindexed_calls == []
    assert store.failed_calls == []


@pytest.mark.parametrize("bucket", ["other-bucket", ""])
def test_parsed_uri_outside_expected_bucket_fails(bucket: str) -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record(bucket=bucket)))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert s3.get_calls == []


def test_bounded_read_rejects_oversized_content_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_PARSED_BYTES", "10")
    document = _parsed_doc()
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport([])
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert transport.calls == []
    assert store.reindexed_calls == []


def test_bounded_read_rejects_oversized_body_when_content_length_understated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_PARSED_BYTES", "10")
    document = _parsed_doc()
    s3 = FakeS3(json.dumps(document).encode(), content_length=0)
    transport = FakeTransport([])
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert transport.calls == []


def test_non_json_parsed_object_fails() -> None:
    s3 = FakeS3(b"not-json")
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.reindexed_calls == []


def test_unsupported_source_format_fails() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(
        _event(_record(source_format="fhir"))
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert s3.get_calls == []


def test_completion_finalizes_when_enqueue_complete_and_counts_reached() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore({"enqueued": 2, "enqueueComplete": True, "reindexed": 1})

    Reindexer(s3, _ok_transport(1), store).process_batch(_event(_record()))

    assert store.finalized == ["job-1"]


def test_completion_skipped_when_counts_below_enqueued() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore({"enqueued": 5, "enqueueComplete": True, "reindexed": 1})

    Reindexer(s3, _ok_transport(1), store).process_batch(_event(_record()))

    assert store.finalized == []


def test_completion_skipped_when_enqueue_not_complete() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore({"enqueued": 1, "enqueueComplete": False})

    Reindexer(s3, _ok_transport(1), store).process_batch(_event(_record()))

    assert store.finalized == []


def test_finalize_error_is_best_effort_and_does_not_fail_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore(
        {"enqueued": 1, "enqueueComplete": True},
        finalize_error=RuntimeError("transient ddb error"),
    )

    with caplog.at_level(logging.ERROR, logger="src.reindexer_handler"):
        result = Reindexer(s3, _ok_transport(1), store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": []}
    assert "reindex_finalize_failed" in caplog.text


def test_logs_never_contain_identifiers_or_uris(
    caplog: pytest.LogCaptureFixture,
) -> None:
    document = _parsed_doc("doc-secret")
    s3 = FakeS3(json.dumps(document).encode())
    store = FakeJobStore()

    with caplog.at_level(logging.INFO, logger="src.reindexer_handler"):
        Reindexer(s3, _ok_transport(1), store).process_batch(
            _event(_record(job_id="job-secret", document_id="doc-secret"))
        )

    assert "job-secret" not in caplog.text
    assert "doc-secret" not in caplog.text
    assert PARSED_BUCKET not in caplog.text


def test_reindexer_module_does_not_import_the_format_parsers() -> None:
    source = Path("src/reindexer_handler.py").read_text()
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    banned = {"src.parser", "src.ccda_parser"}
    assert banned.isdisjoint(imported)
    assert "src.parser" not in source
    assert "src.ccda_parser" not in source


def test_runtime_handler_builds_dependencies_from_env_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENSEARCH_ENDPOINT", "https://search.example.aws")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("OPENSEARCH_SERVICE", "es")
    monkeypatch.setenv("JOBS_TABLE", "jobs")
    reindexer_handler._RUNTIME_REINDEXERS.clear()
    created: list[str] = []

    def fake_client(service: str) -> Any:
        created.append(service)
        return FakeS3() if service == "s3" else FakeDynamo()

    monkeypatch.setattr(reindexer_handler, "_aws_client", fake_client)

    first = runtime_handler(_event(), None)
    second = runtime_handler(_event(), None)

    assert first == {"batchItemFailures": []}
    assert second == {"batchItemFailures": []}
    assert created.count("s3") == 1
    assert created.count("dynamodb") == 1


def test_handler_delegates_to_runtime_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[dict[str, Any], object]] = []

    def fake_runtime(event: dict[str, Any], context: object) -> dict[str, list[dict[str, str]]]:
        captured.append((event, context))
        return {"batchItemFailures": []}

    monkeypatch.setattr(reindexer_handler, "runtime_handler", fake_runtime)
    context = object()

    assert reindexer_handler.handler({"Records": []}, context) == {"batchItemFailures": []}
    assert captured == [({"Records": []}, context)]


def test_dynamo_job_store_record_reindexed_uses_atomic_add_all_new() -> None:
    ddb = FakeDynamo(
        attributes={
            "jobId": {"S": "job-1"},
            "reindexed": {"N": "3"},
            "enqueued": {"N": "5"},
            "enqueueComplete": {"BOOL": True},
            "missingParsed": {"N": "1"},
        }
    )
    store = DynamoJobStore(client=ddb, table_name="jobs")

    counters = store.record_reindexed("job-1", stale=False)

    call = ddb.update_calls[0]
    assert call["TableName"] == "jobs"
    assert call["Key"] == {"jobId": {"S": "job-1"}}
    assert call["UpdateExpression"].startswith("ADD ")
    assert call["ReturnValues"] == "ALL_NEW"
    assert list(call["ExpressionAttributeNames"].values()) == ["reindexed"]
    assert list(call["ExpressionAttributeValues"].values()) == [{"N": "1"}]
    assert counters == {
        "reindexed": 3,
        "enqueued": 5,
        "enqueueComplete": True,
        "missingParsed": 1,
    }


def test_dynamo_job_store_stale_reindexed_adds_both_counters() -> None:
    ddb = FakeDynamo(attributes={"reindexed": {"N": "1"}, "reindexedStaleParser": {"N": "1"}})
    store = DynamoJobStore(client=ddb, table_name="jobs")

    store.record_reindexed("job-1", stale=True)

    call = ddb.update_calls[0]
    assert sorted(call["ExpressionAttributeNames"].values()) == [
        "reindexed",
        "reindexedStaleParser",
    ]
    assert list(call["ExpressionAttributeValues"].values()) == [{"N": "1"}, {"N": "1"}]


def test_dynamo_job_store_missing_and_failed_add_single_counter() -> None:
    ddb = FakeDynamo(attributes={"missingParsed": {"N": "1"}})
    store = DynamoJobStore(client=ddb, table_name="jobs")

    store.record_missing("job-1")
    store.record_failed("job-1")

    assert list(ddb.update_calls[0]["ExpressionAttributeNames"].values()) == ["missingParsed"]
    assert list(ddb.update_calls[1]["ExpressionAttributeNames"].values()) == ["failed"]


def test_dynamo_job_store_finalize_is_conditional() -> None:
    ddb = FakeDynamo()
    store = DynamoJobStore(client=ddb, table_name="jobs")

    store.finalize("job-1")

    call = ddb.update_calls[0]
    assert "ConditionExpression" in call
    assert call["ExpressionAttributeNames"]["#status"] == "status"
    assert call["ExpressionAttributeValues"][":complete"] == {"S": STATUS_COMPLETE}
    assert ":finishedAt" in call["ExpressionAttributeValues"]


def test_dynamo_job_store_finalize_swallows_conditional_check_failure() -> None:
    ddb = FakeDynamo(finalize_error=ConditionalCheckFailedException())
    store = DynamoJobStore(client=ddb, table_name="jobs")

    store.finalize("job-1")

    assert len(ddb.update_calls) == 1


def test_dynamo_job_store_finalize_reraises_other_errors() -> None:
    ddb = FakeDynamo(finalize_error=RuntimeError("boom"))
    store = DynamoJobStore(client=ddb, table_name="jobs")

    with pytest.raises(RuntimeError):
        store.finalize("job-1")


def test_non_string_field_is_rejected_as_malformed() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    store = FakeJobStore()
    event = {
        "Records": [
            {
                "messageId": "m-1",
                "attributes": {"ApproximateReceiveCount": "1"},
                "body": json.dumps(
                    {
                        "jobId": 7,
                        "documentId": "doc-1",
                        "parsedS3Uri": f"s3://{PARSED_BUCKET}/hl7/doc-1.json",
                        "sourceFormat": "hl7-v2",
                    }
                ),
            }
        ]
    }

    result = Reindexer(s3, FakeTransport([]), store).process_batch(event)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert s3.get_calls == []


def test_source_format_mismatch_between_message_and_document_fails() -> None:
    document = _parsed_doc("doc-1", source_format="ccda", parser_version=CCDA_VERSION)
    s3 = FakeS3(json.dumps(document).encode())
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(
        _event(_record(document_id="doc-1", source_format="hl7-v2"))
    )

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.reindexed_calls == []


def test_staleness_not_asserted_when_version_env_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("HL7_PARSER_VERSION", raising=False)
    document = _parsed_doc(parser_version="anything")
    s3 = FakeS3(json.dumps(document).encode())
    store = FakeJobStore()

    Reindexer(s3, _ok_transport(1), store).process_batch(_event(_record()))

    assert store.reindexed_calls == [("job-1", False)]


def test_index_rejection_reports_sanitized_backend_details(
    caplog: pytest.LogCaptureFixture,
) -> None:
    document = _parsed_doc()
    s3 = FakeS3(json.dumps(document).encode())
    transport = FakeTransport(
        [
            (200, {}),
            (
                200,
                {
                    "errors": True,
                    "items": [
                        {
                            "index": {
                                "status": 400,
                                "error": {"type": "mapper_parsing_exception", "reason": "secret"},
                            }
                        }
                    ],
                },
            ),
        ]
    )
    store = FakeJobStore()

    with caplog.at_level(logging.ERROR, logger="src.reindexer_handler"):
        result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.reindexed_calls == []
    logged = json.loads(caplog.records[-1].getMessage())
    assert logged["failureCategory"] == "document_indexing_failed"
    assert logged["httpStatus"] == 400
    assert logged["backendErrorType"] == "mapper_parsing_exception"
    assert "secret" not in caplog.text


def test_invalid_receive_count_attribute_defaults_to_first_attempt() -> None:
    mismatched = json.dumps(_parsed_doc("wrong-id")).encode()
    s3 = FakeS3(mismatched)
    store = FakeJobStore()
    event = {
        "Records": [
            {
                "messageId": "m-1",
                "attributes": {"ApproximateReceiveCount": "not-a-number"},
                "body": _record()["body"],
            }
        ]
    }

    result = Reindexer(s3, FakeTransport([]), store).process_batch(event)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.failed_calls == []


def test_invalid_max_receive_count_env_defaults_to_five(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAX_RECEIVE_COUNT", "not-a-number")
    mismatched = json.dumps(_parsed_doc("wrong-id")).encode()
    s3 = FakeS3(mismatched)
    store = FakeJobStore()

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record(receive_count=5)))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.failed_calls == ["job-1"]


def test_terminal_failure_with_unparseable_job_id_records_nothing() -> None:
    store = FakeJobStore()
    event = {
        "Records": [
            {
                "messageId": "m-1",
                "attributes": {"ApproximateReceiveCount": "9"},
                "body": "{",
            }
        ]
    }

    result = Reindexer(FakeS3(), FakeTransport([]), store).process_batch(event)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert store.failed_calls == []


class _UnreadableBodyS3:
    def __init__(self, body: object) -> None:
        self.body = body

    def get_object(self, **_kwargs: Any) -> dict[str, Any]:
        return {"Body": self.body, "ContentLength": 0}


def test_unreadable_response_body_fails() -> None:
    store = FakeJobStore()
    s3 = _UnreadableBodyS3(object())

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}


def test_non_bytes_response_body_fails() -> None:
    store = FakeJobStore()

    class _StringBody:
        def read(self) -> Any:
            return "not-bytes"

    s3 = _UnreadableBodyS3(_StringBody())

    result = Reindexer(s3, FakeTransport([]), store).process_batch(_event(_record()))

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}


def test_decode_counters_skips_non_numeric_and_non_dict_values() -> None:
    ddb = FakeDynamo(
        attributes={
            "jobId": {"S": "job-1"},
            "reindexed": {"N": "not-a-number"},
            "enqueueComplete": {"BOOL": True},
            "spurious": "unexpected-shape",
        }
    )
    store = DynamoJobStore(client=ddb, table_name="jobs")

    counters = store.record_missing("job-1")

    assert counters == {"reindexed": 0, "enqueueComplete": True}


def test_reindexer_never_attempts_index_creation() -> None:
    """The reindexer role holds only WriteDocument; a PUT to an index path would 403."""
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    transport = _ok_transport(1)

    Reindexer(s3, transport, FakeJobStore()).process_batch(_event(_record()))

    assert all(method != "PUT" for method, _path, _body in transport.calls)


def test_missing_index_is_reported_as_a_retryable_failure_not_created() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    transport = FakeTransport([(404, {})])
    store = FakeJobStore()

    result = Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert result["batchItemFailures"] == [{"itemIdentifier": _record()["messageId"]}]
    assert [m for m, _p, _b in transport.calls] == ["HEAD"]
    assert store.reindexed_calls == []


def test_job_with_a_permanently_failed_document_finalizes_as_failed() -> None:
    """A terminal document failure must not leave the job marked complete."""
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    transport = FakeTransport([(403, {"error": {"type": "security_exception"}})])
    store = FakeJobStore(counters={"enqueueComplete": True, "enqueued": 1})
    record = _record()
    record["attributes"] = {"ApproximateReceiveCount": "5"}

    Reindexer(s3, transport, store).process_batch(_event(record))

    assert store.failed_calls == ["job-1"]
    assert store.finalized_failed == [("job-1", True)]


def test_job_with_all_documents_reindexed_finalizes_as_complete() -> None:
    s3 = FakeS3(json.dumps(_parsed_doc()).encode())
    transport = _ok_transport(1)
    store = FakeJobStore(counters={"enqueueComplete": True, "enqueued": 1, "reindexed": 1})

    Reindexer(s3, transport, store).process_batch(_event(_record()))

    assert store.finalized_failed == [("job-1", False)]


@pytest.mark.parametrize("source_format", ["hl7-v2", "ccda"])
def test_every_field_the_reindexer_adds_is_declared_in_the_index_mapping(
    source_format: str,
) -> None:
    """Regression guard for the 2026-09-30 reingest outage.

    The reindexer role holds WriteDocument but not UpdateIndex. A bulk write that
    introduces a field absent from the index mapping triggers a dynamic mapping update,
    which OpenSearch Serverless rejects with 403 for that role. Any top-level field the
    reindexer adds beyond what the parsed document already carries must therefore be
    declared explicitly in the mapping.
    """
    from src.search_store import CCDA_INDEX_MAPPING, HL7_INDEX_MAPPING

    document = _parsed_doc()
    document["sourceFormat"] = source_format
    s3 = FakeS3(json.dumps(document).encode())
    transport = _ok_transport(1)
    record = _record()
    record["body"] = json.dumps({**json.loads(record["body"]), "sourceFormat": source_format})

    Reindexer(s3, transport, FakeJobStore()).process_batch(_event(record))

    restored = _indexed_documents(transport)[0]
    added_fields = set(restored) - set(document)
    mapping = HL7_INDEX_MAPPING if source_format == "hl7-v2" else CCDA_INDEX_MAPPING
    declared = set(mapping["mappings"]["properties"])
    undeclared = added_fields - declared
    assert not undeclared, f"reindexer adds unmapped fields {sorted(undeclared)}"
    assert "originalIngestTime" in added_fields
