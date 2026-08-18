from typing import Any

import pytest
from src.metadata_store import (
    CREATE_TABLE_SQL,
    UPSERT_SQL,
    DataApiMetadataStore,
    MetadataRecord,
    MetadataStoreError,
)

CREDENTIAL_ARN = "arn:aws:secretsmanager:us-west-2:111122223333:secret:metadata"
SCHEMA_FAILURE_DETAIL = "sensitive schema response"
UPSERT_FAILURE_DETAIL = "sensitive SQL parameters and response"


class FakeDataApi:
    def __init__(self, *, execute_error: bool = False, batch_error: bool = False) -> None:
        self.execute_error = execute_error
        self.batch_error = batch_error
        self.execute_calls: list[dict[str, Any]] = []
        self.batch_calls: list[dict[str, Any]] = []

    def execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.execute_calls.append(kwargs)
        if self.execute_error:
            raise RuntimeError(SCHEMA_FAILURE_DETAIL)
        return {"numberOfRecordsUpdated": 0}

    def batch_execute_statement(self, **kwargs: Any) -> dict[str, Any]:
        self.batch_calls.append(kwargs)
        if self.batch_error:
            raise RuntimeError(UPSERT_FAILURE_DETAIL)
        return {"updateResults": []}


def _record(*, suffix: str = "1", document_time: str | None = None) -> MetadataRecord:
    return MetadataRecord(
        document_id=f"document-{suffix}",
        source_format="hl7-v2",
        document_time=document_time,
        ingested_time="2026-08-10T19:00:00Z",
        raw_s3_uri="s3://raw/incoming/hl7/batch.hl7",
        raw_version_id="raw-version",
        parsed_s3_uri=f"s3://parsed/hl7/document-{suffix}.json",
        parsed_version_id=f"parsed-version-{suffix}",
    )


def _store(client: FakeDataApi, *, table_name: str = "document_metadata") -> DataApiMetadataStore:
    return DataApiMetadataStore(
        client,
        cluster_arn="arn:aws:rds:us-west-2:111122223333:cluster:metadata",
        secret_arn=CREDENTIAL_ARN,
        database="manifest_medex",
        table_name=table_name,
    )


def test_schema_matches_the_agreed_metadata_columns_only() -> None:
    normalized = " ".join(CREATE_TABLE_SQL.split())

    assert "document_id TEXT PRIMARY KEY" in normalized
    assert "source_format TEXT NOT NULL" in normalized
    assert "CHECK (source_format IN ('hl7-v2', 'ccda'))" in normalized
    assert "document_time TIMESTAMPTZ" in normalized
    assert "ingested_time TIMESTAMPTZ NOT NULL" in normalized
    assert "raw_s3_uri TEXT NOT NULL" in normalized
    assert "raw_version_id TEXT" in normalized
    assert "parsed_s3_uri TEXT NOT NULL" in normalized
    assert "parsed_version_id TEXT" in normalized
    assert "message_type" not in normalized
    assert "created_at" not in normalized
    assert "last_updated_at" not in normalized


def test_batch_upsert_is_retry_safe_and_schema_is_initialized_once() -> None:
    client = FakeDataApi()
    store = _store(client)
    records = [
        _record(suffix="1", document_time="2026-08-10T12:00:00Z"),
        _record(suffix="2"),
    ]

    store.upsert(records)
    store.upsert(records)

    assert len(client.execute_calls) == 1
    create_call = client.execute_calls[0]
    assert create_call["sql"] == CREATE_TABLE_SQL
    assert create_call["continueAfterTimeout"] is True
    assert len(client.batch_calls) == 2
    batch_call = client.batch_calls[0]
    assert batch_call["sql"] == UPSERT_SQL
    assert "ON CONFLICT (document_id)" in batch_call["sql"]
    assert "DO UPDATE SET" in batch_call["sql"]
    assert batch_call["resourceArn"].endswith(":cluster:metadata")
    assert batch_call["database"] == "manifest_medex"
    assert len(batch_call["parameterSets"]) == 2

    first_parameters = {
        parameter["name"]: parameter["value"] for parameter in batch_call["parameterSets"][0]
    }
    assert first_parameters == {
        "document_id": {"stringValue": "document-1"},
        "source_format": {"stringValue": "hl7-v2"},
        "document_time": {"stringValue": "2026-08-10T12:00:00Z"},
        "ingested_time": {"stringValue": "2026-08-10T19:00:00Z"},
        "raw_s3_uri": {"stringValue": "s3://raw/incoming/hl7/batch.hl7"},
        "raw_version_id": {"stringValue": "raw-version"},
        "parsed_s3_uri": {"stringValue": "s3://parsed/hl7/document-1.json"},
        "parsed_version_id": {"stringValue": "parsed-version-1"},
    }
    second_parameters = {
        parameter["name"]: parameter["value"] for parameter in batch_call["parameterSets"][1]
    }
    assert second_parameters["document_time"] == {"isNull": True}


def test_empty_upsert_does_not_call_data_api() -> None:
    client = FakeDataApi()

    _store(client).upsert([])

    assert client.execute_calls == []
    assert client.batch_calls == []


@pytest.mark.parametrize("identifier", ["DocumentMetadata", "metadata-table", "x;DROP TABLE x"])
def test_store_rejects_unsafe_database_or_table_identifier(identifier: str) -> None:
    client = FakeDataApi()

    with pytest.raises(ValueError, match="safe SQL identifiers"):
        DataApiMetadataStore(
            client,
            cluster_arn="cluster",
            secret_arn=CREDENTIAL_ARN,
            database=identifier,
            table_name="document_metadata",
        )


def test_custom_safe_table_name_is_applied_to_both_statements() -> None:
    client = FakeDataApi()
    store = _store(client, table_name="clinical_document_metadata")

    store.upsert([_record()])

    assert "clinical_document_metadata" in client.execute_calls[0]["sql"]
    assert "clinical_document_metadata" in client.batch_calls[0]["sql"]
    assert "INSERT INTO document_metadata" not in client.batch_calls[0]["sql"]


@pytest.mark.parametrize(
    ("execute_error", "batch_error", "expected"),
    [
        (True, False, "schema initialization failed"),
        (False, True, "metadata upsert failed"),
    ],
)
def test_data_api_errors_are_sanitized_without_original_details(
    execute_error: bool,
    batch_error: bool,
    expected: str,
) -> None:
    client = FakeDataApi(execute_error=execute_error, batch_error=batch_error)

    with pytest.raises(MetadataStoreError, match=expected) as captured:
        _store(client).upsert([_record()])

    assert captured.value.__cause__ is None
    assert "sensitive" not in str(captured.value)
