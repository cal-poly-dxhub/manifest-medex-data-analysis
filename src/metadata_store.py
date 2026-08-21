"""Persist retry-safe document location metadata through the Aurora Data API."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol

# Clinical content stays in S3/OpenSearch; Aurora stores only durable document locations.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS document_metadata (
    document_id        TEXT PRIMARY KEY,
    source_format      TEXT NOT NULL
                       CHECK (source_format IN ('hl7-v2', 'ccda')),
    document_time      TIMESTAMPTZ,
    ingested_time      TIMESTAMPTZ NOT NULL,
    raw_s3_uri         TEXT NOT NULL,
    raw_version_id     TEXT,
    parsed_s3_uri      TEXT NOT NULL,
    parsed_version_id  TEXT
)
""".strip()

CREATE_INDEX_SQLS = (
    """
CREATE INDEX IF NOT EXISTS document_metadata_ingested_document_idx
ON document_metadata (ingested_time DESC, document_id DESC)
""".strip(),
    """
CREATE INDEX IF NOT EXISTS document_metadata_format_ingested_document_idx
ON document_metadata (source_format, ingested_time DESC, document_id DESC)
""".strip(),
)

UPSERT_SQL = """
INSERT INTO document_metadata (
    document_id,
    source_format,
    document_time,
    ingested_time,
    raw_s3_uri,
    raw_version_id,
    parsed_s3_uri,
    parsed_version_id
) VALUES (
    :document_id,
    :source_format,
    CAST(:document_time AS TIMESTAMPTZ),
    CAST(:ingested_time AS TIMESTAMPTZ),
    :raw_s3_uri,
    :raw_version_id,
    :parsed_s3_uri,
    :parsed_version_id
)
ON CONFLICT (document_id)
DO UPDATE SET
    source_format = EXCLUDED.source_format,
    document_time = EXCLUDED.document_time,
    ingested_time = EXCLUDED.ingested_time,
    raw_s3_uri = EXCLUDED.raw_s3_uri,
    raw_version_id = EXCLUDED.raw_version_id,
    parsed_s3_uri = EXCLUDED.parsed_s3_uri,
    parsed_version_id = EXCLUDED.parsed_version_id
""".strip()

SQL_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
INVALID_IDENTIFIER = "Aurora database and table names must be safe SQL identifiers"
SCHEMA_INITIALIZATION_FAILED = "Aurora metadata schema initialization failed"
UPSERT_FAILED = "Aurora metadata upsert failed"


class MetadataStoreError(RuntimeError):
    """Sanitized Aurora Data API failure with no SQL values or identifiers."""


class _DataApiClient(Protocol):
    def execute_statement(self, **kwargs: Any) -> dict[str, Any]: ...

    def batch_execute_statement(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class MetadataRecord:
    """One normalized metadata row shared by HL7 and CCDA documents."""

    document_id: str
    source_format: str
    document_time: str | None
    ingested_time: str
    raw_s3_uri: str
    raw_version_id: str | None
    parsed_s3_uri: str
    parsed_version_id: str | None


class DataApiMetadataStore:
    """Creates and idempotently upserts document metadata through Aurora Data API."""

    def __init__(
        self,
        client: _DataApiClient,
        *,
        cluster_arn: str,
        secret_arn: str,
        database: str,
        table_name: str,
    ) -> None:
        # SQL identifiers cannot be bound parameters, so validate before interpolation.
        if not SQL_IDENTIFIER_PATTERN.fullmatch(database) or not SQL_IDENTIFIER_PATTERN.fullmatch(
            table_name
        ):
            raise ValueError(INVALID_IDENTIFIER)
        self._client = client
        self._request = {
            "resourceArn": cluster_arn,
            "secretArn": secret_arn,
            "database": database,
        }
        self._create_sql = CREATE_TABLE_SQL.replace("document_metadata", table_name)
        self._index_sqls = tuple(
            statement.replace("document_metadata", table_name) for statement in CREATE_INDEX_SQLS
        )
        self._upsert_sql = UPSERT_SQL.replace("document_metadata", table_name)
        # Cache initialization per warm Lambda environment; CREATE TABLE remains idempotent.
        self._schema_ready = False

    def upsert(self, records: list[MetadataRecord]) -> None:
        """Initialize the schema when needed and idempotently upsert a source batch."""
        if not records:
            return
        self._ensure_schema()
        try:
            self._client.batch_execute_statement(
                **self._request,
                sql=self._upsert_sql,
                parameterSets=[_parameters(record) for record in records],
            )
        except Exception:
            raise MetadataStoreError(UPSERT_FAILED) from None

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        try:
            for statement in (self._create_sql, *self._index_sqls):
                self._client.execute_statement(
                    **self._request,
                    sql=statement,
                    continueAfterTimeout=True,
                )
        except Exception:
            raise MetadataStoreError(SCHEMA_INITIALIZATION_FAILED) from None
        self._schema_ready = True


def _parameters(record: MetadataRecord) -> list[dict[str, Any]]:
    return [
        _parameter("document_id", record.document_id),
        _parameter("source_format", record.source_format),
        _parameter("document_time", record.document_time),
        _parameter("ingested_time", record.ingested_time),
        _parameter("raw_s3_uri", record.raw_s3_uri),
        _parameter("raw_version_id", record.raw_version_id),
        _parameter("parsed_s3_uri", record.parsed_s3_uri),
        _parameter("parsed_version_id", record.parsed_version_id),
    ]


def _parameter(name: str, value: str | None) -> dict[str, Any]:
    return {
        "name": name,
        "value": {"isNull": True} if value is None else {"stringValue": value},
    }
