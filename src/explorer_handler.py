"""Authenticated metadata and message-body explorer for the HTTP API."""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from src.metadata_store import SQL_IDENTIFIER_PATTERN

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 200
DEFAULT_MAX_BODY_BYTES = 4 * 1024 * 1024
MAX_SQL_CHARACTERS = 100_000
MAX_SQL_RESULT_BYTES = 4 * 1024 * 1024
DOCUMENT_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SOURCE_FORMATS = frozenset({"hl7-v2", "ccda"})
INVALID_DATABASE_CONFIG = "Explorer database configuration is invalid"
INVALID_STORAGE_CONFIG = "Explorer storage configuration is invalid"
INVALID_STORAGE_REFERENCE = "Message storage reference is invalid"
BODY_RETRIEVAL_FAILED = "Message body retrieval failed"
METADATA_READ_FAILED = "Metadata read failed"
SQL_EXECUTION_FAILED = "SQL query failed"
LIST_COLUMNS = ("document_id", "source_format", "document_time", "ingested_time")
DETAIL_COLUMNS = (
    "document_id",
    "source_format",
    "document_time",
    "ingested_time",
    "raw_s3_uri",
    "raw_version_id",
    "parsed_s3_uri",
    "parsed_version_id",
)
LIST_SQL = """
SELECT document_id, source_format, document_time, ingested_time
FROM document_metadata
""".strip()
COUNT_SQL = """
SELECT COUNT(*)
FROM document_metadata
""".strip()
DETAIL_SQL = """
SELECT
    document_id,
    source_format,
    document_time,
    ingested_time,
    raw_s3_uri,
    raw_version_id,
    parsed_s3_uri,
    parsed_version_id
FROM document_metadata
WHERE document_id = :document_id
LIMIT 1
""".strip()


class _DataApiClient(Protocol):
    def execute_statement(self, **kwargs: Any) -> dict[str, Any]: ...


class _ReadableBody(Protocol):
    def read(self, amount: int | None = None) -> bytes: ...


class _S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


class ExplorerError(RuntimeError):
    """Sanitized explorer failure that never includes SQL or clinical values."""


class RequestError(ExplorerError):
    """Expected request failure safe to return to the authenticated caller."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


@dataclass(frozen=True)
class BodyResult:
    """Bounded message content returned through API Gateway."""

    content: bytes
    content_type: str


class MessageExplorer:
    """Explore stored messages and execute authenticated SQL through the Data API."""

    def __init__(
        self,
        data_api: _DataApiClient,
        s3_client: _S3Client,
        *,
        cluster_arn: str,
        secret_arn: str,
        database: str,
        table_name: str,
        raw_bucket: str,
        parsed_bucket: str,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    ) -> None:
        if not SQL_IDENTIFIER_PATTERN.fullmatch(database) or not SQL_IDENTIFIER_PATTERN.fullmatch(
            table_name
        ):
            raise ValueError(INVALID_DATABASE_CONFIG)
        if not raw_bucket or not parsed_bucket or max_body_bytes < 1:
            raise ValueError(INVALID_STORAGE_CONFIG)
        self._data_api = data_api
        self._s3 = s3_client
        self._request = {
            "resourceArn": cluster_arn,
            "secretArn": secret_arn,
            "database": database,
        }
        self._list_sql = LIST_SQL.replace("document_metadata", table_name)
        self._count_sql = COUNT_SQL.replace("document_metadata", table_name)
        self._detail_sql = DETAIL_SQL.replace("document_metadata", table_name)
        self._raw_bucket = raw_bucket
        self._parsed_bucket = parsed_bucket
        self._max_body_bytes = max_body_bytes

    def list_messages(self, query: dict[str, str]) -> dict[str, Any]:
        """Return one newest-first keyset page from the narrow metadata projection."""
        limit = _parse_limit(query.get("limit"))
        from_time = _optional_time(query.get("from"))
        to_time = _optional_time(query.get("to"))
        if from_time is not None and to_time is not None and from_time[0] > to_time[0]:
            raise RequestError(400, "invalid_time_range")

        source_format = query.get("source_format")
        if source_format is not None and source_format not in SOURCE_FORMATS:
            raise RequestError(400, "invalid_source_format")
        cursor = _decode_cursor(query.get("cursor"))

        filter_conditions: list[str] = []
        filter_parameters: list[dict[str, Any]] = []
        if from_time is not None:
            filter_conditions.append("ingested_time >= CAST(:from_time AS TIMESTAMPTZ)")
            filter_parameters.append(_string_parameter("from_time", from_time[1]))
        if to_time is not None:
            filter_conditions.append("ingested_time < CAST(:to_time AS TIMESTAMPTZ)")
            filter_parameters.append(_string_parameter("to_time", to_time[1]))
        if source_format is not None:
            filter_conditions.append("source_format = :source_format")
            filter_parameters.append(_string_parameter("source_format", source_format))

        conditions = list(filter_conditions)
        parameters = list(filter_parameters)
        if cursor is not None:
            conditions.append(
                "(ingested_time, document_id) < "
                "(CAST(:cursor_time AS TIMESTAMPTZ), "
                "CAST(:cursor_document_id AS TEXT))"
            )
            parameters.extend(
                [
                    _string_parameter("cursor_time", cursor[0]),
                    _string_parameter("cursor_document_id", cursor[1]),
                ]
            )

        sql = self._list_sql
        if conditions:
            sql += "\nWHERE " + "\n  AND ".join(conditions)
        sql += "\nORDER BY ingested_time DESC, document_id DESC\nLIMIT :page_size"
        parameters.append(_long_parameter("page_size", limit + 1))
        rows = self._execute(sql, parameters, LIST_COLUMNS)
        total_count = self._count_messages(filter_conditions, filter_parameters)
        has_more = len(rows) > limit
        page = rows[:limit]
        next_cursor = None
        if has_more and page:
            next_cursor = _encode_cursor(
                cast(str, page[-1]["ingested_time"]),
                cast(str, page[-1]["document_id"]),
            )
        return {
            "items": [_public_row(row, include_locations=False) for row in page],
            "nextCursor": next_cursor,
            "totalCount": total_count,
        }

    def execute_sql(self, sql: str, caller_sub: str) -> dict[str, Any]:
        """Execute one caller-supplied SQL statement and return a bounded tabular result."""
        if not sql.strip() or len(sql) > MAX_SQL_CHARACTERS:
            raise RequestError(400, "invalid_sql")
        try:
            response = self._data_api.execute_statement(
                **self._request,
                sql=sql,
                includeResultMetadata=True,
            )
        except Exception:
            raise RequestError(400, "sql_query_failed") from None
        try:
            result = _sql_result(response)
        except RequestError:
            raise
        except (KeyError, TypeError, ValueError):
            raise ExplorerError(SQL_EXECUTION_FAILED) from None
        rows = cast(list[list[Any]], result["rows"])
        updated = cast(int, result["numberOfRecordsUpdated"])

        LOGGER.info(
            json.dumps(
                {
                    "callerSub": caller_sub,
                    "event": "sql_query_executed",
                    "numberOfRecordsUpdated": updated,
                    "rowCount": len(rows),
                    "timestamp": datetime.now(UTC).isoformat(),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return result

    def _count_messages(
        self,
        conditions: list[str],
        parameters: list[dict[str, Any]],
    ) -> int:
        sql = self._count_sql
        if conditions:
            sql += "\nWHERE " + "\n  AND ".join(conditions)
        rows = self._execute(sql, list(parameters), ("total_count",))
        if len(rows) != 1:
            raise ExplorerError(METADATA_READ_FAILED)
        total_count = rows[0]["total_count"]
        if isinstance(total_count, bool) or not isinstance(total_count, int) or total_count < 0:
            raise ExplorerError(METADATA_READ_FAILED)
        return total_count

    def get_message(self, document_id: str) -> dict[str, Any]:
        """Return one metadata row by deterministic document ID."""
        row = self._metadata_row(document_id)
        return _public_row(row, include_locations=True)

    def get_body(self, document_id: str, variant: str, caller_sub: str) -> BodyResult:
        """Fetch one exact raw or parsed S3 version and emit a PHI-access audit event."""
        if variant not in {"raw", "parsed"}:
            raise RequestError(400, "invalid_variant")
        row = self._metadata_row(document_id)
        uri_key = "raw_s3_uri" if variant == "raw" else "parsed_s3_uri"
        version_key = "raw_version_id" if variant == "raw" else "parsed_version_id"
        expected_bucket = self._raw_bucket if variant == "raw" else self._parsed_bucket
        bucket, key = _parse_s3_uri(cast(str, row[uri_key]))
        if bucket != expected_bucket:
            raise ExplorerError(INVALID_STORAGE_REFERENCE)

        request: dict[str, Any] = {"Bucket": bucket, "Key": key}
        version_id = row[version_key]
        if isinstance(version_id, str) and version_id:
            request["VersionId"] = version_id
        try:
            response = self._s3.get_object(**request)
        except Exception:
            raise ExplorerError(BODY_RETRIEVAL_FAILED) from None
        if int(response.get("ContentLength", 0)) > self._max_body_bytes:
            raise RequestError(413, "message_body_too_large")
        content = _read_bounded(response["Body"], self._max_body_bytes)

        LOGGER.info(
            json.dumps(
                {
                    "callerSub": caller_sub,
                    "documentId": document_id,
                    "event": "message_body_fetched",
                    "timestamp": datetime.now(UTC).isoformat(),
                    "variant": variant,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        if variant == "parsed":
            content_type = "application/json"
        elif row["source_format"] == "ccda":
            content_type = "application/xml; charset=utf-8"
        else:
            content_type = "text/plain; charset=utf-8"
        return BodyResult(content=content, content_type=content_type)

    def _metadata_row(self, document_id: str) -> dict[str, Any]:
        _validate_document_id(document_id)
        rows = self._execute(
            self._detail_sql,
            [_string_parameter("document_id", document_id)],
            DETAIL_COLUMNS,
        )
        if not rows:
            raise RequestError(404, "message_not_found")
        return rows[0]

    def _execute(
        self,
        sql: str,
        parameters: list[dict[str, Any]],
        columns: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        try:
            response = self._data_api.execute_statement(
                **self._request,
                sql=sql,
                parameters=parameters,
            )
        except Exception:
            raise ExplorerError(METADATA_READ_FAILED) from None
        records = response.get("records", [])
        if not isinstance(records, list):
            raise ExplorerError(METADATA_READ_FAILED)
        try:
            return [_record(columns, fields) for fields in records]
        except (KeyError, TypeError, ValueError):
            raise ExplorerError(METADATA_READ_FAILED) from None


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """Route authenticated HTTP API v2 events without exposing sensitive errors."""
    route_key = str(event.get("routeKey", ""))
    try:
        return _route_request(event, route_key)
    except RequestError as error:
        return _json_response(error.status_code, {"error": error.code})
    except Exception as error:
        LOGGER.error(  # noqa: TRY400 - tracebacks could contain SDK request details
            json.dumps(
                {
                    "errorType": type(error).__name__,
                    "event": "explorer_request_failed",
                    "route": route_key,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return _json_response(500, {"error": "explorer_request_failed"})


def _route_request(event: dict[str, Any], route_key: str) -> dict[str, Any]:
    caller_sub = _caller_sub(event)
    explorer = _runtime_explorer()
    if route_key == "GET /messages":
        query = event.get("queryStringParameters") or {}
        if not isinstance(query, dict):
            raise RequestError(400, "invalid_query")
        return _json_response(200, explorer.list_messages(cast(dict[str, str], query)))
    if route_key == "POST /query":
        request = _request_json(event)
        sql = request.get("sql")
        if not isinstance(sql, str):
            raise RequestError(400, "invalid_sql")
        return _json_response(200, explorer.execute_sql(sql, caller_sub))
    if route_key == "GET /messages/{documentId}":
        document_id = _path_document_id(event)
        return _json_response(200, explorer.get_message(document_id))
    if route_key == "POST /messages/{documentId}/body":
        document_id = _path_document_id(event)
        request = _request_json(event)
        variant = request.get("variant")
        if not isinstance(variant, str):
            raise RequestError(400, "invalid_variant")
        result = explorer.get_body(document_id, variant, caller_sub)
        return {
            "statusCode": 200,
            "headers": {
                "cache-control": "no-store",
                "content-type": result.content_type,
                "x-content-type-options": "nosniff",
            },
            "body": base64.b64encode(result.content).decode("ascii"),
            "isBase64Encoded": True,
        }
    raise RequestError(404, "route_not_found")


_RUNTIME_EXPLORER: MessageExplorer | None = None


def _runtime_explorer() -> MessageExplorer:
    global _RUNTIME_EXPLORER
    if _RUNTIME_EXPLORER is None:
        _RUNTIME_EXPLORER = MessageExplorer(
            _aws_client("rds-data"),
            _aws_client("s3"),
            cluster_arn=os.environ["METADATA_CLUSTER_ARN"],
            secret_arn=os.environ["METADATA_SECRET_ARN"],
            database=os.environ["METADATA_DATABASE"],
            table_name=os.environ["METADATA_TABLE"],
            raw_bucket=os.environ["RAW_BUCKET"],
            parsed_bucket=os.environ["PARSED_BUCKET"],
            max_body_bytes=int(os.getenv("MAX_BODY_BYTES", str(DEFAULT_MAX_BODY_BYTES))),
        )
    return _RUNTIME_EXPLORER


def _caller_sub(event: dict[str, Any]) -> str:
    try:
        sub = event["requestContext"]["authorizer"]["jwt"]["claims"]["sub"]
    except (KeyError, TypeError):
        raise RequestError(401, "authentication_required") from None
    if not isinstance(sub, str) or not sub or len(sub) > 128:
        raise RequestError(401, "authentication_required")
    return sub


def _path_document_id(event: dict[str, Any]) -> str:
    parameters = event.get("pathParameters")
    if not isinstance(parameters, dict):
        raise RequestError(400, "invalid_document_id")
    document_id = parameters.get("documentId")
    if not isinstance(document_id, str):
        raise RequestError(400, "invalid_document_id")
    _validate_document_id(document_id)
    return document_id


def _validate_document_id(document_id: str) -> None:
    if not DOCUMENT_ID_PATTERN.fullmatch(document_id):
        raise RequestError(400, "invalid_document_id")


def _request_json(event: dict[str, Any]) -> dict[str, Any]:
    body = event.get("body")
    if not isinstance(body, str):
        raise RequestError(400, "invalid_json")
    try:
        if event.get("isBase64Encoded") is True:
            body = base64.b64decode(body, validate=True).decode("utf-8")
        parsed = json.loads(body)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
        raise RequestError(400, "invalid_json") from None
    if not isinstance(parsed, dict):
        raise RequestError(400, "invalid_json")
    return cast(dict[str, Any], parsed)


def _parse_limit(value: str | None) -> int:
    if value is None:
        return DEFAULT_PAGE_LIMIT
    try:
        limit = int(value)
    except ValueError:
        raise RequestError(400, "invalid_limit") from None
    if str(limit) != value or not 1 <= limit <= MAX_PAGE_LIMIT:
        raise RequestError(400, "invalid_limit")
    return limit


def _optional_time(value: str | None) -> tuple[datetime, str] | None:
    if value is None:
        return None
    if len(value) > 64:
        raise RequestError(400, "invalid_time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise RequestError(400, "invalid_time") from None
    if parsed.tzinfo is None:
        raise RequestError(400, "invalid_time")
    return parsed, value


def _encode_cursor(ingested_time: str, document_id: str) -> str:
    payload = json.dumps(
        {
            "documentId": document_id,
            "ingestedTime": _normalize_cursor_time(ingested_time),
            "version": 1,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(value: str | None) -> tuple[str, str] | None:
    if value is None:
        return None
    if not value or len(value) > 2048:
        raise RequestError(400, "invalid_cursor")
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        payload = json.loads(decoded)
        ingested_time = payload["ingestedTime"]
        document_id = payload["documentId"]
        ingested_time = _validate_cursor_payload(payload, ingested_time, document_id)
    except (
        binascii.Error,
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ):
        raise RequestError(400, "invalid_cursor") from None
    return ingested_time, document_id


def _validate_cursor_payload(payload: Any, ingested_time: Any, document_id: Any) -> str:
    expected = {
        "documentId": document_id,
        "ingestedTime": ingested_time,
        "version": 1,
    }
    if payload != expected:
        raise ValueError
    if not isinstance(ingested_time, str) or not isinstance(document_id, str):
        raise TypeError
    if not DOCUMENT_ID_PATTERN.fullmatch(document_id):
        raise ValueError
    return _normalize_cursor_time(ingested_time)


def _normalize_cursor_time(value: str) -> str:
    if not value or len(value) > 64:
        raise ValueError
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _record(columns: tuple[str, ...], fields: Any) -> dict[str, Any]:
    if not isinstance(fields, list) or len(fields) != len(columns):
        raise ValueError
    return {column: _field_value(field) for column, field in zip(columns, fields, strict=True)}


def _field_value(field: Any) -> Any:
    if not isinstance(field, dict):
        raise TypeError
    if field.get("isNull") is True:
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in field:
            return field[key]
    raise KeyError


def _public_row(row: dict[str, Any], *, include_locations: bool) -> dict[str, Any]:
    result = {
        "documentId": row["document_id"],
        "sourceFormat": row["source_format"],
        "documentTime": row["document_time"],
        "ingestedTime": row["ingested_time"],
    }
    if include_locations:
        result.update(
            {
                "rawS3Uri": row["raw_s3_uri"],
                "rawVersionId": row["raw_version_id"],
                "parsedS3Uri": row["parsed_s3_uri"],
                "parsedVersionId": row["parsed_version_id"],
            }
        )
    return result


def _string_parameter(name: str, value: str) -> dict[str, Any]:
    return {"name": name, "value": {"stringValue": value}}


def _long_parameter(name: str, value: int) -> dict[str, Any]:
    return {"name": name, "value": {"longValue": value}}


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ExplorerError(INVALID_STORAGE_REFERENCE)
    bucket, separator, key = uri[5:].partition("/")
    if not separator or not bucket or not key:
        raise ExplorerError(INVALID_STORAGE_REFERENCE)
    return bucket, key


def _read_bounded(body: Any, max_bytes: int) -> bytes:
    if not hasattr(body, "read"):
        raise ExplorerError(BODY_RETRIEVAL_FAILED)
    payload = cast(_ReadableBody, body).read(max_bytes + 1)
    if not isinstance(payload, bytes):
        raise ExplorerError(BODY_RETRIEVAL_FAILED)
    if len(payload) > max_bytes:
        raise RequestError(413, "message_body_too_large")
    return payload


def _aws_client(service: str) -> Any:
    import boto3  # type: ignore[import-not-found]

    return boto3.client(service)


def _json_response(status_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "cache-control": "no-store",
            "content-type": "application/json",
            "x-content-type-options": "nosniff",
        },
        "body": json.dumps(payload, separators=(",", ":"), sort_keys=True),
    }


def _sql_column_name(metadata: Any, index: int) -> str:
    if not isinstance(metadata, dict):
        raise TypeError
    name = metadata.get("label") or metadata.get("name")
    return name if isinstance(name, str) and name else f"column_{index + 1}"


def _sql_record(fields: Any, column_count: int) -> list[Any]:
    if not isinstance(fields, list) or len(fields) != column_count:
        raise ValueError
    return [_sql_field_value(field) for field in fields]


def _sql_field_value(field: Any) -> Any:
    if not isinstance(field, dict):
        raise TypeError
    if field.get("isNull") is True:
        return None
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in field:
            return field[key]
    if "blobValue" in field:
        blob = field["blobValue"]
        if not isinstance(blob, bytes):
            raise TypeError
        return base64.b64encode(blob).decode("ascii")
    if "arrayValue" in field:
        return _sql_array_value(field["arrayValue"])
    raise KeyError


def _sql_array_value(value: Any) -> list[Any]:
    if not isinstance(value, dict):
        raise TypeError
    for key in ("stringValues", "longValues", "doubleValues", "booleanValues"):
        if key in value:
            items = value[key]
            if not isinstance(items, list):
                raise TypeError
            return items
    if "arrayValues" in value:
        items = value["arrayValues"]
        if not isinstance(items, list):
            raise TypeError
        return [_sql_array_value(item) for item in items]
    return []


def _sql_result(response: dict[str, Any]) -> dict[str, Any]:
    metadata = response.get("columnMetadata", [])
    records = response.get("records", [])
    if not isinstance(metadata, list) or not isinstance(records, list):
        raise TypeError
    columns = [_sql_column_name(item, index) for index, item in enumerate(metadata)]
    rows = [_sql_record(record, len(columns)) for record in records]
    updated = response.get("numberOfRecordsUpdated", 0)
    if not isinstance(updated, int):
        raise TypeError
    result: dict[str, Any] = {
        "columns": columns,
        "rows": rows,
        "numberOfRecordsUpdated": updated,
    }
    if len(json.dumps(result, separators=(",", ":")).encode("utf-8")) > MAX_SQL_RESULT_BYTES:
        raise RequestError(413, "sql_result_too_large")
    return result
