"""Persist and retrieve Reports definitions as row-granular items in one DynamoDB table.

Every report is stored as a small set of items sharing one partition key so a single
``Query`` reassembles the whole definition in item order:

* ``PK = REPORT#<id>`` for every item belonging to the report.
* ``SK = META`` holds the report header and a ``draft`` visibility flag.
* ``SK = SECTION#<storage_seq:03d>`` holds one section header.
* ``SK = SECTION#<storage_seq:03d>#ROW#<storage_seq:03d>`` holds one row.
* ``SK = AUDIT#...`` records one immutable audit entry (ignored when assembling a report).

``storage_seq`` is an ordering key allocated by the catalog (10, 20, 30, ... on import)
and is kept separate from the definition's own ``seq`` values, which are stored verbatim.
Because the sort key sorts ``META`` before every ``SECTION#`` and each section header
before its own rows, one ``Query`` with ``ScanIndexForward=True`` yields the header, then
each section immediately followed by its rows. The assembled *clean* definition contains
only the definition ``seq`` values, so a canonical import -> items -> export round trip is
byte-identical under ``json.dumps(sort_keys=True, separators=(",", ":"))``. Editor
metadata (``storageSeq`` addresses and row ``updatedAt`` locks) is returned separately so a
handler can drive optimistic row edits without leaking it into the definition body.

Imports are atomic to readers: the ``META`` item is created ``draft=true`` under an
``attribute_not_exists`` guard, sections and rows are written with ``batch_write_item`` in
chunks of at most 25, and the report is published by a single ``TransactWriteItems`` that
flips ``draft`` to ``false`` and writes exactly one ``action=import`` audit item tagged with
its ``source``; no per-row audit items are written on import. Any failure before publish
leaves an invisible draft that ``list_reports`` (which scans only ``META`` items with
``draft=false``) never returns.

Every application-level mutation (``update_row``, ``add_row``, ``delete_row``,
``add_section``) is a single ``TransactWriteItems`` that pairs the mutation with a ``Put`` of
one ``AUDIT#`` item, so the audit row is committed if and only if the mutation commits.
Updates and deletes first read the prior row with a consistent ``get_item`` and record the
full prior item as compact JSON under ``previous``; the same ``expected_updated_at``
condition guards the mutation inside the transaction, so a losing race aborts both writes.
Adds allocate a gapped ``storage_seq`` (append by default, midpoint when inserting into a
gap) and never renumber existing items. ``history`` returns the most recent audit items for
a report in reverse chronological order. The service methods are route neutral: they return
plain data or raise typed, sanitized errors so an HTTP handler owns request parsing and
response shaping.

``delete_report`` removes a whole report idempotently: a single ``Query`` reads the
partition, and when ``META`` is present one ``TransactWriteItems`` deletes it while writing a
durable ``action=delete`` audit under a separate ``AUDITLOG#deleted-reports`` partition
(keyed ``REPORT#<id>#<timestamp>``) carrying the prior clean definition, after which every
remaining ``REPORT#`` item is batch-deleted in chunks of at most 25 with the same retries as
import. Because the deletion audit lives outside the report partition it survives the
cleanup, and a call that finds no ``META`` writes no duplicate audit, so repeats and partial
retries are safe. ``replace_report`` swaps a report's whole definition atomically to readers:
it flips ``META.draft`` to ``true`` under an ``expected_updated_at`` condition without
advancing the lock, batch-deletes only the ``SECTION#`` items, writes the new sections and
rows with fresh gapped ``storage_seq`` values, and republishes with a single
``TransactWriteItems`` that updates the ``META`` header fields, clears ``draft``, stamps a new
lock under the original condition, and writes one ``action=update`` audit carrying the prior
definition. A failure before that final transaction leaves an invisible draft that can be
retried with the original lock.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from src.report_definition import (
    ReportDefinition,
    ReportDefinitionError,
    load_report_definition_json,
    load_report_row,
)

DEFAULT_MAX_CATALOG_ITEMS = 1000

# storage_seq values are gapped so inserts land at a midpoint without renumbering, and
# are zero padded to three digits so the sort key orders them lexicographically.
STORAGE_STEP = 10
MAX_STORAGE_SEQ = 999
BATCH_WRITE_MAX = 25
_UNPROCESSED_RETRIES = 5

# Report ids are used as partition-key components, so restrict them to a safe token.
REPORT_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,127}$")

SK_META = "META"
AUDIT_PREFIX = "AUDIT#"

# Deletion audits outlive the report partition, so they are recorded under their own
# partition keyed by report id and delete timestamp: PK=AUDITLOG#deleted-reports,
# SK=REPORT#<id>#<ISO timestamp>.
DELETED_REPORTS_PK = "AUDITLOG#deleted-reports"

# Audit actions recorded alongside each committed mutation.
AUDIT_CREATE = "create"
AUDIT_UPDATE = "update"
AUDIT_DELETE = "delete"
AUDIT_IMPORT = "import"

# Sources an import may be attributed to; the API uses "api" and the seed tool the others.
SOURCE_API = "api"
SOURCE_SEED = "seed"
SOURCE_MIGRATION = "legacy-s3-migration"
_ALLOWED_IMPORT_SOURCES = frozenset({SOURCE_API, SOURCE_SEED, SOURCE_MIGRATION})

# Fallback audit actor used when a caller identity is not supplied for a delete or a section
# add. Callers that know the authenticated principal pass ``updated_by`` explicitly.
DEFAULT_ACTOR = "system"

# history() bounds: at least one item, never more than a page of two hundred.
HISTORY_MIN_LIMIT = 1
HISTORY_MAX_LIMIT = 200
DEFAULT_HISTORY_LIMIT = 50

INVALID_STORAGE_CONFIG = "Catalog storage configuration is invalid"
CATALOG_READ_FAILED = "Catalog read failed"
CATALOG_SAVE_FAILED = "Catalog save failed"
STORED_DEFINITION_INVALID = "Stored report definition is unreadable"

# Short, caller-safe codes for expected request failures.
INVALID_REPORT_ID = "invalid_report_id"
INVALID_CALLER = "invalid_caller"
INVALID_PRECONDITION = "invalid_precondition"
DEFINITION_INVALID = "invalid_definition"
DEFINITION_NOT_FOUND = "report_not_found"
SECTION_NOT_FOUND = "section_not_found"
REPORT_ALREADY_EXISTS = "report_already_exists"
EDIT_CONFLICT = "edit_conflict"
SEQUENCE_SPACE_EXHAUSTED = "sequence_space_exhausted"
INVALID_LIMIT = "invalid_limit"
INVALID_SOURCE = "invalid_source"

# botocore ClientError codes that mean a conditional write precondition was not satisfied.
_PRECONDITION_CODES = frozenset({"ConditionalCheckFailedException"})

# A TransactWriteItems is cancelled (raising TransactionCanceledException) when any of its
# conditions fails; a bare ConditionalCheckFailedException can surface the same way.
_TRANSACTION_CONFLICT_CODES = frozenset(
    {"ConditionalCheckFailedException", "TransactionCanceledException"}
)


class _DynamoClient(Protocol):
    def put_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def update_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def delete_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def query(self, **kwargs: Any) -> dict[str, Any]: ...

    def scan(self, **kwargs: Any) -> dict[str, Any]: ...

    def batch_write_item(self, **kwargs: Any) -> dict[str, Any]: ...

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]: ...


class CatalogError(RuntimeError):
    """Sanitized catalog failure that never includes DynamoDB details."""


class CatalogRequestError(CatalogError):
    """Expected catalog failure safe to return to the authenticated caller."""

    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


class ReportCatalog:
    """List, read, import, and row-granularly edit Reports definitions in DynamoDB."""

    def __init__(
        self,
        dynamo_client: _DynamoClient,
        *,
        table_name: str,
        max_catalog_items: int = DEFAULT_MAX_CATALOG_ITEMS,
    ) -> None:
        if not table_name or max_catalog_items < 1:
            raise ValueError(INVALID_STORAGE_CONFIG)
        self._dynamo = dynamo_client
        self._table = table_name
        self._max_catalog_items = max_catalog_items

    # -- listing and reading ------------------------------------------------------------

    def list_reports(self) -> list[dict[str, Any]]:
        """Return listing metadata for every published (non-draft) report, sorted by id."""
        entries: list[dict[str, Any]] = []
        request: dict[str, Any] = {
            "TableName": self._table,
            "FilterExpression": "SK = :meta AND #draft = :false",
            "ExpressionAttributeNames": {"#draft": "draft"},
            "ExpressionAttributeValues": {
                ":meta": {"S": SK_META},
                ":false": {"BOOL": False},
            },
        }
        try:
            while True:
                response = self._dynamo.scan(**request)
                for raw in _items(response):
                    entries.append(_listing_entry(raw))
                start_key = response.get("LastEvaluatedKey")
                if len(entries) >= self._max_catalog_items or not start_key:
                    break
                request["ExclusiveStartKey"] = start_key
        except CatalogError:
            raise
        except Exception:
            raise CatalogError(CATALOG_READ_FAILED) from None
        entries = entries[: self._max_catalog_items]
        return sorted(entries, key=lambda entry: entry["reportId"])

    def get_report(self, report_id: str) -> dict[str, Any]:
        """Assemble one report into a clean definition plus separate editor metadata."""
        meta, sections = self._assemble(report_id)
        return {
            "reportId": report_id,
            "definition": _clean_definition(report_id, meta, sections),
            "editor": _editor_metadata(report_id, meta, sections),
        }

    def export_report(self, report_id: str) -> dict[str, Any]:
        """Return the clean definition mapping and its canonical JSON text."""
        meta, sections = self._assemble(report_id)
        definition = _clean_definition(report_id, meta, sections)
        return {
            "reportId": report_id,
            "definition": definition,
            "text": _canonical_text(definition),
        }

    def history(self, report_id: str, limit: int = DEFAULT_HISTORY_LIMIT) -> list[dict[str, Any]]:
        """Return the most recent audit entries for a report, newest first.

        ``limit`` must be between 1 and 200. A single ``Query`` on the report partition with
        ``begins_with(SK, 'AUDIT#')`` and ``ScanIndexForward=False`` returns at most ``limit``
        audit items in reverse chronological order. Each entry is parsed into plain data,
        including the ``previous`` object and ``source`` when present.
        """
        _validate_report_id(report_id)
        bounded = _validate_history_limit(limit)
        request: dict[str, Any] = {
            "TableName": self._table,
            "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": {"S": _pk(report_id)},
                ":prefix": {"S": AUDIT_PREFIX},
            },
            "ScanIndexForward": False,
            "Limit": bounded,
        }
        try:
            response = self._dynamo.query(**request)
        except Exception:
            raise CatalogError(CATALOG_READ_FAILED) from None
        return [_parse_audit(raw) for raw in _items(response)]

    # -- import -------------------------------------------------------------------------

    def import_report(
        self, definition_text: str, *, updated_by: str, source: str = SOURCE_API
    ) -> dict[str, Any]:
        """Validate and import a whole definition as an atomically-published set of items.

        The definition is validated through the definition core, the ``META`` item is
        created ``draft=true`` under an ``attribute_not_exists`` guard (a conflicting
        report id is a sanitized 409), sections and rows are written in ``batch_write_item``
        chunks of at most 25, and the report is published by a single ``TransactWriteItems``
        that flips ``draft`` to ``false`` and writes exactly one ``action=import`` audit item
        tagged with ``source``. Any failure before the publish transaction leaves an
        invisible draft.
        """
        caller = _validate_caller(updated_by)
        origin = _validate_source(source)
        try:
            definition = load_report_definition_json(definition_text)
        except ReportDefinitionError:
            raise CatalogRequestError(400, DEFINITION_INVALID) from None
        _validate_report_id(definition.report_id)
        updated_at = _now()
        self._create_draft_meta(definition, caller, updated_at)
        self._write_body(definition, caller, updated_at)
        self._publish(definition.report_id, caller, updated_at, origin)
        return {
            "reportId": definition.report_id,
            "name": definition.name,
            "description": definition.description,
            "updatedAt": updated_at,
            "updatedBy": caller,
        }

    def _create_draft_meta(
        self,
        definition: ReportDefinition,
        caller: str,
        updated_at: str,
    ) -> None:
        item = {
            "PK": {"S": _pk(definition.report_id)},
            "SK": {"S": SK_META},
            "draft": {"BOOL": True},
            "name": {"S": definition.name},
            "description": {"S": definition.description},
            "partition_field": {"S": definition.partition_field},
            "time_field": {"S": definition.time_field},
            "updated_at": {"S": updated_at},
            "updated_by": {"S": caller},
        }
        try:
            self._dynamo.put_item(
                TableName=self._table,
                Item=item,
                ConditionExpression="attribute_not_exists(PK)",
            )
        except Exception as error:
            if _client_error_code(error) in _PRECONDITION_CODES:
                raise CatalogRequestError(409, REPORT_ALREADY_EXISTS) from None
            raise CatalogError(CATALOG_SAVE_FAILED) from None

    def _write_body(
        self,
        definition: ReportDefinition,
        caller: str,
        updated_at: str,
    ) -> None:
        report_id = definition.report_id
        requests: list[dict[str, Any]] = []
        for section_index, section in enumerate(definition.sections):
            section_storage = _import_storage_seq(section_index)
            requests.append(
                {
                    "PutRequest": {
                        "Item": _section_item(report_id, section_storage, section.seq, section.name)
                    }
                }
            )
            for row_index, row in enumerate(section.rows):
                row_storage = _import_storage_seq(row_index)
                requests.append(
                    {
                        "PutRequest": {
                            "Item": _row_item(
                                report_id,
                                section_storage,
                                row_storage,
                                row.model_dump(mode="json"),
                                caller,
                                updated_at,
                            )
                        }
                    }
                )
        for chunk in _chunks(requests, BATCH_WRITE_MAX):
            self._batch_write(chunk)

    def _batch_write(self, requests: list[dict[str, Any]]) -> None:
        pending: dict[str, list[dict[str, Any]]] = {self._table: requests}
        for _ in range(_UNPROCESSED_RETRIES + 1):
            pending = self._batch_write_once(pending)
            if not pending:
                return
        raise CatalogError(CATALOG_SAVE_FAILED)

    def _batch_write_once(
        self, pending: dict[str, list[dict[str, Any]]]
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            response = self._dynamo.batch_write_item(RequestItems=pending)
        except Exception:
            raise CatalogError(CATALOG_SAVE_FAILED) from None
        unprocessed = response.get("UnprocessedItems") or {}
        return {table: items for table, items in unprocessed.items() if items}

    def _publish(self, report_id: str, caller: str, updated_at: str, source: str) -> None:
        transact_items = [
            {
                "Update": {
                    "TableName": self._table,
                    "Key": {"PK": {"S": _pk(report_id)}, "SK": {"S": SK_META}},
                    "UpdateExpression": "SET #draft = :false",
                    "ConditionExpression": "attribute_exists(PK)",
                    "ExpressionAttributeNames": {"#draft": "draft"},
                    "ExpressionAttributeValues": {":false": {"BOOL": False}},
                }
            },
            {
                "Put": {
                    "TableName": self._table,
                    "Item": _audit_item(
                        report_id,
                        _import_audit_sk(updated_at),
                        AUDIT_IMPORT,
                        caller,
                        updated_at,
                        source=source,
                    ),
                }
            },
        ]
        try:
            self._dynamo.transact_write_items(TransactItems=transact_items)
        except Exception:
            raise CatalogError(CATALOG_SAVE_FAILED) from None

    # -- whole-report delete and replace ------------------------------------------------

    def delete_report(self, report_id: str, *, updated_by: str) -> dict[str, Any]:
        """Delete a whole report idempotently, preserving a durable off-partition audit.

        A single ``Query`` reads the whole report partition. When the ``META`` item is
        present the report still exists, so one ``TransactWriteItems`` deletes ``META`` and
        writes one durable ``action=delete`` audit under a separate ``AUDITLOG#deleted-reports``
        partition (keyed ``REPORT#<id>#<timestamp>``) carrying the full prior clean definition
        as canonical JSON; because that audit lives outside the report partition it survives
        the subsequent cleanup. Every remaining ``REPORT#`` item (section headers, rows, and
        report-scoped audit items) is then removed with ``batch_write_item`` in chunks of at
        most 25 and the same unprocessed-item retries as import.

        The operation is idempotent: when ``META`` is absent the report is already deleted (or
        a prior cleanup was interrupted), so no duplicate deletion audit is written and any
        stray remaining items are simply cleaned up. ``deleted`` reports whether a live report
        header was removed by this call.
        """
        _validate_report_id(report_id)
        caller = _validate_caller(updated_by)
        items = self._query_report(report_id)
        meta_present = any(_string(raw, "SK") == SK_META for raw in items)
        if meta_present:
            meta, sections = self._assemble_items(items)
            updated_at = _now()
            previous_text = _canonical_text(_clean_definition(report_id, meta, sections))
            self._delete_meta_with_audit(report_id, caller, updated_at, previous_text)
        remaining = [_string(raw, "SK") for raw in items if _string(raw, "SK") != SK_META]
        self._batch_delete_keys(report_id, remaining)
        return {"reportId": report_id, "deleted": meta_present}

    def _delete_meta_with_audit(
        self, report_id: str, caller: str, updated_at: str, previous_text: str
    ) -> None:
        """Delete ``META`` and durably record the deletion in one atomic transaction.

        The deletion audit is written under the dedicated ``AUDITLOG#deleted-reports``
        partition so it is never swept away by the report-partition cleanup that follows.
        """
        transact_items = [
            {
                "Delete": {
                    "TableName": self._table,
                    "Key": {"PK": {"S": _pk(report_id)}, "SK": {"S": SK_META}},
                    "ConditionExpression": "attribute_exists(PK)",
                }
            },
            {
                "Put": {
                    "TableName": self._table,
                    "Item": _deletion_audit_item(report_id, caller, updated_at, previous_text),
                }
            },
        ]
        self._transact_write(transact_items, EDIT_CONFLICT)

    def _batch_delete_keys(self, report_id: str, sort_keys: list[str]) -> None:
        """Delete the given report-partition sort keys in chunks of at most 25 with retries."""
        requests = [
            {"DeleteRequest": {"Key": {"PK": {"S": _pk(report_id)}, "SK": {"S": sk}}}}
            for sk in sort_keys
        ]
        for chunk in _chunks(requests, BATCH_WRITE_MAX):
            self._batch_write(chunk)

    def replace_report(
        self,
        definition_text: str,
        *,
        expected_updated_at: str,
        updated_by: str,
    ) -> dict[str, Any]:
        """Replace a report's whole definition under an optimistic lock, atomically to readers.

        The incoming text is validated through the definition core; the definition's own
        ``report_id`` is used and any path/id agreement is the caller's concern. The report is
        first hidden by flipping ``META.draft`` to ``true`` under an
        ``expected_updated_at`` condition **without advancing the lock**, so a replace that
        fails partway can be retried with the same ``expected_updated_at``. Only the
        ``SECTION#`` items (section headers and their rows) are batch-deleted; ``META`` and the
        audit trail are left intact. The new sections and rows are written with fresh gapped
        ``storage_seq`` values, and the report is republished by a single
        ``TransactWriteItems`` that updates the ``META`` header fields, clears ``draft``, and
        stamps a new ``updated_at``/``updated_by`` under the same optimistic-lock condition
        while writing one ``action=update`` audit carrying the full previous definition as
        canonical JSON. Any failure before that final transaction leaves an invisible draft.
        """
        caller = _validate_caller(updated_by)
        expected = _validate_precondition(expected_updated_at)
        try:
            definition = load_report_definition_json(definition_text)
        except ReportDefinitionError:
            raise CatalogRequestError(400, DEFINITION_INVALID) from None
        _validate_report_id(definition.report_id)
        report_id = definition.report_id
        # Capture the current definition for the audit trail; a missing report is a 404.
        previous_meta, previous_sections = self._assemble(report_id)
        previous_text = _canonical_text(
            _clean_definition(report_id, previous_meta, previous_sections)
        )
        # Hide the report without touching the lock so a failed replace stays retryable.
        self._begin_replace(report_id, expected)
        self._delete_section_items(report_id)
        updated_at = _now()
        self._write_body(definition, caller, updated_at)
        self._republish(definition, caller, updated_at, expected, previous_text)
        return {
            "reportId": report_id,
            "name": definition.name,
            "description": definition.description,
            "updatedAt": updated_at,
            "updatedBy": caller,
        }

    def _begin_replace(self, report_id: str, expected: str) -> None:
        """Flip ``META.draft`` to ``true`` under the optimistic lock without advancing it."""
        try:
            self._dynamo.update_item(
                TableName=self._table,
                Key={"PK": {"S": _pk(report_id)}, "SK": {"S": SK_META}},
                UpdateExpression="SET #draft = :true",
                ConditionExpression="attribute_exists(PK) AND #updated_at = :expected",
                ExpressionAttributeNames={"#draft": "draft", "#updated_at": "updated_at"},
                ExpressionAttributeValues={
                    ":true": {"BOOL": True},
                    ":expected": {"S": expected},
                },
            )
        except Exception as error:
            if _client_error_code(error) in _PRECONDITION_CODES:
                raise CatalogRequestError(409, EDIT_CONFLICT) from None
            raise CatalogError(CATALOG_SAVE_FAILED) from None

    def _delete_section_items(self, report_id: str) -> None:
        """Batch-delete only the ``SECTION#`` items (section headers and their rows)."""
        items = self._query_prefix(report_id, "SECTION#")
        self._batch_delete_keys(report_id, [_string(raw, "SK") for raw in items])

    def _republish(
        self,
        definition: ReportDefinition,
        caller: str,
        updated_at: str,
        expected: str,
        previous_text: str,
    ) -> None:
        """Publish the replaced body: update ``META`` under the lock and write the audit."""
        report_id = definition.report_id
        transact_items = [
            {
                "Update": {
                    "TableName": self._table,
                    "Key": {"PK": {"S": _pk(report_id)}, "SK": {"S": SK_META}},
                    "UpdateExpression": (
                        "SET #draft = :false, #name = :name, #description = :description, "
                        "#partition_field = :partition_field, #time_field = :time_field, "
                        "#updated_at = :updated_at, #updated_by = :updated_by"
                    ),
                    "ConditionExpression": "attribute_exists(PK) AND #updated_at = :expected",
                    "ExpressionAttributeNames": {
                        "#draft": "draft",
                        "#name": "name",
                        "#description": "description",
                        "#partition_field": "partition_field",
                        "#time_field": "time_field",
                        "#updated_at": "updated_at",
                        "#updated_by": "updated_by",
                    },
                    "ExpressionAttributeValues": {
                        ":false": {"BOOL": False},
                        ":name": {"S": definition.name},
                        ":description": {"S": definition.description},
                        ":partition_field": {"S": definition.partition_field},
                        ":time_field": {"S": definition.time_field},
                        ":updated_at": {"S": updated_at},
                        ":updated_by": {"S": caller},
                        ":expected": {"S": expected},
                    },
                }
            },
            {
                "Put": {
                    "TableName": self._table,
                    "Item": _audit_item(
                        report_id,
                        _replace_audit_sk(updated_at),
                        AUDIT_UPDATE,
                        caller,
                        updated_at,
                        previous=previous_text,
                    ),
                }
            },
        ]
        self._transact_write(transact_items, EDIT_CONFLICT)

    # -- row edits ----------------------------------------------------------------------

    def update_row(
        self,
        report_id: str,
        section_storage_seq: int,
        row_storage_seq: int,
        row: Mapping[str, Any],
        *,
        expected_updated_at: str,
        updated_by: str,
    ) -> dict[str, Any]:
        """Validate and conditionally overwrite one row, guarding on its ``updated_at``."""
        _validate_report_id(report_id)
        caller = _validate_caller(updated_by)
        expected = _validate_precondition(expected_updated_at)
        payload = _validate_row(row)
        sk = _row_sk(section_storage_seq, row_storage_seq)
        previous = self._prior_snapshot(report_id, sk)
        updated_at = _now()
        update = {
            "Update": {
                "TableName": self._table,
                "Key": {"PK": {"S": _pk(report_id)}, "SK": {"S": sk}},
                "UpdateExpression": (
                    "SET #seq = :seq, #label = :label, #description = :description, "
                    "#index = :index, #query = :query, #updated_at = :updated_at, "
                    "#updated_by = :updated_by"
                ),
                "ConditionExpression": "attribute_exists(PK) AND #updated_at = :expected",
                "ExpressionAttributeNames": {
                    "#seq": "seq",
                    "#label": "label",
                    "#description": "description",
                    "#index": "index",
                    "#query": "query",
                    "#updated_at": "updated_at",
                    "#updated_by": "updated_by",
                },
                "ExpressionAttributeValues": {
                    ":seq": {"N": str(payload["seq"])},
                    ":label": {"S": payload["label"]},
                    ":description": {"S": payload["description"]},
                    ":index": {"S": payload["index"]},
                    ":query": {"S": payload["query"]},
                    ":updated_at": {"S": updated_at},
                    ":updated_by": {"S": caller},
                    ":expected": {"S": expected},
                },
            }
        }
        audit = {
            "Put": {
                "TableName": self._table,
                "Item": _audit_item(
                    report_id,
                    _row_audit_sk(section_storage_seq, row_storage_seq, updated_at),
                    AUDIT_UPDATE,
                    caller,
                    updated_at,
                    previous=previous,
                ),
            }
        }
        self._transact_write([update, audit], EDIT_CONFLICT)
        return {
            "reportId": report_id,
            "sectionStorageSeq": section_storage_seq,
            "rowStorageSeq": row_storage_seq,
            "updatedAt": updated_at,
            "updatedBy": caller,
        }

    def add_row(
        self,
        report_id: str,
        section_storage_seq: int,
        row: Mapping[str, Any],
        *,
        after_storage_seq: int | None = None,
        updated_by: str,
    ) -> dict[str, Any]:
        """Validate a row and insert it at a gapped ``storage_seq`` without renumbering.

        With no ``after_storage_seq`` the row is appended after the section's last row.
        Otherwise it is placed at the midpoint of the gap following ``after_storage_seq``.
        A full gap raises a sanitized 409 rather than renumbering existing rows.
        """
        _validate_report_id(report_id)
        caller = _validate_caller(updated_by)
        payload = _validate_row(row)
        self._require_section(report_id, section_storage_seq)
        existing = self._row_storage_seqs(report_id, section_storage_seq)
        new_storage = _allocate(existing, after_storage_seq)
        updated_at = _now()
        item = _row_item(report_id, section_storage_seq, new_storage, payload, caller, updated_at)
        mutation = {
            "Put": {
                "TableName": self._table,
                "Item": item,
                "ConditionExpression": "attribute_not_exists(PK)",
            }
        }
        audit = {
            "Put": {
                "TableName": self._table,
                "Item": _audit_item(
                    report_id,
                    _row_audit_sk(section_storage_seq, new_storage, updated_at),
                    AUDIT_CREATE,
                    caller,
                    updated_at,
                ),
            }
        }
        self._transact_write([mutation, audit], EDIT_CONFLICT)
        return {
            "reportId": report_id,
            "sectionStorageSeq": section_storage_seq,
            "rowStorageSeq": new_storage,
            "updatedAt": updated_at,
            "updatedBy": caller,
        }

    def delete_row(
        self,
        report_id: str,
        section_storage_seq: int,
        row_storage_seq: int,
        *,
        expected_updated_at: str,
        updated_by: str = DEFAULT_ACTOR,
    ) -> None:
        """Conditionally delete one row, guarding on its ``updated_at`` lock.

        The delete and a ``Put`` of one ``action=delete`` audit item (carrying the full prior
        row under ``previous``) travel in one ``TransactWriteItems`` guarded by the same
        ``expected_updated_at`` condition, so a losing race aborts both writes.
        """
        _validate_report_id(report_id)
        caller = _validate_caller(updated_by)
        expected = _validate_precondition(expected_updated_at)
        sk = _row_sk(section_storage_seq, row_storage_seq)
        previous = self._prior_snapshot(report_id, sk)
        updated_at = _now()
        mutation = {
            "Delete": {
                "TableName": self._table,
                "Key": {"PK": {"S": _pk(report_id)}, "SK": {"S": sk}},
                "ConditionExpression": "attribute_exists(PK) AND #updated_at = :expected",
                "ExpressionAttributeNames": {"#updated_at": "updated_at"},
                "ExpressionAttributeValues": {":expected": {"S": expected}},
            }
        }
        audit = {
            "Put": {
                "TableName": self._table,
                "Item": _audit_item(
                    report_id,
                    _row_audit_sk(section_storage_seq, row_storage_seq, updated_at),
                    AUDIT_DELETE,
                    caller,
                    updated_at,
                    previous=previous,
                ),
            }
        }
        self._transact_write([mutation, audit], EDIT_CONFLICT)

    def add_section(
        self,
        report_id: str,
        section: Mapping[str, Any],
        *,
        after_storage_seq: int | None = None,
        updated_by: str = DEFAULT_ACTOR,
    ) -> dict[str, Any]:
        """Insert a new section header at a gapped ``storage_seq`` without renumbering."""
        _validate_report_id(report_id)
        caller = _validate_caller(updated_by)
        seq, name = _validate_section(section)
        existing = self._section_storage_seqs(report_id)
        new_storage = _allocate(existing, after_storage_seq)
        updated_at = _now()
        item = _section_item(report_id, new_storage, seq, name)
        mutation = {
            "Put": {
                "TableName": self._table,
                "Item": item,
                "ConditionExpression": "attribute_not_exists(PK)",
            }
        }
        audit = {
            "Put": {
                "TableName": self._table,
                "Item": _audit_item(
                    report_id,
                    _section_audit_sk(new_storage, updated_at),
                    AUDIT_CREATE,
                    caller,
                    updated_at,
                ),
            }
        }
        self._transact_write([mutation, audit], EDIT_CONFLICT)
        return {"reportId": report_id, "sectionStorageSeq": new_storage}

    def _transact_write(self, transact_items: list[dict[str, Any]], conflict: str) -> None:
        """Run one ``TransactWriteItems`` mapping cancellations to a sanitized 409."""
        try:
            self._dynamo.transact_write_items(TransactItems=transact_items)
        except Exception as error:
            if _client_error_code(error) in _TRANSACTION_CONFLICT_CODES:
                raise CatalogRequestError(409, conflict) from None
            raise CatalogError(CATALOG_SAVE_FAILED) from None

    def _prior_snapshot(self, report_id: str, sk: str) -> str | None:
        """Consistently read the item at ``sk`` and return its full plain form as compact JSON.

        Returns ``None`` when the item is absent; the guarded mutation then aborts the whole
        transaction, so a missing prior is never persisted as an audit ``previous``.
        """
        try:
            response = self._dynamo.get_item(
                TableName=self._table,
                Key={"PK": {"S": _pk(report_id)}, "SK": {"S": sk}},
                ConsistentRead=True,
            )
        except Exception:
            raise CatalogError(CATALOG_READ_FAILED) from None
        raw = response.get("Item")
        if not isinstance(raw, dict):
            return None
        return _compact(_plain_item(raw))

    # -- internal query helpers ---------------------------------------------------------

    def _assemble(self, report_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        return self._assemble_items(self._query_report(report_id))

    def _assemble_items(
        self, items: list[dict[str, Any]]
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Parse a report partition's items into ``(meta, sections)``, raising 404 if absent.

        The caller supplies items from a single ``Query`` on the report partition so callers
        that already hold the partition (for example ``delete_report``) reuse them without a
        second read.
        """
        meta: dict[str, Any] | None = None
        sections: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None
        for raw in items:
            sk = _string(raw, "SK")
            if sk.startswith(AUDIT_PREFIX):
                # Audit items share the report partition but are not part of the definition.
                continue
            if sk == SK_META:
                meta = _parse_meta(raw)
            elif "#ROW#" in sk:
                if current is None:
                    raise CatalogError(STORED_DEFINITION_INVALID)
                current["rows"].append(_parse_row(raw))
            elif sk.startswith("SECTION#"):
                current = _parse_section(raw)
                sections.append(current)
        if meta is None:
            raise CatalogRequestError(404, DEFINITION_NOT_FOUND)
        return meta, sections

    def _query_report(self, report_id: str) -> list[dict[str, Any]]:
        _validate_report_id(report_id)
        items: list[dict[str, Any]] = []
        request: dict[str, Any] = {
            "TableName": self._table,
            "KeyConditionExpression": "PK = :pk",
            "ExpressionAttributeValues": {":pk": {"S": _pk(report_id)}},
            "ScanIndexForward": True,
        }
        try:
            while True:
                response = self._dynamo.query(**request)
                items.extend(_items(response))
                start_key = response.get("LastEvaluatedKey")
                if not start_key:
                    break
                request["ExclusiveStartKey"] = start_key
        except CatalogError:
            raise
        except Exception:
            raise CatalogError(CATALOG_READ_FAILED) from None
        return items

    def _require_section(self, report_id: str, section_storage_seq: int) -> None:
        sk = _section_sk(section_storage_seq)
        try:
            response = self._dynamo.query(
                TableName=self._table,
                KeyConditionExpression="PK = :pk AND SK = :sk",
                ExpressionAttributeValues={
                    ":pk": {"S": _pk(report_id)},
                    ":sk": {"S": sk},
                },
            )
        except Exception:
            raise CatalogError(CATALOG_READ_FAILED) from None
        if not _items(response):
            raise CatalogRequestError(404, SECTION_NOT_FOUND)

    def _row_storage_seqs(self, report_id: str, section_storage_seq: int) -> list[int]:
        prefix = f"{_section_sk(section_storage_seq)}#ROW#"
        rows = self._query_prefix(report_id, prefix)
        return [_number(raw, "storage_seq") for raw in rows]

    def _section_storage_seqs(self, report_id: str) -> list[int]:
        rows = self._query_prefix(report_id, "SECTION#")
        return [_number(raw, "storage_seq") for raw in rows if "#ROW#" not in _string(raw, "SK")]

    def _query_prefix(self, report_id: str, prefix: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        request: dict[str, Any] = {
            "TableName": self._table,
            "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": {"S": _pk(report_id)},
                ":prefix": {"S": prefix},
            },
            "ScanIndexForward": True,
        }
        try:
            while True:
                response = self._dynamo.query(**request)
                items.extend(_items(response))
                start_key = response.get("LastEvaluatedKey")
                if not start_key:
                    break
                request["ExclusiveStartKey"] = start_key
        except Exception:
            raise CatalogError(CATALOG_READ_FAILED) from None
        return items


# -- item builders ----------------------------------------------------------------------


def _section_item(report_id: str, storage_seq: int, seq: int, name: str) -> dict[str, Any]:
    return {
        "PK": {"S": _pk(report_id)},
        "SK": {"S": _section_sk(storage_seq)},
        "seq": {"N": str(seq)},
        "name": {"S": name},
        "storage_seq": {"N": str(storage_seq)},
    }


def _row_item(
    report_id: str,
    section_storage_seq: int,
    row_storage_seq: int,
    row: Mapping[str, Any],
    updated_by: str,
    updated_at: str,
) -> dict[str, Any]:
    return {
        "PK": {"S": _pk(report_id)},
        "SK": {"S": _row_sk(section_storage_seq, row_storage_seq)},
        "seq": {"N": str(row["seq"])},
        "label": {"S": row["label"]},
        "description": {"S": row["description"]},
        "index": {"S": row["index"]},
        "query": {"S": row["query"] if isinstance(row["query"], str) else _compact(row["query"])},
        "storage_seq": {"N": str(row_storage_seq)},
        "updated_at": {"S": updated_at},
        "updated_by": {"S": updated_by},
    }


def _audit_item(
    report_id: str,
    sk: str,
    action: str,
    updated_by: str,
    updated_at: str,
    *,
    previous: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """Build one immutable ``AUDIT#`` item.

    ``previous`` (the compact JSON of the full prior item) is omitted on creates and imports
    and present on updates and deletes. ``source`` is present only on import audits.
    """
    item: dict[str, Any] = {
        "PK": {"S": _pk(report_id)},
        "SK": {"S": sk},
        "action": {"S": action},
        "updated_by": {"S": updated_by},
        "updated_at": {"S": updated_at},
    }
    if previous is not None:
        item["previous"] = {"S": previous}
    if source is not None:
        item["source"] = {"S": source}
    return item


def _deletion_audit_item(
    report_id: str, updated_by: str, updated_at: str, previous_text: str
) -> dict[str, Any]:
    """Build one durable deletion audit under the ``AUDITLOG#deleted-reports`` partition.

    The audit lives outside the report partition so it survives the report's cleanup. It
    carries the full prior clean definition as canonical JSON under ``previous`` alongside a
    denormalized ``report_id`` for querying every deletion without parsing the sort key.
    """
    return {
        "PK": {"S": DELETED_REPORTS_PK},
        "SK": {"S": _deleted_report_sk(report_id, updated_at)},
        "action": {"S": AUDIT_DELETE},
        "report_id": {"S": report_id},
        "updated_by": {"S": updated_by},
        "updated_at": {"S": updated_at},
        "previous": {"S": previous_text},
    }


def _plain_item(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Unwrap a DynamoDB-typed item into plain scalar values for a compact audit snapshot."""
    plain: dict[str, Any] = {}
    for key, attribute in raw.items():
        if not isinstance(attribute, dict):
            continue
        if "S" in attribute:
            plain[key] = attribute["S"]
        elif "N" in attribute:
            plain[key] = int(attribute["N"])
        elif "BOOL" in attribute:
            plain[key] = bool(attribute["BOOL"])
    return plain


# -- assembly parsing -------------------------------------------------------------------


def _parse_meta(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": _string(raw, "name"),
        "description": _string(raw, "description"),
        "partition_field": _string(raw, "partition_field"),
        "time_field": _string(raw, "time_field"),
        "draft": _boolean(raw, "draft"),
        "updated_at": _optional_string(raw, "updated_at"),
        "updated_by": _optional_string(raw, "updated_by"),
    }


def _parse_section(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "storage_seq": _number(raw, "storage_seq"),
        "seq": _number(raw, "seq"),
        "name": _string(raw, "name"),
        "rows": [],
    }


def _parse_row(raw: dict[str, Any]) -> dict[str, Any]:
    query_text = _string(raw, "query")
    try:
        query = json.loads(query_text)
    except json.JSONDecodeError:
        raise CatalogError(STORED_DEFINITION_INVALID) from None
    # A placeholder row stores the compact JSON null literal, which decodes to None; any
    # other non-object stored query is a stored-shape violation.
    if query is not None and not isinstance(query, dict):
        raise CatalogError(STORED_DEFINITION_INVALID)
    return {
        "storage_seq": _number(raw, "storage_seq"),
        "seq": _number(raw, "seq"),
        "label": _string(raw, "label"),
        "description": _string(raw, "description"),
        "index": _string(raw, "index"),
        "query": query,
        "updated_at": _optional_string(raw, "updated_at"),
        "updated_by": _optional_string(raw, "updated_by"),
    }


def _parse_audit(raw: dict[str, Any]) -> dict[str, Any]:
    """Parse one ``AUDIT#`` item into plain data, decoding ``previous`` back into an object."""
    entry: dict[str, Any] = {
        "reportId": _report_id_from_pk(_string(raw, "PK")),
        "sk": _string(raw, "SK"),
        "action": _string(raw, "action"),
        "updatedAt": _optional_string(raw, "updated_at"),
        "updatedBy": _optional_string(raw, "updated_by"),
    }
    previous_text = _optional_string(raw, "previous")
    if previous_text is not None:
        try:
            entry["previous"] = json.loads(previous_text)
        except json.JSONDecodeError:
            raise CatalogError(STORED_DEFINITION_INVALID) from None
    source = _optional_string(raw, "source")
    if source is not None:
        entry["source"] = source
    return entry


def _clean_definition(
    report_id: str,
    meta: dict[str, Any],
    sections: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "name": meta["name"],
        "description": meta["description"],
        "partition_field": meta["partition_field"],
        "time_field": meta["time_field"],
        "sections": [
            {
                "seq": section["seq"],
                "name": section["name"],
                "rows": [
                    {
                        "seq": row["seq"],
                        "label": row["label"],
                        "description": row["description"],
                        "index": row["index"],
                        "query": row["query"],
                    }
                    for row in section["rows"]
                ],
            }
            for section in sections
        ],
    }


def _editor_metadata(
    report_id: str,
    meta: dict[str, Any],
    sections: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "reportId": report_id,
        "draft": meta["draft"],
        "updatedAt": meta["updated_at"],
        "updatedBy": meta["updated_by"],
        "sections": [
            {
                "storageSeq": section["storage_seq"],
                "seq": section["seq"],
                "name": section["name"],
                "rows": [
                    {
                        "storageSeq": row["storage_seq"],
                        "seq": row["seq"],
                        "label": row["label"],
                        "updatedAt": row["updated_at"],
                        "updatedBy": row["updated_by"],
                    }
                    for row in section["rows"]
                ],
            }
            for section in sections
        ],
    }


def _listing_entry(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise CatalogError(CATALOG_READ_FAILED)
    return {
        "reportId": _report_id_from_pk(_string(raw, "PK")),
        "name": _string(raw, "name"),
        "description": _string(raw, "description"),
        "updatedAt": _optional_string(raw, "updated_at"),
        "updatedBy": _optional_string(raw, "updated_by"),
    }


# -- validation helpers -----------------------------------------------------------------


def _validate_report_id(report_id: str) -> None:
    if not isinstance(report_id, str) or not REPORT_ID_PATTERN.fullmatch(report_id):
        raise CatalogRequestError(400, INVALID_REPORT_ID)


def _validate_caller(caller_sub: str) -> str:
    if not isinstance(caller_sub, str) or not caller_sub or len(caller_sub) > 128:
        raise CatalogRequestError(400, INVALID_CALLER)
    return caller_sub


def _validate_precondition(expected_updated_at: str) -> str:
    if not isinstance(expected_updated_at, str) or not expected_updated_at:
        raise CatalogRequestError(400, INVALID_PRECONDITION)
    return expected_updated_at


def _validate_source(source: str) -> str:
    if source not in _ALLOWED_IMPORT_SOURCES:
        raise CatalogRequestError(400, INVALID_SOURCE)
    return source


def _validate_history_limit(limit: int) -> int:
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or limit < HISTORY_MIN_LIMIT
        or limit > HISTORY_MAX_LIMIT
    ):
        raise CatalogRequestError(400, INVALID_LIMIT)
    return limit


def _validate_row(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        validated = load_report_row(row)
    except ReportDefinitionError:
        raise CatalogRequestError(400, DEFINITION_INVALID) from None
    return {
        "seq": validated.seq,
        "label": validated.label,
        "description": validated.description,
        "index": validated.index,
        "query": _compact(validated.query),
    }


def _validate_section(section: Mapping[str, Any]) -> tuple[int, str]:
    if not isinstance(section, Mapping):
        raise CatalogRequestError(400, DEFINITION_INVALID)
    seq = section.get("seq")
    name = section.get("name")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
        raise CatalogRequestError(400, DEFINITION_INVALID)
    if not isinstance(name, str) or not name.strip():
        raise CatalogRequestError(400, DEFINITION_INVALID)
    return seq, name


# -- allocation -------------------------------------------------------------------------


def _allocate(existing: Sequence[int], after: int | None) -> int:
    """Return a gapped storage_seq: append after the last, or split the gap after ``after``.

    Raises a sanitized 409 when no gap remains rather than renumbering existing items.
    """
    ordered = sorted(existing)
    if after is None:
        if not ordered:
            return STORAGE_STEP
        candidate = ordered[-1] + STORAGE_STEP
        if candidate > MAX_STORAGE_SEQ:
            raise CatalogRequestError(409, SEQUENCE_SPACE_EXHAUSTED)
        return candidate
    higher = [value for value in ordered if value > after]
    if not higher:
        candidate = after + STORAGE_STEP
        if candidate > MAX_STORAGE_SEQ:
            raise CatalogRequestError(409, SEQUENCE_SPACE_EXHAUSTED)
        return candidate
    midpoint = (after + higher[0]) // 2
    if midpoint <= after:
        raise CatalogRequestError(409, SEQUENCE_SPACE_EXHAUSTED)
    return midpoint


# -- key and attribute helpers ----------------------------------------------------------


def _pk(report_id: str) -> str:
    return f"REPORT#{report_id}"


def _report_id_from_pk(pk: str) -> str:
    if not pk.startswith("REPORT#"):
        raise CatalogError(CATALOG_READ_FAILED)
    return pk[len("REPORT#") :]


def _section_sk(storage_seq: int) -> str:
    return f"SECTION#{storage_seq:03d}"


def _row_sk(section_storage_seq: int, row_storage_seq: int) -> str:
    return f"SECTION#{section_storage_seq:03d}#ROW#{row_storage_seq:03d}"


def _row_audit_sk(section_storage_seq: int, row_storage_seq: int, timestamp: str) -> str:
    return f"AUDIT#SECTION#{section_storage_seq:03d}#ROW#{row_storage_seq:03d}#{timestamp}"


def _section_audit_sk(section_storage_seq: int, timestamp: str) -> str:
    return f"AUDIT#SECTION#{section_storage_seq:03d}#{timestamp}"


def _import_audit_sk(timestamp: str) -> str:
    return f"AUDIT#IMPORT#{timestamp}"


def _replace_audit_sk(timestamp: str) -> str:
    return f"AUDIT#REPLACE#{timestamp}"


def _deleted_report_sk(report_id: str, timestamp: str) -> str:
    return f"REPORT#{report_id}#{timestamp}"


def _import_storage_seq(position: int) -> int:
    return (position + 1) * STORAGE_STEP


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _canonical_text(definition: Mapping[str, Any]) -> str:
    return json.dumps(definition, separators=(",", ":"), sort_keys=True)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _chunks(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _items(response: dict[str, Any]) -> list[dict[str, Any]]:
    raw_items = response.get("Items", [])
    if not isinstance(raw_items, list):
        raise CatalogError(CATALOG_READ_FAILED)
    return raw_items


def _string(item: dict[str, Any], key: str) -> str:
    value = _optional_string(item, key)
    if value is None:
        raise CatalogError(CATALOG_READ_FAILED)
    return value


def _optional_string(item: dict[str, Any], key: str) -> str | None:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        return None
    value = attribute.get("S")
    return value if isinstance(value, str) else None


def _number(item: dict[str, Any], key: str) -> int:
    attribute = item.get(key)
    if not isinstance(attribute, dict):
        raise CatalogError(CATALOG_READ_FAILED)
    value = attribute.get("N")
    if not isinstance(value, str):
        raise CatalogError(CATALOG_READ_FAILED)
    try:
        return int(value)
    except ValueError:
        raise CatalogError(CATALOG_READ_FAILED) from None


def _boolean(item: dict[str, Any], key: str) -> bool:
    attribute = item.get(key)
    if not isinstance(attribute, dict) or "BOOL" not in attribute:
        raise CatalogError(CATALOG_READ_FAILED)
    return bool(attribute["BOOL"])


def _client_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if not isinstance(response, dict):
        return None
    error_info = response.get("Error")
    if not isinstance(error_info, dict):
        return None
    code = error_info.get("Code")
    return code if isinstance(code, str) else None
