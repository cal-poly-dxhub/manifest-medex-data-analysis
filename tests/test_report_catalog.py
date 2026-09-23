"""Tests for the row-granular DynamoDB report catalog.

The tests drive a small in-memory DynamoDB fake that understands exactly the low-level
operations, key conditions, filters, and conditional writes the catalog issues. This lets
the behavioural tests (canonical round trip, single-query assembly, optimistic row edits,
gapped inserts, and atomic imports) exercise real item layout rather than canned rows.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from src.report_catalog import (
    AUDIT_CREATE,
    AUDIT_DELETE,
    AUDIT_IMPORT,
    AUDIT_UPDATE,
    BATCH_WRITE_MAX,
    DEFINITION_INVALID,
    DEFINITION_NOT_FOUND,
    DELETED_REPORTS_PK,
    EDIT_CONFLICT,
    INVALID_CALLER,
    INVALID_LIMIT,
    INVALID_PRECONDITION,
    INVALID_REPORT_ID,
    INVALID_SOURCE,
    REPORT_ALREADY_EXISTS,
    SECTION_NOT_FOUND,
    SEQUENCE_SPACE_EXHAUSTED,
    SOURCE_MIGRATION,
    SOURCE_SEED,
    CatalogError,
    CatalogRequestError,
    ReportCatalog,
)
from src.report_definition import load_report_definition_json

CALLER = "auth0|catalog-editor"


class FakeClientError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


def _key(item: dict[str, Any]) -> tuple[str, str]:
    return item["PK"]["S"], item["SK"]["S"]


def _txn_key(op: str, spec: dict[str, Any]) -> tuple[str, str]:
    if op == "Put":
        return _key(spec["Item"])
    return spec["Key"]["PK"]["S"], spec["Key"]["SK"]["S"]


def _resolve(token: str, names: dict[str, str]) -> str:
    return names.get(token, token) if token.startswith("#") else token


class FakeDynamo:
    """An in-memory single-table DynamoDB double keyed on (PK, SK)."""

    def __init__(
        self,
        *,
        batch_error: Exception | None = None,
        batch_unprocessed_once: bool = False,
    ) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.put_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []
        self.scan_calls: list[dict[str, Any]] = []
        self.batch_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []
        self.transact_calls: list[dict[str, Any]] = []
        self._batch_error = batch_error
        self._batch_unprocessed_once = batch_unprocessed_once

    # -- conditional-write evaluation ---------------------------------------------------

    def _condition_ok(
        self,
        existing: dict[str, Any] | None,
        condition: str | None,
        names: dict[str, str],
        values: dict[str, Any],
    ) -> bool:
        if not condition:
            return True
        for raw_term in condition.split(" AND "):
            term = raw_term.strip()
            if term.startswith("attribute_not_exists"):
                if existing is not None:
                    return False
            elif term.startswith("attribute_exists"):
                if existing is None:
                    return False
            else:
                lhs, rhs = (part.strip() for part in term.split("="))
                attribute = _resolve(lhs, names)
                if existing is None or existing.get(attribute) != values[rhs]:
                    return False
        return True

    # -- operations ---------------------------------------------------------------------

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self.put_calls.append(kwargs)
        item = kwargs["Item"]
        key = _key(item)
        existing = self.items.get(key)
        if not self._condition_ok(
            existing,
            kwargs.get("ConditionExpression"),
            kwargs.get("ExpressionAttributeNames", {}),
            kwargs.get("ExpressionAttributeValues", {}),
        ):
            raise FakeClientError("ConditionalCheckFailedException")
        self.items[key] = dict(item)
        return {}

    def update_item(self, **kwargs: Any) -> dict[str, Any]:
        self.update_calls.append(kwargs)
        key = (kwargs["Key"]["PK"]["S"], kwargs["Key"]["SK"]["S"])
        existing = self.items.get(key)
        names = kwargs.get("ExpressionAttributeNames", {})
        values = kwargs.get("ExpressionAttributeValues", {})
        if not self._condition_ok(existing, kwargs.get("ConditionExpression"), names, values):
            raise FakeClientError("ConditionalCheckFailedException")
        item = (
            dict(existing)
            if existing is not None
            else {
                "PK": kwargs["Key"]["PK"],
                "SK": kwargs["Key"]["SK"],
            }
        )
        body = kwargs["UpdateExpression"][len("SET ") :]
        for assignment in body.split(","):
            lhs, rhs = (part.strip() for part in assignment.split("="))
            item[_resolve(lhs, names)] = values[rhs]
        self.items[key] = item
        return {}

    def delete_item(self, **kwargs: Any) -> dict[str, Any]:
        self.delete_calls.append(kwargs)
        key = (kwargs["Key"]["PK"]["S"], kwargs["Key"]["SK"]["S"])
        existing = self.items.get(key)
        if not self._condition_ok(
            existing,
            kwargs.get("ConditionExpression"),
            kwargs.get("ExpressionAttributeNames", {}),
            kwargs.get("ExpressionAttributeValues", {}),
        ):
            raise FakeClientError("ConditionalCheckFailedException")
        self.items.pop(key, None)
        return {}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls.append(kwargs)
        condition = kwargs["KeyConditionExpression"]
        values = kwargs["ExpressionAttributeValues"]
        partition = values[":pk"]["S"]
        matches = [item for (pk, _), item in self.items.items() if pk == partition]
        if "begins_with" in condition:
            prefix = values[":prefix"]["S"]
            matches = [item for item in matches if item["SK"]["S"].startswith(prefix)]
        elif "SK = :sk" in condition:
            exact = values[":sk"]["S"]
            matches = [item for item in matches if item["SK"]["S"] == exact]
        matches.sort(key=lambda item: item["SK"]["S"])
        if kwargs.get("ScanIndexForward", True) is False:
            matches.reverse()
        limit = kwargs.get("Limit")
        if isinstance(limit, int):
            matches = matches[:limit]
        return {"Items": matches}

    def scan(self, **kwargs: Any) -> dict[str, Any]:
        self.scan_calls.append(kwargs)
        names = kwargs.get("ExpressionAttributeNames", {})
        values = kwargs.get("ExpressionAttributeValues", {})
        expression = kwargs.get("FilterExpression")
        matches: list[dict[str, Any]] = []
        for item in self.items.values():
            if expression is None or self._filter_ok(item, expression, names, values):
                matches.append(item)
        return {"Items": matches}

    def _filter_ok(
        self,
        item: dict[str, Any],
        expression: str,
        names: dict[str, str],
        values: dict[str, Any],
    ) -> bool:
        for raw_term in expression.split(" AND "):
            lhs, rhs = (part.strip() for part in raw_term.split("="))
            if item.get(_resolve(lhs, names)) != values[rhs]:
                return False
        return True

    def batch_write_item(self, **kwargs: Any) -> dict[str, Any]:
        self.batch_calls.append(kwargs)
        request = kwargs["RequestItems"]
        if self._batch_error is not None:
            raise self._batch_error
        if self._batch_unprocessed_once and len(self.batch_calls) == 1:
            return {"UnprocessedItems": request}
        for requests in request.values():
            for entry in requests:
                if "PutRequest" in entry:
                    item = entry["PutRequest"]["Item"]
                    self.items[_key(item)] = dict(item)
                elif "DeleteRequest" in entry:
                    key = entry["DeleteRequest"]["Key"]
                    self.items.pop((key["PK"]["S"], key["SK"]["S"]), None)
        return {"UnprocessedItems": {}}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        key = (kwargs["Key"]["PK"]["S"], kwargs["Key"]["SK"]["S"])
        existing = self.items.get(key)
        if existing is None:
            return {}
        return {"Item": dict(existing)}

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.transact_calls.append(kwargs)
        entries = kwargs["TransactItems"]
        # Phase 1: evaluate every condition against the pre-transaction state so the whole
        # transaction commits or aborts atomically, mirroring TransactWriteItems.
        for entry in entries:
            op, spec = next(iter(entry.items()))
            key = _txn_key(op, spec)
            if not self._condition_ok(
                self.items.get(key),
                spec.get("ConditionExpression"),
                spec.get("ExpressionAttributeNames", {}),
                spec.get("ExpressionAttributeValues", {}),
            ):
                raise FakeClientError("TransactionCanceledException")
        # Phase 2: apply mutations.
        for entry in entries:
            op, spec = next(iter(entry.items()))
            if op == "Put":
                self.items[_key(spec["Item"])] = dict(spec["Item"])
            elif op == "Update":
                key = _txn_key(op, spec)
                names = spec.get("ExpressionAttributeNames", {})
                values = spec.get("ExpressionAttributeValues", {})
                existing = self.items.get(key)
                item = (
                    dict(existing)
                    if existing is not None
                    else {"PK": spec["Key"]["PK"], "SK": spec["Key"]["SK"]}
                )
                body = spec["UpdateExpression"][len("SET ") :]
                for assignment in body.split(","):
                    lhs, rhs = (part.strip() for part in assignment.split("="))
                    item[_resolve(lhs, names)] = values[rhs]
                self.items[key] = item
            elif op == "Delete":
                self.items.pop(_txn_key(op, spec), None)
        return {}


def _catalog(dynamo: FakeDynamo, **kwargs: Any) -> ReportCatalog:
    return ReportCatalog(dynamo, table_name="report_catalog", **kwargs)


def _item(dynamo: FakeDynamo, report_id: str, sk: str) -> dict[str, Any]:
    return dynamo.items[(f"REPORT#{report_id}", sk)]


def _row(seq: int, label: str, field: str = "ROOT.PID") -> dict[str, Any]:
    return {
        "seq": seq,
        "label": label,
        "description": f"desc {label}",
        "index": "hl7-messages-v1",
        "query": {"bool": {"filter": [{"exists": {"field": field}}]}},
    }


def _definition(report_id: str = "p4p-prototype") -> dict[str, Any]:
    return {
        "report_id": report_id,
        "name": "P4P Prototype",
        "description": "Prototype report",
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": [
            {"seq": 1, "name": "ADT A08", "rows": [_row(1, "PID-3.1"), _row(2, "PID-5.1")]},
            {"seq": 2, "name": "ORU", "rows": [_row(1, "OBRSegment", "ROOT.OBR")]},
        ],
    }


def _big_definition(
    report_id: str = "big-report", sections: int = 2, rows: int = 15
) -> dict[str, Any]:
    return {
        "report_id": report_id,
        "name": "Big",
        "description": "many rows",
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": [
            {
                "seq": section_index + 1,
                "name": f"Section {section_index + 1}",
                "rows": [
                    _row(row_index + 1, f"S{section_index + 1}-R{row_index + 1}")
                    for row_index in range(rows)
                ],
            }
            for section_index in range(sections)
        ],
    }


def _canonical(definition: dict[str, Any]) -> str:
    return json.dumps(definition, separators=(",", ":"), sort_keys=True)


# -- construction -----------------------------------------------------------------------


@pytest.mark.parametrize("kwargs", [{"table_name": ""}, {"max_catalog_items": 0}])
def test_constructor_rejects_invalid_configuration(kwargs: dict[str, Any]) -> None:
    base: dict[str, Any] = {"table_name": "t"}
    base.update(kwargs)
    with pytest.raises(ValueError, match="configuration is invalid"):
        ReportCatalog(FakeDynamo(), **base)


# -- import + exact item layout ---------------------------------------------------------


def test_import_writes_exact_keys_and_attributes() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    meta = _item(dynamo, "p4p-prototype", "META")
    assert meta["PK"] == {"S": "REPORT#p4p-prototype"}
    assert meta["SK"] == {"S": "META"}
    assert meta["draft"] == {"BOOL": False}
    assert meta["name"] == {"S": "P4P Prototype"}
    assert meta["partition_field"] == {"S": "sourceFacilityId"}
    assert meta["time_field"] == {"S": "messageTime"}
    assert meta["updated_by"] == {"S": CALLER}

    section = _item(dynamo, "p4p-prototype", "SECTION#010")
    assert section["seq"] == {"N": "1"}
    assert section["name"] == {"S": "ADT A08"}
    assert section["storage_seq"] == {"N": "10"}

    first_row = _item(dynamo, "p4p-prototype", "SECTION#010#ROW#010")
    assert first_row["seq"] == {"N": "1"}
    assert first_row["label"] == {"S": "PID-3.1"}
    assert first_row["storage_seq"] == {"N": "10"}
    assert first_row["updated_by"] == {"S": CALLER}
    # The row query is stored as a compact JSON string, not a nested map.
    assert first_row["query"] == {
        "S": json.dumps(
            {"bool": {"filter": [{"exists": {"field": "ROOT.PID"}}]}},
            separators=(",", ":"),
            sort_keys=True,
        )
    }

    # Second row of the first section and the second section use gapped storage seqs.
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#020")["label"] == {"S": "PID-5.1"}
    assert _item(dynamo, "p4p-prototype", "SECTION#020")["name"] == {"S": "ORU"}
    assert _item(dynamo, "p4p-prototype", "SECTION#020#ROW#010")["label"] == {"S": "OBRSegment"}


def test_import_conflicts_when_report_already_exists() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    with pytest.raises(CatalogRequestError) as captured:
        catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    assert captured.value.status_code == 409
    assert captured.value.code == REPORT_ALREADY_EXISTS


def test_import_rejects_invalid_definition() -> None:
    dynamo = FakeDynamo()
    invalid = _definition()
    invalid["partition_field"] = "notAllowed"

    with pytest.raises(CatalogRequestError) as captured:
        _catalog(dynamo).import_report(json.dumps(invalid), updated_by=CALLER)

    assert captured.value.code == DEFINITION_INVALID
    assert dynamo.items == {}


@pytest.mark.parametrize("caller", ["", "x" * 129])
def test_import_rejects_invalid_caller(caller: str) -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).import_report(json.dumps(_definition()), updated_by=caller)

    assert captured.value.code == INVALID_CALLER


# -- canonical round trip ---------------------------------------------------------------


def test_canonical_round_trip_is_byte_identical() -> None:
    text = json.dumps(_definition())
    definition = load_report_definition_json(text)
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    catalog.import_report(text, updated_by=CALLER)
    exported = catalog.export_report("p4p-prototype")

    expected = _canonical(definition.model_dump(mode="json"))
    assert _canonical(exported["definition"]) == expected
    assert exported["text"] == expected


def test_round_trip_preserves_section_and_row_order_regardless_of_seq() -> None:
    # Sections and rows are listed out of seq order; import must preserve list order and
    # the original seq values so the export is byte identical.
    definition = {
        "report_id": "ordered",
        "name": "Ordered",
        "description": "",
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": [
            {"seq": 5, "name": "Beta", "rows": [_row(9, "b-first"), _row(2, "b-second")]},
            {"seq": 3, "name": "Alpha", "rows": [_row(7, "a-first")]},
        ],
    }
    text = json.dumps(definition)
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    catalog.import_report(text, updated_by=CALLER)
    exported = catalog.export_report("ordered")

    assert exported["definition"] == definition
    assert _canonical(exported["definition"]) == _canonical(
        load_report_definition_json(text).model_dump(mode="json")
    )


# -- assembly and editor metadata -------------------------------------------------------


def test_get_report_assembles_with_a_single_query_and_returns_locks() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)
    dynamo.query_calls.clear()

    result = catalog.get_report("p4p-prototype")

    # One Query with ascending sort assembles the whole report.
    assert len(dynamo.query_calls) == 1
    assert dynamo.query_calls[0]["ScanIndexForward"] is True

    # The clean definition never leaks storage seqs or locks.
    definition = result["definition"]
    assert set(definition["sections"][0]["rows"][0]) == {
        "seq",
        "label",
        "description",
        "index",
        "query",
    }

    editor = result["editor"]
    assert editor["draft"] is False
    first_section = editor["sections"][0]
    assert first_section["storageSeq"] == 10
    first_row = first_section["rows"][0]
    assert first_row["storageSeq"] == 10
    assert first_row["label"] == "PID-3.1"
    assert isinstance(first_row["updatedAt"], str)


def test_get_report_missing_is_sanitized_not_found() -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).get_report("missing-report")

    assert captured.value.status_code == 404
    assert captured.value.code == DEFINITION_NOT_FOUND


@pytest.mark.parametrize("report_id", ["Bad_Id", "with space", "", "UPPER"])
def test_get_report_rejects_unsafe_report_id(report_id: str) -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).get_report(report_id)

    assert captured.value.code == INVALID_REPORT_ID


# -- listing hides drafts ---------------------------------------------------------------


def test_list_reports_returns_only_published_reports_sorted() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition("zeta")), updated_by=CALLER)
    catalog.import_report(json.dumps(_definition("alpha")), updated_by=CALLER)
    # A draft that never flipped to published must stay invisible.
    catalog._create_draft_meta(
        load_report_definition_json(json.dumps(_definition("hidden-draft"))),
        CALLER,
        "2026-01-01T00:00:00+00:00",
    )

    reports = catalog.list_reports()

    assert [report["reportId"] for report in reports] == ["alpha", "zeta"]
    assert reports[0]["name"] == "P4P Prototype"
    assert reports[0]["updatedBy"] == CALLER
    assert dynamo.scan_calls[0]["FilterExpression"] == "SK = :meta AND #draft = :false"


def test_list_reports_is_bounded_by_max_catalog_items() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo, max_catalog_items=1)
    catalog.import_report(json.dumps(_definition("a")), updated_by=CALLER)
    catalog.import_report(json.dumps(_definition("b")), updated_by=CALLER)

    assert len(catalog.list_reports()) == 1


def test_list_reports_sanitizes_backend_failure() -> None:
    class ExplodingDynamo(FakeDynamo):
        def scan(self, **_kwargs: Any) -> dict[str, Any]:
            raise FakeClientError("InternalServerError")

    with pytest.raises(CatalogError, match="Catalog read failed"):
        _catalog(ExplodingDynamo()).list_reports()


# -- import batching and draft atomicity ------------------------------------------------


def test_import_batches_body_writes_in_chunks_of_at_most_25() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    # 2 sections x 15 rows = 32 body items (sections + rows) -> two batch writes.
    catalog.import_report(json.dumps(_big_definition()), updated_by=CALLER)

    assert len(dynamo.batch_calls) == 2
    for call in dynamo.batch_calls:
        written = call["RequestItems"]["report_catalog"]
        assert len(written) <= BATCH_WRITE_MAX
    total = sum(len(call["RequestItems"]["report_catalog"]) for call in dynamo.batch_calls)
    assert total == 32


def test_import_creates_draft_before_body_and_publishes_last() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    # META is created first (as a draft) and the only put before any batch write.
    first_put = dynamo.put_calls[0]["Item"]
    assert first_put["SK"] == {"S": "META"}
    assert first_put["draft"] == {"BOOL": True}
    assert dynamo.put_calls[0]["ConditionExpression"] == "attribute_not_exists(PK)"
    # Publishing is the final write: one transaction that clears the draft flag and writes
    # exactly one import audit item.
    publish = dynamo.transact_calls[-1]["TransactItems"]
    assert len(publish) == 2
    update_op, update_spec = next(iter(publish[0].items()))
    assert update_op == "Update"
    assert update_spec["ExpressionAttributeValues"] == {":false": {"BOOL": False}}
    audit_op, audit_spec = next(iter(publish[1].items()))
    assert audit_op == "Put"
    assert audit_spec["Item"]["action"] == {"S": AUDIT_IMPORT}
    assert _item(dynamo, "p4p-prototype", "META")["draft"] == {"BOOL": False}


def test_import_failure_leaves_an_invisible_draft() -> None:
    dynamo = FakeDynamo(batch_error=FakeClientError("ProvisionedThroughputExceededException"))
    catalog = _catalog(dynamo)

    with pytest.raises(CatalogError, match="Catalog save failed"):
        catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    # The draft META survives but is never published, so listing hides it.
    assert _item(dynamo, "p4p-prototype", "META")["draft"] == {"BOOL": True}
    assert catalog.list_reports() == []


def test_import_retries_unprocessed_batch_items() -> None:
    dynamo = FakeDynamo(batch_unprocessed_once=True)
    catalog = _catalog(dynamo)

    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    # The single body chunk was returned unprocessed once, then retried to completion.
    assert len(dynamo.batch_calls) == 2
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#010")["label"] == {"S": "PID-3.1"}


# -- row update (optimistic lock) -------------------------------------------------------


def _imported(report_id: str = "p4p-prototype") -> tuple[FakeDynamo, ReportCatalog]:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition(report_id)), updated_by=CALLER)
    return dynamo, catalog


def _row_updated_at(dynamo: FakeDynamo, report_id: str, sk: str) -> str:
    return str(_item(dynamo, report_id, sk)["updated_at"]["S"])


def test_update_row_replaces_fields_and_advances_the_lock() -> None:
    dynamo, catalog = _imported()
    sk = "SECTION#010#ROW#010"
    expected = _row_updated_at(dynamo, "p4p-prototype", sk)

    result = catalog.update_row(
        "p4p-prototype",
        10,
        10,
        _row(1, "PID-3.1", field="ROOT.PID.PID_3"),
        expected_updated_at=expected,
        updated_by="auth0|second-editor",
    )

    stored = _item(dynamo, "p4p-prototype", sk)
    assert stored["updated_by"] == {"S": "auth0|second-editor"}
    assert stored["updated_at"]["S"] == result["updatedAt"]
    assert stored["updated_at"]["S"] != expected
    assert json.loads(stored["query"]["S"]) == {
        "bool": {"filter": [{"exists": {"field": "ROOT.PID.PID_3"}}]}
    }
    # The mutation travels as an Update inside the transaction with the optimistic-lock guard.
    _op, update_spec = next(iter(dynamo.transact_calls[-1]["TransactItems"][0].items()))
    assert update_spec["ConditionExpression"] == (
        "attribute_exists(PK) AND #updated_at = :expected"
    )


def test_update_row_stale_lock_is_a_sanitized_conflict() -> None:
    _dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.update_row(
            "p4p-prototype",
            10,
            10,
            _row(1, "PID-3.1"),
            expected_updated_at="1999-01-01T00:00:00+00:00",
            updated_by=CALLER,
        )

    assert captured.value.status_code == 409
    assert captured.value.code == EDIT_CONFLICT


def test_update_row_rejects_reserved_field_guardrail_violation() -> None:
    dynamo, catalog = _imported()
    sk = "SECTION#010#ROW#010"
    expected = _row_updated_at(dynamo, "p4p-prototype", sk)
    reserved = _row(1, "PID-3.1")
    reserved["query"] = {"bool": {"filter": [{"term": {"sourceFacilityId": "FAC1"}}]}}

    with pytest.raises(CatalogRequestError) as captured:
        catalog.update_row(
            "p4p-prototype", 10, 10, reserved, expected_updated_at=expected, updated_by=CALLER
        )

    assert captured.value.code == DEFINITION_INVALID


def test_update_row_requires_a_precondition() -> None:
    _dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.update_row(
            "p4p-prototype", 10, 10, _row(1, "PID-3.1"), expected_updated_at="", updated_by=CALLER
        )

    assert captured.value.code == INVALID_PRECONDITION


# -- add row (gapped allocation) --------------------------------------------------------


def test_add_row_appends_after_the_last_row_by_default() -> None:
    dynamo, catalog = _imported()

    result = catalog.add_row("p4p-prototype", 10, _row(3, "PID-7.1"), updated_by=CALLER)

    # First section already has rows at 10 and 20, so an append lands at 30.
    assert result["rowStorageSeq"] == 30
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#030")["label"] == {"S": "PID-7.1"}


def test_add_row_inserts_at_the_midpoint_of_a_gap() -> None:
    dynamo, catalog = _imported()

    result = catalog.add_row(
        "p4p-prototype", 10, _row(3, "PID-7.1"), after_storage_seq=10, updated_by=CALLER
    )

    # Between storage seqs 10 and 20 the midpoint is 15.
    assert result["rowStorageSeq"] == 15
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#015")["label"] == {"S": "PID-7.1"}


def test_add_row_reports_sequence_space_exhausted_without_renumbering() -> None:
    dynamo, catalog = _imported()
    # Insert repeatedly into the same gap after 10 (15, then 12, then 11) until the space
    # between storage seqs 10 and 11 closes and no midpoint remains.
    for index in range(3):
        catalog.add_row(
            "p4p-prototype", 10, _row(9, f"filler-{index}"), after_storage_seq=10, updated_by=CALLER
        )

    with pytest.raises(CatalogRequestError) as captured:
        catalog.add_row(
            "p4p-prototype", 10, _row(9, "overflow"), after_storage_seq=10, updated_by=CALLER
        )

    assert captured.value.status_code == 409
    assert captured.value.code == SEQUENCE_SPACE_EXHAUSTED
    # No existing row was renumbered: the original first row still sits at storage seq 10.
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#010")["label"] == {"S": "PID-3.1"}


def test_add_row_to_missing_section_is_a_sanitized_not_found() -> None:
    _dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.add_row("p4p-prototype", 990, _row(1, "orphan"), updated_by=CALLER)

    assert captured.value.status_code == 404
    assert captured.value.code == SECTION_NOT_FOUND


def test_add_row_rejects_reserved_field_guardrail_violation() -> None:
    _dynamo, catalog = _imported()
    reserved = _row(3, "bad")
    reserved["query"] = {"bool": {"filter": [{"exists": {"field": "messageTime"}}]}}

    with pytest.raises(CatalogRequestError) as captured:
        catalog.add_row("p4p-prototype", 10, reserved, updated_by=CALLER)

    assert captured.value.code == DEFINITION_INVALID


# -- delete row (optimistic lock) -------------------------------------------------------


def test_delete_row_removes_the_item_under_a_matching_lock() -> None:
    dynamo, catalog = _imported()
    sk = "SECTION#010#ROW#020"
    expected = _row_updated_at(dynamo, "p4p-prototype", sk)

    catalog.delete_row("p4p-prototype", 10, 20, expected_updated_at=expected, updated_by=CALLER)

    assert ("REPORT#p4p-prototype", sk) not in dynamo.items


def test_delete_row_stale_lock_is_a_sanitized_conflict() -> None:
    dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.delete_row(
            "p4p-prototype",
            10,
            20,
            expected_updated_at="1999-01-01T00:00:00+00:00",
            updated_by=CALLER,
        )

    assert captured.value.status_code == 409
    assert captured.value.code == EDIT_CONFLICT
    assert ("REPORT#p4p-prototype", "SECTION#010#ROW#020") in dynamo.items


# -- add section (gapped allocation) ----------------------------------------------------


def test_add_section_appends_after_the_last_section_by_default() -> None:
    dynamo, catalog = _imported()

    result = catalog.add_section(
        "p4p-prototype", {"seq": 3, "name": "New Section"}, updated_by=CALLER
    )

    # Existing sections sit at 10 and 20, so an append lands at 30.
    assert result["sectionStorageSeq"] == 30
    section = _item(dynamo, "p4p-prototype", "SECTION#030")
    assert section["name"] == {"S": "New Section"}
    assert section["seq"] == {"N": "3"}


def test_add_section_inserts_at_the_midpoint_of_a_gap() -> None:
    dynamo, catalog = _imported()

    result = catalog.add_section(
        "p4p-prototype", {"seq": 3, "name": "Between"}, after_storage_seq=10, updated_by=CALLER
    )

    assert result["sectionStorageSeq"] == 15
    assert _item(dynamo, "p4p-prototype", "SECTION#015")["name"] == {"S": "Between"}


@pytest.mark.parametrize(
    "section",
    [
        {"seq": 0, "name": "bad seq"},
        {"seq": 1, "name": "   "},
        {"seq": 1},
        {"name": "no seq"},
    ],
)
def test_add_section_rejects_invalid_section(section: dict[str, Any]) -> None:
    _dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.add_section("p4p-prototype", section, updated_by=CALLER)

    assert captured.value.code == DEFINITION_INVALID


# -- sanitized backend failures ---------------------------------------------------------


def test_get_report_backend_query_failure_is_sanitized() -> None:
    class ExplodingDynamo(FakeDynamo):
        def query(self, **_kwargs: Any) -> dict[str, Any]:
            raise FakeClientError("InternalServerError")

    with pytest.raises(CatalogError, match="Catalog read failed") as captured:
        _catalog(ExplodingDynamo()).get_report("p4p-prototype")

    assert not isinstance(captured.value, CatalogRequestError)


@pytest.mark.parametrize("corrupt_query", ["not json", "[]"])
def test_export_rejects_unreadable_stored_query(corrupt_query: str) -> None:
    dynamo, catalog = _imported()
    dynamo.items[("REPORT#p4p-prototype", "SECTION#010#ROW#010")]["query"] = {"S": corrupt_query}

    with pytest.raises(CatalogError, match="unreadable"):
        catalog.export_report("p4p-prototype")


def test_publish_backend_failure_leaves_the_draft_unpublished() -> None:
    class NoPublishDynamo(FakeDynamo):
        def transact_write_items(self, **_kwargs: Any) -> dict[str, Any]:
            raise FakeClientError("InternalServerError")

    dynamo = NoPublishDynamo()
    catalog = _catalog(dynamo)

    with pytest.raises(CatalogError, match="Catalog save failed"):
        catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    assert _item(dynamo, "p4p-prototype", "META")["draft"] == {"BOOL": True}


def test_import_gives_up_after_persistent_unprocessed_items() -> None:
    class AlwaysUnprocessedDynamo(FakeDynamo):
        def batch_write_item(self, **kwargs: Any) -> dict[str, Any]:
            self.batch_calls.append(kwargs)
            return {"UnprocessedItems": kwargs["RequestItems"]}

    with pytest.raises(CatalogError, match="Catalog save failed"):
        _catalog(AlwaysUnprocessedDynamo()).import_report(
            json.dumps(_definition()), updated_by=CALLER
        )


def test_update_row_backend_failure_is_sanitized() -> None:
    class ExplodingTransact(FakeDynamo):
        def __init__(self) -> None:
            super().__init__()
            self.fail_transacts = False

        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            if self.fail_transacts:
                raise FakeClientError("InternalServerError")
            return super().transact_write_items(**kwargs)

    dynamo = ExplodingTransact()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)
    expected = _row_updated_at(dynamo, "p4p-prototype", "SECTION#010#ROW#010")
    dynamo.fail_transacts = True

    with pytest.raises(CatalogError, match="Catalog save failed") as captured:
        catalog.update_row(
            "p4p-prototype",
            10,
            10,
            _row(1, "PID-3.1"),
            expected_updated_at=expected,
            updated_by=CALLER,
        )

    assert not isinstance(captured.value, CatalogRequestError)


def test_add_row_backend_put_failure_is_sanitized() -> None:
    class ExplodingTransact(FakeDynamo):
        def __init__(self) -> None:
            super().__init__()
            self.fail_transacts = False

        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            if self.fail_transacts:
                raise FakeClientError("InternalServerError")
            return super().transact_write_items(**kwargs)

    dynamo = ExplodingTransact()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)
    dynamo.fail_transacts = True

    with pytest.raises(CatalogError, match="Catalog save failed") as captured:
        catalog.add_row("p4p-prototype", 10, _row(3, "new"), updated_by=CALLER)

    assert not isinstance(captured.value, CatalogRequestError)


# -- transactional audit trail ----------------------------------------------------------


def _audit_skeys(dynamo: FakeDynamo, report_id: str) -> list[str]:
    """Return the sort keys of every AUDIT# item under a report, in ascending order."""
    return sorted(
        sk for (pk, sk) in dynamo.items if pk == f"REPORT#{report_id}" and sk.startswith("AUDIT#")
    )


def _mutation_audit_skeys(dynamo: FakeDynamo, report_id: str) -> list[str]:
    """Return only the row/section mutation audit keys, excluding the import audit."""
    return sorted(
        sk
        for (pk, sk) in dynamo.items
        if pk == f"REPORT#{report_id}" and sk.startswith("AUDIT#SECTION#")
    )


def _plain(raw: dict[str, Any]) -> dict[str, Any]:
    """Local mirror of the catalog's plain-item unwrap for exact ``previous`` assertions."""
    plain: dict[str, Any] = {}
    for key, attribute in raw.items():
        if "S" in attribute:
            plain[key] = attribute["S"]
        elif "N" in attribute:
            plain[key] = int(attribute["N"])
        elif "BOOL" in attribute:
            plain[key] = bool(attribute["BOOL"])
    return plain


def test_import_writes_exactly_one_import_audit_and_no_per_row_audits() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    audits = _audit_skeys(dynamo, "p4p-prototype")
    assert len(audits) == 1
    audit = dynamo.items[("REPORT#p4p-prototype", audits[0])]
    assert audit["action"] == {"S": AUDIT_IMPORT}
    assert audit["source"] == {"S": "api"}
    assert audit["updated_by"] == {"S": CALLER}
    assert "updated_at" in audit
    # An import audit is a pure creation marker: it never carries a prior snapshot.
    assert "previous" not in audit
    # No audit rows were smuggled into the batched body writes.
    for call in dynamo.batch_calls:
        for entry in call["RequestItems"]["report_catalog"]:
            assert not entry["PutRequest"]["Item"]["SK"]["S"].startswith("AUDIT#")


def test_import_source_is_recorded_and_validated() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    catalog.import_report(json.dumps(_definition("seeded")), updated_by=CALLER, source=SOURCE_SEED)
    catalog.import_report(
        json.dumps(_definition("migrated")), updated_by=CALLER, source=SOURCE_MIGRATION
    )

    seeded_audit = dynamo.items[("REPORT#seeded", _audit_skeys(dynamo, "seeded")[0])]
    migrated_audit = dynamo.items[("REPORT#migrated", _audit_skeys(dynamo, "migrated")[0])]
    assert seeded_audit["source"] == {"S": "seed"}
    assert migrated_audit["source"] == {"S": "legacy-s3-migration"}


def test_import_rejects_unknown_source() -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).import_report(
            json.dumps(_definition()), updated_by=CALLER, source="anonymous"
        )

    assert captured.value.status_code == 400
    assert captured.value.code == INVALID_SOURCE


def test_update_row_is_one_transaction_pairing_the_update_with_an_audit() -> None:
    dynamo, catalog = _imported()
    sk = "SECTION#010#ROW#010"
    prior_raw = dict(_item(dynamo, "p4p-prototype", sk))
    expected = str(prior_raw["updated_at"]["S"])
    dynamo.transact_calls.clear()

    catalog.update_row(
        "p4p-prototype",
        10,
        10,
        _row(1, "PID-3.1", field="ROOT.PID.PID_3"),
        expected_updated_at=expected,
        updated_by="auth0|editor-2",
    )

    # Exactly one transaction carrying the row Update then the audit Put.
    assert len(dynamo.transact_calls) == 1
    items = dynamo.transact_calls[0]["TransactItems"]
    assert len(items) == 2
    update_op, update_spec = next(iter(items[0].items()))
    audit_op, audit_spec = next(iter(items[1].items()))
    assert update_op == "Update"
    assert audit_op == "Put"
    # The same optimistic-lock condition guards the mutation inside the transaction.
    assert update_spec["ConditionExpression"] == "attribute_exists(PK) AND #updated_at = :expected"
    assert update_spec["ExpressionAttributeValues"][":expected"] == {"S": expected}
    # The audit records the exact full prior item as compact JSON under previous.
    audit_item = audit_spec["Item"]
    assert audit_item["action"] == {"S": AUDIT_UPDATE}
    assert audit_item["SK"]["S"].startswith("AUDIT#SECTION#010#ROW#010#")
    assert json.loads(audit_item["previous"]["S"]) == _plain(prior_raw)
    assert audit_item["previous"]["S"] == json.dumps(
        _plain(prior_raw), separators=(",", ":"), sort_keys=True
    )


def test_update_row_conflict_aborts_both_mutation_and_audit() -> None:
    dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.update_row(
            "p4p-prototype",
            10,
            10,
            _row(1, "PID-3.1"),
            expected_updated_at="1999-01-01T00:00:00+00:00",
            updated_by=CALLER,
        )

    assert captured.value.code == EDIT_CONFLICT
    # Atomicity: a losing race writes neither the row change nor a mutation audit item.
    assert _mutation_audit_skeys(dynamo, "p4p-prototype") == []
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#010")["label"] == {"S": "PID-3.1"}


def test_add_row_transaction_writes_a_create_audit_without_previous() -> None:
    dynamo, catalog = _imported()
    dynamo.transact_calls.clear()

    result = catalog.add_row("p4p-prototype", 10, _row(3, "PID-7.1"), updated_by=CALLER)

    items = dynamo.transact_calls[0]["TransactItems"]
    assert len(items) == 2
    put_op, put_spec = next(iter(items[0].items()))
    audit_op, audit_spec = next(iter(items[1].items()))
    assert put_op == "Put"
    assert put_spec["ConditionExpression"] == "attribute_not_exists(PK)"
    assert audit_op == "Put"
    audit_item = audit_spec["Item"]
    assert audit_item["action"] == {"S": AUDIT_CREATE}
    assert (
        audit_item["SK"]["S"]
        == f"AUDIT#SECTION#010#ROW#{result['rowStorageSeq']:03d}#" + (audit_item["updated_at"]["S"])
    )
    assert "previous" not in audit_item


def test_delete_row_transaction_captures_the_prior_row_under_previous() -> None:
    dynamo, catalog = _imported()
    sk = "SECTION#010#ROW#020"
    prior_raw = dict(_item(dynamo, "p4p-prototype", sk))
    expected = str(prior_raw["updated_at"]["S"])
    dynamo.transact_calls.clear()

    catalog.delete_row("p4p-prototype", 10, 20, expected_updated_at=expected, updated_by=CALLER)

    items = dynamo.transact_calls[0]["TransactItems"]
    delete_op, delete_spec = next(iter(items[0].items()))
    audit_op, audit_spec = next(iter(items[1].items()))
    assert delete_op == "Delete"
    assert delete_spec["ConditionExpression"] == "attribute_exists(PK) AND #updated_at = :expected"
    assert audit_op == "Put"
    audit_item = audit_spec["Item"]
    assert audit_item["action"] == {"S": AUDIT_DELETE}
    assert json.loads(audit_item["previous"]["S"]) == _plain(prior_raw)
    # The row is gone but its delete audit remains under the partition.
    assert ("REPORT#p4p-prototype", sk) not in dynamo.items
    assert len(_mutation_audit_skeys(dynamo, "p4p-prototype")) == 1


def test_delete_row_conflict_aborts_both_mutation_and_audit() -> None:
    dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError):
        catalog.delete_row(
            "p4p-prototype",
            10,
            20,
            expected_updated_at="1999-01-01T00:00:00+00:00",
            updated_by=CALLER,
        )

    assert ("REPORT#p4p-prototype", "SECTION#010#ROW#020") in dynamo.items
    assert _mutation_audit_skeys(dynamo, "p4p-prototype") == []


def test_add_section_transaction_writes_a_section_create_audit() -> None:
    dynamo, catalog = _imported()
    dynamo.transact_calls.clear()

    result = catalog.add_section(
        "p4p-prototype", {"seq": 3, "name": "New Section"}, updated_by=CALLER
    )

    items = dynamo.transact_calls[0]["TransactItems"]
    put_op, put_spec = next(iter(items[0].items()))
    audit_op, audit_spec = next(iter(items[1].items()))
    assert put_op == "Put"
    assert put_spec["ConditionExpression"] == "attribute_not_exists(PK)"
    assert audit_op == "Put"
    audit_item = audit_spec["Item"]
    assert audit_item["action"] == {"S": AUDIT_CREATE}
    # A section-create audit uses the section-only audit SK (no ROW segment).
    assert audit_item["SK"]["S"].startswith(f"AUDIT#SECTION#{result['sectionStorageSeq']:03d}#")
    assert "#ROW#" not in audit_item["SK"]["S"]
    assert "previous" not in audit_item


# -- assembly ignores audit items -------------------------------------------------------


def test_assembly_and_round_trip_ignore_audit_items() -> None:
    text = json.dumps(_definition())
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(text, updated_by=CALLER)

    # Perform a real mutation so audit rows (including a row-level audit with #ROW# in its
    # SK, which superficially resembles a row) coexist with the definition items.
    sk = "SECTION#010#ROW#010"
    expected = str(_item(dynamo, "p4p-prototype", sk)["updated_at"]["S"])
    catalog.update_row(
        "p4p-prototype", 10, 10, _row(1, "PID-3.1"), expected_updated_at=expected, updated_by=CALLER
    )
    assert len(_audit_skeys(dynamo, "p4p-prototype")) >= 1

    # Assembly explicitly skips AUDIT# items, so the export is unchanged by their presence.
    exported = catalog.export_report("p4p-prototype")
    assert _canonical(exported["definition"]) == _canonical(
        load_report_definition_json(text).model_dump(mode="json")
    )
    report = catalog.get_report("p4p-prototype")
    assert len(report["definition"]["sections"]) == 2


# -- history ----------------------------------------------------------------------------


def _seed_audit(
    dynamo: FakeDynamo,
    report_id: str,
    sk: str,
    action: str,
    *,
    previous: dict[str, Any] | None = None,
    source: str | None = None,
) -> None:
    item: dict[str, Any] = {
        "PK": {"S": f"REPORT#{report_id}"},
        "SK": {"S": sk},
        "action": {"S": action},
        "updated_by": {"S": CALLER},
        "updated_at": {"S": sk.rsplit("#", 1)[-1]},
    }
    if previous is not None:
        item["previous"] = {"S": json.dumps(previous, separators=(",", ":"), sort_keys=True)}
    if source is not None:
        item["source"] = {"S": source}
    dynamo.items[(f"REPORT#{report_id}", sk)] = item


def test_history_queries_audit_partition_newest_first_with_a_limit() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    _seed_audit(dynamo, "r", "AUDIT#IMPORT#2026-01-01T00:00:00+00:00", "import", source="api")
    _seed_audit(dynamo, "r", "AUDIT#SECTION#010#ROW#010#2026-02-01T00:00:00+00:00", "update")
    _seed_audit(dynamo, "r", "AUDIT#SECTION#010#ROW#010#2026-03-01T00:00:00+00:00", "delete")

    entries = catalog.history("r")

    call = dynamo.query_calls[-1]
    assert call["ScanIndexForward"] is False
    assert call["Limit"] == 50
    assert call["KeyConditionExpression"] == "PK = :pk AND begins_with(SK, :prefix)"
    assert call["ExpressionAttributeValues"][":prefix"] == {"S": "AUDIT#"}
    # Newest first: the March delete precedes the February update precedes the import.
    assert [entry["action"] for entry in entries] == ["delete", "update", "import"]


def test_history_respects_the_requested_limit() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    for day in range(1, 6):
        _seed_audit(
            dynamo, "r", f"AUDIT#SECTION#010#ROW#010#2026-01-0{day}T00:00:00+00:00", "update"
        )

    entries = catalog.history("r", limit=2)

    assert dynamo.query_calls[-1]["Limit"] == 2
    assert len(entries) == 2
    # The two most recent (highest timestamp) audits are returned.
    assert entries[0]["sk"].endswith("2026-01-05T00:00:00+00:00")
    assert entries[1]["sk"].endswith("2026-01-04T00:00:00+00:00")


def test_history_parses_previous_object_and_source() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    prior = {"seq": 1, "label": "PID-3.1", "storage_seq": 10}
    _seed_audit(
        dynamo,
        "r",
        "AUDIT#SECTION#010#ROW#010#2026-02-01T00:00:00+00:00",
        "delete",
        previous=prior,
    )
    _seed_audit(dynamo, "r", "AUDIT#IMPORT#2026-01-01T00:00:00+00:00", "import", source="seed")

    entries = catalog.history("r")

    delete_entry = next(entry for entry in entries if entry["action"] == "delete")
    import_entry = next(entry for entry in entries if entry["action"] == "import")
    # previous is decoded back into an object, not left as a JSON string.
    assert delete_entry["previous"] == prior
    assert "source" not in delete_entry
    assert import_entry["source"] == "seed"
    assert "previous" not in import_entry


@pytest.mark.parametrize("limit", [0, -1, 201, 1000])
def test_history_rejects_out_of_range_limit(limit: int) -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).history("p4p-prototype", limit=limit)

    assert captured.value.status_code == 400
    assert captured.value.code == INVALID_LIMIT


def test_history_accepts_the_inclusive_bounds() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    assert catalog.history("p4p-prototype", limit=1) == []
    assert dynamo.query_calls[-1]["Limit"] == 1
    assert catalog.history("p4p-prototype", limit=200) == []
    assert dynamo.query_calls[-1]["Limit"] == 200


def test_history_rejects_unsafe_report_id() -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).history("Bad_Id")

    assert captured.value.code == INVALID_REPORT_ID


def test_history_sanitizes_backend_failure() -> None:
    class ExplodingDynamo(FakeDynamo):
        def query(self, **_kwargs: Any) -> dict[str, Any]:
            raise FakeClientError("InternalServerError")

    with pytest.raises(CatalogError, match="Catalog read failed"):
        _catalog(ExplodingDynamo()).history("p4p-prototype")


# -- whole-report delete ----------------------------------------------------------------


def _report_partition_keys(dynamo: FakeDynamo, report_id: str) -> list[str]:
    """Return every sort key still present under a report's own partition."""
    return sorted(sk for (pk, sk) in dynamo.items if pk == f"REPORT#{report_id}")


def _deletion_audits(dynamo: FakeDynamo, report_id: str) -> list[dict[str, Any]]:
    """Return every durable deletion audit recorded for a report id, in ascending SK order."""
    return [
        item
        for (pk, sk), item in sorted(dynamo.items.items())
        if pk == DELETED_REPORTS_PK and sk.startswith(f"REPORT#{report_id}#")
    ]


def test_delete_report_transaction_precedes_batch_cleanup() -> None:
    class OrderedDynamo(FakeDynamo):
        def __init__(self) -> None:
            super().__init__()
            self.ops: list[str] = []

        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            self.ops.append("transact")
            return super().transact_write_items(**kwargs)

        def batch_write_item(self, **kwargs: Any) -> dict[str, Any]:
            self.ops.append("batch")
            return super().batch_write_item(**kwargs)

    dynamo = OrderedDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)
    dynamo.ops.clear()

    result = catalog.delete_report("p4p-prototype", updated_by=CALLER)

    # The META-delete-plus-audit transaction runs first, then the report items are swept.
    assert dynamo.ops == ["transact", "batch"]
    assert result == {"reportId": "p4p-prototype", "deleted": True}
    # The delete transaction removes META and puts the deletion audit atomically.
    transact_items = dynamo.transact_calls[-1]["TransactItems"]
    delete_op, delete_spec = next(iter(transact_items[0].items()))
    audit_op, audit_spec = next(iter(transact_items[1].items()))
    assert delete_op == "Delete"
    assert delete_spec["Key"]["SK"] == {"S": "META"}
    assert delete_spec["ConditionExpression"] == "attribute_exists(PK)"
    assert audit_op == "Put"
    assert audit_spec["Item"]["PK"] == {"S": DELETED_REPORTS_PK}


def test_delete_report_removes_every_report_item_and_keeps_a_durable_audit() -> None:
    text = json.dumps(_definition())
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(text, updated_by=CALLER)

    catalog.delete_report("p4p-prototype", updated_by="auth0|remover")

    # No item remains under the report's own partition (META, sections, rows, audits).
    assert _report_partition_keys(dynamo, "p4p-prototype") == []
    # A single deletion audit survives outside the report partition with the prior definition.
    audits = _deletion_audits(dynamo, "p4p-prototype")
    assert len(audits) == 1
    audit = audits[0]
    assert audit["action"] == {"S": "delete"}
    assert audit["report_id"] == {"S": "p4p-prototype"}
    assert audit["updated_by"] == {"S": "auth0|remover"}
    assert audit["SK"]["S"].startswith("REPORT#p4p-prototype#")
    expected_previous = _canonical(load_report_definition_json(text).model_dump(mode="json"))
    assert audit["previous"] == {"S": expected_previous}


def test_delete_report_is_idempotent_without_duplicate_audits() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    first = catalog.delete_report("p4p-prototype", updated_by=CALLER)
    transacts_after_first = len(dynamo.transact_calls)
    second = catalog.delete_report("p4p-prototype", updated_by=CALLER)

    assert first["deleted"] is True
    # A repeat finds no META, so it neither opens a transaction nor writes a second audit.
    assert second == {"reportId": "p4p-prototype", "deleted": False}
    assert len(dynamo.transact_calls) == transacts_after_first
    assert len(_deletion_audits(dynamo, "p4p-prototype")) == 1


def test_delete_report_partial_cleanup_can_be_retried_without_a_second_audit() -> None:
    class FlakyBatch(FakeDynamo):
        def __init__(self) -> None:
            super().__init__()
            self.fail_batch = False

        def batch_write_item(self, **kwargs: Any) -> dict[str, Any]:
            if self.fail_batch:
                self.batch_calls.append(kwargs)
                raise FakeClientError("ProvisionedThroughputExceededException")
            return super().batch_write_item(**kwargs)

    dynamo = FlakyBatch()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)

    # First delete: the META transaction commits but the batch cleanup fails partway.
    dynamo.fail_batch = True
    with pytest.raises(CatalogError, match="Catalog save failed"):
        catalog.delete_report("p4p-prototype", updated_by=CALLER)

    # META is gone and the durable audit exists, but section/row items still linger.
    assert ("REPORT#p4p-prototype", "META") not in dynamo.items
    assert len(_deletion_audits(dynamo, "p4p-prototype")) == 1
    assert "SECTION#010#ROW#010" in _report_partition_keys(dynamo, "p4p-prototype")

    # Retry: no META means no new transaction and no duplicate audit, only cleanup.
    dynamo.fail_batch = False
    result = catalog.delete_report("p4p-prototype", updated_by=CALLER)

    assert result == {"reportId": "p4p-prototype", "deleted": False}
    assert _report_partition_keys(dynamo, "p4p-prototype") == []
    assert len(_deletion_audits(dynamo, "p4p-prototype")) == 1


def test_delete_report_rejects_unsafe_report_id() -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).delete_report("Bad_Id", updated_by=CALLER)

    assert captured.value.code == INVALID_REPORT_ID


def test_delete_report_of_absent_report_is_a_noop() -> None:
    dynamo = FakeDynamo()
    catalog = _catalog(dynamo)

    result = catalog.delete_report("never-existed", updated_by=CALLER)

    assert result == {"reportId": "never-existed", "deleted": False}
    assert dynamo.transact_calls == []
    assert _deletion_audits(dynamo, "never-existed") == []


# -- whole-definition replacement -------------------------------------------------------


def _replacement_definition(report_id: str = "p4p-prototype") -> dict[str, Any]:
    return {
        "report_id": report_id,
        "name": "P4P Prototype v2",
        "description": "Replaced definition",
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": [
            {"seq": 7, "name": "Rebuilt ADT", "rows": [_row(1, "NEW-1"), _row(2, "NEW-2")]},
            {"seq": 9, "name": "Rebuilt ORU", "rows": [_row(1, "NEW-3", "ROOT.OBR")]},
        ],
    }


def _meta_updated_at(dynamo: FakeDynamo, report_id: str) -> str:
    return str(_item(dynamo, report_id, "META")["updated_at"]["S"])


def test_replace_report_round_trips_the_new_definition_byte_identically() -> None:
    dynamo, catalog = _imported()
    expected = _meta_updated_at(dynamo, "p4p-prototype")
    new_text = json.dumps(_replacement_definition())

    result = catalog.replace_report(
        new_text, expected_updated_at=expected, updated_by="auth0|replacer"
    )

    exported = catalog.export_report("p4p-prototype")
    canonical_new = _canonical(load_report_definition_json(new_text).model_dump(mode="json"))
    assert exported["text"] == canonical_new
    assert _canonical(exported["definition"]) == canonical_new
    # The header metadata and lock advance to the replacement's values.
    assert result["name"] == "P4P Prototype v2"
    assert result["updatedBy"] == "auth0|replacer"
    meta = _item(dynamo, "p4p-prototype", "META")
    assert meta["draft"] == {"BOOL": False}
    assert meta["description"] == {"S": "Replaced definition"}
    assert meta["updated_at"]["S"] == result["updatedAt"]
    assert meta["updated_at"]["S"] != expected


def test_replace_report_hides_the_draft_and_advances_the_lock_only_on_publish() -> None:
    class NoPublishTransact(FakeDynamo):
        def __init__(self) -> None:
            super().__init__()
            self.fail_transacts = False

        def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
            if self.fail_transacts:
                raise FakeClientError("InternalServerError")
            return super().transact_write_items(**kwargs)

    dynamo = NoPublishTransact()
    catalog = _catalog(dynamo)
    catalog.import_report(json.dumps(_definition()), updated_by=CALLER)
    expected = _meta_updated_at(dynamo, "p4p-prototype")
    dynamo.fail_transacts = True

    with pytest.raises(CatalogError, match="Catalog save failed"):
        catalog.replace_report(
            json.dumps(_replacement_definition()), expected_updated_at=expected, updated_by=CALLER
        )

    # A failed publish leaves an invisible draft whose optimistic lock is unchanged.
    meta = _item(dynamo, "p4p-prototype", "META")
    assert meta["draft"] == {"BOOL": True}
    assert meta["updated_at"] == {"S": expected}
    assert catalog.list_reports() == []

    # The same original lock still lets the replace be retried to completion.
    dynamo.fail_transacts = False
    catalog.replace_report(
        json.dumps(_replacement_definition()), expected_updated_at=expected, updated_by=CALLER
    )
    assert _item(dynamo, "p4p-prototype", "META")["draft"] == {"BOOL": False}
    assert [report["reportId"] for report in catalog.list_reports()] == ["p4p-prototype"]


def test_replace_report_stale_lock_conflicts_without_touching_the_definition() -> None:
    dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.replace_report(
            json.dumps(_replacement_definition()),
            expected_updated_at="1999-01-01T00:00:00+00:00",
            updated_by=CALLER,
        )

    assert captured.value.status_code == 409
    assert captured.value.code == EDIT_CONFLICT
    # Nothing was hidden, deleted, or rewritten: the original definition is intact.
    meta = _item(dynamo, "p4p-prototype", "META")
    assert meta["draft"] == {"BOOL": False}
    assert meta["name"] == {"S": "P4P Prototype"}
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#010")["label"] == {"S": "PID-3.1"}
    exported = catalog.export_report("p4p-prototype")
    assert _canonical(exported["definition"]) == _canonical(_definition())


def test_replace_report_writes_one_update_audit_carrying_the_prior_definition() -> None:
    dynamo, catalog = _imported()
    prior_text = catalog.export_report("p4p-prototype")["text"]
    expected = _meta_updated_at(dynamo, "p4p-prototype")
    dynamo.transact_calls.clear()

    catalog.replace_report(
        json.dumps(_replacement_definition()), expected_updated_at=expected, updated_by=CALLER
    )

    # The republish is a single transaction: META update under the lock, then one audit.
    items = dynamo.transact_calls[-1]["TransactItems"]
    assert len(items) == 2
    update_op, update_spec = next(iter(items[0].items()))
    audit_op, audit_spec = next(iter(items[1].items()))
    assert update_op == "Update"
    assert update_spec["ConditionExpression"] == "attribute_exists(PK) AND #updated_at = :expected"
    assert update_spec["ExpressionAttributeValues"][":expected"] == {"S": expected}
    assert audit_op == "Put"
    audit_item = audit_spec["Item"]
    assert audit_item["action"] == {"S": AUDIT_UPDATE}
    assert audit_item["SK"]["S"].startswith("AUDIT#REPLACE#")
    # The audit preserves the full previous definition as canonical JSON.
    assert audit_item["previous"] == {"S": prior_text}


def test_replace_report_preserves_placeholder_null_queries() -> None:
    dynamo, catalog = _imported()
    expected = _meta_updated_at(dynamo, "p4p-prototype")
    with_placeholder = {
        "report_id": "p4p-prototype",
        "name": "With Placeholder",
        "description": "",
        "partition_field": "sourceFacilityId",
        "time_field": "messageTime",
        "sections": [
            {
                "seq": 1,
                "name": "Mixed",
                "rows": [
                    {
                        "seq": 1,
                        "label": "placeholder-row",
                        "description": "no query yet",
                        "index": "hl7-messages-v1",
                        "query": None,
                    },
                    _row(2, "counted-row"),
                ],
            }
        ],
    }
    new_text = json.dumps(with_placeholder)

    catalog.replace_report(new_text, expected_updated_at=expected, updated_by=CALLER)

    exported = catalog.export_report("p4p-prototype")
    assert exported["definition"]["sections"][0]["rows"][0]["query"] is None
    assert _canonical(exported["definition"]) == _canonical(
        load_report_definition_json(new_text).model_dump(mode="json")
    )


def test_replace_report_deletes_only_section_items_not_audits() -> None:
    dynamo, catalog = _imported()
    # Age an audit into the report partition via a real row mutation before replacing.
    sk = "SECTION#010#ROW#010"
    row_lock = _row_updated_at(dynamo, "p4p-prototype", sk)
    catalog.update_row(
        "p4p-prototype", 10, 10, _row(1, "PID-3.1"), expected_updated_at=row_lock, updated_by=CALLER
    )
    audits_before = set(_audit_skeys(dynamo, "p4p-prototype"))
    expected = _meta_updated_at(dynamo, "p4p-prototype")

    catalog.replace_report(
        json.dumps(_replacement_definition()), expected_updated_at=expected, updated_by=CALLER
    )

    # Pre-existing report-scoped audits survive the replacement's SECTION#-only cleanup.
    assert audits_before <= set(_audit_skeys(dynamo, "p4p-prototype"))
    # The old rows are gone and the new body is present under fresh gapped storage seqs.
    assert ("REPORT#p4p-prototype", "SECTION#010#ROW#020") in dynamo.items
    assert _item(dynamo, "p4p-prototype", "SECTION#010#ROW#010")["label"] == {"S": "NEW-1"}


def test_replace_report_rejects_invalid_definition() -> None:
    dynamo, catalog = _imported()
    expected = _meta_updated_at(dynamo, "p4p-prototype")
    invalid = _replacement_definition()
    invalid["partition_field"] = "notAllowed"

    with pytest.raises(CatalogRequestError) as captured:
        catalog.replace_report(json.dumps(invalid), expected_updated_at=expected, updated_by=CALLER)

    assert captured.value.code == DEFINITION_INVALID
    # The original definition is untouched by a rejected replacement.
    assert _item(dynamo, "p4p-prototype", "META")["name"] == {"S": "P4P Prototype"}


def test_replace_report_missing_report_is_sanitized_not_found() -> None:
    with pytest.raises(CatalogRequestError) as captured:
        _catalog(FakeDynamo()).replace_report(
            json.dumps(_replacement_definition("missing-report")),
            expected_updated_at="2026-01-01T00:00:00+00:00",
            updated_by=CALLER,
        )

    assert captured.value.status_code == 404
    assert captured.value.code == DEFINITION_NOT_FOUND


def test_replace_report_requires_a_precondition() -> None:
    _dynamo, catalog = _imported()

    with pytest.raises(CatalogRequestError) as captured:
        catalog.replace_report(
            json.dumps(_replacement_definition()), expected_updated_at="", updated_by=CALLER
        )

    assert captured.value.code == INVALID_PRECONDITION
