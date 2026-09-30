"""Tests for the DynamoDB-only report seeding tool.

The tool imports the seed definition through :meth:`ReportCatalog.import_report` and decides
idempotency by comparing the seed's canonical JSON to the catalog's canonical export. These
tests drive a small in-memory single-table DynamoDB double (the same low-level operations the
catalog issues) so the round trip exercises real item layout, plus a tiny S3 double for the
one-shot migration path.
"""

from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from src.report_definition import load_report_definition_json
from tools.seed_report import (
    STORED_DEFINITION_DIFFERS,
    migrate_definitions,
    seed_report,
)

SEED_PATH = Path(__file__).resolve().parents[1] / "seed" / "p4p-prototype.json"


def _canonical_seed_text() -> str:
    definition = load_report_definition_json(SEED_PATH.read_text(encoding="utf-8"))
    return json.dumps(definition.model_dump(mode="json"), separators=(",", ":"), sort_keys=True)


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

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.put_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []
        self.delete_calls: list[dict[str, Any]] = []
        self.batch_calls: list[dict[str, Any]] = []
        self.transact_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []

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
        item = dict(existing) if existing is not None else dict(kwargs["Key"])
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
        return {"Items": matches}

    def batch_write_item(self, **kwargs: Any) -> dict[str, Any]:
        self.batch_calls.append(kwargs)
        for requests in kwargs["RequestItems"].values():
            for entry in requests:
                item = entry["PutRequest"]["Item"]
                self.items[_key(item)] = dict(item)
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

    def scan(self, **kwargs: Any) -> dict[str, Any]:
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

    def mutation_count(self) -> int:
        return (
            len(self.put_calls)
            + len(self.update_calls)
            + len(self.delete_calls)
            + len(self.batch_calls)
            + len(self.transact_calls)
        )


class FakeS3:
    """A tiny S3 double serving legacy JSON definition objects under a prefix."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = objects

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        prefix = kwargs.get("Prefix", "")
        contents = [{"Key": key} for key in sorted(self._objects) if key.startswith(prefix)]
        return {"Contents": contents, "IsTruncated": False}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        return {"Body": BytesIO(self._objects[kwargs["Key"]])}


def test_seed_imports_then_rerun_is_unchanged_and_writes_nothing() -> None:
    dynamo = FakeDynamo()

    first = seed_report(dynamo=dynamo, table="catalog", seed_path=SEED_PATH)
    assert first == {"reportId": "p4p-prototype", "action": "imported"}

    writes_after_import = dynamo.mutation_count()
    assert writes_after_import > 0

    second = seed_report(dynamo=dynamo, table="catalog", seed_path=SEED_PATH)
    assert second == {"reportId": "p4p-prototype", "action": "unchanged"}
    assert dynamo.mutation_count() == writes_after_import


def test_seed_export_bytes_equal_canonical_seed() -> None:
    dynamo = FakeDynamo()
    seed_report(dynamo=dynamo, table="catalog", seed_path=SEED_PATH)

    from src.report_catalog import ReportCatalog

    catalog = ReportCatalog(dynamo, table_name="catalog")
    exported = catalog.export_report("p4p-prototype")["text"]
    assert exported == _canonical_seed_text()


def test_seed_drift_requires_explicit_overwrite() -> None:
    dynamo = FakeDynamo()
    seed_report(dynamo=dynamo, table="catalog", seed_path=SEED_PATH)

    # Drift the stored report so its canonical export no longer matches the seed.
    meta_key = ("REPORT#p4p-prototype", "META")
    dynamo.items[meta_key]["name"] = {"S": "drifted name"}

    with pytest.raises(RuntimeError, match="--overwrite"):
        seed_report(dynamo=dynamo, table="catalog", seed_path=SEED_PATH)


def test_seed_overwrite_deletes_drift_then_reimports() -> None:
    dynamo = FakeDynamo()
    seed_report(dynamo=dynamo, table="catalog", seed_path=SEED_PATH)

    # Inject a stray row and drift the header; both must be gone after overwrite.
    dynamo.items[("REPORT#p4p-prototype", "META")]["name"] = {"S": "drifted name"}
    stray_key = ("REPORT#p4p-prototype", "SECTION#999#ROW#999")
    dynamo.items[stray_key] = {
        "PK": {"S": "REPORT#p4p-prototype"},
        "SK": {"S": "SECTION#999#ROW#999"},
        "seq": {"N": "1"},
        "label": {"S": "stray"},
        "description": {"S": ""},
        "index": {"S": "hl7-messages-v1"},
        "query": {"S": "{}"},
        "storage_seq": {"N": "999"},
        "updated_at": {"S": "2026-01-01T00:00:00+00:00"},
        "updated_by": {"S": "stray"},
    }

    result = seed_report(
        dynamo=dynamo,
        table="catalog",
        seed_path=SEED_PATH,
        overwrite=True,
    )
    assert result == {"reportId": "p4p-prototype", "action": "overwritten"}
    assert stray_key not in dynamo.items

    from src.report_catalog import ReportCatalog

    catalog = ReportCatalog(dynamo, table_name="catalog")
    assert catalog.export_report("p4p-prototype")["text"] == _canonical_seed_text()


def test_migration_imports_legacy_s3_definitions_then_is_idempotent() -> None:
    dynamo = FakeDynamo()
    s3 = FakeS3(
        {
            "definitions/p4p-prototype.json": SEED_PATH.read_bytes(),
            "definitions/README.txt": b"ignored",
        }
    )

    results = migrate_definitions(dynamo=dynamo, table="catalog", s3=s3, bucket="reports")
    assert results == [
        {
            "key": "definitions/p4p-prototype.json",
            "reportId": "p4p-prototype",
            "action": "imported",
        }
    ]

    from src.report_catalog import ReportCatalog

    catalog = ReportCatalog(dynamo, table_name="catalog")
    assert catalog.export_report("p4p-prototype")["text"] == _canonical_seed_text()

    writes_after_migrate = dynamo.mutation_count()
    rerun = migrate_definitions(dynamo=dynamo, table="catalog", s3=s3, bucket="reports")
    assert rerun[0]["action"] == "unchanged"
    assert dynamo.mutation_count() == writes_after_migrate


def test_migration_drift_requires_overwrite() -> None:
    dynamo = FakeDynamo()
    s3 = FakeS3({"definitions/p4p-prototype.json": SEED_PATH.read_bytes()})
    migrate_definitions(dynamo=dynamo, table="catalog", s3=s3, bucket="reports")

    dynamo.items[("REPORT#p4p-prototype", "META")]["description"] = {"S": "drifted"}

    with pytest.raises(RuntimeError, match=re.escape(STORED_DEFINITION_DIFFERS)):
        migrate_definitions(dynamo=dynamo, table="catalog", s3=s3, bucket="reports")
