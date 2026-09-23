#!/usr/bin/env python3
"""Idempotently seed the validated report definition into the catalog DynamoDB table.

The definition is imported through :meth:`ReportCatalog.import_report`, the single runtime
path that persists a report as row-granular items in one DynamoDB table. The tool performs
no stack discovery or deployment: supply the catalog table name from the CDK outputs.

Idempotency is decided by comparing the seed's canonical JSON to the catalog's canonical
export of the stored report:

* If the report does not exist, it is imported.
* If it exists and its canonical export already equals the seed, nothing is written.
* If it exists but differs, the command fails unless ``--overwrite`` is supplied. Overwrite
  is a development-only escape hatch: it explicitly deletes every item under the report's
  partition key and then re-imports, so no drifted rows survive.

A canonical import -> items -> export round trip is byte-identical, so after every import
the tool re-exports and asserts the bytes match what it imported.

The optional one-shot ``--migrate-bucket``/``--migrate-prefix`` path reads the legacy S3
JSON definition objects (default prefix ``definitions/``) and imports each through the same
catalog path. It exists only to move existing environments off S3 definition storage; no S3
definition code remains in the runtime.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from src.report_catalog import (
    DEFINITION_NOT_FOUND,
    SOURCE_MIGRATION,
    SOURCE_SEED,
    CatalogRequestError,
    ReportCatalog,
)
from src.report_definition import ReportDefinition, load_report_definition_json

DEFAULT_SEED = Path(__file__).resolve().parents[1] / "seed" / "p4p-prototype.json"
DEFAULT_MIGRATE_PREFIX = "definitions/"
SEED_ACTOR = "seed-report-tool"
REPORT_PK_PREFIX = "REPORT#"

STORED_DEFINITION_DIFFERS = "stored report differs; rerun with --overwrite after review (dev only)"
CANONICAL_ROUND_TRIP_FAILED = "canonical export did not match imported definition"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="Reports catalog DynamoDB table name")
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--profile")
    parser.add_argument("--region")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--migrate-bucket",
        help="One-shot: import every legacy S3 JSON definition from this bucket, then exit",
    )
    parser.add_argument(
        "--migrate-prefix",
        default=DEFAULT_MIGRATE_PREFIX,
        help="S3 key prefix holding the legacy JSON definitions (default: definitions/)",
    )
    return parser.parse_args()


def _session(profile: str | None, region: str | None) -> Any:
    import boto3  # type: ignore[import-not-found]

    kwargs: dict[str, str] = {}
    if profile:
        kwargs["profile_name"] = profile
    if region:
        kwargs["region_name"] = region
    return boto3.Session(**kwargs)


def _canonical(definition: ReportDefinition) -> str:
    """Return the canonical JSON text for a definition, matching the catalog's export."""
    return json.dumps(
        definition.model_dump(mode="json"),
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_seed(seed_path: Path) -> tuple[ReportDefinition, str]:
    definition = load_report_definition_json(seed_path.read_text(encoding="utf-8"))
    return definition, _canonical(definition)


def _existing_export(catalog: ReportCatalog, report_id: str) -> str | None:
    """Return the stored report's canonical export text, or ``None`` if it does not exist."""
    try:
        return str(catalog.export_report(report_id)["text"])
    except CatalogRequestError as error:
        if error.code == DEFINITION_NOT_FOUND:
            return None
        raise


def _delete_report_items(dynamo: Any, table: str, report_id: str) -> None:
    """Development-only: delete every item sharing the report's partition key."""
    request: dict[str, Any] = {
        "TableName": table,
        "KeyConditionExpression": "PK = :pk",
        "ExpressionAttributeValues": {":pk": {"S": f"{REPORT_PK_PREFIX}{report_id}"}},
    }
    while True:
        response = dynamo.query(**request)
        for item in response.get("Items", []):
            dynamo.delete_item(
                TableName=table,
                Key={"PK": item["PK"], "SK": item["SK"]},
            )
        start_key = response.get("LastEvaluatedKey")
        if not start_key:
            break
        request["ExclusiveStartKey"] = start_key


def _apply(
    *,
    catalog: ReportCatalog,
    dynamo: Any,
    table: str,
    report_id: str,
    canonical: str,
    overwrite: bool,
    source: str,
) -> str:
    """Import ``canonical`` under ``report_id`` idempotently and return the action taken."""
    existing = _existing_export(catalog, report_id)
    if existing is not None:
        if existing == canonical:
            return "unchanged"
        if not overwrite:
            raise RuntimeError(STORED_DEFINITION_DIFFERS)
        _delete_report_items(dynamo, table, report_id)

    catalog.import_report(canonical, updated_by=SEED_ACTOR, source=source)
    if str(catalog.export_report(report_id)["text"]) != canonical:
        raise RuntimeError(CANONICAL_ROUND_TRIP_FAILED)
    return "overwritten" if existing is not None else "imported"


def seed_report(
    *,
    dynamo: Any,
    table: str,
    seed_path: Path = DEFAULT_SEED,
    overwrite: bool = False,
) -> dict[str, str]:
    """Idempotently import the seed definition into the catalog table."""
    catalog = ReportCatalog(dynamo, table_name=table)
    definition, canonical = _canonical_seed(seed_path)
    action = _apply(
        catalog=catalog,
        dynamo=dynamo,
        table=table,
        report_id=definition.report_id,
        canonical=canonical,
        overwrite=overwrite,
        source=SOURCE_SEED,
    )
    return {"reportId": definition.report_id, "action": action}


def _iter_s3_definitions(s3: Any, bucket: str, prefix: str) -> Iterator[tuple[str, str]]:
    """Yield ``(key, text)`` for every legacy ``.json`` definition object under ``prefix``."""
    request: dict[str, str] = {"Bucket": bucket, "Prefix": prefix}
    while True:
        response = s3.list_objects_v2(**request)
        for entry in response.get("Contents", []):
            key = entry.get("Key")
            if not isinstance(key, str) or not key.endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            text = body.decode("utf-8") if isinstance(body, bytes) else str(body)
            yield key, text
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
        if not token:
            break
        request["ContinuationToken"] = token


def migrate_definitions(
    *,
    dynamo: Any,
    table: str,
    s3: Any,
    bucket: str,
    prefix: str = DEFAULT_MIGRATE_PREFIX,
    overwrite: bool = False,
) -> list[dict[str, str]]:
    """Import every legacy S3 JSON definition through the catalog, idempotently."""
    catalog = ReportCatalog(dynamo, table_name=table)
    results: list[dict[str, str]] = []
    for key, text in _iter_s3_definitions(s3, bucket, prefix):
        definition = load_report_definition_json(text)
        action = _apply(
            catalog=catalog,
            dynamo=dynamo,
            table=table,
            report_id=definition.report_id,
            canonical=_canonical(definition),
            overwrite=overwrite,
            source=SOURCE_MIGRATION,
        )
        results.append({"key": key, "reportId": definition.report_id, "action": action})
    return results


def main() -> int:
    args = parse_args()
    session = _session(args.profile, args.region)
    dynamo = session.client("dynamodb")

    if args.migrate_bucket:
        results = migrate_definitions(
            dynamo=dynamo,
            table=args.table,
            s3=session.client("s3"),
            bucket=args.migrate_bucket,
            prefix=args.migrate_prefix,
            overwrite=args.overwrite,
        )
        for result in results:
            print(f"migrate {result['reportId']} from {result['key']}: {result['action']}")
        print(f"migrated {len(results)} definition(s)")
        return 0

    result = seed_report(
        dynamo=dynamo,
        table=args.table,
        seed_path=args.seed.expanduser().resolve(),
        overwrite=args.overwrite,
    )
    print(f"seed report {result['reportId']}: {result['action']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
