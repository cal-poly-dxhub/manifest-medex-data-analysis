# Manifest MedEx Data Quality Platform

A single Python CDK application with active modules directly under `src/`.

## Files

- `app.py` starts CDK, loads configuration, enables cdk-nag, and creates the stack.
- `src/config.py` validates environment, account, region, cdk-nag, and termination protection.
- `src/stack.py` defines storage, queues, Lambda functions, networking, OpenSearch, logs, and alarms.
- `src/parser.py` contains the compute-independent HL7 splitter, the `python-hl7` adapter, and application-specific projections.
- `src/ccda_parser.py` securely parses CCDA XML into a separate query-driven `CD` projection.
- `src/parse_handler.py` dispatches HL7/CCDA raw-object events and writes parsed/error objects.
- `src/index_handler.py` bulk-indexes parsed documents into format-specific OpenSearch indexes.
- `lambda-requirements.txt` hash-pins dependencies installed into the Lambda asset.
- `tests/` covers configuration, parsing, handlers, infrastructure, and cdk-nag.

There is no extra package folder inside `src/`.

## Implemented data flow

```text
versioned raw S3 object
        |
        v
S3 EventBridge event --> parse SQS queue --> parser Lambda
                                                |-- permanent error --> error S3
                                                |-- transient error --> SQS retry/DLQ
                                                v
                                deterministic parsed JSON in parsed S3
                                                |
                                                v
                                      index SQS queue --> indexer Lambda
                                                                  |
                                                                  v
                                        private OpenSearch Serverless collection
                                      /                                 \
                       hl7-messages-v1                         ccda-documents-v1
```

Both SQS event sources return partial batch failures. Parsed S3 keys and OpenSearch document IDs are deterministic, so duplicate SQS delivery converges on the same logical record.

## Supported parsing behavior

This release uses the hash-pinned `hl7==0.4.5` (`python-hl7`) library for lenient segment, field, repetition, component, subcomponent, and dynamic-delimiter parsing. Application code retains message-boundary handling, safe standard escape decoding, metadata, timestamps, deterministic identity, and the dashboard projection. Parser output identifies this implementation as version `0.2.0`.

Supported UTF-8 HL7 v2 batch behavior includes:

- CR, LF, or CRLF segment separators.
- Optional MLLP framing.
- Multiple concatenated messages split on `MSH` segments.
- Dynamic field/component/repetition/escape/subcomponent delimiters from `MSH`.
- ADT, MDM, ORU, RDE, and VXU messages for metadata and segment counting.
- Unknown and locally defined segments are accepted and counted.
- Deterministic source checksum, message ordinal, and document ID.
- HL7 timestamp normalization; timestamps without an offset are currently interpreted as UTC.
- A 50 MiB raw-object limit for the Lambda path. Larger inputs become structured errors and are candidates for a future ECS/AWS Batch path.

The indexed `ROOT` projection implements the exact MSH/PID/PV1/PV2/OBR/OBX paths referenced by the 23 supplied HL7 dashboard queries. This is an initial compatibility projection, not proof of Prism parity.

### CCDA

CCDA XML is parsed separately with hash-pinned `defusedxml==0.7.1`. DTDs, entities, external references, excessive markup, unsafe depth, malformed XML, and non-`ClinicalDocument` roots are rejected with sanitized errors. One input XML document produces one deterministic `ccda/<document-id>.json` object and one `ccda-documents-v1` document.

The `CD` compatibility projection implements paths referenced by the nine supplied dashboards for patient role, custodian, medications, immunizations, problems, procedures, results, social history, and vital signs. Sections are selected by standard LOINC section codes, and dynamic strings receive `.keyword` multifields required by the supplied queries. Narrative section bodies are intentionally excluded; only queried structured-entry fields are projected. CDA timestamps, source checksums, participant provenance, section counts, and deterministic IDs are retained.

Not yet supported:

- CCDA XSD/Schematron or template-conformance validation; safe well-formed XML and supported structures are parsed leniently.
- Full HL7 field-name projection beyond fields required by the supplied queries.
- Formal HL7 message-profile or conformance validation; ingestion is intentionally lenient.
- Matched customer source/output parity testing; customer artifacts did not contain raw source records.
- Alarm notification actions such as SNS, email, Slack, or incident-management integration.

## Infrastructure

- Rotating customer-managed KMS key.
- Versioned private raw, parsed, and error S3 buckets with KMS encryption and retained deletion policies.
- Encrypted parse/index SQS queues with 14-day DLQs.
- Two Python 3.14 ARM64 Lambda functions with bounded concurrency, encrypted 30-day log groups, and reproducibly bundled, hash-pinned HL7/XML dependencies.
- Isolated no-NAT VPC with S3 gateway endpoint and rejected-traffic flow logs.
- One private OpenSearch Serverless `SEARCH` collection containing separate `hl7-messages-v1` and `ccda-documents-v1` indexes.
- Classic/non-NextGen Serverless operation, which does not scale to zero. No collection group is configured. Production enables standby replicas; development and staging disable standby replicas to reduce cost.
- Customer-managed KMS encryption, collection deletion protection, retained CloudFormation policies, and collection-scoped `aoss:APIAccessAll` for the indexer.
- An OpenSearch Serverless-managed VPC endpoint in isolated subnets. Its security group accepts HTTPS only from the indexer security group, and the network policy disables public access for both collection and Dashboards endpoints.
- A data access policy limited to creating, describing, and updating the two named indexes and writing documents to them.
- Queue-age, DLQ, and Lambda-error alarms. Provisioned-domain log publishing and cluster-red alarms do not apply to Serverless and are not synthesized.

OpenSearch Serverless manages capacity, shards, and refresh settings. The classic model retains a non-zero OCU floor rather than scaling to zero, so review current OCU, standby-replica, VPC endpoint, and storage pricing before deployment. Serverless search has an approximately 10-second refresh interval, automatic snapshots but no manual snapshots, and a different supported API/plugin surface from a provisioned domain.

By default, the collection and Dashboards endpoints are not public. Only the indexer has both network and data-plane access. Development can temporarily expose only the Dashboards endpoint by passing `enable_public_dashboard=true` and an exact same-account IAM role through `dashboard_principal_arn`; the collection API remains private, and the role receives only `DescribeIndex` and `ReadDocument` in this stack's data policy. Staging and production reject this option. The external role must separately have `aoss:APIAccessAll` on the collection ARN and `aoss:DashboardsAccessAll` on `arn:aws:aoss:<region>:<account>:dashboards/default`, typically through its IAM Identity Center permission set. Remove the contexts and redeploy after verification to restore private Dashboards access.

## Benchmark evidence

The complete local synthetic HL7 corpus was parsed without printing patient-like values:

- 6,473 batch files.
- 31,999 messages: 14,114 ADT, 3,684 MDM, 2,499 ORU, 8,081 RDE, and 3,621 VXU.
- 288,308 segments.
- Zero parse failures.
- Zero complete-document mismatches against the pre-migration parser, excluding the intentional parser-version change.
- 18.7 seconds total on the development machine (347 files/second).
- p95 5.43 ms and p99 7.04 ms per batch file.
- Maximum file size 36,305 bytes.
- Peak Python allocation observed with `tracemalloc`: 0.53 MiB.

These synthetic results support Lambda for the HL7 real-time path, but they do not establish customer production volume or Prism parity.

The complete local synthetic CCDA corpus was parsed without printing patient-like values:

- 5,273 `ClinicalDocument` XML files and zero parse failures.
- All documents projected patient/custodian metadata and seven queried structured section families when present.
- Input size: p50 1,286,304 bytes, p95 5,409,385 bytes, p99 15,947,192 bytes, maximum 50,399,038 bytes.
- Parse latency: p50 68.46 ms, p95 309.38 ms, p99 930.27 ms, maximum 3.55 seconds.
- Projected JSON size: p50 400,649 bytes, p95 1,903,303 bytes, p99 5,734,798 bytes, maximum 18,766,462 bytes.
- Two-worker validation completed in 306.5 seconds (17.2 files/second); aggregate worker time was 610.6 seconds.
- Maximum observed worker RSS was 748.5 MiB, below the parser Lambda's 2 GiB allocation.

These synthetic measurements support Lambda parsing for the current CCDA objects, including the largest supplied file, but production volume, customer structure variants, and Prism parity remain unproven.

The largest projected CCDA JSON document is 18,766,462 bytes. Compatibility of an individual document/request of that size with OpenSearch Serverless has not been established. Bulk chunking cannot divide one document, so this is an indexing release gate: verify an official Serverless limit and a non-PHI smoke document before production use, or change the model to a small CCDA summary plus separately indexed structured entries.

## Prerequisites and validation

- Python 3.12 or 3.13 for local development.
- Node.js 22 LTS (`nvm use` reads `.nvmrc`).
- `uv` 0.11 or later.
- AWS CDK CLI 2.x.

```bash
make install
make validate
```

`make validate` runs Ruff, strict mypy, pytest with branch coverage, cdk-nag, and CDK synthesis.

## Synthesis and deployment

Local synthesis does not require AWS credentials:

```bash
uv run cdk synth
```

Review changes against the deployed development stack before deployment:

```bash
uv run cdk diff \
  -c environment=dev \
  -c account=111122223333 \
  -c region=us-west-2 \
  --profile <aws-profile>
```

Deployment creates paid OpenSearch Serverless, Lambda, and VPC endpoint resources and can take significant time:

```bash
uv run cdk deploy \
  -c environment=dev \
  -c account=111122223333 \
  -c region=us-west-2 \
  --profile <aws-profile>
```

OpenSearch Service does not automatically migrate a provisioned domain into a Serverless collection. This stack change creates an empty collection; existing documents must be deliberately reindexed from an approved source after mappings and document-size compatibility are validated. Because the old domain has a retained deletion policy, a CloudFormation update can leave that domain running and billable after it is removed from the stack. Decommission it only through a separate, explicitly approved migration plan after reconciliation and rollback requirements are satisfied.

The parser event source starts consuming existing parse-queue messages as soon as deployment completes. Remove any intentionally retained alarm-test message first, or expect it to be processed. An invalid `.hl7` or `.xml` smoke file is acknowledged after a sanitized structured error is written to error S3.

## Post-deployment smoke test

1. Upload a small valid synthetic HL7 batch and a small valid synthetic CCDA document under non-PHI keys in the raw bucket.
2. Confirm the parse queue drains and its DLQ remains empty.
3. Confirm one `hl7/<document-id>.json` object per HL7 message and one `ccda/<document-id>.json` object per CCDA document in parsed S3.
4. Confirm the index queue drains and its DLQ remains empty.
5. Confirm parser/indexer Lambda `Errors` remain zero.
6. From an explicitly approved private client, confirm documents are counted in both `hl7-messages-v1` and `ccda-documents-v1` after allowing for the approximately 10-second Serverless refresh interval.
7. Inspect error S3 for sanitized failures; raw messages, XML bodies, and clinical values must not appear in logs or errors.

## Data and security rules

- Do not commit customer files or the full local Synthea corpus.
- Do not log clinical values, raw messages, document bodies, or patient identifiers.
- Do not place PHI in S3 keys, SQS attributes, tags, metrics, or alarm descriptions.
- Do not disable production termination protection or retained storage without explicit review.
