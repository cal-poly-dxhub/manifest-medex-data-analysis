# Manifest MedEx Data Quality Platform

A Python AWS CDK application for private, format-specific HL7 v2 and CCDA ingestion.

## Files

- `app.py` loads validated CDK configuration, enables cdk-nag, and creates the stack.
- `src/stack.py` defines storage, format queues, Lambda functions, isolated networking, Aurora PostgreSQL Serverless v2, OpenSearch Serverless, logs, and alarms.
- `src/parser.py` contains the compute-independent HL7 splitter, `python-hl7` adapter, deterministic identity, and query projection.
- `src/ccda_parser.py` securely parses CCDA XML into the separate query-driven `CD` projection.
- `src/document_handler.py` implements the shared read/parse, parsed-S3, OpenSearch, and Aurora workflow.
- `src/hl7_handler.py` and `src/ccda_handler.py` are thin format-specific Lambda entry points.
- `src/search_store.py` contains deterministic OpenSearch mappings, bulk indexing, and SigV4 transport.
- `src/metadata_store.py` contains the Aurora schema and retry-safe Data API upsert.
- `lambda-requirements.txt` hash-pins dependencies installed into the Lambda asset.
- `tests/` covers configuration, parsing, handlers, persistence, search, infrastructure, and cdk-nag.

There is no extra package folder inside `src`.

## Implemented v1 data flow

```text
versioned raw S3 bucket
├── incoming/hl7/*.hl7 or *.txt
│   └── EventBridge → HL7 SQS + DLQ → HL7 Lambda
│       ├── parse every message in the raw batch
│       ├── deterministic hl7/<document-id>.json in parsed S3
│       ├── deterministic _id in hl7-messages-v1
│       └── one Aurora metadata row per parsed message
└── incoming/ccda/*.xml
    └── EventBridge → CCDA SQS + DLQ → CCDA Lambda
        ├── parse one CCDA document
        ├── deterministic ccda/<document-id>.json in parsed S3
        ├── deterministic _id in ccda-documents-v1
        └── one Aurora metadata row per document
```

EventBridge routes by prefix. Each Lambda independently enforces its prefix and supported extensions. Objects outside `incoming/hl7/` and `incoming/ccda/` are not routed, so existing objects and previously uploaded bulk prefixes are not backfilled.

For each SQS record, the format Lambda performs these stages in order:

1. Read the exact raw S3 version and parse it.
2. Write every deterministic parsed JSON object and capture the `VersionId` returned by S3.
3. Index every parsed document using its deterministic document ID.
4. Batch-upsert all metadata rows into Aurora.
5. Return success only after all three durable destinations succeed.

Every failure is returned through SQS partial batch failure reporting. This includes malformed input, oversized input, permanent OpenSearch rejection, and Aurora failure. SQS retries the record and moves it to the format-specific DLQ after five receives. A sanitized error object is attempted on every failure, but diagnostics do not contain S3 keys, document IDs, patient identifiers, clinical values, SQL parameters, or raw OpenSearch responses.

Retries converge safely:

- Parsed S3 uses deterministic keys; because the bucket is versioned, a retry can create a new version and the successful attempt stores that returned version ID in Aurora.
- OpenSearch uses deterministic `_id` values and overwrites the same logical document.
- Aurora uses the document ID primary key and `INSERT ... ON CONFLICT DO UPDATE`.

## Aurora metadata

The private Aurora PostgreSQL Serverless v2 cluster has one writer, Data API enabled, and 0.5–2 ACU capacity. There is no reader, RDS Proxy, direct Lambda PostgreSQL connection, or Lambda-to-cluster port 5432 rule.

The schema is intentionally limited to document-location metadata:

```sql
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
);
```

`document_time` is normalized from HL7 MSH-7 or CCDA `effectiveTime`; `ingested_time` is the S3 EventBridge event time. Locations are durable `s3://bucket/key` references, not presigned URLs. The schema deliberately has no `message_type`, `created_at`, or `last_updated_at` columns.

The first metadata write in each warm Lambda execution environment safely runs `CREATE TABLE IF NOT EXISTS`; subsequent writes from that environment skip it. HL7 messages from one raw batch are sent in one `BatchExecuteStatement`, and CCDA uses a batch of one. Upserts update every stored metadata field on conflict.

The generated database credential is KMS-encrypted and retained. Automatic rotation is intentionally not synthesized for this v1 Data API-only demo because Secrets Manager's database rotation Lambda would require direct database networking and port 5432 access. Rotate the retained credential through an explicitly approved maintenance operation before its required rotation deadline; this exception is narrowly suppressed in cdk-nag.

## Parsing behavior

### HL7 v2

This release uses hash-pinned `hl7==0.4.5` (`python-hl7`) for lenient segment, field, repetition, component, subcomponent, and dynamic-delimiter parsing. Application code retains message-boundary handling, safe standard escape decoding, metadata, timestamps, deterministic identity, and the dashboard projection. Parser output identifies this implementation as version `0.2.0`.

Supported UTF-8 HL7 behavior includes:

- CR, LF, or CRLF segment separators and optional MLLP framing.
- Multiple concatenated messages split on `MSH` segments.
- Dynamic delimiters from `MSH`.
- ADT, MDM, ORU, RDE, and VXU metadata and segment counting.
- Unknown and locally defined segments.
- Deterministic source checksum, message ordinal, and document ID.
- HL7 timestamp normalization; timestamps without an offset are interpreted as UTC.
- A 50 MiB raw-object limit.

The indexed `ROOT` projection implements the MSH/PID/PV1/PV2/OBR/OBX paths referenced by the supplied HL7 dashboard queries. It is an initial compatibility projection, not proof of Prism parity.

### CCDA

CCDA XML is parsed with hash-pinned `defusedxml==0.7.1`. DTDs, entities, external references, excessive markup, unsafe depth, malformed XML, and non-`ClinicalDocument` roots are rejected. One XML input produces one parsed object, one OpenSearch document, and one Aurora row.

The `CD` compatibility projection implements supplied dashboard paths for patient role, custodian, medications, immunizations, problems, procedures, results, social history, and vital signs. Sections are selected by standard LOINC section codes. Narrative section bodies are excluded; only queried structured-entry fields are projected.

Not yet supported:

- CCDA XSD/Schematron or template-conformance validation.
- Full HL7 field-name projection beyond supplied query requirements.
- Formal HL7 message-profile or conformance validation.
- Matched customer source/output parity testing.
- Alarm notification actions such as SNS, email, Slack, or incident-management integration.

## Infrastructure and security

- Rotating customer-managed KMS key.
- Versioned private raw, parsed, and error S3 buckets with KMS encryption and retained deletion policies.
- Separate KMS-encrypted HL7 and CCDA SQS queues, each with a 14-day DLQ and max receive count of five.
- Two Python 3.14 ARM64 Lambda functions with bounded concurrency, 2 GiB memory, 10-minute timeout, encrypted 30-day log groups, and reproducibly bundled dependencies.
- An isolated no-NAT VPC with an S3 gateway endpoint and rejected-traffic flow logs.
- One private Aurora PostgreSQL 16.6 Serverless v2 writer with Data API, KMS storage encryption, retained storage, seven-day non-production backups, and 35-day production backups.
- A private `rds-data` interface endpoint with private DNS. Its endpoint policy permits only the two ingestion roles and only `ExecuteStatement`/`BatchExecuteStatement` on the metadata cluster. Its security group accepts HTTPS only from the ingestion Lambda security group.
- No public Aurora instance, RDS Proxy, read replica, NAT gateway, or port 5432 ingress.
- One private OpenSearch Serverless `SEARCH` collection with `hl7-messages-v1` and `ccda-documents-v1`.
- A private OpenSearch Serverless VPC endpoint accepting HTTPS only from the ingestion Lambda security group.
- Exact data access for both ingestion roles to create/describe/update the two named indexes and write documents. Collection IAM access is scoped to the collection ARN.
- Production Aurora deletion protection, stack termination protection, and OpenSearch standby replicas. Storage and the collection remain retained.
- Separate queue-age, DLQ, and Lambda-error alarms for HL7 and CCDA.

OpenSearch Serverless uses the classic/non-NextGen model here. It does not scale to zero; production enables standby replicas while development and staging disable them. Review current OCU, endpoint, Aurora ACU, storage, I/O, secret, and backup pricing before deployment.

By default, collection and Dashboards endpoints are private. Development can temporarily expose only Dashboards with `enable_public_dashboard=true` and an exact same-account role in `dashboard_principal_arn`; the collection API remains private and the role receives read-only index data access. Staging and production reject this option.

## Benchmark evidence

The local synthetic HL7 corpus previously parsed successfully:

- 6,473 batch files and 31,999 messages.
- 288,308 segments and zero parse failures.
- 18.7 seconds total on the development machine.
- Maximum file size 36,305 bytes and peak observed Python allocation 0.53 MiB.

The local synthetic CCDA corpus previously parsed successfully:

- 5,273 documents and zero parse failures.
- Input p50 1,286,304 bytes; p99 15,947,192 bytes; maximum 50,399,038 bytes.
- Projected JSON p50 400,649 bytes; p99 5,734,798 bytes; maximum 18,766,462 bytes.
- Parse p50 68.46 ms; p99 930.27 ms; maximum 3.55 seconds.
- Maximum observed worker RSS 748.5 MiB.

These synthetic results do not establish production volume, customer structure coverage, or Prism parity. A large individual CCDA document can exceed an OpenSearch request limit even when bulk chunking sends it alone. Such a failure intentionally remains retryable until the CCDA DLQ; validate the official limit with non-PHI data before production or change the indexing model.

## Prerequisites and validation

- Python 3.12 or 3.13.
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

Deployment creates paid Aurora and interface endpoint resources and replaces the prior shared parser/index queue architecture. It must be explicitly approved; this repository change does not deploy anything.

```bash
uv run cdk deploy \
  -c environment=dev \
  -c account=111122223333 \
  -c region=us-west-2 \
  --profile <aws-profile>
```

Only objects uploaded after deployment under the two `incoming/` prefixes are processed. There is no Aurora backfill. Existing objects under other prefixes, including prior synthetic bulk prefixes, remain untouched.

## Post-deployment smoke test

1. Upload a small valid synthetic HL7 file under `incoming/hl7/` and a small valid synthetic CCDA document under `incoming/ccda/`, using non-PHI keys.
2. Confirm both queues drain and both DLQs remain empty.
3. Confirm one `hl7/<document-id>.json` version per HL7 message and one `ccda/<document-id>.json` version per CCDA document.
4. Confirm deterministic documents are present in both OpenSearch indexes after the Serverless refresh interval.
5. Through an approved Data API client, confirm one metadata row per parsed HL7 message and one per CCDA document, including raw and parsed version IDs.
6. Confirm both Lambda `Errors` metrics remain zero and no sanitized error object was written.
7. Verify logs and diagnostics contain no raw messages, XML, S3 keys, document IDs, patient identifiers, clinical values, SQL parameters, or raw backend responses.

## Data and security rules

- Do not commit customer files or the full local synthetic corpus.
- Do not log clinical values, raw messages, XML bodies, S3 keys, document IDs, or patient identifiers.
- Do not place PHI in S3 keys, SQS attributes, tags, metrics, alarms, or diagnostics.
- Do not disable production termination/deletion protection or retained storage without explicit review.
- Do not deploy, backfill, or rotate credentials without explicit approval.

# Collaboration

Thanks for your interest in our solution. Having specific examples of replication and usage allows us to continue to grow and scale our work. If you clone or use this repository, kindly shoot us a quick email to let us know you are interested in this work!

# Disclaimers

**Customers are responsible for making their own independent assessment of the information in this document.**

**This document:**
(a) is for informational purposes only, (b) references AWS product offerings and practices, which are subject to change without notice, (c) does not create any commitments or assurances from AWS and its affiliates, suppliers or licensors. AWS products or services are provided "as is" without warranties, representations, or conditions of any kind, whether express or implied. The responsibilities and liabilities of AWS to its customers are controlled by AWS agreements, and this document is not part of, nor does it modify, any agreement between AWS and its customers, and (d) is not to be considered a recommendation or viewpoint of AWS.

Additionally, you are solely responsible for testing, security and optimizing all code and assets on GitHub repo, and all such code and assets should be considered: (a) as-is and without warranties or representations of any kind, (b) not suitable for production environments, or on production or other critical data, and (c) to include shortcuts in order to support rapid prototyping such as, but not limited to, relaxed authentication and authorization and a lack of strict adherence to security best practices.
