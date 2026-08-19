# Manifest MedEx Data Quality Platform

A private, serverless AWS ingestion platform for parsing, indexing, and tracking HL7 v2 messages and CCDA documents.

| Section | Description |
| --- | --- |
| [Overview](#overview) | Purpose, scope, and current limitations |
| [Architecture](#architecture) | Format-specific ingestion paths and durable destinations |
| [Description](#description) | Technology, repository structure, parsing, and security |
| [Deployment](#deployment) | Prerequisites, configuration, validation, synthesis, and deployment |
| [Usage](#usage) | Input conventions, outputs, and post-deployment checks |
| [Development tools](#development-tools) | HL7 dictionary generation and query-parity validation |
| [Operations and troubleshooting](#operations-and-troubleshooting) | Retries, alarms, costs, and common failures |
| [Data and security rules](#data-and-security-rules) | Requirements for clinical and identifying data |
| [Support](#support) | How to report issues safely |
| [License](#license) | Current licensing status |
| [Disclaimers](#disclaimers) | Evaluation and production-use caveats |

# Overview

Manifest MedEx Data Quality Platform is an AWS CDK application that creates separate HL7 v2 and CCDA ingestion lanes. New objects in a private, versioned Amazon S3 raw-data bucket are routed through Amazon EventBridge and format-specific Amazon SQS queues to two AWS Lambda functions. Each function parses its input, writes deterministic JSON to parsed S3, indexes a query-oriented projection in Amazon OpenSearch Serverless, and upserts document-location metadata through the Amazon Aurora PostgreSQL Data API.

The design keeps the raw object as the complete source of truth while exposing a deliberately bounded search projection for supplied dashboard requirements. It emphasizes private networking, encryption, least-privilege access, deterministic retries, retained storage, and diagnostics that do not reveal clinical content or identifiers.

This repository is an implementation prototype. It does not establish HL7 profile conformance, CCDA template conformance, customer-data parity, production capacity, or regulatory compliance. Validate those concerns independently before handling production or regulated data.

# Architecture Diagram

![Architecture Diagram](doc/arch_diagram.png)

# Description

## Technology stack

| Category | Technology | Purpose |
| --- | --- | --- |
| Infrastructure | [AWS CDK v2](https://docs.aws.amazon.com/cdk/v2/guide/home.html), Python | Defines and synthesizes the complete platform |
| Object storage | [Amazon S3](https://aws.amazon.com/s3/) | Versioned raw, parsed, error, and access-log storage |
| Routing | [Amazon EventBridge](https://aws.amazon.com/eventbridge/) | Routes new objects by format prefix |
| Buffering | [Amazon SQS](https://aws.amazon.com/sqs/) | Isolated processing queues and dead-letter queues |
| Compute | [AWS Lambda](https://aws.amazon.com/lambda/) | Python 3.14 ARM64 HL7 and CCDA processing |
| Search | [Amazon OpenSearch Serverless](https://aws.amazon.com/opensearch-service/features/serverless/) | Query-oriented HL7 and CCDA indexes |
| Metadata | [Amazon Aurora PostgreSQL](https://aws.amazon.com/rds/aurora/) 16.8 | Document-location metadata through the Data API |
| Security | AWS KMS, IAM, VPC endpoints | Encryption, least privilege, and private service access |
| Monitoring | Amazon CloudWatch | Encrypted logs, queue-age alarms, DLQ alarms, and Lambda-error alarms |
| Parsing | `hl7==0.4.5`, `defusedxml==0.7.1` | Lenient HL7 parsing and hardened XML parsing |
| Development | `uv`, Ruff, mypy, pytest, cdk-nag | Reproducible dependencies and automated validation |

Runtime dependencies are hash-pinned in `lambda-requirements.txt`. CDK dependencies and development tools are exactly pinned in `pyproject.toml` and `uv.lock`.

## Project structure

```text
.
├── app.py                         # CDK application entry point and cdk-nag registration
├── cdk.json                       # Default CDK context
├── lambda-requirements.txt        # Hash-pinned Lambda dependencies
├── Makefile                       # Installation, validation, and HL7 tool targets
├── pyproject.toml                 # Project, test, lint, and type-check configuration
├── src/
│   ├── config.py                  # Validated deployment context and safety constraints
│   ├── stack.py                   # AWS infrastructure and Lambda asset bundling
│   ├── parser.py                  # HL7 splitting, parsing, identity, and ROOT projection
│   ├── ccda_parser.py             # Secure CCDA parsing and CD projection
│   ├── document_handler.py        # Shared S3, parse, search, and metadata workflow
│   ├── hl7_handler.py             # HL7 Lambda entry point
│   ├── ccda_handler.py            # CCDA Lambda entry point
│   ├── search_store.py            # OpenSearch mappings, bulk writes, and SigV4 transport
│   └── metadata_store.py          # Aurora schema and retry-safe Data API upserts
├── tests/                         # Unit, integration-style, CDK, and cdk-nag tests
└── tools/
    ├── generate_hl7_dictionary.py # Development-only HL7 v2.6 label generator
    ├── probe_hl7_versions.py      # Cross-version datatype comparison
    └── validate_against_customer_queries.py
                                      # Delivered query-path parity check
```

The Lambda artifact contains the runtime `src` modules and exact runtime dependencies. Tests, tools, caches, `src/stack.py`, and `src/config.py` are excluded from both local and Docker fallback bundles.

## HL7 v2 processing

The HL7 parser supports:

- UTF-8 files with CR, LF, or CRLF segment separators.
- Optional MLLP framing.
- Multiple concatenated messages split at `MSH` segments.
- Delimiters declared dynamically in `MSH`.
- Standard safe escape decoding.
- Message-type metadata extraction, including ADT, MDM, ORU, RDE, and VXU.
- Unknown and locally defined segments.
- Deterministic source checksums, message ordinals, and document IDs.
- HL7 timestamp normalization; timestamps without offsets are interpreted as UTC.
- A 50 MiB raw-object limit.

The indexed `ROOT` projection currently exposes 26 query-oriented MSH, PID, PV1, PV2, OBR, OBX, and NK1 field paths. This is an initial compatibility projection, not a complete HL7 representation. Fields outside it are not retained in parsed JSON or OpenSearch. `segmentCounts` records occurrence counts but not omitted values, so extending the projection requires reprocessing from raw S3.

Parser output identifies the implementation as version `0.3.0`.

## CCDA processing

CCDA XML is parsed with `defusedxml`. The parser rejects DTDs, entities, external references, excessive markup, unsafe nesting depth, malformed XML, and roots other than `ClinicalDocument`.

The indexed `CD` projection covers structured fields used by supplied dashboard queries for:

- Patient role and custodian
- Medications and immunizations
- Problems and procedures
- Results and vital signs
- Social history

Sections are selected by standard LOINC section codes. Narrative section bodies are intentionally excluded from the projection.

## Metadata contract

Aurora stores document identity, source format, normalized document time, ingestion time, and durable S3 locations:

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

`document_time` comes from HL7 MSH-7 or CCDA `effectiveTime`; `ingested_time` comes from the S3 EventBridge event. Stored locations are `s3://bucket/key` references rather than expiring presigned URLs.

## Infrastructure and security

The stack creates:

- One rotating customer-managed KMS key.
- Private, versioned raw, parsed, and error S3 buckets with KMS encryption and retained deletion policies.
- A separate access-log bucket.
- Separate KMS-encrypted HL7 and CCDA queues, each with a 14-day DLQ and a maximum receive count of five.
- Two Python 3.14 ARM64 Lambda functions with 2 GiB memory, 10-minute timeouts, encrypted 30-day logs, and bounded concurrency.
- An isolated, no-NAT VPC with rejected-traffic flow logs.
- An S3 gateway endpoint plus private OpenSearch Serverless and RDS Data API interface endpoints.
- One private Aurora PostgreSQL 16.8 Serverless v2 writer with 0.5–2 ACU, Data API, IAM database authentication, KMS encryption, and retained storage.
- One private OpenSearch Serverless `SEARCH` collection with `hl7-messages-v1` and `ccda-documents-v1`.
- Queue-age, dead-letter-queue, and Lambda-error alarms for both formats.

There is no public Aurora instance, RDS Proxy, reader instance, NAT gateway, direct Lambda PostgreSQL connection, or port 5432 ingress rule.

Production enables stack termination protection, Aurora deletion protection, 35-day Aurora backups, and OpenSearch standby replicas. Non-production uses seven-day Aurora backups. Stored data and the OpenSearch collection remain retained when the stack is deleted; plan an explicitly approved retention or disposal process.

# Deployment

Deployment creates paid AWS resources and changes cloud infrastructure. Run `cdk diff`, review security and retention behavior, and obtain the required approval before deploying. The commands below are instructions only; this README update does not deploy anything.

## Prerequisites

1. An AWS account and a least-privilege deployment role.
2. [Python](https://www.python.org/downloads/) 3.12 or 3.13 for development.
3. [uv](https://docs.astral.sh/uv/) 0.11 or later.
4. [Node.js](https://nodejs.org/) 22 LTS; `nvm use` reads `.nvmrc`.
5. [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/cli.html) 2.x.
6. [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) configured with an approved profile for diff or deployment.
7. Docker only when exact local runtime dependencies are unavailable and CDK must use fallback bundling.

## Install dependencies

From the repository root:

```bash
nvm use
make install
```

`make install` runs `uv sync --all-groups --frozen`, so installation fails rather than silently changing the lockfile.

## Configuration reference

Configuration is supplied through CDK context. Defaults are defined in `cdk.json` and validated by `src/config.py`.

| Context key | Description | Default |
| --- | --- | --- |
| `project_name` | Prefix for stack and resource names; lowercase letters, numbers, and hyphens | `manifest-medex-data-quality` |
| `environment` | Deployment stage: `dev`, `staging`, or `prod` | `dev` |
| `account` | Twelve-digit deployment account; must be supplied with `region` | unset |
| `region` | AWS deployment region; must be supplied with `account` | unset |
| `enable_cdk_nag` | Enable AWS Solutions checks during synthesis | `true` |
| `termination_protection` | Protect the CloudFormation stack; required in production | `true` in production, otherwise `false` |
| `enable_public_dashboard` | Temporarily allow public network access to Dashboards in development only | `false` |
| `dashboard_principal_arn` | Exact same-account IAM role receiving read-only dashboard data access | unset |

If neither `account` nor `region` is supplied, local synthesis is environment-agnostic and does not require AWS credentials. If either is supplied, both are required.

OpenSearch collection and Dashboard endpoints are private by default. The public Dashboard exception is restricted to development, requires an exact same-account IAM role, leaves the collection API private, and grants read-only index access. Staging and production reject that option.

## Validate and synthesize

Run the complete repository validation before reviewing a deployment:

```bash
uv lock --check
make validate
```

`make validate` performs Ruff formatting and lint checks, strict mypy, pytest with branch coverage, cdk-nag, and CDK synthesis.

Synthesis alone does not require AWS credentials:

```bash
uv run cdk synth
```

## Bootstrap, review, and deploy

Bootstrap each target account and region once:

```bash
uv run cdk bootstrap \
  -c account=111122223333 \
  -c region=us-west-2 \
  --profile <approved-aws-profile>
```

Review the proposed development changes:

```bash
uv run cdk diff \
  -c environment=dev \
  -c account=111122223333 \
  -c region=us-west-2 \
  --profile <approved-aws-profile>
```

After review and explicit approval, deploy:

```bash
uv run cdk deploy \
  -c environment=dev \
  -c account=111122223333 \
  -c region=us-west-2 \
  --profile <approved-aws-profile>
```

Do not use production credentials or disable termination/deletion protections merely to simplify deployment.

## Cost considerations

This stack is not a free-tier architecture. Major recurring cost drivers include:

- OpenSearch Serverless OCUs and managed storage; the classic model used here does not scale to zero.
- Aurora Serverless v2 ACUs, storage, backup retention, and I/O.
- Two interface VPC endpoints across the configured Availability Zones.
- Lambda duration and concurrency.
- SQS, EventBridge, S3 storage/versioning, KMS requests, and CloudWatch logs.

Production enables OpenSearch standby replicas and longer Aurora backup retention, increasing cost. Use the [AWS Pricing Calculator](https://calculator.aws/) with the target region, traffic, retention, and data-volume assumptions before deployment.

# Usage

## Upload inputs

Only objects created after deployment under these prefixes are routed:

| Format | S3 key pattern | One input produces |
| --- | --- | --- |
| HL7 v2 | `incoming/hl7/*.hl7` or `incoming/hl7/*.txt` | One parsed document and metadata row per message in the file |
| CCDA | `incoming/ccda/*.xml` | One parsed document and metadata row |

Use non-identifying object keys. Do not put patient names, medical record numbers, or other identifiers in S3 keys.

Example uploads with synthetic, non-PHI data:

```bash
aws s3 cp ./synthetic-message.hl7 \
  s3://<raw-bucket>/incoming/hl7/synthetic-message.hl7 \
  --profile <approved-aws-profile>

aws s3 cp ./synthetic-document.xml \
  s3://<raw-bucket>/incoming/ccda/synthetic-document.xml \
  --profile <approved-aws-profile>
```

## Verify processing

After uploading safe synthetic inputs:

1. Confirm both processing queues drain and both DLQs remain empty.
2. Confirm parsed S3 contains `hl7/<document-id>.json` for each HL7 message and `ccda/<document-id>.json` for each CCDA document.
3. Confirm deterministic documents appear in `hl7-messages-v1` and `ccda-documents-v1` after the OpenSearch refresh interval.
4. Through an approved Data API client, confirm one metadata row per logical document, including raw and parsed S3 version IDs.
5. Confirm both Lambda `Errors` metrics remain zero.
6. Confirm logs and error objects contain no raw messages, XML bodies, S3 keys, document IDs, patient identifiers, clinical values, SQL parameters, or raw backend responses.

# Development tools

The `tools/` directory is development-only and excluded from Lambda assets.

## Generate the HL7 naming dictionary

```bash
make hl7-dictionary
```

This command derives a compact HL7 v2.6 field/component naming table from `hl7apy` and writes `tools/hl7_dictionary.json`. The JSON is ignored by Git, not consumed by the runtime parser, and safe to delete and regenerate.

HL7 v2.6 is used because the delivered dashboard paths expect CWE components for OBR-4, OBX-3, and PV2-3; these fields use CE in HL7 v2.5/2.5.1. Inspect the cross-version comparison with:

```bash
uv run python tools/probe_hl7_versions.py
```

## Validate delivered query paths

By default, parity validation reads the sibling project directory:

```text
../customer_delivery/PrismInfoFor AWS-HL7 DSG Dashboard
```

Run:

```bash
make hl7-parity
```

Override the location without editing source:

```bash
make hl7-parity HL7_QUERY_DIR="/path/to/HL7-dashboard-query-files"
```

The validator fails if the directory is absent, contains no `.txt` files, has a field-label mismatch, or has a required composite-component mismatch. It builds the dictionary in memory on every run, so it cannot pass against stale generated JSON.

These query artifacts may be customer-sensitive and are intentionally outside the Git repository. Do not add them to source control or paste their contents into logs, issues, or pull requests.

# Operations and troubleshooting

## Retry and failure behavior

Each format queue uses partial batch failure reporting and a batch size of one. Malformed or oversized input, permanent OpenSearch rejection, and Aurora failures are retried by SQS. After five receives, the record moves to its format-specific DLQ.

A sanitized error object is attempted for every failure, but failure reporting is not allowed to expose raw content, keys, document IDs, identifiers, clinical values, SQL parameters, or raw OpenSearch responses.

## Monitoring

Review these signals together:

- HL7 and CCDA Lambda `Errors` metrics.
- Queue age and visible-message counts.
- DLQ visible-message counts.
- KMS-encrypted Lambda and VPC flow logs.
- Parsed S3 object versions.
- OpenSearch indexing results.
- Aurora Data API errors and metadata rows.

The stack creates six CloudWatch alarms: queue age, DLQ content, and Lambda errors for each format. Alarm notification actions are not configured; connect them to an approved notification or incident-management path before production use.

## Common problems

### Local synthesis uses Docker unexpectedly

Local bundling requires installed `hl7==0.4.5` and `defusedxml==0.7.1` distributions. Run `make install`. If exact packages are unavailable, CDK falls back to the Python 3.14 ARM64 Docker bundling image.

### Query parity reports a missing directory or zero files

Pass the actual artifact location explicitly:

```bash
make hl7-parity HL7_QUERY_DIR="/absolute/path/to/query-directory"
```

The directory must contain at least one `.txt` query file. An empty scan is an error, not a successful validation.

### A valid object is not processed

Confirm that:

- The object was created after deployment.
- The key starts with `incoming/hl7/` or `incoming/ccda/`.
- HL7 uses `.hl7` or `.txt`; CCDA uses `.xml`.
- The EventBridge rule target and corresponding SQS queue are healthy.

### OpenSearch indexing fails for one large document

The application limits raw objects to 50 MiB and bulk requests to approximately 5 MiB, but one projected document may still exceed an OpenSearch request limit when sent alone. Such a record remains retryable and eventually reaches the DLQ. Test limits with synthetic data and revise the indexing model before production if needed.

### Deployment configuration is rejected

`src/config.py` intentionally rejects unsafe combinations, including:

- Production without termination protection.
- Only one of `account` and `region`.
- Public Dashboards outside development.
- Public Dashboards without an explicit role.
- A Dashboard role from a different account.

Correct the context values rather than bypassing validation.

## Known limitations

- The HL7 `ROOT` projection is not a complete HL7 message representation.
- The CCDA `CD` projection is not a complete clinical-document representation.
- No HL7 message-profile or conformance validation is implemented.
- No CCDA XSD, Schematron, or template-conformance validation is implemented.
- Matched customer source/output parity testing has not been established.
- Existing raw objects are not backfilled automatically.
- Alarm notification actions are not configured.
- Aurora credential rotation requires an explicitly approved maintenance process.
- Capacity and cost have not been validated against production traffic.

# Data and security rules

- Do not commit customer files, clinical data, identifiers, credentials, certificates, or full local synthetic corpora.
- Do not log raw HL7, XML, clinical values, S3 keys, document IDs, patient identifiers, SQL parameters, or raw backend responses.
- Do not place PHI in S3 keys, SQS attributes, tags, metrics, alarms, or diagnostics.
- Use synthetic, non-PHI data for development, tests, parity demonstrations, and smoke checks.
- Use least-privilege AWS credentials and approved profiles.
- Do not disable production termination protection, deletion protection, encryption, versioning, or retained storage without explicit review.
- Do not deploy, backfill, delete retained data, or rotate credentials without explicit approval and a rollback/recovery plan.

# Support

Use the repository's approved issue or code-review channel for defects and feature requests. Include reproducible synthetic inputs, expected behavior, validation output, and the affected environment. Never attach customer query files, PHI, credentials, account-specific secrets, raw clinical payloads, or identifying logs.

For deployment incidents, engage the service owner and the organization's security/operations process rather than posting sensitive details publicly.

# License

This repository does not currently include a `LICENSE` file. Do not assume permission to copy, redistribute, or use the code outside the terms supplied by the repository owner. Add an organization-approved license before publishing the project as open source.

# Disclaimers

Customers and users are responsible for making their own independent assessment of this implementation, including its security, cost, reliability, regulatory suitability, and fitness for a particular purpose.

This project is provided for informational and prototyping purposes. AWS services and practices can change without notice. The implementation creates no commitments or assurances from AWS or its affiliates, suppliers, or licensors. AWS products and services are governed by their applicable agreements.

Treat all code and assets as provided as-is, without warranties or representations of any kind. They are not production-ready by default and may contain deliberate prototype tradeoffs. Test, secure, monitor, operate, and optimize the system independently before using it with production, critical, regulated, or clinical data.
