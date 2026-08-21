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

Manifest MedEx Data Quality Platform is an AWS CDK application that creates separate HL7 v2 and CCDA ingestion lanes plus an authenticated, same-origin clinical-message explorer. New objects in a private, versioned Amazon S3 raw-data bucket are routed through Amazon EventBridge and format-specific Amazon SQS queues to two ingestion AWS Lambda functions. Each function parses its input, writes deterministic JSON to parsed S3, indexes a query-oriented projection in Amazon OpenSearch Serverless, and upserts document-location metadata through the Amazon Aurora PostgreSQL Data API. A third isolated Lambda serves Cognito-authorized metadata and bounded body reads to a React frontend hosted in private S3 behind CloudFront Origin Access Control.

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
| Compute | [AWS Lambda](https://aws.amazon.com/lambda/) | Python 3.14 ARM64 ingestion and authenticated explorer API |
| Web and API | React, Vite, TypeScript, CloudFront, HTTP API | Same-origin clinical-message explorer and API proxy |
| Identity | Amazon Cognito | Hosted UI Authorization Code with PKCE and JWT route authorization |
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
│   ├── explorer_handler.py        # Authenticated list, detail, and bounded body API
│   ├── search_store.py            # OpenSearch mappings, bulk writes, and SigV4 transport
│   └── metadata_store.py          # Aurora schema, indexes, and Data API upserts
├── web/                           # React/Vite/TypeScript authenticated explorer
│   ├── src/                       # OIDC, API client, query, and UI source
│   ├── public/config.json         # Non-secret local runtime-config placeholder
│   ├── package.json               # Exact frontend dependency pins
│   └── package-lock.json          # Reproducible npm dependency graph
├── tests/                         # Unit, integration-style, CDK, and cdk-nag tests
└── tools/
    ├── generate_hl7_dictionary.py # Development-only HL7 v2.6 label generator
    ├── probe_hl7_versions.py      # Cross-version datatype comparison
    └── validate_against_customer_queries.py
                                      # Delivered query-path parity check
```

The Lambda artifact contains the runtime `src` modules and exact runtime dependencies. Tests, tools, caches, all of `web/`, `src/stack.py`, and `src/config.py` are excluded from both local and Docker fallback bundles.

## HL7 v2 processing

The HL7 parser supports:

- UTF-8 files with CR, LF, or CRLF segment separators.
- Optional MLLP framing.
- Multiple concatenated messages split at `MSH` segments.
- Delimiters declared dynamically in `MSH`.
- Standard safe escape decoding.
- Message-type metadata extraction, including ADT, MDM, ORU, RDE, and VXU.
- Unknown and locally defined segments.
- Whole-object provenance checksums and deterministic per-message content IDs.
- HL7 timestamp normalization; timestamps without offsets are interpreted as UTC.
- A 50 MiB raw-object limit.

The indexed `ROOT` projection currently exposes 26 query-oriented MSH, PID, PV1, PV2, OBR, OBX, and NK1 field paths. This is an initial compatibility projection, not a complete HL7 representation. Fields outside it are not retained in parsed JSON or OpenSearch. `segmentCounts` records occurrence counts but not omitted values, so extending the projection requires reprocessing from raw S3.

Parser output identifies the implementation as version `0.4.0`. HL7 document IDs are `SHA-256(bucket + key + SHA-256(normalized message))`; exact duplicate messages under one S3 key collapse to the first occurrence and retain its `messageOrdinal`. S3 version IDs and ETags remain provenance metadata but do not affect HL7 identity.

## CCDA processing

CCDA XML is parsed with `defusedxml`. The parser rejects DTDs, entities, external references, excessive markup, unsafe nesting depth, malformed XML, and roots other than `ClinicalDocument`.

The indexed `CD` projection covers structured fields used by supplied dashboard queries for:

- Patient role and custodian
- Medications and immunizations
- Problems and procedures
- Results and vital signs
- Social history

Sections are selected by standard LOINC section codes. Narrative section bodies are intentionally excluded from the projection.

CCDA parser output identifies the implementation as version `0.2.0`. CCDA document IDs are `SHA-256(bucket + key + SHA-256(exact raw XML bytes))`. Identical XML bytes under one S3 key resolve to the same logical document across source versions; byte-level formatting changes create a new ID. S3 version IDs and ETags remain provenance metadata but do not affect CCDA identity.

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

Newest-first explorer reads use keyset pagination over `(ingested_time, document_id)` with no `OFFSET`. Each list request also runs an exact `COUNT(*)` over the active ingestion-time/source-format filters so the UI can display the total result count and total pages. Exact counts can add latency on very large ranges even with the indexes below. `_ensure_schema()` creates these idempotent indexes for a new or empty table:

```sql
CREATE INDEX IF NOT EXISTS document_metadata_ingested_document_idx
ON document_metadata (ingested_time DESC, document_id DESC);

CREATE INDEX IF NOT EXISTS document_metadata_format_ingested_document_idx
ON document_metadata (source_format, ingested_time DESC, document_id DESC);
```

For an existing production table, normal `CREATE INDEX` can block writes. Run the equivalent `CREATE INDEX CONCURRENTLY IF NOT EXISTS` statements as a separately reviewed, one-off maintenance operation instead. `CONCURRENTLY` is intentionally absent from Lambda initialization because PostgreSQL does not permit it inside the Data API's implicit transaction.

## Infrastructure and security

The stack creates:

- One rotating customer-managed KMS key.
- Private, versioned raw, parsed, and error S3 buckets with KMS encryption and retained deletion policies.
- A private, versioned frontend S3 bucket served only through CloudFront Origin Access Control.
- A separate access-log bucket.
- Separate KMS-encrypted HL7 and CCDA queues, each with a 14-day DLQ and a maximum receive count of five.
- Two Python 3.14 ARM64 ingestion Lambda functions with 2 GiB memory, 10-minute timeouts, encrypted 30-day logs, and bounded concurrency.
- One Python 3.14 ARM64 explorer Lambda in isolated subnets with read-only object access, Data API `ExecuteStatement`, and 4 MiB response caps.
- Cognito Hosted UI, one public PKCE app client, and a Cognito-authorized HTTP API with no default stage.
- One CloudFront distribution using private S3 OAC for static assets and an uncached `/api/*` behavior for HTTP API.
- An isolated, no-NAT VPC with rejected-traffic flow logs.
- An S3 gateway endpoint plus private OpenSearch Serverless and RDS Data API interface endpoints.
- One private Aurora PostgreSQL 16.8 Serverless v2 writer with 0.5–2 ACU, Data API, IAM database authentication, KMS encryption, and retained storage.
- One private OpenSearch Serverless `SEARCH` collection with `hl7-messages-v1` and `ccda-documents-v1`.
- Queue-age, dead-letter-queue, and Lambda-error alarms for both formats.

There is no public Aurora instance, RDS Proxy, reader instance, NAT gateway, direct Lambda PostgreSQL connection, or port 5432 ingress rule.

Production enables stack termination protection, Aurora deletion protection, 35-day Aurora backups, and OpenSearch standby replicas. Non-production uses seven-day Aurora backups. Stored data and the OpenSearch collection remain retained when the stack is deleted; plan an explicitly approved retention or disposal process.

## Authenticated message explorer

The stack also provides a same-origin, authenticated browser explorer:

```text
CloudFront
├── default behavior → private frontend S3 bucket through Origin Access Control
└── /api/*           → HTTP API api stage → explorer Lambda in isolated subnets
                                             ├── RDS Data API endpoint
                                             └── S3 gateway endpoint
```

The React/Vite/TypeScript source is under `web/`. Dependencies use exact versions in `package.json` and a committed npm lockfile. CDK deploys `web/dist` to a private S3 bucket and writes a non-secret runtime `config.json`. CloudFront disables API caching, forwards the bearer token and query values while replacing the origin `Host`, and applies HSTS, `nosniff`, and frame-deny headers. CloudFront request logging and API stage access logging are intentionally disabled because the required routes contain document IDs; the body-read Lambda emits the narrower audit event described below.

Cognito Hosted UI authentication is mandatory. Self-signup is disabled, the password minimum is 14 characters with complexity requirements, software TOTP MFA is available, and the public app client uses Authorization Code with PKCE. Every route uses the same Cognito JWT authorizer; there is no unauthenticated demo route or API default stage. The frontend keeps tokens in memory and stores only transient OIDC state and the PKCE verifier in tab-scoped `sessionStorage` so the Hosted UI redirect can complete.

The API contract is:

- `GET /api/messages?from&to&source_format&limit&cursor` — newest-first metadata with exact `totalCount`; `from` is an inclusive `ingested_time` lower bound and `to` is an exclusive `ingested_time` upper bound, matching Entity Explorer's `createdTime >=` / `<` range behavior; default 50, maximum 200, opaque forward cursor. The UI derives result ranges and total pages from the exact count and exposes numbered controls for sequentially discovered keyset pages without using `OFFSET`.
- `GET /api/messages/{documentId}` — one metadata record and its durable storage references.
- `POST /api/messages/{documentId}/body` with `{ "variant": "raw" | "parsed" }` — direct content with the appropriate text, XML, or JSON content type.
- `POST /api/query` with `{ "sql": "..." }` — execute one unrestricted SQL statement and return generic columns, rows, and the affected-record count. SQL text is limited to 100,000 characters and the serialized result to 4 MiB.

The UI provides Messages and SQL query views. SQL queries can be named, saved, loaded, and deleted using browser `localStorage`; they are not synchronized between browsers or users. A visible loading spinner is shown while SQL executes. Do not save query text containing clinical values or identifiers on shared or unmanaged devices.

Body content is fetched server-side from the exact stored S3 version and is never exposed through a presigned URL. Synchronous bodies are capped at 4 MiB and read through a bounded stream to remain below Lambda/API response limits. A successful body read writes a structured audit event containing the authenticated JWT `sub`, document ID, variant, and timestamp, but never the body, clinical fields, S3 key, SQL, or raw backend response. Successful SQL execution emits the caller `sub`, timestamp, row count, and affected-record count without logging the SQL text or returned values.

This prototype provides authentication but not row-, facility-, or tenant-level authorization: every authenticated user in the pool can read every metadata row and body. The SQL console is deliberately unrestricted and uses the generated Aurora administrative credential, so any authenticated user can execute modifying or destructive SQL against the database, including changing or dropping metadata objects. IAM grants only `rds-data:ExecuteStatement`, but that action does not make SQL read-only. Before production, federate the user pool to the customer identity provider through approved SAML or OIDC configuration, add an authorization policy/data model, and replace the administrative credential with a database role whose grants match the intended console permissions.

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

`make install` runs frozen Python synchronization and `npm ci --prefix web`, so installation fails rather than silently changing either lockfile. To intentionally refresh frontend dependencies, update exact versions in `web/package.json`, run `npm install` under `web/`, review `package-lock.json`, and rerun validation.

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

`make validate` performs Ruff formatting and lint checks, strict mypy, pytest with branch coverage, frontend TypeScript checking and production build, cdk-nag, and CDK synthesis. `web/dist` must exist for synthesis because it is the exact asset uploaded by `BucketDeployment`; the `synth` target builds it automatically.

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
- CloudFront requests/data transfer, HTTP API requests, frontend S3 storage/deployment, and Cognito managed-login/Plus feature usage.

Production enables OpenSearch standby replicas and longer Aurora backup retention, increasing cost. Use the [AWS Pricing Calculator](https://calculator.aws/) with the target region, traffic, retention, and data-volume assumptions before deployment.

# Usage

## Upload inputs

Only objects created after deployment under these prefixes are routed:

| Format | S3 key pattern | One input produces |
| --- | --- | --- |
| HL7 v2 | `incoming/hl7/*.hl7` or `incoming/hl7/*.txt` | One parsed document and metadata row per unique normalized message under that key |
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
2. Confirm parsed S3 contains `hl7/<document-id>.json` for each unique normalized HL7 message and `ccda/<document-id>.json` for each CCDA document.
3. Confirm deterministic documents appear in `hl7-messages-v1` and `ccda-documents-v1` after the OpenSearch refresh interval.
4. Through an approved Data API client, confirm one metadata row per logical document, including raw and parsed S3 version IDs.
5. Confirm both Lambda `Errors` metrics remain zero.
6. Confirm ingestion logs and error objects contain no raw messages, XML bodies, S3 keys, document IDs, patient identifiers, clinical values, SQL parameters, or raw backend responses.

## Open the authenticated explorer

After an approved deployment:

1. Read the `FrontendDistributionDomainName`, `UserPoolId`, `UserPoolClientId`, and `UserPoolHostedUiDomain` stack outputs.
2. Create users through an approved administrative workflow or configure customer-IdP federation; self-signup is intentionally unavailable.
3. Open `https://<FrontendDistributionDomainName>/`. The app redirects unauthenticated users to Cognito Hosted UI and returns to the distribution after Authorization Code + PKCE completes.
4. Apply ingestion-time/source-format filters, page using the opaque keyset cursor, select one document, and open only the required raw or parsed body tab.
5. If SQL access is required, open **SQL query**, enter one statement, and use **Run query**. The loading circle remains visible until execution finishes. Saved queries stay only in that browser's local storage.
6. Confirm successful body access creates exactly one structured `message_body_fetched` audit event with caller `sub`, document ID, variant, and timestamp, and no body or clinical fields. Confirm successful SQL execution logs metadata only, not SQL text or result values.

Do not share distribution URLs, tokens, audit records, or screenshots containing identifiers outside approved clinical-data handling channels.

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
- Explorer authentication does not segment records by tenant or facility; all user-pool members can read all documents.
- The SQL console intentionally uses the generated Aurora administrative secret; authenticated SQL can modify or destroy database objects and data.
- Production identity federation, WAF policy, capacity, and cost have not been validated against production requirements or traffic.

# Data and security rules

- Do not commit customer files, clinical data, identifiers, credentials, certificates, or full local synthetic corpora.
- Do not log raw HL7, XML, clinical values, S3 keys, patient identifiers, SQL parameters, or raw backend responses. Document IDs may appear only in the required, access-controlled explorer body-read audit event alongside JWT `sub`, variant, and timestamp; do not add them to general request, error, or access logs.
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
