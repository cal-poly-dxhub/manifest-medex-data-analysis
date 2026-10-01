# Deployment

This deploys the Manifest MedEx data-quality platform into an AWS account with CDK. See `README.md` for what gets created and how each component behaves; this file covers only how to deploy and operate it.

## Prerequisites

- An AWS account with credentials configured, so that `aws sts get-caller-identity` works. Use an administrator or equivalent deployment role; the stack creates IAM roles, a VPC, KMS keys, and an OpenSearch Serverless collection.
- **Python 3.12 or 3.13** and [uv](https://docs.astral.sh/uv/) 0.11 or later.
- **Node.js 22** and npm (`nvm use` reads `.nvmrc`). The frontend is built during deployment.
- The AWS CDK v2 CLI. The app pins `aws-cdk-lib` 2.263.0; if you see a "Cloud assembly schema version mismatch" error, upgrade with `npm i -g aws-cdk@latest`.
- The AWS CLI v2.
- Docker is **not** required. Lambda code is bundled locally from pure-Python dependencies.

Verify your identity and note the account ID; the config file needs it:

```bash
aws sts get-caller-identity
```

## 1. Configure

```bash
cp config.yaml.sample config.yaml
```

Edit `config.yaml` and set at least:

| Key | Value |
| --- | --- |
| `account` | The twelve-digit account ID from `get-caller-identity` |
| `region` | The region to deploy into, e.g. `us-west-2` (replaces the `<region>` placeholder) |

The remaining keys are optional and documented inline. Defaults deploy the `dev` environment with OpenSearch Dashboards browser access enabled for the role you deploy with. `config.yaml` is git-ignored.

## 2. Deploy

```bash
./deploy.sh --diff   # optional: preview what will be created
./deploy.sh
```

The script installs dependencies, builds the frontend, runs the validation suite, bootstraps CDK if the account/region has not been bootstrapped before, and deploys. The first deployment takes roughly 15–20 minutes, most of it Aurora and the OpenSearch collection. Later deployments take a few minutes.

It refuses to run if `account` is still the `<account-number>` placeholder or if your credentials belong to a different account than the one configured.

When it succeeds it prints the outputs you need next:

| Output | Use |
| --- | --- |
| `FrontendDistributionDomainName` | URL of the PHI Explorer UI |
| `UserPoolId` | Cognito pool for creating users (step 3) |
| `UserPoolClientId`, `UserPoolHostedUiDomain` | Sign-in configuration (informational) |
| `RawBucketName` | Where to upload HL7 and CCDA files (step 4) |
| `OpenSearchCollectionName` | For finding the Dashboards URL |

All outputs are also saved to `cdk-outputs.json`.

### Manual deployment

If you prefer to run the CDK commands directly, or need them for CI, see "Manual deployment" under Reference below.

## 3. Create the first user

Self-signup is turned off, so there is no public "sign up" page. An administrator creates every user directly in the Cognito user pool; the same applies to adding users later or resetting a password. The username is the user's email address.

### CLI: create a user with a password they can use right away

```bash
EMAIL='reviewer@example.com'
PASSWORD='<TheirPassword123!>'

aws cognito-idp admin-create-user \
  --user-pool-id <UserPoolId> \
  --username "$EMAIL" \
  --user-attributes Name=email,Value="$EMAIL" Name=email_verified,Value=true \
  --message-action SUPPRESS

aws cognito-idp admin-set-user-password \
  --user-pool-id <UserPoolId> \
  --username "$EMAIL" \
  --password "$PASSWORD" \
  --permanent
```

The pool uses the email address as the username, so the `--username` and the `email` attribute must be identical; setting `EMAIL` once keeps them in sync.

- `--message-action SUPPRESS` skips the invite email; you hand the user their password directly. `--permanent` lets them sign in immediately with no forced reset.
- Passwords must be at least **14 characters** with upper- and lower-case letters, a number, and a symbol, or Cognito rejects the second command.
- The pool uses Cognito's default email sender, which is limited to about 50 messages per day and is often filtered as spam. Setting the password directly is the reliable path; omit `--message-action SUPPRESS` only if you want Cognito to attempt an invitation email.

### Console alternative

Amazon Cognito → User pools → `manifest-medex-data-quality-dev-explorer` → **Users** → **Create user** → enter the email as the username and set a password.

### Adding more users or resetting a password later

Run `admin-create-user` again for each new person. To reset an existing user's password, run `admin-set-user-password` with a new value. There is no in-app self-service signup or password reset; user management is always an admin action. Multi-factor authentication (authenticator app) is available and optional per user.

Once a user exists, open `https://<FrontendDistributionDomainName>/` and sign in with their email and password.

## 4. Load data

Upload HL7 and CCDA files to the raw bucket under the two required prefixes:

```bash
aws s3 cp sample.hl7 s3://<RawBucketName>/incoming/hl7/sample.hl7
aws s3 cp sample.xml s3://<RawBucketName>/incoming/ccda/participant=<FACILITY_UID>/sample.xml
```

Processing is automatic. Within a minute the document appears in the PHI Explorer **Messages** tab and in OpenSearch.

For bulk or historical loads, use `aws s3 sync` and follow the key layout under Reference → "Key layout for bulk and historical loads": any folder structure is allowed under `incoming/hl7/` and `incoming/ccda/`, HL7 files must end in `.hl7` or `.txt`, CCDA files in `.xml`, and CCDA paths must contain a `participant=<FACILITY_UID>` segment so documents are attributed to a facility. Ingest at least one HL7 and one CCDA document before using the reingestion feature; it restores documents into indexes that the ingestion path creates.

## 5. Open OpenSearch Dashboards

Dashboards is enabled for the deploying role when `enable_public_dashboard` is `true` in `config.yaml` (the default). Find the URL:

```bash
aws opensearchserverless batch-get-collection --names <OpenSearchCollectionName> \
  --query "collectionDetails[0].dashboardEndpoint" --output text
```

Sign in with the same AWS credentials you deployed with. Before using Discover, create two index patterns: `hl7-messages-v1` and `ccda-documents-v1`, both with `ingestTime` as the time field. To grant a different role access, set `dashboard_principal_arn` in `config.yaml` and redeploy.

## 6. Verify

Quick checks (the detailed list is under Reference → "Verify processing"):

1. The HL7 and CCDA queues drain and both dead-letter queues stay empty (SQS console).
2. Documents appear in the **Messages** tab; clicking one shows the parsed and raw versions.
3. **Search** returns results for a filter such as `messageType equals ADT`.
4. A report imported from `seed/p4p-prototype.json` (Reports tab → Import) runs and produces a downloadable zip of per-facility CSVs.

## Notes

- **Deploy once per account.** The stack creates CloudFront resources (origin access control, response headers policy) whose names are global to the account and derived from the stack name. A second deployment of the same stack in another region of the same account fails with `AlreadyExists`. To run two copies in one account, set a different `project_name` in each `config.yaml`; to move regions, delete the first stack before deploying the second.
- Everything runs in an isolated VPC with no NAT gateway; Lambdas reach AWS services through VPC endpoints. There is no public database or search endpoint.
- The S3 buckets, Aurora cluster, OpenSearch collection, DynamoDB tables, KMS key, and Cognito pool are set to **RETAIN** on stack deletion. If you are tearing the system down for good, delete them by hand after `cdk destroy`.
- `enable_public_dashboard` exposes only the Dashboards endpoint (IAM-authenticated) and is refused outside the `dev` environment. For a production posture, reach Dashboards through a VPN or VPC path instead.
- Context flags passed to `cdk deploy` do not persist between runs; `deploy.sh` re-applies them from `config.yaml` every time, which is why the script is the recommended path.

## Reference

Detailed material for the steps above.

### Install dependencies

From the repository root:

```bash
nvm use
make install
```

`make install` runs frozen Python synchronization and `npm ci --prefix web`, so installation fails rather than silently changing either lockfile. To intentionally refresh frontend dependencies, update exact versions in `web/package.json`, run `npm install` under `web/`, review `package-lock.json`, and rerun validation.

### Configuration reference

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

### Validate and synthesize

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

### Manual deployment

The remaining subsections document the underlying commands for reference or for CI pipelines.

Bootstrap each target account and region once:

```bash
uv run cdk bootstrap \
  -c account=<account-number> \
  -c region=us-west-2 \
  --profile <approved-aws-profile>
```

Review the proposed development changes:

```bash
uv run cdk diff \
  -c environment=dev \
  -c account=<account-number> \
  -c region=us-west-2 \
  --profile <approved-aws-profile>
```

After review and explicit approval, deploy:

```bash
uv run cdk deploy \
  -c environment=dev \
  -c account=<account-number> \
  -c region=us-west-2 \
  --profile <approved-aws-profile>
```

Do not use production credentials or disable termination/deletion protections merely to simplify deployment.

### Enable OpenSearch Dashboards access

The collection API and Dashboards are private by default, so a fresh deployment returns 403 in the browser until access is granted. To enable Dashboards for the role you sign in with, add both flags to the deploy (development only):

```bash
uv run cdk deploy \
  -c environment=dev \
  -c account=<account-number> \
  -c region=us-west-2 \
  -c enable_public_dashboard=true \
  -c dashboard_principal_arn=arn:aws:iam::<account-number>:role/<role-name> \
  --profile <approved-aws-profile>
```

Notes:

- The principal must be an IAM **role** in the same account as the stack; the deploy fails validation otherwise. Find both values for the identity you are deploying with:

  ```bash
  aws sts get-caller-identity --profile <approved-aws-profile>
  ```

  Example output and how to read it:

  ```json
  {
      "UserId": "AROA...:jsmith",
      "Account": "<account-number>",
      "Arn": "arn:aws:sts::<account-number>:assumed-role/AWSReservedSSO_AdministratorAccess_0123456789abcdef/jsmith"
  }
  ```

  - `Account` is the value for `-c account=` and the account portion of the principal ARN.
  - The role name is the segment after `assumed-role/` and before the final `/<session-name>` — here `AWSReservedSSO_AdministratorAccess_0123456789abcdef`.
  - The `Arn` shown is a *session* ARN (`sts` / `assumed-role`); the flag needs the *role* ARN (`iam` / `role`). For an IAM Identity Center (SSO) role the path is `aws-reserved/sso.amazonaws.com/<region>/`, so the value becomes:

    ```text
    arn:aws:iam::<account-number>:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_AdministratorAccess_0123456789abcdef
    ```

    For a plain IAM role (not SSO) it is simply `arn:aws:iam::<account-number>:role/<role-name>`.

  To copy the exact role ARN rather than construct it:

  ```bash
  aws iam get-role \
    --role-name AWSReservedSSO_AdministratorAccess_0123456789abcdef \
    --query "Role.Arn" --output text \
    --profile <approved-aws-profile>
  ```

  Or resolve it from the current identity in one step:

  ```bash
  aws iam get-role \
    --role-name "$(aws sts get-caller-identity --query Arn --output text \
      | sed -E 's#arn:aws:sts::[0-9]+:assumed-role/([^/]+)/.*#\1#')" \
    --query "Role.Arn" --output text
  ```

  This requires the current credentials to be an assumed role (SSO or otherwise). If you deploy with an IAM user, the dashboard principal must still be a role; pick the role you will browse Dashboards with and pass its ARN instead.

  **In the console:** the account ID is under your account name in the top-right menu. The role is at IAM → Roles → search for the role name from the `sts` output above → the **ARN** field on the role's summary page has a copy button. SSO roles are listed there too, under their `AWSReservedSSO_…` names.

- The role also needs the IAM permissions `aoss:APIAccessAll` and `aoss:DashboardsAccessAll` on the collection; administrator roles already have them.
- Context flags apply per invocation. A later `cdk deploy` without them reverts Dashboards to private. To make the setting persist for a development environment, add both keys to the `context` block in `cdk.json`.
- Read the Dashboards URL from the collection page in the OpenSearch Serverless console, or with:

  ```bash
  aws opensearchserverless batch-get-collection \
    --names <collection-name> \
    --query "collectionDetails[0].dashboardEndpoint" --output text
  ```

  The collection name is in the `OpenSearchCollectionName` stack output. In Dashboards, create index patterns `hl7-messages-v1` and `ccda-documents-v1` (time field `ingestTime` for both) before using Discover.

### Cost considerations

This stack is not a free-tier architecture. Major recurring cost drivers include:

- OpenSearch Serverless OCUs and managed storage; the classic model used here does not scale to zero.
- Aurora Serverless v2 ACUs, storage, backup retention, and I/O.
- Two interface VPC endpoints across the configured Availability Zones.
- Lambda duration and concurrency.
- SQS, EventBridge, S3 storage/versioning, KMS requests, and CloudWatch logs.
- CloudFront requests/data transfer, HTTP API requests, frontend S3 storage/deployment, and Cognito managed-login/Plus feature usage.

Production enables OpenSearch standby replicas and longer Aurora backup retention, increasing cost. Use the [AWS Pricing Calculator](https://calculator.aws/) with the target region, traffic, retention, and data-volume assumptions before deployment.

### Key layout for bulk and historical loads

The router matches on prefix and extension only, so any folder structure may be nested **under** the required prefixes. A date-partitioned layout such as `year/month/day/hour` is recommended for large loads because it keeps listings and lifecycle rules manageable:

```text
incoming/hl7/2026/01/15/09/RUHS_CA_RUHS_H_ADT_20260115090412_000123.hl7
incoming/ccda/participant=RUHS_CA_RUHS_H/2026/01/15/09/000456.xml
```

Requirements and behavior to plan around:

| Concern | Rule |
| --- | --- |
| Prefix | Must begin with `incoming/hl7/` or `incoming/ccda/`. Objects elsewhere are ignored entirely. |
| Extension | HL7: `.hl7` or `.txt`. CCDA: `.xml`. Any other extension is routed to the queue, rejected as an invalid route, and lands in the DLQ. |
| Facility for HL7 | Taken from **MSH-4.1 inside the message**. The key does not need to carry it. |
| Facility for CCDA | Taken from the **key**: include a `participant=<facility UID>` path segment (see example). Without it, `sourceFacilityId` is empty and per-facility reports and filters exclude the document. |
| Filenames | Free-form, but keep them non-identifying. Facility, message type, and MSH-7 date in the name are fine and useful for browsing. |
| Ordering | Not guaranteed. Objects are processed by parallel consumers from a standard queue, so arrival order is not preserved. This does not affect counts or report results. |
| Re-uploads | Safe. Document identity is derived from the object (bucket, key, version) so a re-drop of an unchanged object updates rather than duplicates. Note that a *changed* object body under a new version produces a new document. |
| Throughput | Default ingestion concurrency is 10 (HL7) and 5 (CCDA). For multi-million-object backfills, upload in batches and watch the queue-age and DLQ alarms rather than dropping everything at once; the queue absorbs bursts but end-to-end latency grows with backlog. |
| Size | Objects over 50 MB are rejected. Multi-megabyte CCDAs are supported. |

Example uploads with synthetic, non-PHI data:

```bash
aws s3 cp ./synthetic-message.hl7 \
  s3://<raw-bucket>/incoming/hl7/synthetic-message.hl7 \
  --profile <approved-aws-profile>

aws s3 cp ./synthetic-document.xml \
  s3://<raw-bucket>/incoming/ccda/synthetic-document.xml \
  --profile <approved-aws-profile>
```

### Verify processing

After uploading safe synthetic inputs:

1. Confirm both processing queues drain and both DLQs remain empty.
2. Confirm parsed S3 contains `hl7/<document-id>.json` for each unique normalized HL7 message and `ccda/<document-id>.json` for each CCDA document.
3. Confirm deterministic documents appear in `hl7-messages-v1` and `ccda-documents-v1` after the OpenSearch refresh interval.
4. Through an approved Data API client, confirm one metadata row per logical document, including raw and parsed S3 version IDs.
5. Confirm both Lambda `Errors` metrics remain zero.
6. Confirm ingestion logs and error objects contain no raw messages, XML bodies, S3 keys, document IDs, patient identifiers, clinical values, SQL parameters, or raw backend responses.

### Open the authenticated explorer

After an approved deployment:

1. Read the `FrontendDistributionDomainName`, `UserPoolId`, `UserPoolClientId`, and `UserPoolHostedUiDomain` stack outputs (`deploy.sh` prints them; they are also in `cdk-outputs.json`).
2. Create at least one user as described in step 3 above.
3. Open `https://<FrontendDistributionDomainName>/`. The app redirects unauthenticated users to Cognito Hosted UI and returns to the distribution after Authorization Code + PKCE completes.
4. Apply ingestion-time/source-format filters, page using the opaque keyset cursor, select one document, and open only the required raw or parsed body tab.
5. If SQL access is required, open **SQL query**, enter one statement, and use **Run query**. The loading circle remains visible until execution finishes. Saved queries stay only in that browser's local storage.
6. Confirm successful body access creates exactly one structured `message_body_fetched` audit event with caller `sub`, document ID, variant, and timestamp, and no body or clinical fields. Confirm successful SQL execution logs metadata only, not SQL text or result values.

Do not share distribution URLs, tokens, audit records, or screenshots containing identifiers outside approved clinical-data handling channels.

