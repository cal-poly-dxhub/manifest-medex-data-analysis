# Deployment

This deploys the Manifest MedEx data-quality platform into an AWS account with CDK. See `README.md` for what gets created and how each component behaves.

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
| `region` | The region to deploy into, e.g. `us-west-2` |

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

If you prefer to run the CDK commands directly, or need them for CI, they are documented in `README.md` under "Bootstrap, review, and deploy".

## 3. Create the first user

Self-signup is turned off, so there is no public "sign up" page. An administrator creates every user directly in the Cognito user pool; the same applies to adding users later or resetting a password. The username is the user's email address.

### CLI: create a user with a password they can use right away

```bash
aws cognito-idp admin-create-user \
  --user-pool-id <UserPoolId> \
  --username reviewer@example.com \
  --user-attributes Name=email,Value=reviewer@example.com Name=email_verified,Value=true \
  --message-action SUPPRESS

aws cognito-idp admin-set-user-password \
  --user-pool-id <UserPoolId> \
  --username reviewer@example.com \
  --password '<TheirPassword123!>' \
  --permanent
```

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

For bulk or historical loads, use `aws s3 sync` and follow the key layout in `README.md` under "Key layout for bulk and historical loads": any folder structure is allowed under `incoming/hl7/` and `incoming/ccda/`, HL7 files must end in `.hl7` or `.txt`, CCDA files in `.xml`, and CCDA paths must contain a `participant=<FACILITY_UID>` segment so documents are attributed to a facility. Ingest at least one HL7 and one CCDA document before using the reingestion feature; it restores documents into indexes that the ingestion path creates.

## 5. Open OpenSearch Dashboards

Dashboards is enabled for the deploying role when `enable_public_dashboard` is `true` in `config.yaml` (the default). Find the URL:

```bash
aws opensearchserverless batch-get-collection --names <OpenSearchCollectionName> \
  --query "collectionDetails[0].dashboardEndpoint" --output text
```

Sign in with the same AWS credentials you deployed with. Before using Discover, create two index patterns: `hl7-messages-v1` and `ccda-documents-v1`, both with `ingestTime` as the time field. To grant a different role access, set `dashboard_principal_arn` in `config.yaml` and redeploy.

## 6. Verify

1. The HL7 and CCDA queues drain and both dead-letter queues stay empty (SQS console).
2. Documents appear in the **Messages** tab; clicking one shows the parsed and raw versions.
3. **Search** returns results for a filter such as `messageType equals ADT`.
4. A report imported from `seed/p4p-prototype.json` (Reports tab → Import) runs and produces a downloadable zip of per-facility CSVs.

## Notes

- Everything runs in an isolated VPC with no NAT gateway; Lambdas reach AWS services through VPC endpoints. There is no public database or search endpoint.
- The S3 buckets, Aurora cluster, OpenSearch collection, DynamoDB tables, KMS key, and Cognito pool are set to **RETAIN** on stack deletion. If you are tearing the system down for good, delete them by hand after `cdk destroy`.
- `enable_public_dashboard` exposes only the Dashboards endpoint (IAM-authenticated) and is refused outside the `dev` environment. For a production posture, reach Dashboards through a VPN or VPC path instead.
- Context flags passed to `cdk deploy` do not persist between runs; `deploy.sh` re-applies them from `config.yaml` every time, which is why the script is the recommended path.
