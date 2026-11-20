# Deployment

This deploys the Manifest MedEx data-quality platform into an AWS account with CDK. See `README.md` for what gets created and how each component behaves; this file covers only how to deploy and operate it.

## Prerequisites

Install these first; each links to its official installation guide.

**Supported platforms:** macOS, Linux, or AWS CloudShell. `deploy.sh` is a bash script that also needs `python3` and `make`. Windows is not supported directly: in Git Bash, `python3` is usually only the Microsoft Store placeholder and `make` is missing. On Windows, deploy from WSL2 or CloudShell.

- **Git** — [install](https://git-scm.com/downloads), to clone the repository.
- **An AWS account with credentials configured**, so that `aws sts get-caller-identity` works ([configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html); for IAM Identity Center use [`aws configure sso`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html)). Use an administrator or equivalent deployment role; the stack creates IAM roles, a VPC, KMS keys, and an OpenSearch Serverless collection.
- **The AWS CLI v2** — [install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- **Python 3.12 or 3.13** — [install](https://www.python.org/downloads/), or let uv manage it ([`uv python install`](https://docs.astral.sh/uv/guides/install-python/)).
- **[uv](https://docs.astral.sh/uv/) 0.11 or later** — [install](https://docs.astral.sh/uv/getting-started/installation/).
- **Node.js 22 and npm** — [install](https://nodejs.org/en/download), or use [nvm](https://github.com/nvm-sh/nvm#installing-and-updating) (`nvm use` reads `.nvmrc`). The frontend is built during deployment.
- **The AWS CDK v2 CLI** — [install](https://docs.aws.amazon.com/cdk/v2/guide/getting-started.html#getting-started-install) (`npm install -g aws-cdk`). The app pins `aws-cdk-lib` 2.263.0; if you see a "Cloud assembly schema version mismatch" error, upgrade with `npm i -g aws-cdk@latest`.
- Docker is **not** required. Lambda code is bundled locally from pure-Python dependencies, and the ECS ingestion worker image is built in AWS CodeBuild during `deploy.sh`.

Verify your identity and note the account ID; the config file needs it:

```bash
aws sts get-caller-identity
```

## 1. Configure

Clone the repository and work from its root; every command in this guide assumes that directory:

```bash
git clone https://github.com/cal-poly-dxhub/manifest-medex-data-analysis.git
cd manifest-medex-data-analysis
```

Create your configuration from the template:

```bash
cp config.yaml.sample config.yaml
```

Edit `config.yaml` and set at least:

| Key | Value |
| --- | --- |
| `account` | The twelve-digit account ID from `get-caller-identity` |
| `region` | The region to deploy into, e.g. `us-west-2` (replaces the `<region>` placeholder) |

The remaining keys are optional and documented inline.

### Deploying into an existing VPC

By default the stack creates its own isolated VPC. To deploy into a VPC your network team provides instead, fill in all four of these keys in `config.yaml`:

> **Recommended layout: dedicated subnets.** Create two or three new private subnets for this stack (one per Availability Zone) attached to a **new route table** that no other workload uses, rather than reusing subnets other applications share. A gateway endpoint for S3 or DynamoDB is implemented as a route on a route table, and a route table can hold only one such route per service. With a dedicated route table the stack creates its own S3 and DynamoDB gateway endpoints without conflicting with any that already exist elsewhere in the VPC, this stack's traffic is never subject to another team's endpoint policy, and this stack's endpoints never widen network access for anything else. The route table needs only the default `local` route plus whatever route your VPN or transit gateway users need to reach the explorer; no internet or NAT route is required. Interface endpoints (SQS, RDS Data API, Lambda, ECR, CloudWatch Logs) are VPC-wide because of private DNS, so those are shared regardless of subnet layout; see the endpoint policy note below.

| Key | Value |
| --- | --- |
| `vpc_id` | The VPC ID, e.g. `vpc-0123456789abcdef0` |
| `subnet_ids` | Comma-separated private subnet IDs, at least two |
| `route_table_ids` | The route table associated with each subnet, same order |
| `availability_zones` | The AZ of each subnet, same order |

Requirements for the subnets:

- **Private** — no route to an internet gateway. A NAT route is tolerated but not needed; every AWS call goes through VPC endpoints.
- **Two or more Availability Zones** — Aurora requires a subnet group spanning at least two AZs; one private subnet per AZ (typically three) is the expected layout. The deploy fails validation otherwise.
- **Free IP space** — the stack creates about six endpoint network interfaces per subnet plus one per running ECS ingestion task (up to 20 at the default maximums) and per Lambda. `/24` or larger per subnet is comfortable.
- **VPC endpoints** — the stack needs endpoints for S3, DynamoDB, RDS Data API, SQS, Lambda, ECR API (`ecr.api`), ECR Docker (`ecr.dkr`), CloudWatch Logs (`logs`), and OpenSearch Serverless. `deploy.sh` inspects the VPC before deploying: endpoints that already exist are reused, except ones this stack created on an earlier deploy (identified by their `aws:cloudformation:stack-name` tag), which stay managed by the stack. For reused endpoints the stack adds HTTPS ingress for its Lambdas and ingestion workers to the endpoint's security group; missing ones are created in your subnets. Existing endpoints must have a security group attached, and their endpoint policies must allow the relevant actions from this account (the stack does not modify policies on endpoints it does not own). To override detection, set `existing_endpoints` in `config.yaml`.

  **Detection happens only in `deploy.sh`.** The CDK app makes no AWS calls during synthesis, so it cannot see what is already in the VPC; it creates every endpoint not listed in `existing_endpoints`. Deploying with `cdk deploy` directly into a VPC that already has any of these endpoints therefore fails partway through and rolls back: a second S3 or DynamoDB gateway endpoint conflicts with the existing route on the route table, and a second SQS, Lambda, RDS Data API, ECR, or Logs interface endpoint is rejected because its private DNS name is already taken. Detection also runs only at deploy time, so if endpoints are added to or removed from the VPC later, run `./deploy.sh` again. If you deploy without the script anyway, pass the existing endpoints yourself (see "Manual deployment" under Reference).

  **If an existing endpoint has a restrictive policy.** An endpoint policy is evaluated before IAM, for every request that passes through the endpoint. If an endpoint this stack imports allows only specific resources (for example an S3 gateway policy listing particular buckets, or an SQS endpoint policy listing particular queues), this stack's Lambdas and ingestion workers will be denied at runtime even though the deploy succeeds and their IAM roles are correct. The symptom is `AccessDenied` (`receive_failed` or `batch_failed`) in the worker logs, messages landing in the dead-letter queues, and nothing appearing in OpenSearch. Before deploying, check each existing endpoint's policy:

  ```bash
  aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values=<vpc-id> \
    --query "VpcEndpoints[].{service:ServiceName,id:VpcEndpointId,policy:PolicyDocument}" --output json
  ```

  A policy of `"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"` (the default) or one scoped to this AWS account needs no change. A resource-scoped policy must be extended with this stack's resources after the first deploy: the four data bucket ARNs (`RawBucketName`, `ParsedBucketName`, `ErrorBucketName`, `ReportsBucketName` outputs, each as the bucket ARN and `<arn>/*`), the three DynamoDB table ARNs, the `ReindexQueueUrl` queue ARN for the SQS endpoint, and the Aurora cluster ARN for the RDS Data API endpoint. The dedicated-subnet layout above removes this concern for S3 and DynamoDB entirely.

  **Customer-owned SQS endpoint.** The ingestion workers poll their queues through the SQS endpoint. If you reuse an SQS endpoint with a restrictive policy, it must also allow the two worker task roles (`Hl7IngestionRole` and `CcdaIngestionRole` in the stack's IAM roles) to call `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, and `sqs:GetQueueAttributes` on the `Hl7QueueUrl` and `CcdaQueueUrl` queue ARNs respectively. Without that, the services start but never consume, and the queues grow until the age alarms fire. Endpoints the stack creates already carry exactly these permissions.

  **Image pulls.** ECR stores image layers in S3, so a restrictive S3 gateway endpoint policy must also allow `s3:GetObject` on `arn:aws:s3:::prod-<region>-starport-layer-bucket/*`, or tasks fail with `CannotPullContainerError`.

  **Scope of the endpoints the stack creates.** The S3 and DynamoDB gateway endpoints are created with the default (service-wide) endpoint policy, and a gateway endpoint adds a route to every subnet that shares the route table. Any other workload in those subnets therefore gains a network path to S3 and DynamoDB as a whole; access to individual buckets and tables remains governed by IAM and resource policies. The interface endpoints (RDS Data API, SQS) carry policies limited to this stack's roles and resources. If your network standards require the gateway endpoints to be limited to specific buckets or tables, apply that policy to the endpoint after deployment or supply your own endpoints via `existing_endpoints`.

To find the route table for a subnet:

```bash
aws ec2 describe-route-tables \
  --filters Name=association.subnet-id,Values=<subnet-id> \
  --query "RouteTables[0].RouteTableId" --output text
```

If a subnet has no explicit association, it uses the VPC's main route table (`aws ec2 describe-route-tables --filters Name=vpc-id,Values=<vpc-id> Name=association.main,Values=true`).

#### DNS requirement for existing VPCs

Interface endpoints with private DNS, including the **OpenSearch Serverless** endpoint, only work when DNS lookups inside the VPC go through the VPC's **AmazonProvidedDNS** resolver. Many enterprise VPCs use a custom DHCP options set that points at their own DNS servers, for example Active Directory domain controllers. If those servers do not forward the endpoint names to the VPC resolver, the names resolve to **public** addresses. For OpenSearch Serverless this fails in a confusing way: the collection rejects the traffic with **HTTP 401 at the indexing stage** (`failureCategory: document_indexing_failed`, `httpStatus: 401` in the worker logs), and every message ends up in the dead-letter queue, even though IAM, the data access policy, and the security groups are all correct. Other services (SQS, Data API, ECR, Logs) keep working over the public path if the subnets have a NAT gateway, but their endpoint policies are then not applied.

`deploy.sh` prints a warning when the VPC's DHCP options use custom DNS servers.

**Before deploying**, check the VPC's DNS servers:

```bash
aws ec2 describe-dhcp-options \
  --dhcp-options-ids "$(aws ec2 describe-vpcs --vpc-ids <vpc-id> --query 'Vpcs[0].DhcpOptionsId' --output text)" \
  --query "DhcpOptions[0].DhcpConfigurations[?Key=='domain-name-servers'].Values[].Value"
```

`["AmazonProvidedDNS"]` needs no action. Anything else means the custom DNS servers must forward the relevant names to the VPC resolver (the VPC CIDR base + 2, e.g. `10.0.0.2`), typically with a conditional forwarder for `<collection-id>.<region>.aoss.amazonaws.com` (or `aoss.amazonaws.com`) and, if you want endpoint policies enforced, the other service names.

**After deploying**, from an instance or CloudShell VPC environment in the same subnets:

```bash
nslookup <collection-id>.<region>.aoss.amazonaws.com
```

It must return **private** IP addresses from the endpoint's subnets. The collection ID is the host part of the `OpenSearchEndpoint` stack output. If it returns public addresses, fix DNS forwarding before loading data. If forwarding is pinned to specific endpoint IPs (for example a Route 53 private hosted zone with A records), update those records whenever the OpenSearch VPC endpoint is replaced.

People on your network reach the PHI Explorer through CloudFront and Cognito, not directly, so no security group changes are needed for it. In this mode the stack does not create VPC flow logs (the VPC is not ours to log). The OpenSearch Serverless endpoint is always created, because it is bound to this stack's collection. Everything else — security groups, any missing endpoints, the Aurora subnet group — is created inside your subnets exactly as in the default mode. Defaults deploy the `dev` environment with OpenSearch Dashboards browser access enabled for the role you deploy with. `config.yaml` is git-ignored.

## 2. Deploy

```bash
./deploy.sh --diff   # optional: preview what will be created
./deploy.sh
```

`deploy.sh` is the only supported way to deploy. It installs dependencies, builds the frontend, runs the validation suite, bootstraps CDK if the account/region has not been bootstrapped before, inspects an existing VPC for endpoints the stack should reuse, builds the ingestion worker image in AWS CodeBuild (skipped when the worker source is unchanged), and deploys. The image build adds about 3–5 minutes when it runs. The first deployment takes roughly 15–20 minutes, most of it Aurora and the OpenSearch collection. Later deployments take a few minutes.

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

### Upgrading from the Lambda ingestion version

Earlier versions ran ingestion in two Lambda functions. The first deploy of this version replaces them with the ECS services:

- The SQS event source mappings and both ingestion Lambdas are removed, and the ECS services start polling the same queues. **Queued messages are kept**: anything waiting while the switch happens is processed by the workers once they start. Messages a Lambda was processing at that moment return after the visibility timeout and are retried.
- Aurora is resized to 2–16 ACU and switched to 35-day backups with deletion protection in every environment. These are in-place changes.
- The old Lambda log groups are retained and still readable.
- A companion stack, `<project_name>-<environment>-worker-build` (ECR repository and CodeBuild project), is created first.

No manual steps are needed; run `./deploy.sh` as usual. If you reuse a customer-owned SQS endpoint, update its policy first (see "Deploying into an existing VPC").

### Manual deployment

Running the CDK commands directly is not supported. They are documented under "Manual deployment" in Reference below so you can see what `deploy.sh` does, or adapt it for CI. Anything you run yourself must repeat every step `deploy.sh` performs, including the endpoint detection described there.

### Optional: message intake API (Prism-compatible)

Senders that today post Prism's **Clinical Message Intake** JSON (`POST /prism/ods/clinical-message`, base64 HL7 v2 or CCDA in `contents[]`) can keep doing so unchanged. Only the hostname and API key change. The intake service validates each request, stores the decoded message under `incoming/...` so the normal pipeline processes it, stores the envelope (patient IDs, MPI ID, facility, event type, and the other fields) as an encrypted sidecar so those fields become searchable, and archives the exact request body in a private bucket. It answers `200` with Prism's `importStatus` only after S3 has stored everything.

**Network.** The load balancer is **internal** (no public address) in the stack's private subnets, listens on **HTTPS 443 only** (TLS 1.2/1.3), and its security group admits only `intake_source_cidrs`. Intake tasks have no public IP and accept traffic only from the load balancer. Senders reach it over your VPN or transit gateway.

**Authentication.** One sender, one API key, generated into Secrets Manager. Senders send it exactly as they did to Prism: `Authorization: ApiKey base64(<sender_id>:<secret>)` (or `Basic` with the same pair). Requests without a valid key get `401` before the body is read; without a configured key the service accepts nothing. This API-key scheme is a deliberate decision made for Prism compatibility. **Mutual TLS (client certificates on the load balancer) is the recommended upgrade** and needs no payload change.

**Before enabling:**
1. **Certificate.** Production: import the intake hostname's certificate from your internal CA into ACM (`aws acm import-certificate`). Testing: a self-signed certificate imported into ACM is fine. Put the ARN in `intake_certificate_arn`.
2. **Sender networks.** Set `intake_source_cidrs` to the sender's source ranges (private IPv4, /16 or narrower).
3. Set `enable_intake: true` and `intake_sender_id` (e.g. `rhapsody`), then run `./deploy.sh`.

**After deploying:**
1. Point the hostname on the certificate at the `IntakeLoadBalancerDnsName` output (a CNAME in your internal DNS).
2. Read the key and hand it to the sender's administrator through your normal secret-sharing process (never email or chat):
   ```bash
   aws secretsmanager get-secret-value --secret-id <IntakeApiKeySecretArn> --query SecretString --output text
   ```
   The value is `{"id": "<sender_id>", "secret": "<secret>"}`; the sender's header is `ApiKey ` + base64 of `<id>:<secret>`.
3. Confirm the gate from inside the network: a request **without** credentials must return `401` with `WWW-Authenticate: ApiKey`, before you send any real message:
   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" -X POST https://<intake-host>/prism/ods/clinical-message -d '{}'
   ```
   Add `-k` only when testing with a self-signed certificate.

**Behavior to know:**
- Required fields (as in Prism's spec): `sourceMessageId`, `messageDate`, `clinicalMessageType` (`HL7V2` or `CCDA`), `sourceFacilityId`, `patientId`, `patientIdAa`, `contents`. Missing or invalid ones get `400`. Requests over `intake_max_request_mb` get `413`. A storage problem returns `503` with `Retry-After`, so the sender retries.
- If `contents` has both, the `PROCESSED` entry is used (Prism's recommendation), otherwise `RAW`.
- A re-sent message (same facility, message date, and `sourceMessageId`) **replaces** the earlier one, as in Prism.
- The envelope's `sourceFacilityId` takes precedence over MSH-4 for facility attribution.
- Archived request bodies are deleted after `intake_archive_retention_days`. Logs never contain bodies, envelope fields, or keys.

**Rotating the key:** `aws secretsmanager put-secret-value` with a new `{"id": ..., "secret": ...}`, then force a new deployment of the intake service (`aws ecs update-service --force-new-deployment`), and update the sender at the same time.

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
- Everything runs in an isolated VPC with no NAT gateway; Lambdas and ECS tasks reach AWS services through VPC endpoints. There is no public database or search endpoint.
- The S3 buckets, Aurora cluster, OpenSearch collection, DynamoDB tables, KMS key, and Cognito pool are set to **RETAIN** on stack deletion. If you are tearing the system down for good, delete them by hand after `cdk destroy`. Aurora deletion protection is on in every environment, so it must be turned off explicitly before the cluster can be deleted. The `<stack>-worker-build` companion stack is not retained; delete it after the main stack.
- `enable_public_dashboard` exposes only the Dashboards endpoint (IAM-authenticated) and is refused outside the `dev` environment. For a production posture, reach Dashboards through a VPN or VPC path instead.
- Context flags passed to `cdk deploy` do not persist between runs; `deploy.sh` re-applies them from `config.yaml` every time, and also detects existing VPC endpoints, which is why it is the only supported deployment path.

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
| `hl7_task_cpu`, `hl7_task_memory` | HL7 worker Fargate task size (CPU units / MiB) | `1024`, `2048` |
| `hl7_workers`, `hl7_receive_batch_size` | HL7 worker processes per task; messages per receive (1–10) | `8`, `10` |
| `hl7_min_tasks`, `hl7_max_tasks` | HL7 service scaling bounds | `2`, `10` |
| `hl7_backlog_target` | HL7 visible messages per running task that target tracking aims for | `1000` |
| `ccda_task_cpu`, `ccda_task_memory` | CCDA worker Fargate task size | `1024`, `8192` |
| `hl7_max_object_mb`, `ccda_max_object_mb` | Largest raw file each lane accepts (MiB). HL7 is streamed in chunks; CCDA must fit the task memory | `512`, `110` |
| `enable_intake` and `intake_*` | Optional message intake API (see "Optional: message intake API"); request cap `intake_max_request_mb` | off; `150` MB, 1 vCPU / 4 GB |
| `ccda_workers`, `ccda_receive_batch_size` | CCDA worker processes per task; messages per receive | `4`, `1` |
| `ccda_min_tasks`, `ccda_max_tasks` | CCDA service scaling bounds | `2`, `10` |
| `ccda_backlog_target` | CCDA visible messages per running task | `200` |
| `work_hours_min_tasks` | Weekday 07:45–17:30 minimum task count for both services; blank disables scheduled scaling | unset (off) |
| `work_hours_timezone` | IANA time zone for the work-hours schedule | `America/Los_Angeles` |
| `aurora_min_acu`, `aurora_max_acu` | Aurora Serverless v2 capacity range, multiples of 0.5 | `2`, `16` |
| `worker_image_digest` | Worker image digest; set by `deploy.sh`, never by hand | set by `deploy.sh` |

Ingestion and Aurora values are production defaults in every environment. Sizing guidance is in the README under "Ingestion workers" and "Aurora capacity".

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

The remaining subsections document the underlying commands for reference or for CI pipelines. Deploying this way is not supported; `deploy.sh` is the supported path. Running `cdk` directly skips the steps the script performs: reading `config.yaml`, building `web/dist`, validation, resolving the Dashboards role, detecting existing VPC endpoints, and building the worker image. Without `-c worker_image_digest=sha256:...` the ECS services reference a placeholder image and their tasks cannot start.

**Existing VPC without `deploy.sh`.** Pass the four VPC keys plus `existing_endpoints` on every `cdk diff` and `cdk deploy`. List each service whose endpoint already exists in the VPC, using the same format as `config.yaml` (gateway services by name; interface services with the endpoint ID and its first security group):

```bash
aws ec2 describe-vpc-endpoints --filters Name=vpc-id,Values=<vpc-id> \
  --query "VpcEndpoints[].[ServiceName,VpcEndpointType,VpcEndpointId,Groups[0].GroupId]" --output table \
  --profile <approved-aws-profile>
```

Only `com.amazonaws.<region>.s3`, `dynamodb`, `rds-data`, `sqs`, `lambda`, `ecr.api`, `ecr.dkr`, and `logs` matter. For example, a VPC with an S3 gateway endpoint and an SQS interface endpoint becomes:

```bash
uv run cdk deploy \
  -c environment=dev \
  -c account=<account-number> \
  -c region=us-west-2 \
  -c vpc_id=vpc-0123456789abcdef0 \
  -c subnet_ids=subnet-0123456789abcdef0,subnet-0fedcba9876543210 \
  -c route_table_ids=rtb-0123456789abcdef0,rtb-0fedcba9876543210 \
  -c availability_zones=us-west-2a,us-west-2b \
  -c existing_endpoints=s3,sqs=vpce-0123456789abcdef0:sg-0123456789abcdef0 \
  --profile <approved-aws-profile>
```

Leave `existing_endpoints` out only if the VPC has none of the eight. Any endpoint you omit is created by the stack, and the deploy fails if one already exists.

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
- Six interface VPC endpoints (plus the OpenSearch Serverless endpoint) across the configured Availability Zones.
- ECS Fargate ingestion tasks: at least two tasks per format run at all times (at the defaults in us-west-2, about $58/month for HL7 and $89/month for CCDA, plus ~$85/month for the intake API if enabled: two 1 vCPU / 4 GB tasks and the internal load balancer), scaling with backlog.
- Lambda duration and concurrency for the explorer, reports, and reingestion.
- CodeBuild minutes and ECR storage for the worker image.
- SQS, EventBridge, S3 storage/versioning, KMS requests, and CloudWatch logs.
- CloudFront requests/data transfer, HTTP API requests, frontend S3 storage/deployment, and Cognito managed-login/Plus feature usage.

Production enables OpenSearch standby replicas, increasing cost. Every environment uses 35-day Aurora backups and a 2-ACU Aurora minimum. Use the [AWS Pricing Calculator](https://calculator.aws/) with the target region, traffic, retention, and data-volume assumptions before deployment.

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
| Throughput | Ingestion runs on ECS workers that scale with the queue backlog (2–10 tasks per format by default; see README "Ingestion workers"). For multi-million-object backfills, upload in batches and watch the queue-age and DLQ alarms rather than dropping everything at once; the queue absorbs bursts but end-to-end latency grows with backlog. |
| Size | Objects over the lane limit (**HL7 512 MiB, CCDA 110 MiB**, configurable) are rejected (`failureCategory: object_too_large`; the error record includes `objectBytes` and `maxObjectBytes`) and reach the DLQ after five attempts. HL7 batches of any message count are processed in 500-message chunks, so there is no need to split them. One file is still handled by one worker, so many medium files finish sooner than one huge file. CCDAs up to ~100 MB, and files with several concatenated CCDAs, are supported. |
| S3 console folders | Creating a "folder" in the S3 console adds a zero-byte key ending in `/`. Workers recognize these and skip them (logged as `record_skipped`, `reason: folder_marker`); they never reach the DLQ. |

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
5. Confirm both ECS services have running tasks and the worker logs show `batch_processed` events with no `batch_failed`.
6. Confirm ingestion logs and error objects contain no raw messages, XML bodies, S3 keys, document IDs, patient identifiers, clinical values, SQL parameters, or raw backend responses.

### Open the authenticated explorer

After an approved deployment:

1. Read the `FrontendDistributionDomainName`, `UserPoolId`, `UserPoolClientId`, and `UserPoolHostedUiDomain` stack outputs (`deploy.sh` prints them; they are also in `cdk-outputs.json`).
2. Create at least one user as described in step 3 above.
3. Open `https://<FrontendDistributionDomainName>/`. The app redirects unauthenticated users to Cognito Hosted UI and returns to the distribution after Authorization Code + PKCE completes.
4. Apply ingestion-time/source-format filters, page using the opaque keyset cursor, select one document, and open only the required raw or parsed body tab.
5. If SQL access is required, open **SQL query**, enter one statement, and use **Run query**. The loading circle remains visible until execution finishes. Saved queries stay only in that browser's local storage.
6. Confirm successful body access creates exactly one structured `message_body_fetched` audit event with caller `sub`, document ID, variant, truncation flag, and timestamp, and no body or clinical fields. For a document over 4 MiB, confirm the Raw tab shows a truncated preview with **Download full file**, and that downloading writes one `message_body_download_url_issued` event that does not contain the URL. Confirm successful SQL execution logs metadata only, not SQL text or result values.

Do not share distribution URLs, tokens, audit records, or screenshots containing identifiers outside approved clinical-data handling channels.

