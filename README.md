# Manifest MedEx Data Quality Infrastructure

A single Python CDK application with active modules directly under `src/`.

## Files

- `app.py` starts CDK, loads configuration, enables cdk-nag, and creates the stack.
- `src/config.py` validates environment, account, region, cdk-nag, and termination-protection settings.
- `src/stack.py` defines the AWS resources.
- `tests/` verifies configuration safety and the synthesized CloudFormation template.

There is no extra package folder inside `src/`, contract model, JSON Schema, parser, Lambda function, OpenSearch domain, deployment pipeline, or AWS deployment in this repository yet.

## Current infrastructure

- One rotating customer-managed KMS key.
- Versioned, private raw, parsed, and error S3 buckets with KMS encryption, TLS 1.2 enforcement, retained deletion policies, and centralized S3 access logs.
- KMS-encrypted parse and index SQS queues, each with a 14-day dead-letter queue.
- Native S3 EventBridge routing from newly created raw objects to the parse queue, with bounded retries and dead-letter delivery.
- CloudWatch alarms for old queue messages and visible DLQ messages.

The alarms currently have no SNS, email, Slack, or incident-management action because no notification destination has been selected.

## Prerequisites

- Python 3.12 or 3.13
- Node.js 22 LTS (`nvm use` reads `.nvmrc`)
- `uv` 0.11 or later
- AWS CDK CLI 2.x

AWS credentials are not required for tests or environment-agnostic synthesis.

## Setup and validation

```bash
make install
make validate
```

Individual commands:

```bash
make format
make lint
make typecheck
make test
make synth
```

## Configuration

Local synthesis defaults to `dev`:

```bash
uv run cdk synth
```

Environment-specific synthesis:

```bash
uv run cdk synth \
  -c environment=staging \
  -c account=111122223333 \
  -c region=us-west-2
```

`src/config.py` requires account and region together, safely parses CDK boolean strings, and prevents production synthesis with termination protection disabled. Nothing is deployed unless an operator separately runs `cdk deploy`.

## Data flow

```text
raw S3 object created
        |
        v
S3 EventBridge event --> parse SQS queue --> future parser
                                  |
                                  +--> parse DLQ after repeated failures

future parser --> parsed/error S3 --> index SQS queue --> future indexer
```

## Data and security rules

- Do not commit customer files or the full local Synthea corpus.
- Do not log clinical values, raw messages, document bodies, or patient identifiers.
- Do not place PHI in S3 keys, SQS attributes, tags, metrics, or alarm descriptions.
- Do not disable production termination protection or retained storage without explicit review.
