#!/usr/bin/env bash
# Deploy the Manifest MedEx data-quality stack from a single config file.
#
#   edit config.yaml (account, region)   # then:
#   ./deploy.sh                          # deploy
#   ./deploy.sh --diff                   # preview changes only
#   ./deploy.sh --config other.yaml      # alternate config file
#
# Reads a simple "key: value" YAML file (no library needed), resolves the Dashboards
# role from the current credentials when not provided, bootstraps CDK only if the
# account/region is not already bootstrapped, optionally validates, then runs
# cdk deploy with the exact -c flags the stack expects.

set -euo pipefail

CONFIG_FILE="config.yaml"
MODE="deploy"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG_FILE="$2"; shift 2 ;;
    --diff)   MODE="diff"; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

cd "$(dirname "$0")"

# ---- helpers -----------------------------------------------------------------------

die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "==> $*"; }

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed. See README → Prerequisites."; }

# Read one key from the config. Handles: key: value / key: "value" / key: 'value' / key:
cfg() {
  python3 - "$CONFIG_FILE" "$1" <<'PY'
import re, sys
path, key = sys.argv[1], sys.argv[2]
for line in open(path, encoding="utf-8"):
    line = line.split("#", 1)[0].rstrip()
    m = re.match(rf"^\s*{re.escape(key)}\s*:\s*(.*)$", line)
    if m:
        value = m.group(1).strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        print(value)
        break
PY
}

is_true() { [[ "$(echo "${1:-}" | tr '[:upper:]' '[:lower:]')" =~ ^(true|yes|1)$ ]]; }

# ---- preflight ---------------------------------------------------------------------

[[ -f "$CONFIG_FILE" ]] || die "Config file '$CONFIG_FILE' not found."
need python3; need uv; need node; need npm; need aws
command -v cdk >/dev/null 2>&1 || info "cdk CLI not on PATH; using 'uv run cdk' (bundled)."

ACCOUNT="$(cfg account)"
REGION="$(cfg region)"
ENVIRONMENT="$(cfg environment)"; ENVIRONMENT="${ENVIRONMENT:-dev}"
PROFILE="$(cfg profile)"
PROJECT_NAME="$(cfg project_name)"; PROJECT_NAME="${PROJECT_NAME:-manifest-medex-data-quality}"
ENABLE_DASH="$(cfg enable_public_dashboard)"
DASH_ARN="$(cfg dashboard_principal_arn)"
DO_VALIDATE="$(cfg validate)"

[[ "$ACCOUNT" != "111122223333" ]] || die "'account' in $CONFIG_FILE is still the placeholder value. Set it to your AWS account ID."
[[ "$ACCOUNT" =~ ^[0-9]{12}$ ]] || die "'account' must be a 12-digit AWS account ID (got '${ACCOUNT:-<empty>}')."
[[ "$REGION" =~ ^[a-z]{2}(-gov)?-[a-z]+-[0-9]$ ]] || die "'region' looks invalid (got '${REGION:-<empty>}')."

AWS_ARGS=(--region "$REGION")
PROFILE_ARGS=()
if [[ -n "$PROFILE" ]]; then
  AWS_ARGS+=(--profile "$PROFILE")
  PROFILE_ARGS=(--profile "$PROFILE")
fi

info "Checking AWS credentials"
CALLER_JSON="$(aws sts get-caller-identity "${AWS_ARGS[@]}" --output json 2>&1)" \
  || die "AWS credentials are not valid. Log in (e.g. 'aws sso login') and retry.
$CALLER_JSON"
CALLER_ACCOUNT="$(echo "$CALLER_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Account"])')"
CALLER_ARN="$(echo "$CALLER_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["Arn"])')"
[[ "$CALLER_ACCOUNT" == "$ACCOUNT" ]] \
  || die "Credentials belong to account $CALLER_ACCOUNT but config says $ACCOUNT. Fix one of them."
info "Deploying as: $CALLER_ARN"

# ---- dashboards principal ----------------------------------------------------------

CONTEXT_ARGS=(
  -c "environment=$ENVIRONMENT"
  -c "account=$ACCOUNT"
  -c "region=$REGION"
  -c "project_name=$PROJECT_NAME"
)

if is_true "$ENABLE_DASH"; then
  if [[ -z "$DASH_ARN" ]]; then
    info "Resolving the Dashboards role from current credentials"
    if [[ "$CALLER_ARN" =~ :assumed-role/([^/]+)/ ]]; then
      ROLE_NAME="${BASH_REMATCH[1]}"
      DASH_ARN="$(aws iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"})" \
        || die "Could not look up role '$ROLE_NAME'. Set dashboard_principal_arn explicitly in $CONFIG_FILE."
    else
      die "Current credentials are not an assumed role ($CALLER_ARN). Dashboards access requires a role: set dashboard_principal_arn in $CONFIG_FILE to the role you will browse with."
    fi
  fi
  [[ "$DASH_ARN" =~ ^arn:aws[a-z-]*:iam::${ACCOUNT}:role/ ]] \
    || die "dashboard_principal_arn must be an IAM role in account $ACCOUNT (got '$DASH_ARN')."
  info "Dashboards will be enabled for: $DASH_ARN"
  CONTEXT_ARGS+=(-c enable_public_dashboard=true -c "dashboard_principal_arn=$DASH_ARN")
else
  info "Dashboards browser access disabled (enable_public_dashboard is not true)."
fi

# ---- dependencies, validation, bootstrap -------------------------------------------

info "Installing dependencies"
uv sync --all-groups --frozen
( cd web && npm ci --silent )

if is_true "$DO_VALIDATE"; then
  info "Running validation (lint, types, tests, synth)"
  make validate
else
  info "Skipping validation; building frontend and synthesizing only"
  make synth
fi

# CDK bootstrap creates a CloudFormation stack named CDKToolkit; its presence tells us
# whether this account/region is ready, so the user never has to know or decide.
if aws cloudformation describe-stacks --stack-name CDKToolkit "${AWS_ARGS[@]}" >/dev/null 2>&1; then
  info "CDK already bootstrapped in $ACCOUNT/$REGION"
else
  info "Bootstrapping CDK in $ACCOUNT/$REGION (first deployment to this account/region)"
  uv run cdk bootstrap "aws://$ACCOUNT/$REGION" "${CONTEXT_ARGS[@]}" ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}
fi

# ---- deploy ------------------------------------------------------------------------

if [[ "$MODE" == "diff" ]]; then
  info "Previewing changes (no deployment)"
  uv run cdk diff "${CONTEXT_ARGS[@]}" ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}
  exit 0
fi

info "Deploying stack ${PROJECT_NAME}-${ENVIRONMENT}"
uv run cdk deploy --require-approval never "${CONTEXT_ARGS[@]}" ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} \
  --outputs-file cdk-outputs.json

# ---- summary -----------------------------------------------------------------------

info "Deployment complete. Key outputs:"
python3 - <<'PY'
import json
outputs = next(iter(json.load(open("cdk-outputs.json")).values()), {})
for key in ("FrontendDistributionDomainName", "UserPoolId", "UserPoolClientId",
            "UserPoolHostedUiDomain", "RawBucketName", "OpenSearchCollectionName"):
    if key in outputs:
        print(f"  {key:32} {outputs[key]}")
PY

if is_true "$ENABLE_DASH"; then
  COLLECTION="$(python3 -c 'import json; o=next(iter(json.load(open("cdk-outputs.json")).values()),{}); print(o.get("OpenSearchCollectionName",""))')"
  if [[ -n "$COLLECTION" ]]; then
    DASH_URL="$(aws opensearchserverless batch-get-collection --names "$COLLECTION" "${AWS_ARGS[@]}" \
      --query 'collectionDetails[0].dashboardEndpoint' --output text 2>/dev/null || true)"
    [[ -n "$DASH_URL" && "$DASH_URL" != "None" ]] && echo "  OpenSearchDashboardsUrl          $DASH_URL"
  fi
fi

cat <<EOF

Next steps:
  1. Create a Cognito user in pool above (Console → Cognito → User pools → Users → Create user).
  2. Open https://<FrontendDistributionDomainName> and sign in.
  3. Upload a test file:  aws s3 cp sample.hl7 s3://<RawBucketName>/incoming/hl7/sample.hl7
  4. See README → "Upload inputs" for the bulk-load key layout and → "Verify processing".
EOF
