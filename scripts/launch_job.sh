#!/usr/bin/env bash

set -euo pipefail

# Submit job.py to EMR Serverless. Override APP, ROLE, BUCKET, INPUT, OUTPUT or NAME via the environment.

APP="${APP:-$(terraform -chdir=infra output -raw emr_application_id)}"
ROLE="${ROLE:-$(terraform -chdir=infra output -raw emr_execution_role_arn)}"
BUCKET="${BUCKET:-$(terraform -chdir=infra output -raw bucket_name)}"
INPUT="${INPUT:-s3://$BUCKET/raw/daily/2025-01-02/}"
OUTPUT="${OUTPUT:-s3://$BUCKET/runs/emr/}"
NAME="${NAME:-market-trades-emr}"

if [[ -z "$APP" || -z "$ROLE" ]]; then
  echo "Error: APP or ROLE is empty. Please check your Terraform outputs."
  exit 1
fi

cat > /tmp/driver.json <<EOF
{"sparkSubmit":{"entryPoint":"s3://$BUCKET/code/job.py","entryPointArguments":["--input","$INPUT","--output","$OUTPUT","--layout","date"]}}
EOF

RUN=$(aws emr-serverless start-job-run \
  --application-id "$APP" --execution-role-arn "$ROLE" \
  --name "$NAME" --job-driver file:///tmp/driver.json \
  --query jobRunId --output text)
echo "job run: $RUN"

aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$RUN" --query 'jobRun.state' --output text
