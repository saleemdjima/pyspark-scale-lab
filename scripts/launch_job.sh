#!/usr/bin/env bash

set -euo pipefail

# This script launches a Spark job on EMR Serverless using the AWS CLI.
# It requires the following environment variables to be set:
APP="${APP:-$(terraform -chdir=infra output -raw emr_application_id)}"
ROLE="${ROLE:-$(terraform -chdir=infra output -raw emr_execution_role_arn)}"
BUCKET=market-lab-09e954a4
INPUT="${INPUT:-s3://$BUCKET/raw/daily/2025-01-02/}"
OUTPUT="${OUTPUT:-s3://$BUCKET/runs/emr/}"
NAME="${NAME:-market-trades-emr}"

# Stop the Script if APP or ROLE is empty
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

# Wait for job to complete
aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$RUN" --query 'jobRun.state' --output text