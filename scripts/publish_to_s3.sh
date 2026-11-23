#!/usr/bin/env bash
# scripts/publish_to_s3.sh — push local Parquet (runs/) to S3
set -euo pipefail

ATTEMPT="${1:-test}"

BUCKET="$(terraform -chdir=infra output -raw bucket_name)"
WG="$(terraform -chdir=infra output -raw athena_workgroup)"

echo ">> Push Parquet to S3..."
aws s3 cp "runs/${ATTEMPT}/" "s3://${BUCKET}/runs/${ATTEMPT}/" --recursive

echo
echo "OK. Parquet to: s3://${BUCKET}/runs/${ATTEMPT}/"
echo "Workgroup Athena : ${WG}"