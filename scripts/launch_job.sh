APP=$(terraform -chdir=infra output -raw emr_application_id)
ROLE=$(terraform -chdir=infra output -raw emr_execution_role_arn)
BUCKET=market-lab-09e954a4

cat > /tmp/driver.json <<EOF
{"sparkSubmit":{"entryPoint":"s3://$BUCKET/code/job.py","entryPointArguments":["--input","s3://$BUCKET/raw/daily/2025-01-02/","--output","s3://$BUCKET/runs/emr/","--date","2025-01-02","--layout","date"]}}
EOF

RUN=$(aws emr-serverless start-job-run \
  --application-id "$APP" --execution-role-arn "$ROLE" \
  --name market-trades-emr --job-driver file:///tmp/driver.json \
  --query jobRunId --output text)
echo "job run: $RUN"

# Wait for job to complete
aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$RUN" --query 'jobRun.state' --output text