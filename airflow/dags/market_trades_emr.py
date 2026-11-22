from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

BUCKET = "market-lab-09e954a4"
APPLICATION_ID = "00gaggti1h644215"
EXECUTION_ROLE_ARN = "arn:aws:iam::840271450344:role/market-lab-emr-exec"

with DAG(
    dag_id="market_trades_emr",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "emr", "market-trades"],
) as dag:

    run_spark = EmrServerlessStartJobOperator(
        task_id="run_spark_job",
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{BUCKET}/code/job.py",
                "entryPointArguments": [
                    "--input", f"s3://{BUCKET}/raw/daily/2025-01-02/",
                    "--output", f"s3://{BUCKET}/runs/airflow/",
                    "--date", "2025-01-02",
                    "--layout", "date",
                ],
            }
        },
        wait_for_completion=True,
    )
