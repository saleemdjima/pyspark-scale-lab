from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

BUCKET = "{{ var.value.market_lab_bucket }}"
APPLICATION_ID = "{{ var.value.emr_application_id }}"
EXECUTION_ROLE_ARN = "{{ var.value.emr_execution_role_arn }}"

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
                    "--input",
                    f"s3://{BUCKET}/raw/daily/2025-01-02/",
                    "--output",
                    f"s3://{BUCKET}/runs/airflow/",
                    "--layout",
                    "date",
                ],
            }
        },
        wait_for_completion=True,
    )
