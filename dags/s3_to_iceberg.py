from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

SOURCE_PATH = "s3://vna-lab-data-storage/data_sources/csv/"
DESTINATION_TABLE = "glue_catalog.sales.customer"
VIRTUAL_CLUSTER_ID = "nqkyosgls6f2u8mtyudzkox41"
EXECUTION_ROLE_ARN = "arn:aws:iam::606876783566:role/iceberg-demo-mwaa-execution"
SPARK_SCRIPT = "s3://vna-lab-data-storage/jobs/etl.py"
LOG_BUCKET = "vna-lab-data-storage"


def print_emr_logs(**context):
    job_id = context["ti"].xcom_pull(task_ids="submit_job")
    
    if not job_id:
        print("No job ID found in XCom — cannot fetch logs.")
        return

    print(f"Fetching logs for EMR job: {job_id}")

    s3 = S3Hook(aws_conn_id="aws_default")

    # EMR on EKS writes logs under this prefix structure
    prefixes_to_try = [
        f"logs/jobs/{job_id}/",
        f"logs/{job_id}/",
        f"logs/jobRuns/{job_id}/",
    ]

    keys = []
    for prefix in prefixes_to_try:
        print(f"Trying prefix: s3://{LOG_BUCKET}/{prefix}")
        found = s3.list_keys(bucket_name=LOG_BUCKET, prefix=prefix)
        if found:
            keys = found
            print(f"Found {len(keys)} log file(s) under {prefix}")
            break

    if not keys:
        print(
            f"No logs found for job {job_id}. "
            f"The job may still be running, or the log path may differ. "
            f"Check s3://{LOG_BUCKET}/logs/ manually."
        )
        return

    for key in keys:
        # Only print readable log files, skip large binary/jar files
        if not any(key.endswith(ext) for ext in [".log", ".out", ".err", ".gz", "stdout", "stderr"]):
            print(f"Skipping non-log file: {key}")
            continue

        print(f"\n{'='*60}")
        print(f"FILE: s3://{LOG_BUCKET}/{key}")
        print('='*60)

        try:
            content = s3.read_key(key=key, bucket_name=LOG_BUCKET)
            # Truncate very large files to avoid flooding the log
            if len(content) > 50_000:
                print(content[:50_000])
                print(f"\n... [truncated — full log at s3://{LOG_BUCKET}/{key}]")
            else:
                print(content)
        except Exception as e:
            print(f"Could not read {key}: {e}")


with DAG(
    dag_id="s3_to_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    submit = EmrContainerOperator(
        task_id="submit_job",
        name="s3-to-iceberg-job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        release_label="emr-7.0.0-latest",
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmitJobDriver": {
                "entryPoint": SPARK_SCRIPT,
                "entryPointArguments": [
                    "--source", SOURCE_PATH,
                    "--destination", DESTINATION_TABLE,
                ],
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{LOG_BUCKET}/logs/"
                }
            }
        },
    )

    wait = EmrContainerSensor(
        task_id="wait_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=submit.output,
        poll_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    fetch_logs = PythonOperator(
        task_id="fetch_emr_logs",
        python_callable=print_emr_logs,
        provide_context=True,
        trigger_rule="all_done",  # runs whether wait_job succeeded or failed
    )

    submit >> wait >> fetch_logs