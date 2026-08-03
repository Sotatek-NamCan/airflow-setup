from datetime import datetime
import json

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


def _upload_report(s3, run_id, report):
    key = f"logs/airflow-reports/{run_id.replace(':', '-').replace('+', '')}.json"
    s3.load_string(
        string_data=json.dumps(report, indent=2),
        key=key,
        bucket_name=LOG_BUCKET,
        replace=True,
    )
    print(f"Report uploaded to s3://{LOG_BUCKET}/{key}")


def print_emr_logs(**context):
    ti = context["ti"]
    job_id = ti.xcom_pull(task_ids="submit_job")
    run_id = context["run_id"]

    s3 = S3Hook(aws_conn_id="aws_default")

    report = {
        "run_id": run_id,
        "job_id": job_id,
        "checked_at": datetime.utcnow().isoformat(),
        "logs": {},
    }

    if not job_id:
        report["error"] = "No job ID found in XCom — submit_job may have failed."
        _upload_report(s3, run_id, report)
        print(report["error"])
        return

    print(f"Fetching logs for EMR job: {job_id}")

    prefixes_to_try = [
        f"logs/jobs/{job_id}/",
        f"logs/{job_id}/",
        f"logs/jobRuns/{job_id}/",
    ]

    keys = []
    matched_prefix = None
    for prefix in prefixes_to_try:
        print(f"Trying prefix: s3://{LOG_BUCKET}/{prefix}")
        found = s3.list_keys(bucket_name=LOG_BUCKET, prefix=prefix)
        if found:
            keys = found
            matched_prefix = prefix
            print(f"Found {len(keys)} file(s) under {prefix}")
            break

    if not keys:
        report["error"] = (
            f"No logs found for job {job_id}. "
            f"Tried prefixes: {prefixes_to_try}. "
            f"Check s3://{LOG_BUCKET}/logs/ manually."
        )
        _upload_report(s3, run_id, report)
        print(report["error"])
        return

    report["log_prefix"] = matched_prefix

    for key in keys:
        if not any(key.endswith(ext) for ext in [".log", ".out", ".err", "stdout", "stderr"]):
            print(f"Skipping non-log file: {key}")
            continue

        print(f"\n{'='*60}")
        print(f"FILE: s3://{LOG_BUCKET}/{key}")
        print("=" * 60)

        try:
            content = s3.read_key(key=key, bucket_name=LOG_BUCKET)
            if len(content) > 50_000:
                truncated = content[:50_000]
                print(truncated)
                print(f"\n... [truncated — full log at s3://{LOG_BUCKET}/{key}]")
                report["logs"][key] = truncated + "\n... [truncated]"
            else:
                print(content)
                report["logs"][key] = content
        except Exception as e:
            msg = f"Could not read {key}: {e}"
            print(msg)
            report["logs"][key] = msg

    _upload_report(s3, run_id, report)


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
        trigger_rule="all_done",
    )

    submit >> wait >> fetch_logs