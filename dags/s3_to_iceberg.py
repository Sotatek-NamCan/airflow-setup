from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor

SOURCE_PATH = "s3://vna-lab-data-storage/data_sources/csv/"
DESTINATION_TABLE = "glue_catalog.sales.customer"
VIRTUAL_CLUSTER_ID = "nqkyosgls6f2u8mtyudzkox41"
EXECUTION_ROLE_ARN = "arn:aws:iam::606876783566:role/iceberg-demo-mwaa-execution"
SPARK_SCRIPT = "s3://vna-lab-data-storage/jobs/etl.py"

with DAG(
    dag_id="s3_to_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    submit = EmrContainerOperator(        
        task_id="submit_job",
        name="s3-to-iceberg",
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
                    "logUri": "s3://vna-lab-data-storage/logs/"
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

    submit >> wait                        