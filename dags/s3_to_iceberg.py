from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

SOURCE_PATH = "s3://vna-lab-data-storage/data_sources/csv/"
DESTINATION_TABLE = "glue_catalog.sales.customer"
EMR_CLUSTER_ID = "nqkyosgls6f2u8mtyudzkox41"
SPARK_SCRIPT = "s3://vna-lab-data-storage/jobs/etl.py"

SPARK_STEPS = [
    {
        "Name": "Load S3 to Iceberg",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                SPARK_SCRIPT,
                "--source",
                SOURCE_PATH,
                "--destination",
                DESTINATION_TABLE,
            ],
        },
    }
]

with DAG(
    dag_id="s3_to_iceberg",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["emr", "iceberg"],
) as dag:

    submit_step = EmrAddStepsOperator(
        task_id="submit_spark_job",
        job_flow_id=EMR_CLUSTER_ID,
        steps=SPARK_STEPS,
    )

    wait_step = EmrStepSensor(
        task_id="wait_for_step",
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ ti.xcom_pull(task_ids='submit_spark_job')[0] }}",
    )

    submit_step >> wait_step