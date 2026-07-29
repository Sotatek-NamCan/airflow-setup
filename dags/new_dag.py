import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "256Mi", "cpu": "100m"},
    limits={"memory": "512Mi", "cpu": "500m"},
)

with DAG(
    dag_id="etl_workflow",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
):
    extract = KubernetesPodOperator(
        task_id="extract",
        image="python:3.11",
        cmds=["python", "-c"],
        arguments=["print('Extracting data...')"],
        container_resources=RESOURCES,
        on_finish_action="keep_pod",
    )

    transform = KubernetesPodOperator(
        task_id="transform",
        image="python:3.11",
        cmds=["python", "-c"],
        arguments=["print('Transforming data...')"],
        container_resources=RESOURCES,
        on_finish_action="keep_pod",
    )

    load = KubernetesPodOperator(
        task_id="load",
        image="python:3.11",
        cmds=["python", "-c"],
        arguments=["print('Loading data...')"],
        container_resources=RESOURCES,
        on_finish_action="keep_pod",
    )

    extract >> transform >> load