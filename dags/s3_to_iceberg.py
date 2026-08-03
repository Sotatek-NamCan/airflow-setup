import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

SOURCE_PATH = "s3://vna-lab-data-storage/data_sources/csv/"
DESTINATION_TABLE = "glue_catalog.sales.customer"
VIRTUAL_CLUSTER_ID = "nqkyosgls6f2u8mtyudzkox41"
EXECUTION_ROLE_ARN = "arn:aws:iam::606876783566:role/iceberg-demo-mwaa-execution"
SPARK_SCRIPT = "s3://vna-lab-data-storage/jobs/etl.py"
LOG_BUCKET = "vna-lab-data-storage"

RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "256Mi", "cpu": "100m"},
    limits={"memory": "512Mi", "cpu": "500m"},
)

EMR_SCRIPT = f"""
set -e

echo "=== Submitting EMR job ==="
echo "Virtual Cluster : {VIRTUAL_CLUSTER_ID}"
echo "Spark Script    : {SPARK_SCRIPT}"
echo "Source Path     : {SOURCE_PATH}"
echo "Destination     : {DESTINATION_TABLE}"
echo ""

JOB_ID=$(aws emr-containers start-job-run \
  --virtual-cluster-id {VIRTUAL_CLUSTER_ID} \
  --name s3-to-iceberg-job \
  --execution-role-arn {EXECUTION_ROLE_ARN} \
  --release-label emr-7.0.0-latest \
  --job-driver '{{
    "sparkSubmitJobDriver": {{
      "entryPoint": "{SPARK_SCRIPT}",
      "entryPointArguments": [
        "--source", "{SOURCE_PATH}",
        "--destination", "{DESTINATION_TABLE}"
      ]
    }}
  }}' \
  --configuration-overrides '{{
    "monitoringConfiguration": {{
      "s3MonitoringConfiguration": {{
        "logUri": "s3://{LOG_BUCKET}/logs/"
      }}
    }}
  }}' \
  --query 'id' \
  --output text)

if [ -z "$JOB_ID" ]; then
  echo "ERROR: Failed to submit EMR job — no job ID returned."
  exit 1
fi

echo "Job submitted successfully: $JOB_ID"
echo ""

# Poll until job reaches a terminal state
POLL_INTERVAL=30
ELAPSED=0
TIMEOUT=3600

echo "=== Polling job status every ${{POLL_INTERVAL}}s (timeout: ${{TIMEOUT}}s) ==="

while true; do
  DESCRIBE=$(aws emr-containers describe-job-run \
    --virtual-cluster-id {VIRTUAL_CLUSTER_ID} \
    --id $JOB_ID)

  STATE=$(echo $DESCRIBE | python3 -c "import sys,json; print(json.load(sys.stdin)['jobRun']['state'])")
  STATE_DETAILS=$(echo $DESCRIBE | python3 -c "import sys,json; print(json.load(sys.stdin)['jobRun'].get('stateDetails', 'N/A'))")

  echo "[$(date -u +%H:%M:%S)] Job $JOB_ID — state: $STATE"

  if [ "$STATE" = "COMPLETED" ]; then
    echo ""
    echo "=== Job completed successfully ==="
    exit 0

  elif [ "$STATE" = "FAILED" ]; then
    echo ""
    echo "=== Job FAILED ==="
    echo "State details : $STATE_DETAILS"
    echo ""
    echo "=== Full describe output ==="
    echo $DESCRIBE | python3 -m json.tool
    exit 1

  elif [ "$STATE" = "CANCELLED" ] || [ "$STATE" = "CANCEL_PENDING" ]; then
    echo ""
    echo "=== Job was CANCELLED ==="
    echo "State details : $STATE_DETAILS"
    exit 1
  fi

  # Check timeout
  ELAPSED=$((ELAPSED + POLL_INTERVAL))
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo ""
    echo "=== ERROR: Timed out after ${{TIMEOUT}}s waiting for job $JOB_ID ==="
    exit 1
  fi

  sleep $POLL_INTERVAL
done
"""

with DAG(
    dag_id="s3_to_iceberg",
    start_date=datetime.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
):
    submit_and_wait = KubernetesPodOperator(
        task_id="submit_and_wait",
        image="amazon/aws-cli:latest",
        cmds=["bash", "-c"],
        arguments=[EMR_SCRIPT],
        container_resources=RESOURCES,
        on_finish_action="keep_pod",
    )