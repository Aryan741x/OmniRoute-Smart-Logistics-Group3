"""
OmniRoute: Streaming Pipeline Teardown
--------------------------------------
This DAG orchestrates the graceful shutdown of the streaming infrastructure.
It ensures that the Kafka producer is stopped, the EMR cluster is terminated,
and the supporting EC2 instances are stopped to minimize costs.

Order of Operations:
1. Stop Kafka Producer: Prevents new data from being sent.
2. Terminate EMR: Shuts down the Spark streaming applications and cluster.
3. Stop EC2: Shuts down the Kafka infrastructure.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.ec2 import EC2StopInstanceOperator
from airflow.models import Variable
from datetime import datetime, timedelta

# ─── CONFIGURATION & VARIABLES ─────────────────────────────────────────────
KAFKA_EC2_ID = Variable.get('kafka_ec2_id')
# POSTGRES_EC2_ID = Variable.get('postgres_ec2_id')
EMR_CLUSTER_ID = Variable.get('emr_streaming_cluster_id')

# ─── DAG DEFINITION ────────────────────────────────────────────────────────
default_args = {
    "owner": "ashish-sah",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="streaming_pipeline_terminate_dag",
    default_args=default_args,
    start_date=datetime(2026, 5, 4),
    schedule_interval=None,
    catchup=False,
    tags=["omniroute", "streaming", "teardown"],
) as dag:

    start = EmptyOperator(task_id="start_shutdown")

    # 1. STOP PRODUCER: Stop the local systemd service.
    stop_kafka_producer = BashOperator(
        task_id="stop_kafka_producer",
        bash_command="sudo systemctl stop kafka-producer"
    )

    # 2. TERMINATE EMR: Shut down the persistent streaming cluster.
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=EMR_CLUSTER_ID,
        aws_conn_id="aws_default"
    )

    # 3. STOP EC2: Shut down the Kafka infrastructure machine.
    stop_kafka_ec2 = EC2StopInstanceOperator(
        task_id="stop_kafka_ec2",
        instance_id=KAFKA_EC2_ID,
        aws_conn_id="aws_default"
    )

    # # 4. Stop the Postgres EC2 machine (Optional, included for completeness)
    # stop_postgres_ec2 = EC2StopInstanceOperator(
    #     task_id="stop_postgres_ec2",
    #     instance_id=POSTGRES_EC2_ID,
    #     aws_conn_id="aws_default"
    # )

    end = EmptyOperator(task_id="end_shutdown")

    # ─── DEPENDENCIES ────────────────────────────────────────────────────────
    start >> stop_kafka_producer >> terminate_emr_cluster >> stop_kafka_ec2 >> end