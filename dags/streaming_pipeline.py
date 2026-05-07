"""
OmniRoute: Medallion Streaming Pipeline Orchestration
------------------------------------------------------
This DAG orchestrates the end-to-end streaming lifecycle for the OmniRoute 
logistics engine. It manages the infrastructure (Kafka/Postgres/EMR) and 
submits the Spark streaming jobs across the Bronze, Silver, and Gold layers.

Workflow Summary:
1. Infrastructure Bootup: Starts Kafka and Postgres EC2 instances.
2. EMR Provisioning: Creates a persistent EMR cluster for streaming workloads.
3. Job Submission: Sequentially launches Spark jobs for:
   - Kafka to Bronze (Raw Ingestion)
   - Bronze to Silver (Cleaning & Enrichment)
   - Silver to Gold (Violation Sessionization & Strike Calculation)
   - Gold to Postgres (Relational Export)
4. Monitoring: Implements self-healing (retries) and failure alerts.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor, EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.session import create_session
from airflow.models.taskinstance import clear_task_instances

# =============================================================================
# CONFIGURATION & VARIABLES
# =============================================================================
KAFKA_EC2_ID = Variable.get('kafka_ec2_id')
POSTGRES_EC2_ID = Variable.get('postgres_ec2_id')
AIRFLOW_EC2_ID = Variable.get('airflow_ec2_id')

# S3 Paths
LOG_URI = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/EMR_logs/EMR-Stream"
SCRIPTS_BUCKET = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group3-silver/scripts"
BOOTSTRAP_SCRIPT = f"{SCRIPTS_BUCKET}/bootstrap_telemetry.sh"

BRONZE_PATH = "poc-bootcamp-group3-bronze/data/bronze/telemetry_stream/_delta_log/00000000000000000000.json"
SILVER_PATH = "poc-bootcamp-group3-silver/data/telemetry_stream/_delta_log/00000000000000000000.json"
GOLD_PATH = "poc-bootcamp-group3-gold/data/gold_violation_incidents/_delta_log/00000000000000000000.json"

BRONZE_BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
SILVER_BUCKET = "ttn-de-bootcamp-silver-us-east-1"
GOLD_BUCKET = "ttn-de-bootcamp-gold-us-east-1"

# ─── HELPER FUNCTIONS ──────────────────────────────────────────────────────

def get_ec2_ip(instance_id, **kwargs):
    """
    Retrieves the public IP address of a target EC2 instance.
    Used for dynamic configuration of Kafka bootstrap and Postgres JDBC URLs.
    """
    hook = AwsBaseHook(aws_conn_id='aws_default', client_type='ec2')
    ec2_client = hook.get_conn()
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress')
    return ip

def set_variable(var_name, task_id, **kwargs):
    """
    Updates an Airflow Variable with a value pulled from XCom.
    """
    ip = kwargs['ti'].xcom_pull(task_ids=task_id)
    Variable.set(var_name, ip)
    print(f"Airflow Variable {var_name} updated to {ip}")

# ─── EMR CLUSTER CONFIGURATION ─────────────────────────────────────────────

# EMR Cluster Overrides
JOB_FLOW_OVERRIDES = {
    "Name": "poc-bootcamp-emr-group3-streaming",
    "ReleaseLabel": "emr-7.12.0",
    "LogUri": LOG_URI,
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "Livy"},
        {"Name": "JupyterEnterpriseGateway"},
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": 15, "VolumeType": "gp3"}, "VolumesPerInstance": 1}
                    ]
                }
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5a.xlarge",
                "InstanceCount": 4,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {"VolumeSpecification": {"SizeInGB": 15, "VolumeType": "gp3"}, "VolumesPerInstance": 1}
                    ]
                }
            }
        ],
        "Ec2SubnetId": "subnet-063127a5d787e4ba1",
        "EmrManagedMasterSecurityGroup": "sg-0ec16a48a5c4876e3",
        "EmrManagedSlaveSecurityGroup": "sg-064b9227088a5bb70",
        "Ec2KeyName": "ashish-sah",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "VisibleToAllUsers": True,
    "StepConcurrencyLevel": 5,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "AmazonEMRServiceRole",
    "Tags": [
        {"Key": "Project", "Value": "Bootcamp"},
        {"Key": "Environment", "Value": "POC"},
        {"Key": "Owner", "Value": "rahul.pupreja@tothenew.com"},
        {"Key": "CreatedBy", "Value": "ashish.sah@tothenew.com"},
        {"Key": "ManagedBy", "Value": "DataEngineering"},
        {"Key": "Name", "Value": "poc-bootcamp-emr-group3-streaming"},
    ],
    "BootstrapActions": [
        {
            "Name": "Bootstrap: install dependencies",
            "ScriptBootstrapAction": {
                "Path": BOOTSTRAP_SCRIPT,
                "Args": []
            }
        }
    ],
}

# =============================================================================
# DAG DEFINITION
# =============================================================================
default_args = {
    "owner": "ashish-sah",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="omniroute_streaming_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 4),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["omniroute", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Start Infrastructure
    start_kafka_ec2 = EC2StartInstanceOperator(
        task_id="start_kafka_ec2",
        instance_id=KAFKA_EC2_ID,
        aws_conn_id="aws_default"
    )

    wait_for_kafka_ec2 = EC2InstanceStateSensor(
        task_id="wait_for_kafka_ec2",
        instance_id=KAFKA_EC2_ID,
        target_state="running",
        aws_conn_id="aws_default"
    )

    get_kafka_ip = PythonOperator(
        task_id="get_kafka_ip",
        python_callable=get_ec2_ip,
        op_kwargs={'instance_id': KAFKA_EC2_ID}
    )

    kafka_ip = "{{ task_instance.xcom_pull(task_ids='get_kafka_ip') }}"
    kafka_bootstrap = f"{kafka_ip}:9092"

    start_kafka_producer = BashOperator(
        task_id="start_kafka_producer_service",
        bash_command=f"echo 'KAFKA_BROKER_SERVERS={kafka_ip}:9092' > /home/ubuntu/kafka.env && sudo systemctl start kafka-producer"
    )

    start_postgres_ec2 = EC2StartInstanceOperator(
        task_id="start_postgres_ec2",
        instance_id=POSTGRES_EC2_ID,
        aws_conn_id="aws_default"
    )

    wait_for_postgres_ec2 = EC2InstanceStateSensor(
        task_id="wait_for_postgres_ec2",
        instance_id=POSTGRES_EC2_ID,
        target_state="running",
        aws_conn_id="aws_default"
    )

    get_pg_ip = PythonOperator(
        task_id="get_pg_ip",
        python_callable=get_ec2_ip,
        op_kwargs={'instance_id': POSTGRES_EC2_ID}
    )

    # 2. EMR Cluster Lifecycle
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default"
    )

    # PostgreSQL connection pieces (read as separate Variables)
    pg_host = "{{ task_instance.xcom_pull(task_ids='get_pg_ip') }}"
    pg_port = Variable.get('pg_port', default_var='5432')
    pg_db = Variable.get('pg_db')
    pg_user = Variable.get('pg_user')
    pg_password = Variable.get('pg_password')
    postgres_jdbc = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    update_cluster_id = PythonOperator(
        task_id="update_cluster_id_var",
        python_callable=set_variable,
        op_kwargs={'var_name': 'emr_streaming_cluster_id', 'task_id': 'create_emr_cluster'}
    )

    cluster_id = Variable.get('emr_streaming_cluster_id')
    
    # Wait for EMR cluster to reach 'WAITING' or 'RUNNING' state.
    wait_for_emr_cluster = EmrJobFlowSensor(
        task_id="wait_for_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        target_states=['WAITING', 'RUNNING'],
        failed_states=['TERMINATED', 'TERMINATED_WITH_ERRORS'],
        aws_conn_id='aws_default',
        poke_interval=60,
        timeout=3600,
        mode='reschedule'
    )

    # ─── MONITORING & RECOVERY ──────────────────────────────────────────────

    def notify_failure(context):
        """
        Custom failure notification logic.
        """
        print("Task failed")
        task_id = context.get('task_instance').task_id
        dag_id = context.get('task_instance').dag_id
        error = context.get('exception')
        print(f"!!! ALERT: Task {task_id} in DAG {dag_id} FAILED. Exception: {error} !!!")


    def resubmit_step(context):
        """
        Idempotent retry logic: Clears the 'AddStep' task if the 'StepSensor' fails,
        allowing the step to be resubmitted to EMR.
        """
        ti = context['task_instance']

        mapping = {
            'wait_k2b_running': 'add_kafka_to_bronze',
            'wait_b2s_running': 'add_bronze_to_silver',
            'wait_s2g_running': 'add_silver_to_gold',
            'wait_g2p_running': 'add_gold_to_postgres'
        }

        add_task_id = mapping.get(ti.task_id)

        if add_task_id:
            dag_run = ti.get_dagrun()
            add_ti = dag_run.get_task_instance(add_task_id)

            if add_ti:
                with create_session() as session:
                    clear_task_instances([add_ti], session=session)
                    print(f"Cleared {add_task_id} for resubmission")

    # ─── PREREQUISITE SENSORS ───────────────────────────────────────────────
    # These sensors ensure that source Delta tables are initialized before
    # starting the downstream streaming consumers.

    wait_for_bronze_table = S3KeySensor(
        task_id="wait_for_bronze_table",
        bucket_name=BRONZE_BUCKET,
        bucket_key=BRONZE_PATH,
        aws_conn_id="aws_default",
        timeout=1800,
        poke_interval=60,
        mode='reschedule'
    )

    wait_for_silver_table = S3KeySensor(
        task_id="wait_for_silver_table",
        bucket_name=SILVER_BUCKET,
        bucket_key=SILVER_PATH,
        aws_conn_id="aws_default",
        timeout=1800,
        poke_interval=60,
        mode='reschedule'
    )

    wait_for_gold_table = S3KeySensor(
        task_id="wait_for_gold_table",
        bucket_name=GOLD_BUCKET,
        bucket_key=GOLD_PATH,
        aws_conn_id="aws_default",
        timeout=1800,
        poke_interval=60,
        mode='reschedule'
    )

    # ─── STREAMING JOB SUBMISSION ──────────────────────────────────────────
    # Jobs are submitted as EMR steps. Each step is monitored by a corresponding sensor.

    # 1. Kafka to Bronze (Raw Ingestion)
    step_k2b = EmrAddStepsOperator(
        task_id="add_kafka_to_bronze",
        job_flow_id=cluster_id,
        steps=[{
            'Name': 'Streaming: Kafka to Bronze',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--conf', "spark.driver.memory=2g",
                    '--conf', 'spark.executor.memory=4g',
                    '--conf', 'spark.executor.cores=2',
                    '--conf', 'spark.executor.instances=2',
                    '--conf', 'spark.sql.shuffle.partitions=4',
                    '--packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.2.0',
                    '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                    '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                    '--conf', 'yarn.scheduler.capacity.maximum-am-resource-percent=0.5',
                    f"{SCRIPTS_BUCKET}/Ingestion/consumer_spark.py",
                    '--topic', 'telemetry_stream',
                    '--bootstrap-servers', kafka_bootstrap 
                ],
            },
        }]
    )

    wait_k2b = EmrStepSensor(
        task_id="wait_k2b_running",
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_kafka_to_bronze')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['FAILED', 'CANCELLED', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
        retries=1,
        on_retry_callback=resubmit_step,
        on_failure_callback=notify_failure,
        poke_interval=60,
        mode='reschedule'
    )

    # 2. Bronze to Silver (Cleaning & Enrichment)
    step_b2s = EmrAddStepsOperator(
        task_id="add_bronze_to_silver",
        job_flow_id=cluster_id,
        steps=[{
            'Name': 'Streaming: Bronze to Silver',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--conf', "spark.driver.memory=2g",
                    '--conf', 'spark.executor.memory=4g',
                    '--conf', 'spark.executor.cores=2',
                    '--conf', 'spark.executor.instances=2',
                    '--conf', 'spark.sql.shuffle.partitions=4',
                    '--conf', 'yarn.scheduler.capacity.maximum-am-resource-percent=0.5',
                    '--packages', 'io.delta:delta-spark_2.12:3.2.0,org.apache.sedona:sedona-spark-3.5_2.12:1.7.0,org.datasyslab:geotools-wrapper:1.7.0-28.5',
                    '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                    '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                    f"{SCRIPTS_BUCKET}/Data-Processing/telemetry_silver.py",
                    '--topic', 'telemetry_stream'
                ],
            },
        }]
    )

    wait_b2s = EmrStepSensor(
        task_id="wait_b2s_running",
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_bronze_to_silver')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['FAILED', 'CANCELLED', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
        retries=1,
        on_retry_callback=resubmit_step,
        on_failure_callback=notify_failure,
        poke_interval=60,
        mode='reschedule'
    )

    # 3. Silver to Gold (Violation Analysis)
    step_s2g = EmrAddStepsOperator(
        task_id="add_silver_to_gold",
        job_flow_id=cluster_id,
        steps=[{
            'Name': 'Streaming: Silver to Gold',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--conf', "spark.driver.memory=2g",
                    '--conf', 'spark.executor.memory=4g',
                    '--conf', 'spark.executor.cores=2',
                    '--conf', 'spark.executor.instances=2',
                    '--conf', "spark.databricks.delta.schema.autoMerge.enabled=true",
                    '--conf', "spark.databricks.delta.optimizeWrite.enabled=true",
                    '--conf', "spark.databricks.delta.autoCompact.enabled=true",
                    '--deploy-mode', 'cluster',
                    '--conf', 'spark.sql.shuffle.partitions=4',
                    '--packages', 'io.delta:delta-spark_2.12:3.2.0',
                    '--conf', 'yarn.scheduler.capacity.maximum-am-resource-percent=0.5',
                    '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                    '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                    f"{SCRIPTS_BUCKET}/Data-Processing/telemetry_gold.py",
                    '--topic', 'telemetry_stream'
                ],
            },
        }]
    )

    wait_s2g = EmrStepSensor(
        task_id="wait_s2g_running",
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_silver_to_gold')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['FAILED', 'CANCELLED', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
        retries=1,
        on_retry_callback=resubmit_step,
        on_failure_callback=notify_failure,
        poke_interval=60,
        mode='reschedule'
    )

    # 4. Gold to Postgres (Relational Sink)
    step_g2p = EmrAddStepsOperator(
        task_id="add_gold_to_postgres",
        job_flow_id=cluster_id,
        steps=[{
            'Name': 'Streaming: Gold to Postgres',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--conf', "spark.driver.memory=2g",
                    '--conf', 'spark.executor.memory=4g',
                    '--conf', 'spark.executor.cores=2',
                    '--conf', 'spark.executor.instances=2',
                    '--conf', 'spark.sql.shuffle.partitions=4',
                    '--packages', 'io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4',
                    '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
                    '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
                    f"{SCRIPTS_BUCKET}/Data-Processing/telemetry_postgres.py",
                    '--gold-dir', 's3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data',
                    '--checkpoint-dir', 's3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/checkpoint-postgres',
                    '--pg-url', postgres_jdbc,
                    '--pg-user', pg_user,
                    '--pg-password', pg_password,
                    '--pg-schema', 'public'
                ],
            },
        }]
    )

    wait_g2p = EmrStepSensor(
        task_id="wait_g2p_running",
        job_flow_id=cluster_id,
        step_id="{{ task_instance.xcom_pull(task_ids='add_gold_to_postgres')[0] }}",
        target_states=['COMPLETED'],
        failed_states=['FAILED', 'CANCELLED', 'TERMINATED', 'TERMINATED_WITH_ERRORS'],
        retries=1,
        on_retry_callback=resubmit_step,
        on_failure_callback=notify_failure,
        poke_interval=60,
        mode='reschedule'
    )

    end = EmptyOperator(task_id = "end", trigger_rule=TriggerRule.ALL_DONE)

    get_airflow_ip = PythonOperator(
        task_id="get_airflow_ip",
        python_callable=get_ec2_ip,
        op_kwargs={'instance_id': AIRFLOW_EC2_ID},
        trigger_rule=TriggerRule.ONE_FAILED
    )

    receiver_email = Variable.get("EMAIL_TO", default_var="[EMAIL_ADDRESS]")

    email_alert = EmailOperator(
        task_id='send_failure_email_alert',
        to=receiver_email,
        subject='Airflow Alert: Task Failed in {{ dag.dag_id }}',
        html_content="""
        <h3>A task failed in the OmniRoute streaming pipeline.</h3>
        <p>Please check the Airflow UI logs here: 
        <a href="http://{{ task_instance.xcom_pull(task_ids='get_airflow_ip') }}:8080/dags/{{ dag.dag_id }}/graph?dag_run_id={{ run_id }}">Link</a></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # =============================================================================
    # DEPENDENCIES
    # =============================================================================
    start >> [start_kafka_ec2, start_postgres_ec2]
    
    start_kafka_ec2 >> wait_for_kafka_ec2 >> get_kafka_ip >> start_kafka_producer
    start_postgres_ec2 >> wait_for_postgres_ec2 >> get_pg_ip
    
    # Create cluster only after IPs are discovered and services are starting
    [start_kafka_producer, get_pg_ip] >> create_emr_cluster >> wait_for_emr_cluster >> update_cluster_id

    # Submission logic with table sensors
    update_cluster_id >> step_k2b >> wait_k2b
    
    update_cluster_id >> wait_for_bronze_table >> step_b2s >> wait_b2s
    update_cluster_id >> wait_for_silver_table >> step_s2g >> wait_s2g
    update_cluster_id >> wait_for_gold_table >> step_g2p >> wait_g2p

    [wait_k2b, wait_b2s, wait_s2g, wait_g2p] >> end
    [wait_k2b, wait_b2s, wait_s2g, wait_g2p] >> get_airflow_ip >> email_alert
    # 10 minute wait + termination ONLY for K2B failure
    wait_10_min_k2b_fail = BashOperator(
        task_id='wait_10_min_k2b_fail',
        bash_command='sleep 600',
        trigger_rule=TriggerRule.ONE_FAILED
    )

    terminate_cluster_k2b_fail = EmrTerminateJobFlowOperator(
        task_id='terminate_cluster_k2b_fail',
        job_flow_id=cluster_id,
        trigger_rule=TriggerRule.NONE_SKIPPED,
        aws_conn_id='aws_default'
    )

    wait_k2b >> wait_10_min_k2b_fail >> terminate_cluster_k2b_fail
