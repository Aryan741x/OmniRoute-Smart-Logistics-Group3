"""
OmniRoute Smart Logistics Engine - Batch Pipeline DAG

Orchestrates the full Bronze → Silver → Gold ETL pipeline on AWS EMR.
Schedule: Daily at 05:00 UTC (per BRD).

Architecture:
  - EMR cluster runs all Spark jobs
  - Scripts are stored in S3 (silver bucket)
  - Data flows: S3 Bronze → S3 Silver → S3 Gold
"""
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator, EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os

# -----------------------------------------------
# CONFIG
# -----------------------------------------------
# Ephemeral Cluster dynamically created by Airflow
CLUSTER_ID = "{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
LOG_URI = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/EMR_logs/"

JOB_FLOW_OVERRIDES = {
    "Name": "poc-bootcamp-emr-group3",
    "ReleaseLabel": "emr-7.12.0",
    "LogUri": LOG_URI,
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
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
                "InstanceCount": 2,
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
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "AmazonEMRServiceRole",
    "Tags": [
        {"Key": "Project", "Value": "Bootcamp"},
        {"Key": "Environment", "Value": "POC"},
        {"Key": "Owner", "Value": "rahul.pupreja@tothenew.com"},
        {"Key": "CreatedBy", "Value": "aryan.sharma@tothenew.com"},
        {"Key": "ManagedBy", "Value": "DataEngineering"},
        {"Key": "Name", "Value": "poc-bootcamp-emr-group3"},
    ],
}
SCRIPTS_BUCKET = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group3-silver/scripts"

# PostgreSQL Connection Details (Managed in Airflow Variables)
PG_PORT = "{{ var.value.pg_port }}"
PG_DB = "{{ var.value.pg_db }}"
PG_USER = "{{ var.value.pg_user }}"
PG_PASSWORD = "{{ var.value.pg_password }}"

# ============================================
# POSTGRES EC2 AUTO START LOGIC
# ============================================
POSTGRES_EC2_ID = "{{ var.value.postgres_ec2_id }}"

def get_ec2_ip(instance_id, **kwargs):
    hook = AwsBaseHook(aws_conn_id='aws_default', client_type='ec2')
    ec2 = hook.get_conn()
    response = ec2.describe_instances(InstanceIds=[instance_id])
    return response['Reservations'][0]['Instances'][0].get('PublicIpAddress')

def emr_step(name, script_path, script_args=""):
    """
    Generate an EMR step that downloads py-files locally first,
    then runs spark-submit with local paths.
    This avoids S3AFileSystem ClassNotFoundException in spark-submit.
    """
    # Use a unique directory for each task to prevent concurrency issues
    import re
    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', name.lower())
    task_dir = f"/tmp/omniroute_{safe_name}"

    bash_cmd = (
        f"set -e && "
        f"rm -rf {task_dir} && mkdir -p {task_dir} && "
        f"aws s3 cp --recursive {SCRIPTS_BUCKET} {task_dir}/ && "
        f"export PG_HOST='{PG_HOST}' && "
        f"export PG_PORT='{PG_PORT}' && "
        f"export PG_DB='{PG_DB}' && "
        f"export PG_USER='{PG_USER}' && "
        f"export PG_PASSWORD='{PG_PASSWORD}' && "
        f"export PYTHONPATH='{task_dir}':$PYTHONPATH && "
        f"spark-submit --master yarn --deploy-mode client "
        f"--py-files {task_dir}/spark_helper.py,{task_dir}/s3_utils.py,{task_dir}/config.py "
        f"--packages io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4 "
        f"--conf \"spark.jars.ivy={task_dir}/.ivy2\" "
        f"--conf \"spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension\" "
        f"--conf \"spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog\" "
        f"{task_dir}/{script_path} {script_args}"
    )
    return [{
        "Name": name,
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["bash", "-c", bash_cmd]
        }
    }]

def is_last_day_of_month(d):
    next_day = d + timedelta(days=1)
    return next_day.month != d.month

def is_last_day_of_year(d):
    next_year = d + timedelta(days=1)
    return next_year.year != d.year

def add_step_and_wait(dag, task_id, script_path, step_name, script_args=""):
    """Helper to create an EmrAddSteps + EmrStepSensor pair.

    Accepts `script_args` which are forwarded to `emr_step`.
    """
    add = EmrAddStepsOperator(
        task_id=f"{task_id}",
        job_flow_id=CLUSTER_ID,
        steps=emr_step(step_name, script_path, script_args),
        dag=dag,
    )
    wait = EmrStepSensor(
        task_id=f"wait_{task_id}",
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='" + task_id + "')[0] }}",
        poke_interval=60,  # Check every 60 seconds
        timeout=86400,     # Wait up to 24 hours for the step to complete
        mode="reschedule", # FREE UP WORKER SLOT WHILE WAITING
        dag=dag,
    )
    add >> wait
    return add, wait

# -----------------------------------------------
# DAG DEFINITION
# -----------------------------------------------
default_args = {
    "owner": "group3",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="omniroute_batch_final",
    default_args=default_args,
    start_date=datetime(2025, 12, 31),
    schedule_interval="0 0,5,7 * * *",  # BRD: 00:00 UTC and 07:00 UTC
    catchup=False,
    tags=["omniroute", "batch", "delta-lake"],
) as dag:

    report_month = Variable.get('REPORT_MONTH', default_var=None)
    def download_from_s3(report_month=None, **context):
        s3 = S3Hook(aws_conn_id='aws_default')

        # Use logical_date (Airflow 2.0+)
        execution_date = context.get('logical_date') or context.get('execution_date')

        # Logic: If no explicit report_month provided, compute from execution_date
        if not report_month:
            if execution_date is not None:
                prev_month_dt = execution_date.replace(day=1) - timedelta(days=1)
                report_month = prev_month_dt.strftime('%Y-%m')
            else:
                report_month = get_previous_month()

        bucket = "ttn-de-bootcamp-gold-us-east-1"
        key = f"poc-bootcamp-group3-gold/data/report/monthly_standing_{report_month}.txt"
        local_dir = "/tmp/report/"
        filename = f"monthly_standing_{report_month}.txt"
        full_local_path = os.path.join(local_dir, filename)

        if not s3.check_for_key(key, bucket_name=bucket):
            raise AirflowSkipException(f"No report found for month {report_month}")

        if os.path.exists(full_local_path):
            try:
                os.remove(full_local_path)
                print(f"Existing file {full_local_path} deleted to ensure a fresh download.")
            except Exception as e:
                print(f"Warning: Could not delete existing file: {e}")
        # download_file returns the full path of the downloaded file
        downloaded_file_path = s3.download_file(
            key=key,
            bucket_name=bucket,
            local_path=local_dir,
            preserve_file_name=True, # This ensures it keeps 'monthly_standing_YYYY-MM.txt'
            use_autogenerated_subdir=False
        )

        print(f"File downloaded to: {downloaded_file_path}")
        return downloaded_file_path
    # ============================================
    # EMR LIFECYCLE (Ephemeral Cluster Creation)
    # ============================================

    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        dag=dag,
    )

    # ============================================
    # POSTGRES EC2 CONTROL
    # ============================================
    start_postgres = EC2StartInstanceOperator(
        task_id="start_postgres_ec2",
        instance_id=POSTGRES_EC2_ID,
        aws_conn_id="aws_default"
    )

    wait_postgres = EC2InstanceStateSensor(
        task_id="wait_for_postgres",
        instance_id=POSTGRES_EC2_ID,
        target_state="running",
        aws_conn_id="aws_default",
        poke_interval=30,
        timeout=600
    )

    get_pg_ip = PythonOperator(
        task_id="get_pg_ip",
        python_callable=get_ec2_ip,
        op_kwargs={"instance_id": POSTGRES_EC2_ID},
        trigger_rule="all_success"
    )

    PG_HOST = "{{ ti.xcom_pull(task_ids='get_pg_ip') }}"

    # ============================================
    # BRONZE LAYER (Ingestion - parallel)
    # ============================================
    add_bronze_reg, wait_bronze_reg = add_step_and_wait(
        dag, "bronze_registry",
        "Ingestion/vehicle_registry_bronze.py", "Bronze Registry")

    add_bronze_asg, wait_bronze_asg = add_step_and_wait(
        dag, "bronze_assignment",
        "Ingestion/vehicle_assignment_bronze.py", "Bronze Assignment")

    add_bronze_fuel, wait_bronze_fuel = add_step_and_wait(
        dag, "bronze_fuel",
        "Ingestion/fuel_transaction_bronze.py", "Bronze Fuel Transaction")

    add_bronze_maint, wait_bronze_maint = add_step_and_wait(
        dag, "bronze_maintenance",
        "Ingestion/vehicle_maintenance_bronze.py", "Bronze Maintenance")

    # ============================================
    # SILVER LAYER (Processing - after respective bronze)
    # ============================================
    silver_reg, wait_silver_reg = add_step_and_wait(
        dag, "silver_registry",
        "Data-Processing/vehicle_registry_silver.py", "Silver Registry")

    silver_asg, wait_silver_asg = add_step_and_wait(
        dag, "silver_assignment",
        "Data-Processing/vehicle_assignment_silver.py", "Silver Assignment")

    silver_fuel, wait_silver_fuel = add_step_and_wait(
        dag, "silver_fuel",
        "Data-Processing/fuel_transaction_silver.py", "Silver Fuel Transaction")

    silver_maint, wait_silver_maint = add_step_and_wait(
        dag, "silver_maintenance",
        "Data-Processing/vehicle_maintenance_silver.py", "Silver Maintenance")

    # ============================================
    # GOLD LAYER (Aggregation - after all silver)
    # ============================================
    gold_vehicle, wait_gold_vehicle = add_step_and_wait(
        dag, "gold_vehicle",
        "Data-Processing/vehicle_gold.py", "Gold Vehicle (SCD2 + Fleet Snapshot)")

    gold_fuel, wait_gold_fuel = add_step_and_wait(
        dag, "gold_fuel_audit",
        "Data-Processing/gold_fuel_efficiency_audit.py", "Gold Fuel Efficiency Audit")

    # ============================================
    # MONTHLY DRIVER STANDING (Runs only on 1st of month)
    # ============================================
    def check_first_of_month(**kwargs):
        execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
        if is_last_day_of_month(execution_date):
            return "gold_driver_standing"
        return "skip_monthly"

    branch_monthly = BranchPythonOperator(
        task_id="check_if_first_of_month",
        python_callable=check_first_of_month,
        dag=dag,
    )

    skip_monthly = EmptyOperator(task_id="skip_monthly", dag=dag)

    gold_standing, wait_gold_standing = add_step_and_wait(
        dag, "gold_driver_standing",
        "Data-Processing/monthly_driver_standing.py", "Gold Monthly Driver Standing",
        script_args="--target-month {% if var.value.get('REPORT_MONTH') %}{{ var.value.REPORT_MONTH }}{% else %}{{ macros.ds_add(ds, -1)[:7] }}{% endif %}")

    # ============================================
    # EXPORT LAYER (Send Gold to Postgres)
    # ============================================
    # The export_postgres task needs to run regardless of whether the monthly job skipped or ran
    export_pg, wait_export_pg = add_step_and_wait(
        dag, "export_postgres",
        "Export/postgres_export.py", "Export Gold to PostgreSQL")
    
    # Airflow 2.0+ doesn't allow changing trigger_rule via the helper directly easily, 
    # but we can modify the add_step directly.
    export_pg.trigger_rule = "none_failed_min_one_success"

    export_pg_5, wait_export_pg_5 = add_step_and_wait(
        dag, "export_postgres_5",
        "Export/postgres_export_5.py", "Export Gold Vehicle & Fuel Audit to PostgreSQL")

    # ============================================
    # TIME-BASED ROUTING
    # ============================================
    def route_tasks(**kwargs):
        """
        Routes the DAG to the correct Bronze tasks based on the logical_date.
        
        BRD Schedule:
          - 00:00 UTC daily → bronze_registry, bronze_assignment
          - 00:00 UTC Jan 1  → + bronze_maintenance (yearly)
          - 07:00 UTC daily → bronze_fuel
        
        Manual Testing (via Trigger DAG w/ Config):
          Use the logical_date field in the trigger dialog to simulate a scenario:
            - "2026-05-06T00:00:00+00:00"  → midnight run (registry + assignment)
            - "2026-05-06T07:00:00+00:00"  → fuel run
            - "2026-06-01T00:00:00+00:00"  → 1st of month (+ monthly driver standing)
            - "2027-01-01T00:00:00+00:00"  → Jan 1st (+ maintenance + monthly standing)
          
          Or pass {"pipeline_stage": "all"} in the config JSON to run everything.
        """
        execution_date = kwargs.get("logical_date") or kwargs.get("execution_date")
        dag_run = kwargs.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}

        # Manual override: run a specific set of Bronze tasks
        manual_stage = conf.get("pipeline_stage")
        if manual_stage:
            stage_map = {
                "all": ["bronze_registry", "bronze_assignment", "bronze_fuel", "bronze_maintenance"],
                "midnight": ["bronze_registry", "bronze_assignment"],
                "fuel": ["bronze_fuel"],
                "maintenance": ["bronze_maintenance"],
                "export5": ["export_postgres_5"],
            }
            return stage_map.get(manual_stage, ["skip_all"])

        # Scheduled / logical_date based routing
        hour = execution_date.hour
        tasks = []
        if hour == 7:
            tasks.extend(["bronze_registry", "bronze_assignment"])
            if is_last_day_of_year(execution_date):
                tasks.append("bronze_maintenance")
        elif hour == 5:
            tasks.append("bronze_fuel")
        elif hour == 0:
            tasks.append("export_postgres_5")

        return tasks if tasks else ["skip_all"]

    branch_time = BranchPythonOperator(
        task_id="time_based_routing",
        python_callable=route_tasks,
        dag=dag,
    )
    
    receiver_email = Variable.get("EMAIL_TO", default_var="[EMAIL_ADDRESS]")

    download_task = PythonOperator(
        task_id="download_report",
        python_callable=download_from_s3,
        op_kwargs={"report_month": report_month}
    )

    email_report = EmailOperator(
        task_id='monthly_driver_report_email',
        to=receiver_email,
        subject='Monthly Driver Report',
        html_content="""
        <h3>Monthly Driver Report</h3>
        <p>Please find the attached report.</p>
        """,
        files=[f"/tmp/report/monthly_standing_{report_month}.txt"],   # attachment
        trigger_rule="all_success"
    )

    skip_all = EmptyOperator(task_id="skip_all", dag=dag)

    # ============================================
    # PIPELINE BOOKENDS
    # ============================================
    pipeline_start = EmptyOperator(task_id="pipeline_start", dag=dag)
    pipeline_end = EmptyOperator(task_id="pipeline_end", trigger_rule="all_done", dag=dag)

    # ============================================
    # PIPELINE FLOW
    # ============================================

    # ============================================
    # POSTGRES EC2 FLOW BEFORE EMR
    # ============================================

    pipeline_start >> start_postgres >> wait_postgres >> get_pg_ip >> create_cluster

    # EMR Creation -> Time Routing
    create_cluster >> branch_time
    branch_time >> [add_bronze_reg, add_bronze_asg, add_bronze_fuel, add_bronze_maint, export_pg_5, skip_all]

    # Bronze → Silver
    wait_bronze_reg >> silver_reg
    wait_bronze_asg >> silver_asg
    wait_bronze_fuel >> silver_fuel
    wait_bronze_maint >> silver_maint

    # Silver dependencies
    wait_silver_reg >> silver_asg

    # Gold Vehicle (Hour 00:00)
    [wait_silver_reg, wait_silver_asg] >> gold_vehicle

    # Gold Fuel (Hour 07:00)
    # Reads directly from Silver Registry/Assignment/Maint generated earlier
    wait_silver_fuel >> gold_fuel

    # Monthly driver standing
    wait_silver_asg >> branch_monthly
    branch_monthly >> [gold_standing, skip_monthly]

    wait_gold_standing >> download_task >> email_report

    # Export runs after any path (excludes gold_vehicle & gold_fuel_audit — those go via export_pg_5 at 5 UTC)
    [wait_gold_vehicle, wait_gold_fuel, wait_gold_standing, skip_monthly, wait_silver_maint] >> export_pg

    # ============================================
    # EMR TEARDOWN
    # ============================================
    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=CLUSTER_ID,
        trigger_rule="all_done",  # Guarantee termination even if earlier tasks fail
        dag=dag,
    )

    [wait_export_pg, wait_export_pg_5, skip_all] >> terminate_cluster >> pipeline_end
