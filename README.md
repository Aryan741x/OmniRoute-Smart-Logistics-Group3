# OmniRoute Smart Logistics Engine - Data Pipeline

A robust, cloud-native data pipeline built for the OmniRoute Logistics platform. It ingests, transforms, and exports telemetry and operational data regarding vehicle assignments, fuel transactions, maintenance logs, and asset registries. 

The pipeline employs a **Medallion Architecture (Bronze, Silver, Gold)** using **Apache Spark** and **Delta Lake** on **AWS EMR**, orchestrated via **Apache Airflow**, and ultimately syncs aggregated business metrics incrementally into a **PostgreSQL** data warehouse.

## 🏗️ Architecture & AWS Setup

### 1. Storage & Computation
* **Amazon S3**: Acts as the central data lake. Data is logically partitioned into Bronze, Silver, and Gold buckets.
* **Amazon EMR**: Used for distributed data processing. Airflow provisions **ephemeral** EMR clusters (`m5a.xlarge`) dynamically on-demand, executing Spark scripts directly from S3, and tearing down the cluster to optimize costs once complete.

### 2. Orchestration (Apache Airflow)
* Dynamically manages the lifecycle of the AWS EMR clusters.
* Leverages advanced **time-based routing** logic to execute different segments of the pipeline according to distinct SLA schedules.
* Integrates directly with Boto3 (`AwsBaseHook`) to query EC2 states and ensure upstream dependencies (e.g., PostgreSQL instance) are active.
* Emails monthly reports to stakeholders via the `EmailOperator`.

### 3. Data Warehouse (PostgreSQL)
* Hosted on a dedicated EC2 instance within the VPC. 
* To reduce costs, the EC2 instance is kept stopped. The Airflow DAG auto-starts the instance and dynamically retrieves its Private IP before spinning up the Spark jobs.
* Sinks aggregated metrics using PySpark JDBC connections.

---

## 🔁 Medallion Data Flow

### 🥉 Bronze Layer (Ingestion)
Reads raw, unstructured data and writes it to the Bronze S3 bucket.
* **Vehicle Registry**: Master dimension table.
* **Vehicle Assignment**: Tracks driver transitions and pay rates.
* **Maintenance Logs**: Represents mandatory downtime.
* **Fuel Transactions**: Logs actual fuel usage to calculate efficiency.

### 🥈 Silver Layer (Processing & Cleansing)
Transforms Bronze data into highly reliable Delta tables.
* Drops duplicates and null values.
* Casts Unix timestamps to native Date/Timestamp formats.
* Resolves incremental data continuity and schema validations.

### 🥇 Gold Layer (Aggregations)
Business-ready datasets utilizing advanced Delta Lake features.
* **Active Fleet Snapshot**: A point-in-time daily overwrite table aggregating the count of active vehicles per model.
* **Asset History (SCD Type 2)**: Tracks changes in vehicle assignment over time using Delta `MERGE` statements.
* **Fuel Efficiency Audit**: Calculates fuel efficiency metrics, identifying anomalies.
* **Monthly Driver Standing**: Summarizes driver performance on the 1st of every month.

### 📤 Export Layer (PostgreSQL Sync)
A specialized Spark job that pushes data from Gold Delta tables into PostgreSQL.
* Utilizes **Delta Lake's Change Data Feed (CDF)** to perform highly efficient **incremental upserts** instead of full table scans.
* Maintains state securely in an S3 `state.json` file to guarantee idempotency across DAG runs.

---

## ⏱️ Scheduling Requirements (BRD)

The DAG implements complex branching (`BranchPythonOperator`) to adhere to the following business logic rules based on logical execution time:

| Data Subject | Execution Schedule (UTC) | Action |
|---|---|---|
| **Registry & Assignment** | Daily @ 00:00 | Process Bronze → Silver → Gold (Asset History & Fleet Snapshot). |
| **Fuel Transactions** | Daily @ 07:00 | Process Bronze → Silver → Gold (Fuel Efficiency Audit). |
| **Maintenance Logs** | Yearly on Jan 1 | Process Bronze → Silver. |
| **Driver Standing** | 1st of every Month | Process Gold Monthly Standing → Export Report & Email. |
| **PostgreSQL Export** | End of paths | Incrementally sink processed data. |

## 🚀 Setup & Execution

1. **Upload Scripts to S3**: All PySpark processing scripts (e.g., `vehicle_gold.py`, `postgres_export.py`, `config.py`) must be uploaded to the target Silver scripts bucket.
2. **Airflow Variables**: Configure the required Airflow Variables:
   * `postgres_ec2_id` (The instance ID for the DB server)
   * `pg_port`, `pg_user`, `pg_password`, `pg_db`
   * `EMAIL_TO` (for monthly standing reports)
3. **Execution**: The DAG `omniroute_batch_final` runs automatically via Airflow's scheduler. For manual testing, you can use the "Trigger DAG w/ config" feature and pass a JSON like `{"pipeline_stage": "midnight"}` or modify the `Logical date` to simulate different time scenarios.
