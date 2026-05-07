"""
Job: Continuous Gold to Postgres Streaming Export

- Streams gold_violation_incidents (Delta Change Data Feed) 24/7
- Pushes real-time incident updates to Postgres driver_safety_status
- Uses Structured Streaming Checkpoints instead of manual state files!

Usage:
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4 \
  --driver-memory 4g \
  telemetry_postgres.py \
  --gold-dir s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data \
  --checkpoint-dir s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/checkpoint-postgres \
  --pg-url jdbc:postgresql://98.83.211.146:5432/omniroute_db \
  --pg-user fleetdb --pg-password postgres --pg-schema public
"""

import argparse
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col, desc, row_number, to_date
)
from pyspark.sql.window import Window

# ─── DATABASE UTILITIES ────────────────────────────────────────────────────

def create_pg_connection(pg_url, pg_properties):
    """
    Establices a direct psycopg2 connection for executing DDL/DML commands.
    
    Args:
        pg_url (str): JDBC-formatted Postgres URL.
        pg_properties (dict): Dictionary containing 'user' and 'password'.
        
    Returns:
        connection: A psycopg2 connection object.
    """
    import psycopg2
    from urllib.parse import urlparse

    # Convert JDBC URL to standard URL for parsing
    jdbc_url = pg_url.replace("jdbc:", "")
    parsed = urlparse(jdbc_url)

    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port,
        dbname=parsed.path[1:],
        user=pg_properties['user'],
        password=pg_properties['password']
    )
    return conn

def upsert_to_postgres(df, conn, pg_url, pg_table, pg_properties, key_columns):
    """
    Performs a staged upsert (Merge) into a Postgres table.
    
    Workflow:
    1. Write the DataFrame to a temporary table using Spark JDBC.
    2. Execute an 'INSERT INTO ... ON CONFLICT' SQL command to merge data.
    3. Drop the temporary table.
    """
    tmp_table = pg_table + "_tmp"

    # Write temp table
    df.coalesce(2).write.jdbc(url=pg_url, table=tmp_table, mode='overwrite', properties=pg_properties)

    cur = conn.cursor()

    cols = df.columns
    non_keys = [c for c in cols if c not in key_columns]
    insert_cols = ",".join(cols)
    select_cols = ",".join([f"t.{c}" for c in cols])
    conflict_target = ",".join(key_columns)
    update_assign = ",".join([f"{c}=EXCLUDED.{c}" for c in non_keys]) if non_keys else ''

    merge_sql = f"""
    INSERT INTO {pg_table} ({insert_cols})
    SELECT {select_cols} FROM {tmp_table} t
    ON CONFLICT ({conflict_target}) DO UPDATE SET {update_assign};

    DROP TABLE IF EXISTS {tmp_table};
    """

    cur.execute(merge_sql)
    cur.close()

# ─── STREAM PROCESSING ─────────────────────────────────────────────────────

def process_batch(batch_df: DataFrame, epoch_id: int, pg_url, pg_properties, pg_schema):
    """
    Structured Streaming 'foreachBatch' handler for Postgres export.
    """
    log_prefix = f"[Batch {epoch_id}]"
    print(f"{log_prefix} Triggering microbatch export...")
    
    batch_df.cache()

    # Capture new inserts and post-update images from the Change Data Feed
    valid_changes = batch_df.filter(col("_change_type").isin("insert", "update_postimage"))
    
    # Use a count check that is less expensive than isEmpty() on some versions
    if valid_changes.rdd.isEmpty():
        batch_df.unpersist()
        return
        
    # Format for Postgres
    driver_safety_status = (
        valid_changes
        .select(
            "driver_id",
            "vin",
            "base_rate",
            col("strike_number").alias("strike_count"),
            col("strike_type").alias("current_strike_type"),
            col("adjusted_rate").alias("current_adjusted_rate"),
            col("status_after_strike").alias("status"),
            to_date(col("incident_ts"), "yyyy-MM-dd").alias("incident_day"),
            col("month_year").alias("month"),
            col("_commit_version") # Include for deduplication
        )
    )

    # Deduplicate to prevent Postgres primary key violations within the same batch.
    # We take the latest record per unique entity using the commit version.
    window = Window.partitionBy("driver_id", "vin", "strike_count", "month").orderBy(desc("_commit_version"))
    dedup_df = (
        driver_safety_status
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn", "_commit_version")
        .coalesce(2)
    )

    # Execute the upsert via JDBC + SQL
    pg_table = f"{pg_schema}.driver_safety_status"
    conn = None
    try:
        conn = create_pg_connection(pg_url, pg_properties)
        upsert_to_postgres(
            dedup_df,
            conn,
            pg_url,
            pg_table,
            pg_properties,
            ["driver_id", "vin", "strike_count", "month"]
        )
        conn.commit()
        print(f"Batch {epoch_id} committed successfully.")
    except Exception as e:
        if conn: conn.rollback()
        print(f"Error in batch {epoch_id}: {e}")
        raise e 
    finally:
        if conn:
            conn.close() # Ensure Python connection is CLOSED
        batch_df.unpersist() # Clean up memory


# ─── APP INITIALIZATION ────────────────────────────────────────────────────

def main():
    """
    Orchestrates the continuous stream from Gold Delta CDF to PostgreSQL.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--gold-dir", default="s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data")
    p.add_argument("--checkpoint-dir", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/checkpoint-postgres")
    p.add_argument("--pg-url", required=True, help="JDBC URL like jdbc:postgresql://host:5432/db")
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-password", required=True)
    p.add_argument("--pg-schema", default="public")
    args = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("gold_to_postgres_streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    gold_base = args.gold_dir.rstrip("/")
    incidents_path = f"{gold_base}/gold_violation_incidents"

    pg_properties = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}

    print("Starting continuous streaming from Gold CDF to Postgres...")

    # ─── 1. PREREQUISITE CHECK ───────────────────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, incidents_path):
        print(f"ERROR: Gold table NOT found at {incidents_path}. "
              "Streaming source unavailable. Exiting gracefully.")
        return

    # ─── 2. SOURCE CONFIGURATION ─────────────────────────────────────────────
    # Enable 'readChangeFeed' to capture incremental modifications.
    stream = (
        spark.readStream
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0) 
        .load(incidents_path)
    )

    # ─── 3. EXECUTION ────────────────────────────────────────────────────────
    # Push updates to Postgres every minute.
    query = (
        stream.writeStream
        .foreachBatch(lambda batch_df, epoch_id: process_batch(batch_df, epoch_id, args.pg_url, pg_properties, args.pg_schema))
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime="1 minute")
        .start()
    )

    query.awaitTermination()

if __name__ == '__main__':
    main()
