import sys, os, subprocess
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import logging
import json
from spark_helper import get_spark
from config import GOLD_FLEET_SNAPSHOT, GOLD_FUEL_AUDIT
from delta.tables import DeltaTable
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Auto-install psycopg2 on Ephemeral EMR if missing
try:
    import psycopg2
except ImportError:
    import site
    logger.info("Installing psycopg2-binary for Postgres Upserts...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    sys.path.append(site.getusersitepackages())
    import psycopg2

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "omniroute")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_PROPERTIES = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}

# S3 State Paths — separate state file for the 5 UTC export
STATE_S3_PATH = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/pg_export_state/state_5.json"
LOCAL_STATE_FILE = "/tmp/pg_export_state_5.json"

def load_state():
    try:
        subprocess.run(["aws", "s3", "cp", STATE_S3_PATH, LOCAL_STATE_FILE], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with open(LOCAL_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Could not load state from S3. Creating new state starting from version 0.")
        return {
            "snapshot_version": 0,
            "fuel_version": 0,
        }

def save_state(state):
    try:
        with open(LOCAL_STATE_FILE, "w") as f:
            json.dump(state, f)
        subprocess.run(["aws", "s3", "cp", LOCAL_STATE_FILE, STATE_S3_PATH], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("✅ State successfully saved to S3")
    except Exception as e:
        logger.error(f"Failed to save state to S3: {e}")

def create_pg_connection():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    return conn

def deduplicate(df, partition_cols):
    """Keep only the most recent update per primary key."""
    window = Window.partitionBy(*partition_cols).orderBy(desc("_commit_version"))
    return df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn", "_commit_version")

def upsert_to_postgres(df, conn, pg_table, key_columns):
    tmp_table = pg_table + "_tmp"
    
    logger.info(f"Writing {df.count()} records to temp table {tmp_table}")
    df.coalesce(2).write.jdbc(url=JDBC_URL, table=tmp_table, mode='overwrite', properties=PG_PROPERTIES)
    
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
    logger.info(f"✅ Successfully upserted into {pg_table}")

def process_table(spark, path, state_key, state, pg_table, key_cols, conn, overwrite=False):
    if not DeltaTable.isDeltaTable(spark, path):
        logger.warning(f"⚠️ {path} is not a Delta table or missing.")
        return

    if overwrite:
        logger.info(f"Overwriting full table for {pg_table}")
        df = spark.read.format("delta").load(path)
        
        if not df.isEmpty():
            df.coalesce(2).write.jdbc(url=JDBC_URL, table=pg_table, mode='overwrite', properties=PG_PROPERTIES)
            logger.info(f"✅ Successfully overwritten {pg_table}")
            
            try:
                dt = DeltaTable.forPath(spark, path)
                latest_version = dt.history(1).select("version").collect()[0][0]
                state[state_key] = latest_version + 1
            except Exception as e:
                logger.warning(f"Could not get history for version sync: {e}")
        return

    start_version = state[state_key]
    logger.info(f"Checking CDF for {pg_table} starting at version {start_version}")
    
    try:
        raw_df = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", start_version) \
            .load(path)
        new_version_row = raw_df.agg({"_commit_version": "max"}).collect()[0][0]
    except Exception as e:
        error_str = str(e)
        if "DELTA_MISSING_CHANGE_DATA" in error_str and start_version == 0:
            history_df = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
            history_df.createOrReplaceTempView("history_view")
            
            query = """
                SELECT version 
                FROM history_view
                WHERE operation = 'SET TBLPROPERTIES' 
                AND operationParameters['properties'] LIKE '%delta.enableChangeDataFeed%true%'
                ORDER BY version ASC
                LIMIT 1
            """
            cdf_enabled_version = spark.sql(query).collect()
            
            if cdf_enabled_version:
                first_v = cdf_enabled_version[0][0]
                logger.info(f"Found CDF enabled at version {first_v}. Retrying...")
                raw_df = spark.read.format("delta") \
                    .option("readChangeFeed", "true") \
                    .option("startingVersion", first_v) \
                    .load(path)
                new_version_row = raw_df.agg({"_commit_version": "max"}).collect()[0][0]
            else:
                logger.error(f"CDF is not enabled on {path}. Please run: ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed=true)")
                return
        else:
            logger.error(f"Failed to read CDF for {path}. Error: {e}")
            return
    if new_version_row is None:
        logger.info(f"No new changes for {pg_table}")
        return

    changes_df = raw_df.filter(col("_change_type").isin("insert", "update_postimage"))
    
    if changes_df.take(1):
        clean_df = changes_df.drop("_change_type", "_commit_timestamp")
        final_df = deduplicate(clean_df, key_cols)
        upsert_to_postgres(final_df, conn, pg_table, key_cols)
    else:
        logger.info(f"Only deletes found, skipping upsert for {pg_table}")

    state[state_key] = new_version_row + 1

def export_gold_vehicle_fuel_to_postgres():
    """
    5 UTC Export: Writes gold_vehicle (fleet snapshot + asset history) 
    and gold_fuel_audit tables to PostgreSQL.
    """
    spark = get_spark("Gold Vehicle & Fuel Audit To PostgreSQL (5 UTC)")

    try:
        logger.info("Starting 5 UTC Gold Export (Vehicle + Fuel Audit) to PostgreSQL...")
        
        state = load_state()
        conn = create_pg_connection()
        
        try:
            # 1. Active Fleet Snapshot (full overwrite)
            process_table(
                spark, GOLD_FLEET_SNAPSHOT, "snapshot_version", state, 
                "gold_active_fleet_snapshot", ["model"], conn, overwrite=True
            )
            
            # 2. Fuel Efficiency Audit (CDC incremental)
            process_table(
                spark, GOLD_FUEL_AUDIT, "fuel_version", state, 
                "gold_fuel_efficiency_audit", ["vin", "audit_date"], conn
            )
            
            conn.commit()
            save_state(state)
            logger.info("✅ 5 UTC PostgreSQL Export Complete!")
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    except Exception as e:
        logger.error(f"❌ Error in export_gold_vehicle_fuel_to_postgres: {str(e)}")
        raise

if __name__ == "__main__":
    export_gold_vehicle_fuel_to_postgres()
