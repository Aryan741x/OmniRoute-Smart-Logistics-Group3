import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.functions import col, count, current_timestamp
import logging
from spark_helper import get_spark
from config import SILVER_REGISTRY as REGISTRY_SILVER, SILVER_ASSIGNMENT as ASSIGNMENT_SILVER, GOLD_ASSET_HISTORY, GOLD_FLEET_SNAPSHOT
from delta.tables import DeltaTable
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = get_spark("Vehicle Gold Layer")

def vehicle_gold():
    try:
        # -----------------------------
        # 1. Read Silver tables
        # -----------------------------
        registry_df = spark.read.format("delta").load(REGISTRY_SILVER)
        assignment_df = spark.read.format("delta").load(ASSIGNMENT_SILVER)
        
        reg_count = registry_df.count()
        asg_count = assignment_df.count()
        logger.info(f"Registry rows: {reg_count}, Assignment rows: {asg_count}")

        # ====================================
        # 2. Asset History SCD2 (FULL HISTORY)
        # ====================================
        # Export complete SCD2 history for compliance & audit trails
        asset_history = assignment_df.select(
            col("vin"),
            col("driver_id"),
            col("start_date"),
            col("end_date"),
            col("daily_rate"),
            col("region"),
            col("status")
        ).orderBy(col("vin"), col("start_date"))
        
        if not DeltaTable.isDeltaTable(spark, GOLD_ASSET_HISTORY):
            logger.info(f"Creating {GOLD_ASSET_HISTORY}...")
            asset_history.write.format("delta").mode("overwrite").save(GOLD_ASSET_HISTORY)
            spark.sql(f"ALTER TABLE delta.`{GOLD_ASSET_HISTORY}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        else:
            logger.info(f"Merging into {GOLD_ASSET_HISTORY}...")
            target_table = DeltaTable.forPath(spark, GOLD_ASSET_HISTORY)
            target_table.alias("t").merge(
                asset_history.alias("s"),
                "t.vin = s.vin AND t.start_date = s.start_date"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

        DeltaTable.forPath(spark, GOLD_ASSET_HISTORY).optimize().executeZOrderBy("vin", "start_date")
        DeltaTable.forPath(spark, GOLD_ASSET_HISTORY).vacuum(168.0)
        logger.info("✅ Asset History SCD2 merged and optimized")

        # ====================================
        # 3. Active Fleet Snapshot (DAILY REPORT)
        # ====================================
        active_df = assignment_df \
            .filter(col("status") == "IN-TRANSIT") \
            .join(registry_df, "vin", how="inner")

        snapshot = active_df.groupBy("model") \
            .agg(count("*").alias("no_of_active_vehicles")) \
            .withColumn("snapshot_time", current_timestamp()) \
            .orderBy(col("no_of_active_vehicles").desc())
        
        if not DeltaTable.isDeltaTable(spark, GOLD_FLEET_SNAPSHOT):
            logger.info(f"Creating {GOLD_FLEET_SNAPSHOT}...")
            snapshot.write.format("delta").mode("overwrite").save(GOLD_FLEET_SNAPSHOT)
            spark.sql(f"ALTER TABLE delta.`{GOLD_FLEET_SNAPSHOT}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        else:
            logger.info(f"Overwriting {GOLD_FLEET_SNAPSHOT} (daily snapshot)...")
            snapshot.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_FLEET_SNAPSHOT)

        DeltaTable.forPath(spark, GOLD_FLEET_SNAPSHOT).optimize().executeZOrderBy("model")
        DeltaTable.forPath(spark, GOLD_FLEET_SNAPSHOT).vacuum(168.0)
        logger.info("✅ Active Fleet Snapshot overwritten and optimized")
        
        # Log summary
        print("\n===== GOLD LAYER SUMMARY =====")
        # compute counts for reporting
        try:
            history_count = asset_history.count()
        except Exception:
            history_count = 0
        try:
            snapshot_count = snapshot.count()
        except Exception:
            snapshot_count = 0

        print(f"✅ Asset History SCD2: {history_count} records")
        print(f"✅ Active Fleet Snapshot: {snapshot_count} models")
        print("✅ Gold Layer Ready")
        
    except Exception as e:
        logger.error(f"❌ Error in vehicle_gold: {str(e)}")
        raise

if __name__ == "__main__":
    vehicle_gold()