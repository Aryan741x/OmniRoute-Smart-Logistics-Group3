import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, to_date, from_unixtime, trim, upper, lit, current_date, lead
import logging
from datetime import datetime
from spark_helper import get_spark
from config import BRONZE_ASSIGNMENT as BRONZE_PATH, SILVER_REGISTRY as REGISTRY_SILVER_PATH, SILVER_ASSIGNMENT as SILVER_PATH, ARCHIVE_ASSIGNMENT_REJECTIONS as ARCHIVE_PATH
from delta.tables import DeltaTable
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# CONFIG: Partition Strategy
# ====================================
DEFAULT_PARTITIONS = int(os.getenv("SPARK_PARTITIONS", "8"))

spark = get_spark("Vehicle Assignment Silver SCD2")

def assignment_silver():

    # Step 0: Ensure our data sources (Bronze assignments and Silver registry) are ready before starting.
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        msg = f"Bronze assignment not found at {BRONZE_PATH}. Cannot proceed."
        logger.error(msg)
        raise RuntimeError(msg)

    if not DeltaTable.isDeltaTable(spark, REGISTRY_SILVER_PATH):
        msg = (f"Registry Silver not found at {REGISTRY_SILVER_PATH}. "
               f"Run registry_silver first.")
        logger.error(msg)
        raise RuntimeError(msg)

    # Step 1: Load the full history of vehicle assignments from Bronze, and fetch unique valid VINs from the Registry.
    df = spark.read.format("delta").load(BRONZE_PATH)
    registry_df = spark.read.format("delta").load(REGISTRY_SILVER_PATH).select("vin").dropDuplicates()

    # Step 2: Convert Unix timestamps into readable dates for our Slowly Changing Dimension (SCD) logic.
    before_clean_count = df.count()

    if before_clean_count == 0:
        msg = "Bronze assignment table has 0 records. Nothing to process."
        logger.error(msg)
        raise RuntimeError(msg)

    if registry_df.count() == 0:
        msg = "Registry Silver is empty. Cannot validate VINs."
        logger.error(msg)
        raise RuntimeError(msg)

    df = df \
        .withColumn("start_date", to_date(from_unixtime(col("start_timestamp")))) \
        .withColumn("end_date", to_date(from_unixtime(col("end_timestamp"))))

    # Standardize string formats (like making VINs uppercase) to ensure reliable matching across tables.
    df = df \
        .withColumn("vin", upper(trim(col("vin")))) \
        .withColumn("driver_id", upper(trim(col("driver_id")))) \
        .withColumn("region", trim(col("region"))) \
        .withColumn("daily_rate", col("daily_rate").cast("double"))

    # Identify and separate records with obvious data quality issues (e.g., missing keys, negative rates, or impossible date ranges).
    valid_condition = (
        col("vin").isNotNull() & (col("vin") != "") &
        col("driver_id").isNotNull() & (col("driver_id") != "") &
        col("region").isNotNull() & (col("region") != "") &
        col("start_date").isNotNull() &
        col("daily_rate").isNotNull() &
        (col("daily_rate") > 0) &
        (col("end_date").isNull() | (col("end_date") >= col("start_date")))
    )
    invalid_core_df = df.filter(~valid_condition)
    valid_df = df.filter(valid_condition)

    # Enforce referential integrity: we only care about assignments for vehicles that actually exist in our registry.
    valid_df = valid_df.join(registry_df, on="vin", how="inner")

    # Save the problematic records to an archive table so data stewards can review them later without breaking the pipeline.
    invalid_count = invalid_core_df.count()
    if invalid_count > 0:
        invalid_core_df = invalid_core_df.withColumn("rejection_reason", lit("INVALID_ASSIGNMENT_RECORD")) \
            .withColumn("rejected_at", current_date())
        invalid_core_df.write.format("delta").mode("append").save(ARCHIVE_PATH)
        logger.warning(f"Archived {invalid_count} invalid assignment rows")

    after_clean_count = valid_df.count()
    dropped_count = before_clean_count - after_clean_count
    if dropped_count > 0:
        logger.warning(f"Dropped {dropped_count} invalid assignment rows during silver cleaning")

    # Step 3: Handle duplicates. If a vehicle has multiple assignments starting on the same day, we prioritize the one with the highest daily rate, then by the most recent ingestion time.
    window_dedup = Window.partitionBy("vin", "start_date") \
        .orderBy(col("daily_rate").desc(), col("ingestion_time").desc())

    df = valid_df.withColumn("rn", row_number().over(window_dedup)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Prevent driver double-booking. A driver shouldn't be assigned to multiple vehicles on the exact same start date.
    # We keep the highest paying assignment and reject the conflicting ones.
    driver_conflict_window = Window.partitionBy("driver_id", "start_date") \
        .orderBy(col("daily_rate").desc(), col("ingestion_time").desc())

    driver_conflict_df = df.withColumn("rn_driver", row_number().over(driver_conflict_window))
    rejected_driver_conflicts = driver_conflict_df.filter(col("rn_driver") > 1)
    rejected_driver_conflict_count = rejected_driver_conflicts.count()
    if rejected_driver_conflict_count > 0:
        rejected_driver_conflicts = rejected_driver_conflicts.withColumn("rejection_reason", lit("DRIVER_MULTI_ASSIGNMENT_CONFLICT")) \
            .withColumn("rejected_at", current_date()).drop("rn_driver")
        rejected_driver_conflicts.write.format("delta").mode("append").save(ARCHIVE_PATH)
        logger.warning(f"Archived driver assignment conflicts: {rejected_driver_conflict_count}")

    df = driver_conflict_df.filter(col("rn_driver") == 1).drop("rn_driver")

    # Step 4: Organize records chronologically for each vehicle to prepare for SCD Type 2 end-dating.
    window_vin = Window.partitionBy("vin").orderBy("start_date")

    df = df.withColumn(
        "next_start_date",
        lead("start_date").over(window_vin)
    )

    # Set the end date of the current assignment to be exactly when the next assignment begins, ensuring continuous history.
    df = df.withColumn("end_date", col("next_start_date")).filter(
        col("end_date").isNull() | (col("end_date") >= col("start_date"))
    )

    # Step 5: Determine the active status. The current active record is the one without an end date.
    df = df \
        .withColumn(
            "status",
            col("end_date").isNull()
        )

    # Step 6: Convert our boolean status into human-readable business terms.
    df = df.withColumn(
        "status",
        col("status").cast("string")
    ).replace(
        {"true": "IN-TRANSIT", "false": "ARCHIVED"},
        subset=["status"]
    )

    # Step 7: Finalize the schema, dropping any intermediate calculation columns.
    final_df = df.select(
        "vin",
        "driver_id",
        "start_date",
        "end_date",
        "daily_rate",
        "region",
        "status"
    )

    # Step 8: Perform a final sanity check for nulls or invalid rates before writing the data.
    null_check = final_df.filter(
        col("vin").isNull() | col("driver_id").isNull() | col("daily_rate").isNull()
    ).count()
    
    if null_check > 0:
        logger.warning(f"{null_check} records with null values detected")
    
    rate_check = final_df.filter(col("daily_rate") <= 0).count()
    if rate_check > 0:
        logger.warning(f" {rate_check} records with invalid (<=0) daily_rate")
    
    final_count = final_df.count()
    logger.info(f"Final assignment records: {final_count}")

    # Step 9: Optimize data layout and write to the Silver layer.
    # We repartition by VIN for better query performance and partition the physical files by start year.
    final_df_partitioned = final_df.repartition(DEFAULT_PARTITIONS, col("vin")) \
        .withColumn("start_year", col("start_date").cast("string").substr(1, 4))
    
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info(f"Creating {SILVER_PATH}...")
        final_df_partitioned.write.format("delta").mode("overwrite") \
            .partitionBy("start_year") \
            .save(SILVER_PATH)
        spark.sql(f"ALTER TABLE delta.`{SILVER_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        logger.info(f"Merging into {SILVER_PATH}...")
        target_table = DeltaTable.forPath(spark, SILVER_PATH)
        target_table.alias("t").merge(
            final_df_partitioned.alias("s"),
            "t.vin = s.vin AND t.start_date = s.start_date"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
    # Regular Delta Lake maintenance: optimize the file sizes, apply Z-Ordering for faster queries, and vacuum old data.
    logger.info("Running OPTIMIZE and VACUUM on Silver Assignment...")
    DeltaTable.forPath(spark, SILVER_PATH).optimize().executeZOrderBy("vin", "start_date")
    DeltaTable.forPath(spark, SILVER_PATH).vacuum(168.0)

    print("\n===== ASSIGNMENT SILVER SUMMARY =====")
    print(f"Records before cleaning: {before_clean_count}")
    print(f"Records dropped in cleaning: {dropped_count}")
    print(f"Records processed: {final_count}")
    print(f"Partitioned by VIN into {DEFAULT_PARTITIONS} partitions")
    print(f"Assignment Silver (SCD Type 2) Ready")

if __name__ == "__main__":
    assignment_silver()