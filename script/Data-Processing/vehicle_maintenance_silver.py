import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_helper import get_spark
from config import BRONZE_MAINTENANCE as INPUT_PATH, ARCHIVE_MAINT_ERRORS as ERROR_PATH, SILVER_MAINT_CLEAN as CLEAN_PATH
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# -------------------------------
# Spark Session
# -------------------------------
spark = get_spark("Vehicle Maintenance Silver")

def vehicle_maintenance_silver():
    
    print("=== Starting Vehicle Maintenance Silver Processing ===")

    # -------------------------------
    # Read Bronze (Parquet)
    # -------------------------------
    bronze_df = spark.read.format("delta").load(INPUT_PATH)

    # -------------------------------
    # Casting + Cleaning strings
    # -------------------------------
    df = bronze_df \
        .withColumn("vin", upper(trim(col("vin")))) \
        .withColumn("service_date", to_date(col("service_date"))) \
        .withColumn("service_type", trim(col("service_type")))

    common_cols = ["vin", "service_date", "service_type", "error_reason"]

    # -------------------------------
    # Step 1: Null or Invalid Keys
    # -------------------------------
    # If service_date was an invalid string, to_date will make it null.
    null_key_df = df.filter(
        col("vin").isNull() | 
        (col("vin") == "") |
        col("service_date").isNull()
    ).withColumn("error_reason", lit("Null or Invalid Key"))

    valid_df = df.filter(
        col("vin").isNotNull() & 
        (col("vin") != "") &
        col("service_date").isNotNull()
    )

    # -------------------------------
    # Step 2: Duplicates (Exact Match)
    # -------------------------------
    # If the same vehicle has the same service on the same date multiple times,
    # keep the most recent one (based on ingestion_time) and send the rest to errors.
    dup_window = Window.partitionBy("vin", "service_date", "service_type").orderBy(desc("ingestion_time"))
    
    ranked_df = valid_df.withColumn("rn", row_number().over(dup_window))

    dup_records = ranked_df.filter(col("rn") > 1) \
        .withColumn("error_reason", lit("Duplicate Maintenance Record")) \
        .drop("rn")

    clean_df = ranked_df.filter(col("rn") == 1).drop("rn")

    # -------------------------------
    # Combine Errors
    # -------------------------------
    error_df = null_key_df \
        .unionByName(dup_records, allowMissingColumns=True) \
        .select(common_cols)

    # -------------------------------
    # Write SILVER
    # -------------------------------
    
    # Write errors
    error_df.write.format("delta").mode("append").save(ERROR_PATH)

    # Write clean data partitioned by year and month of service_date for efficiency
    clean_df = clean_df \
        .withColumn("service_year", year(col("service_date"))) \
        .withColumn("service_month", month(col("service_date")))

    if not DeltaTable.isDeltaTable(spark, CLEAN_PATH):
        logger.info(f"Creating {CLEAN_PATH}...")
        clean_df.write.format("delta").mode("overwrite") \
            .partitionBy("service_year", "service_month") \
            .save(CLEAN_PATH)
        spark.sql(f"ALTER TABLE delta.`{CLEAN_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        logger.info(f"Merging into {CLEAN_PATH}...")
        target_table = DeltaTable.forPath(spark, CLEAN_PATH)
        target_table.alias("t").merge(
            clean_df.alias("s"),
            "t.vin = s.vin AND t.service_date = s.service_date AND t.service_type = s.service_type"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # -------------------------------
    # Delta Lake Maintenance
    logger.info("Running OPTIMIZE and VACUUM on Silver Maintenance tables...")
    DeltaTable.forPath(spark, ERROR_PATH).optimize().executeCompaction()
    DeltaTable.forPath(spark, ERROR_PATH).vacuum(168.0)
    DeltaTable.forPath(spark, CLEAN_PATH).optimize().executeZOrderBy("vin", "service_date")
    DeltaTable.forPath(spark, CLEAN_PATH).vacuum(168.0)

    # -------------------------------
    # Verification
    # -------------------------------
    c_count = clean_df.count()
    e_count = error_df.count()
    
    print("✅ Vehicle Maintenance Silver complete")
    print(f"   Clean records: {c_count:,}")
    print(f"   Error records: {e_count:,}")

if __name__ == "__main__":
    vehicle_maintenance_silver()
