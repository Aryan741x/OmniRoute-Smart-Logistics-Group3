import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, upper, trim, length, countDistinct, lit, current_date
import logging
from datetime import datetime
from spark_helper import get_spark
from config import BRONZE_REGISTRY as BRONZE_PATH, SILVER_REGISTRY as SILVER_PATH, ARCHIVE_REGISTRY_CONFLICTS as ARCHIVE_PATH
from delta.tables import DeltaTable
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# CONFIG: Partition Strategy
# ====================================
DEFAULT_PARTITIONS = int(os.getenv("SPARK_PARTITIONS", "8"))

spark = get_spark("Vehicle Registry Silver")

def registry_silver():

    # -----------------------------
    # 1. Read Bronze
    # -----------------------------
    df = spark.read.format("delta").load(BRONZE_PATH)

    # -----------------------------
    # Process only latest partition
    # -----------------------------
    latest_date = df.selectExpr("max(ingestion_date)").collect()[0][0]
    df = df.filter(col("ingestion_date") == latest_date)

    # -----------------------------
    # 2. Clean formatting
    # -----------------------------
    before_clean_count = df.count()
    current_year = datetime.now().year

    df = df \
        .withColumn("vin", upper(trim(col("vin")))) \
        .withColumn("model", trim(col("model"))) \
        .withColumn("fuel_type", upper(trim(col("fuel_type")))) \
        .withColumn("mfg_year", col("mfg_year").cast("int"))

    # Silver is the quality gate: keep only valid records.
    invalid_core_df = df.filter(
        col("vin").isNull() | (col("vin") == "") |
        col("model").isNull() | (col("model") == "") |
        col("fuel_type").isNull() | (col("fuel_type") == "") |
        col("mfg_year").isNull() |
        (length(col("vin")) != 8) |
        (col("mfg_year") > lit(current_year))
    )

    valid_df = df.subtract(invalid_core_df)

    invalid_vin_len_count = invalid_core_df.filter(length(col("vin")) != 8).count()
    invalid_year_count = invalid_core_df.filter(col("mfg_year") > lit(current_year)).count()
    null_or_blank_count = invalid_core_df.count() - invalid_vin_len_count - invalid_year_count

    if invalid_core_df.count() > 0:
        logger.warning(
            f"⚠️  Invalid registry rows dropped: {invalid_core_df.count()} "
            f"(null/blank={null_or_blank_count}, vin_length={invalid_vin_len_count}, year_gt_current={invalid_year_count})"
        )

    after_clean_count = valid_df.count()
    dropped_count = before_clean_count - after_clean_count

    # -------------------------------------------------
    # 2b. Archive conflicting records: same VIN + same year + different model
    # -------------------------------------------------
    conflict_keys = valid_df.groupBy("vin", "mfg_year").agg(
        countDistinct("model").alias("model_variants")
    ).filter(col("model_variants") > 1).select("vin", "mfg_year")

    conflict_df = valid_df.join(conflict_keys, on=["vin", "mfg_year"], how="inner") \
        .withColumn("archive_reason", lit("SAME_VIN_SAME_YEAR_DIFFERENT_MODEL")) \
        .withColumn("archived_at", current_date())

    conflict_count = conflict_df.count()
    if conflict_count > 0:
        conflict_df.write.format("delta").mode("append").save(ARCHIVE_PATH)
        logger.warning(f"⚠️  Archived {conflict_count} conflicting registry rows")

    df = valid_df.join(conflict_keys, on=["vin", "mfg_year"], how="left_anti")

    if dropped_count > 0:
        logger.warning(f"⚠️  Dropped {dropped_count} invalid registry rows during silver cleaning")

    # -----------------------------
    # 3. Deduplicate VIN
    # Keep latest mfg_year after conflict quarantine
    # -------- ----------------
    window = Window.partitionBy("vin").orderBy(col("mfg_year").desc())

    df = df.withColumn(
        "rn",
        row_number().over(window)
    ).filter(col("rn") == 1).drop("rn")

    # ====================================
    # 4. Data Quality Validation
    # ====================================
    null_check = df.filter(
        col("vin").isNull() | col("model").isNull() | col("fuel_type").isNull()
    ).count()
    
    if null_check > 0:
        logger.warning(f"⚠️  {null_check} records with null values detected")
    
    final_count = df.count()
    logger.info(f"Final registry records: {final_count}")

    # ====================================
    # 5. Optimize Partitioning & Merge Silver
    # ====================================
    df_partitioned = df.repartition(DEFAULT_PARTITIONS, col("vin"))
    
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        logger.info(f"Creating {SILVER_PATH}...")
        df_partitioned.write.format("delta").mode("overwrite") \
            .partitionBy("mfg_year") \
            .save(SILVER_PATH)
        spark.sql(f"ALTER TABLE delta.`{SILVER_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        logger.info(f"Merging into {SILVER_PATH}...")
        target_table = DeltaTable.forPath(spark, SILVER_PATH)
        target_table.alias("t").merge(
            df_partitioned.alias("s"),
            "t.vin = s.vin"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # Delta Lake Maintenance
    logger.info("Running OPTIMIZE and VACUUM on Silver Registry...")
    DeltaTable.forPath(spark, SILVER_PATH).optimize().executeZOrderBy("vin")
    DeltaTable.forPath(spark, SILVER_PATH).vacuum(168.0)

    print("\n===== REGISTRY SILVER SUMMARY =====")
    print(f"✅ Records before cleaning: {before_clean_count}")
    print(f"✅ Records dropped in cleaning: {dropped_count}")
    print(f"✅ Records archived due to conflicts: {conflict_count}")
    print(f"✅ Records processed: {final_count}")
    print(f"✅ Partitioned by VIN into {DEFAULT_PARTITIONS} partitions")
    print(f"✅ Registry Silver Ready")
if __name__ == "__main__":
    registry_silver()