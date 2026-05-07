import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_helper import get_spark
from config import SILVER_FUEL_DISTANCE as DISTANCE_PATH, SILVER_REGISTRY as REGISTRY_PATH, SILVER_ASSIGNMENT as ASSIGNMENT_PATH, SILVER_MAINT_CLEAN as MAINTENANCE_PATH, GOLD_FUEL_AUDIT as GOLD_PATH
from delta.tables import DeltaTable
# -------------------------------
# Spark Session
# -------------------------------
spark = get_spark("Gold - Fuel Efficiency Audit")

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fuel_efficiency_audit():

    print("=== Starting Gold - Fuel Efficiency Audit ===")

    # =========================================
    # 0. Precondition: Verify required Silver tables exist
    # =========================================
    required_tables = {
        "Fuel Distance": DISTANCE_PATH,
        "Vehicle Registry": REGISTRY_PATH,
        "Vehicle Assignment": ASSIGNMENT_PATH,
    }
    for name, path in required_tables.items():
        if not DeltaTable.isDeltaTable(spark, path):
            msg = (f"❌ Required table '{name}' not found at {path}. "
                   f"Cannot run fuel efficiency audit.")
            logger.error(msg)
            raise RuntimeError(msg)

    # =========================================
    # 1. Load all Silver tables
    # =========================================
    dist_df = spark.read.format("delta").load(DISTANCE_PATH).select(
        upper(trim(col("vin"))).alias("vin"),
        col("timestamp"),
        col("fuel_liters"),
        col("distance"),
        col("mileage"),
        to_date(col("timestamp")).alias("txn_date")
    )

    # Small tables → broadcast for performance
    registry_df = spark.read.format("delta").load(REGISTRY_PATH).select(
        upper(trim(col("vin"))).alias("vin"),
        col("model"),
        col("fuel_type")
    ).dropDuplicates(["vin"])

    assignment_df = spark.read.format("delta").load(ASSIGNMENT_PATH).select(
        upper(trim(col("vin"))).alias("vin"),
        col("driver_id"),
        col("start_date"),
        col("end_date"),
        col("status")
    )

    # Non-critical: maintenance (used only for exclusion)
    maintenance_df = None
    if DeltaTable.isDeltaTable(spark, MAINTENANCE_PATH):
        maintenance_df = spark.read.format("delta").load(MAINTENANCE_PATH).select(
            upper(trim(col("vin"))).alias("vin"),
            col("service_date")
        ).distinct()
    else:
        logger.warning("⚠️  Maintenance table not found — will skip maintenance day exclusion")

    # Count & validate — skip if primary data is empty
    dist_count = dist_df.count()
    reg_count = registry_df.count()
    asg_count = assignment_df.count()
    maint_count = maintenance_df.count() if maintenance_df is not None else 0

    print(f"   Distance records:    {dist_count:,}")
    print(f"   Registry VINs:       {reg_count:,}")
    print(f"   Assignment records:  {asg_count:,}")
    print(f"   Maintenance records: {maint_count:,}")

    if dist_count == 0:
        msg = "❌ No fuel distance records found. Nothing to audit."
        logger.error(msg)
        raise RuntimeError(msg)
    if reg_count == 0:
        msg = "❌ Registry is empty. Cannot validate VINs."
        logger.error(msg)
        raise RuntimeError(msg)
    if asg_count == 0:
        msg = "❌ Assignment table is empty. Cannot validate drivers."
        logger.error(msg)
        raise RuntimeError(msg)

    # =========================================
    # 2. JOIN 1 - Validate VIN exists in registry
    #    (Removes ghost/phantom vehicles)
    #    BROADCAST registry (20K rows, ~small)
    # =========================================
    validated_df = dist_df.join(
        broadcast(registry_df),
        on="vin",
        how="inner"
    )
    after_registry = validated_df.count()
    print(f"   After registry join (ghost VINs removed): {after_registry:,}")

    if after_registry == 0:
        msg = "❌ No records survived registry join (no matching VINs). Cannot proceed."
        logger.error(msg)
        raise RuntimeError(msg)

    # =========================================
    # 3. JOIN 2 - Validate VIN has active assignment
    #    (Removes shadow drivers / unassigned vehicles)
    #    Match: txn_date falls between assignment start_date and end_date
    # =========================================
    validated_df = validated_df.join(
        broadcast(assignment_df),
        on=[
            validated_df["vin"] == assignment_df["vin"],
            validated_df["txn_date"] >= assignment_df["start_date"],
            (assignment_df["end_date"].isNull()) | (validated_df["txn_date"] < assignment_df["end_date"])
        ],
        how="inner"
    ).drop(assignment_df["vin"])

    after_assignment = validated_df.count()
    print(f"   After assignment join (shadow drivers removed): {after_assignment:,}")

    if after_assignment == 0:
        msg = "❌ No records survived assignment join. Cannot proceed."
        logger.error(msg)
        raise RuntimeError(msg)

    # =========================================
    # 4. Exclude Weekends (dayofweek: 1=Sun, 7=Sat)
    # =========================================
    validated_df = validated_df.filter(
        ~dayofweek(col("txn_date")).isin(1, 7)
    )
    after_weekend = validated_df.count()
    print(f"   After weekend exclusion: {after_weekend:,}")

    # =========================================
    # 5. JOIN 3 - Exclude Maintenance Days
    #    (Driver shouldn't be penalized for idling at workshop)
    #    BROADCAST maintenance (33K rows, ~small)
    #    Non-critical: if maintenance data is missing, skip exclusion
    # =========================================
    if maintenance_df is not None and maint_count > 0:
        validated_df = validated_df.join(
            broadcast(maintenance_df),
            on=[
                validated_df["vin"] == maintenance_df["vin"],
                validated_df["txn_date"] == maintenance_df["service_date"]
            ],
            how="left_anti"
        )
        after_maint = validated_df.count()
        print(f"   After maintenance exclusion: {after_maint:,}")
    else:
        logger.warning("⚠️  Skipping maintenance exclusion (no maintenance data available)")
        print("   Maintenance exclusion: SKIPPED (no data)")

    # =========================================
    # 6. Aggregate: daily mileage per VIN
    #    (A vehicle may have multiple fill-ups per day)
    # =========================================
    daily_df = validated_df.groupBy("vin", "model", "fuel_type","txn_date").agg(
        sum("distance").alias("total_distance"),
        sum("fuel_liters").alias("total_fuel")
    )

    daily_df = daily_df.withColumn(
        "km_per_liter",
        round(col("total_distance") / col("total_fuel"), 4)
    )

    # =========================================
    # 7. 30-Day Rolling Median Baseline Per Model
    #
    #    Why median instead of mean?
    #    - The mileage data is skewed (outliers from
    #      highway vs city driving). Median is robust
    #      to these outliers and gives a fairer baseline.
    #
    #    How: percentile_approx(col, 0.5) = median
    #    Window: all records for the same MODEL within
    #    the past 30 days (excluding the current day).
    # =========================================
    baseline_window = Window.partitionBy("model","fuel_type") \
        .orderBy(unix_date(col("txn_date"))) \
        .rangeBetween(-30, -1)

    daily_df = daily_df.withColumn(
        "baseline_kmpl",
        round(percentile_approx(col("km_per_liter"), 0.5).over(baseline_window), 4)
    )

    # =========================================
    # 8. Flag: FLAGGED if km_per_liter < baseline * 0.88
    #    (i.e., 12% worse than the model median)
    # =========================================
    audit_df = daily_df.withColumn(
        "status",
        when(col("km_per_liter").isNull(), lit("INSUFFICIENT_DATA"))
        .when(col("baseline_kmpl").isNull(), lit("INSUFFICIENT_DATA"))
        .when(col("km_per_liter") < col("baseline_kmpl") * 0.88, lit("FLAGGED"))
        .otherwise(lit("OK"))
    )

    # =========================================
    # 9. Final output columns (per BRD)
    # =========================================
    gold_df = audit_df.select(
        col("vin"),
        col("model"),
        col("fuel_type"),
        col("txn_date").alias("audit_date"),
        col("km_per_liter"),
        col("baseline_kmpl"),
        col("status")
    )

    # =========================================
    # 10. Write to Gold Layer (Merge)
    # =========================================
    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        logger.info(f"Creating {GOLD_PATH}...")
        gold_df.write.format("delta").mode("overwrite") \
            .partitionBy("status") \
            .save(GOLD_PATH)
        spark.sql(f"ALTER TABLE delta.`{GOLD_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        logger.info(f"Merging into {GOLD_PATH}...")
        target_table = DeltaTable.forPath(spark, GOLD_PATH)
        target_table.alias("t").merge(
            gold_df.alias("s"),
            "t.vin = s.vin AND t.audit_date = s.audit_date"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # Delta Lake Maintenance
    DeltaTable.forPath(spark, GOLD_PATH).optimize().executeZOrderBy("vin", "audit_date")
    DeltaTable.forPath(spark, GOLD_PATH).vacuum(168.0)

    # =========================================
    # 11. Summary
    # =========================================
    total   = gold_df.count()
    flagged = gold_df.filter(col("status") == "FLAGGED").count()
    ok      = gold_df.filter(col("status") == "OK").count()
    insuf   = gold_df.filter(col("status") == "INSUFFICIENT_DATA").count()

    print(f"\n✅ Fuel Efficiency Audit Gold complete")
    print(f"   Total audit records:    {total:,}")
    print(f"   FLAGGED:                {flagged:,}")
    print(f"   OK:                     {ok:,}")
    print(f"   INSUFFICIENT_DATA:      {insuf:,}")

    print("\n--- Sample FLAGGED Vehicles ---")
    gold_df.filter(col("status") == "FLAGGED") \
        .orderBy("audit_date") \
        .show(10, truncate=False)


if __name__ == "__main__":
    fuel_efficiency_audit()
