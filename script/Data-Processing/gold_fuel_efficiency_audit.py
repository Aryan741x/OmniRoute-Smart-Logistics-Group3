import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_helper import get_spark
from config import SILVER_FUEL_DISTANCE as DISTANCE_PATH, SILVER_REGISTRY as REGISTRY_PATH, SILVER_ASSIGNMENT as ASSIGNMENT_PATH, SILVER_MAINT_CLEAN as MAINTENANCE_PATH, GOLD_FUEL_AUDIT as GOLD_PATH
from delta.tables import DeltaTable
# Initialize the Spark session for the Gold layer fuel efficiency audit.
spark = get_spark("Gold - Fuel Efficiency Audit")

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fuel_efficiency_audit():

    print("=== Starting Gold - Fuel Efficiency Audit ===")

    # Step 0: Ensure all prerequisite Silver tables are available before starting the audit.
    required_tables = {
        "Fuel Distance": DISTANCE_PATH,
        "Vehicle Registry": REGISTRY_PATH,
        "Vehicle Assignment": ASSIGNMENT_PATH,
    }
    for name, path in required_tables.items():
        if not DeltaTable.isDeltaTable(spark, path):
            msg = (f"Required table '{name}' not found at {path}. "
                   f"Cannot run fuel efficiency audit.")
            logger.error(msg)
            raise RuntimeError(msg)

    # Step 1: Load data from our Silver layer tables.
    dist_df = spark.read.format("delta").load(DISTANCE_PATH).select(
        upper(trim(col("vin"))).alias("vin"),
        col("timestamp"),
        col("fuel_liters"),
        col("distance"),
        col("mileage"),
        to_date(col("timestamp")).alias("txn_date")
    )

    # Load registry and assignment data. These tables are small enough to be broadcasted to all worker nodes to speed up joins.
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

    # Load maintenance data if available. This isn't critical, but we use it to avoid penalizing drivers if their vehicle was in the shop.
    maintenance_df = None
    if DeltaTable.isDeltaTable(spark, MAINTENANCE_PATH):
        maintenance_df = spark.read.format("delta").load(MAINTENANCE_PATH).select(
            upper(trim(col("vin"))).alias("vin"),
            col("service_date")
        ).distinct()
    else:
        logger.warning("Maintenance table not found — will skip maintenance day exclusion")

    # Check our row counts to ensure we actually have data to process.
    dist_count = dist_df.count()
    reg_count = registry_df.count()
    asg_count = assignment_df.count()
    maint_count = maintenance_df.count() if maintenance_df is not None else 0

    print(f"   Distance records:    {dist_count:,}")
    print(f"   Registry VINs:       {reg_count:,}")
    print(f"   Assignment records:  {asg_count:,}")
    print(f"   Maintenance records: {maint_count:,}")

    if dist_count == 0:
        msg = "No fuel distance records found. Nothing to audit."
        logger.error(msg)
        raise RuntimeError(msg)
    if reg_count == 0:
        msg = " Registry is empty. Cannot validate VINs."
        logger.error(msg)
        raise RuntimeError(msg)
    if asg_count == 0:
        msg = "Assignment table is empty. Cannot validate drivers."
        logger.error(msg)
        raise RuntimeError(msg)

    # Step 2: Cross-reference with the vehicle registry.
    # This filters out any "ghost" vehicles that show up in the fuel logs but aren't in our official system.
    validated_df = dist_df.join(
        broadcast(registry_df),
        on="vin",
        how="inner"
    )
    after_registry = validated_df.count()
    print(f"   After registry join (ghost VINs removed): {after_registry:,}")

    if after_registry == 0:
        msg = "No records survived registry join (no matching VINs). Cannot proceed."
        logger.error(msg)
        raise RuntimeError(msg)

    # Step 3: Check against vehicle assignments.
    # We only want to audit vehicles that were officially assigned to a driver on the day they were fueled.
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
        msg = " No records survived assignment join. Cannot proceed."
        logger.error(msg)
        raise RuntimeError(msg)

    # Step 4: Ignore weekend fuel-ups, as driving patterns and rules may differ on Saturdays and Sundays.
    validated_df = validated_df.filter(
        ~dayofweek(col("txn_date")).isin(1, 7)
    )
    after_weekend = validated_df.count()
    print(f"   After weekend exclusion: {after_weekend:,}")

    # Step 5: Filter out any days where the vehicle was recorded as being in the shop for maintenance.
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
        logger.warning(" Skipping maintenance exclusion (no maintenance data available)")
        print("   Maintenance exclusion: SKIPPED (no data)")

    # Step 6: Calculate total fuel and distance per vehicle for each day.
    # This aggregates multiple fill-ups that might happen on the same day.
    daily_df = validated_df.groupBy("vin", "model", "fuel_type","txn_date").agg(
        sum("distance").alias("total_distance"),
        sum("fuel_liters").alias("total_fuel")
    )

    daily_df = daily_df.withColumn(
        "km_per_liter",
        round(col("total_distance") / col("total_fuel"), 4)
    )

    # Step 7: Establish a fair fuel efficiency baseline.
    # We calculate a 30-day rolling median for each vehicle model.
    # The median is used instead of the average to prevent extreme outliers (like unusual highway trips) from skewing the baseline.
    baseline_window = Window.partitionBy("model","fuel_type") \
        .orderBy(unix_date(col("txn_date"))) \
        .rangeBetween(-30, -1)

    daily_df = daily_df.withColumn(
        "baseline_kmpl",
        round(percentile_approx(col("km_per_liter"), 0.5).over(baseline_window), 4)
    )

    # Step 8: Evaluate the daily efficiency against the baseline.
    # A vehicle is flagged for audit if its efficiency drops below 88% of its model's median (a 12% drop).
    audit_df = daily_df.withColumn(
        "status",
        when(col("km_per_liter").isNull(), lit("INSUFFICIENT_DATA"))
        .when(col("baseline_kmpl").isNull(), lit("INSUFFICIENT_DATA"))
        .when(col("km_per_liter") < col("baseline_kmpl") * 0.88, lit("FLAGGED"))
        .otherwise(lit("OK"))
    )

    # Step 9: Format our final output dataset to include only the required columns.
    gold_df = audit_df.select(
        col("vin"),
        col("model"),
        col("txn_date").alias("audit_date"),
        col("km_per_liter"),
        col("baseline_kmpl"),
        col("status"),
        col("fuel_type"),
    )

    # Step 10: Upsert the audit results into the Gold Delta table.
    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        logger.info(f"Creating {GOLD_PATH}...")
        gold_df.write.format("delta").mode("overwrite") \
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

    # Optimize the Gold table structure for querying and remove obsolete files.
    DeltaTable.forPath(spark, GOLD_PATH).optimize().executeZOrderBy("vin", "audit_date")
    DeltaTable.forPath(spark, GOLD_PATH).vacuum(168.0)

    # Final step: Print a summary report of the audit results to the console.
    total   = gold_df.count()
    flagged = gold_df.filter(col("status") == "FLAGGED").count()
    ok      = gold_df.filter(col("status") == "OK").count()
    insuf   = gold_df.filter(col("status") == "INSUFFICIENT_DATA").count()

    print(f"\nFuel Efficiency Audit Gold complete")
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

