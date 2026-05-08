import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from spark_helper import get_spark
from config import BRONZE_TRANSACTION as INPUT_PATH, ARCHIVE_FUEL_ERRORS as ERROR_PATH, SILVER_FUEL_CLEAN as CLEAN_PATH, SILVER_FUEL_DISTANCE as DIST_PATH
from delta.tables import DeltaTable
# logging
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# Initialize the Spark session for the Silver layer of the fuel transaction pipeline.
spark = get_spark("Fuel Transaction Silver")

def fuel_transaction_silver():

    # Load the raw data from the Bronze Delta table.
    bronze_df = spark.read.format("delta").load(INPUT_PATH)

    # Standardize data types and clean up string formats (e.g., uppercase VINs).
    df = bronze_df \
        .withColumn("transaction_id", col("transaction_id").cast("string")) \
        .withColumn("vin", upper(trim(col("vin")))) \
        .withColumn("fuel_liters", col("fuel_liters").cast("double")) \
        .withColumn("odometer_reading", col("odometer_reading").cast("double")) \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    common_cols = ["transaction_id","vin","fuel_liters","odometer_reading","timestamp","error_reason"]

    # Step 1: Flag any transactions where the fuel amount is zero or negative.
    invalid_fuel_df = df.filter(col("fuel_liters") <= 0) \
        .withColumn("error_reason", lit("Negative or zero fuel"))

    valid_df = df.filter(col("fuel_liters") > 0)

    # Step 2: Identify and separate records that have identical transaction IDs and timestamps.
    dup_df = valid_df.groupBy("transaction_id","timestamp") \
        .count().filter(col("count") > 1)

    dup_records = valid_df.join(dup_df, ["transaction_id","timestamp"]) \
        .withColumn("error_reason", lit("Duplicate txn+timestamp")) \
        .drop("count")

    valid_df = valid_df.join(dup_df, ["transaction_id","timestamp"], "left_anti")

    # Step 3: If there are multiple records for the same transaction, keep only the most recent one.
    window_txn = Window.partitionBy("transaction_id").orderBy(desc("timestamp"))

    ranked_df = valid_df.withColumn("rn", row_number().over(window_txn))

    duplicate_txn_df = ranked_df.filter(col("rn") > 1) \
        .withColumn("error_reason", lit("Old txn")) \
        .drop("rn")

    clean_df = ranked_df.filter(col("rn") == 1).drop("rn")

    # Step 4: Filter out records missing critical identifiers like transaction ID, VIN, or timestamp.
    null_key_df = clean_df.filter(
        col("transaction_id").isNull() |
        col("vin").isNull() |
        col("timestamp").isNull()
    ).withColumn("error_reason", lit("Null keys"))

    clean_df = clean_df.filter(
        col("transaction_id").isNotNull() &
        col("vin").isNotNull() &
        col("timestamp").isNotNull()
    )

    # Step 5: Remove any transactions that claim to have occurred in the future.
    future_df = clean_df.filter(col("timestamp") > current_timestamp()) \
        .withColumn("error_reason", lit("Future timestamp"))

    clean_df = clean_df.filter(col("timestamp") <= current_timestamp())

    # Step 6: Drop any remaining exact duplicates across the key columns.
    clean_df = clean_df.dropDuplicates([
        "vin", "timestamp", "transaction_id", "odometer_reading"
    ])

    # Step 7: Detect odometer rollbacks.
    # We use a cumulative maximum approach here instead of simply comparing with the previous reading.
    # This effectively catches cascading rollbacks in a single pass, which is crucial for preventing negative distances later.
    running_max_window = Window.partitionBy("vin") \
        .orderBy(col("timestamp"), col("transaction_id")) \
        .rowsBetween(Window.unboundedPreceding, -1)

    clean_df = clean_df.withColumn(
        "max_prev_odo",
        max("odometer_reading").over(running_max_window)
    )

    # Flag records where the odometer reading is suspiciously lower than a previous reading.
    odo_error_df = clean_df.filter(
        col("max_prev_odo").isNotNull() &
        (col("odometer_reading") < col("max_prev_odo"))
    ).withColumn("error_reason", lit("Odometer rollback")) \
     .drop("max_prev_odo")

    # Retain only the records with valid, forward-moving odometer readings.
    clean_df = clean_df.filter(
        col("max_prev_odo").isNull() |
        (col("odometer_reading") >= col("max_prev_odo"))
    ).drop("max_prev_odo")

    # Step 8: Look for "soft" duplicates - records with the same vehicle, time, and fuel amount but different IDs.
    soft_dup = clean_df.groupBy("vin","timestamp","fuel_liters") \
        .count().filter(col("count") > 1)

    soft_dup_records = clean_df.join(soft_dup, ["vin","timestamp","fuel_liters"]) \
        .withColumn("error_reason", lit("Soft duplicate")) \
        .drop("count")

    clean_df = clean_df.join(soft_dup, ["vin","timestamp","fuel_liters"], "left_anti")

    # Step 9: Ensure we have actual values for fuel volume and odometer reading.
    null_other = clean_df.filter(
        col("fuel_liters").isNull() |
        col("odometer_reading").isNull()
    ).withColumn("error_reason", lit("Null non-key"))

    clean_df = clean_df.filter(
        col("fuel_liters").isNotNull() &
        col("odometer_reading").isNotNull()
    )

    # Gather all the error records we've flagged across the different validation steps.
    error_df = invalid_fuel_df \
        .unionByName(dup_records, allowMissingColumns=True) \
        .unionByName(duplicate_txn_df, allowMissingColumns=True) \
        .unionByName(null_key_df, allowMissingColumns=True) \
        .unionByName(future_df, allowMissingColumns=True) \
        .unionByName(odo_error_df, allowMissingColumns=True) \
        .unionByName(soft_dup_records, allowMissingColumns=True) \
        .unionByName(null_other, allowMissingColumns=True) \
        .select(common_cols)

    # Save the invalid records to the error log table.
    error_df.write.format("delta").mode("append").save(ERROR_PATH)

    # Save the clean, validated data to our primary Silver table. If it's the first run, create the table; otherwise, merge.
    clean_df = clean_df.withColumn("date", to_date(col("timestamp")))

    if not DeltaTable.isDeltaTable(spark, CLEAN_PATH):
        logger.info(f"Creating {CLEAN_PATH}...")
        clean_df.write.format("delta").mode("overwrite") \
            .partitionBy("date") \
            .save(CLEAN_PATH)
        spark.sql(f"ALTER TABLE delta.`{CLEAN_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        logger.info(f"Merging into {CLEAN_PATH}...")
        target_table = DeltaTable.forPath(spark, CLEAN_PATH)
        target_table.alias("t").merge(
            clean_df.alias("s"),
            "t.transaction_id = s.transaction_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # Calculate the distance traveled since the last fuel transaction for each vehicle.
    dist_window = Window.partitionBy("vin").orderBy(
        col("timestamp"),
        col("transaction_id")
    )

    distance_df = clean_df.withColumn(
        "prev_odo",
        lag("odometer_reading").over(dist_window)
    ).filter(col("prev_odo").isNotNull())

    distance_df = distance_df.withColumn(
        "distance",
        col("odometer_reading") - col("prev_odo")
    )

    # As a final safety check, drop any records where the calculated distance is zero or negative.
    distance_df = distance_df.filter(col("distance") > 0)

    # Compute the vehicle's fuel efficiency (mileage) in kilometers per liter.
    distance_df = distance_df.withColumn(
        "mileage",
        col("distance") / col("fuel_liters")
    )

    if not DeltaTable.isDeltaTable(spark, DIST_PATH):
        logger.info(f"Creating {DIST_PATH}...")
        distance_df.write.format("delta").mode("overwrite").save(DIST_PATH)
        spark.sql(f"ALTER TABLE delta.`{DIST_PATH}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        logger.info(f"Merging into {DIST_PATH}...")
        target_table = DeltaTable.forPath(spark, DIST_PATH)
        target_table.alias("t").merge(
            distance_df.alias("s"),
            "t.transaction_id = s.transaction_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # Perform regular Delta Lake maintenance: OPTIMIZE to compact files (using Z-Ordering for better read performance) and VACUUM to remove stale files.
    print("Running OPTIMIZE and VACUUM on Silver Fuel tables...")
    
    # Optimize and vacuum the error table.
    DeltaTable.forPath(spark, ERROR_PATH).optimize().executeCompaction()
    DeltaTable.forPath(spark, ERROR_PATH).vacuum(168.0)
    
    # Optimize and vacuum the clean data table, clustering by vehicle and time.
    DeltaTable.forPath(spark, CLEAN_PATH).optimize().executeZOrderBy("vin", "timestamp")
    DeltaTable.forPath(spark, CLEAN_PATH).vacuum(168.0)
    
    # Optimize and vacuum the distance table, similarly clustered.
    DeltaTable.forPath(spark, DIST_PATH).optimize().executeZOrderBy("vin", "timestamp")
    DeltaTable.forPath(spark, DIST_PATH).vacuum(168.0)

    # Run a quick sanity check to log our final record counts and ensure no negative distances slipped through.
    neg_count = distance_df.filter(col("distance") < 0).count()
    total_count = distance_df.count()
    print(f"Fuel Transaction Silver complete")
    print(f"   Clean records: {clean_df.count()}")
    print(f"   Error records: {error_df.count()}")
    print(f"   Distance records: {total_count}")
    print(f"   Negative distances: {neg_count}")

if __name__ == "__main__":
    fuel_transaction_silver()