"""
OmniRoute: Bronze to Silver Streaming ETL
------------------------------------------
This script orchestrates the refinement of vehicle telemetry from the Bronze 
(raw ingestion) layer to the Silver (cleaned & enriched) layer.

Key Transformations:
1. Data Cleaning: Handles nulls, deduplication, and GPS error filtering.
2. Geofencing: Utilizes Apache Sedona for spatial indexing (S2 Cell IDs) and 
   restricted zone breach detection via hash-based joins.
3. Asset Enrichment: Joins telemetry with SCD Type 2 asset assignment history
   to validate driver-vehicle pairings.
4. Violation Classification: Flags overspeeding and zone breaches.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    when,
    coalesce,
    lit,
    concat_ws,
    sha2,
    from_unixtime,
    current_timestamp,
    date_format,
    expr,
    min as spark_min,
    max as spark_max
)
from delta.tables import DeltaTable
from sedona.spark import ST_Point, SedonaContext

# ─── MICROBATCH PROCESSING ──────────────────────────────────────────────────

def process_batch(batch_df, epoch_id, silver_topic_path, restricted_zones_path, asset_history_path):
    """
    Handles the 'foreachBatch' logic for the Bronze-to-Silver stream.
    
    This function processes a microbatch of raw telemetry records, performs 
    spatial lookups for zone breaches, validates assignments, and merges
    the cleaned results into the Silver layer.
    """
    if batch_df.isEmpty():
        return
        
    spark = batch_df.sparkSession
    print(f"Processing Batch {epoch_id}:")
    
    # ─── 1. RESTRICTED ZONE INITIALIZATION ──────────────────────────────────
    # Load and validate restricted zones from JSON.
    try:
        raw_restricted_zones = spark.read.option("multiline", "true").json(restricted_zones_path)
        if raw_restricted_zones.isEmpty():
            print(f"WARNING: No valid restricted zones found in {restricted_zones_path}!!")
            return
    except Exception as e:
        print(f"ERROR: Could not load restricted zones from {restricted_zones_path}: {e}")
        return

    print(f"Loaded restricted zones from {restricted_zones_path}.")

    raw_restricted_zones_dedup = raw_restricted_zones.dropDuplicates(["min_lat", "max_lat", "min_long", "max_long"])
    restricted_zones = raw_restricted_zones_dedup.dropna(how = "any", subset=["min_lat", "max_lat", "min_long", "max_long"])
    restricted_zones = restricted_zones.filter(
    (col("min_lat") >= -90) & (col("max_lat") <= 90) &
    (col("min_long") >= -180) & (col("max_long") <= 180) &
    (col("min_lat") < col("max_lat")) &
    (col("min_long") < col("max_long")) &
    ~( (col("min_lat") == 0) & (col("max_lat") == 0) & 
       (col("min_long") == 0) & (col("max_long") == 0) )
    )

    restricted_zones = restricted_zones.withColumn("zone_id", sha2(
        concat_ws("|",
                  col("min_lat").cast("string"),
                  col("max_lat").cast("string"),
                  col("min_long").cast("string"),
                  col("max_long").cast("string")
        ),
        256
    ))


    # ─── 2. SPATIAL INDEXING (ZONES) ────────────────────────────────────────
    # Use Sedona S2 Cell IDs to index zones for high-performance hash joins.
    restricted_zones = restricted_zones.withColumn(
        "geom", 
        expr("ST_MakeEnvelope(min_long, min_lat, max_long, max_lat)")
    ).withColumn(
        "geo_key", 
        # Returns an array of S2 cells at zoom level 15
        expr("ST_S2CellIDs(geom, 15)")  # Returns an array of overlapping hashes
    ).withColumn(
        "geo_key", 
        expr("explode(geo_key)") 
    )

    # ─── 3. DEDUPLICATION & CLEANING ────────────────────────────────────────
    # Apply deterministic ID generation and filter invalid GPS/telemetry data.
    dedup_df = (
        batch_df.withColumns({
            "lat_rounded" : col("lat").cast("decimal(10,6)"),
            "long_rounded" : col("long").cast("decimal(10,6)") 
        })
        .dropDuplicates(["vin", "kafka_timestamp", "lat_rounded", "long_rounded"])
    ).drop("lat_rounded", "long_rounded")

    df_with_event_id = dedup_df.withColumn(
        "event_id",
        sha2(
            concat_ws("|",
                      col("vin"),
                      col("kafka_timestamp").cast("string"),
                      coalesce(col("lat").cast("string"), lit("")),
                      coalesce(col("long").cast("string"), lit("")),
            ),
            256,
        )
    )

    # Basic cleaning
    cleaned_df = df_with_event_id.dropna(how = "any", subset=["driver_id", "vin", "lat", "long"])

    # Additional cleaning: flag records with lat=0 and long=0 as GPS errors
    cleaned_df = cleaned_df.withColumn("error_reason", 
        when((col("lat") == 0) & (col("long") == 0), "GPS_ZERO_ERROR")
         .when((col("lat") < -90) | (col("lat") > 90) | 
               (col("long") < -180) | (col("long") > 180), "GPS_OUT_OF_RANGE")
         .when(col("speed") < 0, "NEGATIVE_SPEED_ERROR")
         .otherwise(None)
    )
    
    # Separate out error records
    error_df = cleaned_df.filter(col("error_reason").isNotNull())
    cleaned_df = cleaned_df.filter(col("error_reason").isNull()).drop("error_reason")

    # Write error records to a separate Delta table
    if not error_df.isEmpty():
        error_df_path = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/archive/telemetry_error_records/"
        (error_df.write
        .format("delta")
        .mode("append")
        .partitionBy("error_reason")
        .save(error_df_path)
        )

    # Final select and rename columns for silver table
    telemetry_df = cleaned_df.select(
        "event_id",
        "driver_id",
        "vin",
        "lat",
        "long",
        "speed",
        "event_timestamp"
    ).withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))

    bounds = telemetry_df.select(
        spark_min("event_timestamp").alias("min_ts"),
        spark_max("event_timestamp").alias("max_ts")
    ).collect()[0]

    batch_min = bounds["min_ts"]
    batch_max = bounds["max_ts"]

    if batch_min is None or batch_max is None:
        return
        
    # Read Asset History freshly inside the batch so it picks up new SCD2 changes automatically
    if not DeltaTable.isDeltaTable(spark, asset_history_path):
        print(f"ERROR: Asset history table NOT found at {asset_history_path}. Skipping batch.")
        return

    assignment_df = spark.read.format('delta').load(asset_history_path)

    # Slice the historical assignment data
    historical_slice = assignment_df.filter(
        (col("start_date") <= lit(batch_max)) &
        (
            (col("end_date") >= lit(batch_min)) |
            (col("end_date").isNull())
        )
    )
    historical_slice = historical_slice.select(
        "vin",
        "driver_id",
        "start_date",
        "end_date"
    )

    t = telemetry_df.alias("t")
    a = historical_slice.alias("a")

    joined_df = t.join(
        broadcast(a),
        (col("t.vin") == col("a.vin")) &
        (col("t.event_timestamp") >= col("a.start_date")) &
        (
            (col("t.event_timestamp") < col("a.end_date")) |
            col("a.end_date").isNull()
        ),
        "left"
    )

    classified_df = joined_df.withColumn(
        "trip_type",
        when(
            col("a.vin").isNull(),  # no assignment found
            "GHOST_TRIP"
        ).when(
            col("t.driver_id") == col("a.driver_id"),
            "VALID"
        ).otherwise(
            "SHADOW_ASSIGNMENT"
        )
    ).drop(col("a.vin"), col("a.driver_id"))

    valid_df = classified_df.filter(col("trip_type") == "VALID")
    invalid_df = classified_df.filter(col("trip_type") != "VALID")

    if not invalid_df.isEmpty():
        invalid_df = invalid_df.withColumn(
            "audit_reason",
            when(col("trip_type") == "GHOST_TRIP", "No assignment found")
            .when(col("trip_type") == "SHADOW_ASSIGNMENT", "Driver mismatch")
            .otherwise("Unknown"),
        )

        invalid_df_path = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/archive/telemetry_invalid_records/"

        (invalid_df.write
        .format("delta")
        .mode("append")
        .partitionBy("event_date", "trip_type")
        .save(invalid_df_path)
        )

    valid_df = valid_df.select("t.*")

    if valid_df.isEmpty():
        return

    # ─── 5. GEOFENCING & BREACH DETECTION ────────────────────────────────────
    # Step 1: Generate S2 Cell IDs for telemetry points (Level 15).
    valid_df = valid_df.withColumn(
        "geo_key",
        expr("ST_S2CellIDs(ST_Point(long, lat), 15)[0]")
    )

    # Step 2: Broadcast Hash Join on geo_key (much faster than range join)
    # Spark will distribute the smaller restricted_zones table and join on equality
    intersected_df = valid_df.join(
        broadcast(restricted_zones),
        on="geo_key",
        how="left"
    )
    
    # Step 3: Precise BBox validation.
    final_df = intersected_df.select(
        "event_id",
        "driver_id",
        "vin",
        "lat",
        "long",
        "speed",
        "event_timestamp",
        "event_date",
        "zone_name",
        current_timestamp().alias("silver_ingestion_timestamp"),
        when(col("speed") > 110, True).otherwise(False).alias("is_speeding"),
        when(
            (col("zone_id").isNotNull()) & 
            (col("lat") >= col("min_lat")) & (col("lat") <= col("max_lat")) &
            (col("long") >= col("min_long")) & (col("long") <= col("max_long")), 
            True
        ).otherwise(False).alias("is_zone_breach"),
    ).withColumn(
        "breached_zone_name",
        when(col("is_zone_breach"), coalesce(col("zone_name"), lit("UNKNOWN_ZONE"))).otherwise(None)
    ).withColumn(
        "is_violation", (col("is_speeding") | col("is_zone_breach"))
    ).drop("geo_key", "min_lat", "max_lat", "min_long", "max_long", "zone_name")

    # ─── 6. MERGE INTO SILVER ────────────────────────────────────────────────
    abs_silver_path = silver_topic_path
    
    if not DeltaTable.isDeltaTable(spark, abs_silver_path):
        print(f"Table not found. Initializing Delta table at {abs_silver_path}")
        (final_df.write
         .format("delta")
         .mode("append")
         .partitionBy("event_date")
         .save(abs_silver_path))
    else:
        print(f"Merging into: {abs_silver_path}")
        target_table = DeltaTable.forPath(spark, abs_silver_path)
        
        merge_condition = """
            t.event_id = s.event_id AND
            t.event_date = s.event_date
        """
        
        (target_table.alias("t")
            .merge(final_df.alias("s"), merge_condition)
            .whenNotMatchedInsertAll()
            .execute()
        )

# ─── STREAMING APP INITIALIZATION ───────────────────────────────────────────

def main():
    """
    Main entry point for the Bronze-to-Silver streaming pipeline.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--topic", required=True)
    p.add_argument("--bronze-dir", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/bronze")
    p.add_argument("--silver-dir", default="s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group3-silver/data")
    p.add_argument("--checkpoint", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/checkpoint-silver")
    p.add_argument("--restricted-zones", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/incoming/Streaming-Data/restricted_zones.json")
    p.add_argument("--asset-history", default="s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data/asset_history_scd2")
    args = p.parse_args()
 
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.sedona.sql.SedonaSqlExtensions")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # 4. Sedona Configuration
       # .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0,org.apache.sedona:sedona-spark-4.1_2.13:1.9.0")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Enable Sedona SQL extension
    spark = SedonaContext.create(spark)

    bronze_topic_dir = args.bronze_dir.rstrip("/") + "/" + args.topic
    silver_topic_dir = args.silver_dir.rstrip("/") + "/" + f"{args.topic}"
    checkpoint_topic_dir = args.checkpoint.rstrip("/")
    restricted_zones_path = args.restricted_zones
    asset_history_path = args.asset_history

    print(f"Starting Bronze stream from {bronze_topic_dir}")

    # Prerequisite Check: Ensure Bronze table exists
    if not DeltaTable.isDeltaTable(spark, bronze_topic_dir):
        print(f"Bronze Delta table NOT found at {bronze_topic_dir}. "
              "Ensure the Bronze job has run and initialized the table first. "
              "Exiting gracefully.")
        return

    # ─── 1. SOURCE CONFIGURATION ─────────────────────────────────────────────
    # Configure the Bronze Delta stream.
    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", "100")
        .load(bronze_topic_dir)
    )

    bronze_stream = bronze_stream.withColumn(
        "event_timestamp", from_unixtime(col("kafka_timestamp")).cast("timestamp")
         ).withWatermark("event_timestamp", "24 hours"
    )
    
    # ─── 2. EXECUTION ────────────────────────────────────────────────────────
    # Trigger the stream with a foreachBatch sink for Silver processing.
    query = (
        bronze_stream.writeStream
        .foreachBatch(lambda batch_df, epoch_id: process_batch(batch_df, epoch_id, silver_topic_dir, restricted_zones_path, asset_history_path))
        .option("checkpointLocation", checkpoint_topic_dir)
        .trigger(processingTime="1 minute")
        .start()
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    main()

"""

spark-submit --driver-memory 4g --packages io.delta:delta-spark_2.12:3.2.0,org.apache.sedona:sedona-spark-4.1_2.13:1.7.0,org.datasyslab:geotools-wrapper:1.7.0-28.5 telemetry_silver.py --topic telemetry_stream
"""