"""
Usage:
    spark-submit \\
      --master yarn \\
      --deploy-mode cluster \\
      --packages io.delta:delta-spark_2.12:3.2.0 \\
      --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \\
      --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \\
      --conf "spark.executor.instances=3" \\
      --conf "spark.executor.cores=2" \\
      --conf "spark.executor.memory=8g" \\
      --conf "spark.executor.memoryOverhead=1536" \\
      --conf "spark.driver.memory=4g" \\
      --conf "spark.databricks.delta.schema.autoMerge.enabled=true" \\
      --conf "spark.databricks.delta.optimizeWrite.enabled=true" \\
      --conf "spark.databricks.delta.autoCompact.enabled=true" \\
      telemetry_gold.py \\
      --topic telemetry_stream \\
      --starting-timestamp "2026-01-01T00:00:00"
"""

import argparse
import logging

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    row_number,
    when,
    lit,
    max  as spark_max,
    min  as spark_min,
    first,
    current_timestamp,
    date_format,
    to_date,
    sha2,
    concat_ws,
    broadcast,
    session_window,
    round
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("silver_to_gold")

# ─── MICROBATCH PROCESSING ──────────────────────────────────────────────────

def process_incidents_batch(
    batch_df: DataFrame,
    epoch_id: int,
    gold_dir: str,
    asset_history_path: str,
) -> None:
    """
    Handles the 'foreachBatch' logic for the violation stream.
    
    This function processes a microbatch of sessionized violations, computes
    deductions by joining with historical assignment data, and merges the 
    results into the Gold violation incidents table.
    """

    # epoch_id alone is sufficient to track progress in logs.
    log.info("Microbatch %s triggered.", epoch_id)

    if batch_df.isEmpty():
        log.info("Batch %s is empty — skipping.", epoch_id)
        return

    spark = batch_df.sparkSession
    incidents_dir = f"{gold_dir}/gold_violation_incidents"

    # ─── 1. ID GENERATION ────────────────────────────────────────────────────
    # Generate deterministic strike_id for exactly-once semantics.
    incidents_with_id = (
        batch_df
        .withColumn(
            "strike_id",
            sha2(
                concat_ws("|",
                    col("driver_id"),
                    col("vin"),
                    col("start_ts").cast("string")
                ),
                256,
            ),
        )
        .withColumn("incident_ts", col("start_ts"))
        .withColumn("month_year",  date_format(col("start_ts"), "yyyy-MM"))
    )

    # ─── 2. DUPLICATION CHECK ───────────────────────────────────────────────
    # Identify new sessions vs. sessions that already exist in Gold.
    if DeltaTable.isDeltaTable(spark, incidents_dir):
        existing_ids = (
            spark.read.format("delta").load(incidents_dir)
            .select("strike_id")           # only need the key for the anti-join
        )
        # Anti-join: keep only rows whose strike_id is NOT already in gold
        new_sessions = incidents_with_id.join(existing_ids, "strike_id", "left_anti")
    else:
        new_sessions = incidents_with_id

    # ─── 3. NEW SESSION PROCESSING ──────────────────────────────────────────
    processed_new_sessions = None

    if not new_sessions.isEmpty():

        # 3a. Filter: Exclude incidents for already-suspended drivers.
        standing_dir = f"{gold_dir}/gold_driver_standing"
        if DeltaTable.isDeltaTable(spark, standing_dir):
            suspended_drivers = (
                spark.read.format("delta").load(standing_dir)
                .filter(col("status") == "SUSPENDED")
                .select("driver_id", "suspension_date")
                .distinct()
            )
            new_sessions = (
                new_sessions
                .join(suspended_drivers, on="driver_id", how="left")
                .filter(
                    col("suspension_date").isNull()
                    | (col("incident_ts") < col("suspension_date"))
                )
                .drop("suspension_date")
            )

        if not new_sessions.isEmpty():

            # 3b. Optimization: Compute time bounds for targeted SCD2 lookup.
            bounds = new_sessions.select(
                spark_min("incident_ts").alias("min_ts"),
                spark_max("incident_ts").alias("max_ts"),
            ).collect()[0]

            # 3c. Rate Lookup: Load relevant slice of historical assignments.
            if not DeltaTable.isDeltaTable(spark, asset_history_path):
                log.error("Asset history table NOT found at %s. Skipping this batch as base rates are missing.", asset_history_path)
                return

            assignment_df = spark.read.format("delta").load(asset_history_path)
            historical_assignment = (
                assignment_df
                .filter(
                    (col("start_date") <= bounds["max_ts"])
                    & (col("end_date").isNull() | (col("end_date") >= bounds["min_ts"]))
                )
                .select(
                    "vin", "driver_id", "start_date", "end_date",
                    col("daily_rate").alias("base_rate"),
                )
            )

            # match are NOT silently dropped
            joined_new = new_sessions.join(
                broadcast(historical_assignment),
                (new_sessions.driver_id == historical_assignment.driver_id)
                & (new_sessions.vin      == historical_assignment.vin)
                & (new_sessions.incident_ts >= historical_assignment.start_date)
                & (
                    (new_sessions.incident_ts < historical_assignment.end_date)
                    | historical_assignment.end_date.isNull()
                ),
                "inner",
            ).drop(historical_assignment.vin, historical_assignment.driver_id)

            if not joined_new.isEmpty():

                months_in_batch = joined_new.select("month_year").distinct().cache()

                # 3d. Sequencer: Get historical strike counts from Gold.
                if DeltaTable.isDeltaTable(spark, incidents_dir):
                    historical_max = (
                        spark.read.format("delta").load(incidents_dir)
                        .join(broadcast(months_in_batch), "month_year")   # pruned read
                        .groupBy("driver_id", "month_year")
                        .agg(spark_max("strike_number").alias("prev_strikes"))
                    )
                else:
                    historical_max = spark.createDataFrame(
                        [], schema="driver_id STRING, month_year STRING, prev_strikes INT"
                    )

                # 3e. Windowing: Sequence new strikes within this batch.
                window_seq = Window.partitionBy("driver_id", "month_year").orderBy("incident_ts")
                batch_sequence = joined_new.withColumn(
                    "batch_strike_seq", row_number().over(window_seq)
                )

                enriched_new = (
                    batch_sequence
                    .join(broadcast(historical_max), ["driver_id", "month_year"], "left")
                    .fillna({"prev_strikes": 0})
                    .withColumn("strike_number", col("prev_strikes") + col("batch_strike_seq"))
                )

                # 3f. Penalty Engine: Calculate deductions and final adjusted rates.
                # FIX [MED-10]: original otherwise(0) meant suspended drivers kept their
                # full base_rate. Strike 10 now sets adjusted_rate = 0.0 explicitly.
                processed_new_sessions = (
                    enriched_new
                    .filter(col("strike_number") <= 10)
                    .withColumn("base_rate", round(col("base_rate"), 2))
                    .withColumn(
                        "deduction_amount",
                        round(
                            when(
                                col("strike_number") < 10,
                                col("base_rate") * col("strike_number") * 0.05,
                            ).otherwise(col("base_rate")),
                            2
                        )
                    )
                    .withColumn(
                        "adjusted_rate",
                        round(
                            when(
                                col("strike_number") >= 10,
                                lit(0.0),
                            ).otherwise(col("base_rate") - col("deduction_amount")),
                            2
                        )
                    )
                    .withColumn(
                        "status_after_strike",
                        when(col("strike_number") >= 10, lit("SUSPENDED")).otherwise(lit("ACTIVE")),
                    )
                    .withColumn("incident_created_ts", current_timestamp())
                    .select(
                        "strike_id", "vin", "driver_id", "strike_type",
                        "incident_ts", "max_speed", "zone_name",
                        "strike_number", "base_rate", "deduction_amount",
                        "adjusted_rate", "status_after_strike",
                        "incident_created_ts", "month_year",
                        "start_ts", "end_ts",
                    )
                )

                # 3g. Automated Suspension: Terminate assignments for strike 10.
                newly_suspended = (
                    processed_new_sessions
                    .filter(col("status_after_strike") == 'SUSPENDED')
                    .select("driver_id", "vin", to_date(col("incident_created_ts")).alias("suspended_date"))
                    .distinct()
                )

                if not newly_suspended.isEmpty():
                    log.info("Detected drivers reaching strike 10. Terminating active assignments.")
                    if DeltaTable.isDeltaTable(spark, asset_history_path):
                        asset_table = DeltaTable.forPath(spark, asset_history_path)
                        (asset_table.alias("target")
                            .merge(
                                newly_suspended.alias("source"),
                                "target.driver_id = source.driver_id AND target.vin = source.vin AND (target.end_date IS NULL OR target.end_date > source.suspended_date)"
                            )
                            .whenMatchedUpdate(set={"end_date": "source.suspended_date"})
                            .execute()
                        )
                    else:
                        log.warning("Asset history table not found at %s. Assignment termination skipped.", asset_history_path)

                months_in_batch.unpersist()

    # ─── 4. MERGE INTO GOLD ──────────────────────────────────────────────────
    if processed_new_sessions is None or processed_new_sessions.isEmpty():
        log.info("Batch %s: no new sessions to write.", epoch_id)
        return

    final_df = processed_new_sessions.drop("start_ts", "end_ts")

    # ─── 5. Sink ───────────────────────────────────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, incidents_dir):
        log.info("Creating gold_violation_incidents at %s", incidents_dir)
        (final_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("month_year")
            .option("delta.enableChangeDataFeed", "true")   # FIX [MED-9]
            .save(incidents_dir)
        )
    else:
        log.info("Merging into gold_violation_incidents at %s", incidents_dir)
        target = DeltaTable.forPath(spark, incidents_dir)

        # MERGE matches on strike_id; whenMatchedUpdate refreshes only the metrics
        # that can legitimately change when a session window extends: max_speed and
        # zone_name. All strike accounting columns are immutable once written.
        (
            target.alias("t")
            .merge(final_df.alias("s"), "t.strike_id = s.strike_id")
            .whenMatchedUpdate(set={
                "max_speed": "s.max_speed",
                "zone_name": "s.zone_name",
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

    log.info("Batch %s complete — incidents written to %s", epoch_id, incidents_dir)

# ─── STREAMING APP INITIALIZATION ───────────────────────────────────────────

def main() -> None:
    """
    Main entry point for the Silver-to-Gold streaming pipeline.
    """
    p = argparse.ArgumentParser(description="Silver → Gold streaming ETL")
    p.add_argument("--topic",       required=True)
    p.add_argument("--silver-dir",  default="s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group3-silver/data")
    p.add_argument("--gold-dir",    default="s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data")
    p.add_argument("--checkpoint",  default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/checkpoint-gold")
    p.add_argument("--asset-history", default="s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data/asset_history_scd2")
    # FIX [HIGH-8]: expose maxFilesPerTrigger so it can be tuned per environment.
    p.add_argument("--max-files-per-trigger", type=int, default=20,
                   help="Max Delta files consumed per streaming trigger. Default 20 (~1 hour of "
                        "hourly partitions). Lower for historical catch-up, raise for steady-state.")
    args = p.parse_args()

    spark = (
        SparkSession.builder
        .appName("silver_to_gold_streaming")
        .config("spark.sql.extensions",         "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled",   "true")   # AQE optimises joins inside foreachBatch
        .config("spark.databricks.delta.schema.autoMerge.enabled",  "true")
        .config("spark.databricks.delta.optimizeWrite.enabled",     "true")
        .config("spark.databricks.delta.autoCompact.enabled",       "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver_topic_dir  = args.silver_dir.rstrip("/")  + f"/{args.topic}"
    gold_dir          = args.gold_dir.rstrip("/")
    checkpoint_dir    = args.checkpoint.rstrip("/")
    asset_history_path = args.asset_history

    log.info("SILVER → GOLD STREAMING  |  topic: %s", args.topic)
    log.info("Silver : %s", silver_topic_dir)
    log.info("Gold   : %s", gold_dir)

    # ─── 1. PREREQUISITE CHECK ───────────────────────────────────────────────
    # Ensure source (Silver) and metadata (Asset History) tables exist.
    if not DeltaTable.isDeltaTable(spark, silver_topic_dir):
        log.warning("Silver Delta table NOT found at %s. "
                    "Ensure the Silver job has run and initialized the table first. "
                    "Exiting gracefully.", silver_topic_dir)
        return

    if not DeltaTable.isDeltaTable(spark, asset_history_path):
        log.warning("Asset History SCD2 table NOT found at %s. "
                    "Prerequisite data missing. Exiting gracefully.", asset_history_path)
        return

    # ─── 2. STREAM DEFINITION ────────────────────────────────────────────────
    # args.max_files_per_trigger (default 20).
    stream_reader = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", str(args.max_files_per_trigger))
    )

    silver_stream = stream_reader.load(silver_topic_dir)

    # ─── 3. VIOLATION FILTERING ──────────────────────────────────────────────
    violations_stream = (
        silver_stream
        .filter(col("is_violation") == True)
        .select(
            col("driver_id"),
            col("vin"),
            col("event_timestamp").alias("event_ts"),
            col("speed"),
            when(col("breached_zone_name").isNotNull(), col("breached_zone_name"))
                .otherwise(lit(None))
                .alias("zone_name"),
            when(
                col("is_speeding") & col("is_zone_breach"),
                lit("ZONE_BREACH_OVERSPEEDING"),
            )
            .when(col("is_zone_breach"), lit("ZONE_BREACH"))
            .when(col("is_speeding"),    lit("OVERSPEEDING"))
            .otherwise(lit(None))
            .alias("strike_type"),          
        )
    )

    # ─── 4. SESSIONIZATION ──────────────────────────────────────────────────
    # Apply a 5-minute session window to group related violation events.
    sessionized_stream = (
        violations_stream
        .withWatermark("event_ts", "10 minutes")    
        .groupBy(
            col("driver_id"),
            col("vin"),
            session_window(col("event_ts"), "5 minutes"),
        )
        .agg(
            spark_max(col("speed")).alias("max_speed"),
            first(col("zone_name"),    ignorenulls=True).alias("zone_name"),
            spark_max(col("strike_type")).alias("strike_type"),  
        )
        .select(
            col("driver_id"),
            col("vin"),
            col("strike_type"),
            col("session_window.start").alias("start_ts"),
            col("session_window.end").alias("end_ts"),
            col("max_speed"),
            col("zone_name"),
        )
    )

    # ─── 5. EXECUTION ───────────────────────────────────────────────────────
    # Trigger the stream with a foreachBatch sink to Gold.
    query = (
        sessionized_stream.writeStream
        .outputMode("append")                  
        .foreachBatch(
            lambda batch_df, epoch_id: process_incidents_batch(
                batch_df, epoch_id, gold_dir, asset_history_path
            )
        )
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="1 minute")
        .start()
    )

    log.info("Streaming query started. Awaiting termination.")
    query.awaitTermination()

if __name__ == "__main__":
    main()