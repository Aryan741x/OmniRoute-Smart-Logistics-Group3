"""
OmniRoute: Kafka to Bronze Streaming Pipeline
----------------------------------------------
This script consumes vehicle telemetry data from a Kafka topic and writes it
to a Delta Lake Bronze table. It handles schema enforcement, metadata 
attachment, and partitioning by ingestion date.

Usage:
    spark-submit --packages ... consumer_spark.py --topic <topic_name>
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date,
    unix_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import argparse

def build_schema():
    """
    Defines the expected JSON schema for the incoming telemetry events.
    
    Returns:
        StructType: The PySpark schema for the telemetry payload.
    """
    return StructType(
        [
            StructField("driver_id", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("long", DoubleType(), True),
            StructField("speed", LongType(), True),
            StructField("vin", StringType(), True),
        ]
    )

def main(args):
    """
    Main entry point for the Kafka-to-Bronze streaming job.
    """
    spark = (
        SparkSession.builder.appName("kafka_to_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        # Optimization: Enable Delta Lake write features
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ─── 1. PREREQUISITE CHECK ────────────────────────────────────────────────
    # Verify Kafka connectivity before starting the stream to avoid silent failures.
    def check_kafka_connection(bootstrap_servers):
        import socket
        for server in bootstrap_servers.split(","):
            try:
                host, port = server.split(":")
                socket.create_connection((host, int(port)), timeout=5)
                return True
            except (socket.timeout, ConnectionRefusedError, socket.gaierror, ValueError):
                continue
        return False

    if not check_kafka_connection(args.bootstrap_servers):
        print(f"ERROR: Kafka bootstrap servers ({args.bootstrap_servers}) are NOT reachable. "
              "Ensure Kafka is running and accessible. Exiting gracefully.")
        return

    # ─── 2. DATA INGESTION ────────────────────────────────────────────────────
    # Configure and load the Kafka streaming source.
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", 60000)  # Throttling batch size for stability
        .load()
    )

    # ─── 3. TRANSFORMATION & DESERIALIZATION ─────────────────────────────────
    # Initial selection: cast binary key/value to string and extract Kafka metadata.
    raw = (
        kafka_df.select(
            col("value").cast("string").alias("value_str"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp_ms"),
        )
    )

    schema = build_schema()
    parsed = raw.withColumn("payload", from_json(col("value_str"), schema))

    # Flatten the JSON payload and enrich with ingestion metadata.
    flattened = (
        parsed.select(
            "value_str",
            "topic",
            "partition",
            "offset",
            unix_timestamp(col("kafka_timestamp_ms")).alias("kafka_timestamp"),
            "payload.*",
        )
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("ingestion_timestamp", unix_timestamp(col("ingestion_ts")))
        .withColumn("ingestion_date", to_date(col("ingestion_ts")))
        .drop("kafka_timestamp_ms", "ingestion_ts")
    )

    # ─── 4. DATA SINK (DELTA BRONZE) ─────────────────────────────────────────
    # Write the stream to Delta Lake Bronze table, partitioned by date.
    out_path = args.out_dir.rstrip("/") + "/" + args.topic

    query = (
        flattened.writeStream
        .format("delta")
        .option("path", out_path)
        .option("checkpointLocation", args.checkpoint)
        .partitionBy("ingestion_date")
        .option("maxRecordsPerFile", str(args.max_records_per_file))
        .trigger(processingTime=args.trigger)
        .outputMode("append")
        .start()
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--topic", required=True)
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--starting-offsets", dest="starting_offsets", default="earliest", help="earliest|latest or JSON offsets")
    p.add_argument("--out-dir", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/bronze", help="Base output dir for bronze")
    p.add_argument("--checkpoint", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/checkpoint-bronze", help="Checkpoint directory for Structured Streaming")
    p.add_argument("--trigger", dest="trigger", default="10 seconds", help="Processing trigger (e.g. '10 seconds')")
    p.add_argument("--max-records-per-file", dest="max_records_per_file", type=int, default=100000, help="Target max records per output file")
    args = p.parse_args()

    main(args)

'''
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.2.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --master local[*] \
    --driver-memory 4G \
    consumer_spark.py \
    --topic telemetry_stream \
    --bootstrap-servers localhost:9092
'''
