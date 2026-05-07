import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pyspark.sql.functions import current_timestamp, current_date, lit
import logging
from datetime import datetime
from spark_helper import get_spark
from s3_utils import list_csv_files, read_csv_header, compute_file_checksum, move_file, path_exists, ensure_directory, get_filename
from config import INCOMING_MAINTENANCE, BRONZE_MAINTENANCE, ARCHIVE_SCHEMA_MAINTENANCE, METADATA_MAINTENANCE
from delta.tables import DeltaTable
from pyspark.sql.functions import md5, concat_ws

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = get_spark("Maintenance Bronze Ingestion")

REQUIRED_COLUMNS = ["vin", "service_date", "service_type"]


def normalize_column_name(column_name):
    return column_name.strip().lower()


def load_processed_manifest():
    if not path_exists(METADATA_MAINTENANCE):
        return None
    try:
        return spark.read.parquet(METADATA_MAINTENANCE)
    except Exception:
        return None


def validate_csv_header(file_path):
    try:
        header = read_csv_header(file_path)
        if not header:
            return False, "EMPTY_FILE"

        normalized_header = [normalize_column_name(column) for column in header]
        missing_required = [column for column in REQUIRED_COLUMNS if column not in normalized_header]

        if missing_required:
            return False, f"MISSING_REQUIRED_COLUMNS:{','.join(missing_required)}"

        return True, "OK"
    except Exception as exc:
        return False, f"UNREADABLE_FILE:{exc}"


def archive_invalid_file(file_path, reason):
    move_file(file_path, ARCHIVE_SCHEMA_MAINTENANCE)
    logger.warning(f"⚠️  Archived invalid file {get_filename(file_path)} because {reason}")


def append_manifest_records(records):
    if not records:
        return
    ensure_directory(METADATA_MAINTENANCE)
    manifest_df = spark.createDataFrame(records)
    manifest_df.write.mode("append").parquet(METADATA_MAINTENANCE)


def maintenance_bronze():
    try:
        ensure_directory(BRONZE_MAINTENANCE)
        ensure_directory(ARCHIVE_SCHEMA_MAINTENANCE)
        ensure_directory(METADATA_MAINTENANCE)

        # ====================================
        # 1. Validate incoming files one by one
        # ====================================
        incoming_files = list_csv_files(INCOMING_MAINTENANCE)
        logger.info(f"Found {len(incoming_files)} incoming CSV files")

        processed_manifest_df = load_processed_manifest()
        processed_checksums = set()
        if processed_manifest_df is not None:
            processed_checksums = {
                row["file_checksum"]
                for row in processed_manifest_df.select("file_checksum").distinct().collect()
            }

        valid_files = []
        rejected_files = []
        skipped_files = []
        manifest_rows = []

        for file_path in incoming_files:
            file_checksum = compute_file_checksum(file_path)

            if file_checksum in processed_checksums:
                skipped_files.append(file_path)
                logger.info(f"Skipping already processed file {get_filename(file_path)}")
                continue

            is_valid, reason = validate_csv_header(file_path)
            if is_valid:
                valid_files.append(file_path)
                manifest_rows.append({
                    "file_name": get_filename(file_path),
                    "file_path": file_path,
                    "file_checksum": file_checksum,
                    "file_status": "PROCESSED",
                    "processed_at": datetime.utcnow().isoformat()
                })
            else:
                rejected_files.append((file_path, reason))
                archive_invalid_file(file_path, reason)
                manifest_rows.append({
                    "file_name": get_filename(file_path),
                    "file_path": file_path,
                    "file_checksum": file_checksum,
                    "file_status": f"REJECTED:{reason}",
                    "processed_at": datetime.utcnow().isoformat()
                })

        if not valid_files:
            logger.info("No valid maintenance CSV files found to ingest")
            append_manifest_records(manifest_rows)
            print("\n===== VEHICLE MAINTENANCE BRONZE SUMMARY =====")
            print(f"✅ Valid files: 0")
            print(f"✅ Rejected files: {len(rejected_files)}")
            print(f"✅ Skipped already processed files: {len(skipped_files)}")
            print("✅ Vehicle Maintenance Bronze Ingested")
            return

        logger.info(f"Valid files accepted: {len(valid_files)}")

        # ====================================
        # 2. Read accepted CSV files
        # ====================================
        df = None
        for file_path in valid_files:
            current_df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
            current_df = current_df.withColumn("source_file", lit(get_filename(file_path))) if "source_file" not in current_df.columns else current_df
            df = current_df if df is None else df.unionByName(current_df, allowMissingColumns=True)

        # ====================================
        # 3. Add metadata & Row Hash
        # ====================================
        df = df \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("ingestion_date", current_date()) \
            .withColumn("_row_hash", md5(concat_ws("||", 
                df["vin"], df["service_date"], df["service_type"]
            )))

        initial_count = df.count()
        logger.info(f"Initial CSV rows across valid files: {initial_count}")

        # ====================================
        # 4. Idempotency (row-level, scalable)
        # ====================================
        existing_today_hashes = None
        if DeltaTable.isDeltaTable(spark, BRONZE_MAINTENANCE):
            try:
                existing_today_hashes = spark.read.format("delta").load(BRONZE_MAINTENANCE).filter(
                    col("ingestion_date") == current_date()
                ).select("_row_hash").distinct()
            except Exception as e:
                logger.warning(f"Could not read existing delta table: {e}")
        else:
            logger.info("Bronze Delta table not found, this is the first ingestion")

        if existing_today_hashes is not None:
            df_to_append = df.join(existing_today_hashes, on="_row_hash", how="left_anti")
        else:
            df_to_append = df

        append_count = df_to_append.count()
        duplicate_count = initial_count - append_count

        if duplicate_count > 0:
            logger.warning(f"⚠️  {duplicate_count} duplicate records already present for today")

        # ====================================
        # 5. Write to Bronze (append only unseen rows)
        # ====================================
        if append_count > 0:
            df_to_append.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("ingestion_date") \
                .save(BRONZE_MAINTENANCE)
            logger.info(f"✅ Appended {append_count} new rows to bronze")
            
            # Maintenance
            DeltaTable.forPath(spark, BRONZE_MAINTENANCE).optimize().executeCompaction()
            DeltaTable.forPath(spark, BRONZE_MAINTENANCE).vacuum(168.0)
        else:
            logger.info("✅ No new rows to append (idempotent run)")

        append_manifest_records(manifest_rows)

        print(f"\n===== VEHICLE MAINTENANCE BRONZE SUMMARY =====")
        print(f"✅ Valid files: {len(valid_files)}")
        print(f"✅ Rejected files: {len(rejected_files)}")
        print(f"✅ Skipped already processed files: {len(skipped_files)}")
        print(f"✅ Incoming rows: {initial_count}")
        print("✅ Vehicle Maintenance Bronze Ingested")

    except Exception as e:
        logger.error(f"❌ Error in maintenance_bronze: {str(e)}")
        raise

if __name__ == "__main__":
    maintenance_bronze()