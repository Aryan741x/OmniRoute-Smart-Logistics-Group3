"""
OmniRoute: Kafka Data Ingestion Producer
-----------------------------------------
This script reads telemetry data stored as JSONL files in S3 and produces them
to a Kafka topic. It features persistent checkpointing to S3 to resume after
interruptions and supports idempotent producing for reliability.

Usage:
    python kafka_producer.py --s3-folder <s3_path> --topic <topic>
"""

import argparse
import json
import logging
import time
import os

from kafka import KafkaProducer
from smart_open import open
import boto3
from botocore.exceptions import ClientError

# ─── CONFIGURATION & LOGGING ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("kafka_producer")

# ─── S3 UTILITIES ─────────────────────────────────────────────────────────

def load_jsonl(s3_path):
    """
    Generator that yields parsed JSON objects from a JSONL file in S3.
    
    Args:
        s3_path (str): Full S3 URI of the file.
        
    Yields:
        dict: A single telemetry record.
    """
    with open(s3_path, 'r', encoding='utf-8') as fin:
        for line_number, line in enumerate(fin, start=1):
            stripped = line.strip()
            if not stripped: 
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                log.error(f"Invalid JSON on line {line_number}: {exc}")
                raise

# ─── KAFKA UTILITIES ──────────────────────────────────────────────────────

def build_producer(bootstrap_servers):
    """
    Initializes a KafkaProducer with robust delivery guarantees.
    
    Args:
        bootstrap_servers (list): List of Kafka brokers.
        
    Returns:
        KafkaProducer: Configured producer instance.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        enable_idempotence=True,    # Ensures exactly-once delivery per partition
        request_timeout_ms=30000,
        acks="all",                 # Wait for all replicas to acknowledge
        linger_ms=20,               # Batching delay for higher throughput
        batch_size=32768,           # 32KB batch limit
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
    )

# ─── CHECKPOINT MANAGEMENT ───────────────────────────────────────────────
s3 = boto3.client('s3')

def read_checkpoint(checkpoint_key, bucket):
    """
    Fetches the last known processing state from S3.
    
    Args:
        checkpoint_key (str): S3 key for the checkpoint JSON.
        bucket (str): S3 bucket name.
        
    Returns:
        dict: Checkpoint data including processed_files and current offsets.
    """
    try:
        response = s3.get_object(Bucket=bucket, Key=checkpoint_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content.strip())
        if "processed_files" not in data:
            data["processed_files"] = []
        return data
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {"processed_files": [], "current_file": None, "record_number": -1}
        raise
    except Exception:
        return {"processed_files": [], "current_file": None, "record_number": -1}

def write_checkpoint(checkpoint_key, processed_files, current_file, record_number, bucket):
    """
    Persists the current processing state to S3.
    """
    checkpoint_data = {
        "processed_files": processed_files,
        "current_file": current_file,
        "record_number": record_number
    }
    s3.put_object(
        Bucket=bucket,
        Key=checkpoint_key,
        Body=json.dumps(checkpoint_data)
    )

def get_s3_files(s3_folder):
    """
    Identifies all JSONL files within an S3 prefix using pagination.
    
    Args:
        s3_folder (str): S3 path (e.g., s3://bucket/prefix).
        
    Returns:
        list: Sorted list of S3 URIs.
    """
    if s3_folder.startswith('s3://'):
        s3_folder = s3_folder[5:]
    bucket = s3_folder.split('/')[0]
    prefix = '/'.join(s3_folder.split('/')[1:])
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    files = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.jsonl'):
                    files.append(f"s3://{bucket}/{obj['Key']}")
    return sorted(files)

# ─── MAIN PROCESSING LOGIC ───────────────────────────────────────────────

def send_jsonl_to_kafka(s3_folder, topic, bootstrap_servers, key_field, pause_seconds, checkpoint_key):
    """
    Orchestrates the transfer of data from S3 to Kafka with state management.
    """
    if bootstrap_servers is None:
        servers = os.environ.get("KAFKA_BROKER_SERVERS")
        bootstrap_servers = str(servers).split(",")
    
    producer = build_producer(bootstrap_servers)

    log.info(f"Scanning S3 folder: {s3_folder}")
    
    # 1. Initialize metadata and find files
    folder_path = s3_folder[5:] if s3_folder.startswith('s3://') else s3_folder
    bucket = folder_path.split('/')[0]
    
    files_to_process = get_s3_files(s3_folder)
    
    if not files_to_process:
        log.info("No JSONL files found in the specified folder.")
        producer.close()
        return

    # 2. Load Checkpoint
    checkpoint = read_checkpoint(checkpoint_key, bucket)
    processed_files = checkpoint.get("processed_files", [])
    last_file = checkpoint.get("current_file")
    last_record = checkpoint.get("record_number", -1)

    log.info(f"Checkpoint loaded: Last File = {last_file}, Last Record = {last_record}, Processed Files Count = {len(processed_files)}")

    # Filter files that have already been completely processed
    pending_files = [f for f in files_to_process if f not in processed_files and f != last_file]
    
    # If the last partially processed file is still around, it should be processed first
    if last_file in files_to_process and last_file not in processed_files:
        pending_files.insert(0, last_file)

    total_sent = 0

    for current_file in pending_files:
        logging.info("Processing file: %s", current_file)
        
        start_record = last_record if (current_file == last_file) else -1
        
        file_records_sent = 0
        current_record_index = -1
        for record_number, record in enumerate(load_jsonl(current_file)):
            current_record_index = record_number
            
            # Skip records already processed according to checkpoint
            if record_number <= start_record:
                continue

            key = None
            if key_field is not None:
                key = record.get(key_field)

            # Asynchronous send
            future = producer.send(topic, key=key, value=record)
            
            # Callback for delivery failures
            def on_error(excp, rec_num=record_number, fname=current_file):
                log.error("Failed to send record %s from file %s: %s", rec_num, fname, excp)
            
            future.add_errback(on_error)

            file_records_sent += 1
            total_sent += 1

            # Periodically flush and checkpoint (every 100 records)
            if file_records_sent % 100 == 0:
                producer.flush()
                write_checkpoint(checkpoint_key, processed_files, current_file, record_number, bucket)
                log.info("Sent %s records from %s so far...", file_records_sent, current_file)

            if pause_seconds > 0:
                time.sleep(pause_seconds)

        # File is finished
        producer.flush()
        
        # Add to processed files and clear current_file state
        processed_files.append(current_file)
        # Update last_file and last_record so they don't affect the next file
        last_file = None
        last_record = -1
        
        write_checkpoint(checkpoint_key, processed_files, None, -1, bucket)

        logging.info("Finished processing file %s. Total records sent from this file: %s", current_file, file_records_sent)

    producer.flush()
    producer.close()
    logging.info("Finished sending %s total records across all files.", total_sent)

def parse_args():
    parser = argparse.ArgumentParser(description="Read JSONL files from a folder and send each record to Kafka.")
    parser.add_argument("--s3-folder", default="s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/incoming/Streaming-Data/newData", help="Path to the input S3 folder containing JSONL files")
    parser.add_argument("--topic", default="telemetry_stream", help="Kafka topic to send records to")
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka bootstrap servers (comma-separated)",
    )
    parser.add_argument(
        "--key-field",
        default="driver_id",
        help="Optional JSON field to use as the Kafka message key (default: driver_id)",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.0,
        help="Optional pause between records (seconds)",
    )
    parser.add_argument(
        "--checkpoint-key",
        default="poc-bootcamp-group3-bronze/data/metadata/Streaming-Data/producer/kafka_producer_checkpoint.json",
        help="S3 key to store last processed file and record number",
    )
    return parser.parse_args()

if __name__ == "__main__":
    try:
        args = parse_args()

        send_jsonl_to_kafka(
            s3_folder=args.s3_folder,
            topic=args.topic,
            bootstrap_servers=args.bootstrap_servers,
            key_field=args.key_field,
            pause_seconds=args.pause_seconds,
            checkpoint_key=args.checkpoint_key
        )

    except Exception as e:
        logging.exception("Kafka producer crashed: %s", e)
        raise

'''
for s3:
python kafka_producer.py \
    --s3-folder s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze/data/incoming/Streaming-Data/newData \
    --topic telemetry_stream \
    --bootstrap-servers 35.170.50.224:9092 \
    --pause-seconds 0
'''
