"""
S3-compatible file utility functions for OmniRoute.
Uses 'hdfs dfs' commands which natively support s3:// on EMR.
No boto3 or extra dependencies required.
"""
import subprocess
import csv
import io
import hashlib
import logging

logger = logging.getLogger(__name__)


def list_csv_files(path):
    """List all .csv files in an S3 prefix using hdfs dfs."""
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.warning(f"Could not list {path}: {result.stderr.strip()}")
        return []

    files = []
    for line in result.stdout.strip().split("\n"):
        parts = line.strip().split()
        if parts and parts[-1].lower().endswith(".csv"):
            files.append(parts[-1])
    return sorted(files)


def read_csv_header(file_path):
    """Read the first line (header) of a CSV file from S3."""
    result = subprocess.run(
        f"hdfs dfs -cat '{file_path}' | head -1",
        shell=True, capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return next(csv.reader(io.StringIO(result.stdout.strip())))


def compute_file_checksum(file_path):
    """Compute SHA-256 hash by streaming the file through sha256sum."""
    result = subprocess.run(
        f"hdfs dfs -cat '{file_path}' | sha256sum",
        shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Checksum failed for {file_path}: {result.stderr.strip()}")
        return ""
    return result.stdout.strip().split()[0]


def move_file(src_path, dst_dir):
    """Move (copy + delete) a file in S3 via hdfs dfs."""
    filename = src_path.rsplit("/", 1)[-1]
    dst_path = dst_dir.rstrip("/") + "/" + filename

    cp_result = subprocess.run(
        ["hdfs", "dfs", "-cp", src_path, dst_path],
        capture_output=True, text=True
    )
    if cp_result.returncode != 0:
        logger.error(f"Copy failed: {cp_result.stderr.strip()}")
        return

    rm_result = subprocess.run(
        ["hdfs", "dfs", "-rm", src_path],
        capture_output=True, text=True
    )
    if rm_result.returncode != 0:
        logger.error(f"Delete failed: {rm_result.stderr.strip()}")
        return

    logger.info(f"Moved {src_path} → {dst_path}")


def path_exists(path):
    """Check if a path exists in S3 using hdfs dfs -test."""
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", path],
        capture_output=True
    )
    return result.returncode == 0


def ensure_directory(path):
    """Create directory (prefix) in S3. Usually a no-op but hdfs mkdir -p is safe."""
    subprocess.run(
        ["hdfs", "dfs", "-mkdir", "-p", path],
        capture_output=True
    )


def get_filename(path):
    """Extract filename from an S3 path."""
    return path.rsplit("/", 1)[-1]