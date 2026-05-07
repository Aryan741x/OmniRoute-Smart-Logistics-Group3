"""
Centralized path configuration for OmniRoute Smart Logistics Engine.
Supports both local development and AWS (S3) deployment.

Usage:
    from config import *
    # Then use paths like BRONZE_REGISTRY, SILVER_ASSIGNMENT, etc.

Set PIPELINE_ENV=aws to use S3 paths, or leave unset for local mode.
"""
import os

ENV = os.getenv("PIPELINE_ENV", "aws")  # "local" or "aws"

# -----------------------------------------------
# Bucket / Root Configuration
# -----------------------------------------------

if ENV == "aws":
    BRONZE_BUCKET = "s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group3-bronze"
    SILVER_BUCKET = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group3-silver"
    GOLD_BUCKET   = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold"
else:
    _BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    BRONZE_BUCKET = _BASE
    SILVER_BUCKET = _BASE
    GOLD_BUCKET   = _BASE

# -----------------------------------------------
# Incoming (landing zone for raw CSVs)
# -----------------------------------------------
INCOMING_REGISTRY    = f"{BRONZE_BUCKET}/data/incoming/vehicle_registry/"
INCOMING_ASSIGNMENT  = f"{BRONZE_BUCKET}/data/incoming/vehicle_assignment/"
INCOMING_FUEL        = f"{BRONZE_BUCKET}/data/incoming/fuel_transactions/"
INCOMING_MAINTENANCE = f"{BRONZE_BUCKET}/data/incoming/vehicle_maintenance/"

# -----------------------------------------------
# Bronze Layer (raw ingested parquet)
# -----------------------------------------------
BRONZE_REGISTRY    = f"{BRONZE_BUCKET}/data/bronze/registry/"
BRONZE_ASSIGNMENT  = f"{BRONZE_BUCKET}/data/bronze/assignment/"
BRONZE_TRANSACTION = f"{BRONZE_BUCKET}/data/bronze/transaction/"
BRONZE_MAINTENANCE = f"{BRONZE_BUCKET}/data/bronze/maintenance/"

# -----------------------------------------------
# Archive (schema violations, errors, conflicts)
# -----------------------------------------------
ARCHIVE_SCHEMA_REGISTRY    = f"{BRONZE_BUCKET}/data/archive/schema_violations/vehicle_registry/"
ARCHIVE_SCHEMA_ASSIGNMENT  = f"{BRONZE_BUCKET}/data/archive/schema_violations/vehicle_assignment/"
ARCHIVE_SCHEMA_FUEL        = f"{BRONZE_BUCKET}/data/archive/schema_violations/fuel_transaction/"
ARCHIVE_SCHEMA_MAINTENANCE = f"{BRONZE_BUCKET}/data/archive/schema_violations/vehicle_maintenance/"
ARCHIVE_REGISTRY_CONFLICTS   = f"{BRONZE_BUCKET}/data/archive/registry_conflicts/"
ARCHIVE_ASSIGNMENT_REJECTIONS = f"{BRONZE_BUCKET}/data/archive/assignment_rejections/"
ARCHIVE_FUEL_ERRORS    = f"{BRONZE_BUCKET}/data/archive/fuel_transaction_errors/"
ARCHIVE_MAINT_ERRORS   = f"{BRONZE_BUCKET}/data/archive/maintenance_errors/"

# -----------------------------------------------
# Metadata (processed file manifests)
# -----------------------------------------------
METADATA_REGISTRY    = f"{BRONZE_BUCKET}/data/metadata/processed_files/vehicle_registry/"
METADATA_ASSIGNMENT  = f"{BRONZE_BUCKET}/data/metadata/processed_files/vehicle_assignment/"
METADATA_FUEL        = f"{BRONZE_BUCKET}/data/metadata/processed_files/fuel_transaction/"
METADATA_MAINTENANCE = f"{BRONZE_BUCKET}/data/metadata/processed_files/vehicle_maintenance/"

# -----------------------------------------------
# Silver Layer (cleaned, validated Delta tables)
# -----------------------------------------------
if ENV == "aws":
    SILVER_REGISTRY      = f"{SILVER_BUCKET}/data/registry/"
    SILVER_ASSIGNMENT    = f"{SILVER_BUCKET}/data/assignment/"
    SILVER_FUEL_CLEAN    = f"{SILVER_BUCKET}/data/transaction/fuel_transaction_clean/"
    SILVER_FUEL_DISTANCE = f"{SILVER_BUCKET}/data/transaction/fuel_transaction_distance/"
    SILVER_MAINT_CLEAN   = f"{SILVER_BUCKET}/data/maintenance/maintenance_clean/"
else:
    SILVER_REGISTRY      = f"{SILVER_BUCKET}/data/silver/registry/"
    SILVER_ASSIGNMENT    = f"{SILVER_BUCKET}/data/silver/assignment/"
    SILVER_FUEL_CLEAN    = f"{SILVER_BUCKET}/data/silver/transaction/fuel_transaction_clean/"
    SILVER_FUEL_DISTANCE = f"{SILVER_BUCKET}/data/silver/transaction/fuel_transaction_distance/"
    SILVER_MAINT_CLEAN   = f"{SILVER_BUCKET}/data/silver/maintenance/maintenance_clean/"

# -----------------------------------------------
# Gold Layer (business aggregations)
# -----------------------------------------------
if ENV == "aws":
    GOLD_ASSET_HISTORY   = f"{GOLD_BUCKET}/data/asset_history_scd2/"
    GOLD_FLEET_SNAPSHOT  = f"{GOLD_BUCKET}/data/active_fleet_snapshot/"
    GOLD_FUEL_AUDIT      = f"{GOLD_BUCKET}/data/fuel_efficiency_audit/"
else:
    GOLD_ASSET_HISTORY   = f"{GOLD_BUCKET}/data/gold/asset_history_scd2/"
    GOLD_FLEET_SNAPSHOT  = f"{GOLD_BUCKET}/data/gold/active_fleet_snapshot/"
    GOLD_FUEL_AUDIT      = f"{GOLD_BUCKET}/data/gold/fuel_efficiency_audit/"
