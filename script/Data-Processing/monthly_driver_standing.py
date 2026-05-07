"""
Monthly Driver Standing Job
Usage:
    spark-submit \
      --packages io.delta:delta-spark_2.12:3.2.0 \
      --driver-memory 4g \
      monthly_driver_standing.py \
      --gold-dir s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_date,
    current_timestamp,
    concat,
    lit,
)
from delta.tables import DeltaTable

from datetime import datetime, timedelta

def get_previous_month():
    today = datetime.today()
    # Get the last day of the previous month by subtracting 1 day from the 1st of this month
    first_day = today.replace(day=1)
    prev_month = first_day - timedelta(days=1)
    return prev_month.strftime("%Y-%m")

def calculate_deductions_and_merge(spark, gold_dir, target_month):
    incidents_dir = f"{gold_dir}/gold_violation_incidents"
    standing_dir = f"{gold_dir}/gold_driver_standing"

    if not DeltaTable.isDeltaTable(spark, incidents_dir):
        msg = f"Incidents table not found at {incidents_dir}. Cannot calculate standings."
        print(msg)
        raise RuntimeError(msg)

    print("Reading incidents table...")
    gold_incidents = spark.read.format("delta").load(incidents_dir)
    
    print(f"Calculating monthly totals (Daily last-strike logic) for month: {target_month}...")
    
    # First, get the final strike status for each driver for each day
    # This prevents summing intermediate strike deductions on the same day.
    daily_stats = (
        gold_incidents
        .filter(col("month_year") == target_month)
        .withColumn("incident_date", to_date(col("incident_ts")))
        .groupBy("driver_id", "incident_date")
        .agg(
            spark_max("strike_number").alias("daily_max_strike"),
            spark_max("deduction_amount").alias("daily_deduction"),
            spark_min("adjusted_rate").alias("daily_final_rate"),
            spark_max("incident_created_ts").alias("max_created_ts")
        )
    )

    if daily_stats.isEmpty():
        msg = f"❌ No violation records found for month {target_month}. Cannot generate standings."
        print(msg)
        raise RuntimeError(msg)

    monthly_totals = (
        daily_stats
        .groupBy("driver_id")
        .agg(
            spark_max("daily_max_strike").alias("current_strikes"),
            spark_sum("daily_deduction").alias("total_deductions"),
            spark_sum("daily_final_rate").alias("final_payable_rate"),
            spark_max("max_created_ts").alias("created_ts"),
            lit(target_month).alias("month_year")
        )
    )

    deduction_df = (
        monthly_totals
        .withColumn(
            "status",
            when(col("current_strikes") >= 10, "SUSPENDED")
            .otherwise("ACTIVE")
        )
        .withColumn(
            "suspension_date",
            when(col("status") == "SUSPENDED", col("created_ts").cast("date")).otherwise(None)
        )
        .withColumn("updated_ts", current_timestamp())
    )

    if not DeltaTable.isDeltaTable(spark, standing_dir):
        print(f"Creating gold_driver_standing at {standing_dir}")
        deduction_df.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("month_year") \
            .save(standing_dir)
            
        spark.sql(f"ALTER TABLE delta.`{standing_dir}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        print(f"Merging into gold_driver_standing at {standing_dir}")
        target = DeltaTable.forPath(spark, standing_dir)

        merge_condition = """
        t.driver_id = s.driver_id AND
        t.month_year = s.month_year
        """

        target.alias("t").merge(
            deduction_df.alias("s"),
            merge_condition
        ).whenMatchedUpdate(
            condition= "t.current_strikes != s.current_strikes OR t.status != s.status",
            set={
                "current_strikes": "s.current_strikes",
                "total_deductions": "s.total_deductions",
                "final_payable_rate": "s.final_payable_rate",
                "status": "s.status",
                "suspension_date": "s.suspension_date",
                "updated_ts": "s.updated_ts"
            }
        ).whenNotMatchedInsertAll().execute()
        
        
    print("Monthly driver standing calculation completed successfully.")

    # --- Generate Text Report ---
    report_path = f"{gold_dir}/report/monthly_standing_{target_month}"
    print(f"Generating text report at: {report_path}")
    
    report_df = deduction_df.select(
        concat(
            lit("Monthly Driver rate deduction report:\n"),
            lit("Report Includes:\n"),
            lit("Driver ID: "), col("driver_id"), lit("\n"),
            lit("Total strikes for the month: "), col("current_strikes").cast("string"), lit("\n"),
            lit("Total rate deductions: "), col("total_deductions").cast("string"), lit("\n"),
            lit("Final payable daily rate: "), col("final_payable_rate").cast("string"), lit("\n"),
            lit("Suspension status: "), col("status"), lit("\n"),
            lit("Suspension date: "), when(col("suspension_date").isNotNull(), col("suspension_date").cast("string")).otherwise(lit("N/A")), lit("\n"),
            lit("------------------------------------\n")
        ).alias("value")
    )

    # Coalesce to 1 partition so it's a single file inside the directory
    report_df.coalesce(1).write.mode("overwrite").text(report_path)
    print("Text report generation completed. Renaming part file to .txt...")
    
    try:
        if report_path.startswith("s3://"):
            import boto3
            s3 = boto3.client('s3')
            bucket = report_path.replace("s3://", "").split("/")[0]
            prefix = report_path.replace(f"s3://{bucket}/", "")
            
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            part_key = None
            keys_to_delete = []
            for obj in response.get('Contents', []):
                keys_to_delete.append(obj['Key'])
                if "part-" in obj['Key']:
                    part_key = obj['Key']
            
            if part_key:
                new_key = f"{prefix.rstrip('/')}.txt" 
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': part_key}, Key=new_key)
                print(f"Successfully moved report to s3://{bucket}/{new_key}")
                
                # Cleanup the old directory
                for key in keys_to_delete:
                    if key != new_key:
                        s3.delete_object(Bucket=bucket, Key=key)
        else:
            print("Local file system detected. Skipping rename (not on S3).")
    except Exception as e:
        print(f"Warning: Could not rename part file. {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gold-dir", default="s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group3-gold/data", help="Path to gold layer")
    p.add_argument("--target-month", default=None, help="Month to process in YYYY-MM format. Defaults to previous month.")
    args = p.parse_args()
    
    target_month = args.target_month if args.target_month else get_previous_month()
    
    spark = (
        SparkSession.builder
        .appName("monthly_driver_standing")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    gold_dir = args.gold_dir.rstrip("/")
    calculate_deductions_and_merge(spark, gold_dir, target_month)

if __name__ == "__main__":
    main()