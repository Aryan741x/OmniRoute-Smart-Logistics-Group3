from pyspark.sql import SparkSession

def get_spark(app_name):
    """
    Create a SparkSession for EMR. 
    Master is managed by YARN/Cluster mode; do not set it manually.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport() \
        .getOrCreate()



# import os
# from pyspark.sql import SparkSession

# def get_spark(app_name):
#     env = os.getenv("PIPELINE_ENV", "aws")

#     builder = SparkSession.builder.appName(app_name)

#     if env == "aws":
#         # DO NOT set .master("local") here. 
#         # EMR provides the master (yarn) via spark-submit arguments.
#         return builder \
#             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#             .enableHiveSupport() \
#             .getOrCreate()
#     else:
#         # Local development mode
#         from delta import configure_spark_with_delta_pip
#         builder = builder.master("local[*]") \
#             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#             .config("spark.hadoop.fs.defaultFS", "file:///")
#         return configure_spark_with_delta_pip(builder).getOrCreate()