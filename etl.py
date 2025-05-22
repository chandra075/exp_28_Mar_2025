import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder \
    .appName("GCS Example") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.5") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .getOrCreate()
def pyspark_code():
    df_raw =  spark\
             .read\
             .json("gs://rawnclean/raw/2025-05-20/*.json", multiLine=True)
    df_flat = df_raw.select(
        F.col("pull_request.id").alias("id"),
        F.col("pull_request.issue_url").alias("issue_url"),
        F.col("pull_request.number").alias("number"),
        F.col("pull_request.state").alias("state"),
        F.to_timestamp(F.col("pull_request.created_at")).alias("created_at"),
        F.to_timestamp(F.col("pull_request.closed_at")).alias("closed_at"),
        F.to_timestamp(F.col("pull_request.updated_at")).alias("updated_at"),
        F.to_timestamp(F.col("pull_request.merged_at")).alias("merged_at"),
        F.col("pull_request.comments_url").alias("comments"),
        F.col("pull_request.title").alias("title")
                           )
    df_flat.show()

