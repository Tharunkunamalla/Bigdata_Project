"""
CSE412 Big Data Project: Bronze to Silver HDFS Ingestion Pipeline
==================================================================
Reads raw crisis records from local bronze / HDFS bronze storage, performs
schema validation, timestamp casting, data normalization, deduplication,
and writes optimized Snappy-compressed Parquet partitioned by `event_type`
to `/disaster_data/silver/` in Hadoop HDFS.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://127.0.0.1:9000")
BRONZE_HDFS_PATH = f"{HDFS_NAMENODE}/disaster_data/bronze/disaster_posts_raw.json"
SILVER_HDFS_PATH = f"{HDFS_NAMENODE}/disaster_data/silver/disaster_posts"
LOCAL_RAW_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw", "disaster_posts_raw.json")
LOCAL_SILVER_PARQUET = os.path.join(os.path.dirname(os.path.abspath(__file__)), "curated", "disaster_posts")

def build_spark_session():
    return (
        SparkSession.builder
        .appName("CSE412_BronzeToSilver_Ingestion")
        .master("local[2]")
        .config("spark.driver.memory", "768m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

def define_raw_schema():
    return StructType([
        StructField("post_id", StringType(), False),
        StructField("event_name", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("timestamp", StringType(), True),
        StructField("text", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_followers_count", IntegerType(), True),
        StructField("user_account_age_days", IntegerType(), True),
        StructField("user_verified", BooleanType(), True),
        StructField("retweet_count", IntegerType(), True),
        StructField("has_external_link", BooleanType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location_name", StringType(), True),
        StructField("humanitarian_category", StringType(), True),
        StructField("urgency_level", DoubleType(), True),
        StructField("is_synthetic", IntegerType(), True),
        StructField("is_rumour", IntegerType(), True),
        StructField("source_provenance", StringType(), True),
        StructField("char_length", IntegerType(), True),
        StructField("word_count", IntegerType(), True),
        StructField("avg_word_length", DoubleType(), True),
        StructField("uppercase_ratio", DoubleType(), True),
        StructField("exclamation_count", IntegerType(), True),
        StructField("burstiness_score", DoubleType(), True),
        StructField("lexical_diversity", DoubleType(), True),
        StructField("synthetic_confidence_prior", DoubleType(), True),
    ])

def run_ingestion():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("==================================================================")
    print("      CSE412 Bronze -> Silver Ingestion Pipeline (PySpark)        ")
    print("==================================================================")
    
    input_source = BRONZE_HDFS_PATH if "--use-hdfs" in sys.argv else LOCAL_RAW_JSON
    print(f"Reading raw records from: {input_source}")
    
    schema = define_raw_schema()
    raw_df = spark.read.schema(schema).json(input_source)
    initial_count = raw_df.count()
    print(f"Loaded {initial_count:,} raw records.")
    
    # Cleaning & Silver Schema Transformations
    silver_df = (
        raw_df
        .filter(F.col("post_id").isNotNull() & F.col("text").isNotNull())
        .dropDuplicates(["post_id"])
        .withColumn("timestamp_utc", F.to_timestamp(F.col("timestamp")))
        .withColumn("created_date", F.to_date(F.col("timestamp_utc")))
        .withColumn(
            "credibility_raw_score",
            F.when(F.col("user_verified") == True, 0.9)
             .otherwise(
                 F.least(
                     F.lit(1.0),
                     (F.col("user_account_age_days") / 365.0) * 0.4 +
                     (F.least(F.col("user_followers_count"), F.lit(1000)) / 1000.0) * 0.3
                 )
             )
        )
    )
    
    clean_count = silver_df.count()
    print(f"Validated and cleaned {clean_count:,} records.")
    
    # Write partitioned Parquet locally for fast driver access
    print(f"Writing Silver Parquet to local cache: {LOCAL_SILVER_PARQUET}")
    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("event_type")
        .parquet(LOCAL_SILVER_PARQUET)
    )
    
    # Also write to HDFS if requested or if HDFS is reachable
    if "--use-hdfs" in sys.argv:
        print(f"Writing Silver Parquet to HDFS: {SILVER_HDFS_PATH}")
        (
            silver_df.write
            .mode("overwrite")
            .partitionBy("event_type")
            .parquet(SILVER_HDFS_PATH)
        )
        print("HDFS Silver partition successfully updated.")
        
    print("\n[SUCCESS] Bronze to Silver Ingestion completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_ingestion()
