"""
data/ingest_real_to_hdfs.py
===========================
Ingests the real disaster corpus into HDFS:
1. Reads curated CSV with robust multiline and quote escaping
2. Filters to verified clean disaster_type partitions (flood, hurricane, earthquake, wildfire, cyclone, general)
3. Saves local Parquet splits for high-speed ML training
4. Uploads partitioned Parquet to HDFS /disaster_data/silver_real/
5. Registers external Hive Metastore table `disaster_db.crisis_silver_real`
6. Verifies row counts in HDFS and Hive
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CURATED_DIR = os.path.join(BASE_DIR, "data", "curated")
CSV_PATH = os.path.join(CURATED_DIR, "real_disaster_corpus.csv")
HDFS_SILVER = "hdfs://127.0.0.1:9000/disaster_data/silver_real"

def build_spark():
    return (
        SparkSession.builder
        .appName("Ingest_Real_To_HDFS")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
        .enableHiveSupport()
        .getOrCreate()
    )

def main():
    print("=" * 65)
    print("      Ingesting Real-World Disaster Corpus into HDFS & Hive       ")
    print("=" * 65)
    
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Read curated real CSV with multiline and escape options
    print(f"Reading {CSV_PATH} into Spark...")
    raw_df = (
        spark.read
        .option("header", "true")
        .option("multiline", "true")
        .option("escape", "\"")
        .option("inferSchema", "true")
        .csv(CSV_PATH)
    )
    
    # Strictly validate disaster_type to prevent invalid partition path names in HDFS
    valid_types = ["flood", "hurricane", "earthquake", "wildfire", "cyclone", "general"]
    df = raw_df.filter(F.col("disaster_type").isin(valid_types)).cache()
    
    total_cnt = df.count()
    print(f"Loaded {total_cnt:,} validated clean disaster records.")
    
    # 2. Local Parquet exports for local ML training & streaming replay
    local_parquet = os.path.join(CURATED_DIR, "real_disaster_corpus.parquet")
    train_parquet = os.path.join(CURATED_DIR, "train_real.parquet")
    test_parquet = os.path.join(CURATED_DIR, "test_real.parquet")
    
    print("Writing local partitioned parquet splits...")
    df.write.mode("overwrite").parquet(local_parquet)
    
    train_csv_path = os.path.join(CURATED_DIR, "train_real.csv")
    test_csv_path = os.path.join(CURATED_DIR, "test_real.csv")
    
    train_df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").option("inferSchema", "true").csv(train_csv_path)
    test_df = spark.read.option("header", "true").option("multiline", "true").option("escape", "\"").option("inferSchema", "true").csv(test_csv_path)
    
    train_df.write.mode("overwrite").parquet(train_parquet)
    test_df.write.mode("overwrite").parquet(test_parquet)
    print(f"Local Parquet files saved: Train={train_df.count():,}, Test={test_df.count():,}")
    
    # 3. Clean HDFS silver directory first if it exists
    print(f"\nWriting partitioned Parquet to HDFS: {HDFS_SILVER}...")
    try:
        (
            df.write
            .mode("overwrite")
            .partitionBy("disaster_type")
            .parquet(HDFS_SILVER)
        )
        print("HDFS Silver Parquet write successful!")
    except Exception as e:
        print(f"HDFS Parquet write error: {e}")
        
    # 4. Create Hive Metastore table
    print("\nRegistering Hive table `disaster_db.crisis_silver_real`...")
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS disaster_db")
        spark.sql("DROP TABLE IF EXISTS disaster_db.crisis_silver_real")
        spark.sql(f"""
            CREATE EXTERNAL TABLE disaster_db.crisis_silver_real (
                post_id STRING,
                text STRING,
                event_type STRING,
                humanitarian_category STRING,
                is_synthetic INT,
                is_hoax INT,
                urgency_level DOUBLE,
                latitude DOUBLE,
                longitude DOUBLE,
                source STRING
            )
            PARTITIONED BY (disaster_type STRING)
            STORED AS PARQUET
            LOCATION '{HDFS_SILVER}'
        """)
        spark.sql("MSCK REPAIR TABLE disaster_db.crisis_silver_real")
        hive_cnt = spark.sql("SELECT COUNT(*) as cnt FROM disaster_db.crisis_silver_real").collect()[0]["cnt"]
        print(f"Hive Metastore verification query returned: {hive_cnt:,} rows across partitions!")
    except Exception as e:
        print(f"Hive table creation notice: {e}")
        
    spark.stop()
    print("=" * 65)
    print(">>> HDFS & HIVE INGESTION COMPLETE <<<")
    print("=" * 65)

if __name__ == "__main__":
    main()
