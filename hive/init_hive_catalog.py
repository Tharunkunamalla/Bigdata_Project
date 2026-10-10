"""
CSE412 Big Data Project: Hive Catalog Initializer via PySpark SQL
==================================================================
Connects to Hive Metastore Thrift service (port 9083), registers the
`cse412_disaster` database and the partitioned external `disaster_posts_silver`
table over HDFS Parquet storage, repairs partitions, and verifies analytical queries.
"""

import sys
from pyspark.sql import SparkSession

HIVE_METASTORE_URI = "thrift://127.0.0.1:9083"

def build_hive_spark_session():
    return (
        SparkSession.builder
        .appName("CSE412_HiveCatalog_Init")
        .master("local[2]")
        .config("spark.driver.memory", "768m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.warehouse.dir", "hdfs://127.0.0.1:9000/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

def main():
    print("==================================================================")
    print("      CSE412 Hive Metastore Catalog & Table Registration          ")
    print("==================================================================")
    spark = build_hive_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("1. Creating Hive Database: cse412_disaster...")
    spark.sql("CREATE DATABASE IF NOT EXISTS cse412_disaster")
    spark.sql("USE cse412_disaster")

    print("2. Registering External Partitioned Table: disaster_posts_silver...")
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS disaster_posts_silver (
            post_id STRING,
            event_name STRING,
            timestamp_utc TIMESTAMP,
            created_date DATE,
            text STRING,
            user_id STRING,
            user_followers_count INT,
            user_account_age_days INT,
            user_verified BOOLEAN,
            retweet_count INT,
            has_external_link BOOLEAN,
            latitude DOUBLE,
            longitude DOUBLE,
            location_name STRING,
            humanitarian_category STRING,
            urgency_level DOUBLE,
            is_synthetic INT,
            is_rumour INT,
            source_provenance STRING,
            char_length INT,
            word_count INT,
            avg_word_length DOUBLE,
            uppercase_ratio DOUBLE,
            exclamation_count INT,
            burstiness_score DOUBLE,
            lexical_diversity DOUBLE,
            synthetic_confidence_prior DOUBLE,
            credibility_raw_score DOUBLE
        )
        PARTITIONED BY (event_type STRING)
        STORED AS PARQUET
        LOCATION 'hdfs://127.0.0.1:9000/disaster_data/silver/disaster_posts'
    """)

    print("3. Discovering and recovering partitions (MSCK REPAIR)...")
    spark.sql("MSCK REPAIR TABLE disaster_posts_silver")

    print("4. Executing analytical test query on Hive catalog:")
    df_summary = spark.sql("""
        SELECT 
            event_type,
            COUNT(*) as total_posts,
            SUM(is_synthetic) as synthetic_posts,
            ROUND(AVG(urgency_level), 3) as avg_urgency,
            ROUND(AVG(credibility_raw_score), 3) as avg_credibility
        FROM disaster_posts_silver
        GROUP BY event_type
        ORDER BY total_posts DESC
    """)
    df_summary.show(truncate=False)

    total_count = spark.sql("SELECT COUNT(*) FROM disaster_posts_silver").collect()[0][0]
    print(f"\n[PASS] Successfully queried Hive catalog! Total verified rows: {total_count:,}")

    # Create Gold Aggregation Table
    print("\n5. Materializing Gold Table: gold_emergency_demand_summary...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold_emergency_demand_summary AS
        SELECT 
            event_type,
            location_name,
            COUNT(*) as total_reports,
            SUM(CASE WHEN urgency_level >= 0.7 THEN 1 ELSE 0 END) as high_urgency_reports,
            SUM(is_synthetic) as synthetic_misinformation_count,
            ROUND(SUM(is_synthetic) * 1.0 / COUNT(*), 4) as misinformation_ratio,
            ROUND(AVG(urgency_level), 4) as avg_urgency,
            CURRENT_TIMESTAMP() as computed_at
        FROM disaster_posts_silver
        GROUP BY event_type, location_name
    """)

    print("Gold demand summary sample:")
    spark.sql("SELECT * FROM gold_emergency_demand_summary LIMIT 5").show(truncate=False)

    print("\n[SUCCESS] Hive tables created and populated in metastore.")
    spark.stop()

if __name__ == "__main__":
    main()
