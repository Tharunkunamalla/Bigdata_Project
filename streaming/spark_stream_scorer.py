"""
CSE412 Big Data Project: Real-Time Spark Structured Streaming Scorer
====================================================================
Subscribes to Kafka topic `disaster_raw_posts`, applies trained MLlib models
in micro-batches, calculates trust-discounted emergency priority scores,
and sinks enriched records to:
1. Kafka topic `disaster_scored_posts` (for real-time dashboard visualization)
2. HDFS Parquet storage (`/disaster_data/streaming_sink/`) with checkpointing
"""

import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, LongType
)
from pyspark.ml import PipelineModel

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
INPUT_TOPIC = "disaster_raw_posts"
OUTPUT_TOPIC = "disaster_scored_posts"
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://127.0.0.1:9000")
CHECKPOINT_DIR = f"{HDFS_NAMENODE}/disaster_data/checkpoints/streaming_scorer"
HDFS_SINK_DIR = f"{HDFS_NAMENODE}/disaster_data/streaming_sink"

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml", "models")
SYNTH_MODEL_PATH = os.path.join(MODEL_DIR, "synthetic_detector_model")
CAT_MODEL_PATH = os.path.join(MODEL_DIR, "humanitarian_category_model")
URG_MODEL_PATH = os.path.join(MODEL_DIR, "urgency_regressor_model")

def get_raw_schema():
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
        StructField("stream_timestamp", DoubleType(), True),
    ])

def build_streaming_spark():
    return (
        SparkSession.builder
        .appName("CSE412_Structured_Streaming_Scorer")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )

def main():
    print("==================================================================")
    print("      CSE412 Real-Time Structured Streaming Scorer Engine        ")
    print("==================================================================")
    print(f"Reading from Kafka: {KAFKA_BROKER} -> Topic: {INPUT_TOPIC}")
    
    spark = build_streaming_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Load MLlib Models
    print(f"Loading ML models from {MODEL_DIR}...")
    synth_model = PipelineModel.load(SYNTH_MODEL_PATH)
    urg_model = PipelineModel.load(URG_MODEL_PATH)
    print("Loaded MLlib scoring pipelines successfully.")

    schema = get_raw_schema()

    # Read Kafka Stream
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), schema).alias("data"))
        .select("data.*")
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

    # Micro-batch batch processor
    def process_micro_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
            
        n_records = batch_df.count()
        print(f"\n[MicroBatch {batch_id}] Processing {n_records} posts...")

        # Score with Synthetic Detector
        scored_synth = synth_model.transform(batch_df)
        
        # Score with Urgency Regressor
        scored_full = urg_model.transform(scored_synth)

        # Vector element extraction UDF for probability vector
        extract_prob = F.udf(lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0, DoubleType())

        enriched_df = (
            scored_full
            .withColumn("prob_synthetic_score", extract_prob(F.col("prob_synthetic")))
            .withColumn("pred_urgency_score", F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.col("prediction"))))
            .withColumn(
                "trust_score",
                F.round(
                    (1.0 - F.col("prob_synthetic_score")) * F.col("credibility_raw_score"),
                    4
                )
            )
            .withColumn(
                "effective_urgency",
                F.round(F.col("pred_urgency_score") * F.col("trust_score"), 4)
            )
            .withColumn(
                "is_flagged_misinformation",
                F.when(F.col("prob_synthetic_score") >= 0.50, 1).otherwise(0)
            )
            .select(
                "post_id", "event_name", "event_type", "timestamp", "text",
                "location_name", "latitude", "longitude", "humanitarian_category",
                "user_id", "retweet_count", "credibility_raw_score",
                "prob_synthetic_score", "pred_urgency_score", "trust_score",
                "effective_urgency", "is_flagged_misinformation"
            )
        )

        # 1. Publish scored results to Kafka output topic
        kafka_payload = (
            enriched_df
            .select(
                F.col("post_id").alias("key"),
                F.to_json(F.struct("*")).alias("value")
            )
        )
        (
            kafka_payload.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", OUTPUT_TOPIC)
            .save()
        )
        print(f"  -> Sunk {n_records} scored posts to Kafka topic '{OUTPUT_TOPIC}'")

        # 2. Append to local curated streaming sink for analytics
        enriched_df.write.mode("append").parquet("data/curated/streaming_sink")

    # Start Streaming Query
    query = (
        parsed_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", "/tmp/spark_stream_checkpoint")
        .start()
    )

    print("Stream processing loop active. Awaiting data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
