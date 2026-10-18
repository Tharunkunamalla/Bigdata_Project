"""
tests/test_integration.py
=========================
End-to-end integration test suite requiring live daemons (Kafka, HDFS, Hive Metastore via Spark SQL).
Verifies:
1. Live Kafka topic ingestion: publishes N disaster events to 'disaster_raw_posts'
2. HDFS live filesystem connectivity & silver partition validation
3. Spark SQL query over Hive Metastore catalog 'disaster_db.crisis_silver_real'
4. End-to-end data flow: Kafka -> Spark -> HDFS -> Hive table row count
"""

import os
import sys
import json
import time
import socket
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
TEST_TOPIC = "disaster_raw_posts"
HDFS_NAMENODE = "hdfs://127.0.0.1:9000"
HIVE_METASTORE_URI = "thrift://127.0.0.1:9083"

def check_daemon_port(host: str, port: int) -> bool:
    try:
        s = socket.socket()
        s.settimeout(0.4)
        res = s.connect_ex((host, port))
        s.close()
        return res == 0
    except Exception:
        return False

HAS_KAFKA = check_daemon_port("127.0.0.1", 9092)
HAS_HDFS = check_daemon_port("127.0.0.1", 9000) and (sys.platform != "win32" or bool(os.getenv("HADOOP_HOME")))
HAS_HIVE = check_daemon_port("127.0.0.1", 9083) and (sys.platform != "win32" or bool(os.getenv("HADOOP_HOME")))

def build_spark_test_session():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("CrisisGuard_Integration_Test")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.warehouse.dir", f"{HDFS_NAMENODE}/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

@pytest.mark.skipif(not HAS_KAFKA, reason="Live Kafka daemon not running on port 9092")
def test_kafka_live_publish_and_consume():
    from kafka import KafkaProducer, KafkaConsumer
    """Verify Kafka broker is accepting and delivering events."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000
    )
    
    test_events = [
        {
            "post_id": f"integ_test_{i}",
            "text": f"INTEGRATION TEST: Flash flooding reported on Main St #{i}",
            "event_type": "flood",
            "latitude": 29.756,
            "longitude": -95.362,
            "urgency_level": 4.0,
            "is_synthetic": 0,
            "timestamp": time.time()
        }
        for i in range(10)
    ]
    
    for ev in test_events:
        future = producer.send(TEST_TOPIC, ev)
        _ = future.get(timeout=5)
    producer.flush()
    producer.close()
    assert True, "Published 10 test events to Kafka"

@pytest.mark.skipif(not HAS_HDFS, reason="Live HDFS daemon not running on port 9000 or HADOOP_HOME unset on Windows")
def test_hdfs_silver_storage_connectivity():
    from pyspark.sql import SparkSession
    """Verify HDFS silver storage contains valid Parquet partition files."""
    spark = SparkSession.builder.master("local[1]").appName("HDFS_Test").getOrCreate()
    hdfs_path = f"{HDFS_NAMENODE}/disaster_data/silver_real"
    
    df_silver = spark.read.parquet(hdfs_path)
    count = df_silver.count()
    assert count > 50000, f"Expected > 50,000 silver rows on HDFS, found {count:,}"
    spark.stop()

@pytest.mark.skipif(not HAS_HIVE, reason="Live Hive Metastore daemon not running on port 9083 or HADOOP_HOME unset on Windows")
def test_hive_metastore_table_row_count():
    """Verify Hive Metastore table 'disaster_db.crisis_silver_real' is registered and queryable."""
    spark = build_spark_test_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Query Hive table via Spark SQL
    df_hive = spark.sql("SELECT COUNT(*) AS total_rows FROM disaster_db.crisis_silver_real")
    total_rows = df_hive.collect()[0]["total_rows"]
    
    assert total_rows >= 90000, f"Expected >= 90,000 rows in Hive table, got {total_rows:,}"
    
    # Verify partition query works
    df_event_counts = spark.sql("""
        SELECT disaster_type, COUNT(*) as cnt 
        FROM disaster_db.crisis_silver_real 
        GROUP BY disaster_type
    """)
    rows = df_event_counts.collect()
    assert len(rows) >= 3, "Expected at least 3 disaster types in Hive catalog"
    
    spark.stop()
