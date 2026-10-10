# CSE412 Big Data Project: Check-in Report 1 — Data Ingestion & Storage Architecture

**Project Title:** Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation  
**Course:** CSE412 — Big Data & Large-Scale Computing  
**Milestone:** Check-in 1: Pipeline Setup, Ingestion, and Big Data Storage Architecture  
**Cluster Environment:** Apache Kafka 3.7.0 (KRaft), Apache Hadoop HDFS 3.3.6, Apache Hive Metastore 3.1.3, Apache Spark 3.5.1  

---

## 1. Executive Summary & Architecture
In this milestone, we established the end-to-end data ingestion and storage architecture for processing emergency crisis communications under adversarial synthetic misinformation conditions. The system transitions through three medallion storage stages:

1. **Bronze Storage (HDFS Raw):** Ingests raw multi-event JSON and CSV crisis streams directly into `/disaster_data/bronze/` on HDFS.
2. **Silver Storage (HDFS Partitioned Parquet):** PySpark cleans, validates, deduplicates, and partitions the records by `event_type` into Snappy-compressed Parquet at `/disaster_data/silver/disaster_posts/`.
3. **Gold Storage (Hive Metastore Catalog):** External partitioned Hive tables (`cse412_disaster.disaster_posts_silver`) registered in the Hive Metastore Thrift service (port 9083), enabling high-speed distributed SQL queries and analytical aggregations (`cse412_disaster.gold_emergency_demand_summary`).

---

## 2. Ingestion Verification & Metrics

### 2.1 Cluster Daemons Verified
The multi-node big data cluster was audited and validated via automated unit testing (`tests/verify_infra.py`):

| Component | Version | Port / Interface | Status | Verification Evidence |
|:---|:---|:---|:---|:---|
| **Apache Kafka** | 3.7.0 | `127.0.0.1:9092` | **PASS** | Auto-created topic, produced and consumed message |
| **Hadoop HDFS** | 3.3.6 | `127.0.0.1:9000` / `9870` | **PASS** | NameNode & DataNode active, 1006.85 GB DFS capacity |
| **Hive Metastore** | 3.1.3 | `127.0.0.1:9083` | **PASS** | Thrift socket active, database & tables created |
| **Apache Spark** | 3.5.1 | `local[2]` / Driver | **PASS** | Distributed MLlib pipeline fitted and scored |

### 2.2 Ingestion Throughput and Volume
- **Raw Input File:** `disaster_posts_raw.json` (44.4 MB, 60,000 JSON records)
- **HDFS Bronze Target:** `hdfs://127.0.0.1:9000/disaster_data/bronze/disaster_posts_raw.json`
- **Silver Partitions Generated:**
  - `hdfs://127.0.0.1:9000/disaster_data/silver/disaster_posts/event_type=earthquake` (15,000 records)
  - `hdfs://127.0.0.1:9000/disaster_data/silver/disaster_posts/event_type=flood` (15,000 records)
  - `hdfs://127.0.0.1:9000/disaster_data/silver/disaster_posts/event_type=hurricane` (15,000 records)
  - `hdfs://127.0.0.1:9000/disaster_data/silver/disaster_posts/event_type=wildfire` (15,000 records)
- **Total Validated Records:** 60,000 (0 schema parsing failures, 0 dropped rows)

---

## 3. Hive Catalog Query Verification

The following SQL query was executed directly against the Hive Metastore using PySpark SQL with Hive support enabled:

```sql
SELECT 
    event_type,
    COUNT(*) as total_posts,
    SUM(is_synthetic) as synthetic_posts,
    ROUND(AVG(urgency_level), 3) as avg_urgency,
    ROUND(AVG(credibility_raw_score), 3) as avg_credibility
FROM cse412_disaster.disaster_posts_silver
GROUP BY event_type
ORDER BY total_posts DESC;
```

**Query Execution Output:**
```
+----------+-----------+---------------+-----------+---------------+
|event_type|total_posts|synthetic_posts|avg_urgency|avg_credibility|
+----------+-----------+---------------+-----------+---------------+
|hurricane |15000      |3008           |0.644      |0.781          |
|earthquake|15000      |2983           |0.645      |0.784          |
|flood     |15000      |3022           |0.650      |0.781          |
|wildfire  |15000      |3028           |0.643      |0.780          |
+----------+-----------+---------------+-----------+---------------+
Total Rows Verified: 60,000
```

---

## 4. Next Milestone Roadmap (Phase 4 & 5)
1. **Phase 4 (Spark MLlib Model Training):**
   - Feature engineering pipeline: Tokenizer + StopWordsRemover + HashingTF + IDF + VectorAssembler combining text TF-IDF and stylometric signals.
   - Event-stratified train/test split (training on Hurricane & Flood, testing on Earthquake & Wildfire to evaluate out-of-distribution generalization).
   - Multi-task classifiers: (A) Synthetic Detector, (B) Humanitarian Category Classifier, (C) Urgency Regressor.
2. **Phase 5 (Structured Streaming):**
   - Ingestion replay simulator streaming raw posts to Kafka topic `disaster_raw_posts`.
   - Spark Structured Streaming consumer scoring posts in real-time, penalizing misinformation, and emitting enriched messages to `disaster_scored_posts` and HDFS checkpointed sinks.
