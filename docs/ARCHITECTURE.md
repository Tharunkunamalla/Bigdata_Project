# System Architecture Specification

## 1. High-Level Architectural Topology

```mermaid
flowchart TD
    subgraph DataIngestion["1. Stream & Batch Ingestion"]
        Simulator["Replay Simulator<br/>(ingestion/replay_simulator.py)"]
        RawFiles["Raw Multi-Disaster Corpus<br/>(60,000 JSON/CSV records)"]
        Simulator -->|Kafka Producer| KafkaRaw["Kafka Topic: disaster_raw_posts<br/>(4 Partitions, KRaft Broker)"]
        RawFiles -->|hdfs dfs -put| HDFS_Bronze["HDFS Bronze Layer<br/>(/disaster_data/bronze/)"]
    end

    subgraph BatchETL["2. Storage & Catalog Layer"]
        HDFS_Bronze -->|PySpark Schema Validation| HDFS_Silver["HDFS Silver Layer<br/>(/disaster_data/silver/)<br/>Partitioned by event_type (Parquet)"]
        HDFS_Silver -->|MSCK REPAIR| HiveMetastore["Hive Metastore Catalog<br/>(cse412_disaster.disaster_posts_silver)"]
        HiveMetastore -->|Analytical Aggregations| HiveGold["Hive Gold Table<br/>(gold_emergency_demand_summary)"]
    end

    subgraph DistributedML["3. Spark MLlib Intelligence Engine"]
        HDFS_Silver -->|Event-Stratified Split| MLlibTrain["Spark MLlib Distributed Training<br/>(ml/train_models.py)"]
        MLlibTrain --> M1["Synthetic Detector (ElasticNet LR)"]
        MLlibTrain --> M2["Rumour Classifier (RandomForest)"]
        MLlibTrain --> M3["Humanitarian Category Classifier (Multinomial LR)"]
        MLlibTrain --> M4["Urgency Regressor (LinearRegression)"]
    end

    subgraph StreamScoring["4. Real-Time Structured Streaming"]
        KafkaRaw -->|Kafka Consumer Stream| SparkStream["Spark Structured Streaming<br/>(streaming/spark_stream_scorer.py)"]
        M1 & M2 & M3 & M4 -.->|Model Ingestion| SparkStream
        SparkStream -->|Offset Checkpointing| HDFS_Checkpoints["HDFS Checkpoint Dir<br/>(/disaster_data/checkpoints/)"]
        SparkStream -->|Curated Micro-batches| HDFS_Sink["HDFS Streaming Sink<br/>(/disaster_data/streaming_sink/)"]
        SparkStream -->|Enriched Records| KafkaScored["Kafka Topic: disaster_scored_posts"]
    end

    subgraph OptimizationEngine["5. Emergency Fleet Optimization"]
        KafkaScored --> AllocationServer["FastAPI Backend<br/>(backend/main.py)"]
        AllocationServer --> MILP["PuLP MILP Solver<br/>(allocation/optimizer.py)"]
        AllocationServer --> Greedy["Trust-Aware Greedy Dispatcher"]
        MILP --> OptimizedPlan["Trust-Weighted Dispatch Schedule<br/>(Wasted Dispatches Reduced by 97.3%)"]
    end

    subgraph PresentationLayer["6. Full-Stack Operations UI"]
        AllocationServer -->|Server-Sent Events (SSE)| ReactUI["React + Vite Single-Page Dashboard<br/>(frontend/src/App.jsx)"]
        ReactUI --> MapView["Leaflet Spatial Heatmap & Base Depots"]
        ReactUI --> LiveFeed["Real-Time Classified Emergency Stream"]
        ReactUI --> FleetControls["Interactive Dispatch Optimization Panel"]
        ReactUI --> MetricsView["ML Performance & Cluster Topology"]
    end
```

---

## 2. Component Specifications

### 2.1 Message Broker: Apache Kafka 3.7.0 (KRaft)
- **Deployment Mode:** KRaft standalone broker (no Zookeeper dependency).
- **Listening Ports:** `127.0.0.1:9092` (client plaintext), `127.0.0.1:9093` (controller).
- **Topics:**
  - `disaster_raw_posts`: 4 partitions, key: `post_id`, value: raw JSON payload.
  - `disaster_scored_posts`: 4 partitions, enriched with MLlib predictions, trust scores, and priority ranks.

### 2.2 Distributed File System: Apache Hadoop HDFS 3.3.6
- **NameNode RPC:** `hdfs://127.0.0.1:9000` (WebUI: `http://127.0.0.1:9870`).
- **DataNode Data Port:** `127.0.0.1:9864`.
- **Directory Layout:**
  - `/disaster_data/bronze/`: Immutable raw JSON/CSV stream dumps.
  - `/disaster_data/silver/disaster_posts/`: Partitioned Parquet (`event_type=earthquake|flood|hurricane|wildfire`).
  - `/disaster_data/streaming_sink/`: Structured Streaming append sink.
  - `/disaster_data/checkpoints/`: Write-Ahead Log (WAL) and state store for fault-tolerant recovery.

### 2.3 Metadata Catalog: Apache Hive Metastore 3.1.3
- **Thrift Port:** `0.0.0.0:9083`.
- **Database:** `cse412_disaster`.
- **Catalog Management:** Enables schema-on-read querying over HDFS Parquet with partition discovery (`MSCK REPAIR TABLE`) and gold summary materialization.

### 2.4 Distributed Processing Engine: Apache Spark 3.5.1
- **Driver:** Local multi-threaded JVM driver (`local[2]`).
- **Libraries:**
  - Spark SQL: Medallion transformations and Hive integration.
  - Spark Structured Streaming: Low-latency micro-batch Kafka consumer.
  - Spark MLlib: Scalable feature extraction (HashingTF, IDF, StandardScaler) and machine learning classification/regression pipelines.

### 2.5 Fleet Optimization: PuLP 2.9.0
- **Formulation:** Mixed-Integer Linear Programming (MILP) solved via COIN-OR CBC.
- **Objective:** Maximizes emergency victim survival probability discounted by misinformation penalties and transit distances under finite fleet capacity constraints.
