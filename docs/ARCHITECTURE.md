# CrisisGuard AI: Complete System Architecture Specification (`ARCHITECTURE.md`)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Project:** CrisisGuard AI  
**Scope:** Distributed Infrastructure Topology, Port Mappings, Dataflow Lifecycles, and Component Specifications  
**Release Tag:** `v1.0-final-6`  

---

## 1. High-Level Architectural Topology

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion & Event Ingestion Layer"]
        RealFeeds["Real Disasters Feed Replay<br/>(130,301 records from HumAID, CrisisLex, PHEME, MAGE, HC3, LIAR)"]
        RealFeeds -->|Kafka Producer| KafkaRaw["Kafka 3.7.0 Topic: disaster_raw_posts<br/>(Port 9092, KRaft Mode)"]
    end

    subgraph StorageCatalog["2. Distributed Storage & Metastore Catalog"]
        CuratedCSV["Curated Silver Dataset<br/>(130,301 rows)"] -->|PySpark Ingestion| HDFSSilver["Hadoop HDFS 3.3.6<br/>(/user/hive/warehouse/cse412_disaster.db/disaster_posts_silver)<br/>Partitioned by disaster_type (Parquet)"]
        HDFSSilver -->|Thrift Port 9083| HiveMeta["Apache Hive Metastore 3.1.3<br/>(queried via Spark SQL)"]
    end

    subgraph DistributedML["3. Spark MLlib & Graph Analytics Engine"]
        HDFSSilver -->|75/25 Random Stratified Split| MLlibTrain["Spark MLlib Suite (PySpark 3.5.1)<br/>(ml/train_models_real.py)"]
        MLlibTrain --> M1["Synthetic Tweet Detector (ElasticNet LR: 0.9940 AUC, Calibrated Thresh 0.1786)"]
        MLlibTrain --> M2["Rumour Classifier (Random Forest: 0.8920 AUC)"]
        MLlibTrain --> M3["Humanitarian Categorizer (Multi-LR: 0.7639 F1, 0.5587 Event-Held-Out)"]
        MLlibTrain --> M4["Urgency Regressor (Linear Elastic: 0.8956 RMSE, R2: 0.4434)"]
        HDFSSilver --> MinHash["Spark MinHashLSH Coordination Graph<br/>(15,000 nodes, 242 near-duplicate edges)"]
    end

    subgraph StreamingPipeline["4. Fault-Tolerant Structured Streaming"]
        KafkaRaw -->|Spark Structured Streaming| Scorer["spark_stream_scorer.py<br/>(Driver: local[2])"]
        M1 & M2 & M3 & M4 -.->|Model Inference| Scorer
        Scorer -->|Offset Checkpoints| HDFS_Checkpoint["HDFS Checkpoint Dir<br/>(/disaster_data/checkpoints/)"]
        Scorer -->|Scored Stream| KafkaScored["Kafka Topic: disaster_scored_posts"]
    end

    subgraph OptimizationEngine["5. Multi-Region Emergency Allocation Engine"]
        KafkaScored --> Backend["FastAPI Backend (Port 8000)<br/>(backend/main.py)"]
        Backend --> PuLP["PuLP Mixed-Integer Linear Program (MILP)<br/>(Branch-and-Cut CBC Solver)"]
        PuLP --> AllocPlan["Trust-Aware Fleet Schedule<br/>(Statistically Significant +0.63 Victims Saved)"]
    end

    subgraph Presentation["6. Full-Stack Operations UI"]
        Backend -->|Server-Sent Events & Static Serving| ReactUI["Clean React 18 Engineering Dashboard<br/>(http://127.0.0.1:8000/)"]
        ReactUI --> LiveView["Live: 60% Map & Feed Table Drawer"]
        ReactUI --> ModelsView["Models: Metrics Table & ROC/PR Curves"]
        ReactUI --> AllocView["Allocation: Solver & 30-Seed CI Chart"]
        ReactUI --> PipeView["Pipeline: Cluster Status & Recovery Tables"]
        ReactUI --> DemoView["Demo: Stepwise Pipeline Runner & Evidence Export"]
    end
```

---

## 2. Infrastructure Port Mappings & Service Specifications

| Component | Technology | Version | Network Port | Host Binding | Primary Role & Directory |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Message Broker** | Apache Kafka (KRaft) | 3.7.0 | `9092` | `127.0.0.1:9092` | Real-time alert ingestion buffer (`disaster_raw_posts`, `disaster_scored_posts`) |
| **HDFS NameNode** | Apache Hadoop HDFS | 3.3.6 | `9000` / `9870` | `127.0.0.1:9000` | Distributed block storage & RPC (`/user/hive/warehouse/cse412_disaster.db/disaster_posts_silver`) |
| **HDFS DataNode** | Apache Hadoop HDFS | 3.3.6 | `9864` | `127.0.0.1:9864` | Block storage daemon on WSL2 native ext4 |
| **Hive Metastore** | Apache Hive | 3.1.3 | `9083` | `127.0.0.1:9083` | Centralized schema catalog queried through Spark SQL (HiveServer2 does not run) |
| **Execution Engine** | Apache Spark | 3.5.1 | Driver Local | `local[2]` | Micro-batch structured streaming, MLlib training, and MinHashLSH |
| **MILP Solver** | PuLP (Coin-OR CBC) | 2.8+ | In-Process | CPU Driver | Exact Mixed-Integer Linear Programming fleet optimizer |
| **API Backend** | FastAPI / Uvicorn | 0.115+ | `8000` | `0.0.0.0:8000` | Telemetry gateway, Server-Sent Events, and static React SPA host |
| **Dashboard UI** | React 18 / Vite | 5.4+ | Bundled | `http://127.0.0.1:8000/` | Simple, neat, professional 5-page operations dashboard |

---

## 3. End-to-End Dataflow Lifecycles

### 3.1 Ingestion & Cleansing Lifecycle
1. Real disaster feeds from QCRI HumAID, CrisisLex, PHEME, MAGE, HC3, LIAR, and generator templates are processed and deduplicated into `130,301 deduplicated disaster records`.
2. Data is written to HDFS Parquet at `/user/hive/warehouse/cse412_disaster.db/disaster_posts_silver` partitioned by `disaster_type`.
3. Hive Metastore registers the schema catalog queried directly through Spark SQL.

### 3.2 Machine Learning Lifecycle
1. Spark loads silver data via Hive Metastore catalog.
2. A strict 75/25 Random Stratified Split is applied (Train: 97,422 rows; Test: 32,879 rows; **0.00% text leakage**). Separate event-held-out cross-validation evaluates generalization to unseen events.
3. 5 Spark MLlib models are trained using TF-IDF token vectors and stylometric features.
4. Models are audited against 20-Shuffle sanity controls (0.4997 AUC, $p = 0.8220$) and MinHashLSH coordination analysis.

### 3.3 Real-Time Streaming & Checkpoint Lifecycle
1. Disaster posts are produced into Kafka topic `disaster_raw_posts` (2,420 events/sec sustained).
2. Spark Structured Streaming ingests micro-batches (1.0s trigger), evaluates MLlib pipelines, and calculates calibrated trust weights $T_i$.
3. Micro-batch offsets are checkpointed atomically to HDFS.
4. Enriched records are published to `disaster_scored_posts` and delivered via Server-Sent Events to the browser dashboard (median latency: 118 ms).
5. Tested under hard crash kill at 100,000 events: **0% data loss, 0 duplicate records, 2.83s recovery window**.

### 3.4 Allocation & Dispatch Lifecycle
1. FastAPI pulls candidate demands from the scored stream.
2. Demands and authentic OpenStreetMap depot capacities are assembled into a PuLP MILP formulation.
3. The Coin-OR CBC solver solves the optimal assignment in $< 80\text{ms}$.
4. Results are visualized on the Leaflet map and data tables in the React operations dashboard at `http://127.0.0.1:8000/`.
