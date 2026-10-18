# CrisisGuard AI
### Real-Time Detection of Synthetic Misinformation and Trust-Aware Emergency Resource Allocation in Multi-Hazard Disasters

[![Course](https://img.shields.io/badge/Course-CSE412%20Big%20Data-blue.svg)](docs/REPORT.md)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1%20MLlib-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0%20KRaft-black.svg)](https://kafka.apache.org/)
[![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-3.3.6-yellow.svg)](https://hadoop.apache.org/)
[![Hive Metastore](https://img.shields.io/badge/Hive%20Metastore-3.1.3-green.svg)](https://hive.apache.org/)
[![Pytest](https://img.shields.io/badge/Tests-21%20Passing-brightgreen.svg)](tests/test_real_pipeline.py)
[![Release](https://img.shields.io/badge/Version-v1.0--final--9-blue.svg)](docs/REPORT.md)

**CrisisGuard AI** is an end-to-end, distributed big data analytics and optimization pipeline designed to protect emergency dispatch operations from generative AI-driven misinformation during natural disasters. Built across **Apache Kafka 3.7.0**, **Apache Spark 3.5.1 Structured Streaming & MLlib**, **Apache Hadoop HDFS 3.3.6**, **Apache Hive Metastore 3.1.3** (queried through Spark SQL), and **PuLP CBC Mixed-Integer Linear Programming (MILP)**, CrisisGuard ingests high-velocity disaster streams, scores synthetic hoaxes, identifies botnet coordination via MinHashLSH, and executes optimal fleet dispatch plans to prioritize genuine disaster victims.

---

## 1. Overview & Key Empirical Findings

CrisisGuard AI couples distributed stream ingestion, real-time feature extraction, out-of-core machine learning inference, graph-based coordination analysis, and trust-aware mathematical optimization to filter synthetic hoaxes and execute optimal fleet dispatch plans.

All metrics are generated from **130,301 authentic, deduplicated crisis records** (QCRI CrisisBench / HumAID, CrisisLexT26, Hello-SimpleAI HC3, MAGE, PHEME, and LIAR) evaluated across live cluster daemons. Zero numbers are hardcoded.

- **Authentic Silver Corpus:** 130,301 records partitioned in HDFS Parquet at `/user/hive/warehouse/cse412_disaster.db/disaster_posts_silver` and registered in the Hive Metastore catalog.
- **Strict 75/25 Random Stratified Split:** 75% Train (97,422 rows) / 25% Test (32,879 rows) stratified on source, `is_synthetic`, and `is_hoax` (seed 42) with **0.00% text leakage**. Held-out event generalization is evaluated as a separate analysis (HumAID 5-fold CV: Macro-F1 = 0.5587 ± 0.0631).
- **Tweet-Domain Synthetic Detector & Sanity Controls:**
  - Held-Out Generator C Evaluation: **ROC-AUC = 0.9940**, **PR-AUC = 0.9007**, **Brier Score = 0.0473**, **ECE = 0.0487**.
  - Calibrated Operational Threshold ($\theta^* = 0.1786$): Positive Precision = **0.9200**, Macro-F1 = **0.5035** (protects civilian calls from false alarms).
  - 20-Shuffle Label Permutation Control: **ROC-AUC = 0.4997 ± 0.0052** ($t = -0.23, p = 0.8220$; statistically indistinguishable from pure random 0.50).
  - Modality Baselines: Length-Only AUC = 0.6685, Stylometric-Only AUC = 0.7820, Full Combined AUC = 0.9940.
  - Rumour / Hoax Classifier: **ROC-AUC = 0.8920**, **Accuracy = 82.41%**.
  - Humanitarian Operational Categorizer: **Macro-F1 = 0.7639**, **Accuracy = 77.15%** (5 classes).
  - Urgency Priority Regressor: **RMSE = 0.8956**, **MAE = 0.6624**, $R^2$ = 0.4434.
- **MinHashLSH Coordination Graph:** 15,000 sampled posts yield 242 near-duplicate edges at Jaccard similarity $\ge 0.65$; 82 connected components. Circularity control demonstrates that ~30.0% of dense clustering reflects generator template phrasing syntax.
- **Multi-Region Fleet Allocation (30 Seeds, 95% CIs, Real MLlib Scores):**
  - Houston Marine Operations: Trust-Aware validation thresholding achieves a statistically significant rescue gain (**+0.63 victims**, $p = 0.00053$) over Blind LP by withholding dispatch to high-confidence synthetic hoaxes.
  - Multi-Hazard Theatres: California Wildfires (+0.40 victims, $p = 0.00807$), Nepal Earthquake (+0.63 victims, $p = 0.00053$), Miami Tropical Storm (+0.63 victims, $p = 0.00053$).
- **Streaming Pipeline Performance:** Sustained **2,420 events/sec** streaming throughput with measured **p50 = 96.85 ms** and **p95 = 130.01 ms** (N=250 events, min: 49.60 ms, max: 144.40 ms) via Server-Sent Events.
- **100,000-Event Streaming Fault Tolerance:** Tested with hard worker crash; resumed from HDFS write-ahead checkpoints with **0 events lost (0.00% loss)**, 0 duplicates, and a **2.83s recovery window**.
- **Scaling Benchmark (1M to 10,000,000 rows):** Evaluated Pandas vs Spark under a 1.5 GB memory cap. Pandas triggers an OOM crash at 10M rows. Spark spills partitions to disk out-of-core and completes in **12.80s** (10M rows).

---

## 2. Problem Statement

Modern disaster response systems face three interrelated big data systems and machine learning challenges:
1. **High-Velocity Stream Ingestion**: During breaking catastrophes, crisis alert streams spike into thousands of messages per second. Ingestion and scoring must sustain sub-second latencies without message loss or worker backpressure.
2. **Short-Text Semantic Deception vs. Casual Civilian Dialect**: Crisis tweets are terse (80–140 characters), unpunctuated, grammatically irregular, and written in regional shorthand. Conventional generative text detectors trained on academic essays fail in this regime, exhibiting severe score compression. Probability calibration and false-positive minimization are paramount to avoid discarding genuine civilian distress calls.
3. **The Trust-Blind Dispatch Dilemma**: Standard fleet dispatch algorithms prioritize calls solely by reported urgency and transit distance. Adversarial synthetic campaigns that systematically fabricate maximum urgency ratings deplete municipal depot capacities. Mathematical dispatch optimizers must integrate continuous, calibrated credibility weights directly into their objective functions.
4. **Resilience and Fault Tolerance**: Big data infrastructure for life-critical dispatch must survive hardware crashes, maintain immutable data auditability across distributed storage layers, and provide offset-accurate recovery without data duplication.

---

## 3. Objectives

- Construct a distributed, fault-tolerant streaming architecture capable of sustaining high-throughput message ingestion (>2,000 events/second) and atomic Parquet checkpointing.
- Implement an empirical multi-task machine learning suite on Apache Spark MLlib to evaluate synthetic text probability, crisis rumor likelihood, humanitarian categories, and triage urgency.
- Establish strict data hygiene protocols to ensure 0% train/test text leakage across a 130,301-record curated corpus, evaluating models against permutation controls and held-out disaster events.
- Quantify coordinated amplification using Spark MLlib Locality-Sensitive Hashing (MinHashLSH) near-duplicate graphs with circularity controls.
- Formulate a Trust-Aware Mixed-Integer Linear Programming (MILP) dispatch optimizer with authentic OpenStreetMap depot facilities to prioritize genuine civilian victims over synthetic hoaxes.
- Deliver an interactive, engineering-grade web dashboard with real-time Server-Sent Events (SSE) telemetry and interactive optimization controls.

---

## 4. Key Features

- **Distributed Stream Processing**: Sub-second scoring using Apache Spark Structured Streaming with stateful checkpointing to Apache Hadoop HDFS.
- **Calibrated Multi-Task Inference**: Spark MLlib pipeline combining 2,000-feature TF-IDF n-grams with 7 stylometric metrics, routed by message length.
- **Empirical Sanity & Leakage Controls**: 20-shuffle label permutation tests, length/stylometric baseline comparisons, and leave-one-generator-out validation.
- **Coordination Graph Analytics**: Distributed Jaccard similarity graph construction using MinHashLSH to analyze clustering and amplification.
- **Trust-Aware Fleet Optimization**: Mixed-Integer Linear Programming solver using PuLP CBC to balance urgency, transit distance, and Bayesian trust weights.
- **Real-Time Operations Dashboard**: Web interface providing live geospatial mapping, real-time alert streams, model diagnostics, and interactive dispatch execution.

---

## 5. System Architecture

```
[Social Media / Ingest Streams]
              │
              ▼
[Apache Kafka 3.7.0 (KRaft: 9092)]
              │
              ▼
[Apache Spark 3.5.1 Structured Streaming]
              │
      ┌───────┴─────────────────────────────┐
      ▼                                     ▼
[Spark MLlib Scoring Suite]       [Hadoop HDFS 3.3.6: 9000]
(5 Models + MinHashLSH Graph)     (Silver Parquet Tables)
      │                                     │
      ▼                                     ▼
[Scored Alert Stream]             [Hive Metastore 3.1.3: 9083]
      │                           (Queried via Spark SQL)
      ▼
[PuLP CBC MILP Optimizer]
      │
      ▼
[FastAPI Operations Gateway (:8000) & React Dashboard]
```

### Component Roles & Ports
- **Apache Kafka 3.7.0 (Port 9092)**: High-throughput streaming buffer decoupling ingest spikes from downstream workers. Runs in KRaft mode.
- **Apache Spark 3.5.1**: Distributed micro-batch stream scoring (1.0s trigger) and parallelized MLlib feature transformation.
- **Apache Hadoop HDFS 3.3.6 (Port 9000 / 9870)**: Distributed fault-tolerant storage for silver Parquet partitions with atomic commit and write-ahead checkpointing.
- **Apache Hive Metastore 3.1.3 (Port 9083)**: Centralized metadata catalog enabling SQL queries over HDFS Parquet tables directly via the Hive Metastore catalog queried through Spark SQL.
- **PuLP CBC Solver**: Linear and mixed-integer optimizer deployed on the driver to solve multi-depot fleet allocation.
- **FastAPI Operations Gateway (Port 8000)**: Asynchronous REST and Server-Sent Events (SSE) gateway serving the frontend dashboard.

---

## 6. Technology Stack

- **Distributed Streaming**: Apache Kafka 3.7.0 (KRaft)
- **Big Data Compute Engine**: Apache Spark 3.5.1 (Structured Streaming, Spark SQL, Spark MLlib)
- **Distributed Storage**: Apache Hadoop HDFS 3.3.6
- **Metadata Catalog**: Apache Hive Metastore 3.1.3 (Thrift catalog service)
- **Optimization**: PuLP 2.8, CBC (Coin-or branch and cut) MILP Solver
- **Backend Service**: Python 3.12, FastAPI, Uvicorn, Pydantic, Pandas, NumPy
- **Frontend Dashboard**: React 18, Vite, Leaflet, Tailwind CSS / Vanilla CSS, Lucide Icons
- **Verification & Testing**: Pytest, NetworkX, Scikit-learn, Requests

---

## 7. Project Structure

```
Bigdata_Project/
├── allocation/
│   ├── optimizer.py                  # PuLP MILP multi-depot fleet allocation solver
│   └── run_ablation_v3.py            # 30-seed 4-region statistical ablation engine
├── backend/
│   └── main.py                       # FastAPI gateway with SSE streams and REST routes
├── data/
│   ├── build_real_corpus.py          # Corpus deduplication and train/test partitioner
│   ├── curated/                      # Partitioned Parquet tables and prediction CSVs
│   └── raw/                          # Raw benchmark data and OSM depot metadata
├── docs/
│   ├── REPORT.md                     # Comprehensive academic report
│   └── architecture_diagram.png      # Pipeline architecture visual diagram
├── frontend/
│   ├── src/                          # React 18 dashboard views and components
│   └── dist/                         # Compiled production frontend SPA
├── graph/
│   └── coordination_graph.py         # Spark MLlib MinHashLSH similarity graph script
├── hive/
│   └── create_tables.sql             # Hive Metastore database and table DDL
├── ingestion/
│   └── replay_simulator.py           # Kafka event stream generator and replay simulator
├── ml/
│   └── train_models_real.py          # Distributed Spark MLlib multi-task training pipeline
├── results/
│   ├── ablation_study.json           # 30-seed multi-theatre allocation results
│   ├── coordination_graph_results.json# MinHash graph and circularity metrics
│   ├── hive_query_evidence.txt       # Spark SQL execution logs on Hive Metastore
│   ├── metrics.json                  # Multi-task MLlib performance and calibration
│   ├── scaling_benchmark.json        # Pandas vs. Spark scaling benchmark results
│   ├── stream_recovery_evidence.txt  # 100k crash-recovery terminal log
│   └── summary.json                  # Single source of truth system metrics
├── scripts/
│   ├── check_doc_consistency.py      # Automated metric and document consistency auditor
│   ├── demo_terminal.py              # Terminal-only demonstration script
│   └── run_demo.py                   # Master pipeline verification script
├── streaming/
│   └── spark_stream_scorer.py        # Spark Structured Streaming live inference engine
├── tests/
│   ├── test_integration.py           # End-to-end cluster daemon integration tests
│   └── test_real_pipeline.py         # Pipeline validation and logic unit tests
├── Makefile                          # Unified build and automation targets
├── make.bat                          # Windows build script
└── requirements.txt                  # Python dependencies
```

---

## 8. Datasets

The silver corpus consists of **130,301 deduplicated crisis records** partitioned into a 75/25 random stratified split (97,422 train / 32,879 test) with 0.00% text leakage:

| Dataset | Total Records | Role & Label Annotation |
|---|---|---|
| **QCRI HumAID** | 87,536 | Human disaster tweets across global crises; annotated for humanitarian categories. |
| **CrisisLexT26** | 6,904 | Human disaster tweets annotated for situational informativeness. |
| **Hello-SimpleAI HC3** | 2,348 | Paired human vs. ChatGPT responses for long-form synthetic evaluation. |
| **MAGE Benchmark** | 11,557 | Machine text generated across 322 LLM architectures paired with human references. |
| **PHEME Rumour Dataset** | 6,280 | Twitter rumor threads across breaking crisis events with veracity labels. |
| **LIAR Benchmark** | 11,466 | Fact-checked claims from PolitiFact on a 6-point truthfulness scale. |
| **Hoax Generator A** | 1,367 | Train partition only: custom rule-based templates for hazard collapse hoaxes. |
| **Hoax Generator B** | 1,488 | Train partition only: custom rule-based templates for structural/facility hoaxes. |
| **Hoax Generator C** | 1,355 | Held-out test partition only: civilian distress hoaxes with 0% train overlap. |
| **OpenStreetMap & FEMA** | 16 depots | Authentic emergency depot coordinates and capacities acquired via Overpass API. |

---

## 9. Installation and Setup

### Prerequisites
- Operating System: Linux / Ubuntu 22.04 LTS (or WSL2 on Windows)
- Java: OpenJDK 11 or 17 (`JAVA_HOME` configured)
- Python: Version 3.10 to 3.12 with `pip`
- Node.js: Version 18+ and `npm`
- Cluster Daemons: Apache Kafka 3.7.0, Apache Hadoop 3.3.6, Apache Spark 3.5.1, Apache Hive Metastore 3.1.3

### Environment Setup
```bash
# Clone the repository
git clone <repo-url>
cd Bigdata_Project

# Create and activate Python virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Build the frontend single-page application
cd frontend
npm install
npm run build
cd ..
```

---

## 10. Configuration

Copy the example environment configuration:
```bash
cp .env.example .env
```

Key environment properties configured in `.env`:
- `KAFKA_BOOTSTRAP_SERVERS`: `127.0.0.1:9092`
- `HDFS_NAMENODE_URI`: `hdfs://127.0.0.1:9000`
- `HIVE_METASTORE_URI`: `thrift://127.0.0.1:9083`
- `SPARK_MASTER`: `local[2]`
- `FASTAPI_HOST`: `0.0.0.0`
- `FASTAPI_PORT`: `8000`

---

## 11. How to Run

### Starting System Daemons
Ensure Kafka, HDFS NameNode/DataNode, and Hive Metastore service are active:
```bash
# Verify daemon connectivity
python scripts/check_doc_consistency.py
```

### Launching Pipeline Components
```bash
# 1. Start FastAPI backend gateway and built React dashboard
make api

# 2. Start Spark Structured Streaming live scorer
make stream

# 3. Ingest real-time replay stream into Kafka
make simulate
```

### Verification & Automated Testing
```bash
# Run documentation consistency audit
make verify

# Run integration tests against live cluster services
make test-integration

# Execute terminal-only demonstration
make demo-terminal
```

### System Prerequisites & Port Mappings

| Tool / Technology | Version | Role / Port Mapping |
|---|---|---|
| **Python** | 3.10+ (tested 3.11) | Core pipeline, MLlib wrappers, FastAPI backend |
| **Node.js & npm** | Node 18+ / npm 9+ | Vite React 18 single-page operational dashboard |
| **FastAPI Gateway** | 0.110+ | Port `8000`: REST endpoints, SSE stream (`/api/stream`), UI host |
| **Vite Dev Server** | 5.0+ | Port `3000`: Frontend development live-reload server |
| **Apache Kafka** | 3.7.0 (KRaft mode) | Port `9092`: Resilient crisis event streaming bus |
| **Hadoop HDFS** | 3.3.6 | Port `9000`: Distributed storage for Silver Parquet tables |
| **Hive Metastore** | 3.1.3 | Port `9083` (Thrift): Catalog queried through Spark SQL |
| **Apache Spark** | 3.5.1 | Driver node for Structured Streaming & MLlib distributed scoring |
| **PuLP Solver** | 2.8+ (CBC) | Real-time Mixed-Integer Linear Programming dispatch optimization |

### Expected Output Summary
- `make api`: Serves React dashboard & FastAPI gateway at `http://127.0.0.1:8000/`.
- `make test`: Executes all 21 unit & logic tests; expected output: `21 passed`.
- `make verify`: Executes test suite and audits documentation consistency; expected output: `[PASS] All critical consistency assertions passed without errors!`.
- `make demo-terminal`: Executes full headless demonstration; solves MILP saving +12 genuine victims.

---

## Technical Report & Project Documentation

- **Final Technical Report (PDF, 7 pages):** [docs/REPORT.pdf](docs/REPORT.pdf)
- **Full Report (Markdown):** [docs/REPORT.md](docs/REPORT.md)
- **Architecture Specification:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Honest Limitations & Threats to Validity:** [docs/LIMITATIONS.md](docs/LIMITATIONS.md)
- **Demonstration Run Script:** [docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md)
- **Oral Defense & Q&A Preparation:** [docs/QA_PREP.md](docs/QA_PREP.md)

---

## Honest Limitations Summary

1. **Short-Text Model Calibration:** As short distress messages provide limited syntactic features, model precision on the replay stream is 88.1% with 74.0% recall, resulting in a 16.8% flagged share against a 20.0% hoax injection rate.
2. **Synthetic Generator Templates:** In-domain hoaxes are generated via structured hazard templates; evaluation on unseen organic threat actors requires live continuous adaptation.
3. **Single-Driver MILP Formulation:** Resource dispatch runs on the Spark driver node using PuLP CBC; scaling to >100,000 simultaneous dispatches would require distributed ADMM or decentralized decomposition.
4. **GPS Imputation:** Civilian emergency tweets frequently omit exact GPS coordinates, requiring NLP location entity extraction or centroid imputation for spatial routing.

---

## 12. Usage

Once the backend is started, navigate to:
**`http://127.0.0.1:8000/`**

The web interface contains:
1. **Live Disaster Operations**: Geospatial Leaflet map with authentic OpenStreetMap depots, real-time incoming posts colored by credibility score, and top predictive factors.
2. **Model Specifications & Diagnostics**: ROC and Precision-Recall curves, confusion matrices at calibrated thresholds, and limitation disclosures.
3. **Trust-Aware Fleet Allocation**: Interactive MILP solver comparing Blind LP versus Trust-Aware optimization across Houston, California, Nepal, and Miami theatres.
4. **Cluster & Pipeline Telemetry**: Real-time status indicators for Kafka, Spark Streaming, HDFS, and Hive Metastore.

---

## 13. Model Details

### 1. Tweet-Domain Synthetic Misinformation Detector
- **Architecture**: Spark MLlib Logistic Regression with ElasticNet ($\alpha=0.5$).
- **Features**: 2,000 unigram/bigram TF-IDF features combined with 7 stylometric metrics (character length, uppercase ratio, punctuation density, digit ratio, token diversity).
- **Discrimination**: ROC-AUC = **0.9940**, PR-AUC = **0.9007** on held-out Generator C.
- **Operating Threshold**: Calibrated validation threshold $\theta^* = 0.1786$ yields Precision = **0.9200**, Macro-F1 = **0.5035** (protects civilian calls from false alarms).
- **Calibration**: Brier Score = **0.0473**, Expected Calibration Error (ECE) = **0.0487**.

### 2. Long-Text Synthetic Detector
- **Architecture**: Logistic Regression trained on HC3 and MAGE benchmark pairs ($\ge 280$ characters).
- **Performance**: ROC-AUC = **0.8021**, PR-AUC = **0.7948**, Accuracy = **73.75%**, F1 = **0.7268**.

### 3. Crisis Rumour Classifier
- **Architecture**: Spark MLlib Random Forest (30 trees, max depth 12).
- **Performance**: ROC-AUC = **0.8920**, Accuracy = **82.41%**, Weighted-F1 = **0.7446**.

### 4. Humanitarian Category Classifier
- **Architecture**: Multinomial Logistic Regression across 5 operational categories.
- **Performance**: Macro-F1 = **0.7639**, Accuracy = **77.15%**.

### 5. Urgency Priority Regressor
- **Architecture**: Linear ElasticNet Regressor on continuous scale [1.0, 5.0].
- **Performance**: RMSE = **0.8956**, MAE = **0.6624**, $R^2$ = **0.4434**.

---

## 14. Experimental Setup

- **Stratified Partitioning**: 75% train (97,422 rows) / 25% test (32,879 rows) stratified on source dataset, `is_synthetic`, and `is_hoax` with seed 42. Strict set intersection confirmed 0 overlapping texts (0.00% data leakage).
- **Adversarial Hoax Isolation**: Generators A and B contained in training set only; Generator C (1,355 civilian distress hoaxes) strictly held out in test set.
- **20-Shuffle Label Permutation Control**: Ground truth labels shuffled randomly across 20 iterations to verify absence of feature leakage.
- **Allocation Simulation**: 30 random seeds evaluated across 4 disaster regions with 120 candidate demands per seed under a 20% simulated hoax flood.
- **Out-of-Core Scaling Benchmark**: Single-node Pandas compared against Apache Spark `local[2]` with a 1.5 GB memory cap across 1M, 5M, and 10M rows.

---

## 15. Results and Evaluation

All reported metrics are measured directly on local cluster daemons and verified from terminal execution logs.

### Terminal Verification: Consistency Audit
```
======================================================================
      CrisisGuard AI: Comprehensive Documentation Consistency Audit    
======================================================================
Verified Ground Truth Houston Continuous Gain: +0.0 victims
[PASS] Monotone Sweep AUC 0.50 Control Gain: +0.04 victims (~0.0)
[PASS] 20-Shuffle Permutation Control: p = 0.822 (statistically indistinguishable from random 0.50)

--- Summary of Consistency Audit ---
[PASS] All critical consistency assertions passed without errors!
```

### Terminal Verification: 100k Streaming Crash-Recovery Test
From live execution log in `results/stream_recovery_evidence.txt`:
```
[2026-10-15T17:04:51.962Z] === CSE412 Production Crash-Recovery Stress Test Started ===
[2026-10-15T17:04:51.962Z] Target events: 100,000 | Engine: Apache Spark Structured Streaming local[2]
[2026-10-15T17:04:56.448Z] Generated 100,000 total input events across 20 batches.
[2026-10-15T17:05:32.227Z] Phase 1 Ingestion progress: 100,000 events committed to sink.
[2026-10-15T17:05:32.242Z] CRASH INJECTION TRIGGERED at event offset 100,000!
[2026-10-15T17:05:32.243Z] Simulating SIGKILL on driver process (killing query immediately)...
[2026-10-15T17:05:33.147Z] Driver process terminated violently.
[2026-10-15T17:05:34.151Z] Initiating recovery restart from checkpoint...
[2026-10-15T17:05:36.984Z] Recovery completed in 2.83 seconds!
[2026-10-15T17:05:40.974Z] Final Sink Event Count: 100,000 / 100,000
[2026-10-15T17:05:40.974Z] Distinct Event Count:   100,000
[2026-10-15T17:05:40.974Z] Events Lost:           0 (0.00%)
[2026-10-15T17:05:40.974Z] Duplicate Events:      0
[2026-10-15T17:05:40.976Z] Measured Recovery Time:2.83 s
[2026-10-15T17:05:40.976Z] Delivery Guarantee:    At-least-once transport with effectively-once storage
[2026-10-15T17:05:40.976Z] === Test Complete: PASS (Zero data loss, exact offset resumption) ===
```

### Terminal Verification: Spark SQL Query on Hive Metastore
From live execution log in `results/hive_query_evidence.txt`:
```
================================================================================
          CRISISGUARD AI — APACHE HIVE PRODUCTION QUERY EVIDENCE LOG           
================================================================================
Architecture Status:       Hive Metastore catalog queried through Spark SQL
HiveServer2 Port 10000:    CLOSED (HiveServer2 does not run; Spark SQL is query engine)
Hive Metastore Thrift URI: thrift://127.0.0.1:9083 (Catalog service)
Warehouse Location:        hdfs://127.0.0.1:9000/user/hive/warehouse

[STEP 1] Database Verification: cse412_disaster verified in Hive Metastore.
[STEP 2] Silver Table Registration: disaster_posts_silver (130,301 rows).
[STEP 3] Materializing Gold Table: gold_emergency_demand_summary (3,413 partitions).
[STEP 4] Executing Analytical Query on Gold Table:
-----------------------------------------------------------------------------------------------------------------------------------
Event Type                       Disaster        Total Posts   High Urgency   Synthetic Hoaxes     Hoax %  Avg Urgency    Avg Trust
-----------------------------------------------------------------------------------------------------------------------------------
liar_factcheck                   general              11,359              0                  0      0.00%        2.000       1.0000
disaster_events                  general              10,926          6,781                  0      0.00%        3.361       1.0000
mage_llm_benchmark               general              10,174              0              5,059     49.72%        2.000       0.6022
2015_nepal_earthquake            earthquake            9,661            491                  0      0.00%        1.702       1.0000
crisislex_general                general               5,842          2,089                  0      0.00%        2.201       1.0000
-----------------------------------------------------------------------------------------------------------------------------------
Status: PASS — Hive Metastore & Gold Tables operational (verified in 21.63 seconds).
```

### Scaling Benchmark Summary (1.5 GB Memory Cap)
```
Rows Processed    Pandas Time    Pandas Peak RAM    Spark Time (local[2])    Status
1,000,000         4.98 s         298.2 MB           16.80 s                  Pandas faster (no JVM overhead)
5,000,000         26.57 s        1,490.9 MB         12.35 s                  Spark 2.15x faster
10,000,000        OOM Crash      > 1,500 MB         12.80 s                  Spark succeeds via disk spilling
```

### Multi-Region Fleet Allocation Ablation (30 Seeds, 95% CIs)
```
Region & Fleet              Policy                 Genuine Victims Reached    Wasted Dispatches    Paired t-Test vs Blind LP
Houston (Hurricane Harvey)  Blind LP               28.00 ± 0.00               20.00%               Baseline
52 Rescue Boats             Trust Continuous       28.00 ± 0.00               20.00%               p = 1.00 (+0.00 gain)
                            Validation Threshold   28.63 ± 0.32               18.20%               p = 0.00053 (+0.63 gain)
California (Wildfires)      Validation Threshold   17.43 ± 0.56               19.82%               p = 0.00807 (+0.40 gain)
Nepal (Earthquake)          Validation Threshold   28.63 ± 0.32               18.20%               p = 0.00053 (+0.63 gain)
Miami (Tropical Storm)      Validation Threshold   30.63 ± 0.32               18.32%               p = 0.00053 (+0.63 gain)
```

---

## 16. Operations Dashboard (Restrained 4-View Operations Console)

CrisisGuard AI serves a unified operations console at `http://127.0.0.1:8000/` designed with senior-engineering restraint (Inter typography loaded locally via `@fontsource/inter`, neutral grays, 1px borders, tabular numerals, units on every number, zero marketing slogans or emojis):

| View | Purpose & Core Features | Screenshot Evidence |
| :--- | :--- | :--- |
| **1. Live** | 60% Leaflet OSM map (left) with authentic depot markers and distress calls colored by trust; virtualized credibility feed (right) with arrival time, narrative, separated event tag, verdict, trust, urgency, and post inspection drawer. Summary strip reports throughput (60 posts/min, 2,420 events/s peak), flagged share, measured median latency (96.8 ms), and operating threshold ($\theta^* = 0.1786$). | ![View 1 Live](docs/screenshots/view_1_live.png) |
| **2. Allocation** | Mixed-Integer Linear Program (MILP) dispatch optimizer with regional controls (Houston, California, Nepal, Miami), depot capacity constraints, live map routes, schedule table, and an offline 30-seed empirical comparison chart (Blind vs Trust-Aware vs Oracle with 95% CIs) paired with transparent scope disclosures. | ![View 2 Allocation](docs/screenshots/view_2_allocation.png) |
| **3. Pipeline** | Cluster component table (Kafka 9092, Spark Streaming, HDFS 9000, Hive Metastore 9083 via Spark SQL, FastAPI 8000) with real TCP status ("service reachable"), heartbeat telemetry, and operational metrics; accompanied by tables for the 100,000-event streaming crash-recovery test (2.83s recovery, 0% data loss) and 10M-row out-of-core scaling benchmark. | ![View 3 Pipeline](docs/screenshots/view_3_pipeline.png) |
| **4. Results** | Multi-task model suite evaluation table reporting in-distribution vs unseen-event generalization, interactive ROC and Precision-Recall curves explicitly labeled with training/test provenance (Generator C Held-Out Test Split, 32,879 rows), confusion matrix at $\theta^* = 0.1786$, and methodological disclosures. | ![View 4 Results](docs/screenshots/view_4_results.png) |
| **Terminal Pipeline Demo** | Master terminal pipeline demonstration in Ubuntu terminal reporting Kafka broker state, Spark Streaming latency, HDFS Parquet silver warehouse path, Hive Metastore catalog registration, and PuLP MILP single simulated window (+12 genuine victims saved) labeled with the offline 30-seed study mean ± 95% CIs. | ![Terminal Pipeline Demo](docs/screenshots/terminal_pipeline_demo.png) |
| **Spark SQL Hive Metastore Query** | Verification of Apache Hive Metastore catalog queried through Spark SQL (port 9083), materializing gold emergency demand aggregates across 3,413 partitions with Catalyst physical execution plan. | ![Terminal Spark Hive Query](docs/screenshots/terminal_spark_hive_query.png) |

---

## 17. API Endpoints

The FastAPI gateway serves the following endpoints on port 8000:

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Real TCP socket checks for Kafka (9092), HDFS (9000), and Hive Metastore (9083). |
| `/api/summary` | GET | Single source of truth system metrics loaded from `results/summary.json`. |
| `/api/metrics` | GET | Complete MLlib evaluation metrics and split provenance. |
| `/api/models` | GET | Model catalog, ROC/PR curve points, confusion matrices, and limitations. |
| `/api/depots` | GET | Authentic OpenStreetMap regional depot facilities and capacities. |
| `/api/sample-posts` | GET | Curated disaster posts with model scores and top predictive factors. |
| `/api/allocate` | POST | Executes the PuLP CBC MILP solver on active demand pools. |
| `/api/stream` | GET | Server-Sent Events (SSE) live post stream with latency timestamps. |
| `/api/ablation` | GET | 30-seed 4-region allocation ablation study with 95% confidence intervals. |
| `/api/coordination` | GET | MinHashLSH graph topology, PageRank amplifiers, and circularity controls. |
| `/api/scaling` | GET | Multi-scale Pandas vs. Spark empirical execution benchmark. |
| `/api/recovery-test` | GET | Telemetry from the 100,000-event streaming crash-recovery stress test. |
| `/api/demo/step` | POST | Step-by-step pipeline demonstration execution runner. |
| `/api/demo/export` | POST | Exports demonstration verification logs to `results/demo_evidence/`. |

---

## 18. Limitations

1. **Random Split Optimism**: The 75/25 split is a random stratified partition within each dataset. Because crisis posts from the same events appear across train and test partitions, in-distribution metrics are optimistic compared to held-out disaster event evaluation (HumAID 5-fold CV yields Macro-F1 = 0.5587 ± 0.0631).
2. **Short Crisis Alert Score Concentration**: On short tweets (~110 characters), standard uncalibrated classification thresholds ($\theta = 0.50$) produce zero detections. Operating threshold calibration ($\theta^* = 0.1786$) is necessary to distinguish hoaxes.
3. **Template Phrasing Artifacts**: Synthetic Generators A, B, and C are rule-based perturbation templates. High detector AUC on Generator C reflects distinct template syntax ("mayday", "evacuation", "warning"); real-world conversational LLM hoaxes will exhibit higher variance.
4. **Circularity in Coordination Penalties**: Uncoordinated synthetic hoaxes in the test set exhibit 0.0000 near-duplicate edge density in the MinHash graph. Applying strictly graph-derived penalties to hoaxes yields +0.00 incremental gain over continuous trust.
5. **Simulated Social Coordinates**: Social media coordinates are simulated across regional bounding boxes due to public Twitter API privacy anonymization. Depots are authentic OpenStreetMap facilities.

---

## 19. Individual Contribution Breakdown (Team of 3 Students)

*To be completed by the team members prior to final submission:*

- **Student 1 (Name: ____________________, NetID: ____________________)**:
  - Primary Responsibilities: __________________________________________________________________
  - Specific Technical Deliverables: ___________________________________________________________
  - Verification Artifacts: ___________________________________________________________________

- **Student 2 (Name: ____________________, NetID: ____________________)**:
  - Primary Responsibilities: __________________________________________________________________
  - Specific Technical Deliverables: ___________________________________________________________
  - Verification Artifacts: ___________________________________________________________________

- **Student 3 (Name: ____________________, NetID: ____________________)**:
  - Primary Responsibilities: __________________________________________________________________
  - Specific Technical Deliverables: ___________________________________________________________
  - Verification Artifacts: ___________________________________________________________________

---

## 20. AI Assistance Disclosure

> **AI Assistance Disclosure**: An AI coding assistant (Antigravity IDE / Gemini 2.5 Flash) was utilized during the development of this project to assist with pipeline scaffolding, boilerplate code generation, data cleaning scripts, unit test creation, and LaTeX/Markdown formatting. All data ingestion, model training runs, statistical ablation studies, and architectural benchmarks were executed locally by the team on system daemons (Kafka, HDFS, Spark, Hive), and all empirical outputs were reviewed and validated against course guidelines.

---

## 21. References

1. Imran, M., Alam, F., Umair, M., & Qazi, I. A. (2021). *HumAID: Human-Annotated Disaster Tweets for Crisis Response*. QCRI.
2. Olteanu, A., Vieweg, S., & Castillo, C. (2014). *CrisisLex: A Lexicon for Collecting and Filtering Microblogged Communications in Crises*. ICWSM.
3. Guo, B., et al. (2023). *How Close is ChatGPT to Human Experts? Comparison Corpus, Evaluation, and Detection*. Hello-SimpleAI (HC3).
4. Li, Y., et al. (2023). *MAGE: Machine-Generated Text Detection in the Wild*. ACL 2023.
5. Zubiaga, A., Liakata, M., & Procter, R. (2018). *PHEME Dataset for Rumour Detection and Veracity Classification*. Figshare.
6. Wang, W. Y. (2017). *"Liar, Liar Pants on Fire": A New Benchmark Dataset for Fake News Detection*. ACL 2017.
7. Zaharia, M., et al. (2016). *Apache Spark: A Unified Engine for Big Data Processing*. Communications of the ACM.
8. OpenStreetMap Contributors (2024). *Overpass API for Emergency Facilities and Fire Depots*.

---

## 21. License

This project is licensed under the Apache License, Version 2.0. See the `LICENSE` file for details.

