# CrisisGuard AI 🛡️🌊
### Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation

[![Course](https://img.shields.io/badge/Course-CSE412%20Big%20Data-blue.svg)](docs/REPORT.md)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1%20MLlib-orange.svg)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7.0%20KRaft-black.svg)](https://kafka.apache.org/)
[![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-3.3.6-yellow.svg)](https://hadoop.apache.org/)
[![Hive Metastore](https://img.shields.io/badge/Hive%20Metastore-3.1.3-green.svg)](https://hive.apache.org/)
[![Pytest](https://img.shields.io/badge/Tests-100%25%20Passing-brightgreen.svg)](tests/test_real_pipeline.py)

**CrisisGuard AI** is an end-to-end, distributed big data analytics and optimization pipeline designed to protect emergency dispatch operations from generative AI-driven misinformation during catastrophic natural disasters. Built natively across **Apache Kafka**, **Apache Hadoop HDFS**, **Apache Hive Metastore**, **Apache Spark MLlib**, and **PuLP Mixed-Integer Linear Programming (MILP)**, CrisisGuard ingests high-velocity social media alerts, filters synthetic hoaxes, identifies botnet coordination via MinHashLSH, and solves optimal fleet dispatch plans to maximize genuine lives saved.

---

## 🌟 Key Empirical Highlights (Single Source of Truth)

All metrics are generated from **98,944 authentic crisis records** (QCRI CrisisBench / HumAID, CrisisLex, and Hello-SimpleAI HC3) evaluated across live cluster daemons. Zero numbers are hardcoded.

- **Authentic Silver Corpus:** 98,944 clean records partitioned in HDFS Parquet at `hdfs://127.0.0.1:9000/disaster_data/silver_real` and registered in Hive Metastore table `disaster_db.crisis_silver_real`.
- **Zero-Leakage Stratified Split:** Train on Hurricane Harvey & Kerala Floods (26,350 rows); test on Nepal Earthquake & California Wildfires (72,594 rows) with **0% text leakage**.
- **Spark MLlib Models & Sanity Controls:**
  - Synthetic Misinformation Detector: **ROC-AUC = 0.7081**, **Accuracy = 96.43%**, **F1 = 0.9556**.
  - Shuffled-Label Sanity Control: **ROC-AUC = 0.5557** (verifies zero latent label leakage; consistent with random chance).
  - Rumour / Hoax Classifier: **ROC-AUC = 0.6785**, **F1 = 0.9036**.
  - Humanitarian Operational Categorizer: **Macro-F1 = 0.5994**, **Accuracy = 61.67%**.
  - Urgency Regressor: **RMSE = 1.2277**, **MAE = 0.9838**.
- **MinHashLSH Coordination Graph:** 14,888 nodes; synthetic hoaxes exhibited **60.82 mean edges** vs **1.86 mean edges** for genuine citizens (**32.74x amplification ratio**).
- **Multi-Region Fleet Allocation (30 Seeds, 95% CIs):**
  - Houston Marine Operations: Genuine victims served increased from **$40.20 \pm 0.94$ to $51.80 \pm 0.15$** while wasted hoax dispatches were reduced from **22.69% to 0.38%** ($p = 1.45 \times 10^{-20}$, paired t-test).
  - Hoax Contamination Sweep (5% to 40%): Genuine victim gains scale up to **+46.93 lives saved** under severe attack intensity.
- **100,000-Event Streaming Fault Tolerance:** Tested with hard driver kill at event 60,000; resumed from HDFS checkpoints with **0 events lost (0.00% loss)**, 0 duplicates, and a **7.72s recovery window**.
- **Scaling Benchmark (10k to 1,000,000 rows):** Evaluated Pandas vs Spark; documented why Pandas is faster at $\le 100\text{k}$ rows due to zero JVM overhead, and why Spark is architecturally required for distributed streaming, HDFS storage, and out-of-core partition management.

---

## 🏛️ System Architecture

```
[Social Media / Alerts] ──> [Apache Kafka 3.7.0 (KRaft)]
                                  │
                                  ▼
                  [Apache Spark 3.5.1 Structured Streaming]
                                  │
         ┌────────────────────────┴────────────────────────┐
         ▼                                                 ▼
[Spark MLlib Classification]                      [Hadoop HDFS 3.3.6]
(4 Models + MinHashLSH Graph)                     (/silver_real Parquet)
         │                                                 │
         ▼                                                 ▼
[Scored Alert Stream]                             [Hive Metastore 3.1.3]
         │                                        (crisis_silver_real)
         ▼
[PuLP MILP Solver (Driver Node)]
         │
         ▼
[FastAPI Backend + React 18 / Vite Operations Dashboard]
```

---

## 🚀 Quickstart & Reproduction Guide

### Prerequisites
- Windows 10/11 with **WSL2 (Ubuntu 24.04 LTS)**
- Java 11 HotSpot (`openjdk-11-jdk`)
- Python 3.11 with virtual environment (`.venv`)
- Node.js 18+ and npm

### 1. Start Big Data Cluster Daemons (WSL2)
```bash
# In WSL2 Ubuntu terminal:
bash /home/sivasai/projects/crisisguard/scripts/start_all.sh
```
*Verifies Kafka (9092), HDFS NameNode (9000/9870), DataNode (9864), and Hive Metastore (9083).*

### 2. Verify Infrastructure & Backend Health
```bash
curl http://127.0.0.1:8000/api/health
```

### 3. Run Automated System Demonstration
```bash
python scripts/run_demo.py
```
*Executes an end-to-end audit, connects to live daemons, verifies 98,944-record dataset provenance, loads MLlib metrics, prints MinHash coordination stats, runs the 100k fault-tolerance summary, and executes a live PuLP MILP dispatch run.*

### 4. Run Comprehensive Pytest Suite
```bash
pytest -v tests/test_real_pipeline.py
```
*Runs 6 unit/integration tests verifying dataset manifests, 98,944-row corpus schema, zero-leakage splits, summary metrics validity, Haversine geometry, and PuLP optimizer capacity constraints.*

### 5. Launch Full-Stack Operations Console
Open your browser to:
👉 **`http://127.0.0.1:8000/`**
- **Tab 1: Live Operations:** Real-time HumAID alert feed, Leaflet crisis map, and live dispatch solver controls.
- **Tab 2: Fleet Dispatcher & Ablation:** Multi-region selector (Houston, California, Nepal, Miami) with 30-seed CIs and paired t-tests.
- **Tab 3: MLlib & Sanity Controls:** Model cards, Shuffled-Label control panel, and MinHash coordination graph metrics.
- **Tab 4: Cluster Topology & Scaling:** Live cluster daemons monitor and Pandas vs Spark 1M row benchmark.

---

## 📂 Project Repository Structure

```
bigdata project/
├── data/
│   ├── raw/                           # Raw datasets & provenance manifest
│   │   └── dataset_provenance_manifest.json  # SHA256 hashes & licenses
│   ├── curated/                       # Cleaned, un-leaked disaster corpora
│   │   ├── real_disaster_corpus.csv   # 98,944 deduplicated records
│   │   ├── train_split.csv            # 26,350 training records (Harvey/Kerala)
│   │   └── test_split.csv             # 72,594 held-out test records (Nepal/California)
│   ├── fetch_real_datasets.py         # Automated corpus download script
│   ├── build_real_corpus.py           # Deduplication & zero-leakage split script
│   └── ingest_real_to_hdfs.py         # Partitioned Parquet ingestion to HDFS
├── ml/
│   ├── models_real/                   # Serialized Spark MLlib models
│   ├── train_models_real.py           # MLlib training pipeline + sanity controls
│   └── model_card.md                  # Comprehensive model card documentation
├── graph/
│   └── coordination_graph.py          # Spark MinHashLSH botnet coordination graph
├── allocation/
│   ├── optimizer.py                   # PuLP Mixed-Integer Linear Program (MILP)
│   └── run_ablation_v2.py             # 30-seed multi-region ablation runner
├── streaming/
│   └── spark_stream_scorer.py         # Spark Structured Streaming pipeline
├── backend/
│   └── main.py                        # FastAPI production backend
├── frontend/
│   ├── src/                           # React 18 / Vite single-page dashboard
│   │   ├── App.jsx                    # 4-tab interactive operations console
│   │   └── index.css                  # Modern dark glassmorphic styling
│   └── dist/                          # Production static build bundle
├── tests/
│   ├── test_real_pipeline.py          # Pytest suite (6 passed, 100% green)
│   └── test_stream_recovery.py        # 100k-event crash recovery test
├── scripts/
│   ├── start_all.sh                   # Cluster daemon startup script
│   ├── stop_all.sh                    # Cluster daemon shutdown script
│   ├── run_demo.py                    # Master demo orchestrator
│   └── build_report_numbers.py        # Single-source-of-truth telemetry builder
├── results/
│   ├── summary.json                   # Machine-readable single source of truth
│   ├── report_numbers_snippet.md      # Auto-generated report table snippet
│   ├── AUDIT_BEFORE.md                # Phase A pre-rebuild integrity audit
│   └── AUDIT_AFTER.md                 # Final post-rebuild verification report
└── docs/
    ├── REPORT.md                      # Comprehensive academic course report (6-10 pages)
    ├── DEMO_SCRIPT.md                 # 10-minute timed presentation script
    ├── QA_PREP.md                     # 25 tough defense questions answered defensibly
    ├── OSS_CONTRIBUTION.md            # 4 upstream open-source PR proposals
    ├── PAPER_OUTLINE.md               # IEEE/ACM format academic paper outline
    ├── DATA.md                        # Dataset architecture & lineage guide
    ├── ARCHITECTURE.md                # Infrastructure topology & port mappings
    ├── LIMITATIONS.md                 # Technical boundaries & ethical scoping
    └── SETUP.md                       # WSL2 environment provisioning guide
```

---

## 👥 Team & Contribution Split (Team of 3 Students)

| Member | Role | Core Technical Responsibilities |
| :--- | :--- | :--- |
| **Student 1** | Infrastructure & Streaming Lead | Kafka 3.7 KRaft, HDFS 3.3.6, Hive Metastore 3.1.3, 100k-event stream crash-recovery test, and 1M scaling benchmark. |
| **Student 2** | Machine Learning & Graph Lead | Curated 98,944-record dataset, zero-leakage split, 4 Spark MLlib models, Shuffled-Label control (0.5557 AUC), and MinHashLSH graph (32.7x ratio). |
| **Student 3** | Allocation & Operations Lead | Trust-Aware PuLP MILP formulation, 30-seed multi-region ablation, paired t-tests ($p < 10^{-20}$), FastAPI backend, and React 18 / Vite dashboard. |

---

## ⚖️ License & Ethical Scoping
CrisisGuard AI is released under the **MIT License** for academic research.  
**Operational Directive:** CrisisGuard AI is an **emergency decision-support triaging assistant** for human incident commanders. It is not an autonomous physical actuator. First responders must never automatically withhold life-saving aid from a citizen based solely on an algorithmic score.
