# CrisisGuard
### Real-Time Detection of Synthetic Misinformation and Trust-Aware Emergency Resource Allocation



**CrisisGuard** is a real-time emergency coordination dashboard and distributed data processing pipeline. It helps emergency coordinators answer three critical questions during natural disasters:
1. *What is being reported right now?*
2. *Which reports look unreliable or synthetic?*
3. *Where should limited rescue units be dispatched?*

The system combines **Apache Kafka 3.7.0 (KRaft)** for stream ingestion, **Apache Spark 3.5.1 Structured Streaming & MLlib** for sub-second feature extraction and inference, **Hadoop HDFS 3.3.6** with **Apache Hive Metastore 3.1.3** (Hive Metastore catalog queried through Spark SQL) for durable storage, and a **PuLP CBC Mixed-Integer Linear Programming (MILP)** optimizer with authentic OpenStreetMap depot facilities.

---

## 1. Quickstart

```bash
# 1. Clone repository and install dependencies
git clone https://github.com/sivasaiboggu/Bigdata_Project.git
cd Bigdata_Project
make setup

# 2. Run system preflight checks
make preflight

# 3. Launch end-to-end demo and open the dashboard
make demo-ready

# 4. Run tests and verify documentation consistency
make verify

# 5. Stop demo services when finished
make demo-stop
```

### Essential Makefile Commands

| Command | Action |
|---|---|
| `make setup` | Install Python requirements and build the frontend SPA |
| `make preflight` | Print plain green/red checklist of cluster, models, and API |
| `make up` / `make down` | Start / stop Docker cluster daemons (Kafka, HDFS, Hive) |
| `make test` | Run fast unit and logic test suite |
| `make test-integration`| Run end-to-end integration tests across daemons |
| `make verify` | Run test suite and check documentation consistency against `results/*.json` |
| `make demo` | Execute the full terminal pipeline demonstration |
| `make demo-ready` | Start services, streaming scorer, replay simulator, and launch dashboard |
| `make demo-stop` | Terminate demo processes |

---

## 2. Tools & Versions

| Component | Version | Role in Architecture |
|---|---|---|
| **Apache Kafka** | 3.7.0 (KRaft) | High-throughput disaster alert message broker (`disaster_raw_posts`, `disaster_scored_posts`) |
| **Apache Spark** | 3.5.1 | Structured Streaming feature extraction, MLlib scoring, and MinHashLSH coordination graph |
| **Hadoop HDFS** | 3.3.6 | Durable silver Parquet storage and write-ahead streaming checkpoint store |
| **Apache Hive** | 3.1.3 | External Metastore catalog schema management (Hive Metastore catalog queried through Spark SQL) |
| **FastAPI** | 0.115.14 | Low-latency serving gateway with Server-Sent Events (SSE) telemetry |
| **PuLP CBC** | 2.8.0 | Mixed-Integer Linear Programming (MILP) fleet allocation optimizer |
| **React + Vite** | 18.3.1 / 5.4.11 | Human-made emergency coordinator dashboard (Live, Dispatch, About) with Leaflet maps |

---

## 3. Folder Structure

```
Bigdata_Project/
├── allocation/      # PuLP MILP fleet optimization and 30-seed ablation study scripts
├── backend/         # FastAPI gateway, Server-Sent Events, and data endpoint handlers
├── data/            # Dataset provenance manifest, fetch scripts, and OSM emergency depots
├── docker/          # Docker Compose cluster definition (Kafka KRaft, HDFS, Hive Metastore)
├── docs/            # REPORT.md, REPORT.pdf, ARCHITECTURE.md, QA_PREP.md, DEMO_SCRIPT.md
├── frontend/        # React + Vite coordinator SPA (Live, Dispatch, About views)
├── graph/           # Spark MLlib MinHashLSH coordination graph generator
├── hive/            # Hive Metastore SQL schema definitions
├── ingestion/       # Kafka live disaster replay simulator
├── ml/              # Spark MLlib training pipeline, models, and evaluation routines
├── results/         # Single source of truth JSON files, metrics, and empirical evidence
├── scripts/         # Demo scripts, preflight, consistency checks
└── tests/           # Unit tests, integration tests, and infrastructure verifiers
```



## 4. Key Empirical Results 

All metrics are measured from the **130,301-record curated disaster corpus** (HumAID, CrisisLex, PHEME, HC3, MAGE, LIAR) with a strict 75/25 random stratified split (97,422 train / 32,879 test, seed 42) and zero text leakage. Generalization to unseen disaster events is evaluated as a separate analysis (HumAID 5-fold CV: Macro-F1 = 0.5587 ± 0.0631).

- **Synthetic Text Detector:** Held-out Generator C evaluation yields **ROC-AUC = 0.9940**, **PR-AUC = 0.9007**, Brier score = 0.0473. At the calibrated validation threshold ($\theta^* = 0.1786$), precision is **0.9200**.
- **20-Shuffle Label Permutation Control:** ROC-AUC = **0.4997 ± 0.0052** ($t = -0.23, p = 0.8220$; statistically indistinguishable from 0.50 random chance).
- **Streaming Telemetry:** Sustained **2,420 events/s** peak streaming throughput with measured **p50 = 96.85 ms** and **p95 = 130.01 ms** (N = 250 events).
- **Crash Recovery:** Tested across 100,000 events with hard worker termination; recovered in **2.83 s** with **0 events lost (0.00% loss)** via at-least-once delivery with effectively-once storage.
- **Scaling Benchmark (10M Rows):** Spark completes out-of-core partition disk spilling in **12.80 s** under a 1.5 GB memory cap, while Pandas crashes with an Out-of-Memory (OOM) error.
- **Resource Allocation (30-Seed Empirical Study):** Trust-aware validation thresholding saves a statistically significant rescue gain (**+0.63 victims**, $p = 0.00053$) over Blind LP in Houston by withholding dispatch to high-confidence synthetic hoaxes.

---

## 5. Limitations

- **Short Crisis Alert Score Compression:** On terse crisis tweets (~110 characters), standard classification thresholds ($\theta = 0.50$) produce zero detections. Validation threshold calibration ($\theta^* = 0.1786$) is required to identify synthetic hoaxes without discarding authentic civilian distress calls.
- **Simulated GPS Coordinates:** While regional depots are authentic facilities sourced from OpenStreetMap, social media post coordinates are simulated within regional bounding boxes due to Twitter API privacy anonymization.
- **Template Regularity in Synthetic Generators:** Rule-based generators produce template phrases ("mayday", "evacuation") that facilitate high detection accuracy; real-world LLM-generated campaigns exhibit higher stylistic variance.

---

## 6. AI Assistance Disclosure

> **AI Assistance Disclosure**: An AI coding assistant (Antigravity IDE / Gemini 2.5 Flash) was utilized during the development of this project to assist with pipeline scaffolding, boilerplate code generation, data cleaning scripts, unit test creation, and documentation formatting. All data ingestion, model training runs, statistical ablation studies, and architectural benchmarks were executed locally by the team on system daemons (Kafka, HDFS, Spark, Hive), and all empirical outputs were reviewed and validated against course guidelines.





