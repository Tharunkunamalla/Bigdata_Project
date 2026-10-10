# CrisisGuard AI: End-to-End Big Data Pipeline Walkthrough

**Project Title:** Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation  
**Course:** CSE412 — Big Data & Large-Scale Computing (Hybrid Tracks D & E)  
**Deliverable Status:** Production Ready & Demo Verified  
**Git Repository:** Clean repository at `c:\Users\HP\OneDrive\Desktop\bigdata project` (`master` branch)  

---

## 1. Executive Summary & Verification Scorecard

CrisisGuard AI is an end-to-end, production-grade big data analytics pipeline and emergency dispatch optimization platform designed to combat generative AI crisis misinformation during extreme natural disasters.

### Acceptance Criteria Scorecard:

| Evaluation Criteria | Target Metric | Measured Value | Status | Evidence File |
|:---|:---:|:---:|:---:|:---|
| **Big Data Tools Integrated** | $\ge 3$ Tools with real data flow | **5 Enterprise Tools** (Kafka, HDFS, Hive, Spark, PuLP) | **PASS** | [TOOL_JUSTIFICATION.md](docs/TOOL_JUSTIFICATION.md) |
| **Dataset Scale & Provenance** | Realistic Big Data Volume | **60,000 records** (4 disaster types, dual provenance) | **PASS** | [DATA.md](docs/DATA.md) |
| **Storage Architecture** | Medallion Pattern | Bronze $\to$ Silver Parquet $\to$ Hive Gold Catalog | **PASS** | [create_tables.sql](hive/create_tables.sql) |
| **ML Model Discrimination** | ROC-AUC $\ge 0.70$ (OOD split) | **ROC-AUC = 1.0000** (Acc = 95.89%) | **PASS** | [metrics.json](results/metrics.json) |
| **Streaming Fault Tolerance** | Zero Data Loss on Crash | **200 / 200 records recovered (0% loss)** | **PASS** | [stream_recovery_test.json](results/stream_recovery_test.json) |
| **Ablation Study (Misinformation)** | Empirically Saves Lives | **-97.29% Wasted Fleet**, **+38.71% Victims Saved** | **PASS** | [ablation_study.json](results/ablation_study.json) |
| **Scaling Benchmark** | Pandas vs Spark Comparison | Benchmarked across 10k, 50k, 100k, 250k rows | **PASS** | [scaling_benchmark.json](results/scaling_benchmark.json) |
| **Sybil Network Graph** | Bot Amplification Analysis | **50,072 users**, **331,466 edges** (20x amplification) | **PASS** | [coordination_graph_results.json](results/coordination_graph_results.json) |
| **Full-Stack Application** | Real-Time UI + REST/SSE | FastAPI + React/Vite Leaflet Dashboard | **PASS** | `http://127.0.0.1:8000/` |
| **Documentation & Demo** | 6–10 Page Report + Demo Script | Comprehensive Report, Architecture, Demo Script | **PASS** | [REPORT.md](docs/REPORT.md) |

---

## 2. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["1. Ingestion Layer"]
        Sim["Replay Simulator<br/>(ingestion/replay_simulator.py)"] -->|Kafka Producer| K_Raw["Kafka: disaster_raw_posts<br/>(127.0.0.1:9092)"]
        Raw["60k Crisis Records (JSON/CSV)"] -->|hdfs dfs -put| H_Bronze["HDFS Bronze Layer<br/>(/disaster_data/bronze/)"]
    end

    subgraph StorageCatalog["2. Medallion Storage & Catalog"]
        H_Bronze -->|PySpark Schema Validation| H_Silver["HDFS Silver Layer<br/>(/disaster_data/silver/)<br/>Partitioned by event_type (Parquet)"]
        H_Silver -->|MSCK REPAIR| HiveCat["Hive Metastore Catalog<br/>(cse412_disaster.disaster_posts_silver)"]
        HiveCat -->|Spark SQL| H_Gold["Hive Gold Table<br/>(gold_emergency_demand_summary)"]
    end

    subgraph ML_Streaming["3. MLlib & Structured Streaming"]
        H_Silver -->|Event-Stratified Train/Test| MLlib["Spark MLlib Model Suite<br/>(Synthetic, Rumour, Urgency)"]
        K_Raw -->|Streaming Ingestion| Scorer["Spark Structured Streaming<br/>(streaming/spark_stream_scorer.py)"]
        MLlib -.->|PipelineModel Load| Scorer
        Scorer -->|Offset Checkpointing| H_Check["HDFS Checkpoint Dir<br/>(Zero Data Loss)"]
        Scorer -->|Enriched Records| K_Scored["Kafka: disaster_scored_posts"]
    end

    subgraph DispatchOptimization["4. Optimization & Web Dashboard"]
        K_Scored --> API["FastAPI Backend (port 8000)<br/>(backend/main.py)"]
        API --> MILP["PuLP MILP Solver<br/>(Trust-Weighted Dispatch)"]
        API -->|SSE Stream & REST| UI["React Operations UI<br/>(Leaflet Map, 5 Panels)"]
    end
```

---

## 3. Key Findings & Quantitative Results

### 3.1 Machine Learning Suite (from results/metrics.json)
- **Evaluation Split:** Trained strictly on Hurricane Harvey & Kerala Floods (30k records); tested out-of-distribution on Kahramanmaraş Earthquake & California Wildfires (30k records).
- **Synthetic Detector:** ROC-AUC: **1.0000** | Accuracy: **95.89%** | F1: **0.9570**.
- **Rumour Classifier:** ROC-AUC: **0.9293** | F1: **0.9304**.
- **Urgency Regressor:** RMSE: **0.2184** | MAE: **0.1627**.

### 3.2 Resource Allocation Ablation Experiment (from results/ablation_study.json)
- **Blind Optimization (Standard MILP):** 28.46% (±7.23%) wasted dispatches to synthetic hoaxes. Only 37.2 / 52 genuine victims saved.
- **Trust-Aware Optimization (Proposed):** **0.77% (±0.94%)** wasted dispatches. **51.6 / 52 genuine victims saved**.
- **Impact:** **97.29% reduction in wasted fleet dispatches**, rescuing **+38.71% more genuine victims**.

### 3.3 Sybil Coordination Graph Analysis (from results/coordination_graph_results.json)
- **Scale:** 50,072 user vertices and 331,466 coordination edges.
- **Organic Citizens (46,732 accounts):** Average retweet count of 14.5; average account age of 2,341 days (~6.4 years).
- **Synthetic Bot Sybils (3,340 accounts):** Average retweet count of **297.2 (20x amplification)**; average account age of **103.5 days**.

---

## 4. Evaluator Quick Start Guide

### Option A: One-Command Master Demo
```bash
python scripts/run_demo.py
```
*Executes all 5 verification steps in under 3 seconds, printing cluster status, MLlib metrics, fault-tolerance recovery stats, and the live fleet MILP solution.*

### Option B: Interactive Operations Dashboard
The FastAPI backend and React Single-Page Application are live:
- Open your browser at: **`http://127.0.0.1:8000/`**
- **Panel 1 (Left):** Real-time incoming crisis posts with AI probability, urgency, and trust score gauges.
- **Panel 2 (Center):** Leaflet interactive map of the Houston disaster perimeter showing genuine victims (green), synthetic hoaxes (red), and naval depots (blue).
- **Panel 3 (Right):** Click **"Solve Fleet Dispatch Plan"** to run the live mathematical optimizer.
- **Tabs (Top):** Navigate between **Live Operations**, **Fleet Dispatcher & Ablation**, **MLlib Performance**, and **Cluster Topology**.
