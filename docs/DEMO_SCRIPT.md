# CrisisGuard AI — Terminal-First Pipeline Demonstration Script (`DEMO_SCRIPT.md`)

**Course:** CSE412 — Big Data Analytics  
**Institution:** Arizona State University  
**Architecture:** Apache Kafka 3.7.0, Apache Spark 3.5.1, Hadoop HDFS 3.3.6, Apache Hive Metastore 3.1.3 (queried via Spark SQL), PuLP CBC MILP Optimizer, FastAPI & Vite/React  
**Demo Paradigm:** Terminal-first execution (**one command at a time**), supporting dashboard at `http://127.0.0.1:8000/` (simple, light theme, honest).  
**Release Target:** `v1.0-final-10`

---

## Executive Overview for Evaluators

This demonstration presents the end-to-end CrisisGuard distributed big data pipeline **directly in the terminal**, executing each verification command one step at a time. The accompanying web dashboard serves as a supporting operational view: strictly light theme, displaying real-time Server-Sent Events, OpenStreetMap rescue coordination, and MILP dispatch planning with zero hardcoded or invented numbers.

All 11 demonstration scripts live in `scripts/demo/`, echo the exact underlying big data command before executing, show real cluster output only, exit non-zero on failure, and complete in under 20 seconds.

---

## 1. Quickstart Environment Setup

Before starting the presentation, activate the environment configuration:

```bash
# 1. Source demo configuration (broker, topics, HDFS paths, metastore URI)
source scripts/demo/env.sh

# 2. Run automated preflight readiness checklist
make preflight
```

`make preflight` verifies cluster services, Kafka topics, HDFS partition files, MLlib model artifacts, and API reachability with a clean green/red checklist.

---

## 2. Step-by-Step Terminal Pipeline Demonstration

### Step 1: Cluster Services & Port Verification
```bash
./scripts/demo/01_services.sh
```
- **Underlying Command:** `+ python tests/verify_infra.py`
- **What it Proves:** Probes live TCP sockets across the distributed cluster: Kafka broker (9092), HDFS NameNode IPC (9000), NameNode WebHDFS (9870), Hive Metastore Thrift listener (9083), and Spark execution environment.
- **Expected Output:**
  ```text
  [PASS] Apache Kafka 3.7      : Produced & consumed 1 message on topic 'infra_test_...'
  [PASS] Hadoop HDFS 3.3.6     : NameNode active (1 live DataNode, 60.7 MB total)
  [PASS] Apache Hive Metastore : Metastore Thrift API listening on port 9083
  [PASS] Apache Spark & MLlib  : Spark 3.5.1 session & MLlib libraries verified (1 test record processed)
  >>> ALL INFRASTRUCTURE GATES PASSED (Phase 2 Gate: GREEN) <<<
  ```

---

### Step 2: Kafka Topics & Partition End Offsets
```bash
./scripts/demo/02_kafka.sh
```
- **Underlying Commands:**
  - `+ kafka-topics --bootstrap-server 127.0.0.1:9092 --list`
  - `+ kafka-topics --bootstrap-server 127.0.0.1:9092 --describe --topic disaster_raw_posts,disaster_scored_posts`
  - `+ kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server 127.0.0.1:9092 --topic disaster_raw_posts,disaster_scored_posts --time -1`
- **What it Proves:** Inspects active Kafka topics and confirms continuous partition offset advancement on raw input (`disaster_raw_posts`) and scored output (`disaster_scored_posts`).

---

### Step 3: HDFS Silver Storage Layer & Disk Utilization
```bash
./scripts/demo/03_hdfs_data.sh
```
- **Underlying Commands:**
  - `+ hdfs dfs -ls -h /disaster_data/silver_real`
  - `+ hdfs dfs -du -s -h /disaster_data/silver_real`
  - `+ hdfs dfs -count -v /disaster_data/silver_real`
- **What it Proves:** Confirms that the curated 130,301-record disaster corpus is stored as snappy-compressed Parquet partition files under `/disaster_data/silver_real/` in HDFS, showing file counts and disk usage.

---

### Step 4: Spark SQL Query on Hive Metastore Catalog
```bash
./scripts/demo/04_hive_sql.sh
```
- **Underlying Commands:**
  - `+ spark-sql -e "SELECT disaster_type, count(*) as count FROM disaster_db.crisis_silver_real GROUP BY disaster_type;"`
  - `+ spark-sql -e "SELECT post_id, event_type, disaster_type, urgency_level FROM disaster_db.crisis_silver_real LIMIT 3;"`
  - `+ spark-sql -e "DESCRIBE EXTENDED disaster_db.crisis_silver_real;"`
- **What it Proves:** Connects to the Hive Metastore catalog `disaster_db.crisis_silver_real` via Spark SQL, running real-time aggregations by disaster category (flood, earthquake, hurricane, wildfire) and inspecting partition metadata.

---

### Step 5: Scored Streaming Message Inspection
```bash
./scripts/demo/05_scored_messages.sh
```
- **Underlying Command:** `+ kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic disaster_scored_posts --max-messages 5`
- **What it Proves:** Consumes 5 real-time scored emergency records from Kafka and displays a structured tabular inspection: timestamp, disaster event, continuous synthetic probability (`p_synthetic`, 4 decimals), binary verdict, urgency level, and per-stage latency trace (`Publication -> Spark Scored -> API Dispatch`).

---

### Step 6: Real-Time Parquet Sink Growth in HDFS
```bash
./scripts/demo/06_scored_in_hdfs.sh
```
- **Underlying Commands:**
  - `+ hdfs dfs -ls -h /disaster_data/streaming_sink`
  - `+ Counting rows in sink at t0 and t15 to verify stream materialization...`
- **What it Proves:** Samples the streaming Parquet sink at $t=0\text{s}$ and $t=15\text{s}$, demonstrating live micro-batch partition writes and proving zero data loss under Write-Ahead Log checkpointing.

---

### Step 7: Machine Learning Model Metrics & Sanity Controls
```bash
./scripts/demo/07_model_results.sh
```
- **Underlying Command:** `+ cat results/summary.json results/sanity_controls.json`
- **What it Proves:** Displays vetted model evaluation figures directly from versioned results files:
  - 130,301-record corpus partitioned via 75/25 random stratified split.
  - Synthetic Detector ROC-AUC: 0.9940 (evaluated against our own template hoaxes).
  - 20-Shuffle Label Permutation Control: Mean AUC $0.4997 \pm 0.0052$ ($p = 0.8220$), statistically indistinguishable from 0.50 random chance, confirming zero feature leakage.
  - HumAID 60-event unseen cross-validation transfer: Macro-F1 $0.5587 \pm 0.0631$.
  - Calibrated operating threshold: $\theta^* = 0.1786$, tuned on validation to limit civilian False Negatives.

---

### Step 8: MILP Emergency Resource Allocation Optimization
```bash
./scripts/demo/08_allocation.sh
```
- **Underlying Command:** `+ python -c 'run solver on Houston 20 units seed 42'`
- **What it Proves:** Runs the real PuLP CBC solver live for Houston (20 rescue boats, seed 42) under two policies:
  1. **Blind LP:** Allocates to reported urgency, dispatching to 8 low-trust hoaxes.
  2. **Trust-Aware ($\theta^* = 0.1786$):** Discounts unverified claims, redirecting all 20 units to genuine distress calls (+8 real civilian gain).
  - Prints the multi-theatre 30-seed statistical bounds from `results/ablation_study.json` ($p < 0.001$).

---

### Step 9: Stream Crash Recovery & Fault Tolerance Evidence
```bash
./scripts/demo/09_recovery.sh
```
- **Underlying Command:** `+ cat results/stream_recovery_evidence.txt`
- **What it Proves:** Reviews the 100,000-event violent crash drill: driver killed via `kill -9`, restarted from HDFS write-ahead checkpoint, recovered in 2.83 seconds with 0 events lost and 0 duplicates (effectively-once storage guarantee).

---

### Step 10: Horizontal Scaling Benchmark (Pandas vs Spark)
```bash
./scripts/demo/10_scaling.sh
```
- **Underlying Command:** `+ cat results/scaling_benchmark.json`
- **What it Proves:** Benchmarks forced disk materialization under a 1.5 GB memory cap:
  - At 1M rows: Pandas is faster (4.98s vs 16.80s) due to JVM startup overhead.
  - At 10M rows: Pandas crashes with Out-of-Memory (OOM, 2.86 GB needed), whereas Spark completes in 12.80s by spilling partitions out-of-core.

---

### Step 11: End-to-End Test Suite Verification
```bash
./scripts/demo/11_tests.sh
```
- **Underlying Command:** `+ make test`
- **What it Proves:** Runs all 24 backend pytest verification tests and 4 frontend client logic test suites, confirming 100% test passage.

---

## 3. Supporting Dashboard Walkthrough (`http://127.0.0.1:8000/`)

Once the terminal commands have demonstrated pipeline mechanics, open the supporting web dashboard at `http://127.0.0.1:8000/`:

1. **Top Bar & Real-Time Status Drawer:**
   - Single unified endpoint served from FastAPI at port 8000.
   - Status badge shows **Live** (active Kafka messages within 10s), **Replay** ("Replay of recorded posts"), or **Offline** (with next step: `The API is not running. Start it with make api`).
   - Clicking the status pill opens the service drawer showing plain lines ("Kafka — running", "Spark — running", "last message 3s ago"; no confusing internal port numbers).

2. **View 1: Live Credibility Feed & Cluster Map:**
   - **Regional Filter:** Houston (Hurricane Harvey flood events), California (Wildfires), Nepal (Earthquake), Miami (Hurricane Irma).
   - **Strict Disaster Provenance:** Feed displays only disaster-relevant posts (HumAID, CrisisLex on-topic, PHEME, and template hoaxes). All off-topic, MAGE, HC3, and LIAR rows are dropped.
   - **Clustered Map:** OpenStreetMap view shows depots (blue squares) and posts with proximity clustering. Normal reports appear in neutral slate (`#64748b`); clusters containing flagged reports highlight in warning red (`#dc2626`).
   - **Row Detail Side Panel:** Click any row to view full text and plain English explanations for why the report was flagged (no raw feature names or internal floating-point values).

3. **View 2: Dispatch Resource Allocation:**
   - Set rescue unit capacity slider (e.g., 20 units).
   - Click **"Plan dispatch"**. State transitions cleanly: `idle` $\to$ `running` $\to$ `result`.
   - Toggle **"Compare without reliability check"** to compare trust-aware optimization against blind baseline. Direct depot-to-call routes draw on the map.

4. **View 3: About & Transparency:**
   - 2×2 clean tool grid with constrained 20px icons (Apache Kafka, Apache Spark, Apache Hadoop, PuLP Optimizer).
   - Plain English descriptions ("recorded disaster posts plus simulated hoaxes", "data stored as Parquet files", "scoring with Spark MLlib models").
   - Direct link to `REPORT.pdf`.

---

## 4. Teardown
```bash
# Stop streaming replay and background services cleanly
make demo-stop
make down
```
