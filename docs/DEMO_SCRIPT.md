# CrisisGuard AI — Live Master Demo Script (CSE412)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Project:** Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation  
**Team Structure:** Team of 3 Students (Presenter 1: Infrastructure & Stream; Presenter 2: MLlib & Graph Analytics; Presenter 3: Allocation Engine & Dashboard)  
**Total Target Duration:** 8 to 10 Minutes

---

## 1. Demo Timeline & Presenter Handoff

```
[00:00 - 02:00] Presenter 1: Problem Statement & Big Data Pipeline Architecture
[02:00 - 04:30] Presenter 2: Real Corpus, Spark MLlib Suite, Sanity Controls, MinHash Graph
[04:30 - 07:15] Presenter 3: Live Dashboard Walkthrough, Real-Time Replay & Fleet Solver
[07:15 - 08:45] Presenter 1: 100k Stream Fault-Tolerance & 1M Scaling Benchmark
[08:45 - 10:00] Presenter 2 & 3: Multi-Region Ablation (30 Seeds, CIs), Limitations, Q&A Prep
```

---

## 2. Minute-by-Minute Speaker Cues & Exact Actions

### [00:00 – 02:00] Segment 1: Motivation & Architecture (Presenter 1)
- **Speaker Cue:**
  > "During natural disasters like Hurricane Harvey or the Nepal Earthquake, emergency response centers monitor social media to locate trapped victims. However, the rise of Generative AI has enabled automated adversarial campaigns—creating thousands of synthetic emergency hoaxes that flood emergency dispatchers and divert scarce rescue boats away from genuine victims. 
  > To solve this, we built **CrisisGuard AI**, an end-to-end distributed big data analytics pipeline using Apache Kafka, Apache Spark, Hadoop HDFS, Hive Metastore, and PuLP MILP."
- **Terminal Action (Live Health Check):**
  ```bash
  curl -s http://127.0.0.1:8000/api/health
  ```
- **Expected Output:**
  ```json
  {"status":"HEALTHY","services":{"kafka":{"status":"CONNECTED"},"hdfs":{"status":"OPERATIONAL"},"hive":{"status":"ACTIVE"},"spark":{"status":"READY"}}}
  ```
- **Visual Aid:** Point to the 4 running daemons (Kafka KRaft 9092, HDFS NameNode 9000, Hive Thrift 9083, Spark 3.5.1).

---

### [02:00 – 04:30] Segment 2: Real Data, MLlib & Graph Analytics (Presenter 2)
- **Speaker Cue:**
  > "Rather than using toy synthetic data, our entire pipeline is rebuilt on authentic disaster corpora: 98,944 deduplicated records sourced from QCRI CrisisBench / HumAID, CrisisLex, and Hello-SimpleAI HC3, with live environmental feeds from USGS and NOAA.
  > We implemented an un-leaked Train/Test split: 26,350 training rows (Hurricane Harvey & Kerala Floods) and 72,594 held-out testing rows (Nepal Earthquake & California Wildfires) with 0% text overlap.
  > In Spark MLlib, we trained four models:
  > 1. A Synthetic Misinformation Detector (Logistic Regression + ElasticNet) achieving **0.7081 ROC-AUC** and **96.4% accuracy**.
  > 2. A Rumour Classifier (Random Forest) with **0.6785 ROC-AUC** and **90.4% F1**.
  > 3. A Humanitarian Operational Categorizer with **0.5994 Macro-F1**.
  > 4. An Urgency Regressor with **1.2277 RMSE**.
  > Crucially, we ran rigorous **Sanity Controls**: when labels were shuffled randomly, ROC-AUC dropped to **0.5557**, confirming zero latent label leakage.
  > Furthermore, using Spark MLlib's **MinHashLSH**, we built a 14,888-node coordination graph discovering 9,563 text similarity edges and 281,344 hashtag co-occurrence edges. Synthetic hoaxes exhibited an average coordination degree of **60.82** versus **1.86** for genuine posts—a **32.7x amplification disparity** that exposes botnet campaigns."
- **Terminal Action (Run Automated Master Demo):**
  ```bash
  python scripts/run_demo.py
  ```

---

### [04:30 – 07:15] Segment 3: Live Dashboard Walkthrough (Presenter 3)
- **Speaker Cue:**
  > "Now let us view the live operations dashboard at `http://127.0.0.1:8000/`."
- **Browser Actions:**
  1. Open Chrome/Browser to `http://127.0.0.1:8000/`.
  2. **Tab 1: Live Operations:**
     - Point out the `[SIMULATED REAL-TIME STREAM]` badge and explain that authentic HumAID records are streamed through Kafka topics.
     - Review the incoming post feed: point to an authentic citizen report (`✓ [VERIFIED] Citizen Demand`) with 92% trust score, and a flagged hoax (`⚠ [FLAGGED] Synthetic Hoax`) with high raw urgency but 8% trust score.
     - Show the Leaflet map of Houston: green pins for genuine victims, red pins for detected hoaxes, and blue anchors for the 4 marine depots (Downtown, Katy, Sugar Land, Cypress).
     - Under "Fleet Dispatch Controls", toggle between **Trust-Aware** and **Blind**, select **PuLP CBC MILP**, and click **"Solve Fleet Dispatch Plan"**.
     - Point out the result: The Trust-Aware LP dispatches rescue boats to genuine victims with 0% wasted to hoaxes, whereas the Blind LP diverts ~22% of boats to fictitious coordinates.
  3. **Tab 2: Fleet Dispatcher & Multi-Region Ablation:**
     - Click **Houston (Boats)**, **California (Wildfire)**, **Nepal (SAR)**, and **Miami (Ambulances)**.
     - Highlight the 30-seed empirical comparison table with 95% Confidence Intervals.
     - Emphasize the Paired t-test result: $t = 23.77, p = 1.45 \times 10^{-20}$ ($p < 0.001$), formally proving statistically significant superiority of the trust-aware formulation over standard MIP.
     - Show the Hoax Contamination Sensitivity Sweep (from 5% to 40% hoax rates), proving genuine victim gains scale up to +46.93 lives saved as adversary intensity surges.
  4. **Tab 3: MLlib & Sanity Controls:**
     - Show the 4 model cards with real metrics, the Shuffled-Label control panel, and the MinHash coordination metrics.
  5. **Tab 4: Cluster Topology & Scaling:**
     - Show active port verifications (Kafka 9092, HDFS 9000, Hive 9083).

---

### [07:15 – 08:45] Segment 4: 100k Fault-Tolerance & Scaling Benchmark (Presenter 1)
- **Speaker Cue:**
  > "To evaluate production resilience, we subjected Spark Structured Streaming to a 100,000-event stress test with hard process kill injection. Ingesting 60,000 events before terminating the driver, the engine restarted from its HDFS checkpoint and consumed the remaining 40,000 events with **zero events lost (0.00% loss)**, zero duplicates, and a 7.72-second recovery window.
  > For scalability, we benchmarked single-node Pandas against Apache Spark across 10k, 100k, 500k, and 1,000,000 rows.
  > An honest big-data finding: Pandas is faster on small in-memory datasets ($\le 100\text{k}$ rows) because it has zero JVM startup, Catalyst planning, or RPC serialization overhead.
  > However, at 1,000,000 rows, Pandas memory usage balloons to 90.8 MB single-threaded, whereas Spark processes partitions out-of-core. Spark is justified not by single-node microsecond latency, but by its distributed streaming from Kafka, HDFS partitioned Parquet storage, and cluster-wide MLlib execution."
- **Terminal Action (Run Pytest Suite):**
  ```bash
  pytest -v tests/test_real_pipeline.py
  ```
- **Expected Output:**
  ```
  6 passed in 2.03s (100% green: provenance, schema, leakage, metrics, haversine, pulp optimizer)
  ```

---

### [08:45 – 10:00] Segment 5: Limitations, Ethical Scope & Q&A (All Presenters)
- **Speaker Cue:**
  > "In summary, CrisisGuard AI demonstrates honest, defensible big data engineering:
  > - Real data: 98,944 authentic records with zero synthetic fabrication or leakage.
  > - Real ML: 4 Spark MLlib models validated against shuffled-label sanity controls.
  > - Real optimization: Multi-region PuLP MILP with 30-seed CIs and $p < 10^{-20}$ significance.
  > - Real cluster tools: Kafka, HDFS, Hive Metastore, and Spark running natively.
  > We explicitly scope this system as a decision-support assistant for human dispatch commanders, not an autonomous physical actuator.
  > Thank you, and we welcome your questions!"

---

## 3. Emergency Presentation Fallback Plan

| Potential Failure Mode | Root Cause | Immediate Fallback Action |
| :--- | :--- | :--- |
| **WSL2 / Daemon Crash** | Memory exhaustion or VM sleep | Run `python scripts/run_demo.py` directly; it automatically pulls telemetry from `results/summary.json`. |
| **Port 8000 Unresponsive** | FastAPI background task stopped | Run `.venv/Scripts/python.exe -m uvicorn backend.main:app --port 8000` in a new terminal, or use the precomputed static tables in `docs/REPORT.md`. |
| **Browser Fails to Open** | CDP / local browser conflict | Open `http://localhost:8000/` manually in standard Chrome/Edge, or present the pre-captured screenshots in `docs/screenshots/`. |
| **Grader asks: "Why did you use PuLP instead of Spark GraphX for allocation?"** | Tool justification question | *Honest answer:* "Resource allocation is an NP-hard Mixed-Integer Linear Program (MILP) with exact integer capacity constraints, which is formulated and solved via Branch-and-Cut (CBC solver). Spark GraphX is an iterative graph-parallel algorithm engine (PageRank, Pregel, BFS), not an exact integer optimization solver. Spark is used for distributed aggregation and MinHashLSH graph clustering, while PuLP solves the exact allocation problem on the driver." |
| **Grader asks: "Why is your Synthetic Detector ROC-AUC 0.7081 instead of 0.99?"** | Metric honesty question | *Honest answer:* "Previous synthetic toy generators created trivial Gaussian or keyword cues that caused 1.0000 AUC leakage. Our model is trained on authentic HumAID tweets and Hello-SimpleAI HC3 comparisons with strict zero-leakage splits. 0.7081 is an honest, empirical AUC reflecting real natural language variability and subtle generative AI text." |
