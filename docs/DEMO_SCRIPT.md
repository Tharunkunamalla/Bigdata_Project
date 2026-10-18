# CrisisGuard AI — Live Master Demonstration Scripts (`DEMO_SCRIPT.md`)

**Course:** CSE412 — Big Data Analytics  
**Institution:** Arizona State University  
**System Address:** Single unified endpoint at **`http://127.0.0.1:8000/`**  
**Release Tag:** `v1.0-final-9`  

---

## 1. 3-Minute Lightning Demo (Rapid Evaluation)

*Ideal for swift TA evaluation, quick check-ins, or executive grading:*

### Minute 1: Problem, Architecture & Real Daemons (0:00 – 1:00)
- **Speaker Cue:**
  > "During natural disasters, Emergency Operations Centers monitor social feeds to triage rescues. However, automated bots can flood communication channels with urgent, fabricated distress calls. When dispatchers use trust-blind algorithms, finite rescue assets are sent to phantom locations, starving real victims.
  > CrisisGuard connects five core big data systems: Kafka for ingestion, Spark Streaming for sub-second scoring, HDFS for Parquet storage, Hive Metastore (queried through Spark SQL) for catalog governance, and PuLP MILP for optimal fleet allocation."
- **Terminal Command:**
  ```bash
  make demo-ready
  ```
- **Action:** Open `http://127.0.0.1:8000/` in browser. Point out the product name "CrisisGuard", tabs (Live, Dispatch, About), and the status indicator.

### Minute 2: Live Feed, Detail Panel & Dispatch Planning (1:00 – 2:00)
- **Action on View 1 (Live):**
  - Show the OpenStreetMap map with regional depots (blue squares) and incoming posts (neutral slate for standard civilian calls, warning amber-red for flagged reports).
  - Point out the 3 buffer counts (Posts received, Flagged, Urgent).
  - Click a flagged row in the feed: highlight the side panel showing full text, event tag, arrival time, and "Why it was flagged" in plain language (no raw probabilities or feature names).
- **Action on View 2 (Dispatch):**
  - Select rescue units and click **"Plan dispatch"**. Point out direct depot-to-call routes on the map and the summary list: "Units sent: N, calls covered: N, flagged calls skipped: N".
  - Toggle **"Ignore report reliability"** to show how trust weighting protects units from phantom calls, beside the transparent note: "This is a simulation on replayed posts."

### Minute 3: Architecture Transparency, Status Drawer & Benchmarks (2:00 – 3:00)
- **Action on View 3 (About) & Status Drawer:**
  - Click the status indicator in the top bar to reveal the **Status Drawer**: show service health for Kafka, Spark streaming, HDFS, Hive metastore, and API ("Running / Not running" with last message time).
  - Review the 5 plain English sentences in the About view, and point to the link to the full project report PDF.
- **Closing Cue:**
  > "Every number shown is sourced from our 130,301-record deduplicated corpus and `results/summary.json`. Zero numbers are invented."

---

## 2. 10-Minute Comprehensive Academic Demo (Team of 3 Students)

### Timeline & Presenter Breakdown
- **[00:00 – 02:30] Presenter 1 (Infrastructure & Streaming Lead):** Motivation, Distributed Architecture, Kafka & HDFS
- **[02:30 – 05:30] Presenter 2 (Machine Learning & Graph Lead):** Corpus Provenance, Spark MLlib Suite, Calibration, MinHashLSH
- **[05:30 – 08:00] Presenter 3 (Optimization & Operations Dashboard Lead):** Restrained 4-View Operations Console, Live Map, Solver Execution, 30-Seed Ablation
- **[08:00 – 10:00] Team Synthesis:** Scaling Benchmark (Pandas vs Spark), Honest Limitations, Q&A Preparation

### Detailed Cue Sheet

#### [00:00 – 02:30] Presenter 1: Motivation & Big Data Pipeline Architecture
- **Speaker Cue:**
  > "Welcome to the demonstration of CrisisGuard AI. When Hurricane Harvey struck Houston, 911 dispatch centers were overwhelmed, and first responders relied heavily on Twitter to triage boat evacuations. Generative AI fundamentally compromises this channel. Malicious actors or panicked citizens can flood channels with high-urgency synthetic distress calls ($U_i \to 5.0$). 
  > Standard dispatch linear programs allocate finite vehicles based purely on reported urgency and distance. Under a synthetic flood, assets are diverted to fictitious coordinates while genuine victims perish.
  > To solve this, we architected an end-to-end distributed pipeline combining Apache Kafka 3.7.0 in KRaft mode (port 9092), Apache Spark 3.5.1 Structured Streaming, Apache Hadoop HDFS 3.3.6 (port 9000), Apache Hive Metastore 3.1.3 (port 9083, queried via Spark SQL), and PuLP CBC MILP optimization."
- **Terminal Action:**
  ```bash
  make api
  ```
- Point out the active daemons and the single unified address: `http://127.0.0.1:8000/`.

#### [02:30 – 05:30] Presenter 2: Data Provenance, MLlib Models & Sanity Controls
- **Speaker Cue:**
  > "We grounded our work in authentic disaster data rather than artificial toy sets. We assembled a 130,301-record corpus from QCRI HumAID, CrisisLex, PHEME, MAGE, HC3, and LIAR, partitioned via a strict 75/25 random stratified split (97,422 train / 32,879 test) with 0.00% text leakage.
  > For adversarial hoaxes, we synthesized template perturbation generators: Generators A and B in training, and Generator C strictly held out in testing.
  > On short crisis tweets, uncalibrated models fail at the default $\theta = 0.50$ decision boundary due to score compression. By tuning a calibrated validation threshold ($\theta^* = 0.1786$), we achieve 0.9200 precision and an ROC-AUC of 0.9940 on held-out Gen C.
  > To prove the model does not rely on text length artifacts, we benchmarked length-only (AUC 0.6685) and stylometric-only (AUC 0.7820) baselines.
  > Crucially, we ran a 20-shuffle permutation control: when labels were randomized, ROC-AUC collapsed to 0.4997 ($p = 0.8220$), proving no data leakage.
  > Finally, in our MinHashLSH coordination graph of 15,000 posts, we discovered 82 dense clusters and PageRank amplifiers, while honestly documenting that ~30% of clustering reflects template phrasing syntax."
- **Terminal Action:**
  - Show the terminal pipeline demo: `make demo`.
  - Highlight the model evaluation results, permutation control (p = 0.8220), and MinHash coordination graph findings.

#### [05:30 – 08:00] Presenter 3: Emergency Coordinator Dashboard (Live, Dispatch, About)
- **Speaker Cue:**
  > "Let us examine the emergency coordinator dashboard, designed as a practical operations tool rather than a data science report.
  > On the **Live** view, social alerts stream via Server-Sent Events with measured median latency of 96.85 ms (p95: 130.01 ms). The map shows OpenStreetMap depot coordinates and incoming reports colored by reliability (neutral slate for standard civilian calls, warning amber-red for flagged reports). Clicking any row opens a side panel explaining why it was flagged in plain language. Three live buffer counts track received, flagged, and urgent reports.
  > On the **Dispatch** view, the coordinator selects the region and number of rescue units and clicks 'Plan dispatch'. Direct transit routes appear on the map alongside units sent, calls covered, and flagged calls skipped.
  > Clicking 'Ignore report reliability' illustrates the cost of dispatching blindly: without trust weighting, units are diverted to synthetic hoaxes.
  > Next to this single simulated window, our 30-seed empirical study proves a statistically significant gain (+0.63 victims, $p = 0.00053$ in Houston; +0.40 victims, $p = 0.00807$ in California).
  > On the **About** view and top-bar status drawer, coordinators can inspect truthful service status (Kafka, Spark, HDFS, Hive) and access the full project report."
- **Browser Action:**
  - Open `http://127.0.0.1:8000/`.
  - Demonstrate Live feed and row detail panel, then Dispatch planning and the 'Ignore report reliability' toggle.
  - Open the Status Drawer from the top-bar indicator.

#### [08:00 – 10:00] Team Synthesis: Scaling, Fault Tolerance & Limits
- **Speaker Cue:**
  > "To verify production resiliency, we subjected Spark streaming to a 100,000-event crash-recovery kill drill. Resuming from HDFS write-ahead checkpoints, the pipeline recovered in 2.83 seconds with 0 data loss and 0 duplicates.
  > For scaling, we benchmarked forced Parquet disk materialization across 1M, 5M, and 10M rows under a 1.5 GB memory cap. Pandas is faster at 1M rows (4.98s vs 16.80s) due to zero JVM startup overhead. However, at 10M rows, Pandas crashes with Out-of-Memory, while Spark finishes in 12.80s by spilling partitions out-of-core.
  > Spark is justified by out-of-core resilience, durable Kafka streaming, and unified Hive catalog querying.
  > We now welcome your questions."
- **Terminal Action:**
  ```bash
  make verify
  ```

---

## 3. Terminal-Only Demonstration Fallback (`make demo-terminal`)

*Use this fallback if browser rendering or presentation projectors are unavailable:*

```bash
make demo-terminal
```

### What `make demo-terminal` Executes:
1. **Kafka Broker Check:** Probes TCP port 9092, displays partition count and throughput (2,420 events/sec).
2. **Spark Streaming Status:** Verifies driver application, 1.0s trigger batch duration, and latency p50/p95 (96.85 ms / 130.01 ms).
3. **HDFS Parquet Partition Listing:** Lists silver Parquet files and write-ahead log checkpoints at `/user/hive/warehouse/cse412_disaster.db/disaster_posts_silver`.
4. **Spark SQL Hive Metastore Query:** Queries catalog schema and reports 130,301 deduplicated disaster records.
5. **Live PuLP Solver Execution:** Runs the real CBC MILP solver comparing Blind LP vs Trust-Aware LP, displaying exact solver runtimes (ms), genuine victims served, and wasted dispatches avoided.
