# CrisisGuard AI — Live Master Demonstration Scripts (`DEMO_SCRIPT.md`)

**Course:** CSE412 — Big Data Analytics  
**Institution:** Arizona State University  
**System Address:** Single unified endpoint at **`http://127.0.0.1:8000/`**  
**Release Tag:** `v1.0-final-8`  

---

## 1. 3-Minute Lightning Demo (Rapid Evaluation)

*Ideal for swift TA evaluation, quick check-ins, or executive grading:*

### Minute 1: Problem, Architecture & Real Daemons (0:00 – 1:00)
- **Speaker Cue:**
  > "During natural disasters, Emergency Operations Centers monitor social feeds to triage rescues. However, generative AI enables automated botnets to flood communication channels with urgent, fabricated distress calls. When dispatchers use trust-blind algorithms, finite rescue assets are sent to phantom locations, starving real victims.
  > CrisisGuard AI connects five core big data systems: Kafka for ingestion, Spark Streaming for sub-second MLlib scoring, HDFS for Parquet storage, Hive Metastore (queried through Spark SQL) for catalog governance, and PuLP MILP for optimal fleet allocation."
- **Terminal Command:**
  ```bash
  curl -s http://127.0.0.1:8000/api/health
  ```
- **Action:** Open `http://127.0.0.1:8000/` in browser. Point out the `LIVE` and `SIMULATED` badges in the clean header.

### Minute 2: Live Feed, Top Factors & Real PuLP Solver (1:00 – 2:00)
- **Action on Page 1 (Live):**
  - Show the 60% Leaflet map with Houston marine depots and incoming posts colored by trust (green = credible, red = suspected synthetic).
  - Click a flagged post in the table: highlight the side drawer showing full text, synthetic probability (e.g. 0.850), and top predictive factors.
- **Action on Page 3 (Allocation):**
  - Click **"Run allocation"**. Point out solver runtime (~24 ms via PuLP CBC), genuine victims rescued (29), and wasted dispatches (0).
  - Highlight the 30-seed offline comparison chart: Trust-Aware validation thresholding achieves a statistically significant gain (+0.63 victims, $p = 0.00053$) over Blind LP.

### Minute 3: Provenance, Fault Tolerance & Evidence Export (2:00 – 3:00)
- **Action on Page 4 (Pipeline) & Page 5 (Demo):**
  - Show cluster daemons table and the crash-recovery table: 2.83s recovery with 0 data loss.
  - Navigate to **Demo** tab: click **"Run step"** on Step 4 (Spark SQL Hive catalog query: 130,301 rows).
  - Click **"Export evidence"** to demonstrate reproducible verification written to `results/demo_evidence/`.
- **Closing Cue:**
  > "Every number shown is sourced from our 130,301-record deduplicated corpus and `results/summary.json`. Zero numbers are invented."

---

## 2. 10-Minute Comprehensive Academic Demo (Team of 3 Students)

### Timeline & Presenter Breakdown
- **[00:00 – 02:30] Presenter 1 (Infrastructure & Streaming Lead):** Motivation, Distributed Architecture, Kafka & HDFS
- **[02:30 – 05:30] Presenter 2 (Machine Learning & Graph Lead):** Corpus Provenance, Spark MLlib Suite, Calibration, MinHashLSH
- **[05:30 – 08:00] Presenter 3 (Optimization & Operations Dashboard Lead):** Clean 5-Page Dashboard, Live Map, Solver Execution, 30-Seed Ablation
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
- **Browser Action:**
  - Navigate to **Models** tab at `http://127.0.0.1:8000/?tab=models`.
  - Show the Models Catalog table, the SVG ROC and PR curves, the confusion matrix at $\theta^* = 0.1786$, and the Known Limits box.

#### [05:30 – 08:00] Presenter 3: Live Dashboard & Trust-Aware Allocation
- **Speaker Cue:**
  > "Let us examine the operational frontend, built with academic engineering clarity: system typography, neutral grays, tabular layouts, and zero decorative emojis.
  > On the **Live** page, incoming social alerts stream via Server-Sent Events with a measured median latency of 96.85 ms (p95: 130.01 ms). The left panel renders a 60% Leaflet map showing OpenStreetMap depot coordinates and incident pins colored by trust. Clicking any row opens a side inspection drawer with full text and score attribution factors.
  > On the **Allocation** page, dispatch commanders run the PuLP CBC optimizer on the current demand window.
  > In our 30-seed statistical evaluation of Houston (52 rescue boats), Blind LP achieves 28.00 victims while wasting 20% of dispatches on hoaxes. Our Trust-Aware validation threshold policy achieves 28.63 ± 0.32 victims, a statistically significant gain of +0.63 victims ($p = 0.00053$) representing 2.6% of theoretical Oracle headroom.
  > We repeat this across California Wildfires (+0.40 victims, $p = 0.00807$), Nepal Earthquake (+0.63 victims, $p = 0.00053$), and Miami Tropical Storm (+0.63 victims, $p = 0.00053$)."
- **Browser Action:**
  - Click **"Run allocation"** on the Allocation tab. Highlight the assignments table and summary cards.
  - Review the two-sentence explanation box: 'What this shows / what it does not show'.

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
