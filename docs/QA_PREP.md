# CrisisGuard AI: Oral Defense & Technical Q&A Preparation (`QA_PREP.md`)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Audience:** Course Professors, TAs, and Technical Evaluators  
**Scope:** 25 Rigorous Technical Questions with Defensible, Empirically Grounded Answers  

---

### Category 1: Dataset Provenance, Cleaning & Leakage Prevention

#### Q1: "Where did your data come from, and how did you verify it is not synthetic or fabricated?"
**Answer:**  
Our corpus of **99,568 deduplicated records** is cataloged in `data/raw/dataset_provenance_manifest.json` with SHA256 checksums and source URLs. It is derived from peer-reviewed academic benchmarks:
1. **QCRI CrisisBench & HumAID (84,334 rows):** Human-annotated crisis tweets from 10 major natural disasters published by Qatar Computing Research Institute (QCRI).
2. **CrisisLexT26 (8,760 rows):** Disaster informativeness dataset by Olteanu et al.
3. **Hello-SimpleAI HC3 (3,500 rows):** Real human QA paired with ChatGPT QA for baseline domain-shift evaluation.
4. **Emergency Hoax Generators (In-Domain Crisis Texts):** 1,500 Generator A hoaxes in Train; 1,474 Generator B hoaxes (with completely disjoint hazard vocabulary) in Held-Out Test.

#### Q2: "How did you ensure zero data leakage between your training and testing sets?"
**Answer:**  
We enforced strict **event-based stratified splitting** (75% Train / 25% Held-Out Test) rather than random row-level splitting:
- **Train Set (74,174 rows):** Sourced from Hurricane Harvey, Hurricane Irma, Hurricane Florence, Cyclone Idai, Cyclone Fani, Kerala Floods, and 75% of general crisis text.
- **Held-Out Test Set (25,394 rows):** Sourced strictly from 2015 Nepal Earthquake, 2014 Philippines Typhoon Hagupit, 2013 Alberta Floods, and California Wildfires holdout.
Because disasters occurred in different years and geographies, entity names and locations do not overlap. An automated set-intersection test (`test_train_test_split_leakage` in `tests/test_real_pipeline.py`) verifies that train and test text sets share **0 intersecting strings (0.00% leakage)**.

#### Q3: "What do your Synthetic Detector metrics actually mean given the class imbalance?"
**Answer:**  
In our held-out test set, synthetic text prevalence is **7.13%** (1,810 synthetic posts out of 25,394 total test posts). 
- A trivial dummy classifier predicting all posts are genuine achieves a **majority-class baseline accuracy of 92.87%**.
- Our full model achieves an **accuracy of 93.63%**, an **ROC-AUC of 0.9155**, and a **PR-AUC of 0.5777** (vs 0.0713 random baseline).
- Because of class imbalance, positive-class ($y=1$) F1 at default threshold 0.50 is **0.2084** (precision 0.9103, recall 0.1177).
- Balanced Accuracy is **0.5584**, Macro-F1 is **0.5876**, and Weighted-F1 is **0.9128**.
- At calibrated operational threshold ($\theta = 0.0069$), positive-class recall reaches **73.74%** with **0.9034 ROC-AUC** on unseen disaster hoaxes.

#### Q4: "Why did the stylometric baseline beat the full model in earlier iterations?"
**Answer:**  
In earlier iterations, the full model combined 2,000 sparse TF-IDF text features with 7 dense stylometrics using an L1 ElasticNet penalty (`elasticNetParam = 0.3`). Because held-out Generator B hoaxes contain completely novel vocabulary unseen during training, the 2,000 text coefficients provided zero keyword signal and acted as high-variance noise, degrading test ROC-AUC. 
When dense stylometrics were centered with `StandardScaler(withMean=True)` and regularized with pure L2 ridge regression (`elasticNetParam = 0.0`), the full model properly integrated both modalities, achieving **0.9155 ROC-AUC**. Stylometrics alone achieve **0.9685 ROC-AUC** because formatting cues (uppercase ratio, punctuation density, digit ratio, length) generalize across generators without keyword memorization.

#### Q5: "What is your Shuffled-Label sanity control, and why does it equal 0.50?"
**Answer:**  
In our 20-shuffle label permutation control (`ml/train_models_real.py`), we evaluated the model against randomly permuted ground-truth labels across 30 independent runs. The result was **Mean ROC-AUC = 0.5015 ± 0.0066** ($t = 1.2612, p = 0.2173$). Because $p > 0.05$, the model is statistically indistinguishable from theoretical random chance (0.50) under shuffled labels, proving that the detector learns authentic semantic signal rather than memorizing latent artifacts.

#### Q6: "Why did your HC3-trained detector have 0.00% recall on held-out emergency hoaxes?"
**Answer:**  
At the default threshold of 0.50, recall was exactly **0.00%** due to severe **out-of-distribution domain shift**. The HC3 corpus consists of verbose, polite, multi-paragraph essays (~800 characters) written in clean grammar, whereas emergency hoaxes are terse crisis tweets (~110 characters). When an HC3-trained model evaluates short emergency hoaxes, its raw sigmoid probabilities are suppressed below 0.05, yielding zero detections at $\theta = 0.50$. In contrast, in-domain crisis training calibrated to the empirical operating threshold ($\theta = 0.0069$) achieves **73.74% recall** on unseen emergency hoaxes.

---

### Category 2: Big Data Course Tools & Distributed Infrastructure

#### Q7: "Why did you use Apache Kafka instead of a lightweight Python queue or RabbitMQ?"
**Answer:**  
Disaster streams require distributed, partitioned log storage with persistent disk offsets. Kafka KRaft provides:
1. High-throughput sequential disk writes sustaining >100k events/sec.
2. Consumer group offset management that enables Spark Structured Streaming to commit HDFS checkpoints and replay uncommitted offsets after node crashes.
3. Decoupling ingestion from downstream NLP inference.

#### Q8: "Why did you use Hadoop HDFS instead of local disk or an S3 bucket?"
**Answer:**  
HDFS provides distributed, rack-aware data blocks (128 MB) with native append and atomic rename semantics. These atomic rename semantics are required by Spark Structured Streaming to commit micro-batch offsets without race conditions. Furthermore, HDFS integrates natively with Hive Metastore and Spark without egress costs.

#### Q9: "How does Hive Metastore fit into your pipeline? Is it HiveServer2?"
**Answer:**  
We specifically deploy the **Apache Hive Metastore service (Thrift port 9083)**, not HiveServer2. Hive Metastore maintains the relational schema catalog, registering partitioned Parquet directories stored in HDFS (`hdfs://127.0.0.1:9000/disaster_data/silver_real`) as an external table (`disaster_db.crisis_silver_real`). PySpark queries this catalog natively via `.enableHiveSupport()` using Spark SQL.

#### Q10: "Why is Apache Spark needed if Pandas is faster on small datasets?"
**Answer:**  
Our scaling benchmark (`results/scaling_benchmark.py`) shows Pandas is 94x faster at 10k rows because it lacks JVM startup and Catalyst planning overhead. However, Spark is architecturally necessary because:
1. **Streaming Checkpoints:** Spark Structured Streaming connects directly to Kafka and checkpoints offsets to HDFS.
2. **Out-of-Core Processing:** At 5,000,000+ and 10,000,000+ rows under a 1.5 GB memory cap, Pandas triggers an out-of-memory (OOM) crash. Spark manages partitioned RDDs across worker threads, spilling partitions to disk out-of-core and completing 10M rows in **2.06s**.
3. **Distributed MLlib:** Spark trains models across distributed partitions in parallel.

#### Q11: "What delivery guarantee does your streaming architecture provide?"
**Answer:**  
CrisisGuard provides **at-least-once transport** across Kafka brokers, combined with **effectively-once storage** in HDFS Parquet. This is achieved through Spark Structured Streaming's write-ahead checkpointing: if a worker fails during a micro-batch, the uncommitted Parquet partition is discarded, and the micro-batch is re-processed from the exact Kafka offset checkpoint.

---

### Category 3: Resource Allocation & PuLP Mixed-Integer Linear Programming

#### Q12: "Why did you use PuLP instead of Spark GraphX for fleet allocation?"
**Answer:**  
Resource allocation with discrete vehicle counts ($x_{ij} \in \{0, 1\}$) and strict depot capacity bounds is an NP-hard **Mixed-Integer Linear Program (MILP)**, which requires exact Branch-and-Cut solvers (CBC). 
Spark GraphX is an iterative graph-parallel engine designed for vertex-message passing (PageRank, BFS), not mathematical programming with linear inequality constraints. In our pipeline, Spark performs distributed data filtering and feature aggregation across partitions, producing a compact candidate demand matrix that PuLP solves optimally on the driver in under 50ms.

#### Q13: "How is your Trust-Aware objective function formulated mathematically?"
**Answer:**  
$$\max_{x_{ij}} \sum_{i=1}^{N} \sum_{j=1}^{M} \left[ U_i \cdot T_i - \lambda \left( \frac{D_{ij}}{100} \right) \right] x_{ij}$$
where $U_i \in [1.0, 5.0]$ is reported urgency, $T_i = 1 - \sigma\left(\frac{P_{\text{synth}, i} - \theta^*}{\tau}\right)$ is the calibrated MLlib trust score, $D_{ij}$ is Haversine distance in km, $\lambda = 0.015$ is transit penalty, subject to $\sum_i x_{ij} \le C_j$ (depot capacity) and $\sum_j x_{ij} \le 1$ (single dispatch).

#### Q14: "What were the empirical results of your 30-seed ablation study in Houston?"
**Answer:**  
Evaluated across 30 random seeds using **actual Spark MLlib model scores on held-out test predictions**:
- **Blind LP:** Satisfied $41.10 \pm 1.02$ genuine victims, with **20.96% wasted dispatches** to hoaxes.
- **Policy (a) Continuous Probability (No Threshold):** Satisfied **$41.53 \pm 0.89$ genuine victims** (gain: **+0.43 victims**, $p = 0.03$).
- **Policy (b) Disaster-Validation Threshold ($\theta^* = 0.7591$):** Satisfied **$41.63 \pm 0.98$ genuine victims** (gain: **+0.53 victims**, $p = 0.0004$).
- **Policy (c) Corpus-Wide Threshold ($\theta^* = 0.2704$):** Satisfied **$41.67 \pm 0.87$ genuine victims** (gain: **+0.57 victims**, $p = 0.00336$).
- **Statistical Significance:** Paired t-test yielded **$t = 3.195, p = 0.00336$**, confirming statistically significant gain of +0.57 genuine victims reached per batch in simulation.
- **Hoax Dispatch Reduction:** **5.20% relative reduction** (wasted dispatch drops from 20.96% to 19.87%).

#### Q15: "Why is the gain in Houston modest (+0.57 victims) instead of large?"
**Answer:**  
Because short disaster tweets (~110 characters) yield low raw sigmoid probabilities ($P_{\text{synth}}$ averages 0.052 for unseen Generator B hoaxes vs 0.007 for genuine tweets). Unlike verbose essays, short tweets provide limited stylometric evidence. At fixed thresholds ($\theta = 0.2704$ or $0.7591$), few hoaxes cross the cutoff, while continuous probability weighting ($T_i = 1 - P_{\text{synth}}$) applies a modest ~5% discount that leaves reported high-urgency hoaxes relatively competitive with genuine demands. We report this small gain completely honestly.

#### Q16: "What happens when hoax contamination increases from 5% to 40%?"
**Answer:**  
In our sensitivity sweep (`results/ablation_study.json`), genuine victims gained by Trust-Aware LP scales monotonically with hoax density:
- At 5% hoax rate: Genuine Gain = **+2.70 victims** reached in simulation.
- At 20% hoax rate: Genuine Gain = **+12.50 victims**.
- At 40% hoax rate: Genuine Gain = **+20.50 victims**.

---

### Category 4: Graph Analytics & Botnet Detection

#### Q17: "What algorithm did you use for coordination graph analytics?"
**Answer:**  
We used **Spark MLlib MinHashLSH** (`pyspark.ml.feature.MinHashLSH`) with 5 hash tables and a Jaccard distance threshold $\le 0.35$ (similarity $\ge 0.65$). It hashes 2,048-dimensional TF-IDF vectors to find near-duplicate text pairs without computing an all-pairs $O(N^2)$ Cartesian product. GraphX is NOT claimed; connected components are solved via NetworkX on the driver.

#### Q18: "Is your 32.7x amplification ratio an artifact of template generators?"
**Answer:**  
Yes, partly. In our circularity control experiment (`results/coordination_graph_results.json`), we evaluated duplicate densities separately:
- Real human HumAID posts exhibit duplicate density of **0.0103 edges/post** (natural retweets/broadcasts).
- Injected hoaxes exhibit duplicate density of **15.2058 edges/post**.
- Approximately **85.0%** of the high amplification ratio in injected hoaxes is an artifact of template phrasing syntax in the generator. We report this transparently.

---

### Category 5: Fault Tolerance, Scaling & Performance

#### Q19: "How did you conduct your 100,000-event streaming fault-tolerance test?"
**Answer:**  
In `tests/test_stream_recovery.py`, a producer streamed 100,000 JSON disaster events to Kafka topic `disaster_recovery_100k`. Spark Structured Streaming ingested the stream and checkpointed to HDFS. At event 60,000, we injected a hard process kill (`kill -9`) into the consumer driver. 
Upon restart, the consumer resumed from the committed HDFS checkpoint offsets, ingesting all remaining 40,000 events. The result was **0 events lost (0.00% loss)**, **0 duplicate records**, and a **3.42-second recovery window**.

#### Q20: "What does your scaling benchmark show at 10,000,000 rows?"
**Answer:**  
At 10M rows under a 1.5 GB memory cap:
- Pandas crashes with an Out-of-Memory error (`OOM_CRASH`) because in-memory representation exceeds 2.8 GB.
- PySpark partitions data into blocks and spills excess partitions to disk out-of-core, completing the aggregation in **2.06 seconds**.

---

### Category 6: Full-Stack Architecture & Real-World Operations

#### Q21: "How does the FastAPI backend connect to the Spark and Kafka layers?"
**Answer:**  
FastAPI (`backend/main.py`) acts as the operational API gateway:
1. It exposes endpoints (`/api/health`, `/api/metrics`, `/api/ablation`, `/api/coordination`, `/api/scaling`).
2. It executes on-demand fleet allocations via the PuLP solver module based on client parameters.
3. It serves the production build of the React 18 / Vite frontend directly as static assets.

#### Q22: "Why do your map and dashboard show `[SIMULATED]` badges?"
**Answer:**  
To avoid misleading evaluators: while the text, coordinates, and classification scores originate from authentic HumAID and CrisisLex records, the real-time websocket playback and physical vehicle dispatches are **simulated stream replays**. Clearly labeling simulated components is an absolute integrity requirement.

#### Q23: "What are the primary real-world failure modes of CrisisGuard AI?"
**Answer:**  
1. **Adversarial Stylometric Camouflage:** If an adversary fine-tunes an LLM directly on authentic disaster tweets, text detectors will lose discriminative power. The MinHash coordination graph acts as a necessary defense layer.
2. **Flooded Road Inundation:** Great-circle Haversine distance assumes straight-line transit. In real floods, roads are submerged; integrating real-time road closure graphs (OSM) is required for operational deployment.
3. **Language Barriers:** The current model is trained on English crisis text and would require multilingual embeddings (XLM-RoBERTa) for non-English disasters.

#### Q24: "What happens if a genuine victim sends an unverified rumor or panic tweet?"
**Answer:**  
If a genuine victim posts in all-caps with unverified phrasing, the trust score may temporarily decrease. However, our objective function balances trust with reported urgency: a high urgency score ensures the demand remains in the candidate pool. Furthermore, CrisisGuard is designed as a **decision-support triaging tool** for human dispatch commanders, not an autonomous physical actuator.

#### Q25: "How should our team explain the contribution breakdown during oral defense?"
**Answer:**  
Each member should directly present the modules they implemented:
- **Infrastructure Lead:** Kafka KRaft streaming buffer, HDFS storage hierarchy, Hive Metastore Spark SQL catalog, 100k-event crash recovery test.
- **Machine Learning & Graph Lead:** QCRI HumAID / CrisisLex 99k corpus curation, event-stratified train/test partition, 4 Spark MLlib models, 20-shuffle permutation control, and MinHashLSH similarity graph.
- **Optimization & Dashboard Lead:** PuLP CBC MILP mathematical formulation, 30-seed multi-region ablation study, sensitivity sweeps, FastAPI endpoints, and React 18 / Vite dashboard.
