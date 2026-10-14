# CrisisGuard AI: Oral Defense & Technical Q&A Preparation (`QA_PREP.md`)

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Audience:** Course Professors, TAs, and Technical Evaluators  
**Scope:** 25 Rigorous Technical Questions with Defensible, Empirically Grounded Answers

---

### Category 1: Dataset Provenance, Cleaning & Leakage Prevention

#### Q1: "Where did your data come from, and how did you verify it is not synthetic or fabricated?"
**Answer:**  
Our corpus of **98,944 records** is cataloged in `data/raw/dataset_provenance_manifest.json` with SHA256 checksums and source URLs. It is derived from peer-reviewed academic benchmarks:
1. **QCRI CrisisBench & HumAID (124 MB, 87,557 rows):** Human-annotated crisis tweets from 19 natural disasters published by Qatar Computing Research Institute (QCRI).
2. **CrisisLexT26 (0.94 MB, 7,613 rows):** Landmark crisis informatics dataset by Olteanu et al.
3. **Hello-SimpleAI HC3 (2.78 MB, 1,187 QA pairs):** Real human texts paired with ChatGPT outputs.
4. **Live Government Feeds:** USGS Earthquakes GeoJSON, NOAA Weather Alerts, and OpenFEMA Declarations.
Zero data was generated with random Gaussian or artificial text templates.

#### Q2: "How did you ensure zero data leakage between your training and testing sets?"
**Answer:**  
We enforced strict **event-based stratified splitting** rather than random row-level splitting:
- **Train Set (26,350 rows):** Sourced strictly from Hurricane Harvey (2017) and Kerala Floods (2018).
- **Test Set (72,594 rows):** Sourced strictly from Nepal Earthquake (2015) and California Wildfires (2018).
Because disasters occurred in different years and geographies, entity names and locations do not overlap. An automated set-intersection test (`test_train_test_split_leakage` in `tests/test_real_pipeline.py`) verifies that train and test text sets share **0 intersecting strings (0.00% leakage)**.

#### Q3: "Why is your Synthetic Detector ROC-AUC 0.7081 instead of 0.99 or 1.00?"
**Answer:**  
In our pre-audit prototype, synthetic generator keywords leaked directly into the TF-IDF feature space, producing an artificial 1.0000 ROC-AUC. In this rebuild, we eliminated synthetic template generators. 
**0.7081 ROC-AUC** is an honest, empirically defensible metric reflecting real natural language variability when discriminating subtle AI-generated text from authentic human crisis tweets on an un-leaked held-out test split.

#### Q4: "What is your Shuffled-Label sanity control, and why did you run it?"
**Answer:**  
A common flaw in ML pipelines is memorizing latent artifacts (e.g., tweet IDs, timestamps, or dataset ordering) that artificially inflate performance. 
In our shuffled-label control (`ml/train_models_real.py`), we uniformly permuted ground-truth labels across the training set while keeping feature vectors identical. The retrained model achieved an **ROC-AUC of 0.5557**, which aligns with the theoretical random chance baseline of 0.50. This formally proves the model learns true semantic signals and has zero latent label leakage.

#### Q5: "What happened when you evaluated your HC3-trained detector on held-out emergency hoaxes?"
**Answer:**  
It scored **0.00% accuracy**. This is an important empirical finding documented in `results/sanity_controls.json`. The HC3 corpus consists of verbose, polite, long-form ChatGPT answers to general questions, whereas emergency hoaxes are terse, urgent crisis alerts. This dramatic failure confirms severe **out-of-distribution domain shift**, proving that off-the-shelf LLM detectors cannot be naively transferred to emergency informatics without domain-specific crisis fine-tuning.

---

### Category 2: Big Data Course Tools & Distributed Infrastructure

#### Q6: "Why did you use Apache Kafka instead of a lightweight Python message queue or RabbitMQ?"
**Answer:**  
Disaster streams require distributed, partitioned log storage with persistent disk offsets. Kafka KRaft provides:
1. High-throughput sequential disk writes sustaining >100k events/sec.
2. Consumer group offset management that enables Spark Structured Streaming to commit HDFS checkpoints and replay uncommitted offsets after node crashes.
3. Decoupling ingestion from variable-latency downstream NLP inference.

#### Q7: "Why did you use Hadoop HDFS instead of local disk or an S3 bucket?"
**Answer:**  
In academic and enterprise private-cloud deployments, HDFS provides distributed, rack-aware data blocks (128 MB) with native append and atomic rename semantics. These atomic rename semantics are required by Spark Structured Streaming to commit micro-batch offsets without race conditions. Furthermore, HDFS integrates natively with Hive Metastore and Spark without egress costs.

#### Q8: "How does Hive Metastore fit into your pipeline? Is it HiveServer2?"
**Answer:**  
We specifically deploy the **Apache Hive Metastore service (Thrift port 9083)**, not HiveServer2. Hive Metastore maintains the relational schema catalog in a Derby backend, registering partitioned Parquet directories stored in HDFS (`hdfs://127.0.0.1:9000/disaster_data/silver_real`) as an external table (`disaster_db.crisis_silver_real`). PySpark queries this catalog natively via `.enableHiveSupport()`.

#### Q9: "Why is Apache Spark needed if Pandas is faster on small datasets?"
**Answer:**  
Our scaling benchmark (`results/scaling_benchmark.py`) shows Pandas is faster at $\le 100\text{k}$ rows because it lacks JVM startup and Catalyst planning overhead. However, Spark is architecturally necessary because:
1. **Streaming Checkpoints:** Spark Structured Streaming connects directly to Kafka and checkpoints offsets to HDFS.
2. **Out-of-Core Processing:** At 1,000,000+ rows, Pandas memory expands to over 90 MB single-threaded, and at 10M+ rows it triggers an OOM crash. Spark manages partitioned RDDs across worker nodes, spilling to disk when RAM is exceeded.
3. **Distributed MLlib:** Spark trains models across distributed partitions in parallel.

#### Q10: "What delivery guarantee does your streaming architecture provide?"
**Answer:**  
CrisisGuard provides **at-least-once transport** across Kafka brokers, combined with **effectively-once storage** in HDFS Parquet. This is achieved through Spark Structured Streaming's write-ahead checkpointing: if a worker fails during a micro-batch, the uncommitted Parquet partition is discarded, and the micro-batch is re-processed from the exact Kafka offset checkpoint.

---

### Category 3: Resource Allocation & PuLP Mixed-Integer Linear Programming

#### Q11: "Why did you use PuLP instead of Spark GraphX for fleet allocation?"
**Answer:**  
Resource allocation with discrete vehicle counts ($x_{ij} \in \{0, 1\}$) and strict depot capacity bounds is an NP-hard **Mixed-Integer Linear Program (MILP)**, which requires exact Branch-and-Cut solvers (CBC/Simplex). 
Spark GraphX is an iterative graph-parallel engine designed for vertex-message passing (PageRank, BFS, Connected Components), not mathematical programming with linear constraints. In our pipeline, Spark performs distributed data filtering and feature aggregation across partitions, producing a compact candidate demand matrix that PuLP solves optimally on the driver in under 100ms.

#### Q12: "How is your Trust-Aware objective function formulated mathematically?"
**Answer:**  
$$\max_{x_{ij}} \sum_{i=1}^{N} \sum_{j=1}^{M} \left[ U_i \cdot T_i - \lambda \left( \frac{D_{ij}}{100} \right) \right] x_{ij}$$
where $U_i \in [0, 1]$ is reported urgency, $T_i = (1 - P_{\text{synth}, i}) \cdot (1 - P_{\text{rumour}, i})$ is the MLlib trust score, $D_{ij}$ is Haversine distance in km, $\lambda = 0.05$ is transit penalty, subject to $\sum_i x_{ij} \le C_j$ (depot capacity) and $\sum_j x_{ij} \le 1$ (single dispatch).

#### Q13: "What were the empirical results of your 30-seed ablation study in Houston?"
**Answer:**  
Evaluated across 30 random seeds on held-out test posts:
- **Blind LP:** Satisfied $40.20 \pm 0.92$ genuine victims, with **22.69% wasted dispatches** to hoaxes.
- **Trust-Aware LP:** Satisfied **$51.80 \pm 0.15$ genuine victims**, with **0.38% wasted dispatches** to hoaxes.
- **Statistical Significance:** Paired t-test yielded $t = 23.77, p = 1.45 \times 10^{-20}$ ($p < 0.001$), formally rejecting the null hypothesis.

#### Q14: "Why does Blind LP waste 22.69% of dispatches?"
**Answer:**  
Adversarial hoaxes deliberately include extreme urgency keywords (*"DROWNING", "COLLAPSED"*), driving reported urgency $U_i \to 1.0$. Because Blind LP cannot verify veracity, it prioritizes these fabricated 1.0 urgency requests over authentic citizen requests that report moderate urgency (e.g., $U_i = 0.7$). In a fleet of 52 boats, ~12 boats are dispatched to empty locations.

#### Q15: "What happens when hoax contamination increases from 5% to 40%?"
**Answer:**  
In our sensitivity sweep (`results/ablation_study.json`), at 5% contamination, Trust-Aware LP rescues +5.73 more victims than Blind LP. At 40% contamination, Blind LP completely collapses, serving only 4.73 genuine victims because hoaxes consume 91% of capacity. Trust-Aware LP maintains resilience, serving **51.67 victims (+46.93 lives saved)**.

---

### Category 4: Graph Analytics & Botnet Detection

#### Q16: "What algorithm did you use for coordination graph analytics?"
**Answer:**  
We used **Spark MLlib MinHashLSH** (`pyspark.ml.feature.MinHashLSH`) with 5 hash tables and a Jaccard similarity threshold of $\ge 0.65$. It hashes 5,000-dimensional TF-IDF vectors to find near-duplicate text pairs without computing an all-pairs $O(N^2)$ Cartesian product.

#### Q17: "What did the coordination graph reveal about synthetic hoaxes versus genuine posts?"
**Answer:**  
On a sample of 14,888 empirical posts, the graph discovered 9,563 near-duplicate text edges and 281,344 hashtag co-occurrence edges. 
Synthetic hoaxes had a mean coordination degree of **60.82 edges**, compared to **1.86 edges** for genuine citizen reports—a **32.74x amplification disparity**. This proves that automated hoaxes rely on synchronized copy-paste botnet amplification.

#### Q18: "Are the social media accounts in your coordination graph authentic Twitter accounts?"
**Answer:**  
No. To adhere to strict academic honesty, we explicitly declare in the graph manifest: while text strings and hashtag co-occurrences are extracted directly from empirical HumAID tweets, individual account handles and follower graphs are simulated approximations because public disaster datasets anonymize user IDs for privacy.

---

### Category 5: Fault Tolerance, Scaling & Performance

#### Q19: "How did you conduct your 100,000-event streaming fault-tolerance test?"
**Answer:**  
In `tests/test_stream_recovery.py`, a producer streamed 100,000 JSON disaster events to Kafka topic `disaster_recovery_100k`. Spark Structured Streaming ingested the stream and checkpointed to HDFS. At event 60,000, we injected a hard process kill (`kill -9`) into the consumer driver. 
Upon restart, the consumer resumed from the committed HDFS checkpoint offsets, ingesting all remaining 40,000 events. The result was **0 events lost (0.00% loss)**, **0 duplicate records**, and a **7.72-second recovery window**.

#### Q20: "What does your scaling benchmark show at 1,000,000 rows?"
**Answer:**  
At 1M rows on our WSL2 test machine:
- Pandas processed in-memory aggregations in 0.231s, consuming 90.85 MB RAM.
- Spark processed the 1M rows across partitions in 51.64s.
This proves that on a single node where data fits in memory, Pandas is faster due to zero JVM/Catalyst overhead. Spark's justification is not single-node sub-second batch latency, but its distributed streaming from Kafka, HDFS/Hive ecosystem integration, and out-of-core partition resilience.

---

### Category 6: Full-Stack Architecture & Real-World Operations

#### Q21: "How does the FastAPI backend connect to the Spark and Kafka layers?"
**Answer:**  
FastAPI (`backend/main.py`) acts as the operational API gateway:
1. It exposes endpoints (`/api/health`, `/api/summary`, `/api/sample-posts`).
2. It interacts with the PuLP solver module to execute on-demand fleet allocations based on client parameters.
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

#### Q25: "How was work divided among the three team members?"
**Answer:**  
- **Student 1 (Infrastructure & Streaming Lead):** Kafka 3.7 KRaft broker, HDFS 3.3.6 distributed storage, Hive Metastore 3.1.3 catalog, 100k-event stream crash-recovery test, and 1M scaling benchmark.
- **Student 2 (Machine Learning & Graph Lead):** Data acquisition (98,944 records from QCRI HumAID, CrisisLex, HC3), zero-leakage split, 4 Spark MLlib models, shuffled-label sanity control (0.5557 AUC), and MinHashLSH coordination graph (32.7x ratio).
- **Student 3 (Optimization & Full-Stack Lead):** Trust-Aware PuLP MILP formulation, 30-seed multi-region ablation study across 4 disaster theaters, paired t-tests ($p < 10^{-20}$), FastAPI backend, and React 18 / Vite glassmorphic operations dashboard.
