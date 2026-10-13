# CSE412 Big Data Project: Phase A Integrity Audit (Pre-Fix Assessment)

**Project Title:** Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation  
**Audit Date:** October 2026  
**Auditor Mode:** Adversarial Big Data & ML Systems Reviewer  
**Repository State:** Initial Implementation Baseline  

---

## 1. Executive Summary: Pre-Fix Audit Scorecard

Every previously claimed "PASS" was treated as unverified and re-audited using real diagnostic execution (`scripts/audit_phase_a.py`). The audit reveals significant issues regarding data provenance, synthetic leakage, tool completeness, and evaluation circularity.

| Audit Item | Scope | Status | Root Cause & Evidence Summary |
|:---|:---|:---:|:---|
| **A1. Data Provenance** | Dataset origin & authenticity | **FAIL** | Main 60,000-record dataset was 100% synthetically generated from ~20 Python string templates. Only 238 unique texts in train, 198 in test. Zero real HumAID/PHEME data was downloaded. |
| **A2. Metric Sanity** | ROC-AUC 1.0000 & accuracy 95.89% | **FAIL** | Extreme synthetic leakage: `burstiness_score` was generated from disjoint Gaussians ($\mu=0.28$ vs $\mu=0.68$). Stylometrics alone yield AUC = 1.0000. Text templates had 100% deterministic category mapping. |
| **A3. Tool Integration** | End-to-end data pipeline flow | **PARTIAL** | Kafka & HDFS & Hive Metastore run, and Spark Structured Streaming exists; but HiveServer2 is not running (only Metastore via Spark SQL), Graph script used PySpark joins (not GraphFrames/GraphX), and FastAPI `/api/stream` read a JSON file instead of Kafka. |
| **A4. Fault Tolerance** | Stream recovery & data loss | **PARTIAL** | Recovery test only processed 200 records. While offset checkpointing works, claiming zero data loss at scale is unverified without a $\ge 100\text{k}$ event crash test. |
| **A5. Scaling Claim** | Pandas vs Spark benchmark | **FAIL** | Spark was 100x–250x slower than Pandas across 10k–250k rows (Pandas 0.04s vs Spark 4.0s–11.5s). Claiming a Spark "win" at this scale is empirically false. |
| **A6. Ablation Validity** | Resource allocation benefit | **FAIL** | `run_ablation.py` sampled from a simulated Beta distribution ($\text{Beta}(8, 2)$) instead of passing actual ML model inference outputs. Evaluated only Houston flood boats. |
| **A7. Reproducibility** | Environment & filesystem | **PARTIAL** | Repository resides on `OneDrive`, causing file-lock risks and slow 9P filesystem translation between WSL2 and `/mnt/c/`. Needs relocation to native WSL. |
| **A8. Documentation** | Tone, report length, claims | **PARTIAL** | Overclaims ("enterprise", "empirically saves lives", "production-ready") must be replaced with measured simulated statements. Report needs formal PDF export verification and image diagrams. |

---

## 2. Detailed Findings by Audit Domain

### A1. Data Provenance Audit
- **Files Audited:** `data/raw/disaster_posts_raw.csv` (17.79 MB, SHA256: `7a0060033111af44...`), `disaster_posts_raw.json` (44.16 MB, SHA256: `8c40804562c719ca...`).
- **Finding:** In `data/download_datasets.py`, the entire 60,000 corpus was created by randomly sampling from a list of 12 human and 6 synthetic string templates per disaster event.
- **Evidence:** Across 30,000 train rows, there are only **238 unique text strings** (diversity ratio: 0.0079). Across 30,000 test rows, there are only **198 unique text strings** (diversity ratio: 0.0066). The dataset is essentially a synthetic permutation of 40 template sentences with city names swapped.
- **Action Required:** Download real disaster tweets (QCRI HumAID ~77k tweets, 19 events), real rumor/fake news benchmarks (PHEME / LIAR / ISOT), and real AI text benchmarks (HC3 / MAGE).

### A2. Metric Sanity & Leakage Investigation
- **The ROC-AUC 1.0000 Anomaly:** A binary classifier achieving a perfect 1.0000 ROC-AUC under supposed out-of-distribution event testing is a hallmark of data leakage.
- **Deconstruction of the Leakage:**
  1. *Stylometric Feature Leakage:* In `download_datasets.py`, `burstiness_score` was sampled from $\mathcal{N}(0.28, 0.06)$ for synthetic text and $\mathcal{N}(0.68, 0.10)$ for human text. Because the 3-sigma intervals do not overlap ($0.28 + 3(0.06) = 0.46 < 0.68 - 2(0.10) = 0.48$), an elementary threshold model on `burstiness_score` achieves **ROC-AUC = 1.0000 and Accuracy = 1.0000** without reading a single word of text.
  2. *Lexical Token Leakage:* Text-only TF-IDF achieves ROC-AUC = 0.9411 because synthetic templates uniformly contain alarmist tokens (`"cholera"`, `"bacteria"`, `"deliberately"`, `"poisoned"`, `"dam"`, `"flee"`, `"fema"`) that never appear in the human templates.
  3. *Humanitarian Label Determinism:* In `download_datasets.py`, `humanitarian_category` was explicitly bound to each template tuple. A check of `df.groupby("text")["humanitarian_category"].nunique()` confirms that 100.0% of text strings map deterministically to exactly one category. The multi-class model was merely memorizing template IDs.
- **Action Required:** Discard simulated feature distributions. Train MLlib pipelines exclusively on real natural language text and authentic platform metadata from HumAID, PHEME, and HC3.

### A3. Tool Integration Proof
- **Apache Kafka 3.7.0:** Broker runs in WSL2. However, calling from host Windows `.venv` failed with `NoBrokersAvailable` due to loopback binding. Must bind to `0.0.0.0` or operate strictly inside WSL.
- **Spark Structured Streaming:** `streaming/spark_stream_scorer.py` correctly implements `readStream.format("kafka")`. However, `backend/main.py` bypassed this stream and replayed from `disaster_posts_raw.json` directly.
- **Apache Hive:** Hive Metastore runs on Thrift port 9083. However, HiveServer2 (port 10000) is absent. The architecture is "Hive Metastore queried via Spark SQL", which must be documented honestly.
- **Graph Analytics:** `graph/coordination_graph.py` used PySpark DataFrame `.join()` operations instead of Apache Spark GraphFrames or GraphX.

### A4. Fault Tolerance
- `tests/test_stream_recovery.py` tested offset recovery on only 200 messages. At this volume, in-memory buffers mask I/O edge cases. A true big data test requires $\ge 100\text{k}$ events with mid-stream `kill -9` of executor processes.

### A5. Scaling Benchmark Interpretation
- Empirical results from `results/scaling_benchmark.json`:
  - 10k rows: Pandas = 0.0454s vs Spark = 11.5009s (Spark is 253x slower)
  - 250k rows: Pandas = 0.0681s vs Spark = 11.3700s (Spark is 167x slower)
- Claiming Spark as a "performance winner" at this scale is invalid. Spark's justification lies in distributed memory scaling, fault-tolerant stream checkpoints, and out-of-core HDFS processing when data exceeds single-node RAM.

### A6. Allocation Ablation Validity
- `allocation/run_ablation.py` used `float(np.random.beta(8, 2))` to sample fake synthetic probabilities during the optimization trials rather than passing the actual trained ML model predictions on held-out posts.
- The dispatch scenarios evaluated only Houston flood boats, omitting earthquakes, wildfires, and medical/supply units.

### A7. Reproducibility & Filesystem
- The workspace is in `C:\Users\HP\OneDrive\Desktop\bigdata project`. Microsoft OneDrive sync locks SQLite, Git packfiles, and Parquet directories, and the WSL2 9P filesystem bridge introduces significant I/O latency. Moving the execution environment to native WSL (`/home/sivasai/crisisguard`) will dramatically improve reliability.

### A8. Documentation & Overclaims
- System documentation contains several marketing phrases ("enterprise", "empirically saves lives", "zero data loss"). These must be pruned and replaced with honest, measured engineering language.

---

## 3. Corrective Plan (Phase B & C Roadmap)
1. **Move to Native WSL:** Setup `/home/sivasai/crisisguard` with automated scripts.
2. **Download Real Datasets:** HumAID (QCRI), PHEME, LIAR, HC3, USGS, OpenFEMA, OpenStreetMap.
3. **Rebuild Real Storage:** Ingest real corpora into HDFS Bronze $\to$ Silver $\to$ Gold.
4. **Retrain MLlib on Real Data:** Event-wise splits, realistic metrics, baselines, sanity controls.
5. **Real Streaming & GraphFrames:** Stream to Kafka, score with Spark Structured Streaming, real GraphFrames analysis.
6. **Realistic Allocation & Ablation:** Feed real detector outputs across multiple regions and disaster types with $\ge 30$ seeds and 95% CIs.
7. **Scaling to 1M+ Rows:** Genuine memory profiling and capability justification.
8. **Final Polish & Defence Prep:** `REPORT.md` (6–10 pages), `QA_PREP.md` (25 questions), `AUDIT_AFTER.md`.
