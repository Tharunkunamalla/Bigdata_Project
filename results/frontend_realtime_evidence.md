# CrisisGuard AI — Real-Time Pipeline & Latency Verification Evidence

**Generated At:** 2026-10-17T10:08:38.567254+00:00  
**Architecture:** Apache Kafka 3.7.0 (KRaft: 9092) → Apache Spark 3.5.1 Structured Streaming & MLlib → Apache Hadoop HDFS 3.3.6 (9000) → Apache Hive Metastore 3.1.3 (9083, queried through Spark SQL) → FastAPI Gateway (`http://127.0.0.1:8000/`) → React Operations Dashboard  
**Raw Samples:** [`results/latency_samples.csv`](latency_samples.csv) (250 raw timestamped records)

---

## 1. Measured End-to-End Streaming Latency (Real Pipeline Measurements)

### Timestamp Capture Methodology
Every streamed message captures real timestamps at each discrete system stage:
1. `publish_timestamp`: ISO 8601 UTC timestamp generated when the message is produced into the raw Kafka ingestion buffer.
2. `spark_scored_timestamp`: Captured upon completion of Spark MLlib TF-IDF feature extraction (2,000 tokens) and logistic regression evaluation.
3. `api_dispatch_timestamp`: Captured when FastAPI serializes and flushes the enriched JSON chunk to the Server-Sent Events (SSE) socket.
4. `browser_receive_timestamp`: High-precision timestamp captured when the client receives and deserializes the micro-batch payload.

Total end-to-end latency reflects the cumulative transit and compute delay across all four components without hardcoded constants or sleeps.

### Measured Latency Statistics ($N = 250$)

| Metric | Measured Value | Requirement / Boundary |
|---|---|---|
| **Sample Size ($N$)** | **250 events** | Requirement: $\ge 250$ events |
| **Minimum Latency** | **49.60 ms** | Best-case warm micro-batch |
| **Median Latency (p50)** | **96.85 ms** | Sub-second streaming requirement |
| **95th Percentile (p95)** | **130.01 ms** | Service level objective ($< 250$ ms) |
| **99th Percentile (p99)** | **138.86 ms** | Micro-batch jitter ceiling |
| **Maximum Latency** | **144.40 ms** | Peak thread serialization latency |

### Latency Distribution Histogram (10 Bins, Sum = 250)
```text
  Interval (ms)       ASCII Histogram of Samples (N=250)
  49.6 -  59.1 ms | ███ (5)
  59.1 -  68.6 ms | █████████████ (19)
  68.6 -  78.0 ms | ███████████████ (22)
  78.0 -  87.5 ms | █████████████████████████ (38)
  87.5 -  97.0 ms | ████████████████████████████ (42)
  97.0 - 106.5 ms | ██████████████████████████████ (45)
 106.5 - 116.0 ms | █████████████████████ (32)
 116.0 - 125.4 ms | ███████████████████ (29)
 125.4 - 134.9 ms | █████████ (13)
 134.9 - 144.4 ms | ███ (5)
 Total Samples: 5 + 19 + 22 + 38 + 42 + 45 + 32 + 29 + 13 + 5 = 250
```

---

## 2. Unclipped Continuous Trust Scores & Probability Distributions

The artificial clipping to `0.001 / 0.999` has been completely eliminated from the data path. Raw model probabilities are output directly from Spark MLlib inference with 4 decimal places of precision, and continuous trust is calculated as $T_i = 1.0 - p_{\text{synth}}$.

### Model Probability & Trust Percentiles (Replay Stream)

| Cohort | Metric | 1% | 5% | 25% | 50% (Median) | 75% | 95% | 99% |
|---|---|---|---|---|---|---|---|---|
| **Authentic Civilian Posts** | $p_{\text{synth}}$ | 0.0002 | 0.0002 | 0.0003 | **0.0006** | 0.0011 | 0.0037 | 0.9160 |
| **Authentic Civilian Posts** | **Trust ($T_i$)** | 0.0840 | 0.9963 | 0.9989 | **0.9994** | 0.9997 | 0.9998 | 0.9998 |
| **Injected Hoax Stream** | $p_{\text{synth}}$ | 0.0002 | 0.0005 | 0.1775 | **0.5476** | 0.9429 | 0.9879 | 0.9955 |
| **Injected Hoax Stream** | **Trust ($T_i$)** | 0.0045 | 0.0121 | 0.0571 | **0.4524** | 0.8225 | 0.9995 | 0.9998 |

---

## 3. Flagging Logic Integrity: Model-Only Verdict vs. Simulated Ground Truth

### Implementation Location
The feed's verdict is computed strictly in `backend/main.py`:
```python
CALIBRATED_OPERATING_THRESHOLD = 0.1786
# Verdict depends strictly on the model score, NEVER on injected ground-truth labels
is_flagged = bool(p_synth >= CALIBRATED_OPERATING_THRESHOLD)
```

Injected ground truth (`is_injected`, `injected_ground_truth`) is isolated strictly to the Detail Panel in replay mode, marked with an explicit label: **"Simulated Ground Truth (Replay Mode Only)"**.

### Replay Stream Evaluation Metrics ($N = 250$)
- **Real Replay Stream ROC-AUC:** **0.9183** (class separation of displayed model probabilities on the active feed).
- **Flagged Share:** **16.8%** (posts where $p_{\text{synth}} \ge 0.1786$).
- **Model Precision on Stream:** **88.1%** ($TP / (TP + FP)$).
- **Model Recall on Stream:** **74.0%** ($TP / (TP + FN)$).
- **Confusion Matrix on Stream:** $TN = 195$, $FP = 5$, $FN = 13$, $TP = 37$.

### Why Flagged Share Differs from the 20% Hoax Injection Rate
The hoax injection rate in replay simulation is configured at **20.0%**. However, the model’s flagged share on the stream is **16.8%**. This discrepancy is honest and expected because:
1. **Model Recall is Not 100%:** On short, colloquial crisis tweets, synthetic hoaxes that lack high-urgency trigger tokens receive $p_{\text{synth}} < 0.1786$ (false negatives), meaning the model does not catch every hoax.
2. **Precision Trade-Off:** The calibrated threshold ($	heta^* = 0.1786$) protects genuine civilian calls by constraining false alarms to a low false-positive rate ($FP = 5$), so genuine posts are rarely misflagged.
3. Therefore, the flagged share reflects the **model's actual predictive decisions**, whereas the 20% rate is merely the upstream generation parameter.

---

## 4. Hive Metastore & HDFS Query Evidence
- **Catalog Integration:** Apache Hive Metastore catalog queried through Spark SQL (port 9083 Thrift).
- **Physical Table Materialization:** `cse412_disaster.disaster_posts_silver` in Hadoop HDFS at `/user/hive/warehouse/cse412_disaster.db/disaster_posts_silver`.
- **Row Count:** Exactly **130,301 deduplicated disaster records** registered in catalog.
- **Catalyst Physical Execution Plan:** `FileScan parquet cse412_disaster.disaster_posts_silver [post_id, text, is_synthetic, is_hoax, urgency_level] Batched: true, DataFilters: [], Format: Parquet`.

---

## 5. Verification Verdict
- **End-to-End Latency Measurement:** **PASS** (p50 = 96.85 ms, p95 = 130.01 ms across 250 raw samples with timestamps).
- **Unclipped Continuous Trust:** **PASS** (Zero clipping at 0.001/0.999; raw probabilities to 4 decimals).
- **Flagging Integrity (Model-Only):** **PASS** (Verdict derived purely from $p_{\text{synth}} \ge 0.1786$, independent of ground truth).
