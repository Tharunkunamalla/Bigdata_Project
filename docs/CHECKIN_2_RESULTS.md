# CSE412 Big Data Project: Check-in Report 2 — ML Performance & Allocation Ablation Results

**Project Title:** Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation  
**Course:** CSE412 — Big Data & Large-Scale Computing  
**Milestone:** Check-in 2: Machine Learning Evaluation & Resource Allocation Ablation Findings  
**Evaluation Date:** October 2026  

---

## 1. Machine Learning Suite Performance

Four distributed Spark MLlib models were trained on 60,000 crisis records using an **out-of-distribution event-wise split**:
- **Training Set (30,000 records):** Hurricane Harvey & Kerala Floods.
- **Testing Set (30,000 records):** Kahramanmaraş Earthquake & California Wildfires.

This evaluation regime strictly tests cross-disaster generalization without temporal or spatial leakage.

### Quantitative MLlib Performance Summary (from `results/metrics.json`):

| Model | Architecture | Primary Metric | Measured Value | Baseline Threshold | Status |
|:---|:---|:---|:---|:---|:---|
| **Synthetic Detector** | LogisticRegression (ElasticNet $\alpha=0.2$) | Area Under ROC | **1.0000** (Acc: 95.89%) | $\ge 0.70$ | **PASS** |
| **Rumour Classifier** | RandomForest (20 trees, depth 7) | Area Under ROC | **0.9293** (F1: 93.04%) | $\ge 0.65$ | **PASS** |
| **Humanitarian Categorizer** | Multinomial Logistic Regression | Classification Accuracy | **54.52%** (Macro F1: 0.467) | $\ge 0.50$ | **PASS** |
| **Urgency Regressor** | ElasticNet Linear Regression | RMSE / MAE | **0.2184** / 0.1627 | $\le 0.30$ | **PASS** |

Total Spark MLlib training runtime across 60,000 records: **164.63 seconds**.

---

## 2. Resource Allocation Ablation Study

To evaluate whether misinformation awareness actually saves lives during disasters, we executed an ablation experiment across 5 random seeds (Seeds 412–416). We compared four dispatching policies under fleet capacity constraints (52 rescue boats across 4 regional emergency bases):

1. **Blind Greedy:** Dispatches nearest vehicle based strictly on raw reported urgency.
2. **Trust-Aware Greedy:** Discounts reported urgency by model-derived trust scores: $\text{EffUrgency} = \text{Urgency} \times \text{Trust}$.
3. **Blind MILP:** Optimal Mixed-Integer Linear Program without misinformation penalty.
4. **Trust-Aware MILP:** Optimal Mixed-Integer Linear Program maximizing trust-discounted net benefit penalized by transit distance.

### Empirical Ablation Findings (from `results/ablation_study.json`):

| Dispatch Policy | Wasted Dispatches to Hoaxes (%) | Genuine Emergency Victims Reached | Average Wasted Fleet Units |
|:---|:---:|:---:|:---:|
| **Blind Greedy** | 26.54% (±7.82%) | 38.2 (±4.1) / 52 | 13.8 boats wasted |
| **Blind MILP (Optimal)** | 28.46% (±7.23%) | 37.2 (±3.8) / 52 | 14.8 boats wasted |
| **Trust-Aware Greedy** | 0.77% (±0.94%) | 51.6 (±0.5) / 52 | 0.4 boats wasted |
| **Trust-Aware MILP (Proposed)** | **0.77% (±0.94%)** | **51.6 (±0.5) / 52** | **0.4 boats wasted** |

### Key Analytical Takeaways:
1. **97.29% Reduction in Wasted Dispatches:** Under adversarial misinformation, blind optimization squanders almost 30% of emergency response assets on fabricated crisis calls. The trust-aware policy eliminates over 97% of these false dispatches.
2. **+38.71% Increase in Critical Lives Reached:** In a capacity-constrained fleet of 52 boats, blind dispatch leaves 15 genuine stranded victims unserved. Trust-aware optimization prioritizes verified victims, reaching **51.6 out of 52** potential saves (+38.71% lift).
3. **Mathematical Justification for Hybrid Pipeline:** Standard algorithmic optimization alone is counterproductive when fed unverified inputs (Blind MILP actually performed slightly worse than Blind Greedy because it hyper-optimized towards false high-urgency clusters). Coupling Spark MLlib trust filtering with PuLP mathematical programming is essential for operational efficacy.

---

## 3. Streaming Fault-Tolerance Verification

The Spark Structured Streaming engine was audited via automated failure-injection testing (`tests/test_stream_recovery.py`):
- 200 messages streamed to Kafka.
- Abrupt executor termination simulated mid-stream.
- Streaming engine restarted with preserved HDFS checkpoint directory.
- **Verification Result:** Total messages processed across pre- and post-recovery = **200 / 200** records (**Zero data loss, 100% recovery fidelity**). Recorded in `results/stream_recovery_test.json`.
