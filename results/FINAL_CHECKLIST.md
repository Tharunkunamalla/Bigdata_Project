# CrisisGuard AI — Final Release Verification Checklist (v1.0-final-4)

**Status:** ALL CHECKS PASSED  
**Release Tag:** `v1.0-final-4`  
**Generated At:** 2026-10-15  
**Audit Script Status:** `scripts/check_doc_consistency.py` -> **0 errors, 0 warnings**  
**Unit Test Status:** `tests/test_real_pipeline.py` -> **11 / 11 tests passed**  

---

## 1. Section 1: Event-Held-Out Evaluation & Rumour Ablation
- [x] **HumAID 5-Fold Cross-Validation (60 Events):**
  - Evaluated in `ml/evaluate_event_held_out.py`
  - Output artifact: [`results/event_held_out_evaluation.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/event_held_out_evaluation.json)
  - Humanitarian Category Macro-F1: $0.5587 \pm 0.0631$ (vs random stratified in-distribution $0.7564$)
  - Urgency Regressor $R^2$: $0.3838 \pm 0.1672$ (MAE: $0.6092 \pm 0.1088$)
- [x] **PHEME 9-Event Leave-One-Event-Out (LOEO):**
  - Evaluated in `ml/evaluate_event_held_out.py`
  - Output artifact: [`results/event_held_out_evaluation.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/event_held_out_evaluation.json)
  - Rumour ROC-AUC: $0.6451 \pm 0.0743$ (Macro-F1: $0.3322 \pm 0.1302$ vs random stratified $0.8920$)
- [x] **7-Configuration Rumour Source Ablation:**
  - Evaluated in `ml/evaluate_event_held_out.py`
  - Output artifact: [`results/rumour_source_ablation.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/rumour_source_ablation.json)
  - Verified: PHEME alone achieves ROC-AUC $0.8620$; adding LIAR political statements drops performance due to vocabulary/style domain divergence.

---

## 2. Section 2: Tweet-Domain Detector, Disjoint Generators & Baselines
- [x] **Disjoint Hoax Generators:**
  - Training set: Generator A (1,367) + Generator B (1,488) = 2,855 samples (~80-140 chars).
  - Held-out Test set: Generator C (1,355 samples, 0% train overlap).
  - Output artifacts: [`results/synthetic_detector_evaluation.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/synthetic_detector_evaluation.json), [`results/metrics.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/metrics.json)
  - Held-out Gen C ROC-AUC: **0.9940**, PR-AUC: **0.9007**, Recall @ 1% FPR: **76.75%**, Recall @ 5% FPR: **100.00%**, Brier Score: **0.0473**, ECE: **0.0487**.
- [x] **Ablation Baselines & Permutation Control:**
  - Output artifact: [`results/sanity_controls.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/sanity_controls.json)
  - Length-Only Model ROC-AUC: **0.6685**
  - Stylometric-Only Model ROC-AUC: **0.7820**
  - Full Combined TF-IDF + Stylometric ROC-AUC: **0.9940**
  - 20-Shuffle Label Permutation Control: Mean ROC-AUC **$0.4997 \pm 0.0052$** ($t = -0.23, p = 0.8220$, perfectly indistinguishable from random chance 0.50).
- [x] **Long-Text Detector & Dual Routing:**
  - Long text detector trained on HC3 + MAGE: ROC-AUC **0.8021**, PR-AUC **0.7948**.
  - Dynamic routing: posts $<280$ characters dispatched to short-tweet detector, $\ge 280$ characters dispatched to long-text detector.
  - Output artifact: [`data/curated/test_predictions.csv`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/data/curated/test_predictions.csv)

---

## 3. Section 3: Graph Analytics & Circularity Control
- [x] **MinHashLSH Coordination Graph:**
  - Script: [`graph/coordination_graph.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/graph/coordination_graph.py)
  - Output artifacts: [`results/coordination_graph_results.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/coordination_graph_results.json), [`results/cluster_penalties.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/cluster_penalties.json)
  - Extracted 242 near-duplicate coordination edges over 15,000 sampled posts.
  - Connected components: 82 clusters, max size 38, mean size 3.2.
  - PageRank ($\alpha = 0.85$): identified central campaign amplifiers up to PR 0.018543.
  - Label propagation: discovered 94 communities, max size 13.
- [x] **3-Way Circularity Control:**
  - Real disaster posts edge density: 0.0163 edges/post.
  - Shuffled template post edge density: 0.0302 edges/post.
  - Synthetic hoaxes edge density: 0.0000 edges/post.
  - Formally identifies ~30.0% template artifact in high-density subgraphs.
- [x] **Cluster Penalty Export:**
  - Applied penalty formula $\Delta T(S_i) = \min(0.40, 0.05 \cdot (S_i - 1))$ to 262 coordinated posts.

---

## 4. Section 4: Resource Allocation Ablation v3
- [x] **6 Policies Evaluated Across 4 Regions (30 Seeds, 95% CIs, Paired t-tests):**
  - Script: [`allocation/run_ablation_v3.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/allocation/run_ablation_v3.py)
  - Output artifact: [`results/ablation_study.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/ablation_study.json)
  - **Houston Flood:**
    - Blind Greedy: 40.40 victims
    - Blind LP: $40.83 \pm 1.04$ victims
    - Trust-Aware Continuous: **$41.40 \pm 1.04$** victims (Gain: **+0.57 victims**, $p = 0.00572$, 5.1% of Oracle)
    - Validation Threshold: **$41.40 \pm 0.98$** victims (Gain: **+0.57 victims**, $p = 0.00716$, 5.1% of Oracle)
    - Cluster-Penalized Trust: **$42.77 \pm 1.06$** victims (Gain: **+1.93 victims**, $p = 9.86 \times 10^{-9}$, 17.3% of Oracle)
    - Oracle: **$52.00$** victims (+11.17 max theoretical headroom)
  - **California Wildfires:** Blind LP 30.70 vs Continuous 30.57 (Gain: -0.13, honest flat) vs Cluster Penalty 32.00 (+1.30, $p = 2.12 \times 10^{-6}$).
  - **Nepal Earthquake:** Blind LP 40.67 vs Continuous 41.13 (+0.47, $p = 0.0457$) vs Cluster Penalty 42.73 (+2.07, $p = 5.41 \times 10^{-9}$).
  - **Miami Tropical Storm:** Blind LP 41.90 vs Continuous 42.47 (+0.57, $p = 0.00572$) vs Cluster Penalty 43.93 (+2.03, $p = 2.48 \times 10^{-8}$).
- [x] **Monotone Binormal Detector Sweep:**
  - AUC 0.50 control gain: strictly **+0.04 victims** ($p = 0.95$, control passes).
  - Monotonic rise: +11.08 (AUC 0.60) $\to$ +16.68 (AUC 0.80) $\to$ +22.48 (AUC 0.99, 90.5% of Oracle).
- [x] **Hoax Rate Stress Test:**
  - Gain rises from +6.05 victims (5% hoax rate) to +24.75 (20%) to +45.20 (40%).

---

## 5. Section 5: Apache Hive Metastore & Big Data Scaling
- [x] **Apache Hive Metastore Verification:**
  - Script: [`scripts/verify_hive_and_query.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/scripts/verify_hive_and_query.py)
  - Output artifact: [`results/hive_query_evidence.txt`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/hive_query_evidence.txt)
  - Connected via PySpark thrift client to `thrift://127.0.0.1:9083`.
  - Registered managed Silver table `disaster_posts_silver` (130,301 rows).
  - Materialized Gold aggregation table `gold_emergency_demand_summary` in HDFS with Catalyst execution plan documented.
- [x] **Scaling Benchmark (100k to 10M rows):**
  - Script: [`results/scaling_benchmark.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/scaling_benchmark.py)
  - Output artifact: [`results/scaling_benchmark.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/scaling_benchmark.json)
  - Pandas OOMs at 5M and 10M rows under 1.5 GB memory limit.
  - Apache Spark executes out-of-core spilling to disk, completing 10M rows in **2.13s**.
- [x] **Fault Tolerance Stream Recovery Test:**
  - 100,000-event streaming kill test completed recovery in **7.72s** with 0 data loss.

---

## 6. Section 6: Data Provenance & Funnel Consistency
- [x] **Corpus Funnel Audit:**
  - Documented in [`docs/DATA.md`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/docs/DATA.md)
  - Raw: 131,538 rows $\to$ Cleaned: 131,435 rows $\to$ Deduplicated: **130,301 rows** (Train: **97,422**, Test: **32,879**).
- [x] **Disclaimers & Authentic Artifacts:**
  - Explicitly declared that post coordinates are simulated uniformly over regional bounding boxes.
  - Verified authentic depot coordinates from OpenStreetMap Overpass API.
  - Complete status table across all 10+ open-source corpora.

---

## 7. Section 7: Dashboard UI & Live Indicators
- [x] **Frontend Indicators & Dynamic Binding:**
  - File: [`frontend/src/App.jsx`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/frontend/src/App.jsx)
  - Added visible "SIMULATED STREAM" and "SIMULATED COORDINATES (OSM REGIONAL DEPOTS)" status badges.
  - Bound KPI metrics dynamically to [`results/summary.json`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/results/summary.json).
  - Built production bundle with `npm run build` (exit code 0).

---

## 8. Section 8: Automated Verification & Consistency Audit
- [x] **Pytest Suite:**
  - File: [`tests/test_real_pipeline.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/tests/test_real_pipeline.py)
  - 11 of 11 integration and unit tests passing.
- [x] **Doc Consistency Audit:**
  - Script: [`scripts/check_doc_consistency.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/scripts/check_doc_consistency.py)
  - Output: **0 errors, 0 warnings**.
  - All metrics synchronized automatically via [`scripts/build_report_numbers.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/scripts/build_report_numbers.py).

---

## 9. Section 9: Academic Report & PDF Compilation
- [x] **Markdown Report:**
  - File: [`docs/REPORT.md`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/docs/REPORT.md)
  - Formatted with structured academic tables, architecture diagram, methodology, and limitations.
- [x] **PDF Document:**
  - File: [`docs/REPORT.pdf`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/docs/REPORT.pdf)
  - Compiled via headless Microsoft Edge in [`scripts/export_report_pdf.py`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/scripts/export_report_pdf.py).
  - Verified length: **9 pages** (strictly compliant with the 6-10 page requirement).
- [x] **Support Documentation:**
  - Synchronized [`docs/LIMITATIONS.md`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/docs/LIMITATIONS.md) and [`docs/QA_PREP.md`](file:///c:/Users/HP/OneDrive/Desktop/bigdata%20project/docs/QA_PREP.md).

---

## 10. Section 10: Final Freeze & Release Tag
- [x] Working tree clean and committed.
- [x] Tagged release: `v1.0-final-4`.
