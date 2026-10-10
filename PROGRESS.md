# CSE412 Big Data Project: Phase Gates & Milestone Tracker

**Project:** Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation  
**Course:** CSE412 — Big Data & Large-Scale Computing (Tracks D & E)  
**Cluster Architecture:** Apache Kafka 3.7.0 (KRaft) + Hadoop HDFS 3.3.6 + Hive Metastore 3.1.3 + Apache Spark 3.5.1 + PuLP 2.9.0 MILP  

---

## Milestone Execution Summary

| Phase | Description | Status | Evidence / Artifact |
| :--- | :--- | :---: | :--- |
| **Phase 1: Plan** | Hardware profiling, version matrix, task decomposition | **COMPLETE** | `docs/PLAN.md`, `task_list.md`, `.venv` initialized |
| **Phase 2: Infrastructure** | Persistent cluster (Kafka, HDFS, Hive, Spark), `make verify-infra` | **COMPLETE** | `tests/verify_infra.py` passing 4/4 gates |
| **Phase 3: Data** | Ingestion of HumAID/HC3, HDFS/Hive bronze-silver-gold | **COMPLETE** | `docs/DATA.md`, `docs/CHECKIN_1_INGESTION.md` (60,000 records) |
| **Phase 4: Models** | Spark MLlib pipelines (synthetic, rumour, category, urgency), event-wise CV | **COMPLETE** | `ml/`, `results/metrics.json`, `ml/model_card.md` (ROC-AUC 1.0) |
| **Phase 5: Streaming** | Replay simulator + Spark Structured Streaming + HDFS/Hive + recovery test | **COMPLETE** | `streaming/`, `tests/test_stream_recovery.py` (Zero data loss) |
| **Phase 6: Allocation** | Windowed trust-demand, greedy + LP optimization, ablation study | **COMPLETE** | `allocation/`, `docs/CHECKIN_2_RESULTS.md` (-97.3% waste, +38.7% saves) |
| **Phase 7: Full Stack** | FastAPI SSE backend, React Leaflet dashboard | **COMPLETE** | `backend/main.py`, `frontend/src/App.jsx` (5 operations panels) |
| **Phase 8: Stretch** | Amplification graph (PySpark), scaling benchmark (Pandas vs Spark) | **COMPLETE** | `graph/coordination_graph.py`, `results/scaling_benchmark.py` |
| **Phase 9: Docs & PR** | 6–10 page report, architecture, justifications, demo script, OSS PR candidate | **COMPLETE** | `docs/REPORT.md`, `docs/ARCHITECTURE.md`, `docs/TOOL_JUSTIFICATION.md`, `docs/DEMO_SCRIPT.md`, `docs/OSS_CONTRIBUTION.md` |
| **Phase 10: Review** | Full acceptance criteria audit & Walkthrough artifact | **COMPLETE** | `scripts/run_demo.py` (`make demo` passing 5/5 steps) |

---

## Live Performance & Evaluation Metrics Summary
1. **Cluster Infrastructure:** Kafka 3.7 (`127.0.0.1:9092`), HDFS NameNode (`9000`), Hive Metastore (`9083`), Spark 3.5.1 all online and passing automated tests.
2. **Machine Learning Suite:**
   - Synthetic Misinformation Detector: **ROC-AUC = 1.0000** (Accuracy = 95.89%)
   - Rumour / Hoax Classifier: **ROC-AUC = 0.9293** (F1 = 0.9304)
   - Humanitarian Category Classifier: **Accuracy = 54.52%** (Macro F1 = 0.4674)
   - Urgency Level Regressor: **RMSE = 0.2184** (MAE = 0.1627)
3. **Streaming Fault Tolerance:** 200/200 records recovered post-crash (**0% data loss**).
4. **Emergency Allocation Ablation:**
   - Wasted dispatches reduced by **97.29%** under Trust-Aware MILP vs. Blind optimization.
   - Critical genuine emergency victims saved increased by **+38.71%**.
5. **Sybil Coordination Network:** 50,072 user vertices and 331,466 coordination edges analyzed; detected 3,340 bot accounts exhibiting **20x retweet amplification** and fresh account tenure.
