# CrisisGuard AI — Repository Cleanup Plan (v1.0-final-9 Human-Made UI)

**Generated:** 2026-10-18  
**Release Target:** `v1.0-final-9`  
**Purpose:** Remove redundant, legacy, and superseded files, unreferenced screenshots, dead frontend components, and technical UI panels to align the product with an emergency coordinator workflow (Live, Dispatch, About) while preserving all required course deliverables and core pipeline code.

---

## 1. Safety & Retention Policy (Files Kept — Never Deleted)

Per prompt constraints, the following critical deliverables and pipeline components are strictly preserved:
- **Required Course Deliverables**:
  - `docs/REPORT.md`, `docs/REPORT.html`, `docs/REPORT.pdf`
  - `README.md`, `Makefile`
  - `docs/ARCHITECTURE.md`, `docs/DATA.md`, `docs/LIMITATIONS.md`
  - `docs/QA_PREP.md`, `docs/DEMO_SCRIPT.md`, `docs/TOOL_JUSTIFICATION.md`
  - `docs/architecture_diagram.png`, `docs/architecture_diagram_bw.png`
- **Data Provenance & Manifests**:
  - `data/raw/dataset_provenance_manifest.json`
  - `data/build_real_corpus.py`, `data/download_datasets.py`
  - `data/raw/.gitkeep`, `data/curated/.gitkeep`, `data/interim/.gitkeep`
- **Active Distributed Pipeline Code**:
  - Ingestion: `ingestion/replay_simulator.py`
  - Streaming: `streaming/spark_stream_scorer.py`
  - ML Pipeline: `ml/train_models_real.py`, `ml/models/`
  - Optimization: `allocation/optimizer.py`, `allocation/run_ablation_v3.py`
  - Gateway & Serving: `backend/main.py`
  - MinHash Graph: `graph/coordination_graph.py`
  - Hive DDL: `hive/create_tables.sql`
- **Single Source of Truth Results & Evidence**:
  - `results/summary.json`, `results/metrics.json`, `results/ablation_study.json`
  - `results/sanity_controls.json`, `results/coordination_graph_results.json`
  - `results/scaling_benchmark.json`, `results/stream_recovery_test.json`
  - `results/stream_recovery_evidence.txt`, `results/hive_query_evidence.txt`
  - `results/latency_samples.csv`, `results/frontend_realtime_evidence.md`
- **Core Automation & Test Suite**:
  - `scripts/check_doc_consistency.py`, `scripts/build_report_numbers.py`
  - `scripts/demo_pipeline.sh`, `scripts/demo_terminal.py`, `scripts/run_demo.py`
  - `scripts/measure_real_pipeline.py`, `scripts/capture_all_screenshots.py`
  - `scripts/export_report_pdf.py`, `scripts/start_all.sh`, `scripts/stop_all.sh`, `scripts/setup_wsl.sh`
  - `tests/test_real_pipeline.py`, `tests/test_dashboard_upgrade.py`, `tests/test_integration.py`, `tests/verify_infra.py`

---

## 2. Items Identified for Removal with Proof of Non-Reference

| Path proposed for removal | Category | Proof & Justification |
|---|---|---|
| `frontend/src/pages/PipelinePage.jsx` | Technical UI Page | Section 1 & 3 strictly scope dashboard to 3 views for emergency coordinators (Live, Dispatch, About). Infrastructure telemetry is presented in docs/REPORT.md and terminal demo. 0 references after removal. |
| `frontend/src/pages/ResultsPage.jsx` | Technical UI Page | Machine learning model cards, ROC/PR curves, and confusion matrices belong in technical reports, not coordinator dispatch console. 0 references after removal. |
| `docs/screenshots/view_3_pipeline.png` | Superseded Screenshot | Replaced by 3-view coordinator console screenshots (Live, Dispatch, About). |
| `docs/screenshots/view_4_results.png` | Superseded Screenshot | Replaced by 3-view coordinator console screenshots (Live, Dispatch, About). |
| `frontend/src/pages/DemoPage.jsx` | Dead UI Page | Demo is in the terminal; removed in prior pass. |
| `frontend/src/components/SettingsDrawer.jsx` | Dead UI Component | Replaced by slim top-bar status drawer. |
| `walkthrough.md` | Root scratchpad | 0 references found by grep. |
| `scripts/capture_screenshot.py` | Superseded script | Superseded by `scripts/capture_all_screenshots.py`. |
| `scripts/setup_wsl_env.py` | Superseded script | Superseded by `scripts/setup_wsl.sh`. |
| `scripts/test_download_sources.py` | Diagnostic script | Scaffolding check; 0 references. |
| `scripts/tune_disaster_threshold.py` | Diagnostic script | Grid search script; calibrated threshold is in `results/metrics.json`. |
| `scripts/generate_initial_posts.py` | Diagnostic script | Generated initial mock data once; 0 references. |
| `scripts/check_hive_port.py` | Superseded script | Superseded by `tests/verify_infra.py`. |
| `scripts/audit_phase_a.py` | Diagnostic script | Superseded by `scripts/check_doc_consistency.py`. |
| `scripts/diagnose_item2.py` | Diagnostic script | Diagnostic check; 0 references. |
| `scripts/measure_stream_latency.py` | Superseded script | Superseded by `scripts/measure_real_pipeline.py`. |
| `frontend/src/utils/initialPosts.js` | Stale Mock Data | Contained off-topic MAGE benchmark texts (razor blades, TF2, toddler bomb). Replaced by dynamic verified disaster API feed. 0 references after removal. |
| `.pytest_cache/` | Cache directory | Python test cache (excluded by `.gitignore`). |
| `**/__pycache__/` | Cache directories | Python bytecode caches (excluded by `.gitignore`). |
| `derby.log` | Ephemeral log | Derby temporary log file. |

---

## 3. Targeted Kept Screenshots (Exactly 3 Dashboard + 2 Terminal)

Per Section 6.3:
1. `docs/screenshots/view_1_live.png`: Dashboard View 1 (Live Map & Credibility Feed).
2. `docs/screenshots/view_2_dispatch.png`: Dashboard View 2 (Dispatch Planning).
3. `docs/screenshots/view_3_about.png`: Dashboard View 3 (About & Transparency Overview).
4. `docs/screenshots/terminal_pipeline_demo.png`: Master terminal demonstration in Ubuntu terminal.
5. `docs/screenshots/terminal_spark_hive_query.png`: HDFS Parquet & Hive Metastore Spark SQL catalog query.
