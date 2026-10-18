# CrisisGuard AI — Repository Cleanup Plan (v1.0-final-9)

**Generated:** 2026-10-18  
**Release Target:** `v1.0-final-9`  
**Purpose:** Remove redundant, legacy, and superseded files, unreferenced screenshots, dead frontend components, and cache directories to maximize repository hygiene and maintainability while preserving all required course deliverables and core pipeline code.

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
| `walkthrough.md` | Root scratchpad | `grep -r "walkthrough.md"` returned 0 references. Root scratchpad leftover from early scaffolding. |
| `scripts/capture_screenshot.py` | Superseded script | Single-view screenshot script superseded by `scripts/capture_all_screenshots.py`. 0 references in Makefile, docs, or tests. |
| `scripts/setup_wsl_env.py` | Superseded script | Replaced by `scripts/setup_wsl.sh` for cluster provisioning. 0 references found in codebase. |
| `scripts/test_download_sources.py` | Temporary diagnostic | One-off check for URL accessibility during Phase A. 0 references found. |
| `scripts/tune_disaster_threshold.py` | One-off search script | Standalone threshold grid-search script. Calibrated threshold ($\theta^* = 0.1786$) is permanently saved in `results/metrics.json`. 0 references. |
| `scripts/generate_initial_posts.py` | One-off helper | Generated initial sample posts once into `frontend/src/utils/initialPosts.js`. 0 external references. |
| `scripts/check_hive_port.py` | Superseded script | 3-line socket check superseded by `tests/verify_infra.py` and backend `/api/health`. 0 active imports. |
| `scripts/audit_phase_a.py` | Diagnostic script | Phase A audit script superseded by `scripts/check_doc_consistency.py`. |
| `scripts/diagnose_item2.py` | Diagnostic script | One-off audit check for trust clipping. 0 operational imports. |
| `scripts/measure_stream_latency.py` | Superseded script | Replaced by `scripts/measure_real_pipeline.py` which records full 4-timestamp end-to-end metrics. |
| `frontend/src/pages/DemoPage.jsx` | Dead UI Page | Section C strictly scopes dashboard to 4 views (Live, Allocation, Pipeline, Results) and specifies "no Demo page (the demo is in the terminal)". |
| `frontend/src/components/SettingsDrawer.jsx` | Dead UI Component | Section C strictly specifies "no settings drawer, column toggles, density options, timezone options, URL-state, export buttons or keyboard-shortcut system". |
| `docs/screenshots/01_live_dark.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/01_live_light.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/01_live_mobile.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/02_models_dark.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/02_models_light.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/03_allocation_dark.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/03_allocation_light.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/04_pipeline_dark.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/04_pipeline_light.png` | Unreferenced screenshot | Old dashboard screenshot. Replaced by fresh screenshots of the 4 rebuilt views. |
| `docs/screenshots/05_demo_dark.png` | Unreferenced screenshot | Old dashboard screenshot for deleted Demo page. |
| `docs/screenshots/05_demo_light.png` | Unreferenced screenshot | Old dashboard screenshot for deleted Demo page. |
| `docs/screenshots/dashboard_overview.png` | Unreferenced screenshot | Old composite mockup. |
| `.pytest_cache/` | Cache directory | Python test run cache (excluded by `.gitignore`). |
| `**/__pycache__/` | Cache directories | Python bytecode caches (excluded by `.gitignore`). |

---

## 3. Targeted Kept Screenshots (Maximum 4 UI + 2 Terminal)

Per Section A.3:
1. `docs/screenshots/view_1_live.png`: Dashboard View 1 (Live GIS Map & Credibility Feed).
2. `docs/screenshots/view_2_allocation.png`: Dashboard View 2 (Trust-Aware MILP Fleet Optimization).
3. `docs/screenshots/view_3_pipeline.png`: Dashboard View 3 (Big Data Cluster & Streaming Health).
4. `docs/screenshots/view_4_results.png`: Dashboard View 4 (Models, ROC/PR Curves & Confusion Matrix).
5. `docs/screenshots/terminal_pipeline_demo.png`: Master terminal demonstration in Ubuntu terminal.
6. `docs/screenshots/terminal_spark_hive_query.png`: HDFS Parquet & Hive Metastore Spark SQL catalog query.

All 6 screenshots will be referenced in `docs/REPORT.md`, `docs/REPORT.pdf`, and `README.md`.
