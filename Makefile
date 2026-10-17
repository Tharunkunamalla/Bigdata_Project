# ==============================================================================
# CSE412 Big Data Project: Makefile
# Real-Time Detection of Synthetic Misinformation & Intelligent Resource Allocation
# ==============================================================================

SHELL := /bin/bash
PYTHON := .venv/bin/python
ifeq ($(OS),Windows_NT)
	PYTHON := .venv/Scripts/python.exe
endif

COMPOSE_FILE := docker/docker-compose.yml

.PHONY: help up down reset verify-infra data train stream simulate api ui ui-build test test-integration build-frontend verify report demo demo-terminal

help:
	@echo "CrisisGuard AI Commands (v1.0-final-8):"
	@echo "  make api               - Start FastAPI backend operations gateway (:8000)"
	@echo "  make ui                - Start React/Vite development server (:5173)"
	@echo "  make ui-build          - Compile production frontend SPA into frontend/dist"
	@echo "  make demo              - Launch full end-to-end master demonstration"
	@echo "  make demo-terminal     - Launch terminal-only pipeline demo (no browser needed)"
	@echo "  make test              - Run fast unit tests"
	@echo "  make test-integration  - Run end-to-end integration tests (needs Kafka/HDFS/Hive)"
	@echo "  make verify            - Run unit tests and documentation consistency checks"

test:
	$(PYTHON) -m pytest tests/test_real_pipeline.py tests/test_dashboard_upgrade.py -v

test-integration:
	$(PYTHON) -m pytest tests/test_integration.py -v

verify: test
	$(PYTHON) scripts/check_doc_consistency.py

ui-build:
	cd frontend && npm run build

build-frontend: ui-build

data:
	$(PYTHON) data/build_real_corpus.py

train:
	$(PYTHON) ml/train_models_real.py

stream:
	$(PYTHON) streaming/spark_stream_scorer.py

simulate:
	$(PYTHON) ingestion/replay_simulator.py

api:
	$(PYTHON) -m uvicorn backend.main:app --host 0.0.0.0 --port 8000

ui:
	cd frontend && npm run dev

report:
	$(PYTHON) scripts/build_report_numbers.py

demo:
	$(PYTHON) scripts/run_demo.py

demo-terminal:
	$(PYTHON) scripts/demo_terminal.py
