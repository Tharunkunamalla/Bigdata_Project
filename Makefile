# ==============================================================================
# CSE412 Big Data Project: Makefile (v1.0-final-9 Cleaned)
# Real-Time Detection of Synthetic Misinformation & Intelligent Resource Allocation
# ==============================================================================

SHELL := /bin/bash
PYTHON := .venv/bin/python
ifeq ($(OS),Windows_NT)
	PYTHON := .venv/Scripts/python.exe
endif

COMPOSE_FILE := docker/docker-compose.yml

.PHONY: help setup up down test test-integration verify stream simulate api ui ui-build demo demo-ui

help:
	@echo "CrisisGuard AI (v1.0-final-9) Operational Commands:"
	@echo "  make setup             - Install Python dependencies and build frontend SPA"
	@echo "  make up                - Start Docker big data cluster services (Kafka, HDFS, Hive)"
	@echo "  make down              - Tear down Docker big data cluster services"
	@echo "  make test              - Run fast unit & logic tests"
	@echo "  make test-integration  - Run end-to-end integration tests (Kafka/HDFS/Hive)"
	@echo "  make verify            - Run test suite and documentation consistency audit"
	@echo "  make stream            - Launch Spark Structured Streaming real-time scorer"
	@echo "  make simulate          - Launch Kafka live replay stream simulator"
	@echo "  make api               - Start FastAPI operations gateway (:8000)"
	@echo "  make ui                - Start Vite development server (:3000)"
	@echo "  make ui-build          - Compile production frontend SPA into frontend/dist"
	@echo "  make demo              - Launch full terminal pipeline demonstration"
	@echo "  make demo-ui           - Launch FastAPI gateway and serve dashboard at http://127.0.0.1:8000/"

setup:
	$(PYTHON) -m pip install -r requirements.txt
	cd frontend && npm install && npm run build

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

test:
	$(PYTHON) -m pytest tests/test_real_pipeline.py tests/test_dashboard_upgrade.py -v

test-integration:
	$(PYTHON) -m pytest tests/test_integration.py -v

verify: test
	$(PYTHON) scripts/check_doc_consistency.py

stream:
	$(PYTHON) streaming/spark_stream_scorer.py

simulate:
	$(PYTHON) ingestion/replay_simulator.py

api:
	$(PYTHON) -m uvicorn backend.main:app --host 0.0.0.0 --port 8000

ui:
	cd frontend && npm run dev

ui-build:
	cd frontend && npm run build

demo:
	bash scripts/demo_pipeline.sh

demo-ui: api
