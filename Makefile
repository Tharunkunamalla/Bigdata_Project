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

.PHONY: help up down reset verify-infra data train stream simulate api ui test verify report demo

help:
	@echo "Available commands:"
	@echo "  make up            - Launch Big Data cluster (Kafka, HDFS, Hive, Spark)"
	@echo "  make down          - Stop Big Data cluster"
	@echo "  make reset         - Tear down cluster and wipe persistent volumes"
	@echo "  make verify-infra  - Verify cluster connectivity and I/O across all tools"
	@echo "  make data          - Download and prepare disaster & synthetic datasets"
	@echo "  make train         - Train and evaluate Spark MLlib classification pipelines"
	@echo "  make stream        - Launch Spark Structured Streaming scoring job"
	@echo "  make simulate      - Run the real-time social & hazard replay simulator"
	@echo "  make api           - Start the FastAPI backend server"
	@echo "  make ui            - Start the React/Vite dashboard"
	@echo "  make test          - Run the test suite"
	@echo "  make verify        - Run end-to-end validation against rubric criteria"
	@echo "  make report        - Generate evaluation metrics, ablation tables & plots"
	@echo "  make demo          - Launch full end-to-end demo"

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

reset:
	docker compose -f $(COMPOSE_FILE) down -v --remove-orphans

verify-infra:
	$(PYTHON) tests/verify_infra.py

data:
	$(PYTHON) data/download_datasets.py

train:
	$(PYTHON) ml/train_models.py

stream:
	$(PYTHON) streaming/spark_stream_scorer.py

simulate:
	$(PYTHON) ingestion/replay_simulator.py

api:
	$(PYTHON) -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

ui:
	cd frontend && npm run dev

test:
	$(PYTHON) -m pytest tests/ -v

verify: verify-infra test
	$(PYTHON) tests/verify_pipeline.py

report:
	$(PYTHON) allocation/run_ablation.py
	$(PYTHON) results/generate_plots.py

demo:
	$(PYTHON) scripts/run_demo.py
