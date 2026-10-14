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

.PHONY: help up down reset verify-infra data train stream simulate api ui test test-integration build-frontend verify report demo

help:
	@echo "Available commands:"
	@echo "  make test              - Run fast unit tests (no cluster needed)"
	@echo "  make test-integration  - Run end-to-end integration tests (needs Kafka/HDFS/Hive)"
	@echo "  make build-frontend    - Build and type-check React/Vite dashboard bundle"
	@echo "  make data              - Build real 99k-record disaster corpus"
	@echo "  make train             - Train Spark MLlib models on real corpus"
	@echo "  make stream            - Launch Spark Structured Streaming scoring job"
	@echo "  make api               - Start FastAPI backend server"
	@echo "  make ui                - Start React/Vite frontend development server"
	@echo "  make demo              - Launch full end-to-end demonstration"

test:
	$(PYTHON) -m pytest tests/test_real_pipeline.py -v

test-integration:
	$(PYTHON) -m pytest tests/test_integration.py -v

build-frontend:
	cd frontend && npm run build

data:
	$(PYTHON) data/build_real_corpus.py

train:
	$(PYTHON) ml/train_models_real.py

stream:
	$(PYTHON) streaming/spark_stream_scorer.py

simulate:
	$(PYTHON) ingestion/replay_simulator.py

api:
	$(PYTHON) -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

ui:
	cd frontend && npm run dev

report:
	$(PYTHON) scripts/build_report_numbers.py

demo:
	$(PYTHON) scripts/run_demo.py
