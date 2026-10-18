# ==============================================================================
# CSE412 Big Data Project: Makefile (v1.0-final-9)
# Real-Time Detection of Synthetic Misinformation & Intelligent Resource Allocation
# ==============================================================================

SHELL := /bin/bash
PYTHON := .venv/bin/python
ifeq ($(OS),Windows_NT)
	PYTHON := .venv/Scripts/python.exe
endif

COMPOSE_FILE := docker/docker-compose.yml

.PHONY: setup up down preflight stream simulate api ui ui-build test test-integration verify demo demo-ready demo-stop

setup:
	$(PYTHON) -m pip install -r requirements.txt
	cd frontend && npm install && npm run build

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

preflight:
	$(PYTHON) scripts/preflight.py

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

test:
	$(PYTHON) -m pytest tests/test_real_pipeline.py tests/test_dashboard_upgrade.py tests/test_ui_cleanliness.py -v
	cd frontend && npm test

test-integration:
	$(PYTHON) -m pytest tests/test_integration.py -v

verify: test
	$(PYTHON) scripts/check_doc_consistency.py

demo:
	$(PYTHON) scripts/demo_terminal.py

demo-ready:
	$(PYTHON) scripts/demo_ready.py

demo-stop:
	$(PYTHON) scripts/demo_stop.py
