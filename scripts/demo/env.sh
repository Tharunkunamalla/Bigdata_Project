#!/usr/bin/env bash
# ==============================================================================
# scripts/demo/env.sh — CrisisGuard Demo Environment Configuration
# ==============================================================================
export KAFKA_BROKER="127.0.0.1:9092"
export RAW_TOPIC="disaster_raw_posts"
export SCORED_TOPIC="disaster_scored_posts"
export HDFS_NAMENODE="hdfs://127.0.0.1:9000"
export HDFS_SILVER_PATH="/disaster_data/silver_real"
export HDFS_SCORED_PATH="/disaster_data/streaming_sink"
export HIVE_METASTORE_URI="thrift://127.0.0.1:9083"
export API_URL="http://127.0.0.1:8000"
# Resolve Python executable (prefer workspace .venv)
if [ -n "$ROOT_DIR" ] && [ -f "$ROOT_DIR/.venv/Scripts/python.exe" ]; then
    PYTHON_CMD="$ROOT_DIR/.venv/Scripts/python.exe"
elif [ -n "$ROOT_DIR" ] && [ -f "$ROOT_DIR/.venv/bin/python" ]; then
    PYTHON_CMD="$ROOT_DIR/.venv/bin/python"
elif [ -f ".venv/Scripts/python.exe" ]; then
    PYTHON_CMD=".venv/Scripts/python.exe"
elif [ -f ".venv/bin/python" ]; then
    PYTHON_CMD=".venv/bin/python"
elif command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    PYTHON_CMD="python"
fi
export PYTHON_CMD

