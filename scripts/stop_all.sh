#!/usr/bin/env bash
# ==============================================================================
# CrisisGuard AI: Cluster Shutdown Script
# ==============================================================================
set -euo pipefail

echo "=================================================================="
echo "      Stopping CrisisGuard Big Data Cluster Daemons               "
echo "=================================================================="

echo "[1/3] Stopping Apache Hive Metastore..."
sudo systemctl stop hive-metastore || true

echo "[2/3] Stopping Apache Hadoop HDFS..."
sudo systemctl stop hadoop-hdfs || true

echo "[3/3] Stopping Apache Kafka..."
sudo systemctl stop kafka || true

echo ""
echo "=== Remaining Java Processes ==="
jps || true

echo ">>> All Cluster Services Stopped Cleanly <<<"
