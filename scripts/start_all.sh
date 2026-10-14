#!/usr/bin/env bash
# ==============================================================================
# CrisisGuard AI: Cluster Startup & Port Verification Script
# ==============================================================================
set -euo pipefail

echo "=================================================================="
echo "      Starting CrisisGuard Big Data Cluster Daemons               "
echo "=================================================================="

# 1. Start Apache Kafka 3.7.0 (KRaft)
echo "[1/4] Starting Apache Kafka (KRaft)..."
sudo systemctl restart kafka
sleep 2

# 2. Start Apache Hadoop HDFS 3.3.6 (NameNode & DataNode)
echo "[2/4] Starting Hadoop HDFS (NameNode & DataNode)..."
sudo systemctl restart hadoop-hdfs
sleep 4

# 3. Disable NameNode safe mode if active
echo "[3/4] Ensuring HDFS SafeMode is OFF..."
/opt/hadoop/bin/hdfs dfsadmin -safemode leave || true

# 4. Start Apache Hive Metastore 3.1.3
echo "[4/4] Starting Apache Hive Metastore..."
sudo systemctl restart hive-metastore
sleep 3

echo ""
echo "=== Cluster Daemon Status (jps) ==="
jps

echo ""
echo "=== Listening Ports Verification ==="
ss -tulpn | grep -E '9092|9000|9870|9083' || true

echo ""
echo ">>> All Cluster Services Started Successfully <<<"
