# CrisisGuard AI: Cluster Setup & Operational Environment Guide

## 1. Prerequisites and Pinned Versions

| Technology | Pinned Version | Execution Role | Port / Socket |
|:---|:---|:---|:---|
| **OpenJDK** | 11.0.24 LTS | Java Runtime for Kafka, Hadoop, Hive, Spark | N/A |
| **Apache Kafka** | 3.7.0 (KRaft mode) | Distributed Stream Ingestion Broker | `127.0.0.1:9092` (Plaintext), `9093` (Controller) |
| **Apache Hadoop HDFS** | 3.3.6 | Distributed File System (NameNode + DataNode) | `127.0.0.1:9000` (IPC), `127.0.0.1:9870` (WebUI) |
| **Apache Hive** | 3.1.3 | Relational Metastore Catalog (`cse412_disaster`) | `127.0.0.1:9083` (Thrift) |
| **Apache Spark** | 3.5.1 | Distributed Processing, MLlib, Structured Streaming | Driver `local[2]` / `local[*]` |
| **PuLP** | 2.9.0 | Mixed-Integer Linear Programming Dispatch Solver | Embedded Driver |
| **Python** | 3.11 / 3.12 | PySpark, FastAPI, Data Science Runtime | `127.0.0.1:8000` (FastAPI + Vite UI) |

---

## 2. Memory Profiles & Resource Footprint

To ensure stable multi-daemon operation within physical RAM limits (16GB physical host, ~6GB WSL2 allocation):

### Lite Native WSL2 Profile (Recommended — <1.8 GB RAM Total):
- **Kafka Heap:** `-Xms256m -Xmx512m` (KRaft without Zookeeper saves ~400MB)
- **HDFS NameNode & DataNode:** `-Xms256m -Xmx512m`
- **Hive Metastore:** `-Xms128m -Xmx384m` (Embedded Derby backend saves Postgres container overhead)
- **Spark Local Driver:** `spark.driver.memory=1g`, `spark.sql.shuffle.partitions=4`
- **Total Combined JVM Memory:** ~1.8 GB peak.

---

## 3. Quickstart: Starting and Verifying the Cluster

```bash
# 1. Idempotently configure and enable systemd units
bash scripts/setup_wsl.sh

# 2. Start all daemons and verify listening ports
bash scripts/start_all.sh

# 3. Verify infrastructure health across all tools
python3 tests/verify_infra.py
```

To stop all services cleanly:
```bash
bash scripts/stop_all.sh
```
