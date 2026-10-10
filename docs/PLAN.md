# IMPLEMENTATION PLAN: Real-Time Detection of Synthetic Misinformation and Intelligent Emergency Resource Allocation

**Course:** CSE412 — Big Data & Large-Scale Computing  
**Project Track:** Hybrid Tracks D & E (Streaming MLlib + Distributed Storage & Hive Analytics)  
**Status:** In Progress — Phase 1 Complete, Transitioning to Phase 2 (Infrastructure)

---

## 1. System Architecture & Data Flow

```
[Simulated / Live Post Ingestion]       [Live Hazard Feeds: USGS / NWS]
                 │                                      │
                 ▼                                      ▼
       Kafka Topic: `raw_posts`              Kafka Topic: `hazard_events`
                 │                                      │
                 └──────────────┬───────────────────────┘
                                ▼
         ┌──────────────────────────────────────────────┐
         │         Spark Structured Streaming           │
         │  - Text Normalization & Stylometrics         │
         │  - Spark MLlib Synthetic Detection (p_synth) │
         │  - Spark MLlib Humanitarian Classification   │
         │  - Spark MLlib Urgency/Severity Scoring      │
         │  - Geospatial Cell Assignment (H3/Geohash)   │
         └──────────────────────┬───────────────────────┘
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
       Kafka: `scored_posts`          HDFS Landing Zone (Parquet)
                 │                             │
                 │                 ┌───────────┴───────────┐
                 │                 │  Bronze / Silver Hive │
                 │                 │  Gold Data Marts      │
                 │                 └───────────┬───────────┘
                 ▼                             ▼
   ┌───────────────────────────┐  ┌───────────────────────────┐
   │ Emergency Resource Engine │  │ HiveServer2 / Metastore   │
   │  - Trust-Weighted Need    │  └─────────────┬─────────────┘
   │  - Greedy vs LP Optimizer │                │
   └─────────────┬─────────────┘                │
                 ▼                              │
        Kafka: `allocations`                    │
                 │                              │
                 └──────────────┬───────────────┘
                                ▼
                     FastAPI Backend (SSE / WS)
                                │
                                ▼
                     React / Vite Dashboard
         (Live Ops, Analytics, Allocation, Cluster Health)
```

---

## 2. Hardware Profile & Pinned Versions

### Detected Host Environment
- **OS:** Windows 11 Home 64-bit with WSL2 Ubuntu 24.04
- **Physical RAM:** 16 GB Total (~2.1 GB free host RAM, ~6.2 GB available in WSL2)
- **Profile Decision:** **`LITE PROFILE` default** to guarantee rock-solid JVM stability without swapping or Docker OOM kills. Full profile available via `.env` toggle.
- **CPU:** AMD multi-core (16 logical processors)
- **GPU:** NVIDIA GeForce RTX 2050 (4 GB VRAM)
- **Disk:** 176.5 GB free disk space
- **Runtime Stack:**
  - Python: 3.11.15 in `.venv` (via `uv`)
  - Java: OpenJDK 17.0.19 LTS
  - Node.js: v26.3.1 (npm 11.16.0)
  - Docker Daemon: Docker v29.1.3 / v29.5.3 (WSL2 bridge on `tcp://localhost:2375`)
  - Build System: GNU Make (WSL) + `make.bat` wrapper for native Windows shell parity

### Pinned Tool Versions (Official & Compatible Matrix)
| Component | Pinned Version | Rationale & Compatibility |
| :--- | :--- | :--- |
| **Apache Kafka** | `3.7.0` (KRaft) | KRaft mode eliminates ZooKeeper overhead and memory footprint |
| **Apache Spark** | `3.5.1` (PySpark + MLlib) | Stable LTS release with Structured Streaming and Parquet v2 support |
| **Apache Hadoop HDFS** | `3.3.6` (Namenode + Datanode) | Rock-solid HDFS Parquet block storage with WebHDFS API |
| **Apache Hive** | `3.1.3` / `4.0.0` (Metastore + HS2) | Postgres-backed metastore, external partitioned tables on HDFS |
| **FastAPI** | `0.111.0` + Uvicorn | High-throughput asynchronous streaming backend with WebSockets/SSE |
| **React + Vite** | React 18 + Vite 5 + Tailwind 3 | Modern responsive UI, Leaflet for geospatial mapping, Recharts |
