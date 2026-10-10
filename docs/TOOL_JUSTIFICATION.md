# CSE412 Big Data Project: Big Data Tool Justifications & Architecture Trade-Offs

## 1. Overview of Technology Stack
The course requires a minimum of three big data tools with real data flowing between them. Our pipeline integrates **five** enterprise big data technologies:

1. **Apache Kafka 3.7.0** (Distributed Messaging & Stream Ingestion)
2. **Apache Hadoop HDFS 3.3.6** (Fault-Tolerant Distributed Storage)
3. **Apache Hive Metastore 3.1.3** (Relational Data Catalog & Warehousing)
4. **Apache Spark 3.5.1 (Core, SQL, Structured Streaming, MLlib)** (Distributed Compute & Analytics)
5. **PuLP 2.9.0 MILP Solver** (Combinatorial Mathematical Optimization Engine)

---

## 2. In-Depth Tool Justifications

### 2.1 Apache Kafka 3.7.0 (KRaft)
- **Role in Pipeline:** Ingests high-velocity social media crisis posts arriving at uneven rates; buffers bursts during peak disaster landfall; decouples producer ingestion from streaming ML scoring consumers.
- **Why Not REST/Polling or WebSockets?**
  - **Buffering & Backpressure:** Social media volume surges by orders of magnitude during emergencies (e.g., dam breach rumors). A standard HTTP endpoint would crash or exhaust memory without distributed queue backpressure.
  - **Replayability & Multi-Consumer Fan-Out:** Kafka allows both the Spark streaming scorer and ad-hoc forensic consumers to replay messages from earlier offsets without burdening the ingestion simulator.
- **KRaft Mode Advantage:** Operating Kafka in KRaft mode eliminates the legacy Apache ZooKeeper daemon, reducing memory footprint by ~400MB and eliminating JVM coordination deadlocks.

### 2.2 Apache Hadoop HDFS 3.3.6
- **Role in Pipeline:** Serves as the primary scalable data lake for the Bronze raw archive, Silver partitioned Parquet storage, and Structured Streaming checkpoint storage.
- **Why Not Local Filesystem or AWS S3?**
  - **Write-Ahead Logging (WAL) for Stream Checkpoints:** Structured Streaming requires atomic rename and write-ahead log operations to guarantee exactly-once processing across worker restarts. HDFS POSIX compliance provides atomic consistency that S3's eventual consistency cannot match without S3Guard.
  - **Block Partitioning & Data Locality:** HDFS splits datasets across 128MB blocks with replication, ensuring fault tolerance against disk failures during disaster response coordination.

### 2.3 Apache Hive Metastore 3.1.3
- **Role in Pipeline:** Maintains the central schema catalog for the `cse412_disaster` database and partitioned external tables over HDFS Parquet files.
- **Why Hive Over Plain Parquet Folders?**
  - **Partition Pruning:** Hive partition discovery (`MSCK REPAIR TABLE`) enables analytical engines to query `WHERE event_type = 'hurricane'` by scanning only the targeted HDFS subdirectories, saving I/O and CPU.
  - **Multi-Engine Interoperability:** External Hive tables allow any downstream SQL query engine (Presto, Trino, Impala, Spark SQL) to query the exact same data lake without schema drift or manual DDL re-specification.

### 2.4 Apache Spark 3.5.1 (SQL, Streaming, MLlib)
- **Role in Pipeline:** Serves as the unified distributed compute engine for batch medallion transformations, out-of-distribution ML model training, and continuous real-time stream scoring.
- **Why Spark Over Pandas / Scikit-Learn?**
  - **Unified Stream-Batch Paradigm:** Spark enables using the exact same MLlib `PipelineModel` trained offline on historical disaster data directly inside `spark.readStream` micro-batches.
  - **Horizontal Scalability:** As shown in our empirical scaling benchmark (`results/scaling_benchmark.py`), Pandas memory usage scales as $O(N)$ on a single thread and crashes when datasets exceed host RAM. Spark distributes computations across JVM executor cores and handles terabyte-scale datasets seamlessly.

### 2.5 PuLP 2.9.0 MILP Optimization Engine
- **Role in Pipeline:** Formulates and solves the fleet dispatch problem as a Mixed-Integer Linear Program (MILP) to assign rescue boats and ambulances to genuine demand hotspots.
- **Why Not Pure Heuristics?**
  - **Global Optimality:** Greedy nearest-neighbor heuristics make short-sighted allocations that exhaust depot capacity early on low-priority or false calls. The MILP formulation guarantees globally optimal asset distribution respecting fleet limits, geographic constraints, and trust discounting.
