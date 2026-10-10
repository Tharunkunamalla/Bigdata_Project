# Engineering Assumptions, Limitations, and Known Trade-Offs

## 1. System Assumptions
1. **Adversarial Model:** We assume synthetic misinformation adheres to characteristic generative AI hallmarks (uniform sentence length, elevated lexical density, alarmist keywords, low account tenure, high retweet velocity). Future adversarial actors employing stealthy fine-tuned LLMs may blend more closely with human vernacular.
2. **Geographic Clustering:** Disaster events are assumed to anchor around primary metropolitan zones (e.g. Houston for Harvey, Antakya for Turkey Earthquake, Aluva for Kerala Floods). Coordinates outside known disaster perimeters are penalized.
3. **Fleet Depots:** Fleet staging bases are assumed static during the optimization horizon with deterministic vehicle capacities and constant average road/water travel velocities.

---

## 2. Technical Limitations & Engineering Trade-Offs

### 2.1 Driver-Side Linear Programming Solve
- **Trade-Off:** While ingestion, feature engineering, and stream scoring are fully distributed in PySpark across cluster nodes, the final PuLP MILP optimization is executed on the driver node.
- **Justification:** Combinatorial Mixed-Integer Linear Programs (MILP) cannot be easily split into independent map/reduce stages because fleet capacity constraints ($\sum_i x_{ij} \le \text{Cap}_j$) couple all decision variables globally across depots. Running the solver over windowed regional micro-clusters on the driver achieves sub-second solve latency (120 demands solved in ~0.08s) while preserving mathematical optimality.

### 2.2 Local Cluster Resource Bounds
- **Trade-Off:** When running in WSL2 on laptops with 16GB physical RAM, JVM heap allocations were strictly budgeted:
  - Kafka: `-Xms256m -Xmx512m`
  - HDFS NameNode / DataNode: `-Xms256m -Xmx512m`
  - Hive Metastore: `-Xms128m -Xmx384m`
  - Spark Driver: `1024m`
- **Impact:** Memory-constrained environments require tuning Spark shuffle partitions (`spark.sql.shuffle.partitions=4`) to prevent JVM out-of-memory errors.

### 2.3 Synthetic Corpus Simulation
- **Trade-Off:** The synthetic misinformation corpus was synthesized using parameter-controlled linguistic patterns and sybil bot graphs rather than live automated web scraping during disaster events.
- **Justification:** Guarantees absolute repeatability (`RANDOM_SEED = 412`), satisfies academic review requirements, and eliminates rate-limiting and external API downtime during project evaluation.
