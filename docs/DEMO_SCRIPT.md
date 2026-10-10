# Evaluator Demo Script: CrisisGuard Big Data Pipeline

**Execution Time:** $\le 8$ minutes  
**Target Audience:** CSE412 Course Instructors, Teaching Assistants, Evaluators  
**System Prerequisites:** Windows 11 with WSL2 (or Linux), Python 3.11, Node.js v18+, Java 11/17  

---

## Step-by-Step Evaluator Walkthrough

### Step 1: Clone and Start the Cluster (1 minute)
Ensure cluster daemons are active:
```bash
# Start cluster services in WSL2 (Kafka, HDFS, Hive Metastore)
sudo systemctl start kafka hadoop-hdfs hive-metastore
```

### Step 2: Run Automated Infrastructure Verification (30 seconds)
```bash
wsl -e bash -c "cd '/mnt/c/Users/HP/OneDrive/Desktop/bigdata project' && python3 tests/verify_infra.py"
```
**Expected Output:**
```
[PASS] Apache Kafka 3.7     : Produced & consumed 1 message on topic 'infra_test'
[PASS] Hadoop HDFS 3.3.6    : NameNode active
[PASS] Apache Hive Metastore : Metastore Thrift API listening on port 9083
[PASS] Apache Spark & MLlib : Spark session & MLlib Pipeline fitted and scored
>>> ALL INFRASTRUCTURE GATES PASSED <<<
```

### Step 3: Run Full Streaming Fault Recovery Audit (1 minute)
```bash
wsl -e bash -c "cd '/mnt/c/Users/HP/OneDrive/Desktop/bigdata project' && python3 tests/test_stream_recovery.py"
```
**Expected Output:**
```
Publishing 200 sequential test messages...
Simulating streaming consumption before failure... Killing stream at 200 records.
Restarting streaming engine with preserved checkpoint...
>>> [PASS] STREAM RECOVERY TEST PASSED (Zero data loss: 200/200 records) <<<
```

### Step 4: Run Resource Allocation Ablation Experiment (1 minute)
```bash
wsl -e bash -c "cd '/mnt/c/Users/HP/OneDrive/Desktop/bigdata project' && python3 allocation/run_ablation.py"
```
**Expected Output:**
```
[Ablation Key Result] Trust-Aware Optimization reduced wasted dispatches by 97.29%
[Ablation Key Result] Genuine emergency victims reached increased by +38.71%
```

### Step 5: Run Scaling Benchmark (1 minute)
```bash
wsl -e bash -c "cd '/mnt/c/Users/HP/OneDrive/Desktop/bigdata project' && python3 results/scaling_benchmark.py"
```
**Expected Output:**
Displays empirical runtime and peak RAM comparisons between Pandas and Spark DataFrames across 10k, 50k, 100k, and 250k rows.

### Step 6: Launch Full-Stack Operations UI (2 minutes)
Launch the FastAPI backend serving both API and the React Single-Page Application:
```bash
.venv\Scripts\python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
```
Open **`http://127.0.0.1:8000/`** in any web browser:
1. **Live Feed:** View real-time incoming posts arriving via SSE, showing predicted urgency, synthetic probability, and trust score.
2. **Interactive Map:** View Houston disaster zone with green pins (genuine victims), red pins (synthetic hoaxes), and blue base depots.
3. **Fleet Dispatcher:** Click **"Solve Fleet Dispatch Plan"** to run the live MILP solver and review the dispatched boat fleet.
4. **Tabs:** Inspect "Fleet Dispatcher & Ablation", "MLlib Performance", and "Cluster Topology".
