"""
CSE412 Big Data Project: Large-Scale Processing Benchmark (Pandas vs Spark)
===========================================================================
Performs empirical scaling benchmarks comparing single-node in-memory Pandas
against distributed PySpark DataFrames across varying dataset scales:
10k, 50k, 100k, and 250k records.

Benchmarks 3 core analytical workloads:
1. Ingestion & Schema Parsing
2. Filter & Multi-Condition Selection
3. GroupBy Aggregation & Metric Calculation

Outputs results to `results/scaling_benchmark.json`.
"""

import os
import sys
import time
import json
import tracemalloc
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RESULTS_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(RESULTS_DIR, exist_ok=True)

def generate_synthetic_benchmark_data(n_rows: int) -> pd.DataFrame:
    """Generates synthetic benchmark DataFrame."""
    events = ["hurricane", "earthquake", "flood", "wildfire"]
    cats = ["rescue_needed", "medical_emergency", "infrastructure_damage", "shelter_supplies", "caution_and_advice"]
    
    return pd.DataFrame({
        "post_id": [f"bench_{i:07d}" for i in range(n_rows)],
        "event_type": np.random.choice(events, size=n_rows),
        "humanitarian_category": np.random.choice(cats, size=n_rows),
        "urgency_level": np.random.uniform(0.1, 1.0, size=n_rows),
        "is_synthetic": np.random.choice([0, 1], p=[0.8, 0.2], size=n_rows),
        "retweet_count": np.random.poisson(25, size=n_rows),
        "user_account_age_days": np.random.randint(1, 3000, size=n_rows)
    })

def benchmark_pandas(df_raw: pd.DataFrame):
    tracemalloc.start()
    t0 = time.time()
    
    # Workload 1: Filter
    df_filtered = df_raw[(df_raw["urgency_level"] > 0.7) & (df_raw["is_synthetic"] == 0)]
    
    # Workload 2: GroupBy Aggregation
    agg_res = df_raw.groupby(["event_type", "humanitarian_category"]).agg({
        "urgency_level": ["count", "mean"],
        "retweet_count": "sum"
    })
    
    # Workload 3: Complex metric calculation
    df_raw["composite_priority"] = df_raw["urgency_level"] * (df_raw["user_account_age_days"] / 365.0)
    
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    return {
        "execution_time_sec": round(elapsed, 4),
        "peak_memory_mb": round(peak / (1024 * 1024), 2)
    }

def benchmark_spark(spark: SparkSession, df_pandas: pd.DataFrame):
    t0 = time.time()
    
    # Create Spark DataFrame
    sdf = spark.createDataFrame(df_pandas)
    
    # Workload 1: Filter
    df_filtered = sdf.filter((F.col("urgency_level") > 0.7) & (F.col("is_synthetic") == 0))
    _ = df_filtered.count()
    
    # Workload 2: GroupBy Aggregation
    agg_res = (
        sdf.groupBy("event_type", "humanitarian_category")
        .agg(
            F.count("urgency_level").alias("cnt"),
            F.avg("urgency_level").alias("mean_urg"),
            F.sum("retweet_count").alias("sum_rt")
        )
    )
    _ = agg_res.collect()
    
    # Workload 3: Composite priority calculation
    sdf_enriched = sdf.withColumn("composite_priority", F.col("urgency_level") * (F.col("user_account_age_days") / 365.0))
    _ = sdf_enriched.select(F.avg("composite_priority")).collect()
    
    elapsed = time.time() - t0
    return {
        "execution_time_sec": round(elapsed, 4)
    }

def run_benchmarks():
    print("==================================================================")
    print("      CSE412 Scaling Benchmark: Pandas vs Apache Spark DataFrames ")
    print("==================================================================")
    
    spark = (
        SparkSession.builder
        .appName("CSE412_Scaling_Benchmark")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    scales = [10000, 50000, 100000, 250000]
    results = {}

    print(f"{'Scale (Rows)':<14} | {'Pandas (sec)':<14} | {'Spark (sec)':<14} | {'Pandas Peak RAM (MB)':<20}")
    print("-------------------------------------------------------------------------")

    for n in scales:
        df_bench = generate_synthetic_benchmark_data(n)
        
        res_pandas = benchmark_pandas(df_bench)
        res_spark = benchmark_spark(spark, df_bench)
        
        results[str(n)] = {
            "rows": n,
            "pandas_time_sec": res_pandas["execution_time_sec"],
            "pandas_peak_ram_mb": res_pandas["peak_memory_mb"],
            "spark_time_sec": res_spark["execution_time_sec"]
        }
        
        print(f"{n:<14,d} | {res_pandas['execution_time_sec']:<14.4f} | {res_spark['execution_time_sec']:<14.4f} | {res_pandas['peak_memory_mb']:<20.2f}")

    spark.stop()

    out_file = os.path.join(RESULTS_DIR, "scaling_benchmark.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("-------------------------------------------------------------------------")
    print(f"Benchmark results written to: {out_file}")
    print("==================================================================")

if __name__ == "__main__":
    run_benchmarks()
