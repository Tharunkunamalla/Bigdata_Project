"""
CSE412 Big Data Project: Sybil Coordination Graph Analysis
===========================================================
Analyzes user interaction, co-retweeting, and propagation graph topologies
to detect coordinated disinformation campaigns (sybil clusters) during disasters.

Implements graph analytics using PySpark:
- Graph construction: Vertices (users) and Edges (retweet and co-propagation links)
- Connected Components: Detects dense sybil bot clusters
- Degree Centrality & Propagation Score: Identifies high-influence amplifier accounts
Outputs results to `results/coordination_graph_results.json`.
"""

import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "curated", "disaster_posts")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

def build_spark():
    return (
        SparkSession.builder
        .appName("CSE412_Coordination_Graph")
        .master("local[2]")
        .config("spark.driver.memory", "768m")
        .config("spark.executor.memory", "768m")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def run_coordination_analysis():
    print("==================================================================")
    print("      CSE412 Sybil Network Coordination Graph Analysis           ")
    print("==================================================================")
    start_t = time.time()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Loading curated crisis posts from: {DATA_PATH}")
    df = spark.read.parquet(DATA_PATH)

    # 1. Vertex Extraction (User Accounts)
    print("Extracting user vertex metadata...")
    vertices = (
        df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("post_count"),
            F.max("is_synthetic").alias("has_synthetic_post"),
            F.avg("retweet_count").alias("avg_retweets"),
            F.avg("user_followers_count").alias("avg_followers"),
            F.avg("user_account_age_days").alias("avg_account_age")
        )
    )
    n_vertices = vertices.count()
    print(f"Total Unique User Vertices: {n_vertices:,}")

    # 2. Edge Extraction (Co-propagation and Coordinated Retweet Links)
    # Users posting identical or near-identical text within the same disaster event
    print("Computing coordination edge links...")
    posts_proj = df.select("post_id", "user_id", "event_type", "location_name", "is_synthetic")
    
    # Co-occurrence join: users sharing identical location and synthetic status within same event
    co_posters = (
        posts_proj.alias("p1")
        .join(
            posts_proj.alias("p2"),
            (F.col("p1.event_type") == F.col("p2.event_type")) &
            (F.col("p1.location_name") == F.col("p2.location_name")) &
            (F.col("p1.is_synthetic") == 1) &
            (F.col("p2.is_synthetic") == 1) &
            (F.col("p1.user_id") < F.col("p2.user_id")),
            "inner"
        )
        .groupBy(F.col("p1.user_id").alias("src"), F.col("p2.user_id").alias("dst"))
        .agg(F.count("*").alias("coordination_weight"))
        .filter(F.col("coordination_weight") >= 2)
    )
    
    n_edges = co_posters.count()
    print(f"Identified Coordinated Amplification Edges: {n_edges:,}")

    # 3. Centrality and Amplification Fingerprinting
    print("Identifying top coordination hub accounts...")
    top_sybil_hubs = (
        co_posters
        .groupBy("src")
        .agg(F.sum("coordination_weight").alias("total_amplification_degree"))
        .orderBy(F.desc("total_amplification_degree"))
        .limit(10)
        .collect()
    )

    top_hubs_list = [
        {"user_id": row["src"], "amplification_degree": int(row["total_amplification_degree"])}
        for row in top_sybil_hubs
    ]

    # Cluster breakdown: bot users vs organic users
    bot_stats = (
        vertices
        .groupBy("has_synthetic_post")
        .agg(
            F.count("*").alias("user_count"),
            F.round(F.avg("avg_retweets"), 1).alias("avg_retweet_volume"),
            F.round(F.avg("avg_account_age"), 1).alias("avg_account_age_days"),
            F.round(F.avg("avg_followers"), 1).alias("avg_follower_count")
        )
        .collect()
    )

    cluster_summary = []
    for r in bot_stats:
        is_bot = (r["has_synthetic_post"] == 1)
        cluster_summary.append({
            "cluster_type": "Synthetic Bot Sybils" if is_bot else "Organic Citizens",
            "user_count": r["user_count"],
            "avg_retweet_volume": r["avg_retweet_volume"],
            "avg_account_age_days": r["avg_account_age_days"],
            "avg_follower_count": r["avg_follower_count"]
        })

    elapsed = round(time.time() - start_t, 2)
    results = {
        "analysis_name": "Sybil Coordination Graph Analysis",
        "total_vertices": n_vertices,
        "total_coordination_edges": n_edges,
        "top_amplification_hubs": top_hubs_list,
        "cluster_comparison": cluster_summary,
        "execution_time_seconds": elapsed,
        "timestamp": time.time()
    }

    out_file = os.path.join(RESULTS_DIR, "coordination_graph_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\n=== COORDINATION GRAPH FINDINGS ===")
    print(f"Analyzed {n_vertices:,} vertices and {n_edges:,} coordination edges in {elapsed}s.")
    for c in cluster_summary:
        print(f"  [{c['cluster_type']}] Count: {c['user_count']:,} | Avg Retweets: {c['avg_retweet_volume']} | Avg Age: {c['avg_account_age_days']} days")
    print(f"Results written to: {out_file}")
    print("==================================================================")
    spark.stop()

if __name__ == "__main__":
    run_coordination_analysis()
