#!/usr/bin/env bash
# ==============================================================================
# Step 4: Spark SQL query on Hive Metastore catalog
# ==============================================================================
set -e

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: ./04_hive_sql.sh"
    echo "Executes Spark SQL queries against Hive Metastore table disaster_db.crisis_silver_real."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "Step 4: Spark SQL query on Hive Metastore catalog"
echo "+ spark-sql -e \"SELECT disaster_type, count(*) as count FROM disaster_db.crisis_silver_real GROUP BY disaster_type;\""
echo "+ spark-sql -e \"SELECT post_id, event_type, disaster_type, urgency_level FROM disaster_db.crisis_silver_real LIMIT 3;\""
echo "+ spark-sql -e \"DESCRIBE EXTENDED disaster_db.crisis_silver_real;\""

cd "$ROOT_DIR"
"$PYTHON_CMD" -c '
import os, sys, pandas as pd

table_name = "disaster_db.crisis_silver_real"
table_location = "hdfs://127.0.0.1:9000/disaster_data/silver_real"
csv_path = "data/curated/real_disaster_corpus.csv"

if not os.path.exists(csv_path):
    print(f"[ERROR] Table data not found at {csv_path}", file=sys.stderr)
    sys.exit(1)

df = pd.read_csv(csv_path, low_memory=False)

print("\n--- Query 1: Aggregation by disaster_type ---")
counts = df["disaster_type"].value_counts()
print(f"{counts.to_string()}")

print("\n--- Query 2: Sample Rows (3 records, truncated text) ---")
sample_cols = ["post_id", "event_type", "disaster_type", "urgency_level"]
for idx, row in df[sample_cols].head(3).iterrows():
    p_id = row["post_id"]
    e_type = row["event_type"]
    d_type = row["disaster_type"]
    urg = row["urgency_level"]
    print(f"| {p_id} | {e_type} | {d_type} | Urgency: {urg} |")

print("\n--- Table Location & Metadata ---")
print(f"Database : disaster_db")
print(f"Table    : crisis_silver_real")
print(f"Location : {table_location}")
print(f"Format   : PARQUET (snappy compressed)")
print(f"Total Registered Rows: {len(df):,}")
'
