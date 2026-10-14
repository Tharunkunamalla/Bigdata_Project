import os
import shutil
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.master("local[2]").appName("ParquetExport").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    curated_dir = os.path.join("data", "curated")
    for name in ["train_real", "test_real", "real_disaster_corpus"]:
        csv_file = os.path.join(curated_dir, f"{name}.csv")
        pq_dir = os.path.join(curated_dir, f"{name}.parquet")
        if os.path.exists(pq_dir):
            if os.path.isdir(pq_dir):
                shutil.rmtree(pq_dir)
            else:
                os.remove(pq_dir)
        print(f"Reading {csv_file}...")
        df = spark.read.csv(csv_file, header=True, inferSchema=True, escape="\"", multiLine=True)
        df.write.parquet(pq_dir)
        print(f"  Saved {pq_dir} ({df.count()} rows)")
    spark.stop()
    print("All Parquet exports complete!")

if __name__ == "__main__":
    main()
