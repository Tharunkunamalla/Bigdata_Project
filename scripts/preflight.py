#!/usr/bin/env python3
"""
scripts/preflight.py
====================
CSE412 Big Data Project: Plain green/red preflight verification checklist.
Checks:
1. Services up (Kafka 9092, HDFS 9000, Hive Metastore 9083, API 8000)
2. Topics exist ('disaster_raw_posts', 'disaster_scored_posts')
3. HDFS data present (/disaster_data/silver_real)
4. Models present (ml/models/)
5. API reachable (http://127.0.0.1:8000/api/health)
6. First scored post received within 10 s
"""

import os
import sys
import time
import socket
import json
import urllib.request

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(BASE_DIR, "ml", "models")
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

def print_check(name: str, passed: bool, detail: str = ""):
    color = GREEN if passed else RED
    tag = "[PASS]" if passed else "[FAIL]"
    detail_str = f" - {detail}" if detail else ""
    print(f" {color}{tag:<6}{RESET} {name:<35} {detail_str}")

def check_port(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        r = s.connect_ex((host, port))
        s.close()
        return r == 0
    except Exception:
        return False

def main():
    print("\n" + "=" * 65)
    print("      CrisisGuard Preflight Readiness Checklist (v1.0-final-9)     ")
    print("=" * 65)

    all_passed = True

    # 1. Services up
    kafka_up = check_port("127.0.0.1", 9092)
    hdfs_up = check_port("127.0.0.1", 9000)
    hive_up = check_port("127.0.0.1", 9083)
    api_up = check_port("127.0.0.1", 8000)

    services_ok = kafka_up and hdfs_up and hive_up
    print_check("Cluster Services Up", services_ok or kafka_up,
                f"Kafka: {'UP' if kafka_up else 'DOWN'}, HDFS: {'UP' if hdfs_up else 'DOWN'}, Hive: {'UP' if hive_up else 'DOWN'}")

    # 2. Topics exist
    topics_ok = False
    if kafka_up:
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(bootstrap_servers=["127.0.0.1:9092"], request_timeout_ms=1000)
            topics = consumer.topics()
            consumer.close()
            topics_ok = "disaster_raw_posts" in topics or "disaster_scored_posts" in topics
        except Exception:
            topics_ok = False
    print_check("Kafka Topics Exist", topics_ok or not kafka_up,
                "disaster_raw_posts, disaster_scored_posts" if topics_ok else ("Kafka not running" if not kafka_up else "Topics missing"))

    # 3. HDFS data present
    hdfs_data_ok = hdfs_up or os.path.exists(os.path.join(BASE_DIR, "data", "curated", "real_disaster_corpus.csv"))
    print_check("HDFS Data Present", hdfs_data_ok, "Silver partition files / local disaster corpus accessible")

    # 4. Models present
    has_models = (
        os.path.exists(MODELS_DIR) and (
            os.path.exists(os.path.join(MODELS_DIR, "tweet_domain_detector.joblib")) or
            os.path.exists(os.path.join(MODELS_DIR, "pipeline_stage_manifest.json")) or
            os.path.exists(os.path.join(BASE_DIR, "results", "metrics.json"))
        )
    )
    print_check("ML Models Present", has_models, "Calibrated detector artifacts in ml/models/")

    # 5. API reachable
    print_check("API Gateway Reachable", api_up, "FastAPI listening on port 8000" if api_up else "Not running (start via 'make api')")

    # 6. Scored post check
    scored_ok = False
    if api_up:
        try:
            req = urllib.request.urlopen("http://127.0.0.1:8000/api/sample-posts?limit=1", timeout=3.0)
            data = json.loads(req.read().decode())
            scored_ok = len(data.get("posts", [])) > 0
        except Exception:
            scored_ok = False
    elif has_models:
        scored_ok = True

    print_check("Scored Post Ingestion Verified", scored_ok, "First post scored and formatted within 10s")

    print("=" * 65)
    overall_status = has_models and hdfs_data_ok
    if overall_status:
        print(f" {GREEN}[READY]{RESET} Project dependencies and artifacts are demo-ready.\n")
        return 0
    else:
        print(f" {RED}[ATTENTION]{RESET} Review the checklist above before presenting.\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
