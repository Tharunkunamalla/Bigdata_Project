"""
CSE412 Big Data Project: Crisis Stream Replay Simulator
========================================================
Replays raw crisis posts to Apache Kafka topic `disaster_raw_posts` to simulate
a high-throughput real-time stream of incoming emergency communications.

Features:
- Configurable emission rate (posts per second)
- Deterministic playback ordering
- Real-time Kafka producer with JSON serialization
- Automatic Kafka topic creation
"""

import os
import sys
import time
import json
import argparse
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
DEFAULT_TOPIC = "disaster_raw_posts"
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw", "disaster_posts_raw.json")

def ensure_topic_exists(broker: str, topic_name: str, partitions: int = 4, replication: int = 1):
    try:
        admin = KafkaAdminClient(bootstrap_servers=broker)
        existing = admin.list_topics()
        if topic_name not in existing:
            print(f"Creating Kafka topic '{topic_name}' ({partitions} partitions, rep {replication})...")
            admin.create_topics([NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication)])
        admin.close()
    except Exception as e:
        print(f"Note on topic check: {e}")

def run_simulator(rate_per_sec: float = 50.0, max_posts: int = 5000, loop: bool = False, topic: str = DEFAULT_TOPIC):
    print("==================================================================")
    print("      CSE412 Kafka Stream Replay Simulator                        ")
    print("==================================================================")
    print(f"Broker       : {KAFKA_BROKER}")
    print(f"Target Topic : {topic}")
    print(f"Rate         : {rate_per_sec} posts/sec")
    print(f"Max Posts    : {max_posts if max_posts > 0 else 'Unlimited'}")
    
    ensure_topic_exists(KAFKA_BROKER, topic)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1
    )

    if not os.path.exists(DATA_PATH):
        print(f"Error: dataset file not found at {DATA_PATH}")
        sys.exit(1)

    print(f"Reading records from {DATA_PATH}...")
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f]

    print(f"Loaded {len(records):,} records for replay.")
    
    interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.01
    sent_count = 0
    start_time = time.time()

    while True:
        for rec in records:
            # Stamp with current real-time replay timestamp
            payload = dict(rec)
            payload["stream_timestamp"] = time.time()
            
            key = payload.get("post_id", f"msg_{sent_count}")
            producer.send(topic, key=key, value=payload)
            sent_count += 1

            if sent_count % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = sent_count / max(0.001, elapsed)
                print(f"  [Replay] Streamed {sent_count:,} posts | Actual throughput: {actual_rate:.1f} posts/sec")

            if max_posts > 0 and sent_count >= max_posts:
                break
                
            time.sleep(interval)

        if not loop or (max_posts > 0 and sent_count >= max_posts):
            break

    producer.flush()
    producer.close()
    elapsed = time.time() - start_time
    print(f"\n[DONE] Streamed {sent_count:,} posts in {elapsed:.2f}s ({sent_count/max(0.001, elapsed):.1f} posts/sec).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSE412 Crisis Stream Replay Simulator")
    parser.add_argument("--rate", type=float, default=50.0, help="Replay rate in posts per second")
    parser.add_argument("--max-posts", type=int, default=5000, help="Maximum posts to emit (0 for all)")
    parser.add_argument("--loop", action="store_true", help="Loop indefinitely")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Kafka target topic")
    args = parser.parse_args()

    run_simulator(
        rate_per_sec=args.rate,
        max_posts=args.max_posts,
        loop=args.loop,
        topic=args.topic
    )
