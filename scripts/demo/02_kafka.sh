#!/usr/bin/env bash
# ==============================================================================
# Step 2: Kafka topics and end offsets
# ==============================================================================
set -e

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: ./02_kafka.sh"
    echo "Lists topics, describes raw/scored topics, and queries end offsets via GetOffsetShell."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "Step 2: Kafka topics and end offsets"
echo "+ kafka-topics --bootstrap-server $KAFKA_BROKER --list"
echo "+ kafka-topics --bootstrap-server $KAFKA_BROKER --describe --topic $RAW_TOPIC,$SCORED_TOPIC"
echo "+ kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $KAFKA_BROKER --topic $RAW_TOPIC,$SCORED_TOPIC --time -1"

cd "$ROOT_DIR"
"$PYTHON_CMD" -c '
import sys, os
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:9092")
raw_topic = os.environ.get("RAW_TOPIC", "disaster_raw_posts")
scored_topic = os.environ.get("SCORED_TOPIC", "disaster_scored_posts")

try:
    admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=3000)
    topics = admin.list_topics()
    print(f"\n[TOPICS] Found {len(topics)} topics on {broker}:")
    for t in sorted(topics):
        print(f"  - {t}")
    admin.close()

    consumer = KafkaConsumer(bootstrap_servers=broker, request_timeout_ms=3000)
    print("\n[PARTITIONS & OFFSETS (GetOffsetShell equivalent)]:")
    for top in [raw_topic, scored_topic]:
        parts = consumer.partitions_for_topic(top)
        if parts:
            tps = [TopicPartition(top, p) for p in parts]
            end_offsets = consumer.end_offsets(tps)
            for tp, off in sorted(end_offsets.items(), key=lambda x: x[0].partition):
                print(f"  {tp.topic}:{tp.partition}:{off}")
        else:
            print(f"  {top}: (no active partitions)")
    consumer.close()
except Exception as e:
    print(f"[ERROR] Failed to query Kafka at {broker}: {e}", file=sys.stderr)
    sys.exit(1)
'
