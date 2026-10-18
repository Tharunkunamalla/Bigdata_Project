#!/usr/bin/env bash
# ==============================================================================
# Step 1: cluster services and port verification
# ==============================================================================
set -e

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: ./01_services.sh"
    echo "Runs infrastructure verification and lists ports checked across big data cluster."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "Step 1: cluster services and port verification"
echo "+ Ports checked: Kafka (9092), HDFS NameNode (9000, 9870), Hive Metastore (9083)"
echo "+ $PYTHON_CMD tests/verify_infra.py"

cd "$ROOT_DIR"
"$PYTHON_CMD" tests/verify_infra.py

