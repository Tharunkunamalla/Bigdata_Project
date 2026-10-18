#!/usr/bin/env bash
# ==============================================================================
# Step 9: Stream crash recovery and fault tolerance evidence
# ==============================================================================
set -e

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: ./09_recovery.sh"
    echo "Prints key findings from the 100,000-event Spark crash recovery stress test."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "Step 9: Stream crash recovery and fault tolerance evidence"
echo "+ cat results/stream_recovery_evidence.txt"

cd "$ROOT_DIR"
"$PYTHON_CMD" -c '
with open("results/stream_recovery_evidence.txt", "r") as f:
    lines = [line.strip() for line in f if line.strip()]

print("\n--- Spark Structured Streaming Crash-Recovery Metrics ---")
key_patterns = [
    "Target events:",
    "Driver process terminated violently",
    "Initiating recovery restart",
    "Final Sink Event Count:",
    "Distinct Event Count:",
    "Events Lost:",
    "Duplicate Events:",
    "Measured Recovery Time:",
    "Delivery Guarantee:",
    "Test Complete:"
]

for line in lines:
    for pat in key_patterns:
        if pat.lower() in line.lower():
            # Clean timestamp prefix if present
            clean = line.split("] ")[-1] if "] " in line else line
            print(f"  {clean}")
            break
'
