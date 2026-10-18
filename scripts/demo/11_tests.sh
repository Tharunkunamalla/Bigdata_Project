#!/usr/bin/env bash
# ==============================================================================
# Step 11: End-to-end unit and client logic test suite
# ==============================================================================
set -e

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    echo "Usage: ./11_tests.sh"
    echo "Runs fast unit and client logic tests and prints total passed assertions."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "Step 11: End-to-end unit and client logic test suite"
echo "+ make test"

cd "$ROOT_DIR"
if command -v make &>/dev/null; then
    make test
elif [ -f "./make.bat" ]; then
    ./make.bat test
else
    "$PYTHON_CMD" -m pytest tests/test_real_pipeline.py tests/test_dashboard_upgrade.py tests/test_ui_cleanliness.py -v
    cd frontend && node tests/test_client_logic.js && cd ..
fi
