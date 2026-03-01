#!/bin/bash

set -euo pipefail

PATH=/usr/local/bin:/usr/bin:/bin
REPO_DIR=/opt/Python-Processing-InfluxDB
VENV_DIR=/opt/Python-Processing-InfluxDB/venv
LOG_FILE=${LOG_FILE:-/var/log/python_auswertung_run.log}

# Ensure log file exists and append both stdout/stderr to it while keeping console output
mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

cd "$REPO_DIR"

echo "[$(date -Is)] Starting run_script"
echo "[$(date -Is)] Pulling latest code from git repository at $REPO_DIR"
# Pull latest code
if ! git pull; then
    echo "[$(date -Is)] Error: git pull failed" >&2
    exit 1
fi

# Activate virtualenv
if [ -f "$VENV_DIR/bin/activate" ]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
else
    echo "[$(date -Is)] Error: venv not found at $VENV_DIR" >&2
    exit 1
fi

# Install deps when requirements changed in last commit range
if git diff --name-only HEAD~1..HEAD | grep -q "requirements"; then
    pip install -r requirements.txt
fi
echo "[$(date -Is)] Running main.py with stage prod"
python src/main.py --stage prod
echo "[$(date -Is)] Finished run_script"