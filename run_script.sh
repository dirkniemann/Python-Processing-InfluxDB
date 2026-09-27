#!/bin/bash

set -euo pipefail

PATH=/usr/local/bin:/usr/bin:/bin
REPO_DIR=/opt/Python-Processing-InfluxDB
VENV_DIR=/opt/Python-Processing-InfluxDB/venv
REQUIREMENTS_STATE=/var/lib/Python_Auswertung/requirements.sha256

cd "$REPO_DIR"

if [ -f "$VENV_DIR/bin/activate" ]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
else
    echo "[$(date -Is)] Error: venv not found at $VENV_DIR" >&2
    exit 1
fi

requirements_hash=$(sha256sum requirements.txt | awk '{print $1}')
if [ ! -f "$REQUIREMENTS_STATE" ] || [ "$(cat "$REQUIREMENTS_STATE")" != "$requirements_hash" ]; then
    python -m pip install -r requirements.txt
    mkdir -p "$(dirname "$REQUIREMENTS_STATE")"
    printf '%s\n' "$requirements_hash" > "$REQUIREMENTS_STATE"
fi

exec python src/main.py --stage prod