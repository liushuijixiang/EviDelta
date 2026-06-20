#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"

./scripts/wait_temporal.sh
"$PYTHON" scripts/smoke_monitoring_schedule_reuse.py
