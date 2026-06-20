#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-.venv/bin/python}"

"$PYTHON" -m pytest -q \
  tests/test_temporal_workflow.py::test_monitoring_cycle_workflow_runs_finite_cycle \
  tests/test_temporal_activities.py::test_start_monitoring_cycle_resolves_job_from_monitor_id \
  tests/test_temporal_activities.py::test_recheck_monitored_sources_creates_and_uses_watch_targets
