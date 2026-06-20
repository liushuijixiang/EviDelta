#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if [[ -f data/temporal-dev.pid ]] && kill -0 "$(cat data/temporal-dev.pid)" 2>/dev/null; then
  echo "temporal-dev: running PID $(cat data/temporal-dev.pid)"
elif pgrep -af '[t]emporal server start-dev.*data/temporal.db' >/dev/null; then
  echo "temporal-dev: running"
else
  echo "temporal-dev: stopped"
fi
pgrep -af '[f]eishu_agent_bot.temporal.worker' || true
pgrep -af '[f]eishu_agent_bot.main' || true
if .venv/bin/python -m feishu_agent_bot.cli temporal health >/dev/null 2>&1; then
  echo "temporal-health: ok"
else
  echo "temporal-health: unavailable"
fi
