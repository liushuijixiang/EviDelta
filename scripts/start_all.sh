#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

./scripts/start_temporal_dev.sh
mkdir -p data

if ! pgrep -af '[f]eishu_agent_bot.temporal.worker' >/dev/null; then
  nohup ./scripts/start_temporal_worker.sh >data/temporal-worker.log 2>&1 &
  echo $! > data/temporal-worker.pid
  echo "Temporal worker PID $(cat data/temporal-worker.pid)"
else
  echo "Temporal worker 已在运行。"
fi

if ! pgrep -af '[f]eishu_agent_bot.main' >/dev/null; then
  nohup ./scripts/start_gateway.sh >data/gateway.log 2>&1 &
  echo $! > data/gateway.pid
  echo "Gateway PID $(cat data/gateway.pid)"
else
  echo "Gateway 已在运行。"
fi

./scripts/status_all.sh
