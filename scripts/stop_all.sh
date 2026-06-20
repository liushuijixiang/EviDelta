#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(dirname "${BASH_SOURCE[0]}")"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

if [[ -f data/temporal-dev.pid ]] && kill -0 "$(cat data/temporal-dev.pid)" 2>/dev/null; then
  kill "$(cat data/temporal-dev.pid)"
  rm -f data/temporal-dev.pid
  echo "已停止 temporal dev server"
fi
pkill -f '[t]emporal server start-dev.*data/temporal.db' || true
pkill -f '[f]eishu_agent_bot.temporal.worker' || true
pkill -f '[f]eishu_agent_bot.main' || true
rm -f data/temporal-worker.pid data/gateway.pid
