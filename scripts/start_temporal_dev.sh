#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

./scripts/check_temporal.sh >/dev/null
mkdir -p data
PID_FILE="data/temporal-dev.pid"
DB_FILE="$PROJECT_DIR/data/temporal.db"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Temporal dev server 已在运行，PID $(cat "$PID_FILE")"
  echo "Web UI: http://localhost:8233"
  exit 0
fi
if pgrep -af "[t]emporal server start-dev.*$DB_FILE" >/dev/null; then
  echo "Temporal dev server 已在运行。"
  echo "Web UI: http://localhost:8233"
  exit 0
fi
if [[ -f "$PID_FILE" ]]; then
  rm -f "$PID_FILE"
fi

echo "启动本地开发 Temporal server，不用于生产。"
nohup temporal server start-dev --db-filename "$DB_FILE" >data/temporal-dev.log 2>&1 &
echo $! > "$PID_FILE"
echo "Temporal dev server PID $(cat "$PID_FILE")"
echo "Web UI: http://localhost:8233"
TEMPORAL_CONNECT_TIMEOUT_SECONDS="${TEMPORAL_CONNECT_TIMEOUT_SECONDS:-30}" \
  ./scripts/wait_temporal.sh
if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "错误：Temporal dev server 启动后已退出，请查看 data/temporal-dev.log" >&2
  rm -f "$PID_FILE"
  exit 1
fi
