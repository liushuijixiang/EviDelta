#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

./scripts/check_temporal.sh >/dev/null
./scripts/start_temporal_dev.sh

RUN_ID="recovery-$(date +%Y%m%d%H%M%S)-$$"
TASK_QUEUE="recovery-check-$RUN_ID"
DATA_DIR="$PROJECT_DIR/data/recovery-check/$RUN_ID"
LOG_DIR="$PROJECT_DIR/data/recovery-check"
mkdir -p "$LOG_DIR"

WORKER_PID=""

env_value() {
  local name="$1"
  local default="$2"
  local current="${!name:-}"
  if [[ -n "$current" ]]; then
    printf '%s\n' "$current"
    return
  fi
  if [[ -f .env ]]; then
    local value
    value="$(
      awk -F= -v key="$name" '
        $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
          sub(/^[^=]*=/, "", $0)
          gsub(/^[[:space:]"'\'']+|[[:space:]"'\'']+$/, "", $0)
          print $0
          exit
        }
      ' .env
    )"
    if [[ -n "$value" ]]; then
      printf '%s\n' "$value"
      return
    fi
  fi
  printf '%s\n' "$default"
}

TEMPORAL_ADDRESS_VALUE="$(env_value TEMPORAL_ADDRESS localhost:7233)"
TEMPORAL_NAMESPACE_VALUE="$(env_value TEMPORAL_NAMESPACE default)"

cleanup() {
  if [[ -n "${WORKER_PID:-}" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_worker() {
  .venv/bin/python -m feishu_agent_bot.temporal.recovery_check \
    worker "$TASK_QUEUE" >"$LOG_DIR/$RUN_ID-worker.log" 2>&1 &
  WORKER_PID=$!
  echo "recovery worker started pid=$WORKER_PID task_queue=$TASK_QUEUE"
}

wait_for_stage() {
  local expected_stage="$1"
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if .venv/bin/python -m feishu_agent_bot.temporal.recovery_check \
      status "$RUN_ID" >"$LOG_DIR/$RUN_ID-status.log" 2>&1; then
      if grep -q "^stage=$expected_stage$" "$LOG_DIR/$RUN_ID-status.log"; then
        cat "$LOG_DIR/$RUN_ID-status.log"
        return 0
      fi
    fi
    sleep 1
  done
  echo "等待 stage=$expected_stage 超时，最后状态：" >&2
  cat "$LOG_DIR/$RUN_ID-status.log" >&2 || true
  return 1
}

start_worker
.venv/bin/python -m feishu_agent_bot.temporal.recovery_check \
  start "$RUN_ID" "$DATA_DIR" "$TASK_QUEUE"
wait_for_stage "waiting_for_restart"

kill "$WORKER_PID"
wait "$WORKER_PID" 2>/dev/null || true
WORKER_PID=""
echo "recovery worker stopped at middle stage"

temporal workflow describe \
  --address "$TEMPORAL_ADDRESS_VALUE" \
  --namespace "$TEMPORAL_NAMESPACE_VALUE" \
  --workflow-id "recovery-$RUN_ID" \
  --disable-config-env \
  --disable-config-file >/dev/null
echo "recovery workflow still exists while worker is stopped"

start_worker
.venv/bin/python -m feishu_agent_bot.temporal.recovery_check continue "$RUN_ID"

REPORT_COUNT="$(find "$DATA_DIR" -maxdepth 1 -name 'report-v*.md' | wc -l)"
if [[ "$REPORT_COUNT" != "1" ]]; then
  echo "恢复验证失败：期望只生成 1 份报告，实际 $REPORT_COUNT" >&2
  exit 1
fi

echo "Temporal 恢复验证通过 run_id=$RUN_ID report_count=$REPORT_COUNT"
