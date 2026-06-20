#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

ENV_FILE="${FEISHU_AGENT_ENV_FILE:-.env}"

env_value() {
  local name="$1"
  local default="$2"
  local current="${!name:-}"
  if [[ -n "$current" ]]; then
    printf '%s\n' "$current"
    return
  fi
  if [[ -f "$ENV_FILE" ]]; then
    local value
    value="$(
      awk -F= -v key="$name" '
        $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
          sub(/^[^=]*=/, "", $0)
          gsub(/^[[:space:]"'\'']+|[[:space:]"'\'']+$/, "", $0)
          print $0
          exit
        }
      ' "$ENV_FILE"
    )"
    if [[ -n "$value" ]]; then
      printf '%s\n' "$value"
      return
    fi
  fi
  printf '%s\n' "$default"
}

if [[ -n "${TEMPORAL_CONNECT_TIMEOUT_SECONDS:-}" ]]; then
  TIMEOUT_SECONDS="$TEMPORAL_CONNECT_TIMEOUT_SECONDS"
elif [[ -f "$ENV_FILE" ]]; then
  TIMEOUT_SECONDS="$(
    awk -F= '
      /^[[:space:]]*TEMPORAL_CONNECT_TIMEOUT_SECONDS[[:space:]]*=/ {
        value=$2
        gsub(/^[[:space:]"'\'']+|[[:space:]"'\'']+$/, "", value)
        print value
        exit
      }
    ' "$ENV_FILE"
  )"
  TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"
else
  TIMEOUT_SECONDS="30"
fi
TEMPORAL_ADDRESS_VALUE="$(env_value TEMPORAL_ADDRESS localhost:7233)"
TEMPORAL_NAMESPACE_VALUE="$(env_value TEMPORAL_NAMESPACE default)"
DEADLINE=$((SECONDS + TIMEOUT_SECONDS))

while (( SECONDS <= DEADLINE )); do
  if temporal operator namespace describe \
      --address "$TEMPORAL_ADDRESS_VALUE" \
      --namespace "$TEMPORAL_NAMESPACE_VALUE" \
      --command-timeout 2s \
      --disable-config-env \
      --disable-config-file >/dev/null 2>&1; then
    echo "Temporal 可连接。"
    exit 0
  fi
  sleep 1
done

echo "错误：Temporal 在 ${TIMEOUT_SECONDS}s 内不可连接。" >&2
exit 1
