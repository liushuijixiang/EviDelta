#!/usr/bin/env bash
set -euo pipefail

if [[ "$EUID" -eq 0 ]]; then
  echo "错误：请以实际运行服务的普通用户执行本脚本，不要直接使用 root。" >&2
  exit 1
fi
if ! groups | tr ' ' '\n' | grep -qx sudo; then
  echo "错误：当前用户不在 sudo 组，无法安装系统服务。" >&2
  exit 1
fi

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_GROUP="$(id -gn)"
MODE="${1:-}"

if [[ -z "$MODE" ]]; then
  if grep -Eq '^EXECUTION_BACKEND=local$' "$PROJECT_DIR/.env" 2>/dev/null; then
    MODE="local"
  else
    MODE="temporal"
  fi
fi

render_template() {
  local template="$1"
  local output="$2"
  local temp_file
  temp_file="$(mktemp)"
  sed \
    -e "s|__USER__|$USER|g" \
    -e "s|__GROUP__|$RUN_GROUP|g" \
    -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
    "$template" > "$temp_file"
  echo "  sudo install -m 0644 $temp_file /etc/systemd/system/$output"
  sudo install -m 0644 "$temp_file" "/etc/systemd/system/$output"
  rm -f "$temp_file"
}

echo "安装模式：$MODE"

case "$MODE" in
  temporal)
    if ! command -v temporal >/dev/null 2>&1; then
      printf '%s\n' \
        "错误：安装 temporal systemd dev server 前需要 temporal CLI。" \
        "请按 Temporal 官方文档安装 CLI：" \
        "  https://docs.temporal.io/cli" >&2
      exit 1
    fi
    echo "即将执行需要 sudo 的 systemd 安装命令。"
    render_template "$PROJECT_DIR/deploy/feishu-agent-temporal-dev.service" \
      "feishu-agent-temporal-dev.service"
    render_template "$PROJECT_DIR/deploy/feishu-agent-worker.service" \
      "feishu-agent-worker.service"
    render_template "$PROJECT_DIR/deploy/feishu-agent-gateway.service" \
      "feishu-agent-gateway.service"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable --now feishu-agent-temporal-dev.service"
    echo "  sudo systemctl enable --now feishu-agent-worker.service"
    echo "  sudo systemctl enable --now feishu-agent-gateway.service"
    sudo systemctl daemon-reload
    sudo systemctl enable --now feishu-agent-temporal-dev.service
    sudo systemctl enable --now feishu-agent-worker.service
    sudo systemctl enable --now feishu-agent-gateway.service
    sudo systemctl status feishu-agent-gateway.service --no-pager
    echo "查看日志：sudo journalctl -u feishu-agent-gateway.service -u feishu-agent-worker.service -f"
    ;;
  local)
    echo "即将执行需要 sudo 的 systemd 安装命令。"
    render_template "$PROJECT_DIR/deploy/feishu-agent-bot.service" \
      "feishu-agent-bot.service"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable --now feishu-agent-bot.service"
    sudo systemctl daemon-reload
    sudo systemctl enable --now feishu-agent-bot.service
    sudo systemctl status feishu-agent-bot.service --no-pager
    echo "查看日志：sudo journalctl -u feishu-agent-bot.service -f"
    ;;
  *)
    echo "用法：$0 [temporal|local]" >&2
    exit 2
    ;;
esac
