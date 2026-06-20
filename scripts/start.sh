#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if systemctl is-active --quiet feishu-agent-bot.service 2>/dev/null; then
  echo "机器人已由 systemd 管理并正在运行，无需重复启动。"
  systemctl show feishu-agent-bot.service \
    -p MainPID -p ActiveState -p ActiveEnterTimestamp --no-pager
  echo "最近长连接状态："
  journalctl -u feishu-agent-bot.service --since "24 hours ago" --no-pager \
    | grep -E "connected to|长连接已断开|长连接重连成功|receive message loop exit" \
    | tail -3 || true
  echo "查看实时日志：sudo journalctl -u feishu-agent-bot.service -f"
  exit 0
fi

exec .venv/bin/python -m feishu_agent_bot.main
