#!/usr/bin/env bash
set -euo pipefail
SERVICE_NAME="feishu-agent-bot.service"

systemctl status "$SERVICE_NAME" --no-pager

echo
echo "最近长连接状态："
journalctl -u "$SERVICE_NAME" --since "24 hours ago" --no-pager 2>/dev/null \
  | grep -E "connected to|长连接已断开|长连接重连成功|receive message loop exit" \
  | tail -5 || true

echo
echo "本机机器人进程："
ps -eo pid,lstart,cmd | grep '[f]eishu_agent_bot.main' || true
