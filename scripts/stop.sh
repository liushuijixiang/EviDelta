#!/usr/bin/env bash
set -euo pipefail
SERVICE_NAME="feishu-agent-bot.service"
if systemctl --user is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
  systemctl --user stop "$SERVICE_NAME"
else
  echo "即将执行：sudo systemctl stop $SERVICE_NAME"
  sudo systemctl stop "$SERVICE_NAME"
fi
