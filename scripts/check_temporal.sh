#!/usr/bin/env bash
set -euo pipefail

if ! command -v temporal >/dev/null 2>&1; then
  printf '%s\n' \
    "错误：未找到 temporal CLI。" \
    "请按 Temporal 官方文档安装 CLI 后重试：" \
    "  https://docs.temporal.io/cli" \
    "" \
    "本项目不会执行来源不明的 root curl 安装脚本。" >&2
  exit 1
fi

temporal --version
