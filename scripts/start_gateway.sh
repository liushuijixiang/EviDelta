#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if grep -Eq '^EXECUTION_BACKEND=temporal$' .env 2>/dev/null; then
  ./scripts/wait_temporal.sh
fi
exec .venv/bin/python -m feishu_agent_bot.main
