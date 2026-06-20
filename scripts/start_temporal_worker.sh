#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

./scripts/wait_temporal.sh
exec .venv/bin/python -m feishu_agent_bot.temporal.worker
