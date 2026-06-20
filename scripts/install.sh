#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

if [[ ! -f /etc/os-release ]]; then
  echo "错误：无法识别操作系统；本脚本面向 Ubuntu 22.04。" >&2
  exit 1
fi
. /etc/os-release
if [[ "${ID:-}" != "ubuntu" ]]; then
  echo "警告：当前系统是 ${PRETTY_NAME:-unknown}，推荐 Ubuntu 22.04。"
fi

python3 - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("错误：需要 Python 3.10 或更高版本")
print("Python:", sys.version.split()[0])
PY

if [[ ! -x .venv/bin/python ]]; then
  if ! python3 -m venv .venv; then
    echo "创建虚拟环境失败。Ubuntu 22.04 请先执行：" >&2
    echo "  sudo apt-get update && sudo apt-get install -y python3-venv" >&2
    exit 1
  fi
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e ".[dev]"
mkdir -p data
if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "已创建 .env，请填入轮换后的飞书凭据。"
fi
chmod 600 .env
.venv/bin/python -m pytest -q

cat <<'EOF'
安装完成。
下一步：
  1. 编辑 .env，填入新 App ID 和已轮换的 App Secret
  2. 前台启动：./scripts/start.sh
  3. 安装 systemd：./scripts/install_systemd.sh
EOF
