#!/bin/bash
# run_daily.sh — 磐石決策系統每日排程
# 由 crontab 在 15:30 呼叫

set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$DIR/daily_log.txt"
PYTHON="$(which python3)"

echo "======================================" >> "$LOG"
echo "$(date '+%Y-%m-%d %H:%M:%S') 開始執行" >> "$LOG"

# 讀取 token（從 ~/.banshi_env 或環境變數）
ENV_FILE="$HOME/.banshi_env"
if [ -f "$ENV_FILE" ]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi
export FINMIND_TOKEN="${FINMIND_TOKEN:-}"

# 1. 抓取當日資料
echo "[1/2] fetch_today.py" >> "$LOG"
"$PYTHON" "$DIR/fetch_today.py" >> "$LOG" 2>&1

# 2. 跑決策引擎
echo "[2/2] main.py" >> "$LOG"
"$PYTHON" "$DIR/main.py" --quiet >> "$LOG" 2>&1

# 3. 備份當日決策 CSV
mkdir -p "$DIR/logs"
cp "$DIR/latest_decisions.csv" "$DIR/logs/decisions_$(date +%F).csv"

echo "$(date '+%Y-%m-%d %H:%M:%S') 完成" >> "$LOG"
