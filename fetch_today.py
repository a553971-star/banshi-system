"""
fetch_today.py — 抓取當日資料寫入 banshi.db
由 cron 呼叫，token 從環境變數 FINMIND_TOKEN 讀取。
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(__file__))
from finmind_fetcher import fetch_and_store

STOCK_IDS = [
    "2330", "2454", "2303", "2317", "2382",
    "3034", "2603", "2615", "6213", "3035",
    "2449", "8299", "3264", "2408", "2412",
]

DB_PATH    = os.path.join(os.path.dirname(__file__), "banshi.db")
TOKEN      = os.environ.get("FINMIND_TOKEN", "")
TODAY      = date.today().isoformat()

if not TOKEN:
    print("WARNING: FINMIND_TOKEN 未設定，將以無登入模式嘗試（可能受速率限制）")

n = fetch_and_store(STOCK_IDS, TODAY, TODAY, token=TOKEN, db_path=DB_PATH)
print(f"fetch_today: {TODAY} 寫入 {n} 筆")
