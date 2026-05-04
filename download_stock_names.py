"""
download_stock_names.py — 下載股票名稱對照表存進 banshi.db
用法：python3 download_stock_names.py
"""
import os
import sqlite3
import requests
import pandas as pd

DB_PATH = os.path.expanduser("~/Documents/banshi_system/banshi.db")
TOKEN = os.getenv("FINMIND_TOKEN")

def main():
    r = requests.get(
        "https://api.finmindtrade.com/api/v4/data",
        params={"dataset": "TaiwanStockInfo", "token": TOKEN},
        timeout=30
    )
    data = r.json()
    if data.get("status") != 200:
        print("抓取失敗:", data.get("msg", "unknown error"))
        return

    df = pd.DataFrame(data["data"])
    df = df[["stock_id", "stock_name", "industry_category"]].drop_duplicates("stock_id")
    df = df[df["stock_id"].str.match(r"^\d{4}$")]
    df.columns = ["stock_id", "name", "industry"]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_names (
            stock_id TEXT PRIMARY KEY,
            name TEXT,
            industry TEXT
        )
    """)
    df.to_sql("stock_names", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()

    print(f"完成，共 {len(df)} 支股票名稱")

if __name__ == "__main__":
    main()
