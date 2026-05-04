"""
import_price_history.py — 把 stock_backtest 的歷史股價匯入 banshi.db
用法：python3 import_price_history.py
"""

import os
import sqlite3
import pandas as pd
from pathlib import Path

RAW_PATH = os.path.expanduser("~/Documents/stock_backtest/data/raw")
DB_PATH = os.path.expanduser("~/Documents/banshi_system/banshi.db")

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 建立 price_history 表格
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            stock_id TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)
    conn.commit()

    files = list(Path(RAW_PATH).glob("*.csv"))
    print(f"找到 {len(files)} 支股票的資料")

    success = 0
    fail = 0

    for i, f in enumerate(files):
        stock_id = f.stem  # 檔名就是股票代號
        try:
            df = pd.read_csv(f)
            df["stock_id"] = stock_id
            df = df[["stock_id", "date", "open", "high", "low", "close", "volume"]]
            df = df.dropna()

            # 寫入（忽略重複）
            df.to_sql("price_history", conn, if_exists="append", index=False,
                      method="multi", chunksize=500)
            success += 1

            if (i+1) % 100 == 0:
                print(f"  [{i+1}/{len(files)}] 已匯入 {success} 支...")
                conn.commit()

        except Exception as e:
            fail += 1

    conn.commit()

    # 建立索引加速查詢
    print("建立索引...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_stock_date ON price_history(stock_id, date)")
    conn.commit()

    # 驗證
    cursor.execute("SELECT COUNT(DISTINCT stock_id), MIN(date), MAX(date), COUNT(*) FROM price_history")
    result = cursor.fetchone()
    print(f"\n匯入完成！")
    print(f"  股票數：{result[0]}")
    print(f"  日期範圍：{result[1]} ~ {result[2]}")
    print(f"  總筆數：{result[3]}")
    print(f"  成功：{success}　失敗：{fail}")

    conn.close()

if __name__ == "__main__":
    main()
