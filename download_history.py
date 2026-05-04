"""
download_history.py — 下載法人+融資歷史資料到本機
可中斷繼續，不會重複下載
用法：python3 download_history.py
"""
import os
import time
import sqlite3
import requests
import pandas as pd

DB_PATH = os.path.expanduser("~/Documents/banshi_system/banshi.db")
UNIVERSE_PATH = os.path.expanduser("~/Documents/banshi_system/universe.csv")
TOKEN = os.getenv("FINMIND_TOKEN")
START = "2024-01-01"

def _fm(dataset, stock_id, start):
    for attempt in range(3):
        try:
            r = requests.get(
                "https://api.finmindtrade.com/api/v4/data",
                params={"dataset": dataset, "data_id": stock_id,
                        "start_date": start, "token": TOKEN},
                timeout=15,
            )
            d = r.json()
            if d.get("status") == 200 and d.get("data"):
                return pd.DataFrame(d["data"])
            return pd.DataFrame()
        except Exception:
            if attempt < 2:
                time.sleep(5)
    return pd.DataFrame()

def setup_db(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS institutional_history (
            stock_id TEXT,
            date TEXT,
            foreign_buy REAL,
            foreign_sell REAL,
            foreign_net REAL,
            investment_buy REAL,
            investment_sell REAL,
            investment_net REAL,
            dealer_net REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS margin_history (
            stock_id TEXT,
            date TEXT,
            margin_balance REAL,
            short_balance REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inst_stock_date ON institutional_history(stock_id, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_margin_stock_date ON margin_history(stock_id, date)")
    conn.commit()

def already_downloaded(conn, table, stock_id):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE stock_id=?", (stock_id,))
    return cursor.fetchone()[0] > 0

def download_institutional(conn, stock_id):
    df = _fm("TaiwanStockInstitutionalInvestorsBuySell", stock_id, START)
    if df.empty:
        return 0
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["buy"] = pd.to_numeric(df["buy"], errors="coerce").fillna(0)
    df["sell"] = pd.to_numeric(df["sell"], errors="coerce").fillna(0)
    pivot = df.pivot_table(index="date", columns="name",
                           values=["buy", "sell"], aggfunc="sum").fillna(0)
    rows = []
    for date in pivot.index:
        rows.append({
            "stock_id": stock_id,
            "date": date,
            "foreign_buy": float(pivot["buy"].get("Foreign_Investor", pd.Series([0])).get(date, 0)),
            "foreign_sell": float(pivot["sell"].get("Foreign_Investor", pd.Series([0])).get(date, 0)),
            "foreign_net": float(pivot["buy"].get("Foreign_Investor", pd.Series([0])).get(date, 0)) -
                          float(pivot["sell"].get("Foreign_Investor", pd.Series([0])).get(date, 0)),
            "investment_buy": float(pivot["buy"].get("Investment_Trust", pd.Series([0])).get(date, 0)),
            "investment_sell": float(pivot["sell"].get("Investment_Trust", pd.Series([0])).get(date, 0)),
            "investment_net": float(pivot["buy"].get("Investment_Trust", pd.Series([0])).get(date, 0)) -
                             float(pivot["sell"].get("Investment_Trust", pd.Series([0])).get(date, 0)),
            "dealer_net": float(pivot["buy"].get("Dealer_self", pd.Series([0])).get(date, 0)) -
                         float(pivot["sell"].get("Dealer_self", pd.Series([0])).get(date, 0)),
        })
    if rows:
        pd.DataFrame(rows).to_sql("institutional_history", conn,
                                   if_exists="append", index=False,
                                   method="multi", chunksize=200)
    return len(rows)

def download_margin(conn, stock_id):
    df = _fm("TaiwanStockMarginPurchaseShortSale", stock_id, START)
    if df.empty:
        return 0
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "stock_id": stock_id,
            "date": r["date"],
            "margin_balance": float(r.get("MarginPurchaseTodayBalance", 0) or 0),
            "short_balance": float(r.get("ShortSaleTodayBalance", 0) or 0),
        })
    if rows:
        pd.DataFrame(rows).to_sql("margin_history", conn,
                                   if_exists="append", index=False,
                                   method="multi", chunksize=200)
    return len(rows)

def main():
    universe = pd.read_csv(UNIVERSE_PATH, dtype=str)
    stock_list = universe["stock_id"].tolist()
    print(f"共 {len(stock_list)} 支股票，開始下載...")

    conn = sqlite3.connect(DB_PATH)
    setup_db(conn)

    for i, sid in enumerate(stock_list):
        inst_done = already_downloaded(conn, "institutional_history", sid)
        margin_done = already_downloaded(conn, "margin_history", sid)

        if inst_done and margin_done:
            print(f"  [{i+1}/{len(stock_list)}] {sid} 已下載，跳過")
            continue

        print(f"  [{i+1}/{len(stock_list)}] {sid}", end=" ", flush=True)

        if not inst_done:
            n = download_institutional(conn, sid)
            print(f"法人{n}筆", end=" ", flush=True)
        if not margin_done:
            n = download_margin(conn, sid)
            print(f"融資{n}筆", end=" ", flush=True)

        conn.commit()
        print()
        time.sleep(0.3)

    conn.close()
    print("\n下載完成！")

if __name__ == "__main__":
    main()
