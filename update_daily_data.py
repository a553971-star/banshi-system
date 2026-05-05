"""
update_daily_data.py — 每日更新 price_history / institutional_history / margin_history
用法：python3 update_daily_data.py [YYYY-MM-DD]
      省略日期則預設今天
"""
import os
import sys
import sqlite3
from datetime import datetime, timedelta, date as _date

import pandas as pd

BASE_PATH     = os.path.dirname(os.path.abspath(__file__))
DB_PATH       = os.path.join(BASE_PATH, "banshi.db")
UNIVERSE_PATH = os.path.join(BASE_PATH, "universe.csv")
TOKEN         = os.getenv("FINMIND_TOKEN", "")


def setup_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            stock_id TEXT, date TEXT, open REAL, high REAL,
            low REAL, close REAL, volume REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS institutional_history (
            stock_id TEXT, date TEXT,
            foreign_buy REAL, foreign_sell REAL, foreign_net REAL,
            investment_buy REAL, investment_sell REAL, investment_net REAL,
            dealer_net REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS margin_history (
            stock_id TEXT, date TEXT,
            margin_balance REAL, short_balance REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shareholding_history (
            stock_id TEXT, date TEXT,
            foreign_shares INTEGER, foreign_ratio REAL,
            PRIMARY KEY (stock_id, date)
        )
    """)


def main():
    import pandas as pd
    end_date = (pd.Timestamp.today() - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")
    start_date = (pd.Timestamp.today() - pd.tseries.offsets.BDay(180)).strftime("%Y-%m-%d")
    print(f"📅 更新範圍：{start_date} ~ {end_date}")

    universe_df = pd.read_csv(UNIVERSE_PATH, dtype=str)
    universe = universe_df["stock_id"].tolist()
    print(f"Universe：{len(universe)} 支")

    from FinMind.data import DataLoader
    api = DataLoader()
    if TOKEN:
        api.login_by_token(api_token=TOKEN)

    # 1. 股價
    try:
        df_price_raw = api.taiwan_stock_daily(start_date=start_date, end_date=end_date)
        if not df_price_raw.empty:
            df_price = df_price_raw[df_price_raw["stock_id"].isin(universe)].copy()
            df_price = df_price.rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
            df_price = df_price[["stock_id", "date", "open", "high", "low", "close", "volume"]]
            df_price = df_price.drop_duplicates(subset=["stock_id", "date"])
        else:
            df_price = pd.DataFrame()
    except Exception as e:
        print(f"  股價抓取失敗：{e}")
        df_price = pd.DataFrame()
    print(f"  股價：{len(df_price)} 筆")

    # 2. 法人（全市場）
    df_inst = pd.DataFrame()
    try:
        df_inst_raw = api.taiwan_stock_institutional_investors(start_date=start_date, end_date=end_date)
        if not df_inst_raw.empty:
            df_inst_raw["buy"]  = pd.to_numeric(df_inst_raw["buy"],  errors="coerce").fillna(0)
            df_inst_raw["sell"] = pd.to_numeric(df_inst_raw["sell"], errors="coerce").fillna(0)
            df_inst_raw["net"]  = df_inst_raw["buy"] - df_inst_raw["sell"]

            pivot = df_inst_raw.pivot_table(
                index=["stock_id", "date"],
                columns="name",
                values=["buy", "sell", "net"],
                aggfunc="sum"
            ).reset_index().fillna(0)

            pivot.columns = [
                "_".join(c).strip("_") if isinstance(c, tuple) else c
                for c in pivot.columns
            ]

            df_inst = pd.DataFrame({
                "stock_id":       pivot["stock_id"],
                "date":           pivot["date"],
                "foreign_buy":    pivot.get("buy_Foreign_Investor",   0),
                "foreign_sell":   pivot.get("sell_Foreign_Investor",  0),
                "foreign_net":    pivot.get("net_Foreign_Investor",   0),
                "investment_buy":  pivot.get("buy_Investment_Trust",  0),
                "investment_sell": pivot.get("sell_Investment_Trust", 0),
                "investment_net":  pivot.get("net_Investment_Trust",  0),
                "dealer_net":     pivot.get("net_Dealer_self",        0),
            })
            df_inst = df_inst[df_inst["stock_id"].isin(universe)]
            df_inst = df_inst.drop_duplicates(subset=["stock_id", "date"])
    except Exception as e:
        print(f"  法人抓取失敗：{e}")
    print(f"  法人：{len(df_inst)} 筆")

    # 3. 融資
    df_margin = pd.DataFrame()
    try:
        df_margin_raw = api.taiwan_stock_margin_purchase_short_sale(start_date=start_date, end_date=end_date)
        if not df_margin_raw.empty:
            df_margin = df_margin_raw[df_margin_raw["stock_id"].isin(universe)].copy()
            df_margin = df_margin.rename(columns={
                "MarginPurchaseTodayBalance": "margin_balance",
                "ShortSaleTodayBalance":      "short_balance",
            })
            df_margin = df_margin[["stock_id", "date", "margin_balance", "short_balance"]]
            df_margin = df_margin.drop_duplicates(subset=["stock_id", "date"])
    except Exception as e:
        print(f"  融資抓取失敗：{e}")
    print(f"  融資：{len(df_margin)} 筆")

    # 4. 外資持股（集保）
    df_sh = pd.DataFrame()
    try:
        df_sh_raw = api.taiwan_stock_shareholding(start_date=start_date, end_date=end_date)
        if not df_sh_raw.empty:
            df_sh_raw.columns = [c.lower() for c in df_sh_raw.columns]
            df_sh = df_sh_raw[df_sh_raw["stock_id"].isin(universe)].copy()
            df_sh = df_sh.rename(columns={
                "foreigninvestmentshares":      "foreign_shares",
                "foreigninvestmentsharesratio": "foreign_ratio",
            })
            df_sh = df_sh[["stock_id", "date", "foreign_shares", "foreign_ratio"]]
            df_sh = df_sh.drop_duplicates(subset=["stock_id", "date"])
    except Exception as e:
        print(f"  外資持股抓取失敗：{e}")
    print(f"  外資持股：{len(df_sh)} 筆")

    # 5. 寫入 SQLite（防重複）
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    setup_tables(cursor)

    def upsert_df(df, table):
        if df.empty:
            return
        cols         = ", ".join(df.columns)
        placeholders = ", ".join(["?"] * len(df.columns))
        sql          = f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})"
        cursor.executemany(sql, df.values.tolist())

    upsert_df(df_price,  "price_history")
    upsert_df(df_inst,   "institutional_history")
    upsert_df(df_margin, "margin_history")
    upsert_df(df_sh,     "shareholding_history")

    # 建立 index 加速查詢
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price  ON price_history(stock_id, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inst   ON institutional_history(stock_id, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_margin ON margin_history(stock_id, date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sh     ON shareholding_history(stock_id, date)")

    # 清理超過90天的舊資料
    cutoff = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
    for table in ["price_history", "institutional_history", "margin_history", "shareholding_history"]:
        cursor.execute(f"DELETE FROM {table} WHERE date < ?", (cutoff,))

    conn.commit()
    conn.close()
    print(f"寫入完成（保留 {cutoff} 之後的資料）")


if __name__ == "__main__":
    main()
