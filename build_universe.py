"""
build_universe.py — 從本機資料建立交易universe
篩選條件：收盤價>20、近20日均量>1000張
輸出：universe.csv
"""
import os
import sqlite3
import pandas as pd

DB_PATH = os.path.expanduser("~/Documents/banshi_system/banshi.db")
OUT_PATH = os.path.expanduser("~/Documents/banshi_system/universe.csv")

def main():
    conn = sqlite3.connect(DB_PATH)

    # 取每支股票最近20個交易日的資料
    query = """
        SELECT stock_id, date, close, volume
        FROM price_history
        WHERE date >= '2026-03-01'
        ORDER BY stock_id, date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("沒有資料")
        return

    # 計算每支股票的最新收盤價和近20日均量
    df["volume_1000"] = df["volume"] / 1000  # 轉換成張

    stats = df.groupby("stock_id").agg(
        latest_close=("close", "last"),
        avg_volume_20d=("volume_1000", "mean"),
        data_days=("date", "count")
    ).reset_index()

    # 篩選條件
    universe = stats[
        (stats["latest_close"] > 20) &
        (stats["avg_volume_20d"] > 1000) &
        (stats["data_days"] >= 10)  # 至少要有10天資料
    ].copy()

    # 只保留4位數代號
    universe = universe[universe["stock_id"].str.match(r"^\d{4}$")]

    # 從 banshi.db 讀取產業分類
    try:
        conn = sqlite3.connect(DB_PATH)
        names_df = pd.read_sql_query("SELECT stock_id, industry FROM stock_names", conn)
        conn.close()
        universe = universe.merge(names_df, on="stock_id", how="left")

        finance_keywords = ["金融", "銀行", "保險", "證券", "票券", "投信", "期貨"]
        def is_finance(industry):
            if not isinstance(industry, str):
                return False
            return any(kw in industry for kw in finance_keywords)

        before = len(universe)
        universe = universe[~universe["industry"].apply(is_finance)]
        print(f"  排除金融股：{before - len(universe)} 支")
    except Exception as e:
        print(f"產業篩選失敗：{e}")

    universe = universe.sort_values("stock_id")

    # 儲存
    universe[["stock_id"]].to_csv(OUT_PATH, index=False)

    print(f"Universe 建立完成")
    print(f"  原始股票數：{len(stats)}")
    print(f"  篩選後：{len(universe)} 支")
    print(f"  儲存至：{OUT_PATH}")
    print(f"\n前20支：{universe['stock_id'].head(20).tolist()}")

if __name__ == "__main__":
    main()
