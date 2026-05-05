"""
build_universe.py — 建立交易 universe
從 FinMind TaiwanStockInfo + TaiwanStockDaily 篩選
輸出：universe.csv + stock_universe 表（SQLite）
"""
import os
import sqlite3
import requests
import pandas as pd

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
OUT_PATH  = os.path.join(BASE_PATH, "universe.csv")
DB_PATH   = os.path.join(BASE_PATH, "banshi.db")
TOKEN     = os.getenv("FINMIND_TOKEN", "")

# 永遠排除
BLACKLIST = {
    "910022",  # 範例：特定問題股
}

# 永遠保留（不受流動性/價格條件限制）
ALLOWED = {
    "2330",  # 台積電
    "2454",  # 聯發科
    "2317",  # 鴻海
}

FINANCE_KEYWORDS = ["金融", "銀行", "保險", "證券", "票券", "投信", "期貨"]


def fetch_stock_info(token: str) -> pd.DataFrame:
    r = requests.get(
        "https://api.finmindtrade.com/api/v4/data",
        params={"dataset": "TaiwanStockInfo", "token": token},
        timeout=30,
    )
    data = r.json()
    if data.get("status") != 200:
        raise RuntimeError(f"TaiwanStockInfo 抓取失敗：{data.get('status')}")
    df = pd.DataFrame(data["data"])
    return df


def fetch_recent_daily(token: str, start_date: str, end_date: str) -> pd.DataFrame:
    from FinMind.data import DataLoader
    api = DataLoader()
    if token:
        api.login_by_token(api_token=token)
    df = api.taiwan_stock_daily(start_date=start_date, end_date=end_date)
    return df


def setup_universe_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_universe (
            stock_id TEXT PRIMARY KEY,
            name     TEXT,
            industry TEXT,
            theme    TEXT
        )
    """)


def main():
    # ── 1. 抓股票基本資料 ─────────────────────────────────────────
    print("抓取 TaiwanStockInfo...")
    df = fetch_stock_info(TOKEN)
    df = df[["stock_id", "stock_name", "industry_category"]].drop_duplicates("stock_id")
    df.columns = ["stock_id", "name", "industry_category"]

    # 資料清洗
    df["industry_category"] = df["industry_category"].fillna("").str.strip()

    total = len(df)
    print(f"全市場總數：{total} 支")

    # ── 2. 只保留 4 位數代號（1000~9999）─────────────────────────
    df = df[df["stock_id"].str.match(r"^[1-9]\d{3}$")]
    print(f"過濾4位數後：{len(df)} 支")

    # ── 3. 排除 ETF / ETN / 非一般股票 ──────────────────────────
    _exclude_pattern = "ETF|ETN|指數|受益證券|存託憑證|大盤|所有證券"
    df = df[~df["industry_category"].str.contains(_exclude_pattern, na=False)]
    print(f"排除 ETF/ETN/非一般股票後：{len(df)} 支")

    # ── 4. 排除金融業 ─────────────────────────────────────────────
    def is_finance(ind):
        return any(kw in ind for kw in FINANCE_KEYWORDS)
    df = df[~df["industry_category"].apply(is_finance)]
    print(f"排除金融後：{len(df)} 支")

    # ── 5. 排除黑名單 ─────────────────────────────────────────────
    before_bl = len(df)
    df = df[~df["stock_id"].isin(BLACKLIST)]
    print(f"排除黑名單：{before_bl - len(df)} 支，剩 {len(df)} 支")

    # ── 6. 流動性 + 價格篩選 ──────────────────────────────────────
    print("抓取近期日頻資料（流動性/價格用）...")
    end_date   = (pd.Timestamp.today() - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")
    start_date = (pd.Timestamp.today() - pd.tseries.offsets.BDay(20)).strftime("%Y-%m-%d")

    try:
        daily = fetch_recent_daily(TOKEN, start_date, end_date)
        daily["Trading_Volume"] = pd.to_numeric(daily["Trading_Volume"], errors="coerce")
        daily["close"]          = pd.to_numeric(daily["close"],          errors="coerce")

        # 均量（近20個交易日）
        avg_vol = (
            daily.groupby("stock_id")["Trading_Volume"]
            .mean()
            .reset_index()
            .rename(columns={"Trading_Volume": "avg_volume"})
        )
        # 最新收盤價 + 最新量
        latest = (
            daily.sort_values("date")
            .groupby("stock_id")
            .last()
            .reset_index()[["stock_id", "close", "Trading_Volume"]]
            .rename(columns={"Trading_Volume": "last_volume"})
        )

        liq = avg_vol.merge(latest, on="stock_id", how="inner")

        # 分出 ALLOWED（不受篩選限制）
        allowed_df = df[df["stock_id"].isin(ALLOWED)].copy()
        rest_df    = df[~df["stock_id"].isin(ALLOWED)].copy()

        rest_merged = rest_df.merge(liq, on="stock_id", how="left")

        # 流動性雙層：均量 > 1,000,000 股 且 最新量 > 500,000 股
        # 價格 > 20
        mask = (
            (rest_merged["avg_volume"]  > 1_000_000) &
            (rest_merged["last_volume"] >   500_000) &
            (rest_merged["close"]       >        20)
        )
        filtered = rest_merged[mask][["stock_id", "name", "industry_category"]].copy()
        df = pd.concat([allowed_df, filtered], ignore_index=True).drop_duplicates("stock_id")
        print(f"流動性+價格篩選後（含 ALLOWED）：{len(df)} 支")

    except Exception as e:
        print(f"  流動性篩選失敗（跳過）：{e}")

    # ── 7. theme 欄位（從 industry_category 對應）────────────────
    THEME_MAP = {
        "半導體": "Chip",
        "電子零組件": "Component",
        "電腦及週邊設備": "PC",
        "光電": "Display",
        "通信網路": "Network",
        "其他電子": "Electronics",
        "電子通路": "Channel",
        "資訊服務": "Software",
        "電機機械": "Machinery",
        "汽車": "Auto",
        "化學": "Chemical",
        "鋼鐵": "Steel",
        "生技醫療": "Biotech",
        "食品": "Food",
        "紡織纖維": "Textile",
        "貿易百貨": "Retail",
        "建材營造": "Construction",
        "航運": "Shipping",
        "觀光": "Tourism",
        "油電燃氣": "Energy",
    }
    def to_theme(ind):
        for kw, theme in THEME_MAP.items():
            if kw in ind:
                return theme
        return "Other"
    df["theme"] = df["industry_category"].apply(to_theme)

    # ── 8. 最終數字 ───────────────────────────────────────────────
    print(f"\n=== 最終結果 ===")
    print(f"全市場總數  X = {total}")
    print(f"最終保留    Z = {len(df)}")
    print(f"\n行業分布：")
    print(df["industry_category"].value_counts().to_string())

    # ── 9. 輸出 CSV ───────────────────────────────────────────────
    df[["stock_id"]].to_csv(OUT_PATH, index=False)
    print(f"\nuniverse.csv 輸出：{len(df)} 支 → {OUT_PATH}")

    # ── 10. 寫入 SQLite ───────────────────────────────────────────
    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    setup_universe_table(cursor)
    cursor.execute("DELETE FROM stock_universe")
    rows = df[["stock_id", "name", "industry_category", "theme"]].rename(
        columns={"industry_category": "industry"}
    )
    rows.to_sql("stock_universe", conn, if_exists="append", index=False)
    conn.commit()
    conn.close()
    print(f"stock_universe 表已更新")


if __name__ == "__main__":
    main()
