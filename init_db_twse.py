"""
init_db_twse.py
從 TWSE 官方 API 抓歷史資料，存進 banshi.db。
完全沿用現有 table schema，不動分析層。

使用方式：
  python init_db_twse.py          # 預設 days=5 測試
  python init_db_twse.py --days 90
"""

import sqlite3
import time
import argparse
import os
from datetime import datetime, timedelta
from io import StringIO
from typing import Optional

import requests
import pandas as pd

DB_PATH = "banshi.db"
UNIVERSE_PATH = "universe.csv"
COMPANIES_PATH = "companies.csv"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"}


# ──────────────────────────────────────────────
# 白名單載入
# ──────────────────────────────────────────────

def load_universe() -> set:
    ids = set()
    for path in (UNIVERSE_PATH, COMPANIES_PATH):
        if os.path.exists(path):
            df = pd.read_csv(path, dtype=str)
            if "stock_id" in df.columns:
                ids.update(df["stock_id"].str.strip().tolist())
    return ids


# ──────────────────────────────────────────────
# TWSE 下載
# ──────────────────────────────────────────────

def _fetch_csv(url: str, date_str: str, header_keyword: str) -> Optional[pd.DataFrame]:
    """
    下載 TWSE CSV（big5），自動找含 header_keyword 的那行作為 header。
    失敗或無資料時回傳 None。
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [WARN] 下載失敗 {url[:60]}… → {e}")
        return None

    try:
        text = resp.content.decode("big5", errors="replace")
    except Exception as e:
        print(f"  [WARN] 解碼失敗 → {e}")
        return None

    lines = text.splitlines()

    # 找含 header_keyword 的行號
    header_idx = None
    for i, line in enumerate(lines):
        if header_keyword in line:
            header_idx = i
            break

    if header_idx is None:
        print(f"  [WARN] {date_str} 找不到 '{header_keyword}'，可能是休假日或無資料")
        return None

    # 只取 header 之後的資料行（去掉尾部雜訊）
    data_lines = lines[header_idx:]
    # TWSE CSV 尾部常有「備註」、空行，過濾掉欄位數差太多的行
    try:
        df = pd.read_csv(StringIO("\n".join(data_lines)), thousands=",", dtype=str)
    except Exception as e:
        print(f"  [WARN] {date_str} 解析 CSV 失敗 → {e}")
        return None

    if df.empty:
        return None

    df.columns = df.columns.str.strip()
    return df


def _clean_numeric(series: pd.Series) -> pd.Series:
    """移除千分位逗號並轉為數值。"""
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )


# ---------- 1. 股價 ----------

def download_price(date_str: str) -> Optional[pd.DataFrame]:
    url = (
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX"
        f"?response=csv&date={date_str}&type=ALLBUT0999"
    )
    df = _fetch_csv(url, date_str, "證券代號")
    if df is None:
        return None

    required = {"證券代號", "開盤價", "最高價", "最低價", "收盤價", "成交股數"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        print(f"  [WARN] price {date_str} 缺欄位：{missing}")
        return None

    out = pd.DataFrame()
    out["stock_id"] = df["證券代號"].astype(str).str.strip()
    out["open"]     = _clean_numeric(df["開盤價"])
    out["high"]     = _clean_numeric(df["最高價"])
    out["low"]      = _clean_numeric(df["最低價"])
    out["close"]    = _clean_numeric(df["收盤價"])
    out["volume"]   = _clean_numeric(df["成交股數"])

    # 過濾非純數字 stock_id（去掉小計、備註行）
    out = out[out["stock_id"].str.match(r"^\d{4,6}$", na=False)]
    out = out.dropna(subset=["close"])
    return out if not out.empty else None


# ---------- 2. 法人 ----------

def download_institutional(date_str: str) -> Optional[pd.DataFrame]:
    url = (
        f"https://www.twse.com.tw/fund/T86"
        f"?response=csv&date={date_str}&selectType=ALL"
    )
    df = _fetch_csv(url, date_str, "證券代號")
    if df is None:
        return None

    col_map = {
        "外陸資買進股數(不含外資自營商)": "foreign_buy",
        "外陸資賣出股數(不含外資自營商)": "foreign_sell",
        "投信買進股數":                    "investment_buy",
        "投信賣出股數":                    "investment_sell",
        "自營商買賣超股數":                "dealer_net",
    }
    missing = set(col_map) - set(df.columns)
    if "證券代號" not in df.columns or missing:
        print(f"  [WARN] inst {date_str} 缺欄位：{missing or '證券代號'}")
        return None

    out = pd.DataFrame()
    out["stock_id"] = df["證券代號"].astype(str).str.strip()
    for src, dst in col_map.items():
        out[dst] = _clean_numeric(df[src])

    out["foreign_net"]    = out["foreign_buy"]    - out["foreign_sell"]
    out["investment_net"] = out["investment_buy"] - out["investment_sell"]

    out = out[out["stock_id"].str.match(r"^\d{4,6}$", na=False)]
    out = out.dropna(subset=["foreign_net"])
    return out if not out.empty else None


# ---------- 3. 融資 ----------

def download_margin(date_str: str) -> Optional[pd.DataFrame]:
    url = (
        f"https://www.twse.com.tw/exchangeReport/MI_MARGN"
        f"?response=csv&date={date_str}&selectType=ALL"
    )
    # 實際 header 欄位名稱是「代號」，不是「股票代號」
    df = _fetch_csv(url, date_str, "代號")
    if df is None:
        return None

    # 融資/融券各有「今日餘額」，用 iloc 按位置取，避免重名衝突
    # 欄位結構：col0=代號, col5=融資今日餘額, col11=融券今日餘額
    if df.shape[1] < 12:
        print(f"  [WARN] margin {date_str} 欄位數不足（{df.shape[1]}），跳過")
        return None

    out = pd.DataFrame()
    # ="XXXX" 格式（Excel 防科學記號）→ 清理成純數字字串
    out["stock_id"]       = (df.iloc[:, 0].astype(str)
                               .str.replace('="', "", regex=False)
                               .str.replace('"', "", regex=False)
                               .str.strip())
    out["margin_balance"] = _clean_numeric(df.iloc[:, 5])
    out["short_balance"]  = _clean_numeric(df.iloc[:, 11])

    out = out[out["stock_id"].str.match(r"^\d{4,6}$", na=False)]
    out = out.dropna(subset=["margin_balance", "short_balance"])
    return out if not out.empty else None


# ──────────────────────────────────────────────
# DB 寫入
# ──────────────────────────────────────────────

def save_to_db(conn: sqlite3.Connection, df: pd.DataFrame, table: str, date_iso: str, universe: set):
    """過濾白名單後以 INSERT OR REPLACE 寫入指定 table。"""
    df = df[df["stock_id"].isin(universe)].copy()
    if df.empty:
        return 0

    df["date"] = date_iso

    col_order = {
        "price_history":         ["stock_id", "date", "open", "high", "low", "close", "volume"],
        "institutional_history": ["stock_id", "date", "foreign_buy", "foreign_sell", "foreign_net",
                                  "investment_buy", "investment_sell", "investment_net", "dealer_net"],
        "margin_history":        ["stock_id", "date", "margin_balance", "short_balance"],
    }
    cols = col_order[table]
    df = df[cols]

    placeholders = ", ".join(["?"] * len(cols))
    col_names    = ", ".join(cols)
    sql = f"INSERT OR REPLACE INTO {table} ({col_names}) VALUES ({placeholders})"

    rows = [tuple(row) for row in df.itertuples(index=False)]
    conn.executemany(sql, rows)
    return len(rows)


# ──────────────────────────────────────────────
# 主程式
# ──────────────────────────────────────────────

def trading_dates(days: int) -> list[str]:
    """往前推 days 個日曆日，取週一～週五。"""
    today = datetime.today().date()
    result = []
    d = today - timedelta(days=1)  # 從昨天開始（今天盤中資料不完整）
    while len(result) < days:
        if d.weekday() < 5:        # 0=Mon … 4=Fri
            result.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return list(reversed(result))  # 舊 → 新


def main(days: int = 5):
    universe = load_universe()
    print(f"[INFO] 白名單股票數：{len(universe)}")
    print(f"[INFO] 目標天數：{days}\n")

    dates = trading_dates(days)
    conn  = sqlite3.connect(DB_PATH)

    total_price = total_inst = total_margin = 0
    success_days = 0

    for date_str in dates:
        date_iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        print(f"── {date_iso} ──")

        day_ok = False

        # 1. 股價
        try:
            df = download_price(date_str)
            if df is not None:
                n = save_to_db(conn, df, "price_history", date_iso, universe)
                print(f"  price_history:         {n} 筆")
                total_price += n
                day_ok = True
        except Exception as e:
            print(f"  [ERROR] price {date_iso} → {e}")

        # 2. 法人
        try:
            df = download_institutional(date_str)
            if df is not None:
                n = save_to_db(conn, df, "institutional_history", date_iso, universe)
                print(f"  institutional_history: {n} 筆")
                total_inst += n
                day_ok = True
        except Exception as e:
            print(f"  [ERROR] inst {date_iso} → {e}")

        # 3. 融資
        try:
            df = download_margin(date_str)
            if df is not None:
                n = save_to_db(conn, df, "margin_history", date_iso, universe)
                print(f"  margin_history:        {n} 筆")
                total_margin += n
                day_ok = True
        except Exception as e:
            print(f"  [ERROR] margin {date_iso} → {e}")

        conn.commit()
        if day_ok:
            success_days += 1

        time.sleep(1.0)

    # ── 結果摘要 ──
    price_max  = conn.execute("SELECT MAX(date) FROM price_history").fetchone()[0]
    price_cnt  = conn.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
    db_size_mb = os.path.getsize(DB_PATH) / 1024 / 1024

    conn.close()

    print("\n══════════════════════════════")
    print(f"完成天數：    {success_days} / {len(dates)}")
    print(f"price_history 新增：  {total_price} 筆（DB 合計 {price_cnt} 筆）")
    print(f"institutional 新增：  {total_inst} 筆")
    print(f"margin 新增：         {total_margin} 筆")
    print(f"最新日期：    {price_max}")
    print(f"DB 大小：     {db_size_mb:.2f} MB")
    print("══════════════════════════════")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=5, help="往前抓幾個交易日（預設 5）")
    args = parser.parse_args()
    main(days=args.days)
