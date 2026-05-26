from typing import Optional
"""
backfill_db.py — 補回指定日期範圍內缺漏的 price/institutional/margin 歷史資料

用法：
  python3 backfill_db.py                     # 自動補齊：DB MAX(date) 之後到今天前一個交易日
  python3 backfill_db.py 2026-05-16          # 指定起點
  python3 backfill_db.py 2026-05-16 2026-05-23  # 指定起點和終點

只補 price_history / institutional_history / margin_history。
shareholding_history 需要 FinMind API，另外處理。
"""
import os
import sys
import sqlite3
from datetime import datetime, timedelta

import requests
from io import StringIO

import pandas as pd

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_PATH, "banshi.db")


# ── 共用函式（直接從 update_daily_data.py 複製，不 import 避免 side effect）─────

def _roc_date(date_iso: str) -> str:
    dt = datetime.strptime(date_iso, "%Y-%m-%d")
    return f"{dt.year-1911:03d}/{dt.month:02d}/{dt.day:02d}"


def _twse_fetch(date_str: str, dataset: str) -> str:
    urls = {
        "price":         f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date={date_str}&type=ALLBUT0999",
        "institutional": f"https://www.twse.com.tw/fund/T86?response=csv&date={date_str}&selectType=ALL",
        "margin":        f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=csv&date={date_str}&selectType=ALL",
    }
    try:
        r = requests.get(urls[dataset], timeout=20)
        r.encoding = "big5"
        text = r.text.strip()
        if ("<html" in text.lower() or len(text) < 800 or "無資料" in text
                or "很抱歉" in text or "查詢過於頻繁" in text):
            return ""
        if dataset == "price" and "證券代號" not in text:
            return ""
        if dataset == "institutional" and "證券代號" not in text:
            return ""
        if dataset == "margin" and "代號" not in text:
            return ""
        return text
    except Exception as e:
        print(f"    TWSE {dataset} 失敗：{e}")
        return ""


def _parse_price(text: str, universe_set: set) -> pd.DataFrame:
    try:
        lines = text.split("\n")
        start_idx = next((i for i, l in enumerate(lines) if "證券代號" in l), None)
        if start_idx is None:
            return pd.DataFrame()
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])))
        volume_col = next((c for c in df.columns if "成交" in c and ("股數" in c or "成交量" in c)), None)
        if volume_col is None:
            return pd.DataFrame()
        df = df.rename(columns={"證券代號": "stock_id", "開盤價": "open", "最高價": "high",
                                 "最低價": "low", "收盤價": "close", volume_col: "volume"})
        df["stock_id"] = (df["stock_id"].astype(str)
                          .str.replace('="', "").str.replace('"', "").str.strip().str.zfill(4))
        df = df[df["stock_id"].isin(universe_set)]
        df = df[["stock_id", "open", "high", "low", "close", "volume"]].copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
        return df.dropna(subset=["close"])
    except Exception:
        return pd.DataFrame()


def _parse_institutional(text: str, universe_set: set) -> pd.DataFrame:
    try:
        df = pd.read_csv(StringIO(text), skiprows=1)
        col_map = {
            "證券代號": "stock_id",
            "外陸資買進股數(不含外資自營商)": "foreign_buy",
            "外陸資賣出股數(不含外資自營商)": "foreign_sell",
            "投信買進股數": "investment_buy",
            "投信賣出股數": "investment_sell",
            "自營商買賣超股數": "dealer_net",
        }
        df = df.rename(columns=col_map)
        if "stock_id" not in df.columns:
            return pd.DataFrame()
        df["stock_id"] = (df["stock_id"].astype(str)
                          .str.replace('="', "").str.replace('"', "").str.strip().str.zfill(4))
        df = df[df["stock_id"].isin(universe_set)]
        for col in ["foreign_buy", "foreign_sell", "investment_buy", "investment_sell", "dealer_net"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
        df["foreign_net"]    = df.get("foreign_buy", 0) - df.get("foreign_sell", 0)
        df["investment_net"] = df.get("investment_buy", 0) - df.get("investment_sell", 0)
        return df[["stock_id", "foreign_buy", "foreign_sell", "foreign_net",
                   "investment_buy", "investment_sell", "investment_net", "dealer_net"]].copy()
    except Exception:
        return pd.DataFrame()


def _parse_margin(text: str, universe_set: set) -> pd.DataFrame:
    try:
        lines = text.split("\n")
        start_idx = next((i for i, l in enumerate(lines) if "代號" in l), None)
        if start_idx is None:
            return pd.DataFrame()
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), header=0)
        df.iloc[:, 0] = (df.iloc[:, 0].astype(str)
                         .str.replace('="', "").str.replace('"', "").str.strip().str.zfill(4))
        df = df[df.iloc[:, 0].isin(universe_set)]
        if df.empty:
            return pd.DataFrame()
        margin_col = next((c for c in df.columns if "融資" in str(c) and "餘額" in str(c)), None)
        short_col  = next((c for c in df.columns if "融券" in str(c) and "餘額" in str(c)), None)
        result = pd.DataFrame()
        result["stock_id"] = df.iloc[:, 0].values
        result["margin_balance"] = pd.to_numeric(
            (df[margin_col] if margin_col else df.iloc[:, 5]).astype(str).str.replace(",", ""), errors="coerce")
        result["short_balance"] = pd.to_numeric(
            (df[short_col] if short_col else df.iloc[:, 11]).astype(str).str.replace(",", ""), errors="coerce")
        return result.dropna(subset=["stock_id"])
    except Exception:
        return pd.DataFrame()


def _fetch_tpex_price(date_iso: str, universe_set: set) -> pd.DataFrame:
    try:
        date_param = date_iso.replace("-", "")
        r = requests.get(
            f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes?date={date_param}",
            timeout=15,
        )
        data = r.json()
        if not isinstance(data, list):
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df = df.rename(columns={"SecuritiesCompanyCode": "stock_id",
                                 "Open": "open", "High": "high", "Low": "low",
                                 "Close": "close", "TradingShares": "volume"})
        df["stock_id"] = df["stock_id"].astype(str).str.strip().str.zfill(4)
        df = df[df["stock_id"].isin(universe_set)]
        df = df[["stock_id", "open", "high", "low", "close", "volume"]].copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
        return df.dropna(subset=["close", "stock_id"])
    except Exception:
        return pd.DataFrame()


def _fetch_tpex_institutional(date_iso: str, universe_set: set) -> pd.DataFrame:
    def to_num(v):
        val = pd.to_numeric(str(v).replace(",", ""), errors="coerce")
        return 0 if pd.isna(val) else val
    try:
        d = _roc_date(date_iso)
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
            f"?l=zh-tw&o=json&se=EW&t=D&d={d}&s=0,asc",
            timeout=15,
        )
        raw = r.json()
        rows = raw["tables"][0]["data"]
        if not rows:
            return pd.DataFrame()
        records = []
        for row in rows:
            if len(row) < 23:
                continue
            sid = str(row[0]).strip().zfill(4)
            if sid not in universe_set:
                continue
            records.append({
                "stock_id": sid,
                "foreign_buy": to_num(row[17]), "foreign_sell": to_num(row[18]),
                "foreign_net": to_num(row[19]),
                "investment_buy": to_num(row[5]), "investment_sell": to_num(row[6]),
                "investment_net": to_num(row[7]),
                "dealer_net": to_num(row[22]),
            })
        df = pd.DataFrame(records)
        return df.dropna(subset=["stock_id"]) if not df.empty else df
    except Exception:
        return pd.DataFrame()


def _fetch_tpex_margin(date_iso: str, universe_set: set) -> pd.DataFrame:
    def to_num(v):
        val = pd.to_numeric(str(v).replace(",", ""), errors="coerce")
        return 0 if pd.isna(val) else val
    try:
        d = _roc_date(date_iso)
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php"
            f"?l=zh-tw&o=json&d={d}",
            timeout=15,
        )
        raw = r.json()
        rows = raw["tables"][0]["data"]
        if not rows:
            return pd.DataFrame()
        records = []
        for row in rows:
            if len(row) < 15:
                continue
            sid = str(row[0]).strip().zfill(4)
            if sid not in universe_set:
                continue
            records.append({"stock_id": sid, "margin_balance": to_num(row[6]), "short_balance": to_num(row[14])})
        df = pd.DataFrame(records)
        return df.dropna(subset=["stock_id"]) if not df.empty else df
    except Exception:
        return pd.DataFrame()


# ── 主邏輯 ──────────────────────────────────────────────────────────────────────

def _trading_days_between(start: str, end: str) -> list[str]:
    """回傳 start ~ end 之間的工作日（簡單用 BDay，不含台灣國定假日）。"""
    rng = pd.bdate_range(start=start, end=end)
    return [d.strftime("%Y-%m-%d") for d in rng]


def _db_max_date(table: str) -> Optional[str]:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(f"SELECT MAX(date) FROM {table} WHERE date IS NOT NULL").fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def _dates_missing_in_db(trading_days: list[str]) -> list[str]:
    """回傳哪些工作日在 price_history 完全沒有資料（不論筆數多寡）。"""
    conn = sqlite3.connect(DB_PATH)
    existing = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM price_history WHERE date IS NOT NULL"
        ).fetchall()
    }
    conn.close()
    return [d for d in trading_days if d not in existing]


def backfill(start_date: Optional[str] = None, end_date: Optional[str] = None) -> None:
    universe_df  = pd.read_csv(os.path.join(BASE_PATH, "universe.csv"), dtype=str)
    universe_set = {str(x).zfill(4) for x in universe_df["stock_id"].tolist()}

    # 決定補洞範圍
    if start_date is None:
        db_max = _db_max_date("price_history") or "2026-01-01"
        start_date = (pd.Timestamp(db_max) + pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = (pd.Timestamp.today() - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

    trading_days = _trading_days_between(start_date, end_date)
    if not trading_days:
        print(f"[backfill] 範圍 {start_date}~{end_date} 無工作日，跳過。")
        return

    missing = _dates_missing_in_db(trading_days)
    if not missing:
        print(f"[backfill] {start_date}~{end_date} 共 {len(trading_days)} 個工作日，DB 已全部有資料。")
        return

    print(f"[backfill] 準備補 {len(missing)} 個缺漏日期：{missing}")

    conn   = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total_price = total_inst = total_margin = 0

    for dt in missing:
        date_str = dt.replace("-", "")
        print(f"\n  ── {dt} ──────────────────────────")

        # 股價
        text = _twse_fetch(date_str, "price")
        df_p = _parse_price(text, universe_set).drop_duplicates(subset=["stock_id"])
        tpex_p = _fetch_tpex_price(dt, universe_set)
        if not tpex_p.empty:
            existing_ids = set(df_p["stock_id"]) if not df_p.empty else set()
            df_p = pd.concat([df_p, tpex_p[~tpex_p["stock_id"].isin(existing_ids)]], ignore_index=True)
        if not df_p.empty:
            df_p["date"] = dt
            cursor.executemany(
                "INSERT OR IGNORE INTO price_history (stock_id,date,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)",
                df_p[["stock_id", "date", "open", "high", "low", "close", "volume"]].values.tolist(),
            )
            total_price += len(df_p)
        print(f"    股價：{len(df_p)} 筆")

        # 法人
        text = _twse_fetch(date_str, "institutional")
        df_i = _parse_institutional(text, universe_set).drop_duplicates(subset=["stock_id"])
        tpex_i = _fetch_tpex_institutional(dt, universe_set)
        if not tpex_i.empty:
            existing_ids = set(df_i["stock_id"]) if not df_i.empty else set()
            df_i = pd.concat([df_i, tpex_i[~tpex_i["stock_id"].isin(existing_ids)]], ignore_index=True)
        if not df_i.empty:
            df_i["date"] = dt
            cursor.executemany(
                "INSERT OR IGNORE INTO institutional_history "
                "(stock_id,date,foreign_buy,foreign_sell,foreign_net,investment_buy,investment_sell,investment_net,dealer_net) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                df_i[["stock_id", "date", "foreign_buy", "foreign_sell", "foreign_net",
                       "investment_buy", "investment_sell", "investment_net", "dealer_net"]].values.tolist(),
            )
            total_inst += len(df_i)
        print(f"    法人：{len(df_i)} 筆")

        # 融資
        text = _twse_fetch(date_str, "margin")
        df_m = _parse_margin(text, universe_set).drop_duplicates(subset=["stock_id"])
        tpex_m = _fetch_tpex_margin(dt, universe_set)
        if not tpex_m.empty:
            existing_ids = set(df_m["stock_id"]) if not df_m.empty else set()
            df_m = pd.concat([df_m, tpex_m[~tpex_m["stock_id"].isin(existing_ids)]], ignore_index=True)
        if not df_m.empty:
            df_m["date"] = dt
            cursor.executemany(
                "INSERT OR IGNORE INTO margin_history (stock_id,date,margin_balance,short_balance) VALUES (?,?,?,?)",
                df_m[["stock_id", "date", "margin_balance", "short_balance"]].values.tolist(),
            )
            total_margin += len(df_m)
        print(f"    融資：{len(df_m)} 筆")

        conn.commit()

    conn.close()

    print(f"\n[backfill] 完成。共補入：股價 {total_price} 筆 / 法人 {total_inst} 筆 / 融資 {total_margin} 筆")

    # 驗證
    for table, label in [("price_history", "price"), ("institutional_history", "inst"), ("margin_history", "margin")]:
        new_max = _db_max_date(table)
        print(f"  {label} MAX(date) = {new_max}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 0:
        backfill()
    elif len(args) == 1:
        backfill(start_date=args[0])
    elif len(args) == 2:
        backfill(start_date=args[0], end_date=args[1])
    else:
        print("用法：python3 backfill_db.py [start_date] [end_date]")
        sys.exit(1)
