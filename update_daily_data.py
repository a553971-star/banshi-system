"""
update_daily_data.py — 每日更新 price_history / institutional_history / margin_history
用法：python3 update_daily_data.py [YYYY-MM-DD]
      省略日期則預設今天
"""
import os
import sys
import sqlite3
from datetime import datetime, timedelta, date as _date
import requests
from io import StringIO

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


def get_db_last_date(db_path: str) -> str:
    """
    查詢三張主表的最新日期，取最舊的那張作為同步基準。
    若無資料則回傳 60 個交易日前（初始化模式）。
    """
    tables = ["price_history", "institutional_history", "margin_history"]
    dates = []
    try:
        conn = sqlite3.connect(db_path)
        for table in tables:
            try:
                row = conn.execute(f"SELECT MAX(date) FROM {table}").fetchone()
                if row and row[0]:
                    dates.append(row[0])
            except Exception:
                pass
        conn.close()
    except Exception:
        pass

    if dates:
        return min(dates)  # 取最舊的，確保所有表都同步
    return (pd.Timestamp.today() - pd.tseries.offsets.BDay(60)).strftime("%Y-%m-%d")


# ──────────────────────────────────────────────
# TWSE 下載 / 解析（免費，無 API 額度）
# ──────────────────────────────────────────────

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
        print(f"  TWSE {dataset} 下載失敗：{e}")
        return ""


def _parse_price(text: str, universe_set: set) -> pd.DataFrame:
    try:
        lines = text.split("\n")
        start_idx = next((i for i, l in enumerate(lines) if "證券代號" in l), None)
        if start_idx is None:
            return pd.DataFrame()
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), skiprows=0)
        volume_col = next((c for c in df.columns if "成交" in c and ("股數" in c or "成交量" in c)), None)
        if volume_col is None:
            return pd.DataFrame()
        df = df.rename(columns={"證券代號": "stock_id", "開盤價": "open", "最高價": "high",
                                 "最低價": "low", "收盤價": "close", volume_col: "volume"})
        df["stock_id"] = (df["stock_id"].astype(str)
                          .str.replace('="', "", regex=False).str.replace('"', "", regex=False)
                          .str.strip().str.zfill(4))
        df = df[df["stock_id"].isin(universe_set)]
        df = df[["stock_id", "open", "high", "low", "close", "volume"]].copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
        return df.dropna(subset=["close"])
    except Exception as e:
        print(f"  解析股價失敗：{e}")
        return pd.DataFrame()


def _parse_institutional(text: str, universe_set: set) -> pd.DataFrame:
    try:
        df = pd.read_csv(StringIO(text), skiprows=1)
        col_map = {
            "證券代號":                          "stock_id",
            "外陸資買進股數(不含外資自營商)":    "foreign_buy",
            "外陸資賣出股數(不含外資自營商)":    "foreign_sell",
            "投信買進股數":                       "investment_buy",
            "投信賣出股數":                       "investment_sell",
            "自營商買賣超股數":                   "dealer_net",
        }
        df = df.rename(columns=col_map)
        if "stock_id" not in df.columns:
            return pd.DataFrame()
        df["stock_id"] = (df["stock_id"].astype(str)
                          .str.replace('="', "", regex=False).str.replace('"', "", regex=False)
                          .str.strip().str.zfill(4))
        df = df[df["stock_id"].isin(universe_set)]
        for col in ["foreign_buy", "foreign_sell", "investment_buy", "investment_sell", "dealer_net"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
        df["foreign_net"]    = df.get("foreign_buy", 0)    - df.get("foreign_sell", 0)
        df["investment_net"] = df.get("investment_buy", 0) - df.get("investment_sell", 0)
        return df[["stock_id", "foreign_buy", "foreign_sell", "foreign_net",
                   "investment_buy", "investment_sell", "investment_net", "dealer_net"]].copy()
    except Exception as e:
        print(f"  解析法人失敗：{e}")
        return pd.DataFrame()


def _parse_margin(text: str, universe_set: set) -> pd.DataFrame:
    try:
        lines = text.split("\n")
        start_idx = next((i for i, l in enumerate(lines) if "代號" in l), None)
        if start_idx is None:
            return pd.DataFrame()
        df = pd.read_csv(StringIO("\n".join(lines[start_idx:])), skiprows=0, header=0)
        df.iloc[:, 0] = (df.iloc[:, 0].astype(str)
                         .str.replace('="', "", regex=False).str.replace('"', "", regex=False)
                         .str.strip().str.zfill(4))
        df = df[df.iloc[:, 0].isin(universe_set)]
        if df.empty:
            return pd.DataFrame()
        margin_col = next((c for c in df.columns if "融資" in str(c) and "餘額" in str(c)), None)
        short_col  = next((c for c in df.columns if "融券" in str(c) and "餘額" in str(c)), None)
        result = pd.DataFrame()
        result["stock_id"] = df.iloc[:, 0].values
        if margin_col:
            result["margin_balance"] = pd.to_numeric(df[margin_col].astype(str).str.replace(",", "", regex=False), errors="coerce")
        else:
            result["margin_balance"] = pd.to_numeric(df.iloc[:, 5].astype(str).str.replace(",", "", regex=False), errors="coerce")
        if short_col:
            result["short_balance"] = pd.to_numeric(df[short_col].astype(str).str.replace(",", "", regex=False), errors="coerce")
        else:
            result["short_balance"] = pd.to_numeric(df.iloc[:, 11].astype(str).str.replace(",", "", regex=False), errors="coerce")
        return result.dropna(subset=["stock_id"])
    except Exception as e:
        print(f"  解析融資失敗：{e}")
        return pd.DataFrame()


# ──────────────────────────────────────────────
# TPEx 下載（上櫃股票補充）
# ──────────────────────────────────────────────

def _roc_date(date_iso: str) -> str:
    """把 YYYY-MM-DD 轉成民國年格式 115/05/15"""
    from datetime import datetime as _dt
    dt = _dt.strptime(date_iso, "%Y-%m-%d")
    return f"{dt.year-1911:03d}/{dt.month:02d}/{dt.day:02d}"


def _fetch_tpex_price(date_iso: str, universe_set: set) -> pd.DataFrame:
    """抓 TPEx 當日股價（OpenAPI JSON）"""
    try:
        date_param = date_iso.replace("-", "")  # YYYYMMDD
        r = requests.get(
            f"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes?date={date_param}",
            timeout=15
        )
        try:
            data = r.json()
            if not isinstance(data, list):
                print("  ⚠️ TPEx 股價回傳格式異常")
                return pd.DataFrame()
        except Exception:
            print("  ⚠️ TPEx 股價 JSON 解析失敗")
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df = df.rename(columns={
            "SecuritiesCompanyCode": "stock_id",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "TradingShares": "volume",
        })
        df["stock_id"] = df["stock_id"].astype(str).str.strip().str.zfill(4)
        df = df[df["stock_id"].isin(universe_set)]
        df = df[["stock_id", "open", "high", "low", "close", "volume"]].copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "", regex=False), errors="coerce")
        return df.dropna(subset=["close", "stock_id"])
    except Exception as e:
        print(f"  TPEx 股價抓取失敗：{e}")
        return pd.DataFrame()


def _fetch_tpex_institutional(date_iso: str, universe_set: set) -> pd.DataFrame:
    """抓 TPEx 三大法人（JSON）"""
    def to_num(v):
        val = pd.to_numeric(str(v).replace(",", ""), errors="coerce")
        return 0 if pd.isna(val) else val
    try:
        d = _roc_date(date_iso)
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=json&se=EW&t=D&d={d}&s=0,asc",
            timeout=15
        )
        try:
            raw = r.json()
            rows = raw["tables"][0]["data"]
        except Exception:
            print("  ⚠️ TPEx 法人 JSON 解析失敗")
            return pd.DataFrame()
        if not rows:
            print("  ⚠️ TPEx 法人資料為空")
            return pd.DataFrame()
        records = []
        for row in rows:
            if len(row) < 23:
                continue
            stock_id = str(row[0]).strip().zfill(4)
            if stock_id not in universe_set:
                continue
            records.append({
                "stock_id":        stock_id,
                "foreign_buy":     to_num(row[17]),
                "foreign_sell":    to_num(row[18]),
                "foreign_net":     to_num(row[19]),
                "investment_buy":  to_num(row[5]),
                "investment_sell": to_num(row[6]),
                "investment_net":  to_num(row[7]),
                "dealer_net":      to_num(row[22]),
            })
        df = pd.DataFrame(records)
        return df.dropna(subset=["stock_id"]) if not df.empty else df
    except Exception as e:
        print(f"  TPEx 法人抓取失敗：{e}")
        return pd.DataFrame()


def _fetch_tpex_margin(date_iso: str, universe_set: set) -> pd.DataFrame:
    """抓 TPEx 融資融券（JSON）"""
    def to_num(v):
        val = pd.to_numeric(str(v).replace(",", ""), errors="coerce")
        return 0 if pd.isna(val) else val
    try:
        d = _roc_date(date_iso)
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=json&d={d}",
            timeout=15
        )
        try:
            raw = r.json()
            rows = raw["tables"][0]["data"]
        except Exception:
            print("  ⚠️ TPEx 融資 JSON 解析失敗")
            return pd.DataFrame()
        if not rows:
            print("  ⚠️ TPEx 融資資料為空")
            return pd.DataFrame()
        records = []
        for row in rows:
            if len(row) < 15:
                continue
            stock_id = str(row[0]).strip().zfill(4)
            if stock_id not in universe_set:
                continue
            records.append({
                "stock_id":       stock_id,
                "margin_balance": to_num(row[6]),
                "short_balance":  to_num(row[14]),
            })
        df = pd.DataFrame(records)
        return df.dropna(subset=["stock_id"]) if not df.empty else df
    except Exception as e:
        print(f"  TPEx 融資抓取失敗：{e}")
        return pd.DataFrame()


def _ai_fetch_one_day(d_iso: str, target_set: set, cursor) -> int:
    """補抓單日股價到 price_history（INSERT OR IGNORE），回傳寫入筆數。"""
    written = 0
    d_nodash = d_iso.replace("-", "")

    frames: list = []
    try:
        text = _twse_fetch(d_nodash, "price")
        if text:
            df = _parse_price(text, target_set)
            if not df.empty:
                frames.append(df)
    except Exception:
        pass
    try:
        df_t = _fetch_tpex_price(d_iso, target_set)
        if not df_t.empty:
            frames.append(df_t)
    except Exception:
        pass

    if not frames:
        return 0

    df_all = (pd.concat(frames, ignore_index=True)
              .drop_duplicates(subset=["stock_id"]))
    df_all = df_all[df_all["stock_id"].isin(target_set)]

    for _, row in df_all.iterrows():
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO price_history
                   (stock_id, date, open, high, low, close, volume)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (row["stock_id"], d_iso,
                 row.get("open"), row.get("high"), row.get("low"),
                 row.get("close"), row.get("volume")),
            )
            written += 1
        except Exception:
            pass
    return written


def update_ai_supplement(date_str: str) -> None:
    """補抓 ai_supply_chain.csv 裡不在 universe 的股票股價。

    - date_str：YYYY-MM-DD
    - 先試 TWSE，再試 TPEx，合併後 INSERT OR IGNORE 寫入 price_history
    - 自己開新連線，不依賴 main() 的 conn（main 結束時已 close）
    - 筆數 < 40 的股票自動補回 80 自然日歷史
    """
    from datetime import datetime, timedelta
    import pandas as pd

    ai_csv  = os.path.join(BASE_PATH, "ai_supply_chain.csv")
    uni_csv = os.path.join(BASE_PATH, "universe.csv")

    if not os.path.exists(ai_csv) or not os.path.exists(uni_csv):
        return

    ai_set  = set(pd.read_csv(ai_csv,  dtype=str)["stock_id"].str.zfill(4).dropna())
    uni_set = set(pd.read_csv(uni_csv, dtype=str)["stock_id"].str.zfill(4).dropna())
    ai_only = ai_set - uni_set

    if not ai_only:
        return

    conn2   = sqlite3.connect(DB_PATH)
    cursor2 = conn2.cursor()

    # ── 查各股現有筆數，分兩組 ────────────────────────────────────────────────
    placeholders = ",".join("?" * len(ai_only))
    rows = cursor2.execute(
        f"SELECT stock_id, COUNT(*) FROM price_history "
        f"WHERE stock_id IN ({placeholders}) GROUP BY stock_id",
        list(ai_only),
    ).fetchall()
    count_map = {r[0]: r[1] for r in rows}

    needs_backfill = {sid for sid in ai_only if count_map.get(sid, 0) < 40}
    normal_update  = ai_only - needs_backfill

    # ── 一般補抓（只補 date_str 當天）────────────────────────────────────────
    twse_n = tpex_n = normal_written = 0
    if normal_update:
        frames: list = []
        d_nodash = date_str.replace("-", "")
        try:
            text = _twse_fetch(d_nodash, "price")
            if text:
                df = _parse_price(text, normal_update)
                if not df.empty:
                    twse_n = len(df)
                    frames.append(df)
        except Exception as e:
            print(f"  AI補抓 TWSE 失敗：{e}")
        try:
            df_t = _fetch_tpex_price(date_str, normal_update)
            if not df_t.empty:
                tpex_n = len(df_t)
                frames.append(df_t)
        except Exception as e:
            print(f"  AI補抓 TPEx 失敗：{e}")

        if frames:
            df_all = (pd.concat(frames, ignore_index=True)
                      .drop_duplicates(subset=["stock_id"]))
            df_all = df_all[df_all["stock_id"].isin(normal_update)]
            for _, row in df_all.iterrows():
                try:
                    cursor2.execute(
                        """INSERT OR IGNORE INTO price_history
                           (stock_id, date, open, high, low, close, volume)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (row["stock_id"], date_str,
                         row.get("open"), row.get("high"), row.get("low"),
                         row.get("close"), row.get("volume")),
                    )
                    normal_written += 1
                except Exception:
                    pass

    # ── 歷史回補（筆數 < 40 的股票補 80 自然日）──────────────────────────────
    bf_written = 0
    if needs_backfill:
        end_dt   = datetime.strptime(date_str, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=80)
        day_list = []
        cur_dt = start_dt
        while cur_dt <= end_dt:
            if cur_dt.weekday() < 5:          # 跳過週末
                day_list.append(cur_dt.strftime("%Y-%m-%d"))
            cur_dt += timedelta(days=1)

        print(f"  AI歷史回補：{len(needs_backfill)} 檔，共 {len(day_list)} 個交易日...")
        for i, d_iso in enumerate(day_list, 1):
            bf_written += _ai_fetch_one_day(d_iso, needs_backfill, cursor2)
            if i % 10 == 0 or i == len(day_list):
                print(f"  AI歷史回補：{d_iso} 完成（{i}/{len(day_list)} 天）")
        conn2.commit()

    conn2.commit()
    conn2.close()

    # ── 統一印出結果 ──────────────────────────────────────────────────────────
    if needs_backfill:
        print(
            f"  AI補抓股價：TWSE {twse_n} 筆 + TPEx {tpex_n} 筆，共 {normal_written} 檔寫入"
            f"（含歷史回補 {len(needs_backfill)} 檔 × 約60天）"
        )
    else:
        print(f"  AI補抓股價：TWSE {twse_n} 筆 + TPEx {tpex_n} 筆，共 {normal_written} 檔寫入")


def main():
    import pandas as pd
    end_date = (pd.Timestamp.today() - pd.tseries.offsets.BDay(1)).strftime("%Y-%m-%d")

    # 增量更新：查三張表最舊的最新日期，作為同步基準
    db_last_date = get_db_last_date(DB_PATH)

    # 回補 3 個交易日（overlap buffer，修復 API 延遲或漏資料）
    # 若 DB 是空的，db_last_date 會是 60 天前，照常初始化
    start_date = (pd.Timestamp(db_last_date) - pd.tseries.offsets.BDay(3)).strftime("%Y-%m-%d")

    print(f"📅 更新範圍：{start_date} ~ {end_date}（DB 最新：{db_last_date}，回補3天buffer）")

    universe_df = pd.read_csv(UNIVERSE_PATH, dtype=str)
    universe = universe_df["stock_id"].tolist()
    print(f"Universe：{len(universe)} 支")

    # universe_set（型別安全，補 zfill 對齊 TWSE 格式）
    universe_set = {str(x).zfill(4) for x in universe}
    expected_count = len(universe_set)
    date_str = end_date.replace("-", "")

    # 1. 股價（TWSE，免費無額度）
    text = _twse_fetch(date_str, "price")
    df_price = _parse_price(text, universe_set)
    df_price = df_price.drop_duplicates(subset=["stock_id"])
    if len(df_price) < expected_count * 0.5:
        print(f"  ⚠️ 股價 completeness low: {len(df_price)}/{expected_count}（含 OTC 正常偏低）")
    print(f"  股價：{len(df_price)} 筆")
    _tpex_price = _fetch_tpex_price(end_date, universe_set)
    if not _tpex_price.empty:
        _existing_price = set(df_price["stock_id"]) if not df_price.empty else set()
        _tpex_price = _tpex_price[~_tpex_price["stock_id"].isin(_existing_price)]
        df_price = pd.concat([df_price, _tpex_price], ignore_index=True)
    print(f"  股價（含TPEx {len(_tpex_price)} 筆）：共 {len(df_price)} 筆")
    if not df_price.empty:
        df_price["date"] = end_date

    # 2. 法人（TWSE）
    text = _twse_fetch(date_str, "institutional")
    df_inst = _parse_institutional(text, universe_set)
    df_inst = df_inst.drop_duplicates(subset=["stock_id"])
    if len(df_inst) < expected_count * 0.5:
        print(f"  ⚠️ 法人 completeness low: {len(df_inst)}/{expected_count}（含 OTC 正常偏低）")
    print(f"  法人：{len(df_inst)} 筆")
    _tpex_inst = _fetch_tpex_institutional(end_date, universe_set)
    if not _tpex_inst.empty:
        _existing_inst = set(df_inst["stock_id"]) if not df_inst.empty else set()
        _tpex_inst = _tpex_inst[~_tpex_inst["stock_id"].isin(_existing_inst)]
        df_inst = pd.concat([df_inst, _tpex_inst], ignore_index=True)
    print(f"  法人（含TPEx {len(_tpex_inst)} 筆）：共 {len(df_inst)} 筆")
    if not df_inst.empty:
        df_inst["date"] = end_date

    # 3. 融資（TWSE）
    text = _twse_fetch(date_str, "margin")
    df_margin = _parse_margin(text, universe_set)
    df_margin = df_margin.drop_duplicates(subset=["stock_id"])
    if len(df_margin) < expected_count * 0.5:
        print(f"  ⚠️ 融資 completeness low: {len(df_margin)}/{expected_count}（含 OTC 正常偏低）")
    print(f"  融資：{len(df_margin)} 筆")
    _tpex_margin = _fetch_tpex_margin(end_date, universe_set)
    if not _tpex_margin.empty:
        _existing_margin = set(df_margin["stock_id"]) if not df_margin.empty else set()
        _tpex_margin = _tpex_margin[~_tpex_margin["stock_id"].isin(_existing_margin)]
        df_margin = pd.concat([df_margin, _tpex_margin], ignore_index=True)
    print(f"  融資（含TPEx {len(_tpex_margin)} 筆）：共 {len(df_margin)} 筆")
    if not df_margin.empty:
        df_margin["date"] = end_date

    # 4. 外資持股（FinMind，週一/週六才跑）
    from FinMind.data import DataLoader
    api = DataLoader()
    if TOKEN:
        api.login_by_token(api_token=TOKEN)

    import datetime as _dt
    _weekday = _dt.date.today().isoweekday()  # 1=Monday, 6=Saturday
    df_sh = pd.DataFrame()
    if _weekday in (1, 6):
        companies_df = pd.read_csv(os.path.join(BASE_PATH, "companies.csv"), dtype=str)
        companies    = companies_df["stock_id"].tolist()
        chunks = []
        for sid in companies:
            try:
                _raw = api.taiwan_stock_shareholding(stock_id=sid, start_date=end_date, end_date=end_date)
                if not _raw.empty:
                    chunks.append(_raw)
            except Exception:
                pass
        if chunks:
            df_sh_raw = pd.concat(chunks, ignore_index=True)
            df_sh_raw.columns = [c.lower() for c in df_sh_raw.columns]
            df_sh = df_sh_raw.rename(columns={
                "foreigninvestmentshares":      "foreign_shares",
                "foreigninvestmentsharesratio": "foreign_ratio",
            })
            df_sh = df_sh[["stock_id", "date", "foreign_shares", "foreign_ratio"]]
            df_sh = df_sh.drop_duplicates(subset=["stock_id", "date"])
        print(f"  外資持股：{len(df_sh)} 筆（週一/週六更新）")
    else:
        print("  外資持股：跳過（非週一/週六，沿用 shareholding_latest.csv）")

    # 5. 寫入 SQLite（防重複）
    _had_data = not df_price.empty or not df_inst.empty or not df_margin.empty

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    setup_tables(cursor)

    # 清除 NULL 日期殘留（先前 date 欄位未寫入的 bug 遺留）
    for _t in ["price_history", "institutional_history", "margin_history"]:
        res = cursor.execute(f"DELETE FROM {_t} WHERE date IS NULL")
        if res.rowcount > 0:
            print(f"  🧹 清除 NULL 日期殘留：{_t} {res.rowcount} 筆")

    # 診斷：寫入前各表最新日期
    _before_max: dict = {}
    for _t in ["price_history", "institutional_history", "margin_history"]:
        row = cursor.execute(f"SELECT MAX(date) FROM {_t}").fetchone()
        _before_max[_t] = row[0]
    print(
        f"  DB 最新日期（寫入前）："
        f"price={_before_max['price_history']}, "
        f"inst={_before_max['institutional_history']}, "
        f"margin={_before_max['margin_history']}"
    )

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

    # 診斷：寫入後各表最新日期
    _write_ok = True
    for _t in ["price_history", "institutional_history", "margin_history"]:
        row = cursor.execute(f"SELECT MAX(date) FROM {_t}").fetchone()
        _after_max = row[0]
        arrow = "✅" if _after_max == end_date else "⚠️"
        print(f"  {arrow} {_t}: {_before_max[_t]} → {_after_max}（目標 {end_date}）")
        if _after_max != end_date:
            _write_ok = False

    conn.close()
    print(f"寫入完成（保留 {cutoff} 之後的資料）")

    if _had_data and not _write_ok:
        print(f"❌ 資料已抓到但 DB max date 未到達 {end_date}，請檢查寫入邏輯", flush=True)
        sys.exit(1)
    elif not _had_data:
        print(f"⚠️ 今日（{end_date}）無可寫入資料（可能為非交易日或 API 空回傳）")

    # Export 外資持股最新一筆到 CSV（供 main.py 讀取，避免 Actions SQLite 重置問題）
    if not df_sh.empty:
        latest_sh = (
            df_sh.sort_values("date")
                 .groupby("stock_id", as_index=False)
                 .last()
        )
        sh_csv_path = os.path.join(BASE_PATH, "shareholding_latest.csv")
        latest_sh.to_csv(sh_csv_path, index=False)
        print(f"  shareholding_latest.csv 已輸出：{len(latest_sh)} 支")

    # ── AI 供應鏈補抓（不在 universe 的 AI 概念股）────────────────────────────
    update_ai_supplement(end_date)


if __name__ == "__main__":
    main()
