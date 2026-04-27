"""
finmind_fetcher.py — 磐石決策系統
使用 FinMind API 抓取台股資料並寫入 banshi.db。

依賴：pip install FinMind
用法：
    from finmind_fetcher import fetch_and_store
    n = fetch_and_store(["2330", "2454"], "2024-01-01", "2024-12-31", token="YOUR_TOKEN")
"""

import logging
import sqlite3
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── dealer_net 來源：這三種 name 合計 buy-sell ────────────────────────────
_DEALER_NAMES = {"Foreign_Dealer_Self", "Dealer_self", "Dealer_Hedging"}
_FOREIGN_NAME = "Foreign_Investor"
_INVEST_NAME  = "Investment_Trust"


# ── 1. fetch_price ────────────────────────────────────────────────────────────

def fetch_price(
    stock_ids: list,
    start: str,
    end: str,
    token: str = "",
) -> pd.DataFrame:
    """抓取 TaiwanStockPrice，回傳原始 DataFrame。

    欄位：date, stock_id, open, max(→high), min(→low), close, Trading_Volume(→volume)
    失敗時回傳空 DataFrame，不 raise。
    """
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        if token:
            dl.login_by_token(api_token=token)

        frames = []
        for sid in stock_ids:
            try:
                df = dl.taiwan_stock_daily(
                    stock_id=sid,
                    start_date=start,
                    end_date=end,
                )
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as exc:
                logger.error("fetch_price: %s 失敗: %s", sid, exc)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    except Exception as exc:
        logger.error("fetch_price 整體失敗: %s", exc)
        return pd.DataFrame()


# ── 2. fetch_institutional ────────────────────────────────────────────────────

def fetch_institutional(
    stock_ids: list,
    start: str,
    end: str,
    token: str = "",
) -> pd.DataFrame:
    """抓取 TaiwanStockInstitutionalInvestorsBuySell 並 pivot 成寬表。

    原始格式：每 (date, stock_id) × 5 個 name 列。
    輸出欄位（每 date × stock_id 一列）：
        foreign_buy, foreign_sell, foreign_net,
        investment_buy, investment_sell, investment_net,
        dealer_net
    dealer_net = sum(buy-sell) for Foreign_Dealer_Self, Dealer_self, Dealer_Hedging
    失敗時回傳空 DataFrame，不 raise。
    """
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        if token:
            dl.login_by_token(api_token=token)

        frames = []
        for sid in stock_ids:
            try:
                df = dl.taiwan_stock_institutional_investors(
                    stock_id=sid,
                    start_date=start,
                    end_date=end,
                )
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as exc:
                logger.error("fetch_institutional: %s 失敗: %s", sid, exc)

        if not frames:
            return pd.DataFrame()

        raw = pd.concat(frames, ignore_index=True)
        return _pivot_institutional(raw)

    except Exception as exc:
        logger.error("fetch_institutional 整體失敗: %s", exc)
        return pd.DataFrame()


def _pivot_institutional(raw: pd.DataFrame) -> pd.DataFrame:
    """將長表 pivot 成寬表，計算各機構欄位。純函式。"""
    if raw.empty:
        return pd.DataFrame()

    raw = raw.groupby(["date", "stock_id", "name"], as_index=False)[["buy", "sell"]].sum()

    result_rows = []
    for (date, stock_id), grp in raw.groupby(["date", "stock_id"], sort=False):
        name_map = {row["name"]: row for _, row in grp.iterrows()}

        def _buy(name):
            r = name_map.get(name)
            return int(r["buy"]) if r is not None else 0

        def _sell(name):
            r = name_map.get(name)
            return int(r["sell"]) if r is not None else 0

        # 外資
        fbuy  = _buy(_FOREIGN_NAME)
        fsell = _sell(_FOREIGN_NAME)
        fnet  = fbuy - fsell

        # 投信
        ibuy  = _buy(_INVEST_NAME)
        isell = _sell(_INVEST_NAME)
        inet  = ibuy - isell

        # 自營商合計（三種 name 的 buy-sell 加總）
        dnet = sum(_buy(n) - _sell(n) for n in _DEALER_NAMES)

        result_rows.append({
            "date":             date,
            "stock_id":         stock_id,
            "foreign_buy":      fbuy,
            "foreign_sell":     fsell,
            "foreign_net":      fnet,
            "investment_buy":   ibuy,
            "investment_sell":  isell,
            "investment_net":   inet,
            "dealer_net":       dnet,
        })

    return pd.DataFrame(result_rows)


# ── 3. fetch_margin ───────────────────────────────────────────────────────────

def fetch_margin(
    stock_ids: list,
    start: str,
    end: str,
    token: str = "",
) -> pd.DataFrame:
    """抓取 TaiwanStockMarginPurchaseShortSale，回傳原始 DataFrame。

    使用欄位：
        MarginPurchaseTodayBalance → margin_balance
        ShortSaleTodayBalance      → short_balance
    失敗時回傳空 DataFrame，不 raise。
    """
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        if token:
            dl.login_by_token(api_token=token)

        frames = []
        for sid in stock_ids:
            try:
                df = dl.taiwan_stock_margin_purchase_short_sale(
                    stock_id=sid,
                    start_date=start,
                    end_date=end,
                )
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception as exc:
                logger.error("fetch_margin: %s 失敗: %s", sid, exc)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    except Exception as exc:
        logger.error("fetch_margin 整體失敗: %s", exc)
        return pd.DataFrame()


# ── 4. merge_to_schema ────────────────────────────────────────────────────────

def merge_to_schema(
    price_df: pd.DataFrame,
    inst_df: pd.DataFrame,
    margin_df: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join 三張表，重命名為 daily_data schema 欄位。

    缺漏欄位 → None。純函式，不修改輸入。
    輸出欄位順序與 schema 一致：
        stock_id, date, open, high, low, close, volume,
        foreign_buy, foreign_sell, foreign_net,
        investment_buy, investment_sell, investment_net,
        dealer_net, margin_balance, short_balance
    """
    if price_df.empty:
        return pd.DataFrame()

    # ── 整理 price ────────────────────────────────────────────────────────
    p = price_df[["stock_id", "date", "open", "max", "min", "close", "Trading_Volume"]].copy()
    p = p.rename(columns={
        "max":             "high",
        "min":             "low",
        "Trading_Volume":  "volume",
    })
    p["volume"] = pd.to_numeric(p["volume"], errors="coerce").astype("Int64")

    # ── 整理 margin ───────────────────────────────────────────────────────
    if not margin_df.empty:
        m = margin_df[["stock_id", "date",
                        "MarginPurchaseTodayBalance",
                        "ShortSaleTodayBalance"]].copy()
        m = m.rename(columns={
            "MarginPurchaseTodayBalance": "margin_balance",
            "ShortSaleTodayBalance":      "short_balance",
        })
    else:
        m = pd.DataFrame(columns=["stock_id", "date", "margin_balance", "short_balance"])

    # ── Left-join price ← institutional ← margin ─────────────────────────
    merged = p.merge(inst_df, on=["stock_id", "date"], how="left") \
               .merge(m,      on=["stock_id", "date"], how="left")

    # ── 統一欄位順序，缺漏欄補 None ──────────────────────────────────────
    schema_cols = [
        "stock_id", "date",
        "open", "high", "low", "close", "volume",
        "foreign_buy", "foreign_sell", "foreign_net",
        "investment_buy", "investment_sell", "investment_net",
        "dealer_net", "margin_balance", "short_balance",
    ]
    for col in schema_cols:
        if col not in merged.columns:
            merged[col] = None

    # NaN → None（保持 schema 規則：缺值一律 None）
    merged = merged[schema_cols].where(merged[schema_cols].notna(), other=None)

    return merged.reset_index(drop=True)


# ── 5. write_to_db ────────────────────────────────────────────────────────────

def write_to_db(
    df: pd.DataFrame,
    db_path: str = "banshi.db",
) -> int:
    """INSERT OR REPLACE df 進 daily_data。

    executemany 批次寫入，單次 commit。
    回傳寫入筆數。不 raise。
    """
    if df.empty:
        return 0

    schema_cols = [
        "stock_id", "date",
        "open", "high", "low", "close", "volume",
        "foreign_buy", "foreign_sell", "foreign_net",
        "investment_buy", "investment_sell", "investment_net",
        "dealer_net", "margin_balance", "short_balance",
    ]

    sql = "INSERT OR REPLACE INTO daily_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

    try:
        rows = []
        for rec in df[schema_cols].itertuples(index=False, name=None):
            row = []
            for v in rec:
                if v is None or v != v:
                    row.append(None)
                elif hasattr(v, 'item'):  # numpy/pandas 型別轉 Python 原生型別
                    row.append(v.item())
                else:
                    row.append(v)
            rows.append(tuple(row))
        conn = sqlite3.connect(db_path)
        conn.executemany(sql, rows)
        conn.commit()
        conn.close()
        return len(rows)
    except Exception as exc:
        logger.error("write_to_db 失敗: %s", exc)
        return 0


# ── 6. fetch_and_store ────────────────────────────────────────────────────────

def fetch_and_store(
    stock_ids: list,
    start: str,
    end: str,
    token: str = "",
    db_path: str = "banshi.db",
) -> int:
    """完整流程：fetch × 3 → merge_to_schema → write_to_db。

    回傳寫入筆數。不 raise。
    """
    try:
        logger.info("開始抓取 %d 檔股票 [%s → %s]", len(stock_ids), start, end)

        price_df = fetch_price(stock_ids, start, end, token)
        inst_df  = fetch_institutional(stock_ids, start, end, token)
        margin_df = fetch_margin(stock_ids, start, end, token)

        merged = merge_to_schema(price_df, inst_df, margin_df)
        if merged.empty:
            logger.warning("merge 結果為空，沒有資料寫入")
            return 0

        n = write_to_db(merged, db_path)
        logger.info("寫入完成：%d 筆", n)
        return n

    except Exception as exc:
        logger.error("fetch_and_store 失敗: %s", exc)
        return 0
