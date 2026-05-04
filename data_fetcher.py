"""
data_fetcher.py — 磐石決策系統 Phase 1
Data access layer for daily_data SQLite table.

Rules:
- All missing fields → None (never NaN, never 0).
- Dates returned as datetime objects, sorted ascending.
- Never raises on missing data; logs warnings instead.
- merge_all() is the primary entry point for downstream consumers.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_DB = "banshi.db"

_OHLCV_COLS = ["open", "high", "low", "close", "volume"]
_INSTITUTIONAL_COLS = [
    "foreign_buy", "foreign_sell", "foreign_net",
    "investment_buy", "investment_sell", "investment_net",
    "dealer_net",
]
_MARGIN_COLS = ["margin_balance", "short_balance"]
_ALL_DATA_COLS = _OHLCV_COLS + _INSTITUTIONAL_COLS + _MARGIN_COLS


def _open_conn(db_path: str) -> Optional[sqlite3.Connection]:
    """Open a SQLite connection with Row factory. Returns None on failure."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as exc:
        logger.error("Cannot open database %s: %s", db_path, exc)
        return None


def _query(
    db_path: str,
    stock_id: str,
    start: str,
    end: str,
    extra_cols: list,
) -> pd.DataFrame:
    """
    Execute SELECT date + extra_cols FROM daily_data for the given stock/range.

    Returns DataFrame sorted ascending by date.
    SQL NULL → Python None (sqlite3.Row preserves None).
    Returns empty DataFrame (with correct columns) on any failure.
    """
    select_cols = ["date"] + extra_cols
    col_sql = ", ".join(select_cols)
    empty = pd.DataFrame(columns=select_cols)

    conn = _open_conn(db_path)
    if conn is None:
        return empty
    try:
        sql = (
            f"SELECT {col_sql} FROM daily_data "
            f"WHERE stock_id = ? AND date BETWEEN ? AND ? "
            f"ORDER BY date ASC"
        )
        cursor = conn.execute(sql, (stock_id, start, end))
        rows = cursor.fetchall()
    except Exception as exc:
        logger.error("Query failed for %s [%s→%s]: %s", stock_id, start, end, exc)
        return empty
    finally:
        conn.close()

    if not rows:
        logger.warning("No rows for %s between %s and %s", stock_id, start, end)
        return empty

    df = pd.DataFrame([dict(r) for r in rows], columns=select_cols)
    df["date"] = pd.to_datetime(df["date"])
    return df


def fetch_ohlcv(
    stock_id: str,
    start: str,
    end: str,
    db_path: str = _DEFAULT_DB,
) -> pd.DataFrame:
    """
    Fetch OHLCV data for a stock over a date range.

    Returns columns: date(datetime), open, high, low, close, volume.
    Missing fields → None. Sorted ascending. Never raises.
    """
    return _query(db_path, stock_id, start, end, _OHLCV_COLS)


def fetch_institutional(
    stock_id: str,
    start: str,
    end: str,
    db_path: str = _DEFAULT_DB,
) -> pd.DataFrame:
    """
    Fetch 3-party institutional flow data.

    Returns columns: date, foreign_buy/sell/net, investment_buy/sell/net, dealer_net.
    Missing fields → None. Sorted ascending. Never raises.
    """
    return _query(db_path, stock_id, start, end, _INSTITUTIONAL_COLS)


def fetch_margin(
    stock_id: str,
    start: str,
    end: str,
    db_path: str = _DEFAULT_DB,
) -> pd.DataFrame:
    """
    Fetch margin and short balance data.

    Returns columns: date, margin_balance, short_balance.
    Missing fields → None. Sorted ascending. Never raises.
    """
    return _query(db_path, stock_id, start, end, _MARGIN_COLS)


def merge_all(
    stock_id: str,
    start: str,
    end: str,
    db_path: str = _DEFAULT_DB,
) -> pd.DataFrame:
    """
    Fetch all columns for a stock as a single unified DataFrame.

    Since all data resides in daily_data, this is a single SELECT of all columns.
    Missing join fields → None. Sorted ascending by date. Never raises.

    Returns columns: date, open, high, low, close, volume,
                     foreign_buy/sell/net, investment_buy/sell/net, dealer_net,
                     margin_balance, short_balance.
    """
    return _query(db_path, stock_id, start, end, _ALL_DATA_COLS)


def get_stock_name(stock_id: str, db_path: str = _DEFAULT_DB) -> str:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM stock_names WHERE stock_id=?", (stock_id,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else stock_id
    except Exception:
        return stock_id


def merge_all_local(
    stock_id: str,
    start: str,
    end: str,
    db_path: str = _DEFAULT_DB,
) -> pd.DataFrame:
    """
    從本機 price_history + institutional_history + margin_history 合併資料。
    取代 FinMind API，完全離線運作。
    """
    try:
        conn = sqlite3.connect(db_path)

        price = pd.read_sql_query("""
            SELECT stock_id, date, open, high, low, close, volume
            FROM price_history
            WHERE stock_id=? AND date>=? AND date<=?
            ORDER BY date
        """, conn, params=(stock_id, start, end))

        if price.empty:
            conn.close()
            return pd.DataFrame()

        inst = pd.read_sql_query("""
            SELECT date, foreign_buy, foreign_sell, foreign_net,
                   investment_buy, investment_sell, investment_net, dealer_net
            FROM institutional_history
            WHERE stock_id=? AND date>=? AND date<=?
            ORDER BY date
        """, conn, params=(stock_id, start, end))

        margin = pd.read_sql_query("""
            SELECT date, margin_balance, short_balance
            FROM margin_history
            WHERE stock_id=? AND date>=? AND date<=?
            ORDER BY date
        """, conn, params=(stock_id, start, end))

        conn.close()

        df = price.copy()
        if not inst.empty:
            df = df.merge(inst, on="date", how="left")
        if not margin.empty:
            df = df.merge(margin, on="date", how="left")

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df

    except Exception as e:
        logger.error("merge_all_local(%s) failed: %s", stock_id, e)
        return pd.DataFrame()
