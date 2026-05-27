"""風險過濾：未處於下跌趨勢（SPEC §4）

近 20 個交易日漲跌幅 ≥ 0%：
    return = (close[t] - close[t-20]) / close[t-20]

回傳 True 代表「通過過濾」（可以進場），False 代表「正處於下跌，淘汰」。
"""
from __future__ import annotations

import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DB = os.path.join(BASE_DIR, "banshi.db")

LOOKBACK_DAYS = 20
THRESHOLD_PCT = 0.0   # ≥ 0% 才通過


def check(stock_id: str, date: str, db_path: str = DEFAULT_DB) -> tuple[bool, dict]:
    """檢查 stock_id 在 date 當日，近 20 個交易日漲跌幅 ≥ 0。

    Args:
        stock_id: 股票代號
        date: YYYY-MM-DD
        db_path: SQLite 路徑

    Returns:
        (passed, details)
    """
    sid = str(stock_id).zfill(4)
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute(
            """
            SELECT date, close FROM price_history
             WHERE stock_id = ? AND date <= ?
             ORDER BY date DESC
             LIMIT ?
            """,
            (sid, date, LOOKBACK_DAYS + 1),
        ).fetchall()
        conn.close()
    except sqlite3.OperationalError as e:
        return False, {"reason": f"db_error: {e}"}

    if not rows or rows[0][0] != date:
        return False, {"reason": "no_data_on_date", "stock_id": sid, "date": date}

    if len(rows) < LOOKBACK_DAYS + 1:
        return False, {
            "reason": "insufficient_history",
            "have": len(rows),
            "need": LOOKBACK_DAYS + 1,
        }

    today_close = rows[0][1]
    base_close = rows[LOOKBACK_DAYS][1]

    if today_close is None or base_close is None or base_close == 0:
        return False, {"reason": "null_or_zero_close"}

    pct = (today_close - base_close) / base_close
    details = {
        "date": rows[0][0],
        "base_date": rows[LOOKBACK_DAYS][0],
        "today_close": today_close,
        "base_close": base_close,
        "return_pct": round(pct, 4),
    }

    if pct >= THRESHOLD_PCT:
        details["reason"] = "ok"
        return True, details
    details["reason"] = "in_downtrend"
    return False, details
