"""訊號 B：爆量上漲

SPEC §4 訊號 B：
- 當日漲幅 ≥ 7%
- 當日成交量 ≥ 5 日均量 × 2

5 日均量計算範圍：當日之「前」5 個交易日（不含當日），避免把當日本身算進去
拉低門檻。資料來源：banshi.db.price_history。
"""
from __future__ import annotations

import os
import sqlite3
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DB = os.path.join(BASE_DIR, "banshi.db")

PRICE_CHANGE_THRESHOLD = 0.07   # 7%
VOLUME_RATIO_THRESHOLD = 2.0    # 2 倍
MA_WINDOW = 5


def check(stock_id: str, date: str, db_path: str = DEFAULT_DB) -> tuple[bool, dict]:
    """檢查 stock_id 在 date 當日是否符合「爆量上漲」。

    Args:
        stock_id: 股票代號（4 碼，e.g. '3090'）
        date: YYYY-MM-DD
        db_path: SQLite 檔案路徑，預設磐石主 DB

    Returns:
        (triggered, details)
        details 一律包含 reason；triggered=True 時包含 price_change_pct、volume_ratio。
    """
    sid = str(stock_id).zfill(4)
    try:
        conn = sqlite3.connect(db_path)
        # 抓 date 當日 + 之前最近 5 個交易日（共 6 筆，倒序）
        rows = conn.execute(
            """
            SELECT date, close, volume
              FROM price_history
             WHERE stock_id = ? AND date <= ?
             ORDER BY date DESC
             LIMIT ?
            """,
            (sid, date, MA_WINDOW + 1),
        ).fetchall()
        conn.close()
    except sqlite3.OperationalError as e:
        return False, {"reason": f"db_error: {e}"}

    if not rows or rows[0][0] != date:
        return False, {"reason": "no_data_on_date", "stock_id": sid, "date": date}

    if len(rows) < MA_WINDOW + 1:
        return False, {
            "reason": "insufficient_history",
            "have": len(rows),
            "need": MA_WINDOW + 1,
        }

    today_date, today_close, today_volume = rows[0]
    prev_close = rows[1][1]
    prev_volumes = [r[2] for r in rows[1 : MA_WINDOW + 1] if r[2] is not None]

    if prev_close is None or today_close is None or prev_close == 0:
        return False, {"reason": "null_or_zero_close"}

    price_change_pct = (today_close - prev_close) / prev_close

    if not prev_volumes:
        return False, {"reason": "no_volume_history"}
    ma_volume = sum(prev_volumes) / len(prev_volumes)

    if ma_volume == 0 or today_volume is None:
        return False, {"reason": "zero_or_null_volume"}
    volume_ratio = today_volume / ma_volume

    details = {
        "date": today_date,
        "today_close": today_close,
        "prev_close": prev_close,
        "price_change_pct": round(price_change_pct, 4),
        "today_volume": today_volume,
        "ma5_volume": round(ma_volume, 0),
        "volume_ratio": round(volume_ratio, 2),
    }

    if price_change_pct >= PRICE_CHANGE_THRESHOLD and volume_ratio >= VOLUME_RATIO_THRESHOLD:
        details["reason"] = "ok"
        return True, details

    details["reason"] = (
        "price_change_below_threshold"
        if price_change_pct < PRICE_CHANGE_THRESHOLD
        else "volume_ratio_below_threshold"
    )
    return False, details
