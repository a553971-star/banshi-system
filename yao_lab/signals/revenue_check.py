"""基本面門檻：月營收連續成長 ≥ 3 個月（SPEC §4）

判斷邏輯：取最近 N+1 個月的月營收，計算連續 N 個 MoM 是否全部 > 0。
N 預設 3（SPEC 要求）。

資料來源：FinMind taiwan_stock_month_revenue。MVP 用 dependency injection，
測試可注入 mock provider 避開網路依賴。

回傳：
- True + 連續成長月數
- False + 失敗原因
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Callable, Optional


MIN_CONSECUTIVE_MONTHS = 3
FETCH_LOOKBACK_MONTHS = 8  # 抓 8 個月以避免邊界月份缺資料


RevenueProvider = Callable[[str, str, str], list[dict]]


def _finmind_revenue_provider(stock_id: str, start_date: str, end_date: str) -> list[dict]:
    """從 FinMind 抓 taiwan_stock_month_revenue。

    回傳 list[dict]，每筆包含 date (YYYY-MM-01)、revenue（千元）。
    失敗時 raise。
    """
    from FinMind.data import DataLoader  # noqa: F401

    dl = DataLoader()
    token = os.getenv("FINMIND_TOKEN", "")
    if token:
        try:
            dl.login_by_token(api_token=token)
        except Exception:
            dl.token = token

    df = dl.taiwan_stock_month_revenue(
        stock_id=stock_id, start_date=start_date, end_date=end_date,
    )
    if df is None or len(df) == 0:
        return []
    df = df[["date", "revenue"]].copy()
    return df.to_dict(orient="records")


def check(
    stock_id: str,
    date: str,
    revenue_provider: Optional[RevenueProvider] = None,
    min_months: int = MIN_CONSECUTIVE_MONTHS,
) -> tuple[bool, dict]:
    """檢查 stock_id 在 date 當下，最近月營收是否連續 `min_months` 個月 MoM > 0。

    Args:
        stock_id: 股票代號
        date: YYYY-MM-DD（用來決定回看視窗的結束點）
        revenue_provider: 注入式抓取函式
        min_months: 要求的連續成長月數，預設 3

    Returns:
        (passed, details)
    """
    sid = str(stock_id).zfill(4)

    try:
        end = dt.date.fromisoformat(date)
    except ValueError:
        return False, {"reason": "invalid_date_format", "date": date}

    # 回看 FETCH_LOOKBACK_MONTHS 個月
    start_y = end.year
    start_m = end.month - FETCH_LOOKBACK_MONTHS
    while start_m <= 0:
        start_m += 12
        start_y -= 1
    start = dt.date(start_y, start_m, 1)

    provider = revenue_provider or _finmind_revenue_provider

    try:
        rows = provider(sid, start.isoformat(), end.isoformat())
    except ImportError:
        return False, {"reason": "provider_unavailable", "detail": "FinMind not installed"}
    except Exception as e:
        return False, {"reason": "provider_error", "detail": str(e)[:120]}

    rows = rows or []
    if len(rows) < min_months + 1:
        return False, {
            "reason": "insufficient_revenue_history",
            "have": len(rows),
            "need": min_months + 1,
        }

    # 依日期排序（升冪）
    rows = sorted(rows, key=lambda r: r.get("date", ""))

    # 取最後 min_months + 1 個月
    window = rows[-(min_months + 1):]
    revenues = [r.get("revenue") for r in window]
    if any(r is None for r in revenues):
        return False, {"reason": "null_revenue_in_window", "window": window}

    # 連續 min_months 個 MoM > 0
    mom_growth = [revenues[i] > revenues[i - 1] for i in range(1, len(revenues))]

    details = {
        "stock_id": sid,
        "window_start_month": window[0]["date"],
        "window_end_month": window[-1]["date"],
        "revenues": revenues,
        "mom_growth": mom_growth,
    }

    if all(mom_growth):
        details["reason"] = "ok"
        details["consecutive_growth_months"] = min_months
        return True, details

    # 計算實際連續成長月數（從尾端往前數）
    consecutive = 0
    for flag in reversed(mom_growth):
        if flag:
            consecutive += 1
        else:
            break
    details["reason"] = "growth_not_consecutive"
    details["consecutive_growth_months"] = consecutive
    return False, details
