"""訊號 A：MOPS 重大公告（SPEC §4）

近 3 個交易日內，該股有重大訊息公告。
資料來源：公開資訊觀測站（MOPS）。MVP 階段使用 FinMind 簡化抓取。

依賴注入：
    check(stock_id, date, event_provider=...)
    event_provider 接受 (stock_id, start_date, end_date) 回傳 list[dict]。
    預設用 _finmind_event_provider；測試可注入 mock。

設計原則：
- FinMind 不可用（未裝、無 token、429 限流）時回傳 (False, {"reason": "provider_unavailable"})
- 不在這層做事件分類過濾（誰來抓事件、過濾什麼類別，由 provider 自己決定）
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Callable, Optional

LOOKBACK_DAYS = 3  # 自然日，非交易日


EventProvider = Callable[[str, str, str], list[dict]]


def _finmind_event_provider(stock_id: str, start_date: str, end_date: str) -> list[dict]:
    """嘗試用 FinMind 抓 MOPS 公告。

    FinMind 的 TaiwanStockNews API 可抓新聞快訊；對於正式 MOPS 公告
    可考慮 TaiwanStockNewsMoneydj。MVP 階段以 NewsMoneydj 為主。

    回傳 list[dict]，每筆至少包含 date、title。失敗時 raise，由 check() 接住。
    """
    from FinMind.data import DataLoader  # noqa: F401

    dl = DataLoader()
    token = os.getenv("FINMIND_TOKEN", "")
    if token:
        try:
            dl.login_by_token(api_token=token)
        except Exception:
            dl.token = token

    df = dl.taiwan_stock_news(stock_id=stock_id, start_date=start_date, end_date=end_date)
    if df is None or len(df) == 0:
        return []
    df = df.rename(columns={"title": "title"})
    return df.to_dict(orient="records")


def check(
    stock_id: str,
    date: str,
    event_provider: Optional[EventProvider] = None,
    lookback_days: int = LOOKBACK_DAYS,
) -> tuple[bool, dict]:
    """檢查 stock_id 在 date 當日往前 `lookback_days` 天內是否有重大公告。

    Args:
        stock_id: 股票代號
        date: YYYY-MM-DD（含當日）
        event_provider: 注入式抓取函式；None 代表使用 FinMind。
        lookback_days: 回看自然日，預設 3。

    Returns:
        (triggered, details)
    """
    sid = str(stock_id).zfill(4)

    try:
        end = dt.date.fromisoformat(date)
    except ValueError:
        return False, {"reason": "invalid_date_format", "date": date}
    start = end - dt.timedelta(days=lookback_days)

    provider = event_provider or _finmind_event_provider

    try:
        events = provider(sid, start.isoformat(), end.isoformat())
    except ImportError:
        return False, {"reason": "provider_unavailable", "detail": "FinMind not installed"}
    except Exception as e:
        return False, {"reason": "provider_error", "detail": str(e)[:120]}

    events = events or []
    details = {
        "stock_id": sid,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "event_count": len(events),
        "events_sample": [
            {k: v for k, v in ev.items() if k in ("date", "title", "category")}
            for ev in events[:3]
        ],
    }

    if events:
        details["reason"] = "ok"
        return True, details
    details["reason"] = "no_events_in_window"
    return False, details
