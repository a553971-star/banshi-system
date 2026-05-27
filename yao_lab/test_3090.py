"""Day 1 訊號模組驗證 — 日電貿(3090)

預設用 2026-05-15 跑（雲端 DB 目前最後一筆）。
本機 backfill_db.py 跑完 + DB 有 5/19 資料後，請改用：
    python3 yao_lab/test_3090.py 2026-05-19

對 5/19 的預期：
  訊號 B（爆量上漲）：應該觸發（媒體曝光當日量價齊揚）
  trend_filter        ：應該通過（近 20 日強勢）
  訊號 A（MOPS 公告） ：本機若已裝 FinMind + 設 FINMIND_TOKEN 才能真實驗證
  revenue_check       ：本機若已裝 FinMind + 設 FINMIND_TOKEN 才能真實驗證
"""
from __future__ import annotations

import sys

from yao_lab.signals import event_scanner, volume_surge, revenue_check, trend_filter


STOCK_ID = "3090"
DEFAULT_DATE = "2026-05-15"


def _mock_events_with_data(stock_id: str, start_date: str, end_date: str) -> list[dict]:
    """Mock：假裝視窗內有一則公告。驗證 event_scanner 正確流程。"""
    return [{"date": end_date, "title": f"mock 公告 for {stock_id}", "category": "重大訊息"}]


def _mock_events_empty(stock_id: str, start_date: str, end_date: str) -> list[dict]:
    return []


def _mock_revenue_growing(stock_id: str, start_date: str, end_date: str) -> list[dict]:
    """Mock：連續 4 個月 MoM 都成長。"""
    return [
        {"date": "2026-01-01", "revenue": 900_000},
        {"date": "2026-02-01", "revenue": 950_000},
        {"date": "2026-03-01", "revenue": 1_050_000},
        {"date": "2026-04-01", "revenue": 1_200_000},
    ]


def _mock_revenue_flat(stock_id: str, start_date: str, end_date: str) -> list[dict]:
    """Mock：第 3 個月 MoM 下降，連續成長中斷。"""
    return [
        {"date": "2026-01-01", "revenue": 1_000_000},
        {"date": "2026-02-01", "revenue": 1_050_000},
        {"date": "2026-03-01", "revenue": 1_020_000},
        {"date": "2026-04-01", "revenue": 1_100_000},
    ]


def main(date: str) -> None:
    print(f"=== Yao Lab Day 1 訊號驗證：{STOCK_ID}({date}) ===\n")

    print("【訊號 B：爆量上漲】")
    triggered, details = volume_surge.check(STOCK_ID, date)
    print(f"  triggered = {triggered}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【風險過濾：近 20 日漲跌】")
    passed, details = trend_filter.check(STOCK_ID, date)
    print(f"  passed = {passed}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【訊號 A：MOPS 公告（mock=有公告）】")
    triggered, details = event_scanner.check(STOCK_ID, date, event_provider=_mock_events_with_data)
    print(f"  triggered = {triggered}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【訊號 A：MOPS 公告（mock=無公告）】")
    triggered, details = event_scanner.check(STOCK_ID, date, event_provider=_mock_events_empty)
    print(f"  triggered = {triggered}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【訊號 A：MOPS 公告（預設 FinMind provider）】")
    triggered, details = event_scanner.check(STOCK_ID, date)
    print(f"  triggered = {triggered}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【基本面：營收成長（mock=連 3 月成長）】")
    passed, details = revenue_check.check(STOCK_ID, date, revenue_provider=_mock_revenue_growing)
    print(f"  passed = {passed}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【基本面：營收成長（mock=中斷）】")
    passed, details = revenue_check.check(STOCK_ID, date, revenue_provider=_mock_revenue_flat)
    print(f"  passed = {passed}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()

    print("【基本面：營收成長（預設 FinMind provider）】")
    passed, details = revenue_check.check(STOCK_ID, date)
    print(f"  passed = {passed}")
    for k, v in details.items():
        print(f"  {k}: {v}")
    print()


if __name__ == "__main__":
    arg_date = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DATE
    main(arg_date)
