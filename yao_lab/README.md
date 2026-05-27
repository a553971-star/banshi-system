# 獵妖實驗室（Yao Lab）

AI 妖股紙上交易實驗。完整規格見 [`SPEC.md`](./SPEC.md)。

## Day 1 已交付

四個訊號模組（`signals/`），對外介面：
```python
signal_module.check(stock_id, date, ...) -> (triggered: bool, details: dict)
```

| 模組 | SPEC 對應 | 資料源 | 純函式 |
|---|---|---|---|
| `volume_surge.py`  | 訊號 B：爆量上漲       | banshi.db.price_history | ✅ |
| `trend_filter.py`  | 風險過濾：近 20 日漲跌  | banshi.db.price_history | ✅ |
| `event_scanner.py` | 訊號 A：MOPS 公告       | FinMind（DI）            | DI |
| `revenue_check.py` | 基本面：營收成長        | FinMind（DI）            | DI |

「DI」= 用 dependency injection，預設用 FinMind，測試可注入 mock。

## 本地驗證

```bash
# 預設用 2026-05-15（雲端 DB 目前最後一筆）
python3 -m yao_lab.test_3090

# 本機 backfill 完成 + DB 有 5/19 後，請改：
python3 -m yao_lab.test_3090 2026-05-19
```

## FinMind 設定（讓真實 provider 跑得起來）

```bash
pip install FinMind
export FINMIND_TOKEN=your_token_here
```

未設定時，`event_scanner` 與 `revenue_check` 在預設模式下會回傳：
```
reason: provider_unavailable
detail: FinMind not installed
```

這是正常容錯行為，不會 crash。

## Day 2 待辦

- `sector_resonance.py`（訊號 C：族群共振）
- `engine/paper_trade.py`、`engine/exit_logic.py`、`engine/alert_filter.py`
- `analytics/performance.py`、`analytics/benchmark.py`
- `daily_scan.py`（主入口）
- `.github/workflows/yao_lab_daily.yml`
