"""
backtester.py — 磐石決策系統 Phase 2
Zero-lookahead multi-stock backtester.

Zero-lookahead guarantee:
    replay_decision() slices df to df[df.date <= date] BEFORE any calculation.
    No future data can leak into any indicator or decision.

Exit conditions (evaluated in strict order, stop at first match):
    1. bias_ma20 <= exit_stop_loss        (default -6%)   → "STOP_LOSS"
    2. bias_ma20 >= exit_profit_bias      (default +8%)
       AND A_days >= exit_profit_a_min    (default 3)     → "PROFIT_TARGET"
    3. hold_days >= exit_time_stop        (default 20)    → "TIME_STOP"
    4. A_days >= exit_trend_exhaustion    (default 10)    → "TREND_EXHAUSTION"
    No match                                              → hold (False, "")
"""

import csv
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from data_fetcher import merge_all
from feature_engine import build_features
from trajectory_engine import compute_trajectory, get_latest_trajectory
from flow_engine import classify_flow, classify_cost
from decision_inspector import (
    check_data_integrity,
    format_panstone_signal,
    is_false_breakout,
)
from good_company import is_good_company, get_company_name

logger = logging.getLogger(__name__)

# Days of history to load before the target date (MA120 needs 120 + buffer)
_LOOKBACK_DAYS = 400

_TRADE_COLS = [
    "stock_id", "entry_date", "exit_date",
    "entry_price", "exit_price", "exit_reason",
    "hold_days", "pnl_pct",
]


def replay_decision(date: str, stock_id: str, params: dict) -> dict:
    """Reconstruct the decision for a given stock on a given date.

    Uses ONLY data available on or before `date` — zero-lookahead guaranteed.

    Steps:
        1. Load raw data (date - 400d → date)
        2. Slice to df[df.date <= date]          ← LOOKAHEAD GUARD
        3. build_features()
        4. compute_trajectory()
        5. Assemble trajectory / flow / cost dicts
        6. format_panstone_signal()

    Returns a complete decision dict. Never raises.
    """
    try:
        db_path   = params.get("db_path", "banshi.db")
        co_path   = params.get("companies_path", "companies.csv")

        target    = pd.to_datetime(date)
        start     = (target - pd.Timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        # ── Load and slice (zero-lookahead) ──────────────────────────────
        df_raw = merge_all(stock_id, start, date, db_path)
        if df_raw.empty:
            logger.warning("replay_decision: no data for %s on %s", stock_id, date)
            return _empty_ignore(stock_id, "", date)

        df_safe = df_raw[df_raw["date"] <= target].copy()
        if df_safe.empty:
            return _empty_ignore(stock_id, "", date)

        # ── Feature + trajectory ──────────────────────────────────────────
        df_feat = build_features(df_safe)
        df_traj = compute_trajectory(df_feat, params)

        traj_info = get_latest_trajectory(df_traj)
        last      = df_traj.iloc[-1]

        def _g(col):
            v = last.get(col) if hasattr(last, "get") else (
                last[col] if col in df_traj.columns else None
            )
            return None if (v is not None and v != v) else v  # NaN guard

        # ── Assemble input dicts ──────────────────────────────────────────
        name         = get_company_name(stock_id, co_path) or stock_id
        integrity_ok = check_data_integrity(df_feat)
        good_co      = is_good_company(stock_id, co_path)

        trajectory = {
            "stock_id":        stock_id,
            "name":            name,
            "date":            traj_info["date"] or date,
            "C_days":          traj_info["C_days"],
            "B_days":          traj_info["B_days"],
            "A_days":          traj_info["A_days"],
            "integrity_ok":    integrity_ok,
            "is_good_company": good_co,
        }

        flow_dict = {
            "flow_status":           classify_flow(
                _g("foreign_consecutive_buy"),
                _g("margin_change_5d"),
                _g("return_10d"),
                _g("volume_ratio"),
                params,
            ),
            "volume_ratio":          _g("volume_ratio"),
            "foreign_consecutive_buy": _g("foreign_consecutive_buy"),
            "margin_change_5d":      _g("margin_change_5d"),
            "return_10d":            _g("return_10d"),
            "bias_ma20":             _g("bias_ma20"),
        }

        cost_result = classify_cost(_g("bias_ma20"), params)

        return format_panstone_signal(
            trajectory=trajectory,
            flow=flow_dict,
            cost=cost_result,
            params=params,
            current_date=date,
            current_price=_g("close"),
        )

    except Exception as exc:
        logger.error("replay_decision(%s, %s) failed: %s", date, stock_id, exc)
        return _empty_ignore(stock_id, "", date)


def check_exit(position: dict, row: dict, params: dict) -> tuple:
    """Evaluate exit conditions for an open position.

    Conditions checked in strict order (stop at first match):
        1. bias_ma20 <= exit_stop_loss              → "STOP_LOSS"
        2. bias_ma20 >= exit_profit_bias AND
           A_days >= exit_profit_a_min              → "PROFIT_TARGET"
        3. hold_days >= exit_time_stop              → "TIME_STOP"
        4. A_days >= exit_trend_exhaustion          → "TREND_EXHAUSTION"

    Parameters:
        position: {"entry_date": str, "entry_price": float, ...}
        row:      {"bias_ma20": float|None, "A_days": int|None,
                   "hold_days": int}
        params:   threshold dict

    Returns:
        (True, reason_str)  — should exit
        (False, "")         — hold
    Pure function.
    """
    try:
        bias   = row.get("bias_ma20")
        a_days = row.get("A_days")
        hdays  = row.get("hold_days", 0)

        sl    = float(params.get("exit_stop_loss",         -0.06))
        pb    = float(params.get("exit_profit_bias",        0.08))
        pamin = int(params.get("exit_profit_a_min",         3))
        ts    = int(params.get("exit_time_stop",            20))
        te    = int(params.get("exit_trend_exhaustion",     10))

        # 1. Stop loss
        if bias is not None and float(bias) <= sl:
            return True, "STOP_LOSS"

        # 2. Profit target
        if (
            bias is not None and a_days is not None
            and float(bias) >= pb
            and int(a_days) >= pamin
        ):
            return True, "PROFIT_TARGET"

        # 3. Time stop
        if int(hdays) >= ts:
            return True, "TIME_STOP"

        # 4. Trend exhaustion
        if a_days is not None and int(a_days) >= te:
            return True, "TREND_EXHAUSTION"

        return False, ""

    except Exception as exc:
        logger.error("check_exit failed: %s", exc)
        return False, ""


def log_trade(trade: dict, path: str = "backtest_trades.csv") -> None:
    """Append a completed trade record to the backtest trades CSV.

    Columns: stock_id, entry_date, exit_date, entry_price, exit_price,
             exit_reason, hold_days, pnl_pct.
    Creates the file with header if absent. Never raises.
    """
    try:
        file_exists = Path(path).exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_TRADE_COLS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow({k: trade.get(k, "") for k in _TRADE_COLS})
    except Exception as exc:
        logger.error("log_trade failed (%s): %s", path, exc)


def run_backtest(
    stock_ids: list,
    start: str,
    end: str,
    params: dict,
    trades_path: str = "backtest_trades.csv",
) -> pd.DataFrame:
    """Run a zero-lookahead backtest across multiple stocks over a date range.

    For each trading day:
        1. replay_decision() for each stock (uses only data up to that date).
        2. If open position: evaluate check_exit().
           If exit triggered: close position, log trade.
        3. If BUY signal and no open position: open position.

    Returns a DataFrame of all completed trades.
    Never raises; individual stock/day errors are logged and skipped.
    """
    try:
        db_path = params.get("db_path", "banshi.db")

        # Collect all trading dates from the data
        from data_fetcher import merge_all as _ma
        date_set: set = set()
        for sid in stock_ids:
            try:
                df_tmp = _ma(sid, start, end, db_path)
                if not df_tmp.empty:
                    date_set.update(df_tmp["date"].dt.strftime("%Y-%m-%d").tolist())
            except Exception as exc:
                logger.error("run_backtest: date collection failed for %s: %s", sid, exc)

        trading_days = sorted(date_set)
        if not trading_days:
            logger.warning("run_backtest: no trading days found in [%s, %s]", start, end)
            return pd.DataFrame(columns=_TRADE_COLS)

        # State: open positions keyed by stock_id
        open_positions: dict = {}
        all_trades: list = []

        for day in trading_days:
            for sid in stock_ids:
                try:
                    decision = replay_decision(day, sid, params)

                    # ── Manage open position ───────────────────────────────
                    if sid in open_positions:
                        pos   = open_positions[sid]
                        entry = pd.to_datetime(pos["entry_date"])
                        curr  = pd.to_datetime(day)
                        hdays = (curr - entry).days

                        row_exit = {
                            "bias_ma20": decision.get("bias_ma20"),
                            "A_days":    decision.get("A_days"),
                            "hold_days": hdays,
                        }
                        should_exit, reason = check_exit(pos, row_exit, params)

                        if should_exit:
                            exit_price = decision.get("current_price")
                            entry_price = pos["entry_price"]
                            pnl = (
                                round((exit_price / entry_price - 1.0) * 100.0, 2)
                                if exit_price and entry_price
                                else None
                            )
                            trade = {
                                "stock_id":    sid,
                                "entry_date":  pos["entry_date"],
                                "exit_date":   day,
                                "entry_price": entry_price,
                                "exit_price":  exit_price,
                                "exit_reason": reason,
                                "hold_days":   hdays,
                                "pnl_pct":     pnl,
                            }
                            log_trade(trade, trades_path)
                            all_trades.append(trade)
                            del open_positions[sid]

                    # ── Open new position on BUY signal ────────────────────
                    if sid not in open_positions and decision.get("decision") == "BUY":
                        entry_price = decision.get("current_price")
                        if entry_price is not None:
                            open_positions[sid] = {
                                "stock_id":    sid,
                                "entry_date":  day,
                                "entry_price": entry_price,
                            }

                except Exception as exc:
                    logger.error("run_backtest: %s on %s failed: %s", sid, day, exc)

        return pd.DataFrame(all_trades, columns=_TRADE_COLS) if all_trades else pd.DataFrame(columns=_TRADE_COLS)

    except Exception as exc:
        logger.error("run_backtest failed: %s", exc)
        return pd.DataFrame(columns=_TRADE_COLS)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _empty_ignore(stock_id: str, name: str, date: str) -> dict:
    """Return a minimal IGNORE decision dict for error cases."""
    return {
        "stock_id": stock_id, "name": name, "date": date,
        "decision": "IGNORE", "state": "INACTIVE", "confidence": 0,
        "risk": "N/A", "risk_flag": None, "false_breakout": False,
        "reason": ["No data available"], "explanation": {},
        "C_days": None, "B_days": None, "A_days": None,
        "flow_status": None, "foreign_streak": None,
        "margin_change_5d": None, "volume_ratio": None,
        "return_10d": None, "bias_ma20": None,
        "cost_level": None, "deviation_percent": None,
        "current_price": None, "pnl_percent": None,
    }
