"""
main.py — 磐石決策系統 Phase 2
Entry point. Orchestrates daily decision pipeline.

Pipeline (per stock):
    merge_all() → build_features() → compute_trajectory()
    → classify_flow() → classify_cost()
    → format_panstone_signal()
    → format_decision_snapshot() → export CSV

Usage:
    python main.py [--date YYYY-MM-DD] [--params params.json]
    python main.py --backtest --start YYYY-MM-DD --end YYYY-MM-DD
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from data_fetcher import merge_all
from feature_engine import build_features
from trajectory_engine import compute_trajectory, get_latest_trajectory
from flow_engine import classify_flow, classify_cost
from decision_inspector import check_data_integrity, format_panstone_signal
from good_company import is_good_company, get_company_name, load_company_list
from exporter import (
    format_data_snapshot,
    format_decision_snapshot,
    export_latest_decisions,
    append_state_log,
)
from backtester import run_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_DEFAULT_PARAMS = "params.json"
_DEFAULT_DB     = "banshi.db"
_LOOKBACK_DAYS  = 400


def load_params(path: str = _DEFAULT_PARAMS) -> dict:
    """Load all decision thresholds from a JSON config file.

    Returns a default params dict if the file is missing or unreadable.
    No hardcoded numbers exist in any logic function — all come from here.
    Never raises.
    """
    defaults = {
        "companies_path":  "companies.csv",
        "db_path":         "banshi.db",

        # Trajectory thresholds
        "c_safe_threshold":    0.98,
        "c_end_consecutive":   3,
        "b_lower":             0.97,
        "b_upper":             1.03,
        "b_vol_threshold":     6.0,
        "a_entry_ma_mult":     1.03,
        "a_entry_vol_ratio":   1.2,
        "a_reset_vol_low":     0.5,
        "a_reset_vol_consec":  3,

        # Decision Gate
        "min_c_days":          5,
        "min_b_days":          3,

        # Flow classification
        "dist_vol_ratio":      1.8,
        "dist_margin_change":  0,
        "dist_return_10d":     5.0,
        "accum_foreign_consec": 2,
        "accum_margin_change": 0,
        "accum_return_10d":    5.0,
        "accum_vol_ratio":     0.8,

        # Cost classification
        "cost_safe_lower":     -0.03,
        "cost_safe_upper":      0.06,
        "cost_high_risk":       0.08,

        # False-breakout guard
        "false_breakout_a_days":    2,
        "false_breakout_vol_ratio": 1.2,

        # Confidence scoring
        "conf_accum":                    20,
        "conf_a_days":                   20,
        "conf_safe":                     15,
        "conf_foreign_streak":           15,
        "conf_vol_ratio":                10,
        "conf_c_days":                   10,
        "conf_b_days":                   10,
        "conf_false_breakout":          -30,
        "conf_foreign_streak_threshold":  3,
        "conf_vol_ratio_threshold":       1.5,
        "conf_c_days_threshold":          8,
        "conf_b_days_threshold":          7,

        # Exit conditions (backtester)
        "exit_stop_loss":        -0.06,
        "exit_profit_bias":       0.08,
        "exit_profit_a_min":      3,
        "exit_time_stop":         20,
        "exit_trend_exhaustion":  10,

        # Personal reminder nudges
        "nudge_pnl_threshold":  10.0,
        "nudge_cost_threshold":  5.0,
    }
    try:
        p = Path(path)
        if not p.exists():
            logger.warning("params.json not found at %s — using defaults", path)
            return defaults
        with p.open(encoding="utf-8") as f:
            loaded = json.load(f)
        defaults.update(loaded)   # loaded values override defaults
        return defaults
    except Exception as exc:
        logger.error("load_params failed (%s): %s — using defaults", path, exc)
        return defaults


def _process_stock(
    stock_id: str,
    date: str,
    params: dict,
    prev_states: dict,
    print_snapshot: bool = True,
) -> Optional[dict]:
    """Run the full decision pipeline for one stock on one date.

    Returns a decision dict, or None on unrecoverable data failure.
    Never raises.
    """
    try:
        db_path = params.get("db_path", _DEFAULT_DB)
        co_path = params.get("companies_path", "companies.csv")

        target    = pd.to_datetime(date)
        start_dt  = (target - pd.Timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        name      = get_company_name(stock_id, co_path) or stock_id

        # ── Data → Features → Trajectory ────────────────────────────────
        df_raw  = merge_all(stock_id, start_dt, date, db_path)
        if df_raw.empty:
            logger.warning("%s: no data available for %s", stock_id, date)
            return None

        df_feat = build_features(df_raw)
        df_traj = compute_trajectory(df_feat, params)

        # ── Print data snapshot ───────────────────────────────────────────
        if print_snapshot:
            print(format_data_snapshot(stock_id, name, df_feat))
            print()

        traj_info    = get_latest_trajectory(df_traj)
        last         = df_traj.iloc[-1]

        def _g(col):
            v = last.get(col) if hasattr(last, "get") else (
                last[col] if col in df_traj.columns else None
            )
            return None if (v is not None and v != v) else v

        # ── Classify flow & cost ──────────────────────────────────────────
        flow_status = classify_flow(
            _g("foreign_consecutive_buy"),
            _g("margin_change_5d"),
            _g("return_10d"),
            _g("volume_ratio"),
            params,
        )
        cost_result = classify_cost(_g("bias_ma20"), params)

        # ── Assemble input dicts ──────────────────────────────────────────
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
            "flow_status":             flow_status,
            "volume_ratio":            _g("volume_ratio"),
            "foreign_consecutive_buy": _g("foreign_consecutive_buy"),
            "margin_change_5d":        _g("margin_change_5d"),
            "return_10d":              _g("return_10d"),
            "bias_ma20":               _g("bias_ma20"),
            "volatility_5d_prev":      _g("volatility_5d_prev"),
        }

        # ── Decision Gate ─────────────────────────────────────────────────
        decision = format_panstone_signal(
            trajectory=trajectory,
            flow=flow_dict,
            cost=cost_result,
            params=params,
            current_date=date,
            current_price=_g("close"),
        )

        # ── Attach observational indicators (no decision impact) ─────────
        for col in ("adx", "atr", "vwap", "kd_k", "kd_d", "bb_upper", "bb_middle", "bb_lower"):
            decision[col] = _g(col)

        # ── Print decision snapshot ───────────────────────────────────────
        if print_snapshot:
            print(format_decision_snapshot(decision))
            print()

        # ── State log ────────────────────────────────────────────────────
        prev = prev_states.get(stock_id)
        append_state_log(decision, prev)
        prev_states[stock_id] = decision.get("state")

        return decision

    except Exception as exc:
        logger.error("_process_stock(%s, %s) failed: %s", stock_id, date, exc)
        return None


def run_daily(
    stock_ids: list,
    date: str,
    params: dict,
    prev_states: dict = None,
    print_snapshots: bool = True,
) -> list:
    """Run the full decision pipeline for all stocks on a given date.

    Returns a list of decision dicts (one per stock that produced a result).
    """
    if prev_states is None:
        prev_states = {}

    results = []
    for sid in stock_ids:
        d = _process_stock(sid, date, params, prev_states, print_snapshots)
        if d is not None:
            results.append(d)

    export_latest_decisions(results)
    logger.info("run_daily complete: %d stocks processed for %s", len(results), date)
    return results


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="磐石決策系統")
    parser.add_argument("--date",    default=datetime.today().strftime("%Y-%m-%d"),
                        help="Target date (YYYY-MM-DD). Default: today.")
    parser.add_argument("--params",  default=_DEFAULT_PARAMS,
                        help="Path to params.json.")
    parser.add_argument("--stocks",  nargs="*",
                        help="Stock IDs to process. Default: all in companies.csv.")
    parser.add_argument("--backtest", action="store_true",
                        help="Run backtester instead of daily pipeline.")
    parser.add_argument("--start",   default=None, help="Backtest start date.")
    parser.add_argument("--end",     default=None, help="Backtest end date.")
    parser.add_argument("--quiet",   action="store_true",
                        help="Suppress snapshot output (CSV only).")
    args = parser.parse_args()

    params = load_params(args.params)

    # Determine stock list
    if args.stocks:
        stock_ids = args.stocks
    else:
        companies = load_company_list(params.get("companies_path", "companies.csv"))
        stock_ids = list(companies.keys())

    if not stock_ids:
        print("No stocks to process. Add entries to companies.csv.")
        sys.exit(0)

    if args.backtest:
        start = args.start or "2024-01-01"
        end   = args.end   or args.date
        print(f"Running backtest: {start} → {end} | {len(stock_ids)} stocks")
        trades_df = run_backtest(stock_ids, start, end, params)
        if not trades_df.empty:
            print(trades_df.to_string(index=False))
        else:
            print("No trades generated.")
    else:
        print(f"Running daily pipeline: {args.date} | {len(stock_ids)} stocks\n")
        run_daily(
            stock_ids,
            args.date,
            params,
            print_snapshots=not args.quiet,
        )


if __name__ == "__main__":
    main()
