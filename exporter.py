"""
exporter.py — 磐石決策系統 Phase 2
SOLE output formatter for all snapshots and CSV exports.

Rules:
- No other module may produce formatted output.
- All snapshots use KEY: VALUE plain-text format (machine-parseable).
- All values are formatted for human readability (%, +/- signs, etc.).
- Never raises; logs errors and returns empty string / skips row on failure.
"""

import csv
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Formatting helpers ────────────────────────────────────────────────────────

def _fmt_float(val, decimals: int = 1) -> str:
    """Format a float with fixed decimals. Returns 'N/A' for None."""
    if val is None:
        return "N/A"
    try:
        return f"{float(val):.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_pct(val, decimals: int = 1) -> str:
    """Format a ratio (0.025) as a signed percentage string ('+2.5').

    Used for bias_ma20 which is stored as a decimal ratio.
    """
    if val is None:
        return "N/A"
    try:
        p = float(val) * 100.0
        sign = "+" if p >= 0 else ""
        return f"{sign}{p:.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_signed(val, decimals: int = 1) -> str:
    """Format a value (already in percent or plain units) with explicit sign."""
    if val is None:
        return "N/A"
    try:
        v = float(val)
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.{decimals}f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_int(val) -> str:
    """Format an integer value. Returns 'N/A' for None."""
    if val is None:
        return "N/A"
    try:
        return str(int(val))
    except (TypeError, ValueError):
        return "N/A"


def _fmt_date(df) -> str:
    """Extract the date string from the last row of a DataFrame."""
    try:
        last = df.iloc[-1]
        d = last.get("date") if hasattr(last, "get") else last["date"]
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d")
        return str(d)
    except Exception:
        return "UNKNOWN"


# ── Snapshot formatters ───────────────────────────────────────────────────────

def format_data_snapshot(stock_id: str, name: str, df) -> str:
    """Format a plain-text data snapshot for a single stock.

    Output format (KEY: VALUE, one per line):
        [Data Snapshot - YYYY-MM-DD]
        STOCK_ID: ...
        NAME: ...
        RECORDS: ...
        INTEGRITY: OK | FAIL
        CLOSE: ...
        MA20: ...
        BIAS_MA20: +/-x.x   (percent)
        VOLUME_RATIO: ...
        FOREIGN_STREAK: ...
        MARGIN_CHANGE_5D: ...

    SOLE formatter for data layer output. Never raises.
    """
    try:
        date_str   = _fmt_date(df)
        records    = len(df)
        last       = df.iloc[-1]

        def _get(col):
            try:
                v = last.get(col) if hasattr(last, "get") else last[col]
                return None if v != v else v   # NaN guard
            except Exception:
                return None

        close      = _get("close")
        ma20       = _get("ma20")
        bias_ma20  = _get("bias_ma20")
        vol_ratio  = _get("volume_ratio")
        fgn_streak = _get("foreign_consecutive_buy")
        mgn_chg    = _get("margin_change_5d")

        required = [close, ma20, bias_ma20, vol_ratio, fgn_streak, mgn_chg]
        integrity = "OK" if all(v is not None for v in required) else "FAIL"

        lines = [
            f"[Data Snapshot - {date_str}]",
            f"STOCK_ID: {stock_id}",
            f"NAME: {name}",
            f"RECORDS: {records}",
            f"INTEGRITY: {integrity}",
            f"CLOSE: {_fmt_float(close, 0)}",
            f"MA20: {_fmt_float(ma20, 0)}",
            f"BIAS_MA20: {_fmt_pct(bias_ma20)}",
            f"VOLUME_RATIO: {_fmt_float(vol_ratio)}",
            f"FOREIGN_STREAK: {_fmt_int(fgn_streak)}",
            f"MARGIN_CHANGE_5D: {_fmt_int(mgn_chg)}",
        ]
        return "\n".join(lines)

    except Exception as exc:
        logger.error("format_data_snapshot(%s) failed: %s", stock_id, exc)
        return f"[Data Snapshot - ERROR]\nSTOCK_ID: {stock_id}\nERROR: {exc}"


def format_decision_snapshot(decision_dict: dict) -> str:
    """Format a plain-text decision snapshot from a decision dict.

    Output format (KEY: VALUE):
        [Decision Snapshot - YYYY-MM-DD]
        STOCK_ID: ...
        NAME: ...
        C_DAYS / B_DAYS / A_DAYS
        FLOW_STATUS / FOREIGN_STREAK / MARGIN_CHANGE_5D / VOLUME_RATIO
        BIAS_MA20 / COST_LEVEL
        DECISION / STATE / CONFIDENCE / REASON / RISK
        ACTION  (only if personal reminder triggered)
        RISK_FLAG (only if risk_flag is set)

    SOLE formatter for decision layer output. Never raises.
    """
    try:
        d = decision_dict
        date_str    = d.get("date", "UNKNOWN")
        stock_id    = d.get("stock_id", "UNKNOWN")
        name        = d.get("name", "")
        c_days      = d.get("C_days")
        b_days      = d.get("B_days")
        a_days      = d.get("A_days")
        flow_status = d.get("flow_status", "N/A") or "N/A"
        fgn_streak  = d.get("foreign_streak")
        mgn_chg     = d.get("margin_change_5d")
        vol_ratio   = d.get("volume_ratio")
        bias_ma20   = d.get("bias_ma20")
        cost_level  = d.get("cost_level", "N/A") or "N/A"
        decision    = d.get("decision", "UNKNOWN")
        state       = d.get("state", "UNKNOWN")
        confidence  = d.get("confidence", 0)
        risk        = d.get("risk", "N/A") or "N/A"
        risk_flag   = d.get("risk_flag")
        reason_list = d.get("reason", [])
        explanation = d.get("explanation", {})

        lines = [
            f"[Decision Snapshot - {date_str}]",
            f"STOCK_ID: {stock_id}",
            f"NAME: {name}",
            f"C_DAYS: {_fmt_int(c_days)}",
            f"B_DAYS: {_fmt_int(b_days)}",
            f"A_DAYS: {_fmt_int(a_days)}",
            f"FLOW_STATUS: {flow_status}",
            f"FOREIGN_STREAK: {_fmt_int(fgn_streak)}",
            f"MARGIN_CHANGE_5D: {_fmt_int(mgn_chg)}",
            f"VOLUME_RATIO: {_fmt_float(vol_ratio)}",
            f"BIAS_MA20: {_fmt_pct(bias_ma20)}",
            f"COST_LEVEL: {cost_level}",
            f"DECISION: {decision}",
            f"STATE: {state}",
            f"CONFIDENCE: {confidence}",
            "REASON:",
        ]
        for r in reason_list:
            lines.append(f"- {r}")

        lines.append(f"RISK: {risk}")

        if risk_flag:
            lines.append(f"RISK_FLAG: {risk_flag}")

        action_msg = explanation.get("action")
        if action_msg:
            lines.append(f"ACTION: {action_msg}")

        risk_msg = explanation.get("risk")
        if risk_msg:
            lines.append(f"COST_WARNING: {risk_msg}")

        return "\n".join(lines)

    except Exception as exc:
        logger.error("format_decision_snapshot failed: %s", exc)
        sid = decision_dict.get("stock_id", "UNKNOWN") if isinstance(decision_dict, dict) else "UNKNOWN"
        return f"[Decision Snapshot - ERROR]\nSTOCK_ID: {sid}\nERROR: {exc}"


# ── CSV exporters ─────────────────────────────────────────────────────────────

_LATEST_COLS = [
    "date", "stock_id", "name", "decision", "confidence",
    "C_days", "B_days", "A_days", "flow_status", "cost_level", "signal_type",
    "adx", "atr", "vwap", "kd_k", "kd_d", "bb_upper", "bb_middle", "bb_lower",
    "B_quality", "B_window_20", "B_validity", "B_phase",
]


def export_latest_decisions(
    decisions: list,
    path: str = "latest_decisions.csv",
) -> None:
    """Write the latest decision for each stock to a CSV (one row per stock).

    Columns: date, stock_id, name, decision, confidence,
             C_days, B_days, A_days, flow_status, cost_level.

    Overwrites the file on each call (represents current run).
    Never raises.
    """
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_LATEST_COLS, extrasaction="ignore")
            writer.writeheader()
            for d in decisions:
                try:
                    row = {col: d.get(col, "") for col in _LATEST_COLS}
                    writer.writerow(row)
                except Exception as exc:
                    logger.error("export_latest_decisions: skipping row: %s", exc)
    except Exception as exc:
        logger.error("export_latest_decisions failed (%s): %s", path, exc)


_STATE_LOG_COLS = ["date", "stock_id", "prev_state", "new_state", "score", "decision"]


def append_state_log(
    decision_dict: dict,
    prev_state: Optional[str],
    path: str = "state_log.csv",
) -> None:
    """Append one row to the state log CSV.

    Columns: date, stock_id, prev_state, new_state, score, decision.
    Creates the file with header if it doesn't exist. Never raises.
    """
    try:
        file_exists = Path(path).exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_STATE_LOG_COLS)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "date":       decision_dict.get("date", ""),
                "stock_id":   decision_dict.get("stock_id", ""),
                "prev_state": prev_state or "",
                "new_state":  decision_dict.get("state", ""),
                "score":      decision_dict.get("confidence", 0),
                "decision":   decision_dict.get("decision", ""),
            })
    except Exception as exc:
        logger.error("append_state_log failed (%s): %s", path, exc)
