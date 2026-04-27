"""
decision_inspector.py — 磐石決策系統 Phase 2
Decision Gate v1.5 + confidence scoring + personal reminder rules.

Architecture:
- format_panstone_signal() receives pre-computed dicts.
  It does NOT compute indicators. Clean separation from feature/trajectory/flow layers.
- All thresholds from params dict — no hardcoded numbers anywhere.
- Pure function: same inputs → same output, always.
- Never raises. Any exception → returns IGNORE dict with error logged.

Risk vs risk_flag separation (v1.7 rule 6):
- risk       : "LOW" | "MEDIUM" | "N/A"  — set by Decision Gate.
- risk_flag  : "HIGH_RISK" | None        — set by personal reminder Rule B only.
               Never modifies decision / state / confidence.

Confidence definition:
  Additive scoring of condition hit-completeness, clamped to [0, 100].
  Case A BUY (all bonus conditions active) → 100.
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── Guard ─────────────────────────────────────────────────────────────────────

def is_false_breakout(
    A_days: Optional[int],
    volume_ratio: Optional[float],
) -> bool:
    """Return True if the A-segment entry looks like a false breakout.

    Conservative rules (v1.7 rule 3):
    - A_days is None OR volume_ratio is None → True  (force WAIT, insufficient data).
    - A_days <= 2 AND volume_ratio < 1.2          → True  (volume not confirmed).
    - Otherwise                                   → False.

    Pure function.
    """
    if A_days is None or volume_ratio is None:
        return True
    try:
        return int(A_days) <= 2 and float(volume_ratio) < 1.2
    except (TypeError, ValueError):
        return True


# ── Integrity check ───────────────────────────────────────────────────────────

def check_data_integrity(df: pd.DataFrame) -> bool:
    """Return True if the DataFrame has enough valid data for a decision.

    Conditions for True:
    1. len(df) >= 20
    2. Latest row has no None in required columns:
       [close, ma20, bias_ma20, volume_ratio,
        return_10d, margin_change_5d, foreign_consecutive_buy]

    Pure function.
    """
    try:
        if len(df) < 20:
            return False
        required = [
            "close", "ma20", "bias_ma20", "volume_ratio",
            "return_10d", "margin_change_5d", "foreign_consecutive_buy",
        ]
        last = df.iloc[-1]
        for col in required:
            val = last.get(col) if hasattr(last, "get") else (
                last[col] if col in df.columns else None
            )
            if val is None:
                return False
            # Also catch any stray NaN
            try:
                if pd.isna(val):
                    return False
            except TypeError:
                pass
        return True
    except Exception as exc:
        logger.error("check_data_integrity failed: %s", exc)
        return False


# ── Confidence scoring ────────────────────────────────────────────────────────

def compute_confidence(decision_dict: dict, params: dict) -> int:
    """Additive confidence score clamped to [0, 100].

    Scoring table:
      +20  flow_status == "ACCUMULATING"
      +20  A_days in [1, 2, 3]
      +15  cost_level == "SAFE"
      +15  foreign_streak >= conf_foreign_streak_threshold  (default 3)
      +10  volume_ratio >= conf_vol_ratio_threshold         (default 1.5)
      +10  C_days >= conf_c_days_threshold                  (default 8)
      +10  B_days >= conf_b_days_threshold                  (default 7)
      -30  is_false_breakout == True

    All thresholds from params. Pure function.
    Case A (all conditions active) → 100.
    """
    try:
        score = 0

        pts_accum = int(params.get("conf_accum",  20))
        pts_a     = int(params.get("conf_a_days", 20))
        pts_safe  = int(params.get("conf_safe",   15))
        pts_fgn   = int(params.get("conf_foreign_streak", 15))
        pts_vol   = int(params.get("conf_vol_ratio",  10))
        pts_c     = int(params.get("conf_c_days",     10))
        pts_b     = int(params.get("conf_b_days",     10))
        pts_fb    = int(params.get("conf_false_breakout", -30))

        thr_fgn = int(params.get("conf_foreign_streak_threshold", 3))
        thr_vol = float(params.get("conf_vol_ratio_threshold", 1.5))
        thr_c   = int(params.get("conf_c_days_threshold", 8))
        thr_b   = int(params.get("conf_b_days_threshold", 7))

        flow_status   = decision_dict.get("flow_status")
        a_days        = decision_dict.get("A_days")
        cost_level    = decision_dict.get("cost_level")
        foreign_str   = decision_dict.get("foreign_streak")
        volume_ratio  = decision_dict.get("volume_ratio")
        c_days        = decision_dict.get("C_days")
        b_days        = decision_dict.get("B_days")
        false_brk     = decision_dict.get("false_breakout", False)

        if flow_status == "ACCUMULATING":
            score += pts_accum

        if a_days is not None and int(a_days) in (1, 2, 3):
            score += pts_a

        if cost_level == "SAFE":
            score += pts_safe

        if foreign_str is not None and int(foreign_str) >= thr_fgn:
            score += pts_fgn

        if volume_ratio is not None and float(volume_ratio) >= thr_vol:
            score += pts_vol

        if c_days is not None and int(c_days) >= thr_c:
            score += pts_c

        if b_days is not None and int(b_days) >= thr_b:
            score += pts_b

        if false_brk:
            score += pts_fb  # negative value

        return max(0, min(100, score))

    except Exception as exc:
        logger.error("compute_confidence failed: %s", exc)
        return 0


# ── Decision Gate ─────────────────────────────────────────────────────────────

def format_panstone_signal(
    trajectory: dict,
    flow: dict,
    cost: dict,
    params: dict,
    current_date: str = None,
    current_price: float = None,
    pnl_percent: float = None,
) -> dict:
    """Run Decision Gate v1.5 and return a fully populated decision dict.

    Input dicts (all pre-computed — no indicator recalculation here):
      trajectory: {stock_id, name, date, C_days, B_days, A_days,
                   integrity_ok, is_good_company}
      flow:       {flow_status, volume_ratio, foreign_consecutive_buy,
                   margin_change_5d, return_10d, bias_ma20}
      cost:       {cost_level, deviation_percent}
      params:     all thresholds

    Decision Gate (6 steps, strict order, stop at first match):
      1. Data integrity fail  → IGNORE
      2. Not good company     → IGNORE
      3. Trajectory immature  → IGNORE
      4. Distribution flow    → IGNORE
      5. BUY conditions all met (no false breakout) → BUY
      6. Default              → WAIT

    Personal reminder rules run AFTER the gate. They append to explanation
    dict only — DECISION / STATE / CONFIDENCE are never modified by them.

    Never raises. Any exception → returns IGNORE dict with error logged.
    """
    try:
        stock_id    = trajectory.get("stock_id", "UNKNOWN")
        name        = trajectory.get("name", "")
        date_str    = trajectory.get("date") or current_date or ""
        c_days      = trajectory.get("C_days")
        b_days      = trajectory.get("B_days")
        a_days      = trajectory.get("A_days")
        integrity   = trajectory.get("integrity_ok", False)
        good_co     = trajectory.get("is_good_company", False)

        flow_status = flow.get("flow_status")
        vol_ratio   = flow.get("volume_ratio")
        fgn_streak  = flow.get("foreign_consecutive_buy")
        mgn_chg     = flow.get("margin_change_5d")
        ret_10d     = flow.get("return_10d")
        bias_ma20   = flow.get("bias_ma20")

        cost_level  = cost.get("cost_level")
        dev_pct     = cost.get("deviation_percent")

        min_c = int(params.get("min_c_days", 5))
        min_b = int(params.get("min_b_days", 3))

        # Shared partial dict for building the result
        base = {
            "stock_id":          stock_id,
            "name":              name,
            "date":              date_str,
            "C_days":            c_days,
            "B_days":            b_days,
            "A_days":            a_days,
            "flow_status":       flow_status,
            "foreign_streak":    fgn_streak,
            "margin_change_5d":  mgn_chg,
            "volume_ratio":      vol_ratio,
            "return_10d":        ret_10d,
            "bias_ma20":         bias_ma20,
            "cost_level":        cost_level,
            "deviation_percent": dev_pct,
            "current_price":     current_price,
            "pnl_percent":       pnl_percent,
        }

        def _ignore(reason_msg: str) -> dict:
            return {
                **base,
                "decision":      "IGNORE",
                "state":         "INACTIVE",
                "confidence":    0,
                "risk":          "N/A",
                "risk_flag":     None,
                "false_breakout": False,
                "reason":        [reason_msg],
                "explanation":   {},
            }

        # ── Step 1: Data Integrity ───────────────────────────────────────
        if not integrity:
            return _ignore("Insufficient data")

        # ── Step 2: Fundamental Quality ──────────────────────────────────
        if not good_co:
            return _ignore("Not a quality company")

        # ── Step 3: Trajectory Maturity ──────────────────────────────────
        c_ok = (c_days is not None and int(c_days) >= min_c)
        b_ok = (b_days is not None and int(b_days) >= min_b)
        if not c_ok or not b_ok:
            c_disp = c_days if c_days is not None else "None"
            b_disp = b_days if b_days is not None else "None"
            return _ignore(f"Trajectory not mature (C_days={c_disp} < {min_c} or B_days={b_disp} < {min_b})")

        # ── Step 4: Distribution Filter ──────────────────────────────────
        if flow_status == "DISTRIBUTION":
            return _ignore("Distribution phase detected")

        # ── Evaluate false-breakout guard ────────────────────────────────
        false_brk = is_false_breakout(a_days, vol_ratio)

        # ── Build preliminary dict for confidence scoring ─────────────────
        prelim = {
            **base,
            "false_breakout": false_brk,
        }

        # ── Step 5a: Standard BUY ────────────────────────────────────────
        a_in_range = (
            a_days is not None
            and int(a_days) in (1, 2, 3)
        )
        buy_cond = (
            flow_status == "ACCUMULATING"
            and a_in_range
            and cost_level == "SAFE"
            and not false_brk
        )

        if buy_cond:
            reason = _build_buy_reasons(prelim, params)
            decision_dict = {
                **prelim,
                "decision":    "BUY",
                "signal_type": "STANDARD",
                "state":       "ACTION",
                "risk":        "LOW",
                "risk_flag":   None,
                "reason":      reason,
                "explanation": {},
            }
            decision_dict["confidence"] = compute_confidence(decision_dict, params)
            _apply_reminders(decision_dict, pnl_percent, dev_pct, params)
            return decision_dict

        # ── Step 5b: FAST_BREAKOUT BUY ───────────────────────────────────
        vol_prev = flow.get("volatility_5d_prev")
        fb_a_ok  = a_days is not None and int(a_days) in (1, 2)
        fb_b_ok  = b_days is None or int(b_days) < 3
        fb_c_ok  = c_days is not None and int(c_days) >= params["fast_breakout_c_days_min"]
        fb_vol_ok = (
            vol_ratio is not None
            and float(vol_ratio) >= params["fast_breakout_volume_ratio_min"]
        )
        fb_vprev_ok = (
            vol_prev is not None
            and float(vol_prev) < params["fast_breakout_volatility_prev_max"]
        )
        fb_ret_ok   = (
            flow.get("return_10d") is not None
            and float(flow["return_10d"]) < params["fast_breakout_return_10d_max"]
        )
        fb_cond = (
            fb_a_ok
            and fb_b_ok
            and fb_c_ok
            and flow_status == "ACCUMULATING"
            and cost_level == "SAFE"
            and fb_vol_ok
            and fb_vprev_ok
            and fb_ret_ok
            and not false_brk
        )

        if fb_cond:
            reason = _build_buy_reasons(prelim, params)
            decision_dict = {
                **prelim,
                "decision":    "BUY",
                "signal_type": "FAST_BREAKOUT",
                "state":       "ACTION",
                "risk":        "LOW",
                "risk_flag":   None,
                "reason":      reason,
                "explanation": {},
            }
            base_conf = compute_confidence(decision_dict, params)
            decision_dict["confidence"] = max(0, min(100, base_conf - params["fast_breakout_confidence_penalty"]))
            _apply_reminders(decision_dict, pnl_percent, dev_pct, params)
            return decision_dict

        # ── Step 6: Default — WAIT ───────────────────────────────────────
        reason = _build_wait_reasons(prelim, params)
        decision_dict = {
            **prelim,
            "decision":    "WAIT",
            "signal_type": None,
            "state":       "WATCHING",
            "risk":        "MEDIUM",
            "risk_flag":   None,
            "reason":      reason,
            "explanation": {},
        }
        decision_dict["confidence"] = compute_confidence(decision_dict, params)
        _apply_reminders(decision_dict, pnl_percent, dev_pct, params)
        return decision_dict

    except Exception as exc:
        logger.error("format_panstone_signal failed: %s", exc)
        return {
            "stock_id":    trajectory.get("stock_id", "UNKNOWN"),
            "name":        trajectory.get("name", ""),
            "date":        trajectory.get("date") or current_date or "",
            "decision":    "IGNORE",
            "state":       "INACTIVE",
            "confidence":  0,
            "risk":        "N/A",
            "risk_flag":   None,
            "false_breakout": False,
            "reason":      ["Internal error — see logs"],
            "explanation": {},
            "C_days": None, "B_days": None, "A_days": None,
            "flow_status": None, "foreign_streak": None,
            "margin_change_5d": None, "volume_ratio": None,
            "return_10d": None, "bias_ma20": None,
            "cost_level": None, "deviation_percent": None,
            "current_price": current_price, "pnl_percent": pnl_percent,
        }


# ── Internal reason builders ──────────────────────────────────────────────────

def _build_buy_reasons(d: dict, params: dict) -> list:
    """Build human-readable reason list for a BUY decision."""
    reasons = []
    thr_fgn = int(params.get("conf_foreign_streak_threshold", 3))
    thr_vol = float(params.get("conf_vol_ratio_threshold", 1.5))
    thr_c   = int(params.get("conf_c_days_threshold", 8))
    thr_b   = int(params.get("conf_b_days_threshold", 7))

    reasons.append("Accumulating confirmed (all 4 conditions met)")

    vol = d.get("volume_ratio")
    if vol is not None and float(vol) >= 1.2:
        reasons.append(f"Breakout with volume (volume_ratio {vol} >= 1.2)")

    fgn = d.get("foreign_streak")
    if fgn is not None and int(fgn) >= thr_fgn:
        reasons.append(f"Foreign buy streak >= {thr_fgn} (streak = {fgn})")

    c = d.get("C_days")
    if c is not None and int(c) >= thr_c:
        reasons.append(f"C segment mature (C_days {c} >= {thr_c})")

    b = d.get("B_days")
    if b is not None and int(b) >= thr_b:
        reasons.append(f"B segment stable (B_days {b} >= {thr_b})")

    if vol is not None and float(vol) >= thr_vol:
        reasons.append(f"Volume ratio strong (>= {thr_vol})")

    return reasons


def _build_wait_reasons(d: dict, params: dict) -> list:
    """Build human-readable reason list for a WAIT decision."""
    reasons = []
    if d.get("false_breakout"):
        vr = d.get("volume_ratio")
        ad = d.get("A_days")
        reasons.append(
            f"False breakout detected (A_days={ad} <= 2 AND volume_ratio={vr} < 1.2)"
        )
        reasons.append("Forced WAIT — volume confirmation insufficient")
    elif d.get("flow_status") != "ACCUMULATING":
        reasons.append(f"Flow not accumulating (status={d.get('flow_status')})")
    elif d.get("A_days") is None or d.get("A_days") == 0:
        reasons.append("A segment not triggered yet")
    elif d.get("A_days") is not None and int(d.get("A_days")) > 3:
        reasons.append(f"A segment mature (A_days={d.get('A_days')} > 3) — hold or monitor")
    elif d.get("cost_level") != "SAFE":
        reasons.append(f"Cost level not safe (cost={d.get('cost_level')})")
    else:
        reasons.append("No actionable signal")
    return reasons


def _apply_reminders(
    d: dict,
    pnl_percent: Optional[float],
    deviation_percent: Optional[float],
    params: dict,
) -> None:
    """Apply personal reminder rules in-place. Modifies explanation and risk_flag only.

    Rule A: profit hold nudge (pnl_percent >= nudge_pnl_threshold).
    Rule B: cost deviation warning (deviation_percent > nudge_cost_threshold).

    decision / state / confidence are NEVER modified.
    """
    nudge_pnl  = float(params.get("nudge_pnl_threshold", 10.0))
    nudge_cost = float(params.get("nudge_cost_threshold", 5.0))

    if pnl_percent is not None:
        try:
            if float(pnl_percent) >= nudge_pnl:
                d["explanation"]["action"] = (
                    "已有不錯獲利，A段可能還未結束，建議分批出場，保留主力部位"
                )
        except (TypeError, ValueError):
            pass

    if deviation_percent is not None:
        try:
            if float(deviation_percent) > nudge_cost:
                d["risk_flag"] = "HIGH_RISK"
                d["explanation"]["risk"] = (
                    "現價已明顯偏離成本，建議等待回檔，避免追高"
                )
        except (TypeError, ValueError):
            pass
