"""
trajectory_engine.py — 磐石決策系統 Phase 2
Strict row-by-row state machine for C / B / A segment computation.

Rules (v1.7):
- No lookahead. Each row is processed using only data up to that row.
- All thresholds read from params dict — no hardcoded numbers.
- Pure function: input DataFrame never mutated; state is local to the call.
- Output columns: C_days (int|None), B_days (int|None), A_days (int|None).

Segment semantics:
  C_days = None  : no C segment has ever started.
  C_days = int   : days elapsed in current (or completed+frozen) C segment.
  B_days = None  : C not yet complete.
  B_days = 0     : C complete, but B conditions not met / reset.
  B_days = int   : consecutive days in B band.
  A_days = None  : B has never reached >= 3.
  A_days = 0     : B eligible, but A not yet triggered / reset.
  A_days = int   : days in active A segment (1-based, never resets unless condition).
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Sentinel used to check "missing" for both None and float NaN
_MISSING = object()


def _val(v) -> Optional[float]:
    """Return None if v is None or NaN, else return float(v)."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if (f != f) else f   # NaN check: NaN != NaN
    except (TypeError, ValueError):
        return None


def compute_trajectory(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    Compute C/B/A segment day-counters row by row.

    Adds columns C_days, B_days, A_days to a copy of the input DataFrame.
    Pure function — input is never modified.
    All thresholds come from params.

    Trajectory rules:
    ── C segment ────────────────────────────────────────────────────────
    Starts  : is_new_low == True → C_days = 1, resets B and A to 0.
    Active  : C_days increments each day.
    Ends    : 3 consecutive days where close >= ma20 * c_safe_threshold
              AND is_new_low == False.  C_days frozen at completion value.
    Restart : Any new is_new_low resets C_days = 1, B_days = 0, A_days = 0.

    ── B segment (requires C complete) ──────────────────────────────────
    Active  : close ∈ [ma20*b_lower, ma20*b_upper] AND volatility_5d < b_vol.
    Break   : any single day failing condition → B_days = 0 (never None).
    Pre-C   : B_days = None.

    ── A segment (requires B_days >= 3 ever reached) ────────────────────
    Entry   : close > ma20 * a_entry_ma AND volume_ratio >= a_entry_vol.
    Active  : A_days increments every day.
    Reset 1 : close < ma20  → A_days = 0 immediately.
    Reset 2 : volume_ratio < a_reset_vol_low for a_reset_vol_consec consecutive days.
    None    : volume_ratio is None → does NOT trigger either reset.
    Pre-B   : A_days = None.
    """
    # ── Load thresholds from params ──────────────────────────────────────
    c_safe   = params.get("c_safe_threshold", 0.98)
    c_end_n  = int(params.get("c_end_consecutive", 3))
    b_lo     = params.get("b_lower", 0.97)
    b_hi     = params.get("b_upper", 1.03)
    b_vol    = params.get("b_vol_threshold", 6.0)
    a_ma_m   = params.get("a_entry_ma_mult", 1.03)
    a_vol_e  = params.get("a_entry_vol_ratio", 1.2)
    a_vol_lo = params.get("a_reset_vol_low", 0.5)
    a_vol_cn = int(params.get("a_reset_vol_consec", 3))

    # ── Extract column values (object lists — may contain None) ──────────
    n = len(df)
    close_v  = df["close"].tolist()      if "close"       in df.columns else [None] * n
    ma20_v   = df["ma20"].tolist()       if "ma20"        in df.columns else [None] * n
    vol5d_v  = df["volatility_5d"].tolist() if "volatility_5d" in df.columns else [None] * n
    volr_v   = df["volume_ratio"].tolist()  if "volume_ratio"  in df.columns else [None] * n
    is_nl_v  = df["is_new_low"].tolist() if "is_new_low"   in df.columns else [None] * n

    # ── Output arrays ────────────────────────────────────────────────────
    c_out = [None] * n
    b_out = [None] * n
    a_out = [None] * n

    # ── State variables (local — no global state) ────────────────────────
    c_state   = "none"    # "none" | "active" | "complete"
    c_days    = 0
    c_frozen  = None      # frozen value once complete
    c_end_cnt = 0         # consecutive safe days toward C completion

    b_days    = 0
    b_eligible = False    # True once b_days >= 3 (ever); survives B resets

    a_state   = "watching"  # "watching" | "active"
    a_days    = 0
    a_low_cnt = 0           # consecutive low-volume days

    for i in range(n):
        close  = _val(close_v[i])
        ma20   = _val(ma20_v[i])
        vol5d  = _val(vol5d_v[i])
        volr   = _val(volr_v[i])
        is_nl  = is_nl_v[i]  # True | False | None

        # ── STEP 1: New low — highest priority, resets everything ────────
        if is_nl is True:
            c_state   = "active"
            c_days    = 1
            c_end_cnt = 0
            c_frozen  = None
            b_days    = 0
            b_eligible = False
            a_state   = "watching"
            a_days    = 0
            a_low_cnt = 0
            c_out[i]  = 1
            b_out[i]  = 0   # explicit reset per spec (not None)
            a_out[i]  = 0   # explicit reset per spec (not None)
            continue

        # ── STEP 2: C segment processing ────────────────────────────────
        if c_state == "active":
            c_days += 1
            # Accumulate consecutive safe days (both conditions must hold)
            if close is not None and ma20 is not None and is_nl is False:
                if close >= ma20 * c_safe:
                    c_end_cnt += 1
                    if c_end_cnt >= c_end_n:
                        c_state  = "complete"
                        c_frozen = c_days
                else:
                    c_end_cnt = 0
            else:
                # is_nl is None or price data missing → reset safe streak
                c_end_cnt = 0
            c_out[i] = c_days

        elif c_state == "complete":
            c_out[i] = c_frozen   # frozen; never changes

        else:
            c_out[i] = None       # no C segment yet

        # ── STEP 3: B segment (only when C complete) ─────────────────────
        if c_state == "complete":
            b_cond = False
            if close is not None and ma20 is not None and vol5d is not None:
                lo = ma20 * b_lo
                hi = ma20 * b_hi
                if lo <= close <= hi and vol5d < b_vol:
                    b_cond = True

            if b_cond:
                b_days += 1
                if b_days >= 3:
                    b_eligible = True
            else:
                b_days = 0   # any break resets (never None once C complete)

            b_out[i] = b_days
        else:
            b_out[i] = None   # C not complete

        # ── STEP 4: A segment (only when b_eligible) ─────────────────────
        if not b_eligible:
            a_out[i] = None
            continue

        if a_state == "active":
            # Check reset conditions before incrementing
            reset = False

            # Condition 1: close < ma20 (immediate reset)
            # Only trigger if both values are available (None → no trigger)
            if close is not None and ma20 is not None:
                if close < ma20:
                    reset = True

            # Condition 2: volume_ratio < threshold for N consecutive days
            # None input → do NOT modify counter, do NOT trigger (v1.7 rule 5)
            if volr is not None:
                if volr < a_vol_lo:
                    a_low_cnt += 1
                    if a_low_cnt >= a_vol_cn:
                        reset = True
                else:
                    a_low_cnt = 0
            # volr is None → counter unchanged, no trigger

            if reset:
                a_days    = 0
                a_state   = "watching"
                a_low_cnt = 0
                a_out[i]  = 0
            else:
                a_days   += 1
                a_out[i]  = a_days

        else:  # a_state == "watching"
            # Check entry trigger
            triggered = False
            if close is not None and ma20 is not None and volr is not None:
                if close > ma20 * a_ma_m and volr >= a_vol_e:
                    triggered = True

            if triggered:
                a_state   = "active"
                a_days    = 1
                a_low_cnt = 0
                a_out[i]  = 1
            else:
                a_out[i] = 0   # eligible but not yet triggered

    # ── Assemble result ──────────────────────────────────────────────────
    result = df.copy()
    result["C_days"] = pd.Series(c_out, index=df.index, dtype=object)
    result["B_days"] = pd.Series(b_out, index=df.index, dtype=object)
    result["A_days"] = pd.Series(a_out, index=df.index, dtype=object)
    return result


def get_latest_trajectory(df: pd.DataFrame) -> dict:
    """Return C/B/A day-counts from the last row of a trajectory DataFrame.

    Pure function. Returns None for all fields if DataFrame is empty.
    """
    if df.empty:
        return {"C_days": None, "B_days": None, "A_days": None, "date": None}
    try:
        last = df.iloc[-1]
        date_val = last.get("date", None)
        date_str = (
            date_val.strftime("%Y-%m-%d")
            if hasattr(date_val, "strftime")
            else str(date_val) if date_val is not None else None
        )
        return {
            "C_days": last.get("C_days", None),
            "B_days": last.get("B_days", None),
            "A_days": last.get("A_days", None),
            "date":   date_str,
        }
    except Exception as exc:
        logger.error("get_latest_trajectory failed: %s", exc)
        return {"C_days": None, "B_days": None, "A_days": None, "date": None}
