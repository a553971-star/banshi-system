"""
flow_engine.py — 磐石決策系統 Phase 2
Market flow and cost-level classification.

Rules:
- All functions pure and None-safe.
- Any None input → return None (not NEUTRAL).
- DISTRIBUTION is checked before ACCUMULATING (mutually exclusive).
- All thresholds from params dict — no hardcoded numbers.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def classify_flow(
    foreign_consecutive_buy: Optional[int],
    margin_change_5d: Optional[int],
    return_10d: Optional[float],
    volume_ratio: Optional[float],
    params: dict,
) -> Optional[str]:
    """Classify current market flow as DISTRIBUTION, ACCUMULATING, or NEUTRAL.

    Priority: DISTRIBUTION is evaluated first. If its conditions pass, returns
    immediately without checking ACCUMULATING (mutually exclusive).

    Returns:
        "DISTRIBUTION"  — high-volume sell-off with rising margin and strong return.
        "ACCUMULATING"  — quiet accumulation with foreign buying and stable price.
        "NEUTRAL"       — neither condition set fully met.
        None            — any required input is None.

    All threshold keys read from params.
    """
    try:
        # Any None → cannot classify
        inputs = [foreign_consecutive_buy, margin_change_5d, return_10d, volume_ratio]
        if any(v is None for v in inputs):
            return None

        # ── DISTRIBUTION (check first) ────────────────────────────────────
        d_vol   = params.get("dist_vol_ratio", 1.8)
        d_mgn   = params.get("dist_margin_change", 0)   # margin_change_5d > 0
        d_ret   = params.get("dist_return_10d", 5.0)

        if (
            float(volume_ratio) >= d_vol
            and float(margin_change_5d) > d_mgn
            and float(return_10d) > d_ret
        ):
            return "DISTRIBUTION"

        # ── ACCUMULATING (check second) ───────────────────────────────────
        a_fgn = params.get("accum_foreign_consec", 2)
        a_mgn = params.get("accum_margin_change", 0)    # margin_change_5d <= 0
        a_ret = params.get("accum_return_10d", 5.0)
        a_vol = params.get("accum_vol_ratio", 0.8)

        if (
            int(foreign_consecutive_buy) >= a_fgn
            and float(margin_change_5d) <= a_mgn
            and float(return_10d) < a_ret
            and float(volume_ratio) >= a_vol
        ):
            return "ACCUMULATING"

        return "NEUTRAL"

    except Exception as exc:
        logger.error("classify_flow failed: %s", exc)
        return None


def classify_cost(bias_ma20: Optional[float], params: dict) -> dict:
    """Classify entry risk based on deviation from MA20.

    Returns a dict:
        {
            "cost_level":        "SAFE" | "HIGH_RISK" | "NEUTRAL" | None,
            "deviation_percent": float (bias_ma20 * 100) | None,
        }

    Levels:
        SAFE      : bias_ma20 ∈ [cost_safe_lower, cost_safe_upper]  (inclusive)
        HIGH_RISK : bias_ma20 > cost_high_risk
        NEUTRAL   : between SAFE upper and HIGH_RISK threshold
        None      : bias_ma20 is None

    All thresholds from params.
    """
    try:
        if bias_ma20 is None:
            return {"cost_level": None, "deviation_percent": None}

        safe_lo = params.get("cost_safe_lower", -0.03)   # -3%
        safe_hi = params.get("cost_safe_upper",  0.06)   #  +6%
        high_rk = params.get("cost_high_risk",   0.08)   #  +8%

        b = float(bias_ma20)
        dev_pct = round(b * 100.0, 2)

        if b > high_rk:
            level = "HIGH_RISK"
        elif safe_lo <= b <= safe_hi:
            level = "SAFE"
        else:
            level = "NEUTRAL"

        return {"cost_level": level, "deviation_percent": dev_pct}

    except Exception as exc:
        logger.error("classify_cost failed: %s", exc)
        return {"cost_level": None, "deviation_percent": None}
