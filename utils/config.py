# 共用設定載入，供 ui/ 和 pages/ 使用，避免 circular import
import json
import logging
import os
from pathlib import Path

_PROJECT_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_PARAMS = os.path.join(_PROJECT_ROOT, "params.json")

logger = logging.getLogger(__name__)


def load_params(path: str = _DEFAULT_PARAMS) -> dict:
    """Load all decision thresholds from a JSON config file.

    Returns a default params dict if the file is missing or unreadable.
    """
    defaults = {
        "companies_path":  os.path.join(_PROJECT_ROOT, "companies.csv"),
        "db_path":         os.path.join(_PROJECT_ROOT, "banshi.db"),

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

        # Fast-breakout signal
        "fast_breakout_c_days_min":           10,
        "fast_breakout_volume_ratio_min":      1.6,
        "fast_breakout_volatility_prev_max":   5.0,
        "fast_breakout_return_10d_max":        8.0,
        "fast_breakout_a_days_max":            2,
        "fast_breakout_confidence_penalty":   10,

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
        defaults.update(loaded)
        return defaults
    except Exception as exc:
        logger.error("load_params failed (%s): %s — using defaults", path, exc)
        return defaults
