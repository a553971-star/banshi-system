"""
feature_engine.py — 磐石決策系統 Phase 1
Computes all derived feature columns from raw OHLCV + institutional data.

Rules enforced:
- All outputs use None (never NaN, never 0) for missing values.
- A single-column failure sets that column to None; others are unaffected.
- build_features() is a pure function: input DataFrame is never mutated.
- All compute_* functions return pd.Series with dtype=object (None-safe).
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ── Internal helper ──────────────────────────────────────────────────────────

def _to_none(series: pd.Series) -> pd.Series:
    """Convert NaN/NA → None in a Series, return object dtype.

    Downstream consumers receive None, never NaN.
    """
    na_mask = pd.isna(series)
    result = series.astype(object)
    result[na_mask] = None
    return result


def _numeric(series: pd.Series) -> pd.Series:
    """Coerce a (potentially object-dtype) Series to float64 for calculation.

    None and non-numeric values become NaN.
    """
    return pd.to_numeric(series, errors="coerce")


# ── Individual compute functions ─────────────────────────────────────────────

def compute_ma(series: pd.Series, window: int) -> pd.Series:
    """Rolling mean of `series` over `window` days.

    Returns None where fewer than `window` valid (non-None) values exist.
    """
    try:
        result = _numeric(series).rolling(window=window, min_periods=window).mean()
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_ma(window=%d) failed: %s", window, exc)
        return pd.Series([None] * len(series), index=series.index, dtype=object)


def compute_bias(close: pd.Series, ma: pd.Series) -> pd.Series:
    """(close / ma) - 1.

    Returns None where either close or ma is None.
    Result is a ratio (e.g. 0.025 means +2.5%).
    """
    try:
        c = _numeric(close)
        m = _numeric(ma)
        result = (c / m) - 1.0
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_bias failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_volume_ratio(volume: pd.Series) -> pd.Series:
    """volume / MA20(volume).

    Returns None where volume is None, MA20 window < 20 valid values,
    or MA20(volume) == 0.
    """
    try:
        v = _numeric(volume)
        ma20_vol = v.rolling(window=20, min_periods=20).mean()
        # Avoid division by zero: replace 0 with NaN before dividing
        ma20_vol_safe = ma20_vol.where(ma20_vol != 0.0, other=float("nan"))
        result = v / ma20_vol_safe
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_volume_ratio failed: %s", exc)
        return pd.Series([None] * len(volume), index=volume.index, dtype=object)


def compute_return_nd(close: pd.Series, n: int) -> pd.Series:
    """(close / close.shift(n)) - 1 as percentage.

    Returns None where fewer than n valid prior rows exist.
    Example: 3.2 means +3.2%.
    """
    try:
        c = _numeric(close)
        shifted = c.shift(n)
        # shifted NaN where insufficient history; 0 denominator guarded
        shifted_safe = shifted.where(shifted != 0.0, other=float("nan"))
        result = ((c / shifted_safe) - 1.0) * 100.0
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_return_nd(n=%d) failed: %s", n, exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_margin_change_nd(margin_balance: pd.Series, n: int) -> pd.Series:
    """margin_balance[t] - margin_balance[t-n].

    Returns None where either endpoint is None or insufficient rows.
    """
    try:
        mb = _numeric(margin_balance)
        result = mb - mb.shift(n)
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_margin_change_nd(n=%d) failed: %s", n, exc)
        return pd.Series([None] * len(margin_balance), index=margin_balance.index, dtype=object)


def compute_foreign_consecutive_buy(foreign_net: pd.Series) -> pd.Series:
    """Streak of consecutive days with foreign_net > 0.

    Rules (v1.7):
    - foreign_net > 0  → streak + 1, output streak value.
    - foreign_net <= 0 → streak resets to 0, output 0.
    - foreign_net is None → streak resets to 0, output None.
    """
    streak = 0
    result = []
    try:
        for val in foreign_net:
            is_null = (val is None) or (isinstance(val, float) and pd.isna(val))
            if is_null:
                streak = 0        # force reset on None (v1.7 rule 4)
                result.append(None)
            elif float(val) > 0:
                streak += 1
                result.append(streak)
            else:
                streak = 0
                result.append(0)
    except Exception as exc:
        logger.error("compute_foreign_consecutive_buy failed: %s", exc)
        return pd.Series([None] * len(foreign_net), index=foreign_net.index, dtype=object)
    return pd.Series(result, index=foreign_net.index, dtype=object)


def compute_is_new_low(close: pd.Series) -> pd.Series:
    """True if close strictly below the minimum of the prior 59 days.

    Formula: reference = close.shift(1).rolling(20, min_periods=20).min()
             is_new_low = close < reference  (strict less-than)

    Returns None where reference or close is None (window < 60 valid rows).
    A flat bottom does NOT trigger is_new_low — only a genuine downward break.
    """
    try:
        c = _numeric(close)
        reference = c.shift(1).rolling(window=30, min_periods=30).min()
        result = []
        for c_val, r_val in zip(c, reference):
            if pd.isna(c_val) or pd.isna(r_val):
                result.append(None)
            else:
                result.append(bool(c_val < r_val))
        return pd.Series(result, index=close.index, dtype=object)
    except Exception as exc:
        logger.error("compute_is_new_low failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_volatility_5d(close: pd.Series) -> pd.Series:
    """Standard deviation of the 5 most-recent daily returns, as percentage.

    daily_return[t] = (close[t] / close[t-1]) - 1
    Returns None where fewer than 5 valid return pairs exist.
    Example: 3.5 means 3.5%.
    """
    try:
        c = _numeric(close)
        daily_ret = c.pct_change() * 100.0  # first row → NaN
        result = daily_ret.rolling(window=5, min_periods=5).std()
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_volatility_5d failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_volatility_5d_prev(close: pd.Series) -> pd.Series:
    """Standard deviation of the 5 daily returns ending the day before yesterday.

    daily_return[t] = (close[t] / close[t-1]) - 1
    volatility_5d_prev[t] = std of returns[t-5 .. t-1]  (shifted by 1)
    Returns None where fewer than 5 valid return pairs exist.
    """
    try:
        c = _numeric(close)
        daily_ret = c.pct_change() * 100.0
        result = daily_ret.shift(1).rolling(window=5, min_periods=5).std()
        return _to_none(result)
    except Exception as exc:
        logger.error("compute_volatility_5d_prev failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


# ── Observational indicators (no decision impact) ────────────────────────────

def compute_adx(
    high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14
) -> pd.Series:
    """Average Directional Index. Returns None where data insufficient."""
    try:
        h = _numeric(high)
        l = _numeric(low)
        c = _numeric(close)
        prev_c = c.shift(1)
        tr = pd.concat([
            h - l,
            (h - prev_c).abs(),
            (l - prev_c).abs(),
        ], axis=1).max(axis=1)
        up   = h - h.shift(1)
        down = l.shift(1) - l
        pos_dm = up.where((up > down) & (up > 0), 0.0)
        neg_dm = down.where((down > up) & (down > 0), 0.0)
        atr_s   = tr.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
        pos_di  = 100 * pos_dm.ewm(alpha=1 / window, min_periods=window, adjust=False).mean() / atr_s
        neg_di  = 100 * neg_dm.ewm(alpha=1 / window, min_periods=window, adjust=False).mean() / atr_s
        dx      = (100 * (pos_di - neg_di).abs() / (pos_di + neg_di).replace(0, float("nan")))
        adx     = dx.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
        return _to_none(adx.round(2))
    except Exception as exc:
        logger.error("compute_adx failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_atr(
    high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14
) -> pd.Series:
    """Average True Range. Returns None where data insufficient."""
    try:
        h = _numeric(high)
        l = _numeric(low)
        c = _numeric(close)
        prev_c = c.shift(1)
        tr = pd.concat([
            h - l,
            (h - prev_c).abs(),
            (l - prev_c).abs(),
        ], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
        return _to_none(atr.round(4))
    except Exception as exc:
        logger.error("compute_atr failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_vwap(
    high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series
) -> pd.Series:
    """Cumulative VWAP from the first row of the series. Returns None where data insufficient."""
    try:
        h = _numeric(high)
        l = _numeric(low)
        c = _numeric(close)
        v = _numeric(volume)
        typical = (h + l + c) / 3.0
        cum_vol = v.cumsum()
        vwap = (typical * v).cumsum() / cum_vol.replace(0, float("nan"))
        return _to_none(vwap.round(4))
    except Exception as exc:
        logger.error("compute_vwap failed: %s", exc)
        return pd.Series([None] * len(close), index=close.index, dtype=object)


def compute_kd(
    close: pd.Series, window: int = 9
) -> tuple[pd.Series, pd.Series]:
    """KD (Stochastic). Returns (K, D) — None where data insufficient."""
    try:
        c = _numeric(close)
        low_n  = c.rolling(window=window, min_periods=window).min()
        high_n = c.rolling(window=window, min_periods=window).max()
        rsv    = 100 * (c - low_n) / (high_n - low_n).replace(0, float("nan"))
        k      = rsv.ewm(alpha=1 / 3, adjust=False).mean()
        d      = k.ewm(alpha=1 / 3, adjust=False).mean()
        return _to_none(k.round(2)), _to_none(d.round(2))
    except Exception as exc:
        logger.error("compute_kd failed: %s", exc)
        nan_s = pd.Series([None] * len(close), index=close.index, dtype=object)
        return nan_s, nan_s.copy()


def compute_bollinger_bands(
    close: pd.Series, window: int = 20, std: int = 2
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger Bands. Returns (upper, middle, lower) — None where data insufficient."""
    try:
        c      = _numeric(close)
        middle = c.rolling(window=window, min_periods=window).mean()
        sigma  = c.rolling(window=window, min_periods=window).std()
        upper  = middle + std * sigma
        lower  = middle - std * sigma
        return _to_none(upper.round(4)), _to_none(middle.round(4)), _to_none(lower.round(4))
    except Exception as exc:
        logger.error("compute_bollinger_bands failed: %s", exc)
        nan_s = pd.Series([None] * len(close), index=close.index, dtype=object)
        return nan_s, nan_s.copy(), nan_s.copy()


# ── Master builder ────────────────────────────────────────────────────────────

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all feature columns and return a new DataFrame.

    Input: raw merged DataFrame from data_fetcher.merge_all().
    Output: new DataFrame with all original columns plus:
            ma20, ma60, ma120, bias_ma20, bias_ma60,
            volume_ratio, return_10d, margin_change_5d,
            foreign_consecutive_buy, is_new_low, volatility_5d.

    Pure function — input is never mutated.
    A single-column failure sets that column to None; others proceed normally.
    No NaN in any output column.
    """
    result = df.copy()

    # Convert all raw columns from NaN → None for consistency downstream
    raw_cols = [
        "open", "high", "low", "close", "volume",
        "foreign_buy", "foreign_sell", "foreign_net",
        "investment_buy", "investment_sell", "investment_net",
        "dealer_net", "margin_balance", "short_balance",
    ]
    for col in raw_cols:
        if col in result.columns:
            try:
                result[col] = _to_none(result[col])
            except Exception as exc:
                logger.error("Failed to clean raw column %s: %s", col, exc)

    # Moving averages
    result["ma20"] = compute_ma(df["close"], 20)
    result["ma60"] = compute_ma(df["close"], 60)
    result["ma120"] = compute_ma(df["close"], 120)

    # Bias (deviation from MA)
    result["bias_ma20"] = compute_bias(df["close"], result["ma20"])
    result["bias_ma60"] = compute_bias(df["close"], result["ma60"])

    # Volume ratio
    result["volume_ratio"] = compute_volume_ratio(df["volume"])

    # Returns
    result["return_10d"] = compute_return_nd(df["close"], 10)

    # Margin change
    if "margin_balance" in df.columns:
        result["margin_change_5d"] = compute_margin_change_nd(df["margin_balance"], 5)
    else:
        result["margin_change_5d"] = pd.Series(
            [None] * len(df), index=df.index, dtype=object
        )

    # Institutional streak
    if "foreign_net" in df.columns:
        result["foreign_consecutive_buy"] = compute_foreign_consecutive_buy(df["foreign_net"])
    else:
        result["foreign_consecutive_buy"] = pd.Series(
            [None] * len(df), index=df.index, dtype=object
        )

    # New low flag
    result["is_new_low"] = compute_is_new_low(df["close"])

    # Volatility
    result["volatility_5d"]      = compute_volatility_5d(df["close"])
    result["volatility_5d_prev"] = compute_volatility_5d_prev(df["close"])

    # Observational indicators — no decision impact
    result["adx"] = compute_adx(df["high"], df["low"], df["close"])
    result["atr"] = compute_atr(df["high"], df["low"], df["close"])
    result["vwap"] = compute_vwap(df["high"], df["low"], df["close"], df["volume"])
    result["kd_k"], result["kd_d"] = compute_kd(df["close"])
    result["bb_upper"], result["bb_middle"], result["bb_lower"] = compute_bollinger_bands(df["close"])

    return result
