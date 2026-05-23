"""
utils/target_price.py — 個股估值層 v1.0

三層估值架構:
- D 估值區間（主決策）: 近 5 年 PE 分位數法 → 便宜 / 合理 / 略貴 / 昂貴
- B 市場共識合理價: 近 1 年平均 PE × 當前 TTM EPS
- C 技術延伸區: 60 日壓力支撐位置描述（嚴格非買賣建議）

設計紀律:
1. 不做預言,只做位置描述
2. 數字精度防假精準（價格整數、% 1 位小數、PE 1 位小數）
3. PE 時間對齊（避免 look-ahead bias）
4. TTM EPS < TTM_EPS_FLOOR 直接標「無法計算估值」
5. AI 題材重估警示必出現在 disclaimer
6. EPS 衰退 + 估值便宜 → 標「價值陷阱」
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.eps_history import (
    ensure_eps_history_table,
    get_quarters_for_stock,
)

VERSION = "v1.0"

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB  = os.path.join(BASE_DIR, "banshi.db")
GROUPS_JSON = os.path.join(BASE_DIR, "config", "rotation_groups.json")
DEFAULT_OUT = os.path.join(BASE_DIR, "data", "target_prices.json")

# ── 估值參數 ───────────────────────────────────────────────────────────────
PE_FILTER_MIN           = 5.0
PE_FILTER_MAX           = 150.0
TTM_EPS_FLOOR           = 0.5
MIN_VALID_RATIO         = 0.6
LOOKBACK_YEARS          = 5
SHORT_LOOKBACK_DAYS     = 252
TECHNICAL_LOOKBACK_DAYS = 60
EPS_TREND_THRESHOLD     = 0.10

DISCLAIMER = "AI 題材可能造成估值體系重估，歷史 PE 區間僅供參考"


# ── EPS 時間對齊工具 ──────────────────────────────────────────────────────
def _get_ttm_eps_as_of(quarters: List[Dict[str, Any]], as_of_date: str) -> Optional[float]:
    """
    截至 as_of_date 可知的 TTM EPS（過去 4 季加總）。

    quarters 由 get_quarters_for_stock 取得,需按 announcement_date 升序。
    """
    if not quarters or not as_of_date:
        return None
    valid = [q for q in quarters if q["announcement_date"] and q["announcement_date"] <= as_of_date]
    if len(valid) < 4:
        return None
    last4 = valid[-4:]
    try:
        return sum(float(q["eps"]) for q in last4)
    except (TypeError, ValueError):
        return None


def _fetch_price_series(
    stock_id: str,
    db_path: str,
    lookback_days: int,
) -> List[Dict[str, Any]]:
    """取最近 lookback_days 個交易日的 (date, close)。最新在前。"""
    try:
        conn = sqlite3.connect(db_path)
        rows = conn.execute("""
            SELECT date, close FROM price_history
             WHERE stock_id = ? AND close IS NOT NULL
             ORDER BY date DESC LIMIT ?
        """, (str(stock_id), lookback_days)).fetchall()
        conn.close()
    except Exception:
        return []
    return [{"date": r[0], "close": float(r[1])} for r in rows]


def _build_pe_series(
    stock_id: str,
    db_path: str,
    lookback_days: int,
) -> List[Dict[str, Any]]:
    """
    為近 lookback_days 個交易日各算一個 PE（用「截至當日可知」的 TTM EPS）。

    過濾:
    - TTM EPS < TTM_EPS_FLOOR → 跳過
    - PE 不在 [PE_FILTER_MIN, PE_FILTER_MAX] → 跳過

    回傳 [{date, close, ttm_eps, pe}]，最新在前。
    """
    quarters = get_quarters_for_stock(stock_id, db_path)
    if not quarters:
        return []
    price_series = _fetch_price_series(stock_id, db_path, lookback_days)
    if not price_series:
        return []

    out: List[Dict[str, Any]] = []
    for p in price_series:
        ttm = _get_ttm_eps_as_of(quarters, p["date"])
        if ttm is None or ttm < TTM_EPS_FLOOR:
            continue
        pe = p["close"] / ttm
        if not (PE_FILTER_MIN <= pe <= PE_FILTER_MAX):
            continue
        out.append({"date": p["date"], "close": p["close"], "ttm_eps": ttm, "pe": pe})
    return out


def _percentile(sorted_values: List[float], q: float) -> float:
    """sorted_values 須已排序升序; q 為 0~1。"""
    if not sorted_values:
        return 0.0
    idx = int(q * (len(sorted_values) - 1))
    return sorted_values[idx]


# ── EPS 趨勢 ───────────────────────────────────────────────────────────────
def calc_eps_trend(stock_id: str, db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """
    比較近 4 季 EPS 平均 vs 前 4 季 EPS 平均。

    回傳:
    {
        valid, trend (growing/flat/declining/None),
        arrow (↗/→/↘/?),
        label (成長中/持平/衰退中/資料不足),
        recent_4q_eps, previous_4q_eps, change_pct, warning
    }
    """
    quarters = get_quarters_for_stock(stock_id, db_path)
    if len(quarters) < 8:
        return {
            "valid": False, "trend": None, "arrow": "?", "label": "資料不足",
            "recent_4q_eps": None, "previous_4q_eps": None,
            "change_pct": None, "warning": "歷史季度 EPS 不足 8 季",
        }
    recent4 = quarters[-4:]
    prev4   = quarters[-8:-4]
    try:
        recent_avg   = sum(float(q["eps"]) for q in recent4) / 4
        previous_avg = sum(float(q["eps"]) for q in prev4) / 4
    except (TypeError, ValueError):
        return {
            "valid": False, "trend": None, "arrow": "?", "label": "資料不足",
            "recent_4q_eps": None, "previous_4q_eps": None,
            "change_pct": None, "warning": "EPS 數值解析失敗",
        }
    if abs(previous_avg) < 0.01:
        return {
            "valid": False, "trend": None, "arrow": "?", "label": "前期 EPS 接近 0,趨勢無意義",
            "recent_4q_eps": round(recent_avg, 2),
            "previous_4q_eps": round(previous_avg, 2),
            "change_pct": None, "warning": "previous_4q_avg 太低",
        }
    change_pct = (recent_avg - previous_avg) / abs(previous_avg) * 100.0
    if change_pct > EPS_TREND_THRESHOLD * 100:
        trend, arrow, label = "growing",   "↗", "成長中"
    elif change_pct < -EPS_TREND_THRESHOLD * 100:
        trend, arrow, label = "declining", "↘", "衰退中"
    else:
        trend, arrow, label = "flat",      "→", "持平"
    return {
        "valid": True, "trend": trend, "arrow": arrow, "label": label,
        "recent_4q_eps":   round(recent_avg, 2),
        "previous_4q_eps": round(previous_avg, 2),
        "change_pct":      round(change_pct, 1),
        "warning":         None,
    }


# ── D 估值區間 ─────────────────────────────────────────────────────────────
def calc_valuation_band(stock_id: str, db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """近 5 年 PE 分位數法的估值區間（便宜 20% / 合理 50% / 昂貴 80%）。"""
    quarters = get_quarters_for_stock(stock_id, db_path)
    if not quarters:
        return {
            "valid": False, "warning": "歷史 EPS 資料不足",
            "data_quality": "missing", "ttm_eps_floor_warning": False,
        }

    lookback_days = LOOKBACK_YEARS * 252

    # 先取真實最新交易日 + 算當前 TTM，提前攔截 TTM EPS floor
    price_series = _fetch_price_series(stock_id, db_path, lookback_days)
    if not price_series:
        return {
            "valid": False, "warning": "無價格資料",
            "data_quality": "missing", "ttm_eps_floor_warning": False,
        }
    true_latest   = price_series[0]
    current_close = true_latest["close"]
    current_ttm   = _get_ttm_eps_as_of(quarters, true_latest["date"])

    if current_ttm is None:
        return {
            "valid": False, "warning": "最新交易日無對應 TTM EPS（4 季未集滿）",
            "data_quality": "missing", "ttm_eps_floor_warning": False,
        }
    if current_ttm < TTM_EPS_FLOOR:
        return {
            "valid": False,
            "warning": f"TTM EPS {current_ttm:.1f} 元 < {TTM_EPS_FLOOR} 元，無法計算估值",
            "data_quality": "missing", "ttm_eps_floor_warning": True,
            "ttm_eps": round(current_ttm, 2),
        }

    # 接下來才算歷史 PE 序列
    pe_series = _build_pe_series(stock_id, db_path, lookback_days)
    if not pe_series:
        return {
            "valid": False, "warning": "無法產生歷史 PE 序列",
            "data_quality": "missing", "ttm_eps_floor_warning": False,
        }

    sample_ratio = len(pe_series) / lookback_days
    if sample_ratio < MIN_VALID_RATIO:
        return {
            "valid": False,
            "warning": f"有效樣本不足({sample_ratio * 100:.0f}%，低於 {MIN_VALID_RATIO * 100:.0f}%)",
            "data_quality": "insufficient",
            "sample_size": len(pe_series), "sample_ratio": round(sample_ratio, 3),
            "ttm_eps_floor_warning": False,
        }

    current_pe = current_close / current_ttm

    pes = sorted(p["pe"] for p in pe_series)
    p20 = _percentile(pes, 0.20)
    p50 = _percentile(pes, 0.50)
    p80 = _percentile(pes, 0.80)

    cheap_price     = p20 * current_ttm
    fair_price      = p50 * current_ttm
    expensive_price = p80 * current_ttm

    n = len(pes)
    rank = sum(1 for p in pes if p <= current_pe) / n * 100.0

    if current_pe <= p20:
        position_label = "便宜"
    elif current_pe <= p50:
        position_label = "合理"
    elif current_pe <= p80:
        position_label = "略貴"
    else:
        position_label = "昂貴"

    return {
        "valid":                      True,
        "ttm_eps":                    round(current_ttm, 2),
        "ttm_eps_floor_warning":      False,
        "current_close":              round(current_close, 2),
        "pe_p20":                     round(p20, 2),
        "pe_p50":                     round(p50, 2),
        "pe_p80":                     round(p80, 2),
        "cheap_price":                round(cheap_price),
        "fair_price":                 round(fair_price),
        "expensive_price":            round(expensive_price),
        "current_pe":                 round(current_pe, 2),
        "current_percentile":         round(rank, 1),
        "position_label":             position_label,
        "distance_to_cheap_pct":      round((cheap_price - current_close)     / current_close * 100, 1),
        "distance_to_fair_pct":       round((fair_price - current_close)      / current_close * 100, 1),
        "distance_to_expensive_pct":  round((expensive_price - current_close) / current_close * 100, 1),
        "sample_size":                n,
        "sample_ratio":               round(sample_ratio, 3),
        "data_quality":               "good",
        "warning":                    None,
    }


# ── B 市場共識合理價 ───────────────────────────────────────────────────────
def calc_consensus_fair_price(stock_id: str, db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """近 1 年（SHORT_LOOKBACK_DAYS）平均 PE × 當前 TTM EPS。"""
    pe_series = _build_pe_series(stock_id, db_path, SHORT_LOOKBACK_DAYS)
    if not pe_series:
        return {"valid": False, "warning": "1 年 PE 序列不足"}

    sample_ratio = len(pe_series) / SHORT_LOOKBACK_DAYS
    if sample_ratio < MIN_VALID_RATIO:
        return {
            "valid": False,
            "warning": f"1 年 PE 樣本不足({sample_ratio * 100:.0f}%)",
        }

    latest = pe_series[0]
    current_close = latest["close"]
    current_ttm   = latest["ttm_eps"]
    avg_pe_1y     = sum(p["pe"] for p in pe_series) / len(pe_series)
    consensus_price = avg_pe_1y * current_ttm
    distance_pct  = (consensus_price - current_close) / current_close * 100

    if abs(distance_pct) < 5:
        position_label = "現價接近共識合理價"
    elif distance_pct > 0:
        position_label = f"低於共識合理價 {abs(distance_pct):.1f}%"
    else:
        position_label = f"高於共識合理價 {abs(distance_pct):.1f}%"

    return {
        "valid":           True,
        "current_close":   round(current_close, 2),
        "avg_pe_1y":       round(avg_pe_1y, 2),
        "consensus_price": round(consensus_price),
        "distance_pct":    round(distance_pct, 1),
        "position_label":  position_label,
        "sample_size":     len(pe_series),
        "sample_ratio":    round(sample_ratio, 3),
    }


# ── C 技術延伸區 ───────────────────────────────────────────────────────────
def calc_technical_zone(stock_id: str, db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """
    60 日壓力支撐位置描述。
    zone_label 只能是「位置描述」,絕對禁止暗示走勢或買賣建議。
    """
    price_series = _fetch_price_series(stock_id, db_path, TECHNICAL_LOOKBACK_DAYS)
    if len(price_series) < TECHNICAL_LOOKBACK_DAYS * MIN_VALID_RATIO:
        return {"valid": False, "warning": "60 日 K 線資料不足"}

    closes  = [p["close"] for p in price_series]
    current = closes[0]
    high60  = max(closes)
    low60   = min(closes)
    ma60    = sum(closes) / len(closes)

    dh = (high60 - current) / current * 100 if current else 0.0
    dl = (low60  - current) / current * 100 if current else 0.0
    dm = (ma60   - current) / current * 100 if current else 0.0

    # 位置標籤（嚴格非預言）
    if current >= high60 * 0.98:
        zone_label = "突破上方壓力區"
    elif current >= high60 * 0.9:
        zone_label = "接近上方壓力"
    elif current <= low60 * 1.02:
        zone_label = "跌破下方支撐區"
    elif current <= low60 * 1.1:
        zone_label = "接近下方支撐"
    else:
        zone_label = "區間整理中"

    return {
        "valid":                True,
        "current_close":        round(current, 2),
        "recent_high_60d":      round(high60),
        "recent_low_60d":       round(low60),
        "ma60":                 round(ma60),
        "distance_to_high_pct": round(dh, 1),
        "distance_to_low_pct":  round(dl, 1),
        "distance_to_ma60_pct": round(dm, 1),
        "zone_label":           zone_label,
        "sample_size":          len(closes),
    }


# ── 綜合判讀 ───────────────────────────────────────────────────────────────
def get_overall_signal(d_position: Optional[str], eps_trend: Optional[str]) -> str:
    """
    D 位置 × EPS 趨勢 的綜合判讀文字。

    特別保護「價值陷阱」: D=便宜 + EPS=declining → 警示
    """
    if d_position is None or d_position == "無資料":
        return "❓ 估值資料不足，無法判讀"

    if d_position == "便宜":
        if eps_trend == "growing":
            return "🟢 估值便宜 + 獲利成長，可分批佈局"
        if eps_trend == "declining":
            return "🟡 估值便宜但獲利衰退，當心價值陷阱"
        return "🟢 估值便宜，觀察獲利動能"

    if d_position == "合理":
        if eps_trend == "growing":
            return "🟢 估值合理 + 獲利成長，正常持有"
        if eps_trend == "declining":
            return "🟡 估值合理但獲利衰退，建議觀望"
        return "⚪ 估值合理，看資金流訊號決定"

    if d_position == "略貴":
        if eps_trend == "growing":
            return "🟡 估值略貴但獲利成長中，觀察動能"
        if eps_trend == "declining":
            return "🔴 估值略貴且獲利衰退，注意風險"
        return "🟡 估值偏高，注意風險"

    if d_position == "昂貴":
        if eps_trend == "growing":
            return "🟡 估值昂貴但獲利強勁，當心情緒拉抬"
        if eps_trend == "declining":
            return "🔴 估值昂貴且獲利衰退，不建議"
        return "🔴 估值偏貴，當心追高"

    return "❓ 無法判讀"


# ── 統合警示 ───────────────────────────────────────────────────────────────
def _compile_warnings(band: Dict[str, Any], eps_trend: Dict[str, Any]) -> List[str]:
    """彙整一檔的「該被看見」警示。AI 重估警示一律加。"""
    out: List[str] = ["AI 題材重估風險"]
    if band.get("ttm_eps_floor_warning"):
        out.append("TTM EPS 過低,估值無意義")
    if eps_trend.get("trend") == "declining":
        out.append("EPS 衰退中")
    if band.get("data_quality") == "insufficient":
        out.append("歷史樣本不足")
    if band.get("valid") and band.get("position_label") == "便宜" and eps_trend.get("trend") == "declining":
        out.append("⚠️ 價值陷阱訊號")
    return out


# ── 載入族群成員 ───────────────────────────────────────────────────────────
def _load_target_stocks() -> List[Dict[str, str]]:
    """從 rotation_groups.json 載入所有族群成員。"""
    with open(GROUPS_JSON, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    seen: Dict[str, Dict[str, str]] = {}
    for g in cfg.get("groups", []):
        for m in g.get("members", []):
            code = str(m.get("code", "")).strip()
            if not code or code in seen:
                continue
            seen[code] = {"code": code, "tier": m.get("tier", "")}
    return list(seen.values())


def _fetch_stock_name(stock_id: str, db_path: str) -> str:
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT name FROM stock_names WHERE stock_id = ?",
            (str(stock_id),)
        ).fetchone()
        conn.close()
        return row[0] if row and row[0] else ""
    except Exception:
        return ""


# ── 主要產出 ───────────────────────────────────────────────────────────────
def build_target_prices(
    db_path:     str = DEFAULT_DB,
    output_path: str = DEFAULT_OUT,
) -> Dict[str, Any]:
    """為所有族群成員計算三層估值,輸出 data/target_prices.json。"""
    ensure_eps_history_table(db_path)

    result: Dict[str, Any] = {
        "version":      VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of_date":   None,
        "disclaimer":   DISCLAIMER,
        "config": {
            "pe_filter_min":    PE_FILTER_MIN,
            "pe_filter_max":    PE_FILTER_MAX,
            "ttm_eps_floor":    TTM_EPS_FLOOR,
            "min_valid_ratio":  MIN_VALID_RATIO,
            "lookback_years":   LOOKBACK_YEARS,
        },
        "stocks":          {},
        "missing_stocks":  [],
    }

    targets = _load_target_stocks()
    latest_dates: List[str] = []

    for t in targets:
        code = t["code"]
        try:
            band      = calc_valuation_band(code, db_path)
            consensus = calc_consensus_fair_price(code, db_path)
            tech      = calc_technical_zone(code, db_path)
            trend     = calc_eps_trend(code, db_path)
            name      = _fetch_stock_name(code, db_path)

            d_pos = band["position_label"] if band.get("valid") else "無資料"
            t_trend = trend["trend"] if trend.get("valid") else None
            overall = get_overall_signal(d_pos, t_trend)
            warnings = _compile_warnings(band, trend)

            # ttm_eps 來自 band 若 valid,否則 trend 推估
            ttm_eps_val = band.get("ttm_eps") if band.get("valid") else trend.get("recent_4q_eps")

            current_close = (
                band.get("current_close")
                or consensus.get("current_close")
                or tech.get("current_close")
            )

            result["stocks"][code] = {
                "name":            name,
                "current_close":   round(current_close) if current_close else None,
                "ttm_eps":         ttm_eps_val,
                "eps_trend":       trend,
                "valuation_band":  band,
                "consensus_fair":  consensus,
                "technical_zone":  tech,
                "summary": {
                    "d_position":       d_pos,
                    "overall_signal":   overall,
                },
                "warnings":        warnings,
            }

            if band.get("valid") and current_close:
                # 找最新日期 (從 price_history 拿最近一筆)
                try:
                    conn = sqlite3.connect(db_path)
                    row = conn.execute(
                        "SELECT MAX(date) FROM price_history WHERE stock_id = ? AND close IS NOT NULL",
                        (code,)
                    ).fetchone()
                    conn.close()
                    if row and row[0]:
                        latest_dates.append(row[0])
                except Exception:
                    pass

        except Exception as e:
            result["missing_stocks"].append({"code": code, "reason": str(e)[:120]})
            print(f"[WARN] target_price failed for {code}: {e}")

    if latest_dates:
        result["as_of_date"] = max(latest_dates)

    # 輸出
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"[WARN] 寫入 target_prices.json 失敗: {e}")

    return result


if __name__ == "__main__":
    r = build_target_prices()
    print(f"\n=== target_prices.json ===")
    print(f"version: {r['version']}")
    print(f"as_of_date: {r['as_of_date']}")
    print(f"total stocks: {len(r['stocks'])}")
    valid_count = sum(1 for s in r["stocks"].values() if s["valuation_band"].get("valid"))
    print(f"valuation_band valid: {valid_count}")
    print(f"missing: {len(r['missing_stocks'])}")
    print(f"disclaimer: {r['disclaimer']}")
