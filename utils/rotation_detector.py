"""
utils/rotation_detector.py — AI 族群輪動偵測模組 v3.1

核心：雙分數系統 + 退潮偵測 + 生命週期 + 五大防呆

防呆機制：
1. 族群權重（解決成員重疊）
2. 成交值加權（解決小型股拉爆）
3. 確認突破（防主力釣魚）
4. 大盤風險濾網（Phase 1 為 neutral 空殼）
5. 資料一致性檢查（三表日期不同步拒絕發訊號）

設計原則：重視「相對變化」，不追求絕對值精準。
所有 score 在 return 前必須過 clamp()。

Version: v3.1
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ── 模組常數 ───────────────────────────────────────────────────────────────
VERSION = "v3.1"

ALGORITHM_CONFIG: Dict[str, Any] = {
    "heat_score_weights": {
        "weighted_5d_return":         0.30,
        "turnover_share":             0.25,
        "leader_confirmed_breakout":  0.20,
        "weighted_volume_expansion":  0.15,
        "foreign_buy_concentration":  0.10,
    },
    "early_score_weights": {
        "foreign_continuous_buy":          0.25,
        "volume_up_price_flat":            0.30,
        "cba_b_ratio":                     0.25,
        "leader_first_confirmed_breakout": 0.20,
    },
    "stage_thresholds": {"cold": 30, "warming": 55, "hot": 80},
    "lifecycle_thresholds": {
        "early":     {"lag_diff_min": 5.0},
        "mid_early": {"lag_diff_min": 2.0, "lag_diff_max": 5.0},
        "mid_late":  {"lag_diff_min": 0.0, "lag_diff_max": 2.0},
        "late":      {"lag_diff_max": 0.0},
    },
    "confirmed_breakout": {
        "lookback_days": 20,
        "hold_days": 3,
        "hold_floor_ratio": 0.95,
        "volume_multiplier": 4.0,
    },
}

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB  = os.path.join(BASE_DIR, "banshi.db")
GROUPS_JSON = os.path.join(BASE_DIR, "config", "rotation_groups.json")
DECISIONS_CSV = os.path.join(BASE_DIR, "latest_decisions.csv")


# ── 工具函式 ───────────────────────────────────────────────────────────────
def clamp(value: Optional[float], lo: float = 0.0, hi: float = 100.0) -> float:
    """限制分數在合理範圍。處理 None / NaN / inf。"""
    if value is None:
        return lo
    try:
        v = float(value)
    except (TypeError, ValueError):
        return lo
    if math.isnan(v) or math.isinf(v):
        return lo
    return max(lo, min(hi, v))


def clamp_ratio(value: Optional[float]) -> float:
    """0~1 比例限制。"""
    return clamp(value, 0.0, 1.0)


def estimate_turnover(close: Optional[float], volume: Optional[float]) -> float:
    """估算成交值（元）= close × volume × 1000（張轉股）。"""
    try:
        c = float(close or 0)
        v = float(volume or 0)
    except (TypeError, ValueError):
        return 0.0
    return c * v * 1000.0


# ── 設定載入 ───────────────────────────────────────────────────────────────
def load_groups() -> List[Dict[str, Any]]:
    with open(GROUPS_JSON, "r", encoding="utf-8") as f:
        return json.load(f)["groups"]


# ── 資料一致性檢查 ─────────────────────────────────────────────────────────
def check_data_integrity(db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """檢查 price_history / institutional_history / margin_history 最新日期是否同步。"""
    tables = ["price_history", "institutional_history", "margin_history"]
    out: Dict[str, Optional[str]] = {}
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        for t in tables:
            try:
                cur.execute(f"SELECT MAX(date) FROM {t}")
                out[t] = cur.fetchone()[0]
            except Exception:
                out[t] = None
        conn.close()
    except Exception as e:
        return {
            "all_synced": False,
            "latest_date": None,
            "tables": {t: None for t in tables},
            "warning": f"DB 連線失敗：{e}",
        }

    dates = [d for d in out.values() if d]
    all_synced = len(dates) == len(tables) and len(set(dates)) == 1
    warning = ""
    if not all_synced:
        warning = f"三表日期不同步：{out}。系統將不發出輪動訊號。"

    return {
        "all_synced": all_synced,
        "latest_date": max(dates) if dates else None,
        "tables": out,
        "warning": warning,
    }


# ── 大盤風險濾網（Phase 1 空殼） ────────────────────────────────────────────
def calc_market_regime(db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """
    Phase 1：強制 neutral，signal_multiplier=0.7（保守 fail-safe）。
    Phase 2：接 TAIEX 資料後升級為完整版（月線/季線/ATR/外資累計/5日報酬）。
    """
    return {
        "market_score": None,
        "regime": "neutral",
        "components": {
            "note": "Phase 1: 空殼版，等待 TAIEX 資料補齊後升級為完整 regime",
        },
        "signal_multiplier": 0.7,
        "phase": "placeholder",
    }


# ── 個股價量資料載入 ───────────────────────────────────────────────────────
def _fetch_price_window(conn: sqlite3.Connection, stock_id: str, days: int = 30) -> List[Dict[str, Any]]:
    """取最近 days 個交易日的價量。回傳由舊到新。"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, open, high, low, close, volume
          FROM price_history
         WHERE stock_id = ?
         ORDER BY date DESC LIMIT ?
        """,
        (str(stock_id), days),
    )
    rows = cur.fetchall()
    rows.reverse()
    return [
        {
            "date":   r[0],
            "open":   r[1],
            "high":   r[2],
            "low":    r[3],
            "close":  r[4],
            "volume": r[5],
            "turnover": estimate_turnover(r[4], r[5]),
        }
        for r in rows
    ]


def _fetch_institutional_window(conn: sqlite3.Connection, stock_id: str, days: int = 10) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, foreign_net, investment_net, dealer_net
          FROM institutional_history
         WHERE stock_id = ?
         ORDER BY date DESC LIMIT ?
        """,
        (str(stock_id), days),
    )
    rows = cur.fetchall()
    rows.reverse()
    return [
        {"date": r[0], "foreign_net": r[1] or 0, "investment_net": r[2] or 0, "dealer_net": r[3] or 0}
        for r in rows
    ]


# ── 確認突破 ──────────────────────────────────────────────────────────────
def is_confirmed_breakout(stock_id: str, db_path: str = DEFAULT_DB) -> Optional[bool]:
    """
    True  = 已確認突破
    False = 還沒突破
    None  = 剛突破但還沒站穩 3 日（待確認）
    """
    cfg = ALGORITHM_CONFIG["confirmed_breakout"]
    lookback = cfg["lookback_days"]
    hold_n   = cfg["hold_days"]
    floor    = cfg["hold_floor_ratio"]
    vol_mul  = cfg["volume_multiplier"]

    needed = lookback + hold_n + 5  # 突破日 + 站穩日 + 突破前均量基期
    try:
        conn = sqlite3.connect(db_path)
        bars = _fetch_price_window(conn, stock_id, days=needed + 5)
        conn.close()
    except Exception:
        return False

    if len(bars) < lookback + hold_n + 1:
        return False

    # 找最近 hold_n+1 日內是否有突破日
    # 由新到舊掃，找第一個突破日
    for offset in range(hold_n, -1, -1):
        # offset = 距今幾日（0=今日，hold_n=hold_n日前）
        # breakout_idx 為突破日在 bars 中的位置
        idx = len(bars) - 1 - offset
        if idx - lookback < 0:
            continue
        prior_max = max(b["close"] for b in bars[idx - lookback:idx])
        if bars[idx]["close"] > prior_max:
            # 找到突破日，檢查後續站穩條件
            after = bars[idx + 1: idx + 1 + hold_n]
            if len(after) < hold_n:
                return None  # 還沒走完站穩期
            breakout_close = bars[idx]["close"]
            if not all(b["close"] > breakout_close * floor for b in after):
                return False
            # 量能條件
            prior5_vol = [b["volume"] or 0 for b in bars[max(0, idx - 5):idx]]
            avg5_vol   = sum(prior5_vol) / max(1, len(prior5_vol))
            after_vol  = sum(b["volume"] or 0 for b in after)
            if avg5_vol > 0 and after_vol > avg5_vol * vol_mul:
                return True
            return False
    return False


def _is_first_confirmed_breakout(stock_id: str, db_path: str = DEFAULT_DB) -> bool:
    """
    最近 10 日內發生確認突破，視為「首次確認」（用於 early_rotation_score）。
    """
    return is_confirmed_breakout(stock_id, db_path) is True


# ── CBA 讀取（single source of truth：latest_decisions.csv） ──────────────
_CBA_CACHE: Optional[Dict[str, Dict[str, Any]]] = None


def _load_cba_lookup() -> Dict[str, Dict[str, Any]]:
    """讀 latest_decisions.csv，依 stock_id 索引。"""
    global _CBA_CACHE
    if _CBA_CACHE is not None:
        return _CBA_CACHE

    lookup: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(DECISIONS_CSV):
        _CBA_CACHE = lookup
        return lookup
    try:
        import csv
        with open(DECISIONS_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sid = str(row.get("stock_id", "")).strip()
                if not sid:
                    continue
                lookup[sid] = row
    except Exception:
        pass
    _CBA_CACHE = lookup
    return lookup


def _row_b_quality(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("B_quality") or 0)
    except (TypeError, ValueError):
        return 0.0


def _row_foreign_consec(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("foreign_consecutive_buy") or 0)
    except (TypeError, ValueError):
        return 0.0


def _row_volume_ratio(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("volume_ratio") or 0)
    except (TypeError, ValueError):
        return 0.0


def _row_return_10d(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("return_10d") or 0)
    except (TypeError, ValueError):
        return 0.0


def _row_flow_status(row: Dict[str, Any]) -> str:
    return str(row.get("flow_status") or "")


# ── 族群指標（成交值加權） ─────────────────────────────────────────────────
def _stock_5d_return(bars: List[Dict[str, Any]]) -> Optional[float]:
    if len(bars) < 6:
        return None
    try:
        return (bars[-1]["close"] - bars[-6]["close"]) / bars[-6]["close"] * 100.0
    except Exception:
        return None


def _stock_volume_expansion(bars: List[Dict[str, Any]]) -> Optional[float]:
    """近 5 日均量 / 前 20 日均量 - 1（百分比）"""
    if len(bars) < 25:
        return None
    recent = [b["volume"] or 0 for b in bars[-5:]]
    base   = [b["volume"] or 0 for b in bars[-25:-5]]
    avg_r  = sum(recent) / max(1, len(recent))
    avg_b  = sum(base)   / max(1, len(base))
    if avg_b <= 0:
        return None
    return (avg_r / avg_b - 1.0) * 100.0


def calc_group_metrics(group: Dict[str, Any], db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """成交值加權後的族群指標。"""
    cba = _load_cba_lookup()
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        return {"error": str(e)}

    members_data: List[Dict[str, Any]] = []
    missing: List[str] = []

    for m in group["members"]:
        sid    = str(m["code"])
        weight = float(m.get("weight") or 1.0)
        tier   = m.get("tier", "second")
        bars   = _fetch_price_window(conn, sid, days=30)
        inst   = _fetch_institutional_window(conn, sid, days=10)
        row    = cba.get(sid, {})

        if not bars:
            missing.append(sid)
            continue

        latest_turnover = bars[-1]["turnover"]
        members_data.append({
            "code":       sid,
            "tier":       tier,
            "weight":     weight,
            "bars":       bars,
            "inst":       inst,
            "row":        row,
            "turnover":   latest_turnover,
            "ret_5d":     _stock_5d_return(bars),
            "vol_expand": _stock_volume_expansion(bars),
            "is_brk":     is_confirmed_breakout(sid, db_path),
        })

    conn.close()

    if not members_data:
        return {
            "n_members":               0,
            "weighted_5d_return":      0.0,
            "weighted_volume_expansion": 0.0,
            "total_turnover":          0.0,
            "leaders":                 [],
            "second":                  [],
            "elastic":                 [],
            "missing":                 missing,
            "leader_confirmed_breakout_ratio": 0.0,
            "foreign_continuous_buy_score":    0.0,
            "volume_up_price_flat_score":      0.0,
            "cba_b_ratio":                     0.0,
        }

    # 成交值加權報酬 / 量能擴張
    def wavg(field: str) -> float:
        num = 0.0
        den = 0.0
        for d in members_data:
            v = d.get(field)
            if v is None:
                continue
            w = d["turnover"] * d["weight"]
            num += v * w
            den += w
        return (num / den) if den > 0 else 0.0

    total_turnover = sum(d["turnover"] * d["weight"] for d in members_data)
    w_5d_return    = wavg("ret_5d")
    w_vol_expand   = wavg("vol_expand")

    leaders = [d for d in members_data if d["tier"] == "leader"]
    second  = [d for d in members_data if d["tier"] == "second"]
    elastic = [d for d in members_data if d["tier"] == "elastic"]

    # leader 確認突破比例
    brk_leaders = [d for d in leaders if d["is_brk"] is True]
    leader_brk_ratio = (len(brk_leaders) / len(leaders)) if leaders else 0.0

    # 外資連買（取近 5 日 foreign_net 為正的成員比例，CBA 也有 foreign_consecutive_buy）
    foreign_buyers = 0
    for d in members_data:
        consec = _row_foreign_consec(d["row"])
        if consec >= 3:
            foreign_buyers += 1
    foreign_continuous_ratio = foreign_buyers / max(1, len(members_data))

    # 量增價未漲（volume_ratio >= 1.3 且 return_10d 在 ±3% 內）
    vup_flat = 0
    for d in members_data:
        vr = _row_volume_ratio(d["row"])
        r10 = _row_return_10d(d["row"])
        if vr >= 1.3 and abs(r10) <= 3.0:
            vup_flat += 1
    vup_flat_ratio = vup_flat / max(1, len(members_data))

    # CBA B 階段比例（B_quality >= 50 且 flow_status != DISTRIBUTION）
    b_count = 0
    for d in members_data:
        bq = _row_b_quality(d["row"])
        fs = _row_flow_status(d["row"])
        if bq >= 50 and fs != "DISTRIBUTION":
            b_count += 1
    cba_b_ratio = b_count / max(1, len(members_data))

    # 外資集中度（族群內外資淨買成交值佔比，近 5 日累計）
    f_net_sum = 0.0
    for d in members_data:
        for ri in d["inst"][-5:]:
            f_net_sum += float(ri.get("foreign_net") or 0)
    # 用張數 → 約略佔成交值（這裡只看正負與相對強度，clamp 後使用）
    foreign_buy_conc = 0.5 + min(0.5, max(-0.5, (f_net_sum / 1e6) / 50.0))

    return {
        "n_members":              len(members_data),
        "weighted_5d_return":     w_5d_return,
        "weighted_volume_expansion": w_vol_expand,
        "total_turnover":         total_turnover,
        "leaders":                leaders,
        "second":                 second,
        "elastic":                elastic,
        "members":                members_data,
        "missing":                missing,
        "leader_confirmed_breakout_ratio": clamp_ratio(leader_brk_ratio),
        "foreign_continuous_buy_score":    clamp_ratio(foreign_continuous_ratio),
        "volume_up_price_flat_score":      clamp_ratio(vup_flat_ratio),
        "cba_b_ratio":                     clamp_ratio(cba_b_ratio),
        "foreign_buy_concentration":       clamp_ratio(foreign_buy_conc),
    }


# ── heat_score（偵測主流） ────────────────────────────────────────────────
def calc_heat_score(group_metrics: Dict[str, Any], turnover_share: float) -> Dict[str, Any]:
    w = ALGORITHM_CONFIG["heat_score_weights"]

    # 將原始指標轉成 0~100
    ret  = group_metrics.get("weighted_5d_return", 0.0)
    ret_norm = clamp(50 + ret * 5)              # 0% → 50, +10% → 100, -10% → 0
    share_norm = clamp(turnover_share * 100 * 5)  # 5% share → 25 分，20%+ → 100
    brk_norm   = clamp(group_metrics.get("leader_confirmed_breakout_ratio", 0.0) * 100)
    vol_norm   = clamp(50 + group_metrics.get("weighted_volume_expansion", 0.0))
    fbc_norm   = clamp(group_metrics.get("foreign_buy_concentration", 0.5) * 100)

    score = (
        ret_norm   * w["weighted_5d_return"]      +
        share_norm * w["turnover_share"]          +
        brk_norm   * w["leader_confirmed_breakout"] +
        vol_norm   * w["weighted_volume_expansion"] +
        fbc_norm   * w["foreign_buy_concentration"]
    )
    score = clamp(score)

    th = ALGORITHM_CONFIG["stage_thresholds"]
    if score < th["cold"]:
        stage = "cold"
    elif score < th["warming"]:
        stage = "warming"
    elif score < th["hot"]:
        stage = "hot"
    else:
        stage = "peaking"

    return {
        "score": score,
        "stage": stage,
        "components": {
            "weighted_5d_return":         ret_norm,
            "turnover_share":             share_norm,
            "leader_confirmed_breakout":  brk_norm,
            "weighted_volume_expansion":  vol_norm,
            "foreign_buy_concentration":  fbc_norm,
        },
    }


# ── early_rotation_score（偵測潛伏） ──────────────────────────────────────
def calc_early_rotation_score(group_metrics: Dict[str, Any]) -> Dict[str, Any]:
    w = ALGORITHM_CONFIG["early_score_weights"]

    fc = clamp(group_metrics.get("foreign_continuous_buy_score", 0.0) * 100)
    vp = clamp(group_metrics.get("volume_up_price_flat_score", 0.0) * 100)
    cb = clamp(group_metrics.get("cba_b_ratio", 0.0) * 100)
    lb = clamp(group_metrics.get("leader_confirmed_breakout_ratio", 0.0) * 100)

    score = (
        fc * w["foreign_continuous_buy"] +
        vp * w["volume_up_price_flat"]   +
        cb * w["cba_b_ratio"]            +
        lb * w["leader_first_confirmed_breakout"]
    )
    score = clamp(score)

    return {
        "score": score,
        "components": {
            "foreign_continuous_buy":          fc,
            "volume_up_price_flat":            vp,
            "cba_b_ratio":                     cb,
            "leader_first_confirmed_breakout": lb,
        },
    }


# ── leader_exhaustion（退潮） ─────────────────────────────────────────────
def calc_leader_exhaustion(group_metrics: Dict[str, Any], heat_score: float) -> Dict[str, Any]:
    triggers: List[str] = []
    leaders = group_metrics.get("leaders") or []

    # 1. heat_score > 75 且 5 日報酬遞減（用 leaders 平均 5 日 vs 10 日近似）
    if heat_score > 75:
        triggers.append("heat_overheat_75+")

    # 2. 爆量不漲：volume_ratio>=1.5 且 |10日報酬|<1% 的 leader 數
    vup_count = 0
    for d in leaders:
        if _row_volume_ratio(d["row"]) >= 1.5 and abs(_row_return_10d(d["row"])) < 1.0:
            vup_count += 1
    if vup_count >= 1:
        triggers.append("leader_volume_no_price")

    # 3. 外資由買轉賣（leader foreign_net 5 日累計轉負）
    for d in leaders:
        nets = [r.get("foreign_net", 0) for r in (d["inst"][-5:] or [])]
        if nets and sum(nets) < 0 and any(n > 0 for n in nets[:2]):
            triggers.append("foreign_flip_to_sell")
            break

    # 4. 高檔長黑：leader 最近一日實體 >3% 且收最低 1/4
    for d in leaders:
        if not d["bars"]:
            continue
        b = d["bars"][-1]
        try:
            o, h, l, c = float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"])
        except (TypeError, ValueError):
            continue
        body_pct = abs(c - o) / o * 100 if o else 0
        rng      = h - l
        if rng <= 0:
            continue
        if c < o and body_pct > 3.0 and (c - l) / rng < 0.25:
            triggers.append("leader_high_bearish")
            break

    n_triggers = len(triggers)
    if n_triggers >= 2:
        risk_level = "high"
        recommended = "減碼觀望"
    elif n_triggers == 1:
        risk_level = "medium"
        recommended = "提高警覺"
    else:
        risk_level = "low"
        recommended = "持有"

    # exhaustion_score: heat_score 加上 triggers 加分
    score = clamp(heat_score * 0.5 + n_triggers * 20)

    return {
        "score":         score,
        "triggers":      triggers,
        "triggers_fired": n_triggers,
        "risk_level":    risk_level,
        "recommended_action": recommended,
    }


# ── leader_lag_diff（生命週期） ───────────────────────────────────────────
def calc_leader_lag_diff(group_metrics: Dict[str, Any]) -> Dict[str, Any]:
    leaders = group_metrics.get("leaders") or []
    second  = group_metrics.get("second") or []
    elastic = group_metrics.get("elastic") or []

    def wret(members: List[Dict[str, Any]]) -> float:
        num, den = 0.0, 0.0
        for d in members:
            r = d.get("ret_5d")
            if r is None:
                continue
            w = d["turnover"] * d["weight"]
            num += r * w
            den += w
        return (num / den) if den > 0 else 0.0

    lead_ret    = wret(leaders)
    second_ret  = wret(second)
    elastic_ret = wret(elastic)

    lag_diff = lead_ret - second_ret

    # 判定生命週期階段
    if lag_diff > 5.0:
        stage = "early"
    elif lag_diff > 2.0:
        stage = "mid_early"
    elif lag_diff > 0.0:
        stage = "mid_late"
    else:
        if elastic_ret > 5.0:
            stage = "late"  # 末升段：龍頭沒動，小型在飆
        else:
            stage = "mid_late"

    return {
        "lag_diff":            lag_diff,
        "leaders_return_5d":   lead_ret,
        "second_return_5d":    second_ret,
        "elastic_return_5d":   elastic_ret,
        "lifecycle_stage":     stage,
    }


# ── 整合偵測 ───────────────────────────────────────────────────────────────
def detect_rotation(db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """主入口。回傳完整 rotation_status JSON。"""
    integrity = check_data_integrity(db_path)
    base = {
        "version":          VERSION,
        "algorithm_config": ALGORITHM_CONFIG,
        "generated_at":     datetime.now().isoformat(timespec="seconds"),
        "data_integrity":   integrity,
        "news_sentiment":   None,
        "as_of_date":       integrity.get("latest_date"),
    }

    if not integrity["all_synced"]:
        return {
            **base,
            "market_regime":  {"regime": "unknown", "signal_multiplier": 0.0},
            "current_leader": None,
            "next_candidates": [],
            "rotation_signal": {
                "probability": "n/a",
                "signals_count": 0,
                "signal_multiplier_applied": 0.0,
                "note": integrity["warning"],
            },
            "all_groups": [],
        }

    regime = calc_market_regime(db_path)
    mult   = regime.get("signal_multiplier", 0.7)

    groups_cfg = load_groups()

    # Step 1: 先算所有族群指標，匯總 total_turnover 算 share
    raw_list = []
    for g in groups_cfg:
        gm = calc_group_metrics(g, db_path)
        raw_list.append((g, gm))

    grand_turnover = sum((r[1].get("total_turnover") or 0.0) for r in raw_list) or 1.0

    # Step 2: 計算雙分數 + 退潮 + 生命週期
    all_groups: List[Dict[str, Any]] = []
    for g, gm in raw_list:
        share = (gm.get("total_turnover") or 0.0) / grand_turnover
        heat  = calc_heat_score(gm, share)
        early = calc_early_rotation_score(gm)
        exh   = calc_leader_exhaustion(gm, heat["score"])
        lifec = calc_leader_lag_diff(gm)

        # 套用 signal_multiplier
        heat_adj  = clamp(heat["score"]  * mult)
        early_adj = clamp(early["score"] * mult)

        all_groups.append({
            "group_id":             g["group_id"],
            "group_name":           g["group_name"],
            "upstream_level":       g["upstream_level"],
            "cycle_bias":           g["cycle_bias"],
            "theme_tags":           g["theme_tags"],
            "n_members":            gm.get("n_members", 0),
            "missing_members":      gm.get("missing", []),
            "turnover_share":       share,
            "weighted_5d_return":   gm.get("weighted_5d_return", 0.0),
            "heat_score":           heat_adj,
            "heat_score_raw":       heat["score"],
            "heat_stage":           heat["stage"],
            "heat_components":      heat["components"],
            "early_rotation_score": early_adj,
            "early_rotation_score_raw": early["score"],
            "early_components":     early["components"],
            "exhaustion":           exh,
            "lifecycle":            lifec,
        })

    # Step 3: 識別 current_leader / next_candidates
    by_heat  = sorted(all_groups, key=lambda x: x["heat_score"], reverse=True)
    by_early = sorted(all_groups, key=lambda x: x["early_rotation_score"], reverse=True)

    current_leader = by_heat[0] if by_heat else None

    # next_candidates: early_score 高，但不是 current_leader
    next_cands = []
    if current_leader:
        for grp in by_early:
            if grp["group_id"] == current_leader["group_id"]:
                continue
            next_cands.append(grp)
            if len(next_cands) >= 3:
                break

    # Step 4: 計算 rotation_signal probability
    signals_count = 0
    if current_leader and current_leader["exhaustion"]["risk_level"] in ("high", "medium"):
        signals_count += 1
    if next_cands and next_cands[0]["early_rotation_score"] > 40:
        signals_count += 1
    if next_cands and next_cands[0]["lifecycle"]["lifecycle_stage"] in ("early", "mid_early"):
        signals_count += 1

    if signals_count >= 3:
        probability = "high"
    elif signals_count == 2:
        probability = "medium"
    elif signals_count == 1:
        probability = "low"
    else:
        probability = "none"

    return {
        **base,
        "market_regime":   regime,
        "current_leader":  current_leader,
        "next_candidates": next_cands,
        "rotation_signal": {
            "probability":              probability,
            "signals_count":            signals_count,
            "signal_multiplier_applied": mult,
        },
        "all_groups": all_groups,
    }


if __name__ == "__main__":
    result = detect_rotation()
    # 移除無法序列化的 bars/inst/row 細節
    def _strip(grp):
        for k in ("members", "leaders", "second", "elastic"):
            grp.pop(k, None) if isinstance(grp, dict) else None
        return grp
    if isinstance(result.get("all_groups"), list):
        for g in result["all_groups"]:
            for k in ("members",):
                g.pop(k, None)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
