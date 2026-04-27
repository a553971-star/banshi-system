import pandas as pd
import datetime
import logging
from typing import Optional

from feature_engine import build_features
from trajectory_engine import compute_trajectory, get_latest_trajectory
from flow_engine import classify_flow, classify_cost
from good_company import is_good_company
from decision_inspector import check_data_integrity, format_panstone_signal
from exporter import format_decision_snapshot, format_data_snapshot
from live_fetcher import merge_all_live
from institutional_engine import calc_foreign_cost_pro, classify_institutional_state, interpret_institutional_state

def classify_B_strength(result, foreign_profit):
    B    = result.get("B_days") or 0
    flow = result.get("flow_status")
    cost = result.get("cost_level")
    if B < 3:
        return "NORMAL_B"
    score = 0
    if B >= 5: score += 2
    if B >= 8: score += 2
    if flow == "ACCUMULATING":      score += 3
    elif flow in ["NEUTRAL", None]: score += 1
    if cost == "SAFE":              score += 2
    if foreign_profit is not None and foreign_profit < 8: score += 1
    if score >= 5: return "STRONG_B"
    if flow not in ["ACCUMULATING"] or (foreign_profit is not None and foreign_profit > 10):
        return "WEAK_B"
    return "NORMAL_B"

def interpret_B_strength(b_type):
    if b_type == "STRONG_B": return "主力建倉中（高品質B）"
    if b_type == "WEAK_B":   return "假建倉（沒有資金支撐）"
    return "普通整理（觀察）"

logger = logging.getLogger(__name__)
_LOOKBACK_DAYS = 400


def get_company_name_safe(stock_id: str, co_path: str = "companies.csv") -> str:
    try:
        df = pd.read_csv(co_path, dtype=str)
        match = df[df["stock_id"] == stock_id]
        if not match.empty:
            return str(match.iloc[0].get("name", stock_id))
    except Exception:
        pass
    return stock_id


def process_stock_live(
    stock_id: str,
    params: dict,
    date: str = None,
    print_snapshot: bool = False,
) -> Optional[dict]:
    """
    全市場即時版決策流程。
    跳過 SQLite，直接從 FinMind 撈資料跑完整盤石分析。
    """
    try:
        if date is None:
            date = datetime.date.today().strftime("%Y-%m-%d")

        co_path  = params.get("companies_path", "companies.csv")
        target   = pd.to_datetime(date)
        start_dt = (target - pd.Timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        name     = get_company_name_safe(stock_id, co_path)

        # ── 即時資料 ──
        df_raw = merge_all_live(stock_id, start_dt, date)
        if df_raw.empty:
            logger.warning("%s: 查無資料", stock_id)
            return None

        df_feat = build_features(df_raw)
        df_traj = compute_trajectory(df_feat, params)

        if print_snapshot:
            print(format_data_snapshot(stock_id, name, df_feat))

        traj_info = get_latest_trajectory(df_traj)
        last      = df_traj.iloc[-1]

        def _g(col):
            v = last.get(col) if hasattr(last, "get") else (
                last[col] if col in df_traj.columns else None
            )
            return None if (v is not None and v != v) else v

        flow_status = classify_flow(
            _g("foreign_consecutive_buy"),
            _g("margin_change_5d"),
            _g("return_10d"),
            _g("volume_ratio"),
            params,
        )

        # ── 主力成本修正 Flow ──────────────────────────────────────────
        try:
            _f_cost_tmp, _, _f_profit_tmp = calc_foreign_cost_pro(df_feat)
            if _f_profit_tmp is not None and flow_status is not None:
                if _f_profit_tmp > 12:
                    flow_status = "DISTRIBUTION"   # 出貨風險蓋過其他
                elif _f_profit_tmp < 4 and flow_status == "NEUTRAL":
                    flow_status = "ACCUMULATING"   # 成本低 + 中性 → 升級
        except Exception:
            pass
        cost_result = classify_cost(_g("bias_ma20"), params)

        integrity_ok = check_data_integrity(df_feat)
        good_co      = True  # 全市場查詢不限好公司

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

        decision = format_panstone_signal(
            trajectory=trajectory,
            flow=flow_dict,
            cost=cost_result,
            params=params,
            current_date=date,
            current_price=_g("close"),
        )

        for col in ("adx", "atr", "vwap", "kd_k", "kd_d", "bb_upper", "bb_middle", "bb_lower"):
            decision[col] = _g(col)

        try:
            f_cost, f_pos, f_profit = calc_foreign_cost_pro(df_feat)
            inst_state = classify_institutional_state(decision, f_cost, f_profit)
            inst_text  = interpret_institutional_state(inst_state, f_profit)
            decision["foreign_cost"]        = f_cost
            decision["foreign_position"]    = f_pos
            decision["foreign_profit_pct"]  = f_profit
            decision["institutional_state"] = inst_state
            decision["institutional_text"]  = inst_text
            b_type = classify_B_strength(decision, f_profit)
            decision["B_type"] = b_type
            decision["B_text"] = interpret_B_strength(b_type)
        except Exception as e:
            logger.warning("institutional analysis failed: %s", e)

        if print_snapshot:
            print(format_decision_snapshot(decision))

        return decision

    except Exception as exc:
        logger.error("process_stock_live(%s) failed: %s", stock_id, exc)
        return None
