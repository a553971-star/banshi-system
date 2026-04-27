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
        cost_result = classify_cost(_g("bias_ma20"), params)

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
        except Exception as e:
            logger.warning("institutional analysis failed: %s", e)

        if print_snapshot:
            print(format_decision_snapshot(decision))

        return decision

    except Exception as exc:
        logger.error("process_stock_live(%s) failed: %s", stock_id, exc)
        return None
