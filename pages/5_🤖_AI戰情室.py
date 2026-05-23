"""
pages/5_🤖_AI戰情室.py
AI 概念股完整戰情室，資料來源 latest_decisions_ai.csv。
共用 app.py 的 render_war_room_body。

v4.0 新增「💰 個股估值參考」區塊（在頁尾）。
"""
import datetime
import json
import os
import sys

import pandas as pd
import streamlit as st

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from app import (
    render_war_room_body,
    get_latest_state_changes,
    load_state_log,
    load_watchlist_overrides,
    render_live_result_block,
)
from pinned_store import load_pinned
from ui.sidebar import render_sidebar_query

st.set_page_config(page_title="AI戰情室", layout="wide")
st.title("🤖 AI 戰情室")
st.caption("AI 概念股 × 完整磐石分析")

# ── 資料載入 ──────────────────────────────────────────────────────────────────
AI_CSV    = os.path.join(BASE_PATH, "latest_decisions_ai.csv")
UNI_CSV   = os.path.join(BASE_PATH, "latest_decisions_universe.csv")
AI_SC_CSV = os.path.join(BASE_PATH, "ai_supply_chain.csv")

if os.path.exists(AI_CSV):
    df = pd.read_csv(AI_CSV, dtype=str)
elif os.path.exists(UNI_CSV):
    st.info("⏳ latest_decisions_ai.csv 尚未產生，暫時顯示 universe 資料中的 AI 股票...")
    try:
        _ai_ids = set(
            pd.read_csv(AI_SC_CSV, dtype=str).iloc[:, 0].astype(str).tolist()
        )
    except Exception:
        _ai_ids = set()
    df = pd.read_csv(UNI_CSV, dtype=str)
    df = df[df["stock_id"].isin(_ai_ids)]
else:
    st.warning("AI 決策資料尚未產生，請等 Actions 跑完")
    st.stop()

if df.empty:
    st.warning("目前無 AI 概念股資料")
    st.stop()

# ── State log ────────────────────────────────────────────────────────────────
state_log = load_state_log()
ai_stock_ids = set(df["stock_id"].astype(str).tolist())

if not state_log.empty and "stock_id" in state_log.columns:
    ai_state_log = state_log[state_log["stock_id"].astype(str).isin(ai_stock_ids)]
else:
    ai_state_log = pd.DataFrame()

state_changes = get_latest_state_changes(ai_state_log) if not ai_state_log.empty else {}

# ── prev_map（昨日對照，供情緒雷達）────────────────────────────────────────────
prev_map = {}
try:
    if not ai_state_log.empty:
        _sl = ai_state_log.copy()
        _sl["stock_id"] = _sl["stock_id"].astype(str)
        _sl["date"] = pd.to_datetime(_sl["date"], errors="coerce")
        _today = pd.Timestamp(datetime.date.today())
        _valid = _sl[_sl["date"] < _today]
        for _sid, _grp in _valid.groupby("stock_id"):
            _latest = _grp.sort_values("date").iloc[-1]
            prev_map[_sid] = {
                "C_days":      int(_latest.get("C_days", 0) or 0),
                "A_days":      int(_latest.get("A_days", 0) or 0),
                "flow_status": str(_latest.get("flow_status", "") or ""),
            }
except Exception:
    prev_map = {}

# ── Session state 初始化 ──────────────────────────────────────────────────────
if "overrides" not in st.session_state:
    st.session_state["overrides"] = load_watchlist_overrides()
if "pinned" not in st.session_state:
    st.session_state["pinned"] = load_pinned()

# ── Sidebar 即時查詢 ──────────────────────────────────────────────────────────
render_sidebar_query(key_suffix="_ai", render_result_fn=render_live_result_block)

st.divider()

# ── 渲染 ─────────────────────────────────────────────────────────────────────
st.caption(f"共 {len(df)} 支 AI 概念股")

render_war_room_body(
    df,
    prev_map,
    state_changes,
    key_prefix="ai_",
    quick_mode=False,
)

# ─────────────────────────────────────────────────────────────────────────────
# 💰 個股估值參考 (v4.0)
# 設計哲學：不做預言，只做位置描述。給「區間」與「位置」，不給單一預測價位。
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("💰 個股估值參考")
st.caption("⚠️ AI 題材可能造成估值體系重估，歷史 PE 區間僅供參考")


TARGET_PRICES_JSON = os.path.join(BASE_PATH, "data", "target_prices.json")


@st.cache_data(ttl=600)
def _load_target_prices() -> dict:
    if not os.path.exists(TARGET_PRICES_JSON):
        return {}
    try:
        with open(TARGET_PRICES_JSON, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _render_valuation_section(tp: dict):
    """渲染單檔三層估值區塊。tp 為 target_prices.json 中單一檔的 dict。"""
    # 統合警示
    if tp.get("warnings"):
        warning_text = "　|　".join([f"⚠️ {w}" for w in tp["warnings"]])
        st.caption(warning_text)

    eps_trend = tp.get("eps_trend") or {}
    if eps_trend.get("valid"):
        c1, c2 = st.columns([1, 3])
        c1.metric(
            "TTM EPS",
            f"{tp.get('ttm_eps', 0):.1f} 元" if tp.get("ttm_eps") is not None else "—",
            f"{eps_trend['arrow']} {eps_trend['label']}",
        )
        c2.caption(
            f"近 4 季平均 {eps_trend['recent_4q_eps']:.1f} 元　"
            f"vs 前 4 季 {eps_trend['previous_4q_eps']:.1f} 元　"
            f"({eps_trend['change_pct']:+.1f}%)"
        )

    band = tp.get("valuation_band") or {}
    if not band.get("valid"):
        st.warning(f"⚠️ 無法計算估值：{band.get('warning', '資料不足')}")
        st.caption(
            "💡 提示：此頁估值需要歷史季度 EPS 資料。等 GitHub Actions（週六）跑完 "
            "`utils.eps_history.fetch_eps_history` 後即會自動產生。"
        )
        return

    # D 估值區間（主畫面）
    st.markdown("#### 📊 估值區間（近 5 年 PE 分位數）")
    cc, cf, ce = st.columns(3)
    cc.metric(
        "🟢 便宜價（20%）",
        f"{band['cheap_price']:.0f} 元",
        f"{band['distance_to_cheap_pct']:+.1f}%",
    )
    cf.metric(
        "🟡 合理價（50%）",
        f"{band['fair_price']:.0f} 元",
        f"{band['distance_to_fair_pct']:+.1f}%",
    )
    ce.metric(
        "🔴 昂貴價（80%）",
        f"{band['expensive_price']:.0f} 元",
        f"{band['distance_to_expensive_pct']:+.1f}%",
    )

    st.info(
        f"📍 **目前位置**：{band['position_label']}　"
        f"（現價 {band['current_close']:.0f} 元 / "
        f"現 PE {band['current_pe']:.1f}x / "
        f"位於歷史 {band['current_percentile']:.0f}th 分位）"
    )

    summary = tp.get("summary") or {}
    if summary.get("overall_signal"):
        st.success(f"💡 **綜合判讀**：{summary['overall_signal']}")

    # AI 重估警示（主畫面底部，必出現）
    st.caption(
        "⚠️ **估值系統已知限制**：AI 題材可能造成估值體系重估，"
        "歷史 PE 區間僅供參考，不代表市場必然回歸均值。"
    )

    # B + C 進階區（expander 收納）
    with st.expander("🔍 進階估值參考（B 共識合理價 + C 技術延伸區）"):
        consensus = tp.get("consensus_fair") or {}
        st.markdown("##### 📐 市場共識合理價（近 1 年平均 PE）")
        if consensus.get("valid"):
            ca, cb = st.columns(2)
            ca.metric(
                "共識合理價",
                f"{consensus['consensus_price']:.0f} 元",
                f"{consensus['distance_pct']:+.1f}%",
            )
            cb.metric("近 1 年平均 PE", f"{consensus['avg_pe_1y']:.1f}x")
            st.caption(f"位置：{consensus['position_label']}")
        else:
            st.warning(consensus.get("warning", "無資料"))

        st.divider()

        tech = tp.get("technical_zone") or {}
        st.markdown("##### 📈 技術延伸區（60 日壓力支撐）")
        st.caption(
            "ℹ️ 技術延伸區僅為**情緒 / 動能參考**，**非買賣建議價位**。"
            "壓力 / 支撐位置僅描述當下技術位置，不暗示後續走勢。"
        )
        if tech.get("valid"):
            ch, cm, cl = st.columns(3)
            ch.metric(
                "上方壓力（60 日高）",
                f"{tech['recent_high_60d']:.0f} 元",
                f"{tech['distance_to_high_pct']:+.1f}%",
            )
            cm.metric(
                "季線（60MA）",
                f"{tech['ma60']:.0f} 元",
                f"{tech['distance_to_ma60_pct']:+.1f}%",
            )
            cl.metric(
                "下方支撐（60 日低）",
                f"{tech['recent_low_60d']:.0f} 元",
                f"{tech['distance_to_low_pct']:+.1f}%",
            )
            st.caption(f"目前位置：{tech['zone_label']}")
        else:
            st.warning(tech.get("warning", "技術資料不足"))


tp_data = _load_target_prices()
stocks = (tp_data or {}).get("stocks") or {}

if not stocks:
    st.info(
        "⏳ 估值資料尚未產生。請等 GitHub Actions（週六）跑完 "
        "`utils.eps_history.fetch_eps_history` + `utils.target_price.build_target_prices`，"
        "或本機跑 `python3 -c \"from utils.target_price import build_target_prices; build_target_prices()\"`。"
    )
else:
    st.caption(
        f"資料時間：`{tp_data.get('generated_at', '—')}`　"
        f"截止：`{tp_data.get('as_of_date', '—')}`　"
        f"涵蓋 {len(stocks)} 檔"
    )
    options = sorted(stocks.keys())
    labels = [f"{c}　{stocks[c].get('name', '')}" for c in options]
    idx = st.selectbox(
        "選擇股票（族群成員）",
        options=list(range(len(options))),
        format_func=lambda i: labels[i],
        index=0,
    )
    selected_code = options[idx]
    _render_valuation_section(stocks[selected_code])
