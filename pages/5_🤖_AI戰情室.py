"""
pages/5_🤖_AI戰情室.py
AI 概念股完整戰情室，資料來源 latest_decisions_ai.csv。
共用 app.py 的 render_war_room_body。
"""
import datetime
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
