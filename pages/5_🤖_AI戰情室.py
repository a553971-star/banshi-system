"""
pages/5_🤖_AI戰情室.py
AI 供應鏈戰情室 — 共用 app.py 的 render_war_room_body() 渲染邏輯。
資料來源：latest_decisions_universe.csv ∩ ai_supply_chain.csv
"""
import datetime
import json
import os

import pandas as pd
import streamlit as st

# ── Import shared logic from app.py ──────────────────────────────────────────
# app.py has `if __name__ == "__main__": main()` so importing is safe.
try:
    from app import (
        _DIR,
        _STATE_LOG_PATH,
        _OVERRIDES_PATH,
        load_state_log,
        load_watchlist_overrides,
        save_watchlist_overrides,
        load_pinned,
        save_pinned,
        add_to_custom_watchlist,
        get_latest_state_changes,
        _cached_daily_verse,
        render_war_room_body,
        render_live_result_block,
        explain_metrics,
        load_foreign_ratio_map,
    )
    from live_analyzer import process_stock_live
except ImportError as _ie:
    import streamlit as st
    st.error(f"❌ Import 失敗：{_ie}")
    st.exception(_ie)
    st.stop()

_AI_CSV = os.path.join(_DIR, "ai_supply_chain.csv")
_UNI_CSV = os.path.join(_DIR, "latest_decisions_universe.csv")

st.set_page_config(page_title="AI戰情室", layout="wide")
st.title("🤖 AI戰情室")
st.caption("AI供應鏈法人建倉 × C/B/A軌跡 × Theme Rotation（主題輪動）")

# ── 即時個股查詢 ──────────────────────────────────────────────────────────────
st.subheader("🔬 即時個股分析")
st.caption("輸入 AI 供應鏈股票代號或中文名稱")

live_input = st.text_input("股票代號或名稱", placeholder="例：2330 或 台積電", key="ai_live_query")

if live_input:
    live_input = live_input.strip()
    live_id = live_input
    try:
        _ai_df_s = pd.read_csv(_AI_CSV, dtype={"stock_id": str})
        _mid = _ai_df_s[_ai_df_s["stock_id"] == live_input]
        _mname = _ai_df_s[_ai_df_s["name"].str.contains(live_input, na=False)]
        if not _mid.empty:
            live_id = live_input
        elif not _mname.empty:
            live_id = _mname.iloc[0]["stock_id"]
            st.caption(f"查詢：{_mname.iloc[0]['name']} ({live_id})")
        else:
            try:
                _uni_s = pd.read_csv(_UNI_CSV, dtype=str)
                _uid = _uni_s[_uni_s["stock_id"] == live_input]
                _uname = _uni_s[_uni_s["name"].str.contains(live_input, na=False)]
                if not _uid.empty:
                    live_id = live_input
                elif not _uname.empty:
                    live_id = _uname.iloc[0]["stock_id"]
                    st.caption(f"查詢：{_uname.iloc[0]['name']} ({live_id})")
            except Exception:
                pass
    except Exception as e:
        st.error(f"搜尋失敗: {e}")

    with st.spinner(f"正在分析 {live_id}..."):
        try:
            from main import load_params
            params = load_params()
            _live_result = process_stock_live(live_id, params, print_snapshot=False)
            if _live_result is None:
                st.error(f"process_stock_live({live_id}) 回傳 None")
        except Exception as e:
            _live_result = None
            st.error(str(e))

    if _live_result is None:
        st.warning("查無資料或分析失敗，請確認代號是否正確")
    else:
        render_live_result_block(live_id, _live_result)

st.divider()

quick_mode = st.toggle("⚡ 快速模式（只看前3檔）", value=False)

today_str = datetime.date.today().isoformat()
st.info(_cached_daily_verse(today_str))

# ── 讀取 AI 決策資料 ──────────────────────────────────────────────────────────
if not os.path.exists(_AI_CSV):
    st.error("找不到 ai_supply_chain.csv")
    st.stop()
if not os.path.exists(_UNI_CSV):
    st.warning("尚無 latest_decisions_universe.csv，請等 Actions 跑完")
    st.stop()

try:
    _ai_meta = pd.read_csv(_AI_CSV, dtype={"stock_id": str})
    _uni_data = pd.read_csv(_UNI_CSV, dtype=str)
    df = _ai_meta.merge(_uni_data, on="stock_id", how="inner", suffixes=("_ai", ""))
    if "name_ai" in df.columns:
        df["name"] = df["name_ai"].fillna(df.get("name", df["name_ai"]))
        df.drop(columns=["name_ai"], inplace=True)
except Exception as e:
    st.error(f"資料載入失敗：{e}")
    st.stop()

if df.empty:
    st.warning("沒有 AI 股票出現在 latest_decisions_universe.csv，請等 Actions 更新。")
    st.stop()

latest_date = df["date"].max() if "date" in df.columns else "—"
n_buy    = (df["decision"] == "BUY").sum()
n_wait   = (df["decision"] == "WAIT").sum()
n_ignore = (df["decision"] == "IGNORE").sum()
c1, c2, c3, c4 = st.columns(4)
c1.metric("🟢 BUY",    n_buy)
c2.metric("🟡 WAIT",   n_wait)
c3.metric("⚪ IGNORE", n_ignore)
c4.metric("📊 AI股票", len(df))
st.caption(f"資料日期：{latest_date}｜涵蓋 {len(df)}/{len(_ai_meta)} 支 AI 供應鏈股票")

# ── 初始化 session state ──────────────────────────────────────────────────────
if "overrides" not in st.session_state:
    st.session_state["overrides"] = load_watchlist_overrides()
st.session_state["pinned"] = load_pinned()

# ── 建立 prev_map ─────────────────────────────────────────────────────────────
state_log = load_state_log()
state_changes = get_latest_state_changes(state_log)
prev_map = {}
try:
    if not state_log.empty:
        _sl = state_log.copy()
        _sl["stock_id"] = _sl["stock_id"].astype(str)
        _sl["date"] = pd.to_datetime(_sl["date"], errors="coerce")
        _today = pd.Timestamp(datetime.date.today())
        _valid = _sl[_sl["date"] < _today]
        for _sid, _grp in _valid.groupby("stock_id"):
            _grp = _grp.sort_values("date")
            _latest = _grp.iloc[-1]
            prev_map[_sid] = {
                "C_days":      int(_latest.get("C_days", 0) or 0),
                "A_days":      int(_latest.get("A_days", 0) or 0),
                "flow_status": str(_latest.get("flow_status", "") or ""),
            }
except Exception:
    prev_map = {}

# ── Render 共用戰情室 ─────────────────────────────────────────────────────────
render_war_room_body(
    df=df,
    prev_map=prev_map,
    state_changes=state_changes,
    key_prefix="ai_",
    quick_mode=quick_mode,
)
