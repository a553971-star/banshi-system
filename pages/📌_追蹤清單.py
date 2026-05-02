import streamlit as st
import pandas as pd
import json
import os
import sys

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from app import explain_metrics
from live_analyzer import process_stock_live

WATCHLIST_PATH = os.path.join(BASE_PATH, "watchlist_custom.json")
COMPANY_PATH   = os.path.join(BASE_PATH, "companies.csv")

st.set_page_config(page_title="📌 追蹤清單", layout="wide")
st.title("📌 自訂追蹤清單")
st.caption("加入想追蹤的股票，一鍵取得即時盤石分析")


# ── 工具函式 ──────────────────────────────────────────────────────────────────

def load_watchlist():
    if os.path.exists(WATCHLIST_PATH):
        try:
            with open(WATCHLIST_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


def save_watchlist(wl):
    with open(WATCHLIST_PATH, "w", encoding="utf-8") as f:
        json.dump(wl, f, ensure_ascii=False, indent=2)


def resolve_stock_id(query: str) -> str | None:
    """中文名稱或代號 → 代號。先查 companies.csv，再查 FinMind。"""
    query = query.strip()
    if not query:
        return None

    # 純數字 → 直接當代號
    if query.isdigit():
        return query

    # 查 companies.csv
    if os.path.exists(COMPANY_PATH):
        try:
            df = pd.read_csv(COMPANY_PATH, dtype=str)
            hit = df[df["name"].str.contains(query, na=False)]
            if not hit.empty:
                return str(hit.iloc[0]["stock_id"])
            # 也嘗試代號欄位包含
            hit2 = df[df["stock_id"].str.contains(query, na=False)]
            if not hit2.empty:
                return str(hit2.iloc[0]["stock_id"])
        except Exception:
            pass

    # 查 FinMind TaiwanStockInfo
    try:
        from FinMind.data import DataLoader
        dl = DataLoader()
        info = dl.taiwan_stock_info()
        hit = info[info["stock_name"].str.contains(query, na=False)]
        if not hit.empty:
            return str(hit.iloc[0]["stock_id"])
    except Exception:
        pass

    return None


def run_analysis(stock_id: str):
    try:
        from main import load_params
        params = load_params()
        return process_stock_live(stock_id, params, print_snapshot=False)
    except Exception:
        return None


def decision_style(dec):
    if dec == "BUY":
        return "🚀 買進", "#28a745", "#e6f4ea"
    elif dec == "WAIT":
        return "👀 觀察", "#ffc107", "#fff8e1"
    else:
        return "⏳ 忽略", "#6c757d", "#f8f9fa"


def render_result_card(stock_id, result, key_prefix):
    dec   = result.get("decision", "IGNORE") if result else "IGNORE"
    conf  = int(result.get("confidence", 0)) if result else 0
    name  = result.get("name", "") if result else ""
    c     = int(result.get("C_days") or 0) if result else 0
    b     = int(result.get("B_days") or 0) if result else 0
    a     = int(result.get("A_days") or 0) if result else 0
    flow  = result.get("flow_status", "-") if result else "-"
    cost  = result.get("cost_level", "-") if result else "-"

    label, border, bg = decision_style(dec)

    try:
        _, coach = explain_metrics(result) if result else ([], "資料不足")
    except Exception:
        coach = "資料不足，繼續觀察"

    st.markdown(f"""
<div style="background:{bg}; border-left:8px solid {border}; border-radius:12px;
            padding:16px 20px; margin-bottom:10px;">
    <div style="display:flex; justify-content:space-between; align-items:center;">
        <span style="font-size:18px; font-weight:bold;">{stock_id} {name}</span>
        <span style="font-size:22px;">{label}
            <span style="background:{border}; color:white; font-size:14px;
                   padding:2px 10px; border-radius:12px; margin-left:8px;">
                信心 {conf}
            </span>
        </span>
    </div>
    <div style="font-size:13px; color:#444; margin-top:8px; line-height:1.9;">
        🧱 C={c}天 &nbsp;|&nbsp; 🏗️ B={b}天 &nbsp;|&nbsp; 🚀 A={a}天
        &nbsp;&nbsp;｜&nbsp;&nbsp;
        Flow: <b>{flow}</b> &nbsp;|&nbsp; Cost: <b>{cost}</b>
    </div>
    <div style="margin-top:8px; padding:6px 10px; background:rgba(0,0,0,0.04);
                border-radius:8px; font-size:13px; color:#333;">
        💬 {coach}
    </div>
</div>
""", unsafe_allow_html=True)

    b_phase    = result.get("B_phase") if result else None
    b_validity = result.get("B_validity") if result else None
    if b_phase or b_validity:
        validity_icon = {"TRUE_B": "✅", "FAKE_B": "❌", "UNCERTAIN": "❓"}.get(b_validity, "")
        phase_map = {
            "LAUNCH": ("🔴", "LAUNCH 發動初期", "盤石最佳進場點"),
            "MATURE": ("🟠", "MATURE 成熟建倉", "主力已在，等待發動"),
            "BUILD":  ("🔵", "BUILD 穩定建倉",  "主力開始進場"),
            "PREPARE":("🟡", "PREPARE 建倉中",  "有人在看，未成形"),
            "LATE":   ("⚫", "LATE 太晚",       "已漲一段，不要追"),
        }
        p_icon, p_label, p_desc = phase_map.get(b_phase, ("⚪", b_phase or "", ""))
        st.markdown(f"**{validity_icon} {b_validity}**　**{p_icon} {p_label}**")
        if p_desc:
            st.caption(p_desc)
        st.caption(f"B_quality: {result.get('B_quality', 'N/A')}　B_window: {result.get('B_window_20', 'N/A')}")


# ── 加入追蹤 ──────────────────────────────────────────────────────────────────

with st.form("add_form", clear_on_submit=True):
    col1, col2 = st.columns([5, 1])
    with col1:
        query = st.text_input("", placeholder="輸入股票代號或中文名稱，例如：2330 或 台積電",
                              label_visibility="collapsed")
    with col2:
        submitted = st.form_submit_button("➕ 加入追蹤", type="primary", use_container_width=True)

if submitted:
    if not query.strip():
        st.warning("請輸入股票代號或名稱")
    else:
        sid = resolve_stock_id(query.strip())
        if not sid:
            st.error(f"找不到「{query}」，請確認代號或名稱是否正確")
        else:
            wl = load_watchlist()
            if sid in wl:
                st.info(f"{sid} 已在追蹤清單中")
            else:
                wl.append(sid)
                save_watchlist(wl)
                st.success(f"✅ 已加入：{sid}")
                st.rerun()

st.divider()

# ── 追蹤清單 ──────────────────────────────────────────────────────────────────

wl = load_watchlist()

if not wl:
    st.info("追蹤清單是空的，在上方輸入股票代號加入吧！")
    st.stop()

# 建立代號→名稱對照表
name_map = {}
try:
    co_df = pd.read_csv(os.path.join(BASE_PATH, "companies.csv"), dtype=str)
    name_map.update(dict(zip(co_df["stock_id"], co_df["name"])))
except Exception:
    pass
try:
    dec_df = pd.read_csv(os.path.join(BASE_PATH, "latest_decisions.csv"), dtype=str)
    name_map.update({k: v for k, v in zip(dec_df["stock_id"], dec_df["name"])
                     if v and v != "nan"})
except Exception:
    pass

# B 資料對照表（從 latest_decisions.csv 離線讀取）
b_map = {}
try:
    dec_df2 = pd.read_csv(os.path.join(BASE_PATH, "latest_decisions.csv"), dtype=str)
    for _, row in dec_df2.iterrows():
        sid = str(row.get("stock_id", ""))
        b_map[sid] = {
            "B_quality":   row.get("B_quality", ""),
            "B_window_20": row.get("B_window_20", ""),
            "B_validity":  row.get("B_validity", ""),
            "B_phase":     row.get("B_phase", ""),
        }
except Exception:
    b_map = {}

col_title, col_refresh = st.columns([7, 1])
with col_title:
    st.subheader(f"📋 追蹤中（{len(wl)} 支）")
with col_refresh:
    st.markdown("<div style='margin-top:8px;'></div>", unsafe_allow_html=True)
    if st.button("🔄 全部重新分析", use_container_width=True):
        for sid in wl:
            st.session_state.pop(f"wl_result_{sid}", None)
        st.rerun()

if "wl_results" not in st.session_state:
    st.session_state["wl_results"] = {}

for stock_id in wl:
    show_key   = f"wl_show_{stock_id}"
    result_key = f"wl_result_{stock_id}"

    col1, col2, col3 = st.columns([6, 2, 1])
    with col1:
        cached = st.session_state["wl_results"].get(stock_id)
        name = (cached.get("name", "") if cached else "") or name_map.get(stock_id, "")
        st.markdown(f"**{stock_id}** {name}")
        b_info     = b_map.get(stock_id, {})
        b_validity = b_info.get("B_validity", "")
        b_phase    = b_info.get("B_phase", "")
        b_quality  = b_info.get("B_quality", "")
        b_window   = b_info.get("B_window_20", "")
        validity_icon = {"TRUE_B": "✅", "FAKE_B": "❌", "UNCERTAIN": "❓"}.get(b_validity, "")
        phase_icon    = {"LAUNCH": "🔴", "MATURE": "🟠", "BUILD": "🔵",
                         "PREPARE": "🟡", "LATE": "⚫"}.get(b_phase, "")
        if b_validity:
            st.markdown(f"**{validity_icon} {b_validity}**　{phase_icon} {b_phase}")
            st.caption(f"B_quality: {b_quality}　B_window: {b_window}")
    with col2:
        live_label = "🔬 收起" if st.session_state.get(show_key, False) else "🔬 即時分析"
        if st.button(live_label, key=f"wl_live_btn_{stock_id}", use_container_width=True):
            st.session_state[show_key] = not st.session_state.get(show_key, False)
            if not st.session_state[show_key]:
                st.session_state["wl_results"].pop(stock_id, None)
            st.rerun()
    with col3:
        if st.button("🗑️", key=f"wl_remove_{stock_id}", help="移除追蹤"):
            wl2 = load_watchlist()
            wl2 = [s for s in wl2 if s != stock_id]
            save_watchlist(wl2)
            st.session_state["wl_results"].pop(stock_id, None)
            st.session_state.pop(show_key, None)
            st.rerun()

    if st.session_state.get(show_key, False):
        if stock_id not in st.session_state["wl_results"]:
            with st.spinner(f"分析 {stock_id} 中..."):
                st.session_state["wl_results"][stock_id] = run_analysis(stock_id)

        result = st.session_state["wl_results"].get(stock_id)
        if result:
            render_result_card(stock_id, result, key_prefix="wl")
        else:
            st.warning(f"⚠️ {stock_id} 無法取得資料，請確認代號是否正確")
