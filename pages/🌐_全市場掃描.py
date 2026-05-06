import json
import os

import pandas as pd
import streamlit as st

BASE_PATH        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH         = os.path.join(BASE_PATH, "latest_decisions_universe.csv")
PIN_PATH         = os.path.join(BASE_PATH, "pinned.json")
UNIVERSE_PATH    = os.path.join(BASE_PATH, "universe.csv")
SHAREHOLDING_PATH= os.path.join(BASE_PATH, "shareholding_latest.csv")

THEME_COLOR = {
    "AI_Core":       ("🔴", "AI核心"),
    "AI_Power":      ("🟠", "AI電源重電"),
    "PCB_Material":  ("🟡", "PCB散熱材料"),
    "Memory_Storage":("🟣", "記憶體儲存"),
    "Comm_Net":      ("🔵", "通信網路光通"),
    "Auto_Elec":     ("⚪", "車用電子"),
    "Biotech_Green": ("🟢", "生技綠能"),
    "Digital_Cloud": ("⚫", "數位雲端"),
}

st.set_page_config(page_title="全市場掃描", layout="wide")
st.title("🌐 全市場掃描")
st.caption("來源：latest_decisions_universe.csv（每日 Actions 更新）")

# ── 主題圖例 ──────────────────────────────────────────────────────────────────
with st.expander("🎨 產業主題圖例", expanded=False):
    legend_cols = st.columns(4)
    legend_items = list(THEME_COLOR.items())
    for i, (theme, (dot, label)) in enumerate(legend_items):
        legend_cols[i % 4].markdown(f"{dot} **{label}**")


def load_pinned() -> set:
    try:
        with open(PIN_PATH, "r") as f:
            data = json.load(f)
            return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def save_pinned(pinned: set) -> None:
    with open(PIN_PATH, "w") as f:
        json.dump(list(pinned), f)


def run_live_analysis(stock_id: str):
    try:
        from main import load_params
        from live_analyzer import process_stock_live
        params = load_params()
        return process_stock_live(stock_id, params, print_snapshot=False)
    except Exception:
        return None


# ── 讀資料 ────────────────────────────────────────────────────────────────────
if not os.path.exists(CSV_PATH):
    st.warning("尚無資料（latest_decisions_universe.csv 不存在，請等 Actions 跑完）")
    st.stop()

df = pd.read_csv(CSV_PATH, dtype=str)
for col in ["C_days", "B_days", "A_days", "B_quality"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Ranking 2.0 total_score ───────────────────────────────────────────────────
_bq = df["B_quality"]
_bq_min, _bq_max = _bq.min(), _bq.max()
df["B_Quality_Norm"] = ((_bq - _bq_min) / (_bq_max - _bq_min) * 100).fillna(0) if _bq_max > _bq_min else 50.0

def _fresh_a_score(a):
    if pd.isna(a): return 0
    a = int(a)
    if a == 0: return 0
    mapping = {1: 100, 2: 95, 3: 85, 4: 70, 5: 50}
    if a <= 5: return mapping.get(a, 0)
    if a <= 8: return 30
    return 10

df["Fresh_A_Score"] = df["A_days"].apply(_fresh_a_score)
df["C_Recency_Score"] = ((30 - df["C_days"].fillna(30).clip(0, 30)) * 3.3).clip(0, 100)

_fp_col = "foreign_profit_pct" if "foreign_profit_pct" in df.columns else None
_fp = pd.to_numeric(df[_fp_col], errors="coerce") if _fp_col else pd.Series(pd.NA, index=df.index, dtype=float)
df["Foreign_Profit_Score"] = (100 - (_fp - 5).abs() * 12).clip(0, 100).fillna(50)

_vr = pd.to_numeric(df["volume_ratio"], errors="coerce") if "volume_ratio" in df.columns else pd.Series(pd.NA, index=df.index, dtype=float)
df["Volume_Norm"] = ((_vr.clip(0.5, 3.0) - 0.5) / 2.5 * 100).clip(0, 100).fillna(50)

df["total_score"] = (
    df["B_Quality_Norm"] * 0.40 +
    df["Foreign_Profit_Score"] * 0.25 +
    df["Fresh_A_Score"] * 0.20 +
    df["Volume_Norm"] * 0.10 +
    df["C_Recency_Score"] * 0.05
).round(1)

# 主題對照表（universe.csv 含 theme 欄位）
theme_map = {}
try:
    u_df = pd.read_csv(UNIVERSE_PATH, dtype=str)
    if "theme" in u_df.columns:
        theme_map = dict(zip(u_df["stock_id"], u_df["theme"]))
except Exception:
    pass

# 外資持股比例對照表
ratio_map = {}
try:
    sh_df = pd.read_csv(SHAREHOLDING_PATH, dtype=str)
    ratio_map = {str(r["stock_id"]): r.get("foreign_ratio", "") for _, r in sh_df.iterrows()}
except Exception:
    pass

# ── 統計區 ────────────────────────────────────────────────────────────────────
n_buy    = (df["decision"] == "BUY").sum()
n_wait   = (df["decision"] == "WAIT").sum()
n_ignore = (df["decision"] == "IGNORE").sum()
total    = len(df)

c1, c2, c3, c4 = st.columns(4)
c1.metric("🟢 BUY",    n_buy)
c2.metric("🟡 WAIT",   n_wait)
c3.metric("⚪ IGNORE", n_ignore)
c4.metric("📊 總計",   total)

date_val = df["date"].iloc[0] if "date" in df.columns and not df.empty else "N/A"
st.caption(f"資料日期：{date_val}")

st.divider()

# ── 篩選器 ────────────────────────────────────────────────────────────────────
col_f1, col_f2, col_f3 = st.columns([2, 2, 3])

with col_f1:
    decision_options = ["WAIT+BUY", "全部", "BUY", "WAIT", "IGNORE"]
    selected_decision = st.selectbox("Decision", decision_options)

with col_f2:
    flow_options = ["全部"] + sorted(df["flow_status"].dropna().unique().tolist())
    selected_flow = st.selectbox("Flow", flow_options)

with col_f3:
    search = st.text_input("搜尋股票代號 / 名稱", placeholder="例：2330 或 台積")

# 套用篩選
filtered = df.copy()
if selected_decision == "WAIT+BUY":
    filtered = filtered[filtered["decision"].isin(["WAIT", "BUY"])]
elif selected_decision != "全部":
    filtered = filtered[filtered["decision"] == selected_decision]
if selected_flow != "全部":
    filtered = filtered[filtered["flow_status"] == selected_flow]
if search:
    q = search.strip()
    filtered = filtered[
        filtered["stock_id"].str.contains(q, na=False) |
        filtered["name"].astype(str).str.contains(q, na=False)
    ]

st.caption(f"顯示 {len(filtered)} / {total} 支")

# ── 排序控制 ──────────────────────────────────────────────────────────────────
if "us_sort_key" not in st.session_state:
    st.session_state["us_sort_key"] = "total_score"
if "us_sort_asc" not in st.session_state:
    st.session_state["us_sort_asc"] = False

sort_cols = st.columns([1, 1, 1, 1, 1, 4])
sort_buttons = [
    ("綜合評分", "total_score"),
    ("C天數", "C_days"),
    ("B天數", "B_days"),
    ("A天數", "A_days"),
    ("B品質", "B_quality"),
]
for i, (label, key) in enumerate(sort_buttons):
    with sort_cols[i]:
        cur_key = st.session_state["us_sort_key"]
        cur_asc = st.session_state["us_sort_asc"]
        arrow = (" ↑" if cur_asc else " ↓") if cur_key == key else ""
        if st.button(f"{label}{arrow}", key=f"us_sort_{key}", use_container_width=True):
            if st.session_state["us_sort_key"] == key:
                st.session_state["us_sort_asc"] = not cur_asc
            else:
                st.session_state["us_sort_key"] = key
                st.session_state["us_sort_asc"] = False
            st.rerun()

# 套用排序
order_map = {"BUY": 0, "WAIT": 1, "IGNORE": 2}
filtered = filtered.copy()
filtered["_order"] = filtered["decision"].map(order_map).fillna(9)

sort_key = st.session_state["us_sort_key"]
sort_asc  = st.session_state["us_sort_asc"]

if sort_key == "_order":
    filtered = filtered.sort_values(["_order", "total_score"], ascending=[True, False])
elif sort_key == "total_score":
    filtered = filtered.sort_values("total_score", ascending=sort_asc, na_position="last")
else:
    filtered = filtered.sort_values([sort_key, "_order"], ascending=[sort_asc, True],
                                    na_position="last")

# ── Session state init ────────────────────────────────────────────────────────
if "us_results" not in st.session_state:
    st.session_state["us_results"] = {}

pinned = load_pinned()

# ── 每支股票列 ────────────────────────────────────────────────────────────────
for _, row in filtered.iterrows():
    sid      = str(row.get("stock_id", ""))
    name     = str(row.get("name", ""))
    decision = str(row.get("decision", ""))
    c_days   = row.get("C_days")
    b_days   = row.get("B_days")
    a_days   = row.get("A_days")
    b_qual   = row.get("B_quality")
    flow     = str(row.get("flow_status", "") or "-")
    f_pos    = row.get("foreign_position")

    b_validity = str(row.get("B_validity", "") or "")
    b_phase    = str(row.get("B_phase", "") or "")

    dec_icon   = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪"}.get(decision, "⚪")
    theme      = theme_map.get(sid, "")
    theme_dot  = THEME_COLOR.get(theme, ("", ""))[0]
    f_ratio    = ratio_map.get(sid, "")

    show_key   = f"us_show_{sid}"
    result_key = f"us_result_{sid}"

    with st.container():
        r1, r2 = st.columns([7, 3])
        with r1:
            c_str  = f"C={int(c_days)}" if pd.notna(c_days) else "C=-"
            b_str  = f"B={int(b_days)}" if pd.notna(b_days) else "B=-"
            a_str  = f"A={int(a_days)}" if pd.notna(a_days) else "A=-"
            bq_str = f"Bq={int(b_qual)}" if pd.notna(b_qual) else "Bq=-"
            if pd.notna(f_pos) and str(f_pos) not in ("", "nan"):
                fp_str = f"外資:{int(float(f_pos)):,}張"
                if f_ratio and str(f_ratio) not in ("", "nan"):
                    fp_str += f"（{float(f_ratio):.1f}%）"
            else:
                fp_str = ""

            validity_icon = {"TRUE_B": "✅", "FAKE_B": "❌", "UNCERTAIN": "❓"}.get(b_validity, "")
            phase_icon    = {"LAUNCH": "🔴", "MATURE": "🟠", "BUILD": "🔵",
                             "PREPARE": "🟡", "LATE": "⚫"}.get(b_phase, "")

            line1 = (
                f"{dec_icon} {theme_dot} **{sid} {name}** &nbsp;｜&nbsp; {decision} &nbsp;｜&nbsp; "
                f"{c_str} {b_str} {a_str} {bq_str} &nbsp;｜&nbsp; {flow}"
                + (f" &nbsp;｜&nbsp; {fp_str}" if fp_str else "")
            )
            st.markdown(line1)
            if b_validity:
                st.markdown(
                    f"<span style='font-size:12px;color:#666;'>"
                    f"{validity_icon} {b_validity} &nbsp; {phase_icon} {b_phase}"
                    f"</span>",
                    unsafe_allow_html=True,
                )
        with r2:
            live_label = "🔬 收起" if st.session_state.get(show_key, False) else "🔬 即時分析"
            if st.button(live_label, key=f"us_live_{sid}", use_container_width=True):
                new_show = not st.session_state.get(show_key, False)
                st.session_state[show_key] = new_show
                if not new_show:
                    st.session_state["us_results"].pop(sid, None)
                st.rerun()
            pin_label = "📌 已追蹤" if sid in pinned else "☆ 追蹤"
            if st.button(pin_label, key=f"us_pin_{sid}", use_container_width=True):
                if sid in pinned:
                    pinned.discard(sid)
                else:
                    pinned.add(sid)
                save_pinned(pinned)
                st.rerun()

    if st.session_state.get(show_key, False):
        if sid not in st.session_state["us_results"]:
            with st.spinner(f"分析 {sid} 中..."):
                st.session_state["us_results"][sid] = run_live_analysis(sid)
        result = st.session_state["us_results"].get(sid)
        if result:
            from app import render_live_result_block
            render_live_result_block(sid, result)
        else:
            st.warning(f"⚠️ {sid} 無法取得資料，請確認代號是否正確")
