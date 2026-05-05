import json
import os

import pandas as pd
import streamlit as st

BASE_PATH   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH    = os.path.join(BASE_PATH, "latest_decisions_universe.csv")
PIN_PATH    = os.path.join(BASE_PATH, "pinned.json")

st.set_page_config(page_title="全市場掃描", layout="wide")
st.title("🌐 全市場掃描")
st.caption("來源：latest_decisions_universe.csv（每日 Actions 更新）")


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


# ── 讀資料 ────────────────────────────────────────────────────────────────────
if not os.path.exists(CSV_PATH):
    st.warning("尚無資料（latest_decisions_universe.csv 不存在，請等 Actions 跑完）")
    st.stop()

df = pd.read_csv(CSV_PATH, dtype=str)
for col in ["C_days", "B_days", "A_days", "B_quality"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

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
    decision_options = ["全部", "BUY", "WAIT", "IGNORE"]
    selected_decision = st.selectbox("Decision", decision_options)

with col_f2:
    flow_options = ["全部"] + sorted(df["flow_status"].dropna().unique().tolist())
    selected_flow = st.selectbox("Flow", flow_options)

with col_f3:
    search = st.text_input("搜尋股票代號 / 名稱", placeholder="例：2330 或 台積")

# 套用篩選
filtered = df.copy()
if selected_decision != "全部":
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

# ── 排序：BUY > WAIT > IGNORE，同類依 B_quality 降序 ─────────────────────────
order_map = {"BUY": 0, "WAIT": 1, "IGNORE": 2}
filtered = filtered.copy()
filtered["_order"] = filtered["decision"].map(order_map).fillna(9)
filtered = filtered.sort_values(["_order", "B_quality"], ascending=[True, False])

# ── 表格 + 📌 按鈕 ────────────────────────────────────────────────────────────
DISPLAY_COLS = ["stock_id", "name", "decision", "C_days", "B_days", "A_days",
                "B_quality", "flow_status", "foreign_position"]

pinned = load_pinned()

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

    dec_icon = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪"}.get(decision, "⚪")
    pin_icon = "📌" if sid in pinned else "☆"

    with st.container():
        r1, r2 = st.columns([10, 1])
        with r1:
            c_str  = f"C={int(c_days)}" if pd.notna(c_days) else "C=-"
            b_str  = f"B={int(b_days)}" if pd.notna(b_days) else "B=-"
            a_str  = f"A={int(a_days)}" if pd.notna(a_days) else "A=-"
            bq_str = f"Bq={int(b_qual)}" if pd.notna(b_qual) else "Bq=-"
            fp_str = f"外資:{int(float(f_pos)):,}張" if pd.notna(f_pos) and str(f_pos) not in ("", "nan") else ""
            st.markdown(
                f"{dec_icon} **{sid} {name}** &nbsp;｜&nbsp; {decision} &nbsp;｜&nbsp; "
                f"{c_str} {b_str} {a_str} {bq_str} &nbsp;｜&nbsp; {flow}"
                + (f" &nbsp;｜&nbsp; {fp_str}" if fp_str else "")
            )
        with r2:
            if st.button(pin_icon, key=f"pin_{sid}"):
                if sid in pinned:
                    pinned.discard(sid)
                else:
                    pinned.add(sid)
                save_pinned(pinned)
                st.rerun()
