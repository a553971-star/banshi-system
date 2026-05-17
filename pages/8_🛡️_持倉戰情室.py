"""
pages/8_🛡️_持倉戰情室.py
持倉管理與離場風險監測。
哲學：結構優先・主力優先・量價優先・損益不主導決策
"""
import datetime
import os
import sys

import streamlit as st

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from portfolio_store import load_portfolio, add_position, remove_position
from engine.position.health_score import get_position_health
from engine.position.exit_signals import get_exit_alert, EXIT_CRITICAL, EXIT_WARNING
from utils.config import load_params

st.set_page_config(page_title="持倉戰情室", layout="wide")
st.title("🛡️ 持倉戰情室")
st.caption("結構優先・主力優先・量價優先・損益不主導決策")

params = load_params()

# ── 語意標籤 ──────────────────────────────────────────────────────────────────
_state_label = {
    "HEALTHY":                "🟢 健康",
    "SHAKEOUT":               "🟡 震盪",
    "WEAKENING":              "🟠 轉弱",
    "TREND_RISK":             "🔴 趨勢風險",
    "DISTRIBUTION_BREAKDOWN": "🔴 派發破位",
}
_exit_label = {
    "HOLD":     "🟢 繼續持有",
    "WATCH":    "🟡 持續觀察",
    "WARNING":  "🟠 開始減碼",
    "CRITICAL": "🔴 立刻出場",
}
_flow_label = {
    "ACCUMULATING": "🟢 吸籌",
    "DISTRIBUTION": "🔴 出貨",
    "NEUTRAL":      "🟠 中性",
}

# ── 載入持倉 ──────────────────────────────────────────────────────────────────
positions = load_portfolio()

# ── 新增持倉 ──────────────────────────────────────────────────────────────────
with st.expander("➕ 新增持倉", expanded=not positions):
    col1, col2, col3 = st.columns(3)
    with col1:
        new_id     = st.text_input("股票代號", placeholder="例：3661")
        new_price  = st.number_input("平均成本", min_value=0.0, step=0.5, format="%.2f")
    with col2:
        new_date   = st.date_input("買入日期", value=datetime.date.today())
        new_shares = st.number_input("持股張數（張，零股請填小數）", min_value=0.001, step=0.001, format="%.3f")
    with col3:
        new_note = st.text_input("備註（選填）")
        st.write("")
        if st.button("✅ 加入持倉", type="primary", use_container_width=True):
            if new_id.strip() and new_price > 0:
                from main import process_stock_live
                _r    = process_stock_live(new_id.strip(), params)
                _name = (_r.get("name") or new_id.strip()) if _r else new_id.strip()
                ok = add_position(
                    stock_id=new_id.strip(),
                    name=_name,
                    entry_price=new_price,
                    entry_date=str(new_date),
                    shares=new_shares,
                    note=new_note,
                )
                if ok:
                    st.success(f"已加入 {_name}")
                    st.rerun()
                else:
                    st.error("儲存失敗，請確認 GH_PAT 設定")
            else:
                st.warning("請填入股票代號和買入價")

if not positions:
    st.info("尚無持倉，請先新增股票")
    st.stop()

st.divider()

# ── 分析所有持倉 ──────────────────────────────────────────────────────────────
analyzed = []
for pos in positions:
    sid = pos["stock_id"]
    with st.spinner(f"分析 {sid}..."):
        try:
            from main import process_stock_live
            result = process_stock_live(sid, params) or {}
        except Exception:
            result = {}

    close   = float(result.get("close") or 0)
    entry   = float(pos.get("entry_price") or 0)
    pnl_pct = ((close - entry) / entry * 100) if entry > 0 and close > 0 else None

    row = dict(result)
    if pnl_pct is not None:
        row["pnl_pct"] = pnl_pct

    health = get_position_health(row)
    alert  = get_exit_alert(row)

    analyzed.append({
        "pos":     pos,
        "result":  result,
        "health":  health,
        "alert":   alert,
        "pnl_pct": pnl_pct,
        "close":   close,
    })

# CRITICAL 和 WARNING 依健康分數由低到高排序（最危險在最上面）
_exit_order = {"CRITICAL": 0, "WARNING": 1, "WATCH": 2, "HOLD": 3}
analyzed.sort(key=lambda x: (
    _exit_order.get(x["alert"]["level"], 9),
    x["health"]["score"]
))

# ── 持倉總覽 ──────────────────────────────────────────────────────────────────
st.subheader("📊 持倉總覽")

header = st.columns([1.5, 1, 1.2, 1, 1, 1.5, 2])
header[0].markdown("**股票**")
header[1].markdown("**健康分**")
header[2].markdown("**狀態**")
header[3].markdown("**Flow**")
header[4].markdown("**損益%**")
header[5].markdown("**警報**")
header[6].markdown("**操作建議**")
st.divider()

for item in analyzed:
    pos    = item["pos"]
    health = item["health"]
    alert  = item["alert"]
    result = item["result"]
    pnl    = item["pnl_pct"]

    cols = st.columns([1.5, 1, 1.2, 1, 1, 1.5, 2])
    cols[0].markdown(f"**{pos['stock_id']}** {pos.get('name', '')}")
    cols[1].markdown(f"**{health['score']}**")
    cols[2].markdown(_state_label.get(health['state'], health['state']))
    cols[3].markdown(_flow_label.get(result.get("flow_status", ""), "—"))

    if pnl is not None:
        cols[4].markdown(f"🟢 +{pnl:.1f}%" if pnl >= 0 else f"🔴 {pnl:.1f}%")
    else:
        cols[4].markdown("—")

    cols[5].markdown(_exit_label.get(alert['level'], alert['level']))
    cols[6].markdown(alert["action"])

st.divider()

# ── 離場警報區 ────────────────────────────────────────────────────────────────
critical_items = sorted(
    [x for x in analyzed if x["alert"]["level"] == EXIT_CRITICAL],
    key=lambda x: x["health"]["score"]
)
warning_items = sorted(
    [x for x in analyzed if x["alert"]["level"] == EXIT_WARNING],
    key=lambda x: x["health"]["score"]
)

if critical_items:
    st.subheader("🔴 立刻出場")
    for item in critical_items:
        pos = item["pos"]
        with st.container(border=True):
            st.markdown(f"### {pos['stock_id']} {pos.get('name', '')}　健康分：{item['health']['score']}")
            for reason in item["alert"]["reasons"]:
                st.markdown(f"- {reason}")
            if item["health"]["pnl_note"]:
                st.caption(item["health"]["pnl_note"])

if warning_items:
    st.subheader("🟠 開始減碼")
    for item in warning_items:
        pos = item["pos"]
        with st.container(border=True):
            st.markdown(f"### {pos['stock_id']} {pos.get('name', '')}　健康分：{item['health']['score']}")
            for reason in item["alert"]["reasons"]:
                st.markdown(f"- {reason}")

st.divider()

# ── 個股詳情 ──────────────────────────────────────────────────────────────────
st.subheader("🔍 個股詳情")
for item in analyzed:
    pos    = item["pos"]
    result = item["result"]
    health = item["health"]
    alert  = item["alert"]
    pnl    = item["pnl_pct"]

    with st.expander(
        f"{_state_label.get(health['state'], health['state'])}　"
        f"{pos['stock_id']} {pos.get('name', '')}　"
        f"健康分 {health['score']}　{_exit_label.get(alert['level'], alert['level'])}"
    ):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("平均成本", f"{pos.get('entry_price', '—')}")
        c2.metric("現價", f"{item['close'] or '—'}")
        c3.metric("損益%", f"{pnl:+.1f}%" if pnl is not None else "—")
        c4.metric("持股張數", f"{pos.get('shares', '—')}")

        st.caption(
            f"買入日期：{pos.get('entry_date', '—')}　"
            f"備註：{pos.get('note', '') or '—'}"
        )
        st.caption(
            f"Flow：{_flow_label.get(result.get('flow_status',''), '—')}　"
            f"成本位：{result.get('cost_level', '—')}　"
            f"B段：{result.get('B_phase', '—')}　"
            f"A天：{result.get('A_days', '—')}"
        )

        if alert["reasons"]:
            st.warning(" / ".join(alert["reasons"]))
        if health["pnl_note"]:
            st.info(health["pnl_note"])

        if st.button(f"🗑️ 移除 {pos['stock_id']}", key=f"remove_{pos['stock_id']}"):
            remove_position(pos["stock_id"])
            st.rerun()
