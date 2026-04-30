import streamlit as st
import pandas as pd
from datetime import datetime
import os

BASE_PATH = os.path.expanduser("~/Documents/banshi_system")
TRADES_PATH = os.path.join(BASE_PATH, "trades.csv")
COMPANY_PATH = os.path.join(BASE_PATH, "companies.csv")
DECISION_PATH = os.path.join(BASE_PATH, "latest_decisions.csv")

st.set_page_config(page_title="交易記錄", layout="wide")
st.title("📒 交易記錄")
st.caption("每筆交易會自動記錄當時的盤石決策狀態，方便之後分析對錯。")

if os.path.exists(COMPANY_PATH):
    companies = pd.read_csv(COMPANY_PATH, dtype=str)
else:
    companies = pd.DataFrame(columns=["stock_id", "name"])

if os.path.exists(DECISION_PATH):
    try:
        decisions = pd.read_csv(DECISION_PATH, dtype=str)
        decisions["stock_id"] = decisions["stock_id"].astype(str)
    except:
        decisions = pd.DataFrame(columns=["stock_id"])
else:
    decisions = pd.DataFrame(columns=["stock_id"])


def get_decision_context(sid: str):
    if decisions.empty or not sid:
        return {}
    row = decisions[decisions["stock_id"] == sid]
    if row.empty:
        return {}
    r = row.iloc[0]
    return {
        "C_days": r.get("C_days", ""),
        "B_days": r.get("B_days", ""),
        "A_days": r.get("A_days", ""),
        "flow_status": r.get("flow_status", ""),
        "cost_level": r.get("cost_level", ""),
        "decision": r.get("decision", ""),
        "confidence": r.get("confidence", ""),
    }


with st.form("trade_form", clear_on_submit=True):
    col1, col2 = st.columns(2)
    with col1:
        stock_id = st.text_input("股票代號", placeholder="2330").strip().upper()
        default_name = ""
        if stock_id:
            match = companies[companies["stock_id"] == stock_id]
            if not match.empty:
                default_name = match.iloc[0]["name"]
        name = st.text_input("股票名稱", value=default_name)
        action = st.selectbox("買賣方向", ["買入", "賣出"])
        shares = st.number_input("張數（零股用小數）", min_value=0.001, step=0.001, value=1.0, format="%.3f")
    with col2:
        price = st.number_input("成交價格", min_value=0.01, step=0.01, value=0.0, format="%.2f")
        now = datetime.now()
        trade_date = st.date_input("交易日期", value=now.date())
        trade_time = st.time_input("交易時間", value=now.time())
        trade_type = st.selectbox("交易類型", ["盤石", "情緒", "測試", "其他"])
        note = st.text_input("備註", placeholder="補登 / 當沖 / 測試...")
    submitted = st.form_submit_button("💾 儲存交易", type="primary")

if submitted:
    if not stock_id:
        st.error("❌ 股票代號不能空白")
    elif price <= 0:
        st.error("❌ 價格必須大於 0")
    else:
        amount = round(shares * price * 1000)
        context = get_decision_context(stock_id)
        new_row = pd.DataFrame([{
            "date": trade_date.strftime("%Y-%m-%d"),
            "time": trade_time.strftime("%H:%M:%S"),
            "stock_id": stock_id,
            "name": name,
            "action": action,
            "shares": shares,
            "price": price,
            "amount": amount,
            "trade_type": trade_type,
            "note": note,
            "當時決策": context.get("decision", ""),
            "當時信心": context.get("confidence", ""),
            "C_days": context.get("C_days", ""),
            "B_days": context.get("B_days", ""),
            "A_days": context.get("A_days", ""),
            "flow_status": context.get("flow_status", ""),
            "cost_level": context.get("cost_level", ""),
        }])
        if os.path.exists(TRADES_PATH):
            try:
                df_old = pd.read_csv(TRADES_PATH, dtype=str)
            except:
                df_old = pd.DataFrame()
            df = pd.concat([df_old, new_row], ignore_index=True)
        else:
            df = new_row
        df.to_csv(TRADES_PATH, index=False, encoding="utf-8-sig")
        st.success(f"✅ 已儲存：{action} {stock_id} {name} {shares:.3f}張 @ {price:.2f}")
        if context.get("decision"):
            st.info(f"📊 當時盤石：{context.get('decision')} | 信心 {context.get('confidence')} | C:{context.get('C_days')} B:{context.get('B_days')} A:{context.get('A_days')}")
        st.rerun()

st.divider()
st.subheader("📊 歷史交易紀錄")
if os.path.exists(TRADES_PATH):
    try:
        df = pd.read_csv(TRADES_PATH)
        df = df.sort_values(by=["date", "time"], ascending=False)
        filter_stock = st.text_input("🔍 篩選股票代號", key="trade_filter")
        if filter_stock:
            df = df[df["stock_id"].astype(str).str.contains(filter_stock, case=False, na=False)]
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"讀取失敗：{e}")
else:
    st.info("目前沒有交易紀錄")
