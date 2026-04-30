import streamlit as st
import pandas as pd
import os
from datetime import datetime

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASH_PATH = os.path.join(BASE_PATH, "cash_log.csv")

st.set_page_config(page_title="現金管理", layout="wide")
st.title("💵 現金管理")
st.caption("記錄入金、出金與初始資金，讓總資產計算真正完整")

with st.form("cash_form", clear_on_submit=True):
    col1, col2 = st.columns(2)
    with col1:
        action = st.selectbox("資金類型", ["初始資金", "入金", "出金"])
        amount = st.number_input("金額（元）", min_value=0.0, step=1000.0, value=0.0, format="%.0f")
    with col2:
        note = st.text_input("備註", placeholder="薪水轉入 / 生活費 / 股票賣出轉入...")
        trade_date = st.date_input("日期", value=datetime.now().date())
    submitted = st.form_submit_button("💾 儲存資金紀錄", type="primary")

if submitted:
    if amount <= 0:
        st.error("❌ 金額必須大於 0")
    else:
        new_row = pd.DataFrame([{
            "date": trade_date.strftime("%Y-%m-%d"),
            "action": action,
            "amount": amount,
            "note": note or ""
        }])
        if os.path.exists(CASH_PATH):
            try:
                df_old = pd.read_csv(CASH_PATH)
                df = pd.concat([df_old, new_row], ignore_index=True)
            except:
                df = new_row
        else:
            df = new_row
        df.to_csv(CASH_PATH, index=False, encoding="utf-8-sig")
        st.success(f"✅ 已記錄 {action} {amount:,.0f} 元")
        st.rerun()

st.divider()
st.subheader("💰 目前現金餘額")
if os.path.exists(CASH_PATH):
    try:
        df = pd.read_csv(CASH_PATH)
        df["signed_amount"] = df["amount"] * df["action"].map(
            {"初始資金": 1, "入金": 1, "出金": -1}).fillna(0)
        cash_balance = df["signed_amount"].sum()
        st.metric("💵 可用現金", f"{cash_balance:,.0f} 元")
        in_total = df[df["action"].isin(["初始資金", "入金"])]["amount"].sum()
        out_total = df[df["action"] == "出金"]["amount"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("總入金（含初始）", f"{in_total:,.0f} 元")
        c2.metric("總出金", f"{out_total:,.0f} 元")
        c3.metric("淨資金流", f"{cash_balance:,.0f} 元")
        st.subheader("📜 資金異動紀錄")
        df_display = df.sort_values(by="date", ascending=False).copy()
        df_display["金額"] = df_display.apply(
            lambda x: f"+{x['amount']:,.0f}" if x["action"] in ["初始資金", "入金"] else f"-{x['amount']:,.0f}", axis=1)
        st.dataframe(df_display[["date", "action", "金額", "note"]], use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"讀取失敗：{e}")
else:
    st.info("目前尚無資金紀錄，請先設定「初始資金」")
    st.caption("💡 建議第一筆先輸入「初始資金」，例如你的起始本金")
