import streamlit as st
import pandas as pd
import os

BASE_PATH = os.path.expanduser("~/Documents/banshi_system")
TRADES_PATH = os.path.join(BASE_PATH, "trades.csv")

st.set_page_config(page_title="持倉管理", layout="wide")
st.title("📊 持倉管理")
st.caption("根據交易記錄自動計算持倉、平均成本、未實現損益")

if not os.path.exists(TRADES_PATH):
    st.warning("目前沒有交易資料，請先到「交易記錄」頁面新增")
    st.stop()

try:
    df = pd.read_csv(TRADES_PATH)
except Exception as e:
    st.error(f"讀取失敗：{e}")
    st.stop()

df["stock_id"] = df["stock_id"].astype(str)
df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
df["signed_shares"] = df["shares"] * df["action"].map({"買入": 1, "賣出": -1}).fillna(0)
df["signed_amount"] = df["amount"] * df["action"].map({"買入": 1, "賣出": -1}).fillna(0)

grouped = df.groupby(["stock_id", "name"], as_index=False).agg(
    {"signed_shares": "sum", "signed_amount": "sum"}).copy()
grouped = grouped[grouped["signed_shares"] > 0].reset_index(drop=True)

if grouped.empty:
    st.info("目前沒有持倉")
    st.stop()

grouped["avg_cost"] = grouped.apply(
    lambda r: round(r["signed_amount"] / (r["signed_shares"] * 1000), 2)
    if r["signed_shares"] > 0 else 0, axis=1)

st.subheader("輸入目前價格")
price_map = {}
cols = st.columns(3)
for i, row in grouped.iterrows():
    with cols[i % 3]:
        price_map[row["stock_id"]] = st.number_input(
            f"{row['stock_id']} {row['name']}",
            min_value=0.0, step=0.01,
            value=float(row["avg_cost"]),
            key=f"price_{row['stock_id']}")

grouped["現價"] = grouped["stock_id"].map(price_map).fillna(0)
grouped["未實現損益"] = ((grouped["現價"] - grouped["avg_cost"]) *
                       grouped["signed_shares"] * 1000).fillna(0).round(0)
grouped["報酬率%"] = grouped.apply(
    lambda r: round((r["現價"] - r["avg_cost"]) / r["avg_cost"] * 100, 2)
    if r["avg_cost"] > 0 else 0, axis=1)

st.divider()
st.subheader("📈 持倉總覽")
display = grouped.rename(columns={
    "signed_shares": "持有張數",
    "avg_cost": "平均成本",
    "signed_amount": "投入成本"}).copy()
display = display[["stock_id", "name", "持有張數", "平均成本", "現價", "未實現損益", "報酬率%", "投入成本"]]
display_show = display.copy()
display_show["未實現損益"] = display_show["未實現損益"].map(lambda x: f"{x:,.0f}")
display_show["投入成本"] = display_show["投入成本"].map(lambda x: f"{x:,.0f}")
display_show["現價"] = display_show["現價"].map(lambda x: f"{x:.2f}")
display_show["平均成本"] = display_show["平均成本"].map(lambda x: f"{x:.2f}")
st.dataframe(display_show, use_container_width=True, hide_index=True)

total_pnl = grouped["未實現損益"].sum()
total_invested = grouped["signed_amount"].sum()
m1, m2, m3 = st.columns(3)
m1.metric("總未實現損益", f"{total_pnl:,.0f} 元")
m2.metric("總投入成本", f"{total_invested:,.0f} 元")
if total_invested > 0:
    m3.metric("整體報酬率", f"{(total_pnl / total_invested * 100):.2f}%")
