import streamlit as st
import pandas as pd
import os

BASE_PATH = os.path.expanduser("~/Documents/banshi_system")
TRADES_PATH = os.path.join(BASE_PATH, "trades.csv")

st.set_page_config(page_title="績效分析", layout="wide")
st.title("📈 績效分析")
st.caption("看你怎麼賺錢、怎麼賠錢——沒有這頁，你永遠不知道自己在進步還是重複犯錯")

if not os.path.exists(TRADES_PATH):
    st.warning("尚無交易資料")
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

results = []
for stock, g in df.groupby("stock_id"):
    g = g.sort_values(by=["date", "time"]).reset_index(drop=True)
    position, total_cost, realized = 0.0, 0.0, 0.0
    name = g["name"].iloc[0] if "name" in g.columns and not g.empty else stock
    for _, r in g.iterrows():
        shares = float(r["shares"])
        price = float(r["price"])
        if r["action"] == "買入":
            total_cost += price * shares
            position += shares
        elif r["action"] == "賣出" and position > 0:
            sell = min(shares, position)
            avg = total_cost / position if position > 0 else 0
            realized += (price - avg) * sell * 1000
            total_cost -= avg * sell
            position -= sell
    results.append({"stock_id": stock, "name": name, "已實現損益": round(realized, 0), "剩餘張數": round(position, 3)})

pnl_df = pd.DataFrame(results)
total_realized = pnl_df["已實現損益"].sum()

st.subheader("📊 總體績效")
c1, c2, c3 = st.columns(3)
c1.metric("總已實現損益", f"{total_realized:,.0f} 元")
c2.metric("交易總筆數", len(df))
c3.metric("個股數", len(pnl_df))

st.subheader("📈 個股已實現損益")
pnl_show = pnl_df.copy()
pnl_show["已實現損益"] = pnl_show["已實現損益"].map(lambda x: f"{x:,.0f}")
st.dataframe(pnl_show, use_container_width=True, hide_index=True)

st.subheader("📌 交易類型分析")
if "trade_type" in df.columns:
    type_df = df.groupby("trade_type").agg(交易次數=("amount", "count"), 總金額=("amount", "sum")).reset_index()
    st.dataframe(type_df, use_container_width=True, hide_index=True)

st.subheader("🧠 行為分析")
if "trade_type" in df.columns:
    emotion_buys = len(df[(df["trade_type"] == "情緒") & (df["action"] == "買入")])
    banshi_buys = len(df[(df["trade_type"] == "盤石") & (df["action"] == "買入")])
    st.write(f"**盤石策略買入**：{banshi_buys} 次")
    st.write(f"**情緒交易買入**：{emotion_buys} 次")
    total_buys = emotion_buys + banshi_buys
    if total_buys > 0:
        st.caption(f"情緒交易佔比：**{emotion_buys/total_buys*100:.1f}%**（建議控制在 30% 以下）")
