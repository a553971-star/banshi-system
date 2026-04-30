import streamlit as st
import pandas as pd
import os

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRADES_PATH = os.path.join(BASE_PATH, "trades.csv")
CASH_PATH = os.path.join(BASE_PATH, "cash_log.csv")

st.set_page_config(page_title="總資產管理", layout="wide")
st.title("💰 總資產管理")
st.caption("現金 + 持倉市值 = 你真正的總資產")

# 現金餘額
cash_balance = 0.0
if os.path.exists(CASH_PATH):
    try:
        cash_df = pd.read_csv(CASH_PATH)
        cash_df["signed_amount"] = cash_df["amount"] * cash_df["action"].map(
            {"初始資金": 1, "入金": 1, "出金": -1}).fillna(0)
        cash_balance = cash_df["signed_amount"].sum()
    except:
        cash_balance = 0.0

# 持倉計算
holding_value = 0.0
total_unrealized = 0.0
if os.path.exists(TRADES_PATH):
    try:
        df = pd.read_csv(TRADES_PATH)
        df["stock_id"] = df["stock_id"].astype(str)
        df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        df["signed_shares"] = df["shares"] * df["action"].map({"買入": 1, "賣出": -1}).fillna(0)
        df["signed_amount"] = df["amount"] * df["action"].map({"買入": 1, "賣出": -1}).fillna(0)
        grouped = df.groupby(["stock_id", "name"], as_index=False).agg(
            {"signed_shares": "sum", "signed_amount": "sum"})
        grouped = grouped[grouped["signed_shares"] > 0].copy()
        grouped["avg_cost"] = grouped.apply(
            lambda r: round(r["signed_amount"] / (r["signed_shares"] * 1000), 2)
            if r["signed_shares"] > 0 else 0, axis=1)

        if not grouped.empty:
            st.subheader("輸入目前價格")
            price_map = {}
            cols = st.columns(3)
            for i, row in grouped.iterrows():
                with cols[i % 3]:
                    price_map[row["stock_id"]] = st.number_input(
                        f"{row['stock_id']} {row['name']}",
                        min_value=0.0, step=0.01,
                        value=float(row["avg_cost"]),
                        key=f"tp_{row['stock_id']}")
            grouped["現價"] = grouped["stock_id"].map(price_map).fillna(0)
            grouped["現值"] = grouped["現價"] * grouped["signed_shares"] * 1000
            grouped["未實現損益"] = grouped["現值"] - grouped["signed_amount"]
            holding_value = grouped["現值"].sum()
            total_unrealized = grouped["未實現損益"].sum()
    except:
        pass

# 已實現損益
total_realized = 0.0
if os.path.exists(TRADES_PATH):
    try:
        df = pd.read_csv(TRADES_PATH)
        df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
        df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
        for stock, g in df.groupby("stock_id"):
            g = g.sort_values(by=["date", "time"]).reset_index(drop=True)
            position, total_cost, realized = 0.0, 0.0, 0.0
            for _, r in g.iterrows():
                if r["action"] == "買入":
                    total_cost += r["price"] * r["shares"]
                    position += r["shares"]
                elif r["action"] == "賣出" and position > 0:
                    sell = min(r["shares"], position)
                    avg = total_cost / position if position > 0 else 0
                    realized += (r["price"] - avg) * sell * 1000
                    total_cost -= avg * sell
                    position -= sell
            total_realized += realized
    except:
        pass

total_assets = cash_balance + holding_value

st.divider()
st.subheader("📊 總資產概覽")
c1, c2, c3, c4 = st.columns(4)
c1.metric("💵 現金", f"{cash_balance:,.0f} 元")
c2.metric("📈 持倉市值", f"{holding_value:,.0f} 元")
c3.metric("✅ 已實現損益", f"{total_realized:,.0f} 元")
c4.metric("🏦 總資產", f"{total_assets:,.0f} 元",
          delta=f"損益 {total_realized + total_unrealized:,.0f}")
