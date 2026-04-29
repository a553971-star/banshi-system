import time
import requests
import pandas as pd
import os

TOKEN = os.getenv("FINMIND_TOKEN")


def _fm(dataset, stock_id, start):
    for attempt in range(3):
        try:
            r = requests.get(
                "https://api.finmindtrade.com/api/v4/data",
                params={"dataset": dataset, "data_id": stock_id, "start_date": start, "token": TOKEN},
                timeout=15,
            )
            d = r.json()
            return pd.DataFrame(d["data"]) if d.get("status") == 200 and d.get("data") else pd.DataFrame()
        except Exception:
            if attempt < 2:
                time.sleep(5)
    return pd.DataFrame()


def merge_all_live(stock_id: str, start: str, end: str, db_path: str = None) -> pd.DataFrame:
    # 價格
    price = _fm("TaiwanStockPrice", stock_id, start)
    if price.empty:
        return pd.DataFrame()
    price = price.rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
    price = price[["date", "open", "high", "low", "close", "volume"]].copy()
    price["date"] = pd.to_datetime(price["date"])
    price = price.sort_values("date")

    # 法人
    inst = _fm("TaiwanStockInstitutionalInvestorsBuySell", stock_id, start)
    inst_wide = pd.DataFrame()
    if not inst.empty:
        inst["date"] = pd.to_datetime(inst["date"])
        inst["buy"]  = pd.to_numeric(inst["buy"],  errors="coerce").fillna(0)
        inst["sell"] = pd.to_numeric(inst["sell"], errors="coerce").fillna(0)
        pivot = inst.pivot_table(index="date", columns="name", values=["buy", "sell"], aggfunc="sum").fillna(0)
        foreign_buy  = pivot["buy"].get("Foreign_Investor", 0)
        foreign_sell = pivot["sell"].get("Foreign_Investor", 0)
        inv_buy      = pivot["buy"].get("Investment_Trust", 0)
        inv_sell     = pivot["sell"].get("Investment_Trust", 0)
        dealer_net   = (
            pivot["buy"].get("Dealer_self", 0) - pivot["sell"].get("Dealer_self", 0)
            + pivot["buy"].get("Dealer_Hedging", 0) - pivot["sell"].get("Dealer_Hedging", 0)
        )
        inst_wide = pd.DataFrame({
            "date":            pivot.index,
            "foreign_buy":     foreign_buy,
            "foreign_sell":    foreign_sell,
            "foreign_net":     foreign_buy - foreign_sell,
            "investment_buy":  inv_buy,
            "investment_sell": inv_sell,
            "investment_net":  inv_buy - inv_sell,
            "dealer_net":      dealer_net,
        }).reset_index(drop=True)

    # 融資
    mg = _fm("TaiwanStockMarginPurchaseShortSale", stock_id, start)
    mg_wide = pd.DataFrame()
    if not mg.empty:
        mg["date"] = pd.to_datetime(mg["date"])
        mg_wide = mg.rename(columns={
            "MarginPurchaseTodayBalance": "margin_balance",
            "ShortSaleTodayBalance":      "short_balance",
        })[["date", "margin_balance", "short_balance"]]

    # 合併
    df = price.copy()
    if not inst_wide.empty:
        df = df.merge(inst_wide, on="date", how="left")
    if not mg_wide.empty:
        df = df.merge(mg_wide, on="date", how="left")
    df = df.sort_values("date").reset_index(drop=True)
    df = df[
        (df["date"] >= pd.to_datetime(start))
        & (df["date"] <= pd.to_datetime(end))
    ]
    return df
