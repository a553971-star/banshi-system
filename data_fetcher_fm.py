import requests
import pandas as pd
import os
from datetime import datetime, timedelta

TOKEN = os.getenv("FINMIND_TOKEN")

def fetch_stock_data(stock_id: str, days: int = 365) -> pd.DataFrame:
    if not TOKEN:
        raise EnvironmentError("找不到 FINMIND_TOKEN，請先 export FINMIND_TOKEN=你的token")
    start_date = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockPrice", "data_id": stock_id, "start_date": start_date, "token": TOKEN}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"請求失敗：{e}")
        return pd.DataFrame()
    if data.get("status") != 200:
        print(f"API錯誤：{data.get('msg')}")
        return pd.DataFrame()
    records = data.get("data", [])
    if not records:
        print("查無資料")
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df = df.rename(columns={"max": "high", "min": "low", "Trading_Volume": "volume"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)
    return df[["date","open","high","low","close","volume"]]