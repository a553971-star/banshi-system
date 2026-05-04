"""
build_universe.py — 建立交易 universe
從 FinMind TaiwanStockInfo 抓全市場股票，排除金融產業
輸出：universe.csv
"""
import os
import requests
import pandas as pd

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
OUT_PATH  = os.path.join(BASE_PATH, "universe.csv")
TOKEN     = os.getenv("FINMIND_TOKEN")


def main():
    r = requests.get(
        "https://api.finmindtrade.com/api/v4/data",
        params={"dataset": "TaiwanStockInfo", "token": TOKEN},
        timeout=30
    )
    data = r.json()
    if data.get("status") != 200:
        print(f"抓取失敗：{data.get('status')}")
        return

    df = pd.DataFrame(data["data"])
    df = df[["stock_id", "stock_name", "industry_category"]].drop_duplicates("stock_id")
    df.columns = ["stock_id", "name", "industry"]

    # 只保留4位數代號
    df = df[df["stock_id"].str.match(r"^\d{4}$")]

    # 排除金融產業
    finance_keywords = ["金融", "銀行", "保險", "證券", "票券", "投信", "期貨"]
    def is_finance(industry):
        if not isinstance(industry, str):
            return False
        return any(kw in industry for kw in finance_keywords)

    before = len(df)
    df = df[~df["industry"].apply(is_finance)]
    print(f"  排除金融股：{before - len(df)} 支")

    # 排除 ETF 和非一般股票，只保留 1000~9999
    df = df[df["stock_id"].str.match(r"^[1-9]\d{3}$")]

    universe = df[["stock_id"]].copy()
    universe.to_csv(OUT_PATH, index=False)
    print(f"Universe 建立完成：{len(universe)} 支")
    print(f"儲存至：{OUT_PATH}")


if __name__ == "__main__":
    main()
