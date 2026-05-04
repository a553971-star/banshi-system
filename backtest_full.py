"""
backtest_full.py — TRUE_B A/B 對照回測
A組：companies.csv 白名單
B組：隨機抽樣 x3
比較：勝率 / 避雷能力 / 訊號密度
用法：python3 backtest_full.py
"""

import os
import sys
import random
import datetime
import requests
import pandas as pd
from live_fetcher import merge_all_live
from feature_engine import build_features
from trajectory_engine import compute_trajectory
from good_company import load_company_list
from main import load_params

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
TOKEN = os.getenv("FINMIND_TOKEN")
RANDOM_SEED = 42

def get_all_stock_ids():
    try:
        r = requests.get(
            "https://api.finmindtrade.com/api/v4/data",
            params={"dataset": "TaiwanStockInfo", "token": TOKEN},
            timeout=30
        )
        data = r.json()
        if data.get("status") != 200:
            return []
        df = pd.DataFrame(data["data"])
        df = df[df["stock_id"].str.match(r"^\d{4}$")]
        return df["stock_id"].unique().tolist()
    except Exception as e:
        print(f"抓股票清單失敗：{e}")
        return []

def calc_b_phase(b_quality, a_days):
    try:
        b_quality = int(float(b_quality or 0))
        a_days = int(float(a_days or 0))
    except Exception:
        return "PREPARE"
    if a_days >= 5: return "LATE"
    elif b_quality >= 70 and 1 <= a_days <= 2: return "LAUNCH"
    elif b_quality >= 70 and a_days == 0: return "MATURE"
    elif b_quality >= 40: return "BUILD"
    else: return "PREPARE"

def backtest_one(sid, params, start="2023-01-01"):
    end = datetime.date.today().strftime("%Y-%m-%d")
    try:
        df = merge_all_live(sid, start, end)
        if df is None or len(df) < 20:
            return []
        df = build_features(df)
        df = compute_trajectory(df, params)
    except Exception:
        return []

    required = ["B_quality", "B_window_20", "A_days", "close",
                "ma20", "volume_ratio", "foreign_consecutive_buy"]
    for col in required:
        if col not in df.columns:
            return []

    df = df.reset_index(drop=True)
    results = []

    for i in range(len(df) - 30):  # 預留30天觀察期
        row = df.iloc[i]

        try:
            bq = int(float(row.get("B_quality") or 0))
            a_days = int(float(row.get("A_days") or 0))
            fcb = float(row.get("foreign_consecutive_buy") or 0)
        except Exception:
            continue

        if calc_b_phase(bq, a_days) != "MATURE": continue
        if bq < 75: continue
        if a_days != 0: continue
        if fcb < 2: continue

        # Risk Filter（避雷層）
        try:
            close = float(row.get("close") or 0)
            ma20 = float(row.get("ma20") or 0)
            vol_ratio = float(row.get("volume_ratio") or 0)
        except Exception:
            continue

        if ma20 > 0 and close < ma20: continue        # 不能在均線下
        if vol_ratio < 0.5: continue                   # 完全沒量才過濾
        if vol_ratio > 2.5: continue   # 太爆量
        try:
            ma60 = float(row.get("ma60") or 0)
        except Exception:
            ma60 = 0
        if ma60 > 0 and close > 0 and (close / ma60) >= 1.15: continue  # 位置太高

        # 7天後實際報酬
        future_7 = df.iloc[i+1:i+8]
        if len(future_7) < 7:
            continue

        try:
            t0_close = float(row.get("close") or 0)
            t7_close = float(future_7.iloc[-1].get("close") or 0)
        except Exception:
            continue

        if t0_close <= 0 or t7_close <= 0:
            continue

        ret_7d = (t7_close - t0_close) / t0_close
        success = ret_7d > 0
        days_to_launch = None

        # 30天跌破MA20（對照用）
        future_30 = df.iloc[i+1:i+31]
        broke_ma20 = False
        for _, f in future_30.iterrows():
            try:
                f_close = float(f.get("close") or 0)
                f_ma20 = float(f.get("ma20") or 0)
                if f_ma20 > 0 and f_close < f_ma20 * 0.97:
                    broke_ma20 = True
                    break
            except Exception:
                continue

        results.append({
            "stock_id": sid,
            "T0_date": str(row.get("date", "")),
            "B_quality": bq,
            "B_window_20": int(float(row.get("B_window_20") or 0)),
            "foreign_consecutive_buy": fcb,
            "outcome": "WIN" if success else "LOSS",
            "ret_7d": round(ret_7d * 100, 2),
            "days_to_launch": days_to_launch,
            "broke_ma20_30d": broke_ma20,
        })

    return results

def run_group(name, stock_list, params):
    print(f"\n{'='*50}")
    print(f"跑 {name}（{len(stock_list)} 支）...")
    all_results = []
    for i, sid in enumerate(stock_list):
        print(f"  [{i+1}/{len(stock_list)}] {sid}", end=" ", flush=True)
        res = backtest_one(sid, params)
        print(f"→ {len(res)} 筆")
        all_results.extend(res)
    return all_results

def print_group_summary(name, results):
    if not results:
        print(f"\n{name}：無有效樣本")
        return
    df = pd.DataFrame(results)
    total = len(df)
    wins = (df["outcome"] == "WIN").sum()
    win_rate = wins / total
    avg_ret = df["ret_7d"].mean()
    win_avg = df[df["outcome"] == "WIN"]["ret_7d"].mean()
    loss_avg = df[df["outcome"] == "LOSS"]["ret_7d"].mean()

    print(f"\n【{name}】")
    print(f"  T0 樣本數：{total}")
    print(f"  勝率（7天後收盤上漲）：{win_rate:.1%}")
    print(f"  平均報酬：{avg_ret:.2f}%")
    print(f"  贏的平均：{win_avg:.2f}%　輸的平均：{loss_avg:.2f}%")

def main():
    params = load_params()

    # A組：白名單
    co_dict = load_company_list(os.path.join(BASE_PATH, "companies.csv"))
    a_list = list(co_dict.keys())
    print(f"A組（白名單）：{len(a_list)} 支")

    # 抓全市場
    print("抓全市場股票清單...")
    all_ids = get_all_stock_ids()
    non_whitelist = [s for s in all_ids if s not in co_dict]
    print(f"全市場（排除白名單後）：{len(non_whitelist)} 支")

    # B組：隨機 x3
    random.seed(RANDOM_SEED)
    n = len(a_list)
    b1 = random.sample(non_whitelist, min(n, len(non_whitelist)))
    b2 = []
    b3 = []

    # 跑四組
    a_results  = run_group("A組（白名單）", a_list, params)
    b1_results = run_group("B1組（隨機）", b1, params)
    b2_results = []
    b3_results = []

    # 輸出比較
    print(f"\n{'='*50}")
    print("【A/B 對照結果】")
    print_group_summary("A組（白名單）", a_results)
    print_group_summary("B1組（隨機）", b1_results)
    print_group_summary("B2組（隨機）", b2_results)
    print_group_summary("B3組（隨機）", b3_results)

    # 訊號密度
    print(f"\n【訊號密度（T0數/股票數）】")
    print(f"  A組：{len(a_results)}/{len(a_list)} = {len(a_results)/len(a_list):.1f} 個T0/支")
    for name, res, lst in [("B1", b1_results, b1), ("B2", b2_results, b2), ("B3", b3_results, b3)]:
        if lst:
            print(f"  {name}組：{len(res)}/{len(lst)} = {len(res)/len(lst):.1f} 個T0/支")

    # 儲存
    all_data = []
    for label, res in [("A", a_results), ("B1", b1_results),
                       ("B2", b2_results), ("B3", b3_results)]:
        for r in res:
            r["group"] = label
            all_data.append(r)

    if all_data:
        out = os.path.join(BASE_PATH, "backtest_full_result.csv")
        pd.DataFrame(all_data).to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n結果存至 {out}")

if __name__ == "__main__":
    main()
