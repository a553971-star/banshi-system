"""
backtest_b.py — TRUE_B → LAUNCH 成功率回測（Phase 0 驗證版）
注意：使用今日候選清單回測歷史，有生存者偏差，結果僅供方向驗證。
用法：python3 backtest_b.py
"""

import os
import datetime
import pandas as pd
from live_fetcher import merge_all_live
from feature_engine import build_features
from trajectory_engine import compute_trajectory
from main import load_params

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

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

def backtest_one(sid, params, start="2024-01-01"):
    end = datetime.date.today().strftime("%Y-%m-%d")
    try:
        df = merge_all_live(sid, start, end)
        df = build_features(df)
        df = compute_trajectory(df, params)
    except Exception as e:
        print(f"失敗：{e}")
        return []

    if df.empty or len(df) < 15:
        return []

    for col in ["B_quality", "B_window_20", "A_days", "close", "ma20", "volume_ratio"]:
        if col not in df.columns:
            return []

    df = df.reset_index(drop=True)
    results = []

    for i in range(len(df) - 10):
        row = df.iloc[i]
        try:
            bq = int(float(row.get("B_quality") or 0))
            a_days = int(float(row.get("A_days") or 0))
        except Exception:
            continue

        b_phase = calc_b_phase(bq, a_days)
        if b_phase != "MATURE": continue
        if bq < 60: continue
        if a_days != 0: continue

        flow = str(row.get("flow_status") or "")
        if flow == "DISTRIBUTION": continue

        # 用外資連續買超判斷資金是否在推
        try:
            fcb = float(row.get("foreign_consecutive_buy") or 0)
        except Exception:
            fcb = 0
        if fcb <= 0: continue  # 外資沒在連續買，跳過

        future = df.iloc[i+1:i+11]
        success = False
        days_to_launch = None

        for j, (_, f) in enumerate(future.iterrows()):
            try:
                f_a = int(float(f.get("A_days") or 0))
                f_vol = float(f.get("volume_ratio") or 0)
                f_close = float(f.get("close") or 0)
                f_ma20 = float(f.get("ma20") or 0)
            except Exception:
                continue

            if f_a in [1, 2] and f_vol >= 1.2:
                success = True
                days_to_launch = j + 1
                break

            if f_ma20 > 0 and f_close < f_ma20:
                break

        outcome = "WIN" if success else "LOSS"
        results.append({
            "stock_id": sid,
            "T0_date": str(row.get("date", "")),
            "B_quality": bq,
            "B_window_20": int(float(row.get("B_window_20") or 0)),
            "flow": flow,
            "outcome": outcome,
            "days_to_launch": days_to_launch,
        })

    return results

def main():
    params = load_params()

    decisions_path = os.path.join(BASE_PATH, "latest_decisions.csv")
    try:
        df_dec = pd.read_csv(decisions_path, dtype=str)
        stock_list = df_dec["stock_id"].dropna().unique().tolist()
    except Exception:
        print("無法讀取 latest_decisions.csv")
        return

    stock_list = stock_list[:10]  # 小樣本測試，確認邏輯後移除
    print(f"開始回測 {len(stock_list)} 支股票（小樣本測試）...")
    print("注意：有生存者偏差，結果僅供方向驗證。")
    all_results = []

    for i, sid in enumerate(stock_list):
        print(f"  [{i+1}/{len(stock_list)}] {sid}", end=" ", flush=True)
        res = backtest_one(sid, params)
        print(f"→ {len(res)} 筆 T0")
        all_results.extend(res)

    if not all_results:
        print("無有效回測資料（可能這10支都沒有符合 MATURE 條件的歷史時間點）")
        return

    df_result = pd.DataFrame(all_results)
    total = len(df_result)
    wins = (df_result["outcome"] == "WIN").sum()
    win_rate = wins / total if total > 0 else 0

    print(f"\n{'='*40}")
    print(f"總樣本數：{total}")
    print(f"WIN：{wins}　LOSS：{total-wins}")
    print(f"勝率：{win_rate:.1%}")

    launched = df_result[df_result["days_to_launch"].notna()]
    if not launched.empty:
        print(f"\n發動時間（{len(launched)} 筆成功）：")
        print(f"  3天內：{(launched['days_to_launch'] <= 3).mean():.1%}")
        print(f"  5天內：{(launched['days_to_launch'] <= 5).mean():.1%}")
        print(f"  平均：{launched['days_to_launch'].mean():.1f} 天")

    print(f"{'='*40}")

    # 梯度測試：不同 B_quality 門檻的勝率
    print("\nB_quality 門檻梯度測試：")
    print(f"{'門檻':<10} {'樣本數':<10} {'勝率':<10} {'5天發動率'}")
    for threshold in [60, 65, 70, 75, 80]:
        sub = df_result[df_result["B_quality"] >= threshold]
        if len(sub) == 0:
            print(f">={threshold:<8} 0筆")
            continue
        wr = (sub["outcome"] == "WIN").mean()
        launched_sub = sub[sub["days_to_launch"].notna()]
        d5 = (launched_sub["days_to_launch"] <= 5).mean() if len(launched_sub) > 0 else 0
        print(f">={threshold:<8} {len(sub):<10} {wr:.1%}{'':5} {d5:.1%}")

    out_path = os.path.join(BASE_PATH, "backtest_result.csv")
    df_result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n結果存至 {out_path}")

if __name__ == "__main__":
    main()
