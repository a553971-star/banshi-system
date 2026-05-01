import pandas as pd

def calc_foreign_cost_pro(df, window=60):
    df = df.copy().tail(window).reset_index(drop=True)
    if "foreign_buy" not in df.columns or "foreign_sell" not in df.columns:
        return None, None, None
    df["foreign_net"] = pd.to_numeric(df["foreign_buy"], errors="coerce").fillna(0) -                         pd.to_numeric(df["foreign_sell"], errors="coerce").fillna(0)
    position = 0.0
    total_cost = 0.0
    for _, row in df.iterrows():
        net = row["foreign_net"]
        price = row["close"]
        if net > 0:
            total_cost += price * net
            position += net
        elif net < 0 and position > 0:
            sell = min(abs(net), position)
            avg_cost = total_cost / position
            total_cost -= avg_cost * sell
            position -= sell
    if position <= 0:
        return None, None, None
    avg_cost = total_cost / position
    current_price = df.iloc[-1]["close"]
    profit_pct = (current_price - avg_cost) / avg_cost * 100
    return round(avg_cost, 2), int(position), round(profit_pct, 2)

def classify_institutional_state(decision, cost, profit_pct):
    if cost is None or profit_pct is None:
        return "UNKNOWN"
    B    = decision.get("B_days") or 0
    A    = decision.get("A_days") or 0
    flow = decision.get("flow_status")
    if profit_pct > 10 and B == 0:
        return "DISTRIBUTION"
    if B >= 2 and flow == "ACCUMULATING" and profit_pct < 5:
        return "ACCUMULATION"
    if B >= 1 and A == 0 and profit_pct > 3:
        return "SHAKEOUT"
    if A >= 3 and profit_pct > 8:
        return "EXTENDED"
    return "NEUTRAL"

def interpret_institutional_state(state, profit_pct):
    if state == "ACCUMULATION":
        return "主力建倉中（低風險區）"
    elif state == "SHAKEOUT":
        return "洗盤中（震盪吸籌）"
    elif state == "DISTRIBUTION":
        return "主力出貨（高風險）"
    elif state == "EXTENDED":
        return f"主力已獲利 {profit_pct:.1f}%（延伸段，注意回檔）"
    elif profit_pct is not None:
        if profit_pct > 12:
            return f"主力高度獲利 +{profit_pct:.1f}% → 出貨風險高"
        elif profit_pct > 5:
            return f"主力獲利中 +{profit_pct:.1f}% → 健康區"
        elif abs(profit_pct) <= 3:
            return f"主力成本區（±{profit_pct:.1f}%）→ 關鍵支撐"
        else:
            return f"主力被套 {profit_pct:.1f}% → 潛在支撐但弱"
    return "主力行為不明確"


def calc_b_validity(df, b_quality, window=20):
    """判斷 B 段是真建倉還是假整理"""
    if df is None or len(df) < 5:
        return "UNCERTAIN"

    recent = df.tail(window).copy()

    # 外資連續賣出天數
    foreign_sell_consecutive = 0
    if "foreign_net" in recent.columns:
        for net in reversed(pd.to_numeric(recent["foreign_net"], errors="coerce").fillna(0).tolist()):
            if net < 0:
                foreign_sell_consecutive += 1
            else:
                break

    # 價格相對 MA60 位置
    price_bias = "OK"
    if "close" in df.columns and "ma60" in df.columns:
        last = df.iloc[-1]
        try:
            close = float(last["close"])
            ma60  = float(last["ma60"])
            if close < ma60 * 0.97:
                price_bias = "WEAK"
        except (TypeError, ValueError):
            pass

    # 真假判斷
    if b_quality >= 60 and foreign_sell_consecutive < 3 and price_bias != "WEAK":
        return "TRUE_B"
    elif b_quality < 50 and foreign_sell_consecutive >= 5:
        return "FAKE_B"
    else:
        return "UNCERTAIN"