import pandas as pd

def score_strong_B(row):
    score = 0
    try:
        B = int(float(row.get("B_days") or 0))
    except:
        B = 0
    flow = row.get("flow_status")
    cost = row.get("cost_level")
    A    = row.get("A_days") or 0
    conf = row.get("confidence") or 0
    if B >= 8:   score += 30
    elif B >= 5: score += 20
    elif B >= 3: score += 10
    if flow == "ACCUMULATING":       score += 30
    elif flow in ["NEUTRAL", None]:  score += 5
    if cost == "SAFE":       score += 15
    elif cost == "HIGH_RISK": score -= 10
    if A == 0:   score += 15
    elif A <= 2: score += 5
    score += min(conf, 10)
    return score

def get_top_strong_B(df, top_n=5):
    df = df.copy()
    df["B_days"] = pd.to_numeric(df["B_days"], errors="coerce").fillna(0)
    df["B_score"] = df.apply(score_strong_B, axis=1)
    df = df[df["B_days"] >= 3]
    return df.sort_values("B_score", ascending=False).head(top_n)