# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 持倉健康度評分 — Position Health Score
#
# 核心哲學：
# - 結構優先、主力優先、量價優先
# - 均線看節奏，不看過熱
# - 損益不主導決策
# - 過熱（Overextended）+ DISTRIBUTION 才是真正高風險
# - AI 主升股可能長期超買仍持續噴發，不因漲多就減分
#
# Rule precedence：
# 1. flow_status（主力方向，最高權重）
# 2. cost_level（成本位置）
# 3. B_phase（生命週期）
# 4. A_days（加速段位置）
# 5. volume_ratio / B_quality（量能與建倉強度）
# pnl_pct：只作提醒，完全不影響核心分數

HEALTH_HEALTHY                = "HEALTHY"
HEALTH_SHAKEOUT               = "SHAKEOUT"
HEALTH_WEAKENING              = "WEAKENING"
HEALTH_TREND_RISK             = "TREND_RISK"
HEALTH_DISTRIBUTION_BREAKDOWN = "DISTRIBUTION_BREAKDOWN"

ALL_HEALTH_STATES = [
    HEALTH_HEALTHY,
    HEALTH_SHAKEOUT,
    HEALTH_WEAKENING,
    HEALTH_TREND_RISK,
    HEALTH_DISTRIBUTION_BREAKDOWN,
]

def calc_health_score(row: dict) -> int:
    """
    計算持倉健康分數（0-100）。
    輸入 row 需包含：
        flow_status, cost_level, B_phase, A_days,
        volume_ratio, B_quality
    pnl_pct 可選，不影響分數。
    """
    score = 55

    flow  = str(row.get("flow_status", "") or "")
    cost  = str(row.get("cost_level", "") or "")
    phase = str(row.get("B_phase", "") or "")
    vr    = float(row.get("volume_ratio", 0) or 0)

    try:
        a_days = int(float(row.get("A_days", 0) or 0))
    except (ValueError, TypeError):
        a_days = 0

    try:
        b_quality = int(float(row.get("B_quality", 0) or 0))
    except (ValueError, TypeError):
        b_quality = 0

    # === flow_status（最高權重）===
    # 注意：過熱不扣分，只有 DISTRIBUTION 才扣
    if flow == "ACCUMULATING":
        score += 25
    elif flow == "DISTRIBUTION":
        score -= 30  # 主力在出貨，最危險
                     # 單獨 DISTRIBUTION：50-30=20 → TREND_RISK

    # === cost_level ===
    if cost == "SAFE":
        score += 15
    elif cost == "HIGH_RISK":
        score -= 20

    # === B_phase 生命週期 ===
    if phase == "MATURE":
        score += 15
    elif phase == "LAUNCH":
        score += 10
    elif phase == "BUILD":
        score += 5
    elif phase == "LATE":
        score -= 15  # 末升段

    # === A_days 加速段位置 ===
    if 1 <= a_days <= 2:
        score += 20
    elif 3 <= a_days <= 4:
        score += 5
    elif a_days >= 5:
        score -= 20

    # === volume_ratio ===
    if vr >= 1.5:
        score += 10
    elif vr < 0.5:
        score -= 5

    # === B_quality ===
    if b_quality >= 70:
        score += 10
    elif b_quality >= 40:
        score += 5

    return max(0, min(100, score))


def classify_health_state(score: int) -> str:
    """
    Rule precedence: 分數由高到低判斷。
    70+ → HEALTHY
    50-69 → SHAKEOUT
    30-49 → WEAKENING
    10-29 → TREND_RISK
    <10   → DISTRIBUTION_BREAKDOWN
    """
    if score >= 70:
        return HEALTH_HEALTHY
    if score >= 50:
        return HEALTH_SHAKEOUT
    if score >= 30:
        return HEALTH_WEAKENING
    if score >= 10:
        return HEALTH_TREND_RISK
    return HEALTH_DISTRIBUTION_BREAKDOWN


def get_position_health(row: dict) -> dict:
    """
    主入口：回傳完整持倉健康分析。
    輸出：{
        "score": int,
        "state": str,
        "pnl_note": str,
    }
    """
    score = calc_health_score(row)
    state = classify_health_state(score)

    pnl_note = ""
    try:
        pnl = float(row.get("pnl_pct", 0) or 0)
        if pnl <= -6:
            pnl_note = f"⚠️ 虧損 {pnl:.1f}%，注意停損"
        elif pnl >= 20:
            pnl_note = f"💰 獲利 {pnl:.1f}%，可考慮部分減碼"
        elif pnl >= 10:
            pnl_note = f"📈 獲利 {pnl:.1f}%，持續觀察"
    except (ValueError, TypeError):
        pass

    return {
        "score": score,
        "state": state,
        "pnl_note": pnl_note,
    }
