# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 離場警報系統 — Exit Alert
#
# 核心哲學：
# - 過熱（Overextended）本身不是離場訊號
# - 過熱 + DISTRIBUTION 才是真正高風險
# - AI 主升股長期超買仍可持有，等主力出場訊號
# - 損益只輔助，不主導離場決策
#
# Rule precedence:
# 1. DISTRIBUTION + HIGH_RISK → CRITICAL
# 2. DISTRIBUTION + A_days >= 5 → CRITICAL
# 3. DISTRIBUTION + volume spike → CRITICAL
# 4. DISTRIBUTION 單獨 → WARNING
# 5. A_days >= 5 + HIGH_RISK → WARNING
# 6. HIGH_RISK 單獨 → WATCH
# 7. A_days >= 5 單獨 → WATCH
# 8. pnl <= -6 → WATCH（補充）
# 9. 其他 → HOLD

EXIT_CRITICAL = "CRITICAL"
EXIT_WARNING  = "WARNING"
EXIT_WATCH    = "WATCH"
EXIT_HOLD     = "HOLD"

def get_exit_alert(row: dict) -> dict:
    """
    回傳離場警報。
    輸入 row 需包含：
        flow_status, cost_level, A_days, volume_ratio
    pnl_pct 可選，只作補充。
    輸出：{
        "level": str,
        "reasons": list,
        "action": str,
    }
    """
    flow = str(row.get("flow_status", "") or "")
    cost = str(row.get("cost_level", "") or "")

    try:
        a_days = int(float(row.get("A_days", 0) or 0))
    except (ValueError, TypeError):
        a_days = 0

    try:
        vr = float(row.get("volume_ratio", 0) or 0)
    except (ValueError, TypeError):
        vr = 0

    try:
        pnl = float(row.get("pnl_pct", 0) or 0)
    except (ValueError, TypeError):
        pnl = 0

    reasons = []
    level   = EXIT_HOLD

    is_dist          = flow == "DISTRIBUTION"
    is_high_risk     = cost == "HIGH_RISK"
    is_late          = a_days >= 5
    is_volume_spike  = vr > 1.5

    # CRITICAL
    if is_dist and is_high_risk:
        reasons.append("主力出貨 + 成本偏高")
        level = EXIT_CRITICAL
    elif is_dist and is_late:
        reasons.append("主力出貨 + A段過長（末升）")
        level = EXIT_CRITICAL
    elif is_dist and is_volume_spike:
        reasons.append("主力出貨 + 爆量（疑似派發）")
        level = EXIT_CRITICAL

    # WARNING
    elif is_dist:
        reasons.append("主力開始出貨")
        level = EXIT_WARNING
    elif is_late and is_high_risk:
        reasons.append("A段過長 + 成本偏高")
        level = EXIT_WARNING

    # WATCH
    elif is_high_risk:
        reasons.append("成本位置偏高")
        level = EXIT_WATCH
    elif is_late:
        reasons.append(f"A段已走 {a_days} 天，結構偏晚")
        level = EXIT_WATCH

    # pnl 補充（不影響主要 level）
    if pnl <= -6 and level == EXIT_HOLD:
        reasons.append(f"虧損 {pnl:.1f}%，接近停損線")
        level = EXIT_WATCH

    action_map = {
        EXIT_CRITICAL: "🔴 立刻減碼或出場",
        EXIT_WARNING:  "🟠 開始減碼，密切監控",
        EXIT_WATCH:    "🟡 持續觀察，準備應變",
        EXIT_HOLD:     "🟢 繼續持有",
    }

    return {
        "level":   level,
        "reasons": reasons,
        "action":  action_map[level],
    }
