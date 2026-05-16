# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 戰情室訊號判定 — Phase 1
# 來源：app.py _classify_war（1428 行）
# ⚠️ b_phase（main.py/live_analyzer.py）不動
#
# Precedence: DISTRIBUTION > ATTACK > LAUNCH > PREPARE


def get_battle_room(row):
    """
    判定戰情室訊號等級。
    回傳 "ATTACK" / "LAUNCH" / "PREPARE" / None

    優先順序（第一個符合即回傳）：
      1. flow == DISTRIBUTION → None（直接封鎖）
      2. B >= 8, 2 <= A <= 6, C >= 3 → ATTACK
      3. bq >= 45, B >= 8, 1 <= A <= 2 → LAUNCH
      4. bq >= 45, B >= 8, vr >= 0.7 → PREPARE
    """
    try:
        B    = int(row.get("B_days") or 0)
        A    = int(row.get("A_days") or 0)
        C    = int(row.get("C_days") or 0)
        bq   = float(row.get("B_quality") or 0)
        vr   = float(row.get("volume_ratio") or 0)
        flow = str(row.get("flow_status") or "")
    except (ValueError, TypeError):
        return None

    if flow == "DISTRIBUTION":
        return None
    if B >= 8 and 2 <= A <= 6 and C >= 3:
        return "ATTACK"
    if bq >= 45 and B >= 8 and 1 <= A <= 2:
        return "LAUNCH"
    if bq >= 45 and B >= 8 and vr >= 0.7:
        return "PREPARE"
    return None
