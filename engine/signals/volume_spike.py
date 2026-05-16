# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 爆量訊號判定 — Phase 1
# 來源：app.py _volume_spike_tag（第 401 行）
# ⚠️ ui/sidebar.py 簡化版廢棄，以本版為準
#
# Rule precedence:
# 1. 未達爆量門檻（vr <= 1.5 或 abs(dr) <= 3）→ None
# 2. 達門檻 + DISTRIBUTION 或 HIGH_RISK → VOLUME_DISTRIBUTION
# 3. 達門檻 + 其他 → VOLUME_ATTACK

# Signal constants（避免 typo）
VOLUME_ATTACK       = "VOLUME_ATTACK"
VOLUME_DISTRIBUTION = "VOLUME_DISTRIBUTION"

# Presentation mapping（Phase 2 後移至 ui/labels.py）
VOLUME_SIGNAL_LABELS = {
    VOLUME_ATTACK:       "🟢 放量攻擊",
    VOLUME_DISTRIBUTION: "🔴 放量出貨",
}


def get_volume_spike_tag(row):
    """
    判定爆量訊號。
    回傳 VOLUME_ATTACK / VOLUME_DISTRIBUTION / None

    門檻：vr > 1.5 AND abs(daily_return_pct) > 3
    """
    try:
        vr   = float(row.get("volume_ratio", 0) or 0)
        dr   = float(row.get("daily_return_pct", 0) or 0)
        flow = str(row.get("flow_status", "") or "")
        cost = str(row.get("cost_level", "") or "")
    except (ValueError, TypeError):
        return None
    if vr <= 1.5 or abs(dr) <= 3:
        return None
    if flow == "DISTRIBUTION" or cost == "HIGH_RISK":
        return VOLUME_DISTRIBUTION
    return VOLUME_ATTACK
