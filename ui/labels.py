"""
ui/labels.py — 盤石交易語意標籤
Single Source of Truth for UI display labels.
Engine 保持英文常數，UI 全中文化。
"""

FLOW_LABELS = {
    "ACCUMULATING": "🟢 主力吸籌",
    "DISTRIBUTION": "🔴 主力出貨",
    "NEUTRAL":      "🟠 中性整理",
}

COST_LABELS = {
    "SAFE":      "🟢 安全區",
    "NEUTRAL":   "🟠 中性區",
    "HIGH_RISK": "🔴 高風險區",
}

B_PHASE_LABELS = {
    "PREPARE": "⚪ 準備期",
    "BUILD":   "🔵 建倉期",
    "MATURE":  "🟡 成熟期",
    "LAUNCH":  "🔴 發動期",
    "LATE":    "⚫ 末升段",
}

B_VALIDITY_LABELS = {
    "TRUE_B":    "✅ 真建倉",
    "FAKE_B":    "❌ 假建倉",
    "UNCERTAIN": "❓ 結構模糊",
}

DECISION_LABELS = {
    "BUY":    "🟢 可進場",
    "WAIT":   "🟡 觀察中",
    "IGNORE": "⚪ 無視",
    "SELL":   "🔴 賣出",
}

HEALTH_LABELS = {
    "HEALTHY":                "🟢 健康",
    "SHAKEOUT":               "🟡 洗盤震盪",
    "WEAKENING":              "🟠 轉弱",
    "TREND_RISK":             "🔴 趨勢風險",
    "DISTRIBUTION_BREAKDOWN": "💀 結構崩壞",
}

EXIT_LABELS = {
    "HOLD":     "🟢 繼續持有",
    "WATCH":    "🟡 持續觀察",
    "WARNING":  "🟠 開始減碼",
    "CRITICAL": "🔴 立刻出場",
}

FOREIGN_LEVEL_LABELS = {
    "HEAVY":  "🔴 重倉",
    "MEDIUM": "🟠 中倉",
    "LIGHT":  "🔵 輕倉",
    "NONE":   "— 無持倉",
}

OBV_LABELS = {
    "OBV_ACCUMULATION": "🟢 籌碼累積",
    "OBV_CONFIRM":      "🔵 量價同步",
    "OBV_DIVERGENCE":   "🟠 量價背離",
    "OBV_DISTRIBUTION": "🔴 出貨跡象",
    "OBV_NEUTRAL":      "⚪ 中性",
}

def get_label(mapping: dict, key: str, fallback: str = "—") -> str:
    """
    通用取標籤函式。
    - key 為 None 或空字串時回傳 fallback
    - key 不在 mapping 時回傳原始 key（方便 debug 新 signal）
    """
    if key is None or key == "":
        return fallback
    return mapping.get(str(key), str(key))
