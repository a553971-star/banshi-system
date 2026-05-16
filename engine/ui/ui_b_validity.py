# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# UI 層 B_validity 重建 — Phase 1
# 來源：app.py calc_b_validity_from_row（1178 行）
# ⚠️ 這是 UI 層重建版，不是底層 feature engine
# ⚠️ institutional_engine.py 的版本完全不動


def rebuild_b_validity(row):
    """
    根據已計算好的 row 欄位重建 B_validity。
    UI 層使用，輸入為 decision row dict。
    """
    try:
        b_quality = int(float(row.get("B_quality") or 0))
    except (ValueError, TypeError):
        b_quality = 0
    flow = str(row.get("flow_status") or "")
    if b_quality >= 75 and flow != "DISTRIBUTION":
        return "TRUE_B"
    elif b_quality < 60 and flow == "DISTRIBUTION":
        return "FAKE_B"
    else:
        return "UNCERTAIN"
