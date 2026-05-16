# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# UI 層 B_validity 重建 — Phase 1
# 來源：app.py calc_b_validity_from_row
# ⚠️ 這是 UI 層重建版，不是底層 feature engine
# ⚠️ institutional_engine.py 的版本完全不動


def rebuild_b_validity(row):
    raise NotImplementedError("待步驟3從 app.py 抽離")
