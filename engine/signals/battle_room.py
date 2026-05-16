# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 戰情室訊號判定 — Phase 1
# 來源：app.py _classify_war（war_class，UI層）
# ⚠️ b_phase（main.py/live_analyzer.py）不動


def get_battle_room(row):
    raise NotImplementedError("待步驟4從 app.py 抽離")
