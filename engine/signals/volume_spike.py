# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 爆量燈號訊號 — Phase 1
# 來源：app.py _volume_spike_tag + ui/sidebar.py 重複定義


def get_volume_spike_tag(row):
    raise NotImplementedError("待步驟5從 app.py 抽離")
