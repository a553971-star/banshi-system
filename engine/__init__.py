# 磐石引擎模組 — Single Source of Truth
# Phase 1：規則集中化
# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
