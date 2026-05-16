# 磐石引擎層 — 架構規範

## Engine Layer Rules
- 不可 import Streamlit
- 不可 import app.py / pages
- 僅負責規則與分析
- 盡量保持 pure function
- 不直接讀 CSV / DB

## 命名規範
- feature 層：classify_* / calc_*_features
- signal 層：get_*_signal
- UI reconstruction 層：rebuild_*

## 目錄結構
engine/
  signals/      ← 交易訊號（ATTACK/LAUNCH/PREPARE、爆量燈號）
  ui/           ← UI 層重建邏輯（非底層 feature engine）
  tests/        ← 黃金測試案例
