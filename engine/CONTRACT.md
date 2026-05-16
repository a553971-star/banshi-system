# Engine Contract — 盤石引擎層約束

## 所有 engine function 必須遵守

1. Pure function
   - 相同輸入永遠回傳相同輸出
   - 不依賴外部狀態

2. 不可依賴 Streamlit
   - 不可 import streamlit
   - 不可使用 st.session_state / st.secrets

3. 不可依賴 app.py / pages/
   - engine 只能被 import，不能反向 import UI 層

4. 不直接讀 CSV / DB
   - 資料由呼叫方準備好後傳入

5. 不可修改 input
   - 不修改傳入的 row dict

6. 必須可測試
   - 每個 engine module 必須有對應 golden tests

## 回傳值規範（Phase 2 執行）
- engine 回傳語意值（TRUE_B / FAKE_B / ATTACK / VOLUME_ATTACK）
- UI 層負責翻譯成顯示文字或 emoji
- ⚠️ 現階段暫時保留中文回傳值，Phase 2 統一處理

## Signal Layer 規範
- engine signal 必須 machine-readable
- engine 不回傳 UI 顯示字串、emoji、或中文
- presentation layer 負責翻譯 signal 為顯示文字
- signal constant 化：使用模組內定義的常數，不硬寫字串

## 違反 Contract 的警示
以下情況表示 engine 邊界被污染：
- engine/ 出現 import streamlit
- engine/ 出現 from app import
- engine function 內部讀取檔案或 DB
- engine function 修改傳入的 row
