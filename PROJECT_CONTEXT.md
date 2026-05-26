# 磐石決策系統 — 專案座標系

> 這份文件是「磐石決策系統」的核心知識文件。
> 用途：任何人（包含未來的 AI 助手、未來的你自己）讀完這份，能在 5 分鐘內進入狀況。
> 維護原則：架構或重要決策變動時手動更新，commit 訊息標 `docs: update context`。

---

## 1. 系統定位

**磐石決策系統**是一個**台股量化決策平台**，由阿立（hsc）自主開發，使用 Claude / Claude Code 協作。

**核心理念**：透過「機構籌碼累積」識別中長線買點，不做日內、不追熱題。
**取名由來**：「磐石」象徵穩固——買在機構默默累積的「磐石位置」。

**不做什麼**：

- ❌ 不做技術指標派（MA、KD、MACD 不是主訊號）
- ❌ 不做新聞題材派
- ❌ 不做極短線當沖
- ❌ 不做選擇權、期貨

**做什麼**：

- ✅ CBA 三層籌碼分析（見第 3 節）
- ✅ 中長線（持有週 ~ 月為單位）
- ✅ 機構行為解讀（三大法人、融資融券、外資持股）
- ✅ 估值參考（個股估值層 v4.0，等待驗收合併）

---

## 2. 系統架構（雙環境並行）

### 本機（開發環境）

- **角色**：開發、實驗、緊急補洞
- **位置**：`/Users/hsc/Documents/banshi_system/`
- **Python 版本**：**3.9.6**（重要：寫程式碼時不能用 Python 3.10+ 的新語法，例如 `str | None`，要用 `Optional[str]`）
- **資料庫**：本機 `banshi.db`（獨立、不必跟雲端同步）
- **介面**：本機 Streamlit `streamlit run app.py`

### 雲端（生產環境）

- **角色**：每日自動化、跨平台統一資料源
- **GitHub repo**：[github.com/a553971-star/banshi-system](https://github.com/a553971-star/banshi-system)
- **執行**：GitHub Actions（`.github/workflows/daily_update.yml`，每天 18:30 台灣時間）
- **資料庫**：GitHub 倉庫裡的 `banshi.db`（由 `banshi-bot` 每日 commit 維護）
- **介面**：Streamlit Cloud（多平台共用）

### 同步原則

**本機跟雲端的 `banshi.db` 各自獨立、互不同步**。

理由：

- 本機跑緊急任務（如 backfill）寫入本機 db
- 雲端 GitHub Actions 跑 daily update 寫入雲端 db
- 若強制同步會造成 git 衝突（banshi.db 是二進位檔，git 處理會踩雷）
- **看盤主要看 Streamlit Cloud**——本機 db 只是備援

---

## 3. CBA 三層籌碼分析（核心邏輯）

**C / B / A 是三個遞進的訊號層**，描述機構在不同階段的行為：

| 層級 | 名稱 | 含義 |
|---|---|---|
| **C** | 累積（Accumulation） | 機構低調進場，價量未明顯異動 |
| **B** | 突破（Breakout） | 機構加速買進，量能放大 |
| **A** | 加碼（Acceleration） | 機構加大買進，趨勢確立 |

**訊號優先級**：A > B > C

**輸入資料**：

- TWSE / TPEx 每日股價（OHLCV）
- 三大法人買賣超（外資、投信、自營商）
- 融資融券餘額
- 外資持股比例（週一/週六更新）

**輸出**：每日決策清單（`latest_decisions.csv`），標示每檔股票的 CBA 狀態 + 信號分數。

---

## 4. 股票池（Universe）

- **規模**：約 600~700 檔（每日由 `build_universe.py` 篩選）
- **來源**：上市（TWSE）+ 上櫃（TPEx）
- **篩選邏輯**：基本面 + 流動性 + 籌碼可分析性
- **特殊池**：`pages/5_AI戰情室` 涵蓋約 198 檔 AI 概念股（半導體、PCB、伺服器、散熱等）

---

## 5. 主要檔案地圖

### 資料更新（核心管線）

| 檔案 | 用途 |
|---|---|
| `update_daily_data.py` | 每日資料更新主程式（GitHub Actions 跑） |
| `backfill_db.py` | 補洞工具（補缺漏日期的歷史資料） |
| `init_db_twse.py` | 資料庫初始化（首次建表 + 灌入歷史資料） |
| `download_stock_names.py` | 抓取股票中文名稱對照表 |

### 決策引擎

| 檔案 | 用途 |
|---|---|
| `main.py` | 每日決策主流程 |
| `b_ranker.py` | B 層級評分 |
| `flow_engine.py` | 資金流引擎 |
| `trajectory_engine.py` | 軌跡分析（CBA 狀態判定） |
| `institutional_engine.py` | 法人籌碼分析 |
| `feature_engine.py` | 特徵工程 |

### Streamlit 介面

| 檔案 | 用途 |
|---|---|
| `app.py` | 主入口頁面 |
| `pages/5_*` | AI 戰情室（最常用） |
| `pages/7_*` | 本益比觀察 |
| `pages/8_*` | 持倉戰情室 |
| `pages/9_*` | 筆記 |
| `pages/10_*` | 族群輪動偵測 |

### 自動化

| 檔案 | 用途 |
|---|---|
| `.github/workflows/daily_update.yml` | GitHub Actions 排程：每天 18:30 跑資料更新 + 決策 |
| `run_daily.sh` | 本機跑用的 shell（現已少用） |

### 資料庫

| 檔案 | 用途 |
|---|---|
| `banshi.db` | SQLite 主資料庫 |
| `bible.json` | 聖經內容（每日金句用） |
| `pinned.json`、`notes.json`、`notes_book.json`、`portfolio.json` | 持久化使用者狀態（透過 GitHub API 同步） |

### `banshi.db` 主要表

| 表 | 內容 |
|---|---|
| `price_history` | 股價歷史（PK: stock_id + date） |
| `institutional_history` | 三大法人 |
| `margin_history` | 融資融券 |
| `shareholding_history` | 外資持股（週一/週六更新） |
| `stock_names` | 股票中文名對照 |
| `stock_universe` | 當期股票池 |
| `eps_history` | 季度 EPS（個股估值層 v4.0 用，待合併） |
| `daily_data` | 每日決策結果 |

---

## 6. 已知問題與環境限制

### TWSE 抓取對台灣家用 IP 不穩

- **症狀**：本機跑 `update_daily_data.py` 或 `backfill_db.py`，TWSE 經常 `Read timed out`
- **對策**：盡量靠 GitHub Actions（美國 IP）跑；本機只用於緊急補洞
- **不要修**：不是程式 bug，是 TWSE 的限流

### Python 版本

- **本機 Python 3.9.6**——不能寫 `str | None` 這種 3.10+ 才支援的語法
- **GitHub Actions 環境**——通常是 Python 3.11+
- **寫程式時請用 `Optional[str]` 形式**，雙向相容

### SQLite + Git 的雷

- `banshi.db` 是二進位檔，git 對它的處理常出意外
- `git restore banshi.db` 會丟掉未 commit 的寫入（**踩過坑**）
- 跑完 `update_daily_data.py` 後 `git status` 會顯示 `modified: banshi.db`，這是正常的（檔案 metadata 變更，但實質內容可能沒差）

### 沉默失敗（已修復，2026-05-26）

- 過去 `update_daily_data.py` 寫入時忘了帶 `date` 欄位，導致 `MAX(date)` 凍結
- 已修：所有寫入後驗證 `MAX(date)` 是否前進，若沒前進 `sys.exit(1)` 讓 Actions 變紅
- **以後不會再發生沉默失敗**

### shareholding_history 未實作

- 目前 `shareholding_history` 表是空的
- 原因：FinMind 整合還沒做
- **不影響主流程**，CBA 不靠這張表（只是輔助資訊）

---

## 7. 開發守則（給未來的我跟 AI 助手）

### 動程式碼前

1. 先 `grep -n` 找線索，**不要無腦讀整個檔案**
2. 改 `banshi.db` 前一定先 `cp banshi.db banshi.db.backup-$(date +%Y-%m-%d)`
3. 改完先本機跑驗證，再 commit、push
4. **不確定的設計選擇先停下來問**，不要擅自決定

### 動 git 前

1. `git status` 看狀態再動手
2. 改動範圍要明確，**不要 commit 整批不相關的東西**
3. push 之前 `git log --oneline -5` 確認沒推錯
4. **不要建新分支**——所有改動直接到 main

### 處理資料

1. **不要 commit banshi.db**——讓它各自演化
2. backup 檔案（`*.backup-*`）已被 .gitignore 排除
3. 大量歷史補洞用 `backfill_db.py`，不要重新 init

### 跟 TWSE 互動

1. **本機抓取常超時是正常的**——不要當成 bug 修
2. 緊急補洞失敗時，**push 後讓 GitHub Actions 接力**（美國 IP 通常順）
3. 不要短時間連續呼叫（會被 ban）

---

## 8. 重大歷史事件

### 2026-04 起

從 FinMind 批次 API 遷移到 TWSE（CSV）+ TPEx（JSON）為主資料源，FinMind 使用量從 1600+/run 降至 ~100~200/run。

### 2026-05-23

開發「個股估值層 v4.0」（commit `ac9fb1f`），三層估值架構（D 區間 / B 合理價 / C 技術延伸）。**已開發、未合併到 main**——還在 `claude/market-cap-layer-v3.2-mkUYh` 分支等待驗收。

### 2026-05-26（重大修復日）

- 發現 `update_daily_data.py` 有「沉默失敗」bug：寫入時 date 欄位 NULL，`MAX(date)` 從 5/15 凍結到 5/25
- 修復：cherry-pick 三個 fix commit 到 main
- 新增 `backfill_db.py` 補洞工具
- 加入 `sys.exit(1)` 健康檢查，沉默失敗永遠不會再發生
- 安全治理：撤銷外洩的 GitHub token、設定 Mac Keychain 存新 token、git remote URL 去除嵌入式 token

---

## 9. 系統健康度檢查（每天看一次）

打開 banshi_system 資料夾，跑：

```bash
sqlite3 banshi.db "SELECT MAX(date) FROM price_history;"
```

預期結果：**「前一個營業日」或更新**（例如今天是週二，預期看到上週五或週一的日期）

若停在更早的日期 → 自動更新可能斷了，去看 GitHub Actions：
[github.com/a553971-star/banshi-system/actions](https://github.com/a553971-star/banshi-system/actions)

---

## 10. 重要連結

- **GitHub 倉庫**：<https://github.com/a553971-star/banshi-system>
- **GitHub Actions**：<https://github.com/a553971-star/banshi-system/actions>
- **手動觸發每日更新**：<https://github.com/a553971-star/banshi-system/actions/workflows/daily_update.yml>
- **Streamlit Cloud**：（待補）
- **Goodinfo（看盤）**：<https://goodinfo.tw>

---

*本文件最後更新：2026-05-26（重大修復日）*
