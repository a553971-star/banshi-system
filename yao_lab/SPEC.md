# 獵妖實驗室 · 系統規格書(Yao Lab SPEC v2.0)
**建立日期**: 2026-05-27
**作者**: 阿立 + Claude(規劃對話)
**版本**: v2.0(放寬訊號版,從日電貿實戰案例反推)
**狀態**: 已定稿,待 Claude Code 實作
**目標上線**: 2 天內 MVP 上線

---

## 一、北極星(One-Sentence Mission)
> **「主動掃出『事件驅動 或 爆量上漲 或 族群共振』的 AI 標的,搭配營收成長背書、未處於下跌趨勢,在媒體大幅曝光前進場,賺 2-5 天的新聞動能,用規則化出場避免感覺判斷。」**

**這份規格書的任何決定,如果偏離這句話,以這句話為準。**

---

## 二、系統定位
| 維度 | 設定 |
|---|---|
| 性質 | **策略驗證實驗,非實盤交易** |
| 開發時程 | **2 天 MVP 上線**,邊跑邊調 |
| 驗證期 | 6-10 月跑紙上交易,9 月初新生兒到來前必須穩定 |
| 對照組 | 0050、00891(中信關鍵半導體)、磐石 A 級訊號 |
| 與磐石關係 | **完全獨立邏輯,共用 repo,獨立資料庫** |
| 開發節奏 | 8 月底前必須完工,9 月後不再改架構 |

---

## 三、Universe(選股池)
### 主要 Universe:磐石 AI War Room 198 檔
- 來源:`banshi.db` 既有資料
- 用途:獵妖每日掃描範圍
- 不再另建龍頭/二軍名單(MVP 階段先簡化)

---

## 四、進場訊號(放寬版)
### 觸發邏輯
```
進場 = (訊號A OR 訊號B OR 訊號C) AND 基本面背書 AND 未處於下跌
```

### 訊號 A:MOPS 重大公告
- 近 3 個交易日內,該股有重大訊息公告
- 涵蓋類別:法人說明會、重大訂單、營運展望、合作公告等
- 資料來源:公開資訊觀測站(MOPS)
- **MVP 階段:可先用 FinMind API 簡化抓取**

### 訊號 B:爆量上漲
- 當日漲幅 ≥ 7%
- 當日成交量 ≥ 5 日均量 × 2
- 邏輯:市場已注意到 = 媒體效應的代理指標

### 訊號 C:族群共振
- 同產業(以磐石的族群分類為準)≥ 3 檔同日漲幅 ≥ 5%
- 邏輯:族群題材爆發

### 基本面門檻(必要)
- **月營收連續成長 ≥ 3 個月**(MoM > 0 連續 3 個月)
- 資料來源:FinMind 或 MOPS

### 風險過濾(必要)
- **近 20 日漲跌幅 ≥ 0%**(不處於下跌趨勢)
- **不**設漲幅上限(妖股本質就會漲很多)

---

## 五、紙上交易規則
### 進場
- 訊號觸發隔日,以**開盤價**模擬買入
- 每筆固定假設投入 100 萬(只算報酬率)
- 計算手續費 0.1425% × 2 + 證交稅 0.3%

### 出場規則(4 條,任一觸發)
| 規則 | 條件 | 出場動作 |
|---|---|---|
| 停利 | 獲利 ≥ 25% | 全部出清 |
| 停損 | 收盤跌破進場價 -7% | 全部出清 |
| 時間停損 | 持有滿 5 個交易日 | 全部出清 |
| 警戒 | 被列為處置股 | 出 50% |

**MVP 階段先用簡單規則,跑一段時間後再優化。**

---

## 六、技術架構

### 檔案結構(放在磐石 repo 內)
```
banshi-push/                     ← 既有磐石專案
├── (磐石原有檔案保持不動)
│
└── yao_lab/                      ← 獵妖實驗室(新增)
    ├── SPEC.md                   ← 本文件
    ├── README.md
    │
    ├── signals/
    │   ├── __init__.py
    │   ├── event_scanner.py      ← 訊號 A:MOPS 公告
    │   ├── volume_surge.py       ← 訊號 B:爆量上漲
    │   ├── sector_resonance.py   ← 訊號 C:族群共振
    │   ├── revenue_check.py      ← 基本面:營收成長
    │   └── trend_filter.py       ← 風險過濾:近 20 日漲跌
    │
    ├── engine/
    │   ├── paper_trade.py        ← 紙上交易執行
    │   ├── exit_logic.py         ← 出場規則
    │   └── alert_filter.py       ← 處置股警戒
    │
    ├── analytics/
    │   ├── performance.py        ← 績效計算
    │   └── benchmark.py          ← 對照組比較
    │
    ├── data/
    │   └── yao_lab.db            ← 獨立 SQLite
    │
    ├── reports/
    │   └── YYYY-MM-DD.md         ← 每日報告
    │
    └── daily_scan.py              ← 主入口
```

### 資料庫設計(yao_lab.db)
```sql
-- 每日候選名單
CREATE TABLE candidates (
    date TEXT,
    stock_id TEXT,
    stock_name TEXT,
    signal_a INTEGER,        -- 0/1
    signal_b INTEGER,
    signal_c INTEGER,
    revenue_pass INTEGER,
    trend_pass INTEGER,
    final_pass INTEGER,      -- 是否通過所有條件
    PRIMARY KEY (date, stock_id)
);

-- 紙上交易紀錄
CREATE TABLE paper_trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id TEXT,
    stock_name TEXT,
    entry_date TEXT,
    entry_price REAL,
    exit_date TEXT,
    exit_price REAL,
    exit_reason TEXT,
    pnl_pct REAL,
    holding_days INTEGER,
    signal_at_entry TEXT     -- JSON: 進場時哪個訊號觸發
);

-- 每月績效
CREATE TABLE monthly_performance (
    month TEXT PRIMARY KEY,
    total_trades INTEGER,
    win_rate REAL,
    avg_win_pct REAL,
    avg_loss_pct REAL,
    profit_loss_ratio REAL,
    cumulative_return REAL,
    benchmark_0050 REAL,
    benchmark_00891 REAL
);
```

---

## 七、GitHub Actions 自動化

新增 `.github/workflows/yao_lab_daily.yml`:
```yaml
name: Yao Lab Daily Scan
on:
  schedule:
    - cron: '50 6 * * 1-5'   # 每週一到五 UTC 06:50(台灣 14:50)
  workflow_dispatch:
jobs:
  scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run yao lab scan
        run: python yao_lab/daily_scan.py
      - name: Commit reports & db
        run: |
          git config user.name "github-actions"
          git config user.email "actions@github.com"
          git add yao_lab/reports/ yao_lab/data/yao_lab.db
          git commit -m "yao_lab: daily scan $(date +%Y-%m-%d)" || exit 0
          git push
```

---

## 八、每日報告格式

```markdown
# 獵妖實驗室日報
日期: YYYY-MM-DD

## 🎯 今日候選名單(進場池)
| 股號 | 名稱 | 觸發訊號 | 營收成長月數 | 近20日漲幅 |
|---|---|---|---|---|
| ... | ... | A+B / B / C | 5 個月 | +12% |

## 📊 持有中(紙上)
| 股號 | 進場日 | 進場價 | 今日 | 損益 | 天數 |
|---|---|---|---|---|---|

## ⚠️ 今日出場
| 股號 | 進場 | 出場 | 損益 | 原因 |
|---|---|---|---|---|

## 📈 本月績效
- 紙上交易: X 筆
- 勝率: X%
- 累積報酬: X%
- vs 0050: X% / vs 00891: X%
```

---

## 九、兩天實作計畫

### Day 1(明天)
1. 建立 `yao_lab/` 資料夾結構
2. 寫 `event_scanner.py`(MOPS 公告抓取,可用 FinMind 簡化)
3. 寫 `volume_surge.py`(爆量上漲判斷)
4. 寫 `revenue_check.py`(營收成長判斷)
5. 寫 `trend_filter.py`(近 20 日漲跌判斷)
6. **能跑出今日候選名單即過關**

### Day 2(後天)
1. 寫 `sector_resonance.py`(族群共振)
2. 寫紙上交易引擎 + 出場規則
3. 寫每日 Markdown 報告生成
4. 設定 GitHub Actions
5. **能自動產出每日報告即上線**

---

## 十、明確不做的事(避免範圍蔓延)
- ❌ NLP 題材偵測
- ❌ 即時看盤
- ❌ Streamlit UI(看 GitHub Markdown 就好)
- ❌ ATR 動態停損(MVP 用簡單規則)
- ❌ 部位管理
- ❌ 接券商 API
- ❌ 複雜回測介面
- ❌ 媒體爬蟲(用爆量上漲當代理)
- ❌ 龍頭/二軍雙模式(MVP 階段簡化)

---

## 十一、實驗紀律
### 終止/檢討條件
- 跑滿 3 個月(8 月底)後做第一次總檢討
- 6 個月(11 月)後決定是否實盤
- **9 月初新生兒到來前,所有開發必須暫停**

### 必填假設記錄
每筆訊號產生時,自動記錄:
- 觸發了哪些訊號(A/B/C)
- 當時的營收成長月數
- 當時的近 20 日漲幅
- T+N 結算結果

3 個月後可分析:
- 哪個訊號(A/B/C)單獨用最有效?
- 哪些組合(A+B, A+C, B+C, A+B+C)勝率最高?
- 訊號 B 的「爆量」門檻要不要調整?
- 營收成長月數從 3 改 6 會更準嗎?

### 對照組
每月計算與以下標的的比較:
- 0050(大盤)
- 00891(中信關鍵半導體)
- 磐石 A 級訊號

---

## 十二、版本歷史
- **v1.0**(2026-05-27 上午):量價突破 + 籌碼集中
- **v1.5**(2026-05-27 下午):因「主力沒時間建倉」洞察,改均線突破
- **v2.0**(2026-05-27 晚上):**從日電貿實戰案例反推,改為事件驅動 + 基本面背書,訊號 OR 邏輯放寬版**

---

## 十三、阿立的進場原則(從日電貿萃取)
從 2026/5/18-5/21 日電貿(3090)實戰交易萃取:
- **進場價** 149 元
- **出場價** 196 元
- **持有 3 天,獲利 31.5%**

### 真實決策模型
1. 外部觸發(媒體/朋友分享)→ 系統 = 主動掃描取代
2. 利多消息確認 → 訊號 A(MOPS)
3. 體質快速檢查 → 營收成長門檻
4. 不錯就買了 → 隔日開盤紙上進場
5. 兩三天噴出 → 5 日時間停損
6. 30% 心理錨點 → **25% 停利規則化**(避免感覺判斷)
7. 看到處置新聞 → 處置警戒規則

---

> **獵妖系統的紀律:**
> 「我們不預測哪檔會妖,我們只承認哪檔已經在妖。」
> 「我們不靠人性判斷出場,我們交給規則。」
> 「我們不貪心、不戀棧,訊號消失就走。」
>
> **「神離開他,要試驗他,好知道他心內如何。」(歷代志下 32:31)**
> 獵妖的每一筆交易,都是一場考試。系統的規則,是阿立給自己的法則。
