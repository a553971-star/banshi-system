"""
pages/11_🚀_AI狂飆掃描.py
掃描 AI 供應鏈名單，計算 5 個技術指標加總分數，找出當日最強勢標的。

指標 / 分數配比：
  唐奇安 20 日突破  +35
  爆量（vol ≥ MA20×3） +30
  ADX(14) > 25      +15
  均線多頭（close>MA5>MA10>MA20） +10
  貼近布林上軌（close >= 上軌×0.98） +10
"""
import os
import sys
import sqlite3

import numpy as np
import pandas as pd
import streamlit as st

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from app import render_live_result_block
from pinned_store import load_pinned, save_pinned
from ui.sidebar import render_sidebar_query

# ── 常數 ─────────────────────────────────────────────────────────────────────
DB_PATH             = os.path.join(BASE_PATH, "banshi.db")
AI_CSV              = os.path.join(BASE_PATH, "ai_supply_chain.csv")
STOCK_NAMES_CSV     = os.path.join(BASE_PATH, "stock_names.csv")
MIN_HISTORY_DAYS    = 40
FETCH_DAYS          = 160

SCORE_DONCHIAN      = 35
SCORE_VOLUME        = 30
SCORE_ADX           = 15
SCORE_MA            = 10
SCORE_BOLL          = 10

VOL_RATIO_THRESHOLD = 3.0
ADX_THRESHOLD       = 25.0
BOLL_NEAR_RATIO     = 0.98

# ── 快取資料載入 ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def _load_universe() -> list[str]:
    df = pd.read_csv(AI_CSV, dtype=str)
    return df["stock_id"].str.zfill(4).dropna().unique().tolist()


@st.cache_data(ttl=600)
def _load_stock_names() -> dict[str, str]:
    df = pd.read_csv(STOCK_NAMES_CSV, dtype=str)
    return {r["stock_id"].zfill(4): r["name"] for _, r in df.iterrows()}


@st.cache_data(ttl=600)
def _get_max_date() -> str:
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT MAX(date) FROM price_history WHERE date IS NOT NULL").fetchone()
    conn.close()
    return row[0] if row and row[0] else str(pd.Timestamp.today().date())


@st.cache_data(ttl=600)
def _load_price_data(stock_id: str, as_of_date: str) -> pd.DataFrame:
    """讀取 stock_id 截至 as_of_date 的最近 FETCH_DAYS 筆 K 線。"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT date, open, high, low, close, volume
          FROM price_history
         WHERE stock_id = ? AND date <= ?
         ORDER BY date DESC
         LIMIT ?
        """,
        conn,
        params=(stock_id, as_of_date, FETCH_DAYS),
    )
    conn.close()
    return df.iloc[::-1].reset_index(drop=True)  # 改成升冪


# ── 技術指標計算 ──────────────────────────────────────────────────────────────

def _calc_adx(df: pd.DataFrame, period: int = 14) -> float:
    """計算最後一日的 ADX(period)。回傳 float，失敗回傳 0.0。"""
    high  = df["high"].values.astype(float)
    low   = df["low"].values.astype(float)
    close = df["close"].values.astype(float)
    n = len(close)
    if n < period * 2 + 1:
        return 0.0

    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)
    tr       = np.zeros(n)

    for i in range(1, n):
        up   = high[i] - high[i - 1]
        down = low[i - 1] - low[i]
        plus_dm[i]  = up   if up > down and up > 0   else 0.0
        minus_dm[i] = down if down > up and down > 0 else 0.0
        tr[i] = max(high[i] - low[i],
                    abs(high[i] - close[i - 1]),
                    abs(low[i]  - close[i - 1]))

    # Wilder 平滑（initial = sum of first `period` values，之後遞推）
    def _wilder(arr: np.ndarray) -> np.ndarray:
        sm = np.zeros(n)
        sm[period] = arr[1:period + 1].sum()
        for i in range(period + 1, n):
            sm[i] = sm[i - 1] - sm[i - 1] / period + arr[i]
        return sm

    sm_tr       = _wilder(tr)
    sm_plus_dm  = _wilder(plus_dm)
    sm_minus_dm = _wilder(minus_dm)

    plus_di  = np.where(sm_tr != 0, 100 * sm_plus_dm  / sm_tr, 0.0)
    minus_di = np.where(sm_tr != 0, 100 * sm_minus_dm / sm_tr, 0.0)
    di_sum   = plus_di + minus_di
    dx       = np.where(di_sum != 0, 100 * np.abs(plus_di - minus_di) / di_sum, 0.0)

    # ADX = Wilder smooth of DX
    adx = np.zeros(n)
    if period * 2 > n:
        return 0.0
    adx[period * 2] = dx[period + 1:period * 2 + 1].mean()
    for i in range(period * 2 + 1, n):
        adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period

    return float(adx[-1])


def _score_stock(df: pd.DataFrame) -> dict:
    """計算單檔當日 5 指標分數。回傳 dict 含各指標 bool 和 details。"""
    close  = df["close"].values.astype(float)
    high   = df["high"].values.astype(float)
    volume = df["volume"].values.astype(float)
    n = len(close)

    # ── 1. 唐奇安 20 日突破（今日 close >= 過去 20 日最高 high）────────────
    donchian_ok  = False
    if n >= 21:
        donchian_high = high[-21:-1].max()   # 前 20 日（不含今日）
        donchian_ok   = bool(close[-1] >= donchian_high)

    # ── 2. 爆量（今日 volume >= 前 20 日均量 × 3）─────────────────────────
    volume_ok    = False
    volume_ratio = 0.0
    if n >= 21:
        ma20_vol     = volume[-21:-1].mean()
        if ma20_vol > 0:
            volume_ratio = volume[-1] / ma20_vol
            volume_ok    = bool(volume_ratio >= VOL_RATIO_THRESHOLD)

    # ── 3. ADX(14) > 25 ────────────────────────────────────────────────────
    adx_val = _calc_adx(df)
    adx_ok  = bool(adx_val > ADX_THRESHOLD)

    # ── 4. 均線多頭 close > MA5 > MA10 > MA20 ─────────────────────────────
    ma_ok = False
    if n >= 20:
        ma5  = close[-5:].mean()
        ma10 = close[-10:].mean()
        ma20 = close[-20:].mean()
        ma_ok = bool(close[-1] > ma5 > ma10 > ma20)

    # ── 5. 貼近布林上軌（close >= 上軌 × 0.98）────────────────────────────
    boll_ok   = False
    boll_upper = float("nan")
    if n >= 20:
        ma20_c     = close[-20:].mean()
        std20      = close[-20:].std(ddof=1)
        boll_upper = ma20_c + 2 * std20
        boll_ok    = bool(close[-1] >= boll_upper * BOLL_NEAR_RATIO)

    score = (
        (SCORE_DONCHIAN if donchian_ok  else 0)
        + (SCORE_VOLUME  if volume_ok   else 0)
        + (SCORE_ADX     if adx_ok      else 0)
        + (SCORE_MA      if ma_ok       else 0)
        + (SCORE_BOLL    if boll_ok     else 0)
    )

    return {
        "score":        score,
        "close":        round(float(close[-1]), 2),
        "volume_ratio": round(volume_ratio, 2),
        "adx":          round(adx_val, 1),
        "donchian_ok":  donchian_ok,
        "volume_ok":    volume_ok,
        "adx_ok":       adx_ok,
        "ma_ok":        ma_ok,
        "boll_ok":      boll_ok,
    }


# ── 掃描主函式 ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def run_scan(as_of_date: str) -> tuple[list[dict], int, int]:
    """掃描所有 AI 供應鏈標的，回傳 (rows, ok_count, fail_count)。"""
    universe   = _load_universe()
    name_map   = _load_stock_names()
    rows: list[dict] = []
    ok_cnt = fail_cnt = 0

    for sid in universe:
        try:
            df = _load_price_data(sid, as_of_date)
            if len(df) < MIN_HISTORY_DAYS:
                continue
            s = _score_stock(df)
            rows.append({
                "stock_id":     sid,
                "name":         name_map.get(sid, sid),
                **s,
            })
            ok_cnt += 1
        except Exception:
            fail_cnt += 1

    rows.sort(key=lambda r: r["score"], reverse=True)
    return rows, ok_cnt, fail_cnt


# ── 輔助顯示函式 ──────────────────────────────────────────────────────────────

def _signal_label(score: int) -> str:
    if score >= 75:
        return "🔥極強"
    if score >= 60:
        return "⚡強"
    if score >= 45:
        return "⭐中強"
    return "中性"


def _bool_icon(v: bool) -> str:
    return "✅" if v else "❌"


# ── 主頁面 ────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="AI狂飆掃描", layout="wide")
st.title("🚀 AI 狂飆掃描器")
st.caption("掃描 AI 供應鏈名單，找出當日最強勢的標的")

# ── Sidebar ───────────────────────────────────────────────────────────────────
render_sidebar_query(key_suffix="_yao", render_result_fn=render_live_result_block)

st.divider()

# ── 日期選擇器 ────────────────────────────────────────────────────────────────
max_date_str = _get_max_date()
max_date     = pd.Timestamp(max_date_str).date()

selected_date = st.date_input(
    "選擇交易日（預設為資料庫最新日期）",
    value=max_date,
    max_value=max_date,
    format="YYYY-MM-DD",
)
as_of = str(selected_date)

# ── 掃描 ─────────────────────────────────────────────────────────────────────
with st.spinner("掃描中..."):
    scan_rows, ok_count, fail_count = run_scan(as_of)

# ── 頂部 metrics ──────────────────────────────────────────────────────────────
total_count  = len(scan_rows)
strong_count = sum(1 for r in scan_rows if r["score"] >= 60)
ultra_count  = sum(1 for r in scan_rows if r["score"] >= 75)

m1, m2, m3 = st.columns(3)
m1.metric("總掃描檔數", total_count)
m2.metric("強勢（≥60分）", strong_count)
m3.metric("極強（≥75分）", ultra_count)

st.sidebar.caption(f"掃描成功 {ok_count} 檔 / 失敗 {fail_count} 檔")

# ── 分數門檻 slider ───────────────────────────────────────────────────────────
threshold = st.slider("顯示分數門檻（0 = 全部顯示）", 0, 100, 0, step=5)
visible   = [r for r in scan_rows if r["score"] >= threshold]

# ── 強度分布柱狀圖 ────────────────────────────────────────────────────────────
bins   = ["0-20", "20-40", "40-60", "60-80", "80-100"]
ranges = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]
dist   = {b: sum(1 for r in scan_rows if lo <= r["score"] < hi)
          for b, (lo, hi) in zip(bins, ranges)}
# 最後一個 bucket 含 100
dist["80-100"] = sum(1 for r in scan_rows if r["score"] >= 80)

st.bar_chart(dist)

# ── 下方表格 + 追蹤清單 ───────────────────────────────────────────────────────
if not visible:
    st.info(f"目前門檻 {threshold} 分以上沒有符合的股票。")
    st.stop()

# 建立 data_editor 用的 DataFrame
pinned_now = load_pinned()
table_data = pd.DataFrame([
    {
        "排名":   i + 1,
        "代碼":   r["stock_id"],
        "名稱":   r["name"],
        "信號強度": _signal_label(r["score"]),
        "總分":   r["score"],
        "收盤價":  r["close"],
        "爆量倍數": r["volume_ratio"],
        "唐奇安":  _bool_icon(r["donchian_ok"]),
        "爆量":   _bool_icon(r["volume_ok"]),
        "ADX":    _bool_icon(r["adx_ok"]),
        "均線":   _bool_icon(r["ma_ok"]),
        "布林":   _bool_icon(r["boll_ok"]),
        "加入追蹤": r["stock_id"] in pinned_now,
    }
    for i, r in enumerate(visible)
])

st.markdown(f"**顯示 {len(visible)} 檔（門檻 ≥{threshold} 分）**")

if st.button("📌 將勾選的股票加入追蹤清單"):
    edited = st.session_state.get("scan_table_editor")
    if edited is not None and "added_rows" in edited:
        # data_editor 回傳 edited_rows，格式是 {row_idx: {col: val}}
        pinned_add = set()
        for row_idx, changes in (edited.get("edited_rows") or {}).items():
            if changes.get("加入追蹤"):
                pinned_add.add(table_data.iloc[int(row_idx)]["代碼"])
        # 也納入原本就勾選的列
        for i, row in table_data.iterrows():
            if row["加入追蹤"]:
                pinned_add.add(row["代碼"])
        if pinned_add:
            new_pinned = pinned_now | pinned_add
            save_pinned(new_pinned)
            st.success(f"已將 {len(pinned_add)} 檔加入追蹤清單：{', '.join(sorted(pinned_add))}")
        else:
            st.warning("沒有任何勾選。請在表格中勾選「加入追蹤」後再按此按鈕。")

st.data_editor(
    table_data,
    key="scan_table_editor",
    use_container_width=True,
    hide_index=True,
    column_config={
        "加入追蹤": st.column_config.CheckboxColumn(default=False),
        "總分":    st.column_config.NumberColumn(format="%d"),
        "收盤價":  st.column_config.NumberColumn(format="%.1f"),
        "爆量倍數": st.column_config.NumberColumn(format="%.2f"),
    },
    disabled=["排名", "代碼", "名稱", "信號強度", "總分", "收盤價",
              "爆量倍數", "唐奇安", "爆量", "ADX", "均線", "布林"],
)
