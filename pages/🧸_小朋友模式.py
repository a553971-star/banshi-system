import streamlit as st
import pandas as pd
import datetime
import os
import sys

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from app import load_latest_decisions, explain_metrics, _cached_daily_verse
from live_analyzer import process_stock_live

st.set_page_config(
    page_title="🧸 磐石小朋友模式",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
    .kid-card {
        transition: all 0.25s ease;
        border-radius: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        margin-bottom: 14px;
    }
    .kid-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 10px 24px rgba(0,0,0,0.15);
    }
    @media (max-width: 768px) {
        .kid-card { border-radius: 14px !important; padding: 16px !important; }
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<h1 style="text-align:center; font-size:48px; margin-bottom:4px;">🪨 磐石小幫手</h1>
<h3 style="text-align:center; color:#FF6B6B;">小朋友也能看懂的股票幫手</h3>
""", unsafe_allow_html=True)

st.info("""
🎯 今日任務：
1️⃣ 找出一支「可以買」的股票
2️⃣ 確認它是「剛起飛」不是「飛太高」
3️⃣ 信心分數要超過 60 才考慮
""")

st.markdown("---")

# ── 即時查詢 ──────────────────────────────────────────────────────
st.subheader("🔍 想看哪一支股票呢？")
live_input = st.text_input("", placeholder="輸入代號或名字，例如：2330 或 台積電",
                           key="kid_live_query", label_visibility="collapsed")

if live_input:
    with st.spinner("小幫手正在努力分析中... 🧐"):
        try:
            from main import load_params
            params = load_params()
            result = process_stock_live(live_input.strip(), params, print_snapshot=False)
        except Exception:
            result = None

    if result:
        dec = result.get("decision", "IGNORE")
        conf = int(result.get("confidence", 0))
        c = int(result.get("C_days") or 0)
        b = int(result.get("B_days") or 0)
        a = int(result.get("A_days") or 0)

        if dec == "BUY":
            emoji, color = "🚀 可以買！", "#28a745"
        elif dec == "WAIT":
            emoji, color = "👀 值得觀察", "#ffc107"
        else:
            emoji, color = "⏳ 先不要買", "#6c757d"

        st.markdown(f"""
<div style="background:{color}20; border:5px solid {color}; border-radius:24px;
            padding:28px; text-align:center; margin:16px 0;">
    <h2 style="font-size:36px;">{result.get('stock_id','')} {result.get('name','')}</h2>
    <h1 style="font-size:48px; margin:12px 0;">{emoji}</h1>
    <h3 style="font-size:26px;">信心：{conf} 分</h3>
</div>
""", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("🧱 地基穩固", f"{c} 天")
        c2.metric("🏗️ 有人在買", f"{b} 天")
        c3.metric("🚀 已經起飛", f"{a} 天")

        try:
            _, coach = explain_metrics(result)
        except Exception:
            coach = "繼續觀察喔～"
        st.success(f"🧸 小幫手說：{coach}")
    else:
        st.warning("找不到這支股票，試試看輸入代號數字？")

st.divider()

# ── 讀取今日資料 ──────────────────────────────────────────────────
df = load_latest_decisions()
if df.empty:
    st.info("今天還沒有資料，請等系統更新！")
    st.stop()

df["decision"] = df["decision"].fillna("")
for col in ["C_days", "B_days", "A_days", "confidence"]:
    df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0).astype(int)

action_df = df[df["decision"] == "BUY"].sort_values("confidence", ascending=False).copy()
wait_df   = df[df["decision"] == "WAIT"].sort_values("confidence", ascending=False).copy()

# ── 今日最強 ──────────────────────────────────────────────────────
st.subheader("🏆 今天最強的股票")
if not action_df.empty:
    top = action_df.iloc[0]
    st.markdown(f"""
<div style="background:linear-gradient(90deg, #28a745, #20c997); color:white;
            padding:24px; border-radius:20px; text-align:center; margin-bottom:16px;">
    <div style="font-size:28px; font-weight:bold;">{top['stock_id']} {top.get('name','')}</div>
    <div style="font-size:40px; margin:8px 0;">🚀 可以買！</div>
    <div style="font-size:20px;">信心 {int(top.get('confidence', 0))} 分</div>
</div>
""", unsafe_allow_html=True)
else:
    st.info("今天還沒有系統認可的買進機會，明天再看看！")


# ── 卡片函式 ──────────────────────────────────────────────────────
def render_kid_card(row):
    stock_id = str(row.get("stock_id", ""))
    name     = str(row.get("name", ""))
    conf     = int(row.get("confidence", 0))
    c        = int(row.get("C_days", 0))
    b        = int(row.get("B_days", 0))
    a        = int(row.get("A_days", 0))

    if conf >= 75:
        label, border, bg, conf_color = "🚀 可以買！", "#28a745", "#e6f4ea", "#28a745"
    elif conf >= 60:
        label, border, bg, conf_color = "👀 值得觀察", "#ffc107", "#fff8e1", "#ffc107"
    else:
        label, border, bg, conf_color = "⏳ 先看看",  "#6c757d", "#f8f9fa", "#6c757d"

    if a == 0:
        fly = "還在準備 🛬"
    elif a <= 2:
        fly = f"剛起飛 ✈️（{a}天）"
    elif a <= 4:
        fly = f"飛一段了 🛫（{a}天）"
    else:
        fly = f"飛太高了 ⚠️（{a}天）"

    base  = (f"穩 👍（{c}天）" if c >= 5 else
             f"快好了（{c}天）" if c >= 2 else
             f"還不穩 ⚠️（{c}天）")
    build = (f"很多人在買 💪（{b}天）" if b >= 5 else
             f"有人在買 👍（{b}天）" if b >= 2 else
             f"還沒人買 😐（{b}天）")

    try:
        _, coach = explain_metrics(row.to_dict())
    except Exception:
        coach = "繼續觀察喔～"

    st.markdown(f"""
<div class="kid-card" style="background:{bg}; border-left:10px solid {border}; padding:18px 20px;">
    <div style="font-size:19px; font-weight:bold;">{stock_id} {name}</div>
    <div style="font-size:26px; margin:6px 0;">{label}</div>
    <div style="font-size:14px; color:#333; line-height:1.8;">
        🧱 地基：{base}<br>
        🏗️ 有人在買：{build}<br>
        🚀 起飛狀態：{fly}
    </div>
    <div style="margin-top:10px;">
        信心：<span style="background:{conf_color}; color:white; padding:3px 10px;
        border-radius:20px; font-weight:bold;">{conf}</span>
    </div>
    <div style="margin-top:10px; padding:8px 12px; background:rgba(0,0,0,0.04);
    border-radius:8px; font-size:13px; color:#444;">
        🧸 小幫手說：{coach}
    </div>
</div>
""", unsafe_allow_html=True)


# ── 可以考慮的股票 ────────────────────────────────────────────────
st.subheader("📋 今天可以考慮的股票")
if not action_df.empty:
    cols = st.columns(2)
    for i, (_, row) in enumerate(action_df.head(20).iterrows()):
        with cols[i % 2]:
            render_kid_card(row)
else:
    st.info("今天沒有系統認可的買進機會")

# ── 值得觀察的股票 ────────────────────────────────────────────────
if not wait_df.empty:
    st.subheader("👀 值得觀察的股票")
    cols2 = st.columns(2)
    for i, (_, row) in enumerate(wait_df.head(10).iterrows()):
        with cols2[i % 2]:
            render_kid_card(row)

# ── 教學區 ────────────────────────────────────────────────────────
st.divider()
with st.expander("📖 小朋友看這裡！三個神奇數字是什麼意思？", expanded=False):
    st.markdown("""
| 圖示 | 名字 | 意思 |
|------|------|------|
| 🧱 | 地基 | 股票停止下跌的天數，越多越穩 |
| 🏗️ | 有人在買 | 大人（法人）偷偷買的天數，越多越強 |
| 🚀 | 起飛狀態 | 開始上漲的天數，1~2天最好 |

**最好的情況：地基穩 + 有人在買 + 剛剛起飛 = 好機會！**

**小規則：**
- 🔴 飛超過 5 天：不要追，太貴了
- 🟡 信心低於 60：先跳過
- 🟢 信心超過 75：值得認真考慮
""")

today_str = datetime.date.today().isoformat()
st.info(_cached_daily_verse(today_str))
st.caption("🧸 磐石小幫手 — 專為小朋友設計")
