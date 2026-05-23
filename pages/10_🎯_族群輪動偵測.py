"""
pages/10_🎯_族群輪動偵測.py
族群層級的資金流地圖。判斷下一棒會輪到哪個族群。
與 pages/5 AI 戰情室為不同層級工具，完全獨立，不互相 import。
"""
import json
import os
import sys

import pandas as pd
import streamlit as st

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from utils.rotation_detector import detect_rotation

st.set_page_config(page_title="族群輪動偵測", layout="wide")

# ── 頁首 ──────────────────────────────────────────────────────────────────
st.title("🎯 AI 族群輪動偵測")
st.caption("族群層級的資金流地圖｜判斷下一棒會輪到哪個族群")
st.caption("👉 個股分析請至 **pages/5 AI 戰情室**；本頁不做個股訊號")

DATA_PATH = os.path.join(BASE_PATH, "data", "rotation_status.json")


# ── 讀取資料：優先讀 JSON，沒有就現算 ─────────────────────────────────────
@st.cache_data(ttl=600)
def _load_status() -> dict:
    if os.path.exists(DATA_PATH):
        try:
            with open(DATA_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return detect_rotation()


col_refresh, col_age = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 重新計算"):
        st.cache_data.clear()
        st.rerun()

status = _load_status()
with col_age:
    st.caption(f"資料時間：`{status.get('generated_at', '—')}`　版本：`{status.get('version', '—')}`　資料截止：`{status.get('as_of_date', '—')}`")

# ── 側邊欄：市值篩選（view-level，不污染原始資料） ─────────────────────────
has_market_cap = bool(status.get("has_market_cap"))
mc_meta = status.get("market_cap", {}) or {}

with st.sidebar:
    st.markdown("### 🎚️ 顯示設定")
    if has_market_cap:
        st.caption(
            f"市值資料：`{mc_meta.get('as_of_date', '—')}`　"
            f"涵蓋 {mc_meta.get('n_stocks', 0)} 檔　"
            f"缺漏 {mc_meta.get('missing_count', 0)} 檔"
        )
    else:
        st.caption("市值資料尚未產生（執行 `python3 utils/market_cap.py` 建立）")

    show_foreign_only = st.checkbox(
        "只看外資鎖定名單",
        value=False,
        disabled=not has_market_cap,
        help="勾選後僅顯示外資配置標的" if has_market_cap else "等待 market_cap.json 生成",
    )

    tier_filter = st.multiselect(
        "市值階層",
        options=["mega", "large", "mid", "small"],
        default=["mega", "large", "mid", "small"] if has_market_cap else [],
        disabled=not has_market_cap,
        help="mega ≥5000 億 / large 1000-5000 / mid 300-1000 / small <300",
    )


def _filter_members(members: list, tier_filter: list, foreign_only: bool) -> list:
    """View-level filter：只用於渲染，不寫回 status。"""
    if not members:
        return []
    out = []
    for m in members:
        if tier_filter and m.get("tier") not in tier_filter:
            continue
        if foreign_only and not m.get("foreign_focus"):
            continue
        out.append(m)
    return out


TIER_EMOJI = {"mega": "🐳", "large": "🐋", "mid": "🐟", "small": "🐠"}

# ── 頁首三大狀態 ──────────────────────────────────────────────────────────
integrity = status.get("data_integrity", {})
regime    = status.get("market_regime", {})

c1, c2, c3 = st.columns(3)
with c1:
    regime_name = regime.get("regime", "unknown")
    regime_label = {"risk_on": "🟢 risk_on", "neutral": "🟡 neutral", "risk_off": "🔴 risk_off"}.get(regime_name, f"⚪ {regime_name}")
    suffix = "（空殼版・訊號保守 ×0.7）" if regime.get("phase") == "placeholder" else ""
    st.metric("大盤狀態", regime_label, help=f"signal_multiplier = {regime.get('signal_multiplier')}{suffix}")
with c2:
    sync_label = "✅ 同步" if integrity.get("all_synced") else "❌ 不同步"
    st.metric("資料一致性", sync_label, help=str(integrity.get("tables", {})))
with c3:
    sig = status.get("rotation_signal", {})
    prob_label = {"high": "🔴 高", "medium": "🟠 中", "low": "🟡 低", "none": "⚪ 無", "n/a": "—"}.get(sig.get("probability"), "—")
    st.metric("輪動切換機率", prob_label, help=f"signals_count = {sig.get('signals_count')}")

# ── 資料不同步：警告 + 中止 ─────────────────────────────────────────────────
if not integrity.get("all_synced"):
    st.error(f"⚠️ 資料未同步，所有訊號暫停。{integrity.get('warning', '')}")
    st.json(integrity.get("tables", {}))
    st.stop()

st.divider()

# ── 區塊 A：輪動雷達總覽 ──────────────────────────────────────────────────
st.subheader("🛰️ 輪動雷達總覽")

current = status.get("current_leader")
candidates = status.get("next_candidates", [])

a1, a2 = st.columns([1, 1])
with a1:
    st.markdown("##### 目前主流族群")
    if current:
        st.markdown(f"### {current['group_name']}")
        exh = current.get("exhaustion", {})
        st.progress(min(1.0, exh.get("score", 0) / 100.0), text=f"退潮分數 {exh.get('score', 0):.0f} / 100　風險：{exh.get('risk_level', '—')}")
        st.caption(f"heat={current['heat_score']:.1f}（{current['heat_stage']}）｜lifecycle={current['lifecycle']['lifecycle_stage']}")
        if exh.get("triggers"):
            st.warning("退潮訊號：" + " / ".join(exh["triggers"]))
        st.caption("建議：" + exh.get("recommended_action", "—"))
    else:
        st.info("尚未識別到主流族群")

with a2:
    st.markdown("##### 下一棒候選（前 3 名）")
    if candidates:
        for i, c in enumerate(candidates, 1):
            with st.container(border=True):
                st.markdown(f"**#{i} {c['group_name']}**　`{c['cycle_bias']}`")
                cc1, cc2, cc3 = st.columns(3)
                cc1.metric("early_score", f"{c['early_rotation_score']:.1f}")
                cc2.metric("heat", f"{c['heat_score']:.1f}", help=c["heat_stage"])
                cc3.metric("lifecycle", c["lifecycle"]["lifecycle_stage"])
    else:
        st.info("無潛伏候選")

st.divider()

# ── 區塊 B：雙分數矩陣圖 ──────────────────────────────────────────────────
st.subheader("📊 雙分數矩陣（heat × early）")

groups = status.get("all_groups", [])
if groups:
    df = pd.DataFrame([
        {
            "族群":     g["group_name"],
            "heat":     g["heat_score"],
            "early":    g["early_rotation_score"],
            "成交占比": g["turnover_share"] * 100,
            "lifecycle": g["lifecycle"]["lifecycle_stage"],
            "cycle":    g["cycle_bias"],
        }
        for g in groups
    ])

    try:
        import plotly.express as px
        fig = px.scatter(
            df, x="heat", y="early",
            size="成交占比", color="lifecycle",
            text="族群",
            hover_data=["cycle", "成交占比"],
            range_x=[0, 100], range_y=[0, 100],
        )
        fig.update_traces(textposition="top center")
        fig.add_shape(type="line", x0=50, x1=50, y0=0, y1=100, line=dict(color="gray", dash="dash"))
        fig.add_shape(type="line", x0=0, x1=100, y0=50, y1=50, line=dict(color="gray", dash="dash"))
        fig.update_layout(height=500, xaxis_title="heat_score（主流熱度）", yaxis_title="early_rotation_score（潛伏度）")
        st.plotly_chart(fig, use_container_width=True)
    except Exception:
        st.dataframe(df, use_container_width=True)

    st.caption("📍 四象限解讀：左上潛伏（最佳買點）｜右上過熱（避免追高）｜右下退燒｜左下冷門")

st.divider()

# ── 區塊 B2：族群成員市值排行（v3.2 新增） ────────────────────────────────
if has_market_cap:
    st.subheader("💰 族群成員市值排行")
    st.caption("⭐ = 外資鎖定名單　🐳 mega ≥ 5000 億　🐋 large 1000-5000　🐟 mid 300-1000　🐠 small < 300")

    if not tier_filter:
        st.info("請在側邊欄選擇至少一個市值階層")
    else:
        for g in groups:
            members_with_cap = g.get("members_with_cap") or []
            if not members_with_cap:
                continue

            filtered = _filter_members(members_with_cap, tier_filter, show_foreign_only)
            if not filtered:
                continue

            with st.expander(
                f"**{g['group_name']}**　"
                f"總市值 {g.get('group_total_market_cap', 0):,.0f} 億　"
                f"成員 {len(members_with_cap)} 檔（顯示 {len(filtered)} 檔）"
            ):
                cap_df = pd.DataFrame([
                    {
                        "代號":   m["code"],
                        "名稱":   m["name"],
                        "市值(億)": m["market_cap_billion"],
                        "階層":   f"{TIER_EMOJI.get(m['tier'], '')} {m['tier']}",
                        "層級":   m["stock_tier"],
                        "外資鎖定": "⭐" if m["foreign_focus"] else "",
                    }
                    for m in filtered
                ])
                st.dataframe(cap_df, use_container_width=True, hide_index=True)
else:
    st.info("💰 市值排行尚未啟用 — 等待 `data/market_cap.json` 生成後自動顯示")

st.divider()

# ── 區塊 C：族群生命週期 ──────────────────────────────────────────────────
st.subheader("🌱 族群生命週期")

stage_color = {"early": "🌱", "mid_early": "🌿", "mid_late": "🍂", "late": "🍁"}
life_df = pd.DataFrame([
    {
        "族群":       g["group_name"],
        "生命週期":   f"{stage_color.get(g['lifecycle']['lifecycle_stage'], '—')} {g['lifecycle']['lifecycle_stage']}",
        "lag_diff":   g["lifecycle"]["lag_diff"],
        "龍頭 5d%":   g["lifecycle"]["leaders_return_5d"],
        "二線 5d%":   g["lifecycle"]["second_return_5d"],
        "彈性 5d%":   g["lifecycle"]["elastic_return_5d"],
    }
    for g in groups
])
st.dataframe(life_df, use_container_width=True, hide_index=True)

st.divider()

# ── 區塊 D：退潮警示 ──────────────────────────────────────────────────────
st.subheader("⚠️ 退潮警示面板")

risk_groups = [g for g in groups if g["exhaustion"]["risk_level"] in ("high", "medium")]
if risk_groups:
    for g in risk_groups:
        exh = g["exhaustion"]
        emoji = "🔴" if exh["risk_level"] == "high" else "🟠"
        with st.container(border=True):
            st.markdown(f"### {emoji} {g['group_name']}　風險：{exh['risk_level']}")
            st.caption(f"退潮分數 {exh['score']:.0f}　heat {g['heat_score']:.1f}　建議：{exh['recommended_action']}")
            if exh.get("triggers"):
                for t in exh["triggers"]:
                    st.markdown(f"- {t}")
else:
    st.success("目前無族群觸發退潮警示")

st.divider()

# ── 區塊 E：歷史軌跡（佔位） ──────────────────────────────────────────────
st.subheader("📈 歷史軌跡")
st.info("功能規劃中：累積每日 rotation_status 後可顯示 60 日 heat / early 折線圖。")

st.divider()

# ── 區塊 F：資料品質監控 ──────────────────────────────────────────────────
st.subheader("🔧 資料品質監控")
tab = integrity.get("tables", {})
qd = pd.DataFrame([
    {"資料表": "price_history",         "最新日期": tab.get("price_history", "—")},
    {"資料表": "institutional_history", "最新日期": tab.get("institutional_history", "—")},
    {"資料表": "margin_history",        "最新日期": tab.get("margin_history", "—")},
])
st.dataframe(qd, use_container_width=True, hide_index=True)

# 缺漏成員
missing_all = []
for g in groups:
    for m in g.get("missing_members", []):
        missing_all.append({"族群": g["group_name"], "缺漏代號": m})
if missing_all:
    with st.expander(f"查無資料的成員（{len(missing_all)} 檔）"):
        st.dataframe(pd.DataFrame(missing_all), use_container_width=True, hide_index=True)

# ── 演算法設定（開發者檢視） ──────────────────────────────────────────────
with st.expander("📐 演算法設定（version + weights）"):
    st.json(status.get("algorithm_config", {}))
