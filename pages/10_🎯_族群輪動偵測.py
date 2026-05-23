"""
pages/10_🎯_族群輪動偵測.py
族群層級的資金流地圖。判斷下一棒會輪到哪個族群。
與 pages/5 AI 戰情室為不同層級工具，完全獨立，不互相 import。

v3.3 UX 改造：戰情室版
- 4 欄位摘要列（族群名 | 狀態標籤 | heat▲▼ | early▲▼ | ⭐外資焦點）
- 一句話狀態 emoji + 短語
- 1 日變化箭頭（紅漲綠跌）
- 龍頭卡片區（Top 3 市值）
- 簡潔模式 toggle
"""
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import Optional, Tuple

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

# ── 觀察期提醒 ────────────────────────────────────────────────────────────
OBSERVATION_START  = datetime(2026, 5, 23)
HEALTH_CHECK_DATE  = OBSERVATION_START + timedelta(days=21)
CHECKLIST_PATH     = os.path.join(BASE_PATH, "notes", "health_check_checklist.md")

_today     = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
_days_left = (HEALTH_CHECK_DATE - _today).days
_days_into = (_today - OBSERVATION_START).days


def _load_checklist() -> str:
    if os.path.exists(CHECKLIST_PATH):
        try:
            with open(CHECKLIST_PATH, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass
    return "（檢查清單尚未建立：`notes/health_check_checklist.md`）"


if _days_left > 0:
    st.info(
        f"🔍 **觀察期進行中**　第 **{_days_into}** 天 / 共 21 天　"
        f"距離健檢日 `{HEALTH_CHECK_DATE.strftime('%Y-%m-%d')}` 還有 **{_days_left}** 天"
    )
    with st.expander("📋 觀察期該做什麼 / 不該做什麼"):
        st.markdown(_load_checklist())
elif _days_left == 0:
    st.success(
        f"✅ **今天是觀察期健檢日**（{HEALTH_CHECK_DATE.strftime('%Y-%m-%d')}）—— "
        "請依下方檢查清單回顧 21 天的訊號表現,決定是否進入下一階段"
    )
    with st.expander("📋 健檢清單（建議展開）", expanded=True):
        st.markdown(_load_checklist())
else:
    overdue_days = -_days_left
    st.warning(
        f"⏰ **健檢日已過期 {overdue_days} 天**（原訂 `{HEALTH_CHECK_DATE.strftime('%Y-%m-%d')}`）—— "
        "請儘早完成系統健檢"
    )
    with st.expander("📋 健檢清單"):
        st.markdown(_load_checklist())

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
    st.caption(
        f"資料時間：`{status.get('generated_at', '—')}`　"
        f"版本：`{status.get('version', '—')}`　"
        f"資料截止：`{status.get('as_of_date', '—')}`　"
        f"變化對照：`{status.get('compare_with') or '無前日對照'}`"
    )

# ── 側邊欄：簡潔模式 + 進階篩選（view-level，不污染原始資料） ─────────────
has_market_cap = bool(status.get("has_market_cap"))
mc_meta = status.get("market_cap", {}) or {}

with st.sidebar:
    simple_mode = st.toggle(
        "🎯 簡潔模式", value=True,
        help="關閉以開啟外資鎖定 / 市值階層篩選",
    )
    st.caption("簡潔模式預設開啟，先看戰情再進階篩選。")

    if not simple_mode:
        st.markdown("### 🎚️ 進階篩選")
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
        )
        tier_filter = st.multiselect(
            "市值階層",
            options=["mega", "large", "mid", "small"],
            default=["mega", "large", "mid", "small"],
            disabled=not has_market_cap,
            help="mega ≥5000 億 / large 1000-5000 / mid 300-1000 / small <300",
        )
    else:
        show_foreign_only = False
        tier_filter = ["mega", "large", "mid", "small"]


# ── 工具函式 ──────────────────────────────────────────────────────────────
TIER_EMOJI = {"mega": "🟣", "large": "🔵", "mid": "🟢", "small": "⚪"}
RANK_EMOJI = {1: "🥇", 2: "🥈", 3: "🥉"}


def _format_change(value: Optional[float]) -> str:
    """純文字箭頭（給 expander label 用，無顏色）。"""
    if value is None:
        return ""
    if abs(value) < 0.5:
        return " →"
    if value > 0:
        return f" ▲+{value:.1f}"
    return f" ▼{value:.1f}"


def _format_change_md(value: Optional[float]) -> str:
    """含 Streamlit markdown 顏色（紅漲綠跌）。"""
    if value is None:
        return ":gray[—]"
    if abs(value) < 0.5:
        return ":gray[→]"
    if value > 0:
        return f":red[▲ +{value:.1f}]"
    return f":green[▼ {value:.1f}]"


def _get_status_label(group: dict) -> Tuple[str, str]:
    """根據 group 內各項指標判定一句話狀態。優先序由危險到平穩。"""
    heat  = float(group.get("heat_score") or 0)
    early = float(group.get("early_rotation_score") or 0)
    exh   = float((group.get("exhaustion") or {}).get("score") or 0)
    lifec = (group.get("lifecycle") or {}).get("lifecycle_stage", "")
    hc    = group.get("heat_change_1d")
    ec    = group.get("early_change_1d")
    hc_v  = hc if hc is not None else 0
    ec_v  = ec if ec is not None else 0

    if exh > 60:
        return "⚠️", "過熱震盪"
    if lifec == "late" and hc_v < 0:
        return "🔻", "末升段"
    if hc_v > 2 and ec_v > 2:
        return "🔥", "資金加速"
    if early > 30 and heat < 40:
        return "👀", "資金卡位"
    if lifec == "early" and hc_v > 0:
        return "🌅", "主升初期"
    if heat > 30 and abs(hc_v) <= 2:
        return "📊", "主流延續"
    if heat < 20 and early < 10:
        return "💤", "尚未啟動"
    return "—", "觀察中"


@st.cache_data(ttl=600)
def _fetch_stock_metrics(code: str) -> dict:
    """從 banshi.db 取 5d 報酬 + 外資 5 日累計（億元），從 latest_decisions.csv 取 CBA。"""
    out = {"return_5d": None, "foreign_5d_billion": None, "cba_stage": "—"}
    try:
        conn = sqlite3.connect(os.path.join(BASE_PATH, "banshi.db"))
        # 6 個交易日的 close 算 5 日報酬
        rows = conn.execute(
            "SELECT close FROM price_history WHERE stock_id=? AND close IS NOT NULL "
            "ORDER BY date DESC LIMIT 6",
            (code,),
        ).fetchall()
        latest_close = rows[0][0] if rows else None
        if len(rows) >= 6 and rows[5][0]:
            out["return_5d"] = (rows[0][0] - rows[5][0]) / rows[5][0] * 100.0
        # 外資 5 日累計（股）× 收盤 / 1e8 → 億元
        inst = conn.execute(
            "SELECT foreign_net FROM institutional_history WHERE stock_id=? "
            "ORDER BY date DESC LIMIT 5",
            (code,),
        ).fetchall()
        if inst and latest_close:
            net_shares = sum((r[0] or 0) for r in inst)
            out["foreign_5d_billion"] = net_shares * latest_close / 1e8
        conn.close()
    except Exception:
        pass

    # CBA：B_phase 為主，否則 flow_status
    try:
        with open(os.path.join(BASE_PATH, "latest_decisions.csv"), encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("stock_id") == code:
                    out["cba_stage"] = row.get("B_phase") or row.get("flow_status") or "—"
                    break
    except Exception:
        pass

    return out


@st.cache_data(ttl=600)
def _market_cap_stocks() -> dict:
    """市值索引的 stocks 子集快取，供 UI 反查 close/shares。"""
    try:
        with open(os.path.join(BASE_PATH, "data", "market_cap.json"), encoding="utf-8") as f:
            return (json.load(f) or {}).get("stocks", {}) or {}
    except Exception:
        return {}


def _render_leader_card(stock: dict):
    """單一龍頭股的卡片渲染。"""
    metrics = _fetch_stock_metrics(stock["code"])
    tier_e  = TIER_EMOJI.get(stock["tier"], "")
    focus_e = "⭐" if stock.get("foreign_focus") else ""
    full    = _market_cap_stocks().get(stock["code"], {})
    close   = full.get("close")

    with st.container(border=True):
        st.markdown(f"#### {tier_e} `{stock['code']}` **{stock['name']}** {focus_e}")
        c1, c2 = st.columns(2)
        c1.metric("收盤", f"{close:,.1f} 元" if close else "—")
        c2.metric("市值", f"{stock['market_cap_billion']:,.0f} 億")

        c3, c4, c5 = st.columns(3)
        r5 = metrics["return_5d"]
        f5 = metrics["foreign_5d_billion"]
        c3.metric("5日漲跌", f"{r5:+.1f}%" if r5 is not None else "—")
        c4.metric("外資 5 日", f"{f5:+.1f} 億" if f5 is not None else "—")
        c5.metric("CBA", metrics["cba_stage"] or "—")


# ── 大盤狀態 + 資料一致性（壓縮一行） ─────────────────────────────────────
integrity = status.get("data_integrity", {})
regime    = status.get("market_regime", {})

regime_name = regime.get("regime", "unknown")
regime_label = {
    "risk_on":  "🟢 risk_on",
    "neutral":  "🟡 neutral",
    "risk_off": "🔴 risk_off",
}.get(regime_name, f"⚪ {regime_name}")
suffix = "（空殼版・訊號保守 ×0.7）" if regime.get("phase") == "placeholder" else ""
sync_label = "✅ 同步" if integrity.get("all_synced") else "❌ 不同步"
sig = status.get("rotation_signal", {})
prob_label = {
    "high":   "🔴 高",
    "medium": "🟠 中",
    "low":    "🟡 低",
    "none":   "⚪ 無",
    "n/a":    "—",
}.get(sig.get("probability"), "—")

st.markdown(
    f"**大盤狀態** {regime_label}{suffix}　｜　"
    f"**資料一致性** {sync_label}　｜　"
    f"**輪動切換機率** {prob_label}"
)

# ── 資料不同步：警告 + 中止 ─────────────────────────────────────────────────
if not integrity.get("all_synced"):
    st.error(f"⚠️ 資料未同步，所有訊號暫停。{integrity.get('warning', '')}")
    st.json(integrity.get("tables", {}))
    st.stop()

st.divider()

# ── 區塊 A：輪動雷達總覽（壓縮版） ────────────────────────────────────────
st.subheader("🛰️ 輪動雷達總覽")

current = status.get("current_leader")
candidates = status.get("next_candidates", [])

a1, a2 = st.columns([1, 2])
with a1:
    st.markdown("##### 目前主流")
    if current:
        st.markdown(f"**{current['group_name']}**")
        st.caption(
            f"heat {current['heat_score']:.1f}　"
            f"lifecycle {current['lifecycle']['lifecycle_stage']}　"
            f"退潮 {current['exhaustion']['score']:.0f}　"
            f"風險 {current['exhaustion']['risk_level']}"
        )
    else:
        st.caption("尚未識別到主流族群")

with a2:
    st.markdown("##### 下一棒候選（前 3）")
    if candidates:
        chip_cols = st.columns(min(3, len(candidates)))
        for i, c in enumerate(candidates):
            with chip_cols[i]:
                st.markdown(f"**#{i + 1} {c['group_name']}**")
                st.caption(
                    f"early {c['early_rotation_score']:.1f}　"
                    f"heat {c['heat_score']:.1f}　"
                    f"{c['lifecycle']['lifecycle_stage']}"
                )
    else:
        st.caption("無潛伏候選")

st.divider()

# ── 區塊 B【主舞台】：族群檢視 ────────────────────────────────────────────
st.subheader("🗺️ 族群檢視")
st.caption("按 heat_score 由高到低排序，🥇🥈🥉 為當下最熱前三。預設摺起，點擊展開看龍頭。")

groups = status.get("all_groups", [])
groups_by_heat = sorted(groups, key=lambda g: (g.get("heat_score") or 0), reverse=True)

if not groups_by_heat:
    st.info("尚無族群資料")
else:
    for rank, g in enumerate(groups_by_heat, 1):
        # ── 摘要列（4 欄資訊）──────────────────────────────────────────
        rank_e = RANK_EMOJI.get(rank, "　")
        emoji, label = _get_status_label(g)
        hc_str = _format_change(g.get("heat_change_1d"))
        ec_str = _format_change(g.get("early_change_1d"))
        focus_n = len(g.get("foreign_focus_members") or [])
        focus_str = f"⭐ {focus_n}" if has_market_cap else ""

        summary = (
            f"{rank_e}  **{g['group_name']}**　"
            f"{emoji} {label}　｜　"
            f"heat **{g['heat_score']:.0f}**{hc_str}　"
            f"early **{g['early_rotation_score']:.0f}**{ec_str}"
            + (f"　｜　{focus_str}" if focus_str else "")
        )

        with st.expander(summary, expanded=False):
            # ── 變化 metric 列（含顏色）──────────────────────────────
            mc1, mc2, mc3 = st.columns(3)
            mc1.markdown(f"**heat** {g['heat_score']:.1f}　{_format_change_md(g.get('heat_change_1d'))}")
            mc2.markdown(f"**early** {g['early_rotation_score']:.1f}　{_format_change_md(g.get('early_change_1d'))}")
            mc3.markdown(f"**狀態** {emoji} {label}")

            # ── 龍頭區（Top 3 市值）──────────────────────────────────
            members_with_cap = g.get("members_with_cap") or []
            if has_market_cap and members_with_cap:
                visible = [
                    m for m in members_with_cap
                    if m.get("tier") in tier_filter
                    and (not show_foreign_only or m.get("foreign_focus"))
                ]
                top3 = visible[:3]
                if top3:
                    st.markdown("##### 🎯 龍頭區（市值前 3）")
                    cols = st.columns(len(top3))
                    for col, stock in zip(cols, top3):
                        with col:
                            _render_leader_card(stock)
                else:
                    st.caption("（依目前篩選條件，無符合的龍頭）")

                # ── 完整成員表格（巢狀 expander，預設摺起）────────
                with st.expander(f"📊 完整成員表格（{len(visible)}/{len(members_with_cap)}）", expanded=False):
                    if visible:
                        table_rows = []
                        for idx, m in enumerate(visible, 1):
                            metrics = _fetch_stock_metrics(m["code"])
                            r5 = metrics["return_5d"]
                            f5 = metrics["foreign_5d_billion"]
                            table_rows.append({
                                "#":        idx,
                                "代號":      m["code"],
                                "名稱":      m["name"],
                                "市值(億)":  m["market_cap_billion"],
                                "階層":      f"{TIER_EMOJI.get(m['tier'], '')} {m['tier']}",
                                "層級":      m.get("stock_tier", ""),
                                "外資焦點":  "⭐" if m.get("foreign_focus") else "",
                                "5日漲跌":   f"{r5:+.1f}%" if r5 is not None else "—",
                                "外資5日":   f"{f5:+.1f} 億" if f5 is not None else "—",
                                "CBA":       metrics["cba_stage"],
                            })
                        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)
                    else:
                        st.caption("（無符合篩選的成員）")
            else:
                st.caption("⚠️ 市值資料未生成，無法顯示龍頭區。執行 `python3 utils/market_cap.py` 建立。")

            # ── 族群資訊（次要資訊放最下）─────────────────────────────
            with st.expander("ℹ️ 族群資訊（lifecycle / 總市值 / 退潮）", expanded=False):
                ig1, ig2, ig3 = st.columns(3)
                lc = g.get("lifecycle") or {}
                ig1.markdown(
                    f"**lifecycle**：{lc.get('lifecycle_stage', '—')}　"
                    f"(lag_diff {lc.get('lag_diff', 0):.2f})"
                )
                if has_market_cap:
                    ig2.markdown(f"**總市值**：{g.get('group_total_market_cap', 0):,.0f} 億")
                ig3.markdown(
                    f"**退潮**：{g['exhaustion']['score']:.0f}　"
                    f"風險 {g['exhaustion']['risk_level']}"
                )
                if g["exhaustion"].get("triggers"):
                    st.caption("退潮訊號：" + " / ".join(g["exhaustion"]["triggers"]))
                st.caption(
                    f"龍頭 5d {lc.get('leaders_return_5d', 0):.2f}%　"
                    f"二線 5d {lc.get('second_return_5d', 0):.2f}%　"
                    f"彈性 5d {lc.get('elastic_return_5d', 0):.2f}%　"
                    f"成員 {g.get('n_members', 0)} 檔　"
                    f"成交占比 {g.get('turnover_share', 0) * 100:.1f}%"
                )

st.divider()

# ── 區塊 C：雙分數矩陣圖 ──────────────────────────────────────────────────
with st.expander("📊 雙分數矩陣（heat × early）", expanded=False):
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
            fig.update_layout(height=500, xaxis_title="heat_score", yaxis_title="early_rotation_score")
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.dataframe(df, use_container_width=True)
        st.caption("📍 左上潛伏（最佳買點）｜右上過熱（避免追高）｜右下退燒｜左下冷門")

# ── 區塊 D：退潮警示 ──────────────────────────────────────────────────────
risk_groups = [g for g in groups if g["exhaustion"]["risk_level"] in ("high", "medium")]
with st.expander(f"⚠️ 退潮警示面板（{len(risk_groups)} 個）", expanded=bool(risk_groups)):
    if risk_groups:
        for g in risk_groups:
            exh = g["exhaustion"]
            emoji = "🔴" if exh["risk_level"] == "high" else "🟠"
            with st.container(border=True):
                st.markdown(f"### {emoji} {g['group_name']}　風險：{exh['risk_level']}")
                st.caption(
                    f"退潮分數 {exh['score']:.0f}　heat {g['heat_score']:.1f}　"
                    f"建議：{exh['recommended_action']}"
                )
                if exh.get("triggers"):
                    for t in exh["triggers"]:
                        st.markdown(f"- {t}")
    else:
        st.success("目前無族群觸發退潮警示")

# ── 區塊 E：歷史軌跡（佔位） ──────────────────────────────────────────────
with st.expander("📈 歷史軌跡", expanded=False):
    st.info("功能規劃中：累積每日 rotation_status 後可顯示 60 日 heat / early 折線圖。")

# ── 區塊 F：資料品質監控 ──────────────────────────────────────────────────
with st.expander("🔧 資料品質監控", expanded=False):
    tab = integrity.get("tables", {})
    qd = pd.DataFrame([
        {"資料表": "price_history",         "最新日期": tab.get("price_history", "—")},
        {"資料表": "institutional_history", "最新日期": tab.get("institutional_history", "—")},
        {"資料表": "margin_history",        "最新日期": tab.get("margin_history", "—")},
    ])
    st.dataframe(qd, use_container_width=True, hide_index=True)

    missing_all = []
    for g in groups:
        for m in g.get("missing_members", []):
            missing_all.append({"族群": g["group_name"], "缺漏代號": m})
    if missing_all:
        st.markdown(f"##### 查無資料的成員（{len(missing_all)} 檔）")
        st.dataframe(pd.DataFrame(missing_all), use_container_width=True, hide_index=True)

with st.expander("📐 演算法設定（version + weights）", expanded=False):
    st.json(status.get("algorithm_config", {}))
