"""
pages/4_🤖_AI供應鏈雷達.py
AI 上下游供應鏈雷達，共用 Ranking 2.0 + C/B/A 引擎。
"""
import os

import pandas as pd
import streamlit as st
from pinned_store import load_pinned, save_pinned

BASE_PATH    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AI_CSV       = os.path.join(BASE_PATH, "ai_supply_chain.csv")
UNIVERSE_CSV = os.path.join(BASE_PATH, "latest_decisions_universe.csv")

THEME_LABEL = {
    "AI_Core":    "🔴 AI核心（晶片/封裝）",
    "AI_Server":  "🟠 AI伺服器（ODM/機殼）",
    "AI_PCB":     "🟡 PCB/載板",
    "AI_Thermal": "🔵 散熱/液冷",
    "AI_Network": "🟢 光通/網通",
    "AI_Memory":  "🟣 記憶體/儲存",
    "AI_Power":   "⚡ 電源/重電",
    "AI_Edge":    "⚫ 邊緣運算",
}
THEME_ORDER = list(THEME_LABEL.keys())

st.set_page_config(page_title="AI供應鏈雷達", layout="wide")
st.title("🤖 AI供應鏈雷達")
st.caption("AI上下游完整架構 × 磐石 Ranking 2.0 × C/B/A軌跡")


# ── 讀資料 ────────────────────────────────────────────────────────────────────
if not os.path.exists(AI_CSV):
    st.error("找不到 ai_supply_chain.csv")
    st.stop()
if not os.path.exists(UNIVERSE_CSV):
    st.warning("尚無 latest_decisions_universe.csv，請等 Actions 跑完")
    st.stop()

ai_df  = pd.read_csv(AI_CSV, dtype={"stock_id": str})
UNIVERSE_THEME_CSV = os.path.join(BASE_PATH, "universe.csv")
theme_df = pd.read_csv(UNIVERSE_THEME_CSV, dtype=str)[["stock_id", "theme"]] if os.path.exists(UNIVERSE_THEME_CSV) else pd.DataFrame(columns=["stock_id", "theme"])
ai_df = ai_df.merge(theme_df, on="stock_id", how="left")
uni_df = pd.read_csv(UNIVERSE_CSV, dtype=str)

# inner join
df = ai_df.merge(uni_df, on="stock_id", how="inner", suffixes=("_ai", ""))

# 若 name 欄位合併有衝突，優先用 ai_supply_chain 的 name
if "name_ai" in df.columns:
    df["name"] = df["name_ai"].fillna(df.get("name", df["name_ai"]))
    df.drop(columns=["name_ai"], inplace=True)

# numeric conversions
for col in ["C_days", "B_days", "A_days", "B_quality", "confidence"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

if df.empty:
    st.warning("沒有任何 AI 供應鏈股票出現在 latest_decisions_universe.csv，請確認 Actions 已跑過全市場掃描。")
    st.stop()


# ── Ranking 2.0 total_score ───────────────────────────────────────────────────
def calc_total_score(src: pd.DataFrame) -> pd.Series:
    bq = pd.to_numeric(src["B_quality"], errors="coerce")
    bq_min, bq_max = bq.min(), bq.max()
    bq_norm = ((bq - bq_min) / (bq_max - bq_min) * 100).fillna(0) if bq_max > bq_min else pd.Series(50.0, index=src.index)

    def _fa(a):
        if pd.isna(a): return 0
        a = int(a)
        if a == 0: return 0
        m = {1: 100, 2: 95, 3: 85, 4: 70, 5: 50}
        if a <= 5: return m.get(a, 0)
        if a <= 8: return 30
        return 10

    fa = pd.to_numeric(src["A_days"], errors="coerce").apply(_fa)
    c_rec = ((30 - pd.to_numeric(src["C_days"], errors="coerce").fillna(30).clip(0, 30)) * 3.3).clip(0, 100)

    fp = pd.to_numeric(src["foreign_profit_pct"], errors="coerce") if "foreign_profit_pct" in src.columns else pd.Series(pd.NA, index=src.index, dtype=float)
    fp_score = (100 - (fp - 5).abs() * 12).clip(0, 100).fillna(50)

    vr = pd.to_numeric(src["volume_ratio"], errors="coerce") if "volume_ratio" in src.columns else pd.Series(pd.NA, index=src.index, dtype=float)
    vr_norm = ((vr.clip(0.5, 3.0) - 0.5) / 2.5 * 100).clip(0, 100).fillna(50)

    return (bq_norm * 0.40 + fp_score * 0.25 + fa * 0.20 + vr_norm * 0.10 + c_rec * 0.05).round(1)


df["total_score"] = calc_total_score(df)


# ── Theme Hotness ─────────────────────────────────────────────────────────────
st.subheader("🌡️ 族群熱度")

hotness_rows = []
for theme in THEME_ORDER:
    tdf = df[df["theme"] == theme]
    if tdf.empty:
        continue
    avg_bq      = pd.to_numeric(tdf["B_quality"], errors="coerce").mean() or 0
    new_a_count = (pd.to_numeric(tdf["A_days"], errors="coerce").fillna(0) >= 1).sum()
    count       = len(tdf)
    hotness     = round(avg_bq * 0.6 + new_a_count * 8 + count * 0.1, 1)
    leader      = tdf.sort_values("total_score", ascending=False).iloc[0]
    hotness_rows.append({
        "theme":   theme,
        "label":   THEME_LABEL.get(theme, theme),
        "hotness": hotness,
        "avg_bq":  round(avg_bq, 1),
        "new_a":   int(new_a_count),
        "count":   int(count),
        "leader":  f"{leader['stock_id']} {leader['name']}",
        "leader_score": leader["total_score"],
        "leader_decision": leader.get("decision", "N/A"),
    })

hotness_df = pd.DataFrame(hotness_rows).sort_values("hotness", ascending=False).reset_index(drop=True)

h_cols = st.columns(min(len(hotness_df), 4))
for i, row in hotness_df.iterrows():
    col = h_cols[i % 4]
    dec_icon = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪"}.get(row["leader_decision"], "⚪")
    col.metric(
        label=row["label"],
        value=f"🌡️ {row['hotness']}",
        delta=f"龍頭 {dec_icon}{row['leader']}",
    )
    col.caption(f"B均質 {row['avg_bq']} ｜ A啟動 {row['new_a']} 支 ｜ 共 {row['count']} 支")

st.divider()


# ── AI Ranking Top 20 ─────────────────────────────────────────────────────────
st.subheader("🏆 AI Ranking Top 20（WAIT + BUY）")

top20 = (
    df[df["decision"].isin(["WAIT", "BUY"])]
    .sort_values("total_score", ascending=False)
    .head(20)
    .reset_index(drop=True)
)

if top20.empty:
    st.info("暫無 WAIT/BUY 訊號")
else:
    pinned = load_pinned()
    for rank, row in top20.iterrows():
        sid      = str(row["stock_id"])
        name     = str(row.get("name", sid))
        decision = str(row.get("decision", "N/A"))
        score    = row["total_score"]
        bq       = row.get("B_quality")
        a_days   = row.get("A_days")
        theme    = str(row.get("theme", ""))
        dec_icon = {"BUY": "🟢", "WAIT": "🟡"}.get(decision, "⚪")
        bq_str   = str(int(bq)) if pd.notna(bq) else "N/A"
        a_str    = str(int(a_days)) if pd.notna(a_days) else "N/A"

        r1, r2 = st.columns([8, 2])
        with r1:
            st.markdown(
                f"**#{rank+1}** {dec_icon} `{sid}` **{name}**　"
                f"總分 **{score}** ｜ B品質 {bq_str} ｜ A天 {a_str} ｜ {THEME_LABEL.get(theme, theme)}"
            )
        with r2:
            is_pinned = sid in pinned
            btn_label = "📌 已追蹤" if is_pinned else "☆ 追蹤"
            if st.button(btn_label, key=f"ai_top_pin_{sid}", use_container_width=True):
                if is_pinned:
                    pinned.discard(sid)
                else:
                    pinned.add(sid)
                save_pinned(pinned)
                st.rerun()

st.divider()


# ── 子產業 Tabs ───────────────────────────────────────────────────────────────
st.subheader("📂 子產業詳細")

present_themes = [t for t in THEME_ORDER if t in df["theme"].values]
tabs = st.tabs([THEME_LABEL.get(t, t) for t in present_themes])

for tab, theme in zip(tabs, present_themes):
    with tab:
        tdf = df[df["theme"] == theme].sort_values("total_score", ascending=False).reset_index(drop=True)
        if tdf.empty:
            st.caption("無資料")
            continue

        pinned = load_pinned()

        # 子族群小統計
        n_buy  = (tdf["decision"] == "BUY").sum()
        n_wait = (tdf["decision"] == "WAIT").sum()
        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("🟢 BUY",  n_buy)
        sc2.metric("🟡 WAIT", n_wait)
        sc3.metric("📊 合計",  len(tdf))

        for _, row in tdf.iterrows():
            sid      = str(row["stock_id"])
            name     = str(row.get("name", sid))
            segment  = str(row.get("segment", ""))
            decision = str(row.get("decision", "N/A"))
            score    = row["total_score"]
            bq       = row.get("B_quality")
            b_days   = row.get("B_days")
            a_days   = row.get("A_days")
            c_days   = row.get("C_days")
            purity   = row.get("ai_purity", "")
            notes    = str(row.get("notes", ""))
            dec_icon = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪"}.get(decision, "⚪")

            bq_str = str(int(bq))     if pd.notna(bq)     else "N/A"
            b_str  = str(int(b_days)) if pd.notna(b_days)  else "N/A"
            a_str  = str(int(a_days)) if pd.notna(a_days)  else "N/A"
            c_str  = str(int(c_days)) if pd.notna(c_days)  else "N/A"

            purity_stars = "★" * int(purity) if str(purity).isdigit() else ""

            col1, col2 = st.columns([8, 2])
            with col1:
                st.markdown(
                    f"{dec_icon} `{sid}` **{name}** ({segment})　"
                    f"總分 **{score}** ｜ B品質 {bq_str} ｜ B天 {b_str} ｜ A天 {a_str} ｜ C天 {c_str}　"
                    f"AI純度 {purity_stars}"
                )
                if notes:
                    st.caption(notes)
            with col2:
                is_pinned = sid in pinned
                btn_label = "📌 已追蹤" if is_pinned else "☆ 追蹤"
                if st.button(btn_label, key=f"ai_tab_{theme}_{sid}", use_container_width=True):
                    if is_pinned:
                        pinned.discard(sid)
                    else:
                        pinned.add(sid)
                    save_pinned(pinned)
                    st.rerun()
