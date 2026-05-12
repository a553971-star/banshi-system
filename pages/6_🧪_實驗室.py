"""
pages/6_🧪_實驗室.py
磐石 2.0 評分拆解實驗室 — 只做 UI 顯示，不改任何現有邏輯。
"""
import datetime
import os

import pandas as pd
import streamlit as st

_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="實驗室", layout="wide")
st.title("🧪 磐石 2.0 實驗室")
st.caption("評分拆解透視 — 只讀不改，不影響任何現有邏輯")

# ── 輸入 ───────────────────────────────────────────────────────────────────────
sid = st.text_input("股票代號", placeholder="例：2330", key="lab_query")

if not sid:
    st.info("輸入股票代號後開始分析")
    st.stop()

sid = sid.strip()

with st.spinner(f"分析 {sid} 中..."):
    try:
        from main import load_params
        from live_analyzer import process_stock_live
        params = load_params()
        r = process_stock_live(sid, params, print_snapshot=False)
    except Exception as e:
        st.error(f"分析失敗：{e}")
        st.stop()

if r is None:
    st.warning("查無資料或分析失敗，請確認代號是否正確")
    st.stop()

# ── 安全取值工具 ───────────────────────────────────────────────────────────────
def _n(key, default=0):
    try:
        raw = r.get(key)
        v = float(raw if raw is not None else default)
        return default if v != v else v  # NaN guard
    except (TypeError, ValueError):
        return default

def _s(key, default="—"):
    v = r.get(key)
    return str(v) if v is not None else default

def _i(key, default=0):
    try:
        return int(_n(key, default))
    except Exception:
        return default

# ── 擷取主要欄位 ───────────────────────────────────────────────────────────────
c_days   = _i("C_days")
b_days   = _i("B_days")
a_days   = _i("A_days")
bq       = _i("B_quality")
bw20     = _i("B_window_20")
vol_r    = _n("volume_ratio", 1.0)
kd_k     = _n("kd_k", 50)
adx      = _n("adx", 0)
cost     = _s("cost_level", "UNKNOWN")
flow     = _s("flow_status", "UNKNOWN")
b_valid  = _s("B_validity", "")
b_phase  = _s("B_phase", "")
fcb      = _i("foreign_consecutive_buy")
fp_pct   = _n("foreign_profit_pct", None)
f_cost   = _n("foreign_cost", None)
f_pos    = _n("foreign_position", None)
inst     = _s("institutional_state", "UNKNOWN")
ret_10d  = _n("return_10d", 0)
bias20   = _n("bias_ma20", 0)
margin5d   = _n("margin_change_5d", 0)
margin_ci  = _n("margin_consecutive_increase", None)
margin_pct = _n("margin_change_pct", None)
vol5prev = _n("volatility_5d_prev", 0)
conf     = _i("confidence")
decision = _s("decision", "N/A")

# ═══════════════════════════════════════════════════════════════════════════════
# Section 1 — FinalConfidenceScore 總覽
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("① FinalConfidenceScore 總覽")

# --- 分項計算（實驗性定義，不影響原系統） ---
# StructureScore
ss_c    = min(25, c_days * 3)        # C成熟最多 25
ss_b    = min(20, b_days * 2)        # B結構最多 20
ss_bq   = min(20, round(bq / 80 * 20, 1))  # B品質以80為滿分正規化 → 0-20
ss_vol  = 10 if 0.8 <= vol_r <= 2.5 else 0
ss_cool = 10 if a_days <= 2 else (5 if a_days <= 4 else 0)
structure_score = ss_c + ss_b + ss_bq + ss_vol + ss_cool

# FlowScore
fs_flow = {"ACCUMULATING": 25, "NEUTRAL": 12, "DISTRIBUTION": -10,
           "BREAKOUT": 20, "PREPARE": 5}.get(flow, 0)
fs_fcb  = min(15, fcb * 3)
fs_bw   = min(10, bw20 // 3)
flow_score = max(0, fs_flow + fs_fcb + fs_bw)

# CapitalBehaviorScore
cs_inst  = {"ACCUMULATION": 20, "SHAKEOUT": 10, "NEUTRAL": 5,
            "EXTENDED": 0, "DISTRIBUTION": -10}.get(inst, 0)
cs_bv    = {"TRUE_B": 15, "UNCERTAIN": 5, "FAKE_B": -5}.get(b_valid, 0)
cs_fp    = 10 if fp_pct is not None and -5 <= fp_pct <= 20 else 0
capital_score = max(0, cs_inst + cs_bv + cs_fp)

# RiskPenalty
rp_cost  = -20 if cost == "HIGH_RISK" else 0
rp_a     = -15 if a_days >= 5 else (-8 if a_days >= 3 else 0)
rp_ret   = -10 if ret_10d > 12 else (-5 if ret_10d > 8 else 0)
rp_dist  = -15 if flow == "DISTRIBUTION" else 0
risk_penalty = rp_cost + rp_a + rp_ret + rp_dist

# --- 正規化到 0-100 ---
_S_MAX = 85   # 25+20+20+10+10
_F_MAX = 50   # 25+15+10
_C_MAX = 45   # 20+15+10

s_norm = round(min(100, structure_score / _S_MAX * 100), 1)
f_norm = round(min(100, flow_score      / _F_MAX * 100), 1)
c_norm = round(min(100, capital_score   / _C_MAX * 100), 1)

# FinalScore = S×0.40 + F×0.25 + C×0.25，再減去風險扣分
_base        = s_norm * 0.40 + f_norm * 0.25 + c_norm * 0.25
_risk_deduct = round(abs(risk_penalty) * 0.10, 1)
final_score  = round(max(0, min(100, _base - _risk_deduct)), 1)

dec_icon = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪", "SELL": "🔴"}.get(decision, "⚪")
st.markdown(f"### {dec_icon} `{sid}` {r.get('name','')}　**{decision}**　信心分數 **{conf}**")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("🏗 Structure",  f"{s_norm}",  help=f"原始={structure_score}，滿分{_S_MAX}正規化")
col2.metric("🌊 Flow",       f"{f_norm}",  help=f"原始={flow_score}，滿分{_F_MAX}正規化")
col3.metric("💰 Capital",    f"{c_norm}",  help=f"原始={capital_score}，滿分{_C_MAX}正規化")
col4.metric("⚠️ Risk 扣分",  f"-{_risk_deduct}",  help=f"罰分={risk_penalty}，×0.10後從加權分中扣除")
col5.metric("📊 加權合計",   f"{final_score}",
            help="(S×0.40 + F×0.25 + C×0.25) − Risk扣分，滿分100")

st.caption("⚠️ 以上分項為實驗室自定義拆解，與原系統 confidence 計算方式不同，僅供參考。")

# ═══════════════════════════════════════════════════════════════════════════════
# Section 2 — StructureScore 拆解表格
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("② StructureScore 拆解")

rows = [
    {
        "條件":   "C天成熟",
        "狀態":   "✅ 已成熟" if c_days >= 5 else f"⚠️ 仍在發展（{c_days}天）",
        "得分":   ss_c,
        "原始資料": f"C_days = {c_days}",
        "門檻":   "≥ 5天",
        "達標":   "✅" if c_days >= 5 else "❌",
    },
    {
        "條件":   "B結構形成",
        "狀態":   "✅ 結構完整" if b_days >= 5 else f"⚠️ 尚在整理（{b_days}天）",
        "得分":   ss_b,
        "原始資料": f"B_days = {b_days}",
        "門檻":   "≥ 5天",
        "達標":   "✅" if b_days >= 5 else "❌",
    },
    {
        "條件":   "B品質",
        "狀態":   "✅ 品質良好" if bq >= 60 else f"⚠️ 品質偏低（{bq}）",
        "得分":   ss_bq,
        "原始資料": f"B_quality = {bq}",
        "門檻":   "≥ 60",
        "達標":   "✅" if bq >= 60 else "❌",
    },
    {
        "條件":   "量價健康",
        "狀態":   "✅ 量比正常" if 0.8 <= vol_r <= 2.5 else f"⚠️ 量比異常（{vol_r:.2f}）",
        "得分":   ss_vol,
        "原始資料": f"volume_ratio = {vol_r:.2f}",
        "門檻":   "0.8 ~ 2.5",
        "達標":   "✅" if 0.8 <= vol_r <= 2.5 else "❌",
    },
    {
        "條件":   "結構未過熱",
        "狀態":   "✅ 尚未過熱" if a_days <= 2 else f"⚠️ A延伸（{a_days}天）",
        "得分":   ss_cool,
        "原始資料": f"A_days = {a_days}",
        "門檻":   "A ≤ 2",
        "達標":   "✅" if a_days <= 2 else "❌",
    },
]

st.table(pd.DataFrame(rows).set_index("條件"))

# ═══════════════════════════════════════════════════════════════════════════════
# Section 3 — SmartMoney 燈號
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("③ SmartMoney 燈號")
st.caption(f"DEBUG margin_change_pct raw: {r.get('margin_change_pct')}")

# ok=None → 資料不足，不計入 pass/total
sm_conds = [
    ("股價距半年低點 < 20%",  bias20 <= 20,  f"bias_ma20 = {bias20:.1f}%",          "≤ 20%"),
    ("融資連增 ≥ 3天",  (margin_ci  >= 3)  if margin_ci  is not None else None,
                       f"margin_consecutive_increase = {int(margin_ci) if margin_ci is not None else 'N/A'}",  "≥ 3"),
    ("融資增幅 > 5%",  (margin_pct > 5)   if margin_pct is not None else None,
                       f"margin_change_pct = {margin_pct:.1f}%" if margin_pct is not None else "N/A",         "> 5%"),
    ("量回溫（量比 ≥ 0.8）",  vol_r >= 0.8,  f"volume_ratio = {vol_r:.2f}",          "≥ 0.8"),
    ("成本位 SAFE",           cost == "SAFE", f"cost_level = {cost}",                 "SAFE"),
    ("KD K值 < 70",           kd_k < 70,     f"kd_k = {kd_k:.1f}",                  "< 70"),
]

passed = sum(1 for _, ok, _, _ in sm_conds if ok is True)
total  = sum(1 for _, ok, _, _ in sm_conds if ok is not None)

if total > 0 and passed == total:
    lamp = "🔴 SmartMoney 強烈啟動"
elif total > 0 and passed >= total - 1:
    lamp = "🟠 SmartMoney 部分啟動"
elif passed >= 2:
    lamp = "🟡 SmartMoney 弱訊號"
else:
    lamp = "⚪ SmartMoney 未啟動"

_n_pending = sum(1 for _, ok, _, _ in sm_conds if ok is None)
st.markdown(f"### {lamp}　（{passed}/{total} 條件達標" + (f"，{_n_pending} 條待補）" if _n_pending else "）"))

for label, ok, raw, threshold in sm_conds:
    if ok is None:
        icon = "⚠️"
        st.markdown(f"{icon} **{label}**　`{raw}`　門檻：{threshold}　_（資料不足，待補）_")
    else:
        icon = "✅" if ok else "❌"
        st.markdown(f"{icon} **{label}**　`{raw}`　門檻：{threshold}")

if passed < 2:
    st.warning("⚪ SmartMoney 品質過濾器未通過（有效條件達標不足 2 項）")

# ═══════════════════════════════════════════════════════════════════════════════
# Section 4 — ForeignContextTag（背景資訊，不計分）
# ═══════════════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("④ ForeignContextTag（背景資訊，不計分）")
st.caption("以下僅提供法人行為背景，不影響任何評分。")

fc1, fc2, fc3, fc4 = st.columns(4)
fc1.metric("外資成本估計",   f"{f_cost:.1f}" if f_cost else "N/A")
fc2.metric("外資連買天數",   f"{fcb} 天" if fcb else "0 天")
fc3.metric("外資獲利%",
           f"{fp_pct:.1f}%" if fp_pct is not None else "N/A",
           help="外資平均成本相對現價的獲利估算")
fc4.metric("法人狀態",       inst)

cred_map = {
    "ACCUMULATION":  ("🟢 高可信度", "外資持續建倉，訊號可信"),
    "SHAKEOUT":      ("🟡 中可信度", "洗盤整理，結構尚完整"),
    "NEUTRAL":       ("🟡 中可信度", "中性，觀望為主"),
    "EXTENDED":      ("🟠 低可信度", "延伸過高，追高風險大"),
    "DISTRIBUTION":  ("🔴 低可信度", "外資出貨，結構惡化"),
    "UNKNOWN":       ("⚪ 無資料",   "無法判斷法人行為"),
}
cred_label, cred_desc = cred_map.get(inst, ("⚪ 無資料", "—"))
st.info(f"{cred_label}　{cred_desc}")

if f_pos:
    st.caption(f"外資持倉估計：{int(f_pos):,} 張")
