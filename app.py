"""
app.py — 磐石決策系統戰情室（Phase 3-1 骨架版）
執行：streamlit run app.py
讀取：latest_decisions.csv, state_log.csv, watchlist_overrides.json
"""

import csv
import datetime
import json
import os

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from bible_loader import get_daily_verse
from live_analyzer import process_stock_live

def _to_num(val, default=0.0):
    """安全轉 float，NaN / None / 非數字 → default"""
    try:
        v = float(val)
        return default if (v != v) else v  # NaN → default
    except (TypeError, ValueError):
        return default

def explain_metrics(result):
    try:
        c    = _to_num(result.get("C_days"))
        b    = _to_num(result.get("B_days"))
        a    = _to_num(result.get("A_days"))
        adx  = _to_num(result.get("adx"),   default=None)
        k    = _to_num(result.get("kd_k"),  default=None)
        cost = result.get("cost_level")

        lines = []
        lines.append("C " + ("底部已形成 ✅" if c >= 5 else "尚未止跌 ⚠️"))
        lines.append("B " + ("主力可能建倉 ✅" if b >= 2 else "無整理，結構不完整 ⚠️"))
        if a >= 5:
            lines.append("A 已延伸，追高風險 🔴")
        elif a >= 1:
            lines.append("A 剛啟動 🟡")
        else:
            lines.append("A 尚未發動")
        if adx is not None:
            lines.append("ADX " + ("有趨勢 ✅" if adx >= 25 else ("趨勢不明" if adx >= 20 else "無趨勢 ⚠️")))
        if k is not None:
            lines.append("KD " + ("過熱區 🔴" if k >= 80 else ("超賣區 🟢" if k <= 20 else "正常區")))
        if cost == "HIGH_RISK":
            lines.append("成本 位置偏高，風險大 🔴")
        elif cost == "SAFE":
            lines.append("成本 位置合理 ✅")

        if c >= 5 and b >= 2 and a <= 2:
            coach = "👉 結構完整，值得關注"
        elif a >= 5 or cost == "HIGH_RISK":
            coach = "👉 結構偏晚或成本過高，不值得出手"
        elif b == 0:
            coach = "👉 無B段，結構不完整，等待"
        else:
            coach = "👉 結構未成熟，繼續觀察"

        return lines, coach
    except Exception:
        return [], "👉 資料不足，無法判讀"
from data_fetcher_fm import fetch_stock_data

_DIR = os.path.dirname(os.path.abspath(__file__))
_DECISIONS_PATH = os.path.join(_DIR, "latest_decisions.csv")
_STATE_LOG_PATH = os.path.join(_DIR, "state_log.csv")
_OVERRIDES_PATH = os.path.join(_DIR, "watchlist_overrides.json")
PIN_PATH = os.path.join(_DIR, "pinned.json")
TRADES_PATH = os.path.join(_DIR, "trades_log.csv")
_CUSTOM_WL_PATH = os.path.join(_DIR, "watchlist_custom.json")


def add_to_custom_watchlist(stock_id: str) -> None:
    try:
        with open(_CUSTOM_WL_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = []
    if stock_id not in data:
        data.append(stock_id)
        with open(_CUSTOM_WL_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)


# ── 資料載入 ──────────────────────────────────────────────────────────────────

def load_latest_decisions(path: str = _DECISIONS_PATH) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def load_state_log(path: str = _STATE_LOG_PATH) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str)
    except Exception:
        return pd.DataFrame()


def load_watchlist_overrides(path: str = _OVERRIDES_PATH) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_watchlist_overrides(overrides: dict, path: str = _OVERRIDES_PATH) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)


def load_pinned() -> set:
    try:
        with open(PIN_PATH, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            return set()
    except Exception:
        return set()


def save_pinned(pinned: set) -> None:
    with open(PIN_PATH, "w") as f:
        json.dump(list(pinned), f)


# ── 交易紀錄 ──────────────────────────────────────────────────────────────────

def log_trade(row: pd.Series, action: str) -> None:
    file_exists = os.path.exists(TRADES_PATH)
    timestamp = datetime.datetime.now().isoformat()
    with open(TRADES_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "date", "stock_id", "action", "price", "reason",
                "decision", "signal_type", "C_days", "B_days", "A_days",
                "flow", "cost", "confidence",
            ])
        writer.writerow([
            timestamp,
            row.get("date", ""), row.get("stock_id", ""),
            action, "", "",
            row.get("decision", ""), row.get("signal_type", ""),
            row.get("C_days", ""), row.get("B_days", ""), row.get("A_days", ""),
            row.get("flow_status", ""), row.get("cost_level", ""),
            row.get("confidence", ""),
        ])


def analyze_mistakes() -> None:
    if not os.path.exists(TRADES_PATH):
        st.info("尚無交易紀錄")
        return
    try:
        df = pd.read_csv(TRADES_PATH)
    except Exception:
        st.error("交易紀錄檔損壞")
        return
    if df.empty:
        st.info("尚無資料")
        return
    st.write(f"總筆數：{len(df)}")
    st.write(f"BUY：{(df['action'] == 'BUY').sum()}")
    st.write(f"SELL：{(df['action'] == 'SELL').sum()}")
    st.write(f"SKIP：{(df['action'] == 'SKIP').sum()}")
    df["A_days"] = pd.to_numeric(df["A_days"], errors="coerce")
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    chase = df[(df["A_days"] >= 5) & (df["action"] == "BUY")]
    low_conf = df[(df["confidence"] < 60) & (df["action"] == "BUY")]
    high_conf_skip = df[(df["confidence"] >= 75) & (df["action"] == "SKIP")]
    st.write(f"追高次數（A≥5還買）：{len(chase)}")
    st.write(f"低信心硬買：{len(low_conf)}")
    st.write(f"錯過高信心機會：{len(high_conf_skip)}")


def analyze_winrate() -> None:
    if not os.path.exists(TRADES_PATH):
        st.info("尚無交易紀錄")
        return
    try:
        trades = pd.read_csv(TRADES_PATH)
    except Exception:
        st.error("交易紀錄檔損壞")
        return
    trades = trades[trades["action"] == "BUY"].copy()
    if trades.empty:
        st.info("尚無 BUY 紀錄")
        return
    try:
        decisions = pd.read_csv(_DECISIONS_PATH)
    except Exception:
        st.error("decisions 檔案損壞")
        return
    st.caption("⚠️ 使用最新資料估算，僅供參考（非完整歷史回測）")
    decisions["date"] = pd.to_datetime(decisions["date"], errors="coerce")
    trades["date"] = pd.to_datetime(trades["date"], errors="coerce")
    results = []
    for _, row in trades.iterrows():
        stock = str(row["stock_id"])
        buy_date = row["date"]
        df = decisions[decisions["stock_id"].astype(str) == stock].copy()
        df = df.sort_values("date")
        df = df[df["date"] >= buy_date]
        if len(df) < 6:
            continue
        try:
            buy_price = float(df.iloc[0].get("close"))
            price_3d = float(df.iloc[3].get("close"))
            price_5d = float(df.iloc[5].get("close"))
        except Exception:
            continue
        if pd.isna(buy_price) or buy_price == 0:
            continue
        r3 = (price_3d - buy_price) / buy_price * 100
        r5 = (price_5d - buy_price) / buy_price * 100
        results.append({"stock": stock, "r3": r3, "r5": r5})
    if not results:
        st.info("資料不足（需至少6天後續資料）")
        return
    df_res = pd.DataFrame(results)
    win3 = (df_res["r3"] > 0).mean() * 100
    win5 = (df_res["r5"] > 0).mean() * 100
    avg3 = df_res["r3"].mean()
    avg5 = df_res["r5"].mean()
    st.write(f"有效交易筆數：{len(df_res)}")
    st.write(f"3日勝率：{win3:.1f}%　平均報酬：{avg3:.2f}%")
    st.write(f"5日勝率：{win5:.1f}%　平均報酬：{avg5:.2f}%")


# ── 分類邏輯 ──────────────────────────────────────────────────────────────────

def classify_rows(
    df: pd.DataFrame, overrides: dict
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    def _int_or_none(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    action_mask = df["decision"] == "BUY"

    override_ids = {k for k, v in overrides.items() if v}
    in_overrides = df["stock_id"].isin(override_ids)
    watchlist_mask = ((df["decision"] == "WAIT") | in_overrides) & ~action_mask

    c_days_numeric = df["C_days"].apply(_int_or_none)
    candidate_mask = (
        (df["decision"] == "IGNORE")
        & (c_days_numeric >= 5).fillna(False)
        & ~action_mask
        & ~watchlist_mask
    )

    return df[action_mask].copy(), df[watchlist_mask].copy(), df[candidate_mask].copy()


# ── 表格欄位整理 ──────────────────────────────────────────────────────────────

def _fmt_days(val) -> str:
    try:
        return str(int(val))
    except (TypeError, ValueError):
        return "-"


def _fmt_trajectory(row: pd.Series) -> str:
    parts = []
    c = _fmt_days(row.get("C_days"))
    b = _fmt_days(row.get("B_days"))
    a = _fmt_days(row.get("A_days"))
    if c != "-":
        parts.append(f"C{c}")
    if b != "-":
        parts.append(f"B{b}")
    if a != "-":
        parts.append(f"A{a}")
    return " ".join(parts) if parts else "-"


def _clean(val) -> str:
    s = str(val)
    return "-" if s in ("nan", "None", "", "N/A") else s


def build_display_row(row: pd.Series) -> dict:
    stock_id = str(row.get("stock_id", ""))
    name = str(row.get("name", ""))
    stock_label = f"{stock_id} {name}".strip()

    try:
        conf = int(row.get("confidence", 0))
    except (TypeError, ValueError):
        conf = "-"

    return {
        "股票":  stock_label,
        "決策":  _clean(row.get("decision")),
        "軌跡":  _fmt_trajectory(row),
        "資金流": _clean(row.get("flow_status")),
        "成本位": _clean(row.get("cost_level")),
        "信心":  conf,
        "C天":   _fmt_days(row.get("C_days")),
        "B天":   _fmt_days(row.get("B_days")),
        "A天":   _fmt_days(row.get("A_days")),
    }


def build_display_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["股票", "決策", "軌跡", "資金流", "成本位", "信心", "C天", "B天", "A天"])
    rows = [build_display_row(row) for _, row in df.iterrows()]
    return pd.DataFrame(rows)


def style_decision_table(df: pd.DataFrame):
    def style_row(row):
        decision   = row.get("決策", "")
        confidence = row.get("信心", 0)
        bg = ""
        if decision == "BUY":
            bg = "background-color: #d4edda;"
        elif decision == "WAIT":
            bg = "background-color: #fff3cd;"
        elif decision == "IGNORE":
            bg = "background-color: #e2e3e5;"
        weight = "font-weight: bold;" if isinstance(confidence, int) and confidence >= 75 else ""
        return [bg + weight] * len(row)
    return df.style.apply(style_row, axis=1)


# ── AI Snapshot ───────────────────────────────────────────────────────────────

def build_ai_snapshot(row: pd.Series) -> str:
    try:
        stock_id = str(row.get("stock_id", ""))
        name = str(row.get("name", ""))
        decision = str(row.get("decision", ""))
        trajectory = _fmt_trajectory(row)
        flow_status = str(row.get("flow_status", ""))
        cost_level = str(row.get("cost_level", ""))
        try:
            confidence = int(row.get("confidence", 0))
        except (TypeError, ValueError):
            confidence = "-"

        header = f"【{stock_id} {name}】{decision} | {trajectory} | {flow_status} | {cost_level} | 信心 {confidence}"

        explanation = str(row.get("explanation", ""))
        if explanation and explanation != "nan":
            return f"{header}\n說明：{explanation}"
        return header
    except Exception:
        return ""


# ── 每日聖經經文 ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=86400)
def _cached_daily_verse(date_str: str) -> str:
    text, reference = get_daily_verse(date_str)
    return f"「{text}」\n— {reference}"


# ── 工具函式 ─────────────────────────────────────────────────────────────────

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return "-"

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return "-"

def _safe_round(x):
    return round(x, 1) if x is not None else "N/A"

def safe_str(x):
    if x is None:
        return "-"
    s = str(x)
    return "-" if s.lower() == "nan" or s.strip() == "" else s


# ── 情緒雷達 helpers ───────────────────────────────────────────────────────────

def get_c_arrow(today_c, prev):
    if prev is None: return ""
    if today_c > prev["C_days"]: return "↑"
    elif today_c == prev["C_days"]: return "→"
    else: return "↓"

def get_warning(row, prev):
    if prev is None: return ""
    if row["C_days"] == prev["C_days"] and row["A_days"] >= 2:
        return "🟡 動能停滯"
    if row["flow_status"] == "NEUTRAL" and prev["flow_status"] in ["ACCUMULATING", "INFLOW"]:
        return "🟡 資金轉弱"
    if row["C_days"] < prev["C_days"]:
        return "🟠 動能下降"
    if row["C_days"] == 0 and prev["C_days"] >= 2:
        return "🔴 發動失敗"
    return ""

def get_action_signal(warning, level):
    if warning == "🔴 發動失敗": return "賣掉"
    elif warning == "🟠 動能下降": return "賣一半"
    elif warning in ["🟡 動能停滯", "🟡 資金轉弱"]: return "先不要買"
    elif level == "🚀 過熱": return "不要買／慢慢賣"
    elif level == "🔥 強爆": return "可以抱"
    elif level == "⚠️ 初爆": return "小買"
    return "等"

def get_coach_message(warning, level):
    if warning == "🔴 發動失敗": return "🔴 快逃！現在要賣掉，不要等！"
    elif warning == "🟠 動能下降": return "🟠 開始變弱了，先賣一半，剩下觀察！"
    elif warning == "🟡 動能停滯": return "🟡 有點怪怪的，先不要買，看看再說"
    elif warning == "🟡 資金轉弱": return "🟡 大人開始不買了，先等等不要動"
    elif level == "🚀 過熱": return "🔴 太熱了！不要追，手上有的可以慢慢賣"
    elif level == "🔥 強爆": return "🟢 很強，可以抱著，但要準備隨時賣"
    elif level == "⚠️ 初爆": return "🟡 剛開始，可以小小試試看，不要買太多"
    return "⚪ 先觀察，不要亂動"


# ── 搜尋層 ────────────────────────────────────────────────────────────────────

def search_stocks(df: pd.DataFrame, query: str) -> pd.DataFrame:
    if not query or df.empty:
        return pd.DataFrame()
    q = query.strip().lower()
    mask = (
        df["stock_id"].astype(str).str.lower().str.contains(q, na=False)
        | df["name"].astype(str).str.lower().str.contains(q, na=False)
    )
    return df[mask].copy()


def build_decision_view(row: pd.Series) -> dict:
    return {
        "決策": safe_str(row.get("decision")),
        "信心": safe_int(row.get("confidence")),
        "signal_type": safe_str(row.get("signal_type")),
        "C天": safe_int(row.get("C_days")),
        "B天": safe_int(row.get("B_days")),
        "A天": safe_int(row.get("A_days")),
        "Flow": safe_str(row.get("flow_status")),
        "Cost": safe_str(row.get("cost_level")),
    }


def build_indicator_view(row: pd.Series) -> dict:
    return {
        "ADX": safe_float(row.get("adx")),
        "ATR": safe_float(row.get("atr")),
        "VWAP": safe_float(row.get("vwap")),
        "KD_K": safe_float(row.get("kd_k")),
        "KD_D": safe_float(row.get("kd_d")),
        "BB上軌": safe_float(row.get("bb_upper")),
        "BB中軌": safe_float(row.get("bb_middle")),
        "BB下軌": safe_float(row.get("bb_lower")),
    }


def build_basic_info_view(row: pd.Series) -> dict:
    return {
        "股票代號": safe_str(row.get("stock_id")),
        "名稱": safe_str(row.get("name")),
        "資料日期": safe_str(row.get("date")),
    }


def render_stock_search_section(df: pd.DataFrame) -> None:

    # ── 強B排行榜 ─────────────────────────────────────────────────────
    st.divider()
    st.subheader("🔥 強B排行榜（主力建倉候選）")
    try:
        from b_ranker import get_top_strong_B
        top_b = get_top_strong_B(df, top_n=5)
        if top_b.empty:
            st.info("今日無強B候選")
        else:
            for _, row in top_b.iterrows():
                stock = row.get("stock_id","")
                name  = row.get("name","")
                score = int(row.get("B_score",0))
                B     = row.get("B_days","")
                flow  = row.get("flow_status","")
                cost  = row.get("cost_level","")
                st.markdown(f"**🟢 {stock} {name}｜分數 {score}**")
                st.caption(f"B={B}｜Flow={flow}｜Cost={cost}")
    except Exception as e:
        st.warning(f"強B排行榜載入失敗：{e}")

    st.subheader("🔍 個股查詢")
    query = st.text_input("輸入股票代號或名稱", key="detail_search")
    if not query:
        return
    results = search_stocks(df, query)
    if results.empty:
        st.warning("查無此股票")
        return
    for idx, (_, row) in enumerate(results.iterrows()):
        stock_id = str(row.get("stock_id", ""))
        name = safe_str(row.get("name"))
        label = f"{stock_id} {name}"
        line1 = (
            f"【決策】{safe_str(row.get('decision'))}｜信心：{safe_int(row.get('confidence'))}｜"
            f"signal_type：{safe_str(row.get('signal_type'))}｜"
            f"C天：{safe_int(row.get('C_days'))}｜B天：{safe_int(row.get('B_days'))}｜"
            f"A天：{safe_int(row.get('A_days'))}｜Flow：{safe_str(row.get('flow_status'))}｜"
            f"Cost：{safe_str(row.get('cost_level'))}"
        )
        line2 = (
            f"【指標】ADX：{safe_float(row.get('adx'))}｜ATR：{safe_float(row.get('atr'))}｜"
            f"VWAP：{safe_float(row.get('vwap'))}｜KD_K：{safe_float(row.get('kd_k'))}｜"
            f"KD_D：{safe_float(row.get('kd_d'))}｜BB上軌：{safe_float(row.get('bb_upper'))}｜"
            f"BB中軌：{safe_float(row.get('bb_middle'))}｜BB下軌：{safe_float(row.get('bb_lower'))}"
        )
        line3 = f"【日期】{safe_str(row.get('date'))}"

        st.text(label)
        st.text(line1)
        st.text(line2)
        st.text(line3)

        col1, col2, col3 = st.columns(3)
        ai_key = f"ai_{idx}"
        with col3:
            if st.button("🤖 AI 分析", key=ai_key):
                st.session_state[f"show_ai_{idx}"] = True

        if st.session_state.get(f"show_ai_{idx}", False):
            decision    = safe_str(row.get("decision"))
            confidence  = safe_int(row.get("confidence"))
            signal_type = safe_str(row.get("signal_type"))
            C           = safe_int(row.get("C_days"))
            B           = safe_int(row.get("B_days"))
            A           = safe_int(row.get("A_days"))
            flow        = safe_str(row.get("flow_status"))
            cost        = safe_str(row.get("cost_level"))
            adx         = safe_float(row.get("adx"))
            atr         = safe_float(row.get("atr"))
            vwap        = safe_float(row.get("vwap"))
            kd_k        = safe_float(row.get("kd_k"))
            kd_d        = safe_float(row.get("kd_d"))
            bb_u        = safe_float(row.get("bb_upper"))
            bb_m        = safe_float(row.get("bb_middle"))
            bb_l        = safe_float(row.get("bb_lower"))
            date        = safe_str(row.get("date"))
            _b_type = result.get("B_type", "N/A") or "N/A"
            _b_text = result.get("B_text", "") or ""
            _f_cost = str(result.get("foreign_cost", "N/A"))
            _f_pos  = str(result.get("foreign_position", "N/A"))
            _f_prof = str(result.get("foreign_profit_pct", "N/A"))
            _i_state = result.get("institutional_state", "N/A") or "N/A"
            _i_text  = result.get("institutional_text", "") or ""
            prompt = f"""你是專業短線交易員 + 市場分析師，擅長結構分析、資金流、成本位與事件驅動判讀。
請用「結構優先、消息輔助、橫向比較」的原則分析以下股票。

⚠️ 核心規則：
- 絕對不推翻盤石決策
- 結構 > 指標 > 消息 > 橫向比較（優先順序不可顛倒）
- 消息面必須附來源與可信度，禁止虛構新聞
- 若無法確認來源，必須寫「未找到可靠消息來源」
- 若資料不足，請明確標註「資料不足」

【股票】{stock_id} {name}

【盤石決策】
決策：{decision}｜信心：{confidence}｜型態：{signal_type}
C天：{C}｜B天：{B}｜A天：{A}
Flow：{flow}｜Cost：{cost}
結構品質：{_b_type}（{_b_text})
B_window_20：{result.get("B_window_20", "N/A")}（近20日符合B條件天數）
B_quality：{result.get("B_quality", "N/A")}（建倉強度分數，越高越強）
主力成本：{_f_cost}｜持倉：{_f_pos}張｜主力獲利：{_f_prof}%
主力狀態：{_i_state}（{_i_text})

【技術指標】
ADX：{adx}｜ATR：{atr}｜VWAP：{vwap}
KD：{kd_k}/{kd_d}
布林：上 {bb_u} / 中 {bb_m} / 下 {bb_l}

【日期】{date}

請依序回答：

【1️⃣ 結構階段】
- 階段：起漲 / 發動初期 / 延伸 / 末段 / 情緒段
- 是否「無B直接A」：是 / 否

【2️⃣ 結構 vs 消息】
（A）結構面（必填）
- C/B/A 狀態與可持續性
- Flow / Cost 判讀

（B）消息面（嚴格規範）
- 是否有明確消息：是 / 否
若「是」：
  消息內容：
  來源：
  日期：
  可信度：高 / 中 / 低
  與股價關聯性：強 / 中 / 弱
若「否」：
  未找到可靠消息來源，可能為資金短期推動
⚠️ 禁止模糊來源

【3️⃣ 橫向比較（同產業 2~3 檔）】
比較維度：結構強度、資金流、成本位、技術位置、近期消息

（1）同業排序（由強到弱）
1️⃣
2️⃣
3️⃣

（2）本股定位：領先股 / 跟隨股 / 補漲股 / 情緒股

（3）是否有更好的替代標的？（有/沒有 + 說明）

【4️⃣ 風險評估】
- 風險等級：低 / 中 / 高
- 最大風險來源（只能一點）

【5️⃣ 時間框架】
- 短線（1~5天）
- 中線（1~4週）
- 長線（1~3月）

【6️⃣ 操作屬性】
結構股 / 情緒股 / 題材股（選一，說明理由）

【7️⃣ 最終結論】
👉 類型：
👉 階段：
👉 同業位置：
👉 風險：
👉 是否為消息驅動：是 / 否 / 不確定
👉 如果只能選一檔，會不會選這檔？（會/不會 + 一句理由）

回答風格：專業直接，像交易員開會報告，禁止空話。
"""
            st.code(prompt)

        key_id = f"{stock_id}_{row.get('date', '')}"
        log_key = f"logged_{key_id}"
        if log_key not in st.session_state:
            st.session_state[log_key] = False
        if not st.session_state[log_key]:
            bcol1, bcol2, bcol3 = st.columns(3)
            if bcol1.button("🟢 買入", key=f"buy_{key_id}"):
                log_trade(row, "BUY")
                st.session_state[log_key] = True
                st.success("已記錄 BUY")
            if bcol2.button("🔴 賣出", key=f"sell_{key_id}"):
                log_trade(row, "SELL")
                st.session_state[log_key] = True
                st.success("已記錄 SELL")
            if bcol3.button("⚪ 忽略", key=f"skip_{key_id}"):
                log_trade(row, "SKIP")
                st.session_state[log_key] = True
                st.success("已記錄 SKIP")
        else:
            st.caption("✅ 已記錄本日操作")

        # ── 即時行情 + 法人動向 ──────────────────────────────────────────
        fm_key = f"show_fm_{idx}"
        if st.button("📡 即時行情", key=f"fm_{idx}"):
            st.session_state[fm_key] = not st.session_state.get(fm_key, False)

        if st.session_state.get(fm_key, False):
            with st.spinner(f"抓取 {stock_id} 資料中..."):
                try:
                    fm_df = fetch_stock_data(stock_id, days=60)
                except EnvironmentError as e:
                    st.error(str(e))
                    fm_df = None

            if fm_df is not None and not fm_df.empty:
                latest = fm_df.iloc[-1]
                prev   = fm_df.iloc[-2] if len(fm_df) >= 2 else latest
                chg    = (latest["close"] - prev["close"]) / prev["close"] * 100
                icon   = "🔴" if chg > 0 else ("🟢" if chg < 0 else "⬜")

                st.markdown(f"#### {icon} {stock_id} 即時行情　{chg:+.2f}%")
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("收盤", f"{latest['close']:.2f}")
                m2.metric("最高", f"{latest['high']:.2f}")
                m3.metric("最低", f"{latest['low']:.2f}")
                m4.metric("成交量", f"{int(latest['volume']):,}")
                st.line_chart(fm_df.set_index("date")["close"])

                st.markdown("#### 👥 法人動向（近20日）")
                try:
                    import requests
                    TOKEN = os.getenv("FINMIND_TOKEN")
                    start = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y-%m-%d")

                    def _fm_fetch(dataset):
                        r = requests.get(
                            "https://api.finmindtrade.com/api/v4/data",
                            params={"dataset": dataset, "data_id": stock_id,
                                    "start_date": start, "token": TOKEN},
                            timeout=15,
                        )
                        d = r.json()
                        return pd.DataFrame(d["data"]) if d.get("status") == 200 and d.get("data") else pd.DataFrame()

                    fi_df = _fm_fetch("TaiwanStockInstitutionalInvestorsBuySell")
                    mg_df = _fm_fetch("TaiwanStockMarginPurchaseShortSale")

                    if not fi_df.empty:
                        fi_df["date"] = pd.to_datetime(fi_df["date"])
                        fi_df = fi_df.sort_values("date")
                        foreign = fi_df[fi_df["name"] == "Foreign_Investor"]
                        trust   = fi_df[fi_df["name"] == "Investment_Trust"]
                        fa1, fa2 = st.columns(2)
                        if not foreign.empty:
                            f_net = int(foreign.iloc[-1].get("buy", 0)) - int(foreign.iloc[-1].get("sell", 0))
                            fa1.metric("外資最新", f"{f_net:+,} 張")
                        if not trust.empty:
                            t_net = int(trust.iloc[-1].get("buy", 0)) - int(trust.iloc[-1].get("sell", 0))
                            fa2.metric("投信最新", f"{t_net:+,} 張")

                    if not mg_df.empty:
                        mg_df["date"] = pd.to_datetime(mg_df["date"])
                        mg_df = mg_df.sort_values("date")
                        m_last = mg_df.iloc[-1]
                        mg_net = int(m_last.get("MarginPurchaseBuy", 0)) - int(m_last.get("MarginPurchaseSell", 0))
                        st.metric("融資增減", f"{mg_net:+,} 張")

                except Exception as e:
                    st.caption(f"法人資料抓取失敗：{e}")
            elif fm_df is not None:
                st.warning("查無即時資料")


# ── UI 輔助函式 ───────────────────────────────────────────────────────────────

def format_decision_label(decision: str) -> str:
    if decision == "BUY":
        return "🟢 BUY"
    elif decision == "WAIT":
        return "🟡 WAIT"
    elif decision == "IGNORE":
        return "⚪ IGNORE"
    return decision


def format_signal_type(signal_type: str) -> str:
    if signal_type == "FAST_BREAKOUT":
        return "🚀 FAST"
    elif signal_type == "STANDARD":
        return "📈 STD"
    return "-"


def render_confidence_bar(conf) -> str:
    try:
        conf = int(conf)
    except Exception:
        conf = 0
    conf = max(0, min(100, conf))
    color = "#28a745" if conf >= 75 else "#ffc107" if conf >= 50 else "#6c757d"
    return (
        f'<div style="background:#222;border-radius:6px;height:8px;width:120px;">'
        f'<div style="background:{color};width:{conf}%;height:8px;border-radius:6px;"></div></div>'
    )


def _row_html(d: dict, signal_type_str: str) -> str:
    return (
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'padding:6px 0;border-bottom:1px solid #333;">'
        f'<div style="flex:3;">{d["股票"]} ｜ {format_decision_label(d["決策"])} ｜ '
        f'{format_signal_type(signal_type_str)} ｜ {d["軌跡"]}<br>'
        f'<small>{d["資金流"]} ｜ {d["成本位"]}</small></div>'
        f'<div style="flex:1;text-align:right;">{d["信心"]}<br>'
        f'{render_confidence_bar(d["信心"])}</div>'
        f'</div>'
    )


# ── 狀態變化偵測 ─────────────────────────────────────────────────────────────

def detect_state_change(prev: str, curr: str):
    if prev == "WAIT" and curr == "BUY":
        return ("🟢 轉強", "WAIT → BUY")
    if prev == "BUY" and curr == "IGNORE":
        return ("🔴 轉弱", "BUY → IGNORE")
    if prev == "IGNORE" and curr == "WAIT":
        return ("🟡 結構形成", "IGNORE → WAIT")
    return None


def get_latest_state_changes(state_log: pd.DataFrame) -> dict:
    if state_log.empty:
        return {}
    state_log = state_log.sort_values("date")
    latest = {}
    for stock_id, group in state_log.groupby("stock_id"):
        if len(group) < 2:
            continue
        prev = group.iloc[-2]
        curr = group.iloc[-1]
        change = detect_state_change(prev["decision"], curr["decision"])
        if change:
            latest[stock_id] = change
    return latest


# ── 即時分析結果渲染（共用）────────────────────────────────────────────────────

def render_live_result_block(stock_id: str, result: dict) -> None:
    decision   = result.get("decision", "N/A")
    confidence = result.get("confidence", 0)
    dec_color  = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪", "SELL": "🔴"}.get(decision, "⚪")

    st.markdown(f"### {dec_color} {stock_id} {result.get('name','')}　**{decision}**　信心 {confidence}")
    st.caption(f"資料日期：{result.get('date', 'N/A')}")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("C天", result.get("C_days", "N/A"))
    c2.metric("B天", result.get("B_days", "N/A"))
    c3.metric("A天", result.get("A_days", "N/A"))
    c4.metric("Flow", result.get("flow_status") or "N/A")
    c5.metric("成本位", result.get("cost_level") or "N/A")

    bw = result.get("B_window_20")
    bq = result.get("B_quality")
    if bw is not None or bq is not None:
        bw1, bw2 = st.columns(2)
        bw1.metric("B_window_20（近20日建倉密度）", bw if bw is not None else "N/A")
        bw2.metric("B_quality（建倉強度）", bq if bq is not None else "N/A")

    t1, t2, t3, t4 = st.columns(4)
    t1.metric("收盤", result.get("current_price") or "N/A")
    t2.metric("ADX", _safe_round(result.get("adx")))
    t3.metric("KD", str(_safe_round(result.get("kd_k"))) + "/" + str(_safe_round(result.get("kd_d"))))
    t4.metric("ATR", _safe_round(result.get("atr")))

    reason = result.get("reason")
    if isinstance(reason, list) and reason:
        st.info("📋 " + " ／ ".join(reason))
    elif isinstance(reason, str) and reason:
        st.info("📋 " + reason)

    exp_lines, coach = explain_metrics(result)
    st.markdown("#### 🧠 教練解讀")
    st.success(coach)
    for line in exp_lines:
        st.caption(line)

    inst_state = result.get("institutional_state")
    inst_text  = result.get("institutional_text")
    if inst_state and inst_state != "UNKNOWN":
        st.markdown("#### 🏦 主力分析")
        color_map = {
            "ACCUMULATION": "#28a745",
            "SHAKEOUT":     "#ffc107",
            "DISTRIBUTION": "#dc3545",
            "EXTENDED":     "#fd7e14",
            "NEUTRAL":      "#6c757d",
        }
        color = color_map.get(inst_state, "#6c757d")
        i1, i2, i3 = st.columns(3)
        i1.metric("外資成本", result.get("foreign_cost") or "N/A")
        i2.metric("持倉估計(張)", f'{result.get("foreign_position"):,}' if result.get("foreign_position") else "N/A")
        i3.metric("主力獲利%", f'{result.get("foreign_profit_pct"):.1f}%' if result.get("foreign_profit_pct") is not None else "N/A")
        st.markdown(f"""
<div style="padding:10px;border-radius:8px;background:#1a1a2e;margin:8px 0;">
  <b style="color:{color};font-size:16px;">主力狀態：{inst_state}</b><br>
  <span style="color:#ccc;">{inst_text}</span>
</div>
""", unsafe_allow_html=True)

    b_type = result.get("B_type")
    b_text = result.get("B_text")
    if b_type:
        b_icon = {"STRONG_B": "🟢", "WEAK_B": "🔴", "NORMAL_B": "🟡"}.get(b_type, "⚪")
        st.markdown("#### 🧱 結構品質")
        st.markdown(f"**{b_icon} {b_type}**")
        if b_text:
            st.info(b_text)

    b_phase = result.get("B_phase")
    if b_phase:
        phase_map = {
            "LAUNCH": ("🔴", "LAUNCH 發動初期", "盤石最佳進場點，剛突破，有量"),
            "MATURE": ("🟠", "MATURE 成熟建倉", "主力已在裡面，等待發動，最值得盯"),
            "BUILD":  ("🔵", "BUILD 穩定建倉",  "主力開始進場，結構成形中"),
            "PREPARE":("🟡", "PREPARE 建倉中",  "有人在看，但還沒形成優勢"),
            "LATE":   ("⚫", "LATE 太晚",       "已漲一段，不要追"),
        }
        icon, label, desc = phase_map.get(b_phase, ("⚪", f"UNKNOWN ({b_phase})", ""))
        st.markdown("#### 📍 主力階段")
        st.markdown(f"**{icon} {label}**")
        if desc:
            st.caption(desc)

    b_validity = result.get("B_validity")
    if b_validity:
        validity_map = {
            "TRUE_B":    ("✅", "TRUE_B 真建倉",    "有人在做，結構可信"),
            "FAKE_B":    ("❌", "FAKE_B 假整理",    "只是盤整，外資在賣，不要碰"),
            "UNCERTAIN": ("❓", "UNCERTAIN 待確認", "訊號不明確，繼續觀察"),
        }
        icon, label, desc = validity_map.get(b_validity, ("⚪", b_validity, ""))
        st.markdown("#### 🔍 建倉真偽")
        st.markdown(f"**{icon} {label}**")
        if desc:
            st.caption(desc)

    with st.expander("📋 產生 AI 分析 Prompt"):
        name        = result.get("name", stock_id)
        signal_type = result.get("signal_type") or "-"
        C    = result.get("C_days", "N/A")
        B    = result.get("B_days", "N/A")
        A    = result.get("A_days", "N/A")
        flow = result.get("flow_status") or "N/A"
        cost = result.get("cost_level") or "N/A"
        adx  = _safe_round(result.get("adx"))
        atr  = _safe_round(result.get("atr"))
        vwap = _safe_round(result.get("vwap"))
        kd_k = _safe_round(result.get("kd_k"))
        kd_d = _safe_round(result.get("kd_d"))
        bb_u = _safe_round(result.get("bb_upper"))
        bb_m = _safe_round(result.get("bb_middle"))
        bb_l = _safe_round(result.get("bb_lower"))
        date = result.get("date", "N/A")
        _b_type  = result.get("B_type", "N/A") or "N/A"
        _b_text  = result.get("B_text", "") or ""
        _f_cost  = str(result.get("foreign_cost", "N/A"))
        _f_pos   = str(result.get("foreign_position", "N/A"))
        _f_prof  = str(result.get("foreign_profit_pct", "N/A"))
        _i_state = result.get("institutional_state", "N/A") or "N/A"
        _i_text  = result.get("institutional_text", "") or ""
        prompt = f"""你是專業短線交易員 + 產業分析師，擅長結構分析、資金流、成本位與同產業橫向比較。
請用「結構優先、消息輔助、橫向比較」的原則，對以下股票做詳細分析。

⚠️ 核心規則：
- 絕對不推翻盤石決策
- 結構 > 指標 > 消息 > 橫向比較（優先順序不可顛倒）
- 消息面與比較必須使用最新公開資訊，並標註來源或日期

━━━━━━━━━━━━━━━━━━
【股票】
{stock_id} {name}

【盤石決策】
決策：{decision}｜信心：{confidence}｜型態：{signal_type}
C天：{C}｜B天：{B}｜A天：{A}
Flow：{flow}｜Cost：{cost}
結構品質：{_b_type}（{_b_text})
B_window_20：{result.get("B_window_20", "N/A")}（近20日符合B條件天數）
B_quality：{result.get("B_quality", "N/A")}（建倉強度分數，越高越強）
主力成本：{_f_cost}｜持倉：{_f_pos}張｜主力獲利：{_f_prof}%
主力狀態：{_i_state}（{_i_text})

【技術指標】
ADX：{adx}｜ATR：{atr}｜VWAP：{vwap}
KD：{kd_k}/{kd_d}
布林：上 {bb_u} / 中 {bb_m} / 下 {bb_l}

【日期】
{date}
━━━━━━━━━━━━━━━━━━
請依序詳細回答：

【1️⃣ 結構階段判斷】
- 目前處於哪個階段？（起漲 / 發動初期 / 延伸 / 末段 / 情緒段）
- 是否為「無B直接A」類型？（是 / 否）

【2️⃣ 結構 vs 消息 驅動拆解】
（A）結構面原因（必填）
- C/B/A 狀態的意義與可持續性
- Flow 與 Cost 的判讀

（B）外部驅動因素（消息 / 題材 / 事件）
- 請搜尋並列出最近可能影響該股的公開消息、訂單、認證、政策等
- 若無明確消息 → 寫「目前未觀察到重大催化劑」

【3️⃣ 深入橫向比較（同產業）】
請選擇 2~3 家最相關的同產業公司進行比較。
比較維度（必須涵蓋以下全部）：
- 結構強度（C/B/A 天數與完整度）
- 資金流動能（外資/投信/融資變化）
- 成本位安全度（相對於均線位置）
- 技術指標相對位置（KD、RSI、ADX、布林）
- 近期消息/題材差異

請清楚指出：
- 本股在同業中的相對位置（領先 / 中間 / 落後）
- 本股的優勢與劣勢（相較同業）

【4️⃣ 風險評估】
- 風險等級：低 / 中 / 高
- 最大風險來源（只講最關鍵的一點）

【5️⃣ 時間框架分析】
- 短線（1~5天）
- 中線（1~4週）
- 長線（1~3月）

【6️⃣ 操作屬性分類】
請三選一並說明理由：
- 結構股（可持續）
- 情緒股（短期波動）
- 題材股（消息驅動）

【7️⃣ 最終結論】
👉 類型：結構股 / 情緒股 / 題材股
👉 階段：
👉 同業相對位置：
👉 風險：
👉 一句話評價：
━━━━━━━━━━━━━━━━━━
回答風格：專業、直接、像資深交易員在開會報告。橫向比較要具體、有數據對比，並突出關鍵差異。"""
        st.code(prompt, language="")


# ── 主程式 ────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(page_title="磐石決策系統", layout="wide")
    st.title("磐石決策系統 戰情室")

    # ── 全市場即時個股分析 ────────────────────────────────────────────────
    st.subheader("🔬 全市場即時個股分析")
    st.caption("輸入股票代號或中文名稱，即時跑完整盤石分析")

    live_input = st.text_input("股票代號或名稱", placeholder="例：2330 或 台積電", key="live_query")

    if live_input:
        live_input = live_input.strip()
        try:
            import requests as _req, os as _os
            _token = _os.getenv("FINMIND_TOKEN")
            co_df = pd.read_csv("companies.csv", dtype=str)
            match_id   = co_df[co_df["stock_id"] == live_input]
            match_name = co_df[co_df["name"].str.contains(live_input, na=False)]
            if not match_id.empty:
                live_id = live_input
            elif not match_name.empty:
                live_id = match_name.iloc[0]["stock_id"]
                st.caption(f"查詢：{match_name.iloc[0]['name']} ({live_id})")
            else:
                _r = _req.get("https://api.finmindtrade.com/api/v4/data",
                    params={"dataset": "TaiwanStockInfo", "token": _token}, timeout=15)
                _info = pd.DataFrame(_r.json().get("data", []))
                if not _info.empty:
                    _m = _info[_info["stock_name"].str.contains(live_input, na=False)]
                    # 只保留4位數股票代號（過濾權證/ETF衍生商品）
                    _m = _m[_m["stock_id"].str.match(r"^\d{4}$")]
                    if not _m.empty:
                        live_id = _m.iloc[0]["stock_id"]
                        st.caption(f"查詢：{_m.iloc[0]['stock_name']} ({live_id})")
                    else:
                        live_id = live_input
                else:
                    live_id = live_input
        except Exception:
            live_id = live_input

        with st.spinner(f"正在分析 {live_id}..."):
            try:
                from main import load_params
                params = load_params()
                result = process_stock_live(live_id, params, print_snapshot=False)
            except Exception as e:
                result = None
                st.error(str(e))

        if result is None:
            st.warning("查無資料或分析失敗，請確認代號是否正確")
        else:
            decision   = result.get("decision", "N/A")
            confidence = result.get("confidence", 0)
            dec_color  = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪", "SELL": "🔴"}.get(decision, "⚪")

            def safe_round(x):
                return round(x, 1) if x is not None else "N/A"

            st.markdown(f"### {dec_color} {live_id} {result.get('name','')}　**{decision}**　信心 {confidence}")
            st.caption(f"資料日期：{result.get('date', 'N/A')}")

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("C天", result.get("C_days", "N/A"))
            c2.metric("B天", result.get("B_days", "N/A"))
            c3.metric("A天", result.get("A_days", "N/A"))
            c4.metric("Flow", result.get("flow_status") or "N/A")
            c5.metric("成本位", result.get("cost_level") or "N/A")

            bw = result.get("B_window_20")
            bq = result.get("B_quality")
            if bw is not None or bq is not None:
                bw1, bw2 = st.columns(2)
                bw1.metric("B_window_20（近20日建倉密度）", bw if bw is not None else "N/A")
                bw2.metric("B_quality（建倉強度）", bq if bq is not None else "N/A")

            t1, t2, t3, t4 = st.columns(4)
            t1.metric("收盤", result.get("current_price") or "N/A")
            t2.metric("ADX", safe_round(result.get("adx")))
            t3.metric("KD", str(safe_round(result.get("kd_k")))+"/"+str(safe_round(result.get("kd_d"))))
            t4.metric("ATR", safe_round(result.get("atr")))

            reason = result.get("reason")
            if isinstance(reason, list) and reason:
                st.info("📋 " + " ／ ".join(reason))
            elif isinstance(reason, str) and reason:
                st.info("📋 " + reason)

            exp_lines, coach = explain_metrics(result)
            st.markdown("#### 🧠 教練解讀")
            st.success(coach)
            for line in exp_lines:
                st.caption(line)

            # ── 主力分析 ──────────────────────────────────────────────
            inst_state = result.get("institutional_state")
            inst_text  = result.get("institutional_text")
            if inst_state and inst_state != "UNKNOWN":
                st.markdown("#### 🏦 主力分析")
                color_map = {
                    "ACCUMULATION": "#28a745",
                    "SHAKEOUT":     "#ffc107",
                    "DISTRIBUTION": "#dc3545",
                    "EXTENDED":     "#fd7e14",
                    "NEUTRAL":      "#6c757d"
                }
                color = color_map.get(inst_state, "#6c757d")
                i1, i2, i3 = st.columns(3)
                i1.metric("外資成本", result.get("foreign_cost") or "N/A")
                i2.metric("持倉估計(張)", f'{result.get("foreign_position"):,}' if result.get("foreign_position") else "N/A")
                i3.metric("主力獲利%", f'{result.get("foreign_profit_pct"):.1f}%' if result.get("foreign_profit_pct") is not None else "N/A")
                st.markdown(f"""
<div style="padding:10px;border-radius:8px;background:#1a1a2e;margin:8px 0;">
  <b style="color:{color};font-size:16px;">主力狀態：{inst_state}</b><br>
  <span style="color:#ccc;">{inst_text}</span>
</div>
""", unsafe_allow_html=True)

            # ── 結構品質（強B/弱B）─────────────────────────────────
            b_type = result.get("B_type")
            b_text = result.get("B_text")
            if b_type:
                b_icon = {"STRONG_B": "🟢", "WEAK_B": "🔴", "NORMAL_B": "🟡"}.get(b_type, "⚪")
                st.markdown("#### 🧱 結構品質")
                st.markdown(f"**{b_icon} {b_type}**")
                if b_text:
                    st.info(b_text)

            b_phase = result.get("B_phase")
            if b_phase:
                phase_map = {
                    "LAUNCH": ("🔴", "LAUNCH 發動初期", "盤石最佳進場點，剛突破，有量"),
                    "MATURE": ("🟠", "MATURE 成熟建倉", "主力已在裡面，等待發動，最值得盯"),
                    "BUILD":  ("🔵", "BUILD 穩定建倉",  "主力開始進場，結構成形中"),
                    "PREPARE":("🟡", "PREPARE 建倉中",  "有人在看，但還沒形成優勢"),
                    "LATE":   ("⚫", "LATE 太晚",       "已漲一段，不要追"),
                }
                icon, label, desc = phase_map.get(b_phase, ("⚪", f"UNKNOWN ({b_phase})", ""))
                st.markdown("#### 📍 主力階段")
                st.markdown(f"**{icon} {label}**")
                if desc:
                    st.caption(desc)

            b_validity = result.get("B_validity")
            if b_validity:
                validity_map = {
                    "TRUE_B":    ("✅", "TRUE_B 真建倉",    "有人在做，結構可信"),
                    "FAKE_B":    ("❌", "FAKE_B 假整理",    "只是盤整，外資在賣，不要碰"),
                    "UNCERTAIN": ("❓", "UNCERTAIN 待確認", "訊號不明確，繼續觀察"),
                }
                icon, label, desc = validity_map.get(b_validity, ("⚪", b_validity, ""))
                st.markdown("#### 🔍 建倉真偽")
                st.markdown(f"**{icon} {label}**")
                if desc:
                    st.caption(desc)

            # ── AI Prompt ──────────────────────────────────────────
            with st.expander("📋 產生 AI 分析 Prompt"):
                name    = result.get("name", live_id)
                decision = result.get("decision", "N/A")
                confidence = result.get("confidence", 0)
                signal_type = result.get("signal_type") or "-"
                C    = result.get("C_days", "N/A")
                B    = result.get("B_days", "N/A")
                A    = result.get("A_days", "N/A")
                flow = result.get("flow_status") or "N/A"
                cost = result.get("cost_level") or "N/A"
                adx  = safe_round(result.get("adx"))
                atr  = safe_round(result.get("atr"))
                vwap = safe_round(result.get("vwap"))
                kd_k = safe_round(result.get("kd_k"))
                kd_d = safe_round(result.get("kd_d"))
                bb_u = safe_round(result.get("bb_upper"))
                bb_m = safe_round(result.get("bb_middle"))
                bb_l = safe_round(result.get("bb_lower"))
                date = result.get("date", "N/A")

                _b_type = result.get("B_type", "N/A") or "N/A"
                _b_text = result.get("B_text", "") or ""
                _f_cost = str(result.get("foreign_cost", "N/A"))
                _f_pos  = str(result.get("foreign_position", "N/A"))
                _f_prof = str(result.get("foreign_profit_pct", "N/A"))
                _i_state = result.get("institutional_state", "N/A") or "N/A"
                _i_text  = result.get("institutional_text", "") or ""
                prompt = f"""你是專業短線交易員 + 產業分析師，擅長結構分析、資金流、成本位與同產業橫向比較。
請用「結構優先、消息輔助、橫向比較」的原則，對以下股票做詳細分析。

⚠️ 核心規則：
- 絕對不推翻盤石決策
- 結構 > 指標 > 消息 > 橫向比較（優先順序不可顛倒）
- 消息面與比較必須使用最新公開資訊，並標註來源或日期

━━━━━━━━━━━━━━━━━━
【股票】
{live_id} {name}

【盤石決策】
決策：{decision}｜信心：{confidence}｜型態：{signal_type}
C天：{C}｜B天：{B}｜A天：{A}
Flow：{flow}｜Cost：{cost}
結構品質：{_b_type}（{_b_text})
B_window_20：{result.get("B_window_20", "N/A")}（近20日符合B條件天數）
B_quality：{result.get("B_quality", "N/A")}（建倉強度分數，越高越強）
主力成本：{_f_cost}｜持倉：{_f_pos}張｜主力獲利：{_f_prof}%
主力狀態：{_i_state}（{_i_text})

【技術指標】
ADX：{adx}｜ATR：{atr}｜VWAP：{vwap}
KD：{kd_k}/{kd_d}
布林：上 {bb_u} / 中 {bb_m} / 下 {bb_l}

【日期】
{date}
━━━━━━━━━━━━━━━━━━
請依序詳細回答：

【1️⃣ 結構階段判斷】
- 目前處於哪個階段？（起漲 / 發動初期 / 延伸 / 末段 / 情緒段）
- 是否為「無B直接A」類型？（是 / 否）

【2️⃣ 結構 vs 消息 驅動拆解】
（A）結構面原因（必填）
- C/B/A 狀態的意義與可持續性
- Flow 與 Cost 的判讀

（B）外部驅動因素（消息 / 題材 / 事件）
- 請搜尋並列出最近可能影響該股的公開消息、訂單、認證、政策等
- 若無明確消息 → 寫「目前未觀察到重大催化劑」

【3️⃣ 深入橫向比較（同產業）】
請選擇 2~3 家最相關的同產業公司進行比較。
比較維度（必須涵蓋以下全部）：
- 結構強度（C/B/A 天數與完整度）
- 資金流動能（外資/投信/融資變化）
- 成本位安全度（相對於均線位置）
- 技術指標相對位置（KD、RSI、ADX、布林）
- 近期消息/題材差異

請清楚指出：
- 本股在同業中的相對位置（領先 / 中間 / 落後）
- 本股的優勢與劣勢（相較同業）

【4️⃣ 風險評估】
- 風險等級：低 / 中 / 高
- 最大風險來源（只講最關鍵的一點）

【5️⃣ 時間框架分析】
- 短線（1~5天）
- 中線（1~4週）
- 長線（1~3月）

【6️⃣ 操作屬性分類】
請三選一並說明理由：
- 結構股（可持續）
- 情緒股（短期波動）
- 題材股（消息驅動）

【7️⃣ 最終結論】
👉 類型：結構股 / 情緒股 / 題材股
👉 階段：
👉 同業相對位置：
👉 風險：
👉 一句話評價：
━━━━━━━━━━━━━━━━━━
回答風格：專業、直接、像資深交易員在開會報告。橫向比較要具體、有數據對比，並突出關鍵差異。"""

                st.code(prompt, language="")

            fomo_flags = []
            if (result.get("B_days") or 0) == 0 and (result.get("A_days") or 0) >= 1:
                fomo_flags.append("無B直接A（可能追高）")
            if (result.get("A_days") or 0) >= 3:
                fomo_flags.append("A段已延伸")
            if result.get("cost_level") == "HIGH_RISK":
                fomo_flags.append("成本位過高")
            if fomo_flags:
                st.warning("⚠️ 這檔你很可能會想買，但要小心：")
                for f in fomo_flags:
                    st.caption(f"👉 {f}")

    st.divider()

    quick_mode = st.toggle("⚡ 快速模式（只看前3檔）", value=False)
    if quick_mode:
        st.caption("⚡ 快速模式：僅顯示最關鍵標的")


    show_help = st.toggle("📖 顯示說明", value=False)
    if show_help:
        st.info("🧠 使用方式：先看『今日最強』→ 再看 Action → 最後看 Watchlist")
        with st.expander("📊 結構（最重要）", expanded=True):
            st.markdown(
                "**C天（止跌）**：代表底部是否穩定，越長越好\n\n"
                "**B天（建倉）**：主力在偷偷買，這段最關鍵\n\n"
                "**A天（突破）**：開始上漲，越早（1~2天）越安全\n\n"
                "👉 核心：C → B → A 才是完整結構"
            )
        with st.expander("⚠️ 風險判斷"):
            st.markdown(
                "**A太長（≥5）** → 追高風險\n\n"
                "**Flow = NEUTRAL** → 沒人推\n\n"
                "**Cost = HIGH_RISK** → 太貴\n\n"
                "**ADX < 20** → 沒趨勢（容易假突破）"
            )
        with st.expander("🎯 決策意義"):
            st.markdown(
                "🟢 **BUY**：條件齊全\n\n"
                "🟡 **WAIT**：還在觀察\n\n"
                "⚪ **IGNORE**：不該出手"
            )
        with st.expander("📈 指標說明"):
            st.markdown(
                "**ADX**：趨勢強度（>25 才有趨勢）\n\n"
                "**KD**：短線動能（>80 過熱）\n\n"
                "**VWAP**：主力成本\n\n"
                "**BB**：波動範圍"
            )
        with st.expander("🚀 型態"):
            st.markdown(
                "🚀 **FAST**：直接突破（強但風險高）\n\n"
                "📈 **STD**：標準型（最穩）"
            )
        with st.expander("🌐 中英對照"):
            st.markdown(
                "C_days：止跌\n\n"
                "B_days：建倉\n\n"
                "A_days：突破\n\n"
                "Flow：資金流\n\n"
                "Cost：成本位\n\n"
                "Confidence：信心分數"
            )

    today_str = datetime.date.today().isoformat()
    st.info(_cached_daily_verse(today_str))

    df = load_latest_decisions()
    if df.empty:
        st.caption("尚無資料（latest_decisions.csv 不存在或為空）")
        return

    latest_date = df["date"].max() if "date" in df.columns else "—"
    st.caption(f"資料日期：{latest_date}")

    if "overrides" not in st.session_state:
        st.session_state["overrides"] = load_watchlist_overrides()
    if "pinned" not in st.session_state:
        st.session_state["pinned"] = load_pinned()

    state_log = load_state_log()
    state_changes = get_latest_state_changes(state_log)

    # 昨日對照表（供情緒雷達使用）
    prev_map = {}
    try:
        if not state_log.empty:
            _sl = state_log.copy()
            if "stock_id" in _sl.columns:
                _sl["stock_id"] = _sl["stock_id"].astype(str)
            if "date" in _sl.columns:
                _sl["date"] = pd.to_datetime(_sl["date"], errors="coerce")
            _today = pd.Timestamp(datetime.date.today())
            _valid = _sl[_sl["date"] < _today]
            for _sid, _grp in _valid.groupby("stock_id"):
                _grp = _grp.sort_values("date")
                _latest = _grp.iloc[-1]
                prev_map[_sid] = {
                    "C_days":     int(_latest.get("C_days", 0) or 0),
                    "A_days":     int(_latest.get("A_days", 0) or 0),
                    "flow_status": str(_latest.get("flow_status", "") or ""),
                }
    except Exception:
        prev_map = {}

    overrides = st.session_state["overrides"]
    action_df, watchlist_df, candidate_df = classify_rows(df, overrides)

    # 排序
    action_df["confidence"] = pd.to_numeric(action_df["confidence"], errors="coerce")
    watchlist_df["confidence"] = pd.to_numeric(watchlist_df["confidence"], errors="coerce")
    candidate_df["C_days"] = pd.to_numeric(candidate_df["C_days"], errors="coerce")
    action_df = action_df.sort_values(by="confidence", ascending=False)
    watchlist_df = watchlist_df.sort_values(by="confidence", ascending=False)
    candidate_df = candidate_df.sort_values(by="C_days", ascending=False)

    if quick_mode:
        action_df = action_df.head(3)
        watchlist_df = watchlist_df.head(3)
        candidate_df = candidate_df.head(5)

    # 全局搜尋 filter（僅影響顯示）
    def filter_df(src: pd.DataFrame) -> pd.DataFrame:
        if True or src.empty:  # 搜尋框已移除
            return src
        q = query.strip().lower()
        return src[
            src["stock_id"].astype(str).str.lower().str.contains(q, na=False)
            | src["name"].astype(str).str.lower().str.contains(q, na=False)
        ]

    filtered_action_df = filter_df(action_df)
    filtered_watchlist_df = filter_df(watchlist_df)
    filtered_candidate_df = filter_df(candidate_df)

    # ── 今日變化 ──────────────────────────────────────────────────────────
    st.subheader("📊 今日變化")
    if state_changes:
        for sid, (label, detail) in state_changes.items():
            st.text(f"{sid}｜{label}｜{detail}")
    else:
        st.caption("今日無關鍵變化")

    # ── 強B排行榜 ─────────────────────────────────────────────────────────
    st.subheader("🔥 強B排行榜（主力建倉候選）")
    try:
        from b_ranker import get_top_strong_B
        top_b = get_top_strong_B(df, top_n=5)
        if top_b.empty:
            st.info("今日無強B候選")
        else:
            for _, row in top_b.iterrows():
                stock = row.get("stock_id", "")
                name  = row.get("name", "")
                score = int(row.get("B_score", 0))
                B     = row.get("B_days", "")
                flow  = row.get("flow_status", "")
                cost  = row.get("cost_level", "")
                st.markdown(f"**🟢 {stock} {name}｜分數 {score}**")
                st.caption(f"B={B}｜Flow={flow}｜Cost={cost}")
    except Exception as e:
        st.warning(f"強B排行榜載入失敗：{e}")

    # ── 戰情室 War Room ──────────────────────────────────────────────────
    st.subheader("⚔️ 戰情室 War Room")
    st.info("⚡ 行動清單是盤石說『現在可以買』；戰情室是盤石還沒說，但主力已經在布局。兩個互補——行動清單給你確認，戰情室讓你提前看到。")

    def get_c_icon(c):
        if c == 0: return "⚫"
        elif c == 1: return "⚪"
        elif c == 2: return "🟡"
        elif 3 <= c <= 4: return "🔵"
        else: return "🔴"

    def get_a_icon(a):
        if a == 0: return "⚪"
        elif 1 <= a <= 2: return "🟢"
        else: return "🟢🟢"

    def get_status_tag(a, c):
        if a >= 2 and c >= 3: return "🔥 發動中 Launching"
        elif a >= 1: return "⚠️ 剛轉強 Turning"
        elif c == 0: return "⏳ 等待 Waiting"
        else: return ""

    try:
        war_df = df.copy()
        for _col in ["B_days", "A_days", "C_days"]:
            war_df[_col] = pd.to_numeric(war_df[_col], errors="coerce").fillna(0).astype(int)

        def _classify_war(row):
            B      = int(row["B_days"])
            A      = int(row["A_days"])
            C      = int(row["C_days"])
            b_type = str(row.get("B_type", "") or "")
            flow   = str(row.get("flow_status", "") or "")
            if B >= 8 and A >= 2 and C >= 3:
                return "ATTACK"
            if b_type == "STRONG_B" and B >= 10 and A >= 1 and C >= 2 and flow in ["ACCUMULATING", "NEUTRAL"]:
                return "LAUNCH"
            if b_type == "STRONG_B" and B >= 10:
                return "PREPARE"
            return None

        war_df["war_class"] = war_df.apply(_classify_war, axis=1)
        attack_df  = war_df[war_df["war_class"] == "ATTACK"].copy()
        launch_df  = war_df[war_df["war_class"] == "LAUNCH"].copy()
        prepare_df = war_df[war_df["war_class"] == "PREPARE"].copy()

        attack_df["_score"]  = attack_df["C_days"] * 3 + attack_df["A_days"] * 2 + attack_df["B_days"]
        launch_df["_score"]  = launch_df["C_days"] * 3 + launch_df["A_days"] * 2 + launch_df["B_days"]
        prepare_df["_score"] = prepare_df["B_days"] * 3 + prepare_df["A_days"] * 2 + prepare_df["C_days"]

        attack_df  = attack_df.sort_values("_score", ascending=False)
        launch_df  = launch_df.sort_values("_score", ascending=False)
        prepare_df = prepare_df.sort_values("_score", ascending=False)

        def _render_war_section(section_df, emoji, title_en, title_zh, caption_text):
            st.markdown(f"#### {emoji} {title_en}　{title_zh}")
            st.caption(caption_text)
            if section_df.empty:
                st.caption("（無符合條件 No qualified stocks）")
                return
            for rank, (_, r) in enumerate(section_df.iterrows(), 1):
                sid  = str(r.get("stock_id", ""))
                name = str(r.get("name", ""))
                B    = int(r["B_days"])
                A    = int(r["A_days"])
                C    = int(r["C_days"])
                flow = str(r.get("flow_status", "-") or "-")
                tag  = get_status_tag(A, C)
                wcol1, wcol2 = st.columns([9, 1])
                with wcol1:
                    st.markdown(
                        f"**#{rank} {sid} {name}**　"
                        f"B={B}｜{get_a_icon(A)}A={A}｜{get_c_icon(C)}C={C}｜Flow={flow}　{tag}"
                    )
                with wcol2:
                    if st.button("📌", key=f"track_war_{title_en}_{sid}", help="加入自訂追蹤清單"):
                        add_to_custom_watchlist(sid)
                        st.toast(f"已加入追蹤：{sid}")

        _render_war_section(
            launch_df, "🟠", "LAUNCH", "即將發動",
            "主力建倉完成，股票開始脫離底部。不是現在買，而是提前盯住——等它真正發動再出手。",
        )
        _render_war_section(
            attack_df, "🔴", "ATTACK", "正在發動",
            "已經開始發動，結構完整。盤石考慮進場的候選，等決策系統確認再動作。",
        )
        _render_war_section(
            prepare_df, "🔵", "PREPARE", "建倉完成",
            "主力在偷偷買，還沒有要拉的跡象。先放進視野，等出現 A 訊號再說。",
        )

    except Exception as e:
        st.warning(f"戰情室載入失敗：{e}")

    # ── 情緒雷達 Momentum Radar ──────────────────────────────────────────
    st.subheader("⚡ 情緒雷達 Momentum Radar")
    st.info(
        "【這是什麼】抓「沒有B卻突然噴」的股票。盤石負責穩定賺，情緒雷達負責抓爆發機會。\n"
        "【強度等級】🚀 過熱：不追高，慢慢賣　🔥 強爆：可持倉，準備走　⚠️ 初爆：小倉試單\n"
        "【轉弱警報】🟡 先不要買　🟠 減碼　🔴 立刻出場\n"
        "【鐵則】❌ 不能當主倉　❌ 不能凹單　✅ 一轉弱就走"
    )
    st.caption("記住：🔴賣掉、🟠賣一半、🟡等等、🟢抱住")
    try:
        mdf = df.copy()
        mdf = mdf.fillna(0)
        for _mc in ["B_days", "A_days", "C_days"]:
            mdf[_mc] = pd.to_numeric(mdf[_mc], errors="coerce").fillna(0).astype(int)
        for _mc in ["close", "vwap"]:
            if _mc in mdf.columns:
                mdf[_mc] = pd.to_numeric(mdf[_mc], errors="coerce")

        use_vwap = ("close" in mdf.columns) and ("vwap" in mdf.columns)
        cond = (
            (mdf["B_days"] <= 2) &
            (mdf["A_days"] >= 2) &
            (mdf["C_days"] >= 2) &
            (mdf["flow_status"] != "DISTRIBUTION")
        )
        if use_vwap:
            cond = cond & (mdf["close"] > mdf["vwap"])
        momentum_df = mdf.loc[cond].copy()

        momentum_df["score"] = momentum_df["A_days"] * 2 + momentum_df["C_days"] * 3

        def get_momentum_level(score):
            if score >= 13: return "🚀 過熱"
            elif score >= 9: return "🔥 強爆"
            elif score >= 6: return "⚠️ 初爆"
            else: return ""

        def _fmt_abc(row):
            def _ci(c):
                if c == 0: return "⚫"
                elif c == 1: return "⚪"
                elif c == 2: return "🟡"
                elif 3 <= c <= 4: return "🔵"
                else: return "🔴"
            def _ai(a):
                if a == 0: return "⚪"
                elif 1 <= a <= 2: return "🟢"
                else: return "🟢🟢"
            return f"{row['B_days']} / {row['A_days']}{_ai(row['A_days'])} / {row['C_days']}{_ci(row['C_days'])}"

        momentum_df = momentum_df.sort_values(by="score", ascending=False).reset_index(drop=True)
        momentum_df["rank"] = momentum_df.index + 1

        display_df = momentum_df.copy()
        display_df = display_df.fillna(0)
        if "stock_id" in display_df.columns:
            display_df["stock_id"] = display_df["stock_id"].astype(str)
        display_df["B/A/C"] = display_df.apply(_fmt_abc, axis=1)
        display_df["C變化"] = display_df.apply(
            lambda r: f"{r['C_days']}{get_c_icon(int(r['C_days']))} {get_c_arrow(int(r['C_days']), prev_map.get(str(r['stock_id'])))}",
            axis=1,
        )
        display_df["轉弱訊號"] = display_df.apply(
            lambda r: get_warning(r, prev_map.get(str(r["stock_id"]))), axis=1
        )
        display_df["強度"] = display_df["score"].apply(get_momentum_level)
        display_df["⚡動作"] = display_df.apply(
            lambda r: get_action_signal(r["轉弱訊號"], r["強度"]), axis=1
        )
        display_df["🧠教練"] = display_df.apply(
            lambda r: get_coach_message(r["轉弱訊號"], r["強度"]), axis=1
        )
        display_df = display_df[["rank", "stock_id", "name", "B/A/C", "C變化", "flow_status", "強度", "⚡動作", "🧠教練"]]

        if momentum_df.empty:
            st.write("（今日無爆發訊號 No momentum spike today）")
        else:
            st.dataframe(display_df, use_container_width=True)
    except Exception as e:
        st.warning(f"情緒雷達載入失敗：{e}")

    # ── ⭐ 重點觀察 ───────────────────────────────────────────────────────
    pinned_ids = st.session_state["pinned"]
    frames = [src for src in [action_df, watchlist_df, candidate_df] if not src.empty]
    pinned_df = pd.concat(frames) if frames else pd.DataFrame()
    if not pinned_df.empty:
        pinned_df = pinned_df[pinned_df["stock_id"].isin(pinned_ids)]
    if not pinned_df.empty:
        pinned_df["confidence"] = pd.to_numeric(pinned_df["confidence"], errors="coerce")
        pinned_df = pinned_df.sort_values(by="confidence", ascending=False)
        st.subheader("⭐ 重點觀察")
        for _, row in pinned_df.iterrows():
            d = build_display_row(row)
            st.text(f"{d['股票']} ｜ {format_decision_label(d['決策'])} ｜ {d['軌跡']} ｜ 信心 {d['信心']}")

    # ── 今日最強 ──────────────────────────────────────────────────────────
    st.subheader("🔥 今日最強")
    if not action_df.empty:
        top1 = action_df.iloc[0]
        d = build_display_row(top1)
        top_html = (
            '<div style="background:#111;padding:12px;border-radius:10px;'
            'border:1px solid #444;margin-bottom:10px;">'
            f'<div style="font-size:16px;font-weight:bold;">{d["股票"]} ｜ '
            f'{format_decision_label(d["決策"])} ｜ {format_signal_type(str(top1.get("signal_type","")))} </div>'
            f'<div style="margin-top:6px;">{d["軌跡"]} ｜ {d["資金流"]} ｜ {d["成本位"]}</div>'
            f'<div style="margin-top:8px;">信心：{d["信心"]}<br>{render_confidence_bar(d["信心"])}</div>'
            '</div>'
        )
        components.html(top_html, height=120)
    else:
        st.info("今日沒有最強標的（無 BUY 訊號）")

    # ── 行動清單 ──────────────────────────────────────────────────────────
    st.subheader("行動清單（Action）")
    if action_df.empty:
        st.info("今日沒有符合條件的進場機會")
    st.dataframe(style_decision_table(build_display_table(filtered_action_df)), use_container_width=True)
    if not filtered_action_df.empty:
        action_ids = [str(r.get("stock_id", "")) for _, r in filtered_action_df.iterrows() if r.get("stock_id")]
        track_cols = st.columns(min(len(action_ids), 8))
        for i, sid in enumerate(action_ids):
            with track_cols[i % 8]:
                if st.button(f"📌 {sid}", key=f"track_action_{sid}", help="加入自訂追蹤清單"):
                    add_to_custom_watchlist(sid)
                    st.toast(f"已加入追蹤：{sid}")

    # ── 觀察名單 ──────────────────────────────────────────────────────────
    st.subheader("觀察名單（Watchlist）")
    for _, row in filtered_watchlist_df.iterrows():
        stock_id = str(row.get("stock_id", ""))
        d = build_display_row(row)
        is_pinned = stock_id in st.session_state["pinned"]
        wl_live_key = f"wl_live_show_{stock_id}"
        col1, col2, col3, col4, col5 = st.columns([5, 1, 2, 1, 1])
        with col1:
            components.html(_row_html(d, str(row.get("signal_type", ""))), height=80)
            if stock_id in state_changes:
                chg_label, chg_detail = state_changes[stock_id]
                st.caption(f"{chg_label}｜{chg_detail}")
        with col2:
            pin_label = "★" if is_pinned else "⭐"
            if st.button(pin_label, key=f"pin_{stock_id}"):
                if is_pinned:
                    st.session_state["pinned"].discard(stock_id)
                else:
                    st.session_state["pinned"].add(stock_id)
                save_pinned(st.session_state["pinned"])
                st.rerun()
        with col3:
            live_label = "🔬 收起" if st.session_state.get(wl_live_key, False) else "🔬 即時分析"
            if st.button(live_label, key=f"wl_live_btn_{stock_id}"):
                st.session_state[wl_live_key] = not st.session_state.get(wl_live_key, False)
                if not st.session_state[wl_live_key]:
                    st.session_state.pop(f"wl_live_result_{stock_id}", None)
                st.rerun()
        with col4:
            if st.button("📌", key=f"track_wl_{stock_id}", help="加入自訂追蹤清單"):
                add_to_custom_watchlist(stock_id)
                st.toast(f"已加入追蹤：{stock_id}")
        with col5:
            if st.button("✕ 移除", key=f"remove_{stock_id}"):
                if stock_id in st.session_state["overrides"]:
                    st.session_state["overrides"].pop(stock_id)
                    save_watchlist_overrides(st.session_state["overrides"])
                st.rerun()

        if st.session_state.get(wl_live_key, False):
            wl_result_key = f"wl_live_result_{stock_id}"
            if wl_result_key not in st.session_state:
                with st.spinner(f"分析 {stock_id} 中..."):
                    try:
                        from main import load_params
                        params = load_params()
                        st.session_state[wl_result_key] = process_stock_live(stock_id, params, print_snapshot=False)
                    except Exception as e:
                        st.error(str(e))
                        st.session_state[wl_result_key] = None
            wl_res = st.session_state.get(wl_result_key)
            if wl_res:
                render_live_result_block(stock_id, wl_res)
            elif wl_res is None:
                st.warning("分析失敗，請確認代號是否正確")

    # ── 候選清單 ──────────────────────────────────────────────────────────
    st.subheader("候選清單（Candidate）")
    for _, row in filtered_candidate_df.iterrows():
        stock_id = str(row.get("stock_id", ""))
        d = build_display_row(row)
        is_pinned = stock_id in st.session_state["pinned"]
        cd_live_key = f"cd_live_show_{stock_id}"
        col1, col2, col3, col4, col5 = st.columns([5, 1, 2, 1, 1])
        with col1:
            components.html(_row_html(d, str(row.get("signal_type", ""))), height=80)
            if stock_id in state_changes:
                chg_label, chg_detail = state_changes[stock_id]
                st.caption(f"{chg_label}｜{chg_detail}")
        with col2:
            pin_label = "★" if is_pinned else "⭐"
            if st.button(pin_label, key=f"pin_{stock_id}"):
                if is_pinned:
                    st.session_state["pinned"].discard(stock_id)
                else:
                    st.session_state["pinned"].add(stock_id)
                save_pinned(st.session_state["pinned"])
                st.rerun()
        with col3:
            live_label = "🔬 收起" if st.session_state.get(cd_live_key, False) else "🔬 即時分析"
            if st.button(live_label, key=f"cd_live_btn_{stock_id}"):
                st.session_state[cd_live_key] = not st.session_state.get(cd_live_key, False)
                if not st.session_state[cd_live_key]:
                    st.session_state.pop(f"cd_live_result_{stock_id}", None)
                st.rerun()
        with col4:
            if st.button("📌", key=f"track_cd_{stock_id}", help="加入自訂追蹤清單"):
                add_to_custom_watchlist(stock_id)
                st.toast(f"已加入追蹤：{stock_id}")
        with col5:
            if st.button("+ 加入觀察", key=f"add_{stock_id}"):
                st.session_state["overrides"][stock_id] = True
                save_watchlist_overrides(st.session_state["overrides"])
                st.rerun()

        if st.session_state.get(cd_live_key, False):
            cd_result_key = f"cd_live_result_{stock_id}"
            if cd_result_key not in st.session_state:
                with st.spinner(f"分析 {stock_id} 中..."):
                    try:
                        from main import load_params
                        params = load_params()
                        st.session_state[cd_result_key] = process_stock_live(stock_id, params, print_snapshot=False)
                    except Exception as e:
                        st.error(str(e))
                        st.session_state[cd_result_key] = None
            cd_res = st.session_state.get(cd_result_key)
            if cd_res:
                render_live_result_block(stock_id, cd_res)
            elif cd_res is None:
                st.warning("分析失敗，請確認代號是否正確")


    # ── 今日快照 ──────────────────────────────────────────────────────────
    st.divider()
    st.subheader("📋 今日快照")

    def build_snapshot(rows: pd.DataFrame) -> str:
        lines = []
        for _, r in rows.iterrows():
            d = build_display_row(r)
            lines.append(f"{d['股票']} | {d['決策']} | {d['軌跡']} | 信心 {d['信心']}")
        return "\n".join(lines) if lines else "（今日無訊號）"

    snap_frames = [src for src in [action_df, watchlist_df] if not src.empty]
    snapshot_rows = pd.concat(snap_frames).head(10) if snap_frames else pd.DataFrame()
    st.code(build_snapshot(snapshot_rows))

    # ── 交易行為統計 ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("📊 交易行為統計")
    analyze_mistakes()

    st.subheader("📈 勝率統計")
    analyze_winrate()



if __name__ == "__main__":
    main()
