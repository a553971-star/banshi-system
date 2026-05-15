"""
pages/7_📊_本益比.py
低本益比觀察區：PE < 14 × 磐石 CBA 結構過濾
"""
import os
import pandas as pd
import streamlit as st

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NEED_COLS = [
    "stock_id", "name", "C_days", "B_days", "A_days",
    "flow_status", "cost_level", "B_validity", "volume_ratio",
    "institutional_state", "vwap",
]

st.set_page_config(page_title="低本益比觀察區", layout="wide")
st.title("📊 低本益比觀察區")
st.caption("本頁目的是找『市場低估但可能開始轉強』的股票，不是單純尋找便宜股票。便宜很多時候是有原因的。")
st.warning("⚠️ 本頁不代表基本面轉強，僅代表目前市場給予較低估值。低 PE 不等於必漲，請搭配盤石 CBA 狀態綜合判斷。")
st.info("💡 最有價值的組合：TRUE_B + ACCUMULATING + PE < 14 → ⭐ HIGH CONVICTION")


# ── 載入資料 ──────────────────────────────────────────────────────────────────
eps_csv = os.path.join(BASE_PATH, "eps_latest.csv")
if not os.path.exists(eps_csv):
    st.warning("EPS 資料尚未產生，請等週六 Actions 跑完")
    st.stop()

eps_df = pd.read_csv(eps_csv, dtype={"stock_id": str})


def _load_decisions(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype=str)
        keep = [c for c in NEED_COLS if c in df.columns]
        return df[keep]
    except Exception:
        return pd.DataFrame()


dec1 = _load_decisions(os.path.join(BASE_PATH, "latest_decisions.csv"))
dec2 = _load_decisions(os.path.join(BASE_PATH, "latest_decisions_universe.csv"))

dec = pd.concat([dec1, dec2], ignore_index=True).drop_duplicates(
    subset=["stock_id"], keep="first"
)

# left join EPS
df = dec.merge(eps_df[["stock_id", "EPS_TTM", "eps_status"]], on="stock_id", how="left")

# load theme from universe.csv
try:
    uni = pd.read_csv(os.path.join(BASE_PATH, "universe.csv"), dtype=str)[["stock_id", "theme"]]
    df = df.merge(uni, on="stock_id", how="left")
except Exception:
    df["theme"] = ""

df["theme"] = df["theme"].fillna("").astype(str)


# ── 計算 close（用 vwap 代替）和 PE ──────────────────────────────────────────
# latest_decisions.csv 沒有 close 欄，用 vwap 作為當日價格代理
if "vwap" in df.columns:
    df["close"] = pd.to_numeric(df["vwap"], errors="coerce")
else:
    df["close"] = float("nan")

df["EPS_TTM"] = pd.to_numeric(df["EPS_TTM"], errors="coerce")

# 排除殭屍股與無價格
df = df[df["close"].notna() & (df["close"] > 20)]

# 計算 PE
def _calc_pe(row):
    eps = row["EPS_TTM"]
    price = row["close"]
    if pd.isna(eps) or eps <= 0 or pd.isna(price) or price <= 0:
        return None
    pe = price / eps
    return None if (pe != pe or pe > 999) else round(pe, 2)

df["PE"] = df.apply(_calc_pe, axis=1)

def _calc_upside(pe):
    if pe and pe > 0:
        return round(14 / pe, 2)
    return None

df["upside_room"] = df["PE"].apply(_calc_upside)


# ── 排除金融保險銀行 ─────────────────────────────────────────────────────────
exclude_mask = df["theme"].str.contains("金融|保險|銀行", na=False)
df = df[~exclude_mask]


# ── 流動性篩選 ───────────────────────────────────────────────────────────────
if "volume_ratio" in df.columns:
    df["volume_ratio"] = pd.to_numeric(df["volume_ratio"], errors="coerce")
    df = df[df["volume_ratio"].isna() | (df["volume_ratio"] >= 0.5)]


# ── 低本益比篩選 ─────────────────────────────────────────────────────────────
filtered_df = df[df["PE"].notna() & (df["PE"] > 0) & (df["PE"] < 14)].copy()


# ── HIGH CONVICTION 標籤 ─────────────────────────────────────────────────────
def _conviction(row):
    if (str(row.get("B_validity", "")) == "TRUE_B"
            and str(row.get("flow_status", "")) == "ACCUMULATING"
            and row.get("PE") is not None
            and row["PE"] < 14):
        return "⭐ HIGH CONVICTION"
    return ""

filtered_df["conviction"] = filtered_df.apply(_conviction, axis=1)


# ── 排序 ─────────────────────────────────────────────────────────────────────
_validity_order = {"TRUE_B": 0, "UNCERTAIN": 1, "FAKE_B": 2}
filtered_df["_v_order"] = filtered_df["B_validity"].map(_validity_order).fillna(9)
filtered_df = filtered_df.sort_values(["PE", "_v_order"]).reset_index(drop=True)


# ── PE 顯示格式 ───────────────────────────────────────────────────────────────
def _fmt_pe(pe):
    if pe is None or (isinstance(pe, float) and pe != pe):
        return "N/A"
    if pe < 8:
        return f"🟢 {pe:.1f}x 超低估"
    return f"⚪ {pe:.1f}x"

def _fmt_eps(v):
    try:
        f = float(v)
        return f"{f:.2f}" if f == f else "N/A"
    except Exception:
        return "N/A"

def _fmt_upside(v):
    try:
        f = float(v)
        return f"{f:.2f}x" if f == f else "N/A"
    except Exception:
        return "N/A"


# ── 顯示 ─────────────────────────────────────────────────────────────────────
st.metric("符合條件股票數", len(filtered_df))

if filtered_df.empty:
    st.info("目前無符合條件的股票（PE < 14、有 EPS、非金融保險）")
    st.stop()

display_cols = {
    "conviction":          "亮點",
    "stock_id":            "代號",
    "name":                "名稱",
    "close":               "收盤(vwap)",
    "EPS_TTM":             "EPS(TTM)",
    "PE":                  "本益比",
    "upside_room":         "估值空間",
    "C_days":              "C天",
    "B_days":              "B天",
    "A_days":              "A天",
    "B_validity":          "結構",
    "institutional_state": "主力狀態",
    "flow_status":         "Flow",
    "cost_level":          "成本位",
}

# Build display dataframe
out = pd.DataFrame()
for col, label in display_cols.items():
    if col == "PE":
        out[label] = filtered_df["PE"].apply(_fmt_pe)
    elif col == "EPS_TTM":
        out[label] = filtered_df["EPS_TTM"].apply(_fmt_eps)
    elif col == "upside_room":
        out[label] = filtered_df["upside_room"].apply(_fmt_upside)
    elif col == "close":
        out[label] = filtered_df["close"].apply(
            lambda v: f"{v:.1f}" if pd.notna(v) else "N/A"
        )
    elif col in filtered_df.columns:
        out[label] = filtered_df[col].fillna("").astype(str)
    else:
        out[label] = ""

st.dataframe(out, use_container_width=True, hide_index=True)
