# Engine Layer Rules
# - 不可 import Streamlit
# - 不可 import app.py / pages
# - 僅負責規則與分析
# - 盡量保持 pure function
# - 不直接讀 CSV / DB
#
# 行動清單分類 — Phase 2 Step B
# 來源：app.py classify_rows（第 247 行）
#
# 分類邏輯：
# action    → decision == "BUY"
# watchlist → decision == "WAIT" 或在 overrides 中，且非 action
# candidate → decision == "IGNORE" 且 C_days >= 5，且非 action/watchlist

import pandas as pd

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
