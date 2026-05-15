"""
fetch_eps.py — 抓取全市場 EPS_TTM（近四季加總）
Usage: python fetch_eps.py
Output: eps_latest.csv
"""

import os
import time
import datetime
import pandas as pd
from FinMind.data import DataLoader

TOKEN = os.getenv("FINMIND_TOKEN", "")

SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
EPS_CSV        = os.path.join(SCRIPT_DIR, "eps_latest.csv")
COMPANIES_CSV  = os.path.join(SCRIPT_DIR, "companies.csv")
UNIVERSE_CSV   = os.path.join(SCRIPT_DIR, "universe.csv")
AI_CSV         = os.path.join(SCRIPT_DIR, "ai_supply_chain.csv")
CACHE_DAYS     = 7
SLEEP_SEC      = 0.4
PROGRESS_EVERY = 30


def _load_stock_ids() -> list:
    ids = set()
    for path, col in [
        (COMPANIES_CSV, "stock_id"),
        (UNIVERSE_CSV,  "stock_id"),
    ]:
        try:
            df = pd.read_csv(path, dtype=str)
            if col in df.columns:
                ids.update(df[col].dropna().tolist())
        except Exception:
            pass
    try:
        df = pd.read_csv(AI_CSV, dtype=str, header=None)
        ids.update(df.iloc[:, 0].dropna().tolist())
    except Exception:
        pass

    cleaned = []
    for raw in ids:
        s = str(raw).strip()
        if s.endswith(".0"):
            s = s[:-2]
        if s and s.isdigit() and 4 <= len(s) <= 6:
            cleaned.append(s)
    return sorted(set(cleaned))


def _cache_valid() -> bool:
    if not os.path.exists(EPS_CSV):
        return False
    try:
        df = pd.read_csv(EPS_CSV, dtype=str)
        if "eps_update_date" not in df.columns or df.empty:
            return False
        last = pd.to_datetime(df["eps_update_date"].iloc[0], errors="coerce")
        if pd.isna(last):
            return False
        age = (datetime.date.today() - last.date()).days
        if age <= CACHE_DAYS:
            print(f"沿用快取：eps_latest.csv（{last.date()}）")
            return True
    except Exception:
        pass
    return False


def _detect_columns(df: pd.DataFrame):
    cols = {c.lower(): c for c in df.columns}
    print(f"  欄位：{list(df.columns)}")

    # EPS filter column
    type_col = None
    for candidate in ["type", "origin_name"]:
        if candidate in cols:
            type_col = cols[candidate]
            break

    # value column
    value_col = cols.get("value")

    # date column
    date_col = cols.get("date")

    return type_col, value_col, date_col


def _fetch_eps(dl: DataLoader, stock_id: str, type_col, value_col, date_col):
    try:
        df = dl.taiwan_stock_financial_statement(
            stock_id=stock_id,
            start_date="2022-01-01",
        )
    except Exception as e:
        return None, None, 0, str(e)

    if df is None or (hasattr(df, "empty") and df.empty):
        return None, None, 0, "empty"

    # First call: detect columns
    if type_col is None and value_col is None:
        return df, None, 0, "detect"

    # Filter EPS rows
    try:
        if type_col:
            mask = df[type_col].astype(str).str.upper().str.contains("EPS")
            eps_df = df[mask].copy()
        else:
            eps_df = df.copy()
    except Exception:
        return None, None, 0, "filter_error"

    if eps_df.empty:
        return None, None, 0, "no_eps_rows"

    if not value_col or value_col not in eps_df.columns:
        return None, None, 0, "no_value_col"
    if not date_col or date_col not in eps_df.columns:
        return None, None, 0, "no_date_col"

    eps_df = eps_df.copy()
    eps_df["_val"]  = pd.to_numeric(eps_df[value_col], errors="coerce")
    eps_df["_date"] = pd.to_datetime(eps_df[date_col], errors="coerce")
    eps_df = eps_df.dropna(subset=["_val", "_date"])

    if eps_df.empty:
        return None, None, 0, "all_nan"

    # Deduplicate by year+quarter (keep latest date per quarter)
    eps_df["_year"]    = eps_df["_date"].dt.year
    eps_df["_quarter"] = eps_df["_date"].dt.month.apply(lambda m: (m - 1) // 3 + 1)
    eps_df = (
        eps_df
        .sort_values("_date", ascending=False)
        .drop_duplicates(subset=["_year", "_quarter"])
        .sort_values("_date", ascending=False)
    )

    recent = eps_df.head(4)
    quarters_used = len(recent)
    if quarters_used == 0:
        return None, None, 0, "no_quarters"

    ttm = round(recent["_val"].sum(), 2)
    last_date = recent["_date"].iloc[0].date()
    return ttm, last_date, quarters_used, "ok"


def main():
    stock_ids = _load_stock_ids()
    print(f"股票清單：{len(stock_ids)} 支")

    if _cache_valid():
        return

    dl = DataLoader()
    if TOKEN:
        dl.token = TOKEN

    today = datetime.date.today().isoformat()
    rows = []

    type_col = value_col = date_col = None
    columns_detected = False
    consecutive_fail = 0

    for i, sid in enumerate(stock_ids):
        if i > 0 and i % PROGRESS_EVERY == 0:
            print(f"  進度：{i}/{len(stock_ids)}")

        # --- first call: detect columns ---
        if not columns_detected:
            raw, _, _, status = _fetch_eps(dl, sid, None, None, None)
            time.sleep(SLEEP_SEC)
            if status == "detect" and raw is not None:
                type_col, value_col, date_col = _detect_columns(raw)
                columns_detected = True
                # now re-process the same stock with detected columns
                ttm, last_date, quarters_used, status = _fetch_eps(
                    dl, sid, type_col, value_col, date_col
                )
            elif status == "empty" or raw is None:
                rows.append({
                    "stock_id": sid, "EPS_TTM": None,
                    "last_eps_date": None, "eps_quarters_used": 0,
                    "eps_update_date": today, "eps_status": "MISSING",
                })
                consecutive_fail += 1
                if consecutive_fail > 20:
                    print(f"⚠️ 連續失敗 {consecutive_fail} 次，請確認 API 狀態")
                continue
            else:
                rows.append({
                    "stock_id": sid, "EPS_TTM": None,
                    "last_eps_date": None, "eps_quarters_used": 0,
                    "eps_update_date": today, "eps_status": "MISSING",
                })
                consecutive_fail += 1
                if consecutive_fail > 20:
                    print(f"⚠️ 連續失敗 {consecutive_fail} 次，請確認 API 狀態")
                continue
        else:
            ttm, last_date, quarters_used, status = _fetch_eps(
                dl, sid, type_col, value_col, date_col
            )
            time.sleep(SLEEP_SEC)

        if ttm is None or status not in ("ok",):
            eps_status = "MISSING"
            rows.append({
                "stock_id": sid, "EPS_TTM": None,
                "last_eps_date": None, "eps_quarters_used": 0,
                "eps_update_date": today, "eps_status": eps_status,
            })
            consecutive_fail += 1
            if consecutive_fail > 20:
                print(f"⚠️ 連續失敗 {consecutive_fail} 次，請確認 API 狀態")
        else:
            consecutive_fail = 0
            eps_status = "OK" if ttm > 0 else "NEGATIVE"
            rows.append({
                "stock_id":          sid,
                "EPS_TTM":           ttm,
                "last_eps_date":     str(last_date),
                "eps_quarters_used": quarters_used,
                "eps_update_date":   today,
                "eps_status":        eps_status,
            })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(EPS_CSV, index=False)

    n_ok  = (out_df["eps_status"] == "OK").sum()
    n_neg = (out_df["eps_status"] == "NEGATIVE").sum()
    n_mis = (out_df["eps_status"] == "MISSING").sum()
    print(f"\n完成：總計 {len(out_df)} 支 ｜ OK {n_ok} ｜ NEGATIVE {n_neg} ｜ MISSING {n_mis}")
    print(f"輸出：{EPS_CSV}")


if __name__ == "__main__":
    main()
