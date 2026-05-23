"""
utils/eps_history.py — 季度 EPS 歷史抓取 + DB schema
v1.0

提供:
- ensure_eps_history_table(): CREATE TABLE IF NOT EXISTS
- fetch_eps_history(stock_ids): 從 FinMind 抓多季 EPS 寫入 banshi.db
- get_quarters_for_stock(stock_id): 讀取單檔的歷史季度 EPS（給 target_price 用）

設計原則:
1. schema migration 冪等(CREATE TABLE IF NOT EXISTS)
2. 抓資料容錯(單檔失敗不中斷整體)
3. UPSERT 邏輯(再次跑相同季度資料會被更新,而非重複插入)
4. 預設只抓 rotation_groups.json 成員(46 檔),不是全市場 655 檔
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB  = os.path.join(BASE_DIR, "banshi.db")
GROUPS_JSON = os.path.join(BASE_DIR, "config", "rotation_groups.json")

# FinMind 設定
FETCH_START_DATE = "2019-01-01"  # 近 6-7 年,確保有 5 年 PE 樣本
SLEEP_SEC        = 0.4


def ensure_eps_history_table(db_path: str = DEFAULT_DB) -> None:
    """建立 eps_history 表（冪等）。"""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS eps_history (
                stock_id           TEXT NOT NULL,
                quarter            TEXT NOT NULL,
                quarter_end_date   TEXT,
                announcement_date  TEXT NOT NULL,
                eps                REAL,
                fetched_at         TEXT,
                PRIMARY KEY (stock_id, quarter)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_eps_history_stock_announce
            ON eps_history(stock_id, announcement_date)
        """)
        conn.commit()
    finally:
        conn.close()


def _load_target_stock_ids() -> List[str]:
    """從 rotation_groups.json 取所有族群成員代號。"""
    with open(GROUPS_JSON, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    ids: List[str] = []
    for g in cfg.get("groups", []):
        for m in g.get("members", []):
            code = str(m.get("code", "")).strip()
            if code and code not in ids:
                ids.append(code)
    return ids


def _quarter_from_date(date_str: str) -> Optional[str]:
    """從季底日期推算季度標籤 e.g. '2024-09-30' → '2024Q3'。"""
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d")
        q = (d.month - 1) // 3 + 1
        return f"{d.year}Q{q}"
    except Exception:
        return None


def fetch_eps_history(
    stock_ids: Optional[List[str]] = None,
    db_path: str = DEFAULT_DB,
    start_date: str = FETCH_START_DATE,
) -> Dict[str, Any]:
    """
    從 FinMind 抓季度 EPS 歷史，寫入 banshi.db.eps_history（UPSERT）。

    容錯紀律:
    - 單檔失敗加入 missing_stocks,不中斷整體
    - FinMind 連線失敗整體 abort 但回傳 partial result
    """
    ensure_eps_history_table(db_path)

    if stock_ids is None:
        stock_ids = _load_target_stock_ids()

    result: Dict[str, Any] = {
        "version":        "v1.0",
        "generated_at":   datetime.now().isoformat(timespec="seconds"),
        "total_stocks":   len(stock_ids),
        "ok_stocks":      0,
        "missing_stocks": [],
        "total_quarters_written": 0,
    }

    try:
        from FinMind.data import DataLoader
    except ImportError as e:
        result["missing_stocks"].append({"code": "*FinMind*", "reason": f"import failed: {e}"})
        return result

    dl = DataLoader()
    token = os.getenv("FINMIND_TOKEN", "")
    if token:
        try:
            dl.login_by_token(api_token=token)
        except Exception:
            dl.token = token

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    fetched_at = datetime.now().isoformat(timespec="seconds")

    for i, sid in enumerate(stock_ids):
        if i > 0 and i % 10 == 0:
            print(f"  進度: {i}/{len(stock_ids)}")
        try:
            df = dl.taiwan_stock_financial_statement(
                stock_id=sid, start_date=start_date,
            )
        except Exception as e:
            result["missing_stocks"].append({"code": sid, "reason": f"fetch error: {str(e)[:80]}"})
            time.sleep(SLEEP_SEC)
            continue

        if df is None or df.empty:
            result["missing_stocks"].append({"code": sid, "reason": "empty"})
            time.sleep(SLEEP_SEC)
            continue

        try:
            import pandas as pd
            # 找 EPS 欄位（FinMind 用 type 或 origin_name 標記項目類型）
            type_col = next((c for c in ("type", "origin_name") if c in df.columns), None)
            value_col = "value" if "value" in df.columns else None
            date_col  = "date" if "date" in df.columns else None
            if not (type_col and value_col and date_col):
                result["missing_stocks"].append({"code": sid, "reason": "schema mismatch"})
                time.sleep(SLEEP_SEC)
                continue

            mask = df[type_col].astype(str).str.upper().str.contains("EPS", na=False)
            eps_df = df[mask].copy()
            if eps_df.empty:
                result["missing_stocks"].append({"code": sid, "reason": "no EPS rows"})
                time.sleep(SLEEP_SEC)
                continue

            eps_df["_eps"]  = pd.to_numeric(eps_df[value_col], errors="coerce")
            eps_df["_date"] = pd.to_datetime(eps_df[date_col], errors="coerce")
            eps_df = eps_df.dropna(subset=["_eps", "_date"])

            # 一個季度可能有多筆,取最新 announcement_date
            eps_df = eps_df.sort_values("_date").drop_duplicates(
                subset=[eps_df["_date"].dt.to_period("Q").astype(str).name],
                keep="last",
            ) if False else eps_df  # 用下方更穩的 group-by

            eps_df["_period"] = eps_df["_date"].dt.to_period("Q").astype(str)
            # 每個 period 取最新 announcement_date
            eps_df = eps_df.sort_values("_date").groupby("_period", as_index=False).last()

            quarters_written = 0
            for _, row in eps_df.iterrows():
                announcement_date = row["_date"].strftime("%Y-%m-%d")
                # period 格式如 "2024Q3"
                quarter = row["_period"]
                # 季底日期推估
                yr, qn = quarter.split("Q")
                quarter_end = {"1": "03-31", "2": "06-30", "3": "09-30", "4": "12-31"}[qn]
                quarter_end_date = f"{yr}-{quarter_end}"
                eps_val = float(row["_eps"])

                cur.execute("""
                    INSERT INTO eps_history(stock_id, quarter, quarter_end_date, announcement_date, eps, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(stock_id, quarter) DO UPDATE SET
                        quarter_end_date  = excluded.quarter_end_date,
                        announcement_date = excluded.announcement_date,
                        eps               = excluded.eps,
                        fetched_at        = excluded.fetched_at
                """, (sid, quarter, quarter_end_date, announcement_date, eps_val, fetched_at))
                quarters_written += 1

            conn.commit()
            result["ok_stocks"] += 1
            result["total_quarters_written"] += quarters_written
        except Exception as e:
            result["missing_stocks"].append({"code": sid, "reason": f"parse error: {str(e)[:80]}"})

        time.sleep(SLEEP_SEC)

    conn.close()
    return result


def get_quarters_for_stock(stock_id: str, db_path: str = DEFAULT_DB) -> List[Dict[str, Any]]:
    """
    取單檔所有歷史季度 EPS,by announcement_date asc。
    回傳 [] 表示無資料（表不存在或該檔無資料）。
    """
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        cur.execute("""
            SELECT quarter, quarter_end_date, announcement_date, eps
              FROM eps_history
             WHERE stock_id = ?
             ORDER BY announcement_date ASC
        """, (str(stock_id),))
        rows = cur.fetchall()
        conn.close()
        return [
            {
                "quarter":           r[0],
                "quarter_end_date":  r[1],
                "announcement_date": r[2],
                "eps":               r[3],
            }
            for r in rows
        ]
    except sqlite3.OperationalError:
        # 表不存在
        return []
    except Exception:
        return []


if __name__ == "__main__":
    ensure_eps_history_table()
    print("eps_history table ensured.")
    ids = _load_target_stock_ids()
    print(f"target stocks ({len(ids)}): {ids[:5]}...")
    if os.getenv("FINMIND_TOKEN"):
        print("FINMIND_TOKEN found, starting fetch...")
        r = fetch_eps_history()
        print(f"OK stocks: {r['ok_stocks']}/{r['total_stocks']}")
        print(f"Quarters written: {r['total_quarters_written']}")
        if r["missing_stocks"]:
            print(f"Missing: {len(r['missing_stocks'])}")
            for m in r["missing_stocks"][:5]:
                print(f"  - {m['code']}: {m['reason']}")
    else:
        print("No FINMIND_TOKEN, skipping actual fetch. Run with FINMIND_TOKEN set in Actions.")
