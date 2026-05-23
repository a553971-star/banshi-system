"""
utils/market_cap.py — 市值自動計算模組 v1.0

設計原則：
1. 市值是 enhancement，rotation 訊號不得依賴它
2. 策略與資料分離，rotation_groups.json 不動，市值用 join 注入
3. 單檔失敗不中斷整體 build
4. 排序使用 stable sort（(- market_cap, stock_code)）
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

VERSION = "v1.0"

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB  = os.path.join(BASE_DIR, "banshi.db")
GROUPS_JSON = os.path.join(BASE_DIR, "config", "rotation_groups.json")
DEFAULT_OUT = os.path.join(BASE_DIR, "data", "market_cap.json")


# ── 市值階層門檻（億元） ───────────────────────────────────────────────────
MARKET_CAP_TIERS = {
    "mega":  5000,
    "large": 1000,
    "mid":   300,
    "small": 0,
}


# ── 外資鎖定名單 ───────────────────────────────────────────────────────────
FOREIGN_FOCUS_STOCKS = [
    "3037", "8046",
    "6488",
    "3017", "3324",
    "2308", "2301",
    "2383", "6274",
    "2345", "3533",
    "2317", "2382", "6669", "3231",
    "2330", "2454", "3711",
]


# ── 內建股本備援表（單位：千股） ───────────────────────────────────────────
# 用途：banshi.db 無股本資料、FinMind TaiwanStockInfo 也未涵蓋時使用
# 來源：2026/05 估算（個股財報公告流通股數）
FALLBACK_SHARES: Dict[str, int] = {
    # AI 伺服器 / ODM
    "2317": 13864000, "2382":   386700, "6669":   175200, "3231": 2877000,
    "2376":   633000, "2356":  3590000, "3706":  1107000,

    # 電源
    "2308":  2598000, "2301":  2350000, "6412":   322000,
    "3653":   215000, "6282":  1500000,

    # 散熱
    "3017":   372000, "3324":   178000, "2421":   327000,
    "3483":    95000, "8996":    65000, "2402":    65000, "6124":    60000,

    # ABF 載板
    "3037":  1517000, "8046":   645000, "3189":   446000, "4958":   928000,

    # 矽晶圓
    "6488":   435000, "6182":  1310000, "3016":   460000,
    "3532":   380000, "5483":   698000,

    # CCL / PCB 材料
    "2383":   322000, "6274":   285000, "6213":   396000, "6672":   130000,
    "8358":   142000, "8021":   110000, "5498":    85000,
    "1815":   350000, "5475":    90000,

    # 光通訊 / CPO / AEC
    "3363":    76000, "3081":    95000, "4979":   110000, "3163":    80000,
    "3533":   100000, "8155":   100000, "3665":   120000,
    "2345":   553000, "3596":   226000, "3380":   250000,

    # 跨棒（巨型對照）
    "2330": 25930000, "2454":   160000, "3711":  4348000,
}


# ── 工具函式 ───────────────────────────────────────────────────────────────
def classify_tier(market_cap_billion: float) -> str:
    """根據市值（億元）分類為 mega / large / mid / small。"""
    if market_cap_billion >= MARKET_CAP_TIERS["mega"]:
        return "mega"
    if market_cap_billion >= MARKET_CAP_TIERS["large"]:
        return "large"
    if market_cap_billion >= MARKET_CAP_TIERS["mid"]:
        return "mid"
    return "small"


def is_foreign_focus(stock_code: str) -> bool:
    return str(stock_code) in FOREIGN_FOCUS_STOCKS


def get_shares_outstanding(stock_code: str, db_path: str = DEFAULT_DB) -> Optional[int]:
    """
    取得流通股數（股）。
    優先序：banshi.db (未來擴充) → FALLBACK_SHARES（千股 × 1000）
    查不到回傳 None。
    """
    code = str(stock_code).strip()
    # 未來若 banshi.db 加入 shares_outstanding 欄位，這裡接資料庫
    # 目前直接走 FALLBACK_SHARES
    in_thousand = FALLBACK_SHARES.get(code)
    if in_thousand is None:
        return None
    return int(in_thousand) * 1000


def _fetch_latest_close(conn: sqlite3.Connection, stock_code: str) -> Optional[Dict[str, Any]]:
    """取最近一筆收盤價與日期。"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, close
          FROM price_history
         WHERE stock_id = ?
           AND close IS NOT NULL
         ORDER BY date DESC
         LIMIT 1
        """,
        (str(stock_code),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {"date": row[0], "close": float(row[1])}


def _fetch_stock_name(conn: sqlite3.Connection, stock_code: str) -> str:
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM stock_names WHERE stock_id = ?", (str(stock_code),))
        row = cur.fetchone()
        return row[0] if row and row[0] else ""
    except Exception:
        return ""


def calc_market_cap(stock_code: str, db_path: str = DEFAULT_DB) -> Dict[str, Any]:
    """
    計算單一個股市值。

    失敗時 raise ValueError，由上層 build_market_cap_index 處理。
    """
    code = str(stock_code).strip()

    shares = get_shares_outstanding(code, db_path)
    if shares is None:
        raise ValueError(f"no shares_outstanding data for {code}")

    conn = sqlite3.connect(db_path)
    try:
        price_info = _fetch_latest_close(conn, code)
        name = _fetch_stock_name(conn, code)
    finally:
        conn.close()

    if not price_info:
        raise ValueError(f"no price data for {code}")

    close = price_info["close"]
    market_cap = close * shares                  # 元
    market_cap_billion = market_cap / 1e8        # 億元
    tier = classify_tier(market_cap_billion)

    return {
        "stock_code":         code,
        "name":               name,
        "close":              close,
        "shares_outstanding": shares,
        "market_cap":         market_cap,
        "market_cap_billion": round(market_cap_billion, 2),
        "tier":               tier,
        "foreign_focus":      is_foreign_focus(code),
        "float_ratio":        None,   # 預留欄位，Phase 3 用
        "as_of_date":         price_info["date"],
    }


# ── 主要產出函式 ───────────────────────────────────────────────────────────
def build_market_cap_index(
    config_path: str = GROUPS_JSON,
    output_path: str = DEFAULT_OUT,
    db_path: str = DEFAULT_DB,
) -> Dict[str, Any]:
    """
    為所有族群成員計算市值，輸出索引檔 data/market_cap.json。

    容錯紀律：
    - 任一股票失敗時 try/except 並加入 missing_stocks
    - 單檔 exception 絕對不中斷整體 build
    - 即使所有股票都失敗，仍回傳結構完整的 dict（stocks 為空）

    排序紀律（stable sort）：
    - 主排序：market_cap_billion desc
    - 次排序：stock_code asc（tie-breaker）
    """
    result: Dict[str, Any] = {
        "version":              VERSION,
        "generated_at":         datetime.now().isoformat(timespec="seconds"),
        "as_of_date":           None,
        "stocks":               {},
        "groups_sorted_by_cap": {},
        "missing_stocks":       [],
    }

    # 載入族群設定
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            groups_config = json.load(f)
    except Exception as e:
        result["missing_stocks"].append({
            "code":   "*config*",
            "reason": f"failed to load rotation_groups.json: {e}",
        })
        return result

    # stock_code -> set(group_id) 反查
    code_groups: Dict[str, List[str]] = {}
    all_codes: List[str] = []
    for group in groups_config.get("groups", []):
        gid = group.get("group_id", "")
        for member in group.get("members", []):
            code = str(member.get("code", "")).strip()
            if not code:
                continue
            code_groups.setdefault(code, []).append(gid)
            if code not in all_codes:
                all_codes.append(code)

    # 逐檔計算（單檔失敗不中斷）
    latest_dates: List[str] = []
    for code in all_codes:
        try:
            info = calc_market_cap(code, db_path)
            result["stocks"][code] = {
                "name":               info["name"],
                "close":              info["close"],
                "shares_outstanding": info["shares_outstanding"],
                "market_cap_billion": info["market_cap_billion"],
                "tier":               info["tier"],
                "foreign_focus":      info["foreign_focus"],
                "float_ratio":        info["float_ratio"],
                "as_of_date":         info["as_of_date"],
                "groups":             code_groups.get(code, []),
            }
            if info["as_of_date"]:
                latest_dates.append(info["as_of_date"])
        except Exception as e:
            reason = str(e)[:100]
            result["missing_stocks"].append({"code": code, "reason": reason})
            print(f"[WARN] 市值計算失敗 {code}: {reason}")
            continue  # 關鍵：不中斷

    # 取最新 as_of_date
    if latest_dates:
        result["as_of_date"] = max(latest_dates)

    # 為每個族群產生「按市值排序」清單（stable sort）
    for group in groups_config.get("groups", []):
        gid = group.get("group_id", "")
        members_with_cap = []
        for member in group.get("members", []):
            code = str(member.get("code", "")).strip()
            stock_info = result["stocks"].get(code)
            if stock_info is None:
                continue
            members_with_cap.append({
                "code":               code,
                "name":               stock_info["name"],
                "market_cap_billion": stock_info["market_cap_billion"],
                "tier":               stock_info["tier"],
                "foreign_focus":      stock_info["foreign_focus"],
                "stock_tier":         member.get("tier", "second"),  # leader/second/elastic
            })
        # Stable sort：market_cap_billion desc，再 stock_code asc
        members_with_cap.sort(
            key=lambda x: (-x["market_cap_billion"], x["code"])
        )
        result["groups_sorted_by_cap"][gid] = members_with_cap

    # 寫檔
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        print(f"[WARN] 寫入 market_cap.json 失敗: {e}")

    return result


if __name__ == "__main__":
    r = build_market_cap_index()
    print(f"\n=== market_cap.json 建立結果 ===")
    print(f"version:       {r['version']}")
    print(f"as_of_date:    {r['as_of_date']}")
    print(f"總成員數:      {len(r['stocks'])}")
    print(f"missing 成員:  {len(r['missing_stocks'])}")
    if r["missing_stocks"]:
        for m in r["missing_stocks"]:
            print(f"  - {m['code']}: {m['reason']}")

    sorted_by_cap = sorted(
        r["stocks"].items(),
        key=lambda kv: (-kv[1]["market_cap_billion"], kv[0]),
    )
    print(f"\n市值最大三檔：")
    for code, info in sorted_by_cap[:3]:
        print(f"  {code} {info['name']:<8} {info['market_cap_billion']:>10.2f} 億  ({info['tier']})")

    print(f"\n市值最小三檔：")
    for code, info in sorted_by_cap[-3:]:
        print(f"  {code} {info['name']:<8} {info['market_cap_billion']:>10.2f} 億  ({info['tier']})")
