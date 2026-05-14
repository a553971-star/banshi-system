"""
analyze_ai.py — 產生 AI 供應鏈決策清單
策略：
  1. 先從 latest_decisions_universe.csv 撈已算好的 AI 股票（最快，不重算）
  2. 不在 universe 的 AI 股票，再個別跑 _process_stock()
輸出 latest_decisions_ai.csv
用法：python3 analyze_ai.py [--date YYYY-MM-DD]
"""
import argparse
import logging
import os
from datetime import datetime

import pandas as pd

from main import _process_stock, load_params
from exporter import _LATEST_COLS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_PATH    = os.path.dirname(os.path.abspath(__file__))
AI_CSV_PATH  = os.path.join(BASE_PATH, "ai_supply_chain.csv")
UNI_CSV_PATH = os.path.join(BASE_PATH, "latest_decisions_universe.csv")
OUTPUT_PATH  = os.path.join(BASE_PATH, "latest_decisions_ai.csv")


def main():
    parser = argparse.ArgumentParser(description="AI 供應鏈磐石掃描")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"),
                        help="分析日期 (YYYY-MM-DD)，預設今天")
    parser.add_argument("--params", default="params.json",
                        help="params.json 路徑")
    args = parser.parse_args()

    ai_df     = pd.read_csv(AI_CSV_PATH, dtype=str)
    ai_ids    = set(ai_df["stock_id"].dropna().astype(str).unique())
    logger.info("AI 供應鏈：%d 支，分析日期：%s", len(ai_ids), args.date)

    # ── 名稱對照表（stock_names.csv 優先）────────────────────────────────────
    name_map = {}
    try:
        sn_path = os.path.join(BASE_PATH, "stock_names.csv")
        if os.path.exists(sn_path):
            sn_df = pd.read_csv(sn_path, dtype=str)
            name_map = dict(zip(sn_df["stock_id"], sn_df["name"]))
            logger.info("載入 stock_names.csv：%d 筆", len(name_map))
    except Exception as e:
        logger.warning("無法載入 stock_names.csv：%s", e)

    # ── Step 1：從 universe 結果直接撈 ─────────────────────────────────────────
    rows = []
    covered = set()
    try:
        uni_df = pd.read_csv(UNI_CSV_PATH, dtype=str)
        ai_from_uni = uni_df[uni_df["stock_id"].isin(ai_ids)].copy()
        if name_map:
            ai_from_uni["name"] = ai_from_uni["stock_id"].map(name_map).fillna(ai_from_uni["name"])
        rows.extend(ai_from_uni.to_dict("records"))
        covered = set(ai_from_uni["stock_id"].astype(str))
        logger.info("從 universe 撈到 %d 支 AI 股票", len(covered))
    except Exception as e:
        logger.warning("無法讀取 latest_decisions_universe.csv：%s", e)

    # ── Step 2：剩餘未涵蓋的 AI 股票個別跑引擎 ─────────────────────────────────
    remaining = sorted(ai_ids - covered)
    logger.info("剩餘需個別分析：%d 支", len(remaining))

    if remaining:
        params = load_params(args.params)
        params["companies_path"] = AI_CSV_PATH
        prev_states = {}
        failed = 0

        for i, sid in enumerate(remaining, 1):
            try:
                decision = _process_stock(
                    stock_id=sid,
                    date=args.date,
                    params=params,
                    prev_states=prev_states,
                    print_snapshot=False,
                )
                if decision is not None:
                    rows.append({col: decision.get(col, "") for col in _LATEST_COLS})
            except Exception as exc:
                failed += 1
                print(f"❌ {sid} failed: {exc}")

        logger.info("個別分析完成：成功 %d / %d，失敗 %d",
                    len(remaining) - failed, len(remaining), failed)

    logger.info("總計：%d 筆 AI 股票決策", len(rows))

    if rows:
        out_df = pd.DataFrame(rows)
        for col in _LATEST_COLS:
            if col not in out_df.columns:
                out_df[col] = ""
        out_df[_LATEST_COLS].to_csv(OUTPUT_PATH, index=False)
        logger.info("輸出：%s（%d 筆）", OUTPUT_PATH, len(rows))
    else:
        logger.warning("無任何結果，未輸出檔案")


if __name__ == "__main__":
    main()
