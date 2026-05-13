"""
analyze_ai.py — 對 AI 供應鏈清單跑完整磐石三引擎
讀取 ai_supply_chain.csv，逐支呼叫 _process_stock()，輸出 latest_decisions_ai.csv
不經過 universe 流動性篩選，直接執行三引擎。
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

BASE_PATH   = os.path.dirname(os.path.abspath(__file__))
AI_CSV_PATH = os.path.join(BASE_PATH, "ai_supply_chain.csv")
OUTPUT_PATH = os.path.join(BASE_PATH, "latest_decisions_ai.csv")


def main():
    parser = argparse.ArgumentParser(description="AI 供應鏈磐石掃描")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"),
                        help="分析日期 (YYYY-MM-DD)，預設今天")
    parser.add_argument("--params", default="params.json",
                        help="params.json 路徑")
    args = parser.parse_args()

    ai_df     = pd.read_csv(AI_CSV_PATH, dtype=str)
    stock_ids = ai_df["stock_id"].dropna().unique().tolist()
    logger.info("AI 供應鏈：%d 支，分析日期：%s", len(stock_ids), args.date)

    params = load_params(args.params)
    prev_states = {}
    results     = []
    failed      = 0

    for i, sid in enumerate(stock_ids, 1):
        try:
            decision = _process_stock(
                stock_id=sid,
                date=args.date,
                params=params,
                prev_states=prev_states,
                print_snapshot=False,
            )
            if decision is not None:
                results.append(decision)
        except Exception as exc:
            failed += 1
            print(f"❌ {sid} failed: {exc}")

        if i % 50 == 0:
            logger.info("進度：%d / %d（成功 %d，失敗 %d）",
                        i, len(stock_ids), len(results), failed)

    logger.info("掃描完成：成功 %d / %d，失敗 %d", len(results), len(stock_ids), failed)

    if results:
        rows = [{col: d.get(col, "") for col in _LATEST_COLS} for d in results]
        pd.DataFrame(rows).to_csv(OUTPUT_PATH, index=False)
        logger.info("輸出：%s（%d 筆）", OUTPUT_PATH, len(rows))
    else:
        logger.warning("無任何結果，未輸出檔案")


if __name__ == "__main__":
    main()
