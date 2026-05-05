"""
analyze_universe.py — 對全市場 universe 跑完整磐石三引擎
讀取 universe.csv，逐支呼叫 _process_stock()，輸出 latest_decisions_universe.csv
用法：python3 analyze_universe.py [--date YYYY-MM-DD]
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

BASE_PATH      = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_PATH  = os.path.join(BASE_PATH, "universe.csv")
OUTPUT_PATH    = os.path.join(BASE_PATH, "latest_decisions_universe.csv")


def main():
    parser = argparse.ArgumentParser(description="全市場磐石掃描")
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"),
                        help="分析日期 (YYYY-MM-DD)，預設今天")
    parser.add_argument("--params", default="params.json",
                        help="params.json 路徑")
    args = parser.parse_args()

    # 讀 universe
    universe_df = pd.read_csv(UNIVERSE_PATH, dtype=str)
    stock_ids   = universe_df["stock_id"].tolist()
    logger.info("Universe：%d 支，分析日期：%s", len(stock_ids), args.date)

    params      = load_params(args.params)
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

    # 輸出 CSV（欄位與 latest_decisions.csv 一致）
    if results:
        rows = [{col: d.get(col, "") for col in _LATEST_COLS} for d in results]
        pd.DataFrame(rows).to_csv(OUTPUT_PATH, index=False)
        logger.info("輸出：%s（%d 筆）", OUTPUT_PATH, len(rows))
    else:
        logger.warning("無任何結果，未輸出檔案")


if __name__ == "__main__":
    main()
