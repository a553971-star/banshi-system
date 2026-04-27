"""
good_company.py — 磐石決策系統 Phase 2
Fundamental quality whitelist.

Rules:
- companies.csv is manually maintained. No auto-filtering.
- is_good_company() is a pure function: deterministic for same input.
- Missing file → returns False (logs warning). Never raises.
"""

import csv
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_PATH = "companies.csv"


def load_company_list(path: str = _DEFAULT_PATH) -> dict:
    """Load the whitelist CSV into a dict keyed by stock_id.

    CSV must have at minimum a 'stock_id' column.
    Optional 'name' column is captured when present.
    Returns empty dict if file is missing or unreadable (logs warning).
    Never raises.
    """
    result: dict = {}
    try:
        p = Path(path)
        if not p.exists():
            logger.warning("companies.csv not found at %s", path)
            return result
        with p.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sid = (row.get("stock_id") or "").strip()
                if sid:
                    result[sid] = {
                        "name": (row.get("name") or "").strip(),
                    }
    except Exception as exc:
        logger.error("load_company_list failed (%s): %s", path, exc)
    return result


def is_good_company(stock_id: str, path: str = _DEFAULT_PATH) -> bool:
    """Return True if stock_id is present in the whitelist CSV.

    Pure function. Deterministic for the same stock_id and file state.
    Returns False if stock_id is absent or file is missing. Never raises.
    """
    try:
        companies = load_company_list(path)
        return stock_id in companies
    except Exception as exc:
        logger.error("is_good_company(%s) failed: %s", stock_id, exc)
        return False


def get_company_name(stock_id: str, path: str = _DEFAULT_PATH) -> Optional[str]:
    """Return the display name for a stock_id, or None if not found.

    Pure function. Never raises.
    """
    try:
        companies = load_company_list(path)
        entry = companies.get(stock_id)
        if entry:
            name = entry.get("name", "")
            return name if name else None
        return None
    except Exception as exc:
        logger.error("get_company_name(%s) failed: %s", stock_id, exc)
        return None
