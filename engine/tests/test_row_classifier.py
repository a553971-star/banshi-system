import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import pandas as pd
from engine.signals.row_classifier import classify_rows

def make_df(rows):
    return pd.DataFrame(rows)

EMPTY_OVERRIDES = {}

def run_tests():
    failed = 0

    print("=== Unit Tests ===")

    df = make_df([
        {"stock_id": "2330", "decision": "BUY",    "C_days": 3},
        {"stock_id": "2317", "decision": "WAIT",   "C_days": 3},
        {"stock_id": "2412", "decision": "IGNORE", "C_days": 6},
    ])
    action, watchlist, candidate = classify_rows(df, EMPTY_OVERRIDES)
    assert len(action) == 1 and action.iloc[0]["stock_id"] == "2330", "FAIL: action"
    assert len(watchlist) == 1 and watchlist.iloc[0]["stock_id"] == "2317", "FAIL: watchlist"
    assert len(candidate) == 1 and candidate.iloc[0]["stock_id"] == "2412", "FAIL: candidate"
    print("✅ basic_classification")

    df = make_df([
        {"stock_id": "2412", "decision": "IGNORE", "C_days": 4},
        {"stock_id": "2454", "decision": "IGNORE", "C_days": 5},
        {"stock_id": "2308", "decision": "IGNORE", "C_days": 6},
    ])
    _, _, candidate = classify_rows(df, EMPTY_OVERRIDES)
    assert len(candidate) == 2, f"FAIL: candidate C_days filter, got {len(candidate)}"
    print("✅ candidate_c_days_filter")

    print("\n=== Precedence Tests ===")

    df = make_df([
        {"stock_id": "2330", "decision": "BUY",    "C_days": 3},
        {"stock_id": "2317", "decision": "IGNORE", "C_days": 3},
    ])
    overrides = {"2317": True}
    action, watchlist, candidate = classify_rows(df, overrides)
    assert len(action) == 1, "FAIL: BUY stays in action"
    assert len(watchlist) == 1 and watchlist.iloc[0]["stock_id"] == "2317", "FAIL: override to watchlist"
    assert len(candidate) == 0, "FAIL: override not in candidate"
    print("✅ override_moves_to_watchlist")

    df = make_df([
        {"stock_id": "2330", "decision": "BUY", "C_days": 6},
    ])
    overrides = {"2330": True}
    action, watchlist, candidate = classify_rows(df, overrides)
    assert len(action) == 1, "FAIL: BUY stays in action"
    assert len(watchlist) == 0, "FAIL: BUY not in watchlist"
    print("✅ action_priority_over_override")

    print("\n=== Boundary Tests ===")

    df = make_df([
        {"stock_id": "A", "decision": "IGNORE", "C_days": 4},
        {"stock_id": "B", "decision": "IGNORE", "C_days": 5},
    ])
    _, _, candidate = classify_rows(df, EMPTY_OVERRIDES)
    ids = set(candidate["stock_id"].tolist())
    assert "A" not in ids, "FAIL: C_days=4 not candidate"
    assert "B" in ids, "FAIL: C_days=5 is candidate"
    print("✅ boundary_c_days_4_vs_5")

    print("\n=== Tolerance Tests ===")

    action, watchlist, candidate = classify_rows(pd.DataFrame(), EMPTY_OVERRIDES)
    assert action.empty and watchlist.empty and candidate.empty, "FAIL: empty df"
    print("✅ tolerance_empty_df")

    df = make_df([{"stock_id": "2330", "decision": "BUY", "C_days": 3}])
    action, _, _ = classify_rows(df, {})
    assert len(action) == 1, "FAIL: empty overrides"
    print("✅ tolerance_empty_overrides")

    print(f"\n全部通過")

if __name__ == '__main__':
    run_tests()
