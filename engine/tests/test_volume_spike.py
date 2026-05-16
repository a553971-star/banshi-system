# 黃金測試案例 — get_volume_spike_tag
# 固定 row dict，不依賴外部資料或真實股票
# 未來修改 engine 邏輯後必須跑這份確認

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from engine.signals.volume_spike import (
    get_volume_spike_tag,
    VOLUME_SIGNAL_LABELS,
    VOLUME_ATTACK,
    VOLUME_DISTRIBUTION,
)

GOLDEN_CASES = [

    # --- Unit Tests：基本條件 ---
    {
        "name": "volume_attack_basic",
        "row": {"volume_ratio": 2.0, "daily_return_pct": 4.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": VOLUME_ATTACK,
    },
    {
        "name": "volume_distribution_by_flow",
        "row": {"volume_ratio": 2.0, "daily_return_pct": 4.0,
                "flow_status": "DISTRIBUTION", "cost_level": "SAFE"},
        "expected": VOLUME_DISTRIBUTION,
    },
    {
        "name": "volume_distribution_by_cost",
        "row": {"volume_ratio": 2.0, "daily_return_pct": 4.0,
                "flow_status": "ACCUMULATING", "cost_level": "HIGH_RISK"},
        "expected": VOLUME_DISTRIBUTION,
    },
    {
        "name": "no_signal_low_volume",
        "row": {"volume_ratio": 1.2, "daily_return_pct": 4.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": None,
    },
    {
        "name": "no_signal_low_return",
        "row": {"volume_ratio": 2.0, "daily_return_pct": 2.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": None,
    },

    # --- Precedence Tests：優先順序 ---
    {
        "name": "precedence_distribution_over_attack",
        "row": {"volume_ratio": 5.0, "daily_return_pct": 10.0,
                "flow_status": "DISTRIBUTION", "cost_level": "SAFE"},
        "expected": VOLUME_DISTRIBUTION,
        # 即使量能極大，DISTRIBUTION 仍判為出貨
    },
    {
        "name": "precedence_high_risk_over_attack",
        "row": {"volume_ratio": 5.0, "daily_return_pct": 10.0,
                "flow_status": "ACCUMULATING", "cost_level": "HIGH_RISK"},
        "expected": VOLUME_DISTRIBUTION,
        # HIGH_RISK 也判為出貨訊號
    },

    # --- Boundary Tests：邊界碰撞（保護 rounding drift）---
    {
        "name": "boundary_vr_exactly_15_no_signal",
        "row": {"volume_ratio": 1.5, "daily_return_pct": 4.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": None,
        # vr=1.5 不符合嚴格大於 > 1.5
    },
    {
        "name": "boundary_vr_151_triggers",
        "row": {"volume_ratio": 1.51, "daily_return_pct": 4.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": VOLUME_ATTACK,
    },
    {
        "name": "boundary_dr_exactly_3_no_signal",
        "row": {"volume_ratio": 2.0, "daily_return_pct": 3.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": None,
        # abs(dr)=3 不符合嚴格大於 > 3
    },
    {
        "name": "boundary_dr_negative_large",
        "row": {"volume_ratio": 2.0, "daily_return_pct": -4.0,
                "flow_status": "ACCUMULATING", "cost_level": "SAFE"},
        "expected": VOLUME_ATTACK,
        # 跌幅也算爆量，abs(-4.0) > 3
    },

    # --- Tolerance Tests ---
    {
        "name": "tolerance_empty_row",
        "row": {},
        "expected": None,
    },
]

PRESENTATION_CASES = [
    {
        "name": "label_volume_attack",
        "signal": VOLUME_ATTACK,
        "expected": "🟢 放量攻擊",
    },
    {
        "name": "label_volume_distribution",
        "signal": VOLUME_DISTRIBUTION,
        "expected": "🔴 放量出貨",
    },
    {
        "name": "label_none_returns_empty",
        "signal": None,
        "expected": "",
    },
]


def run_tests():
    failed = 0

    print("=== Signal Tests ===")
    for case in GOLDEN_CASES:
        result = get_volume_spike_tag(case["row"])
        ok = result == case["expected"]
        status = "✅" if ok else "❌"
        print(f'{status} {case["name"]}: 預期={case["expected"]}, 實際={result}')
        if not ok:
            failed += 1
            print(f'   FAIL detail | row={case["row"]}')

    print("\n=== Presentation Mapping Tests ===")
    for case in PRESENTATION_CASES:
        result = VOLUME_SIGNAL_LABELS.get(case["signal"], "")
        ok = result == case["expected"]
        status = "✅" if ok else "❌"
        print(f'{status} {case["name"]}: 預期={case["expected"]}, 實際={result}')
        if not ok:
            failed += 1

    total = len(GOLDEN_CASES) + len(PRESENTATION_CASES)
    if failed:
        raise AssertionError(f'{failed} 個測試失敗')
    print(f'\n全部通過：{total}/{total}')


if __name__ == '__main__':
    run_tests()
