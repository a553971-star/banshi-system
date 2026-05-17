# 黃金測試案例 — classify_b_phase
# 固定 row dict，不依賴外部資料或真實股票
# 未來修改 engine 邏輯後必須跑這份確認

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from engine.signals.b_phase import (
    classify_b_phase,
    B_PHASE_LATE,
    B_PHASE_LAUNCH,
    B_PHASE_MATURE,
    B_PHASE_BUILD,
    B_PHASE_PREPARE,
    ALL_B_PHASES,
)

GOLDEN_CASES = [

    # --- Unit Tests：基本狀態 ---
    {
        "name": "unit_late_a5",
        "row": {"B_quality": 90, "A_days": 5},
        "expected": B_PHASE_LATE,
        # Override rule：A_days >= 5 優先於所有條件
    },
    {
        "name": "unit_late_a10",
        "row": {"B_quality": 0, "A_days": 10},
        "expected": B_PHASE_LATE,
    },
    {
        "name": "unit_launch_a1",
        "row": {"B_quality": 70, "A_days": 1},
        "expected": B_PHASE_LAUNCH,
    },
    {
        "name": "unit_launch_a2",
        "row": {"B_quality": 80, "A_days": 2},
        "expected": B_PHASE_LAUNCH,
    },
    {
        "name": "unit_mature_a0",
        "row": {"B_quality": 70, "A_days": 0},
        "expected": B_PHASE_MATURE,
    },
    {
        "name": "unit_build_b50_a0",
        "row": {"B_quality": 50, "A_days": 0},
        "expected": B_PHASE_BUILD,
    },
    {
        "name": "unit_build_b40_a3",
        "row": {"B_quality": 40, "A_days": 3},
        "expected": B_PHASE_BUILD,
    },
    {
        "name": "unit_prepare_b30",
        "row": {"B_quality": 30, "A_days": 0},
        "expected": B_PHASE_PREPARE,
    },

    # --- Precedence Tests：Override rule 優先 ---
    {
        "name": "precedence_late_overrides_launch_quality",
        "row": {"B_quality": 95, "A_days": 5},
        "expected": B_PHASE_LATE,
        # 即使 B_quality 極高，A_days >= 5 仍判 LATE
    },
    {
        "name": "precedence_late_overrides_mature_quality",
        "row": {"B_quality": 100, "A_days": 6},
        "expected": B_PHASE_LATE,
    },

    # --- State Gap Tests：A_days 3–4 不落入 LAUNCH ---
    {
        "name": "state_gap_a3_high_quality_falls_to_build",
        "row": {"B_quality": 70, "A_days": 3},
        "expected": B_PHASE_BUILD,
        # A_days=3 不在 LAUNCH 範圍（1–2），且 B_quality >= 40 → BUILD
    },
    {
        "name": "state_gap_a4_high_quality_falls_to_build",
        "row": {"B_quality": 80, "A_days": 4},
        "expected": B_PHASE_BUILD,
    },
    {
        "name": "state_gap_a3_low_quality_falls_to_prepare",
        "row": {"B_quality": 30, "A_days": 3},
        "expected": B_PHASE_PREPARE,
        # A_days=3，B_quality < 40 → PREPARE
    },

    # --- Boundary Tests：邊界值保護 ---
    {
        "name": "boundary_b_quality_exactly_70_a0",
        "row": {"B_quality": 70, "A_days": 0},
        "expected": B_PHASE_MATURE,
    },
    {
        "name": "boundary_b_quality_69_a0",
        "row": {"B_quality": 69, "A_days": 0},
        "expected": B_PHASE_BUILD,
        # 69 < 70，不進入 MATURE
    },
    {
        "name": "boundary_b_quality_exactly_40",
        "row": {"B_quality": 40, "A_days": 0},
        "expected": B_PHASE_BUILD,
    },
    {
        "name": "boundary_b_quality_39",
        "row": {"B_quality": 39, "A_days": 0},
        "expected": B_PHASE_PREPARE,
        # 39 < 40，不進入 BUILD
    },

    # --- Tolerance Tests：異常輸入容錯 ---
    {
        "name": "tolerance_empty_row",
        "row": {},
        "expected": B_PHASE_PREPARE,
        # 缺欄位時預設 0/0 → PREPARE
    },
    {
        "name": "tolerance_none_values",
        "row": {"B_quality": None, "A_days": None},
        "expected": B_PHASE_PREPARE,
    },
    {
        "name": "tolerance_string_numeric",
        "row": {"B_quality": "75", "A_days": "1"},
        "expected": B_PHASE_LAUNCH,
    },
    {
        "name": "tolerance_float_values",
        "row": {"B_quality": 70.9, "A_days": 1.7},
        "expected": B_PHASE_LAUNCH,
        # int(float(70.9))=70, int(float(1.7))=1
    },
]

CONSTANTS_CASES = [
    {
        "name": "all_phases_contains_late",
        "check": B_PHASE_LATE in ALL_B_PHASES,
    },
    {
        "name": "all_phases_contains_launch",
        "check": B_PHASE_LAUNCH in ALL_B_PHASES,
    },
    {
        "name": "all_phases_contains_mature",
        "check": B_PHASE_MATURE in ALL_B_PHASES,
    },
    {
        "name": "all_phases_contains_build",
        "check": B_PHASE_BUILD in ALL_B_PHASES,
    },
    {
        "name": "all_phases_contains_prepare",
        "check": B_PHASE_PREPARE in ALL_B_PHASES,
    },
    {
        "name": "all_phases_length",
        "check": len(ALL_B_PHASES) == 5,
    },
]


def run_tests():
    failed = 0

    print("=== Golden Tests ===")
    for case in GOLDEN_CASES:
        result = classify_b_phase(case["row"])
        ok = result == case["expected"]
        status = "✅" if ok else "❌"
        print(f'{status} {case["name"]}: 預期={case["expected"]}, 實際={result}')
        if not ok:
            failed += 1
            print(f'   FAIL detail | row={case["row"]}')

    print("\n=== Constants Tests ===")
    for case in CONSTANTS_CASES:
        ok = case["check"]
        status = "✅" if ok else "❌"
        print(f'{status} {case["name"]}')
        if not ok:
            failed += 1

    total = len(GOLDEN_CASES) + len(CONSTANTS_CASES)
    if failed:
        raise AssertionError(f'{failed} 個測試失敗')
    print(f'\n全部通過：{total}/{total}')


if __name__ == '__main__':
    run_tests()
