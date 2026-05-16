# 黃金測試案例 — get_battle_room
# 固定 row dict，不依賴外部資料或真實股票
# 未來修改 engine 邏輯後必須跑這份確認

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from engine.signals.battle_room import get_battle_room

GOLDEN_CASES = [

    # --- Unit Tests：基本條件 ---
    {
        "name": "attack_full_conditions",
        "row": {"B_days": 8, "A_days": 3, "C_days": 4, "B_quality": 50, "volume_ratio": 1.0, "flow_status": "ACCUMULATING"},
        "expected": "ATTACK",
    },
    {
        "name": "launch_a1_not_attack",
        "row": {"B_days": 8, "A_days": 1, "C_days": 0, "B_quality": 45, "volume_ratio": 0.5, "flow_status": "ACCUMULATING"},
        "expected": "LAUNCH",
    },
    {
        "name": "prepare_a0_vr07",
        "row": {"B_days": 8, "A_days": 0, "C_days": 0, "B_quality": 45, "volume_ratio": 0.7, "flow_status": "ACCUMULATING"},
        "expected": "PREPARE",
    },
    {
        "name": "b_too_low",
        "row": {"B_days": 7, "A_days": 3, "C_days": 4, "B_quality": 50, "volume_ratio": 1.0, "flow_status": "ACCUMULATING"},
        "expected": None,
    },
    {
        "name": "bq_too_low_for_launch_prepare",
        "row": {"B_days": 8, "A_days": 1, "C_days": 0, "B_quality": 44, "volume_ratio": 0.7, "flow_status": "ACCUMULATING"},
        "expected": None,
    },

    # --- Precedence Tests：優先順序驗證 ---
    {
        "name": "precedence_distribution_blocks_all",
        "row": {"B_days": 8, "A_days": 3, "C_days": 4, "B_quality": 50, "volume_ratio": 1.0, "flow_status": "DISTRIBUTION"},
        "expected": None,
        # DISTRIBUTION 優先於所有 signal
    },
    {
        "name": "precedence_distribution_blocks_even_extreme",
        "row": {"B_days": 20, "A_days": 5, "C_days": 10, "B_quality": 99, "volume_ratio": 5.0, "flow_status": "DISTRIBUTION"},
        "expected": None,
        # 即使所有條件超標，DISTRIBUTION 仍然 block
    },
    {
        "name": "precedence_attack_over_launch_boundary_a2",
        "row": {"B_days": 8, "A_days": 2, "C_days": 3, "B_quality": 45, "volume_ratio": 0.5, "flow_status": "ACCUMULATING"},
        "expected": "ATTACK",
        # A=2 同時符合 ATTACK(2<=A<=6,C>=3) 與 LAUNCH(1<=A<=2)
        # 驗證 ATTACK 優先於 LAUNCH
    },

    # --- Boundary Tests：邊界碰撞 ---
    {
        "name": "boundary_bq_exactly_45",
        "row": {"B_days": 8, "A_days": 1, "C_days": 0, "B_quality": 45, "volume_ratio": 0.5, "flow_status": "ACCUMULATING"},
        "expected": "LAUNCH",
        # bq=45 剛好符合 LAUNCH 門檻
    },
    {
        "name": "boundary_bq_44_below_threshold",
        "row": {"B_days": 8, "A_days": 1, "C_days": 0, "B_quality": 44, "volume_ratio": 0.5, "flow_status": "ACCUMULATING"},
        "expected": None,
        # bq=44 剛好低於 LAUNCH 門檻
    },
    {
        "name": "boundary_b_exactly_8",
        "row": {"B_days": 8, "A_days": 3, "C_days": 4, "B_quality": 50, "volume_ratio": 1.0, "flow_status": "ACCUMULATING"},
        "expected": "ATTACK",
        # B=8 剛好符合門檻
    },
    {
        "name": "boundary_c_exactly_3",
        "row": {"B_days": 8, "A_days": 2, "C_days": 3, "B_quality": 45, "volume_ratio": 0.5, "flow_status": "ACCUMULATING"},
        "expected": "ATTACK",
        # C=3 剛好符合 ATTACK 門檻
    },

    # --- Tolerance Tests：容錯 ---
    {
        "name": "tolerance_empty_row",
        "row": {},
        "expected": None,
    },
]


def run_tests():
    passed = 0
    failed = 0
    for case in GOLDEN_CASES:
        result = get_battle_room(case["row"])
        ok = result == case["expected"]
        status = "✅" if ok else "❌"
        print(f'{status} {case["name"]}: 預期={case["expected"]}, 實際={result}')
        if not ok:
            failed += 1
            print(f'   FAIL detail | row={case["row"]}')
    if failed:
        raise AssertionError(f'{failed} 個測試失敗，請檢查上方輸出')
    print(f'\n全部通過：{passed + (len(GOLDEN_CASES) - failed)}/{len(GOLDEN_CASES)}')


if __name__ == '__main__':
    run_tests()
