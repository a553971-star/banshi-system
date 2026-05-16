# 黃金測試案例 — rebuild_b_validity
# 固定 row dict，不依賴外部資料或真實股票
# 未來修改 engine 邏輯後必須跑這份確認

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from engine.ui.ui_b_validity import rebuild_b_validity

GOLDEN_CASES = [
    # (row, expected, 說明)
    ({'B_quality': 80, 'flow_status': 'ACCUMULATING'}, 'TRUE_B',    'B高+非DIST → TRUE_B'),
    ({'B_quality': 75, 'flow_status': 'NEUTRAL'},      'TRUE_B',    'B剛好75+NEUTRAL → TRUE_B'),
    ({'B_quality': 75, 'flow_status': 'DISTRIBUTION'}, 'UNCERTAIN', 'B剛好75+DIST → UNCERTAIN（非TRUE_B）'),
    ({'B_quality': 59, 'flow_status': 'DISTRIBUTION'}, 'FAKE_B',    'B<60+DIST → FAKE_B'),
    ({'B_quality': 40, 'flow_status': 'ACCUMULATING'}, 'UNCERTAIN', 'B低+非DIST → UNCERTAIN'),
    ({'B_quality': 60, 'flow_status': 'DISTRIBUTION'}, 'UNCERTAIN', 'B=60+DIST → UNCERTAIN（非FAKE_B）'),
    ({'B_quality': None, 'flow_status': 'ACCUMULATING'}, 'UNCERTAIN', 'None值容錯'),
    ({},                                                'UNCERTAIN', '空row容錯'),
]

def run_tests():
    passed = 0
    for row, expected, desc in GOLDEN_CASES:
        result = rebuild_b_validity(row)
        status = '✅' if result == expected else '❌'
        print(f'{status} {desc}: 預期={expected}, 實際={result}')
        if result != expected:
            raise AssertionError(f'FAIL: {desc}')
        passed += 1
    print(f'\n全部通過：{passed}/{len(GOLDEN_CASES)}')

if __name__ == '__main__':
    run_tests()
