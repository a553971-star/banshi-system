import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from engine.position.health_score import (
    calc_health_score, classify_health_state, get_position_health,
    HEALTH_HEALTHY, HEALTH_SHAKEOUT, HEALTH_WEAKENING,
    HEALTH_TREND_RISK, HEALTH_DISTRIBUTION_BREAKDOWN,
)
from engine.position.exit_signals import (
    get_exit_alert,
    EXIT_CRITICAL, EXIT_WARNING, EXIT_WATCH, EXIT_HOLD,
)

GOLDEN_CASES = [
    {
        "name": "healthy_accumulating_mature",
        "row": {"flow_status": "ACCUMULATING", "cost_level": "SAFE",
                "B_phase": "MATURE", "A_days": 2, "volume_ratio": 1.6,
                "B_quality": 75, "pnl_pct": 12},
        "expected_score_range": (85, 100),
        "expected_state": HEALTH_HEALTHY,
        "expected_exit": EXIT_HOLD,
    },
    {
        "name": "critical_distribution_high_risk",
        "row": {"flow_status": "DISTRIBUTION", "cost_level": "HIGH_RISK",
                "B_phase": "LATE", "A_days": 6, "volume_ratio": 2.0,
                "B_quality": 30, "pnl_pct": -8},
        "expected_score_range": (0, 20),
        "expected_state": HEALTH_DISTRIBUTION_BREAKDOWN,
        "expected_exit": EXIT_CRITICAL,
    },
    {
        "name": "watch_late_a_days",
        "row": {"flow_status": "NEUTRAL", "cost_level": "SAFE",
                "B_phase": "LATE", "A_days": 5, "volume_ratio": 1.0,
                "B_quality": 50},
        "expected_score_range": (30, 55),
        "expected_state": HEALTH_WEAKENING,
        "expected_exit": EXIT_WATCH,
    },
    {
        "name": "warning_distribution_only",
        "row": {"flow_status": "DISTRIBUTION", "cost_level": "SAFE",
                "B_phase": "BUILD", "A_days": 1, "volume_ratio": 1.0,
                "B_quality": 50},
        "expected_score_range": (65, 75),
        "expected_state": HEALTH_HEALTHY,
        "expected_exit": EXIT_WARNING,
    },
    {
        "name": "ai_stock_overextended_no_distribution",
        "row": {"flow_status": "ACCUMULATING", "cost_level": "HIGH_RISK",
                "B_phase": "LAUNCH", "A_days": 2, "volume_ratio": 2.5,
                "B_quality": 80, "pnl_pct": 35},
        "expected_score_range": (85, 100),
        "expected_state": HEALTH_HEALTHY,
        "expected_exit": EXIT_WATCH,
        # 過熱但無 DISTRIBUTION，不 CRITICAL，體現 AI 主升股哲學
    },
    {
        "name": "tolerance_empty_row",
        "row": {},
        "expected_score_range": (45, 60),
        "expected_state": HEALTH_SHAKEOUT,
        "expected_exit": EXIT_HOLD,
    },
]

def run_tests():
    failed = 0
    for case in GOLDEN_CASES:
        score = calc_health_score(case["row"])
        state = classify_health_state(score)
        alert = get_exit_alert(case["row"])

        ok_score = case["expected_score_range"][0] <= score <= case["expected_score_range"][1]
        ok_state = state == case["expected_state"]
        ok_exit  = alert["level"] == case["expected_exit"]

        ok = ok_score and ok_state and ok_exit
        status = "✅" if ok else "❌"
        print(f'{status} {case["name"]}: score={score}, state={state}, exit={alert["level"]}')
        if not ok_score:
            print(f'   score FAIL: {score} 不在 {case["expected_score_range"]}')
        if not ok_state:
            print(f'   state FAIL: 預期={case["expected_state"]}, 實際={state}')
        if not ok_exit:
            print(f'   exit FAIL: 預期={case["expected_exit"]}, 實際={alert["level"]}')
        if not ok:
            failed += 1

    if failed:
        raise AssertionError(f'{failed} 個測試失敗')
    print(f'\n全部通過：{len(GOLDEN_CASES)}/{len(GOLDEN_CASES)}')

if __name__ == '__main__':
    run_tests()
