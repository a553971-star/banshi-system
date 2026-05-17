"""
engine/signals/b_phase.py — B 波段階段分類

Signal constants:
    B_PHASE_LATE    : A_days >= 5，趨勢末段，Override rule（最高優先）
    B_PHASE_LAUNCH  : 高品質 B 波剛起漲（A_days 1–2）
    B_PHASE_MATURE  : 高品質 B 波整理中（A_days 0）
    B_PHASE_BUILD   : 中品質 B 波蓄積
    B_PHASE_PREPARE : 品質不足，尚在準備

State Gap（設計意圖）：A_days 3–4 不屬於 LAUNCH，
    落入 BUILD（若 B_quality >= 40）或 PREPARE。
"""

# ── Signal constants ──────────────────────────────────────────────────────────
B_PHASE_LATE    = "LATE"
B_PHASE_LAUNCH  = "LAUNCH"
B_PHASE_MATURE  = "MATURE"
B_PHASE_BUILD   = "BUILD"
B_PHASE_PREPARE = "PREPARE"

ALL_B_PHASES = (
    B_PHASE_LATE,
    B_PHASE_LAUNCH,
    B_PHASE_MATURE,
    B_PHASE_BUILD,
    B_PHASE_PREPARE,
)


def classify_b_phase(row) -> str:
    """Classify the B-wave phase from a row dict.

    Pure function — no I/O, no side effects, no mutation of row.

    Parameters
    ----------
    row : dict-like with keys "B_quality" (int/float) and "A_days" (int/float).

    Returns
    -------
    One of B_PHASE_LATE / B_PHASE_LAUNCH / B_PHASE_MATURE / B_PHASE_BUILD / B_PHASE_PREPARE.
    Never returns None.
    """
    try:
        b_quality = int(float(row.get("B_quality") or 0))
    except Exception:
        b_quality = 0
    try:
        a_days = int(float(row.get("A_days") or 0))
    except Exception:
        a_days = 0

    # Override rule: late-stage regardless of B_quality
    if a_days >= 5:
        return B_PHASE_LATE

    if b_quality >= 70 and 1 <= a_days <= 2:
        return B_PHASE_LAUNCH

    if b_quality >= 70 and a_days == 0:
        return B_PHASE_MATURE

    if b_quality >= 40:
        return B_PHASE_BUILD

    return B_PHASE_PREPARE
