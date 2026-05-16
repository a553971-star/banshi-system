# 磐石引擎化遷移記錄

## 已抽離
| 模組 | function | 來源 | precedence documented |
|------|----------|------|----------------------|
| engine/ui/ui_b_validity.py | rebuild_b_validity | app.py:1179 | — |
| engine/signals/battle_room.py | get_battle_room | app.py:1428 | ✅ DISTRIBUTION > ATTACK > LAUNCH > PREPARE |

## Deprecated
| 函式 | 位置 | 移至 |
|------|------|------|
| calc_b_validity_from_row | app.py | engine/ui/ui_b_validity.py |
| _classify_war | app.py | engine/signals/battle_room.py |

## 未完成
- engine/signals/volume_spike.py（_volume_spike_tag）
