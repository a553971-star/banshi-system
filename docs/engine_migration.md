# 磐石引擎化遷移記錄

## 已抽離
| 模組 | function | 來源 | precedence documented |
|------|----------|------|----------------------|
| engine/ui/ui_b_validity.py | rebuild_b_validity | app.py:1179 | — |
| engine/signals/battle_room.py | get_battle_room | app.py:1428 | ✅ DISTRIBUTION > ATTACK > LAUNCH > PREPARE |
| engine/signals/volume_spike.py | get_volume_spike_tag | app.py:401 | ✅ DISTRIBUTION/HIGH_RISK > VOLUME_ATTACK |
| engine/signals/b_phase.py | classify_b_phase | app.py:1169 | ✅ LATE（override） > LAUNCH > MATURE > BUILD > PREPARE |

## Signal Constants
```
VOLUME_ATTACK = "VOLUME_ATTACK"
VOLUME_DISTRIBUTION = "VOLUME_DISTRIBUTION"
```

## Deprecated
| 函式 | 位置 | 移至 |
|------|------|------|
| calc_b_validity_from_row | app.py | engine/ui/ui_b_validity.py |
| _classify_war | app.py | engine/signals/battle_room.py |
| _volume_spike_tag | app.py | engine/signals/volume_spike.py |
| _volume_spike_tag（簡化版）| ui/sidebar.py | engine/signals/volume_spike.py |
| calc_b_phase_from_row | app.py | engine/signals/b_phase.py |

## Phase 2 已完成
| 模組/修改 | 說明 |
|----------|------|
| engine/signals/b_phase.py | classify_b_phase，State Machine，27 golden cases |
| engine/signals/row_classifier.py | classify_rows，行動清單分類，7 golden cases |
| main.py process_stock_live wrapper | live/batch 共用同一 _process_stock engine |
| main.py inline b_phase | 替換為 classify_b_phase，canonical Single Source of Truth |

## Phase 2 尚未完成
- analyze_stock() 中央分析入口（視需求決定時機，非緊急）
- trajectory_engine 磁碟缺失（既有環境問題，需確認）
- Presentation Layer：ui/labels.py，VOLUME_SIGNAL_LABELS 移出 engine
- signal constant 化：ATTACK、LAUNCH、PREPARE、TRUE_B 加入 constants 模組
