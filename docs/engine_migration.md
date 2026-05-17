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

## Phase 2 預計
- ui/labels.py：統一 presentation mapping（VOLUME_SIGNAL_LABELS 移出 engine）
- signal constant 化：ATTACK、LAUNCH、PREPARE、TRUE_B 加入 constants 模組
- analyze_stock() / analyze_market() 中央分析入口

## 未完成
- Phase 2 Step B：extract analyze_mistakes / analyze_winrate / classify_rows（UI pollution）
- Phase 2 Step C：central analyze_stock(row)，統一 main.py / live_analyzer.py 內 inline b_phase
- Presentation Layer：ui/labels.py，VOLUME_SIGNAL_LABELS 移出 engine
