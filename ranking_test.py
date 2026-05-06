import pandas as pd
import numpy as np

df = pd.read_csv('latest_decisions_universe.csv')
print(f"載入資料筆數: {len(df)} 筆")

# 只看 WAIT + BUY
df = df[df['decision'].isin(['WAIT', 'BUY'])].copy()
print(f"WAIT+BUY 篩選後: {len(df)} 筆")

# B_Quality_Norm：Min-Max（不 clip 30）
_bq = pd.to_numeric(df['B_quality'], errors='coerce')
_bq_min, _bq_max = _bq.min(), _bq.max()
if _bq_max > _bq_min:
    df['B_Quality_Norm'] = ((_bq - _bq_min) / (_bq_max - _bq_min) * 100).fillna(0)
else:
    df['B_Quality_Norm'] = 50.0

# Foreign_Profit_Score
foreign_col = next((col for col in ['foreign_profit_pct', 'Foreign_Profit_Pct', 'profit_pct'] if col in df.columns), None)
if foreign_col:
    df['foreign_profit_pct'] = pd.to_numeric(df[foreign_col], errors='coerce')
else:
    df['foreign_profit_pct'] = np.nan
df['Foreign_Profit_Score'] = (100 - (df['foreign_profit_pct'] - 5).abs() * 12).clip(0, 100).fillna(50)

# Fresh_A_Score：A<=5 照舊，A 6~8 給 30，A>8 給 10
def get_fresh_a_score(a):
    if pd.isna(a): return 0
    a = int(a)
    if a == 0: return 0
    mapping = {1: 100, 2: 95, 3: 85, 4: 70, 5: 50}
    if a <= 5: return mapping.get(a, 0)
    if a <= 8: return 30
    return 10

df['A_days_num'] = pd.to_numeric(df['A_days'], errors='coerce')
df['Fresh_A_Score'] = df['A_days_num'].apply(get_fresh_a_score)

# Volume_Norm
if 'volume_ratio' in df.columns:
    df['volume_ratio_num'] = pd.to_numeric(df['volume_ratio'], errors='coerce')
    df['Volume_Norm'] = ((df['volume_ratio_num'].clip(0.5, 3.0) - 0.5) / 2.5 * 100).clip(0, 100).fillna(50)
else:
    df['Volume_Norm'] = 50.0

# C_Recency_Score：max(0, 30 - C_days) * 3.3
df['C_days_num'] = pd.to_numeric(df['C_days'], errors='coerce').fillna(30)
df['C_Recency_Score'] = ((30 - df['C_days_num'].clip(0, 30)) * 3.3).clip(0, 100)

# Total Score
df['total_score'] = (
    df['B_Quality_Norm'].fillna(0)        * 0.40 +
    df['Foreign_Profit_Score'].fillna(50) * 0.25 +
    df['Fresh_A_Score'].fillna(0)         * 0.20 +
    df['Volume_Norm'].fillna(50)          * 0.10 +
    df['C_Recency_Score'].fillna(0)       * 0.05
).round(1)

df = df.sort_values(by='total_score', ascending=False).reset_index(drop=True)

display_cols = ['stock_id', 'name', 'total_score', 'B_quality', 'B_Quality_Norm',
                'Foreign_Profit_Score', 'Fresh_A_Score', 'A_days',
                'Volume_Norm', 'C_days', 'C_Recency_Score', 'decision']
display_cols = [c for c in display_cols if c in df.columns]

print("\n=== Ranking 2.0 前15名（WAIT+BUY）===")
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)
print(df[display_cols].head(15).to_string(index=True))

df.to_csv('ranking_test_result.csv', index=False, encoding='utf-8-sig')
print("\n✅ 測試完成！結果已儲存至 ranking_test_result.csv")
