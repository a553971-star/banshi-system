"""
seed_fake_data.py — 磐石系統假資料生成器
執行時間目標：< 1 秒
不 import 任何專案模組
"""
import sqlite3
import numpy as np
from datetime import date, timedelta

DB_PATH = "banshi.db"

# 生成交易日（週一到週五）
def trading_days(start: date, end: date):
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d += timedelta(days=1)
    return days

dates = trading_days(date(2025, 1, 1), date(2026, 4, 24))
N = len(dates)  # 約 330 天

rng = np.random.default_rng(42)  # 固定 seed，deterministic

def make_random_stock(stock_id, base_price, n):
    """一般隨機股票，不設計任何訊號"""
    rows = []
    price = base_price
    for i, d in enumerate(dates):
        price = max(price * (1 + rng.uniform(-0.02, 0.02)), 10.0)
        vol = int(rng.uniform(5000, 20000))
        rows.append((
            stock_id, d,
            round(price * 0.99, 1),   # open
            round(price * 1.01, 1),   # high
            round(price * 0.98, 1),   # low
            round(price, 1),          # close
            vol,                      # volume
            int(rng.uniform(-500, 500)),   # foreign_buy (net用)
            int(rng.uniform(0, 500)),      # foreign_sell
            int(rng.uniform(-300, 300)),   # foreign_net
            int(rng.uniform(-200, 200)),   # investment_buy
            int(rng.uniform(0, 200)),      # investment_sell
            int(rng.uniform(-100, 100)),   # investment_net
            int(rng.uniform(-100, 100)),   # dealer_net
            int(rng.uniform(10000, 20000)),# margin_balance
            int(rng.uniform(1000, 5000)),  # short_balance
        ))
    return rows

def make_buy_stock(stock_id, base_price):
    """
    6213 專用：最後 25 天手動設計成必然觸發 BUY
    前 305 天：隨機背景資料（close 緩慢上漲，不創新低）
    最後 25 天：
      天 1–8：連續創 60 日新低（做 C 段，確保 C_days >= 5）
      天 9–13：貼近 ma20 ±3%，低波動（做 B 段）
      天 14–16：突破 ma20×1.03，量放大（做 A 段）
      天 17–25：foreign_net > 0，margin 持平或下降（ACCUMULATING）
    """
    rows = []
    price = base_price
    margin = 15000

    # 前 305 天：背景資料，價格在 base_price ± 10% 緩步波動
    background_prices = []
    p = base_price
    for i in range(N - 25):
        p = p * (1 + rng.uniform(-0.008, 0.012))
        p = max(p, base_price * 0.85)
        background_prices.append(round(p, 1))

    for i, d in enumerate(dates[:N-25]):
        cp = background_prices[i]
        vol = int(rng.uniform(8000, 15000))
        rows.append((
            stock_id, d,
            round(cp * 0.995, 1),
            round(cp * 1.005, 1),
            round(cp * 0.990, 1),
            cp, vol,
            int(rng.uniform(0, 200)),
            int(rng.uniform(0, 200)),
            int(rng.uniform(-100, 100)),
            int(rng.uniform(-50, 50)),
            int(rng.uniform(0, 50)),
            int(rng.uniform(-30, 30)),
            int(rng.uniform(-30, 30)),
            margin,
            int(rng.uniform(1000, 3000)),
        ))

    # 最後 25 天：手動設計，所有價格基於 C 段低點往上建
    last_dates = dates[N-25:]

    # C 段錨點：從背景最後一個價格開始跌
    c_start = background_prices[-1]  # 例如 280 左右

    # 天 1–8：連續創新低，每天跌 1.5%
    # 這讓 trajectory_engine 確認 is_new_low，C_days 累積到 8
    c_prices = []
    cp = c_start
    for i in range(8):
        cp = round(cp * 0.985, 1)
        c_prices.append(cp)

    for i in range(8):
        cp = c_prices[i]
        rows.append((
            stock_id, last_dates[i],
            round(cp * 1.002, 1), round(cp * 1.005, 1),
            round(cp * 0.993, 1), cp,
            int(rng.uniform(8000, 12000)),
            50, 100, -50,
            20, 30, -10, -10,
            margin - i * 100,
            int(rng.uniform(1000, 2000)),
        ))

    # C 段結束後，ma20 已被低點拉低
    # 估算 C 段結束時的 ma20：背景最後20天 + C段8天的混合均值
    c_segment_prices = background_prices[-12:] + c_prices  # 20 天
    ma20_after_c = round(float(np.mean(c_segment_prices)), 1)

    # 天 9–13：貼近 ma20_after_c（±2%），低波動（做 B 段）
    # close 必須在 [ma20_after_c * 0.97, ma20_after_c * 1.03]
    b_price = round(ma20_after_c * 0.995, 1)
    for i in range(5):
        cp = round(b_price * (1 + rng.uniform(-0.004, 0.004)), 1)
        rows.append((
            stock_id, last_dates[8+i],
            round(cp * 0.998, 1), round(cp * 1.003, 1),
            round(cp * 0.997, 1), cp,
            int(rng.uniform(9000, 13000)),
            100, 80, 20,
            10, 15, -5, -5,
            margin - 800 - i * 50,
            int(rng.uniform(1000, 2000)),
        ))

    # 天 14–16：突破 ma20_after_c × 1.035，量放大（做 A 段）
    breakout_price = round(ma20_after_c * 1.035, 1)
    for i in range(3):
        cp = round(breakout_price * (1 + i * 0.003), 1)
        rows.append((
            stock_id, last_dates[13+i],
            round(cp * 0.998, 1), round(cp * 1.008, 1),
            round(cp * 0.995, 1), cp,
            int(18000 + i * 1000),
            300, 100, 200,
            30, 20, 10, 10,
            margin - 1050 - i * 80,
            int(rng.uniform(800, 1500)),
        ))

    # 天 17–25：維持突破，ACCUMULATING 確認
    hold_price = round(ma20_after_c * 1.038, 1)
    for i in range(9):
        cp = round(hold_price * (1 + rng.uniform(-0.003, 0.005)), 1)
        rows.append((
            stock_id, last_dates[16+i],
            round(cp * 0.998, 1), round(cp * 1.005, 1),
            round(cp * 0.995, 1), cp,
            int(rng.uniform(14000, 18000)),
            250, 80, 170,
            20, 15, 5, 5,
            margin - 1290 - i * 60,
            int(rng.uniform(800, 1500)),
        ))

    return rows

# 建立 DB 和資料表
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS daily_data (
    stock_id TEXT NOT NULL,
    date DATE NOT NULL,
    open REAL, high REAL, low REAL, close REAL, volume INTEGER,
    foreign_buy INTEGER, foreign_sell INTEGER, foreign_net INTEGER,
    investment_buy INTEGER, investment_sell INTEGER, investment_net INTEGER,
    dealer_net INTEGER, margin_balance INTEGER, short_balance INTEGER,
    PRIMARY KEY (stock_id, date)
)
""")

cur.execute("DELETE FROM daily_data")  # 清空舊資料

insert_sql = """
INSERT INTO daily_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

all_rows = []
all_rows.extend(make_random_stock("2330", 800.0, N))
all_rows.extend(make_random_stock("2454", 950.0, N))
all_rows.extend(make_buy_stock("6213", 280.0))

cur.executemany(insert_sql, all_rows)
conn.commit()
conn.close()

print(f"完成：{len(all_rows)} 筆資料寫入 {DB_PATH}")
print(f"交易日數：{N}")
print("6213 最後 25 天已設計為必然觸發 BUY 訊號")
