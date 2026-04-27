-- 磐石決策系統 — SQLite Schema
-- Single table: daily_data
-- All optional columns stored as NULL (Python None) when absent.
-- No DEFAULT values. No AUTO-FILL. Absence of data is explicit NULL.

CREATE TABLE IF NOT EXISTS daily_data (
    stock_id         TEXT     NOT NULL,
    date             DATE     NOT NULL,   -- stored as TEXT ISO-8601 YYYY-MM-DD

    -- Price OHLCV
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    volume           INTEGER,

    -- Institutional flows (3-party breakdown)
    foreign_buy      INTEGER,
    foreign_sell     INTEGER,
    foreign_net      INTEGER,

    investment_buy   INTEGER,
    investment_sell  INTEGER,
    investment_net   INTEGER,

    dealer_net       INTEGER,

    -- Margin & short
    margin_balance   INTEGER,
    short_balance    INTEGER,

    PRIMARY KEY (stock_id, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_stock_date
    ON daily_data (stock_id, date);
