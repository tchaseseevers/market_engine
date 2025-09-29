PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000;
PRAGMA foreign_keys = ON;
PRAGMA user_version = 2;

CREATE TABLE IF NOT EXISTS events (
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms             INTEGER    NOT NULL,
  symbol            TEXT       NOT NULL,
  first_update_id   INTEGER,
  final_update_id   INTEGER,
  side              TEXT       NOT NULL CHECK(side IN ('bid','ask')),
  price             TEXT       NOT NULL,
  qty               TEXT       NOT NULL,
  recv_unix         REAL,
  recv_iso          TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_symbol_ts ON events(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS idx_events_final_id ON events(final_update_id);

CREATE TABLE IF NOT EXISTS agg_trades (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  event_time_ms  INTEGER,
  symbol         TEXT,
  trade_id       INTEGER,
  price          TEXT,
  quantity       TEXT,
  trade_time_ms  INTEGER,
  is_buyer_maker INTEGER,
  recv_unix      REAL,
  recv_iso       TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON agg_trades(symbol, event_time_ms);

CREATE TABLE IF NOT EXISTS klines (
  id                        INTEGER PRIMARY KEY AUTOINCREMENT,
  event_time_ms             INTEGER,
  symbol                    TEXT,
  interval                  TEXT,
  open_time                 INTEGER,
  close_time                INTEGER,
  open_price                TEXT,
  close_price               TEXT,
  high_price                TEXT,
  low_price                 TEXT,
  volume                    TEXT,
  quote_volume              TEXT,
  trades_count              INTEGER,
  taker_buy_volume          TEXT,
  taker_buy_quote_volume    TEXT,
  is_closed                 INTEGER,
  recv_unix                 REAL,
  recv_iso                  TEXT
);
CREATE INDEX IF NOT EXISTS idx_klines_symbol_interval ON klines(symbol, interval, open_time);

CREATE TABLE IF NOT EXISTS tickers (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  event_time_ms      INTEGER,
  symbol             TEXT,
  price_change       TEXT,
  price_change_pct   TEXT,
  weighted_avg_price TEXT,
  prev_close_price   TEXT,
  last_price         TEXT,
  last_qty           TEXT,
  best_bid_price     TEXT,
  best_bid_qty       TEXT,
  best_ask_price     TEXT,
  best_ask_qty       TEXT,
  open_price         TEXT,
  high_price         TEXT,
  low_price          TEXT,
  volume             TEXT,
  quote_volume       TEXT,
  open_time          INTEGER,
  close_time         INTEGER,
  first_trade_id     INTEGER,
  last_trade_id      INTEGER,
  trade_count        INTEGER,
  recv_unix          REAL,
  recv_iso           TEXT
);
CREATE INDEX IF NOT EXISTS idx_tickers_symbol_ts ON tickers(symbol, event_time_ms);

-- Book snapshots table for C program output
CREATE TABLE IF NOT EXISTS book_snapshots (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms      INTEGER    NOT NULL,
  symbol     TEXT       NOT NULL,
  best_bid   TEXT,
  bid1_qty   TEXT,
  best_ask   TEXT,
  ask1_qty   TEXT,
  spread     TEXT,
  mid_price  TEXT,
  total_bid_qty    TEXT,
  total_ask_qty    TEXT,
  depth_imbalance  REAL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_book_snapshots_symbol_ts ON book_snapshots(symbol, ts_ms);