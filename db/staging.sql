CREATE TABLE IF NOT EXISTS stage_events (
  event_time_ms    INTEGER,
  symbol           TEXT,
  first_update_id  INTEGER,
  final_update_id  INTEGER,
  price            TEXT,
  qty              TEXT,
  side             TEXT,
  recv_unix        REAL,
  recv_iso         TEXT
);
CREATE TABLE IF NOT EXISTS stage_trades (
  event_time_ms    INTEGER,
  symbol           TEXT,
  trade_id         INTEGER,
  price            TEXT,
  quantity         TEXT,
  trade_time_ms    INTEGER,
  is_buyer_maker   INTEGER,
  recv_unix        REAL,
  recv_iso         TEXT
);
CREATE TABLE IF NOT EXISTS stage_klines (
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
CREATE TABLE IF NOT EXISTS stage_tickers (
  event_time_ms      INTEGER,
  symbol             TEXT,
  price_change       TEXT,
  price_change_percent TEXT,
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

DELETE FROM stage_events;
DELETE FROM stage_trades;
DELETE FROM stage_klines;
DELETE FROM stage_tickers;
