PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000;
PRAGMA foreign_keys = ON;
PRAGMA user_version = 3;


CREATE TABLE IF NOT EXISTS agg_trade (                
  event_type                     TEXT,
  event_time                     INTEGER,                            
  symbol                         TEXT,
  aggregate_trade_id             INTEGER,
  price                          REAL,
  quantity                       REAL,
  first_trade_id                 INTEGER,
  last_trade_id                  INTEGER,
  trade_time                     INTEGER,                            
  is_the_buyer_the_market_maker  TEXT,                            
  recv_unix                      REAL,
  recv_iso                       TEXT,
  PRIMARY KEY (symbol, aggregate_trade_id)
);
CREATE INDEX IF NOT EXISTS index_agg_trade_symbol_recv ON agg_trade(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS trade (
  event_type                     TEXT,
  event_time                     INTEGER,
  symbol                         TEXT,
  trade_id                       INTEGER,
  price                          REAL,
  quantity                       REAL,
  buyer_order_id                 INTEGER,
  seller_order_id                INTEGER,
  trade_time                     INTEGER,
  is_the_buyer_the_market_maker  TEXT,
  recv_unix                      REAL,
  recv_iso                       TEXT,
  PRIMARY KEY (symbol, trade_id)
);
CREATE INDEX IF NOT EXISTS index_trade_symbol_recv ON trade(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS klines1 (
  event_type                      TEXT,
  event_time                      INTEGER,
  symbol                          TEXT,
  kline_start_time                INTEGER,
  kline_close_time                INTEGER,
  symbol2                         TEXT,
  interval                        TEXT,      
  first_trade_id                  INTEGER,
  last_trade_id                   INTEGER,
  open_price                      REAL,
  close_price                     REAL,
  high_price                      REAL,
  low_price                       REAL,
  base_asset_volume               REAL,
  number_of_trades                INTEGER,
  is_this_kline_closed            TEXT,   
  quote_asset_volume              REAL,
  taker_buy_base_asset_volume     REAL,
  taker_buy_quote_asset_volume    REAL,
  recv_unix                       REAL,
  recv_iso                        TEXT,
  PRIMARY KEY (symbol, event_time)
);
CREATE INDEX IF NOT EXISTS index_klines1_symbol_recv ON klines1(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS klines3 (
  event_type                      TEXT,
  event_time                      INTEGER,
  symbol                          TEXT,
  kline_start_time                INTEGER,
  kline_close_time                INTEGER,
  symbol2                         TEXT,
  interval                        TEXT,      
  first_trade_id                  INTEGER,
  last_trade_id                   INTEGER,
  open_price                      REAL,
  close_price                     REAL,
  high_price                      REAL,
  low_price                       REAL,
  base_asset_volume               REAL,
  number_of_trades                INTEGER,
  is_this_kline_closed            TEXT,   
  quote_asset_volume              REAL,
  taker_buy_base_asset_volume     REAL,
  taker_buy_quote_asset_volume    REAL,
  recv_unix                       REAL,
  recv_iso                        TEXT,
  PRIMARY KEY (symbol, event_time)
);
CREATE INDEX IF NOT EXISTS index_klines3_symbol_recv ON klines3(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS klines5 (
  event_type                      TEXT,
  event_time                      INTEGER,
  symbol                          TEXT,
  kline_start_time                INTEGER,
  kline_close_time                INTEGER,
  symbol2                         TEXT,
  interval                        TEXT,      
  first_trade_id                  INTEGER,
  last_trade_id                   INTEGER,
  open_price                      REAL,
  close_price                     REAL,
  high_price                      REAL,
  low_price                       REAL,
  base_asset_volume               REAL,
  number_of_trades                INTEGER,
  is_this_kline_closed            TEXT,   
  quote_asset_volume              REAL,
  taker_buy_base_asset_volume     REAL,
  taker_buy_quote_asset_volume    REAL,
  recv_unix                       REAL,
  recv_iso                        TEXT,
  PRIMARY KEY (symbol, event_time)
);
CREATE INDEX IF NOT EXISTS index_klines5_symbol_recv ON klines5(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS ticker (
  event_type                       TEXT,
  event_time                       INTEGER,
  symbol                           TEXT,
  price_change                     REAL,
  price_change_percent             REAL,
  weighted_average_price           REAL,
  prev_close_price                 REAL,
  last_price                       REAL,
  last_quantity                    REAL,
  best_bid_price                   REAL,
  best_bid_quantity                REAL,
  best_ask_price                   REAL,
  best_ask_quantity                REAL,
  open_price                       REAL,
  high_price                       REAL,
  low_price                        REAL,
  total_traded_base_asset_volume   REAL,
  total_traded_quote_asset_volume  REAL,
  statistics_open_time             INTEGER,
  statistics_close_time            INTEGER,
  first_trade_id                   INTEGER,
  last_trade_id                    INTEGER,
  total_number_of_trades           INTEGER,
  recv_unix                        REAL,
  recv_iso                         TEXT,
  PRIMARY KEY (symbol, event_time)
);
CREATE INDEX IF NOT EXISTS index_ticker_symbol_recv ON ticker(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS bookTicker (
  order_book_update_id   INTEGER,
  symbol                 TEXT,
  best_bid_price         REAL,
  best_bid_qty           REAL,
  best_ask_price         REAL,
  best_ask_qty           REAL,
  recv_unix              REAL,
  recv_iso               TEXT,
  PRIMARY KEY (symbol, order_book_update_id)
);
CREATE INDEX IF NOT EXISTS index_book_ticker_symbol_recv ON bookTicker(symbol, recv_unix);


CREATE TABLE IF NOT EXISTS events (
  event_type        TEXT,
  event_time        INTEGER,
  symbol            TEXT ,
  first_update_id   INTEGER,
  final_update_id   INTEGER,
  price             REAL,
  qty               REAL,
  side              TEXT CHECK(side IN ('bid','ask')),
  recv_unix         REAL,
  recv_iso          TEXT,
  PRIMARY KEY (symbol, final_update_id, price, side)
);
CREATE INDEX IF NOT EXISTS index_events_symbol_recv_side ON events(symbol, recv_unix, side);
