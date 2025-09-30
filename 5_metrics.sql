PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 30000000000;
PRAGMA foreign_keys = ON;
BEGIN;


--
CREATE TABLE IF NOT EXISTS minute_trades (
  symbol       TEXT    NOT NULL,
  bucket_ms    INTEGER NOT NULL,      -- minute start (ms since epoch)
  n_trades     INTEGER NOT NULL,
  qty_sum      REAL    NOT NULL,
  vwap         REAL,
  first_ts_ms  INTEGER,
  last_ts_ms   INTEGER,
  PRIMARY KEY (symbol, bucket_ms)
);
INSERT OR REPLACE INTO minute_trades
SELECT
  t.symbol,
  (CAST(t.recv_unix * 1000 AS INTEGER) / 60000) * 60000 AS bucket_ms,
  COUNT(*)                                                AS n_trades,
  SUM(CASE WHEN t.quantity > 0 THEN t.quantity ELSE 0 END) AS qty_sum,
  CASE
    WHEN SUM(CASE WHEN t.quantity > 0 THEN t.quantity ELSE 0 END) > 0
    THEN SUM(CASE WHEN t.quantity > 0 AND t.price > 0 THEN t.price * t.quantity ELSE 0 END)
         / SUM(CASE WHEN t.quantity > 0 THEN t.quantity ELSE 0 END)
    ELSE NULL
  END AS vwap,
  MIN(CAST(t.recv_unix * 1000 AS INTEGER)) AS first_ts_ms,
  MAX(CAST(t.recv_unix * 1000 AS INTEGER)) AS last_ts_ms
FROM trade t
WHERE t.price > 0 AND t.quantity > 0
GROUP BY t.symbol, (CAST(t.recv_unix * 1000 AS INTEGER) / 60000) * 60000;


-- 
CREATE TABLE IF NOT EXISTS spreads (
  symbol     TEXT    NOT NULL,
  bucket_ms  INTEGER NOT NULL,
  best_bid   REAL,
  best_ask   REAL,
  spread     REAL,                 -- ask - bid
  mid        REAL,                 -- (ask + bid)/2
  src_ts_ms  INTEGER,              -- timestamp of chosen snapshot
  PRIMARY KEY (symbol, bucket_ms)
);
WITH bt AS (
  SELECT
    symbol,
    CAST(recv_unix * 1000 AS INTEGER) AS ts_ms,
    best_bid_price AS bid,
    best_ask_price AS ask
  FROM bookTicker
  WHERE best_bid_price > 0 AND best_ask_price > 0
),
last_in_min AS (
  SELECT
    symbol,
    (ts_ms/60000)*60000 AS bucket_ms,
    MAX(ts_ms)          AS max_ts
  FROM bt
  GROUP BY symbol, (ts_ms/60000)*60000
)
INSERT OR REPLACE INTO spreads
SELECT
  b.symbol,
  (b.ts_ms/60000)*60000 AS bucket_ms,
  b.bid,
  b.ask,
  CASE WHEN b.ask >= b.bid THEN (b.ask - b.bid) ELSE NULL END AS spread,
  CASE WHEN b.ask >= b.bid THEN (b.ask + b.bid)/2.0 ELSE NULL END AS mid,
  b.ts_ms AS src_ts_ms
FROM bt b
JOIN last_in_min l
  ON l.symbol = b.symbol AND l.max_ts = b.ts_ms;


---
CREATE TABLE IF NOT EXISTS book_imbalance (
  symbol     TEXT    NOT NULL,
  bucket_ms  INTEGER NOT NULL,
  bid_qty_1s REAL,
  ask_qty_1s REAL,
  imb        REAL,                 -- (bid-ask)/(bid+ask)
  last_ts_ms INTEGER,
  PRIMARY KEY (symbol, bucket_ms)
);

WITH e AS (
  SELECT
    symbol,
    CAST(recv_unix * 1000 AS INTEGER) AS ts_ms,
    qty,
    side
  FROM events
  WHERE qty > 0 AND (side='bid' OR side='ask')
),
m AS (
  SELECT symbol, (ts_ms/60000)*60000 AS bucket_ms, MAX(ts_ms) AS last_ts_ms
  FROM e
  GROUP BY symbol, (ts_ms/60000)*60000
),
w AS (
  SELECT e.symbol, m.bucket_ms, m.last_ts_ms, e.qty, e.side
  FROM e
  JOIN m
    ON m.symbol = e.symbol
   AND (e.ts_ms/60000)*60000 = m.bucket_ms
   AND e.ts_ms >  m.last_ts_ms - 1000
   AND e.ts_ms <= m.last_ts_ms
)
INSERT OR REPLACE INTO book_imbalance
SELECT
  symbol,
  bucket_ms,
  SUM(CASE WHEN side='bid' THEN qty ELSE 0 END) AS bid_qty_1s,
  SUM(CASE WHEN side='ask' THEN qty ELSE 0 END) AS ask_qty_1s,
  CASE
    WHEN (SUM(CASE WHEN side='bid' THEN qty ELSE 0 END) +
          SUM(CASE WHEN side='ask' THEN qty ELSE 0 END)) > 0
    THEN (SUM(CASE WHEN side='bid' THEN qty ELSE 0 END) -
          SUM(CASE WHEN side='ask' THEN qty ELSE 0 END)) * 1.0
         / (SUM(CASE WHEN side='bid' THEN qty ELSE 0 END) +
            SUM(CASE WHEN side='ask' THEN qty ELSE 0 END))
    ELSE NULL
  END AS imb,
  MAX(last_ts_ms) AS last_ts_ms
FROM w
GROUP BY symbol, bucket_ms;

----------------------------------------------------------------
-- 4) Rolling 5m volatility proxy from ticker closes
-- Close = last ticker in minute; ret = px/lag(px)-1; vol5m = AVG(ABS(ret)) over 5 minutes.
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rolling_vol_5m (
  symbol     TEXT    NOT NULL,
  bucket_ms  INTEGER NOT NULL,
  px_close   REAL,
  ret        REAL,
  vol5m      REAL,
  PRIMARY KEY (symbol, bucket_ms)
);

WITH tk AS (
  SELECT
    symbol,
    CAST(recv_unix * 1000 AS INTEGER) AS ts_ms,
    last_price
  FROM ticker
  WHERE last_price > 0
),
last_px AS (
  SELECT symbol, (ts_ms/60000)*60000 AS bucket_ms, MAX(ts_ms) AS max_ts
  FROM tk
  GROUP BY symbol, (ts_ms/60000)*60000
),
px AS (
  SELECT t.symbol,
         (t.ts_ms/60000)*60000 AS bucket_ms,
         t.last_price          AS px_close
  FROM tk t
  JOIN last_px lp
    ON lp.symbol = t.symbol AND lp.max_ts = t.ts_ms
),
rets AS (
  SELECT
    symbol,
    bucket_ms,
    px_close,
    (px_close / LAG(px_close) OVER (PARTITION BY symbol ORDER BY bucket_ms)) - 1.0 AS ret
  FROM px
),
vol AS (
  SELECT
    symbol,
    bucket_ms,
    px_close,
    ret,
    AVG(ABS(ret)) OVER (
      PARTITION BY symbol
      ORDER BY bucket_ms
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS vol5m
  FROM rets
)
INSERT OR REPLACE INTO rolling_vol_5m
SELECT symbol, bucket_ms, px_close, ret, vol5m
FROM vol;

----------------------------------------------------------------
-- 5) features_minute: unified join on the union of available minutes
-- Union avoids dropping minutes that have quotes but no prints, or vice versa.
----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features_minute (
  symbol     TEXT    NOT NULL,
  bucket_ms  INTEGER NOT NULL,
  n_trades   INTEGER,
  qty_sum    REAL,
  vwap       REAL,
  imb        REAL,
  spread     REAL,
  mid        REAL,
  vol5m      REAL,
  PRIMARY KEY (symbol, bucket_ms)
);

WITH universe AS (
  SELECT symbol, bucket_ms FROM minute_trades
  UNION
  SELECT symbol, bucket_ms FROM spreads
  UNION
  SELECT symbol, bucket_ms FROM rolling_vol_5m
)
INSERT OR REPLACE INTO features_minute
SELECT
  u.symbol,
  u.bucket_ms,
  mt.n_trades,
  mt.qty_sum,
  mt.vwap,
  bi.imb,
  sp.spread,
  sp.mid,
  rv.vol5m
FROM universe u
LEFT JOIN minute_trades   mt ON mt.symbol = u.symbol AND mt.bucket_ms = u.bucket_ms
LEFT JOIN book_imbalance  bi ON bi.symbol = u.symbol AND bi.bucket_ms = u.bucket_ms
LEFT JOIN spreads         sp ON sp.symbol = u.symbol AND sp.bucket_ms = u.bucket_ms
LEFT JOIN rolling_vol_5m  rv ON rv.symbol = u.symbol AND rv.bucket_ms = u.bucket_ms;

COMMIT;

PRAGMA optimize;
