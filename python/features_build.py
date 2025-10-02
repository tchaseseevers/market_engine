from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any
import numpy as np
import pandas as pd

DB_PATH = "lobx.db"
OUT_DIR_DATA = Path("data")
OUT_DIR_MODELS = Path("models")
OUT_DIR_DATA.mkdir(parents=True, exist_ok=True)
OUT_DIR_MODELS.mkdir(parents=True, exist_ok=True)
HORIZON_S = 30  
MIN_ROLL = 5    


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.row_factory = sqlite3.Row
    return con


def _read_base(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Bring in per-minute base features plus last close from rolling_vol_5m
    and the timestamp of the chosen book snapshot in 'spreads' (src_ts_ms).
    """
    sql = """
    SELECT
      fm.symbol,
      fm.bucket_ms,
      fm.n_trades,
      fm.qty_sum,
      fm.vwap,
      fm.imb,           -- 1s flow imbalance inside minute
      fm.spread,
      fm.mid,
      fm.vol5m,
      rv.px_close AS last_price,
      sp.src_ts_ms
    FROM features_minute AS fm
    LEFT JOIN rolling_vol_5m AS rv
      ON rv.symbol = fm.symbol AND rv.bucket_ms = fm.bucket_ms
    LEFT JOIN spreads AS sp
      ON sp.symbol = fm.symbol AND sp.bucket_ms = fm.bucket_ms
    ORDER BY fm.symbol, fm.bucket_ms;
    """
    return pd.read_sql_query(sql, con)


def _read_next30_mid(con: sqlite3.Connection) -> pd.DataFrame:
    sql = """
    WITH bt AS (
      SELECT
        symbol,
        CAST(recv_unix * 1000 AS INTEGER) AS ts_ms,
        (best_bid_price + best_ask_price) / 2.0 AS mid
      FROM bookTicker
      WHERE best_bid_price > 0 AND best_ask_price > 0
    )
    SELECT
      fm.symbol,
      fm.bucket_ms,
      (
        SELECT b.mid
        FROM bt b
        WHERE b.symbol = fm.symbol
          AND b.ts_ms > fm.bucket_ms
          AND b.ts_ms <= fm.bucket_ms + (? * 1000)
        ORDER BY b.ts_ms DESC
        LIMIT 1
      ) AS mid_plus_30s
    FROM features_minute fm
    ORDER BY fm.symbol, fm.bucket_ms;
    """
    return pd.read_sql_query(sql, con, params=(HORIZON_S,))


def _read_taker_trade_flow(con: sqlite3.Connection) -> pd.DataFrame:
    sql = """
    SELECT
      symbol,
      (CAST(recv_unix * 1000 AS INTEGER) / 60000) * 60000 AS bucket_ms,
      SUM(CASE WHEN is_the_buyer_the_market_maker='False' THEN quantity ELSE 0 END) AS taker_buy_qty,
      SUM(CASE WHEN is_the_buyer_the_market_maker='True'  THEN quantity ELSE 0 END) AS taker_sell_qty,
      COUNT(*) AS trade_count
    FROM trade
    WHERE price > 0 AND quantity > 0
    GROUP BY symbol, (CAST(recv_unix * 1000 AS INTEGER) / 60000) * 60000
    ORDER BY symbol, bucket_ms;
    """
    return pd.read_sql_query(sql, con)


def _read_top1_qty(con: sqlite3.Connection) -> pd.DataFrame:
    sql = """
    WITH bt AS (
      SELECT
        symbol,
        CAST(recv_unix * 1000 AS INTEGER) AS ts_ms,
        best_bid_price AS bid,
        best_ask_price AS ask,
        best_bid_qty   AS bid_qty,
        best_ask_qty   AS ask_qty
      FROM bookTicker
      WHERE best_bid_price > 0 AND best_ask_price > 0
    ),
    last_in_min AS (
      SELECT symbol, (ts_ms/60000)*60000 AS bucket_ms, MAX(ts_ms) AS max_ts
      FROM bt
      GROUP BY symbol, (ts_ms/60000)*60000
    )
    SELECT b.symbol,
           (b.ts_ms/60000)*60000 AS bucket_ms,
           b.bid_qty,
           b.ask_qty
    FROM bt b
    JOIN last_in_min l
      ON l.symbol=b.symbol AND l.max_ts=b.ts_ms
    ORDER BY b.symbol, (b.ts_ms/60000)*60000;
    """
    return pd.read_sql_query(sql, con)


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    out = np.where(den.astype(float) != 0, num.astype(float) / den.astype(float), np.nan)
    return pd.Series(out, index=num.index, dtype=float)


def _zscore(s: pd.Series, win: int) -> pd.Series:
    m = s.rolling(win, min_periods=min(win, MIN_ROLL)).mean()
    v = s.rolling(win, min_periods=min(win, MIN_ROLL)).std(ddof=0)
    return (s - m) / v


def _ffill_and_median(df: pd.DataFrame, group_col: str, cols: list[str]) -> pd.DataFrame:
    df = df.sort_values([group_col, "bucket_ms"]).reset_index(drop=True)
    for c in cols:
        if c in df.columns and df[c].isna().any():
            df[c] = df.groupby(group_col, observed=True)[c].ffill()
            med = df[c].median()
            df[c] = df[c].fillna(med)
    return df


def _roll_std_gby(series: pd.Series, win: int, min_req: int | None = None) -> pd.Series:
    min_req = min_req if min_req is not None else min(win, MIN_ROLL)
    min_req = min(min_req, win)  # ensure valid for pandas
    return (
        series.rolling(win, min_periods=min_req)
              .std(ddof=0)
              .reset_index(level=0, drop=True)
    )


def main() -> None:
    con = _connect(DB_PATH)
    base   = _read_base(con)
    nxt    = _read_next30_mid(con)
    taker  = _read_taker_trade_flow(con)
    topq   = _read_top1_qty(con)
    con.close()
    df = (
        base.merge(nxt,  on=["symbol","bucket_ms"], how="left")
            .merge(taker,on=["symbol","bucket_ms"], how="left")
            .merge(topq, on=["symbol","bucket_ms"], how="left")
    )
    df = df.dropna(subset=["mid_plus_30s", "mid"]).copy()
    df["direction_next_30s"] = np.sign(df["mid_plus_30s"] - df["mid"]).astype(np.int8)
    df["spread_bp"] = 1e4 * _safe_div(df["spread"], df["mid"])
    df["d_spread_bp"] = df.groupby("symbol", observed=True)["spread_bp"].diff()
    df["quote_staleness_ms"] = (df["bucket_ms"] + 60000) - df["src_ts_ms"]
    df["ret_1m"] = np.log(_safe_div(
        df["mid"], df.groupby("symbol", observed=True)["mid"].shift(1)
    ))
    df["ret_2m"] = np.log(_safe_div(
        df["mid"], df.groupby("symbol", observed=True)["mid"].shift(2)
    ))
    df["ret_5m"] = np.log(_safe_div(
        df["mid"], df.groupby("symbol", observed=True)["mid"].shift(5)
    ))
    g = df.groupby("symbol", observed=True)["ret_1m"]
    df["rv_3m"]  = _roll_std_gby(g, win=3,  min_req=3)  
    df["rv_10m"] = _roll_std_gby(g, win=10, min_req=5)
    df["taker_buy_qty"]  = df["taker_buy_qty"].astype(float)
    df["taker_sell_qty"] = df["taker_sell_qty"].astype(float)
    df["taker_imb"] = _safe_div(
        df["taker_buy_qty"] - df["taker_sell_qty"],
        df["taker_buy_qty"] + df["taker_sell_qty"]
    )
    df["taker_qty_tot"]   = (df["taker_buy_qty"].fillna(0) + df["taker_sell_qty"].fillna(0))
    df["taker_qty_z_30"]  = _zscore(df["taker_qty_tot"].astype(float), 30)
    depth_den = (df["bid_qty"].astype(float) + df["ask_qty"].astype(float))
    df["depth_imb_top1"] = _safe_div(
        df["bid_qty"].astype(float) - df["ask_qty"].astype(float), depth_den
    )
    bid = df["mid"] - 0.5 * df["spread"]
    ask = df["mid"] + 0.5 * df["spread"]
    microprice = _safe_div(
        ask * df["bid_qty"].astype(float) + bid * df["ask_qty"].astype(float), depth_den
    )
    df["microprice_premium_bp"] = 1e4 * _safe_div(microprice - df["mid"], df["mid"])
    df["vwap_premium_bp"] = 1e4 * _safe_div(df["vwap"] - df["mid"], df["mid"])
    df["d_imb_1m"]        = df.groupby("symbol", observed=True)["imb"].diff()
    df["imb_z_30"]        = _zscore(df["imb"].astype(float), 30)
    df["spread_z_30"]     = _zscore(df["spread"].astype(float), 30)
    df["qty_sum_z_30"]    = _zscore(df["qty_sum"].astype(float), 30)
    minute = ((df["bucket_ms"] // 60000) % 60).astype(float)
    hour   = ((df["bucket_ms"] // 3600000) % 24).astype(float)
    df["min_sin"]  = np.sin(2*np.pi*minute/60.0)
    df["min_cos"]  = np.cos(2*np.pi*minute/60.0)
    df["hour_sin"] = np.sin(2*np.pi*hour/24.0)
    df["hour_cos"] = np.cos(2*np.pi*hour/24.0)
    numeric_cols = [
        "n_trades","qty_sum","vwap","imb","spread","mid","vol5m","last_price","src_ts_ms",
        "spread_bp","d_spread_bp","quote_staleness_ms",
        "ret_1m","ret_2m","ret_5m","rv_3m","rv_10m",
        "taker_buy_qty","taker_sell_qty","taker_imb","taker_qty_tot","taker_qty_z_30",
        "bid_qty","ask_qty","depth_imb_top1","microprice_premium_bp",
        "vwap_premium_bp","d_imb_1m","imb_z_30","spread_z_30","qty_sum_z_30",
        "min_sin","min_cos","hour_sin","hour_cos"
    ]
    df = _ffill_and_median(df, "symbol", numeric_cols)
    feature_cols = [
        "n_trades","qty_sum","vwap","imb","spread","mid","vol5m","last_price",
        "spread_bp","d_spread_bp","quote_staleness_ms",
        "ret_1m","ret_2m","ret_5m","rv_3m","rv_10m",
        "taker_imb","taker_qty_tot","taker_qty_z_30","depth_imb_top1","microprice_premium_bp",
        "vwap_premium_bp","d_imb_1m","imb_z_30","spread_z_30","qty_sum_z_30",
        "min_sin","min_cos","hour_sin","hour_cos",
    ]
    label_col = "direction_next_30s"
    out_cols = ["symbol","bucket_ms"] + feature_cols + [label_col]
    out_df = df[out_cols].copy()


    out_path_parquet = OUT_DIR_DATA / "features.parquet"
    out_path_csv = OUT_DIR_DATA / "features.csv"
    try:
        out_df.to_parquet(out_path_parquet, index=False)
        out_df.to_csv(out_path_csv, index=False)
        wrote = str(out_path_parquet)
        wrote = str(out_path_csv)
    except Exception:
        out_df.to_csv(out_path_csv, index=False)
        wrote = str(out_path_csv)


    def _dtype_str(s: pd.Series) -> str:
        if pd.api.types.is_integer_dtype(s): return "int64"
        if pd.api.types.is_float_dtype(s):   return "float64"
        if pd.api.types.is_bool_dtype(s):    return "bool"
        return "string"
    schema: Dict[str, Any] = {
        "version": 2,
        "horizon_seconds": HORIZON_S,
        "index_cols": ["symbol","bucket_ms"],
        "feature_cols": feature_cols,
        "label_col": label_col,
        "dtypes": {c: _dtype_str(out_df[c]) for c in out_cols},
        "row_count": int(len(out_df))
    }
    with open(OUT_DIR_MODELS / "feature_schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    print(f"[features_build] rows={len(out_df)} wrote={wrote}")


if __name__ == "__main__":
    main()
    df = pd.read_csv("data/features.csv")  
    print(df)
