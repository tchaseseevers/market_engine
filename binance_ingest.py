#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Binance.US BTCUSD multi-stream capture
- Open combined WS stream for: depth@100ms, aggTrade, kline_1m, ticker
- Buffer events and append them to CSVs
- No order book management/merging logic here (you said you'll do that in C).

Outputs (created in working directory):
  events_bids.csv
  events_asks.csv
  trades.csv
  klines.csv
  ticker.csv
"""

import asyncio
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import websockets  # pip install websockets

# Combined stream URL - multiple streams in one connection
WS_URL = "wss://stream.binance.us:9443/stream?streams=btcusd@depth@100ms/btcusd@aggTrade/btcusd@kline_1m/btcusd@ticker"

# CSV file paths
EVENTS_BIDS_CSV = Path("csvs/events_bids.csv")
EVENTS_ASKS_CSV = Path("csvs/events_asks.csv")
TRADES_CSV = Path("csvs/trades.csv")
KLINES_CSV = Path("csvs/klines.csv")
TICKER_CSV = Path("csvs/ticker.csv")

def _ensure_csv_headers():
    # Events: append forever with headers if file didn't exist
    if not EVENTS_BIDS_CSV.exists():
        with EVENTS_BIDS_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_time_ms", "symbol", "first_update_id", "final_update_id",
                "price", "qty", "side", "recv_unix", "recv_iso"
            ])
    if not EVENTS_ASKS_CSV.exists():
        with EVENTS_ASKS_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_time_ms", "symbol", "first_update_id", "final_update_id",
                "price", "qty", "side", "recv_unix", "recv_iso"
            ])

    # Trades: append forever with headers if file didn't exist
    if not TRADES_CSV.exists():
        with TRADES_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_time_ms", "symbol", "trade_id", "price", "quantity", 
                "trade_time_ms", "is_buyer_maker", "recv_unix", "recv_iso"
            ])

    # Klines: append forever with headers if file didn't exist
    if not KLINES_CSV.exists():
        with KLINES_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_time_ms", "symbol", "interval", "open_time", "close_time",
                "open_price", "close_price", "high_price", "low_price", 
                "volume", "quote_volume", "trades_count", "taker_buy_volume", 
                "taker_buy_quote_volume", "is_closed", "recv_unix", "recv_iso"
            ])

    # Ticker: append forever with headers if file didn't exist
    if not TICKER_CSV.exists():
        with TICKER_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_time_ms", "symbol", "price_change", "price_change_percent",
                "weighted_avg_price", "prev_close_price", "last_price", "last_qty",
                "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty",
                "open_price", "high_price", "low_price", "volume", "quote_volume",
                "open_time", "close_time", "first_trade_id", "last_trade_id",
                "trade_count", "recv_unix", "recv_iso"
            ])

async def stream_and_buffer_events():
    """Open WS, buffer/append each event to respective CSVs."""
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, max_queue=None) as ws:
                print("[ws] connected:", WS_URL)
                async for raw in ws:
                    recv_ts = time.time()
                    recv_iso = datetime.utcfromtimestamp(recv_ts).isoformat() + "Z"

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Combined stream format: {"stream": "<stream_name>", "data": {...}}
                    if "stream" not in msg or "data" not in msg:
                        continue

                    stream_name = msg["stream"]
                    data = msg["data"]

                    # Handle depth updates
                    if "@depth" in stream_name:
                        E = data.get("E")
                        s = data.get("s")
                        U = data.get("U")
                        u = data.get("u")
                        bids = data.get("b", [])
                        asks = data.get("a", [])

                        # Append to CSVs. Each row = one level
                        if bids:
                            with EVENTS_BIDS_CSV.open("a", newline="") as f:
                                w = csv.writer(f)
                                for price, qty in bids:
                                    w.writerow([E, s, U, u, price, qty, "bid", f"{recv_ts:.3f}", recv_iso])
                        if asks:
                            with EVENTS_ASKS_CSV.open("a", newline="") as f:
                                w = csv.writer(f)
                                for price, qty in asks:
                                    w.writerow([E, s, U, u, price, qty, "ask", f"{recv_ts:.3f}", recv_iso])

                    # Handle aggregate trade updates
                    elif "@aggTrade" in stream_name:
                        with TRADES_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("E"),      # event_time_ms
                                data.get("s"),      # symbol
                                data.get("a"),      # trade_id
                                data.get("p"),      # price
                                data.get("q"),      # quantity
                                data.get("T"),      # trade_time_ms
                                data.get("m"),      # is_buyer_maker
                                f"{recv_ts:.3f}",   # recv_unix
                                recv_iso            # recv_iso
                            ])

                    # Handle kline/candlestick updates
                    elif "@kline" in stream_name:
                        k = data.get("k", {})
                        with KLINES_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("E"),      # event_time_ms
                                data.get("s"),      # symbol
                                k.get("i"),         # interval
                                k.get("t"),         # open_time
                                k.get("T"),         # close_time
                                k.get("o"),         # open_price
                                k.get("c"),         # close_price
                                k.get("h"),         # high_price
                                k.get("l"),         # low_price
                                k.get("v"),         # volume
                                k.get("q"),         # quote_volume
                                k.get("n"),         # trades_count
                                k.get("V"),         # taker_buy_volume
                                k.get("Q"),         # taker_buy_quote_volume
                                k.get("x"),         # is_closed
                                f"{recv_ts:.3f}",   # recv_unix
                                recv_iso            # recv_iso
                            ])

                    # Handle ticker updates
                    elif "@ticker" in stream_name:
                        with TICKER_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("E"),      # event_time_ms
                                data.get("s"),      # symbol
                                data.get("p"),      # price_change
                                data.get("P"),      # price_change_percent
                                data.get("w"),      # weighted_avg_price
                                data.get("x"),      # prev_close_price
                                data.get("c"),      # last_price
                                data.get("Q"),      # last_qty
                                data.get("b"),      # best_bid_price
                                data.get("B"),      # best_bid_qty
                                data.get("a"),      # best_ask_price
                                data.get("A"),      # best_ask_qty
                                data.get("o"),      # open_price
                                data.get("h"),      # high_price
                                data.get("l"),      # low_price
                                data.get("v"),      # volume
                                data.get("q"),      # quote_volume
                                data.get("O"),      # open_time
                                data.get("C"),      # close_time
                                data.get("F"),      # first_trade_id
                                data.get("L"),      # last_trade_id
                                data.get("n"),      # trade_count
                                f"{recv_ts:.3f}",   # recv_unix
                                recv_iso            # recv_iso
                            ])

        except (websockets.ConnectionClosedError, websockets.InvalidStatusCode) as e:
            print(f"[ws] connection error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[ws] unexpected error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)

async def main():
    _ensure_csv_headers()
    # Start streaming indefinitely
    await stream_and_buffer_events()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[exit] keyboard interrupt")
        sys.exit(0)