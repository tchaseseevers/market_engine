import asyncio
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
import websockets  

WS_URL = (
    "wss://stream.binance.us:9443/stream?streams=btcusd@aggTrade/btcusd@trade/btcusd@kline_1m/btcusd@kline_3m/btcusd@kline_5m/btcusd@ticker/btcusd@bookTicker/btcusd@depth@100ms"
    )

AGGTRADE_CSV = Path("csvs/aggTrade.csv") 
TRADE_CSV = Path("csvs/trade.csv")      
KLINE1_CSV = Path("csvs/kline1m.csv")    
KLINE3_CSV = Path("csvs/kline3m.csv")    
KLINE5_CSV = Path("csvs/kline5m.csv")    
TICKER_CSV = Path("csvs/ticker.csv")    
BOOKTICKER_CSV = Path("csvs/bookTicker.csv")  
EVENTS_BIDS_CSV = Path("csvs/events_bids.csv") 
EVENTS_ASKS_CSV = Path("csvs/events_asks.csv")  

def _ensure_csv_headers():
    if not AGGTRADE_CSV.exists():
        with AGGTRADE_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time",
                "symbol",
                "aggregate_trade_id",
                "price",
                "quantity",
                "first_trade_id",
                "last_trade_id",
                "trade_time",
                "is_the_buyer_the_market_maker",
                "recv_unix", 
                "recv_iso"
            ])
    if not TRADE_CSV.exists():
        with TRADE_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time",
                "symbol",
                "trade_id",
                "price",
                "quantity",
                "buyer_order_id",
                "seller_order_id",
                "trade_time",
                "is_the_buyer_the_market_maker",
                "recv_unix", 
                "recv_iso"
            ])
    if not KLINE1_CSV.exists():
        with KLINE1_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time",
                "symbol",
                "kline_start_time",
                "kline_close_time",
                "symbol2",
                "interval",
                "first_trade_id",
                "last_trade_id",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "base_asset_volume",
                "number_of_trades",
                "is_this_kline_closed",
                "quote_asset_volume",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "recv_unix", 
                "recv_iso"
            ])
    if not KLINE3_CSV.exists():
        with KLINE3_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time",
                "symbol",
                "kline_start_time",
                "kline_close_time",
                "symbol2",
                "interval",
                "first_trade_id",
                "last_trade_id",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "base_asset_volume",
                "number_of_trades",
                "is_this_kline_closed",
                "quote_asset_volume",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "recv_unix", 
                "recv_iso"
            ])
    if not KLINE5_CSV.exists():
        with KLINE5_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time",
                "symbol",
                "kline_start_time",
                "kline_close_time",
                "symbol2",
                "interval",
                "first_trade_id",
                "last_trade_id",
                "open_price",
                "close_price",
                "high_price",
                "low_price",
                "base_asset_volume",
                "number_of_trades",
                "is_this_kline_closed",
                "quote_asset_volume",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
                "recv_unix", 
                "recv_iso"
            ])
    if not TICKER_CSV.exists():
        with TICKER_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time",
                "symbol",
                "price_change",
                "price_change_percent",
                "weighted_average_price",
                "prev_close_price",
                "last_price",
                "last_quantity",
                "best_bid_price",
                "best_bid_quantity",
                "best_ask_price",
                "best_ask_quantity",
                "open_price",
                "high_price",
                "low_price",
                "total_traded_base_asset_volume",
                "total_traded_quote_asset_volume",
                "statistics_open_time",
                "statistics_close_time",
                "first_trade_id",
                "last_trade_id",
                "total_number_of_trades",
                "recv_unix", 
                "recv_iso"
            ])
    if not BOOKTICKER_CSV.exists():
        with BOOKTICKER_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "order_book_update_id", 
                "symbol", 
                "best_bid_price", 
                "best_bid_qty",
                "best_ask_price", 
                "best_ask_qty", 
                "recv_unix", 
                "recv_iso"
            ])
    if not EVENTS_BIDS_CSV.exists():
        with EVENTS_BIDS_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time", 
                "symbol", 
                "first_update_id", 
                "final_update_id",
                "price", 
                "qty", 
                "side", 
                "recv_unix", 
                "recv_iso"
            ])
    if not EVENTS_ASKS_CSV.exists():
        with EVENTS_ASKS_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_type",
                "event_time", 
                "symbol", 
                "first_update_id", 
                "final_update_id",
                "price", 
                "qty", 
                "side", 
                "recv_unix", 
                "recv_iso"
            ])

async def stream_and_buffer_events():
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
                    if "stream" not in msg or "data" not in msg:
                        continue
                    stream_name = msg["stream"]
                    data = msg["data"]
                    if "@aggTrade" in stream_name:
                        with AGGTRADE_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("e"),
                                data.get("E"),
                                data.get("s"),
                                data.get("a"),
                                data.get("p"),
                                data.get("q"),
                                data.get("f"),
                                data.get("l"),
                                data.get("T"),
                                data.get("m"),
                                f"{recv_ts:.3f}",
                                recv_iso
                            ])
                    elif "@trade" in stream_name:
                        with TRADE_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("e"),
                                data.get("E"),
                                data.get("s"),
                                data.get("t"),
                                data.get("p"),
                                data.get("q"),
                                data.get("b"),
                                data.get("a"),
                                data.get("T"),
                                data.get("m"),
                                f"{recv_ts:.3f}",
                                recv_iso
                            ])
                    elif "@kline_1" in stream_name:
                        k = data.get("k", {})
                        with KLINE1_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("e"),
                                data.get("E"),      
                                data.get("s"),      
                                k.get("t"),         
                                k.get("T"),         
                                k.get("s"),         
                                k.get("i"),        
                                k.get("f"),         
                                k.get("L"),         
                                k.get("o"),         
                                k.get("c"),       
                                k.get("h"),         
                                k.get("l"),         
                                k.get("v"),        
                                k.get("n"),         
                                k.get("x"),        
                                k.get("q"),
                                k.get("V"),    
                                k.get("Q"),  
                                f"{recv_ts:.3f}",   
                                recv_iso            
                            ])
                    elif "@kline_3" in stream_name:
                        k = data.get("k", {})
                        with KLINE3_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("e"),
                                data.get("E"),      
                                data.get("s"),      
                                k.get("t"),         
                                k.get("T"),         
                                k.get("s"),         
                                k.get("i"),        
                                k.get("f"),         
                                k.get("L"),         
                                k.get("o"),         
                                k.get("c"),       
                                k.get("h"),         
                                k.get("l"),         
                                k.get("v"),        
                                k.get("n"),         
                                k.get("x"),        
                                k.get("q"),
                                k.get("V"),    
                                k.get("Q"),  
                                f"{recv_ts:.3f}",   
                                recv_iso            
                            ])
                    elif "@kline_5" in stream_name:
                        k = data.get("k", {})
                        with KLINE5_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("e"),
                                data.get("E"),      
                                data.get("s"),      
                                k.get("t"),         
                                k.get("T"),         
                                k.get("s"),         
                                k.get("i"),        
                                k.get("f"),         
                                k.get("L"),         
                                k.get("o"),         
                                k.get("c"),       
                                k.get("h"),         
                                k.get("l"),         
                                k.get("v"),        
                                k.get("n"),         
                                k.get("x"),        
                                k.get("q"),
                                k.get("V"),    
                                k.get("Q"),  
                                f"{recv_ts:.3f}",   
                                recv_iso            
                            ])
                    elif "@ticker" in stream_name:
                        with TICKER_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("e"),
                                data.get("E"),
                                data.get("s"),
                                data.get("p"),
                                data.get("P"),
                                data.get("w"),
                                data.get("x"),
                                data.get("c"),
                                data.get("Q"),
                                data.get("b"),
                                data.get("B"),
                                data.get("a"),
                                data.get("A"),
                                data.get("o"),
                                data.get("h"),
                                data.get("l"),
                                data.get("v"),
                                data.get("q"),
                                data.get("O"),
                                data.get("C"),
                                data.get("F"),
                                data.get("L"),
                                data.get("n"),
                                f"{recv_ts:.3f}",
                                recv_iso
                            ])
                    elif "@bookTicker" in stream_name:
                        with BOOKTICKER_CSV.open("a", newline="") as f:
                            w = csv.writer(f)
                            w.writerow([
                                data.get("u"),
                                data.get("s"),
                                data.get("b"),
                                data.get("B"),
                                data.get("a"),
                                data.get("A"),
                                f"{recv_ts:.3f}",
                                recv_iso
                            ])
                    elif "@depth" in stream_name:
                        e = data.get("e")
                        E = data.get("E")
                        s = data.get("s")
                        U = data.get("U")
                        u = data.get("u")
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if bids:
                            with EVENTS_BIDS_CSV.open("a", newline="") as f:
                                w = csv.writer(f)
                                for price, qty in bids:
                                    w.writerow([e, E, s, U, u, price, qty, "bid", f"{recv_ts:.3f}", recv_iso])
                        if asks:
                            with EVENTS_ASKS_CSV.open("a", newline="") as f:
                                w = csv.writer(f)
                                for price, qty in asks:
                                    w.writerow([e, E, s, U, u, price, qty, "ask", f"{recv_ts:.3f}", recv_iso])
        except (websockets.ConnectionClosedError, websockets.InvalidStatusCode) as e:
            print(f"[ws] connection error: {e} — reconnecting in 3s")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"[ws] unexpected error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)

async def main():
    _ensure_csv_headers()
    await stream_and_buffer_events()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[exit] keyboard interrupt")
        sys.exit(0)