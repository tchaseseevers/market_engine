-- events
INSERT INTO events (ts_ms, symbol, first_update_id, final_update_id, side, price, qty, recv_unix, recv_iso)
SELECT event_time_ms, symbol, first_update_id, final_update_id, side, price, qty, recv_unix, recv_iso
FROM stage_events;

-- trades
INSERT INTO agg_trades (event_time_ms, symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, recv_unix, recv_iso)
SELECT event_time_ms, symbol, trade_id, price, quantity, trade_time_ms, is_buyer_maker, recv_unix, recv_iso
FROM stage_trades;

-- klines
INSERT INTO klines (event_time_ms, symbol, interval, open_time, close_time, open_price, close_price, high_price, low_price,
                    volume, quote_volume, trades_count, taker_buy_volume, taker_buy_quote_volume, is_closed, recv_unix, recv_iso)
SELECT event_time_ms, symbol, interval, open_time, close_time, open_price, close_price, high_price, low_price,
       volume, quote_volume, trades_count, taker_buy_volume, taker_buy_quote_volume, is_closed, recv_unix, recv_iso
FROM stage_klines;

-- tickers
INSERT INTO tickers (event_time_ms, symbol, price_change, price_change_pct, weighted_avg_price, prev_close_price,
                     last_price, last_qty, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty,
                     open_price, high_price, low_price, volume, quote_volume, open_time, close_time,
                     first_trade_id, last_trade_id, trade_count, recv_unix, recv_iso)
SELECT event_time_ms, symbol, price_change, price_change_percent, weighted_avg_price, prev_close_price,
       last_price, last_qty, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty,
       open_price, high_price, low_price, volume, quote_volume, open_time, close_time,
       first_trade_id, last_trade_id, trade_count, recv_unix, recv_iso
FROM stage_tickers;
