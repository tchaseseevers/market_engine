clean:
	rm -f lobx.db
	rm -f csvs/*.csv


ingest:
	python .\1_binance_ingest.py

initial_tables: 
	sqlite3 lobx.db ".read 2_schema.sql"
	sqlite3 lobx.db ".read 3_staging.sql"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\aggTrade.csv stage_agg_trade"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\trade.csv stage_trade"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline1m.csv stage_klines1"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline3m.csv stage_klines3"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline5m.csv stage_klines5"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\ticker.csv stage_ticker"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\bookTicker.csv stage_bookTicker"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_bids.csv stage_events"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_asks.csv stage_events"
	sqlite3 lobx.db ".read 4_stage_to_final.sql"
	sqlite3 lobx.db ".read 5_metrics.sql"


refresh_tables:
	sqlite3 lobx.db ".read 3_staging.sql"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\aggTrade.csv stage_agg_trade"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\trade.csv stage_trade"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline1m.csv stage_klines1"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline3m.csv stage_klines3"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline5m.csv stage_klines5"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\ticker.csv stage_ticker"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\bookTicker.csv stage_bookTicker"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_bids.csv stage_events"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_asks.csv stage_events"
	sqlite3 lobx.db ".read 4_stage_to_final.sql"
	sqlite3 lobx.db ".read 5_metrics.sql" 

	


