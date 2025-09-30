clean:
	rm -f lobx.db
	rm -f csvs/*.csv
	rm -f data/* 
	rm -f models/*
	rm -f reports/* 

begin:
	python .\python\1_binance_ingest.py
	sqlite3 lobx.db ".read sql\2_schema.sql"
	sqlite3 lobx.db ".read sql\3_staging.sql"
	
stage_tables: 
	sqlite3 lobx.db ".read sql\3_staging.sql"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\aggTrade.csv stage_agg_trade"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\trade.csv stage_trade"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline1m.csv stage_klines1"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline3m.csv stage_klines3"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\kline5m.csv stage_klines5"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\ticker.csv stage_ticker"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\bookTicker.csv stage_bookTicker"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_bids.csv stage_events"
	sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_asks.csv stage_events"

staging_to_final:
	sqlite3 lobx.db ".read sql\4_stage_to_final.sql"
	sqlite3 lobx.db ".read sql\5_metrics.sql"

build_features:
	python .\features_build.py

train:
	python .\training.py



	


