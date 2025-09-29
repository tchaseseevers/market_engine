# FINAL PIPELINE FLOW
BINANCE WS -> CSV FILES -> SQLITE -> Order book Reconstruction in C (store in SQLITE) -> C Analytics, organized feature tables in SQL -> Python ML

# RUN SCRIPT
rm lobx.db
rm csvs\*.csv

python .\binance_ingest.py

sqlite3 lobx.db ".read db\schema.sql"
sqlite3 lobx.db ".read db\staging.sql"

sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_bids.csv stage_events"
sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\events_asks.csv stage_events"
sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\trades.csv stage_trades"
sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\klines.csv stage_klines"
sqlite3 lobx.db ".mode csv" ".import --skip 1 csvs\ticker.csv stage_tickers"

sqlite3 lobx.db ".read db\stage_to_final.sql"

gcc -o c\book_reconstruction.exe c\book_reconstruction.c -lsqlite3 -lm
./c\book_reconstruction.exe lobx.db BTCUSD 50






