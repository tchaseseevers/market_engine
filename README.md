# Critical Path

**T0–T1: Repo state DONE**
**T1–T2: Metrics SQL (metrics.sql)**
**T2–T3: Indexes**


**T3–T4: Feature build (features_build.py)**
* Connect to `lobx.db`
* Build `features.parquet` with:
  * `minute_trades.*`, `book_imbalance.value`, `spreads.spread`, `ticker.last_price`
  * Label: `direction_next_30s = sign(mid_price_t+30s − mid_price_t)`
* Persist schema JSON

**T4–T5: Baseline ML (training.py)**
* Train logistic regression or gradient boosting
* Metrics: accuracy, F1, ROC AUC
* Persist: `models/model.pkl`, `models/feature_schema.json`, `reports/metrics.json`

**T5–T6: Live inference (live_infer.py)**
* Every 5s: query latest feature row, transform, predict
* Print: timestamp, prob_up, spread, imbalance, last_price




**T6–T7: C order-book module (systems signal)**
* Implement top-10 levels per side with fixed arrays + binary insertion
* Batch every 100 updates; output `csvs/orderbook_metrics.csv` with ts, spread, mid, depth_imbalance, top-k notional
* `make build_c` + `make run_c`


**T7–T8: Tests + sanity**
* Pytest:
  * Row count > 0 after load
  * No duplicate `(symbol, trade_id)` in `trade`
  * Feature columns present; label coverage > 95%
* Simple timing decorator on feature build and live infer paths


**T8–T9: Performance pass**
* EXPLAIN QUERY PLAN on metrics + features joins; confirm index usage
* PRAGMA check (WAL, synchronous=NORMAL already set)
* Optional: gzip old CSV snapshots


**T9–T10: README + visuals**
* ASCII architecture diagram
* Dataflow section
* C rationale
* SQL optimization notes
* Results table + one figure (spread vs prob_up)
* How-to with Make targets


**T10–T11: Screens + artifacts**
* Save terminal screenshot of live inference
* Save matplotlib plot(s) under `images/`
* Summarize correlation of imbalance vs next move


**T11–T12: Finalization**
* Full dry-run from clean: `make clean` → ingest snapshot → load → metrics → features → model → live → run_c
* MIT license, `.gitignore`, tag `v1.0`, push




