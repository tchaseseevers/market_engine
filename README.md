**12‑Hour Execution Plan (resume‑ready polish + data + analytics + C component + ML)**

High‑level deliverables:
1. Hardened, idempotent SQLite loading (no dupes).
2. Derived analytical tables (microstructure + aggregation).
3. C module computing fast order book + imbalance metrics from depth events.
4. Python feature engineering + lightweight predictive model.
5. Live inference demo script.
6. Tests + profiling + README narrative (project story).
7. Clean Makefile targets.

---

### Hour 0–0.5: Setup & Baseline
- Freeze current state (git init, first commit).
- Create Python venv, requirements.txt (websockets, pandas, scikit-learn, sqlite-utils or SQLAlchemy).
- Add .gitignore (venv, *.db, csvs/*.csv, __pycache__, models/).

### Hour 0.5–1.5: Idempotent Loading Layer
- Add UNIQUE indexes in final tables to allow UPSERT (e.g. trade(trade_id), agg_trade(aggregate_trade_id), klines*(kline_start_time, interval), ticker(event_time), bookTicker(order_book_update_id), events(final_update_id, price, side) OR (event_time, price, side)).
- Replace raw INSERT in 4_stage_to_final.sql with INSERT OR IGNORE (fast) or INSERT ... ON CONFLICT DO NOTHING.
- Add new Makefile target snapshot_load that:
  - Copies current csvs to a timestamped snapshot dir (atomic read window).
  - Imports from snapshot (so ingestion can continue).
- Add VACUUM / ANALYZE step optional.

### Hour 1.5–2.5: Derived SQL Metrics
Create new SQL script metrics.sql:
- minute_trades: aggregate trades per 1‑minute (COUNT, SUM(qty), VWAP).
- book_imbalance: from latest N (e.g. last 1s) events: (bid_volume - ask_volume)/(bid_volume + ask_volume).
- spreads: from bookTicker best_ask_price - best_bid_price + mid price.
- rolling_vol_5m: stddev(last_price returns) over trailing 5m (window via self join or incremental table).
- Persist results into tables with indexes for fast feature extraction (e.g. features_minute).

### Hour 2.5–4: C Order Book Metrics Module
Write C program to:
- Read events CSV incrementally (events_bids + events_asks merged logic equivalent to stage_events).
- Maintain top 10 levels per side (sorted arrays).
- After each batch (e.g. every 100 updates) compute:
  - Spread
  - Mid price
  - Depth imbalance (Σ bid_qty_10 / (Σ bid_qty_10 + Σ ask_qty_10))
  - Weighted mid (bid*ask symmetrical)
  - Top‑k cumulative notional (price * qty)
- Output rows to csvs/orderbook_metrics.csv with timestamp.
- This shows systems + performance skill (simple arrays + insertion).
Add Makefile target build_c / run_c.

Skeleton:

````c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LEVELS 10
typedef struct {
    double price;
    double qty;
} Level;

static Level bids[MAX_LEVELS];
static Level asks[MAX_LEVELS];
static int bid_count = 0;
static int ask_count = 0;

void insert_level(Level *arr, int *count, double price, double qty, int is_bid) {
    if (qty == 0.0) return; // simple skip deletes
    int i;
    for (i = 0; i < *count; i++) {
        if (arr[i].price == price) { arr[i].qty = qty; return; }
    }
    if (*count < MAX_LEVELS) {
        arr[*count].price = price;
        arr[*count].qty = qty;
        (*count)++;
    } else {
        // replace if better
        int worst = 0;
        for (i = 1; i < *count; i++) {
            if (is_bid) {
                if (arr[i].price < arr[worst].price) worst = i;
            } else {
                if (arr[i].price > arr[worst].price) worst = i;
            }
        }
        if ((is_bid && price > arr[worst].price) || (!is_bid && price < arr[worst].price)) {
            arr[worst].price = price;
            arr[worst].qty = qty;
        }
    }
    // sort
    for (i = 0; i < *count - 1; i++) {
        for (int j = i + 1; j < *count; j++) {
            int cond = is_bid ? arr[j].price > arr[i].price : arr[j].price < arr[i].price;
            if (cond) {
                Level tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp;
            }
        }
    }
}

void compute_and_write(FILE *out, double ts_unix) {
    if (bid_count == 0 || ask_count == 0) return;
    double best_bid = bids[0].price;
    double best_ask = asks[0].price;
    double spread = best_ask - best_bid;
    double mid = (best_ask + best_bid)/2.0;
    double bid_sum = 0, ask_sum = 0;
    for (int i = 0; i < bid_count; i++) bid_sum += bids[i].qty;
    for (int i = 0; i < ask_count; i++) ask_sum += asks[i].qty;
    double imbalance = (bid_sum - ask_sum) / (bid_sum + ask_sum);
    fprintf(out, "%.3f,%.2f,%.2f,%.6f,%.4f,%.4f\n", ts_unix, best_bid, best_ask, mid, spread, imbalance);
    fflush(out);
}

int main() {
    FILE *in = fopen("csvs/events_bids.csv", "r");
    FILE *in2 = fopen("csvs/events_asks.csv", "r");
    FILE *out = fopen("csvs/orderbook_metrics.csv", "w");
    if (!in || !in2 || !out) { fprintf(stderr, "file error\n"); return 1; }
    fprintf(out, "ts_unix,best_bid,best_ask,mid,spread,imbalance\n");

    char line[512];
    // skip headers
    fgets(line, sizeof line, in);
    fgets(line, sizeof line, in2);
    // naive merge loop (improvement: timestamp sort)
    int counter = 0;
    while (fgets(line, sizeof line, in)) {
        // event_type,event_time,symbol,first_update_id,final_update_id,price,qty,side,recv_unix,recv_iso
        char *token;
        int col = 0;
        double price = 0, qty = 0, recv_unix = 0;
        char side[8] = {0};
        char *copy = strdup(line);
        char *p = strtok(copy, ",");
        while (p) {
            if (col == 5) price = atof(p);
            else if (col == 6) qty = atof(p);
            else if (col == 7) strncpy(side, p, 7);
            else if (col == 8) recv_unix = atof(p);
            p = strtok(NULL, ",");
            col++;
        }
        if (side[0] == 'b') insert_level(bids, &bid_count, price, qty, 1);
        counter++;
        if (counter % 100 == 0) compute_and_write(out, recv_unix);
        free(copy);
    }
    // (Repeat for asks file similarly or unify into single merged iteration)
    fclose(in); fclose(in2); fclose(out);
    return 0;
}
// ...existing code...
````

(Keep logic simple; optional: unify both files sorted by recv_unix.)

### Hour 4–5: Feature Engineering Script
Python script features_build.py:
- Connect to lobx.db.
- Join minute_trades + book_imbalance + spreads + ticker last_price.
- Create supervised label: direction_next_30s = sign(mid_price(t+30s) - mid_price(t)).
- Save dataset to data/features.parquet.

### Hour 5–6: ML Model MVP
- Train simple gradient boosting or logistic regression (fast, explainable).
- Evaluate accuracy, F1, ROC AUC.
- Persist model (models/model.pkl) + feature schema JSON.

### Hour 6–7: Live Inference Demo
Script live_infer.py:
- Every 5s: query latest rows, build current feature vector (reuse transform).
- Load model, output probability of upward move + current spread + imbalance.
- Print concise terminal dashboard.

### Hour 7–8: Testing + Validation
- Add tests (pytest) for:
  - Row count > 0 after snapshot_load.
  - No duplicate trade_id in trade.
  - Feature pipeline outputs expected columns.
- Add timing profiler (simple decorator) for feature build.

### Hour 8–9: README Overhaul
Sections:
- Problem statement (real‑time microstructure + predictive signal).
- Architecture diagram (ASCII).
- Data flow: WebSocket → CSV snapshot → staging → final → metrics → features → model → live inference.
- C component rationale (performance + systems).
- SQL optimization (indexes, UPSERT, WAL).
- Results: sample metrics + model performance.
- How to run (make ingest, make snapshot_load, make metrics, make build_c, make model, make live).

### Hour 9–10: Performance + Polishing
- Add indexes if EXPLAIN QUERY PLAN shows scans (especially on symbol, event_time, trade_id).
- Add PRAGMA optimizations already present; ensure synchronous=NORMAL, WAL set once.
- Optionally compress historical CSV snapshots (zip).

### Hour 10–11: Resume Artifact + Screenshots
- Generate sample chart (matplotlib) of spread vs predicted probability.
- Add images/ (plots) referenced in README.
- Summarize quantitative improvement (e.g., imbalance correlation with next move).

### Hour 11–12: Final Pass
- Git tag v1.0.
- Quick code comment review (clarity, no dead code).
- Dry run from blank state (delete db + csvs, run end‑to‑end).
- Push to GitHub (public). Add license (MIT).

---

### Minimal Makefile Additions
Targets to add: snapshot_load, metrics, build_c, features, model, live.

````make
# ...existing code...
snapshot_load:
	mkdir -p snapshots
	set TS=%TIME: =_% & set DT=%DATE: =_% 
	# (Simplify on Windows: use PowerShell for timestamp)
	powershell Copy-Item csvs\*.csv snapshots\
	sqlite3 lobx.db ".read 2_schema.sql"
	sqlite3 lobx.db ".read 3_staging.sql"
	# imports from snapshots/*.csv instead of live csvs
	# (Repeat .import lines pointing at snapshots)
	sqlite3 lobx.db ".read 4_stage_to_final.sql"

metrics:
	sqlite3 lobx.db ".read metrics.sql"

build_c:
	gcc c\orderbook_metrics.c -O2 -o orderbook_metrics.exe

orderbook_metrics:
	.\orderbook_metrics.exe

features:
	python features_build.py

model:
	python model_train.py

live:
	python live_infer.py
# ...existing code...
````

---

### Resume Story (use in README)
Built a real‑time crypto microstructure analytics & predictive signal platform:
- Multi‑stream ingestion (8 Binance streams) → SQLite (idempotent, indexed).
- Systems layer in C for low‑latency order book analytics (top‑10 depth, imbalance).
- Analytical SQL layer (aggregations, rolling volatility, spreads).
- Feature engineering + supervised model predicting short‑horizon mid‑price direction.
- Live inference loop producing probabilities with sub‑X second latency.
- Emphasis on data integrity (UPSERT constraints), performance (WAL, indexes), and reproducibility (snapshot loads, versioned artifacts).

---

