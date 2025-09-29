#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <math.h>

#define MAX_LEVELS 20000     // simple fixed cap for demo
#define SCALE      100000000 // 1e8 fixed-point to avoid float rounding

typedef struct { long long px; long long qty; } Level;

typedef struct {
    Level bids[MAX_LEVELS]; int nbids;  // sorted: px desc
    Level asks[MAX_LEVELS]; int nasks;  // sorted: px asc
} Book;

// -- helpers --
// convert "0.00854600" -> scaled int64 (px * 1e8). simple/robust: strtod then llround.
static long long parse_scaled(const char *s) {
    double d = strtod(s, NULL);
    long long v = (long long)(d * (double)SCALE + (d>=0?0.5:-0.5));
    return v;
}

static void to_str(long long val, char *buf, size_t n) {
    long long ip = val / SCALE;
    long long fp = llabs(val % SCALE);
    // print with 8 decimals
    snprintf(buf, n, "%lld.%08lld", ip, fp);
}

static int cmp_bid(const void *a, const void *b) { // desc by px
    const Level *x = (const Level*)a, *y = (const Level*)b;
    if (x->px == y->px) return 0;
    return (x->px > y->px) ? -1 : 1;
}
static int cmp_ask(const void *a, const void *b) { // asc by px
    const Level *x = (const Level*)a, *y = (const Level*)b;
    if (x->px == y->px) return 0;
    return (x->px < y->px) ? -1 : 1;
}

// linear find 
static int find_level(Level *arr, int n, long long px) {
    for (int i=0;i<n;i++) if (arr[i].px == px) return i;
    return -1;
}

static void upsert_level(Level *arr, int *n, int is_bid, long long px, long long qty) {
    int idx = find_level(arr, *n, px);
    if (qty == 0) {
        if (idx >= 0) { // remove
            for (int j=idx+1;j<*n;j++) arr[j-1] = arr[j];
            (*n)--;
        }
        return;
    }
    if (idx >= 0) {
        arr[idx].qty = qty; // update qty
    } else if (*n < MAX_LEVELS) {
        arr[*n].px = px; arr[*n].qty = qty; (*n)++;
    }
    if (is_bid) qsort(arr, *n, sizeof(Level), cmp_bid);
    else        qsort(arr, *n, sizeof(Level), cmp_ask);
}

static int best_bid(const Book *b, Level *out) { if (b->nbids==0) return 0; *out=b->bids[0]; return 1; }
static int best_ask(const Book *b, Level *out) { if (b->nasks==0) return 0; *out=b->asks[0]; return 1; }

// Calculate additional metrics
static long long calc_spread(const Book *book) {
    Level bb, ba;
    if (best_bid(book, &bb) && best_ask(book, &ba)) {
        return ba.px - bb.px;
    }
    return 0;
}

static long long calc_mid_price(const Book *book) {
    Level bb, ba;
    if (best_bid(book, &bb) && best_ask(book, &ba)) {
        return (bb.px + ba.px) / 2;
    }
    return 0;
}

static long long calc_total_qty(const Level *arr, int n, int max_levels) {
    long long total = 0;
    int limit = (max_levels > 0 && max_levels < n) ? max_levels : n;
    for (int i = 0; i < limit; i++) {
        total += (long long)(arr[i].qty); // Convert to satoshis or similar integer representation
    }
    return total;
}

static double calc_depth_imbalance(const Book *book, int levels) {
    long long bid_qty = calc_total_qty(book->bids, book->nbids, levels);
    long long ask_qty = calc_total_qty(book->asks, book->nasks, levels);
    
    if (bid_qty + ask_qty == 0) return 0.0;
    
    return (double)(bid_qty - ask_qty) / (double)(bid_qty + ask_qty);
}

// main
int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <db> <symbol> <snap_every>\n", argv[0]);
        return 2;
    }
    const char *dbpath = argv[1];
    const char *symbol = argv[2];
    int snap_every = atoi(argv[3]);
    if (snap_every <= 0) snap_every = 200;

    printf("[info] db=%s symbol=%s snap_every=%d\n", dbpath, symbol, snap_every);

    sqlite3 *db = NULL;
    if (sqlite3_open(dbpath, &db) != SQLITE_OK) {
        fprintf(stderr, "sqlite open failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    // optional pragmas for speed
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);

    // pull ALL events for this symbol, ordered
    const char *Q =
        "SELECT ts_ms, side, price, qty "
        "FROM events WHERE symbol=? ORDER BY ts_ms, id";
    sqlite3_stmt *q = NULL;
    if (sqlite3_prepare_v2(db, Q, -1, &q, NULL) != SQLITE_OK) {
        fprintf(stderr, "prepare q failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    sqlite3_bind_text(q, 1, symbol, -1, SQLITE_STATIC);

    // Enhanced snapshot insert with additional metrics
    const char *I =
        "INSERT OR REPLACE INTO book_snapshots "
        "(ts_ms, symbol, best_bid, bid1_qty, best_ask, ask1_qty, "
        " spread, mid_price, total_bid_qty, total_ask_qty, depth_imbalance) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)";
    sqlite3_stmt *ins = NULL;
    if (sqlite3_prepare_v2(db, I, -1, &ins, NULL) != SQLITE_OK) {
        fprintf(stderr, "prepare ins failed: %s\n", sqlite3_errmsg(db));
        return 1;
    }

    Book book = {0};
    long long last_ts_ms = 0;
    int processed = 0, snapshots = 0;

    printf("[info] starting order book reconstruction...\n");

    // process loop
    while (sqlite3_step(q) == SQLITE_ROW) {
        long long ts_ms = sqlite3_column_int64(q, 0);
        const unsigned char *side = sqlite3_column_text(q, 1);
        const char *pstr = (const char*)sqlite3_column_text(q, 2);
        const char *qstr = (const char*)sqlite3_column_text(q, 3);

        if (!pstr || !qstr) {
            printf("[warn] null price/qty at row %d, skipping\n", processed);
            continue;
        }

        long long px  = parse_scaled(pstr);
        long long qty = parse_scaled(qstr);

        if (side && (side[0]=='b' || side[0]=='B')) {
            upsert_level(book.bids, &book.nbids, 1, px, qty);
        } else if (side && (side[0]=='a' || side[0]=='A')) {
            upsert_level(book.asks, &book.nasks, 0, px, qty);
        } else {
            printf("[warn] unknown side '%s' at row %d\n", side ? (char*)side : "NULL", processed);
            continue;
        }

        last_ts_ms = ts_ms;
        processed++;

        if (processed % snap_every == 0) {
            Level bb, ba; 
            int has_bb = best_bid(&book, &bb);
            int has_ba = best_ask(&book, &ba);
            
            // Calculate additional metrics
            long long spread = calc_spread(&book);
            long long mid_price = calc_mid_price(&book);
            long long total_bid_qty = calc_total_qty(book.bids, book.nbids, 10); // top 10 levels
            long long total_ask_qty = calc_total_qty(book.asks, book.nasks, 10);
            double depth_imbalance = calc_depth_imbalance(&book, 10);

            sqlite3_reset(ins); 
            sqlite3_clear_bindings(ins);
            
            sqlite3_bind_int64(ins, 1, last_ts_ms);
            sqlite3_bind_text(ins, 2, symbol, -1, SQLITE_STATIC);
            
            // Best bid/ask
            if (has_bb) {
                char s1[64], s2[64]; 
                to_str(bb.px, s1, sizeof s1); 
                to_str(bb.qty, s2, sizeof s2);
                sqlite3_bind_text(ins, 3, s1, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 4, s2, -1, SQLITE_TRANSIENT);
            } else { 
                sqlite3_bind_null(ins, 3); 
                sqlite3_bind_null(ins, 4); 
            }
            
            if (has_ba) {
                char s1[64], s2[64]; 
                to_str(ba.px, s1, sizeof s1); 
                to_str(ba.qty, s2, sizeof s2);
                sqlite3_bind_text(ins, 5, s1, -1, SQLITE_TRANSIENT);
                sqlite3_bind_text(ins, 6, s2, -1, SQLITE_TRANSIENT);
            } else { 
                sqlite3_bind_null(ins, 5); 
                sqlite3_bind_null(ins, 6); 
            }

            // Additional metrics
            if (spread > 0) {
                char s[64]; to_str(spread, s, sizeof s);
                sqlite3_bind_text(ins, 7, s, -1, SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(ins, 7);
            }
            
            if (mid_price > 0) {
                char s[64]; to_str(mid_price, s, sizeof s);
                sqlite3_bind_text(ins, 8, s, -1, SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(ins, 8);
            }
            
            char s1[64], s2[64];
            to_str(total_bid_qty, s1, sizeof s1);
            to_str(total_ask_qty, s2, sizeof s2);
            sqlite3_bind_text(ins, 9, s1, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 10, s2, -1, SQLITE_TRANSIENT);
            sqlite3_bind_double(ins, 11, depth_imbalance);

            if (sqlite3_step(ins) != SQLITE_DONE) {
                fprintf(stderr, "insert snapshot failed: %s\n", sqlite3_errmsg(db));
                break;
            }
            snapshots++;
            
            if (snapshots % 10 == 0) {
                printf("[progress] processed %d events, %d snapshots (bid levels: %d, ask levels: %d)\n", 
                       processed, snapshots, book.nbids, book.nasks);
            }
        }
    }

    printf("[info] processed %d events\n", processed);
    printf("[info] wrote %d snapshots to book_snapshots\n", snapshots);
    printf("[info] final book state: %d bid levels, %d ask levels\n", book.nbids, book.nasks);

    // Show final best bid/ask
    Level bb, ba;
    if (best_bid(&book, &bb) && best_ask(&book, &ba)) {
        char bid_str[64], ask_str[64];
        to_str(bb.px, bid_str, sizeof bid_str);
        to_str(ba.px, ask_str, sizeof ask_str);
        printf("[info] final best bid: %s, best ask: %s\n", bid_str, ask_str);
    }

    sqlite3_finalize(q);
    sqlite3_finalize(ins);
    sqlite3_close(db);
    return 0;
}