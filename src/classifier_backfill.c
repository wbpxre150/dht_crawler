#include "classifier_backfill.h"
#include "classifier.h"
#include "database.h"
#include "dht_crawler.h"

#include <errno.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

/* Rows per keyset page, and per write transaction. */
#define CLF_BACKFILL_BATCH 2000

/*
 * Filenames fed to the classifier per torrent.
 *
 * The ingest path hands classifier_classify() the whole BEP9 file list
 * (tree_metadata.c builds torrent_metadata_t from it), and tparse walks all of
 * it: `__nfiles_<n>` and the `__n{video,audio,...}_<n>` feature tokens count
 * the full list. The backfill reads the torrent's ENTIRE stored file list too -
 * uncapped BY DESIGN - so its feature text, and therefore its label, is
 * bit-identical to the ingest-time one.
 *
 * The per-torrent file fetch orders by torrent_files.id (the rowid), not by
 * file_index: ingest inserts the rows strictly in BEP9 file order, so ascending
 * rowid reconstructs that order even where the int16_t file_index wrapped
 * negative past 32767 files - see clf_bf_fetch_files().
 *
 * A cap here (the first stage 8 build shipped one at 50 files) makes any
 * torrent with more files classify differently from ingest: on the production
 * database it flipped ~10% of the >50-file rows between adult/category labels.
 * The transient cost of the full read is the same the ingest path already pays
 * for that torrent (a file_info_t array plus one strdup per path; the largest
 * phone torrent is ~42k files, a few MB), so truncating saves nothing.
 */

/* Yield between pages; the job only runs on spare I/O and CPU. */
#define CLF_BACKFILL_SLEEP_US 100000

/* Per-thread nice of the backfill thread (Linux: PRIO_PROCESS targets a tid).
 * 19 is the lowest priority, below the crawler's worker threads. */
#define CLF_BACKFILL_NICE 19

/* classifier_meta key holding the resume cursor. */
#define CLF_BACKFILL_CKPT_KEY "backfill_ckpt"

/* ---- job state --------------------------------------------------------- */

typedef struct {
    int64_t id;
    int64_t size_bytes;
    char *name;    /* owned by the page */
    int has_ver;   /* 1 when the torrent already has a torrent_class row */
    long long ver; /* that row's clf_ver */
} clf_page_row_t;

typedef struct {
    int64_t id;
    classification_t cls;
} clf_result_t;

typedef struct {
    pthread_mutex_t mutex;     /* guards every field below */
    pthread_t thread;
    int thread_started;        /* pthread_create() succeeded and the thread is joinable */
    atomic_bool stop;          /* shutdown request, checked between pages and torrents */
    classifier_backfill_stats_t stats;
} clf_backfill_t;

static clf_backfill_t bf = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .thread_started = 0,
    .stop = false,
    .stats = {
        .state = CLASSIFIER_BACKFILL_IDLE,
    },
};

/* Publish a full progress snapshot: the /stats reader always sees a coherent
 * set of fields, never a half-updated page count. */
static void clf_bf_publish(classifier_backfill_state_t state, int available, uint64_t done,
                           uint64_t total, double rate_rows_s, long long ckpt,
                           long long clf_ver) {
    pthread_mutex_lock(&bf.mutex);
    bf.stats.state = (int)state;
    bf.stats.available = available;
    bf.stats.done = done;
    bf.stats.total = total;
    bf.stats.rate_rows_s = rate_rows_s;
    bf.stats.ckpt = ckpt;
    bf.stats.clf_ver = clf_ver;
    pthread_mutex_unlock(&bf.mutex);
}

/* nanosleep() rather than usleep(): it is what POSIX.1-2008 declares, so the
 * file needs no _DEFAULT_SOURCE (unlike the older callers of usleep). */
static void clf_bf_sleep_us(long usec) {
    struct timespec ts;
    ts.tv_sec = usec / 1000000;
    ts.tv_nsec = (usec % 1000000) * 1000;
    nanosleep(&ts, NULL);
}

static double clf_bf_elapsed_s(const struct timespec *t0) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    int64_t ns = (int64_t)(now.tv_sec - t0->tv_sec) * 1000000000LL +
                 ((int64_t)now.tv_nsec - t0->tv_nsec);
    if (ns <= 0) return 0.0;
    return (double)ns / 1000000000.0;
}

/* Per-thread nice: gettid() (not pthread_self()) is what PRIO_PROCESS wants on
 * Linux, and syscall(SYS_gettid) is the one form both glibc and bionic export
 * under the Makefile's -std=c99 -D_POSIX_C_SOURCE=200809L. */
static void clf_bf_set_thread_nice(void) {
    long tid = (long)syscall(SYS_gettid);
    if (setpriority(PRIO_PROCESS, (id_t)tid, CLF_BACKFILL_NICE) != 0) {
        log_msg(LOG_WARN, "Classifier backfill: setpriority(%d) failed: %s", CLF_BACKFILL_NICE,
                strerror(errno));
    } else {
        log_msg(LOG_DEBUG, "Classifier backfill: running at nice %d (tid %ld)", CLF_BACKFILL_NICE,
                tid);
    }
}

/* ---- database helpers -------------------------------------------------- */

/* Resume cursor, 0 when the job never wrote one. */
static long long clf_bf_read_ckpt(database_t *db) {
    static const char *sql = "SELECT value FROM classifier_meta WHERE key = ?";
    sqlite3_stmt *stmt = NULL;
    long long ckpt = 0;

    database_read_lock(db);
    database_write_lock(db);
    if (sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_DEBUG, "Classifier backfill: no checkpoint to resume from (%s)",
                sqlite3_errmsg(db->db));
        database_write_unlock(db);
        database_read_unlock(db);
        return 0;
    }
    sqlite3_bind_text(stmt, 1, CLF_BACKFILL_CKPT_KEY, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *value = sqlite3_column_text(stmt, 0);
        if (value) ckpt = atoll((const char *)value);
    }
    sqlite3_finalize(stmt);
    database_write_unlock(db);
    database_read_unlock(db);
    return (ckpt > 0) ? ckpt : 0;
}

/*
 * Rows the sweep starting at from_id has to classify: no torrent_class row, or
 * one below the loaded model version. One pass over the id space per sweep;
 * the count is what makes /stats able to show done/total.
 *
 * This is the one read that does not take the connection mutex: on a large
 * database it is a full scan with one primary-key probe per torrent, and
 * holding the writer's mutex for its duration would stall batch flushes. The
 * read lock is taken, so a concurrent checkpoint waits for the scan instead of
 * being asked to run behind an active statement; the count itself is only
 * progress reporting, so an ingest row inserted mid-scan can shift it by one.
 */
static uint64_t clf_bf_count_pending(database_t *db, long long from_id, long long clf_ver) {
    static const char *sql =
        "SELECT COUNT(*) FROM torrents t "
        "LEFT JOIN torrent_class c ON c.torrent_id = t.id "
        "WHERE t.id > ? AND (c.torrent_id IS NULL OR c.clf_ver < ?)";
    sqlite3_stmt *stmt = NULL;
    uint64_t total = 0;

    database_read_lock(db);
    if (sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Classifier backfill: pending-count query failed: %s",
                sqlite3_errmsg(db->db));
        database_read_unlock(db);
        return 0;
    }
    sqlite3_bind_int64(stmt, 1, from_id);
    sqlite3_bind_int64(stmt, 2, clf_ver);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        total = (uint64_t)sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    database_read_unlock(db);
    return total;
}

static void clf_bf_free_page(clf_page_row_t *rows, int n) {
    if (!rows) return;
    for (int i = 0; i < n; i++) free(rows[i].name);
    free(rows);
}

/*
 * Read the next keyset page plus the class version each row already carries.
 * Returns the number of rows (0 = end of the id space) or -1 on error; the
 * caller owns *out and frees it with clf_bf_free_page().
 *
 * Locking follows database_check_exists(): the read lock keeps a concurrent WAL
 * checkpoint off the statement, and the connection mutex keeps the read out of
 * the batch writer's open transaction on the same connection (its BEGIN..COMMIT
 * holds that mutex too). Both are released once the statement is finalized, so
 * the implicit read transaction cannot block the write path's checkpoint. The
 * order is always read lock -> mutex, never the other way round.
 */
static int clf_bf_fetch_page(database_t *db, long long from_id, clf_page_row_t **out) {
    static const char *sql =
        "SELECT t.id, t.name, t.size_bytes, c.clf_ver FROM torrents t "
        "LEFT JOIN torrent_class c ON c.torrent_id = t.id "
        "WHERE t.id > ? ORDER BY t.id LIMIT ?";
    clf_page_row_t *rows = calloc(CLF_BACKFILL_BATCH, sizeof(*rows));
    if (!rows) {
        log_msg(LOG_ERROR, "Classifier backfill: out of memory for a page");
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    int n = 0;
    int failed = 0;

    *out = NULL;
    database_read_lock(db);
    database_write_lock(db);
    if (sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Classifier backfill: page query failed: %s", sqlite3_errmsg(db->db));
        database_write_unlock(db);
        database_read_unlock(db);
        free(rows);
        return -1;
    }

    sqlite3_bind_int64(stmt, 1, from_id);
    sqlite3_bind_int(stmt, 2, CLF_BACKFILL_BATCH);
    while (n < CLF_BACKFILL_BATCH) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE) break;
        if (rc != SQLITE_ROW) {
            log_msg(LOG_ERROR, "Classifier backfill: page read failed: %s", sqlite3_errmsg(db->db));
            failed = 1;
            break;
        }
        const unsigned char *name = sqlite3_column_text(stmt, 1);
        rows[n].id = sqlite3_column_int64(stmt, 0);
        rows[n].size_bytes = sqlite3_column_int64(stmt, 2);
        rows[n].name = strdup(name ? (const char *)name : "");
        if (!rows[n].name) {
            log_msg(LOG_ERROR, "Classifier backfill: out of memory for a torrent name");
            failed = 1;
            break;
        }
        if (sqlite3_column_type(stmt, 3) != SQLITE_NULL) {
            rows[n].has_ver = 1;
            rows[n].ver = sqlite3_column_int64(stmt, 3);
        }
        n++;
    }
    sqlite3_finalize(stmt);
    database_write_unlock(db);
    database_read_unlock(db);

    if (failed) {
        clf_bf_free_page(rows, n);
        return -1;
    }
    *out = rows;
    return n;
}

/*
 * Release a file list returned by clf_bf_fetch_files(): the paths and the array.
 */
static void clf_bf_free_files(file_info_t *files, int n) {
    if (!files) return;
    for (int i = 0; i < n; i++) free(files[i].path);
    free(files);
}

/*
 * Every filename of one torrent, in the same shape the ingest path passes to
 * classifier_classify(): the COALESCE(prefix || '/' || filename) path /browse
 * shows, in ascending torrent_files.id (rowid) order. torrent_files rows are
 * inserted strictly in BEP9 file order inside the batch insert, so ascending
 * rowid reconstructs the original order even where file_index (int16_t) wrapped
 * negative past 32767 files; ingest itself feeds the in-memory list in original
 * order, so this restores bit-identical input. The read is uncapped
 * BY DESIGN - see the note at the top of this file - so the caller gets exactly
 * the list ingest saw.
 *
 * *out is set to a malloc'd array the caller owns (NULL when the torrent has no
 * file rows); free it with clf_bf_free_files(). Returns the number of files
 * (0 is valid: name-only classification), or -1 on error.
 */
static int clf_bf_fetch_files(database_t *db, int64_t torrent_id, file_info_t **out) {
    static const char *sql =
        "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename), tf.size_bytes "
        "FROM torrent_files tf "
        "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
        "WHERE tf.torrent_id = ? ORDER BY tf.id";
    sqlite3_stmt *stmt = NULL;
    file_info_t *files = NULL;
    size_t cap = 0;
    int n = 0;
    int failed = 0;

    *out = NULL;

    database_read_lock(db);
    database_write_lock(db);
    if (sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Classifier backfill: file query failed: %s", sqlite3_errmsg(db->db));
        database_write_unlock(db);
        database_read_unlock(db);
        return -1;
    }

    sqlite3_bind_int64(stmt, 1, torrent_id);
    for (;;) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_DONE) break;
        if (rc != SQLITE_ROW) {
            log_msg(LOG_ERROR, "Classifier backfill: file read failed for torrent %lld: %s",
                    (long long)torrent_id, sqlite3_errmsg(db->db));
            failed = 1;
            break;
        }
        /* Grown on demand rather than bounded: the ingest path has no cap
         * either, so the whole list is read whatever its size (the largest
         * phone torrent is ~42k files, a few MB - the same cost ingest already
         * pays for that torrent). */
        if ((size_t)n == cap) {
            size_t new_cap = cap ? cap * 2 : 64;
            file_info_t *grown = realloc(files, new_cap * sizeof(*grown));
            if (!grown) {
                log_msg(LOG_ERROR, "Classifier backfill: out of memory for a file list");
                failed = 1;
                break;
            }
            files = grown;
            cap = new_cap;
        }
        const unsigned char *path = sqlite3_column_text(stmt, 0);
        files[n].path = strdup(path ? (const char *)path : "");
        if (!files[n].path) {
            log_msg(LOG_ERROR, "Classifier backfill: out of memory for a file path");
            failed = 1;
            break;
        }
        files[n].size_bytes = sqlite3_column_int64(stmt, 1);
        files[n].file_index = (int16_t)n;
        n++;
    }
    sqlite3_finalize(stmt);
    database_write_unlock(db);
    database_read_unlock(db);

    if (failed) {
        clf_bf_free_files(files, n);
        return -1;
    }
    *out = files;
    return n;
}

/*
 * Classify one stored torrent exactly like the ingest call site does, from the
 * stored name, size and full stored file list (uncapped, so the labels match
 * ingest bit for bit). Filters are not re-applied: the row is already in the
 * database, so the only question left is what its class row says.
 * Returns 0 and fills *out, or 1 when no classification is available.
 */
static int clf_bf_classify_row(database_t *db, const clf_page_row_t *row, classification_t *out) {
    /* Sized to the torrent's real file count by clf_bf_fetch_files(): the same
     * list, in the BEP9/rowid order, the ingest path hands
     * classifier_classify(). */
    file_info_t *files = NULL;
    int n_files = clf_bf_fetch_files(db, row->id, &files);
    if (n_files < 0) return 1;

    torrent_metadata_t meta;
    memset(&meta, 0, sizeof(meta));
    meta.name = row->name; /* classifier_classify() only reads it */
    meta.size_bytes = row->size_bytes;
    meta.files = files;
    meta.num_files = n_files;

    int rc = classifier_classify(&meta, out);
    clf_bf_free_files(files, n_files);
    return rc;
}

/*
 * Commit one page: the torrent_class upserts plus the checkpoint, in a single
 * short transaction (the same shape database_insert_torrent_unsafe() uses for
 * the ingest path, but replacing an existing row: a stale clf_ver is exactly
 * what this job is here to fix).
 *
 * Returns 0 on success (including "nothing to write"), -1 when the page was
 * rolled back.
 */
static int clf_bf_write_page(database_t *db, const clf_result_t *results, size_t n_results,
                             long long ckpt_after) {
    static const char *upsert_sql =
        "INSERT OR REPLACE INTO torrent_class "
        "(torrent_id, category, tv_kind, adult, adult_p, season_lo, season_hi, "
        " episode_lo, episode_hi, resolution, confidence, clf_ver) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    static const char *ckpt_sql =
        "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES (?, ?)";

    sqlite3_stmt *upsert = NULL;
    sqlite3_stmt *ckpt = NULL;
    int failed = 0;

    database_write_lock(db);
    if (database_begin_transaction(db) != 0) {
        database_write_unlock(db);
        return -1;
    }

    if (sqlite3_prepare_v2(db->db, upsert_sql, -1, &upsert, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Classifier backfill: prepare torrent_class upsert failed: %s",
                sqlite3_errmsg(db->db));
        failed = 1;
    }

    for (size_t i = 0; !failed && i < n_results; i++) {
        const classification_t *c = &results[i].cls;
        sqlite3_reset(upsert);
        sqlite3_bind_int64(upsert, 1, results[i].id);
        sqlite3_bind_int(upsert, 2, c->category);
        if (c->tv_kind > 0) {
            sqlite3_bind_int(upsert, 3, c->tv_kind);
        } else {
            sqlite3_bind_null(upsert, 3); /* NULL unless tv/anime */
        }
        sqlite3_bind_int(upsert, 4, c->adult);
        sqlite3_bind_double(upsert, 5, c->adult_p);
        sqlite3_bind_int(upsert, 6, c->season_lo);
        sqlite3_bind_int(upsert, 7, c->season_hi);
        sqlite3_bind_int(upsert, 8, c->episode_lo);
        sqlite3_bind_int(upsert, 9, c->episode_hi);
        sqlite3_bind_int(upsert, 10, c->resolution);
        sqlite3_bind_double(upsert, 11, c->confidence);
        sqlite3_bind_int64(upsert, 12, c->clf_ver);
        if (sqlite3_step(upsert) != SQLITE_DONE) {
            log_msg(LOG_ERROR, "Classifier backfill: torrent_class write for %lld failed: %s",
                    (long long)results[i].id, sqlite3_errmsg(db->db));
            failed = 1;
        }
    }
    if (upsert) sqlite3_finalize(upsert);

    if (!failed) {
        char value[32];
        snprintf(value, sizeof(value), "%lld", ckpt_after);
        if (sqlite3_prepare_v2(db->db, ckpt_sql, -1, &ckpt, NULL) != SQLITE_OK) {
            log_msg(LOG_ERROR, "Classifier backfill: prepare checkpoint write failed: %s",
                    sqlite3_errmsg(db->db));
            failed = 1;
        } else {
            sqlite3_bind_text(ckpt, 1, CLF_BACKFILL_CKPT_KEY, -1, SQLITE_STATIC);
            sqlite3_bind_text(ckpt, 2, value, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(ckpt) != SQLITE_DONE) {
                log_msg(LOG_ERROR, "Classifier backfill: checkpoint write failed: %s",
                        sqlite3_errmsg(db->db));
                failed = 1;
            }
        }
        if (ckpt) sqlite3_finalize(ckpt);
    }

    if (failed) {
        database_rollback_transaction(db);
        database_write_unlock(db);
        return -1;
    }
    if (database_commit_transaction(db) != 0) {
        database_rollback_transaction(db);
        database_write_unlock(db);
        return -1;
    }
    database_write_unlock(db);
    return 0;
}

/*
 * One sweep of the id space starting at from_id. Returns the number of rows
 * classified, or -1 when the sweep was cut short (stop requested or a database
 * error) - the caller tells the two apart by the stop flag.
 *
 * Classification runs outside any lock; only the page fetch and the page write
 * touch the database, and the write lock is held for the transaction alone.
 */
static long long clf_bf_sweep(database_t *db, long long from_id, long long clf_ver,
                              uint64_t total, long long *out_ckpt) {
    struct timespec sweep_start;
    clock_gettime(CLOCK_MONOTONIC, &sweep_start);

    long long ckpt = from_id;
    uint64_t done = 0;
    clf_page_row_t *rows = NULL;
    clf_result_t *results = NULL;
    int n;

    /* The cursor reached is handed back on every exit path (also the aborted
     * ones): it is what /stats reports and where a restart resumes. */
    *out_ckpt = ckpt;
    n = clf_bf_fetch_page(db, ckpt, &rows);
    if (n < 0) return -1;

    while (n > 0) {
        if (atomic_load(&bf.stop)) break;

        results = calloc((size_t)n, sizeof(*results));
        if (!results) {
            log_msg(LOG_ERROR, "Classifier backfill: out of memory for a page of results");
            clf_bf_free_page(rows, n);
            *out_ckpt = ckpt;
            return -1;
        }

        size_t n_results = 0;
        int aborted = 0;
        for (int i = 0; i < n; i++) {
            /* Checked per torrent as well as per page: shutdown must not wait
             * for a whole 2000-row page of classifications. */
            if (atomic_load(&bf.stop)) {
                aborted = 1;
                break;
            }
            if (rows[i].has_ver && rows[i].ver >= clf_ver) continue; /* already current */
            if (clf_bf_classify_row(db, &rows[i], &results[n_results].cls) != 0) {
                /* Not classifiable right now (models unavailable, parser
                 * rejected the name): leave the row alone, a later daemon
                 * start retries it. */
                continue;
            }
            results[n_results].id = rows[i].id;
            n_results++;
        }

        long long ckpt_after = rows[n - 1].id;
        int wrote = 0;
        if (!aborted && n_results > 0) {
            if (clf_bf_write_page(db, results, n_results, ckpt_after) != 0) {
                log_msg(LOG_ERROR, "Classifier backfill: page ending at id %lld rolled back",
                        ckpt_after);
                free(results);
                clf_bf_free_page(rows, n);
                *out_ckpt = ckpt;
                return -1;
            }
            wrote = 1;
            done += n_results;
        }

        free(results);
        results = NULL;
        clf_bf_free_page(rows, n);
        rows = NULL;
        ckpt = ckpt_after;
        *out_ckpt = ckpt;

        if (wrote) {
            /* Same discipline as batch_writer_flush(): one RESTART checkpoint
             * per write batch, so the WAL cannot grow without bound. */
            database_wal_checkpoint(db);
        }

        double elapsed = clf_bf_elapsed_s(&sweep_start);
        double rate = (elapsed > 0.0) ? (double)done / elapsed : 0.0;
        clf_bf_publish(CLASSIFIER_BACKFILL_RUNNING, 1, done, total, rate, ckpt, clf_ver);

        if (aborted || atomic_load(&bf.stop)) break;

        clf_bf_sleep_us(CLF_BACKFILL_SLEEP_US);
        n = clf_bf_fetch_page(db, ckpt, &rows);
        if (n < 0) return -1; /* rows stays NULL */
    }

    clf_bf_free_page(rows, n > 0 ? n : 0);
    return (long long)done;
}

/* ---- thread ------------------------------------------------------------ */

static void *clf_bf_thread_func(void *arg) {
    database_t *db = (database_t *)arg;
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    clf_bf_set_thread_nice();

    if (!classifier_enabled()) {
        /* Nothing to classify: a build without CLF_ENABLED, or
         * classifier_enabled=0 in config.ini. Leave the /stats object at
         * available=false/state=idle rather than sweeping for nothing. */
        log_msg(LOG_INFO, "Classifier backfill: classifier not available; job inactive");
        clf_bf_publish(CLASSIFIER_BACKFILL_IDLE, 0, 0, 0, 0.0, 0, 0);
        return NULL;
    }

    long long clf_ver = classifier_current_version();
    long long ckpt = clf_bf_read_ckpt(db);
    if (ckpt > 0) {
        log_msg(LOG_INFO, "Classifier backfill: resuming sweep at id > %lld (clf_ver=%lld)", ckpt,
                clf_ver);
    } else {
        log_msg(LOG_INFO, "Classifier backfill: sweeping from id 0 (clf_ver=%lld)", clf_ver);
    }

    long long done = 0;
    uint64_t total = 0;

    for (;;) {
        /* A sweep that started at 0 and classified nothing proves the whole id
         * space is current (including the rows below a resumed checkpoint). */
        int started_at_zero = (ckpt == 0);

        clf_bf_publish(CLASSIFIER_BACKFILL_RUNNING, 1, 0, 0, 0.0, ckpt, clf_ver);
        total = clf_bf_count_pending(db, ckpt, clf_ver);
        log_msg(LOG_INFO, "Classifier backfill: sweep from id > %lld has %llu row(s) to classify",
                ckpt, (unsigned long long)total);
        clf_bf_publish(CLASSIFIER_BACKFILL_RUNNING, 1, 0, total, 0.0, ckpt, clf_ver);

        done = 0;
        if (total > 0) {
            /* The sweep also hands back the cursor it reached, so an aborted
             * sweep still reports the id a restart will resume at. */
            done = clf_bf_sweep(db, ckpt, clf_ver, total, &ckpt);
            if (done < 0) {
                if (atomic_load(&bf.stop)) {
                    log_msg(LOG_DEBUG, "Classifier backfill: stopped mid-sweep at id %lld", ckpt);
                } else {
                    log_msg(LOG_ERROR, "Classifier backfill: sweep aborted by a database error");
                }
                clf_bf_publish(CLASSIFIER_BACKFILL_IDLE, 1, 0, total, 0.0, ckpt, clf_ver);
                return NULL;
            }
        }

        if (atomic_load(&bf.stop)) break;

        if (started_at_zero && done == 0) {
            log_msg(LOG_INFO,
                    "Classifier backfill: nothing left to classify (sweep from id 0 found nothing); job complete in %.1fs",
                    clf_bf_elapsed_s(&t0));
            clf_bf_publish(CLASSIFIER_BACKFILL_COMPLETE, 1, 0, 0, 0.0, ckpt, clf_ver);
            return NULL;
        }

        /* This sweep classified rows: sweep again from 0, which catches rows
         * left stale below the checkpoint the sweep resumed at (a model
         * version bump re-classifies the whole id space). */
        ckpt = 0;
    }

    /* Shutdown: keep the interrupted sweep's progress visible, with the cursor
     * a restart resumes at. */
    double elapsed = clf_bf_elapsed_s(&t0);
    clf_bf_publish(CLASSIFIER_BACKFILL_IDLE, 1, (uint64_t)done, total,
                   (elapsed > 0.0) ? (double)done / elapsed : 0.0, ckpt, clf_ver);
    log_msg(LOG_DEBUG, "Classifier backfill: thread exiting");
    return NULL;
}

/* ---- public API -------------------------------------------------------- */

int classifier_backfill_start(database_t *db) {
    if (!db || !db->db) {
        log_msg(LOG_WARN, "Classifier backfill: no database to sweep");
        return -1;
    }

    pthread_mutex_lock(&bf.mutex);
    if (bf.thread_started) {
        pthread_mutex_unlock(&bf.mutex);
        return 0;
    }
    atomic_store(&bf.stop, false);

    int rc = pthread_create(&bf.thread, NULL, clf_bf_thread_func, db);
    if (rc != 0) {
        log_msg(LOG_WARN, "Classifier backfill: thread could not be started: %s", strerror(rc));
        pthread_mutex_unlock(&bf.mutex);
        return -1;
    }
    bf.thread_started = 1;
    pthread_mutex_unlock(&bf.mutex);

    log_msg(LOG_DEBUG, "Classifier backfill: job started");
    return 0;
}

void classifier_backfill_stop(void) {
    pthread_mutex_lock(&bf.mutex);
    if (!bf.thread_started) {
        pthread_mutex_unlock(&bf.mutex);
        return;
    }
    atomic_store(&bf.stop, true);
    pthread_t thread = bf.thread;
    pthread_mutex_unlock(&bf.mutex);

    pthread_join(thread, NULL);

    pthread_mutex_lock(&bf.mutex);
    bf.thread_started = 0;
    pthread_mutex_unlock(&bf.mutex);
    log_msg(LOG_DEBUG, "Classifier backfill: job stopped");
}

void classifier_backfill_get_stats(classifier_backfill_stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&bf.mutex);
    *out = bf.stats;
    pthread_mutex_unlock(&bf.mutex);
}

const char *classifier_backfill_state_name(int state) {
    switch (state) {
        case CLASSIFIER_BACKFILL_RUNNING:
            return "running";
        case CLASSIFIER_BACKFILL_COMPLETE:
            return "complete";
        default:
            return "idle";
    }
}
