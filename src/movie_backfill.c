/*
 * Resumable movie-kind backfill: relabel the torrent_class rows of an existing
 * database with the generation the *loaded* movie manifest describes.
 *
 * A sibling of src/classify_backfill.c, not a copy of its policy: this engine
 * owns torrent_class.movie_kind_mask / movie_kind_clf_ver and the two
 * movie_* meta keys, and it never writes clf_ver, category, adult or any other
 * fastText column - a movie pass must never make the fastText engine think a
 * row is up to date (tests/test_movie_backfill.c asserts this row by row).
 *
 * The pass walks torrent ids in windows, materialises each window before it
 * writes anything (the JOIN that produced the candidates must not still be
 * stepping while the batch UPDATEs torrent_class), classifies from the name
 * alone - the v9 pipeline renders "Torrent: <name>\nFilm genres:" and ignores
 * files - and commits one transaction per batch with the busy-retry discipline
 * the fastText engine uses. The resume key is written in the same transaction
 * as the rows it covers, so a crash can only lose a batch, never skip one.
 *
 * Thermal back-off: between batches the engine reads the thermal zones and, at
 * or above opts.thermal_backoff_c, pauses in short sleeps that honour the stop
 * flag until the hottest zone drops below threshold - 2 C. Both the reader and
 * the sleeper are seams (MOVIE_BACKFILL_TEST), so the host test drives a pause
 * without a hot CPU.
 */
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE /* for strdup under -std=c99 */
#endif

#include "movie_backfill.h"

#include "dht_crawler.h"
#include "movie_kind.h"

#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------------ *
 * Tunables
 * ------------------------------------------------------------------------ */

#define MB_CHUNK_IDS_DEFAULT 10000
#define MB_BATCH_ROWS_DEFAULT 500

/* A busy writer is retried; the UPDATEs are idempotent, so replaying a whole
 * batch is safe. sqlite3_busy_timeout() already waits per statement; the
 * retries cover a writer that holds the lock longer than that. */
#define MB_BUSY_TIMEOUT_MS 30000
#define MB_BUSY_ATTEMPTS 5
#define MB_BUSY_SLEEP_MS 50

/* Thermal gate. The pause is slept in slices this long so a stop request is
 * honoured within 250 ms even while the device is too hot to continue, and the
 * pass resumes only once the hottest zone is below threshold - this margin. */
#define MB_THERMAL_SLEEP_SLICE_MS 250
#define MB_THERMAL_RESUME_MARGIN_C 2
#define MB_THERMAL_ZONES_MAX 32
#define MB_THERMAL_ZONE_PATH_FMT "/sys/class/thermal/thermal_zone%d/temp"

/* torrent_class.category value for a movie: TP_CAT_MOVIE from the vendored
 * dht_crawler-android/tparse/taxonomy.h. That header only exists on the Android
 * wrapper's include path while this file also compiles for the crawler's own
 * host `make`, so the value is mirrored here - exactly what src/http_api.c does
 * for its category filters. It is frozen by the data already on the phone. */
#define MB_CAT_MOVIE 1

/* Bench (--movie-kind-bench). */
#define MB_BENCH_ROWS_DEFAULT 1000
#define MB_BENCH_ROWS_MAX 200000
#define MB_BENCH_MIN_SECONDS 600.0
/* Upper bound on the retained timing samples (~1.6 MiB): a ten-minute run over
 * 1000 names needs ~15k, so this is headroom, not a limit a real run meets. */
#define MB_BENCH_SAMPLE_CAP 200000

/* Cooperative cancel: set by movie_backfill_request_stop(), cleared at the
 * start of every pass, read between batches (never inside one). */
static volatile sig_atomic_t mb_stop_requested = 0;

/* ------------------------------------------------------------------------ *
 * Test seams
 *
 * Every call into the movie-kind module - and the thermal reader and the
 * sleeper - goes through a file-static pointer that defaults to the real
 * function, so a host test can drive the whole engine without a model and
 * without a hot CPU. The pointers are file-static on purpose: the daemon build
 * exports no test surface, and the swap-in entry point below only exists in the
 * test build (-DMOVIE_BACKFILL_TEST, set by the tests-movie-backfill target).
 * ------------------------------------------------------------------------ */

typedef int (*mb_classify_fn)(const char *name, uint16_t *mask);
typedef int (*mb_flag_fn)(void);
typedef long long (*mb_version_fn)(void);
typedef int (*mb_thermal_fn)(void);
typedef void (*mb_sleep_fn)(long long ms);
typedef void (*mb_pause_fn)(void);

static int mb_thermal_max_c_real(void);
static void mb_sleep_real(long long ms);
/* Defined with the bench, but the `--dump-masks` observation mode of the real
 * pass prints through the same two helpers (and the same skip-on-failure rule)
 * from mb_classify_one() above them. */
static void mb_print_mask_line(uint16_t mask, const char *name);
static int mb_should_print_mask(int classify_rc);

static mb_classify_fn mb_classify = movie_kind_classify;
static mb_flag_fn mb_enabled = movie_kind_enabled;
static mb_version_fn mb_generation = movie_kind_generation;
static mb_thermal_fn mb_thermal_max_c = mb_thermal_max_c_real;
static mb_sleep_fn mb_sleep = mb_sleep_real;
static mb_pause_fn mb_note_thermal_pause = movie_kind_note_thermal_pause;

#ifdef MOVIE_BACKFILL_TEST
void movie_backfill_set_hooks(const movie_backfill_hooks_t *hooks) {
    if (!hooks) {
        return;
    }
    if (hooks->classify) {
        mb_classify = hooks->classify;
    }
    if (hooks->enabled) {
        mb_enabled = hooks->enabled;
    }
    if (hooks->generation) {
        mb_generation = hooks->generation;
    }
    if (hooks->thermal_max_c) {
        mb_thermal_max_c = hooks->thermal_max_c;
    }
    if (hooks->sleep_ms) {
        mb_sleep = hooks->sleep_ms;
    }
    if (hooks->note_thermal_pause) {
        mb_note_thermal_pause = hooks->note_thermal_pause;
    }
}
#endif /* MOVIE_BACKFILL_TEST */

/* ------------------------------------------------------------------------ *
 * Types
 * ------------------------------------------------------------------------ */

/* One candidate of the current id window. The window is read into memory before
 * the first write of the window. */
typedef struct {
    long long id;
    char *name; /* owned copy of torrents.name, byte-exact */
} mb_candidate_t;

/* One labelled row waiting for the next batch transaction. */
typedef struct {
    long long torrent_id;
    uint16_t mask;
} mb_pending_t;

typedef struct {
    database_t *db;
    long long target;
    int chunk_ids;
    int batch_rows;
    long long limit_rows;
    int thermal_backoff_c;
    int thermal_resume_c;
    sqlite3_stmt *hi_stmt;
    sqlite3_stmt *cand_stmt;
    sqlite3_stmt *update_stmt;
    sqlite3_stmt *meta_stmt;
    mb_pending_t *batch;
    int batch_count;
    int dump_masks; /* opts.dump_masks: echo every labelled row's mask */
    movie_backfill_progress_t *prog;
} mb_ctx_t;

/* A batch body that must run with db->mutex held and returns a SQLite code. */
typedef int (*mb_locked_fn_t)(mb_ctx_t *c);

/* ------------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------------ */

const char *movie_backfill_state_name(int state) {
    switch (state) {
    case MOVIE_BACKFILL_IDLE:
        return "idle";
    case MOVIE_BACKFILL_RUNNING:
        return "running";
    case MOVIE_BACKFILL_DONE:
        return "done";
    case MOVIE_BACKFILL_STOPPED:
        return "stopped";
    case MOVIE_BACKFILL_DISABLED:
        return "disabled";
    default:
        return "unknown";
    }
}

/* The final summary line of every pass (the acceptance runs grep it). */
static void mb_log_summary(const movie_backfill_progress_t *p) {
    log_msg(LOG_INFO,
            "movie-backfill: done state=%s target_clf_ver=%lld total=%lld done=%lld "
            "updated=%lld skipped=%lld failed=%lld thermal_pauses=%lld last_id=%lld",
            movie_backfill_state_name(p->state), p->target_clf_ver, p->rows_total,
            p->rows_done, p->rows_updated, p->rows_skipped, p->rows_failed,
            p->thermal_pauses, p->last_id);
}

/* Copy a TEXT column byte-exactly. The snapshot has names that are not valid
 * UTF-8; re-encoding them would change the bytes the v9 pipeline sees, so the
 * bytes are passed through untouched. */
static char *mb_dup_column_text(sqlite3_stmt *stmt, int col) {
    const unsigned char *text = sqlite3_column_text(stmt, col);
    int bytes = sqlite3_column_bytes(stmt, col);
    char *copy;

    if (!text) {
        return strdup("");
    }
    copy = malloc((size_t)bytes + 1);
    if (!copy) {
        return NULL;
    }
    memcpy(copy, text, (size_t)bytes);
    copy[bytes] = '\0';
    return copy;
}

static void mb_free_candidates(mb_candidate_t *cands, int count) {
    int i;
    if (!cands) {
        return;
    }
    for (i = 0; i < count; i++) {
        free(cands[i].name);
    }
    free(cands);
}

/* INSERT OR REPLACE one classifier_meta row; returns the SQLite result code. */
static int mb_meta_put(sqlite3_stmt *stmt, const char *key, const char *value) {
    int rc;

    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    sqlite3_reset(stmt);
    return rc;
}

static void mb_sleep_real(long long ms) {
    struct timespec ts;

    if (ms <= 0) {
        return;
    }
    ts.tv_sec = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((ms % 1000) * 1000000L);
    nanosleep(&ts, NULL);
}

/* Hottest thermal zone in degrees Celsius, or -1 when the platform exposes
 * none (a desktop host, or a kernel without the thermal class). Each zone's
 * `temp` is millidegrees. */
static int mb_thermal_max_c_real(void) {
    char path[128];
    int best = -1;
    int zone;

    for (zone = 0; zone < MB_THERMAL_ZONES_MAX; zone++) {
        long milli = 0;
        FILE *f;
        int celsius;

        snprintf(path, sizeof path, MB_THERMAL_ZONE_PATH_FMT, zone);
        f = fopen(path, "r");
        if (!f) {
            continue;
        }
        if (fscanf(f, "%ld", &milli) == 1) {
            celsius = (int)(milli / 1000);
            if (celsius > best) {
                best = celsius;
            }
        }
        fclose(f);
    }
    return best;
}

/* ------------------------------------------------------------------------ *
 * Read helpers (no mutex: the pass reads on the shared handle and only takes
 * db->mutex around each BEGIN ... COMMIT batch)
 * ------------------------------------------------------------------------ */

/* Resume point for this generation, or 0 when there is none to honour. */
static long long mb_resume_last_id(database_t *db, long long target) {
    sqlite3_stmt *stmt = NULL;
    long long last_id = 0;

    if (sqlite3_prepare_v2(db->db, "SELECT value FROM classifier_meta WHERE key = ?", -1, &stmt,
                           NULL) != SQLITE_OK) {
        log_msg(LOG_DEBUG, "movie-backfill: resume key unavailable: %s", sqlite3_errmsg(db->db));
        return 0;
    }
    sqlite3_bind_text(stmt, 1, MOVIE_BACKFILL_RESUME_KEY, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *value = sqlite3_column_text(stmt, 0);
        if (value) {
            const char *sep = strchr((const char *)value, ':');
            if (sep) {
                long long generation = strtoll((const char *)value, NULL, 10);
                long long stored_id = strtoll(sep + 1, NULL, 10);
                /* Only this generation's key may skip rows; a tombstoned or
                 * foreign value parses to nothing usable and is ignored. */
                if (generation == target && stored_id > 0) {
                    last_id = stored_id;
                }
            }
        }
    }
    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);
    return last_id;
}

/* Number of candidates a pass starting after `start_id` would visit. */
static int mb_count_candidates(database_t *db, long long start_id, long long target,
                               long long *count_out) {
    sqlite3_stmt *stmt = NULL;
    long long count = 0;
    int rc;

    rc = sqlite3_prepare_v2(db->db,
                            "SELECT COUNT(*) FROM torrents t "
                            "JOIN torrent_class tc ON tc.torrent_id = t.id "
                            "WHERE t.id > ? AND tc.category = ? AND tc.adult = 0 AND "
                            "(tc.movie_kind_clf_ver IS NULL OR tc.movie_kind_clf_ver < ?)",
                            -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "movie-backfill: candidate count failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        return -1;
    }
    sqlite3_bind_int64(stmt, 1, start_id);
    sqlite3_bind_int(stmt, 2, MB_CAT_MOVIE);
    sqlite3_bind_int64(stmt, 3, target);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        log_msg(LOG_ERROR, "movie-backfill: candidate count step failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        sqlite3_reset(stmt);
        sqlite3_finalize(stmt);
        return -1;
    }
    count = sqlite3_column_int64(stmt, 0);
    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);

    *count_out = count;
    return 0;
}

/* Highest torrent id of the next chunk_ids window after `lo`; 0 when the walk
 * is over. Same shape as the rebuild-docs chunk bound. */
static int mb_next_hi(mb_ctx_t *c, long long lo, long long *hi_out) {
    sqlite3_stmt *stmt = c->hi_stmt;
    int rc;

    *hi_out = 0;
    sqlite3_reset(stmt);
    sqlite3_bind_int64(stmt, 1, lo);
    sqlite3_bind_int(stmt, 2, c->chunk_ids);
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
            *hi_out = sqlite3_column_int64(stmt, 0);
        }
    } else if (rc != SQLITE_DONE) {
        sqlite3_reset(stmt);
        log_msg(LOG_ERROR, "movie-backfill: chunk bound failed (%d: %s)", rc, sqlite3_errstr(rc));
        return -1;
    }
    sqlite3_reset(stmt);
    return 0;
}

/* Read the candidates of one id window (lo, hi] into memory. */
static int mb_load_window(mb_ctx_t *c, long long lo, long long hi, mb_candidate_t **out,
                          int *out_count) {
    sqlite3_stmt *stmt = c->cand_stmt;
    mb_candidate_t *cands = NULL;
    int count = 0;
    int cap = 0;
    int rc;

    *out = NULL;
    *out_count = 0;

    sqlite3_reset(stmt);
    sqlite3_bind_int64(stmt, 1, lo);
    sqlite3_bind_int64(stmt, 2, hi);
    sqlite3_bind_int(stmt, 3, MB_CAT_MOVIE);
    sqlite3_bind_int64(stmt, 4, c->target);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (count == cap) {
            int new_cap = cap > 0 ? cap * 2 : 64;
            mb_candidate_t *grown = realloc(cands, (size_t)new_cap * sizeof(*grown));
            if (!grown) {
                sqlite3_reset(stmt);
                mb_free_candidates(cands, count);
                log_msg(LOG_ERROR, "movie-backfill: out of memory loading window (%lld, %lld]",
                        lo, hi);
                return -1;
            }
            cands = grown;
            cap = new_cap;
        }
        cands[count].name = mb_dup_column_text(stmt, 1);
        if (!cands[count].name) {
            sqlite3_reset(stmt);
            mb_free_candidates(cands, count);
            log_msg(LOG_ERROR, "movie-backfill: out of memory loading window (%lld, %lld]", lo,
                    hi);
            return -1;
        }
        cands[count].id = sqlite3_column_int64(stmt, 0);
        count++;
    }
    sqlite3_reset(stmt);
    if (rc != SQLITE_DONE) {
        mb_free_candidates(cands, count);
        log_msg(LOG_ERROR, "movie-backfill: window (%lld, %lld] failed (%d: %s)", lo, hi, rc,
                sqlite3_errstr(rc));
        return -1;
    }
    *out = cands;
    *out_count = count;
    return 0;
}

/* Label one candidate and queue its update. Returns 0 when the candidate was
 * handled (labelled or counted as failed), -1 on a memory error. */
static int mb_classify_one(mb_ctx_t *c, const mb_candidate_t *cand) {
    uint16_t mask = MOVIE_KIND_NONE;
    int rc;

    rc = mb_classify(cand->name, &mask);
    /* `--dump-masks` on the real pass: the same "<mask> <hex(name)>" line the
     * bench prints, and the same Stage-9 rule - a row whose classify failed is
     * skipped, because a printed 0 would be indistinguishable from a genuine
     * `none` verdict. */
    if (c->dump_masks && mb_should_print_mask(rc)) {
        mb_print_mask_line(mask, cand->name);
    }
    if (rc != 0) {
        /* No verdict (feature off, model unusable, encode failure): leave the
         * stored columns exactly as they are rather than invent a label. */
        c->prog->rows_failed++;
        return 0;
    }
    if (c->batch_count >= c->batch_rows) {
        log_msg(LOG_ERROR, "movie-backfill: batch overflow at torrent %lld", cand->id);
        return -1;
    }
    c->batch[c->batch_count].torrent_id = cand->id;
    c->batch[c->batch_count].mask = mask;
    c->batch_count++;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Thermal back-off
 * ------------------------------------------------------------------------ */

/* Block while the hottest zone is at/above the resume temperature.
 *
 * Called between batches, never inside one, so the pass's own locking contract
 * is untouched. The pause is counted once per episode (not once per slice) and
 * reported to the movie-kind module, whose counter /stats renders. Returns 1
 * when the pass must stop (a stop request arrived mid-pause), 0 otherwise. */
static int mb_thermal_gate(mb_ctx_t *c) {
    int max_c;
    int paused = 0;

    if (c->thermal_backoff_c <= 0) {
        return 0;
    }
    max_c = mb_thermal_max_c();
    if (max_c < c->thermal_backoff_c) {
        return 0;
    }

    while (max_c >= c->thermal_resume_c) {
        if (!paused) {
            paused = 1;
            c->prog->thermal_pauses++;
            mb_note_thermal_pause();
            log_msg(LOG_WARN,
                    "movie-backfill: thermal pause (%d C >= %d C); resuming below %d C",
                    max_c, c->thermal_backoff_c, c->thermal_resume_c);
        }
        if (mb_stop_requested) {
            log_msg(LOG_INFO, "movie-backfill: stop requested during a thermal pause");
            return 1;
        }
        mb_sleep(MB_THERMAL_SLEEP_SLICE_MS);
        max_c = mb_thermal_max_c();
        if (max_c < 0) {
            /* The platform stopped reporting temperatures: do not stall the
             * pass on a reader that cannot answer. */
            break;
        }
    }
    if (paused) {
        log_msg(LOG_INFO, "movie-backfill: thermal pause over (hottest zone %d C)", max_c);
    }
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Batch transactions
 * ------------------------------------------------------------------------ */

/* Run `fn` with db->mutex held, retrying on a busy/locked database. */
static int mb_run_locked(mb_ctx_t *c, mb_locked_fn_t fn, const char *what) {
    int attempt;

    for (attempt = 0; attempt < MB_BUSY_ATTEMPTS; attempt++) {
        int rc;
        uv_mutex_lock(&c->db->mutex);
        rc = fn(c);
        uv_mutex_unlock(&c->db->mutex);
        if (rc == SQLITE_OK) {
            return 0;
        }
        if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
            log_msg(LOG_ERROR, "movie-backfill: %s failed (%d: %s)", what, rc,
                    sqlite3_errstr(rc));
            return -1;
        }
        log_msg(LOG_WARN, "movie-backfill: %s busy (attempt %d/%d), retrying", what, attempt + 1,
                MB_BUSY_ATTEMPTS);
        mb_sleep_real(MB_BUSY_SLEEP_MS);
    }
    log_msg(LOG_ERROR, "movie-backfill: %s still busy after %d attempts", what, MB_BUSY_ATTEMPTS);
    return -1;
}

/* One attempt at the pending batch. Called with db->mutex held.
 *
 * The UPDATE names only the two movie-kind columns: clf_ver, category, adult
 * and the tparse columns of the row are untouched by construction, and no row
 * is ever inserted (a candidate always has a class row - the predicate joins
 * it). */
static int mb_flush_locked(mb_ctx_t *c) {
    sqlite3 *db = c->db->db;
    sqlite3_stmt *stmt = c->update_stmt;
    char resume[64];
    int rc;
    int i;

    rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }

    for (i = 0; i < c->batch_count; i++) {
        const mb_pending_t *p = &c->batch[i];

        sqlite3_reset(stmt);
        sqlite3_bind_int(stmt, 1, (int)p->mask);
        /* The generation this pass resolved, never the model's own field: the
         * selection predicate and the stored value must be the same number. */
        sqlite3_bind_int64(stmt, 2, c->target);
        sqlite3_bind_int64(stmt, 3, p->torrent_id);
        rc = sqlite3_step(stmt);
        sqlite3_reset(stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            return rc;
        }
        /* A row that vanished between the window read and this batch is a
         * failure, not an update: nothing was labelled. */
        if (sqlite3_changes(db) > 0) {
            c->prog->rows_updated++;
        } else {
            c->prog->rows_failed++;
        }
    }

    /* Committed ids and the resume key are written by the same transaction, so
     * a crash can only lose a batch, never skip one. */
    snprintf(resume, sizeof resume, "%lld:%lld", c->target,
             c->batch[c->batch_count - 1].torrent_id);
    rc = mb_meta_put(c->meta_stmt, MOVIE_BACKFILL_RESUME_KEY, resume);
    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }

    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    return SQLITE_OK;
}

/* One attempt at the completion bookkeeping: publish the generation the whole
 * pass reached and tombstone the resume key so the next generation starts from
 * the beginning. Called with db->mutex held. */
static int mb_finish_locked(mb_ctx_t *c) {
    sqlite3 *db = c->db->db;
    char generation[32];
    int rc;

    rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }
    snprintf(generation, sizeof generation, "%lld", c->target);
    rc = mb_meta_put(c->meta_stmt, MOVIE_BACKFILL_VER_KEY, generation);
    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    rc = mb_meta_put(c->meta_stmt, MOVIE_BACKFILL_RESUME_KEY, "");
    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    return SQLITE_OK;
}

static int mb_flush(mb_ctx_t *c) {
    if (c->batch_count == 0) {
        return 0;
    }
    if (mb_run_locked(c, mb_flush_locked, "batch commit") != 0) {
        return -1;
    }
    c->prog->last_id = c->batch[c->batch_count - 1].torrent_id;
    c->batch_count = 0;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Statement lifecycle
 * ------------------------------------------------------------------------ */

static void mb_finalize_statements(mb_ctx_t *c) {
    sqlite3_finalize(c->hi_stmt);
    sqlite3_finalize(c->cand_stmt);
    sqlite3_finalize(c->update_stmt);
    sqlite3_finalize(c->meta_stmt);
    c->hi_stmt = NULL;
    c->cand_stmt = NULL;
    c->update_stmt = NULL;
    c->meta_stmt = NULL;
}

static int mb_prepare_statements(mb_ctx_t *c) {
    static const char *const sql_hi =
        "SELECT MAX(id) FROM (SELECT id FROM torrents WHERE id > ? ORDER BY id LIMIT ?)";
    static const char *const sql_cand =
        "SELECT t.id, t.name FROM torrents t "
        "JOIN torrent_class tc ON tc.torrent_id = t.id "
        "WHERE t.id > ? AND t.id <= ? AND tc.category = ? AND tc.adult = 0 AND "
        "(tc.movie_kind_clf_ver IS NULL OR tc.movie_kind_clf_ver < ?) "
        "ORDER BY t.id";
    static const char *const sql_update =
        "UPDATE torrent_class SET movie_kind_mask = ?, movie_kind_clf_ver = ? "
        "WHERE torrent_id = ?";
    static const char *const sql_meta =
        "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES (?, ?)";
    sqlite3 *db = c->db->db;
    int rc;

    rc = sqlite3_prepare_v2(db, sql_hi, -1, &c->hi_stmt, NULL);
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_cand, -1, &c->cand_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_update, -1, &c->update_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_meta, -1, &c->meta_stmt, NULL);
    }
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "movie-backfill: statement preparation failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        mb_finalize_statements(c);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Live progress snapshot
 * ------------------------------------------------------------------------ */

/* run() keeps its counters in a function-local struct, so without this channel
 * nothing could see a pass in flight. g_progress is the pollable copy - the
 * engine republishes it at every observable step (run start, each committed
 * batch, each window, each exit path) and any thread may read the last one
 * through movie_backfill_get_progress().
 *
 * Locking: g_progress_mutex is never held while db->mutex is held and never
 * held across classification, a sleep or I/O. */
static movie_backfill_progress_t g_progress;
static pthread_mutex_t g_progress_mutex = PTHREAD_MUTEX_INITIALIZER;

static void mb_report(const movie_backfill_progress_t *p, movie_backfill_progress_t *out) {
    pthread_mutex_lock(&g_progress_mutex);
    g_progress = *p;
    pthread_mutex_unlock(&g_progress_mutex);
    if (out) {
        *out = *p;
    }
}

void movie_backfill_get_progress(movie_backfill_progress_t *out) {
    if (!out) {
        return; /* nothing to report to; polling must never crash a caller */
    }
    pthread_mutex_lock(&g_progress_mutex);
    *out = g_progress;
    pthread_mutex_unlock(&g_progress_mutex);
}

/* ------------------------------------------------------------------------ *
 * Public API
 * ------------------------------------------------------------------------ */

int movie_backfill_run(database_t *db, const movie_backfill_opts_t *opts,
                       movie_backfill_progress_t *out) {
    movie_backfill_progress_t progress;
    mb_ctx_t ctx;
    long long target;
    long long start_id;
    long long rows_total;
    long long lo;
    int dry_run;
    int stopped = 0;
    int result = -1;

    memset(&progress, 0, sizeof(progress));
    progress.state = MOVIE_BACKFILL_IDLE;
    memset(&ctx, 0, sizeof(ctx));

    if (!db || !db->db) {
        log_msg(LOG_ERROR, "movie-backfill: no database connection");
        progress.state = MOVIE_BACKFILL_STOPPED;
        mb_report(&progress, out);
        return -1;
    }

    /* The caller owns movie_kind_init(); the engine only reads what it
     * resolved. A build without MK_ENABLED, a config with movie_kind_enabled=0
     * and a phone without the verified v9 assets all land here.
     *
     * A dry run is exempt on purpose: it only counts rows, so
     * `--movie-kind-backfill <db> --dry-run` answers on a host build too. With no
     * manifest there is no target generation, and the predicate then counts the
     * unlabelled movie rows only - the honest answer for "a pass that does not
     * know its generation". */
    mb_stop_requested = 0;
    dry_run = (opts && opts->dry_run) ? 1 : 0;
    target = (opts && opts->target_clf_ver > 0) ? opts->target_clf_ver : mb_generation();
    if (!dry_run && (!mb_enabled() || target <= 0)) {
        log_msg(LOG_WARN,
                "movie-backfill: disabled (enabled=%d, target generation=%lld); "
                "nothing written",
                mb_enabled(), target);
        progress.state = MOVIE_BACKFILL_DISABLED;
        progress.target_clf_ver = target;
        mb_log_summary(&progress);
        mb_report(&progress, out);
        return 1;
    }

    ctx.db = db;
    ctx.target = target;
    ctx.chunk_ids = (opts && opts->chunk_ids > 0) ? opts->chunk_ids : MB_CHUNK_IDS_DEFAULT;
    ctx.batch_rows = (opts && opts->batch_rows > 0) ? opts->batch_rows : MB_BATCH_ROWS_DEFAULT;
    ctx.limit_rows = (opts && opts->limit_rows > 0) ? opts->limit_rows : 0;
    ctx.thermal_backoff_c = (opts && opts->thermal_backoff_c > 0) ? opts->thermal_backoff_c : 0;
    ctx.thermal_resume_c = ctx.thermal_backoff_c > MB_THERMAL_RESUME_MARGIN_C
                               ? ctx.thermal_backoff_c - MB_THERMAL_RESUME_MARGIN_C
                               : 0;
    ctx.dump_masks = (opts && opts->dump_masks) ? 1 : 0;
    ctx.prog = &progress;

    progress.target_clf_ver = target;
    sqlite3_busy_timeout(db->db, MB_BUSY_TIMEOUT_MS);

    start_id = mb_resume_last_id(db, target);
    progress.last_id = start_id;
    if (mb_count_candidates(db, start_id, target, &rows_total) != 0) {
        progress.state = MOVIE_BACKFILL_STOPPED;
        mb_log_summary(&progress);
        mb_report(&progress, out);
        return -1;
    }
    progress.rows_total = rows_total;

    if (dry_run) {
        log_msg(LOG_INFO,
                "movie-backfill: dry run, %lld candidate(s) at generation %lld, nothing written",
                rows_total, target);
        progress.state = MOVIE_BACKFILL_DONE;
        mb_log_summary(&progress);
        mb_report(&progress, out);
        return 0;
    }

    /* The pass is live from here on: publish RUNNING with the counters it just
     * resolved, so a poller sees the work start before the first batch. A dry
     * run is excluded on purpose - it never processes a row. */
    progress.state = MOVIE_BACKFILL_RUNNING;
    mb_report(&progress, NULL);

    ctx.batch = malloc((size_t)ctx.batch_rows * sizeof(*ctx.batch));
    if (!ctx.batch) {
        log_msg(LOG_ERROR, "movie-backfill: out of memory for the batch buffer");
        progress.state = MOVIE_BACKFILL_STOPPED;
        goto cleanup;
    }
    if (mb_prepare_statements(&ctx) != 0) {
        progress.state = MOVIE_BACKFILL_STOPPED;
        goto cleanup;
    }

    lo = start_id;
    for (;;) {
        long long hi = 0;
        mb_candidate_t *cands = NULL;
        int num_cands = 0;
        int i;

        if (mb_stop_requested) {
            stopped = 1;
            break;
        }
        if (mb_next_hi(&ctx, lo, &hi) != 0) {
            progress.state = MOVIE_BACKFILL_STOPPED;
            goto cleanup;
        }
        if (hi <= 0) {
            break; /* nothing left to visit */
        }
        if (mb_load_window(&ctx, lo, hi, &cands, &num_cands) != 0) {
            progress.state = MOVIE_BACKFILL_STOPPED;
            goto cleanup;
        }

        for (i = 0; i < num_cands; i++) {
            if (ctx.limit_rows > 0 && progress.rows_done >= ctx.limit_rows) {
                stopped = 1;
                break;
            }
            if (mb_classify_one(&ctx, &cands[i]) != 0) {
                mb_free_candidates(cands, num_cands);
                progress.state = MOVIE_BACKFILL_STOPPED;
                goto cleanup;
            }
            progress.rows_done++;
            if (ctx.batch_count >= ctx.batch_rows) {
                if (mb_flush(&ctx) != 0) {
                    mb_free_candidates(cands, num_cands);
                    progress.state = MOVIE_BACKFILL_STOPPED;
                    goto cleanup;
                }
                /* One committed batch: republish so rows_done/rows_updated/
                 * rows_failed/last_id move for a poller while the pass walks. */
                mb_report(&progress, NULL);
                /* The batch in flight always commits before a stop is seen. */
                if (mb_stop_requested) {
                    stopped = 1;
                    break;
                }
                if (ctx.limit_rows > 0 && progress.rows_done >= ctx.limit_rows) {
                    stopped = 1;
                    break;
                }
                /* Between batches: give the device a chance to cool down. */
                if (mb_thermal_gate(&ctx) != 0) {
                    stopped = 1;
                    break;
                }
            }
        }
        mb_free_candidates(cands, num_cands);

        if (ctx.batch_count > 0 && mb_flush(&ctx) != 0) {
            progress.state = MOVIE_BACKFILL_STOPPED;
            goto cleanup;
        }

        log_msg(LOG_INFO,
                "movie-backfill: %lld/%lld updated=%lld skipped=%lld failed=%lld "
                "thermal_pauses=%lld last_id=%lld",
                progress.rows_done, progress.rows_total, progress.rows_updated,
                progress.rows_skipped, progress.rows_failed, progress.thermal_pauses,
                progress.last_id);
        /* End of a window: publish the counters the log line just reported. */
        mb_report(&progress, NULL);

        if (stopped) {
            break;
        }
        lo = hi;
    }

    if (stopped) {
        if (progress.rows_total > progress.rows_done) {
            progress.rows_skipped = progress.rows_total - progress.rows_done;
        }
        progress.state = MOVIE_BACKFILL_STOPPED;
        log_msg(LOG_INFO, "movie-backfill: stopped at %lld row(s), resuming after id %lld",
                progress.rows_done, progress.last_id);
        result = 0;
    } else if (mb_run_locked(&ctx, mb_finish_locked, "completion metadata") != 0) {
        progress.state = MOVIE_BACKFILL_STOPPED;
        goto cleanup;
    } else {
        progress.state = MOVIE_BACKFILL_DONE;
        result = 0;
    }

cleanup:
    mb_finalize_statements(&ctx);
    free(ctx.batch);
    ctx.batch = NULL;
    mb_log_summary(&progress);
    mb_report(&progress, out);
    return result;
}

int movie_backfill_needed(database_t *db, long long target_clf_ver, long long *rows_left) {
    sqlite3_stmt *stmt = NULL;
    long long target;
    long long meta_ver = 0;
    long long left = 0;

    if (rows_left) {
        *rows_left = 0;
    }
    if (!db || !db->db || !mb_enabled()) {
        return 0;
    }
    target = (target_clf_ver > 0) ? target_clf_ver : mb_generation();
    if (target <= 0) {
        return 0;
    }

    /* A missing meta row means "no generation recorded yet". */
    if (sqlite3_prepare_v2(db->db, "SELECT value FROM classifier_meta WHERE key = ?", -1, &stmt,
                           NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, MOVIE_BACKFILL_VER_KEY, -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *value = sqlite3_column_text(stmt, 0);
            if (value) {
                meta_ver = strtoll((const char *)value, NULL, 10);
            }
        }
        sqlite3_reset(stmt);
        sqlite3_finalize(stmt);
    }
    if (mb_count_candidates(db, 0, target, &left) != 0) {
        left = 0;
    }
    if (rows_left) {
        *rows_left = left;
    }
    /* Both directions: the stored generation can be behind while every row is
     * already converted, because the meta write happens only at completion. */
    return (meta_ver < target || left > 0) ? 1 : 0;
}

void movie_backfill_request_stop(void) {
    mb_stop_requested = 1;
}

/* ------------------------------------------------------------------------ *
 * --movie-kind-bench
 * ------------------------------------------------------------------------ */

static double mb_now_seconds(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

/* Nearest-rank percentile over an ascending sample array (microseconds). */
static long long mb_percentile_us(const long long *sorted, long long n, int pct) {
    long long rank;

    if (n <= 0) {
        return 0;
    }
    if (pct <= 0) {
        return sorted[0];
    }
    if (pct >= 100) {
        return sorted[n - 1];
    }
    rank = (long long)(((double)pct * (double)n) / 100.0 + 0.999999);
    if (rank < 1) {
        rank = 1;
    }
    if (rank > n) {
        rank = n;
    }
    return sorted[rank - 1];
}

static int mb_cmp_ll(const void *a, const void *b) {
    long long x = *(const long long *)a;
    long long y = *(const long long *)b;

    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/* VmHWM from /proc/self/status, in KiB. That counter includes the file-backed
 * pages of the mmap'd GGUF, which is exactly what the G6 memory bar is about. */
static long long mb_vmhwm_kib(void) {
    FILE *f = fopen("/proc/self/status", "r");
    char line[256];
    long long kib = 0;

    if (!f) {
        return 0;
    }
    while (fgets(line, sizeof line, f)) {
        if (strncmp(line, "VmHWM:", 6) == 0) {
            if (sscanf(line + 6, "%lld", &kib) != 1) {
                kib = 0;
            }
            break;
        }
    }
    fclose(f);
    return kib;
}

/* Print "<mask> <hex(name)>" for one row: hex of the raw database bytes, so the
 * dump is comparable even for names that are not valid UTF-8. */
static void mb_print_mask_line(uint16_t mask, const char *name) {
    size_t len = strlen(name);
    size_t i;

    printf("%u ", (unsigned)mask);
    for (i = 0; i < len; i++) {
        printf("%02x", (unsigned)(unsigned char)name[i]);
    }
    putchar('\n');
}

/* Whether one row of the `--dump-masks` loop gets a mask line. A failed classify
 * call must never reach the dump: mask 0 is a genuine `none` verdict, so printing
 * the untouched 0 of a failed row would be indistinguishable from a real verdict
 * and would silently weaken the on-device spot check. The row is skipped instead
 * and the bench JSON `failures` counter stays the loud per-run signal. */
static int mb_should_print_mask(int classify_rc) {
    return classify_rc == 0;
}

#ifdef MOVIE_BACKFILL_TEST
/* Test-only entry point (see include/movie_backfill.h): the daemon build keeps
 * the decision private. */
int movie_backfill_test_should_print_mask(int classify_rc) {
    return mb_should_print_mask(classify_rc);
}
#endif /* MOVIE_BACKFILL_TEST */

int movie_backfill_bench(const char *db_path, int rows, int threads, int dump_masks) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    char **names = NULL;
    long long *samples = NULL;
    long long n = 0;
    long long samples_n = 0;
    long long failures = 0;
    long long generation;
    int max_temp_c = -1;
    int rc;
    int result = -1;
    double started;
    double elapsed;
    long long i;

    if (!db_path) {
        log_msg(LOG_ERROR, "movie-kind-bench: no database path");
        return -1;
    }
    if (rows <= 0) {
        rows = MB_BENCH_ROWS_DEFAULT;
    }
    if (rows > MB_BENCH_ROWS_MAX) {
        rows = MB_BENCH_ROWS_MAX;
    }
    if (threads < 1) {
        threads = 1;
    }
    if (threads > 8) {
        threads = 8; /* movie_kind_set_threads() clamps to the same range */
    }

    /* No verified manifest or no encoder in this build: say so loudly and print
     * nothing, so a caller can never mistake an empty run for a measurement. */
    if (!mb_enabled()) {
        log_msg(LOG_ERROR,
                "movie-kind-bench: the movie-kind module is disabled (no verified v9 "
                "manifest, or this build has no encoder); nothing measured");
        return 1;
    }

    /* Read-only by construction: the bench must write nothing, not even a
     * journal-mode pragma or the schema migration. */
    rc = sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "movie-kind-bench: cannot open %s read-only: %s", db_path,
                db ? sqlite3_errmsg(db) : sqlite3_errstr(rc));
        if (db) {
            sqlite3_close(db);
        }
        return -1;
    }
    sqlite3_busy_timeout(db, MB_BUSY_TIMEOUT_MS);

    rc = sqlite3_prepare_v2(db,
                            "SELECT t.name FROM torrents t "
                            "JOIN torrent_class tc ON tc.torrent_id = t.id "
                            "WHERE tc.category = ? AND tc.adult = 0 "
                            "ORDER BY t.id LIMIT ?",
                            -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "movie-kind-bench: candidate query failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        goto done;
    }
    sqlite3_bind_int(stmt, 1, MB_CAT_MOVIE);
    sqlite3_bind_int(stmt, 2, rows);
    names = calloc((size_t)rows, sizeof(*names));
    if (!names) {
        log_msg(LOG_ERROR, "movie-kind-bench: out of memory for the name list");
        goto done;
    }
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        names[n] = mb_dup_column_text(stmt, 0);
        if (!names[n]) {
            log_msg(LOG_ERROR, "movie-kind-bench: out of memory copying a name");
            goto done;
        }
        n++;
        if (n >= rows) {
            break;
        }
    }
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
        log_msg(LOG_ERROR, "movie-kind-bench: candidate walk failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        goto done;
    }
    sqlite3_finalize(stmt);
    stmt = NULL;
    sqlite3_close(db);
    db = NULL;

    if (n == 0) {
        log_msg(LOG_WARN, "movie-kind-bench: no movie+adult=0 rows in %s", db_path);
    }

    samples = malloc((size_t)MB_BENCH_SAMPLE_CAP * sizeof(*samples));
    if (!samples) {
        log_msg(LOG_ERROR, "movie-kind-bench: out of memory for the samples");
        goto done;
    }

    generation = mb_generation();
    log_msg(LOG_INFO,
            "movie-kind-bench: %lld name(s) at generation %lld, %d thread(s), %s",
            n, generation, threads, dump_masks ? "mask dump" : "sustained loop");

    /* Warm-up: the first call loads and verifies the 152.8 MiB backbone, which
     * is not a per-name cost and must not land in the median. It is not part of
     * the mask dump either - the dump loop below classifies every row again, so
     * the dump stays one line per row and in row order. */
    if (n > 0) {
        uint16_t warm_mask = MOVIE_KIND_NONE;
        (void)mb_classify(names[0], &warm_mask);
    }

    started = mb_now_seconds();
    for (;;) {
        for (i = 0; i < n; i++) {
            uint16_t mask = MOVIE_KIND_NONE;
            double t0 = mb_now_seconds();
            int classify_rc = mb_classify(names[i], &mask);
            double t1 = mb_now_seconds();
            int temp_c;

            if (classify_rc == 0) {
                if (samples_n < MB_BENCH_SAMPLE_CAP) {
                    samples[samples_n] = (long long)((t1 - t0) * 1000000.0 + 0.5);
                }
                samples_n++;
            } else {
                failures++;
            }
            if (dump_masks && mb_should_print_mask(classify_rc)) {
                mb_print_mask_line(mask, names[i]);
            }
            temp_c = mb_thermal_max_c();
            if (temp_c > max_temp_c) {
                max_temp_c = temp_c;
            }
        }
        elapsed = mb_now_seconds() - started;
        if (dump_masks || elapsed >= MB_BENCH_MIN_SECONDS || samples_n <= 0) {
            break;
        }
        if (n == 0) {
            break;
        }
    }
    elapsed = mb_now_seconds() - started;

    /* Only the retained samples are sorted; `samples` reports how many names
     * were actually measured, so a run longer than the sample cap is honest
     * about the difference. */
    {
        long long kept = samples_n < MB_BENCH_SAMPLE_CAP ? samples_n : MB_BENCH_SAMPLE_CAP;
        long long median_us = 0;
        long long p95_us = 0;
        long long vmhwm_kib = mb_vmhwm_kib();

        if (kept > 0) {
            qsort(samples, (size_t)kept, sizeof(*samples), mb_cmp_ll);
            median_us = mb_percentile_us(samples, kept, 50);
            p95_us = mb_percentile_us(samples, kept, 95);
        }
        printf("{\"median_ms\":%.3f,\"p95_ms\":%.3f,\"vmhwm_kib\":%lld,"
               "\"max_cpu_temp_c\":%d,\"threads\":%d,\"rows\":%lld,\"samples\":%lld,"
               "\"failures\":%lld,\"generation\":%lld,\"elapsed_s\":%.1f,"
               "\"dump_masks\":%d}\n",
               (double)median_us / 1000.0, (double)p95_us / 1000.0, vmhwm_kib, max_temp_c,
               threads, n, samples_n, failures, generation, elapsed, dump_masks ? 1 : 0);
        fflush(stdout);
    }
    result = 0;

done:
    sqlite3_finalize(stmt);
    if (db) {
        sqlite3_close(db);
    }
    if (names) {
        for (i = 0; i < n; i++) {
            free(names[i]);
        }
        free(names);
    }
    free(samples);
    return result;
}

/* ------------------------------------------------------------------------ *
 * Poisoned-row reset (--movie-kind-backfill <db> --reset)
 *
 * See include/movie_backfill.h: the one-shot that undoes a pass that labelled
 * rows with a broken encoder. It writes the two movie columns, the resume-key
 * tombstone and the completion-key delete in ONE transaction, so a busy writer
 * can only make it retry - never leave the database half reset.
 * ------------------------------------------------------------------------ */

/* One attempt at the whole reset. Returns a SQLite result code. */
static int mb_reset_once(sqlite3 *db, long long *rows_reset) {
    char update_sql[256];
    sqlite3_stmt *stmt = NULL;
    int rc;

    snprintf(update_sql, sizeof update_sql,
             "UPDATE torrent_class SET movie_kind_mask = NULL, movie_kind_clf_ver = NULL "
             "WHERE movie_kind_clf_ver >= %d",
             MOVIE_BACKFILL_RESET_MIN_GENERATION);

    rc = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }
    rc = sqlite3_exec(db, update_sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    *rows_reset = (long long)sqlite3_changes(db);

    /* Tombstone the resume key. The poisoned pass left "<generation>:<id>", and
     * a restarted pass honours that id - it would start after it and silently
     * skip exactly the rows this reset just cleared. */
    rc = sqlite3_prepare_v2(db,
                            "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES (?, ?)",
                            -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    sqlite3_bind_text(stmt, 1, MOVIE_BACKFILL_RESUME_KEY, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, "", -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }

    /* Defensive: the poisoned pass never completed, so the completion key should
     * be absent. Delete it anyway so a stale generation cannot make the engine
     * believe the whole pass already sits at that generation. */
    rc = sqlite3_exec(db,
                      "DELETE FROM classifier_meta WHERE key = '" MOVIE_BACKFILL_VER_KEY "'",
                      NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }

    return sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
}

int movie_backfill_reset(const char *db_path, long long *rows_reset) {
    sqlite3 *db = NULL;
    long long cleared = 0;
    int attempt;
    int rc;

    if (rows_reset) {
        *rows_reset = 0;
    }
    if (!db_path) {
        log_msg(LOG_ERROR, "movie-kind-reset: no database path");
        return -1;
    }

    rc = sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "movie-kind-reset: cannot open %s: %s", db_path,
                db ? sqlite3_errmsg(db) : sqlite3_errstr(rc));
        if (db) {
            sqlite3_close(db);
        }
        return -1;
    }
    sqlite3_busy_timeout(db, MB_BUSY_TIMEOUT_MS);

    /* Loud on stdout *and* in the log: run after a good pass this wipes correct
     * labels, so it must never look like a routine maintenance step. */
    printf("movie-kind-reset: WARNING: destructive reset of movie_kind_mask/movie_kind_clf_ver "
           "for every row at generation >= %d.\n"
           "movie-kind-reset: WARNING: run only while the movie-kind pass is stopped.\n",
           MOVIE_BACKFILL_RESET_MIN_GENERATION);
    fflush(stdout);
    log_msg(LOG_WARN,
            "movie-kind-reset: WARNING: destructive reset of movie_kind_mask/movie_kind_clf_ver "
            "for every row at generation >= %d; run only while the pass is stopped",
            MOVIE_BACKFILL_RESET_MIN_GENERATION);

    /* The engine's busy-retry discipline: sqlite3_busy_timeout() already waits
     * per statement, these attempts cover a writer that holds the lock longer. */
    for (attempt = 0; attempt < MB_BUSY_ATTEMPTS; attempt++) {
        cleared = 0;
        rc = mb_reset_once(db, &cleared);
        if (rc == SQLITE_OK) {
            break;
        }
        if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
            log_msg(LOG_ERROR, "movie-kind-reset: reset of %s failed (%d: %s)", db_path, rc,
                    sqlite3_errmsg(db));
            sqlite3_close(db);
            return -1;
        }
        log_msg(LOG_WARN, "movie-kind-reset: database busy (attempt %d/%d), retrying",
                attempt + 1, MB_BUSY_ATTEMPTS);
        mb_sleep_real(MB_BUSY_SLEEP_MS);
    }
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "movie-kind-reset: %s still busy after %d attempts", db_path,
                MB_BUSY_ATTEMPTS);
        sqlite3_close(db);
        return -1;
    }
    sqlite3_close(db);

    /* The count is the point of the one-shot, so it goes to stdout as well. No
     * row count is assumed: whatever the predicate matched is what is printed. */
    printf("movie-kind-reset: cleared %lld row(s) at generation >= %d; resume key tombstoned, "
           "completion key cleared\n",
           cleared, MOVIE_BACKFILL_RESET_MIN_GENERATION);
    fflush(stdout);
    log_msg(LOG_INFO,
            "movie-kind-reset: cleared %lld row(s) at generation >= %d; resume key tombstoned, "
            "completion key cleared",
            cleared, MOVIE_BACKFILL_RESET_MIN_GENERATION);

    if (rows_reset) {
        *rows_reset = cleared;
    }
    return 0;
}
