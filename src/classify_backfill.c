#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE  /* For usleep */
#endif

#include "classify_backfill.h"
#include "classifier.h"
#include "dht_crawler.h"

#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------------ *
 * Tunables
 * ------------------------------------------------------------------------ */

#define CB_CHUNK_IDS_DEFAULT 10000
#define CB_BATCH_ROWS_DEFAULT 500

/* classifier_meta key holding "<generation>:<last committed torrent id>".
 * Generation-scoped on purpose: a value left behind by an older generation can
 * never make a newer pass skip rows. A completed pass tombstones the value. */
#define CB_RESUME_KEY "backfill_last_id"

/* A busy writer is retried; the upserts are idempotent, so replaying a whole
 * batch is safe. sqlite3_busy_timeout() already waits per statement; the
 * retries cover a writer that holds the lock longer than that. */
#define CB_BUSY_TIMEOUT_MS 30000
#define CB_BUSY_ATTEMPTS 5
#define CB_BUSY_SLEEP_US 50000

/* Cooperative cancel: set by classify_backfill_request_stop(), cleared at the
 * start of every pass, read between batches (never inside one). */
static volatile sig_atomic_t cb_stop_requested = 0;

/* ------------------------------------------------------------------------ *
 * Types
 * ------------------------------------------------------------------------ */

/* One candidate of the current id window. The window is read into memory
 * before the first write of the window: the LEFT JOIN that produced these rows
 * must not still be stepping while the batch upserts torrent_class. */
typedef struct {
    long long id;
    char *name;            /* owned copy of torrents.name */
    long long size_bytes;
    int had_class_row;     /* 1 when a torrent_class row already existed */
} cb_candidate_t;

/* One classified row waiting for the next batch transaction. */
typedef struct {
    long long torrent_id;
    int had_class_row;
    classification_t cls;
} cb_pending_t;

typedef struct {
    database_t *db;
    long long target;
    int chunk_ids;
    int batch_rows;
    long long limit_rows;
    sqlite3_stmt *hi_stmt;
    sqlite3_stmt *cand_stmt;
    sqlite3_stmt *files_stmt;
    sqlite3_stmt *upsert_stmt;
    sqlite3_stmt *meta_stmt;
    cb_pending_t *batch;
    int batch_count;
    classify_backfill_progress_t *prog;
} cb_ctx_t;

/* A batch body that must run with db->mutex held and returns a SQLite code. */
typedef int (*cb_locked_fn_t)(cb_ctx_t *c);

/* ------------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------------ */

const char *classify_backfill_state_name(int state) {
    switch (state) {
    case CLASSIFY_BACKFILL_IDLE:
        return "idle";
    case CLASSIFY_BACKFILL_RUNNING:
        return "running";
    case CLASSIFY_BACKFILL_DONE:
        return "done";
    case CLASSIFY_BACKFILL_FAILED:
        return "failed";
    case CLASSIFY_BACKFILL_DISABLED:
        return "disabled";
    default:
        return "unknown";
    }
}

/* The final summary line (exit-criteria 4 and 5 grep it). */
static void cb_log_summary(const classify_backfill_progress_t *p) {
    log_msg(LOG_INFO,
            "classify-backfill: done state=%s total=%lld done=%lld updated=%lld "
            "inserted=%lld skipped=%lld failed=%lld",
            classify_backfill_state_name(p->state), p->rows_total, p->rows_done,
            p->rows_updated, p->rows_inserted, p->rows_skipped, p->rows_failed);
}

/* Copy a TEXT column byte-exactly. The snapshot has filenames that are not
 * valid UTF-8; re-encoding them would change what tparse sees on the predict
 * path, so the bytes are passed through untouched. */
static char *cb_dup_column_text(sqlite3_stmt *stmt, int col) {
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

static void cb_free_files(file_info_t *files, int num_files) {
    int i;
    if (!files) {
        return;
    }
    for (i = 0; i < num_files; i++) {
        free(files[i].path);
    }
    free(files);
}

static void cb_free_candidates(cb_candidate_t *cands, int count) {
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
static int cb_meta_put(sqlite3_stmt *stmt, const char *key, const char *value) {
    int rc;

    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    sqlite3_reset(stmt);
    return rc;
}

/* ------------------------------------------------------------------------ *
 * Read helpers (no mutex: the pass reads on the shared handle and only takes
 * db->mutex around each BEGIN ... COMMIT batch)
 * ------------------------------------------------------------------------ */

/* Resume point for this generation, or 0 when there is none to honour. */
static long long cb_resume_last_id(database_t *db, long long target) {
    sqlite3_stmt *stmt = NULL;
    long long last_id = 0;

    if (sqlite3_prepare_v2(db->db,
                           "SELECT value FROM classifier_meta WHERE key = ?",
                           -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_DEBUG, "classify-backfill: resume key unavailable: %s",
                sqlite3_errmsg(db->db));
        return 0;
    }
    sqlite3_bind_text(stmt, 1, CB_RESUME_KEY, -1, SQLITE_STATIC);
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

/* Number of candidates a pass starting after `start_id` would visit: class row
 * missing, or written by an older generation. */
static int cb_count_candidates(database_t *db, long long start_id, long long target,
                               long long *count_out) {
    sqlite3_stmt *stmt = NULL;
    long long count = 0;
    int rc;

    rc = sqlite3_prepare_v2(db->db,
                            "SELECT COUNT(*) FROM torrents t "
                            "LEFT JOIN torrent_class tc ON tc.torrent_id = t.id "
                            "WHERE t.id > ? AND "
                            "(tc.torrent_id IS NULL OR tc.clf_ver < ?)",
                            -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "classify-backfill: candidate count failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        return -1;
    }
    sqlite3_bind_int64(stmt, 1, start_id);
    sqlite3_bind_int64(stmt, 2, target);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        log_msg(LOG_ERROR, "classify-backfill: candidate count step failed (%d: %s)", rc,
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
static int cb_next_hi(cb_ctx_t *c, long long lo, long long *hi_out) {
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
        log_msg(LOG_ERROR, "classify-backfill: chunk bound failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        return -1;
    }
    sqlite3_reset(stmt);
    return 0;
}

/* Rebuild one torrent's full file paths: path_prefixes.prefix plus the leaf
 * filename, byte-exact and in torrent_files rowid order (not file_index, which
 * wraps past 32767). Returns 0 with *out_files NULL when the torrent has no
 * file rows — those classify on the name alone and must not be skipped. */
static int cb_load_files(cb_ctx_t *c, long long torrent_id, file_info_t **out_files,
                         int *out_num) {
    sqlite3_stmt *stmt = c->files_stmt;
    file_info_t *files = NULL;
    int num = 0;
    int cap = 0;
    int rc;

    *out_files = NULL;
    *out_num = 0;

    sqlite3_reset(stmt);
    sqlite3_bind_int64(stmt, 1, torrent_id);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (num == cap) {
            int new_cap = cap > 0 ? cap * 2 : 16;
            file_info_t *grown = realloc(files, (size_t)new_cap * sizeof(*grown));
            if (!grown) {
                sqlite3_reset(stmt);
                cb_free_files(files, num);
                log_msg(LOG_ERROR, "classify-backfill: out of memory loading files of %lld",
                        torrent_id);
                return -1;
            }
            files = grown;
            cap = new_cap;
        }
        files[num].path = cb_dup_column_text(stmt, 0);
        if (!files[num].path) {
            sqlite3_reset(stmt);
            cb_free_files(files, num);
            log_msg(LOG_ERROR, "classify-backfill: out of memory loading files of %lld",
                    torrent_id);
            return -1;
        }
        files[num].size_bytes = sqlite3_column_int64(stmt, 1);
        files[num].file_index = (int16_t)num; /* ignored on the predict path */
        num++;
    }
    sqlite3_reset(stmt);
    if (rc != SQLITE_DONE) {
        cb_free_files(files, num);
        log_msg(LOG_ERROR, "classify-backfill: file load of %lld failed (%d: %s)", torrent_id,
                rc, sqlite3_errstr(rc));
        return -1;
    }
    *out_files = files;
    *out_num = num;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Batch transactions
 * ------------------------------------------------------------------------ */

/* Run `fn` with db->mutex held, retrying on a busy/locked database. */
static int cb_run_locked(cb_ctx_t *c, cb_locked_fn_t fn, const char *what) {
    int attempt;

    for (attempt = 0; attempt < CB_BUSY_ATTEMPTS; attempt++) {
        int rc;
        uv_mutex_lock(&c->db->mutex);
        rc = fn(c);
        uv_mutex_unlock(&c->db->mutex);
        if (rc == SQLITE_OK) {
            return 0;
        }
        if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
            log_msg(LOG_ERROR, "classify-backfill: %s failed (%d: %s)", what, rc,
                    sqlite3_errstr(rc));
            return -1;
        }
        log_msg(LOG_WARN, "classify-backfill: %s busy (attempt %d/%d), retrying", what,
                attempt + 1, CB_BUSY_ATTEMPTS);
        usleep(CB_BUSY_SLEEP_US);
    }
    log_msg(LOG_ERROR, "classify-backfill: %s still busy after %d attempts", what,
            CB_BUSY_ATTEMPTS);
    return -1;
}

/* One attempt at the pending batch. Called with db->mutex held. */
static int cb_flush_locked(cb_ctx_t *c) {
    sqlite3 *db = c->db->db;
    sqlite3_stmt *stmt = c->upsert_stmt;
    char resume[64];
    int rc;
    int i;

    rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }

    for (i = 0; i < c->batch_count; i++) {
        const cb_pending_t *p = &c->batch[i];
        const classification_t *cls = &p->cls;

        sqlite3_reset(stmt);
        sqlite3_bind_int64(stmt, 1, p->torrent_id);
        sqlite3_bind_int(stmt, 2, cls->category);
        if (cls->tv_kind > 0) {
            sqlite3_bind_int(stmt, 3, cls->tv_kind);
        } else {
            sqlite3_bind_null(stmt, 3); /* NULL unless tv/anime */
        }
        sqlite3_bind_int(stmt, 4, cls->adult);
        sqlite3_bind_double(stmt, 5, cls->adult_p);
        sqlite3_bind_int(stmt, 6, cls->season_lo);
        sqlite3_bind_int(stmt, 7, cls->season_hi);
        sqlite3_bind_int(stmt, 8, cls->episode_lo);
        sqlite3_bind_int(stmt, 9, cls->episode_hi);
        sqlite3_bind_int(stmt, 10, cls->resolution);
        sqlite3_bind_double(stmt, 11, cls->confidence);
        /* The generation this pass resolved, never the model's own field: the
         * selection predicate and the stored value must be the same number. */
        sqlite3_bind_int64(stmt, 12, c->target);
        rc = sqlite3_step(stmt);
        sqlite3_reset(stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            return rc;
        }
    }

    /* Committed ids and the resume key are written by the same transaction, so
     * a crash can only lose a batch, never skip one. */
    snprintf(resume, sizeof(resume), "%lld:%lld", c->target,
             c->batch[c->batch_count - 1].torrent_id);
    rc = cb_meta_put(c->meta_stmt, CB_RESUME_KEY, resume);
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

/* One attempt at the completion bookkeeping: publish the generation's metadata
 * (clf_ver, the three model sha256s, the effective adult threshold) and
 * tombstone the resume key so the next generation starts from the beginning.
 * Called with db->mutex held. */
static int cb_finish_locked(cb_ctx_t *c) {
    sqlite3 *db = c->db->db;
    int rc;

    rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }
    classifier_write_meta(db);
    rc = cb_meta_put(c->meta_stmt, CB_RESUME_KEY, "");
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

static int cb_flush(cb_ctx_t *c) {
    int i;

    if (c->batch_count == 0) {
        return 0;
    }
    if (cb_run_locked(c, cb_flush_locked, "batch commit") != 0) {
        return -1;
    }
    for (i = 0; i < c->batch_count; i++) {
        if (c->batch[i].had_class_row) {
            c->prog->rows_updated++;
        } else {
            c->prog->rows_inserted++;
        }
    }
    c->prog->last_id = c->batch[c->batch_count - 1].torrent_id;
    c->batch_count = 0;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Window walk
 * ------------------------------------------------------------------------ */

/* Read the candidates of one id window (lo, hi] into memory. */
static int cb_load_window(cb_ctx_t *c, long long lo, long long hi, cb_candidate_t **out,
                          int *out_count) {
    sqlite3_stmt *stmt = c->cand_stmt;
    cb_candidate_t *cands = NULL;
    int count = 0;
    int cap = 0;
    int rc;

    *out = NULL;
    *out_count = 0;

    sqlite3_reset(stmt);
    sqlite3_bind_int64(stmt, 1, lo);
    sqlite3_bind_int64(stmt, 2, hi);
    sqlite3_bind_int64(stmt, 3, c->target);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (count == cap) {
            int new_cap = cap > 0 ? cap * 2 : 64;
            cb_candidate_t *grown = realloc(cands, (size_t)new_cap * sizeof(*grown));
            if (!grown) {
                sqlite3_reset(stmt);
                cb_free_candidates(cands, count);
                log_msg(LOG_ERROR, "classify-backfill: out of memory loading window (%lld, %lld]",
                        lo, hi);
                return -1;
            }
            cands = grown;
            cap = new_cap;
        }
        cands[count].name = cb_dup_column_text(stmt, 1);
        if (!cands[count].name) {
            sqlite3_reset(stmt);
            cb_free_candidates(cands, count);
            log_msg(LOG_ERROR, "classify-backfill: out of memory loading window (%lld, %lld]",
                    lo, hi);
            return -1;
        }
        cands[count].id = sqlite3_column_int64(stmt, 0);
        cands[count].size_bytes = sqlite3_column_int64(stmt, 2);
        cands[count].had_class_row = (sqlite3_column_type(stmt, 3) != SQLITE_NULL);
        count++;
    }
    sqlite3_reset(stmt);
    if (rc != SQLITE_DONE) {
        cb_free_candidates(cands, count);
        log_msg(LOG_ERROR, "classify-backfill: window (%lld, %lld] failed (%d: %s)", lo, hi, rc,
                sqlite3_errstr(rc));
        return -1;
    }
    *out = cands;
    *out_count = count;
    return 0;
}

/* Classify one candidate and queue its upsert. Returns 0 when the candidate was
 * handled (classified or counted as failed), -1 on a SQL/memory error. */
static int cb_classify_one(cb_ctx_t *c, const cb_candidate_t *cand) {
    file_info_t *files = NULL;
    torrent_metadata_t meta;
    classification_t cls;
    cb_pending_t *slot;
    int num_files = 0;
    int rc;

    if (cb_load_files(c, cand->id, &files, &num_files) != 0) {
        return -1;
    }

    memset(&meta, 0, sizeof(meta));
    meta.name = cand->name;
    meta.size_bytes = cand->size_bytes;
    meta.files = files;
    meta.num_files = num_files;

    memset(&cls, 0, sizeof(cls));
    rc = classifier_classify(&meta, &cls);
    cb_free_files(files, num_files);

    if (rc != 0) {
        /* No verdict (parser-only fallback or no models): leave the stored row
         * exactly as it is rather than downgrade a good one. */
        c->prog->rows_failed++;
        return 0;
    }

    slot = &c->batch[c->batch_count++];
    slot->torrent_id = cand->id;
    slot->had_class_row = cand->had_class_row;
    slot->cls = cls;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Statement lifecycle
 * ------------------------------------------------------------------------ */

static void cb_finalize_statements(cb_ctx_t *c) {
    sqlite3_finalize(c->hi_stmt);
    sqlite3_finalize(c->cand_stmt);
    sqlite3_finalize(c->files_stmt);
    sqlite3_finalize(c->upsert_stmt);
    sqlite3_finalize(c->meta_stmt);
    c->hi_stmt = NULL;
    c->cand_stmt = NULL;
    c->files_stmt = NULL;
    c->upsert_stmt = NULL;
    c->meta_stmt = NULL;
}

static int cb_prepare_statements(cb_ctx_t *c) {
    static const char *const sql_hi =
        "SELECT MAX(id) FROM (SELECT id FROM torrents WHERE id > ? ORDER BY id LIMIT ?)";
    static const char *const sql_cand =
        "SELECT t.id, t.name, t.size_bytes, tc.clf_ver FROM torrents t "
        "LEFT JOIN torrent_class tc ON tc.torrent_id = t.id "
        "WHERE t.id > ? AND t.id <= ? AND (tc.clf_ver IS NULL OR tc.clf_ver < ?) "
        "ORDER BY t.id";
    static const char *const sql_files =
        "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename), tf.size_bytes "
        "FROM torrent_files tf LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
        "WHERE tf.torrent_id = ? ORDER BY tf.id";
    static const char *const sql_upsert =
        "INSERT INTO torrent_class (torrent_id, category, tv_kind, adult, adult_p, "
        "season_lo, season_hi, episode_lo, episode_hi, resolution, confidence, clf_ver) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(torrent_id) DO UPDATE SET category=excluded.category, "
        "tv_kind=excluded.tv_kind, adult=excluded.adult, adult_p=excluded.adult_p, "
        "season_lo=excluded.season_lo, season_hi=excluded.season_hi, "
        "episode_lo=excluded.episode_lo, episode_hi=excluded.episode_hi, "
        "resolution=excluded.resolution, confidence=excluded.confidence, "
        "clf_ver=excluded.clf_ver";
    static const char *const sql_meta =
        "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES (?, ?)";
    sqlite3 *db = c->db->db;
    int rc;

    rc = sqlite3_prepare_v2(db, sql_hi, -1, &c->hi_stmt, NULL);
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_cand, -1, &c->cand_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_files, -1, &c->files_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_upsert, -1, &c->upsert_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_meta, -1, &c->meta_stmt, NULL);
    }
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "classify-backfill: statement preparation failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        cb_finalize_statements(c);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Public API
 * ------------------------------------------------------------------------ */

int classify_backfill_run(database_t *db, const classify_backfill_opts_t *opts,
                          classify_backfill_progress_t *out) {
    classify_backfill_progress_t progress;
    cb_ctx_t ctx;
    long long target;
    long long start_id;
    long long rows_total;
    long long lo;
    int dry_run;
    int stopped = 0;
    int result = -1;

    memset(&progress, 0, sizeof(progress));
    progress.state = CLASSIFY_BACKFILL_IDLE;
    memset(&ctx, 0, sizeof(ctx));

    if (!db || !db->db) {
        log_msg(LOG_ERROR, "classify-backfill: no database connection");
        progress.state = CLASSIFY_BACKFILL_FAILED;
        if (out) {
            *out = progress;
        }
        return -1;
    }

    /* The caller owns classifier_init(); the engine only reads what it
     * resolved. A build without CLF_ENABLED has no models and lands here. */
    cb_stop_requested = 0;
    target = (opts && opts->target_clf_ver > 0) ? opts->target_clf_ver
                                                : classifier_current_version();
    if (!classifier_models_loaded() || target <= 0) {
        log_msg(LOG_WARN,
                "classify-backfill: disabled (models_loaded=%d, target generation=%lld); "
                "nothing written",
                classifier_models_loaded(), target);
        progress.state = CLASSIFY_BACKFILL_DISABLED;
        progress.target_clf_ver = target;
        cb_log_summary(&progress);
        if (out) {
            *out = progress;
        }
        return 1;
    }

    ctx.db = db;
    ctx.target = target;
    ctx.chunk_ids = (opts && opts->chunk_ids > 0) ? opts->chunk_ids : CB_CHUNK_IDS_DEFAULT;
    ctx.batch_rows = (opts && opts->batch_rows > 0) ? opts->batch_rows : CB_BATCH_ROWS_DEFAULT;
    ctx.limit_rows = (opts && opts->limit_rows > 0) ? opts->limit_rows : 0;
    ctx.prog = &progress;
    dry_run = (opts && opts->dry_run) ? 1 : 0;

    progress.target_clf_ver = target;
    sqlite3_busy_timeout(db->db, CB_BUSY_TIMEOUT_MS);

    start_id = cb_resume_last_id(db, target);
    progress.last_id = start_id;
    if (cb_count_candidates(db, start_id, target, &rows_total) != 0) {
        progress.state = CLASSIFY_BACKFILL_FAILED;
        cb_log_summary(&progress);
        if (out) {
            *out = progress;
        }
        return -1;
    }
    progress.rows_total = rows_total;

    if (dry_run) {
        log_msg(LOG_INFO,
                "classify-backfill: dry run, %lld candidate(s) at generation %lld, "
                "nothing written",
                rows_total, target);
        progress.state = CLASSIFY_BACKFILL_DONE;
        cb_log_summary(&progress);
        if (out) {
            *out = progress;
        }
        return 0;
    }

    ctx.batch = malloc((size_t)ctx.batch_rows * sizeof(*ctx.batch));
    if (!ctx.batch) {
        log_msg(LOG_ERROR, "classify-backfill: out of memory for the batch buffer");
        progress.state = CLASSIFY_BACKFILL_FAILED;
        goto cleanup;
    }
    if (cb_prepare_statements(&ctx) != 0) {
        progress.state = CLASSIFY_BACKFILL_FAILED;
        goto cleanup;
    }

    lo = start_id;
    for (;;) {
        long long hi = 0;
        cb_candidate_t *cands = NULL;
        int num_cands = 0;
        int i;

        if (cb_stop_requested) {
            stopped = 1;
            break;
        }
        if (cb_next_hi(&ctx, lo, &hi) != 0) {
            progress.state = CLASSIFY_BACKFILL_FAILED;
            goto cleanup;
        }
        if (hi <= 0) {
            break; /* nothing left to visit */
        }
        if (cb_load_window(&ctx, lo, hi, &cands, &num_cands) != 0) {
            progress.state = CLASSIFY_BACKFILL_FAILED;
            goto cleanup;
        }

        for (i = 0; i < num_cands; i++) {
            if (ctx.limit_rows > 0 && progress.rows_done >= ctx.limit_rows) {
                stopped = 1;
                break;
            }
            if (cb_classify_one(&ctx, &cands[i]) != 0) {
                cb_free_candidates(cands, num_cands);
                progress.state = CLASSIFY_BACKFILL_FAILED;
                goto cleanup;
            }
            progress.rows_done++;
            if (ctx.batch_count >= ctx.batch_rows) {
                if (cb_flush(&ctx) != 0) {
                    cb_free_candidates(cands, num_cands);
                    progress.state = CLASSIFY_BACKFILL_FAILED;
                    goto cleanup;
                }
                /* The batch in flight always commits before a stop is seen. */
                if (cb_stop_requested) {
                    stopped = 1;
                    break;
                }
                if (ctx.limit_rows > 0 && progress.rows_done >= ctx.limit_rows) {
                    stopped = 1;
                    break;
                }
            }
        }
        cb_free_candidates(cands, num_cands);

        if (ctx.batch_count > 0 && cb_flush(&ctx) != 0) {
            progress.state = CLASSIFY_BACKFILL_FAILED;
            goto cleanup;
        }

        log_msg(LOG_INFO,
                "classify-backfill: %lld/%lld updated=%lld inserted=%lld skipped=%lld "
                "failed=%lld last_id=%lld",
                progress.rows_done, progress.rows_total, progress.rows_updated,
                progress.rows_inserted, progress.rows_skipped, progress.rows_failed,
                progress.last_id);

        if (stopped) {
            break;
        }
        lo = hi;
    }

    if (stopped) {
        if (progress.rows_total > progress.rows_done) {
            progress.rows_skipped = progress.rows_total - progress.rows_done;
        }
        progress.state = CLASSIFY_BACKFILL_RUNNING;
        log_msg(LOG_INFO, "classify-backfill: stopped at %lld row(s), resuming after id %lld",
                progress.rows_done, progress.last_id);
        result = 0;
    } else if (cb_run_locked(&ctx, cb_finish_locked, "completion metadata") != 0) {
        progress.state = CLASSIFY_BACKFILL_FAILED;
        goto cleanup;
    } else {
        progress.state = CLASSIFY_BACKFILL_DONE;
        result = 0;
    }

cleanup:
    cb_finalize_statements(&ctx);
    free(ctx.batch);
    ctx.batch = NULL;
    cb_log_summary(&progress);
    if (out) {
        *out = progress;
    }
    return result;
}

int classify_backfill_needed(database_t *db, long long target_clf_ver, long long *rows_left) {
    sqlite3_stmt *stmt = NULL;
    long long target;
    long long meta_ver = 0;
    long long left = 0;

    if (rows_left) {
        *rows_left = 0;
    }
    if (!db || !db->db || !classifier_models_loaded()) {
        return 0;
    }
    target = (target_clf_ver > 0) ? target_clf_ver : classifier_current_version();
    if (target <= 0) {
        return 0;
    }

    /* A missing meta row means "no generation recorded yet". */
    if (sqlite3_prepare_v2(db->db, "SELECT value FROM classifier_meta WHERE key = 'clf_ver'",
                           -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *value = sqlite3_column_text(stmt, 0);
            if (value) {
                meta_ver = strtoll((const char *)value, NULL, 10);
            }
        }
        sqlite3_reset(stmt);
        sqlite3_finalize(stmt);
    }
    if (cb_count_candidates(db, 0, target, &left) != 0) {
        left = 0;
    }
    if (rows_left) {
        *rows_left = left;
    }
    /* Both directions: the stored generation can be behind while every row is
     * already converted, because the meta write happens only at completion. */
    return (meta_ver < target || left > 0) ? 1 : 0;
}

void classify_backfill_request_stop(void) {
    cb_stop_requested = 1;
}
