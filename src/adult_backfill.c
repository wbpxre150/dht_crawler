#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE  /* For usleep */
#endif

#include "adult_backfill.h"
#include "classifier.h"
#include "dht_crawler.h"
#include "maintenance_progress.h"

#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef CLF_ENABLED
#include "tparse.h"
#endif

/* ------------------------------------------------------------------------ *
 * Tunables
 * ------------------------------------------------------------------------ */

#define AB_CHUNK_IDS_DEFAULT 10000
#define AB_BATCH_ROWS_DEFAULT 500

/* classifier_meta key holding "<generation>:<last committed torrent id>".
 * Generation-scoped on purpose: a value left behind by an older generation can
 * never make a newer pass skip rows. A completed pass tombstones the value. */
#define AB_RESUME_KEY "adult_backfill_last_id"

/* A busy writer is retried; the UPDATEs are idempotent, so replaying a whole
 * batch is safe. sqlite3_busy_timeout() already waits per statement; the
 * retries cover a writer that holds the lock longer than that. */
#define AB_BUSY_TIMEOUT_MS 30000
#define AB_BUSY_ATTEMPTS 5
#define AB_BUSY_SLEEP_US 50000

/* The token family the parser emits for the adult stage (tparse.c
 * put_adult_tokens): __adult_jav, __adult_site and the name-side __adult_set.
 * One prefix covers all three. */
#define AB_TOKEN "__adult_"

/* Same cap classifier.c uses for the feature text (its CLF_FEATURE_CAP is
 * private to that translation unit). Keep them equal: a different cap would
 * truncate a different number of characters and the gate would judge a
 * different string than the student scores. */
#define AB_FEATURE_CAP 8192

/* Cooperative cancel: set by adult_backfill_request_stop(), cleared at the
 * start of every pass, read between batches (never inside one). */
static volatile sig_atomic_t ab_stop_requested = 0;

/* ------------------------------------------------------------------------ *
 * Types
 * ------------------------------------------------------------------------ */

/* One candidate of the current id window. The window is read into memory
 * before the first write of the window: the LEFT JOIN that produced these rows
 * must not still be stepping while the batch UPDATEs torrent_class. */
typedef struct {
    long long id;
    char *name; /* owned copy of torrents.name */
    long long size_bytes;
} ab_candidate_t;

/* One scored row waiting for the next batch transaction. */
typedef struct {
    long long torrent_id;
    int adult;
    double adult_p;
} ab_pending_t;

typedef struct {
    database_t *db;
    long long target;
    int chunk_ids;
    int batch_rows;
    long long limit_rows;
    sqlite3_stmt *hi_stmt;
    sqlite3_stmt *cand_stmt;
    sqlite3_stmt *files_stmt;
    sqlite3_stmt *update_stmt;
    sqlite3_stmt *meta_stmt;
    ab_pending_t *batch;
    int batch_count;
    int batch_changed; /* UPDATEs of the pending batch that touched a row */
    adult_backfill_progress_t *prog;
} ab_ctx_t;

/* A batch body that must run with db->mutex held and returns a SQLite code. */
typedef int (*ab_locked_fn_t)(ab_ctx_t *c);

/* ------------------------------------------------------------------------ *
 * Test seam
 *
 * The adult student is reached through a file-static pointer that defaults to
 * the real entry point, so a model-free host test can drive the gate, the
 * UPDATE and the watermark path with canned verdicts. The setter only exists
 * in the test build (-DADULT_BACKFILL_TEST, set by tests-adult-backfill).
 * ------------------------------------------------------------------------ */

typedef int (*ab_verdict_fn)(const torrent_metadata_t *meta, int *adult, double *adult_p);

static ab_verdict_fn ab_verdict = classifier_classify_adult;

#ifdef ADULT_BACKFILL_TEST
void adult_backfill_set_verdict_hook(ab_verdict_fn fn) {
    if (fn) {
        ab_verdict = fn;
    }
}
#endif /* ADULT_BACKFILL_TEST */

/* ------------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------------ */

const char *adult_backfill_state_name(int state) {
    switch (state) {
    case ADULT_BACKFILL_IDLE:
        return "idle";
    case ADULT_BACKFILL_RUNNING:
        return "running";
    case ADULT_BACKFILL_DONE:
        return "done";
    case ADULT_BACKFILL_FAILED:
        return "failed";
    case ADULT_BACKFILL_DISABLED:
        return "disabled";
    default:
        return "unknown";
    }
}

/* Copy a TEXT column byte-exactly. The snapshot has filenames that are not
 * valid UTF-8; re-encoding them would change what tparse sees on the predict
 * path, so the bytes are passed through untouched. */
static char *ab_dup_column_text(sqlite3_stmt *stmt, int col) {
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

static void ab_free_files(file_info_t *files, int num_files) {
    int i;
    if (!files) {
        return;
    }
    for (i = 0; i < num_files; i++) {
        free(files[i].path);
    }
    free(files);
}

static void ab_free_candidates(ab_candidate_t *cands, int count) {
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
static int ab_meta_put(sqlite3_stmt *stmt, const char *key, const char *value) {
    int rc;

    sqlite3_reset(stmt);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    sqlite3_reset(stmt);
    return rc;
}

/* ------------------------------------------------------------------------ *
 * The token gate
 * ------------------------------------------------------------------------ */

/* 1 when the rendered feature text carries an __adult_ token.
 *
 * The first call passes the name alone (files == NULL, num_files == 0): that is
 * the cheap gate that decides whether the 287 M-row file table is worth
 * touching. The second call passes the real file list and re-checks the full
 * text, which also carries the file-derived tokens.
 *
 * Without CLF_ENABLED there is no tparse, so there is no gate to apply - and
 * every run is DISABLED before it can get here. Answering 1 ("nothing gated
 * out") is what lets the model-free host tests drive the SQL and watermark path
 * with a stubbed verdict instead of a parser. */
static int ab_gate_text(const char *name, const file_info_t *files, int num_files,
                        long long size_bytes) {
#ifdef CLF_ENABLED
    tp_file_t *tp_files = NULL;
    tp_result_t parsed;
    char feature[AB_FEATURE_CAP];
    size_t need;
    size_t cap;
    int hit;

    if (num_files > 0) {
        tp_files = (tp_file_t *)malloc((size_t)num_files * sizeof(tp_file_t));
        if (!tp_files) {
            log_msg(LOG_WARN, "adult-backfill: out of memory for %d files; gating out",
                    num_files);
            return 0;
        }
        for (int i = 0; i < num_files; i++) {
            tp_files[i].path = files[i].path ? files[i].path : "";
            tp_files[i].size = files[i].size_bytes;
        }
    }

    memset(&parsed, 0, sizeof(parsed));
    if (tp_parse(name ? name : "", tp_files, num_files, size_bytes, &parsed) != TP_OK) {
        /* An unparsable name cannot fire a token, and it must not fail the pass:
         * the row simply is not a candidate. */
        free(tp_files);
        return 0;
    }

    need = tp_feature_text(name ? name : "", tp_files, num_files, &parsed, NULL, 0);
    cap = sizeof(feature);
    if (need + 1 < cap) cap = need + 1;
    tp_feature_text(name ? name : "", tp_files, num_files, &parsed, feature, cap);
    feature[cap - 1] = '\0';
    free(tp_files);

    hit = (strstr(feature, AB_TOKEN) != NULL) ? 1 : 0;
    return hit;
#else
    (void)name;
    (void)files;
    (void)num_files;
    (void)size_bytes;
    return 1;
#endif
}

/* Test seam for the gate, mirroring the verdict hook: the model-free host build
 * has no tparse, so ab_gate_text() passes every row and a test could never see
 * a gate-out. This lets one drive the gate and prove the DRY RUN walks it.
 * Only compiled into the tests-adult-backfill build. */
typedef int (*ab_gate_fn)(const char *name, const file_info_t *files, int num_files,
                          long long size_bytes);

static ab_gate_fn ab_gate = ab_gate_text;

#ifdef ADULT_BACKFILL_TEST
void adult_backfill_set_gate_hook(ab_gate_fn fn) {
    ab_gate = fn ? fn : ab_gate_text;
}
#endif /* ADULT_BACKFILL_TEST */

/* ------------------------------------------------------------------------ *
 * Read helpers (no mutex: the pass reads on the shared handle and only takes
 * db->mutex around each BEGIN ... COMMIT batch)
 * ------------------------------------------------------------------------ */

/* Resume point for this generation, or 0 when there is none to honour. */
static long long ab_resume_last_id(database_t *db, long long target) {
    sqlite3_stmt *stmt = NULL;
    long long last_id = 0;

    if (sqlite3_prepare_v2(db->db,
                           "SELECT value FROM classifier_meta WHERE key = ?",
                           -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_DEBUG, "adult-backfill: resume key unavailable: %s",
                sqlite3_errmsg(db->db));
        return 0;
    }
    sqlite3_bind_text(stmt, 1, AB_RESUME_KEY, -1, SQLITE_STATIC);
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

/* Number of candidates a pass starting after `start_id` would visit: no class
 * row at all, or one whose adult generation is below the target. */
static int ab_count_candidates(database_t *db, long long start_id, long long target,
                               long long *count_out) {
    sqlite3_stmt *stmt = NULL;
    long long count = 0;
    int rc;

    rc = sqlite3_prepare_v2(db->db,
                            "SELECT COUNT(*) FROM torrents t "
                            "LEFT JOIN torrent_class tc ON tc.torrent_id = t.id "
                            "WHERE t.id > ? AND "
                            "(tc.adult_clf_ver IS NULL OR tc.adult_clf_ver < ?)",
                            -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "adult-backfill: candidate count failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        return -1;
    }
    sqlite3_bind_int64(stmt, 1, start_id);
    sqlite3_bind_int64(stmt, 2, target);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        log_msg(LOG_ERROR, "adult-backfill: candidate count step failed (%d: %s)", rc,
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
 * is over. Same shape as the other engines' window bound. */
static int ab_next_hi(ab_ctx_t *c, long long lo, long long *hi_out) {
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
        log_msg(LOG_ERROR, "adult-backfill: window bound failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        return -1;
    }
    sqlite3_reset(stmt);
    return 0;
}

/* Rebuild one torrent's full file paths: path_prefixes.prefix plus the leaf
 * filename, byte-exact and in torrent_files rowid order (not file_index, which
 * wraps past 32767). Returns 0 with *out_files NULL when the torrent has no
 * file rows. */
static int ab_load_files(ab_ctx_t *c, long long torrent_id, file_info_t **out_files,
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
                ab_free_files(files, num);
                log_msg(LOG_ERROR, "adult-backfill: out of memory loading files of %lld",
                        torrent_id);
                return -1;
            }
            files = grown;
            cap = new_cap;
        }
        files[num].path = ab_dup_column_text(stmt, 0);
        if (!files[num].path) {
            sqlite3_reset(stmt);
            ab_free_files(files, num);
            log_msg(LOG_ERROR, "adult-backfill: out of memory loading files of %lld",
                    torrent_id);
            return -1;
        }
        files[num].size_bytes = sqlite3_column_int64(stmt, 1);
        files[num].file_index = (int16_t)num; /* ignored on the predict path */
        num++;
    }
    sqlite3_reset(stmt);
    if (rc != SQLITE_DONE) {
        ab_free_files(files, num);
        log_msg(LOG_ERROR, "adult-backfill: file load of %lld failed (%d: %s)", torrent_id,
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
static int ab_run_locked(ab_ctx_t *c, ab_locked_fn_t fn, const char *what) {
    int attempt;

    for (attempt = 0; attempt < AB_BUSY_ATTEMPTS; attempt++) {
        int rc;
        uv_mutex_lock(&c->db->mutex);
        rc = fn(c);
        uv_mutex_unlock(&c->db->mutex);
        if (rc == SQLITE_OK) {
            return 0;
        }
        if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
            log_msg(LOG_ERROR, "adult-backfill: %s failed (%d: %s)", what, rc,
                    sqlite3_errstr(rc));
            return -1;
        }
        log_msg(LOG_WARN, "adult-backfill: %s busy (attempt %d/%d), retrying", what,
                attempt + 1, AB_BUSY_ATTEMPTS);
        usleep(AB_BUSY_SLEEP_US);
    }
    log_msg(LOG_ERROR, "adult-backfill: %s still busy after %d attempts", what,
            AB_BUSY_ATTEMPTS);
    return -1;
}

/* One attempt at the pending batch. Called with db->mutex held. */
static int ab_flush_locked(ab_ctx_t *c) {
    sqlite3 *db = c->db->db;
    sqlite3_stmt *stmt = c->update_stmt;
    char resume[64];
    int changed = 0;
    int rc;
    int i;

    rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }

    for (i = 0; i < c->batch_count; i++) {
        const ab_pending_t *p = &c->batch[i];

        sqlite3_reset(stmt);
        sqlite3_bind_int(stmt, 1, p->adult);
        sqlite3_bind_double(stmt, 2, p->adult_p);
        /* The generation this pass resolved, never the model's own field: the
         * selection predicate and the stored value must be the same number. */
        sqlite3_bind_int64(stmt, 3, c->target);
        sqlite3_bind_int64(stmt, 4, p->torrent_id);
        rc = sqlite3_step(stmt);
        sqlite3_reset(stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            return rc;
        }
        /* A candidate can have no torrent_class row at all (the LEFT JOIN makes
         * those adult_clf_ver IS NULL). There is nothing to update for them and
         * inventing a class row here would fabricate a category; they stay
         * candidates and are counted as scored-but-not-updated. */
        if (sqlite3_changes(db) > 0) {
            changed++;
        }
    }

    /* Committed ids and the resume key are written by the same transaction, so
     * a crash can only lose a batch, never skip one. */
    snprintf(resume, sizeof(resume), "%lld:%lld", c->target,
             c->batch[c->batch_count - 1].torrent_id);
    rc = ab_meta_put(c->meta_stmt, AB_RESUME_KEY, resume);
    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }

    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return rc;
    }
    c->batch_changed = changed;
    return SQLITE_OK;
}

/* One attempt at the completion bookkeeping: tombstone the resume key so the
 * next generation starts from the beginning. Deliberately does NOT call
 * classifier_write_meta(): this pass changes adult labels only, and rewriting
 * the fastText provenance from it would claim a full re-classification that
 * never happened. Called with db->mutex held. */
static int ab_finish_locked(ab_ctx_t *c) {
    sqlite3 *db = c->db->db;
    int rc;

    rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        return rc;
    }
    rc = ab_meta_put(c->meta_stmt, AB_RESUME_KEY, "");
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

static int ab_flush(ab_ctx_t *c) {
    if (c->batch_count == 0) {
        return 0;
    }
    if (ab_run_locked(c, ab_flush_locked, "batch commit") != 0) {
        return -1;
    }
    c->prog->rows_updated += c->batch_changed;
    c->prog->last_id = c->batch[c->batch_count - 1].torrent_id;
    c->batch_count = 0;
    c->batch_changed = 0;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Window walk
 * ------------------------------------------------------------------------ */

/* Read the candidates of one id window (lo, hi] into memory. */
static int ab_load_window(ab_ctx_t *c, long long lo, long long hi, ab_candidate_t **out,
                          int *out_count) {
    sqlite3_stmt *stmt = c->cand_stmt;
    ab_candidate_t *cands = NULL;
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
            ab_candidate_t *grown = realloc(cands, (size_t)new_cap * sizeof(*grown));
            if (!grown) {
                sqlite3_reset(stmt);
                ab_free_candidates(cands, count);
                log_msg(LOG_ERROR, "adult-backfill: out of memory loading window (%lld, %lld]",
                        lo, hi);
                return -1;
            }
            cands = grown;
            cap = new_cap;
        }
        cands[count].name = ab_dup_column_text(stmt, 1);
        if (!cands[count].name) {
            sqlite3_reset(stmt);
            ab_free_candidates(cands, count);
            log_msg(LOG_ERROR, "adult-backfill: out of memory loading window (%lld, %lld]",
                    lo, hi);
            return -1;
        }
        cands[count].id = sqlite3_column_int64(stmt, 0);
        cands[count].size_bytes = sqlite3_column_int64(stmt, 2);
        count++;
    }
    sqlite3_reset(stmt);
    if (rc != SQLITE_DONE) {
        ab_free_candidates(cands, count);
        log_msg(LOG_ERROR, "adult-backfill: window (%lld, %lld] failed (%d: %s)", lo, hi, rc,
                sqlite3_errstr(rc));
        return -1;
    }
    *out = cands;
    *out_count = count;
    return 0;
}

/* The two-stage token gate, shared verbatim by the dry run and the write pass.
 *
 * Stage 1 renders the name alone - the cheap gate that decides whether the
 * 287 M-row file table is worth touching. Stage 2 re-renders the full text with
 * the file list, which also carries the file-derived tokens. A row that fails
 * either stage is counted in rows_gated_out and never reaches the student.
 *
 * Returns 1 when the candidate passed (the caller owns *out_files / *out_num),
 * 0 when it was gated out, -1 on a SQL/memory error.
 *
 * Both modes call this one function, which is what makes `rows_total -
 * rows_gated_out` the exact set the write pass would score: the dry run cannot
 * drift from the write pass if there is only one gate. */
static int ab_gate_candidate(ab_ctx_t *c, const ab_candidate_t *cand, file_info_t **out_files,
                             int *out_num) {
    *out_files = NULL;
    *out_num = 0;

    if (!ab_gate(cand->name, NULL, 0, cand->size_bytes)) {
        c->prog->rows_gated_out++;
        return 0;
    }

    if (ab_load_files(c, cand->id, out_files, out_num) != 0) {
        return -1;
    }

    if (!ab_gate(cand->name, *out_files, *out_num, cand->size_bytes)) {
        ab_free_files(*out_files, *out_num);
        *out_files = NULL;
        *out_num = 0;
        c->prog->rows_gated_out++;
        return 0;
    }

    return 1;
}

/* Handle one candidate: gate it, then either count it as gated-in (dry run) or
 * score it and queue its UPDATE (write pass). Returns 0 when the candidate was
 * handled (gated out, gated in, scored or counted as failed), -1 on a SQL or
 * memory error. */
static int ab_handle_one(ab_ctx_t *c, const ab_candidate_t *cand, int dry_run) {
    file_info_t *files = NULL;
    torrent_metadata_t meta;
    ab_pending_t *slot;
    int num_files = 0;
    int adult = 0;
    double adult_p = 0.0;
    int gated;
    int rc;

    gated = ab_gate_candidate(c, cand, &files, &num_files);
    if (gated <= 0) {
        return gated; /* gated out (0), or a load error (-1) */
    }

    /* Dry run: the gate is the whole job. No student, no UPDATE, no commit -
     * and nothing extra to count, because the gated-in set is
     * rows_total - rows_gated_out. */
    if (dry_run) {
        ab_free_files(files, num_files);
        return 0;
    }

    memset(&meta, 0, sizeof(meta));
    meta.name = cand->name;
    meta.size_bytes = cand->size_bytes;
    meta.files = files;
    meta.num_files = num_files;

    rc = ab_verdict(&meta, &adult, &adult_p);
    ab_free_files(files, num_files);

    if (rc != 0) {
        /* No verdict (no adult model, parser-only fallback): leave the stored
         * row exactly as it is rather than downgrade a good one. */
        c->prog->rows_failed++;
        return 0;
    }

    slot = &c->batch[c->batch_count++];
    slot->torrent_id = cand->id;
    slot->adult = adult;
    slot->adult_p = adult_p;
    c->prog->rows_scored++;
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Statement lifecycle
 * ------------------------------------------------------------------------ */

static void ab_finalize_statements(ab_ctx_t *c) {
    sqlite3_finalize(c->hi_stmt);
    sqlite3_finalize(c->cand_stmt);
    sqlite3_finalize(c->files_stmt);
    sqlite3_finalize(c->update_stmt);
    sqlite3_finalize(c->meta_stmt);
    c->hi_stmt = NULL;
    c->cand_stmt = NULL;
    c->files_stmt = NULL;
    c->update_stmt = NULL;
    c->meta_stmt = NULL;
}

static int ab_prepare_statements(ab_ctx_t *c) {
    static const char *const sql_hi =
        "SELECT MAX(id) FROM (SELECT id FROM torrents WHERE id > ? ORDER BY id LIMIT ?)";
    static const char *const sql_cand =
        "SELECT t.id, t.name, t.size_bytes FROM torrents t "
        "LEFT JOIN torrent_class tc ON tc.torrent_id = t.id "
        "WHERE t.id > ? AND t.id <= ? "
        "AND (tc.adult_clf_ver IS NULL OR tc.adult_clf_ver < ?) "
        "ORDER BY t.id";
    static const char *const sql_files =
        "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename), tf.size_bytes "
        "FROM torrent_files tf LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
        "WHERE tf.torrent_id = ? ORDER BY tf.id";
    /* adult_clf_ver is the watermark AND the candidate predicate, so it is
     * bound to the pass target, never to the model's own field. */
    static const char *const sql_update =
        "UPDATE torrent_class SET adult = ?, adult_p = ?, adult_clf_ver = ? "
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
        rc = sqlite3_prepare_v2(db, sql_files, -1, &c->files_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_update, -1, &c->update_stmt, NULL);
    }
    if (rc == SQLITE_OK) {
        rc = sqlite3_prepare_v2(db, sql_meta, -1, &c->meta_stmt, NULL);
    }
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "adult-backfill: statement preparation failed (%d: %s)", rc,
                sqlite3_errstr(rc));
        ab_finalize_statements(c);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------------ *
 * Live progress snapshot
 * ------------------------------------------------------------------------ */

/* run() keeps its counters in a function-local struct, so without this channel
 * nothing could see a pass in flight: the caller's *out was filled once, at
 * return. g_progress is the pollable copy - the engine republishes it at every
 * observable step (run start, each committed batch, each window, each exit
 * path) and any thread may read the last one through
 * adult_backfill_get_progress(). */
static pthread_mutex_t g_progress_mutex = PTHREAD_MUTEX_INITIALIZER;
static adult_backfill_progress_t g_progress = { .state = ADULT_BACKFILL_IDLE };

static void ab_report(const adult_backfill_progress_t *p, adult_backfill_progress_t *out) {
    pthread_mutex_lock(&g_progress_mutex);
    g_progress = *p;
    pthread_mutex_unlock(&g_progress_mutex);
    if (out) {
        *out = *p;
    }
}

void adult_backfill_get_progress(adult_backfill_progress_t *out) {
    if (!out) {
        return;
    }
    pthread_mutex_lock(&g_progress_mutex);
    *out = g_progress;
    pthread_mutex_unlock(&g_progress_mutex);
}

/* Rows a pass has dealt with, by its own mode's definition: a write pass is
 * capped by the rows it scored, a dry run (which scores nothing) by the
 * candidates it gated. Used only for --adult-backfill-limit. */
static long long ab_rows_done(const adult_backfill_progress_t *p, int dry_run) {
    return dry_run ? p->rows_gated_out + p->rows_scored + p->rows_failed : p->rows_scored;
}

/* The final summary line the CLI prints and the log carries. */
static void ab_log_summary(const adult_backfill_progress_t *p) {
    log_msg(LOG_INFO,
            "adult-backfill: done state=%s target=%lld total=%lld gated_out=%lld "
            "scored=%lld updated=%lld failed=%lld last_id=%lld",
            adult_backfill_state_name(p->state), p->target_adult_clf_ver, p->rows_total,
            p->rows_gated_out, p->rows_scored, p->rows_updated, p->rows_failed, p->last_id);
}

/* ------------------------------------------------------------------------ *
 * Public API
 * ------------------------------------------------------------------------ */

int adult_backfill_run(database_t *db, const adult_backfill_opts_t *opts,
                       adult_backfill_progress_t *out) {
    adult_backfill_progress_t progress;
    ab_ctx_t ctx;
    long long target;
    long long start_id;
    long long rows_total;
    long long lo;
    int dry_run;
    int stopped = 0;
    int result = -1;
    mprog_throttle_t scan_prog;

    memset(&progress, 0, sizeof(progress));
    progress.state = ADULT_BACKFILL_IDLE;
    memset(&ctx, 0, sizeof(ctx));
    mprog_throttle_init(&scan_prog);

    if (!db || !db->db) {
        log_msg(LOG_ERROR, "adult-backfill: no database connection");
        progress.state = ADULT_BACKFILL_FAILED;
        ab_report(&progress, out);
        return -1;
    }

    /* The caller owns classifier_init(); the engine only reads what it
     * resolved. A build without CLF_ENABLED has no models and lands here. */
    ab_stop_requested = 0;
    target = (opts && opts->target_adult_clf_ver > 0) ? opts->target_adult_clf_ver
                                                     : classifier_current_version();
    if (!classifier_models_loaded() || target <= 0) {
        log_msg(LOG_WARN,
                "adult-backfill: disabled (models_loaded=%d, target generation=%lld); "
                "nothing written",
                classifier_models_loaded(), target);
        progress.state = ADULT_BACKFILL_DISABLED;
        progress.target_adult_clf_ver = target;
        ab_log_summary(&progress);
        ab_report(&progress, out);
        return 1;
    }

    ctx.db = db;
    ctx.target = target;
    ctx.chunk_ids = (opts && opts->chunk_ids > 0) ? opts->chunk_ids : AB_CHUNK_IDS_DEFAULT;
    ctx.batch_rows = (opts && opts->batch_rows > 0) ? opts->batch_rows : AB_BATCH_ROWS_DEFAULT;
    ctx.limit_rows = (opts && opts->limit_rows > 0) ? opts->limit_rows : 0;
    ctx.prog = &progress;
    dry_run = (opts && opts->dry_run) ? 1 : 0;

    progress.target_adult_clf_ver = target;
    sqlite3_busy_timeout(db->db, AB_BUSY_TIMEOUT_MS);

    start_id = ab_resume_last_id(db, target);
    progress.last_id = start_id;
    if (ab_count_candidates(db, start_id, target, &rows_total) != 0) {
        progress.state = ADULT_BACKFILL_FAILED;
        ab_log_summary(&progress);
        ab_report(&progress, out);
        return -1;
    }
    progress.rows_total = rows_total;

    /* Both modes walk the same windows in the same order from here on. The dry
     * run stops after the token gate - no student, no UPDATE, no commit, no
     * completion tombstone - so `rows_total - rows_gated_out` is exactly the set
     * the write pass would score. That is the number G3 is judged on, and it can
     * only mean that if the two paths share one gate. */
    if (dry_run) {
        log_msg(LOG_INFO,
                "adult-backfill: dry run, %lld candidate(s) at generation %lld, "
                "walking the token gate; nothing written",
                rows_total, target);
    }

    /* The pass is live from here on: publish RUNNING with the counters it just
     * resolved, so a poller sees the work start before the first batch. */
    progress.state = ADULT_BACKFILL_RUNNING;
    ab_report(&progress, NULL);
    mprog_emit("adult-backfill", "count", 0, rows_total, dry_run ? "dry_run=1" : NULL);

    ctx.batch = malloc((size_t)ctx.batch_rows * sizeof(*ctx.batch));
    if (!ctx.batch) {
        log_msg(LOG_ERROR, "adult-backfill: out of memory for the batch buffer");
        progress.state = ADULT_BACKFILL_FAILED;
        goto cleanup;
    }
    if (ab_prepare_statements(&ctx) != 0) {
        progress.state = ADULT_BACKFILL_FAILED;
        goto cleanup;
    }

    lo = start_id;
    for (;;) {
        long long hi = 0;
        ab_candidate_t *cands = NULL;
        int num_cands = 0;
        int i;

        if (ab_stop_requested) {
            stopped = 1;
            break;
        }
        if (ab_next_hi(&ctx, lo, &hi) != 0) {
            progress.state = ADULT_BACKFILL_FAILED;
            goto cleanup;
        }
        if (hi <= 0) {
            break; /* nothing left to visit */
        }
        if (ab_load_window(&ctx, lo, hi, &cands, &num_cands) != 0) {
            progress.state = ADULT_BACKFILL_FAILED;
            goto cleanup;
        }

        for (i = 0; i < num_cands; i++) {
            if (ctx.limit_rows > 0 && ab_rows_done(&progress, dry_run) >= ctx.limit_rows) {
                stopped = 1;
                break;
            }
            if (ab_handle_one(&ctx, &cands[i], dry_run) != 0) {
                ab_free_candidates(cands, num_cands);
                progress.state = ADULT_BACKFILL_FAILED;
                goto cleanup;
            }
            if (ctx.batch_count >= ctx.batch_rows) {
                if (ab_flush(&ctx) != 0) {
                    ab_free_candidates(cands, num_cands);
                    progress.state = ADULT_BACKFILL_FAILED;
                    goto cleanup;
                }
                /* One committed batch: republish so a poller sees the counters
                 * move while the pass walks. */
                ab_report(&progress, NULL);
                /* The batch in flight always commits before a stop is seen. */
                if (ab_stop_requested) {
                    stopped = 1;
                    break;
                }
                if (ctx.limit_rows > 0 &&
                    ab_rows_done(&progress, dry_run) >= ctx.limit_rows) {
                    stopped = 1;
                    break;
                }
            }
        }
        ab_free_candidates(cands, num_cands);

        if (ctx.batch_count > 0 && ab_flush(&ctx) != 0) {
            progress.state = ADULT_BACKFILL_FAILED;
            goto cleanup;
        }

        log_msg(LOG_INFO,
                "adult-backfill: %lld/%lld gated_out=%lld scored=%lld updated=%lld "
                "failed=%lld last_id=%lld",
                progress.rows_gated_out + progress.rows_scored + progress.rows_failed,
                progress.rows_total, progress.rows_gated_out, progress.rows_scored,
                progress.rows_updated, progress.rows_failed, progress.last_id);
        ab_report(&progress, NULL);
        mprog_progress(&scan_prog, "adult-backfill", "scan",
                       progress.rows_gated_out + progress.rows_scored + progress.rows_failed,
                       progress.rows_total, NULL);

        if (stopped) {
            break;
        }
        lo = hi;
    }

    if (stopped) {
        progress.state = ADULT_BACKFILL_RUNNING;
        if (dry_run) {
            /* Nothing was committed, so there is no resume key to continue from:
             * this dry run simply did not finish its walk. */
            log_msg(LOG_INFO, "adult-backfill: dry run stopped after %lld row(s)",
                    ab_rows_done(&progress, dry_run));
        } else {
            log_msg(LOG_INFO, "adult-backfill: stopped at %lld row(s), resuming after id %lld",
                    progress.rows_scored, progress.last_id);
        }
        result = 0;
    } else if (dry_run) {
        /* The walk finished and nothing was written: no batch to commit and no
         * completion tombstone to write. */
        progress.state = ADULT_BACKFILL_DONE;
        result = 0;
    } else if (ab_run_locked(&ctx, ab_finish_locked, "completion metadata") != 0) {
        progress.state = ADULT_BACKFILL_FAILED;
        goto cleanup;
    } else {
        progress.state = ADULT_BACKFILL_DONE;
        result = 0;
    }

cleanup:
    ab_finalize_statements(&ctx);
    free(ctx.batch);
    ctx.batch = NULL;
    ab_log_summary(&progress);
    ab_report(&progress, out);
    return result;
}

int adult_backfill_needed(database_t *db, long long target_adult_clf_ver,
                          long long *rows_left) {
    long long target;
    long long left = 0;

    if (rows_left) {
        *rows_left = 0;
    }
    if (!db || !db->db || !classifier_models_loaded()) {
        return 0;
    }
    target = (target_adult_clf_ver > 0) ? target_adult_clf_ver : classifier_current_version();
    if (target <= 0) {
        return 0;
    }
    if (ab_count_candidates(db, 0, target, &left) != 0) {
        left = 0;
    }
    if (rows_left) {
        *rows_left = left;
    }
    /* The row predicate is the whole answer: there is no meta key to fall
     * behind, so a finished pass reports 0 and a re-run writes nothing. */
    return left > 0 ? 1 : 0;
}

void adult_backfill_request_stop(void) {
    ab_stop_requested = 1;
}
