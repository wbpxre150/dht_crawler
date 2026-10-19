#include "category_filter_cleanup.h"
#include "language_filter_cleanup.h"
#include "dht_crawler.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include <sqlite3.h>

/* Rows deleted per transaction, so WAL growth and the memory the delete needs
 * stay bounded on a multi-million-row database. */
#define DELETE_BATCH_SIZE 5000

/* ------------------------------------------------------------------------ *
 * Taxonomy tables (names only: the codes themselves live in
 * dht_crawler-android/tparse/taxonomy.h, which config.c and tree_metadata.c
 * cannot include).
 * ------------------------------------------------------------------------ */

static const char *const CATEGORY_NAMES[CATEGORY_FILTER_NUM_CATEGORIES] = {
    "unknown", "movie", "tv",       "anime", "music", "audiobook",
    "ebook",   "comic", "software", "game",  "image", "other"
};

static const char *const TV_KIND_NAMES[CATEGORY_FILTER_NUM_TV_KINDS] = {
    "none", "episode", "multi_episode", "season_pack", "multi_season",
    "complete_series", "daily"
};

/* Trimmed, case-insensitive name lookup with a decimal-index fallback so a
 * hand-written config can also say "save_categories = 1,2,3". */
static int parse_token(const char *token, const char *const *names, int count) {
    if (!token) return -1;

    while (*token == ' ' || *token == '\t') token++;
    size_t n = strlen(token);
    while (n > 0 && (token[n - 1] == ' ' || token[n - 1] == '\t' ||
                     token[n - 1] == '\r' || token[n - 1] == '\n')) {
        n--;
    }

    char buf[64];
    if (n == 0 || n >= sizeof(buf)) return -1;
    memcpy(buf, token, n);
    buf[n] = '\0';

    for (int i = 0; i < count; i++) {
        if (strcasecmp(buf, names[i]) == 0) return i;
    }

    char *end = NULL;
    long v = strtol(buf, &end, 10);
    if (end && *end == '\0' && v >= 0 && v < count) return (int)v;

    return -1;
}

int category_filter_category_parse(const char *token) {
    return parse_token(token, CATEGORY_NAMES, CATEGORY_FILTER_NUM_CATEGORIES);
}

int category_filter_tv_kind_parse(const char *token) {
    return parse_token(token, TV_KIND_NAMES, CATEGORY_FILTER_NUM_TV_KINDS);
}

uint16_t category_filter_category_bit(int category) {
    if (category < 0 || category >= CATEGORY_FILTER_NUM_CATEGORIES) return 0;
    return (uint16_t)(1u << category);
}

uint16_t category_filter_tv_kind_bit(int tv_kind) {
    if (tv_kind < 0 || tv_kind >= CATEGORY_FILTER_NUM_TV_KINDS) return 0;
    return (uint16_t)(1u << tv_kind);
}

/* ------------------------------------------------------------------------ *
 * Ingest decision
 * ------------------------------------------------------------------------ */

category_filter_verdict_t category_filter_should_drop(int category, int tv_kind,
                                                      uint16_t category_mask,
                                                      uint16_t tv_kind_mask) {
    /* An unclassified item carries category 0 (unknown) / tv_kind 0 (none),
     * i.e. it is judged exactly like a row the classifier could only place in
     * the unknown bucket. */
    uint16_t cat_bit = category_filter_category_bit(category);
    if (cat_bit == 0) cat_bit = category_filter_category_bit(0);
    if ((category_mask & cat_bit) == 0) return CATEGORY_FILTER_DROP_CATEGORY;

    if (category == CATEGORY_FILTER_CAT_TV || category == CATEGORY_FILTER_CAT_ANIME) {
        uint16_t tv_bit = category_filter_tv_kind_bit(tv_kind);
        if (tv_bit == 0) tv_bit = category_filter_tv_kind_bit(0);  /* "none" */
        if ((tv_kind_mask & tv_bit) == 0) return CATEGORY_FILTER_DROP_TV_KIND;
    }

    return CATEGORY_FILTER_KEEP;
}

/* ------------------------------------------------------------------------ *
 * Ingest counters (/stats "category_filter")
 * ------------------------------------------------------------------------ */

static struct {
    pthread_mutex_t mutex;
    category_filter_stats_t stats;
} cf_stats = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .stats = {0},
};

void category_filter_note_category_dropped(void) {
    pthread_mutex_lock(&cf_stats.mutex);
    cf_stats.stats.dropped_by_category++;
    cf_stats.stats.total_dropped++;
    pthread_mutex_unlock(&cf_stats.mutex);
}

void category_filter_note_tv_kind_dropped(void) {
    pthread_mutex_lock(&cf_stats.mutex);
    cf_stats.stats.dropped_by_tv_kind++;
    cf_stats.stats.total_dropped++;
    pthread_mutex_unlock(&cf_stats.mutex);
}

void category_filter_get_stats(category_filter_stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&cf_stats.mutex);
    *out = cf_stats.stats;
    pthread_mutex_unlock(&cf_stats.mutex);
}

/* ------------------------------------------------------------------------ *
 * Maintenance pass
 * ------------------------------------------------------------------------ */

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        log_msg(LOG_ERROR, "SQL failed (%s): %s", sql, err ? err : "?");
        sqlite3_free(err);
        return -1;
    }
    return 0;
}

static int apply_session_pragmas(sqlite3 *db) {
    if (exec_sql(db, "PRAGMA cache_size = -1048576;") != 0) return -1;
    if (exec_sql(db, "PRAGMA temp_store = MEMORY;") != 0) return -1;
    return 0;
}

/* Comma-separated list of the indices whose bit is clear in `mask`.
 * Returns 1 when the list is non-empty, 0 when every bit is set (nothing to
 * purge) and -1 when `out` is too small. */
static int build_unselected_list(uint16_t mask, int count, char *out, size_t out_size) {
    size_t off = 0;
    out[0] = '\0';

    for (int i = 0; i < count; i++) {
        if (mask & (uint16_t)(1u << i)) continue;
        int written = snprintf(out + off, out_size - off, "%s%d", off > 0 ? "," : "", i);
        if (written < 0 || (size_t)written >= out_size - off) return -1;
        off += (size_t)written;
    }

    return off > 0 ? 1 : 0;
}

/* Count the torrents the selection would delete (both rules, de-duplicated). */
static int64_t count_selection(sqlite3 *db, const char *category_where,
                               const char *tv_kind_where) {
    char sql[2048];
    int written;

    if (category_where && tv_kind_where) {
        written = snprintf(sql, sizeof(sql),
            "SELECT COUNT(*) FROM torrents WHERE id IN ("
            "SELECT torrent_id FROM torrent_class WHERE %s"
            " UNION SELECT torrent_id FROM torrent_class WHERE %s);",
            category_where, tv_kind_where);
    } else if (category_where || tv_kind_where) {
        written = snprintf(sql, sizeof(sql),
            "SELECT COUNT(*) FROM torrents WHERE id IN ("
            "SELECT torrent_id FROM torrent_class WHERE %s);",
            category_where ? category_where : tv_kind_where);
    } else {
        return 0;
    }

    if (written < 0 || (size_t)written >= sizeof(sql)) {
        log_msg(LOG_ERROR, "category filter: selection count SQL too long");
        return -1;
    }

    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "category filter: failed to prepare the selection count: %s",
                sqlite3_errmsg(db));
        return -1;
    }

    int64_t count = -1;
    if (sqlite3_step(st) == SQLITE_ROW) count = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return count;
}

/*
 * Delete every torrent whose torrent_class row matches `where`, in batches of
 * DELETE_BATCH_SIZE inside one transaction each.
 *
 * Keyset pagination on torrent_class.torrent_id (its primary key): every batch
 * asks for the next DELETE_BATCH_SIZE ids past the last one it deleted, so the
 * whole selection is walked exactly once and the batch size bounds only WAL
 * growth, not what has to be re-scanned. The FOREIGN KEY cascade removes
 * torrent_class/torrent_files rows and the FTS delete triggers clean the search
 * index.
 *
 * Returns the number of torrent rows deleted, or -1 on failure.
 */
static int64_t purge_batched(sqlite3 *db, const char *label, const char *where) {
    char select_sql[1024];
    int written = snprintf(select_sql, sizeof(select_sql),
        "SELECT torrent_id FROM torrent_class WHERE %s"
        " AND torrent_id > ? ORDER BY torrent_id LIMIT %d",
        where, DELETE_BATCH_SIZE);
    if (written < 0 || (size_t)written >= sizeof(select_sql)) {
        log_msg(LOG_ERROR, "%s: selection SQL too long", label);
        return -1;
    }

    /* "DELETE FROM torrents WHERE id IN (?,?,...)", one bind per id. */
    char delete_sql[DELETE_BATCH_SIZE * 2 + 64];
    size_t off = (size_t)snprintf(delete_sql, sizeof(delete_sql),
                                  "DELETE FROM torrents WHERE id IN (?");
    if (off >= sizeof(delete_sql) - 1) {
        log_msg(LOG_ERROR, "%s: delete SQL too long", label);
        return -1;
    }
    for (int i = 1; i < DELETE_BATCH_SIZE; i++) {
        delete_sql[off++] = ',';
        delete_sql[off++] = '?';
    }
    snprintf(delete_sql + off, sizeof(delete_sql) - off, ")");

    sqlite3_stmt *select_stmt = NULL;
    sqlite3_stmt *delete_stmt = NULL;
    if (sqlite3_prepare_v2(db, select_sql, -1, &select_stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "%s: failed to prepare the selection: %s", label, sqlite3_errmsg(db));
        return -1;
    }
    if (sqlite3_prepare_v2(db, delete_sql, -1, &delete_stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "%s: failed to prepare the delete: %s", label, sqlite3_errmsg(db));
        sqlite3_finalize(select_stmt);
        return -1;
    }

    int64_t *ids = malloc(sizeof(int64_t) * DELETE_BATCH_SIZE);
    if (!ids) {
        log_msg(LOG_ERROR, "%s: out of memory", label);
        sqlite3_finalize(select_stmt);
        sqlite3_finalize(delete_stmt);
        return -1;
    }

    int64_t deleted = 0;
    int64_t watermark = 0;
    int rc = 0;

    for (;;) {
        int n = 0;
        sqlite3_reset(select_stmt);
        sqlite3_bind_int64(select_stmt, 1, watermark);
        while (n < DELETE_BATCH_SIZE && (rc = sqlite3_step(select_stmt)) == SQLITE_ROW) {
            ids[n++] = sqlite3_column_int64(select_stmt, 0);
        }
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
            log_msg(LOG_ERROR, "%s: selection failed: %s", label, sqlite3_errmsg(db));
            deleted = -1;
            break;
        }
        if (n == 0) break;

        watermark = ids[n - 1];

        if (exec_sql(db, "BEGIN;") != 0) {
            deleted = -1;
            break;
        }

        sqlite3_reset(delete_stmt);
        for (int i = 0; i < n; i++) {
            sqlite3_bind_int64(delete_stmt, i + 1, ids[i]);
        }
        int drc = sqlite3_step(delete_stmt);
        sqlite3_reset(delete_stmt);
        sqlite3_clear_bindings(delete_stmt);

        if (drc != SQLITE_DONE) {
            log_msg(LOG_ERROR, "%s: delete failed: %s", label, sqlite3_errmsg(db));
            exec_sql(db, "ROLLBACK;");
            deleted = -1;
            break;
        }

        int changed = sqlite3_changes(db);
        if (exec_sql(db, "COMMIT;") != 0) {
            exec_sql(db, "ROLLBACK;");
            deleted = -1;
            break;
        }

        deleted += changed;
        log_msg(LOG_INFO, "%s: deleted %d rows (total %lld)", label, changed,
                (long long)deleted);
    }

    free(ids);
    sqlite3_finalize(select_stmt);
    sqlite3_finalize(delete_stmt);
    return deleted;
}

int category_filter_update_run(database_t *db_handle, const char *db_path,
                               uint16_t category_mask, uint16_t tv_kind_mask) {
    if (!db_handle || !db_handle->db) {
        log_msg(LOG_ERROR, "category filter: invalid database handle");
        return 1;
    }
    sqlite3 *db = db_handle->db;

    char unselected_categories[64];
    char unselected_tv_kinds[32];
    int cat_rc = build_unselected_list(category_mask, CATEGORY_FILTER_NUM_CATEGORIES,
                                       unselected_categories, sizeof(unselected_categories));
    int tv_rc = build_unselected_list(tv_kind_mask, CATEGORY_FILTER_NUM_TV_KINDS,
                                      unselected_tv_kinds, sizeof(unselected_tv_kinds));
    if (cat_rc < 0 || tv_rc < 0) {
        log_msg(LOG_ERROR, "category filter: could not build the unselected lists");
        return 1;
    }

    char category_where[128];
    char tv_kind_where[192];
    const char *category_where_p = NULL;
    const char *tv_kind_where_p = NULL;

    if (cat_rc == 1) {
        snprintf(category_where, sizeof(category_where),
                 "category IN (%s)", unselected_categories);
        category_where_p = category_where;
    }
    if (tv_rc == 1) {
        /* tv_kind 0 ("none") is stored as NULL, hence the COALESCE. */
        snprintf(tv_kind_where, sizeof(tv_kind_where),
                 "category IN (%d,%d) AND COALESCE(tv_kind, 0) IN (%s)",
                 CATEGORY_FILTER_CAT_TV, CATEGORY_FILTER_CAT_ANIME, unselected_tv_kinds);
        tv_kind_where_p = tv_kind_where;
    }

    log_msg(LOG_INFO, "category filter: save_categories=0x%03x save_tv_kinds=0x%02x "
            "(unselected categories: %s; unselected tv kinds: %s)",
            (unsigned)category_mask, (unsigned)tv_kind_mask,
            category_where_p ? unselected_categories : "none",
            tv_kind_where_p ? unselected_tv_kinds : "none");

    if (apply_session_pragmas(db) != 0) {
        return 1;
    }

    int64_t total = 0;
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM torrents", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    log_msg(LOG_INFO, "category filter: torrents in DB = %lld", (long long)total);

    /* Safety: never let the selection empty the database. Unreachable from the
     * config parser (an empty list means "all bits set"), but a hand-edited or
     * corrupted mask must not wipe the crawl. */
    if (category_where_p || tv_kind_where_p) {
        int64_t doomed = count_selection(db, category_where_p, tv_kind_where_p);
        if (doomed < 0) {
            return 1;
        }
        log_msg(LOG_INFO, "category filter: selection would delete %lld of %lld torrents",
                (long long)doomed, (long long)total);
        if (total > 0 && doomed >= total) {
            log_msg(LOG_ERROR, "category filter: the selection covers every row in the "
                    "database (%lld/%lld) — refusing to empty it. Check save_categories/"
                    "save_tv_kinds.", (long long)doomed, (long long)total);
            return 1;
        }
    }

    time_t started = time(NULL);

    /* Step 1: category. */
    int64_t deleted_by_category = 0;
    if (category_where_p) {
        deleted_by_category = purge_batched(db, "category filter: category pass", category_where);
        if (deleted_by_category < 0) {
            log_msg(LOG_ERROR, "category filter: category pass failed");
            return 1;
        }
    } else {
        log_msg(LOG_INFO, "category filter: every category is selected, skipping the category pass");
    }

    /* Step 2: tv_kind inside the tv/anime categories. */
    int64_t deleted_by_tv_kind = 0;
    if (tv_kind_where_p) {
        deleted_by_tv_kind = purge_batched(db, "category filter: tv_kind pass", tv_kind_where_p);
        if (deleted_by_tv_kind < 0) {
            log_msg(LOG_ERROR, "category filter: tv_kind pass failed");
            return 1;
        }
    } else {
        log_msg(LOG_INFO, "category filter: every tv_kind is selected, skipping the tv_kind pass");
    }

    /* Step 3: the db-wide language pass (no backup prompt: no TTY). */
    int lrc = language_filter_cleanup_run(db_handle, db_path, 0);
    if (lrc != 0) {
        log_msg(LOG_ERROR, "category filter: language pass failed (%d)", lrc);
        return lrc;
    }

    /* Step 4: fold the WAL back into the main file (hygiene; this is not a
     * compaction — --category-filter-update no longer vacuums the database). */

    long elapsed = (long)(time(NULL) - started);
    log_msg(LOG_INFO, "category filter: purge done — deleted_by_category=%lld "
            "deleted_by_tv_kind=%lld total=%lld elapsed=%lds",
            (long long)deleted_by_category, (long long)deleted_by_tv_kind,
            (long long)(deleted_by_category + deleted_by_tv_kind), elapsed);
    printf("\nSummary: deleted_by_category=%lld deleted_by_tv_kind=%lld elapsed=%lds\n",
           (long long)deleted_by_category, (long long)deleted_by_tv_kind, elapsed);

    return 0;
}
