/**
 * Host unit test for the schema-v2 torrent-level word index (torrent_docs).
 *
 * Exercises the real product code, not a copy of it:
 *   - the DDL applied by database_create_schema()          (src/database.c)
 *   - ingest through database_insert_batch()               (src/database.c),
 *     the exact function batch_writer_flush() calls        (src/batch_writer.c)
 *   - the torrents_docs_ad delete trigger (FK cascade + contentless DELETE)
 *   - database_rebuild_docs() twice, i.e. `--rebuild-docs` idempotency
 *
 * The seed is fixed so every expected value is exact: torrent 1 is
 * "Zebra Alpha Pack" (no `ac` token), torrent 2 is "The Best Of AC DC Live"
 * (the only torrent carrying standalone `ac`/`dc` tokens — the point of the
 * index is that 2-byte tokens are searchable at all), torrents 3..300 are
 * "Filler torrent number <i>". Torrent 150 is the one deleted mid-test.
 *
 * The scratch database is left in place: /tmp/torrent-docs-test.db is reused by
 * the `--rebuild-docs` CLI criterion. It is self-contained because
 * database_rebuild_docs() ends with a TRUNCATE WAL checkpoint.
 *
 * Standalone: no test framework, no dependency on the daemon build (the test
 * links src/database.c and src/bloom_filter.c directly, with tests/stub_log.c
 * for log_msg()).
 *
 *     make tests        # or: make tests-docs / make tests-asan
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "database.h"
#include "dht_crawler.h"

#define TEST_DB "/tmp/torrent-docs-test.db"
#define SEED_TORRENTS 300
#define DELETED_ID 150

/* Failing assert: print it and exit non-zero (no test framework here). */
static void fail(const char *what, const char *detail) {
    fprintf(stderr, "test_torrent_docs: FAILED: %s", what);
    if (detail && *detail) {
        fprintf(stderr, ": %s", detail);
    }
    fprintf(stderr, "\n");
    exit(1);
}

/* Run `sql` and return column 0 of the first row (SQL NULL -> 0). */
static sqlite3_int64 scalar_sql(sqlite3 *db, const char *sql, const char *what) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fail(what, sqlite3_errmsg(db));
    }
    sqlite3_int64 value = 0;
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        value = sqlite3_column_int64(stmt, 0);
    } else if (rc != SQLITE_DONE) {
        char detail[256];
        snprintf(detail, sizeof detail, "step rc=%d: %s", rc, sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        fail(what, detail);
    }
    sqlite3_finalize(stmt);
    return value;
}

static void expect_count(sqlite3 *db, const char *sql, sqlite3_int64 want,
                         const char *what) {
    sqlite3_int64 got = scalar_sql(db, sql, what);
    if (got != want) {
        char detail[128];
        snprintf(detail, sizeof detail, "got %lld, want %lld",
                 (long long)got, (long long)want);
        fail(what, detail);
    }
}

/* Assert that `torrent_docs MATCH <match>` returns exactly the rowids in
 * `want` (ascending, `want_n` entries) and nothing else. */
static void expect_match(sqlite3 *db, const char *match,
                         const sqlite3_int64 *want, int want_n) {
    char what[160];
    snprintf(what, sizeof what, "torrent_docs MATCH '%s' rowids", match);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db,
                           "SELECT rowid FROM torrent_docs "
                           "WHERE torrent_docs MATCH ?1 ORDER BY rowid",
                           -1, &stmt, NULL) != SQLITE_OK) {
        fail(what, sqlite3_errmsg(db));
    }
    sqlite3_bind_text(stmt, 1, match, -1, SQLITE_STATIC);

    int got_n = 0;
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        sqlite3_int64 rowid = sqlite3_column_int64(stmt, 0);
        if (got_n >= want_n || rowid != want[got_n]) {
            char detail[160];
            if (got_n >= want_n) {
                snprintf(detail, sizeof detail,
                         "unexpected extra rowid %lld (wanted %d rows)",
                         (long long)rowid, want_n);
            } else {
                snprintf(detail, sizeof detail,
                         "rowid %lld where %lld was expected (row %d)",
                         (long long)rowid, (long long)want[got_n], got_n);
            }
            sqlite3_finalize(stmt);
            fail(what, detail);
        }
        got_n++;
    }
    if (rc != SQLITE_DONE) {
        char detail[256];
        snprintf(detail, sizeof detail, "step rc=%d: %s", rc, sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        fail(what, detail);
    }
    sqlite3_finalize(stmt);

    if (got_n != want_n) {
        char detail[128];
        snprintf(detail, sizeof detail, "got %d rows, want %d", got_n, want_n);
        fail(what, detail);
    }
}

static void exec_sql(sqlite3 *db, const char *sql, const char *what) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        char detail[512];
        snprintf(detail, sizeof detail, "%s", err ? err : "(no message)");
        sqlite3_free(err);
        fail(what, detail);
    }
}

/* Remove the scratch database and its WAL sidecars so every run starts clean. */
static void remove_scratch_db(void) {
    static const char *const SUFFIXES[] = {"", "-wal", "-shm"};
    for (size_t i = 0; i < sizeof SUFFIXES / sizeof SUFFIXES[0]; i++) {
        char path[512];
        snprintf(path, sizeof path, "%s%s", TEST_DB, SUFFIXES[i]);
        if (unlink(path) != 0) {
            /* ENOENT is the normal case; anything else surfaces at open time. */
            FILE *probe = fopen(path, "rb");
            if (probe) {
                fclose(probe);
                fail("remove stale scratch database", path);
            }
        }
    }
}

/* Heap-allocate one torrent_metadata_t (and its file array) per the seed spec. */
static torrent_metadata_t *make_torrent(int id, const char *name,
                                        const char *const *paths, int num_files) {
    torrent_metadata_t *torrent = calloc(1, sizeof(*torrent));
    if (!torrent) {
        fail("calloc torrent_metadata_t", NULL);
    }

    /* Distinct info_hash per torrent: the id little-endian in the first two
     * bytes, then a fixed pattern. One byte alone cannot distinguish 300 ids
     * (id 257 would collide with id 1 and INSERT OR REPLACE would silently
     * replace the earlier torrent). */
    torrent->info_hash[0] = (uint8_t)(id & 0xFF);
    torrent->info_hash[1] = (uint8_t)((id >> 8) & 0xFF);
    for (int i = 2; i < SHA1_DIGEST_LENGTH; i++) {
        torrent->info_hash[i] = (uint8_t)(i * 13 + 5);
    }
    torrent->name = strdup(name);
    if (!torrent->name) {
        fail("strdup torrent name", NULL);
    }
    torrent->size_bytes = (int64_t)1024 * id;
    torrent->total_peers = id % 7;
    torrent->added_timestamp = 1700000000 + id;
    torrent->has_classification = 0;   /* no torrent_class row */
    torrent->num_files = num_files;

    if (num_files > 0) {
        torrent->files = calloc((size_t)num_files, sizeof(file_info_t));
        if (!torrent->files) {
            fail("calloc file_info_t array", NULL);
        }
        for (int i = 0; i < num_files; i++) {
            torrent->files[i].path = strdup(paths[i]);
            if (!torrent->files[i].path) {
                fail("strdup file path", NULL);
            }
            torrent->files[i].size_bytes = 1000 + i;
            torrent->files[i].file_index = (int16_t)i;
        }
    }

    return torrent;
}

static void free_torrent(torrent_metadata_t *torrent) {
    if (!torrent) {
        return;
    }
    for (int i = 0; i < torrent->num_files; i++) {
        free(torrent->files[i].path);
    }
    free(torrent->files);
    free(torrent->name);
    free(torrent);
}

int main(void) {
    static const char *const FILES_T1[] = {
        "zebra/alpha-one.bin",
        "zebra/alpha-two.bin",
    };
    static const char *const FILES_T2[] = {
        "disc1/AC-DC-Back-In-Black.flac",
        "disc1/AC-DC-Highway-To-Hell.flac",
        "disc2/readme.txt",
    };

    torrent_metadata_t *seed[SEED_TORRENTS];

    remove_scratch_db();

    /* Real DDL + real prepared statements, exactly as the daemon sets them up. */
    app_context_t app_ctx;
    memset(&app_ctx, 0, sizeof app_ctx);

    database_t db;
    if (database_init(&db, TEST_DB, &app_ctx) != 0) {
        fail("database_init", TEST_DB);
    }
    if (database_create_schema(&db) != 0) {
        fail("database_create_schema", TEST_DB);
    }


    /* --- seed ------------------------------------------------------------ */
    seed[0] = make_torrent(1, "Zebra Alpha Pack", FILES_T1, 2);
    seed[1] = make_torrent(2, "The Best Of AC DC Live", FILES_T2, 3);
    for (int id = 3; id <= SEED_TORRENTS; id++) {
        char name[64];
        char file_a[128];
        char file_b[128];
        snprintf(name, sizeof name, "Filler torrent number %d", id);
        snprintf(file_a, sizeof file_a, "folder%d/file-%d-1.bin", id, id);
        snprintf(file_b, sizeof file_b, "folder%d/file-%d-2.bin", id, id);
        const char *const paths[2] = {file_a, file_b};
        seed[id - 1] = make_torrent(id, name, paths, 2);
    }

    /* Ingest through the flush path's own entry point (one BEGIN...COMMIT). */
    size_t written = 0;
    if (database_insert_batch(&db, seed, SEED_TORRENTS, &written) != 0) {
        fail("database_insert_batch", "returned -1");
    }
    if (written != SEED_TORRENTS) {
        char detail[128];
        snprintf(detail, sizeof detail, "wrote %zu of %d",
                 written, SEED_TORRENTS);
        fail("database_insert_batch item count", detail);
    }

    /* --- ingest maintained torrent_docs ---------------------------------- */
    expect_count(db.db, "SELECT count(*) FROM torrent_docs", SEED_TORRENTS,
                 "count(*) FROM torrent_docs after ingest");

    /* Sanity: the seed really is 300 distinct torrents (a hash collision would
     * let INSERT OR REPLACE replace an earlier row and shift the expected ids).
     */
    expect_count(db.db, "SELECT count(*) FROM torrents", SEED_TORRENTS,
                 "count(*) FROM torrents after ingest");

    /* 2-byte token: only torrent 2 carries a standalone `ac`. */
    static const sqlite3_int64 AC_ROWIDS[] = {2};
    expect_match(db.db, "ac", AC_ROWIDS, 1);
    expect_match(db.db, "best AND of AND ac AND dc", AC_ROWIDS, 1);

    /* A 3-digit number is one token, and only torrent 150 has it. */
    static const sqlite3_int64 ID150_ROWIDS[] = {DELETED_ID};
    expect_match(db.db, "150", ID150_ROWIDS, 1);

    /* --- delete trigger (FK cascade + contentless DELETE by rowid) -------- */
    exec_sql(db.db, "DELETE FROM torrents WHERE id = 150;",
             "DELETE FROM torrents WHERE id = 150");
    {
        int changes = sqlite3_changes(db.db);
        if (changes != 1) {
            char detail[64];
            snprintf(detail, sizeof detail, "%d rows deleted", changes);
            fail("DELETE FROM torrents WHERE id = 150 changes", detail);
        }
    }

    expect_count(db.db, "SELECT count(*) FROM torrent_docs", SEED_TORRENTS - 1,
                 "count(*) FROM torrent_docs after deleting torrent 150");
    expect_match(db.db, "150", NULL, 0);
    expect_match(db.db, "ac", AC_ROWIDS, 1);

    /* --- --rebuild-docs, twice: idempotent rebuild ----------------------- */
    if (database_rebuild_docs(TEST_DB) != 0) {
        fail("database_rebuild_docs (first run)", "returned -1");
    }
    expect_count(db.db, "SELECT count(*) FROM torrent_docs", SEED_TORRENTS - 1,
                 "count(*) FROM torrent_docs after the first --rebuild-docs");
    expect_count(db.db, "SELECT count(DISTINCT rowid) FROM torrent_docs",
                 SEED_TORRENTS - 1,
                 "duplicate torrent_docs rowids after the first --rebuild-docs");

    if (database_rebuild_docs(TEST_DB) != 0) {
        fail("database_rebuild_docs (second run)", "returned -1");
    }
    expect_count(db.db, "SELECT count(*) FROM torrent_docs", SEED_TORRENTS - 1,
                 "count(*) FROM torrent_docs after the second --rebuild-docs");
    expect_count(db.db, "SELECT count(DISTINCT rowid) FROM torrent_docs",
                 SEED_TORRENTS - 1,
                 "duplicate torrent_docs rowids after the second --rebuild-docs");

    /* Truncating final checkpoint, so the file is left self-contained. */
    database_cleanup(&db);

    for (int i = 0; i < SEED_TORRENTS; i++) {
        free_torrent(seed[i]);
    }

    printf("test_torrent_docs: all assertions passed\n");
    return 0;
}
