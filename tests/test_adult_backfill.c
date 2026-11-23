/**
 * Host unit test for the token-gated adult backfill engine
 * (include/adult_backfill.h, src/adult_backfill.c).
 *
 * Exercises the real product code, not a copy of it:
 *   - the additive migration that puts adult_clf_ver on a database created
 *     before the column existed (database_create_schema() + its
 *     database_ensure_column() ALTER), including the "existing rows survive"
 *     half - the device DB is 7.55 GiB and the ALTER runs on every start;
 *   - the engine's own candidate predicate (adult_clf_ver NULL / older than the
 *     target / already at the target);
 *   - the UPDATE and the watermark: one pass marks the candidates with the
 *     target generation, and the next pass selects nothing;
 *   - the dry run, which walks the SAME two-stage token gate the write pass
 *     walks, reports the gated-in count as `total - gated_out`, and commits
 *     nothing - and the write pass on that same fixture then scores exactly
 *     that many rows;
 *   - resume after a stop: a pass interrupted after its first committed batch
 *     resumes from the watermark and never scores a row twice.
 *
 * The token gate needs tparse, which this build does not have: without
 * CLF_ENABLED the gate answers "nothing gated out" and every run is DISABLED
 * before it could reach the gate anyway. That is exactly what lets this test
 * drive the SQL/UPDATE/watermark path with a canned verdict through the
 * -DADULT_BACKFILL_TEST seam.
 *
 * The three classifier symbols the engine references are defined at the bottom
 * of this file instead of linking src/classifier.c: the host build compiles
 * that module without CLF_ENABLED, i.e. as inert stubs that can never produce a
 * verdict (same trick tests-backfill plays). The stub reports generation 11 -
 * deliberately NOT the fixture's 6 - so a passing run proves the engine labels
 * rows with whatever generation it is told and never hardcodes one.
 *
 * Fixture (/tmp/adult-backfill-test.db, removed before and after the run),
 * one row per gate outcome - the names carry the three parser token families
 * (__adult_jav, __adult_site, __adult_set) plus three gate negatives, so the
 * gated-in count is neither zero nor everything:
 *   torrent 1 "alpha.jav.uncensored"       clf_ver=6  jav   -> gated in
 *   torrent 2 "beta-site-rip"              clf_ver=6  site  -> gated in
 *   torrent 3 "gamma-control"              clf_ver=6  none  -> gated out
 *   torrent 4 "delta.uncensored.set.720p"  clf_ver=6  set   -> gated in
 *   torrent 5 "epsilon-documentary"        clf_ver=6  none  -> gated out
 *   torrent 6 "zeta-lecture-notes"         clf_ver=6  none  -> gated out
 * The torrent_class table is created WITHOUT adult_clf_ver, i.e. the shape the
 * app shipped before patch 16; database_create_schema() must widen it.
 *
 * Standalone: no test framework, no dependency on the daemon build (the test
 * links src/adult_backfill.c, src/database.c and src/bloom_filter.c directly,
 * with tests/stub_log.c for log_msg()).
 *
 *     make tests        # or: make tests-adult-backfill
 */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "adult_backfill.h"
#include "classifier.h"
#include "database.h"
#include "dht_crawler.h"

#define TEST_DB "/tmp/adult-backfill-test.db"

/* Generation the stub classifier reports. NOT the fixture's 6 on purpose: see
 * the header. */
#define STUB_TARGET 11
/* Generation the fixture's pre-existing rows were labelled with. */
#define OLD_CLF_VER 6
/* A generation between the fixture's and the target: still a candidate. */
#define STALE_ADULT_VER 4

#define TORRENT_A 1
#define TORRENT_B 2
#define TORRENT_C 3
#define TORRENT_D 4
#define TORRENT_E 5
#define TORRENT_F 6
#define FIXTURE_TORRENTS 6
/* Of those six, three names fire an __adult_* family and three do not. */
#define FIXTURE_GATED_IN 3
#define FIXTURE_GATED_OUT (FIXTURE_TORRENTS - FIXTURE_GATED_IN)

/* database_init() stores the pointer, so this has to outlive every fixture. */
static app_context_t test_app_ctx;

static int failures;

static void check(int condition, const char *what, const char *detail) {
    if (condition) {
        return;
    }
    fprintf(stderr, "FAIL: %s%s%s\n", what, detail ? " - " : "", detail ? detail : "");
    failures++;
}

static void check_ll(long long got, long long want, const char *what) {
    if (got == want) {
        return;
    }
    fprintf(stderr, "FAIL: %s (got %lld, want %lld)\n", what, got, want);
    failures++;
}

/* ------------------------------------------------------------------------- *
 * Fixture
 * ------------------------------------------------------------------------- */

static void remove_db(void) {
    unlink(TEST_DB);
    unlink(TEST_DB "-wal");
    unlink(TEST_DB "-shm");
}

/* The DDL of the app BEFORE adult_clf_ver: the live torrents table, a
 * torrent_class that stops at the movie-kind columns, and classifier_meta. */
static const char *OLD_SHAPE_DDL =
    "CREATE TABLE torrents ("
    "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "    info_hash BLOB(20) NOT NULL UNIQUE,"
    "    name TEXT NOT NULL,"
    "    size_bytes INTEGER NOT NULL,"
    "    total_peers INTEGER DEFAULT 0,"
    "    added_timestamp INTEGER NOT NULL);"
    "CREATE TABLE torrent_class ("
    "    torrent_id INTEGER PRIMARY KEY REFERENCES torrents(id) ON DELETE CASCADE,"
    "    category INTEGER NOT NULL,"
    "    tv_kind INTEGER,"
    "    adult INTEGER NOT NULL DEFAULT 0,"
    "    adult_p REAL,"
    "    season_lo INTEGER, season_hi INTEGER,"
    "    episode_lo INTEGER, episode_hi INTEGER,"
    "    resolution INTEGER,"
    "    confidence REAL,"
    "    clf_ver INTEGER NOT NULL,"
    "    movie_kind_mask INTEGER, movie_kind_clf_ver INTEGER);"
    "CREATE TABLE classifier_meta (key TEXT PRIMARY KEY, value TEXT);";

static void exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "FAIL: sql failed: %s\n  %s\n", err ? err : "?", sql);
        if (err) {
            sqlite3_free(err);
        }
        failures++;
    }
}

/* Build the pre-patch-16 database: three torrents, three class rows at the old
 * generation, and NO adult_clf_ver column anywhere. */
static void create_old_shape_fixture(void) {
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    static const char *insert_torrent =
        "INSERT INTO torrents (id, info_hash, name, size_bytes, total_peers, added_timestamp)"
        " VALUES (?, ?, ?, ?, 0, 0)";
    static const char *insert_class =
        "INSERT INTO torrent_class (torrent_id, category, tv_kind, adult, adult_p, clf_ver)"
        " VALUES (?, 1, NULL, 0, 0.0, ?)";
    static const char *names[FIXTURE_TORRENTS] = {
        "alpha.jav.uncensored",
        "beta-site-rip",
        "gamma-control",
        "delta.uncensored.set.720p",
        "epsilon-documentary",
        "zeta-lecture-notes",
    };
    int i;

    remove_db();
    if (sqlite3_open(TEST_DB, &db) != SQLITE_OK) {
        fprintf(stderr, "FAIL: cannot create %s\n", TEST_DB);
        failures++;
        if (db) {
            sqlite3_close(db);
        }
        return;
    }
    exec_sql(db, OLD_SHAPE_DDL);

    if (sqlite3_prepare_v2(db, insert_torrent, -1, &stmt, NULL) == SQLITE_OK) {
        for (i = 0; i < FIXTURE_TORRENTS; i++) {
            unsigned char hash[20];
            memset(hash, (int)(0xA0 + i), sizeof hash);
            sqlite3_reset(stmt);
            sqlite3_bind_int64(stmt, 1, i + 1);
            sqlite3_bind_blob(stmt, 2, hash, (int)sizeof hash, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, names[i], -1, SQLITE_STATIC);
            sqlite3_bind_int64(stmt, 4, 1000 + i);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                fprintf(stderr, "FAIL: seeding torrents (%s)\n", sqlite3_errmsg(db));
                failures++;
            }
        }
        sqlite3_finalize(stmt);
    } else {
        fprintf(stderr, "FAIL: prepare insert torrent\n");
        failures++;
    }

    if (sqlite3_prepare_v2(db, insert_class, -1, &stmt, NULL) == SQLITE_OK) {
        for (i = 0; i < FIXTURE_TORRENTS; i++) {
            sqlite3_reset(stmt);
            sqlite3_bind_int64(stmt, 1, i + 1);
            sqlite3_bind_int64(stmt, 2, OLD_CLF_VER);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                fprintf(stderr, "FAIL: seeding torrent_class (%s)\n", sqlite3_errmsg(db));
                failures++;
            }
        }
        sqlite3_finalize(stmt);
    } else {
        fprintf(stderr, "FAIL: prepare insert class\n");
        failures++;
    }

    sqlite3_close(db);
}

/* Open the fixture through the product's own open + schema path. */
static int open_fixture(database_t *db) {
    if (database_init(db, TEST_DB, &test_app_ctx) != 0) {
        fprintf(stderr, "FAIL: database_init(%s)\n", TEST_DB);
        failures++;
        return -1;
    }
    if (database_create_schema(db) != 0) {
        fprintf(stderr, "FAIL: database_create_schema(%s)\n", TEST_DB);
        failures++;
        return -1;
    }
    return 0;
}

static int column_exists(sqlite3 *db, const char *table, const char *column) {
    sqlite3_stmt *stmt = NULL;
    char sql[128];
    int found = 0;

    snprintf(sql, sizeof sql, "PRAGMA table_info(%s)", table);
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *name = sqlite3_column_text(stmt, 1);
        if (name && strcmp((const char *)name, column) == 0) {
            found = 1;
            break;
        }
    }
    sqlite3_finalize(stmt);
    return found;
}

/* Read one torrent_class row: is adult_clf_ver NULL, and what is it worth. */
static void read_class_row(sqlite3 *db, long long id, int *is_null, long long *value,
                           int *adult, double *adult_p, long long *clf_ver) {
    sqlite3_stmt *stmt = NULL;
    static const char *sql =
        "SELECT adult_clf_ver, adult, adult_p, clf_ver FROM torrent_class WHERE torrent_id = ?";

    *is_null = 1;
    *value = 0;
    if (adult) *adult = 0;
    if (adult_p) *adult_p = 0.0;
    if (clf_ver) *clf_ver = 0;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: prepare class row read (%s)\n", sqlite3_errmsg(db));
        failures++;
        return;
    }
    sqlite3_bind_int64(stmt, 1, id);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        if (sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
            *is_null = 0;
            *value = sqlite3_column_int64(stmt, 0);
        }
        if (adult) *adult = sqlite3_column_int(stmt, 1);
        if (adult_p) *adult_p = sqlite3_column_double(stmt, 2);
        if (clf_ver) *clf_ver = sqlite3_column_int64(stmt, 3);
    } else {
        fprintf(stderr, "FAIL: torrent_class row %lld missing\n", id);
        failures++;
    }
    sqlite3_reset(stmt);
    sqlite3_finalize(stmt);
}

/* ------------------------------------------------------------------------- *
 * Canned verdict (the -DADULT_BACKFILL_TEST seam)
 * ------------------------------------------------------------------------- */

#define FAKE_ADULT 1
#define FAKE_ADULT_P 0.9375

static int fake_verdict_calls;

static int fake_verdict(const torrent_metadata_t *meta, int *adult, double *adult_p) {
    (void)meta;
    fake_verdict_calls++;
    if (adult) *adult = FAKE_ADULT;
    if (adult_p) *adult_p = FAKE_ADULT_P;
    return 0;
}

/* Same canned verdict, but it asks the pass to stop once it has produced
 * `stop_after_verdicts` verdicts - the cooperative cancel the engine honours
 * between committed batches. */
static int stop_after_verdicts;

static int stopping_verdict(const torrent_metadata_t *meta, int *adult, double *adult_p) {
    int rc = fake_verdict(meta, adult, adult_p);
    if (stop_after_verdicts > 0 && fake_verdict_calls >= stop_after_verdicts) {
        adult_backfill_request_stop();
    }
    return rc;
}

/* Canned gate (the -DADULT_BACKFILL_TEST gate seam). The model-free host build
 * has no tparse, so the real gate passes every row and a test could never see a
 * gate-out; this stands in for it with the same contract and the same three
 * token families the parser emits, read off the fixture's names. Stage 1 sees
 * the name alone; stage 2 also sees the file paths. */
static int gate_calls;

static int family_hit(const char *text) {
    return text != NULL &&
           (strstr(text, "jav") != NULL || strstr(text, "site") != NULL ||
            strstr(text, "set") != NULL);
}

static int fake_gate(const char *name, const file_info_t *files, int num_files,
                     long long size_bytes) {
    int i;

    (void)size_bytes;
    gate_calls++;
    if (family_hit(name)) {
        return 1;
    }
    for (i = 0; i < num_files; i++) {
        if (family_hit(files[i].path)) {
            return 1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------------- *
 * Tests
 * ------------------------------------------------------------------------- */

/* The migration half: database_create_schema() must widen a torrent_class that
 * predates adult_clf_ver, and must not disturb the rows already in it. */
static void test_additive_migration(void) {
    database_t db;
    int is_null = 0;
    long long value = 0;
    int adult = 0;
    double adult_p = 0.0;
    long long clf_ver = 0;

    create_old_shape_fixture();
    if (open_fixture(&db) != 0) {
        return;
    }

    check(column_exists(db.db, "torrent_class", "adult_clf_ver"),
          "migration adds torrent_class.adult_clf_ver", NULL);
    /* Idempotent: a second run on the widened table must be a no-op. */
    check(database_create_schema(&db) == 0, "migration is idempotent", NULL);

    /* Every pre-existing row survives untouched, and starts as a candidate. */
    read_class_row(db.db, TORRENT_A, &is_null, &value, &adult, &adult_p, &clf_ver);
    check(is_null == 1, "pre-existing row has adult_clf_ver NULL", NULL);
    check_ll(clf_ver, OLD_CLF_VER, "pre-existing row keeps its clf_ver");
    check_ll(adult, 0, "pre-existing row keeps its adult flag");
    check_ll((long long)(adult_p * 1000.0), 0, "pre-existing row keeps its adult_p");

    database_cleanup(&db);
    remove_db();
}

/* Predicate + UPDATE + watermark, on one fixture: two of the three rows are
 * candidates (NULL and stale), one is already at the target. */
static void test_write_and_watermark(void) {
    database_t db;
    adult_backfill_opts_t opts;
    adult_backfill_progress_t progress;
    long long rows_left = -1;
    int needed;
    int is_null = 0;
    long long value = 0;
    int adult = 0;
    double adult_p = 0.0;
    long long clf_ver = 0;
    char detail[128];

    create_old_shape_fixture();
    if (open_fixture(&db) != 0) {
        return;
    }

    /* Row A stays NULL (never adult-scored); row B carries a stale adult
     * generation; row C is already at the target and must be left alone. */
    snprintf(detail, sizeof detail,
             "UPDATE torrent_class SET adult_clf_ver = %d WHERE torrent_id = %d",
             STALE_ADULT_VER, TORRENT_B);
    exec_sql(db.db, detail);
    snprintf(detail, sizeof detail,
             "UPDATE torrent_class SET adult_clf_ver = %d WHERE torrent_id = %d",
             STUB_TARGET, TORRENT_C);
    exec_sql(db.db, detail);

    needed = adult_backfill_needed(&db, 0, &rows_left);
    check(needed == 1, "pass is needed while candidates exist", NULL);
    check_ll(rows_left, FIXTURE_TORRENTS - 1, "candidate count = NULL rows + stale row");

    adult_backfill_set_verdict_hook(fake_verdict);
    fake_verdict_calls = 0;

    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);
    check(adult_backfill_run(&db, &opts, &progress) == 0, "write pass returns 0", NULL);
    check_ll(progress.rows_total, FIXTURE_TORRENTS - 1, "write pass counted 5 candidates");
    check_ll(progress.rows_scored, FIXTURE_TORRENTS - 1, "write pass scored 5 rows");
    check_ll(progress.rows_gated_out, 0, "no gate in the model-free host build");
    check_ll(progress.rows_updated, FIXTURE_TORRENTS - 1, "write pass updated 5 rows");
    check_ll(progress.rows_failed, 0, "write pass failed no row");
    check_ll(progress.target_adult_clf_ver, STUB_TARGET,
             "target comes from the classifier, never a literal");
    check_ll(fake_verdict_calls, FIXTURE_TORRENTS - 1,
             "the verdict hook ran once per candidate");

    /* The UPDATE wrote the adult verdict and the pass generation - and nothing
     * else. clf_ver must be untouched: this pass never re-classifies. */
    read_class_row(db.db, TORRENT_A, &is_null, &value, &adult, &adult_p, &clf_ver);
    check(is_null == 0, "row A is no longer a candidate", NULL);
    check_ll(value, STUB_TARGET, "row A stamped with the pass target");
    check_ll(adult, FAKE_ADULT, "row A carries the adult verdict");
    check_ll((long long)(adult_p * 10000.0), (long long)(FAKE_ADULT_P * 10000.0),
             "row A carries the adult probability");
    check_ll(clf_ver, OLD_CLF_VER, "row A keeps its category generation");

    read_class_row(db.db, TORRENT_B, &is_null, &value, &adult, &adult_p, &clf_ver);
    check_ll(value, STUB_TARGET, "row B (stale) is stamped with the pass target");

    /* Row C was already at the target: it must not even be visited. */
    read_class_row(db.db, TORRENT_C, &is_null, &value, &adult, &adult_p, &clf_ver);
    check_ll(value, STUB_TARGET, "row C keeps its generation");
    check_ll(adult, 0, "row C was not re-scored");

    /* Watermark: nothing is left, so a second run selects nothing. */
    needed = adult_backfill_needed(&db, 0, &rows_left);
    check(needed == 0, "second pass is not needed", NULL);
    check_ll(rows_left, 0, "second pass has no candidates");

    memset(&progress, 0, sizeof progress);
    fake_verdict_calls = 0;
    check(adult_backfill_run(&db, &opts, &progress) == 0, "second pass returns 0", NULL);
    check_ll(progress.rows_total, 0, "second pass counts nothing");
    check_ll(progress.rows_updated, 0, "second pass updates nothing");
    check_ll(fake_verdict_calls, 0, "second pass scores nothing");

    /* The completion path tombstones the resume key, so a later generation
     * starts from the beginning rather than from a stale id. */
    check_ll(progress.state, ADULT_BACKFILL_DONE, "second pass reports done");

    database_cleanup(&db);
    remove_db();
}

/* The dry run walks the SAME two-stage token gate the write pass walks and
 * commits nothing - that is what makes `total - gated_out` the gated-in
 * candidate set the G3 band is judged on, before any write. */
static void test_dry_run_writes_nothing(void) {
    database_t db;
    adult_backfill_opts_t opts;
    adult_backfill_progress_t progress;
    int is_null = 0;
    long long value = 0;
    int i;

    create_old_shape_fixture();
    if (open_fixture(&db) != 0) {
        return;
    }

    adult_backfill_set_verdict_hook(fake_verdict);
    adult_backfill_set_gate_hook(fake_gate);
    fake_verdict_calls = 0;
    gate_calls = 0;

    memset(&opts, 0, sizeof opts);
    opts.dry_run = 1;
    memset(&progress, 0, sizeof progress);

    check(adult_backfill_run(&db, &opts, &progress) == 0, "dry run returns 0", NULL);
    check_ll(progress.state, ADULT_BACKFILL_DONE, "dry run reports done");
    check_ll(progress.rows_total, FIXTURE_TORRENTS,
             "dry run counts every predicate candidate");
    check_ll(progress.rows_gated_out, FIXTURE_GATED_OUT,
             "dry run gates out the three __adult_-free names");
    check_ll(progress.rows_total - progress.rows_gated_out, FIXTURE_GATED_IN,
             "total - gated_out is the gated-in candidate set");
    check_ll(progress.rows_scored, 0, "dry run scores nothing");
    check_ll(progress.rows_updated, 0, "dry run updates nothing");
    check_ll(progress.rows_failed, 0, "dry run fails nothing");
    check_ll(fake_verdict_calls, 0, "dry run runs no inference");
    /* Two gate stages for every row that passes stage 1, one for every row that
     * fails it: proof the dry run walks the full gate and short-circuits it. */
    check_ll(gate_calls, FIXTURE_GATED_IN * 2 + FIXTURE_GATED_OUT,
             "dry run runs both gate stages for passers only");

    /* And the table is untouched: every row is still a candidate. */
    for (i = 1; i <= FIXTURE_TORRENTS; i++) {
        read_class_row(db.db, i, &is_null, &value, NULL, NULL, NULL);
        check(is_null == 1, "dry run left every row a candidate", NULL);
    }

    adult_backfill_set_gate_hook(NULL);

    database_cleanup(&db);
    remove_db();
}

/* The other half of the G3 claim: on the same fixture the write pass scores
 * exactly the gated-in count the dry run reported, and stamps exactly those
 * rows. If the two numbers could differ the gate would be worthless. */
static void test_write_scores_the_dry_runs_gated_in_set(void) {
    database_t db;
    adult_backfill_opts_t opts;
    adult_backfill_progress_t progress;
    long long dry_gated_in;
    int is_null = 0;
    long long value = 0;

    create_old_shape_fixture();
    if (open_fixture(&db) != 0) {
        return;
    }

    adult_backfill_set_verdict_hook(fake_verdict);
    adult_backfill_set_gate_hook(fake_gate);

    /* Dry run first: record what the gate lets through. */
    memset(&opts, 0, sizeof opts);
    opts.dry_run = 1;
    memset(&progress, 0, sizeof progress);
    check(adult_backfill_run(&db, &opts, &progress) == 0, "dry run returns 0", NULL);
    dry_gated_in = progress.rows_total - progress.rows_gated_out;
    check_ll(dry_gated_in, FIXTURE_GATED_IN, "dry run reports the gated-in set");

    /* Then the write pass on the same fixture. */
    fake_verdict_calls = 0;
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);
    check(adult_backfill_run(&db, &opts, &progress) == 0, "write pass returns 0", NULL);
    check_ll(progress.state, ADULT_BACKFILL_DONE, "write pass reports done");
    check_ll(progress.rows_total, FIXTURE_TORRENTS, "write pass sees the same candidates");
    check_ll(progress.rows_gated_out, FIXTURE_GATED_OUT, "write pass gates out the same rows");
    check_ll(progress.rows_scored, dry_gated_in,
             "write pass scores exactly the dry run's gated-in count");
    check_ll(progress.rows_updated, dry_gated_in, "write pass updates exactly those rows");
    check_ll(fake_verdict_calls, dry_gated_in, "one verdict per gated-in row");

    /* Exactly the gated-in rows carry the pass target; the rest stay candidates. */
    read_class_row(db.db, TORRENT_A, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "row A (jav) was stamped");
    read_class_row(db.db, TORRENT_B, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "row B (site) was stamped");
    read_class_row(db.db, TORRENT_C, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "row C (control) was not stamped", NULL);
    read_class_row(db.db, TORRENT_D, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "row D (set) was stamped");
    read_class_row(db.db, TORRENT_E, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "row E (control) was not stamped", NULL);
    read_class_row(db.db, TORRENT_F, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "row F (control) was not stamped", NULL);

    adult_backfill_set_gate_hook(NULL);

    database_cleanup(&db);
    remove_db();
}

/* Stop after the first committed batch, then resume: the watermark must make the
 * second run pick up after the last committed id, so no row is scored twice and
 * the final row set is the one an uninterrupted pass produces. */
static void test_resume_after_stop(void) {
    database_t db;
    adult_backfill_opts_t opts;
    adult_backfill_progress_t progress;
    int is_null = 0;
    long long value = 0;

    create_old_shape_fixture();
    if (open_fixture(&db) != 0) {
        return;
    }

    adult_backfill_set_verdict_hook(stopping_verdict);
    adult_backfill_set_gate_hook(fake_gate);
    fake_verdict_calls = 0;
    gate_calls = 0;
    stop_after_verdicts = 1;

    memset(&opts, 0, sizeof opts);
    opts.batch_rows = 1; /* one scored row per committed batch */
    memset(&progress, 0, sizeof progress);

    check(adult_backfill_run(&db, &opts, &progress) == 0, "stopped pass returns 0", NULL);
    check_ll(progress.state, ADULT_BACKFILL_RUNNING, "stopped pass stays RUNNING");
    check_ll(progress.rows_scored, 1, "the stop landed after the first committed batch");
    check_ll(progress.rows_updated, 1, "that batch was committed");
    check_ll(progress.last_id, TORRENT_A, "the watermark is the last committed id");
    check_ll(fake_verdict_calls, 1, "one row was scored before the stop");

    read_class_row(db.db, TORRENT_A, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "the committed batch was written");
    read_class_row(db.db, TORRENT_B, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "the row after the stop was not written", NULL);

    /* Resume, no stop this time. */
    stop_after_verdicts = 0;
    memset(&progress, 0, sizeof progress);
    check(adult_backfill_run(&db, &opts, &progress) == 0, "resumed pass returns 0", NULL);
    check_ll(progress.state, ADULT_BACKFILL_DONE, "resumed pass completes");
    check_ll(progress.rows_total, FIXTURE_TORRENTS - 1,
             "the resumed pass only counts rows after the watermark");
    check_ll(progress.rows_scored, FIXTURE_GATED_IN - 1,
             "the resumed pass scores the remaining gated-in rows");
    check_ll(progress.rows_updated, FIXTURE_GATED_IN - 1, "and updates them");
    check_ll(progress.last_id, TORRENT_D, "the watermark ends at the last stamped id");
    /* No row was scored twice: the verdict calls across both runs add up to the
     * gated-in count of one uninterrupted pass. */
    check_ll(fake_verdict_calls, FIXTURE_GATED_IN,
             "no row was scored twice across the stop and the resume");

    /* Final row set identical to an uninterrupted run. */
    read_class_row(db.db, TORRENT_A, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "row A stamped");
    read_class_row(db.db, TORRENT_B, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "row B stamped by the resumed pass");
    read_class_row(db.db, TORRENT_C, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "row C (control) never stamped", NULL);
    read_class_row(db.db, TORRENT_D, &is_null, &value, NULL, NULL, NULL);
    check_ll(value, STUB_TARGET, "row D stamped by the resumed pass");
    read_class_row(db.db, TORRENT_E, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "row E (control) never stamped", NULL);
    read_class_row(db.db, TORRENT_F, &is_null, &value, NULL, NULL, NULL);
    check(is_null == 1, "row F (control) never stamped", NULL);

    adult_backfill_set_gate_hook(NULL);

    database_cleanup(&db);
    remove_db();
}

/* The engine declines before touching the database when no models are loaded:
 * a backfill that cannot run must never look like a success. */
static void test_disabled_without_models(void) {
    database_t db;
    adult_backfill_progress_t progress;
    long long rows_left = -1;

    create_old_shape_fixture();
    if (open_fixture(&db) != 0) {
        return;
    }

    /* Rebuild the fixture so the DB handle is real but the classifier is off:
     * the stub answers models_loaded from this flag. */
    extern int test_models_loaded;
    test_models_loaded = 0;
    check(adult_backfill_needed(&db, 0, &rows_left) == 0, "needed is 0 without models", NULL);

    memset(&progress, 0, sizeof progress);
    check(adult_backfill_run(&db, NULL, &progress) == 1, "run reports DISABLED (1)", NULL);
    check_ll(progress.state, ADULT_BACKFILL_DISABLED, "state is disabled");
    check_ll(progress.rows_updated, 0, "disabled run writes nothing");
    test_models_loaded = 1;

    database_cleanup(&db);
    remove_db();
}

/* ------------------------------------------------------------------------- *
 * Classifier symbols: src/adult_backfill.c references these. They answer from
 * the fakes, so nothing here needs model files.
 * ------------------------------------------------------------------------- */

int test_models_loaded = 1;

int classifier_models_loaded(void) {
    return test_models_loaded;
}

int classifier_enabled(void) {
    return test_models_loaded;
}

long long classifier_current_version(void) {
    return STUB_TARGET;
}

void classifier_write_meta(sqlite3 *db) {
    (void)db;
}

int classifier_classify_adult(const torrent_metadata_t *meta, int *adult, double *adult_p) {
    /* Reached only when the test has not installed the hook; the engine must
     * then leave the stored row alone rather than fabricate a verdict. */
    (void)meta;
    if (adult) *adult = 0;
    if (adult_p) *adult_p = 0.0;
    return 1;
}

int main(void) {
    memset(&test_app_ctx, 0, sizeof test_app_ctx);

    test_additive_migration();
    test_write_and_watermark();
    test_dry_run_writes_nothing();
    test_write_scores_the_dry_runs_gated_in_set();
    test_resume_after_stop();
    test_disabled_without_models();

    remove_db();

    if (failures != 0) {
        fprintf(stderr, "adult-backfill test: %d failure(s)\n", failures);
        return 1;
    }
    printf("adult-backfill test: all checks passed\n");
    return 0;
}
