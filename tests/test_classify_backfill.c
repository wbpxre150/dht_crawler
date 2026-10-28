/**
 * Host unit test for the generic, resumable classify-backfill engine
 * (include/classify_backfill.h, src/classify_backfill.c).
 *
 * Exercises the real product code, not a copy of it:
 *   - the engine's own selection predicate, window walk, batch upsert and
 *     completion bookkeeping, against the schema database_create_schema() builds;
 *   - the path reconstruction the engine feeds the classifier: the fixture's
 *     paths are handed to database_insert_batch(), i.e. through the real
 *     split_path()/path_prefixes/torrent_files round trip, and the stub below
 *     records exactly what the engine hands back to the classifier — including
 *     the root-level file that is stored with prefix_id IS NULL;
 *   - the fixture's seed writes the pre-existing rows through the ingest path,
 *     so "untouched" and "no writes" assertions compare against real rows.
 *
 * The four classifier symbols the engine references are defined at the bottom of
 * this file instead of linking src/classifier.c: the host build compiles that
 * module without CLF_ENABLED, i.e. as inert stubs that can never produce a
 * verdict (same trick tests/stub_log.c plays for log_msg()). The stub reports
 * generation 7 — deliberately NOT 4 — so a passing run proves the engine is
 * model-agnostic: it labels rows with whatever generation it is told, and it
 * never hardcodes one.
 *
 * Fixture (/tmp/classify-backfill-test.db, removed before and after the run):
 *   torrent 1 "alpha-prefixed"  class row at clf_ver=3; files dir1/file1.mkv,
 *                               dir1/file2.mkv, rootfile.exe (the last one has no
 *                               '/' and is therefore stored with prefix_id NULL)
 *   torrent 2 "beta-no-class"   no class row at all; file solo.bin
 *   torrent 3 "FAIL-gamma"      class row at clf_ver=3; file g.mkv
 * The stub refuses a verdict for any name starting with "FAIL", so torrent 3 is
 * the "classification cannot run" case: its stored row must survive untouched.
 *
 * Standalone: no test framework, no dependency on the daemon build (the test
 * links src/classify_backfill.c, src/database.c and src/bloom_filter.c directly,
 * with tests/stub_log.c for log_msg()).
 *
 *     make tests        # or: make tests-backfill / make tests-asan-backfill
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "classifier.h"
#include "classify_backfill.h"
#include "database.h"
#include "dht_crawler.h"

#define TEST_DB "/tmp/classify-backfill-test.db"

/* Generation the stub classifier reports. NOT 4 on purpose: see the header. */
#define STUB_CLF_VER 7
/* Generation the fixture's pre-existing rows were labelled with. */
#define OLD_CLF_VER 3

#define TORRENT_A 1
#define TORRENT_B 2
#define TORRENT_C 3
#define FIXTURE_TORRENTS 3

/* The stub's canned verdict. Every value differs from what the fixture's own
 * rows carry, so an untouched row can never pass by coincidence. */
#define STUB_CATEGORY 9
#define STUB_RESOLUTION 2160
#define STUB_CONFIDENCE 0.875
#define STUB_ADULT_P 0.125

#define STUB_MAX_CALLS 8
#define STUB_MAX_FILES 8
#define STUB_PATH_MAX 256
#define STUB_NAME_MAX 128

/* One recorded classify() call: everything the engine fed the classifier. */
typedef struct {
    char name[STUB_NAME_MAX];
    long long size_bytes;
    int num_files;
    char paths[STUB_MAX_FILES][STUB_PATH_MAX];
} stub_call_t;

static int stub_models_loaded = 1;
static int stub_fail_all = 0;
static int stub_write_meta_calls = 0;
static stub_call_t stub_calls[STUB_MAX_CALLS];
static int stub_call_count;

/* --- live-progress case (10) state ---------------------------------------
 * The poller thread below reads the published snapshot while a pass runs; the
 * stub classifier stalls on a name starting with "SLOW" until the poller has
 * actually observed the pass, so the case cannot race the engine. */
static volatile int stub_stall_for_poller = 0;
static volatile int poller_stop_flag = 0;
static volatile int poller_saw_running = 0;
static volatile int poller_monotonic = 1;
static volatile long long poller_max_done = -1;

/* database_init() stores the pointer, so this has to outlive every fixture. */
static app_context_t test_app_ctx;

/* Failing assert: print it and exit non-zero (no test framework here). */
static void fail(const char *what, const char *detail) {
    fprintf(stderr, "test_classify_backfill: FAILED: %s", what);
    if (detail && *detail) {
        fprintf(stderr, ": %s", detail);
    }
    fprintf(stderr, "\n");
    exit(1);
}

/* Run `sql` and return column 0 of the first row (SQL NULL -> 0). */
static sqlite3_int64 scalar_sql(sqlite3 *db, const char *sql, const char *what) {
    sqlite3_stmt *stmt = NULL;
    sqlite3_int64 value = 0;
    int rc;

    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fail(what, sqlite3_errmsg(db));
    }
    rc = sqlite3_step(stmt);
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
        snprintf(detail, sizeof detail, "got %lld, want %lld", (long long)got,
                 (long long)want);
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

/* ------------------------------------------------------------------------- *
 * torrent_class row readback
 * ------------------------------------------------------------------------- */

typedef struct {
    long long category;
    int tv_kind_is_null;      /* the engine binds SQL NULL for tv_kind <= 0 */
    long long tv_kind;        /* COALESCE(tv_kind, -1) */
    long long adult;
    double adult_p;
    long long season_lo, season_hi, episode_lo, episode_hi;
    long long resolution;
    double confidence;
    long long clf_ver;
} class_row_t;

static void expect_class_row(sqlite3 *db, long long torrent_id, const class_row_t *want,
                             const char *what) {
    static const char *const SQL =
        "SELECT category, tv_kind IS NULL, COALESCE(tv_kind, -1), adult, adult_p, "
        "season_lo, season_hi, episode_lo, episode_hi, resolution, confidence, clf_ver "
        "FROM torrent_class WHERE torrent_id = ?";
    sqlite3_stmt *stmt = NULL;
    class_row_t got;
    char detail[320];
    int rc;

    memset(&got, 0, sizeof got);
    if (sqlite3_prepare_v2(db, SQL, -1, &stmt, NULL) != SQLITE_OK) {
        fail(what, sqlite3_errmsg(db));
    }
    sqlite3_bind_int64(stmt, 1, torrent_id);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        snprintf(detail, sizeof detail,
                 "no torrent_class row for torrent %lld (step rc=%d)", torrent_id, rc);
        fail(what, detail);
    }
    got.category = sqlite3_column_int64(stmt, 0);
    got.tv_kind_is_null = sqlite3_column_int(stmt, 1);
    got.tv_kind = sqlite3_column_int64(stmt, 2);
    got.adult = sqlite3_column_int64(stmt, 3);
    got.adult_p = sqlite3_column_double(stmt, 4);
    got.season_lo = sqlite3_column_int64(stmt, 5);
    got.season_hi = sqlite3_column_int64(stmt, 6);
    got.episode_lo = sqlite3_column_int64(stmt, 7);
    got.episode_hi = sqlite3_column_int64(stmt, 8);
    got.resolution = sqlite3_column_int64(stmt, 9);
    got.confidence = sqlite3_column_double(stmt, 10);
    got.clf_ver = sqlite3_column_int64(stmt, 11);
    sqlite3_finalize(stmt);

    if (got.category != want->category || got.tv_kind_is_null != want->tv_kind_is_null ||
        got.tv_kind != want->tv_kind || got.adult != want->adult ||
        got.adult_p != want->adult_p || got.season_lo != want->season_lo ||
        got.season_hi != want->season_hi || got.episode_lo != want->episode_lo ||
        got.episode_hi != want->episode_hi || got.resolution != want->resolution ||
        got.confidence != want->confidence || got.clf_ver != want->clf_ver) {
        snprintf(detail, sizeof detail,
                 "torrent %lld row is cat=%lld tv_null=%d tv=%lld adult=%lld "
                 "adult_p=%.6f s=%lld/%lld e=%lld/%lld res=%lld conf=%.6f ver=%lld",
                 torrent_id, got.category, got.tv_kind_is_null, got.tv_kind, got.adult,
                 got.adult_p, got.season_lo, got.season_hi, got.episode_lo,
                 got.episode_hi, got.resolution, got.confidence, got.clf_ver);
        fail(what, detail);
    }
}

/* The values ingest wrote for the fixture's torrent `id`: make_torrent() fills
 * the classification_t below from the same formulas, so this mirrors it. */
static void seeded_row(long long id, class_row_t *row) {
    memset(row, 0, sizeof *row);
    row->category = 1 + id;
    row->tv_kind_is_null = 1;
    row->tv_kind = -1;
    row->adult = 0;
    row->adult_p = 0.25 * (double)id;
    row->season_lo = id;
    row->season_hi = id + 1;
    row->episode_lo = id + 2;
    row->episode_hi = id + 3;
    row->resolution = 720;
    row->confidence = 0.5;
    row->clf_ver = OLD_CLF_VER;
}

/* The stub classifier's canned verdict, as the engine stores it. */
static void stub_row(class_row_t *row) {
    memset(row, 0, sizeof *row);
    row->category = STUB_CATEGORY;
    row->tv_kind_is_null = 1;   /* tv_kind 0 (TP_TV_NONE) is stored as SQL NULL */
    row->tv_kind = -1;
    row->adult = 0;
    row->adult_p = STUB_ADULT_P;
    row->season_lo = 11;
    row->season_hi = 12;
    row->episode_lo = 21;
    row->episode_hi = 22;
    row->resolution = STUB_RESOLUTION;
    row->confidence = STUB_CONFIDENCE;
    row->clf_ver = STUB_CLF_VER;
}

/* ------------------------------------------------------------------------- *
 * Fixture
 * ------------------------------------------------------------------------- */

static const char *const FILES_A[] = {"dir1/file1.mkv", "dir1/file2.mkv", "rootfile.exe"};
static const char *const FILES_B[] = {"solo.bin"};
static const char *const FILES_C[] = {"g.mkv"};

/* Heap-allocate one torrent_metadata_t (and its file array) per the seed spec.
 * `has_classification` mirrors what ingest decides: 1 writes the torrent_class
 * row (labelled with `clf_ver`), 0 leaves the torrent without one. */
static torrent_metadata_t *make_torrent(int id, const char *name,
                                        const char *const *paths, int num_files,
                                        int has_classification, long long clf_ver) {
    torrent_metadata_t *torrent = calloc(1, sizeof(*torrent));
    if (!torrent) {
        fail("calloc torrent_metadata_t", NULL);
    }

    /* Distinct info_hash per torrent: the id little-endian in the first two
     * bytes, then a fixed pattern (a collision would let INSERT OR REPLACE on
     * torrents replace an earlier row and shift every expected id). */
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
    torrent->has_classification = has_classification;
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

    if (has_classification) {
        /* seeded_row() mirrors these formulas — keep the two in step. */
        torrent->classification.category = 1 + id;
        torrent->classification.tv_kind = 0;
        torrent->classification.adult = 0;
        torrent->classification.adult_p = 0.25 * (double)id;
        torrent->classification.season_lo = id;
        torrent->classification.season_hi = id + 1;
        torrent->classification.episode_lo = id + 2;
        torrent->classification.episode_hi = id + 3;
        torrent->classification.resolution = 720;
        torrent->classification.confidence = 0.5;
        torrent->classification.clf_ver = clf_ver;
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

/* Fresh scratch database seeded with the three fixture torrents, through the
 * same entry point batch_writer_flush() uses (one BEGIN ... COMMIT).
 * `torrent_c_name` decides whether the third torrent is one the stub classifier
 * refuses ("FAIL...") or one it can label. */
static void build_fixture_named(database_t *db, const char *torrent_c_name) {
    torrent_metadata_t *seed[FIXTURE_TORRENTS];
    size_t written = 0;

    remove_scratch_db();

    if (database_init(db, TEST_DB, &test_app_ctx) != 0) {
        fail("database_init", TEST_DB);
    }
    if (database_create_schema(db) != 0) {
        fail("database_create_schema", TEST_DB);
    }

    seed[0] = make_torrent(TORRENT_A, "alpha-prefixed", FILES_A, 3, 1, OLD_CLF_VER);
    seed[1] = make_torrent(TORRENT_B, "beta-no-class", FILES_B, 1, 0, 0);
    seed[2] = make_torrent(TORRENT_C, torrent_c_name, FILES_C, 1, 1, OLD_CLF_VER);

    if (database_insert_batch(db, seed, FIXTURE_TORRENTS, &written) != 0) {
        fail("database_insert_batch", "returned -1");
    }
    if (written != FIXTURE_TORRENTS) {
        char detail[128];
        snprintf(detail, sizeof detail, "wrote %zu of %d", written, FIXTURE_TORRENTS);
        fail("database_insert_batch item count", detail);
    }
    for (int i = 0; i < FIXTURE_TORRENTS; i++) {
        free_torrent(seed[i]);
    }
}

/* The fixture of the plan: torrent C's classification cannot run, so its stored
 * row has to survive a pass untouched. */
static void build_fixture(database_t *db) {
    build_fixture_named(db, "FAIL-gamma");
}

/* The same three torrents, but every one of them gets a verdict, so a pass
 * converts the whole database. This is the fixture a "the second run is a
 * no-op" assertion needs: with a permanently unclassifiable torrent in the
 * database the engine keeps offering it as a candidate (it never gives up on a
 * row), so rows_total can only reach 0 once nothing is left below the target. */
static void build_convertible_fixture(database_t *db) {
    build_fixture_named(db, "gamma-convertible");
}

/* The fixture as the engine must see it before any pass: two rows at the old
 * generation, torrent B unlabelled. Used by the "wrote nothing" cases. */
static void expect_untouched_fixture(sqlite3 *db, const char *what) {
    class_row_t row;

    seeded_row(TORRENT_A, &row);
    expect_class_row(db, TORRENT_A, &row, what);
    seeded_row(TORRENT_C, &row);
    expect_class_row(db, TORRENT_C, &row, what);
    expect_count(db, "SELECT count(*) FROM torrent_class WHERE torrent_id = 2", 0, what);
}

/* ------------------------------------------------------------------------- *
 * Stub classifier (the four symbols the engine references)
 * ------------------------------------------------------------------------- */

/* Record every input the engine feeds classify(): the caller's name, its size
 * and each file path, copied into bounded buffers. */
static void record_call(const torrent_metadata_t *meta) {
    stub_call_t *call;
    int i;

    if (stub_call_count >= STUB_MAX_CALLS) {
        stub_call_count++;
        return;
    }
    call = &stub_calls[stub_call_count];
    memset(call, 0, sizeof *call);
    snprintf(call->name, sizeof call->name, "%s", meta->name ? meta->name : "");
    call->size_bytes = (long long)meta->size_bytes;
    call->num_files = meta->num_files;
    if (meta->files) {
        for (i = 0; i < meta->num_files && i < STUB_MAX_FILES; i++) {
            snprintf(call->paths[i], STUB_PATH_MAX, "%s",
                     meta->files[i].path ? meta->files[i].path : "");
        }
    }
    stub_call_count++;
}

static const stub_call_t *find_call(const char *name) {
    for (int i = 0; i < stub_call_count && i < STUB_MAX_CALLS; i++) {
        if (strcmp(stub_calls[i].name, name) == 0) {
            return &stub_calls[i];
        }
    }
    return NULL;
}

int classifier_models_loaded(void) {
    return stub_models_loaded;
}

long long classifier_current_version(void) {
    return STUB_CLF_VER;
}

/* Mirror the real classifier_write_meta() for the one key /stats reads: the
 * generation the rows were just labelled with. */
void classifier_write_meta(sqlite3 *db) {
    static const char *const SQL =
        "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES ('clf_ver', ?)";
    sqlite3_stmt *stmt = NULL;
    char value[16];

    stub_write_meta_calls++;
    if (!db) {
        return;
    }
    if (sqlite3_prepare_v2(db, SQL, -1, &stmt, NULL) != SQLITE_OK) {
        return;   /* best effort, exactly like the real implementation */
    }
    snprintf(value, sizeof value, "%d", STUB_CLF_VER);
    sqlite3_bind_text(stmt, 1, value, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

/* 0 + a canned verdict, or 1 ("no classification available") when the caller
 * asked for a failure or the torrent name starts with FAIL. */
int classifier_classify(const torrent_metadata_t *meta, classification_t *out) {
    if (!meta || !out) {
        return 1;
    }
    record_call(meta);

    /* Case 10 only: hold the pass inside the classifier until the poller thread
     * has read a RUNNING snapshot that carries progress. Bounded, so a broken
     * accessor fails the case instead of hanging the suite. */
    if (stub_stall_for_poller && meta->name && strncmp(meta->name, "SLOW", 4) == 0) {
        for (int waited_us = 0; waited_us < 2000000 && poller_max_done < 1;
             waited_us += 1000) {
            usleep(1000);
        }
    }

    if (stub_fail_all || (meta->name && strncmp(meta->name, "FAIL", 4) == 0)) {
        return 1;
    }

    memset(out, 0, sizeof(*out));
    out->category = STUB_CATEGORY;
    out->tv_kind = 0;
    out->adult = 0;
    out->adult_p = STUB_ADULT_P;
    out->season_lo = 11;
    out->season_hi = 12;
    out->episode_lo = 21;
    out->episode_hi = 22;
    out->resolution = STUB_RESOLUTION;
    out->confidence = STUB_CONFIDENCE;
    out->clf_ver = STUB_CLF_VER;
    return 0;
}

/* ------------------------------------------------------------------------- *
 * Cases
 * ------------------------------------------------------------------------- */

/* 1: the decision helper sees three torrents behind the loaded generation. */
static void case_needed_before(database_t *db) {
    long long left = -1;

    if (classify_backfill_needed(db, 0, &left) != 1) {
        fail("classify_backfill_needed before the pass", "returned 0");
    }
    if (left != FIXTURE_TORRENTS) {
        char detail[64];
        snprintf(detail, sizeof detail, "rows_left=%lld, want %d", left,
                 FIXTURE_TORRENTS);
        fail("classify_backfill_needed rows_left", detail);
    }
    printf("  case 1 ok: needed=1 rows_left=%lld\n", left);
}

/* 2: dry run counts the candidates and writes (and classifies) nothing. */
static void case_dry_run(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    opts.dry_run = 1;
    stub_call_count = 0;

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("dry run", "returned non-zero");
    }
    if (prog.state != CLASSIFY_BACKFILL_DONE) {
        fail("dry run state", classify_backfill_state_name(prog.state));
    }
    if (prog.rows_total != FIXTURE_TORRENTS) {
        char detail[64];
        snprintf(detail, sizeof detail, "rows_total=%lld", prog.rows_total);
        fail("dry run candidate count", detail);
    }
    if (prog.rows_done != 0 || prog.rows_updated != 0 || prog.rows_inserted != 0 ||
        prog.rows_failed != 0) {
        fail("dry run counters", "a dry run must not process or write a row");
    }
    if (stub_call_count != 0) {
        fail("dry run classified", "a dry run must not call the classifier");
    }
    expect_untouched_fixture(db->db, "fixture after the dry run");
    printf("  case 2 ok: dry run counted %lld candidates and wrote nothing\n",
           prog.rows_total);
}

/* 3 + 4 + 5: one real pass — path parity, write outcomes, tombstone + meta. */
static void case_write_pass(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    const stub_call_t *call;
    class_row_t row;

    memset(&opts, 0, sizeof opts);   /* defaults: write mode, unlimited */
    memset(&prog, 0, sizeof prog);
    stub_call_count = 0;
    stub_write_meta_calls = 0;

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("write pass", "returned non-zero");
    }
    if (prog.state != CLASSIFY_BACKFILL_DONE) {
        fail("write pass state", classify_backfill_state_name(prog.state));
    }
    if (prog.target_clf_ver != STUB_CLF_VER) {
        char detail[64];
        snprintf(detail, sizeof detail, "target_clf_ver=%lld", prog.target_clf_ver);
        fail("write pass target generation", detail);
    }
    if (prog.rows_total != FIXTURE_TORRENTS || prog.rows_done != FIXTURE_TORRENTS) {
        char detail[96];
        snprintf(detail, sizeof detail, "total=%lld done=%lld", prog.rows_total,
                 prog.rows_done);
        fail("write pass candidate counters", detail);
    }

    /* --- 3: path parity ------------------------------------------------ */
    call = find_call("alpha-prefixed");
    if (!call) {
        fail("path parity", "the engine never classified torrent A");
    }
    if (call->size_bytes != 1024 * TORRENT_A) {
        char detail[96];
        snprintf(detail, sizeof detail, "size_bytes=%lld", call->size_bytes);
        fail("path parity: torrent A size fed to the classifier", detail);
    }
    if (call->num_files != 3) {
        char detail[96];
        snprintf(detail, sizeof detail, "num_files=%d, want 3", call->num_files);
        fail("path parity: torrent A file count", detail);
    }
    for (int i = 0; i < 3; i++) {
        if (strcmp(call->paths[i], FILES_A[i]) != 0) {
            char detail[STUB_PATH_MAX + 128];
            snprintf(detail, sizeof detail, "file %d is \"%s\", want \"%s\" (the full "
                     "path must be reproduced from prefix + leaf)", i, call->paths[i],
                     FILES_A[i]);
            fail("path parity: torrent A file paths", detail);
        }
    }
    printf("  case 3 ok: torrent A reconstructed as {%s, %s, %s} (root file has no "
           "leading '/')\n", call->paths[0], call->paths[1], call->paths[2]);

    /* --- 4: write outcomes --------------------------------------------- */
    if (prog.rows_updated < 1) {
        fail("write pass rows_updated", "torrent A's old row was not replaced");
    }
    if (prog.rows_inserted != 1) {
        char detail[64];
        snprintf(detail, sizeof detail, "rows_inserted=%lld, want 1", prog.rows_inserted);
        fail("write pass rows_inserted", detail);
    }
    if (prog.rows_failed != 1) {
        char detail[64];
        snprintf(detail, sizeof detail, "rows_failed=%lld, want 1", prog.rows_failed);
        fail("write pass rows_failed", detail);
    }
    stub_row(&row);
    expect_class_row(db->db, TORRENT_A, &row, "torrent A after the pass (updated)");
    expect_class_row(db->db, TORRENT_B, &row, "torrent B after the pass (inserted)");
    seeded_row(TORRENT_C, &row);
    expect_class_row(db->db, TORRENT_C, &row,
                     "torrent C after the pass (no verdict: row must stay untouched)");
    expect_count(db->db, "SELECT count(*) FROM torrent_class WHERE clf_ver = 7", 2,
                 "torrent_class rows at the new generation");
    expect_count(db->db, "SELECT count(*) FROM torrents", FIXTURE_TORRENTS,
                 "torrents must survive the pass");
    printf("  case 4 ok: A updated, B inserted at clf_ver=%d, C left at clf_ver=%d\n",
           STUB_CLF_VER, OLD_CLF_VER);

    /* --- 5: completion metadata + resume tombstone --------------------- */
    if (stub_write_meta_calls < 1) {
        fail("completion metadata", "the engine never published the generation");
    }
    expect_count(db->db,
                 "SELECT count(*) FROM classifier_meta WHERE key = 'clf_ver' AND value = '7'",
                 1, "classifier_meta clf_ver after the pass");
    expect_count(db->db,
                 "SELECT count(*) FROM classifier_meta WHERE key = 'backfill_last_id'",
                 1, "backfill_last_id is tombstoned, not deleted");
    expect_count(db->db,
                 "SELECT count(*) FROM classifier_meta "
                 "WHERE key = 'backfill_last_id' AND value = ''",
                 1, "backfill_last_id tombstone is the empty string");
    printf("  case 5 ok: classifier_meta clf_ver=%d, backfill_last_id tombstoned "
           "(no DELETE)\n", STUB_CLF_VER);
}

/* One pass over the convertible fixture: every torrent gets a verdict, so after
 * it nothing is left below the target generation. */
static void run_convertible_pass(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    class_row_t row;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("convertible pass", "returned non-zero");
    }
    if (prog.state != CLASSIFY_BACKFILL_DONE || prog.rows_done != FIXTURE_TORRENTS ||
        prog.rows_failed != 0) {
        char detail[128];
        snprintf(detail, sizeof detail, "state=%s done=%lld failed=%lld",
                 classify_backfill_state_name(prog.state), prog.rows_done, prog.rows_failed);
        fail("convertible pass must classify every torrent", detail);
    }
    stub_row(&row);
    expect_class_row(db->db, TORRENT_A, &row, "torrent A after the convertible pass");
    expect_class_row(db->db, TORRENT_B, &row, "torrent B after the convertible pass");
    expect_class_row(db->db, TORRENT_C, &row, "torrent C after the convertible pass");
}
/* 6a: re-running over the plan's fixture writes nothing. The one row still below
 * the target is torrent C, which cannot be classified: the engine retries it
 * (it never records "gave up") and leaves its row alone again. */
static void case_rerun_with_failed_row(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    const stub_call_t *call;
    class_row_t row;
    long long left = -1;
    int needed;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    stub_call_count = 0;

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("re-run with a failed row", "returned non-zero");
    }
    if (prog.rows_updated != 0 || prog.rows_inserted != 0) {
        char detail[128];
        snprintf(detail, sizeof detail, "updated=%lld inserted=%lld",
                 prog.rows_updated, prog.rows_inserted);
        fail("re-run with a failed row wrote something", detail);
    }
    if (prog.rows_total != 1 || prog.rows_done != 1 || prog.rows_failed != 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "total=%lld done=%lld failed=%lld",
                 prog.rows_total, prog.rows_done, prog.rows_failed);
        fail("only the unclassifiable torrent may remain a candidate", detail);
    }
    call = find_call("FAIL-gamma");
    if (!call || stub_call_count != 1) {
        fail("re-run with a failed row", "torrent C was not the one retried");
    }
    seeded_row(TORRENT_C, &row);
    expect_class_row(db->db, TORRENT_C, &row, "torrent C after the re-run");
    needed = classify_backfill_needed(db, 0, &left);
    if (needed != 1 || left != 1) {
        char detail[64];
        snprintf(detail, sizeof detail, "needed=%d rows_left=%lld", needed, left);
        fail("a row still below the target must be reported", detail);
    }
    printf("  case 6a ok: re-run wrote nothing, retried the unclassifiable torrent "
           "(rows_left=%lld)\n", left);
}

/* 6: a second run over a fully converted database is a no-op. */
static void case_noop_rerun(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    long long left = -1;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    stub_call_count = 0;

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("second run", "returned non-zero");
    }
    if (prog.rows_total != 0 || prog.rows_updated != 0 || prog.rows_inserted != 0) {
        char detail[128];
        snprintf(detail, sizeof detail, "total=%lld updated=%lld inserted=%lld",
                 prog.rows_total, prog.rows_updated, prog.rows_inserted);
        fail("second run must be a no-op", detail);
    }
    if (stub_call_count != 0) {
        fail("second run classified", "no row was left behind the target generation");
    }
    if (classify_backfill_needed(db, 0, &left) != 0) {
        fail("classify_backfill_needed after the pass", "returned 1");
    }
    if (left != 0) {
        char detail[64];
        snprintf(detail, sizeof detail, "rows_left=%lld, want 0", left);
        fail("classify_backfill_needed rows_left after the pass", detail);
    }
    printf("  case 6 ok: re-run processed 0 rows, needed=0 rows_left=0\n");
}

/* 7: limit_rows caps the pass — the --classify-backfill-limit equivalent. */
static void case_limit(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    class_row_t row;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    opts.limit_rows = 1;
    stub_call_count = 0;

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("limited pass", "returned non-zero");
    }
    if (prog.rows_done != 1) {
        char detail[64];
        snprintf(detail, sizeof detail, "rows_done=%lld, want 1", prog.rows_done);
        fail("limited pass rows_done", detail);
    }
    if (prog.state != CLASSIFY_BACKFILL_RUNNING) {
        fail("limited pass state", classify_backfill_state_name(prog.state));
    }
    if (stub_call_count != 1) {
        char detail[64];
        snprintf(detail, sizeof detail, "%d classify call(s), want 1", stub_call_count);
        fail("limited pass classified too many rows", detail);
    }
    stub_row(&row);
    expect_class_row(db->db, TORRENT_A, &row, "torrent A after the limited pass");
    expect_count(db->db, "SELECT count(*) FROM torrent_class WHERE torrent_id = 2", 0,
                 "torrent B after the limited pass");
    seeded_row(TORRENT_C, &row);
    expect_class_row(db->db, TORRENT_C, &row, "torrent C after the limited pass");
    printf("  case 7 ok: limit_rows=1 processed 1 of %lld candidates, 2 untouched\n",
           prog.rows_total);
}

/* 8: no models loaded -> DISABLED, nothing written. */
static void case_disabled(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    stub_models_loaded = 0;
    stub_call_count = 0;
    stub_write_meta_calls = 0;

    if (classify_backfill_run(db, &opts, &prog) != 1) {
        stub_models_loaded = 1;
        fail("disabled pass", "classify_backfill_run did not return 1");
    }
    stub_models_loaded = 1;
    if (prog.state != CLASSIFY_BACKFILL_DISABLED) {
        fail("disabled pass state", classify_backfill_state_name(prog.state));
    }
    if (prog.rows_total != 0 || prog.rows_updated != 0 || prog.rows_inserted != 0 ||
        prog.rows_failed != 0) {
        fail("disabled pass counters", "a disabled pass must not count or write a row");
    }
    if (stub_call_count != 0 || stub_write_meta_calls != 0) {
        fail("disabled pass side effects",
             "a disabled pass must not classify or publish a generation");
    }
    expect_untouched_fixture(db->db, "fixture after the disabled pass");
    printf("  case 8 ok: models absent -> state=%s, nothing written\n",
           classify_backfill_state_name(prog.state));
}

/* ------------------------------------------------------------------------- *
 * Live progress (classify_backfill_get_progress)
 * ------------------------------------------------------------------------- */

/* 9: after a completed pass the published snapshot is exactly the progress the
 * caller's *out carries — a late poller sees the final state, never a stale
 * mid-run copy. */
static void case_progress_after_run(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    classify_backfill_progress_t snap;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    memset(&snap, 0, sizeof snap);

    if (classify_backfill_run(db, &opts, &prog) != 0) {
        fail("pass before the progress comparison", "returned non-zero");
    }
    classify_backfill_get_progress(&snap);

    if (prog.state != CLASSIFY_BACKFILL_DONE) {
        fail("progress after a pass: run state", classify_backfill_state_name(prog.state));
    }
    if (snap.state != CLASSIFY_BACKFILL_DONE) {
        fail("progress after a pass: published state",
             classify_backfill_state_name(snap.state));
    }
    /* Guard against a vacuous comparison: this fixture has candidates. */
    if (prog.rows_total != FIXTURE_TORRENTS || prog.rows_done != FIXTURE_TORRENTS) {
        char detail[96];
        snprintf(detail, sizeof detail, "total=%lld done=%lld", prog.rows_total,
                 prog.rows_done);
        fail("progress after a pass: the fixture was not fully processed", detail);
    }
    if (snap.target_clf_ver != prog.target_clf_ver || snap.rows_total != prog.rows_total ||
        snap.rows_done != prog.rows_done || snap.rows_updated != prog.rows_updated ||
        snap.rows_inserted != prog.rows_inserted || snap.rows_skipped != prog.rows_skipped ||
        snap.rows_failed != prog.rows_failed || snap.last_id != prog.last_id) {
        char detail[384];
        snprintf(detail, sizeof detail,
                 "published target=%lld total=%lld done=%lld updated=%lld inserted=%lld "
                 "skipped=%lld failed=%lld last_id=%lld, *out target=%lld total=%lld "
                 "done=%lld updated=%lld inserted=%lld skipped=%lld failed=%lld last_id=%lld",
                 snap.target_clf_ver, snap.rows_total, snap.rows_done, snap.rows_updated,
                 snap.rows_inserted, snap.rows_skipped, snap.rows_failed, snap.last_id,
                 prog.target_clf_ver, prog.rows_total, prog.rows_done, prog.rows_updated,
                 prog.rows_inserted, prog.rows_skipped, prog.rows_failed, prog.last_id);
        fail("the published snapshot must match the caller's *out", detail);
    }
    printf("  case 9 ok: get_progress() reports state=%s total=%lld done=%lld updated=%lld "
           "inserted=%lld failed=%lld, same as *out\n",
           classify_backfill_state_name(snap.state), snap.rows_total, snap.rows_done,
           snap.rows_updated, snap.rows_inserted, snap.rows_failed);
}

/* Case 10's poller: reads the snapshot exactly like a /stats reader would and
 * records whether the pass was visible while it ran. */
static void *progress_poller_main(void *arg) {
    classify_backfill_progress_t seen;
    long long prev_done = 0;
    int running_seen = 0;

    (void)arg;
    while (!poller_stop_flag) {
        classify_backfill_get_progress(&seen);
        if (seen.state == CLASSIFY_BACKFILL_RUNNING) {
            if (running_seen && seen.rows_done < prev_done) {
                poller_monotonic = 0;   /* live progress must never go backwards */
            }
            prev_done = seen.rows_done;
            running_seen = 1;
            poller_saw_running = 1;
            poller_max_done = seen.rows_done;
        }
        usleep(200);
    }
    return NULL;
}

/* 10: mid-run visibility. A poller thread reads the published snapshot while the
 * pass is in flight: it must observe state=RUNNING at least once, with rows_done
 * non-decreasing, and the snapshot must end up DONE. batch_rows=1 forces a
 * commit (and therefore a publication) after every row, and the fixture's third
 * torrent stalls inside the stub classifier until the poller has seen RUNNING
 * with progress in it — that is what a much bigger fixture would otherwise buy. */
static void case_live_progress(database_t *db) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t prog;
    classify_backfill_progress_t snap;
    pthread_t poller;
    int rc;

    memset(&opts, 0, sizeof opts);
    memset(&prog, 0, sizeof prog);
    memset(&snap, 0, sizeof snap);
    opts.batch_rows = 1;   /* one publication per row instead of per 500 */

    poller_stop_flag = 0;
    poller_saw_running = 0;
    poller_monotonic = 1;
    poller_max_done = -1;
    stub_stall_for_poller = 1;

    if (pthread_create(&poller, NULL, progress_poller_main, NULL) != 0) {
        stub_stall_for_poller = 0;
        fail("progress poller", "pthread_create failed");
    }

    rc = classify_backfill_run(db, &opts, &prog);

    poller_stop_flag = 1;
    pthread_join(poller, NULL);
    stub_stall_for_poller = 0;

    if (rc != 0) {
        fail("pass under a polling reader", "returned non-zero");
    }
    if (prog.state != CLASSIFY_BACKFILL_DONE) {
        fail("pass under a polling reader: run state",
             classify_backfill_state_name(prog.state));
    }
    if (!poller_saw_running) {
        fail("mid-run visibility",
             "the poller never saw state=running while the pass was in flight");
    }
    if (!poller_monotonic) {
        fail("mid-run visibility", "rows_done went backwards between two polls");
    }
    if (poller_max_done < 1) {
        fail("mid-run visibility",
             "the poller never saw a running snapshot with rows_done > 0");
    }
    classify_backfill_get_progress(&snap);
    if (snap.state != CLASSIFY_BACKFILL_DONE || snap.rows_done != prog.rows_done) {
        char detail[128];
        snprintf(detail, sizeof detail, "state=%s done=%lld, run ended state=%s done=%lld",
                 classify_backfill_state_name(snap.state), snap.rows_done,
                 classify_backfill_state_name(prog.state), prog.rows_done);
        fail("the published snapshot must end at the run's final state", detail);
    }
    printf("  case 10 ok: poller saw state=running (rows_done up to %lld, monotonic) and "
           "the snapshot ended state=%s done=%lld\n",
           poller_max_done, classify_backfill_state_name(snap.state), snap.rows_done);
}

/* 11: the accessor tolerates a NULL out (no-op, no crash) and the snapshot
 * before the first pass of the process is the zeroed IDLE state. */
static void case_progress_null_safe(void) {
    classify_backfill_progress_t snap;

    memset(&snap, 0xFF, sizeof snap);   /* poisoned: the accessor must fill it */
    classify_backfill_get_progress(&snap);
    if (snap.state != CLASSIFY_BACKFILL_IDLE || snap.target_clf_ver != 0 ||
        snap.rows_total != 0 || snap.rows_done != 0 || snap.rows_updated != 0 ||
        snap.rows_inserted != 0 || snap.rows_skipped != 0 || snap.rows_failed != 0 ||
        snap.last_id != 0) {
        char detail[160];
        snprintf(detail, sizeof detail,
                 "state=%s target=%lld total=%lld done=%lld last_id=%lld",
                 classify_backfill_state_name(snap.state), snap.target_clf_ver,
                 snap.rows_total, snap.rows_done, snap.last_id);
        fail("before the first pass the snapshot must be a zeroed IDLE state", detail);
    }
    classify_backfill_get_progress(NULL);   /* must not crash */
    classify_backfill_get_progress(&snap);
    if (snap.state != CLASSIFY_BACKFILL_IDLE) {
        fail("get_progress(NULL)", "the no-op changed the published snapshot");
    }
    printf("  case 11 ok: get_progress(NULL) is a no-op; the snapshot starts IDLE\n");
}

int main(void) {
    database_t db;

    /* Case 11 needs no fixture: it pins the pre-pass snapshot and NULL safety. */
    case_progress_null_safe();

    /* Cases 1-5 and 6a share one fixture: 1-2 write nothing, 3-5 is the single
     * real pass, 6a proves a re-run writes nothing even when a row cannot be
     * classified. */
    build_fixture(&db);
    case_needed_before(&db);
    case_dry_run(&db);
    case_write_pass(&db);
    case_rerun_with_failed_row(&db);
    database_cleanup(&db);

    /* Case 6 needs a database with nothing left below the target generation, so
     * it converts a fully classifiable fixture first. */
    build_convertible_fixture(&db);
    run_convertible_pass(&db);
    case_noop_rerun(&db);
    database_cleanup(&db);

    /* Case 7 needs a pass that stops early, so it starts from a fresh fixture. */
    build_fixture(&db);
    case_limit(&db);
    database_cleanup(&db);

    /* Case 8 flips the models toggle, so it runs last. */
    build_fixture(&db);
    case_disabled(&db);
    database_cleanup(&db);

    /* Case 9 compares the published snapshot with a completed pass's *out. */
    build_fixture(&db);
    case_progress_after_run(&db);
    database_cleanup(&db);

    /* Case 10 needs a pass slow enough to be observed in flight: its fixture's
     * third torrent stalls in the stub classifier until the poller thread has
     * read a RUNNING snapshot that carries progress. */
    build_fixture_named(&db, "SLOW-gamma");
    case_live_progress(&db);
    database_cleanup(&db);

    remove_scratch_db();

    printf("test_classify_backfill: all assertions passed\n");
    return 0;
}
