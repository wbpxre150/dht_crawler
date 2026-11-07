/**
 * Host unit test for the resumable movie-kind backfill engine
 * (include/movie_backfill.h, src/movie_backfill.c).
 *
 * Exercises the real product code against a real database: the fixture is built
 * through the ingest path (database_init / database_create_schema /
 * database_insert_batch), so "untouched" and "no writes" assertions compare
 * against rows the daemon itself would have written.
 *
 * The movie-kind module is linked for real (src/movie_kind.c compiles on the host
 * without MK_ENABLED: the manifest/sha256 half is plain C99 + cJSON + OpenSSL),
 * but every call the engine makes into it - plus the thermal reader and the
 * sleeper - is replaced through the engine's test hooks
 * (-DMOVIE_BACKFILL_TEST), so the pass can label rows without a 152.8 MiB model
 * and can pause for heat without a hot CPU. No link-level stubs and no hooks are
 * mixed for the same symbol: the module is real, the seams are the hooks.
 *
 * Cases:
 *   1 disabled (no verified manifest / no encoder)   -> DISABLED, nothing written
 *   2 dry run                                       -> DONE, counts the candidates,
 *                                                      writes nothing at all
 *   3 full pass                                     -> every candidate labelled at the
 *                                                      target generation, the fastText
 *                                                      columns byte-identical (the
 *                                                      invariant), completion meta
 *                                                      written, resume key tombstoned,
 *                                                      second pass a no-op
 *   4 mask 0 is a valid label                       -> `none` rows are labelled and
 *                                                      never re-offered
 *   5 no verdict                                    -> stored columns untouched, the
 *                                                      row is counted failed and stays
 *                                                      a candidate for the next pass
 *   6 resume key                                    -> written per batch, honoured for
 *                                                      its own generation only,
 *                                                      tombstoned at completion
 *   7 thermal back-off                              -> pause counted once, slept in
 *                                                      slices, pass resumes and
 *                                                      completes; a stop request during
 *                                                      the pause ends it as stopped
 *   8 movie_backfill_needed()                       -> meta-only-behind, rows-left and
 *                                                      disabled cases
 *   9 --dump-masks                                  -> a failed classify call gets no mask
 *                                                      line (mask 0 is a real verdict)
 *
 *     make tests-movie-backfill      # or: make tests / make tests-asan-movie-backfill
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "database.h"
#include "dht_crawler.h"
#include "movie_backfill.h"
#include "movie_kind.h"

#define TEST_DB "/tmp/movie-backfill-test.db"

/* Generation the fake movie manifest reports. Deliberately not a real generation:
 * the engine must label rows with whatever it is told, never a literal. */
#define STUB_GENERATION 11
/* Generation the fixture's pre-existing movie labels were written with. */
#define OLD_MOVIE_VER 3

#define TORRENT_MOVIE_A 1  /* category=movie, adult=0, unlabelled  -> candidate */
#define TORRENT_MOVIE_B 2  /* category=movie, adult=0, unlabelled  -> candidate */
#define TORRENT_TV 3       /* category=tv                          -> not a candidate */
#define TORRENT_ADULT 4    /* category=movie, adult=1              -> not a candidate */
#define TORRENT_OLD 5      /* category=movie, adult=0, ver 3       -> candidate */
#define TORRENT_CURRENT 6  /* category=movie, adult=0, ver 11, mask 0 -> not a candidate */
#define TORRENT_NONE 7     /* category=movie, adult=0, stub says `none` -> mask 0 */
#define FIXTURE_TORRENTS 7

#define NAME_MOVIE_A "alpha-movie"
#define NAME_MOVIE_B "beta-movie"
#define NAME_TV "gamma-tv"
#define NAME_ADULT "delta-adult"
#define NAME_OLD "epsilon-old"
#define NAME_CURRENT "zeta-current"
#define NAME_NONE "zero-none"

/* Candidates of the fixture: A, B, OLD and NONE. */
#define FIXTURE_CANDIDATES 4

/* What the fake classifier returns for a name: a bit pattern that is distinct per
 * torrent, plus 0 for the `none` case (a *valid* label, not a marker). */
#define STUB_MASK_A 0x0005u
#define STUB_MASK_B 0x4001u
#define STUB_MASK_OLD 0x0002u
#define STUB_MASK_CURRENT 0x0100u
#define STUB_MASK_NONE 0x0000u

/* Any name starting with this gets no verdict, like a row the model cannot label. */
#define FAIL_PREFIX "FAIL-"

#define STUB_MAX_CALLS 64
#define STUB_NAME_MAX 128

static app_context_t test_app_ctx;

/* --- fake movie-kind module (installed through the engine's hooks) --------- */

static int stub_enabled = 1;
static long long stub_generation = STUB_GENERATION;
static int stub_fail_all = 0;
static int stub_call_count = 0;
static char stub_calls[STUB_MAX_CALLS][STUB_NAME_MAX];

/* Thermal seam: the reader returns the scripted values in order and then a cool
 * temperature, so a pause is one bounded episode. The sleeper counts its calls and
 * optionally requests a stop (the "shutdown during a pause" case). */
#define STUB_TEMP_SCRIPT_MAX 8
static int stub_temps[STUB_TEMP_SCRIPT_MAX];
static int stub_temp_count = 0;
static int stub_temp_next = 0;
static int stub_cool_c = 60;
static int stub_sleep_calls = 0;
static int stub_sleep_stop = 0;
static int stub_pause_calls = 0;

static int stub_classify(const char *name, uint16_t *mask) {
    if (stub_call_count < STUB_MAX_CALLS) {
        snprintf(stub_calls[stub_call_count], STUB_NAME_MAX, "%s", name ? name : "");
    }
    stub_call_count++;

    if (stub_fail_all || (name && strncmp(name, FAIL_PREFIX, strlen(FAIL_PREFIX)) == 0)) {
        return 1;
    }
    if (!mask) {
        return 1;
    }
    if (strcmp(name, NAME_MOVIE_A) == 0) {
        *mask = STUB_MASK_A;
    } else if (strcmp(name, NAME_MOVIE_B) == 0) {
        *mask = STUB_MASK_B;
    } else if (strcmp(name, NAME_OLD) == 0) {
        *mask = STUB_MASK_OLD;
    } else if (strcmp(name, NAME_CURRENT) == 0) {
        *mask = STUB_MASK_CURRENT;
    } else {
        *mask = STUB_MASK_NONE;
    }
    return 0;
}

static int stub_enabled_fn(void) {
    return stub_enabled;
}

static long long stub_generation_fn(void) {
    return stub_generation;
}

static int stub_thermal_fn(void) {
    if (stub_temp_next < stub_temp_count) {
        return stub_temps[stub_temp_next++];
    }
    return stub_cool_c;
}

static void stub_sleep_fn(long long ms) {
    (void)ms;
    stub_sleep_calls++;
    if (stub_sleep_stop) {
        movie_backfill_request_stop();
    }
}

static void stub_pause_fn(void) {
    stub_pause_calls++;
}

static void install_hooks(void) {
    movie_backfill_hooks_t hooks;

    memset(&hooks, 0, sizeof hooks);
    hooks.classify = stub_classify;
    hooks.enabled = stub_enabled_fn;
    hooks.generation = stub_generation_fn;
    hooks.thermal_max_c = stub_thermal_fn;
    hooks.sleep_ms = stub_sleep_fn;
    hooks.note_thermal_pause = stub_pause_fn;
    movie_backfill_set_hooks(&hooks);
}

static void reset_stub(void) {
    stub_enabled = 1;
    stub_generation = STUB_GENERATION;
    stub_fail_all = 0;
    stub_call_count = 0;
    memset(stub_calls, 0, sizeof stub_calls);
    memset(stub_temps, 0, sizeof stub_temps);
    stub_temp_count = 0;
    stub_temp_next = 0;
    stub_cool_c = 60;
    stub_sleep_calls = 0;
    stub_sleep_stop = 0;
    stub_pause_calls = 0;
}

/* --- small helpers -------------------------------------------------------- */

static void fail(const char *what, const char *detail) {
    fprintf(stderr, "test_movie_backfill: FAILED: %s", what);
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

static void expect_count(sqlite3 *db, const char *sql, sqlite3_int64 want, const char *what) {
    sqlite3_int64 got = scalar_sql(db, sql, what);
    if (got != want) {
        char detail[128];
        snprintf(detail, sizeof detail, "got %lld, want %lld", (long long)got, (long long)want);
        fail(what, detail);
    }
}

static void remove_scratch_db(void) {
    static const char *const SUFFIXES[] = {"", "-wal", "-shm"};
    for (size_t i = 0; i < sizeof SUFFIXES / sizeof SUFFIXES[0]; i++) {
        char path[512];
        snprintf(path, sizeof path, "%s%s", TEST_DB, SUFFIXES[i]);
        if (unlink(path) != 0) {
            FILE *probe = fopen(path, "rb");
            if (probe) {
                fclose(probe);
                fail("remove stale scratch database", path);
            }
        }
    }
}

static void exec_sql(sqlite3 *db, const char *sql, const char *what) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        char detail[512];
        snprintf(detail, sizeof detail, "%s", err ? err : "unknown error");
        sqlite3_free(err);
        fail(what, detail);
    }
}

/* --- torrent_class row readback ------------------------------------------- */

/* The fastText half of the row: the columns a movie pass must never touch. */
typedef struct {
    long long category;
    long long tv_kind_is_null;
    long long adult;
    double adult_p;
    long long season_lo, season_hi, episode_lo, episode_hi;
    long long resolution;
    double confidence;
    long long clf_ver;
} fasttext_row_t;

/* The movie-kind half plus whether each column is SQL NULL. */
typedef struct {
    int mask_is_null;
    long long mask;
    int ver_is_null;
    long long ver;
} movie_row_t;

static void read_row(sqlite3 *db, long long id, fasttext_row_t *ft, movie_row_t *mv,
                     const char *what) {
    static const char *const SQL =
        "SELECT category, tv_kind IS NULL, adult, adult_p, season_lo, season_hi, "
        "episode_lo, episode_hi, resolution, confidence, clf_ver, "
        "movie_kind_mask IS NULL, COALESCE(movie_kind_mask, -1), "
        "movie_kind_clf_ver IS NULL, COALESCE(movie_kind_clf_ver, -1) "
        "FROM torrent_class WHERE torrent_id = ?";
    sqlite3_stmt *stmt = NULL;
    char detail[128];
    int rc;

    memset(ft, 0, sizeof *ft);
    memset(mv, 0, sizeof *mv);
    if (sqlite3_prepare_v2(db, SQL, -1, &stmt, NULL) != SQLITE_OK) {
        fail(what, sqlite3_errmsg(db));
    }
    sqlite3_bind_int64(stmt, 1, id);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        snprintf(detail, sizeof detail, "no torrent_class row for %lld (step rc=%d)", id, rc);
        sqlite3_finalize(stmt);
        fail(what, detail);
    }
    ft->category = sqlite3_column_int64(stmt, 0);
    ft->tv_kind_is_null = sqlite3_column_int(stmt, 1);
    ft->adult = sqlite3_column_int64(stmt, 2);
    ft->adult_p = sqlite3_column_double(stmt, 3);
    ft->season_lo = sqlite3_column_int64(stmt, 4);
    ft->season_hi = sqlite3_column_int64(stmt, 5);
    ft->episode_lo = sqlite3_column_int64(stmt, 6);
    ft->episode_hi = sqlite3_column_int64(stmt, 7);
    ft->resolution = sqlite3_column_int64(stmt, 8);
    ft->confidence = sqlite3_column_double(stmt, 9);
    ft->clf_ver = sqlite3_column_int64(stmt, 10);
    mv->mask_is_null = sqlite3_column_int(stmt, 11);
    mv->mask = sqlite3_column_int64(stmt, 12);
    mv->ver_is_null = sqlite3_column_int(stmt, 13);
    mv->ver = sqlite3_column_int64(stmt, 14);
    sqlite3_finalize(stmt);
}

/* One fixture torrent and the class row it must carry. Every fastText field is a
 * distinctive value derived from the id (see seed_fasttext), so a movie pass that
 * wrote any of them shows up immediately. */
typedef struct {
    long long id;
    const char *name;
    long long category;
    long long adult;
    int have_movie; /* 0 = movie_kind_clf_ver IS NULL (unlabelled) */
    long long movie_ver;
    long long movie_mask;
} fixture_spec_t;

static const fixture_spec_t FIXTURE[FIXTURE_TORRENTS] = {
    {TORRENT_MOVIE_A, NAME_MOVIE_A, 1, 0, 0, 0, 0},
    {TORRENT_MOVIE_B, NAME_MOVIE_B, 1, 0, 0, 0, 0},
    {TORRENT_TV, NAME_TV, 2, 0, 0, 0, 0},
    {TORRENT_ADULT, NAME_ADULT, 1, 1, 0, 0, 0},
    {TORRENT_OLD, NAME_OLD, 1, 0, 1, OLD_MOVIE_VER, 0x0020},
    /* Already at the target generation with the `none` mask: NOT a candidate - mask
     * 0 is a label, the generation is the marker. */
    {TORRENT_CURRENT, NAME_CURRENT, 1, 0, 1, STUB_GENERATION, STUB_MASK_NONE},
    {TORRENT_NONE, NAME_NONE, 1, 0, 0, 0, 0},
};

static const fixture_spec_t *spec_for(long long id) {
    for (size_t i = 0; i < sizeof FIXTURE / sizeof FIXTURE[0]; i++) {
        if (FIXTURE[i].id == id) {
            return &FIXTURE[i];
        }
    }
    fail("fixture spec", "no such torrent id");
    return NULL;
}

/* The fastText columns as the fixture seeds them. */
static void seed_fasttext(const fixture_spec_t *spec, fasttext_row_t *row) {
    long long id = spec->id;

    memset(row, 0, sizeof *row);
    row->category = spec->category;
    row->tv_kind_is_null = 1;
    row->adult = spec->adult;
    row->adult_p = 0.125 * (double)id;
    row->season_lo = id;
    row->season_hi = id + 1;
    row->episode_lo = id + 2;
    row->episode_hi = id + 3;
    row->resolution = 720;
    row->confidence = 0.375;
    row->clf_ver = 4; /* the fastText generation, unrelated to the movie one */
}

static void expect_fasttext_unchanged(sqlite3 *db, long long id, const char *what) {
    fasttext_row_t want;
    fasttext_row_t got;
    movie_row_t mv;
    char detail[320];

    seed_fasttext(spec_for(id), &want);
    read_row(db, id, &got, &mv, what);
    if (got.category != want.category || got.tv_kind_is_null != want.tv_kind_is_null ||
        got.adult != want.adult || got.adult_p != want.adult_p ||
        got.season_lo != want.season_lo || got.season_hi != want.season_hi ||
        got.episode_lo != want.episode_lo || got.episode_hi != want.episode_hi ||
        got.resolution != want.resolution || got.confidence != want.confidence ||
        got.clf_ver != want.clf_ver) {
        snprintf(detail, sizeof detail,
                 "torrent %lld fastText row changed: cat=%lld/%lld tv_null=%lld/%lld "
                 "adult=%lld/%lld adult_p=%.6f/%.6f s=%lld/%lld e=%lld/%lld res=%lld/%lld "
                 "conf=%.6f/%.6f ver=%lld/%lld",
                 id, got.category, want.category, got.tv_kind_is_null, want.tv_kind_is_null,
                 got.adult, want.adult, got.adult_p, want.adult_p, got.season_lo,
                 want.season_lo, got.episode_hi, want.episode_hi, got.resolution,
                 want.resolution, got.confidence, want.confidence, got.clf_ver, want.clf_ver);
        fail(what, detail);
    }
}

/* --- fixture -------------------------------------------------------------- */

/* Insert one torrent through the ingest path. The class row ingest writes is then
 * overwritten by seed_class_row(), which is what makes the fixture's category /
 * adult / clf_ver / movie columns exact. */
static void insert_torrent(database_t *db, int id, const char *name) {
    torrent_metadata_t *torrent = calloc(1, sizeof(*torrent));
    torrent_metadata_t *seed[1];
    size_t written = 0;

    if (!torrent) {
        fail("calloc torrent_metadata_t", NULL);
    }
    torrent->info_hash[0] = (uint8_t)(id & 0xFF);
    torrent->info_hash[1] = (uint8_t)((id >> 8) & 0xFF);
    for (int i = 2; i < SHA1_DIGEST_LENGTH; i++) {
        torrent->info_hash[i] = (uint8_t)(i * 13 + 5);
    }
    torrent->name = strdup(name);
    if (!torrent->name) {
        fail("strdup torrent name", NULL);
    }
    torrent->size_bytes = (int64_t)2048 * id;
    torrent->total_peers = id % 5;
    torrent->added_timestamp = 1700000000 + id;
    torrent->has_classification = 1;
    torrent->classification.category = 1;
    torrent->classification.clf_ver = 4;
    torrent->num_files = 1;
    torrent->files = calloc(1, sizeof(file_info_t));
    if (!torrent->files) {
        fail("calloc file_info_t", NULL);
    }
    torrent->files[0].path = strdup("movie.mkv");
    if (!torrent->files[0].path) {
        fail("strdup file path", NULL);
    }
    torrent->files[0].size_bytes = 1024;
    torrent->files[0].file_index = 0;

    seed[0] = torrent;
    if (database_insert_batch(db, seed, 1, &written) != 0 || written != 1) {
        fail("database_insert_batch", name);
    }
    free(torrent->files[0].path);
    free(torrent->files);
    free(torrent->name);
    free(torrent);
}

/* Write the fixture's class row exactly: fastText half from seed_fasttext(), movie
 * half either NULL (unlabelled) or a given generation + mask. */
static void seed_class_row(database_t *db, const fixture_spec_t *spec) {
    static const char *const SQL =
        "UPDATE torrent_class SET category = ?, tv_kind = NULL, adult = ?, adult_p = ?, "
        "season_lo = ?, season_hi = ?, episode_lo = ?, episode_hi = ?, resolution = ?, "
        "confidence = ?, clf_ver = ?, movie_kind_mask = ?, movie_kind_clf_ver = ? "
        "WHERE torrent_id = ?";
    fasttext_row_t ft;
    sqlite3_stmt *stmt = NULL;
    int rc;

    seed_fasttext(spec, &ft);
    if (sqlite3_prepare_v2(db->db, SQL, -1, &stmt, NULL) != SQLITE_OK) {
        fail(spec->name, sqlite3_errmsg(db->db));
    }
    sqlite3_bind_int64(stmt, 1, spec->category);
    sqlite3_bind_int64(stmt, 2, spec->adult);
    sqlite3_bind_double(stmt, 3, ft.adult_p);
    sqlite3_bind_int64(stmt, 4, ft.season_lo);
    sqlite3_bind_int64(stmt, 5, ft.season_hi);
    sqlite3_bind_int64(stmt, 6, ft.episode_lo);
    sqlite3_bind_int64(stmt, 7, ft.episode_hi);
    sqlite3_bind_int64(stmt, 8, ft.resolution);
    sqlite3_bind_double(stmt, 9, ft.confidence);
    sqlite3_bind_int64(stmt, 10, ft.clf_ver);
    if (spec->have_movie) {
        sqlite3_bind_int64(stmt, 11, spec->movie_mask);
        sqlite3_bind_int64(stmt, 12, spec->movie_ver);
    } else {
        sqlite3_bind_null(stmt, 11);
        sqlite3_bind_null(stmt, 12);
    }
    sqlite3_bind_int64(stmt, 13, spec->id);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        fail(spec->name, sqlite3_errstr(rc));
    }
    if (sqlite3_changes(db->db) != 1) {
        fail(spec->name, "no row updated");
    }
}

/* Fresh scratch database with the seven fixture torrents. */
static void build_fixture(database_t *db) {
    remove_scratch_db();
    if (database_init(db, TEST_DB, &test_app_ctx) != 0) {
        fail("database_init", TEST_DB);
    }
    if (database_create_schema(db) != 0) {
        fail("database_create_schema", TEST_DB);
    }

    /* Ingest writes a class row per torrent; seed_class_row() then makes the
     * category / adult / clf_ver / movie columns exact. */
    for (size_t i = 0; i < sizeof FIXTURE / sizeof FIXTURE[0]; i++) {
        insert_torrent(db, (int)FIXTURE[i].id, FIXTURE[i].name);
    }
    for (size_t i = 0; i < sizeof FIXTURE / sizeof FIXTURE[0]; i++) {
        seed_class_row(db, &FIXTURE[i]);
    }
}

static void expect_untouched_fixture(database_t *db, const char *what) {
    fasttext_row_t ft;
    movie_row_t mv;

    for (long long id = 1; id <= FIXTURE_TORRENTS; id++) {
        expect_fasttext_unchanged(db->db, id, what);
    }
    /* The movie columns of every row are exactly as seeded. */
    read_row(db->db, TORRENT_MOVIE_A, &ft, &mv, what);
    if (!mv.mask_is_null || !mv.ver_is_null) {
        fail(what, "torrent A gained a movie label");
    }
    read_row(db->db, TORRENT_OLD, &ft, &mv, what);
    if (mv.mask_is_null || mv.mask != 0x0020 || mv.ver_is_null || mv.ver != OLD_MOVIE_VER) {
        fail(what, "torrent OLD's stored movie label changed");
    }
    read_row(db->db, TORRENT_CURRENT, &ft, &mv, what);
    if (mv.mask_is_null || mv.mask != STUB_MASK_NONE || mv.ver_is_null ||
        mv.ver != STUB_GENERATION) {
        fail(what, "torrent CURRENT's stored movie label changed");
    }
    /* No row was inserted or deleted. */
    expect_count(db->db, "SELECT count(*) FROM torrent_class", FIXTURE_TORRENTS, what);
}

/* The meta value of `key`, or NULL when absent. Caller frees. */
static char *meta_value(sqlite3 *db, const char *key, const char *what) {
    sqlite3_stmt *stmt = NULL;
    char *copy = NULL;

    if (sqlite3_prepare_v2(db, "SELECT value FROM classifier_meta WHERE key = ?", -1, &stmt,
                           NULL) != SQLITE_OK) {
        fail(what, sqlite3_errmsg(db));
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *text = sqlite3_column_text(stmt, 0);
        copy = strdup(text ? (const char *)text : "");
    }
    sqlite3_finalize(stmt);
    return copy;
}

static void expect_meta(sqlite3 *db, const char *key, const char *want, const char *what) {
    char *got = meta_value(db, key, what);
    char detail[256];

    if (!got) {
        snprintf(detail, sizeof detail, "%s is missing, want '%s'", key, want);
        fail(what, detail);
    }
    if (strcmp(got, want) != 0) {
        snprintf(detail, sizeof detail, "%s is '%s', want '%s'", key, got, want);
        free(got);
        fail(what, detail);
    }
    free(got);
}

static void expect_meta_absent(sqlite3 *db, const char *key, const char *what) {
    char *got = meta_value(db, key, what);
    char detail[192];

    if (got) {
        snprintf(detail, sizeof detail, "%s is '%s', want it absent", key, got);
        free(got);
        fail(what, detail);
    }
}

/* A candidate must end up labelled with the target generation and the stub's mask. */
static void expect_labelled(sqlite3 *db, long long id, long long want_mask, const char *what) {
    fasttext_row_t ft;
    movie_row_t mv;
    char detail[256];

    read_row(db, id, &ft, &mv, what);
    if (mv.mask_is_null || mv.mask != want_mask || mv.ver_is_null || mv.ver != STUB_GENERATION) {
        snprintf(detail, sizeof detail,
                 "torrent %lld is mask=%s%lld ver=%s%lld, want mask=%lld ver=%d", id,
                 mv.mask_is_null ? "NULL/" : "", mv.mask, mv.ver_is_null ? "NULL/" : "", mv.ver,
                 want_mask, STUB_GENERATION);
        fail(what, detail);
    }
}

/* --- cases ---------------------------------------------------------------- */

/* 1: no verified manifest / no encoder -> DISABLED, nothing written. */
static void case_disabled(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;

    build_fixture(db);
    stub_enabled = 0;
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_run(db, &opts, &progress) != 1) {
        fail("case 1 disabled", "run() did not report DISABLED");
    }
    if (progress.state != MOVIE_BACKFILL_DISABLED ||
        strcmp(movie_backfill_state_name(progress.state), "disabled") != 0) {
        fail("case 1 disabled", "state is not DISABLED/\"disabled\"");
    }
    if (stub_call_count != 0) {
        fail("case 1 disabled", "the classifier was called for a disabled pass");
    }
    expect_untouched_fixture(db, "case 1 disabled");
    expect_meta_absent(db->db, MOVIE_BACKFILL_VER_KEY, "case 1 disabled");
    expect_meta_absent(db->db, MOVIE_BACKFILL_RESUME_KEY, "case 1 disabled");
    if (movie_backfill_needed(db, 0, NULL) != 0) {
        fail("case 1 disabled", "needed() reported work while the module is disabled");
    }

    /* No target generation either: still disabled, still nothing written. */
    stub_enabled = 1;
    stub_generation = 0;
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 1 ||
        progress.state != MOVIE_BACKFILL_DISABLED) {
        fail("case 1 disabled", "a pass without a target generation did not report DISABLED");
    }
    expect_untouched_fixture(db, "case 1 disabled (no target)");
    reset_stub();
    printf("  case 1 ok: a disabled module reports DISABLED and writes nothing\n");
}

/* 2: a dry run counts the candidates and writes nothing at all. */
static void case_dry_run(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;

    build_fixture(db);
    memset(&opts, 0, sizeof opts);
    opts.dry_run = 1;
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 2 dry run", "run() failed");
    }
    if (progress.state != MOVIE_BACKFILL_DONE) {
        fail("case 2 dry run", "state is not DONE");
    }
    if (progress.rows_total != FIXTURE_CANDIDATES) {
        char detail[128];
        snprintf(detail, sizeof detail, "rows_total=%lld, want %d", progress.rows_total,
                 FIXTURE_CANDIDATES);
        fail("case 2 dry run", detail);
    }
    if (progress.rows_done != 0 || progress.rows_updated != 0) {
        fail("case 2 dry run", "a dry run processed rows");
    }
    if (stub_call_count != 0) {
        fail("case 2 dry run", "a dry run called the classifier");
    }
    expect_untouched_fixture(db, "case 2 dry run");
    expect_meta_absent(db->db, MOVIE_BACKFILL_VER_KEY, "case 2 dry run");
    expect_meta_absent(db->db, MOVIE_BACKFILL_RESUME_KEY, "case 2 dry run");

    /* A dry run needs no encoder: with the module disabled (the host build, or a
     * phone without the v9 assets) it still counts. Without a target generation
     * the predicate counts the unlabelled movie rows - A, B and NONE here, not
     * OLD (labelled at generation 3) and not CURRENT (labelled at the target).
     * This is the path `--movie-kind-backfill <db> --dry-run` takes. */
    stub_enabled = 0;
    stub_generation = 0;
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 0 ||
        progress.state != MOVIE_BACKFILL_DONE) {
        fail("case 2 dry run (no model)", "a dry run without a model did not complete");
    }
    if (progress.rows_total != FIXTURE_CANDIDATES - 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "rows_total=%lld, want %d", progress.rows_total,
                 FIXTURE_CANDIDATES - 1);
        fail("case 2 dry run (no model)", detail);
    }
    if (progress.rows_updated != 0 || stub_call_count != 0) {
        fail("case 2 dry run (no model)", "a dry run without a model wrote or classified");
    }
    expect_untouched_fixture(db, "case 2 dry run (no model)");
    reset_stub();
    printf("  case 2 ok: a dry run counts %d candidate(s) (and %d without a model) and "
           "writes nothing\n",
           FIXTURE_CANDIDATES, FIXTURE_CANDIDATES - 1);
}

/* 3: the full pass labels every candidate and leaves the fastText columns alone. */
static void case_full_pass(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;
    long long rows_left = -1;
    char want_ver[32];

    build_fixture(db);
    memset(&opts, 0, sizeof opts);
    opts.chunk_ids = 2;   /* several windows, so the walk and its resume key move */
    opts.batch_rows = 2;  /* and several batch commits */
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_needed(db, 0, &rows_left) != 1 || rows_left != FIXTURE_CANDIDATES) {
        fail("case 3 full pass", "needed() did not report the fixture's candidates");
    }

    stub_call_count = 0;
    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 3 full pass", "run() failed");
    }
    /* Exactly the candidates were classified - not the tv row, not the adult row,
     * not the row already at the target generation. */
    if (stub_call_count != FIXTURE_CANDIDATES) {
        char detail[128];
        snprintf(detail, sizeof detail, "classifier calls=%d, want %d", stub_call_count,
                 FIXTURE_CANDIDATES);
        fail("case 3 full pass (candidate set)", detail);
    }
    if (progress.state != MOVIE_BACKFILL_DONE ||
        strcmp(movie_backfill_state_name(progress.state), "done") != 0) {
        fail("case 3 full pass", "state is not DONE/\"done\"");
    }
    if (progress.target_clf_ver != STUB_GENERATION) {
        fail("case 3 full pass", "target_clf_ver is not the manifest's generation");
    }
    if (progress.rows_total != FIXTURE_CANDIDATES ||
        progress.rows_done != FIXTURE_CANDIDATES ||
        progress.rows_updated != FIXTURE_CANDIDATES || progress.rows_failed != 0) {
        char detail[192];
        snprintf(detail, sizeof detail, "total=%lld done=%lld updated=%lld failed=%lld",
                 progress.rows_total, progress.rows_done, progress.rows_updated,
                 progress.rows_failed);
        fail("case 3 full pass", detail);
    }

    /* Every candidate is labelled with the target generation and the stub's mask -
     * including the `none` verdict, which is a *valid* mask of 0. */
    expect_labelled(db->db, TORRENT_MOVIE_A, STUB_MASK_A, "case 3 full pass");
    expect_labelled(db->db, TORRENT_MOVIE_B, STUB_MASK_B, "case 3 full pass");
    expect_labelled(db->db, TORRENT_OLD, STUB_MASK_OLD, "case 3 full pass");
    expect_labelled(db->db, TORRENT_NONE, STUB_MASK_NONE, "case 3 full pass");

    /* The invariant: the movie pass wrote no fastText column, on any row. */
    for (long long id = 1; id <= FIXTURE_TORRENTS; id++) {
        expect_fasttext_unchanged(db->db, id, "case 3 invariant (fastText columns)");
    }
    /* The non-candidates were not even labelled. */
    {
        fasttext_row_t ft;
        movie_row_t mv;
        read_row(db->db, TORRENT_TV, &ft, &mv, "case 3 full pass");
        if (!mv.mask_is_null || !mv.ver_is_null) {
            fail("case 3 full pass", "a tv row was labelled by the movie pass");
        }
        read_row(db->db, TORRENT_ADULT, &ft, &mv, "case 3 full pass");
        if (!mv.mask_is_null || !mv.ver_is_null) {
            fail("case 3 full pass", "an adult row was labelled by the movie pass");
        }
        read_row(db->db, TORRENT_CURRENT, &ft, &mv, "case 3 full pass");
        if (mv.mask_is_null || mv.mask != STUB_MASK_NONE || mv.ver != STUB_GENERATION) {
            fail("case 3 full pass", "a row already at the target generation was rewritten");
        }
    }
    expect_count(db->db, "SELECT count(*) FROM torrent_class", FIXTURE_TORRENTS,
                 "case 3 full pass (no rows inserted)");

    /* Completion bookkeeping: the generation is published and the resume key is
     * tombstoned, so the next generation starts from the beginning. */
    snprintf(want_ver, sizeof want_ver, "%d", STUB_GENERATION);
    expect_meta(db->db, MOVIE_BACKFILL_VER_KEY, want_ver, "case 3 completion meta");
    expect_meta(db->db, MOVIE_BACKFILL_RESUME_KEY, "", "case 3 completion meta");

    /* A second pass has nothing to do and writes nothing. */
    rows_left = -1;
    if (movie_backfill_needed(db, 0, &rows_left) != 0 || rows_left != 0) {
        char detail[128];
        snprintf(detail, sizeof detail, "needed()=%lld rows_left", rows_left);
        fail("case 3 second pass", detail);
    }
    stub_call_count = 0;
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 0 ||
        progress.rows_total != 0 || progress.rows_updated != 0) {
        fail("case 3 second pass", "a converted database still had work");
    }
    if (stub_call_count != 0) {
        fail("case 3 second pass", "the classifier was called for zero candidates");
    }
    printf("  case 3 ok: %d candidate(s) labelled, fastText columns untouched, second pass a "
           "no-op\n",
           FIXTURE_CANDIDATES);
}

/* 4: mask 0 is a valid label - a `none` verdict is written and not re-offered. */
static void case_mask_zero(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;
    fasttext_row_t ft;
    movie_row_t mv;
    long long rows_left = -1;

    build_fixture(db);
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 4 mask zero", "run() failed");
    }
    read_row(db->db, TORRENT_NONE, &ft, &mv, "case 4 mask zero");
    if (mv.mask_is_null || mv.mask != 0 || mv.ver_is_null || mv.ver != STUB_GENERATION) {
        fail("case 4 mask zero", "the `none` verdict was not stored as mask 0 at the target");
    }
    /* It is not offered again: the generation, not the mask, decides membership. */
    if (movie_backfill_needed(db, 0, &rows_left) != 0 || rows_left != 0) {
        fail("case 4 mask zero", "a mask-0 row was still offered as a candidate");
    }
    printf("  case 4 ok: mask 0 is a valid label and is never re-offered\n");
}

/* 5: no verdict leaves the stored columns untouched and the row a candidate. */
static void case_no_verdict(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;
    fasttext_row_t ft;
    movie_row_t mv;
    long long rows_left = -1;

    build_fixture(db);
    /* Rename one candidate into the fake classifier's refusal range. */
    exec_sql(db->db, "UPDATE torrents SET name = 'FAIL-alpha' WHERE id = 1",
             "case 5 rename");
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 5 no verdict", "run() failed");
    }
    if (progress.rows_failed != 1 || progress.rows_updated != FIXTURE_CANDIDATES - 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "failed=%lld updated=%lld", progress.rows_failed,
                 progress.rows_updated);
        fail("case 5 no verdict", detail);
    }
    read_row(db->db, TORRENT_MOVIE_A, &ft, &mv, "case 5 no verdict");
    if (!mv.mask_is_null || !mv.ver_is_null) {
        fail("case 5 no verdict", "a row without a verdict was written anyway");
    }
    expect_fasttext_unchanged(db->db, TORRENT_MOVIE_A, "case 5 no verdict");
    /* Still a candidate: the next pass retries it. */
    if (movie_backfill_needed(db, 0, &rows_left) != 1 || rows_left != 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "needed()=%lld rows_left", rows_left);
        fail("case 5 no verdict", detail);
    }
    printf("  case 5 ok: a row without a verdict is left untouched and stays a candidate\n");
}

/* 6: the resume key is per batch, generation-scoped and tombstoned at completion. */
static void case_resume(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;
    char want_resume[64];

    build_fixture(db);
    memset(&opts, 0, sizeof opts);
    opts.chunk_ids = 2;
    opts.batch_rows = 1;
    opts.limit_rows = 1; /* stop after the first candidate */
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 6 resume", "the limited pass failed");
    }
    if (progress.state != MOVIE_BACKFILL_STOPPED ||
        strcmp(movie_backfill_state_name(progress.state), "stopped") != 0) {
        fail("case 6 resume", "a limited pass did not report STOPPED/\"stopped\"");
    }
    if (progress.rows_done != 1 || progress.rows_skipped != FIXTURE_CANDIDATES - 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "done=%lld skipped=%lld", progress.rows_done,
                 progress.rows_skipped);
        fail("case 6 resume", detail);
    }
    /* The resume key names the generation and the last committed id. */
    snprintf(want_resume, sizeof want_resume, "%d:%lld", STUB_GENERATION, progress.last_id);
    expect_meta(db->db, MOVIE_BACKFILL_RESUME_KEY, want_resume, "case 6 resume key");
    /* ... and the generation is NOT published by an interrupted pass. */
    expect_meta_absent(db->db, MOVIE_BACKFILL_VER_KEY, "case 6 resume (no completion meta)");

    /* The continuation resumes after the committed id: one candidate fewer. */
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 0 ||
        progress.state != MOVIE_BACKFILL_DONE) {
        fail("case 6 resume", "the continuation did not finish");
    }
    if (progress.rows_total != FIXTURE_CANDIDATES - 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "rows_total=%lld, want %d (resume not honoured)",
                 progress.rows_total, FIXTURE_CANDIDATES - 1);
        fail("case 6 resume", detail);
    }
    expect_meta(db->db, MOVIE_BACKFILL_RESUME_KEY, "", "case 6 completion");

    /* A key from another generation can never skip rows. */
    database_cleanup(db);
    build_fixture(db);
    exec_sql(db->db,
             "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES "
             "('movie_backfill_last_id', '9999:999999')",
             "case 6 foreign resume key");
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 6 foreign key", "run() failed");
    }
    if (progress.rows_total != FIXTURE_CANDIDATES || progress.rows_done != FIXTURE_CANDIDATES) {
        char detail[128];
        snprintf(detail, sizeof detail, "total=%lld done=%lld", progress.rows_total,
                 progress.rows_done);
        fail("case 6 foreign key", detail);
    }
    printf("  case 6 ok: resume key is per batch, generation-scoped and tombstoned\n");
}

/* 7: thermal back-off - counted once, slept in slices, resumed; a stop during the
 * pause ends the pass as stopped. */
static void case_thermal(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;

    build_fixture(db);
    /* Hot enough to pause (>= 82 C), then still hot twice, then below 80 C. */
    stub_temps[0] = 90;
    stub_temps[1] = 88;
    stub_temps[2] = 85;
    stub_temp_count = 3;
    stub_cool_c = 79;

    memset(&opts, 0, sizeof opts);
    opts.chunk_ids = 2;
    opts.batch_rows = 1;         /* a gate check after every committed batch */
    opts.thermal_backoff_c = 82; /* resume margin is 2 C, so 80 C */
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_run(db, &opts, &progress) != 0 ||
        progress.state != MOVIE_BACKFILL_DONE) {
        fail("case 7 thermal", "the pass did not complete after the pause");
    }
    if (progress.thermal_pauses != 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "thermal_pauses=%lld, want 1",
                 progress.thermal_pauses);
        fail("case 7 thermal", detail);
    }
    if (stub_sleep_calls < 3) {
        fail("case 7 thermal", "the pause was not slept in slices");
    }
    if (stub_pause_calls != 1) {
        fail("case 7 thermal", "the pause was not reported to the movie-kind module once");
    }
    expect_labelled(db->db, TORRENT_MOVIE_A, STUB_MASK_A, "case 7 thermal");
    expect_labelled(db->db, TORRENT_NONE, STUB_MASK_NONE, "case 7 thermal");

    /* The same heat, but a shutdown arrives while the pass is paused: it ends as
     * stopped and resumable rather than finishing. */
    database_cleanup(db);
    build_fixture(db);
    reset_stub();
    stub_temps[0] = 90;
    stub_temp_count = 1;
    stub_cool_c = 90; /* never cools: only the stop request ends the pause */
    stub_sleep_stop = 1;

    memset(&opts, 0, sizeof opts);
    opts.chunk_ids = 2;
    opts.batch_rows = 1;
    opts.thermal_backoff_c = 82;
    memset(&progress, 0, sizeof progress);

    if (movie_backfill_run(db, &opts, &progress) != 0 ||
        progress.state != MOVIE_BACKFILL_STOPPED) {
        fail("case 7 thermal stop", "a stop during the pause did not end the pass as stopped");
    }
    if (progress.thermal_pauses != 1 || stub_sleep_calls < 1) {
        fail("case 7 thermal stop", "the pause was not counted/slept");
    }
    /* Whatever committed before the pause is labelled; the rest is still a
     * candidate for the next pass. */
    if (movie_backfill_needed(db, 0, NULL) != 1) {
        fail("case 7 thermal stop", "the stopped pass reported nothing left to do");
    }
    reset_stub();
    printf("  case 7 ok: thermal back-off pauses, counts once, resumes, and honours a stop\n");
}

/* 8: movie_backfill_needed() in both directions. */
static void case_needed(database_t *db) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;
    long long rows_left = -1;

    build_fixture(db);
    if (movie_backfill_needed(db, 0, &rows_left) != 1 || rows_left != FIXTURE_CANDIDATES) {
        fail("case 8 needed", "candidates were not reported");
    }
    /* Every row converted but the meta key never written (a pass that died after
     * its last batch): still "needed", with zero rows left. */
    memset(&opts, 0, sizeof opts);
    memset(&progress, 0, sizeof progress);
    if (movie_backfill_run(db, &opts, &progress) != 0) {
        fail("case 8 needed", "run() failed");
    }
    exec_sql(db->db, "DELETE FROM classifier_meta WHERE key = 'movie_kind_clf_ver'",
             "case 8 needed (drop the meta row)");
    rows_left = -1;
    if (movie_backfill_needed(db, 0, &rows_left) != 1 || rows_left != 0) {
        char detail[128];
        snprintf(detail, sizeof detail, "needed()=%lld rows_left", rows_left);
        fail("case 8 needed", detail);
    }
    /* An explicit target generation is honoured as given: one generation further
     * on, the row that was already current falls behind too - so every movie row
     * of the fixture (the four candidates plus the one that was current) is
     * offered again, and the tv/adult rows still are not. */
    rows_left = -1;
    if (movie_backfill_needed(db, STUB_GENERATION + 1, &rows_left) != 1 ||
        rows_left != FIXTURE_CANDIDATES + 1) {
        char detail[128];
        snprintf(detail, sizeof detail, "rows_left=%lld, want %d", rows_left,
                 FIXTURE_CANDIDATES + 1);
        fail("case 8 needed", detail);
    }
    printf("  case 8 ok: needed() reports rows-left and meta-only-behind in both directions\n");
}

/* 9: the `--dump-masks` loop prints a mask line only for a row that classified.
 * A failed row must be skipped - never printed as mask 0, which is a real `none`
 * verdict and would make a failure indistinguishable from it. */
static void case_dump_masks(void) {
    if (!movie_backfill_test_should_print_mask(0)) {
        fail("case 9 dump masks", "a measured row must print its mask line");
    }
    if (movie_backfill_test_should_print_mask(-1)) {
        fail("case 9 dump masks", "an I/O error row must not print a mask line");
    }
    if (movie_backfill_test_should_print_mask(1)) {
        fail("case 9 dump masks", "a no-verdict row must not print a mask line");
    }
    printf("  case 9 ok: --dump-masks prints no mask line for a failed row\n");
}

int main(void) {
    database_t db;

    memset(&db, 0, sizeof db);
    memset(&test_app_ctx, 0, sizeof test_app_ctx);
    install_hooks();

    printf("test_movie_backfill: resumable movie-kind backfill engine\n");

    /* Every case rebuilds the scratch database, so the previous connection is
     * closed first: the sanitizer gate counts a leaked handle as a failure. */
    case_disabled(&db);
    database_cleanup(&db);
    case_dry_run(&db);
    database_cleanup(&db);
    case_full_pass(&db);
    database_cleanup(&db);
    case_mask_zero(&db);
    database_cleanup(&db);
    case_no_verdict(&db);
    database_cleanup(&db);
    case_resume(&db);
    database_cleanup(&db);
    case_thermal(&db);
    database_cleanup(&db);
    case_needed(&db);
    database_cleanup(&db);
    case_dump_masks();

    remove_scratch_db();
    printf("test_movie_backfill: all assertions passed\n");
    return 0;
}
