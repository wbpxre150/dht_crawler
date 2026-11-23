/**
 * Host unit test pinning the CONJUNCTIVE movie_kind facet.
 *
 * The Stage 8 facet selects rows by torrent_class.movie_kind_mask. The
 * movie-kind-conjunctive patch makes that test all-of: a row must carry EVERY
 * selected genre, and "none" (mask SQL NULL or 0) stays an additional OR arm.
 * The predicate exists twice in src/http_api.c — once as SQL emitted by
 * append_class_conditions() (used by /search, /browse and the /random top-up
 * fill) and once as the C mirror cls_movie_kind_row_matches() the /random probe
 * pass runs per candidate. Both are pinned here, against the real code:
 *
 *   1. the fragment text (and that the one mask bind is referenced twice, so
 *      bind_class_filter() and CLS_FILTER_MAX_BINDS stay correct);
 *   2. the row sets the fragment returns over a fixed seed of five rows;
 *   3. the probe predicate's verdicts, including the load-bearing
 *      want_bits != 0 guard.
 *
 * src/http_api.c is compiled INTO this translation unit: cls_filter_t,
 * append_class_conditions() and bind_class_filter() are file-local there, and
 * this is the only way to exercise the real predicate instead of a copy of it.
 * The Makefile target builds with -ffunction-sections/--gc-sections, so every
 * part of http_api.c this test does not reach is dropped at link time and the
 * binary needs nothing but SQLite (plus src/database.c for the schema).
 *
 * The seed is fixed so every expected row set is exact:
 *   R1 mask = action|comedy   R2 mask = action   R3 mask = comedy
 *   R4 mask = 0 (a valid "none" verdict)   R5 mask = SQL NULL (never classified)
 *
 * Standalone: no test framework, no dependency on the daemon build.
 *
 *     make tests        # or: make tests-movie-kind-filter
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "database.h"
#include "dht_crawler.h"

/* The subject under test: see the file header for why the .c is included. */
#include "../src/http_api.c"

/* CLS_MOVIE_KIND_NAMES[0] = "action", [1] = "comedy"; the facet stores the OR
 * of 1 << index, so any two genres behave like these two. */
#define G1_BIT (1 << 0)
#define G2_BIT (1 << 1)

#define R1_MASK (G1_BIT | G2_BIT)
#define R2_MASK (G1_BIT)
#define R3_MASK (G2_BIT)
#define R4_MASK (0)

/* Failing assert: print it and exit non-zero (no test framework here). */
static void tf_fail(const char *what, const char *detail) {
    fprintf(stderr, "test_movie_kind_filter: FAILED: %s", what);
    if (detail && *detail) {
        fprintf(stderr, ": %s", detail);
    }
    fprintf(stderr, "\n");
    exit(1);
}

static void tf_expect_str(const char *what, const char *got, const char *want) {
    if (strcmp(got, want) != 0) {
        char detail[512];
        snprintf(detail, sizeof(detail), "got [%s], want [%s]", got, want);
        tf_fail(what, detail);
    }
}

static void tf_expect_contains(const char *what, const char *haystack, const char *needle) {
    if (strstr(haystack, needle) == NULL) {
        char detail[512];
        snprintf(detail, sizeof(detail), "[%s] missing from [%s]", needle, haystack);
        tf_fail(what, detail);
    }
}

static void tf_expect_absent(const char *what, const char *haystack, const char *needle) {
    if (strstr(haystack, needle) != NULL) {
        char detail[512];
        snprintf(detail, sizeof(detail), "[%s] present in [%s]", needle, haystack);
        tf_fail(what, detail);
    }
}

static void tf_expect_int(const char *what, int got, int want) {
    if (got != want) {
        char detail[128];
        snprintf(detail, sizeof(detail), "got %d, want %d", got, want);
        tf_fail(what, detail);
    }
}

/* The movie-kind filter of a query: no adult / category / tv_kind condition, so
 * the genre-mask fragment is the whole " AND ..." tail. */
static void tf_filter(cls_filter_t *f, int want_bits, int want_none) {
    memset(f, 0, sizeof(*f));
    f->adult = CLS_ADULT_SHOW;
    f->has_movie_kind = 1;
    f->movie_genre_bits = want_bits;
    f->movie_none = want_none;
}

/* Emit the fragment for *f* onto " WHERE 1" and return the bind count. */
static int tf_fragment(const cls_filter_t *f, char *buf, size_t bufsize) {
    snprintf(buf, bufsize, " WHERE 1");
    int pos = (int)strlen(buf);
    return append_class_conditions(buf, bufsize, &pos, f, 1);
}

/* Seed the five rows through the crawler's own database_init()/schema DDL, so
 * the test runs against the real torrent_class definition (nullable
 * movie_kind_mask included). */
static void tf_seed(database_t *db, app_context_t *ctx) {
    if (database_init(db, ":memory:", ctx) != 0) {
        tf_fail("database_init(:memory:)", NULL);
    }
    if (database_create_schema(db) != 0) {
        tf_fail("database_create_schema()", NULL);
    }

    static const char *const names[] = {"R1", "R2", "R3", "R4", "R5"};
    static const int masks[] = {R1_MASK, R2_MASK, R3_MASK, R4_MASK, -1}; /* -1 => NULL */
    char detail[128];

    for (int i = 0; i < 5; i++) {
        sqlite3_stmt *stmt = NULL;
        const char *torrent_sql =
            "INSERT INTO torrents (id, info_hash, name, size_bytes, total_peers, added_timestamp) "
            "VALUES (?, ?, ?, 1, 0, 1)";
        if (sqlite3_prepare_v2(db->db, torrent_sql, -1, &stmt, NULL) != SQLITE_OK) {
            tf_fail("prepare torrents insert", sqlite3_errmsg(db->db));
        }
        unsigned char info_hash[20];
        memset(info_hash, 0, sizeof(info_hash));
        info_hash[19] = (unsigned char)(i + 1);
        sqlite3_bind_int(stmt, 1, i + 1);
        sqlite3_bind_blob(stmt, 2, info_hash, sizeof(info_hash), SQLITE_STATIC);
        sqlite3_bind_text(stmt, 3, names[i], -1, SQLITE_STATIC);
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            snprintf(detail, sizeof(detail), "torrent %d: %s", i + 1, sqlite3_errmsg(db->db));
            tf_fail("insert torrents row", detail);
        }
        sqlite3_finalize(stmt);

        const char *class_sql =
            "INSERT INTO torrent_class (torrent_id, category, tv_kind, adult, clf_ver, movie_kind_mask) "
            "VALUES (?, ?, NULL, 0, 1, ?)";
        if (sqlite3_prepare_v2(db->db, class_sql, -1, &stmt, NULL) != SQLITE_OK) {
            tf_fail("prepare torrent_class insert", sqlite3_errmsg(db->db));
        }
        sqlite3_bind_int(stmt, 1, i + 1);
        sqlite3_bind_int(stmt, 2, CLS_CAT_MOVIE);
        if (masks[i] < 0) {
            sqlite3_bind_null(stmt, 3);
        } else {
            sqlite3_bind_int(stmt, 3, masks[i]);
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            snprintf(detail, sizeof(detail), "class row %d: %s", i + 1, sqlite3_errmsg(db->db));
            tf_fail("insert torrent_class row", detail);
        }
        sqlite3_finalize(stmt);
    }
}

/* Run the emitted fragment + binder and render the matching torrent_ids as
 * "1,4,5" (ascending). */
static void tf_expect_rows(database_t *db, int want_bits, int want_none,
                           const char *want_ids, const char *what) {
    cls_filter_t f;
    tf_filter(&f, want_bits, want_none);

    char fragment[512];
    int binds = tf_fragment(&f, fragment, sizeof(fragment));
    /* Only the genre-mask form consumes a bind; "none" alone is pure SQL. */
    int want_binds = want_bits != 0 ? 1 : 0;
    if (binds != want_binds) {
        char detail[640];
        snprintf(detail, sizeof(detail), "fragment [%s] consumed %d binds, want %d",
                 fragment, binds, want_binds);
        tf_fail(what, detail);
    }

    char sql[1024];
    snprintf(sql, sizeof(sql),
             "SELECT c.torrent_id FROM torrent_class c%s ORDER BY c.torrent_id", fragment);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        char detail[1200];
        snprintf(detail, sizeof(detail), "sql [%s]: %s", sql, sqlite3_errmsg(db->db));
        tf_fail(what, detail);
    }
    bind_class_filter(stmt, 1, &f);

    char got[256];
    size_t len = 0;
    got[0] = '\0';
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        len += (size_t)snprintf(got + len, sizeof(got) - len, "%s%d", len ? "," : "",
                                sqlite3_column_int(stmt, 0));
    }
    if (rc != SQLITE_DONE) {
        char detail[1200];
        snprintf(detail, sizeof(detail), "sql [%s]: %s", sql, sqlite3_errmsg(db->db));
        tf_fail(what, detail);
    }
    sqlite3_finalize(stmt);

    tf_expect_str(what, got, want_ids);
}

int main(void) {
    /* --- 1. fragment text ------------------------------------------------- */
    cls_filter_t f;
    char fragment[512];

    tf_filter(&f, R1_MASK, 0);
    int binds = tf_fragment(&f, fragment, sizeof(fragment));
    tf_expect_int("genre-only fragment bind count", binds, 1);
    tf_expect_contains("genre-only fragment is conjunctive", fragment,
                       "(c.movie_kind_mask & ?1) = ?1");
    tf_expect_absent("genre-only fragment is not an any-of test", fragment, "<> 0");
    tf_expect_absent("genre-only fragment has no none arm", fragment,
                     "c.movie_kind_mask IS NULL");

    tf_filter(&f, R1_MASK, 1);
    binds = tf_fragment(&f, fragment, sizeof(fragment));
    tf_expect_int("genre+none fragment bind count", binds, 1);
    tf_expect_contains("genre+none fragment is conjunctive", fragment,
                       "(c.movie_kind_mask & ?1) = ?1");
    tf_expect_contains("genre+none fragment keeps the none arm", fragment,
                       "OR c.movie_kind_mask IS NULL OR c.movie_kind_mask = 0");
    tf_expect_absent("genre+none fragment is not an any-of test", fragment, "<> 0");

    /* --- 2. SQL behaviour over the fixed seed ----------------------------- */
    database_t db;
    app_context_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    tf_seed(&db, &ctx);

    tf_expect_rows(&db, R1_MASK, 0, "1", "all selected genres (R1 only)");
    tf_expect_rows(&db, R1_MASK, 1, "1,4,5", "all selected genres or none");
    tf_expect_rows(&db, 0, 1, "4,5", "none alone");
    tf_expect_rows(&db, G1_BIT, 0, "1,2", "one genre");

    database_cleanup(&db);

    /* --- 3. probe predicate mirror --------------------------------------- */
    tf_expect_int("probe: both genres on a both-genre row",
                  cls_movie_kind_row_matches(R1_MASK, 0, 1, R1_MASK), 1);
    tf_expect_int("probe: both genres on a one-genre row",
                  cls_movie_kind_row_matches(R1_MASK, 0, 1, R2_MASK), 0);
    tf_expect_int("probe: both genres or none on a mask-0 row",
                  cls_movie_kind_row_matches(R1_MASK, 1, 1, R4_MASK), 1);
    tf_expect_int("probe: both genres or none on an unclassified row",
                  cls_movie_kind_row_matches(R1_MASK, 1, 0, 0), 1);
    tf_expect_int("probe: none alone does not match a classified row",
                  cls_movie_kind_row_matches(0, 1, 1, R1_MASK), 0);
    /* (0, none=0) is unreachable in product code: random_candidate_eligible_ex()
     * calls this helper only under `want_movie_bits != 0 || want_movie_none`, and
     * "no movie constraint at all" is owned by its no-class-row tail (which
     * returns 1 when want_movie_bits == 0). Pinned as the helper behaves: with no
     * genre and no none arm, nothing hits. */
    tf_expect_int("probe: no genre and no none arm never matches",
                  cls_movie_kind_row_matches(0, 0, 1, R1_MASK), 0);

    printf("test_movie_kind_filter: OK (fragment text, SQL row sets, probe mirror)\n");
    return 0;
}
