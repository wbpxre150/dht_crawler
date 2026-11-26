/**
 * Host unit tests for the non-Latin language filter (include/language_filter.h).
 *
 * The module is pure string logic over torrent_metadata_t, so the whole thing
 * runs without a database, a bloom filter or a uv loop - it needs only the log
 * stub. The invariants that matter, and that this file pins:
 *
 *   (a) `threshold` is a floor: a torrent AT the threshold is filtered, one
 *       below it is not (the header comment used to say "exceed", which
 *       disagreed with the comparison);
 *   (b) the percentage test is exact, not truncating: 1 non-Latin char out of 3
 *       is 33.3 %, i.e. at the 33 % threshold, and must filter;
 *   (c) 0 disables the filter completely - fail-open, and nothing is even
 *       counted, so a disabled filter cannot look like a filtering one;
 *   (d) an uninitialized module is fail-open too;
 *   (e) the out-of-range values are clamped (150 -> 100, -5 -> disabled);
 *   (f) the name AND every file path are checked: one non-Latin episode name
 *       drops the whole torrent;
 *   (g) ASCII digits, punctuation and Latin-with-diacritics are neutral/Latin,
 *       and Latin Extended Additional counts as Latin;
 *   (h) the stats block reports the effective state, so /stats can tell
 *       "disabled" from "nothing seen yet".
 *
 * Standalone: no test framework.
 *
 *     make tests-language-filter
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "database.h"
#include "language_filter.h"

static int failures = 0;

#define CHECK(cond, ...) do {                                       \
        if (!(cond)) {                                              \
            printf("FAIL: ");                                       \
            printf(__VA_ARGS__);                                    \
            printf(" (%s:%d)\n", __FILE__, __LINE__);               \
            failures++;                                             \
        }                                                           \
    } while (0)

/* Fixtures: "Latin" = 5 latin chars, 0 non-latin. */
static const char *NAME_LATIN        = "Ubuntu 24.04 x86_64";
static const char *NAME_CJK          = "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e";  /* 日本語 */
static const char *NAME_CYRILLIC     = "\xd0\x9c\xd0\xb0\xd1\x82\xd1\x80\xd1\x91\xd1\x88\xd0\xba\xd0\xb0"; /* Матрёшка */
static const char *NAME_LATIN_DIAC   = "Caf\xc3\xa9 M\xc3\xbcnchen";            /* Café München */
/* U+1E9E (ẞ) lives in Latin Extended Additional: Latin, not non-Latin. The
 * string is split because \x escapes are greedy - "\x9ee" would parse as one
 * (out-of-range) hex escape instead of U+1E9E followed by 'e'. */
static const char *NAME_LATIN_EXTADD = "Stra\xe1\xba\x9e" "e";                /* Straẞe */
/* 2 Latin + 1 non-Latin = 3 script chars, 33.33 % non-Latin: at a 33 %
 * threshold, so it must be filtered (the boundary case). */
static const char *NAME_ONE_IN_THREE = "ab\xe6\x97\xa5";
/* 1 non-Latin char in 10 letters = 10 %, well below 33 %. */
static const char *NAME_ONE_IN_TEN   = "abcdefghi\xe6\x97\xa5";

static torrent_metadata_t make_meta(const char *name) {
    torrent_metadata_t m;
    memset(&m, 0, sizeof(m));
    m.name = (char *)name;
    return m;
}

static file_info_t make_file(const char *path) {
    file_info_t f;
    memset(&f, 0, sizeof(f));
    f.path = (char *)path;
    return f;
}

int main(void) {
    printf("test_language_filter: threshold semantics, fail-open, stats\n");

    /* (d) Before any init: fail-open, and nothing counted. */
    language_filter_cleanup();
    {
        torrent_metadata_t m = make_meta(NAME_CJK);
        language_filter_stats_t st;
        CHECK(language_filter_check(&m) == 0,
              "an uninitialized filter must never filter");
        language_filter_get_stats(&st);
        CHECK(st.enabled == 0, "uninitialized filter must report enabled=0 (got %d)", st.enabled);
        CHECK(st.total_checked == 0, "an uninitialized filter must not count (got %llu)",
              (unsigned long long)st.total_checked);
    }

    /* (a) The shipped default: 33 %, enabled. */
    CHECK(language_filter_init(33) == 0, "init(33) failed");
    {
        language_filter_stats_t st;
        language_filter_get_stats(&st);
        CHECK(st.enabled == 1, "init(33) must report enabled=1");
        CHECK(st.threshold == 33, "init(33) must report threshold 33 (got %d)", st.threshold);
    }

    {
        torrent_metadata_t latin = make_meta(NAME_LATIN);
        torrent_metadata_t cjk = make_meta(NAME_CJK);
        torrent_metadata_t cyr = make_meta(NAME_CYRILLIC);
        torrent_metadata_t dia = make_meta(NAME_LATIN_DIAC);
        torrent_metadata_t ext = make_meta(NAME_LATIN_EXTADD);
        torrent_metadata_t one3 = make_meta(NAME_ONE_IN_THREE);
        torrent_metadata_t one10 = make_meta(NAME_ONE_IN_TEN);

        CHECK(language_filter_check(&latin) == 0, "ASCII name must pass");
        CHECK(language_filter_check(&cjk) == 1, "CJK name must be filtered");
        CHECK(language_filter_check(&cyr) == 1, "Cyrillic name must be filtered");
        CHECK(language_filter_check(&dia) == 0,
              "Latin with diacritics (U+00E9/U+00FC) must pass");
        CHECK(language_filter_check(&ext) == 0,
              "Latin Extended Additional (U+1E9E) must pass");
        /* (b) exactly at the threshold filters. */
        CHECK(language_filter_check(&one3) == 1,
              "1 non-Latin in 3 (33.3%%) must filter at a 33%% threshold");
        CHECK(language_filter_check(&one10) == 0,
              "1 non-Latin in 10 (10%%) must pass a 33%% threshold");
    }

    /* (h) Counters: every enabled check counts, only the matches add to the
     * filtered buckets. The block above ran 7 checks, 3 of them filtered. */
    {
        language_filter_stats_t st;
        language_filter_get_stats(&st);
        CHECK(st.total_checked == 7, "total_checked must count enabled checks (got %llu)",
              (unsigned long long)st.total_checked);
        CHECK(st.total_filtered == 3, "total_filtered must be 3 (got %llu)",
              (unsigned long long)st.total_filtered);
        CHECK(st.filtered_by_non_latin == 3, "filtered_by_non_latin must be 3 (got %llu)",
              (unsigned long long)st.filtered_by_non_latin);
    }

    /* (f) A Latin name with one non-Latin file path drops the whole torrent. */
    {
        torrent_metadata_t m = make_meta(NAME_LATIN);
        file_info_t files[2];
        files[0] = make_file("Season 1/episode-01.mkv");
        /* The ratio is over the whole path, so a CJK directory with an ASCII
         * filename can fall below the threshold ("第一季/episode-02.mkv" is 3
         * non-Latin in 13 script chars = 23 %). A path where the non-Latin
         * share dominates does trip it. */
        files[1] = make_file("\xe7\xac\xac\xe4\xb8\x80\xe5\xad\xa3.mkv");  /* 第一季.mkv */
        m.files = files;
        m.num_files = 2;
        CHECK(language_filter_check(&m) == 1,
              "one non-Latin file path must filter the torrent");

        /* …but a Latin file list does not. */
        m.num_files = 1;
        CHECK(language_filter_check(&m) == 0,
              "a Latin file list must pass");
    }

    /* (c) 0 disables the filter: fail-open and not counted. */
    CHECK(language_filter_init(0) == 0, "init(0) failed");
    {
        torrent_metadata_t cjk = make_meta(NAME_CJK);
        language_filter_stats_t before, after;
        language_filter_get_stats(&before);
        CHECK(language_filter_check(&cjk) == 0, "a disabled filter must never filter");
        language_filter_get_stats(&after);
        CHECK(after.enabled == 0, "a disabled filter must report enabled=0");
        CHECK(after.threshold == 0, "a disabled filter must report threshold 0 (got %d)",
              after.threshold);
        CHECK(after.total_checked == before.total_checked,
              "a disabled filter must not count checks (%llu -> %llu)",
              (unsigned long long)before.total_checked,
              (unsigned long long)after.total_checked);
    }

    /* (e) Clamps. */
    CHECK(language_filter_init(150) == 0, "init(150) failed");
    {
        language_filter_stats_t st;
        language_filter_get_stats(&st);
        CHECK(st.threshold == 100, "150 must clamp to 100 (got %d)", st.threshold);
        CHECK(st.enabled == 1, "a clamped 100 must stay enabled");
    }
    {
        /* At 100 % nothing with any Latin letter can be filtered, and a
         * non-Latin-only name is exactly 100 %. */
        torrent_metadata_t cjk = make_meta(NAME_CJK);
        torrent_metadata_t latin = make_meta(NAME_LATIN);
        CHECK(language_filter_check(&cjk) == 1, "100%% non-Latin must filter at 100");
        CHECK(language_filter_check(&latin) == 0, "pure Latin must never filter");
    }
    CHECK(language_filter_init(-5) == 0, "init(-5) failed");
    {
        language_filter_stats_t st;
        language_filter_get_stats(&st);
        CHECK(st.threshold == 0, "a negative threshold must clamp to 0 (got %d)", st.threshold);
        CHECK(st.enabled == 0, "a clamped 0 must be disabled");
    }

    /* NULL tolerance: fail-open, like the disabled path. */
    CHECK(language_filter_init(33) == 0, "re-init(33) failed");
    CHECK(language_filter_check(NULL) == 0, "a NULL metadata must fail open");

    language_filter_cleanup();

    if (failures != 0) {
        printf("test_language_filter: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_language_filter: all checks passed\n");
    return 0;
}
