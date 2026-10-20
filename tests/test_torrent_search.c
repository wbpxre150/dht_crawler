/**
 * Host unit tests for extract_media_title().
 *
 * Covers the scene-release pivot ladder (year -> season -> quality) plus the
 * legacy keyword-stripping path, NULL and empty input.
 *
 * Standalone: no test framework, no dependency on the daemon build.
 * Run from the repository root (the keyword file is opened relative to cwd):
 *
 *     make tests        # or: make tests-asan
 */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "torrent_search.h"
#include "dht_crawler.h"

typedef struct {
    const char *input;
    const char *expected;
    const char *note;
} title_case_t;

/* Keyword file lives at the repo root; the test binary runs with cwd=repo root. */
static const char *const KEYWORD_FILE = "torrent_search_keywords.txt";

static const title_case_t CASES[] = {
    /* 1. Year-shaped title survives: year pivot is inclusive. */
    {"1917.2019.1080p.BluRay.x264-NOGRP", "1917 2019", "year-shaped title"},
    /* 2. Plain year pivot. */
    {"Some.Movie.2020.1080p.WEB-DL.x264-RARBG", "Some Movie 2020", "year pivot"},
    /* 3. Title word that looks like a year -> FIRST hit wins. */
    {"Blade.Runner.2049.2017.2160p.WEB-DL.x265-GROUP", "Blade Runner 2049", "year-shaped title word"},
    /* 4. Title word "IT" collides with keyword-file entry "it": the pivot path
     *    must not consult keywords, so the title survives intact. */
    {"It.2017.1080p.BluRay.x264-GRP", "It 2017", "keyword collision on pivot path"},
    /* 5. Season pivot. */
    {"Show.Name.S01E05.1080p.x264-GRP", "Show Name", "season pivot"},
    /* 6. Year beats season. */
    {"Movie.Name.2019.Season.1.1080p", "Movie Name 2019", "year beats season"},
    /* 7. Quality pivot, no year. */
    {"Some.Movie.1080p.BluRay.x264", "Some Movie", "quality pivot"},
    /* 8. Parenthesised year is kept (and stripped of parens) then pivots. */
    {"Movie.Name.(2020).1080p", "Movie Name 2020", "parens year"},
    /* 9. "Season N" marker with no year -> legacy truncation, no keywords left. */
    {"Weeds Season 2 1080p", "Weeds", "season marker, no year"},
    /* 10. Legacy path: no pivot at all (no year, no season, no quality tag), so
     *     keyword stripping runs. "BluRay" is a whole-word keyword-file entry
     *     (#32 "BluRay", stored lowercased); no other entry matches a token. */
    {"My BluRay Adventure", "My Adventure", "legacy keyword strip"},
    /* 11. Tracker hostname prefix (www) with a dash separator is dropped, and
     *     the year pivot then lands on the real release year. */
    {"www.UIndex.org    -    Yellowstone.2018.S04E08.No.Kindness.1080p.BluRay.x264-OFT",
     "Yellowstone 2018", "www tracker prefix"},
    /* 12. Same prefix, season pivot. */
    {"www.Torrenting.com - The.Pitt.S01E05.1080p.AMZN.WEB-DL.DDP5.1.H.264-FLUX",
     "The Pitt", "www tracker prefix, season pivot"},
    /* 13. Non-TLD host suffix ("ing") is fine for a www prefix; parens year. */
    {"www.1TamilMV.ing - Kattalan (2026) Tamil HQ HDRip - 1080p - x265 - HEVC",
     "Kattalan 2026", "www tracker prefix, parens year"},
    /* 14. Bare tracker domain (no www) whose TLD is a known one. */
    {"rarbg.com - Some.Movie.2020.1080p.WEB-DL.x264",
     "Some Movie 2020", "bare tracker domain prefix"},
    /* 15. Bracketed prefix: the host itself is removed by remove_brackets, the
     *     leftover leading separator must not survive either. */
    {"[ www.torrenting.com ] - Carlin.At.Carnegie.1983.iNTERNAL.DVDRip.x264-REGRET",
     "Carlin At Carnegie 1983", "bracketed tracker prefix, leading dash"},
    /* 16. Non-regression: dot-separated title words before a dash are a title,
     *     not a hostname ("Games" is no tracker TLD), so nothing is stripped. */
    {"The.Hunger.Games - Mockingjay.Part.1.2014.1080p",
     "The Hunger Games - Mockingjay Part 1 2014", "title with dash, not a host"},
    /* 17. Non-regression: a leading separator glued to the title ("-30-") is
     *     part of the title and must not be trimmed. */
    {"-30-.2011.1080p.BluRay.x264-GRP", "-30- 2011", "leading dash in title"},
    /* 18. Bare tracker domain with a two-letter TLD (.re) and a pipe-free dash. */
    {"eztv.re - Show.Name.S01E01.720p", "Show Name", "bare tracker domain (.re)"},
    /* 19. Hyphenated host: the inner hyphen must not be taken for the separator
     *     (a partial strip would leave "bay org - Some Movie 2020"). */
    {"www.torrent-bay.org - Some.Movie.2020.1080p.WEB-DL",
     "Some Movie 2020", "hyphenated tracker host"},
};

#define NUM_CASES (sizeof(CASES) / sizeof(CASES[0]))

static int failures = 0;

static void check_title(const title_case_t *tc) {
    char *got = extract_media_title(tc->input);

    if (got == NULL) {
        fprintf(stderr, "FAIL [%s]: extract_media_title(\"%s\") returned NULL, expected \"%s\"\n",
                tc->note, tc->input, tc->expected);
        failures++;
        return;
    }

    if (strcmp(got, tc->expected) != 0) {
        fprintf(stderr, "FAIL [%s]: extract_media_title(\"%s\") = \"%s\", expected \"%s\"\n",
                tc->note, tc->input, got, tc->expected);
        failures++;
        free(got);
        return;
    }

    free(got);
}

int main(void) {
    size_t total = 0;

    if (torrent_search_init(KEYWORD_FILE) != 0) {
        fprintf(stderr, "FAIL: torrent_search_init(\"%s\") != 0\n", KEYWORD_FILE);
        return EXIT_FAILURE;
    }

    for (size_t i = 0; i < NUM_CASES; i++) {
        check_title(&CASES[i]);
        total++;
    }

    /* NULL input -> NULL. */
    {
        char *got = extract_media_title(NULL);
        total++;
        if (got != NULL) {
            fprintf(stderr, "FAIL [null input]: expected NULL, got \"%s\"\n", got);
            failures++;
            free(got);
        }
    }

    /* Empty string -> non-NULL empty string. */
    {
        char *got = extract_media_title("");
        total++;
        if (got == NULL) {
            fprintf(stderr, "FAIL [empty input]: expected non-NULL empty string, got NULL\n");
            failures++;
        } else {
            if (strcmp(got, "") != 0) {
                fprintf(stderr, "FAIL [empty input]: expected \"\", got \"%s\"\n", got);
                failures++;
            }
            free(got);
        }
    }

    torrent_search_cleanup();

    if (failures != 0) {
        fprintf(stderr, "FAIL %d/%zu cases failed\n", failures, total);
        return EXIT_FAILURE;
    }

    printf("PASS %zu/%zu cases\n", total, total);
    return EXIT_SUCCESS;
}
