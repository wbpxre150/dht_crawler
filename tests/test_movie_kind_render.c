/*
 * Renderer tests for src/movie_kind_render.c — the stage-11 regression gate.
 *
 * Why this file exists: the renderer used to live inside the MK_ENABLED half of
 * src/movie_kind.c, so the host build compiled a stub returning -1 and the one
 * broken function had no host coverage at all. utf8proc_map() was handed length
 * 0 (it does not NUL-scan), it returned an empty string, and every movie
 * rendered "Torrent: \nFilm genres:" — one constant embedding, one constant
 * mask. The renderer is ungated now, so this test compiles the real file in BOTH
 * flavours:
 *
 *   plain     — no MK_NFC_ENABLED: the documented passthrough (bytes unchanged)
 *   utf8proc  — -DMK_NFC_ENABLED plus the vendored utf8proc, i.e. what the
 *               shipped NDK binary compiles
 *
 * The plain variant alone would have caught the gating hole (case (a): the name
 * missing from the prompt); the utf8proc variant catches the length bug itself
 * (an empty NFC result also drops the name from the prompt).
 */

#include "movie_kind.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

static void fail(const char *what, const char *got) {
    printf("FAIL: %s (got \"%s\")\n", what, got ? got : "(null)");
    failures++;
}

/* (a) The prompt contains the name. This is the exact stage-11 regression: with
 * length 0 the NFC result was empty and the name never reached the prompt. */
static void case_name_present(void) {
    const char *name = "They Nest.avi";
    char prompt[256];
    if (movie_kind_prompt_text(name, prompt, sizeof(prompt)) != 0) {
        fail("the renderer succeeds", "rc != 0");
        return;
    }
    if (strstr(prompt, name) == NULL) {
        fail("the prompt contains the name", prompt);
        return;
    }
    printf("  case a ok: the prompt contains the name\n");
}

/* (b) Two different names must not render the same prompt: a constant prompt is a
 * constant embedding, which is the P0's whole failure mode. */
static void case_names_differ(void) {
    char first[256];
    char second[256];
    if (movie_kind_prompt_text("They Nest.avi", first, sizeof(first)) != 0 ||
        movie_kind_prompt_text("City Beneath the Sea [1971 - USA].sci fi", second,
                               sizeof(second)) != 0) {
        fail("both renders succeed", "rc != 0");
        return;
    }
    if (strcmp(first, second) == 0) {
        fail("two different names render different prompts", first);
        return;
    }
    printf("  case b ok: two different names render different prompts\n");
}

/* (c) NFC: "Ame" + U+0301 + "lie" reaches the model composed in the utf8proc
 * flavour, and byte-identically unchanged in the passthrough flavour. */
static void case_nfc(void) {
    const char decomposed[] = "Ame\u0301lie";
    char prompt[256];
    if (movie_kind_prompt_text(decomposed, prompt, sizeof(prompt)) != 0) {
        fail("the renderer succeeds", "rc != 0");
        return;
    }
#ifdef MK_NFC_ENABLED
    if (strcmp(prompt, "Torrent: Am\u00e9lie\nFilm genres:") != 0) {
        fail("a decomposed name is NFC-composed", prompt);
        return;
    }
    printf("  case c ok: a decomposed name is NFC-composed\n");
#else
    if (strcmp(prompt, "Torrent: Ame\u0301lie\nFilm genres:") != 0) {
        fail("without MK_NFC_ENABLED the bytes pass through unchanged", prompt);
        return;
    }
    printf("  case c ok: passthrough build leaves the bytes alone (no NFC)\n");
#endif
}

/* (d) Invalid UTF-8 is not an error: utf8proc_map() refuses such input, so the
 * bytes are used as they are. DB names are not always valid (stage-7 finding),
 * and a dropped or rewritten row would be a different bug. */
static void case_invalid_utf8(void) {
    const char invalid[] = "Bad\xFFname\xC3";
    char prompt[256];
    if (movie_kind_prompt_text(invalid, prompt, sizeof(prompt)) != 0) {
        fail("the renderer accepts invalid UTF-8", "rc != 0");
        return;
    }
    if (strcmp(prompt, "Torrent: Bad\xFFname\xC3\nFilm genres:") != 0) {
        fail("invalid UTF-8 passes through byte-identically", prompt);
        return;
    }
    printf("  case d ok: invalid UTF-8 passes through byte-identically\n");
}

/* (e) The wrapper is byte-exact: "Torrent: " prefix, "\nFilm genres:" suffix,
 * nothing else around the name. Pinned against a literal (not the macros) so a
 * change to MOVIE_KIND_PROMPT_PREFIX/SUFFIX has to be deliberate. */
static void case_wrapper_exact(void) {
    char prompt[256];
    if (movie_kind_prompt_text("They Nest.avi", prompt, sizeof(prompt)) != 0) {
        fail("the renderer succeeds", "rc != 0");
        return;
    }
    if (strcmp(prompt, "Torrent: They Nest.avi\nFilm genres:") != 0) {
        fail("the wrapper is byte-exact", prompt);
        return;
    }
    printf("  case e ok: the wrapper is byte-exact\n");
}

int main(void) {
#ifdef MK_NFC_ENABLED
    const char *flavour = "utf8proc";
#else
    const char *flavour = "passthrough";
#endif

    case_name_present();
    case_names_differ();
    case_nfc();
    case_invalid_utf8();
    case_wrapper_exact();

    if (failures != 0) {
        printf("test_movie_kind_render (%s): %d failure(s)\n", flavour, failures);
        return 1;
    }
    printf("test_movie_kind_render (%s): all 5 cases passed\n", flavour);
    return 0;
}
