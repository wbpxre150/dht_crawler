/* movie_kind_render — the serving text, see include/movie_kind.h.
 *
 * Deliberately *ungated*: this file is not inside `#ifdef MK_ENABLED` and is
 * picked up by both builds' source glob (the host Makefile and the Android
 * wrapper's CMake configure). That is the structural half of the stage-11 P0
 * fix: while the renderer lived in the MK_ENABLED half, the host build compiled
 * a stub returning -1 for movie_kind_prompt_text(), so the one broken function
 * had no host coverage at all — utf8proc_map() was handed length 0 (utf8proc
 * does not NUL-scan), it returned an empty string, and every movie rendered the
 * identical prompt "Torrent: \nFilm genres:" (one constant embedding, one
 * constant mask).
 *
 * Only the NFC step needs a dependency, so it has its own switch:
 *
 *   - MK_NFC_ENABLED — real Unicode NFC. The Android wrapper defines it beside
 *     MK_ENABLED whenever LLAMA_INSTALL is set, so the shipped binary keeps NFC
 *     (utf8proc is linked there for exactly this);
 *   - without it (the vendored host `make` build, a plain CMake configure) the
 *     documented passthrough is used: the name's bytes are taken as they are.
 *     That is already what the NFC step degrades to on invalid UTF-8, so the
 *     wrapper contract is identical in both flavours and
 *     tests/test_movie_kind_render.c pins it byte-exactly in both.
 */

#include "movie_kind.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef MK_NFC_ENABLED
#include <utf8proc.h>
#endif

/* NFC-normalise the name. Invalid UTF-8 is *not* an error here: DB names are
 * not always valid (stage-7 finding), and utf8proc_map() refuses invalid input
 * by writing nothing, so a failure means "use the bytes as they are". */
static char *mk_nfc_dup(const char *name) {
#ifdef MK_NFC_ENABLED
    utf8proc_uint8_t *normalised = NULL;
    /* NFC is "compose, stably": utf8proc has no UTF8PROC_NFC option (its
     * utf8proc_NFC() shortcut leaves the destination uninitialised on error, so
     * the explicit utf8proc_map() call with a NULLed-out destination is used).
     * The length is strlen(name): utf8proc_map() does NOT NUL-scan, and passing
     * 0 here was the stage-11 P0 — it produced an empty string, so the name never
     * reached the serving text. */
    utf8proc_ssize_t rc =
        utf8proc_map((const utf8proc_uint8_t *)name, (utf8proc_ssize_t)strlen(name),
                     &normalised, (utf8proc_option_t)(UTF8PROC_COMPOSE | UTF8PROC_STABLE));
    if (rc < 0 || normalised == NULL) {
        free(normalised);
        return strdup(name);
    }
    char *copy = strdup((const char *)normalised);
    free(normalised);
    return copy;
#else
    /* No NFC in this build (see the file header): bytes in, same bytes out. */
    return strdup(name);
#endif
}

/* The serving text, allocated: "Torrent: " + NFC(name) + "\nFilm genres:". */
static char *mk_prompt_dup(const char *name) {
    char *normalised = mk_nfc_dup(name);
    if (!normalised) return NULL;
    size_t cap = strlen(normalised) + sizeof(MOVIE_KIND_PROMPT_PREFIX) +
                 sizeof(MOVIE_KIND_PROMPT_SUFFIX);
    char *prompt = (char *)malloc(cap);
    if (!prompt) {
        free(normalised);
        return NULL;
    }
    snprintf(prompt, cap, "%s%s%s", MOVIE_KIND_PROMPT_PREFIX, normalised,
             MOVIE_KIND_PROMPT_SUFFIX);
    free(normalised);
    return prompt;
}

int movie_kind_prompt_text(const char *name, char *out, size_t cap) {
    if (!name || !out || cap == 0) return -1;
    char *prompt = mk_prompt_dup(name);
    if (!prompt) return -1;
    int written = snprintf(out, cap, "%s", prompt);
    free(prompt);
    return (written > 0 && (size_t)written < cap) ? 0 : -1;
}
