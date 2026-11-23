#ifndef MOVIE_KIND_H
#define MOVIE_KIND_H

#include <stddef.h>
#include <stdint.h>

/*
 * Movie-genre classification (the "movie kind" of a movie torrent).
 *
 * A torrent's *kind* is a 16-bit genre mask produced by the v10 JEV head
 * (`include/jevhead.h`) over the **unnormalised last-token embedding** of
 *
 *     "Torrent: " + NFC(name) + "\nFilm genres:"
 *
 * encoded by the quantised GGUF backbone (`models/v10/…Q4_0.gguf`) through a
 * direct `llama_encode` call — not through any `llama-embedding`-shaped
 * harness, whose 512-token padding was the v1 §10.6 blocker. Bit j of the mask
 * is `manifest.genres[j]` (taxonomy index j+1; `none` is not a bit), and a
 * mask of 0 is the `none` answer.
 *
 * Everything is gated on the *pinned* manifest: `movie_kind_init()` refuses to
 * enable the module unless `<model_dir>/manifest.json` parses and both files it
 * names match their pinned byte size **and** sha256. A mismatch is loud, never
 * a silent degradation — the fastText classifier's "silently degrades to
 * parser-only" precedent is exactly what this gate exists to avoid.
 *
 * Build gating: the encoder half needs llama.cpp, which only the Android wrapper
 * build provides (it defines `MK_ENABLED`). The manifest / sha256 / generation /
 * genre-order core is plain C99 + cJSON + OpenSSL and is compiled in every
 * flavour, which is what the host test (`tests/test_movie_kind.c`) exercises.
 * The serving-text renderer (`src/movie_kind_render.c`) is *not* gated either —
 * `tests/test_movie_kind_render.c` compiles the real thing — and only its NFC
 * step needs utf8proc, behind its own `MK_NFC_ENABLED` define (set by the same
 * Android wrapper; without it the name's bytes pass through unchanged). Without
 * `MK_ENABLED`,
 * `movie_kind_enabled()`, `movie_kind_model_ready()` and
 * `movie_kind_classify()` answer 0/0/1 (never a verdict) and the module logs
 * once that it was built out — mirroring `src/classifier.c`'s inert half.
 */

/*: v9 taxonomy size. The mask is 16 bits, so a manifest may not declare more. */
#define MOVIE_KIND_MAX_GENRES 16
/*: Genre bits that survived their threshold; 0 is the `none` answer. */
#define MOVIE_KIND_NONE 0u
/*: Production token window: ids are cut to this many before `llama_encode`. */
#define MOVIE_KIND_MAX_TOKENS 64
/*: Longest genre name copied out of the manifest, including the NUL. */
#define MOVIE_KIND_GENRE_CAP 24
/*: Longest model-directory path, including the NUL (matches config's char[512]). */
#define MOVIE_KIND_PATH_CAP 512
/*: Longest file name from the manifest, including the NUL. */
#define MOVIE_KIND_FILE_CAP 128
/*: Lowercase sha256 hex plus the NUL. */
#define MOVIE_KIND_SHA_CAP 65

/*: The serving text's wrapper, split so the renderer never formats it ad hoc. */
#define MOVIE_KIND_PROMPT_PREFIX "Torrent: "
#define MOVIE_KIND_PROMPT_SUFFIX "\nFilm genres:"

/*: Default model directory, cwd-relative like the keyword files and `models/v1`. */
#define MOVIE_KIND_MODEL_DIR_DEFAULT "models/v10"

typedef struct {
    int enabled;                 /* 1 when movie_kind_init() verified the manifest
                                  * and this build has an embedding provider
                                  * (MK_ENABLED, or the test seam) */
    int model_ready;             /* 1 once the GGUF + head are resident (lazy) */
    long long generation;        /* digits of manifest.version ("v9" -> 9); 0 = none */
    int genre_count;             /* manifest.genres entries (bit count) */
    int n_labels;                /* head.n_labels from the manifest */
    int hidden;                  /* backbone.hidden (the embedding width; 1024 in v10) */
    char model_dir[MOVIE_KIND_PATH_CAP];
    char backbone_sha[MOVIE_KIND_SHA_CAP]; /* pinned sha256 actually verified */
    char head_sha[MOVIE_KIND_SHA_CAP];
    uint64_t total_classified;   /* names that produced a mask */
    uint64_t total_us;           /* summed classify() wall time, microseconds */
    uint64_t failures;           /* classify() calls that produced no verdict */
    uint64_t model_load_failures;/* unusable manifest (counted once) plus each
                                  * model/head that failed to load or verify */
    uint64_t thermal_pauses;     /* back-off pauses reported by the caller
                                  * (classify_backfill-style engine, Stage C) */
    uint64_t by_genre[MOVIE_KIND_MAX_GENRES]; /* per genre bit, all verdicts */
} movie_kind_stats_t;

/*
 * The embedding provider: the one step that needs llama.cpp - render the
 * serving text, tokenise it with the GGUF's own tokenizer, `llama_encode`, read
 * the pooled last-token vector. Everything downstream of it (the zero-embedding
 * guard, the JEV head, the mask) is plain arithmetic, which is why this step is
 * the module's seam: a host test installs a synthetic provider and drives the
 * real classify path with 1024-float vectors, no 478.2 MiB GGUF required.
 *
 * @param userdata whatever was registered with the provider
 * @param name     raw torrent name, byte-exact from the database
 * @param out      destination, exactly `hidden` floats, *unnormalised*
 * @param hidden   the manifest's backbone.hidden (1024 in v10)
 * @return 0 when `out` holds a real embedding; non-zero when none is available
 *         (tokenize/encode failure, or no pooled sequence) - a classify
 *         failure, never a verdict
 */
typedef int (*movie_kind_embedding_provider_t)(void *userdata, const char *name, float *out,
                                              int hidden);

/*
 * Initialise the module for `model_dir` (NULL/empty falls back to
 * "models/v10"). Reads and verifies the manifest; the 478.2 MiB backbone is NOT
 * mapped here — the first movie_kind_classify() call loads it, so a crawler
 * that never backfills never pays for it.
 *
 * Non-fatal by contract, like classifier_init(): an unreadable manifest, a
 * missing/short/wrong-sha file or a manifest without a usable generation only
 * leaves the module disabled, with a LOG_ERROR naming the reason and
 * `model_load_failures` bumped. Call once from main.c before the worker
 * threads start, and only when config.movie_kind_enabled is set.
 *
 * @return 0 when the module is usable, -1 when it stayed disabled.
 */
int movie_kind_init(const char *model_dir);

/*
 * Classify one torrent name: NFC-normalise, render the serving text, tokenise
 * with the GGUF's own tokenizer, cut to MOVIE_KIND_MAX_TOKENS, one-row
 * `llama_encode`, unnormalised last-token embedding, then the JEV head.
 *
 * Thread-safe (one model and one context behind an internal mutex) and
 * lazy-loading: the first call pays the model load.
 *
 * @param name raw torrent name; invalid UTF-8 is passed through untouched
 *             (DB names are not always valid — NFC failure means "use the
 *             bytes as they are", never a dropped row).
 * @param mask out: the genre bit mask; MOVIE_KIND_NONE is a *successful*
 *             `none` verdict, not a failure.
 * @return 0 and *mask filled on success; 1 when no verdict is available
 *         (feature disabled, built without MK_ENABLED, model/head
 *         missing/unusable, an encode failure, or a non-empty name whose
 *         serving text lost the name — the stage-11 empty-prompt tripwire).
 *         The caller leaves the row untouched on 1 — the
 *         `classifier_classify()` contract.
 */
int movie_kind_classify(const char *name, uint16_t *mask);

/*
 * Render the exact serving text for `name` — MOVIE_KIND_PROMPT_PREFIX + NFC(name)
 * + MOVIE_KIND_PROMPT_SUFFIX — into `out`. Exposed so the byte-level contract can
 * be pinned by a test (`tests/test_movie_kind_render.c`, built in both the
 * passthrough and the utf8proc flavour); returns 0 on success, -1 when `out` is
 * too small or the renderer cannot allocate. NFC is applied when the build
 * defines `MK_NFC_ENABLED` (the shipped NDK binary); without it the name's bytes
 * pass through unchanged, which is also what invalid UTF-8 gets in either
 * flavour. `cap` should be strlen(name) + 32.
 */
int movie_kind_prompt_text(const char *name, char *out, size_t cap);

/* Encoder thread count (config.movie_kind_threads, default 4). Applied on the
 * next model load; call before the first classify. Clamped to 1..8. */
void movie_kind_set_threads(int threads);

/* 1 when the manifest verified AND this build has an embedding provider
 * (MK_ENABLED, or the test seam). */
int movie_kind_enabled(void);

/* 1 when the manifest parsed and both pinned files verified their size+sha256.
 * Answerable in every flavour: it is what the host test asserts. */
int movie_kind_manifest_ok(void);

/* 1 once the backbone + head are resident (always 0 without MK_ENABLED). */
int movie_kind_model_ready(void);

/* Model generation from the manifest ("v9" -> 9), for `movie_kind_clf_ver`
 * and for generation-scoped backfill resumes. 0 when no manifest was read. */
long long movie_kind_generation(void);

/* manifest.genres count, and bit -> name in manifest order (NULL when the
 * index is out of range or no manifest was read). */
int movie_kind_genre_count(void);
const char *movie_kind_genre_name(int index);

/* Copy the current counters into *out (whole-struct copy under the mutex). */
void movie_kind_get_stats(movie_kind_stats_t *out);

/* Count one thermal back-off pause (the backfill engine reports it here). */
void movie_kind_note_thermal_pause(void);

#ifdef MOVIE_KIND_TEST
/*
 * Test-only seam, compiled only when MOVIE_KIND_TEST is defined (the
 * tests-movie-kind Makefile target): install a synthetic embedding provider so
 * a host build can exercise the *guarded* classify path - the zero-embedding
 * guard, the head arithmetic, the mask - without llama.cpp and without a model.
 *
 * The module is enabled exactly when the manifest verified AND a provider is
 * installed, so passing NULL restores the inert host behaviour (`enabled` 0,
 * `classify` 1). The daemon build exports none of this.
 */
void movie_kind_test_set_embedding_provider(movie_kind_embedding_provider_t provider,
                                            void *userdata);
#endif /* MOVIE_KIND_TEST */

#endif /* MOVIE_KIND_H */
