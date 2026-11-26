/* movie_kind — see include/movie_kind.h.
 *
 * Two halves, split by *dependency* rather than by feature (the same shape
 * src/classifier.c uses: its category table, its stats struct and its sha
 * helper live outside `#ifdef CLF_ENABLED`, and only the model-dependent code
 * is gated):
 *
 *   - the core — manifest parse (cJSON), generation resolution, genre order,
 *     pinned-size + sha256 verification, the stats block — is plain C99 with
 *     cJSON and OpenSSL, so the crawler's own host `make` build compiles it
 *     and tests/test_movie_kind.c exercises the *real* refusal paths;
 *   - the encoder — llama.cpp model/context and `llama_encode`/`llama_decode` — is
 *     `MK_ENABLED`-gated, with an inert `#else` that answers "no verdict"
 *     exactly like classifier.c's host stubs, so a plain CMake configure and
 *     the host Makefile stay llama-free;
 *   - the serving-text renderer (NFC + the fixed wrapper) is *not* gated at
 *     all: it lives in src/movie_kind_render.c, which both builds compile, and
 *     only its NFC step has a switch (`MK_NFC_ENABLED`, defined by the Android
 *     wrapper beside `MK_ENABLED`). Stage 11 moved it out because the renderer
 *     used to sit in the MK_ENABLED half, where the host build compiled a stub
 *     returning -1 — so the stage-11 P0 (`utf8proc_map()` handed length 0, so
 *     every movie rendered the bare wrapper "Torrent: \nFilm genres:") had no
 *     host coverage whatsoever.
 *
 * Without MK_ENABLED this flavour reports `movie_kind_enabled()` 0 -- never a
 * verdict -- unless a test installs a synthetic embedding provider through the
 * seam, which is the same contract as classifier.c's inert half.
 */

#include "movie_kind.h"

#include "clf_sha256.h"
#include "dht_crawler.h"
#include "jevhead.h"

#include <cJSON.h>

#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef MK_ENABLED
/* llama.cpp's public headers are C11 (redundant typedefs are legal there) while
 * the crawler compiles as C99, so the one diagnostic that disagrees is silenced
 * for this include rather than relaxing the whole target to C11. Clang-only by
 * construction: the host build is gcc and never defines MK_ENABLED. */
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wtypedef-redefinition"
#endif
#include <llama.h>
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
#endif

/* A manifest is a few KiB; the cap is the same one clf_load_manifest() uses, so
 * a runaway file can never be slurped into the daemon. */
#define MK_MANIFEST_CAP (1024 * 1024)
/* model_dir (<= 511) + '/' + file name (<= 127) + NUL, with room to spare. */
#define MK_MODEL_PATH_CAP (MOVIE_KIND_PATH_CAP + MOVIE_KIND_FILE_CAP + 2)
/* config.movie_kind_threads default; 8 is 35-40 % slower on the target SoC than 4. */
#define MK_DEFAULT_THREADS 4
#define MK_MIN_THREADS 1
#define MK_MAX_THREADS 8

static struct {
    /* Manifest state: written once by movie_kind_init(), read-only afterwards,
     * so the accessors below need no lock for it (main.c calls init before any
     * worker thread exists, like classifier_init()). */
    int manifest_ok;    /* manifest parsed and both pinned files verified */
    int enabled;        /* manifest_ok AND this build has an encoder */
    long long generation;
    int genre_count;
    int n_labels;
    int hidden;
    char genres[MOVIE_KIND_MAX_GENRES][MOVIE_KIND_GENRE_CAP];
    char model_dir[MOVIE_KIND_PATH_CAP];
    char backbone_file[MOVIE_KIND_FILE_CAP];
    char head_file[MOVIE_KIND_FILE_CAP];
    long long backbone_bytes;
    long long head_bytes;
    char backbone_sha[MOVIE_KIND_SHA_CAP];
    char head_sha[MOVIE_KIND_SHA_CAP];
    int threads;

    /* Everything below is behind `mutex`: the lazily loaded model/context, the
     * head, and the counters (whole-struct copy on read, like
     * classifier_get_stats()). */
    pthread_mutex_t mutex;
    int model_ready;
    int head_ready;                    /* jevhead loaded + scratch allocated */
    jevhead head;
    /* The embedding seam: the one step that needs llama.cpp. Defaults to the
     * llama.cpp encoder in an MK_ENABLED build and to NULL otherwise (a host
     * test installs a synthetic one through movie_kind_test_set_embedding_
     * provider()). */
    movie_kind_embedding_provider_t embed_provider;
    void *embed_userdata;
    /* hidden floats, allocated with the head and reused under `mutex`:
     * classify is serialised, so one scratch vector is enough. */
    float *embedding;
    movie_kind_stats_t stats;
#ifdef MK_ENABLED
    struct llama_model *model;
    struct llama_context *context;
#endif
} mk = {
    .model_dir = MOVIE_KIND_MODEL_DIR_DEFAULT,
    .threads = MK_DEFAULT_THREADS,
    .mutex = PTHREAD_MUTEX_INITIALIZER,
};

/* ---- shared internals, declared ahead of the gated encoder half -------- */

/* Head load plus the scratch embedding vector. jevhead.c is pure arithmetic, so
 * this half exists in every flavour and the host test drives it directly. */
static int mk_head_ensure_locked(void);
static void mk_head_release_locked(void);
/* enabled == "the manifest verified AND this build has an embedding provider". */
static void mk_update_enabled_locked(void);
/* Which ensure the classify path runs: the full backbone load in an MK_ENABLED
 * build, the head alone otherwise (a test provider supplies the embedding). */
static int mk_ensure_locked(void);

#ifdef MK_ENABLED
/* The llama.cpp embedding provider (render -> tokenise -> llama_encode for an
 * encoder backbone / llama_decode for a decoder one -> read the pooled
 * last-token vector) and the backbone/context load behind it. */
static int mk_llama_embed(void *userdata, const char *name, float *out, int hidden);
static int mk_model_ensure_locked(void);
#endif

/* ---- small helpers ----------------------------------------------------- */


/* Whole manifest as a NUL-terminated string; NULL when unreadable or oversized. */
static char *mk_read_text_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long size = ftell(f);
    if (size <= 0 || size > MK_MANIFEST_CAP) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *text = (char *)malloc((size_t)size + 1);
    if (!text) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(text, 1, (size_t)size, f);
    fclose(f);
    if (got != (size_t)size) {
        free(text);
        return NULL;
    }
    text[size] = '\0';
    return text;
}

static const cJSON *mk_json_object(const cJSON *parent, const char *key) {
    const cJSON *item = parent ? cJSON_GetObjectItemCaseSensitive(parent, key) : NULL;
    return cJSON_IsObject(item) ? item : NULL;
}

static long long mk_json_int(const cJSON *parent, const char *key, long long fallback) {
    const cJSON *item = parent ? cJSON_GetObjectItemCaseSensitive(parent, key) : NULL;
    if (!cJSON_IsNumber(item)) return fallback;
    return (long long)item->valuedouble;
}

/* `manifest.version` is the generation string ("v9"): take its digits, never a
 * literal, so shipping v10 needs no code change. Returns 0 and *out set on
 * success, -1 when there is no positive number in the string. */
static int mk_parse_generation(const char *version, long long *out) {
    if (!version || !out) return -1;
    const char *p = version;
    while (*p && (*p < '0' || *p > '9')) p++;
    if (!*p) return -1;
    long long value = 0;
    while (*p >= '0' && *p <= '9') {
        if (value > 100000000LL) return -1; /* absurd; refuse rather than wrap */
        value = value * 10 + (*p - '0');
        p++;
    }
    if (value <= 0) return -1;
    *out = value;
    return 0;
}

/* One pinned file: it must exist, be exactly `bytes` long and hash to
 * `expected_sha`. Unverified bytes are never loaded — that is the whole point
 * of the manifest, and the reason a mismatch is LOG_ERROR rather than a quiet
 * "parser-only" degradation. */
static int mk_verify_file(const char *dir, const char *file, long long bytes,
                          const char *expected_sha) {
    char path[MK_MODEL_PATH_CAP];
    snprintf(path, sizeof(path), "%s/%s", dir, file);

    FILE *f = fopen(path, "rb");
    if (!f) {
        log_msg(LOG_ERROR, "MovieKind: %s is missing; refusing to load", path);
        return -1;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        log_msg(LOG_ERROR, "MovieKind: %s cannot be sized; refusing to load", path);
        return -1;
    }
    long size = ftell(f);
    fclose(f);
    if (size < 0) {
        log_msg(LOG_ERROR, "MovieKind: %s cannot be sized; refusing to load", path);
        return -1;
    }
    if ((long long)size != bytes) {
        log_msg(LOG_ERROR, "MovieKind: %s is %ld bytes, the manifest pins %lld; refusing to load",
                path, size, bytes);
        return -1;
    }

    char actual[CLF_SHA_HEX_LEN + 1];
    if (clf_sha256_file(path, actual) != 0) {
        log_msg(LOG_ERROR, "MovieKind: %s is unreadable; refusing to load", path);
        return -1;
    }
    if (strcmp(actual, expected_sha) != 0) {
        log_msg(LOG_ERROR, "MovieKind: %s sha256 mismatch (manifest %.16s.., file %.16s..); "
                           "refusing to load", path, expected_sha, actual);
        return -1;
    }
    return 0;
}

/* Pull {file, bytes, sha256} out of a manifest sub-object. */
static int mk_read_file_spec(const cJSON *entry, const char *what, char *file, size_t file_cap,
                             long long *bytes, char *sha, size_t sha_cap) {
    const cJSON *name = entry ? cJSON_GetObjectItemCaseSensitive(entry, "file") : NULL;
    const cJSON *size = entry ? cJSON_GetObjectItemCaseSensitive(entry, "bytes") : NULL;
    const cJSON *hash = entry ? cJSON_GetObjectItemCaseSensitive(entry, "sha256") : NULL;

    if (!cJSON_IsString(name) || name->valuestring[0] == '\0' ||
        strlen(name->valuestring) >= file_cap) {
        log_msg(LOG_ERROR, "MovieKind: manifest has no usable %s.file", what);
        return -1;
    }
    if (!cJSON_IsNumber(size) || size->valuedouble <= 0) {
        log_msg(LOG_ERROR, "MovieKind: manifest has no usable %s.bytes", what);
        return -1;
    }
    if (!cJSON_IsString(hash) || strlen(hash->valuestring) != CLF_SHA_HEX_LEN) {
        log_msg(LOG_ERROR, "MovieKind: manifest has no usable %s.sha256", what);
        return -1;
    }
    snprintf(file, file_cap, "%s", name->valuestring);
    *bytes = (long long)size->valuedouble;
    snprintf(sha, sha_cap, "%s", hash->valuestring);
    return 0;
}

/* ---- manifest ---------------------------------------------------------- */

/* Parse <model_dir>/manifest.json and verify both files it pins. Fills the
 * manifest half of `mk`; returns 0 when the module may be enabled. Called with
 * `mutex` held (it bumps model_load_failures). */
static int mk_manifest_load_locked(void) {
    char path[MK_MODEL_PATH_CAP];
    snprintf(path, sizeof(path), "%s/manifest.json", mk.model_dir);

    char *text = mk_read_text_file(path);
    if (!text) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s not readable; movie-kind disabled", path);
        mk.stats.model_load_failures++;
        return -1;
    }
    cJSON *root = cJSON_Parse(text);
    free(text);
    if (!root) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s is not valid JSON; movie-kind disabled", path);
        mk.stats.model_load_failures++;
        return -1;
    }

    int status = -1;

    const cJSON *version = cJSON_GetObjectItemCaseSensitive(root, "version");
    if (!cJSON_IsString(version) || mk_parse_generation(version->valuestring, &mk.generation) != 0) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s has no usable version (\"v9\" form); "
                           "movie-kind disabled", path);
        mk.stats.model_load_failures++;
        goto done;
    }

    /* Bit j of the mask is genres[j], in manifest order: copy, never re-sort. */
    const cJSON *genres = cJSON_GetObjectItemCaseSensitive(root, "genres");
    if (!cJSON_IsArray(genres)) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s has no genres array; movie-kind disabled", path);
        mk.stats.model_load_failures++;
        goto done;
    }
    int count = cJSON_GetArraySize(genres);
    if (count < 1 || count > MOVIE_KIND_MAX_GENRES) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s declares %d genres, this build supports 1..%d",
                path, count, MOVIE_KIND_MAX_GENRES);
        mk.stats.model_load_failures++;
        goto done;
    }
    for (int index = 0; index < count; index++) {
        const cJSON *item = cJSON_GetArrayItem(genres, index);
        if (!cJSON_IsString(item) || item->valuestring[0] == '\0' ||
            strlen(item->valuestring) >= MOVIE_KIND_GENRE_CAP) {
            log_msg(LOG_ERROR, "MovieKind: manifest %s genre %d is unusable", path, index);
            mk.stats.model_load_failures++;
            goto done;
        }
        snprintf(mk.genres[index], sizeof(mk.genres[index]), "%s", item->valuestring);
    }
    mk.genre_count = count;

    const cJSON *backbone = mk_json_object(root, "backbone");
    const cJSON *head = mk_json_object(root, "head");
    if (!backbone || !head) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s lacks a backbone or head object; "
                           "movie-kind disabled", path);
        mk.stats.model_load_failures++;
        goto done;
    }

    long long hidden = mk_json_int(backbone, "hidden", 0);
    long long n_labels = mk_json_int(head, "n_labels", 0);
    if (hidden <= 0 || n_labels <= 0) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s has no usable hidden/n_labels; movie-kind disabled",
                path);
        mk.stats.model_load_failures++;
        goto done;
    }
    /* A label count that disagrees with the genre list makes "bit j = genres[j]"
     * undefined, so it is a refusal rather than a clamp. */
    long long head_hidden = mk_json_int(head, "hidden", hidden);
    if (n_labels != (long long)mk.genre_count || head_hidden != hidden) {
        log_msg(LOG_ERROR, "MovieKind: manifest %s head is %lld labels / %lld hidden but declares "
                           "%d genres / %lld hidden; movie-kind disabled",
                path, n_labels, head_hidden, mk.genre_count, hidden);
        mk.stats.model_load_failures++;
        goto done;
    }
    mk.n_labels = (int)n_labels;
    mk.hidden = (int)hidden;

    if (mk_read_file_spec(backbone, "backbone", mk.backbone_file, sizeof(mk.backbone_file),
                          &mk.backbone_bytes, mk.backbone_sha, sizeof(mk.backbone_sha)) != 0 ||
        mk_read_file_spec(head, "head", mk.head_file, sizeof(mk.head_file), &mk.head_bytes,
                          mk.head_sha, sizeof(mk.head_sha)) != 0) {
        mk.stats.model_load_failures++;
        goto done;
    }

    if (mk_verify_file(mk.model_dir, mk.backbone_file, mk.backbone_bytes, mk.backbone_sha) != 0 ||
        mk_verify_file(mk.model_dir, mk.head_file, mk.head_bytes, mk.head_sha) != 0) {
        mk.stats.model_load_failures++;
        goto done;
    }

    log_msg(LOG_INFO, "MovieKind: manifest %s verified (generation %lld, %d genres, "
                      "backbone %lld B %.16s.., head %lld B %.16s..)",
            path, mk.generation, mk.genre_count, mk.backbone_bytes, mk.backbone_sha,
            mk.head_bytes, mk.head_sha);
    mk.manifest_ok = 1;
    status = 0;

done:
    cJSON_Delete(root);
    return status;
}

/* ---- public API: initialisation and accessors --------------------------- */

void movie_kind_set_threads(int threads) {
    if (threads < MK_MIN_THREADS) threads = MK_MIN_THREADS;
    if (threads > MK_MAX_THREADS) threads = MK_MAX_THREADS;
    pthread_mutex_lock(&mk.mutex);
    mk.threads = threads;
    pthread_mutex_unlock(&mk.mutex);
}

int movie_kind_init(const char *model_dir) {
    const char *dir = (model_dir && model_dir[0]) ? model_dir : MOVIE_KIND_MODEL_DIR_DEFAULT;

    pthread_mutex_lock(&mk.mutex);
    /* Everything init owns is reset; the mutex and the thread count survive so
     * movie_kind_set_threads() keeps its value across a re-init. A re-init must
     * not leak the previous head or scratch vector. */
    mk_head_release_locked();
    mk.manifest_ok = 0;
    mk.enabled = 0;
    mk.generation = 0;
    mk.genre_count = 0;
    mk.n_labels = 0;
    mk.hidden = 0;
    mk.model_ready = 0;
    mk.backbone_file[0] = '\0';
    mk.head_file[0] = '\0';
    mk.backbone_bytes = 0;
    mk.head_bytes = 0;
    mk.backbone_sha[0] = '\0';
    mk.head_sha[0] = '\0';
    memset(mk.genres, 0, sizeof(mk.genres));
    memset(&mk.stats, 0, sizeof(mk.stats));
    snprintf(mk.model_dir, sizeof(mk.model_dir), "%s", dir);

    if (mk_manifest_load_locked() == 0) {
        mk_update_enabled_locked();
        if (!mk.enabled) {
            /* Host build (plain gcc, no llama.cpp, no utf8proc) with no test
             * provider: the manifest and its pins are still checked -- that is
             * what the host test asserts -- but this flavour can never produce
             * a verdict. */
            log_msg(LOG_WARN,
                    "MovieKind: built without MK_ENABLED; movie-kind classification disabled");
        }
    }
    int manifest_ok = mk.manifest_ok;
    pthread_mutex_unlock(&mk.mutex);

    /* 0 means "the manifest and its pins verified"; whether this build can then
     * produce verdicts is movie_kind_enabled()'s answer (never, without
     * MK_ENABLED). */
    return manifest_ok ? 0 : -1;
}

int movie_kind_enabled(void) {
    /* Set once by movie_kind_init() before any worker thread exists; without
     * MK_ENABLED it is never set at all. */
    return mk.enabled ? 1 : 0;
}

int movie_kind_manifest_ok(void) {
    return mk.manifest_ok ? 1 : 0;
}

int movie_kind_model_ready(void) {
    pthread_mutex_lock(&mk.mutex);
    int ready = mk.model_ready;
    pthread_mutex_unlock(&mk.mutex);
    return ready;
}

long long movie_kind_generation(void) {
    return mk.generation;
}

int movie_kind_genre_count(void) {
    return mk.genre_count;
}

const char *movie_kind_genre_name(int index) {
    if (index < 0 || index >= mk.genre_count) return NULL;
    return mk.genres[index];
}

void movie_kind_get_stats(movie_kind_stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&mk.mutex);
    *out = mk.stats;
    /* The descriptive fields are module state, not counters: publish them on
     * every read so /stats can tell "no data" from "not built in". */
    out->enabled = mk.enabled;
    out->model_ready = mk.model_ready;
    out->generation = mk.generation;
    out->genre_count = mk.genre_count;
    out->n_labels = mk.n_labels;
    out->hidden = mk.hidden;
    snprintf(out->model_dir, sizeof(out->model_dir), "%s", mk.model_dir);
    snprintf(out->backbone_sha, sizeof(out->backbone_sha), "%s", mk.backbone_sha);
    snprintf(out->head_sha, sizeof(out->head_sha), "%s", mk.head_sha);
    pthread_mutex_unlock(&mk.mutex);
}

void movie_kind_note_thermal_pause(void) {
    pthread_mutex_lock(&mk.mutex);
    mk.stats.thermal_pauses++;
    pthread_mutex_unlock(&mk.mutex);
}

/* ---- the encoder half -------------------------------------------------- */

#ifdef MK_ENABLED

/* llama.cpp's backend init is process-global and idempotent-by-convention: run
 * it once, on the first model load (not at init, which must stay cheap). */
static int mk_backend_ready = 0;

/* Load the backbone (and the head it has to agree with) on first use. Called
 * with `mutex` held. */
static int mk_model_ensure_locked(void) {
    char path[MK_MODEL_PATH_CAP];

    if (mk.model_ready) return 0;
    if (mk_head_ensure_locked() != 0) return -1;

    if (!mk_backend_ready) {
        llama_backend_init();
        mk_backend_ready = 1;
    }

    snprintf(path, sizeof(path), "%s/%s", mk.model_dir, mk.backbone_file);
    struct llama_model_params model_params = llama_model_default_params();
    /* CPU-only on the phone; mmap is the default load mode and is what keeps
     * the 478.2 MiB out of RSS until the pages are touched. */
    model_params.n_gpu_layers = 0;
    mk.model = llama_model_load_from_file(path, model_params);
    if (!mk.model) {
        log_msg(LOG_ERROR, "MovieKind: backbone %s failed to load; movie-kind disabled", path);
        mk_head_release_locked();
        mk.stats.model_load_failures++;
        return -1;
    }

    int n_embd = (int)llama_model_n_embd(mk.model);
    if (n_embd != mk.hidden) {
        log_msg(LOG_ERROR, "MovieKind: backbone %s embeds %d floats, the head expects %d; "
                           "refusing to use it", path, n_embd, mk.hidden);
        llama_model_free(mk.model);
        mk.model = NULL;
        mk_head_release_locked();
        mk.stats.model_load_failures++;
        return -1;
    }

    /* The production 64-token window, expressible only through this direct API
     * (a llama-embedding-shaped harness pads to n_ctx_seq — the v1 §10.6 blocker). */
    struct llama_context_params context_params = llama_context_default_params();
    context_params.n_ctx = MOVIE_KIND_MAX_TOKENS;
    context_params.n_batch = MOVIE_KIND_MAX_TOKENS;
    context_params.n_ubatch = MOVIE_KIND_MAX_TOKENS;
    context_params.n_seq_max = 1;
    context_params.n_threads = mk.threads;
    context_params.n_threads_batch = mk.threads;
    /* LAST, not CLS: the v10 head was fitted on the final-token state of the
     * one unpadded sequence, so the pooled vector below must be that state. */
    context_params.pooling_type = LLAMA_POOLING_TYPE_LAST;
    context_params.embeddings = true;
    mk.context = llama_init_from_model(mk.model, context_params);
    if (!mk.context) {
        log_msg(LOG_ERROR, "MovieKind: context for %s failed to initialise; movie-kind disabled", path);
        llama_model_free(mk.model);
        mk.model = NULL;
        mk_head_release_locked();
        mk.stats.model_load_failures++;
        return -1;
    }

    mk.model_ready = 1;
    log_msg(LOG_INFO, "MovieKind: model loaded (generation %lld, %d genres, %d threads): %s",
            mk.generation, mk.genre_count, mk.threads, path);
    return 0;
}

/* The llama.cpp embedding provider: tokenise the serving text with the GGUF's
 * own tokenizer, cut to MOVIE_KIND_MAX_TOKENS, run one row through the entry
 * point the backbone demands, read the pooled last-token vector. Called with
 * `mutex` held and the model resident.
 *
 * The entry point is a property of the model, not a habit: llama_encode runs
 * the encoder graph and llama_decode the decoder graph. A decoder backbone
 * (v10's qwen35) faults inside build_inp_mem_hybrid when handed the encoder
 * graph, so the choice below is load-bearing, not stylistic.
 *
 * The read is llama_get_embeddings_seq(ctx, 0) - the *pooled* accessor. The
 * context is always built with LLAMA_POOLING_TYPE_LAST, so the one unpadded
 * sequence's pooled vector is its final token's state: exactly the vector the
 * v10 JEVH head was fitted on (`models/v10/manifest.json`, "pooling": "last").
 * llama_get_embeddings() is documented as the LLAMA_POOLING_TYPE_NONE
 * accessor: with pooling set it does not return the pooled vector. On the
 * phone it handed back a zero-filled `hidden`-float buffer, so
 * sigmoid(0 + b_j) >= thr_j produced one constant mask for every movie (the
 * stage-9 P0: mask 20744 on all 500 sampled names). There is deliberately no
 * NONE-pooling fallback: this module never pools NONE, so the fallback could
 * only be unreachable dead code that re-opens the wrong path. */
static int mk_llama_embed(void *userdata, const char *name, float *out, int hidden) {
    (void)userdata;
    /* The serving text comes from the ungated renderer (src/movie_kind_render.c),
     * so the encoder cannot drift from what tests/test_movie_kind_render.c pins.
     * strlen(name) + 32 covers the wrapper with room to spare (include/movie_kind.h). */
    size_t prompt_cap = strlen(name) + 32;
    char *prompt = (char *)malloc(prompt_cap);
    if (!prompt) return -1;
    if (movie_kind_prompt_text(name, prompt, prompt_cap) != 0) {
        free(prompt);
        return -1;
    }

    const struct llama_vocab *vocab = llama_model_get_vocab(mk.model);
    if (!vocab) {
        free(prompt);
        return -1;
    }

    int32_t text_len = (int32_t)strlen(prompt);
    int32_t needed = llama_tokenize(vocab, prompt, text_len, NULL, 0, true, false);
    if (needed == 0) {
        free(prompt);
        return -1;
    }
    if (needed < 0) needed = -needed; /* the tokenizer reports the shortfall negative */
    llama_token *tokens = (llama_token *)malloc((size_t)needed * sizeof(llama_token));
    if (!tokens) {
        free(prompt);
        return -1;
    }
    int32_t got = llama_tokenize(vocab, prompt, text_len, tokens, needed, true, false);
    free(prompt);
    if (got <= 0) {
        free(tokens);
        return -1;
    }

    int32_t n_tokens = got;
    if (n_tokens > MOVIE_KIND_MAX_TOKENS) n_tokens = MOVIE_KIND_MAX_TOKENS;
    /* Entry point by model: an encoder backbone (v9's modern-bert) needs
     * llama_encode, a decoder backbone (v10's qwen35) needs llama_decode.
     * Handing a hybrid decoder the encoder graph makes build_inp_mem_hybrid
     * read a null recurrent-memory context and SIGSEGV on the first movie row.
     * Encoder-decoder models are refused loudly, as llama.cpp's own embedding
     * example does: this module has no use for the cross-attention half. */
    const int has_encoder = llama_model_has_encoder(mk.model) ? 1 : 0;
    const int has_decoder = llama_model_has_decoder(mk.model) ? 1 : 0;
    if (has_encoder && has_decoder) {
        log_msg(LOG_WARN, "MovieKind: the backbone is an encoder-decoder model; movie-kind "
                          "needs a pure encoder or a pure decoder backbone");
        free(tokens);
        return -1;
    }

    /* Every token is an output. llama_batch_get_one() leaves logits == NULL,
     * which llama_batch_compat reads as "only the last token is an output";
     * with embeddings on, llama_batch_allocr then warns "some input tokens were
     * not marked as outputs -> overriding" once per classified item and fills
     * the flags in itself. Building the batch explicitly states the intent,
     * drops that per-item warning, and hands the backend the same all-output
     * mask the override produced. */
    struct llama_batch batch = llama_batch_init(n_tokens, 0, 1);
    memcpy(batch.token, tokens, (size_t)n_tokens * sizeof(llama_token));
    batch.n_tokens = n_tokens;
    for (int32_t i = 0; i < n_tokens; i++) {
        batch.pos[i]       = i;
        batch.n_seq_id[i]  = 1;
        batch.seq_id[i][0] = 0;
        batch.logits[i]    = 1;
    }
    int32_t ran;
    if (has_encoder) {
        /* llama_encode does not use the KV cache, so it needs no memory clear. */
        ran = llama_encode(mk.context, batch);
    } else {
        /* Decode *does* carry memory across calls: without the per-call clear
         * every name after the first would start from the previous name's
         * final recurrent/KV state and pool the wrong vector. */
        llama_memory_clear(llama_get_memory(mk.context), true);
        ran = llama_decode(mk.context, batch);
    }
    free(tokens);
    llama_batch_free(batch);
    if (ran != 0) {
        log_msg(LOG_WARN, "MovieKind: llama_encode/llama_decode failed (%d)", ran);
        return -1;
    }

    /* Pooling is LAST (the final token of the single unpadded sequence) and the
     * head was fitted on *unnormalised* states: do not L2-normalise here,
     * whatever an embedding example does. */
    const float *embedding = llama_get_embeddings_seq(mk.context, 0);
    if (!embedding) {
        log_msg(LOG_WARN, "MovieKind: the model produced no pooled last-token embedding (seq 0)");
        return -1;
    }
    memcpy(out, embedding, (size_t)hidden * sizeof(float));
    return 0;
}

#endif /* MK_ENABLED (backend init, NFC, prompt text, backbone/context, provider) */

/* ---- the classify path: shared, provider-independent ------------------- *
 *
 * Everything below is plain C99: the elapsed-time accounting, the head load,
 * the zero-embedding guard, the head arithmetic and the mask. Only the
 * embedding itself comes from the provider, which is why this half compiles in
 * the host build too - the seam below installs a synthetic provider and the
 * *real* guarded path runs, no GGUF and no llama.cpp required.
 * ------------------------------------------------------------------------ */

/* Classify wall time, microseconds. Shared: every flavour measures. */
static uint64_t mk_elapsed_us(const struct timespec *t0, const struct timespec *t1) {
    int64_t ns = (int64_t)(t1->tv_sec - t0->tv_sec) * 1000000000LL +
                 ((int64_t)t1->tv_nsec - t0->tv_nsec);
    if (ns <= 0) return 0;
    return (uint64_t)(ns / 1000);
}

/* Load the head on first use and allocate the scratch embedding vector. Called
 * with `mutex` held. Shared: jevhead.c is pure arithmetic. */
static int mk_head_ensure_locked(void) {
    if (mk.head_ready) return 0;
    if (!mk.manifest_ok) return -1;

    char path[MK_MODEL_PATH_CAP];
    char err[160];

    snprintf(path, sizeof(path), "%s/%s", mk.model_dir, mk.head_file);
    int head_status = jevhead_load(path, &mk.head, err, sizeof(err));
    if (head_status != JEVHEAD_OK) {
        log_msg(LOG_ERROR, "MovieKind: head %s unusable (%s); movie-kind disabled", path, err);
        mk.stats.model_load_failures++;
        return -1;
    }
    if ((int)mk.head.n_labels != mk.genre_count || (int)mk.head.hidden != mk.hidden) {
        log_msg(LOG_ERROR, "MovieKind: head %s is %u labels / %u hidden, the manifest says %d/%d; "
                           "refusing to use it", path, mk.head.n_labels, mk.head.hidden,
                mk.genre_count, mk.hidden);
        jevhead_free(&mk.head);
        memset(&mk.head, 0, sizeof(mk.head));
        mk.stats.model_load_failures++;
        return -1;
    }

    /* One scratch vector, reused under `mutex` (classify is serialised), so a
     * classify never allocates. */
    mk.embedding = (float *)malloc((size_t)mk.hidden * sizeof(float));
    if (!mk.embedding) {
        log_msg(LOG_ERROR, "MovieKind: out of memory for the %d-float embedding", mk.hidden);
        jevhead_free(&mk.head);
        memset(&mk.head, 0, sizeof(mk.head));
        mk.stats.model_load_failures++;
        return -1;
    }
    mk.head_ready = 1;
    return 0;
}

/* Drop the head and its scratch vector: a failed backbone load disables the
 * module, and a re-init must not leak either. Called with `mutex` held. */
static void mk_head_release_locked(void) {
    if (mk.head_ready) {
        jevhead_free(&mk.head);
        memset(&mk.head, 0, sizeof(mk.head));
    }
    free(mk.embedding);
    mk.embedding = NULL;
    mk.head_ready = 0;
}

/* enabled == "the manifest verified AND this build has an embedding provider".
 * Called with `mutex` held. */
static void mk_update_enabled_locked(void) {
#ifdef MK_ENABLED
    /* This build always has the llama.cpp encoder; a real re-init puts it back
     * over anything the test seam installed. */
    mk.embed_provider = mk_llama_embed;
#endif
    mk.enabled = (mk.manifest_ok && mk.embed_provider != NULL) ? 1 : 0;
}

/* Which ensure the classify path runs. */
static int mk_ensure_locked(void) {
#ifdef MK_ENABLED
    return mk_model_ensure_locked();
#else
    return mk_head_ensure_locked();
#endif
}

/* Ask the provider for one embedding, then guard it and run the head. Called
 * with `mutex` held and the head resident. */
static int mk_embed_mask_locked(const char *name, uint16_t *out_mask) {
    if (!mk.embed_provider || !mk.embedding) return -1;

    if (mk.embed_provider(mk.embed_userdata, name, mk.embedding, mk.hidden) != 0) {
        /* The provider logs its own reason (tokenize/encode/no pooled sequence). */
        return -1;
    }

    /* Zero-embedding guard: an all-zero vector is a broken read path, not an
     * embedding. A genuine unnormalised last-token vector has a large L1 (the
     * v9 768-float states measured ~750, ||x|| ~ 41), so L1 == 0 always means
     * the encoder handed back nothing. Failing here is what makes the whole
     * zero-buffer bug class loud: without it, sigmoid(0 + b_j) >= thr_j emits
     * one degenerate mask for every movie. */
    double l1 = 0.0;
    for (int index = 0; index < mk.hidden; index++) {
        l1 += fabs((double)mk.embedding[index]);
    }
    if (l1 == 0.0) {
        log_msg(LOG_WARN,
                "MovieKind: the %d-float embedding is all zeros (L1 == 0); the encoder read path "
                "is broken, refusing to classify", mk.hidden);
        return -1;
    }

    uint32_t mask = 0;
    uint32_t count = 0;
    if (jevhead_predict(&mk.head, mk.embedding, &mask, &count) != JEVHEAD_OK) {
        return -1;
    }
    if (mk.genre_count > 0 && mk.genre_count < 32) {
        mask &= ((uint32_t)1u << mk.genre_count) - 1u;
    }
    uint16_t bits = (uint16_t)mask;
    for (int bit = 0; bit < mk.genre_count; bit++) {
        if (bits & (uint16_t)((uint16_t)1u << bit)) mk.stats.by_genre[bit]++;
    }
    *out_mask = bits;
    return 0;
}

/* The empty-prompt signature: a non-empty name whose serving text equals the one
 * the *empty* name renders, i.e. the name contributed nothing. Stage 11 is
 * exactly that failure: utf8proc_map() was given length 0, so NFC(name) was
 * empty for every movie, the embedding was constant and so was the mask. The
 * check is state-free (no "same prompt twice" memory to keep) and costs two
 * strdup+snprintf pairs, i.e. nothing beside the encode it guards. An empty
 * input name legitimately renders the bare wrapper, so it is exempt. */
static int mk_prompt_lacks_name(const char *name) {
    if (name[0] == 0) return 0;
    char bare[sizeof(MOVIE_KIND_PROMPT_PREFIX) + sizeof(MOVIE_KIND_PROMPT_SUFFIX)];
    char rendered[sizeof(bare)];
    if (movie_kind_prompt_text("", bare, sizeof(bare)) != 0) return 0;
    /* A name long enough to truncate (-1) cannot be the empty-prompt signature. */
    if (movie_kind_prompt_text(name, rendered, sizeof(rendered)) != 0) return 0;
    return strcmp(rendered, bare) == 0;
}

int movie_kind_classify(const char *name, uint16_t *mask) {
    if (!name || !mask) return 1;

    struct timespec t0;
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    pthread_mutex_lock(&mk.mutex);
    uint16_t bits = MOVIE_KIND_NONE;
    int status = 1;
    if (mk.enabled) {
        if (mk_prompt_lacks_name(name)) {
            /* Tripwire for the stage-11 bug class: a non-empty name whose serving
             * text is the bare wrapper would encode to one constant embedding. */
            log_msg(LOG_WARN,
                    "MovieKind: a non-empty name rendered the bare serving text -- the "
                    "renderer is broken, refusing to classify");
            mk.stats.failures++;
        } else if (mk_ensure_locked() == 0 && mk_embed_mask_locked(name, &bits) == 0) {
            status = 0;
            mk.stats.total_classified++;
        } else {
            /* The module is on but produced no verdict: a real failure, unlike a
             * call on a disabled/absent module, which the caller never makes. */
            mk.stats.failures++;
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    mk.stats.total_us += mk_elapsed_us(&t0, &t1);
    pthread_mutex_unlock(&mk.mutex);

    if (status == 0) *mask = bits;
    return status;
}

#ifdef MOVIE_KIND_TEST
void movie_kind_test_set_embedding_provider(movie_kind_embedding_provider_t provider,
                                            void *userdata) {
    pthread_mutex_lock(&mk.mutex);
    mk.embed_provider = provider;
    mk.embed_userdata = userdata;
    /* Installing a provider is what turns the host build into a classifier, so
     * `enabled` has to follow it (the manifest was verified by the caller). */
    mk_update_enabled_locked();
    pthread_mutex_unlock(&mk.mutex);
}
#endif /* MOVIE_KIND_TEST */

