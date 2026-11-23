#ifndef CRAM_H
#define CRAM_H

#include <stddef.h>
#include <stdint.h>

/*
 * CRAM (generation v15) -- the single on-device engine for `category`,
 * `tv_kind` and `adult`.
 *
 * The model is a joint linear scorer over 48 (category, kind, adult) states
 * fitted upstream in the cram workspace and exported to two C-friendly files
 * (`scripts/cram_export.py` in that repo owns the layout; this header mirrors
 * it). Nothing here refits or interprets anything: the engine reproduces the
 * published arithmetic in the published order, which is what makes the host
 * fixtures (tests/fixtures/cram/) checkable entry-by-entry.
 *
 * Energy direction is a named trap: **E is a cost**, `P = softmax(-E)`, the
 * answer is `argmin(E)`, and `adult_p` is the probability side, so a high
 * `adult_p` means adult. Two upstream sign bugs came from reading it the other
 * way round.
 *
 * Shape: pure C99, no Android dependencies, no global mutable state, host
 * testable on its own (`tests-cram` links `src/cram.c` directly -- never
 * `src/classifier.c`, whose real half only exists under `CLF_ENABLED`). One
 * loaded model is immutable except for its counters, so `cram_features()` and
 * `cram_predict()` may be called from many threads at once.
 *
 * Loading is all-or-nothing and never fatal: any failure returns NULL, leaves
 * no partial state, and moves the process-wide `load_failures` counter so a
 * degraded crawler can report why it fell back to the parser-only path.
 *
 * This module does not hash files. `clf_sha256_file()` (include/clf_sha256.h)
 * stays the one sha256 primitive in the tree; the caller passes it in.
 */

/*: Container magic of `cram.bin`. */
#define CRAM_MAGIC "CRAM"
/*: Container magic of `cram_vocab.bin`. */
#define CRAM_VOCAB_MAGIC "CRV1"
/*: On-disk format version of both files. */
#define CRAM_FORMAT_VERSION 1u

/*: Output columns of one prediction (`bias` has one entry each). */
#define CRAM_N_OUTPUTS 28u
/*: Category classes (unary block 0..11). */
#define CRAM_N_CAT 12u
/*: Kinds (joint unary block 12..18 and the shipped head 21..27). */
#define CRAM_N_KIND 7u
/*: Adult classes (unary block 19..20). */
#define CRAM_N_ADULT 2u
/*: Joint states; E_s = u[scol] + u[12+skol] + u[19+asol]. */
#define CRAM_N_STATES 48u

/*: First output column of the joint kind unary (`cat_Wk`) -- NOT the head. */
#define CRAM_KIND_UNARY_BASE 12u
/*: First output column of the adult unary (`cat_Wa`). */
#define CRAM_ADULT_UNARY_BASE 19u
/*: First output column of the shipped standalone tv_kind head (`kind_W`).
 * Swapping this with CRAM_KIND_UNARY_BASE still runs and silently changes the
 * kind verdict -- upstream trap 8. */
#define CRAM_KIND_HEAD_BASE 21u

/*: Character n-gram widths of the hashed tail. */
#define CRAM_CHAR_GRAM_MIN 2u
#define CRAM_CHAR_GRAM_MAX 6u

/*: Longest sha256 hex plus NUL. */
#define CRAM_SHA_CAP 65
/*: Longest model directory path plus NUL. */
#define CRAM_PATH_CAP 512

/*: Distinct feature columns one row may carry. Measured fixture maximum is 628
 * (mean 164); `classifier_classify()` caps the evidence at 50 files and 8191
 * bytes of feature text, so this is generous headroom. A row that would exceed
 * it keeps the lowest columns, sets `cram_row_t.truncated` and moves the
 * `feature_cap_hits` counter -- truncation is counted, never silent. */
#define CRAM_MAX_FEATURES 4096

/*: Default adult operating point when the caller passes 0 (`verdict.adult` is
 * `adult_p >= threshold`; the shipped v15 point is 0.375 and comes from
 * config.ini > manifest, so the caller normally passes it explicitly). */
#define CRAM_ADULT_THRESHOLD_DEFAULT 0.5

/*: Loader / call status. Negative never escapes to the caller as a crash: every
 * failure is reported through this enum (and `load_failures` for the loader). */
typedef enum {
    CRAM_OK = 0,
    CRAM_ERR_ARG = -1,          /* NULL/invalid argument */
    CRAM_ERR_NOMEM = -2,        /* allocation failure */
    CRAM_ERR_IO = -3,           /* open/read/seek failure, or an empty file */
    CRAM_ERR_TRUNCATED = -4,    /* file shorter than its own header claims */
    CRAM_ERR_MAGIC = -5,        /* "CRAM"/"CRV1" not found */
    CRAM_ERR_VERSION = -6,      /* format_version != 1 */
    CRAM_ERR_LAYOUT = -7,       /* header field combination is impossible */
    CRAM_ERR_ROWPTR = -8,       /* rowptr not monotonic / not 0..nnz */
    CRAM_ERR_OUT_COL = -9,      /* an entry names an output column >= n_outputs */
    CRAM_ERR_STATES = -10,      /* a state index is out of range */
    CRAM_ERR_TEMP = -11,        /* temperature not finite/positive */
    CRAM_ERR_VOCAB = -12,       /* vocab block offsets or strings are unusable */
    CRAM_ERR_SHA = -13,         /* computed digest != the expected one */
    CRAM_ERR_SHAPE = -14        /* container and vocab disagree (ncols/char tail) */
} cram_status_t;

/*: Human-readable name of a status, for logs. Never NULL. */
const char *cram_status_str(cram_status_t status);

/*: One row of feature evidence: ascending feature columns and their
 * `(float)log1p(count)` values. Filled by `cram_features()`; a caller-owned
 * buffer so the engine keeps no per-call state. */
typedef struct {
    uint32_t indices[CRAM_MAX_FEATURES];
    float values[CRAM_MAX_FEATURES];
    int count;      /* entries used, 0..CRAM_MAX_FEATURES */
    int truncated;  /* 1 when the row overflowed the cap (see CRAM_MAX_FEATURES) */
} cram_row_t;

/*: One prediction. `category`/`kind` are class indices; `adult` is the tagged
 * decision at the caller's threshold, `adult_p` the raw marginal it came from. */
typedef struct {
    int category;     /* 0..CRAM_N_CAT-1 */
    int kind;         /* 0..CRAM_N_KIND-1, standalone head (apply on tv/anime) */
    int adult;        /* 1 when adult_p >= the caller's threshold */
    int state;        /* 0..CRAM_N_STATES-1 winning joint state */
    double adult_p;   /* adult marginal of the RAW softmax(-E), no temperature */
    double confidence;/* category marginal of softmax(-E/T) */
} cram_verdict_t;

/*: Engine counters. Copied out under no lock: each field is read and written
 * atomically, so a snapshot is a coherent per-field view rather than a torn
 * one (the daemon has a single model, so the window is a non-issue). */
typedef struct {
    uint64_t rows_predicted;          /* successful cram_predict() calls */
    uint64_t load_failures;           /* process-wide: cram_load() failures */
    uint64_t kind_head_used;          /* predictions that filled `verdict.kind` */
    uint64_t adult_tagged_predictions;/* predictions with adult == 1 */
    uint64_t feature_cap_hits;        /* rows truncated at CRAM_MAX_FEATURES */
} cram_stats_t;

/*: Opaque loaded engine. */
typedef struct cram_model cram_model_t;

/*
 * The sha256 primitive, injected by the caller: `clf_sha256_file()` in the
 * daemon, a stub in the tests. Signature matches include/clf_sha256.h.
 * @return 0 on success with `out_hex` filled (64 lowercase hex + NUL), -1 else.
 */
typedef int (*cram_sha256_file_fn)(const char *path, char *out_hex);

/*: What `cram_load()` verifies. Digests come from `models/v15/manifest.json`;
 * `sha256_file == NULL` or a NULL digest skips that check (tests, and a caller
 * that already verified the bytes itself). */
typedef struct {
    cram_sha256_file_fn sha256_file;
    const char *cram_sha256;   /* expected hex of cram.bin, or NULL */
    const char *vocab_sha256;  /* expected hex of cram_vocab.bin, or NULL */
} cram_digests_t;

/*
 * Load `<dir>/cram.bin` + `<dir>/cram_vocab.bin`, verify the digests the caller
 * supplies, bounds-check every offset and freeze the result.
 *
 * Non-fatal by contract: on any failure it returns NULL, frees whatever it had
 * built, bumps `load_failures` and leaves `*status` naming the reason. Nothing
 * partial is ever visible, and a NULL model makes every other call a no-op.
 *
 * @param dir     model directory ("models/v15"), not NULL
 * @param digests verification inputs, may be NULL (nothing verified)
 * @param status  optional; receives CRAM_OK or the failure reason
 */
cram_model_t *cram_load(const char *dir, const cram_digests_t *digests,
                        cram_status_t *status);

/*: Free a model. Only call once every worker thread has stopped using it. */
void cram_free(cram_model_t *model);

/*
 * Parse and validate one `cram.bin`. Exposed because the loader's rejection
 * paths are the interesting half: `tests/fixtures/cram/tiny_containers.json`
 * carries one valid 4-column container and eight mutations of it.
 * The container is opaque; use it through `cram_container_predict()`.
 */
typedef struct cram_container cram_container_t;

cram_container_t *cram_container_load(const char *path, cram_status_t *status);
void cram_container_free(cram_container_t *container);
/*: Columns the container was built for (the row's index space). */
uint32_t cram_container_ncols(const cram_container_t *container);

/*
 * Fill `out[CRAM_N_OUTPUTS]` with the 28 accumulated energies of `row`:
 * `bias[o] + sum_f value(f) * weight(f, o)`, walked in ascending feature order.
 * Exposed because that order IS the model contract -- the host fixtures carry
 * the exact 28 doubles per row, so `tests-cram` pins the accumulation
 * entry-by-entry instead of only through the verdict. `cram_predict()` uses it.
 */
cram_status_t cram_container_unary(const cram_container_t *container, const cram_row_t *row,
                                   double *out);

/*: Score one already-built row against a standalone container (loader tests). */
int cram_container_predict(const cram_container_t *container, const cram_row_t *row,
                           double adult_threshold, cram_verdict_t *out);

/*
 * Turn `ft` (the tparse feature text) into feature columns.
 *
 * Whitespace split; `__`-prefixed struct tokens hit their exact column and
 * contribute nothing else; every other token hits its word column when it is
 * in the vocabulary *and* contributes the char n-grams of `<token>` (widths
 * 2..6, a local zlib-compatible CRC32 over the UTF-8 bytes including the
 * markers) to the hashed tail; adjacent name tokens contribute word bigrams.
 * Columns are deduped, sorted ascending and valued `(float)log1p(count)`.
 *
 * Empty `ft` is legal (no features: the verdict is then bias-only) and so is an
 * all-OOV `ft` (char tail only). Neither is an error. Per-call scratch only, so
 * this is thread-safe.
 *
 * @return CRAM_OK, or CRAM_ERR_ARG for a NULL model/row.
 */
cram_status_t cram_features(cram_model_t *model, const char *ft, cram_row_t *out);

/*
 * One forward pass: energies in double, accumulated per output column in
 * ascending feature-index order.
 *
 * `category` = masked argmin over the 48 joint states (first minimum wins, the
 * numpy tie-break); `confidence` = category marginal of `softmax(-E/T)` with
 * the container's T; `kind` = argmin of the 7 standalone-head energies (the
 * caller applies it only on CRAM-decided tv/anime rows); `adult_p` = adult
 * marginal of the RAW `softmax(-E)`, no temperature, and `adult` = `adult_p >=
 * adult_threshold` (so `adult_p == threshold` tags).
 *
 * @param adult_threshold the operating point; <= 0 uses CRAM_ADULT_THRESHOLD_DEFAULT
 * @return CRAM_OK, or CRAM_ERR_ARG on NULL / an out-of-range row index.
 */

cram_status_t cram_predict(cram_model_t *model, const cram_row_t *row,
                           double adult_threshold, cram_verdict_t *out);

/*
 * Serving-side canonicalisation of the feature text: remove every
 * whitespace-delimited token that begins `__adult_` from `ft`, in place and
 * together with one adjacent separator, leaving the rest byte-for-byte
 * unchanged. The result is byte-identical to the text tparse would have
 * rendered had it not emitted those tokens.
 *
 * The frozen model's training corpus carries these tokens on 20 of 270,390 rows
 * while the vendored parser emits them unconditionally, so the serving path
 * canonicalises to the training distribution. `src/adult_backfill.c` keeps its
 * own gate over the RAW text and must not go through this (plan assumption 13).
 *
 * In place, no allocation, idempotent: stripping an already-stripped string
 * removes nothing.
 *
 * @return bytes removed (0 when `ft` carried no such token, or is NULL).
 */
size_t cram_strip_adult_tokens(char *ft);

/*: Copy the counters. `model` may be NULL: `load_failures` is process-wide and
 * stays readable after a failed load, every other field reads 0. */
void cram_get_stats(const cram_model_t *model, cram_stats_t *out);

/*: The two digests the model was loaded under ("" when not verified), for
 * `/stats`. Both buffers are CRAM_SHA_CAP bytes. */
void cram_get_digests(const cram_model_t *model, char *cram_sha256, char *vocab_sha256);

#endif /* CRAM_H */
