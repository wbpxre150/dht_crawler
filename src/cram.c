/*
 * CRAM (generation v15) -- the joint category / tv_kind / adult engine.
 *
 * See include/cram.h for the contract. This file is the arithmetic half of
 * `scripts/cram_export.py` in the cram workspace, and it must stay bit-for-bit
 * faithful to it: the export writes the same numbers into
 * `tests/fixtures/cram/` (every sampled row carries its exact 28-value double
 * `unary`), and `tests-cram` compares them entry-by-entry. Any reordering of a
 * sum, any widening done the other way round, and the fixtures fail.
 *
 * The three rules that make that possible:
 *   1. weights are float32 on disk and are widened to double exactly once;
 *   2. energies accumulate per output column in ASCENDING feature-index order
 *      (the CSR is feature-major, so walking the row's features in order is the
 *      nonzero subsequence of the dense dot's order);
 *   3. a softmax marginal sums per-state `e / total` terms (not `sum(e)/total`),
 *      which is what the Python reference does and therefore what rounds here.
 *
 * E is a cost: P = softmax(-E), the answer is argmin(E), and `adult_p` is a
 * probability, so high means adult.
 *
 * Both files are little-endian and packed (`uint8 out_col` + `float32 weight`
 * is 5 bytes, unaligned), so every read goes through a byte assembler instead
 * of a cast.
 */

#include "cram.h"

#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*: Container header: 4 magic bytes + 15 uint32. */
#define CRAM_HEADER_BYTES 64u
/*: Vocab header: 4 magic bytes + 15 uint32. */
#define CRAM_VOCAB_HEADER_BYTES 64u
/*: Bytes per stored nonzero: uint8 out_col + float32 weight, packed. */
#define CRAM_ENTRY_BYTES 5u

/* ------------------------------------------------------------------ */
/* counters                                                            */
/* ------------------------------------------------------------------ */

/* Module-owned counters, the `src/movie_kind.c` / `src/meta_peer_fail.c` shape:
 * one static struct behind its own mutex, bumped at the event and copied out
 * under the lock -- the mutex is never held across the predict math, so the
 * classify hot path pays one lock pair per row. A failed load leaves no model
 * handle, so `load_failures` has to live here: a handle-held counter would be
 * unreadable exactly when the load failed. */
static cram_stats_t cram_stats;
static pthread_mutex_t cram_stats_mutex = PTHREAD_MUTEX_INITIALIZER;

static void stats_bump(uint64_t *field, uint64_t n) {
    pthread_mutex_lock(&cram_stats_mutex);
    *field += n;
    pthread_mutex_unlock(&cram_stats_mutex);
}

const char *cram_status_str(cram_status_t status) {
    switch (status) {
    case CRAM_OK: return "ok";
    case CRAM_ERR_ARG: return "invalid argument";
    case CRAM_ERR_NOMEM: return "out of memory";
    case CRAM_ERR_IO: return "cannot read file";
    case CRAM_ERR_TRUNCATED: return "file truncated";
    case CRAM_ERR_MAGIC: return "bad magic";
    case CRAM_ERR_VERSION: return "unsupported format version";
    case CRAM_ERR_LAYOUT: return "impossible header layout";
    case CRAM_ERR_ROWPTR: return "broken CSR rowptr";
    case CRAM_ERR_OUT_COL: return "output column out of range";
    case CRAM_ERR_STATES: return "state index out of range";
    case CRAM_ERR_TEMP: return "unusable temperature";
    case CRAM_ERR_VOCAB: return "unusable vocabulary";
    case CRAM_ERR_SHA: return "sha256 mismatch";
    case CRAM_ERR_SHAPE: return "container and vocabulary disagree";
    default: return "unknown";
    }
}

struct cram_model {
    struct cram_container *container;
    struct cram_vocab *vocab;
    char cram_sha256[CRAM_SHA_CAP];
    char vocab_sha256[CRAM_SHA_CAP];
};

/* ------------------------------------------------------------------ */
/* little-endian readers                                               */
/* ------------------------------------------------------------------ */

static uint32_t le_u32(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static uint64_t le_u64(const unsigned char *p) {
    return (uint64_t)le_u32(p) | ((uint64_t)le_u32(p + 4) << 32);
}

static float le_f32(const unsigned char *p) {
    uint32_t bits = le_u32(p);
    float f;
    memcpy(&f, &bits, sizeof(f));
    return f;
}

static double le_f64(const unsigned char *p) {
    uint64_t bits = le_u64(p);
    double d;
    memcpy(&d, &bits, sizeof(d));
    return d;
}

/* Read a whole file into a fresh buffer. An empty file is an error: both
 * formats have a header, so a zero-byte file is never a usable model. */
static unsigned char *read_whole_file(const char *path, size_t *out_bytes) {
    FILE *f = fopen(path, "rb");
    unsigned char *buf;
    long size;

    *out_bytes = 0;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) <= 0 ||
        fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    buf = (unsigned char *)malloc((size_t)size);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    if (fread(buf, 1, (size_t)size, f) != (size_t)size) {
        free(buf);
        fclose(f);
        return NULL;
    }
    fclose(f);
    *out_bytes = (size_t)size;
    return buf;
}

/* ------------------------------------------------------------------ */
/* container (cram.bin)                                                */
/* ------------------------------------------------------------------ */

struct cram_container {
    unsigned char *data; /* the whole file, owned, never mutated after load */
    size_t bytes;
    uint32_t format_version;
    uint32_t ncols;
    uint32_t n_outputs;
    uint32_t n_states;
    uint32_t n_cat;
    uint32_t n_kind;
    uint32_t n_adult;
    uint32_t nnz;
    uint32_t rowptr_off;
    uint32_t entries_off;
    uint32_t bias_off;
    uint32_t states_off;
    uint32_t temperature_off;
    uint32_t file_bytes;
    double temperature;
};

static uint32_t container_u32(const struct cram_container *c, size_t off) {
    return le_u32(c->data + off);
}

static float container_f32(const struct cram_container *c, size_t off) {
    return le_f32(c->data + off);
}

void cram_container_free(cram_container_t *container) {
    if (!container) return;
    free(container->data);
    free(container);
}

uint32_t cram_container_ncols(const cram_container_t *container) {
    return container ? container->ncols : 0;
}

/* Every header offset is derived from the fields before it, so a single wrong
 * field cannot be papered over by a plausible-looking one: the mutation cases
 * in tests/fixtures/cram/tiny_containers.json each break exactly one link. */
static cram_status_t container_check(const struct cram_container *c) {
    uint32_t s;

    if (c->bytes < CRAM_HEADER_BYTES) return CRAM_ERR_TRUNCATED;
    if (memcmp(c->data, CRAM_MAGIC, 4) != 0) return CRAM_ERR_MAGIC;
    if (c->n_outputs != CRAM_N_OUTPUTS || c->n_states != CRAM_N_STATES ||
        c->n_cat != CRAM_N_CAT || c->n_kind != CRAM_N_KIND ||
        c->n_adult != CRAM_N_ADULT) {
        return CRAM_ERR_LAYOUT;
    }
    if (c->ncols == 0) return CRAM_ERR_LAYOUT;
    if (c->rowptr_off != CRAM_HEADER_BYTES) return CRAM_ERR_LAYOUT;
    if (c->entries_off != CRAM_HEADER_BYTES + 4u * (c->ncols + 1u)) return CRAM_ERR_LAYOUT;
    if (c->bias_off != c->entries_off + CRAM_ENTRY_BYTES * c->nnz) return CRAM_ERR_LAYOUT;
    if (c->states_off != c->bias_off + 4u * c->n_outputs) return CRAM_ERR_LAYOUT;
    if (c->temperature_off != c->states_off + 3u * c->n_states) return CRAM_ERR_LAYOUT;
    if (c->file_bytes != c->temperature_off + 8u) return CRAM_ERR_LAYOUT;
    if ((size_t)c->file_bytes != c->bytes) return CRAM_ERR_TRUNCATED;

    if (!isfinite(c->temperature) || c->temperature <= 0.0) return CRAM_ERR_TEMP;

    if (container_u32(c, c->rowptr_off) != 0) return CRAM_ERR_ROWPTR;
    for (s = 0; s < c->ncols; s++) {
        uint32_t lo = container_u32(c, c->rowptr_off + 4u * s);
        uint32_t hi = container_u32(c, c->rowptr_off + 4u * (s + 1u));
        if (hi < lo || hi > c->nnz) return CRAM_ERR_ROWPTR;
    }
    if (container_u32(c, c->rowptr_off + 4u * c->ncols) != c->nnz) return CRAM_ERR_ROWPTR;

    for (s = 0; s < c->nnz; s++) {
        if (c->data[c->entries_off + CRAM_ENTRY_BYTES * (size_t)s] >= c->n_outputs) {
            return CRAM_ERR_OUT_COL;
        }
    }
    for (s = 0; s < c->n_states; s++) {
        const unsigned char *p = c->data + c->states_off + 3u * (size_t)s;
        if (p[0] >= c->n_cat || p[1] >= c->n_kind || p[2] >= c->n_adult) {
            return CRAM_ERR_STATES;
        }
    }
    return CRAM_OK;
}

cram_container_t *cram_container_load(const char *path, cram_status_t *status) {
    struct cram_container *c;
    cram_status_t st;
    size_t bytes = 0;

    if (status) *status = CRAM_ERR_ARG;
    if (!path || !*path) return NULL;

    c = (struct cram_container *)calloc(1, sizeof(*c));
    if (!c) {
        if (status) *status = CRAM_ERR_NOMEM;
        return NULL;
    }
    c->data = read_whole_file(path, &bytes);
    if (!c->data) {
        free(c);
        if (status) *status = CRAM_ERR_IO;
        return NULL;
    }
    c->bytes = bytes;
    if (bytes < CRAM_HEADER_BYTES) {
        cram_container_free(c);
        if (status) *status = CRAM_ERR_TRUNCATED;
        return NULL;
    }

    c->format_version = container_u32(c, 4);
    c->ncols = container_u32(c, 8);
    c->n_outputs = container_u32(c, 12);
    c->n_states = container_u32(c, 16);
    c->n_cat = container_u32(c, 20);
    c->n_kind = container_u32(c, 24);
    c->n_adult = container_u32(c, 28);
    c->nnz = container_u32(c, 32);
    c->rowptr_off = container_u32(c, 36);
    c->entries_off = container_u32(c, 40);
    c->bias_off = container_u32(c, 44);
    c->states_off = container_u32(c, 48);
    c->temperature_off = container_u32(c, 52);
    c->file_bytes = container_u32(c, 56);

    /* The magic and the version are checked before any field is trusted. */
    if (memcmp(c->data, CRAM_MAGIC, 4) != 0) {
        cram_container_free(c);
        if (status) *status = CRAM_ERR_MAGIC;
        return NULL;
    }
    if (c->format_version != CRAM_FORMAT_VERSION) {
        cram_container_free(c);
        if (status) *status = CRAM_ERR_VERSION;
        return NULL;
    }
    /* Only now is the temperature offset known to be inside the file. */
    if ((size_t)c->temperature_off + 8u > bytes) {
        cram_container_free(c);
        if (status) *status = CRAM_ERR_TRUNCATED;
        return NULL;
    }
    c->temperature = le_f64(c->data + c->temperature_off);

    st = container_check(c);
    if (st != CRAM_OK) {
        cram_container_free(c);
        if (status) *status = st;
        return NULL;
    }
    if (status) *status = CRAM_OK;
    return c;
}

/* ------------------------------------------------------------------ */
/* vocabulary (cram_vocab.bin)                                         */
/* ------------------------------------------------------------------ */

struct cram_vocab {
    unsigned char *data;
    size_t bytes;
    uint32_t n_structs;
    uint32_t n_words;
    uint32_t n_bigrams;
    uint32_t col_struct;
    uint32_t col_words;
    uint32_t col_bigrams;
    uint32_t col_char;
    uint32_t buckets;
    uint32_t blob_off;
    uint32_t blob_bytes;
    uint32_t off_structs; /* byte offset of each block's offset table */
    uint32_t off_words;
    uint32_t off_bigrams;
};

static void vocab_free(struct cram_vocab *v) {
    if (!v) return;
    free(v->data);
    free(v);
}

/* Offsets are relative to the blob and each block ends where the next begins. */
static uint32_t vocab_str_off(const struct cram_vocab *v, uint32_t table_off, uint32_t i) {
    return le_u32(v->data + table_off + 4u * i);
}

static const char *vocab_str(const struct cram_vocab *v, uint32_t table_off, uint32_t i) {
    return (const char *)(v->data + v->blob_off + vocab_str_off(v, table_off, i));
}

/* Compare a vocabulary entry with a NON-NUL-terminated token: ft is one long
 * string, so every lookup is length-aware. UTF-8 byte order equals Python's
 * codepoint order, which is why a byte compare reproduces `dict` membership. */
static int vocab_cmp_token(const char *entry, const char *token, size_t token_len) {
    size_t i = 0;
    while (i < token_len) {
        unsigned char a = (unsigned char)entry[i];
        unsigned char b = (unsigned char)token[i];
        if (a != b) return (a < b) ? -1 : 1;
        i++;
    }
    return (entry[i] == '\0') ? 0 : 1;
}

/* Compare a vocabulary entry with the lazy key "left SP right". */
static int vocab_cmp_pair(const char *entry, const char *left, size_t left_len,
                          const char *right, size_t right_len) {
    size_t i = 0;
    size_t k;
    for (k = 0; k < left_len; k++, i++) {
        unsigned char a = (unsigned char)entry[i];
        unsigned char b = (unsigned char)left[k];
        if (a != b) return (a < b) ? -1 : 1;
    }
    if (entry[i] != ' ') return (entry[i] == '\0') ? -1 : 1;
    i++;
    for (k = 0; k < right_len; k++, i++) {
        unsigned char a = (unsigned char)entry[i];
        unsigned char b = (unsigned char)right[k];
        if (a != b) return (a < b) ? -1 : 1;
    }
    return (entry[i] == '\0') ? 0 : 1;
}

static int vocab_find_token(const struct cram_vocab *v, uint32_t table_off, uint32_t count,
                            const char *token, size_t token_len, uint32_t *index) {
    uint32_t lo = 0;
    uint32_t hi = count;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int cmp = vocab_cmp_token(vocab_str(v, table_off, mid), token, token_len);
        if (cmp == 0) {
            *index = mid;
            return 1;
        }
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return 0;
}

static int vocab_find_pair(const struct cram_vocab *v, const char *left, size_t left_len,
                           const char *right, size_t right_len, uint32_t *index) {
    uint32_t lo = 0;
    uint32_t hi = v->n_bigrams;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int cmp = vocab_cmp_pair(vocab_str(v, v->off_bigrams, mid), left, left_len, right,
                                 right_len);
        if (cmp == 0) {
            *index = mid;
            return 1;
        }
        if (cmp < 0) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return 0;
}

/* Offsets are relative to the blob and the three blocks tile it, so each block
 * must start where the previous one ended and the last must end at blob_bytes.
 * Inside a block the offsets are strictly increasing (no entry is empty) and the
 * strings are NUL-terminated and sorted, because the bsearch above is only sound
 * if the block is. This is O(total strings) on load and it is what turns a
 * truncated or reordered vocab into a clean rejection instead of a wrong
 * verdict. */
static int vocab_block_ok(const struct cram_vocab *v, uint32_t table_off, uint32_t count,
                          uint32_t expected_first, uint32_t expected_end) {
    uint32_t i;
    if (vocab_str_off(v, table_off, 0) != expected_first) return 0;
    for (i = 0; i < count; i++) {
        uint32_t lo = vocab_str_off(v, table_off, i);
        uint32_t hi = vocab_str_off(v, table_off, i + 1);
        if (hi <= lo || hi > v->blob_bytes) return 0;
        if (v->data[v->blob_off + hi - 1] != '\0') return 0;
        if (i + 1 < count &&
            strcmp(vocab_str(v, table_off, i), vocab_str(v, table_off, i + 1)) >= 0) {
            return 0;
        }
    }
    return vocab_str_off(v, table_off, count) == expected_end;
}

static struct cram_vocab *vocab_load(const char *path, cram_status_t *status) {
    struct cram_vocab *v;
    size_t bytes = 0;
    uint32_t n_blocks;
    uint32_t file_bytes;
    uint32_t reserved;

    if (status) *status = CRAM_ERR_ARG;
    if (!path || !*path) return NULL;

    v = (struct cram_vocab *)calloc(1, sizeof(*v));
    if (!v) {
        if (status) *status = CRAM_ERR_NOMEM;
        return NULL;
    }
    v->data = read_whole_file(path, &bytes);
    if (!v->data) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_IO;
        return NULL;
    }
    v->bytes = bytes;
    if (bytes < CRAM_VOCAB_HEADER_BYTES) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_TRUNCATED;
        return NULL;
    }
    if (memcmp(v->data, CRAM_VOCAB_MAGIC, 4) != 0) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_MAGIC;
        return NULL;
    }
    if (le_u32(v->data + 4) != CRAM_FORMAT_VERSION) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_VERSION;
        return NULL;
    }

    n_blocks = le_u32(v->data + 8);
    v->n_structs = le_u32(v->data + 12);
    v->n_words = le_u32(v->data + 16);
    v->n_bigrams = le_u32(v->data + 20);
    v->col_struct = le_u32(v->data + 24);
    v->col_words = le_u32(v->data + 28);
    v->col_bigrams = le_u32(v->data + 32);
    v->col_char = le_u32(v->data + 36);
    v->buckets = le_u32(v->data + 40);
    v->blob_bytes = le_u32(v->data + 44);
    v->blob_off = le_u32(v->data + 52);
    file_bytes = le_u32(v->data + 56);
    reserved = le_u32(v->data + 60);

    if (n_blocks != 3 || reserved != 0 || le_u32(v->data + 48) != CRAM_VOCAB_HEADER_BYTES) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_LAYOUT;
        return NULL;
    }
    if (v->buckets == 0 || (v->buckets & (v->buckets - 1)) != 0) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_LAYOUT;
        return NULL;
    }
    if (v->col_struct != 0 || v->col_words != v->col_struct + v->n_structs ||
        v->col_bigrams != v->col_words + v->n_words ||
        v->col_char != v->col_bigrams + v->n_bigrams) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_LAYOUT;
        return NULL;
    }
    if (v->blob_off != CRAM_VOCAB_HEADER_BYTES +
                           4u * ((v->n_structs + 1u) + (v->n_words + 1u) + (v->n_bigrams + 1u))) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_LAYOUT;
        return NULL;
    }
    if ((uint64_t)v->blob_off + v->blob_bytes != file_bytes || file_bytes != bytes) {
        vocab_free(v);
        if (status) *status = CRAM_ERR_TRUNCATED;
        return NULL;
    }

    v->off_structs = CRAM_VOCAB_HEADER_BYTES;
    v->off_words = v->off_structs + 4u * (v->n_structs + 1u);
    v->off_bigrams = v->off_words + 4u * (v->n_words + 1u);

    {
        uint32_t first_words = vocab_str_off(v, v->off_words, 0);
        uint32_t first_bigrams = vocab_str_off(v, v->off_bigrams, 0);
        if (!vocab_block_ok(v, v->off_structs, v->n_structs, 0, first_words) ||
            !vocab_block_ok(v, v->off_words, v->n_words, first_words, first_bigrams) ||
            !vocab_block_ok(v, v->off_bigrams, v->n_bigrams, first_bigrams, v->blob_bytes)) {
            vocab_free(v);
            if (status) *status = CRAM_ERR_VOCAB;
            return NULL;
        }
    }
    if (status) *status = CRAM_OK;
    return v;
}

/* ------------------------------------------------------------------ */
/* features                                                            */
/* ------------------------------------------------------------------ */

/* zlib-compatible CRC32 (reflected polynomial 0xEDB88320) over the UTF-8 bytes
 * of the gram, `<`/`>` markers included. Bitwise on purpose: a 1 KiB table
 * would be either global mutable state or a per-call rebuild, and a row touches
 * only a few thousand gram bytes. Incremental because a gram is a *codepoint*
 * window (see hit_char_grams), so its bytes arrive as up to six runs. */
#define CRAM_CRC32_INIT 0xFFFFFFFFu

static uint32_t cram_crc32_update(uint32_t crc, const char *bytes, size_t len) {
    size_t i;
    int bit;
    for (i = 0; i < len; i++) {
        crc ^= (uint32_t)(unsigned char)bytes[i];
        for (bit = 0; bit < 8; bit++) {
            uint32_t mask = (uint32_t)(-(int32_t)(crc & 1u));
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return crc;
}

/* Length of the UTF-8 sequence at `s` (1..4), or 1 for a malformed lead byte or
 * a broken continuation run: the feature text is UTF-8 by construction, and a
 * byte that is not must not derail the whole row. */
static size_t utf8_seq_len(const char *s, size_t len) {
    unsigned char lead = (unsigned char)s[0];
    size_t n;
    size_t k;

    if (lead < 0x80u) return 1;
    if ((lead & 0xE0u) == 0xC0u) {
        n = 2;
    } else if ((lead & 0xF0u) == 0xE0u) {
        n = 3;
    } else if ((lead & 0xF8u) == 0xF0u) {
        n = 4;
    } else {
        return 1;
    }
    if (n > len) return 1;
    for (k = 1; k < n; k++) {
        if (((unsigned char)s[k] & 0xC0u) != 0x80u) return 1;
    }
    return n;
}

/* ft is tparse-normalised, so the token separator is ASCII whitespace. Any
 * Unicode whitespace drift is a G1 concern, not an engine one. */
static int is_ft_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

/* Serving-side canonicalisation of the feature text: drop every `__adult_*`
 * struct token from `ft` in place, together with one adjacent separator, so the
 * result is byte-identical to what tparse would have rendered without those
 * tokens. Every other byte is left alone.
 *
 * Why this lives here rather than in tparse: serving canonicalises the feature
 * text before the model sees it -- the frozen v15 model was trained on a
 * corpus that carries these tokens on 20 of 270,390 rows while the vendored
 * parser emits them unconditionally. The tokens are struct-only markers: they hit columns 0..2 exactly and never join a bigram,
 * which is what makes dropping them a pure column removal.
 *
 * Pure C99, in place, no allocation, idempotent: the scan restarts at the cut,
 * so a second call over its own output matches nothing and removes 0 bytes.
 *
 * @return the number of bytes removed (0 when the text carried no such token).
 */
size_t cram_strip_adult_tokens(char *ft) {
    static const char PREFIX[] = "__adult_";
    const size_t prefix_len = sizeof(PREFIX) - 1u;
    size_t len;
    size_t pos = 0;
    size_t removed = 0;

    if (!ft) return 0;
    len = strlen(ft);

    while (pos < len) {
        size_t start;
        size_t token_len;
        size_t cut_from;
        size_t cut_len;

        while (pos < len && is_ft_space(ft[pos])) pos++;
        start = pos;
        while (pos < len && !is_ft_space(ft[pos])) pos++;
        token_len = pos - start;
        if (token_len < prefix_len) continue;
        if (memcmp(ft + start, PREFIX, prefix_len) != 0) continue;

        /* One adjacent separator goes with the token. The preceding one is
         * preferred, which is what keeps the cut a single memmove and makes
         * `a TOKEN b` -> `a b`; at the buffer start the following separator
         * goes instead, and a lone token loses nothing but itself. */
        if (start > 0) {
            cut_from = start - 1u;
            cut_len = token_len + 1u;
        } else if (pos < len) {
            cut_from = start;
            cut_len = token_len + 1u;
        } else {
            cut_from = start;
            cut_len = token_len;
        }
        memmove(ft + cut_from, ft + cut_from + cut_len,
                len - (cut_from + cut_len) + 1u);
        len -= cut_len;
        removed += cut_len;
        pos = cut_from;
    }

    return removed;
}

/* Raw hit collector: one uint32 per column hit, before dedup, grown by
 * doubling. The deduped row is bounded by CRAM_MAX_FEATURES but the raw hit
 * count is not (a name token of L bytes contributes up to ~5L char grams), so
 * the scratch cannot live inside cram_row_t. It is allocated and freed inside
 * the call, which is what keeps this function thread-safe with no global state.
 */
typedef struct {
    uint32_t *hits;
    size_t count;
    size_t capacity;
    int failed;
} hit_buf_t;

static void hit_push(hit_buf_t *buf, uint32_t column) {
    if (buf->failed) return;
    if (buf->count == buf->capacity) {
        size_t next = buf->capacity ? buf->capacity * 2u : 1024u;
        uint32_t *grown = (uint32_t *)realloc(buf->hits, next * sizeof(*grown));
        if (!grown) {
            buf->failed = 1;
            return;
        }
        buf->hits = grown;
        buf->capacity = next;
    }
    buf->hits[buf->count++] = column;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/* Add the char n-grams of one name token to the hashed tail.
 *
 * The grams are windows over the *codepoints* of `<token>`, not over its bytes:
 * the reference (`cram_repl.gram_buckets`) slices a Python str and then encodes
 * that slice to UTF-8, so a multi-byte codepoint is never split. Byte windows
 * agree on ASCII and silently disagree on every non-ASCII name -- exactly the
 * "UTF-8 token" cases in tests/fixtures/cram/crc32_vectors.json.
 *
 * A ring of the last CRAM_CHAR_GRAM_MAX padded pieces (the `<` marker, the
 * codepoints, the `>` marker) is all the state a window needs, so this is one
 * pass over the token with no scratch allocation.
 */
typedef struct {
    const char *bytes;
    size_t len;
} gram_piece_t;

static void hit_char_grams(const struct cram_vocab *v, const char *token, size_t len,
                           hit_buf_t *buf) {
    gram_piece_t ring[CRAM_CHAR_GRAM_MAX];
    size_t pushed = 0;
    size_t pos = 0;

    for (;;) {
        gram_piece_t piece;
        uint32_t width;

        if (pushed == 0) {
            piece.bytes = "<";
            piece.len = 1;
        } else if (pos < len) {
            size_t step = utf8_seq_len(token + pos, len - pos);
            piece.bytes = token + pos;
            piece.len = step;
            pos += step;
        } else if (pos == len) {
            piece.bytes = ">";
            piece.len = 1;
            pos = len + 1u;
        } else {
            break;
        }

        ring[pushed % CRAM_CHAR_GRAM_MAX] = piece;
        pushed++;
        for (width = CRAM_CHAR_GRAM_MIN; width <= CRAM_CHAR_GRAM_MAX; width++) {
            uint32_t crc = CRAM_CRC32_INIT;
            size_t k;
            if (pushed < width) continue;
            for (k = 0; k < width; k++) {
                const gram_piece_t *part =
                    &ring[(pushed - width + k) % CRAM_CHAR_GRAM_MAX];
                crc = cram_crc32_update(crc, part->bytes, part->len);
            }
            hit_push(buf, v->col_char + ((crc ^ 0xFFFFFFFFu) & (v->buckets - 1u)));
        }
    }
}

cram_status_t cram_features(cram_model_t *model, const char *ft, cram_row_t *out) {
    const struct cram_vocab *v;
    hit_buf_t hits;
    const char *prev = NULL;
    size_t prev_len = 0;
    size_t pos = 0;
    size_t len;
    size_t i;

    if (!model || !out) return CRAM_ERR_ARG;
    v = model->vocab;
    if (!v) return CRAM_ERR_ARG;

    out->count = 0;
    out->truncated = 0;
    if (!ft) {
        return CRAM_OK;
    }
    len = strlen(ft);
    memset(&hits, 0, sizeof(hits));

    while (pos < len) {
        size_t start;
        size_t token_len;
        uint32_t index;

        while (pos < len && is_ft_space(ft[pos])) pos++;
        start = pos;
        while (pos < len && !is_ft_space(ft[pos])) pos++;
        token_len = pos - start;
        if (token_len == 0) break;

        if (token_len >= 2 && ft[start] == '_' && ft[start + 1] == '_') {
            /* Struct marker: exact column only, and it never joins a bigram. */
            if (vocab_find_token(v, v->off_structs, v->n_structs, ft + start, token_len,
                                 &index)) {
                hit_push(&hits, v->col_struct + index);
            }
            continue;
        }

        if (vocab_find_token(v, v->off_words, v->n_words, ft + start, token_len, &index)) {
            hit_push(&hits, v->col_words + index);
        }
        hit_char_grams(v, ft + start, token_len, &hits);

        if (prev) {
            if (vocab_find_pair(v, prev, prev_len, ft + start, token_len, &index)) {
                hit_push(&hits, v->col_bigrams + index);
            }
        }
        prev = ft + start;
        prev_len = token_len;
    }

    if (hits.failed) {
        free(hits.hits);
        return CRAM_ERR_NOMEM;
    }

    if (hits.count) {
        qsort(hits.hits, hits.count, sizeof(hits.hits[0]), cmp_u32);
        i = 0;
        while (i < hits.count) {
            uint32_t column = hits.hits[i];
            size_t run = 1;
            while (i + run < hits.count && hits.hits[i + run] == column) run++;
            if (out->count < CRAM_MAX_FEATURES) {
                out->indices[out->count] = column;
                out->values[out->count] = (float)log1p((double)run);
                out->count++;
            } else {
                /* Deterministic: ascending order means the kept columns are the
                 * lowest ones (the named blocks before the hashed tail). */
                out->truncated = 1;
            }
            i += run;
        }
    }
    free(hits.hits);
    if (out->truncated) stats_bump(&cram_stats.feature_cap_hits, 1);
    return CRAM_OK;
}

/* ------------------------------------------------------------------ */
/* inference                                                           */
/* ------------------------------------------------------------------ */

/* u[o] = bias[o] + sum_f value(f) * weight(f, o), features in ascending order:
 * the nonzero subsequence of the dense dot's order, which is the order the
 * export's fixtures were produced in. */
static cram_status_t unary_fill(const struct cram_container *c, const cram_row_t *row,
                                double *u) {
    uint32_t o;
    int i;

    if (row->count < 0 || row->count > CRAM_MAX_FEATURES) return CRAM_ERR_ARG;
    for (o = 0; o < CRAM_N_OUTPUTS; o++) {
        u[o] = (double)container_f32(c, (size_t)c->bias_off + 4u * o);
    }
    for (i = 0; i < row->count; i++) {
        uint32_t feature = row->indices[i];
        double value = (double)row->values[i];
        uint32_t lo;
        uint32_t hi;
        uint32_t j;
        if (feature >= c->ncols) return CRAM_ERR_ARG;
        lo = container_u32(c, (size_t)c->rowptr_off + 4u * feature);
        hi = container_u32(c, (size_t)c->rowptr_off + 4u * (feature + 1u));
        for (j = lo; j < hi; j++) {
            const unsigned char *entry = c->data + c->entries_off + CRAM_ENTRY_BYTES * (size_t)j;
            u[entry[0]] += value * (double)le_f32(entry + 1);
        }
    }
    return CRAM_OK;
}

/* The whole decision path, over any container: shared by cram_predict() (which
 * also counts) and by the loader tests (which drive a 4-column toy container
 * through cram_container_predict()). */
static cram_status_t predict_core(const struct cram_container *c, const cram_row_t *row,
                                  double threshold, cram_verdict_t *out) {
    double u[CRAM_N_OUTPUTS];
    double energy[CRAM_N_STATES];
    double probs[CRAM_N_STATES];
    double tempered[CRAM_N_STATES];
    double lowest;
    double total;
    double confidence;
    double adult_p;
    cram_status_t st;
    int best;
    int kind;
    int i;

    if (!c || !row || !out) return CRAM_ERR_ARG;
    if (!(threshold > 0.0)) threshold = CRAM_ADULT_THRESHOLD_DEFAULT;

    st = unary_fill(c, row, u);
    if (st != CRAM_OK) return st;

    /* E_s = u[cat] + u[12 + kind] + u[19 + adult], left to right. The unaries
     * live in three different blocks: reading u[skol]/u[asol] is the
     * kind_W/cat_Wk confusion in a different disguise. */
    for (i = 0; i < (int)CRAM_N_STATES; i++) {
        const unsigned char *p = c->data + c->states_off + 3u * (size_t)i;
        energy[i] = u[p[0]] + u[CRAM_KIND_UNARY_BASE + p[1]] + u[CRAM_ADULT_UNARY_BASE + p[2]];
    }

    best = 0;
    for (i = 1; i < (int)CRAM_N_STATES; i++) {
        if (energy[i] < energy[best]) best = i; /* first minimum wins */
    }

    /* Raw softmax(-E): the adult marginal, no temperature. */
    lowest = energy[0];
    for (i = 1; i < (int)CRAM_N_STATES; i++) {
        if (energy[i] < lowest) lowest = energy[i];
    }
    total = 0.0;
    for (i = 0; i < (int)CRAM_N_STATES; i++) {
        probs[i] = exp(lowest - energy[i]);
        total += probs[i];
    }
    adult_p = 0.0;
    for (i = 0; i < (int)CRAM_N_STATES; i++) {
        probs[i] /= total;
        if (c->data[c->states_off + 3u * (size_t)i + 2u] == 1u) adult_p += probs[i];
    }

    /* softmax(-E/T): the category confidence. */
    lowest = energy[0] / c->temperature;
    for (i = 1; i < (int)CRAM_N_STATES; i++) {
        double scaled = energy[i] / c->temperature;
        if (scaled < lowest) lowest = scaled;
    }
    total = 0.0;
    for (i = 0; i < (int)CRAM_N_STATES; i++) {
        tempered[i] = exp(lowest - energy[i] / c->temperature);
        total += tempered[i];
    }
    confidence = 0.0;
    for (i = 0; i < (int)CRAM_N_STATES; i++) {
        tempered[i] /= total;
        if (c->data[c->states_off + 3u * (size_t)i] == c->data[c->states_off + 3u * (size_t)best]) {
            confidence += tempered[i];
        }
    }

    kind = 0;
    for (i = 1; i < (int)CRAM_N_KIND; i++) {
        if (u[CRAM_KIND_HEAD_BASE + i] < u[CRAM_KIND_HEAD_BASE + kind]) kind = i;
    }

    out->category = (int)c->data[c->states_off + 3u * (size_t)best];
    out->kind = kind;
    out->state = best;
    out->adult_p = adult_p;
    out->adult = (adult_p >= threshold) ? 1 : 0;
    out->confidence = confidence;
    return CRAM_OK;
}

cram_status_t cram_container_unary(const cram_container_t *container, const cram_row_t *row,
                                   double *out) {
    if (!container || !row || !out) return CRAM_ERR_ARG;
    return unary_fill(container, row, out);
}

int cram_container_predict(const cram_container_t *container, const cram_row_t *row,
                           double adult_threshold, cram_verdict_t *out) {
    return (int)predict_core(container, row, adult_threshold, out);
}

cram_status_t cram_predict(cram_model_t *model, const cram_row_t *row, double adult_threshold,
                           cram_verdict_t *out) {
    cram_status_t st;
    if (!model) return CRAM_ERR_ARG;
    st = predict_core(model->container, row, adult_threshold, out);
    if (st != CRAM_OK) return st;
    stats_bump(&cram_stats.rows_predicted, 1);
    stats_bump(&cram_stats.kind_head_used, 1);
    if (out->adult) stats_bump(&cram_stats.adult_tagged_predictions, 1);
    return CRAM_OK;
}

/* ------------------------------------------------------------------ */
/* the model                                                           */
/* ------------------------------------------------------------------ */

static int sha_matches(cram_sha256_file_fn sha256_file, const char *path, const char *expected) {
    char actual[CRAM_SHA_CAP];
    if (!sha256_file || !expected || !*expected) return 1; /* nothing to check */
    if (sha256_file(path, actual) != 0) return 0;
    return strcmp(actual, expected) == 0;
}

cram_model_t *cram_load(const char *dir, const cram_digests_t *digests, cram_status_t *status) {
    struct cram_model *model;
    cram_status_t st = CRAM_OK;
    char cram_path[CRAM_PATH_CAP * 2];
    char vocab_path[CRAM_PATH_CAP * 2];

    if (status) *status = CRAM_ERR_ARG;
    if (!dir || !*dir ||
        snprintf(cram_path, sizeof(cram_path), "%s/cram.bin", dir) >= (int)sizeof(cram_path) ||
        snprintf(vocab_path, sizeof(vocab_path), "%s/cram_vocab.bin", dir) >=
            (int)sizeof(vocab_path)) {
        stats_bump(&cram_stats.load_failures, 1);
        return NULL;
    }

    model = (struct cram_model *)calloc(1, sizeof(*model));
    if (!model) {
        stats_bump(&cram_stats.load_failures, 1);
        if (status) *status = CRAM_ERR_NOMEM;
        return NULL;
    }

    /* Unverified bytes are never loaded: the digest gate runs before any parse,
     * with the caller's primitive (clf_sha256_file() in the daemon). */
    if (!sha_matches(digests ? digests->sha256_file : NULL, cram_path,
                     digests ? digests->cram_sha256 : NULL) ||
        !sha_matches(digests ? digests->sha256_file : NULL, vocab_path,
                     digests ? digests->vocab_sha256 : NULL)) {
        free(model);
        stats_bump(&cram_stats.load_failures, 1);
        if (status) *status = CRAM_ERR_SHA;
        return NULL;
    }
    if (digests && digests->cram_sha256) {
        snprintf(model->cram_sha256, sizeof(model->cram_sha256), "%s", digests->cram_sha256);
    }
    if (digests && digests->vocab_sha256) {
        snprintf(model->vocab_sha256, sizeof(model->vocab_sha256), "%s", digests->vocab_sha256);
    }

    model->container = cram_container_load(cram_path, &st);
    if (!model->container) {
        free(model);
        stats_bump(&cram_stats.load_failures, 1);
        if (status) *status = st;
        return NULL;
    }
    model->vocab = vocab_load(vocab_path, &st);
    if (!model->vocab) {
        cram_container_free(model->container);
        free(model);
        stats_bump(&cram_stats.load_failures, 1);
        if (status) *status = st;
        return NULL;
    }

    /* The two files describe one column space: the container's ncols must be
     * exactly the named blocks plus the hashed tail. A mismatch means a mixed
     * generation on disk, which must never produce a verdict. */
    if (model->container->ncols !=
        (uint32_t)(model->vocab->col_char + model->vocab->buckets)) {
        vocab_free(model->vocab);
        cram_container_free(model->container);
        free(model);
        stats_bump(&cram_stats.load_failures, 1);
        if (status) *status = CRAM_ERR_SHAPE;
        return NULL;
    }

    if (status) *status = CRAM_OK;
    return model;
}

void cram_free(cram_model_t *model) {
    if (!model) return;
    vocab_free(model->vocab);
    cram_container_free(model->container);
    free(model);
}

void cram_get_stats(const cram_model_t *model, cram_stats_t *out) {
    (void)model; /* one engine per process; the counters are module-owned */
    if (!out) return;
    pthread_mutex_lock(&cram_stats_mutex);
    *out = cram_stats;
    pthread_mutex_unlock(&cram_stats_mutex);
}

void cram_get_digests(const cram_model_t *model, char *cram_sha256, char *vocab_sha256) {
    if (cram_sha256) {
        cram_sha256[0] = '\0';
        if (model) snprintf(cram_sha256, CRAM_SHA_CAP, "%s", model->cram_sha256);
    }
    if (vocab_sha256) {
        vocab_sha256[0] = '\0';
        if (model) snprintf(vocab_sha256, CRAM_SHA_CAP, "%s", model->vocab_sha256);
    }
}
