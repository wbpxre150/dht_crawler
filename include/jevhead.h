/* jevhead — the `movie_kind` JEV head, in C11 with nothing but libc + libm.
 *
 * Imported verbatim from the classifier-model reference implementation,
 * `native/jevhead/jevhead.h` (source sha256
 * e749c7109ff4be878886b65f65d1862277e16ea3aae62b47c8e79d76b29b71bc), movie-kind
 * Stage B. Keep this file byte-comparable with the reference: it is what pins
 * the on-device head arithmetic to the parity harness that fitted it.
 *
 * `models/v9/movie_kind_head.bin` is a `JEVH` file: a little-endian header, then fp32
 * `W[n_labels * hidden]` (row-major), `b[n_labels]` and `thr[n_labels]`. The head is
 * `sigmoid(W x + b) >= thr` per genre, where `x` is the **unnormalised** CLS embedding of
 * `Torrent: {name}\nFilm genres:` out of the quantised GGUF backbone.
 *
 * Deliberately no dependency on llama.cpp: the embedding is an input, the head is pure
 * arithmetic, and the phone can test the head without a 152 MiB GGUF on the device.
 */

#ifndef JEVHEAD_H
#define JEVHEAD_H

#include <stddef.h>
#include <stdint.h>

#define JEVHEAD_MAGIC "JEVH"
#define JEVHEAD_VERSION 1u
/*: "JEVH" + version + hidden + n_labels + taxonomy sha256. */
#define JEVHEAD_HEADER_SIZE 48u
/*: `jevhead_predict` returns a 32-bit mask, so a head may not be wider than that. */
#define JEVHEAD_MAX_LABELS 32u

typedef struct {
    uint32_t version;
    uint32_t hidden;
    uint32_t n_labels;
    uint8_t  taxonomy_sha256[32];
    float   *weights;    /* n_labels * hidden, row-major: label i is weights[i*hidden .. ] */
    float   *bias;       /* n_labels */
    float   *thresholds; /* n_labels */
} jevhead;

enum jevhead_status {
    JEVHEAD_OK = 0,
    JEVHEAD_ERR_IO = -1,
    JEVHEAD_ERR_MAGIC = -2,
    JEVHEAD_ERR_VERSION = -3,
    JEVHEAD_ERR_SHAPE = -4,
    JEVHEAD_ERR_MEMORY = -5,
    JEVHEAD_ERR_WIDTH = -6
};

/*: Load a `JEVH` head file. On failure returns a negative status and, if `err` is not NULL,
 *: writes a one-line reason into it. A partially loaded head is freed before returning, so a
 *: caller only has to call `jevhead_free` on success. */
int jevhead_load(const char *path, jevhead *head, char *err, size_t err_size);

void jevhead_free(jevhead *head);

/*: `JEVHEAD_HEADER_SIZE + 4 * (n_labels * hidden + 2 * n_labels)` — the only legal size. */
size_t jevhead_expected_bytes(const jevhead *head);

/*: `logit[i] = bias[i] + dot(weights[i], embedding)`. `out_logits` holds `n_labels` floats. */
int jevhead_logits(const jevhead *head, const float *embedding, float *out_logits);

/*: `sigmoid(logit[i]) >= thresholds[i]`, as a bit mask (bit i = label i) plus a popcount.
 *: An all-zero mask is the `none` answer — the same rule as `jev.apply_thresholds`. */
int jevhead_predict(const jevhead *head, const float *embedding, uint32_t *out_mask,
                    uint32_t *out_count);

const char *jevhead_strerror(int status);

#endif /* JEVHEAD_H */
