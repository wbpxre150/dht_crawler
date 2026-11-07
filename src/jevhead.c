/* jevhead — see jevhead.h. C11, libc + libm only, no llama.cpp, no allocation beyond the
 * three float arrays the header declares.
 *
 * Imported verbatim from the classifier-model reference implementation,
 * `native/jevhead/jevhead.c` (source sha256
 * 566ce478c13316394b6a0ae2efe9a98a167e035c194c7d08e319186dc28bfc51), movie-kind
 * Stage B. Not MK_ENABLED-gated on purpose: it is plain C99/C11 arithmetic, the host
 * test pins it against the committed fixtures, and only its *caller*
 * (src/movie_kind.c) needs llama.cpp.
 */

#include "jevhead.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static uint32_t read_u32le(const uint8_t *bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8) | ((uint32_t)bytes[2] << 16) |
           ((uint32_t)bytes[3] << 24);
}

static float read_f32le(const uint8_t *bytes) {
    uint32_t bits = read_u32le(bytes);
    float value;
    /* The JEVH payload is little-endian by definition; memcpy into a float and let the host
     * decide what that means, which is exact on every target the phone and the workstation
     * use (all little-endian) and still well-defined if one is not. */
    memcpy(&value, &bits, sizeof(value));
    return value;
}

static void set_error(char *err, size_t err_size, const char *message) {
    if (err != NULL && err_size > 0) {
        snprintf(err, err_size, "%s", message);
    }
}

const char *jevhead_strerror(int status) {
    switch (status) {
        case JEVHEAD_OK:            return "ok";
        case JEVHEAD_ERR_IO:        return "cannot read the head file";
        case JEVHEAD_ERR_MAGIC:     return "not a JEVH file";
        case JEVHEAD_ERR_VERSION:   return "unsupported JEVH version";
        case JEVHEAD_ERR_SHAPE:     return "head file size disagrees with its header";
        case JEVHEAD_ERR_MEMORY:    return "out of memory";
        case JEVHEAD_ERR_WIDTH:     return "head is wider than the 32-bit mask";
        default:                    return "unknown jevhead status";
    }
}

size_t jevhead_expected_bytes(const jevhead *head) {
    return (size_t)JEVHEAD_HEADER_SIZE +
           4u * ((size_t)head->n_labels * (size_t)head->hidden + 2u * (size_t)head->n_labels);
}

void jevhead_free(jevhead *head) {
    if (head == NULL) {
        return;
    }
    free(head->weights);
    free(head->bias);
    free(head->thresholds);
    head->weights = NULL;
    head->bias = NULL;
    head->thresholds = NULL;
    head->hidden = 0;
    head->n_labels = 0;
    head->version = 0;
    memset(head->taxonomy_sha256, 0, sizeof(head->taxonomy_sha256));
}

int jevhead_load(const char *path, jevhead *head, char *err, size_t err_size) {
    if (path == NULL || head == NULL) {
        set_error(err, err_size, "jevhead_load needs a path and a head");
        return JEVHEAD_ERR_IO;
    }
    memset(head, 0, sizeof(*head));

    FILE *fh = fopen(path, "rb");
    if (fh == NULL) {
        set_error(err, err_size, "cannot open the head file");
        return JEVHEAD_ERR_IO;
    }
    if (fseek(fh, 0, SEEK_END) != 0) {
        fclose(fh);
        set_error(err, err_size, "cannot seek in the head file");
        return JEVHEAD_ERR_IO;
    }
    long size = ftell(fh);
    if (size < 0 || fseek(fh, 0, SEEK_SET) != 0) {
        fclose(fh);
        set_error(err, err_size, "cannot seek in the head file");
        return JEVHEAD_ERR_IO;
    }
    if ((size_t)size < (size_t)JEVHEAD_HEADER_SIZE) {
        fclose(fh);
        set_error(err, err_size, "file is shorter than the JEVH header");
        return JEVHEAD_ERR_SHAPE;
    }
    uint8_t *payload = (uint8_t *)malloc((size_t)size);
    if (payload == NULL) {
        fclose(fh);
        set_error(err, err_size, "out of memory for the head file");
        return JEVHEAD_ERR_MEMORY;
    }
    size_t got = fread(payload, 1, (size_t)size, fh);
    fclose(fh);
    if (got != (size_t)size) {
        free(payload);
        set_error(err, err_size, "short read on the head file");
        return JEVHEAD_ERR_IO;
    }

    int status = JEVHEAD_OK;
    if (memcmp(payload, JEVHEAD_MAGIC, 4) != 0) {
        status = JEVHEAD_ERR_MAGIC;
        set_error(err, err_size, "magic is not 'JEVH'");
        goto done;
    }
    head->version = read_u32le(payload + 4);
    head->hidden = read_u32le(payload + 8);
    head->n_labels = read_u32le(payload + 12);
    memcpy(head->taxonomy_sha256, payload + 16, sizeof(head->taxonomy_sha256));
    if (head->version != JEVHEAD_VERSION) {
        status = JEVHEAD_ERR_VERSION;
        set_error(err, err_size, "header version is not 1");
        goto done;
    }
    if (head->hidden == 0 || head->n_labels == 0) {
        status = JEVHEAD_ERR_SHAPE;
        set_error(err, err_size, "header declares a zero hidden size or zero labels");
        goto done;
    }
    if ((size_t)size != jevhead_expected_bytes(head)) {
        status = JEVHEAD_ERR_SHAPE;
        set_error(err, err_size, "file size disagrees with the header's shape");
        goto done;
    }

    size_t n_weights = (size_t)head->n_labels * (size_t)head->hidden;
    head->weights = (float *)malloc(n_weights * sizeof(float));
    head->bias = (float *)malloc((size_t)head->n_labels * sizeof(float));
    head->thresholds = (float *)malloc((size_t)head->n_labels * sizeof(float));
    if (head->weights == NULL || head->bias == NULL || head->thresholds == NULL) {
        status = JEVHEAD_ERR_MEMORY;
        set_error(err, err_size, "out of memory for the head arrays");
        goto done;
    }
    const uint8_t *cursor = payload + JEVHEAD_HEADER_SIZE;
    for (size_t index = 0; index < n_weights; index++) {
        head->weights[index] = read_f32le(cursor + 4u * index);
    }
    cursor += 4u * n_weights;
    for (uint32_t index = 0; index < head->n_labels; index++) {
        head->bias[index] = read_f32le(cursor + 4u * index);
    }
    cursor += 4u * head->n_labels;
    for (uint32_t index = 0; index < head->n_labels; index++) {
        head->thresholds[index] = read_f32le(cursor + 4u * index);
    }
    for (uint32_t index = 0; index < head->n_labels; index++) {
        if (!(head->thresholds[index] > 0.0f && head->thresholds[index] <= 1.0f)) {
            status = JEVHEAD_ERR_SHAPE;
            set_error(err, err_size, "a threshold is outside (0, 1]");
            goto done;
        }
    }

done:
    free(payload);
    if (status != JEVHEAD_OK) {
        jevhead_free(head);
    }
    return status;
}

int jevhead_logits(const jevhead *head, const float *embedding, float *out_logits) {
    if (head == NULL || embedding == NULL || out_logits == NULL || head->weights == NULL) {
        return JEVHEAD_ERR_IO;
    }
    for (uint32_t label = 0; label < head->n_labels; label++) {
        const float *row = head->weights + (size_t)label * (size_t)head->hidden;
        /* fp32 accumulation in index order: the same order the Python reference uses, so a
         * disagreement here is a real one and not a reassociated sum. */
        float total = 0.0f;
        for (uint32_t index = 0; index < head->hidden; index++) {
            total += row[index] * embedding[index];
        }
        out_logits[label] = total + head->bias[label];
    }
    return JEVHEAD_OK;
}

int jevhead_predict(const jevhead *head, const float *embedding, uint32_t *out_mask,
                    uint32_t *out_count) {
    if (head == NULL || embedding == NULL || out_mask == NULL || out_count == NULL) {
        return JEVHEAD_ERR_IO;
    }
    if (head->n_labels > JEVHEAD_MAX_LABELS) {
        return JEVHEAD_ERR_WIDTH;
    }
    uint32_t mask = 0;
    uint32_t count = 0;
    for (uint32_t label = 0; label < head->n_labels; label++) {
        const float *row = head->weights + (size_t)label * (size_t)head->hidden;
        float total = 0.0f;
        for (uint32_t index = 0; index < head->hidden; index++) {
            total += row[index] * embedding[index];
        }
        double probability = 1.0 / (1.0 + exp(-(double)(total + head->bias[label])));
        /* `>=`: a probability exactly at its calibrated cut is a hit, matching
         * `jev.apply_thresholds` and the `>=` in `calibrate_thresholds`. */
        if (probability >= (double)head->thresholds[label]) {
            mask |= (uint32_t)1u << label;
            count++;
        }
    }
    *out_mask = mask;
    *out_count = count;
    return JEVHEAD_OK;
}
