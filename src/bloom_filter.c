#include "bloom_filter.h"
#include "dht_crawler.h"
#include "../lib/libbloom/bloom.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <uv.h>

struct bloom_filter {
    struct bloom bloom;
    uint64_t capacity;
    double error_rate;
    uint64_t items_added;
    uv_mutex_t mutex;
};

bloom_filter_t* bloom_filter_init(uint64_t capacity, double error_rate) {
    bloom_filter_t *filter = calloc(1, sizeof(bloom_filter_t));
    if (!filter) {
        log_msg(LOG_ERROR, "Failed to allocate bloom filter");
        return NULL;
    }

    filter->capacity = capacity;
    filter->error_rate = error_rate;
    filter->items_added = 0;

    // Initialize libbloom structure
    int ret = bloom_init(&filter->bloom, capacity, error_rate);
    if (ret != 0) {
        log_msg(LOG_ERROR, "Failed to initialize bloom filter (capacity=%lu, error_rate=%.6f)",
                capacity, error_rate);
        free(filter);
        return NULL;
    }

    // Initialize mutex for thread safety
    if (uv_mutex_init(&filter->mutex) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize bloom filter mutex");
        bloom_free(&filter->bloom);
        free(filter);
        return NULL;
    }

    uint64_t bytes = filter->bloom.bytes;
    log_msg(LOG_DEBUG, "Bloom filter initialized: capacity=%lu, error_rate=%.6f, memory=%lu bytes (%.2f MB)",
            capacity, error_rate, bytes, bytes / (1024.0 * 1024.0));

    return filter;
}

void bloom_filter_add(bloom_filter_t *filter, const unsigned char *hash) {
    if (!filter || !hash) {
        return;
    }

    uv_mutex_lock(&filter->mutex);
    bloom_add(&filter->bloom, hash, 20);
    filter->items_added++;
    uv_mutex_unlock(&filter->mutex);
}

bool bloom_filter_check(bloom_filter_t *filter, const unsigned char *hash) {
    if (!filter || !hash) {
        return false;
    }

    uv_mutex_lock(&filter->mutex);
    bool result = bloom_check(&filter->bloom, hash, 20) == 1;
    uv_mutex_unlock(&filter->mutex);
    return result;
}

void bloom_filter_reset(bloom_filter_t *filter) {
    if (!filter) {
        return;
    }

    uv_mutex_lock(&filter->mutex);
    // Reset bloom filter bits
    memset(filter->bloom.bf, 0, filter->bloom.bytes);
    filter->items_added = 0;
    uv_mutex_unlock(&filter->mutex);

    log_msg(LOG_DEBUG, "Bloom filter reset");
}

void bloom_filter_stats(bloom_filter_t *filter, uint64_t *out_capacity,
                       double *out_error_rate, uint64_t *out_bytes) {
    if (!filter) {
        return;
    }

    uv_mutex_lock(&filter->mutex);
    if (out_capacity) {
        *out_capacity = filter->capacity;
    }
    if (out_error_rate) {
        *out_error_rate = filter->error_rate;
    }
    if (out_bytes) {
        *out_bytes = filter->bloom.bytes;
    }
    uv_mutex_unlock(&filter->mutex);
}

int bloom_filter_save(bloom_filter_t *filter, const char *path) {
    if (!filter || !path) {
        return -1;
    }

    FILE *fp = fopen(path, "wb");
    if (!fp) {
        log_msg(LOG_ERROR, "Failed to open bloom filter file for writing: %s", path);
        return -1;
    }

    uv_mutex_lock(&filter->mutex);

    // Write metadata
    if (fwrite(&filter->capacity, sizeof(uint64_t), 1, fp) != 1 ||
        fwrite(&filter->error_rate, sizeof(double), 1, fp) != 1 ||
        fwrite(&filter->items_added, sizeof(uint64_t), 1, fp) != 1 ||
        fwrite(&filter->bloom.bytes, sizeof(uint64_t), 1, fp) != 1 ||
        fwrite(&filter->bloom.bits, sizeof(uint64_t), 1, fp) != 1 ||
        fwrite(&filter->bloom.hashes, sizeof(uint32_t), 1, fp) != 1) {
        log_msg(LOG_ERROR, "Failed to write bloom filter metadata");
        uv_mutex_unlock(&filter->mutex);
        fclose(fp);
        return -1;
    }

    // Write bit array
    size_t bytes = filter->bloom.bytes;
    uint64_t items = filter->items_added;
    if (fwrite(filter->bloom.bf, 1, bytes, fp) != bytes) {
        log_msg(LOG_ERROR, "Failed to write bloom filter data");
        uv_mutex_unlock(&filter->mutex);
        fclose(fp);
        return -1;
    }

    uv_mutex_unlock(&filter->mutex);

    fclose(fp);
    log_msg(LOG_DEBUG, "Bloom filter saved to %s (%lu items, %.2f MB)",
            path, items, bytes / (1024.0 * 1024.0));
    return 0;
}

bloom_filter_t* bloom_filter_load(const char *path) {
    if (!path) {
        return NULL;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        log_msg(LOG_WARN, "Failed to open bloom filter file for reading: %s", path);
        return NULL;
    }

    bloom_filter_t *filter = calloc(1, sizeof(bloom_filter_t));
    if (!filter) {
        fclose(fp);
        return NULL;
    }

    // Read metadata
    uint64_t bytes, bits;
    uint32_t hashes;
    if (fread(&filter->capacity, sizeof(uint64_t), 1, fp) != 1 ||
        fread(&filter->error_rate, sizeof(double), 1, fp) != 1 ||
        fread(&filter->items_added, sizeof(uint64_t), 1, fp) != 1 ||
        fread(&bytes, sizeof(uint64_t), 1, fp) != 1 ||
        fread(&bits, sizeof(uint64_t), 1, fp) != 1 ||
        fread(&hashes, sizeof(uint32_t), 1, fp) != 1) {
        log_msg(LOG_ERROR, "Failed to read bloom filter metadata");
        free(filter);
        fclose(fp);
        return NULL;
    }

    // Allocate bit array
    filter->bloom.bytes = bytes;
    filter->bloom.bits = bits;
    filter->bloom.hashes = hashes;
    filter->bloom.bf = calloc(bytes, 1);
    if (!filter->bloom.bf) {
        log_msg(LOG_ERROR, "Failed to allocate bloom filter data");
        free(filter);
        fclose(fp);
        return NULL;
    }

    // Read bit array
    if (fread(filter->bloom.bf, 1, bytes, fp) != bytes) {
        log_msg(LOG_ERROR, "Failed to read bloom filter data");
        free(filter->bloom.bf);
        free(filter);
        fclose(fp);
        return NULL;
    }

    // Mark as ready (required by libbloom)
    filter->bloom.ready = 1;

    // Initialize mutex for thread safety
    if (uv_mutex_init(&filter->mutex) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize bloom filter mutex");
        free(filter->bloom.bf);
        free(filter);
        fclose(fp);
        return NULL;
    }

    fclose(fp);
    log_msg(LOG_DEBUG, "Bloom filter loaded from %s (%lu items, %.2f MB)",
            path, filter->items_added, bytes / (1024.0 * 1024.0));
    return filter;
}

void bloom_filter_cleanup(bloom_filter_t *filter) {
    if (!filter) {
        return;
    }

    // Destroy mutex
    uv_mutex_destroy(&filter->mutex);

    if (filter->bloom.bf) {
        free(filter->bloom.bf);
    }
    free(filter);
}
