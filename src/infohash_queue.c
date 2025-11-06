#include "infohash_queue.h"
#include "dht_crawler.h"
#include "database.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Initialize the info hash queue */
int infohash_queue_init(infohash_queue_t *queue, size_t capacity) {
    if (!queue || capacity == 0) {
        return -1;
    }

    queue->entries = (infohash_entry_t *)calloc(capacity, sizeof(infohash_entry_t));
    if (!queue->entries) {
        return -1;
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->bloom = NULL;
    queue->db = NULL;
    queue->duplicates_filtered = 0;

    if (uv_mutex_init(&queue->mutex) != 0) {
        free(queue->entries);
        return -1;
    }

    if (uv_cond_init(&queue->cond_not_empty) != 0) {
        uv_mutex_destroy(&queue->mutex);
        free(queue->entries);
        return -1;
    }

    if (uv_cond_init(&queue->cond_not_full) != 0) {
        uv_cond_destroy(&queue->cond_not_empty);
        uv_mutex_destroy(&queue->mutex);
        free(queue->entries);
        return -1;
    }

    return 0;
}

/* Set bloom filter for duplicate detection */
void infohash_queue_set_bloom(infohash_queue_t *queue, bloom_filter_t *bloom) {
    if (!queue) {
        return;
    }
    queue->bloom = bloom;
}

/* Set database for duplicate checking */
void infohash_queue_set_database(infohash_queue_t *queue, struct database *db) {
    if (!queue) {
        return;
    }
    queue->db = db;
}

/* Push an info hash to the queue (blocks if full) */
int infohash_queue_push(infohash_queue_t *queue, const uint8_t *info_hash) {
    if (!queue || !info_hash) {
        return -1;
    }

    /* Pre-check bloom filter + database before acquiring queue mutex (optional optimization) */
    if (queue->bloom && bloom_filter_check(queue->bloom, info_hash)) {
        /* Likely duplicate - check database to confirm */
        if (queue->db && database_has_infohash((database_t *)queue->db, info_hash)) {
            /* Confirmed duplicate - increment counter under lock */
            uv_mutex_lock(&queue->mutex);
            queue->duplicates_filtered++;
            uv_mutex_unlock(&queue->mutex);
            return 0;  /* Success but filtered */
        }
    }

    uv_mutex_lock(&queue->mutex);

    /* Double-check bloom filter under lock to prevent TOCTOU race */
    if (queue->bloom && bloom_filter_check(queue->bloom, info_hash)) {
        if (queue->db && database_has_infohash((database_t *)queue->db, info_hash)) {
            queue->duplicates_filtered++;
            uv_mutex_unlock(&queue->mutex);
            return 0;
        }
    }

    /* Wait if queue is full */
    while (queue->count >= queue->capacity) {
        uv_cond_wait(&queue->cond_not_full, &queue->mutex);
    }

    /* Add to tail */
    memcpy(queue->entries[queue->tail].info_hash, info_hash, SHA1_DIGEST_LENGTH);
    queue->entries[queue->tail].timestamp = time(NULL);

    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;

    /* NOTE: Bloom filter is NOT updated here - it's updated only after successful
     * database write in database_insert_batch() to prevent data loss from failed
     * metadata fetches. The bloom filter check above is read-only for duplicate detection. */

    /* Signal that queue is not empty */
    uv_cond_signal(&queue->cond_not_empty);
    uv_mutex_unlock(&queue->mutex);

    return 0;
}

/* Pop an info hash from the queue (blocks if empty) */
int infohash_queue_pop(infohash_queue_t *queue, uint8_t *info_hash) {
    if (!queue || !info_hash) {
        return -1;
    }

    uv_mutex_lock(&queue->mutex);

    /* Wait if queue is empty */
    while (queue->count == 0) {
        uv_cond_wait(&queue->cond_not_empty, &queue->mutex);
    }

    /* Remove from head */
    memcpy(info_hash, queue->entries[queue->head].info_hash, SHA1_DIGEST_LENGTH);

    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    /* Signal that queue is not full */
    uv_cond_signal(&queue->cond_not_full);
    uv_mutex_unlock(&queue->mutex);

    return 0;
}

/* Try to pop with timeout */
int infohash_queue_try_pop(infohash_queue_t *queue, uint8_t *info_hash, int timeout_ms) {
    if (!queue || !info_hash) {
        return -1;
    }

    uv_mutex_lock(&queue->mutex);

    if (queue->count == 0) {
        uv_mutex_unlock(&queue->mutex);
        
        if (timeout_ms == 0) {
            return -1;
        }

        /* Sleep for the timeout period, then retry once
         * This prevents busy-looping when queue is empty */
        struct timespec ts;
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000;
        nanosleep(&ts, NULL);
        
        /* Try one more time after timeout */
        uv_mutex_lock(&queue->mutex);
        if (queue->count == 0) {
            uv_mutex_unlock(&queue->mutex);
            return -1;
        }
        /* Fall through to pop if queue now has items */
    }

    /* Remove from head */
    memcpy(info_hash, queue->entries[queue->head].info_hash, SHA1_DIGEST_LENGTH);

    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    /* Signal that queue is not full */
    uv_cond_signal(&queue->cond_not_full);
    uv_mutex_unlock(&queue->mutex);

    return 0;
}

/* Get queue size */
size_t infohash_queue_size(infohash_queue_t *queue) {
    if (!queue) {
        return 0;
    }

    uv_mutex_lock(&queue->mutex);
    size_t size = queue->count;
    uv_mutex_unlock(&queue->mutex);

    return size;
}

/* Get queue capacity */
size_t infohash_queue_capacity(infohash_queue_t *queue) {
    if (!queue) {
        return 0;
    }
    return queue->capacity;
}

/* Check if queue is empty */
int infohash_queue_is_empty(infohash_queue_t *queue) {
    return infohash_queue_size(queue) == 0;
}

/* Check if queue is full */
int infohash_queue_is_full(infohash_queue_t *queue) {
    if (!queue) {
        return 0;
    }

    uv_mutex_lock(&queue->mutex);
    int full = (queue->count >= queue->capacity);
    uv_mutex_unlock(&queue->mutex);

    return full;
}

/* Get number of duplicates filtered */
uint64_t infohash_queue_get_duplicates(infohash_queue_t *queue) {
    if (!queue) {
        return 0;
    }

    uv_mutex_lock(&queue->mutex);
    uint64_t dups = queue->duplicates_filtered;
    uv_mutex_unlock(&queue->mutex);

    return dups;
}

/* Cleanup queue */
void infohash_queue_cleanup(infohash_queue_t *queue) {
    if (!queue) {
        return;
    }

    if (queue->entries) {
        free(queue->entries);
        queue->entries = NULL;
    }

    uv_cond_destroy(&queue->cond_not_full);
    uv_cond_destroy(&queue->cond_not_empty);
    uv_mutex_destroy(&queue->mutex);
}
