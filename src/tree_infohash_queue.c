#include "tree_infohash_queue.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>

tree_infohash_queue_t *tree_infohash_queue_create(int capacity) {
    if (capacity <= 0) {
        return NULL;
    }

    tree_infohash_queue_t *q = calloc(1, sizeof(tree_infohash_queue_t));
    if (!q) {
        return NULL;
    }

    q->entries = calloc(capacity, sizeof(infohash_entry_t));
    if (!q->entries) {
        free(q);
        return NULL;
    }

    q->capacity = capacity;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = false;
    q->infohash_set = NULL;  /* Initialize empty hash table */
    q->total_push_attempts = 0;
    q->duplicates_rejected = 0;

    if (pthread_mutex_init(&q->lock, NULL) != 0) {
        free(q->entries);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->lock);
        free(q->entries);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->lock);
        free(q->entries);
        free(q);
        return NULL;
    }

    return q;
}

void tree_infohash_queue_destroy(tree_infohash_queue_t *q) {
    if (!q) {
        return;
    }

    /* Signal shutdown first */
    tree_infohash_queue_signal_shutdown(q);

    pthread_mutex_lock(&q->lock);

    /* Free hash table entries */
    infohash_set_entry_t *entry, *tmp;
    HASH_ITER(hh, q->infohash_set, entry, tmp) {
        HASH_DEL(q->infohash_set, entry);
        free(entry);
    }
    q->infohash_set = NULL;

    pthread_mutex_unlock(&q->lock);

    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->lock);

    free(q->entries);
    free(q);
}

int tree_infohash_queue_push(tree_infohash_queue_t *q, const uint8_t *infohash) {
    if (!q || !infohash) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    q->total_push_attempts++;

    /* Check for duplicates in hash table (O(1) lookup) */
    infohash_set_entry_t *existing = NULL;
    HASH_FIND(hh, q->infohash_set, infohash, 20, existing);
    if (existing) {
        /* Duplicate detected - already in queue */
        q->duplicates_rejected++;
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Wait for space */
    while (q->count >= q->capacity && !q->shutdown) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Add to circular buffer */
    memcpy(q->entries[q->tail].infohash, infohash, 20);
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    /* Add to hash table for duplicate tracking */
    infohash_set_entry_t *new_entry = malloc(sizeof(infohash_set_entry_t));
    if (new_entry) {
        memcpy(new_entry->infohash, infohash, 20);
        HASH_ADD(hh, q->infohash_set, infohash, 20, new_entry);
    }
    /* Note: If malloc fails, we still added to queue but lose duplicate detection
     * for this entry. This is acceptable degradation - won't corrupt state. */

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

int tree_infohash_queue_pop(tree_infohash_queue_t *q, uint8_t *infohash, int timeout_ms) {
    if (!q || !infohash) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    /* Wait for data */
    if (timeout_ms < 0) {
        /* Infinite wait */
        while (q->count == 0 && !q->shutdown) {
            pthread_cond_wait(&q->not_empty, &q->lock);
        }
    } else {
        /* Timed wait */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }

        while (q->count == 0 && !q->shutdown) {
            int ret = pthread_cond_timedwait(&q->not_empty, &q->lock, &ts);
            if (ret == ETIMEDOUT) {
                pthread_mutex_unlock(&q->lock);
                return -1;
            }
        }
    }

    if (q->shutdown && q->count == 0) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Pop from circular buffer */
    memcpy(infohash, q->entries[q->head].infohash, 20);
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    /* Remove from hash table (synchronize with queue state) */
    infohash_set_entry_t *entry = NULL;
    HASH_FIND(hh, q->infohash_set, infohash, 20, entry);
    if (entry) {
        HASH_DEL(q->infohash_set, entry);
        free(entry);
    }
    /* Note: If not found in hash (malloc failed during push), that's ok -
     * just means we lost duplicate detection for this entry. */

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

int tree_infohash_queue_try_push(tree_infohash_queue_t *q, const uint8_t *infohash) {
    if (!q || !infohash) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    q->total_push_attempts++;

    /* Check for duplicates in hash table (O(1) lookup) */
    infohash_set_entry_t *existing = NULL;
    HASH_FIND(hh, q->infohash_set, infohash, 20, existing);
    if (existing) {
        /* Duplicate detected - already in queue */
        q->duplicates_rejected++;
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    if (q->shutdown || q->count >= q->capacity) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Add to circular buffer */
    memcpy(q->entries[q->tail].infohash, infohash, 20);
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    /* Add to hash table for duplicate tracking */
    infohash_set_entry_t *new_entry = malloc(sizeof(infohash_set_entry_t));
    if (new_entry) {
        memcpy(new_entry->infohash, infohash, 20);
        HASH_ADD(hh, q->infohash_set, infohash, 20, new_entry);
    }

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

void tree_infohash_queue_signal_shutdown(tree_infohash_queue_t *q) {
    if (!q) {
        return;
    }

    pthread_mutex_lock(&q->lock);
    q->shutdown = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}

int tree_infohash_queue_count(tree_infohash_queue_t *q) {
    if (!q) {
        return 0;
    }

    pthread_mutex_lock(&q->lock);
    int count = q->count;
    pthread_mutex_unlock(&q->lock);

    return count;
}

void tree_infohash_queue_get_stats(tree_infohash_queue_t *q,
                                    uint64_t *out_total,
                                    uint64_t *out_duplicates) {
    if (!q) {
        return;
    }

    pthread_mutex_lock(&q->lock);
    if (out_total) {
        *out_total = q->total_push_attempts;
    }
    if (out_duplicates) {
        *out_duplicates = q->duplicates_rejected;
    }
    pthread_mutex_unlock(&q->lock);
}
