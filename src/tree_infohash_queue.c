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

    /* Wait for space */
    while (q->count >= q->capacity && !q->shutdown) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Add to queue */
    memcpy(q->entries[q->tail].infohash, infohash, 20);
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

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

    /* Pop from queue */
    memcpy(infohash, q->entries[q->head].infohash, 20);
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

int tree_infohash_queue_try_push(tree_infohash_queue_t *q, const uint8_t *infohash) {
    if (!q || !infohash) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    if (q->shutdown || q->count >= q->capacity) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Add to queue */
    memcpy(q->entries[q->tail].infohash, infohash, 20);
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

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
