#include "tree_peers_queue.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

tree_peers_queue_t *tree_peers_queue_create(int capacity) {
    if (capacity <= 0) {
        return NULL;
    }

    tree_peers_queue_t *q = calloc(1, sizeof(tree_peers_queue_t));
    if (!q) {
        return NULL;
    }

    q->entries = calloc(capacity, sizeof(peer_entry_t));
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

void tree_peers_queue_destroy(tree_peers_queue_t *q) {
    if (!q) {
        return;
    }

    tree_peers_queue_signal_shutdown(q);

    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->lock);
    free(q->entries);
    free(q);
}

int tree_peers_queue_push(tree_peers_queue_t *q, const peer_entry_t *entry) {
    if (!q || !entry) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    /* Wait while full (unless shutdown) */
    while (q->count >= q->capacity && !q->shutdown) {
        pthread_cond_wait(&q->not_full, &q->lock);
    }

    if (q->shutdown) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Copy entry to queue */
    memcpy(&q->entries[q->tail], entry, sizeof(peer_entry_t));
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

int tree_peers_queue_pop(tree_peers_queue_t *q, peer_entry_t *entry, int timeout_ms) {
    if (!q || !entry) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    /* Calculate absolute timeout */
    struct timespec ts;
    if (timeout_ms >= 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }
    }

    /* Wait while empty (unless shutdown or timeout) */
    while (q->count == 0 && !q->shutdown) {
        int rc;
        if (timeout_ms < 0) {
            rc = pthread_cond_wait(&q->not_empty, &q->lock);
        } else {
            rc = pthread_cond_timedwait(&q->not_empty, &q->lock, &ts);
        }

        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&q->lock);
            return -1;  /* Timeout */
        }
    }

    if (q->count == 0) {
        pthread_mutex_unlock(&q->lock);
        return -1;  /* Shutdown with empty queue */
    }

    /* Copy entry from queue */
    memcpy(entry, &q->entries[q->head], sizeof(peer_entry_t));
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

int tree_peers_queue_try_push(tree_peers_queue_t *q, const peer_entry_t *entry) {
    if (!q || !entry) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    if (q->count >= q->capacity || q->shutdown) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Copy entry to queue */
    memcpy(&q->entries[q->tail], entry, sizeof(peer_entry_t));
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

void tree_peers_queue_signal_shutdown(tree_peers_queue_t *q) {
    if (!q) {
        return;
    }

    pthread_mutex_lock(&q->lock);
    q->shutdown = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->lock);
}

int tree_peers_queue_count(tree_peers_queue_t *q) {
    if (!q) {
        return 0;
    }

    pthread_mutex_lock(&q->lock);
    int count = q->count;
    pthread_mutex_unlock(&q->lock);

    return count;
}
