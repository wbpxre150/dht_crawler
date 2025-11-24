#include "tree_response_queue.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

tree_response_queue_t *tree_response_queue_create(int capacity) {
    if (capacity <= 0) {
        return NULL;
    }

    tree_response_queue_t *q = calloc(1, sizeof(tree_response_queue_t));
    if (!q) {
        return NULL;
    }

    q->responses = calloc(capacity, sizeof(tree_response_t));
    if (!q->responses) {
        free(q);
        return NULL;
    }

    q->capacity = capacity;
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->shutdown = false;

    if (pthread_mutex_init(&q->lock, NULL) != 0) {
        free(q->responses);
        free(q);
        return NULL;
    }

    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->lock);
        free(q->responses);
        free(q);
        return NULL;
    }

    return q;
}

void tree_response_queue_destroy(tree_response_queue_t *q) {
    if (!q) {
        return;
    }

    tree_response_queue_signal_shutdown(q);

    pthread_cond_destroy(&q->not_empty);
    pthread_mutex_destroy(&q->lock);
    free(q->responses);
    free(q);
}

int tree_response_queue_try_push(tree_response_queue_t *q, const tree_response_t *response) {
    if (!q || !response) {
        return -1;
    }

    pthread_mutex_lock(&q->lock);

    if (q->count >= q->capacity || q->shutdown) {
        pthread_mutex_unlock(&q->lock);
        return -1;
    }

    /* Copy response to queue */
    memcpy(&q->responses[q->tail], response, sizeof(tree_response_t));
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);

    return 0;
}

int tree_response_queue_pop(tree_response_queue_t *q, tree_response_t *response, int timeout_ms) {
    if (!q || !response) {
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

    /* Copy response from queue */
    memcpy(response, &q->responses[q->head], sizeof(tree_response_t));
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    pthread_mutex_unlock(&q->lock);

    return 0;
}

void tree_response_queue_signal_shutdown(tree_response_queue_t *q) {
    if (!q) {
        return;
    }

    pthread_mutex_lock(&q->lock);
    q->shutdown = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

int tree_response_queue_count(tree_response_queue_t *q) {
    if (!q) {
        return 0;
    }

    pthread_mutex_lock(&q->lock);
    int count = q->count;
    pthread_mutex_unlock(&q->lock);

    return count;
}
