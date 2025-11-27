#include "refresh_request_queue.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/time.h>

refresh_request_queue_t *refresh_request_queue_create(int capacity) {
    if (capacity <= 0) {
        return NULL;
    }

    refresh_request_queue_t *queue = malloc(sizeof(refresh_request_queue_t));
    if (!queue) {
        return NULL;
    }

    queue->entries = malloc(sizeof(refresh_request_t) * capacity);
    if (!queue->entries) {
        free(queue);
        return NULL;
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->shutdown = false;

    if (pthread_mutex_init(&queue->lock, NULL) != 0) {
        free(queue->entries);
        free(queue);
        return NULL;
    }

    if (pthread_cond_init(&queue->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&queue->lock);
        free(queue->entries);
        free(queue);
        return NULL;
    }

    if (pthread_cond_init(&queue->not_full, NULL) != 0) {
        pthread_cond_destroy(&queue->not_empty);
        pthread_mutex_destroy(&queue->lock);
        free(queue->entries);
        free(queue);
        return NULL;
    }

    return queue;
}

void refresh_request_queue_destroy(refresh_request_queue_t *queue) {
    if (!queue) {
        return;
    }

    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->lock);
    free(queue->entries);
    free(queue);
}

int refresh_request_queue_push(refresh_request_queue_t *queue,
                               const uint8_t *infohash,
                               int timeout_ms) {
    if (!queue || !infohash) {
        return -1;
    }

    pthread_mutex_lock(&queue->lock);

    /* Wait while queue is full */
    if (timeout_ms < 0) {
        /* Infinite timeout */
        while (queue->count >= queue->capacity && !queue->shutdown) {
            pthread_cond_wait(&queue->not_full, &queue->lock);
        }
    } else {
        /* Timed wait */
        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);

        timeout.tv_sec = now.tv_sec + (timeout_ms / 1000);
        timeout.tv_nsec = (now.tv_usec + (timeout_ms % 1000) * 1000) * 1000;

        /* Handle nanosecond overflow */
        if (timeout.tv_nsec >= 1000000000) {
            timeout.tv_sec += 1;
            timeout.tv_nsec -= 1000000000;
        }

        while (queue->count >= queue->capacity && !queue->shutdown) {
            int rc = pthread_cond_timedwait(&queue->not_full, &queue->lock, &timeout);
            if (rc == ETIMEDOUT) {
                pthread_mutex_unlock(&queue->lock);
                return -1;
            }
        }
    }

    /* Check if shutdown was signaled */
    if (queue->shutdown) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }

    /* Add request to queue */
    memcpy(queue->entries[queue->tail].infohash, infohash, 20);
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;

    /* Signal waiting consumers */
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->lock);

    return 0;
}

int refresh_request_queue_pop(refresh_request_queue_t *queue,
                              refresh_request_t *out,
                              int timeout_ms) {
    if (!queue || !out) {
        return -1;
    }

    pthread_mutex_lock(&queue->lock);

    /* Wait while queue is empty */
    if (timeout_ms < 0) {
        /* Infinite timeout */
        while (queue->count == 0 && !queue->shutdown) {
            pthread_cond_wait(&queue->not_empty, &queue->lock);
        }
    } else {
        /* Timed wait */
        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);

        timeout.tv_sec = now.tv_sec + (timeout_ms / 1000);
        timeout.tv_nsec = (now.tv_usec + (timeout_ms % 1000) * 1000) * 1000;

        /* Handle nanosecond overflow */
        if (timeout.tv_nsec >= 1000000000) {
            timeout.tv_sec += 1;
            timeout.tv_nsec -= 1000000000;
        }

        while (queue->count == 0 && !queue->shutdown) {
            int rc = pthread_cond_timedwait(&queue->not_empty, &queue->lock, &timeout);
            if (rc == ETIMEDOUT) {
                pthread_mutex_unlock(&queue->lock);
                return -1;
            }
        }
    }

    /* Check if shutdown was signaled and queue is empty */
    if (queue->shutdown && queue->count == 0) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }

    /* Remove request from queue */
    memcpy(out->infohash, queue->entries[queue->head].infohash, 20);
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    /* Signal waiting producers */
    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->lock);

    return 0;
}

void refresh_request_queue_shutdown(refresh_request_queue_t *queue) {
    if (!queue) {
        return;
    }

    pthread_mutex_lock(&queue->lock);
    queue->shutdown = true;
    pthread_cond_broadcast(&queue->not_empty);
    pthread_cond_broadcast(&queue->not_full);
    pthread_mutex_unlock(&queue->lock);
}
