#include "connection_request_queue.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Connection request queue implementation */
struct connection_request_queue {
    connection_request_t **requests;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    uv_mutex_t mutex;
};

/* Initialize connection request queue */
connection_request_queue_t* connection_request_queue_init(size_t capacity) {
    if (capacity == 0) {
        return NULL;
    }

    connection_request_queue_t *queue = (connection_request_queue_t *)malloc(sizeof(connection_request_queue_t));
    if (!queue) {
        return NULL;
    }

    queue->requests = (connection_request_t **)calloc(capacity, sizeof(connection_request_t *));
    if (!queue->requests) {
        free(queue);
        return NULL;
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;

    if (uv_mutex_init(&queue->mutex) != 0) {
        free(queue->requests);
        free(queue);
        return NULL;
    }

    return queue;
}

/* Push a connection request to the queue (thread-safe) */
int connection_request_queue_push(connection_request_queue_t *queue, connection_request_t *req) {
    if (!queue || !req) {
        return -1;
    }

    uv_mutex_lock(&queue->mutex);

    /* Check if queue is full */
    if (queue->count >= queue->capacity) {
        uv_mutex_unlock(&queue->mutex);

        /* Use rate-limited logging to prevent log floods */
        static time_t last_warn_time = 0;
        static size_t dropped_count = 0;
        time_t now = time(NULL);
        dropped_count++;

        if (now - last_warn_time >= 5) {  /* Warn at most once per 5 seconds */
            log_msg(LOG_WARN, "Connection request queue full (capacity=%zu), dropped %zu requests in last %ld seconds",
                    queue->capacity, dropped_count, (long)(now - last_warn_time));
            last_warn_time = now;
            dropped_count = 0;
        }

        return -1;
    }

    /* Add to queue */
    queue->requests[queue->tail] = req;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;

    uv_mutex_unlock(&queue->mutex);
    return 0;
}

/* Try to pop a connection request from the queue (thread-safe) */
connection_request_t* connection_request_queue_try_pop(connection_request_queue_t *queue) {
    if (!queue) {
        return NULL;
    }

    uv_mutex_lock(&queue->mutex);

    /* Check if queue is empty */
    if (queue->count == 0) {
        uv_mutex_unlock(&queue->mutex);
        return NULL;
    }

    /* Remove from queue */
    connection_request_t *req = queue->requests[queue->head];
    queue->requests[queue->head] = NULL;
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;

    uv_mutex_unlock(&queue->mutex);
    return req;
}

/* Get current queue size (thread-safe) */
size_t connection_request_queue_size(connection_request_queue_t *queue) {
    if (!queue) {
        return 0;
    }

    uv_mutex_lock(&queue->mutex);
    size_t size = queue->count;
    uv_mutex_unlock(&queue->mutex);

    return size;
}

/* Cleanup connection request queue */
void connection_request_queue_cleanup(connection_request_queue_t *queue) {
    if (!queue) {
        return;
    }

    /* Free any remaining requests */
    uv_mutex_lock(&queue->mutex);
    for (size_t i = 0; i < queue->capacity; i++) {
        if (queue->requests[i]) {
            free(queue->requests[i]);
            queue->requests[i] = NULL;
        }
    }
    uv_mutex_unlock(&queue->mutex);

    uv_mutex_destroy(&queue->mutex);
    free(queue->requests);
    free(queue);
}
