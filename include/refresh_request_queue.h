#ifndef REFRESH_REQUEST_QUEUE_H
#define REFRESH_REQUEST_QUEUE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef struct refresh_request {
    uint8_t infohash[20];
} refresh_request_t;

typedef struct refresh_request_queue {
    refresh_request_t *entries;
    int capacity;
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    bool shutdown;
} refresh_request_queue_t;

/**
 * Create a new refresh request queue
 * @param capacity Maximum number of requests the queue can hold
 * @return New queue, or NULL on error
 */
refresh_request_queue_t *refresh_request_queue_create(int capacity);

/**
 * Destroy a refresh request queue
 * @param queue Queue to destroy
 */
void refresh_request_queue_destroy(refresh_request_queue_t *queue);

/**
 * Push a refresh request onto the queue (blocking with timeout)
 * @param queue Queue to push to
 * @param infohash 20-byte infohash
 * @param timeout_ms Timeout in milliseconds, -1 for infinite
 * @return 0 on success, -1 on timeout or shutdown
 */
int refresh_request_queue_push(refresh_request_queue_t *queue,
                               const uint8_t *infohash,
                               int timeout_ms);

/**
 * Pop a refresh request from the queue (blocking with timeout)
 * @param queue Queue to pop from
 * @param out Output buffer for request
 * @param timeout_ms Timeout in milliseconds, -1 for infinite
 * @return 0 on success, -1 on timeout or shutdown
 */
int refresh_request_queue_pop(refresh_request_queue_t *queue,
                              refresh_request_t *out,
                              int timeout_ms);

/**
 * Signal shutdown to the queue (wakes all waiting threads)
 * @param queue Queue to shutdown
 */
void refresh_request_queue_shutdown(refresh_request_queue_t *queue);

#endif /* REFRESH_REQUEST_QUEUE_H */
