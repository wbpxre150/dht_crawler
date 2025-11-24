#ifndef TREE_RESPONSE_QUEUE_H
#define TREE_RESPONSE_QUEUE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/socket.h>

/**
 * Thread-safe response queue for UDP receiver → worker communication
 *
 * Each worker has its own response queue. The UDP receiver thread routes
 * incoming responses to the appropriate worker queue based on transaction ID.
 */

#define MAX_RESPONSE_SIZE 2048

typedef struct tree_response {
    uint8_t data[MAX_RESPONSE_SIZE];
    int len;
    struct sockaddr_storage from;
} tree_response_t;

typedef struct tree_response_queue {
    tree_response_t *responses;
    int capacity;
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    bool shutdown;
} tree_response_queue_t;

/**
 * Create a new response queue
 * @param capacity Maximum number of responses to buffer
 * @return Pointer to queue, or NULL on error
 */
tree_response_queue_t *tree_response_queue_create(int capacity);

/**
 * Destroy a response queue
 * @param q Queue to destroy
 */
void tree_response_queue_destroy(tree_response_queue_t *q);

/**
 * Push a response to the queue (non-blocking, drops if full)
 * @param q Queue
 * @param response Response to push
 * @return 0 on success, -1 if full or error
 */
int tree_response_queue_try_push(tree_response_queue_t *q, const tree_response_t *response);

/**
 * Pop a response from the queue (blocking with timeout)
 * @param q Queue
 * @param response Buffer to receive response
 * @param timeout_ms Timeout in milliseconds (-1 for infinite)
 * @return 0 on success, -1 on timeout/shutdown/error
 */
int tree_response_queue_pop(tree_response_queue_t *q, tree_response_t *response, int timeout_ms);

/**
 * Signal all waiting threads to wake up and exit
 * @param q Queue
 */
void tree_response_queue_signal_shutdown(tree_response_queue_t *q);

/**
 * Get current queue count
 * @param q Queue
 * @return Number of items in queue
 */
int tree_response_queue_count(tree_response_queue_t *q);

#endif /* TREE_RESPONSE_QUEUE_H */
