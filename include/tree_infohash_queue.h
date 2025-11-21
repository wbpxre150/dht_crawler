#ifndef TREE_INFOHASH_QUEUE_H
#define TREE_INFOHASH_QUEUE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

/**
 * Thread-safe infohash queue for Stage 3
 *
 * Private queue for each thread tree to store discovered infohashes
 * before passing them to get_peers workers.
 */

typedef struct infohash_entry {
    uint8_t infohash[20];
} infohash_entry_t;

typedef struct tree_infohash_queue {
    infohash_entry_t *entries;
    int capacity;
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    bool shutdown;
} tree_infohash_queue_t;

/**
 * Create a new infohash queue
 * @param capacity Maximum number of infohashes to store
 * @return Pointer to queue, or NULL on error
 */
tree_infohash_queue_t *tree_infohash_queue_create(int capacity);

/**
 * Destroy an infohash queue
 * @param q Queue to destroy
 */
void tree_infohash_queue_destroy(tree_infohash_queue_t *q);

/**
 * Push an infohash to the queue (blocking if full)
 * @param q Queue
 * @param infohash 20-byte infohash to push
 * @return 0 on success, -1 on shutdown or error
 */
int tree_infohash_queue_push(tree_infohash_queue_t *q, const uint8_t *infohash);

/**
 * Pop an infohash from the queue (blocking with timeout)
 * @param q Queue
 * @param infohash Buffer to receive 20-byte infohash
 * @param timeout_ms Timeout in milliseconds (-1 for infinite)
 * @return 0 on success, -1 on timeout/shutdown/error
 */
int tree_infohash_queue_pop(tree_infohash_queue_t *q, uint8_t *infohash, int timeout_ms);

/**
 * Try to push an infohash without blocking
 * @param q Queue
 * @param infohash 20-byte infohash to push
 * @return 0 on success, -1 if full or error
 */
int tree_infohash_queue_try_push(tree_infohash_queue_t *q, const uint8_t *infohash);

/**
 * Signal all waiting threads to wake up and exit
 * @param q Queue
 */
void tree_infohash_queue_signal_shutdown(tree_infohash_queue_t *q);

/**
 * Get current queue count
 * @param q Queue
 * @return Number of items in queue
 */
int tree_infohash_queue_count(tree_infohash_queue_t *q);

#endif /* TREE_INFOHASH_QUEUE_H */
