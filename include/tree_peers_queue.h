#ifndef TREE_PEERS_QUEUE_H
#define TREE_PEERS_QUEUE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/socket.h>

/**
 * Thread-safe peers queue for Stage 4
 *
 * Stores peer addresses discovered via get_peers for each infohash.
 * Get_peers workers push entries, metadata workers pop them.
 */

#define MAX_PEERS_PER_ENTRY 50

typedef struct peer_entry {
    uint8_t infohash[20];
    struct sockaddr_storage peers[MAX_PEERS_PER_ENTRY];
    int peer_count;
} peer_entry_t;

typedef struct tree_peers_queue {
    peer_entry_t *entries;
    int capacity;
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    bool shutdown;
} tree_peers_queue_t;

/**
 * Create a new peers queue
 * @param capacity Maximum number of peer entries to store
 * @return Pointer to queue, or NULL on error
 */
tree_peers_queue_t *tree_peers_queue_create(int capacity);

/**
 * Destroy a peers queue
 * @param q Queue to destroy
 */
void tree_peers_queue_destroy(tree_peers_queue_t *q);

/**
 * Push a peer entry to the queue (blocking if full)
 * @param q Queue
 * @param entry Peer entry to push
 * @return 0 on success, -1 on shutdown or error
 */
int tree_peers_queue_push(tree_peers_queue_t *q, const peer_entry_t *entry);

/**
 * Pop a peer entry from the queue (blocking with timeout)
 * @param q Queue
 * @param entry Buffer to receive peer entry
 * @param timeout_ms Timeout in milliseconds (-1 for infinite)
 * @return 0 on success, -1 on timeout/shutdown/error
 */
int tree_peers_queue_pop(tree_peers_queue_t *q, peer_entry_t *entry, int timeout_ms);

/**
 * Try to push a peer entry without blocking
 * @param q Queue
 * @param entry Peer entry to push
 * @return 0 on success, -1 if full or error
 */
int tree_peers_queue_try_push(tree_peers_queue_t *q, const peer_entry_t *entry);

/**
 * Signal all waiting threads to wake up and exit
 * @param q Queue
 */
void tree_peers_queue_signal_shutdown(tree_peers_queue_t *q);

/**
 * Get current queue count
 * @param q Queue
 * @return Number of items in queue
 */
int tree_peers_queue_count(tree_peers_queue_t *q);

#endif /* TREE_PEERS_QUEUE_H */
