#ifndef TREE_DISPATCHER_H
#define TREE_DISPATCHER_H

#include "tree_response_queue.h"
#include "tree_socket.h"
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

/**
 * UDP Response Dispatcher
 *
 * Centralized UDP receiver thread that routes incoming responses to worker
 * response queues based on transaction ID (TID). This eliminates the response
 * stealing problem where multiple workers share a single socket.
 *
 * Design:
 * - One dispatcher thread per tree reads all UDP packets
 * - Workers register their response queue with a TID
 * - Dispatcher extracts TID from response and routes to correct queue
 * - TID registrations have timeouts and are automatically cleaned up
 */

/* Forward declaration */
struct thread_tree;

/* Maximum TID size (4 bytes for 32-bit counter) */
#define MAX_TID_SIZE 4

/* TID registration entry */
typedef struct tid_registration {
    uint8_t tid[MAX_TID_SIZE];
    int tid_len;
    tree_response_queue_t *response_queue;
    time_t registered_at;
    struct tid_registration *next;  /* For hash table chaining */
} tid_registration_t;

/* Dispatcher structure */
typedef struct tree_dispatcher {
    struct thread_tree *tree;
    tree_socket_t *socket;

    /* TID → response queue mapping (simple hash table) */
    tid_registration_t **tid_map;
    int tid_map_capacity;
    pthread_rwlock_t tid_map_lock;

    /* Dispatcher thread */
    pthread_t dispatcher_thread;
    atomic_bool running;

    /* Statistics */
    atomic_uint_fast64_t total_responses;
    atomic_uint_fast64_t routed_responses;
    atomic_uint_fast64_t dropped_responses;  /* No matching TID */
    atomic_uint_fast64_t queue_full_drops;
} tree_dispatcher_t;

/**
 * Create a new dispatcher for a tree
 * @param tree Thread tree that owns this dispatcher
 * @param socket UDP socket to receive from
 * @return Pointer to dispatcher, or NULL on error
 */
tree_dispatcher_t *tree_dispatcher_create(struct thread_tree *tree, tree_socket_t *socket);

/**
 * Start the dispatcher thread
 * @param dispatcher Dispatcher instance
 * @return 0 on success, -1 on error
 */
int tree_dispatcher_start(tree_dispatcher_t *dispatcher);

/**
 * Stop the dispatcher thread
 * @param dispatcher Dispatcher instance
 */
void tree_dispatcher_stop(tree_dispatcher_t *dispatcher);

/**
 * Destroy a dispatcher (stops thread, frees memory)
 * @param dispatcher Dispatcher to destroy
 */
void tree_dispatcher_destroy(tree_dispatcher_t *dispatcher);

/**
 * Register a TID → response queue mapping
 * @param dispatcher Dispatcher instance
 * @param tid Transaction ID bytes
 * @param tid_len Length of TID
 * @param response_queue Queue to route responses to
 * @return 0 on success, -1 on error
 */
int tree_dispatcher_register_tid(tree_dispatcher_t *dispatcher,
                                  const uint8_t *tid, int tid_len,
                                  tree_response_queue_t *response_queue);

/**
 * Unregister a TID mapping
 * @param dispatcher Dispatcher instance
 * @param tid Transaction ID bytes
 * @param tid_len Length of TID
 */
void tree_dispatcher_unregister_tid(tree_dispatcher_t *dispatcher,
                                    const uint8_t *tid, int tid_len);

/**
 * Cleanup stale TID registrations (older than timeout)
 * @param dispatcher Dispatcher instance
 * @param timeout_sec Age in seconds after which to remove registration
 * @return Number of registrations removed
 */
int tree_dispatcher_cleanup_stale_tids(tree_dispatcher_t *dispatcher, int timeout_sec);

#endif /* TREE_DISPATCHER_H */
