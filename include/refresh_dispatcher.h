#ifndef REFRESH_DISPATCHER_H
#define REFRESH_DISPATCHER_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/socket.h>

/* Forward declarations */
typedef struct tree_socket tree_socket_t;
typedef struct tree_response_queue tree_response_queue_t;

/**
 * Standalone UDP dispatcher for refresh thread
 *
 * This is a clean, independent implementation that does NOT depend on
 * thread_tree structures. It receives UDP packets and routes them to
 * worker queues based on transaction ID.
 */

#define REFRESH_MAX_TID_SIZE 4
#define REFRESH_MAX_RESPONSE_SIZE 2048

/* Forward declare the TID registration type (uthash needs this) */
typedef struct refresh_tid_registration refresh_tid_registration_t;

/**
 * Refresh dispatcher structure
 */
typedef struct refresh_dispatcher {
    /* Socket to receive from */
    tree_socket_t *socket;

    /* TID registration map */
    refresh_tid_registration_t *tid_map_head;  /* uthash hash table */
    pthread_rwlock_t tid_map_lock;

    /* Dispatcher thread */
    pthread_t dispatcher_thread;
    atomic_bool running;

    /* Statistics */
    atomic_ulong total_responses;
    atomic_ulong routed_responses;
    atomic_ulong dropped_responses;
} refresh_dispatcher_t;

/**
 * Create a refresh dispatcher
 * @param socket Socket to receive UDP packets from
 * @return New dispatcher, or NULL on error
 */
refresh_dispatcher_t *refresh_dispatcher_create(tree_socket_t *socket);

/**
 * Start the dispatcher thread
 * @param dispatcher Dispatcher
 * @return 0 on success, -1 on error
 */
int refresh_dispatcher_start(refresh_dispatcher_t *dispatcher);

/**
 * Stop the dispatcher thread
 * @param dispatcher Dispatcher
 */
void refresh_dispatcher_stop(refresh_dispatcher_t *dispatcher);

/**
 * Destroy the dispatcher and free resources
 * @param dispatcher Dispatcher
 */
void refresh_dispatcher_destroy(refresh_dispatcher_t *dispatcher);

/**
 * Register a transaction ID to route responses to a queue
 * @param dispatcher Dispatcher
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param queue Queue to route responses to
 * @return 0 on success, -1 on error
 */
int refresh_dispatcher_register_tid(refresh_dispatcher_t *dispatcher,
                                    const uint8_t *tid, int tid_len,
                                    tree_response_queue_t *queue);

/**
 * Unregister a transaction ID
 * @param dispatcher Dispatcher
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 */
void refresh_dispatcher_unregister_tid(refresh_dispatcher_t *dispatcher,
                                       const uint8_t *tid, int tid_len);

#endif /* REFRESH_DISPATCHER_H */
