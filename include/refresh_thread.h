#ifndef REFRESH_THREAD_H
#define REFRESH_THREAD_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>
#include "refresh_query.h"

/* Forward declarations */
typedef struct shared_node_pool shared_node_pool_t;
typedef struct tree_routing_table tree_routing_table_t;
typedef struct tree_socket tree_socket_t;
typedef struct tree_dispatcher tree_dispatcher_t;
typedef struct refresh_request_queue refresh_request_queue_t;

typedef struct refresh_thread_config {
    int bootstrap_sample_size;        /* Default: 1000 */
    int routing_table_target;         /* Default: 500 */
    int ping_worker_count;            /* Default: 1 */
    int find_node_worker_count;       /* Default: 1 */
    int get_peers_worker_count;       /* Default: 1 */
    int request_queue_capacity;       /* Default: 100 */
    int get_peers_timeout_ms;         /* Default: 500 */
    int max_iterations;               /* Default: 3 */
} refresh_thread_config_t;

typedef struct refresh_thread {
    /* Configuration */
    refresh_thread_config_t config;

    /* Identity */
    uint8_t node_id[20];

    /* Core components */
    tree_routing_table_t *routing_table;
    tree_socket_t *socket;
    tree_dispatcher_t *dispatcher;
    refresh_request_queue_t *request_queue;

    /* External references (not owned) */
    shared_node_pool_t *shared_node_pool;
    refresh_query_store_t *refresh_query_store;

    /* Worker threads */
    pthread_t *ping_workers;
    pthread_t *find_node_workers;
    pthread_t *get_peers_workers;
    pthread_t bootstrap_thread;

    /* Synchronization */
    atomic_bool shutdown_requested;
    atomic_bool initialized;
    pthread_mutex_t lock;
} refresh_thread_t;

/**
 * Create a new refresh thread
 * @param config Configuration
 * @param shared_pool Shared node pool for bootstrap
 * @param query_store Refresh query store for HTTP coordination
 * @return New refresh thread, or NULL on error
 */
refresh_thread_t *refresh_thread_create(const refresh_thread_config_t *config,
                                       shared_node_pool_t *shared_pool,
                                       refresh_query_store_t *query_store);

/**
 * Start the refresh thread (spawns workers)
 * @param thread Refresh thread
 * @return 0 on success, -1 on error
 */
int refresh_thread_start(refresh_thread_t *thread);

/**
 * Request shutdown of the refresh thread
 * @param thread Refresh thread
 */
void refresh_thread_request_shutdown(refresh_thread_t *thread);

/**
 * Destroy the refresh thread (joins all threads, frees resources)
 * @param thread Refresh thread
 */
void refresh_thread_destroy(refresh_thread_t *thread);

/**
 * Submit a refresh request to the thread
 * @param thread Refresh thread
 * @param infohash 20-byte infohash
 * @return 0 on success, -1 on error (queue full)
 */
int refresh_thread_submit_request(refresh_thread_t *thread,
                                  const uint8_t *infohash);

#endif /* REFRESH_THREAD_H */
