#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include <stdint.h>
#include <pthread.h>
#include "thread_tree.h"

/* Forward declarations */
struct batch_writer;
struct bloom_filter;
struct shared_node_pool;

/**
 * Supervisor: Manages multiple thread trees for DHT crawling
 *
 * The supervisor:
 * - Maintains an array of thread trees
 * - Monitors tree performance (metadata rate)
 * - Replaces underperforming trees
 * - Provides shared resources (batch_writer, bloom_filter) to all trees
 */

/* Configuration for supervisor */
typedef struct supervisor_config {
    int max_trees;                  /* Maximum number of concurrent trees */
    double min_metadata_rate;       /* Minimum metadata/sec before tree restart */

    /* Worker counts per tree */
    int num_find_node_workers;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Stage 2 settings (Global Bootstrap - NEW) */
    int global_bootstrap_target;         /* Target nodes for shared pool (default: 5000) */
    int global_bootstrap_timeout_sec;    /* Global bootstrap timeout (default: 60) */
    int global_bootstrap_workers;        /* Bootstrap worker threads (default: 50) */
    int per_tree_sample_size;            /* Nodes each tree samples from pool (default: 1000) */

    /* Stage 5 settings */
    int rate_check_interval_sec;    /* Rate check interval (default: 10) */
    int rate_grace_period_sec;      /* Grace period before shutdown (default: 30) */
    int tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */

    /* Shared resources */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
} supervisor_config_t;

/* Supervisor structure */
typedef struct supervisor {
    thread_tree_t **trees;          /* Array of tree pointers */
    int max_trees;
    int active_trees;
    pthread_mutex_t trees_lock;

    /* Shared resources (only these are shared between trees) */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    struct shared_node_pool *shared_node_pool;  /* NEW: Shared bootstrap node pool */

    /* Configuration */
    double min_metadata_rate;
    int num_find_node_workers;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Stage 2 settings (Global Bootstrap - NEW) */
    int global_bootstrap_target;
    int global_bootstrap_timeout_sec;
    int global_bootstrap_workers;
    int per_tree_sample_size;

    /* Stage 5 settings */
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int tcp_connect_timeout_ms;

    /* Tree ID counter */
    uint32_t next_tree_id;

    /* Monitor thread */
    pthread_t monitor_thread;
    int monitor_running;
} supervisor_t;

/**
 * Create supervisor
 * @param config Supervisor configuration
 * @return Pointer to supervisor, or NULL on error
 */
supervisor_t *supervisor_create(supervisor_config_t *config);

/**
 * Start supervisor (spawns all trees and monitor thread)
 * @param sup Supervisor instance
 */
void supervisor_start(supervisor_t *sup);

/**
 * Stop supervisor (shuts down all trees)
 * @param sup Supervisor instance
 */
void supervisor_stop(supervisor_t *sup);

/**
 * Destroy supervisor (cleanup and free)
 * @param sup Supervisor instance
 */
void supervisor_destroy(supervisor_t *sup);

/**
 * Callback when a tree shuts down
 * @param tree Thread tree that shut down
 */
void supervisor_on_tree_shutdown(thread_tree_t *tree);

/**
 * Get supervisor statistics
 * @param sup Supervisor instance
 * @param out_active_trees Output: number of active trees
 * @param out_total_metadata Output: total metadata fetched across all trees
 */
void supervisor_stats(supervisor_t *sup, int *out_active_trees, uint64_t *out_total_metadata);

#endif /* SUPERVISOR_H */
