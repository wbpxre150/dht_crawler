#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include <stdint.h>
#include <pthread.h>
#include "thread_tree.h"

/* Forward declarations */
struct batch_writer;
struct bloom_filter;

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
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Stage 2 settings (Bootstrap) */
    int bootstrap_timeout_sec;      /* Bootstrap phase timeout (default: 30) */
    int routing_threshold;          /* Nodes required before BEP51 phase (default: 500) */

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

    /* Configuration */
    double min_metadata_rate;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Stage 2 settings (Bootstrap) */
    int bootstrap_timeout_sec;
    int routing_threshold;

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
