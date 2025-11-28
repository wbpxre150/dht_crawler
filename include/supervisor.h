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
    bool use_keyspace_partitioning; /* Enable keyspace partitioning (default: true) */

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

    /* Stage 3 settings (BEP51) */
    int infohash_queue_capacity;    /* Infohash queue capacity per tree (default: 5000) */
    int bep51_query_interval_ms;    /* BEP51 query interval (default: 10) */

    /* Stage 4 settings (get_peers) */
    int peers_queue_capacity;       /* Peers queue capacity per tree (default: 2000) */
    int get_peers_timeout_ms;       /* get_peers timeout (default: 3000) */

    /* Find_node throttling settings */
    int infohash_pause_threshold;   /* Queue size to pause find_node (default: 2000) */
    int infohash_resume_threshold;  /* Queue size to resume find_node (default: 1000) */

    /* Get_peers throttling settings */
    int peers_pause_threshold;      /* Peers queue size to pause get_peers (default: 2000) */
    int peers_resume_threshold;     /* Peers queue size to resume get_peers (default: 1000) */

    /* Stage 5 settings */
    int tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */

    /* Bloom-based respawn settings */
    double max_bloom_duplicate_rate;   /* Max bloom duplicate rate before respawn (default: 0.70) */
    int bloom_check_interval_sec;      /* Bloom rate check interval (default: 60) */
    int bloom_check_sample_size;       /* Min samples before check (default: 100) */
    int bloom_grace_period_sec;        /* Grace period before respawn (default: 120) */
    int bloom_min_lifetime_minutes;    /* Min lifetime before bloom checks (default: 10) */

    /* Porn filter settings */
    int porn_filter_enabled;           /* Enable porn filter (0=disabled, 1=enabled) */

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

    /* Keyspace partitioning */
    bool use_keyspace_partitioning; /* Enable keyspace partitioning */

    /* Shared resources (only these are shared between trees) */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    struct shared_node_pool *shared_node_pool;  /* NEW: Shared bootstrap node pool */

    /* Configuration */
    int num_find_node_workers;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Stage 2 settings (Global Bootstrap - NEW) */
    int global_bootstrap_target;
    int global_bootstrap_timeout_sec;
    int global_bootstrap_workers;
    int per_tree_sample_size;

    /* Stage 3 settings (BEP51) */
    int infohash_queue_capacity;
    int bep51_query_interval_ms;

    /* Stage 4 settings (get_peers) */
    int peers_queue_capacity;
    int get_peers_timeout_ms;

    /* Find_node throttling settings */
    int infohash_pause_threshold;
    int infohash_resume_threshold;

    /* Get_peers throttling settings */
    int peers_pause_threshold;
    int peers_resume_threshold;

    /* Stage 5 settings */
    int tcp_connect_timeout_ms;

    /* Bloom-based respawn settings */
    double max_bloom_duplicate_rate;
    int bloom_check_interval_sec;
    int bloom_check_sample_size;
    int bloom_grace_period_sec;
    int bloom_min_lifetime_minutes;

    /* Porn filter settings */
    int porn_filter_enabled;           /* Enable porn filter */

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

/**
 * Get total active connections across all trees
 * @param sup Supervisor instance
 * @return Total active connections
 */
int supervisor_get_total_connections(supervisor_t *sup);

#endif /* SUPERVISOR_H */
