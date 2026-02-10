#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include "thread_tree.h"

/* Forward declarations */
struct batch_writer;
struct bloom_filter;
struct shared_node_pool;
struct tree_socket;
struct tree_dispatcher;

/* Per-partition performance tracking for adaptive keyspace */
typedef struct partition_stats {
    uint64_t total_metadata;           /* Cumulative metadata from trees in this partition */
    int consecutive_zero_respawns;     /* Consecutive respawns with zero metadata */
    int current_tree_count;            /* Active trees currently in this partition */
} partition_stats_t;

/* Draining tree tracking */
typedef struct draining_tree {
    thread_tree_t *tree;           /* Pointer to draining tree */
    time_t drain_start_time;       /* When tree entered draining state */
    int original_slot;             /* Original slot index in active trees array */
} draining_tree_t;

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
    int dht_port;                   /* DHT UDP port (0 = ephemeral, otherwise shared with SO_REUSEPORT) */

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

    /* BEP51 cache settings */
    char bep51_cache_path[512];          /* Cache file path */
    int bep51_cache_capacity;            /* Max nodes in cache */
    int bep51_cache_submit_percent;      /* % of BEP51 responses to cache */

    /* Stage 3 settings (BEP51) */
    int infohash_queue_capacity;    /* Infohash queue capacity per tree (default: 5000) */
    int bep51_query_interval_ms;    /* BEP51 query interval (default: 10) */
    int bep51_node_cooldown_sec;    /* Cooldown between BEP51 queries to same node (default: 30) */

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

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;          /* Min metadata rate before respawn (default: 0.01) */
    int rate_check_interval_sec;       /* Rate check interval (default: 60) */
    int rate_grace_period_sec;         /* Grace period before respawn (default: 30) */
    int min_lifetime_minutes;          /* Min lifetime before rate checks (default: 10) */
    int require_empty_queue;           /* Only respawn if queue empty (default: 1) */

    /* Respawn overlapping configuration */
    int respawn_spawn_threshold;       /* Spawn replacement when connections drop below this */
    int respawn_drain_timeout_sec;     /* Force destroy draining tree after this timeout */
    int max_draining_trees;            /* Maximum trees allowed in draining state */

    /* Porn filter settings */
    int porn_filter_enabled;           /* Enable porn filter (0=disabled, 1=enabled) */

    /* Adaptive keyspace partitioning settings */
    int dead_partition_threshold;       /* Consecutive zero-metadata respawns before migration (default: 3) */
    int max_trees_per_partition;        /* Max trees allowed in one partition (default: 4) */

    /* Shared resources */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;

    /* Bloom filter settings for failure tracking */
    uint64_t failure_bloom_capacity;     /* Capacity for failure bloom filter */
    double bloom_error_rate;             /* Error rate for bloom filters */
} supervisor_config_t;

/* Supervisor structure */
typedef struct supervisor {
    thread_tree_t **trees;          /* Array of tree pointers */
    int max_trees;
    int active_trees;
    pthread_mutex_t trees_lock;

    /* Keyspace partitioning */
    bool use_keyspace_partitioning; /* Enable keyspace partitioning */
    int dht_port;                   /* DHT UDP port for all trees */

    /* Shared resources (only these are shared between trees) */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    struct bloom_filter *failure_bloom;        /* NEW: Failure bloom filter for two-strike filtering */
    const char *failure_bloom_path;            /* NEW: Persistence path for failure bloom */
    struct shared_node_pool *shared_node_pool;  /* Shared bootstrap node pool */
    struct bep51_cache *bep51_cache;            /* BEP51 node cache for persistent bootstrap */

    /* Shared socket and dispatcher for all trees (when using fixed port) */
    struct tree_socket *shared_socket;
    struct tree_dispatcher *shared_dispatcher;

    /* Bloom filter configuration */
    uint64_t failure_bloom_capacity;           /* Failure bloom filter capacity */
    double bloom_error_rate;                   /* Error rate for bloom filters */

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

    /* BEP51 cache settings */
    char bep51_cache_path[512];
    int bep51_cache_capacity;
    int bep51_cache_submit_percent;

    /* Stage 3 settings (BEP51) */
    int infohash_queue_capacity;
    int bep51_query_interval_ms;
    int bep51_node_cooldown_sec;

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

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int min_lifetime_minutes;
    int require_empty_queue;

    /* Porn filter settings */
    int porn_filter_enabled;           /* Enable porn filter */

    /* Tree ID counter */
    uint32_t next_tree_id;

    /* Monitor thread */
    pthread_t monitor_thread;
    int monitor_running;

    /* Cumulative statistics (persist across tree respawns) */
    atomic_uint_least64_t cumulative_metadata_count;
    atomic_uint_least64_t cumulative_first_strike_failures;
    atomic_uint_least64_t cumulative_second_strike_failures;
    atomic_uint_least64_t cumulative_filtered_count;

    /* Draining trees management */
    draining_tree_t *draining_trees;   /* Array of trees in draining state */
    int draining_count;                 /* Current number of draining trees */
    int max_draining_trees;             /* Maximum allowed draining trees (from config) */
    pthread_mutex_t draining_lock;      /* Protects draining_trees array */

    /* Respawn configuration */
    int respawn_spawn_threshold;        /* Spawn replacement at this connection count */
    int respawn_drain_timeout_sec;      /* Force destroy after this timeout */

    /* Adaptive keyspace partitioning */
    partition_stats_t *partition_stats;  /* Array of size max_trees (one per partition) */
    int dead_partition_threshold;        /* Consecutive zero-metadata respawns before migration */
    int max_trees_per_partition;         /* Max trees allowed in one partition */
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
 * @param out_first_strike Output: total first-strike failures (optional, can be NULL)
 * @param out_second_strike Output: total second-strike failures (optional, can be NULL)
 */
void supervisor_stats(supervisor_t *sup, int *out_active_trees, uint64_t *out_total_metadata,
                     uint64_t *out_first_strike, uint64_t *out_second_strike);

/**
 * Get total active connections across all trees
 * @param sup Supervisor instance
 * @return Total active connections
 */
int supervisor_get_total_connections(supervisor_t *sup);

/**
 * Get draining tree statistics (for debugging/monitoring)
 * @param sup Supervisor instance
 * @param count Output: current number of draining trees
 * @param max_count Output: maximum draining trees allowed
 * @param total_connections Output: total connections across draining trees
 */
void supervisor_get_draining_stats(supervisor_t *sup, int *count, int *max_count, int *total_connections);

#endif /* SUPERVISOR_H */
