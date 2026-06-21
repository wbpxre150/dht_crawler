#ifndef SUPERVISOR_H
#define SUPERVISOR_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>
#include "thread_tree.h"

/* Forward declarations */
struct batch_writer;
struct bloom_filter;
struct tree_socket;
struct tree_dispatcher;

/* Per-partition performance tracking for adaptive keyspace */
typedef struct partition_stats {
    int current_tree_count;           /* Active trees in this partition */
    int dead_consecutive;             /* Consecutive zero-rate respawns */
    int total_respawns;               /* Total respawns for this partition */
    double metadata_rate;             /* Metadata rate (metadata/sec) */
} partition_stats_t;

/* Draining tree tracking */
typedef struct draining_tree {
    thread_tree_t *tree;              /* Tree being drained */
    int slot_index;                   /* Original slot */
    int original_slot;                /* Original slot before migration */
    uint32_t partition_index;         /* Partition this tree belongs to */
    time_t drain_start;               /* When drain started */
    bool migrated;                    /* True if migrated from home */
} draining_tree_t;

/**
 * Supervisor: Manages multiple thread trees for DHT crawling
 *
 * Responsibilities:
 * - Maintain a pool of thread trees across max_trees slots
 * - Monitor tree performance (metadata rate) and trigger respawns
 * - Handle tree crashes and respawn with perturbed node IDs
 * - Keyspace partitioning for even coverage
 * - Draining trees during respawn to avoid connection drops
 */

/* Configuration for supervisor */
typedef struct supervisor_config {
    int max_trees;                  /* Maximum number of concurrent trees */
    bool use_keyspace_partitioning; /* Enable keyspace partitioning (default: true) */
    int dht_port;                   /* DHT UDP port (0 = ephemeral, otherwise shared with SO_REUSEPORT) */

    /* Shared resources */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    /* Worker counts per tree */
    int num_find_node_workers;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;
    /* Bloom filter configuration */
    uint64_t failure_bloom_capacity;
    int failure_strike_count;            /* Failures before permanent block (1 or 2, default: 2) */
    double bloom_error_rate;

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
    int tcp_connect_timeout_ms;
    int parallel_peers;

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;          /* Floor for dynamic threshold */
    double dynamic_rate_margin;        /* Margin subtracted from per-tree average */
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int min_lifetime_minutes;
    bool require_empty_queue;
    double rate_ema_alpha;             /* EMA smoothing alpha (0.0-1.0) */

    /* Porn filter settings */
    bool porn_filter_enabled;

    /* Tree bootstrap settings */

    /* Keyspace partitioning */
    int dead_partition_threshold;        /* Consecutive zero-metadata respawns before migration */
    int max_trees_per_partition;         /* Max trees per partition */

    /* Supervisor-level global bootstrap */
    int global_bootstrap_target;         /* Target node count for global bootstrap */
    int global_bootstrap_timeout_sec;    /* Timeout for global bootstrap */
    int global_bootstrap_workers;        /* Worker count for global bootstrap */

    /* Respawn overlapping configuration */
    int respawn_spawn_threshold;         /* Spawn replacement when connections below threshold */
    int respawn_drain_timeout_sec;       /* Force destroy after drain timeout */
    int max_draining_trees;              /* Maximum draining trees */
} supervisor_config_t;

/* Supervisor structure */
typedef struct supervisor {
    int max_trees;                  /* Maximum concurrent trees */
    int active_trees;               /* Currently active trees */
    uint32_t next_tree_id;          /* Monotonic counter for tree IDs */
    atomic_int monitor_running;     /* Flag for monitor thread loop */

    /* Tree array (indexed by slot) */
    thread_tree_t **trees;          /* Array of thread_tree pointers */
    pthread_mutex_t trees_lock;     /* Protects trees array access */

    /* Keyspace partitioning */
    bool use_keyspace_partitioning; /* Enable keyspace partitioning */
    int dht_port;                   /* DHT UDP port for all trees */

    /* Shared resources (only these are shared between trees) */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    struct bloom_filter *failure_bloom;        /* NEW: Failure bloom filter for two-strike filtering */
    const char *failure_bloom_path;            /* NEW: Persistence path for failure bloom */
    struct bep51_cache *bep51_cache;            /* BEP51 node cache for persistent bootstrap */

    /* Shared socket and dispatcher for all trees (when using fixed port) */
    struct tree_socket *shared_socket;
    struct tree_dispatcher *shared_dispatcher;

    /* Bloom filter configuration */
    uint64_t failure_bloom_capacity;           /* Failure bloom filter capacity */
    int failure_strike_count;                  /* Failures before permanent block (1 or 2, default: 2) */
    double bloom_error_rate;                   /* Error rate for bloom filters */

    /* Configuration */
    int num_find_node_workers;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

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
    int parallel_peers;

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;          /* Floor for dynamic threshold */
    double dynamic_rate_margin;        /* Margin subtracted from per-tree average */
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int min_lifetime_minutes;
    bool require_empty_queue;
    double rate_ema_alpha;

    /* Porn filter settings */
    bool porn_filter_enabled;

    /* Tree bootstrap settings */

    /* Keyspace partitioning */
    partition_stats_t *partition_stats;  /* Per-partition stats */
    uint32_t *home_partitions;           /* Home partition for each slot */
    int dead_partition_threshold;
    int max_trees_per_partition;

    /* Respawn overlapping configuration */
    int respawn_spawn_threshold;
    int respawn_drain_timeout_sec;
    int max_draining_trees;

    /* Supervisor-level global bootstrap config */
    int global_bootstrap_target;         /* Target node count for global bootstrap */
    int global_bootstrap_timeout_sec;    /* Timeout for global bootstrap */
    int global_bootstrap_workers;        /* Worker count for global bootstrap */

    /* Supervisor bootstrap ephemeral state (torn down after bootstrap) */
    void *bootstrap_routing_table;       /* tree_routing_table_t* */
    void *bootstrap_socket;              /* tree_socket_t* */
    void *bootstrap_dispatcher;          /* tree_dispatcher_t* */
    pthread_t *bootstrap_workers;        /* worker thread handles */
    int bootstrap_worker_count;

    /* Draining trees */
    draining_tree_t *draining_trees;
    int draining_count;
    pthread_mutex_t draining_lock;

    /* Rate-based respawn tracking */
    time_t *rate_below_since;    /* When rate dropped below threshold per slot */

    /* Cumulative statistics (persisted across tree respawns) */
    atomic_uint_fast64_t cumulative_metadata_count;
    atomic_uint_fast64_t cumulative_first_strike_failures;
    atomic_uint_fast64_t cumulative_second_strike_failures;
    atomic_uint_fast64_t cumulative_filtered_count;
    atomic_uint_fast64_t cumulative_metadata_attempts;

    /* Monitor thread */
    pthread_t monitor_thread;

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
 * @param out_active_trees Pointer to store active tree count
 * @param out_total_metadata Pointer to store cumulative metadata count
 * @param out_first_strike Pointer to store cumulative first strike failures
 * @param out_second_strike Pointer to store cumulative second strike failures
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
 * @param out_count Number of currently draining trees
 * @param out_max_count Maximum draining trees allowed
 * @param out_total_connections Total connections across draining trees
 */
void supervisor_get_draining_stats(supervisor_t *sup, int *count, int *max_count, int *total_connections);

#endif /* SUPERVISOR_H */
