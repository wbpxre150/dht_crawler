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
    struct inflight_set *inflight;   /* Shared in-flight claim set (NULL = dedup disabled) */
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
    char bep51_cache_url[512];           /* Published cache URL; empty = never fetch */
    int bep51_cache_fetch_timeout_sec;   /* Total transfer deadline for that fetch */
    char bep51_cache_ca_file[512];       /* CA bundle override; empty = system store */
    char bep51_cache_ca_dir[512];        /* Hashed CA dir override; empty = none */
    int bep51_cache_submit_percent;      /* % of BEP51 responses to cache */

    /* Stage 3 settings (BEP51) */
    int infohash_queue_capacity;    /* Infohash queue capacity per tree (default: 5000) */
    int bep51_query_interval_ms;    /* BEP51 query interval (default: 10) */
    int bep51_node_cooldown_sec;    /* Cooldown between BEP51 queries to same node (default: 30) */

    /* Stage 4 settings (get_peers) */
    int peers_queue_capacity;       /* Peers queue capacity per tree (default: 2000) */
    int get_peers_timeout_ms;       /* get_peers timeout (default: 1500) */
    int get_peers_deadline_ms;      /* get_peers lookup deadline in ms (default: 6000, 500-8000) */
    int get_peers_fanout;           /* get_peers queries per lookup (default: 8, 2-12) */
    int get_peers_max_queries;      /* get_peers query budget (default: 16, 8-32, >= fanout) */
    int get_peers_round_idle_ms;    /* per-round silence cap (default: 1500, 0 = off, 250-6000) */
    int get_peers_penalize_misses;  /* 1 = charge unanswered round slots against the node (default: 0, 0-1) */
    int get_peers_slot_budget_ms;   /* per-slot answer budget in ms (default: 2000, 0 = off, 500-6000) */
    int get_peers_finish_on_peers;  /* 1 = end a lookup as soon as peers are found (default: 1, 0-1) */
    /* Stage 5 settings */
    int tcp_connect_timeout_ms;
    int parallel_peers;
    int fresh_peer_retries;          /* Fresh-peer re-lookups before failure strike (default: 1, 0-3) */
    int metadata_timeout_ms;         /* Metadata fetch deadline in ms (default: 30000, 5000-600000) */
    int metadata_piece_idle_ms;      /* Piece-progress eviction window in ms (default: 15000, 0 = off) */
    int metadata_peer_blacklist_ttl_sec; /* Per-address TCP connect-failure memory window (default: 0 = off, 60-3600) */

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;          /* Floor for dynamic threshold */
    double dynamic_rate_margin;        /* Margin subtracted from per-tree average */
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int min_lifetime_minutes;
    bool require_empty_queue;
    double rate_ema_alpha;             /* EMA smoothing alpha (0.0-1.0) */

    /* Stage 6 adult handling */
    char adult_mode[8];                  /* "tag" (default) or "drop" at ingest */

    /* "Categories to save" bitmasks (see category_filter_cleanup.h) */
    uint16_t save_category_mask;         /* bit i = tp_category_t i saved; all bits = everything */
    uint16_t save_tv_kind_mask;          /* bit i = tp_tv_kind_t i saved; all bits = everything */

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
    struct inflight_set *inflight;             /* Shared in-flight claim set (NULL = dedup disabled) */
    struct meta_peer_fail_set *meta_peer_fail; /* Shared per-address connect-failure map (NULL = disabled) */
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
    char bep51_cache_url[512];
    int bep51_cache_fetch_timeout_sec;
    char bep51_cache_ca_file[512];
    char bep51_cache_ca_dir[512];
    int bep51_cache_submit_percent;

    /* Stage 3 settings (BEP51) */
    int infohash_queue_capacity;
    int bep51_query_interval_ms;
    int bep51_node_cooldown_sec;

    /* Stage 4 settings (get_peers) */
    int peers_queue_capacity;
    int get_peers_timeout_ms;
    int get_peers_deadline_ms;
    int get_peers_fanout;
    int get_peers_max_queries;
    int get_peers_round_idle_ms;     /* Per-round silence cap (default: 1500, 0 = off) */
    int get_peers_penalize_misses;   /* Charge unanswered round slots against the node (default: 0) */
    int get_peers_slot_budget_ms;    /* Per-slot answer budget (default: 2000, 0 = off) */
    int get_peers_finish_on_peers;   /* End a lookup as soon as peers are found (default: 1) */
    /* Stage 5 settings */
    int tcp_connect_timeout_ms;
    int parallel_peers;
    int fresh_peer_retries;          /* Fresh-peer re-lookups before metadata failure strike (0-3) */
    int metadata_timeout_ms;         /* Metadata fetch deadline in ms (default: 30000, 5000-600000) */
    int metadata_piece_idle_ms;      /* Piece-progress eviction window in ms (default: 15000, 0 = off) */
    int metadata_peer_blacklist_ttl_sec; /* Per-address TCP connect-failure memory window (0 = off) */

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;          /* Floor for dynamic threshold */
    double dynamic_rate_margin;        /* Margin subtracted from per-tree average */
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int min_lifetime_minutes;
    bool require_empty_queue;
    double rate_ema_alpha;

    /* Stage 6 adult handling */
    char adult_mode[8];                  /* "tag" (default) or "drop" at ingest */

    /* "Categories to save" bitmasks, propagated into each tree's config */
    uint16_t save_category_mask;
    uint16_t save_tv_kind_mask;

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
    atomic_uint_fast64_t cumulative_fresh_peer_relookups;
    atomic_uint_fast64_t cumulative_fresh_peer_rescues;
    atomic_uint_fast64_t cumulative_metadata_piece_reasks;
    atomic_uint_fast64_t cumulative_second_strike_failures;
    atomic_uint_fast64_t cumulative_filtered_count;
    atomic_uint_fast64_t cumulative_metadata_attempts;
    /* Metadata connection-failure attribution accumulated from respawned trees */
    atomic_uint_fast64_t cumulative_metadata_fail_connect;
    atomic_uint_fast64_t cumulative_metadata_fail_handshake;
    atomic_uint_fast64_t cumulative_metadata_fail_stall;
    atomic_uint_fast64_t cumulative_metadata_fail_ut_reject;
    atomic_uint_fast64_t cumulative_metadata_fail_size;
    atomic_uint_fast64_t cumulative_metadata_fail_hash;
    atomic_uint_fast64_t cumulative_metadata_fail_deadline;
    atomic_uint_fast64_t cumulative_metadata_fail_other;
    /* get_peers lookup instrumentation accumulated from respawned trees */
    atomic_uint_fast64_t cumulative_gp_lookups;
    atomic_uint_fast64_t cumulative_gp_lookups_with_peers;
    atomic_uint_fast64_t cumulative_gp_lookups_from_retry;
    atomic_uint_fast64_t cumulative_gp_packets_sent;
    atomic_uint_fast64_t cumulative_gp_answers;
    atomic_uint_fast64_t cumulative_gp_lookup_us_total;
    atomic_uint_fast64_t cumulative_gp_rounds_total;
    /* get_peers lookup exit reasons and silent-node penalties */
    atomic_uint_fast64_t cumulative_gp_idle_exits;
    atomic_uint_fast64_t cumulative_gp_deadline_exits;
    atomic_uint_fast64_t cumulative_gp_unanswered_slots;
    atomic_uint_fast64_t cumulative_gp_miss_penalties;
    /* get_peers round-completion counters (rev2) */
    atomic_uint_fast64_t cumulative_gp_slot_expired_total;
    atomic_uint_fast64_t cumulative_gp_early_finish_total;
    /* BEP51 query + dedup-funnel instrumentation accumulated from respawned
     * trees (patch 23). The per-tree half lives on thread_tree_t. */
    atomic_uint_fast64_t cumulative_bep51_queries_sent;
    atomic_uint_fast64_t cumulative_bep51_responses_received;
    atomic_uint_fast64_t cumulative_bep51_timeouts;
    atomic_uint_fast64_t cumulative_bep51_samples_offered;
    atomic_uint_fast64_t cumulative_bep51_bloom_rejected;
    atomic_uint_fast64_t cumulative_bep51_inflight_refused;
    atomic_uint_fast64_t cumulative_bep51_queue_full;
    atomic_uint_fast64_t cumulative_bep51_samples_pushed;

    /* Last-interval ("windowed") rate publishing (patch 20). Every counter
     * above is cumulative since process start, so one /stats snapshot cannot
     * say how fast anything is happening right now. The monitor thread samples
     * the same cumulative+live folds every 10 s (src/stats_window.c) and stores
     * the delta since its previous tick here; /stats publishes them beside the
     * lifetime totals as *_last_interval. All zeros until the second tick.
     * Atomic because the civetweb worker threads read them while the monitor
     * thread writes them (unlike the non-atomic per-tree metadata_rate). */
    atomic_uint_fast64_t interval_metadata_attempts;
    atomic_uint_fast64_t interval_metadata_fetched;
    atomic_uint_fast64_t interval_gp_lookups;
    atomic_uint_fast64_t interval_gp_with_peers;
    atomic_int interval_elapsed_sec;   /* seconds the last completed interval covered */

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
                     uint64_t *out_first_strike, uint64_t *out_second_strike,
                     uint64_t *out_fresh_relookups, uint64_t *out_fresh_rescues,
                     uint64_t *out_metadata_piece_reasks);

/**
 * Get aggregate get_peers lookup statistics (cumulative from respawned trees
 * plus all live trees)
 * @param sup Supervisor instance
 * @param out_lookups Pointer to store total tree_lookup_peers() calls
 * @param out_with_peers Pointer to store lookups that returned peers
 * @param out_from_retry Pointer to store lookups started by the retry path
 * @param out_packets Pointer to store get_peers packets attempted
 * @param out_answers Pointer to store responses consumed
 * @param out_lookup_us Pointer to store total microseconds inside lookups
 * @param out_rounds Pointer to store completed get_peers rounds after the first
 */
void supervisor_getpeers_stats(supervisor_t *sup, uint64_t *out_lookups,
                               uint64_t *out_with_peers, uint64_t *out_from_retry,
                               uint64_t *out_packets, uint64_t *out_answers,
                               uint64_t *out_lookup_us, uint64_t *out_rounds);

/**
 * Get the get_peers exit-reason and silent-node counters (cumulative respawned
 * trees + live trees): idle exits, deadline exits, unanswered round slots and
 * miss penalties, plus the rev2 round-completion counters (slots that ran out
 * their per-slot answer budget, and lookups finished early on peers). Any out
 * pointer may be NULL.
 */
void supervisor_getpeers_exit_stats(supervisor_t *sup, uint64_t *out_idle_exits,
                                    uint64_t *out_deadline_exits,
                                    uint64_t *out_unanswered_slots,
                                    uint64_t *out_miss_penalties,
                                    uint64_t *out_slot_expired,
                                    uint64_t *out_early_finish);

/**
 * Get the BEP51 query and dedup-funnel counters (cumulative respawned trees +
 * live trees): sample_infohashes queries sent, answers received, timeouts, and
 * the four-gate split of the infohashes a response offered (bloom-rejected,
 * in-flight-refused, queue-full, pushed). Per window the offered total equals
 * the sum of the four gates. Any out pointer may be NULL.
 */
void supervisor_bep51_stats(supervisor_t *sup,
                            uint64_t *out_queries_sent,
                            uint64_t *out_responses_received,
                            uint64_t *out_timeouts,
                            uint64_t *out_samples_offered,
                            uint64_t *out_bloom_rejected,
                            uint64_t *out_inflight_refused,
                            uint64_t *out_queue_full,
                            uint64_t *out_samples_pushed);

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
