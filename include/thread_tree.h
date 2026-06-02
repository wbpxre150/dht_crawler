#ifndef THREAD_TREE_H
#define THREAD_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>

/* Forward declarations */
struct batch_writer;
struct bloom_filter;
struct thread_tree;  /* Forward declare for callback typedef */
struct tree_infohash_queue;
struct tree_peers_queue;
struct tree_dispatcher;

/**
 * Thread Tree: Isolated DHT crawler unit with private state
 * Each tree operates independently with its own:
 * - Node ID (keyspace partitioned or random)
 * - Routing table
 * - UDP socket/dispatcher (or shared with supervisor)
 * - Infohash queue, peers queue
 *
 * Trees are managed by a supervisor that monitors performance
 * and respawns underperforming trees.
 */

/* Phase of the thread tree lifecycle */
typedef enum {
    TREE_PHASE_BOOTSTRAP,       /* Gathering initial nodes */
    TREE_PHASE_BEP51,           /* Active BEP51 discovery */
    TREE_PHASE_GET_PEERS,       /* Active get_peers discovery */
    TREE_PHASE_METADATA,        /* Active metadata fetching */
    TREE_PHASE_SHUTTING_DOWN    /* Graceful shutdown in progress */
} tree_phase_t;

/* Reason for shutdown */
typedef enum {
    SHUTDOWN_REASON_NONE,
    SHUTDOWN_REASON_RATE_BASED,
    SHUTDOWN_REASON_SUPERVISOR
} shutdown_reason_t;

/* Forward declarations for shared resources */
struct tree_socket;
struct tree_dispatcher;

/* Configuration for a thread tree */
typedef struct tree_config {
    /* Keyspace partitioning */
    uint32_t partition_index;       /* Keyspace partition for this tree */
    uint32_t num_partitions;        /* Total partitions in the system */
    bool use_keyspace_partitioning; /* Enable keyspace partitioning (vs random node ID) */
    int dht_port;                   /* DHT UDP port (0 = ephemeral) */
    int tree_bootstrap_timeout_sec; /* Tree-native bootstrap deadline (default: 30) */

    /* Shared socket/dispatcher from supervisor (NULL = create private) */
    struct tree_socket *shared_socket;
    struct tree_dispatcher *shared_dispatcher;

    int num_find_node_workers;      /* Continuous find_node workers (default: 30) */
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    int find_node_target_nodes;       /* Routing target — sourced from supervisor's bep51_cache_capacity */

    /* Stage 3: BEP51 settings */
    int infohash_queue_capacity;    /* Infohash queue size (default: 5000) */
    int bep51_query_interval_ms;    /* Delay between BEP51 queries (default: 10) */
    int bep51_node_cooldown_sec;    /* Cooldown between BEP51 queries to same node (default: 30) */

    /* Stage 4: Get_peers settings */
    int peers_queue_capacity;       /* Peers queue size (default: 2000) */
    int get_peers_timeout_ms;       /* Get_peers response timeout (default: 3000) */

    /* Find_node throttling settings */
    int infohash_pause_threshold;   /* Queue size to pause find_node (default: 2000) */
    int infohash_resume_threshold;  /* Queue size to resume find_node (default: 1000) */

    /* Get_peers throttling settings */
    int peers_pause_threshold;      /* Peers queue size to pause get_peers (default: 2000) */
    int peers_resume_threshold;     /* Peers queue size to resume get_peers (default: 1000) */

    /* Stage 5: Metadata fetcher settings */
    int tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */
    int parallel_peers;             /* Parallel peer connections per infohash (default: 2) */

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;           /* Min metadata rate before respawn (default: 0.01) */
    int rate_check_interval_sec;        /* Rate check interval (default: 60) */
    int rate_grace_period_sec;          /* Grace period before respawn (default: 30) */
    int min_lifetime_minutes;           /* Min lifetime before rate checks (default: 10) */
    bool require_empty_queue;           /* Only respawn if queue empty (default: true) */
    double ema_alpha;                   /* EMA smoothing alpha for metadata rate (default: 0.3) */

    /* Porn filter settings */
    int porn_filter_enabled;            /* Enable porn filter (0=disabled, 1=enabled) */

    /* Shared resources from supervisor */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    struct bloom_filter *failure_bloom;    /* NEW: Failure bloom filter for two-strike filtering */

    /* Supervisor callback context */
    void *supervisor_ctx;
    void (*on_shutdown)(struct thread_tree *tree);
} tree_config_t;

/* Thread tree structure */
typedef struct thread_tree {
    uint32_t tree_id;
    uint8_t node_id[20];           /* Private node_id for this tree */
    uint32_t partition_index;      /* Keyspace partition this tree belongs to */
    uint32_t home_partition;       /* Original partition assigned at startup */
    uint32_t num_partitions;       /* Total number of partitions in the system */

    /* Private data structures (no sharing between trees) */
    void *routing_table;           /* Private routing table (tree_routing_table_t*) */
    struct tree_infohash_queue *infohash_queue;  /* Stage 3: Private infohash queue */
    void *peers_queue;             /* Private peers queue */
    void *socket;                  /* Private UDP socket (tree_socket_t*) */
    struct tree_dispatcher *dispatcher;  /* UDP response dispatcher */
    bool owns_socket;              /* true = destroy socket on cleanup */
    bool owns_dispatcher;          /* true = destroy dispatcher on cleanup */

    /* Per-tree bootstrap response queue */
    struct tree_response_queue *bootstrap_response_queue;

    /* Shared resources (from supervisor) */
    struct bloom_filter *shared_bloom;     /* Stage 3: Shared bloom filter (thread-safe) */
    struct bloom_filter *failure_bloom;    /* NEW: Failure bloom filter for two-strike filtering */


    /* Stage 3 config */
    int infohash_queue_capacity;
    int bep51_query_interval_ms;
    int bep51_node_cooldown_sec;     /* Cooldown between BEP51 queries to same node */

    /* Stage 4 config */
    int peers_queue_capacity;
    int get_peers_timeout_ms;

    /* Stage 5 config */
    int tcp_connect_timeout_ms;
    int parallel_peers;

    /* Shared resources (from supervisor) */
    struct batch_writer *shared_batch_writer;

    /* Tree-native bootstrap timeout */
    int tree_bootstrap_timeout_sec;

    /* Phase management */
    atomic_int current_phase;  /* Use atomic_int to store tree_phase_t enum values */
    _Atomic shutdown_reason_t shutdown_reason;  /* Reason for shutdown */
    atomic_bool shutdown_requested;
    atomic_bool needs_respawn;          /* Signals supervisor to respawn this tree */

    /* Discovery throttling state (find_node + BEP51) */
    atomic_bool discovery_paused;           /* Signal to pause discovery workers (find_node + BEP51) */
    pthread_mutex_t throttle_lock;          /* Protects throttle state changes */
    pthread_cond_t throttle_resume;         /* Condition variable for resuming workers */
    int infohash_pause_threshold;           /* Queue size to pause (default: 2000) */
    int infohash_resume_threshold;          /* Queue size to resume (default: 1000) */

    /* Get_peers throttling state (separate from discovery throttling) */
    atomic_bool get_peers_paused;           /* Signal to pause get_peers workers */
    pthread_mutex_t get_peers_throttle_lock;/* Protects get_peers throttle state */
    pthread_cond_t get_peers_throttle_resume; /* Condition variable for resuming get_peers */
    int peers_pause_threshold;              /* Peers queue size to pause (default: 2000) */
    int peers_resume_threshold;             /* Peers queue size to resume (default: 1000) */

    /* Thread handles */
    pthread_t bootstrap_thread;
    pthread_t *find_node_workers;   /* Continuous find_node workers */
    pthread_t *bep51_threads;
    pthread_t *get_peers_threads;
    pthread_t *metadata_threads;
    pthread_t rate_monitor_thread;      /* Metadata rate monitor */
    pthread_t throttle_monitor_thread;  /* Monitors queue size for throttling */

    /* Thread counts */
    int num_find_node_workers;      /* Continuous find_node workers */
    int find_node_target_nodes;
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Statistics */
    atomic_uint_fast64_t metadata_count;
    atomic_uint_fast64_t metadata_attempts;  /* Total infohashes attempted (including failures) */
    atomic_uint_fast64_t filtered_count;    /* Filtered by porn filter */
    atomic_uint_fast64_t first_strike_failures;   /* Infohashes with first failure (retry allowed) */
    atomic_uint_fast64_t second_strike_failures;  /* Infohashes with second failure (permanently blocked) */
    atomic_uint_fast64_t last_metadata_time;
    double metadata_rate;
    atomic_int active_connections;  /* Track active TCP connections */

    /* BEP51 cache statistics */
    atomic_uint_fast64_t bep51_nodes_cached; /* Nodes submitted to BEP51 cache */

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;               /* Min metadata rate before respawn (0.01 = 1 metadata per 100 seconds) */
    int rate_check_interval_sec;            /* How often to check rate (default: 60s) */
    int rate_grace_period_sec;              /* Grace period before respawn (default: 30s) */
    int min_lifetime_sec;                   /* Minimum lifetime before rate checks (default: 600s = 10min) */
    bool require_empty_queue;               /* Only respawn if infohash queue empty */
    double ema_alpha;                       /* EMA smoothing alpha for metadata rate */

    /* Porn filter settings */
    int porn_filter_enabled;                /* Enable porn filter (0=disabled, 1=enabled) */

    /* Lifecycle tracking */
    time_t creation_time;           /* When tree was created */

    /* Supervisor callback */
    void (*on_shutdown)(struct thread_tree *tree);
    void *supervisor_ctx;
    struct supervisor *supervisor;  /* Backlink to supervisor for accessing shared bep51_cache */
} thread_tree_t;

/**
 * Create a new thread tree
 * @param tree_id Unique identifier for this tree
 * @param config Configuration for the tree
 * @return Pointer to thread tree, or NULL on error
 */
thread_tree_t *thread_tree_create(uint32_t tree_id, tree_config_t *config);

/**
 * Destroy a thread tree (joins all threads, frees all memory)
 * @param tree Thread tree to destroy
 */
void thread_tree_destroy(thread_tree_t *tree);

/**
 * Start the thread tree (spawns bootstrap thread)
 * @param tree Thread tree to start
 */
void thread_tree_start(thread_tree_t *tree);

/**
 * Request graceful shutdown of thread tree
 * @param tree Thread tree to shut down
 * @param reason Reason for shutdown (rate-based or supervisor-initiated)
 */
void thread_tree_request_shutdown(thread_tree_t *tree, shutdown_reason_t reason);

/**
 * Get phase name as string
 * @param phase Phase enum value
 * @return Phase name string
 */
const char *thread_tree_phase_name(tree_phase_t phase);

#endif /* THREAD_TREE_H */
