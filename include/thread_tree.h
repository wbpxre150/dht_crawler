#ifndef THREAD_TREE_H
#define THREAD_TREE_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <stdbool.h>
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
 *
 * Each thread tree has its own:
 * - Node ID (for DHT identity)
 * - Routing table
 * - Infohash queue
 * - Peers queue
 * - Worker threads
 *
 * Only shared resources are batch_writer and bloom_filter from supervisor.
 */

/* Phase of the thread tree lifecycle */
typedef enum {
    TREE_PHASE_BOOTSTRAP,       /* Initial bootstrap phase */
    TREE_PHASE_BEP51,           /* sample_infohashes discovery */
    TREE_PHASE_GET_PEERS,       /* get_peers for peer discovery */
    TREE_PHASE_METADATA,        /* Metadata fetching */
    TREE_PHASE_SHUTTING_DOWN    /* Graceful shutdown in progress */
} tree_phase_t;

/* Reason for shutdown */
typedef enum {
    SHUTDOWN_REASON_NONE,           /* Not shutting down */
    SHUTDOWN_REASON_RATE_BASED,     /* Low metadata rate (voluntary) */
    SHUTDOWN_REASON_SUPERVISOR      /* Supervisor requested (forced) */
} shutdown_reason_t;

/* Configuration for a thread tree */
typedef struct tree_config {
    /* Keyspace partitioning */
    uint32_t partition_index;       /* Keyspace partition for this tree */
    uint32_t num_partitions;        /* Total partitions in the system */
    bool use_keyspace_partitioning; /* Enable keyspace partitioning (vs random node ID) */

    int num_bootstrap_workers;      /* Stage 2: Find_node workers for bootstrap (default: 10) */
    int num_find_node_workers;      /* Continuous find_node workers (default: 30) */
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Stage 2: Bootstrap settings */
    int bootstrap_timeout_sec;      /* Bootstrap phase timeout */
    int routing_threshold;          /* Nodes required before BEP51 phase */

    /* Stage 3: BEP51 settings */
    int infohash_queue_capacity;    /* Infohash queue size (default: 5000) */
    int bep51_query_interval_ms;    /* Delay between BEP51 queries (default: 10) */

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

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;           /* Min metadata rate before respawn (default: 0.01) */
    int rate_check_interval_sec;        /* Rate check interval (default: 60) */
    int rate_grace_period_sec;          /* Grace period before respawn (default: 30) */
    int min_lifetime_minutes;           /* Min lifetime before rate checks (default: 10) */
    bool require_empty_queue;           /* Only respawn if queue empty (default: true) */

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
    uint32_t num_partitions;       /* Total number of partitions in the system */

    /* Private data structures (no sharing between trees) */
    void *routing_table;           /* Private routing table (tree_routing_table_t*) */
    struct tree_infohash_queue *infohash_queue;  /* Stage 3: Private infohash queue */
    void *peers_queue;             /* Private peers queue */
    void *socket;                  /* Private UDP socket (tree_socket_t*) */
    struct tree_dispatcher *dispatcher;  /* UDP response dispatcher */

    /* Shared resources (from supervisor) */
    struct bloom_filter *shared_bloom;     /* Stage 3: Shared bloom filter (thread-safe) */
    struct bloom_filter *failure_bloom;    /* NEW: Failure bloom filter for two-strike filtering */

    /* Stage 2 config */
    int bootstrap_timeout_sec;
    int routing_threshold;

    /* Stage 3 config */
    int infohash_queue_capacity;
    int bep51_query_interval_ms;

    /* Stage 4 config */
    int peers_queue_capacity;
    int get_peers_timeout_ms;

    /* Stage 5 config */
    int tcp_connect_timeout_ms;

    /* Shared resources (from supervisor) */
    struct batch_writer *shared_batch_writer;

    /* Phase management */
    atomic_int current_phase;  /* Use atomic_int to store tree_phase_t enum values */
    shutdown_reason_t shutdown_reason;  /* Reason for shutdown */
    atomic_bool shutdown_requested;

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
    pthread_t *bootstrap_workers;   /* Stage 2: Find_node workers for bootstrap */
    pthread_t *find_node_workers;   /* Continuous find_node workers */
    pthread_t *bep51_threads;
    pthread_t *get_peers_threads;
    pthread_t *metadata_threads;
    pthread_t rate_monitor_thread;      /* Metadata rate monitor */
    pthread_t throttle_monitor_thread;  /* Monitors queue size for throttling */

    /* Thread counts */
    int num_bootstrap_workers;      /* Stage 2: Number of bootstrap find_node workers */
    int num_find_node_workers;      /* Continuous find_node workers */
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

    /* Porn filter settings */
    int porn_filter_enabled;                /* Enable porn filter (0=disabled, 1=enabled) */

    /* Lifecycle tracking */
    time_t creation_time;           /* When tree was created */

    /* Supervisor callback */
    void (*on_shutdown)(struct thread_tree *tree);
    void *supervisor_ctx;
    struct supervisor *supervisor;  /* NEW: Backlink to supervisor for accessing shared_node_pool */
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
