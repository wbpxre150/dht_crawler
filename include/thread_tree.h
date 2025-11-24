#ifndef THREAD_TREE_H
#define THREAD_TREE_H

#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <stdbool.h>

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

/* Configuration for a thread tree */
typedef struct tree_config {
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

    /* Stage 5: Metadata fetcher settings */
    double min_metadata_rate;       /* Minimum metadata/sec before shutdown (default: 0.5) */
    int rate_check_interval_sec;    /* Rate check interval (default: 10) */
    int rate_grace_period_sec;      /* Grace period before shutdown (default: 30) */
    int tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */

    /* Shared resources from supervisor */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;

    /* Supervisor callback context */
    void *supervisor_ctx;
    void (*on_shutdown)(struct thread_tree *tree);
} tree_config_t;

/* Thread tree structure */
typedef struct thread_tree {
    uint32_t tree_id;
    uint8_t node_id[20];           /* Private node_id for this tree */

    /* Private data structures (no sharing between trees) */
    void *routing_table;           /* Private routing table (tree_routing_table_t*) */
    struct tree_infohash_queue *infohash_queue;  /* Stage 3: Private infohash queue */
    void *peers_queue;             /* Private peers queue */
    void *socket;                  /* Private UDP socket (tree_socket_t*) */
    struct tree_dispatcher *dispatcher;  /* UDP response dispatcher */

    /* Shared resources (from supervisor) */
    struct bloom_filter *shared_bloom;  /* Stage 3: Shared bloom filter (thread-safe) */

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
    double min_metadata_rate;
    int rate_check_interval_sec;
    int rate_grace_period_sec;
    int tcp_connect_timeout_ms;

    /* Shared resources (from supervisor) */
    struct batch_writer *shared_batch_writer;

    /* Phase management */
    tree_phase_t current_phase;
    atomic_bool shutdown_requested;

    /* Thread handles */
    pthread_t bootstrap_thread;
    pthread_t *bootstrap_workers;   /* Stage 2: Find_node workers for bootstrap */
    pthread_t *find_node_workers;   /* Continuous find_node workers */
    pthread_t *bep51_threads;
    pthread_t *get_peers_threads;
    pthread_t *metadata_threads;
    pthread_t rate_monitor_thread;  /* Stage 5: Rate monitor thread */

    /* Thread counts */
    int num_bootstrap_workers;      /* Stage 2: Number of bootstrap find_node workers */
    int num_find_node_workers;      /* Continuous find_node workers */
    int num_bep51_workers;
    int num_get_peers_workers;
    int num_metadata_workers;

    /* Statistics */
    atomic_uint_fast64_t metadata_count;
    atomic_uint_fast64_t last_metadata_time;
    double metadata_rate;

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
 */
void thread_tree_request_shutdown(thread_tree_t *tree);

/**
 * Get phase name as string
 * @param phase Phase enum value
 * @return Phase name string
 */
const char *thread_tree_phase_name(tree_phase_t phase);

#endif /* THREAD_TREE_H */
