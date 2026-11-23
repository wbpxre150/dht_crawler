#ifndef THREAD_TREE_H
#define THREAD_TREE_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>

#include "tree_peers_queue.h"    /* peer_entry_t for tree_lookup_peers() */
#include "tree_response_queue.h" /* tree_response_queue_t for tree_lookup_peers() */

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
    int get_peers_timeout_ms;       /* Get_peers response timeout (default: 1500) */
    int get_peers_deadline_ms;      /* Get_peers lookup deadline (default: 6000, 500-8000) */
    int get_peers_fanout;           /* Get_peers queries per lookup (default: 8, 2-12) */
    int get_peers_max_queries;      /* Get_peers query budget (default: 16, 8-32, >= fanout) */
    int get_peers_round_idle_ms;    /* Get_peers per-round silence cap (default: 0 = off, 250-6000) */
    int get_peers_penalize_misses;  /* 1 = charge an unanswered round slot against the node (default: 0) */
    int get_peers_slot_budget_ms;   /* Per-slot answer budget in ms (default: 0 = off, 500-6000) */
    int get_peers_finish_on_peers;  /* End the lookup as soon as peers are found (default: 0, 0-1) */

    /* Stage 5: Metadata fetcher settings */
    int tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */
    int parallel_peers;             /* Parallel peer connections per infohash (default: 2, 1-4) */
    int fresh_peer_retries;         /* Fresh-peer re-lookups before failure strike (default: 1, 0-3) */
    int metadata_timeout_ms;        /* Metadata fetch deadline in ms (default: 60000, 5000-600000) */
    int metadata_piece_idle_ms;     /* Piece-progress eviction window in ms (default: 0 = off, 3000-60000) */

    /* Metadata rate-based respawn settings */
    double min_metadata_rate;           /* Min metadata rate before respawn (default: 0.01) */
    int rate_check_interval_sec;        /* Rate check interval (default: 60) */
    int rate_grace_period_sec;          /* Grace period before respawn (default: 30) */
    int min_lifetime_minutes;           /* Min lifetime before rate checks (default: 10) */
    bool require_empty_queue;           /* Only respawn if queue empty (default: true) */
    double ema_alpha;                   /* EMA smoothing alpha for metadata rate (default: 0.3) */

    /* Stage 6 adult handling */
    char adult_mode[8];                  /* "tag" (default) or "drop" at ingest */

    /* "Categories to save" bitmasks (see category_filter_cleanup.h) */
    uint16_t save_category_mask;         /* bit i = tp_category_t i saved */
    uint16_t save_tv_kind_mask;          /* bit i = tp_tv_kind_t i saved */

    /* Shared resources from supervisor */
    struct batch_writer *batch_writer;
    struct bloom_filter *bloom_filter;
    struct inflight_set *inflight;         /* Shared in-flight claim set (NULL = dedup disabled) */
    struct bloom_filter *failure_bloom;    /* NEW: Failure bloom filter for two-strike filtering */
    int failure_strike_count;              /* Failures before permanent block (1 or 2, default: 2) */

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
    struct inflight_set *shared_inflight;  /* Shared in-flight claim set (NULL = dedup disabled) */
    struct bloom_filter *failure_bloom;    /* NEW: Failure bloom filter for two-strike filtering */
    int failure_strike_count;              /* Failures before permanent block (1 or 2, default: 2) */


    /* Stage 3 config */
    int infohash_queue_capacity;
    int bep51_query_interval_ms;
    int bep51_node_cooldown_sec;     /* Cooldown between BEP51 queries to same node */

    /* Stage 4 config */
    int peers_queue_capacity;
    int get_peers_timeout_ms;
    int get_peers_deadline_ms;
    int get_peers_fanout;
    int get_peers_max_queries;
    int get_peers_round_idle_ms;    /* Per-round silence cap before the round is abandoned (0 = off) */
    int get_peers_penalize_misses;  /* 1 = tree_routing_mark_failed() the nodes that left a slot unanswered (default: 0) */
    int get_peers_slot_budget_ms;   /* A slot stops pinning its round after this long (0 = off) */
    int get_peers_finish_on_peers;  /* 1 = stop waiting on the remaining slots once peers are found */

    /* Stage 5 config */
    int tcp_connect_timeout_ms;
    int parallel_peers;
    int fresh_peer_retries;         /* Fresh-peer re-lookups before a metadata failure strike (0-3) */
    int metadata_timeout_ms;        /* Metadata fetch deadline in ms (default: 30000, 5000-600000) */
    int metadata_piece_idle_ms;     /* Evict a conn with no new data piece for this long (0 = off) */

    /* Shared resources (from supervisor) */
    struct batch_writer *shared_batch_writer;


    /* Phase management */
    atomic_int current_phase;  /* Use atomic_int to store tree_phase_t enum values */
    _Atomic shutdown_reason_t shutdown_reason;  /* Reason for shutdown */
    atomic_bool shutdown_requested;
    atomic_bool needs_respawn;          /* Signals supervisor to respawn this tree */

    /* Discovery throttling state (find_node + BEP51) */
    atomic_bool discovery_paused;           /* Signal to pause discovery workers (find_node + BEP51) */
    pthread_mutex_t throttle_lock;          /* Protects throttle state changes */
    pthread_cond_t throttle_resume;         /* Condition variable for resuming workers */
    int infohash_pause_threshold;           /* Derived: pause discovery at 90% of infohash_queue_capacity */
    int infohash_resume_threshold;          /* Derived: resume discovery at 33% of infohash_queue_capacity */

    /* Get_peers throttling state (separate from discovery throttling) */
    atomic_bool get_peers_paused;           /* Signal to pause get_peers workers */
    pthread_mutex_t get_peers_throttle_lock;/* Protects get_peers throttle state */
    pthread_cond_t get_peers_throttle_resume; /* Condition variable for resuming get_peers */
    int peers_pause_threshold;              /* Derived: pause get_peers at 90% of peers_queue_capacity */
    int peers_resume_threshold;             /* Derived: resume get_peers at 33% of peers_queue_capacity */

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
    /* Metadata connection-failure attribution. Each failed metadata connection is
     * counted exactly once, under the outcome that first detected it, so a window
     * of these partitions the failure mass that metadata_attempts - metadata_count
     * only bounds. Per-attempt events (deadline) are counted once per attempt. */
    atomic_uint_fast64_t metadata_fail_connect;    /* socket/nonblock/connect/send_handshake error */
    atomic_uint_fast64_t metadata_fail_handshake;  /* bad BT handshake, extended-handshake failure, EOF while handshaking */
    atomic_uint_fast64_t metadata_fail_stall;      /* silence watchdog (no readable data for CONN_SILENCE_CLOSE_MS) */
    atomic_uint_fast64_t metadata_fail_ut_reject;  /* peer answered UT_METADATA_REJECT */
    atomic_uint_fast64_t metadata_fail_size;       /* size over the cap, or peers disagreeing on size */
    atomic_uint_fast64_t metadata_fail_hash;       /* SHA-1 mismatch event (incl. the one that triggers the reset) */
    atomic_uint_fast64_t metadata_fail_deadline;   /* one per attempt that hit the global metadata deadline */
    atomic_uint_fast64_t metadata_fail_other;      /* any other ERROR transition (bad message, EOF while requesting, ...) */
    atomic_uint_fast64_t filtered_count;    /* Dropped by the ingest filters (hard block/language/adult) */
    atomic_uint_fast64_t first_strike_failures;   /* Infohashes with first failure (retry allowed) */
    atomic_uint_fast64_t second_strike_failures;  /* Infohashes with second failure (permanently blocked) */
    atomic_uint_fast64_t fresh_peer_relookups;    /* Metadata failures that triggered a fresh-peer re-lookup */
    atomic_uint_fast64_t fresh_peer_rescues;      /* Re-lookups whose retry fetched metadata */
    atomic_uint_fast64_t metadata_piece_reasks;   /* Pieces re-asked from another conn after the 3 s cooldown */
    /* get_peers lookup instrumentation (per tree) */
    atomic_uint_fast64_t gp_lookups;             /* tree_lookup_peers() calls that passed the argument guard */
    atomic_uint_fast64_t gp_lookups_with_peers;  /* Lookups whose result had peer_count > 0 */
    atomic_uint_fast64_t gp_lookups_from_retry;  /* Lookups started by the metadata fresh-peer retry path */
    atomic_uint_fast64_t gp_packets_sent;        /* get_peers packets attempted (failures counted too) */
    atomic_uint_fast64_t gp_answers;             /* Responses consumed from the queue (pop_result == 0) */
    atomic_uint_fast64_t gp_lookup_us_total;     /* Total microseconds spent inside tree_lookup_peers() */
    atomic_uint_fast64_t last_metadata_time;
    atomic_uint_fast64_t gp_rounds_total;       /* Completed rounds after the first, summed over lookups */
    atomic_uint_fast64_t gp_idle_exits;          /* Lookups that ended on the round-idle cap, not the deadline */
    atomic_uint_fast64_t gp_deadline_exits;      /* Lookups whose terminal cause was the per-infohash deadline */
    atomic_uint_fast64_t gp_unanswered_slots;    /* Round slots queried and never answered (any exit) */
    atomic_uint_fast64_t gp_miss_penalties;      /* Unanswered slots charged via tree_routing_mark_failed() */
    atomic_uint_fast64_t gp_slot_expired_total;  /* Unanswered slots whose per-slot answer budget had run out */
    atomic_uint_fast64_t gp_early_finish_total;  /* Lookups ended by finish-on-peers once peers were found */
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

    /* Stage 6 adult handling */
    char adult_mode[8];                  /* "tag": store the verdict, "drop": discard */

    /* "Categories to save": ingest drops items outside these bitmasks */
    uint16_t save_category_mask;         /* bit i = tp_category_t i saved */
    uint16_t save_tv_kind_mask;          /* bit i = tp_tv_kind_t i saved */

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

/**
 * Run an iterative get_peers lookup for infohash and collect discovered peers.
 *
 * Queries the closest routing-table nodes first, then — while no peers turn up —
 * the closer nodes those responders name, in rounds of up to
 * tree->get_peers_fanout candidates each, and only if a candidate is XOR-closer
 * to the infohash than the closest node that has actually
 * responded. Bounded by tree->get_peers_max_queries total packets and
 * tree->get_peers_deadline_ms wall time per infohash. Peers are written into
 * out->peers and out->infohash is set to infohash. This helper performs no
 * queue push and does not change the tree phase; callers decide what to do with
 * the result.
 *
 * Instrumentation only: updates the tree's gp_* counters and adds the elapsed
 * microseconds to gp_lookup_us_total; no behaviour change.
 *
 * @param tree Thread tree (its routing table, socket and dispatcher are used)
 * @param infohash Target infohash (20 bytes)
 * @param rsp_queue Per-caller response queue used to receive replies
 * @param out Peer entry to fill (zeroed, infohash set, peers collected)
 * @return Number of peers found (out->peer_count); 0 if none
 */
int tree_lookup_peers(thread_tree_t *tree, const uint8_t infohash[20],
                      tree_response_queue_t *rsp_queue, peer_entry_t *out);

#endif /* THREAD_TREE_H */
