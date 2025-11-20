/*
 * wbpxre-dht: Modern BitTorrent DHT implementation in C
 * Inspired by bitmagnet's multi-pipeline concurrent architecture
 *
 * Copyright (c) 2025
 * MIT License
 */

#ifndef WBPXRE_DHT_H
#define WBPXRE_DHT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <urcu.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

#define WBPXRE_NODE_ID_LEN 20
#define WBPXRE_INFO_HASH_LEN 20
#define WBPXRE_TRANSACTION_ID_LEN 2
#define WBPXRE_MAX_UDP_PACKET 65507
#define WBPXRE_COMPACT_NODE_INFO_LEN 26  /* 20 bytes ID + 4 bytes IPv4 + 2 bytes port */
#define WBPXRE_COMPACT_PEER_LEN 6        /* 4 bytes IPv4 + 2 bytes port */
#define WBPXRE_METADATA_PIECE_SIZE 16384 /* 16 KiB */

/* Magic number for validating wbpxre_message_t structures */
#define WBPXRE_MESSAGE_MAGIC 0x57425058  /* "WBPX" in hex */

/* Default configuration values */
#define WBPXRE_DEFAULT_QUERY_TIMEOUT 5
#define WBPXRE_DEFAULT_TARGET_ROTATION_INTERVAL 10
#define WBPXRE_DEFAULT_SAMPLE_INTERVAL 60
#define WBPXRE_DEFAULT_EMPTY_SAMPLE_PENALTY 300

/* Queue capacities (scaled for 50K routing table) */
#define WBPXRE_DISCOVERED_NODES_CAPACITY 50000
#define WBPXRE_PING_QUEUE_CAPACITY 2000
#define WBPXRE_FIND_NODE_QUEUE_CAPACITY 2000
#define WBPXRE_SAMPLE_INFOHASHES_CAPACITY 15000  /* Increased from 2000 to prevent queue saturation */
#define WBPXRE_INFOHASH_TRIAGE_CAPACITY 20000
#define WBPXRE_GET_PEERS_CAPACITY 10000
#define WBPXRE_METAINFO_CAPACITY 2000

/* ============================================================================
 * Enums and Type Definitions
 * ============================================================================ */

/* DHT message types */
typedef enum {
    WBPXRE_MSG_QUERY = 'q',
    WBPXRE_MSG_RESPONSE = 'r',
    WBPXRE_MSG_ERROR = 'e'
} wbpxre_msg_type_t;

/* DHT query methods */
typedef enum {
    WBPXRE_METHOD_PING,
    WBPXRE_METHOD_FIND_NODE,
    WBPXRE_METHOD_GET_PEERS,
    WBPXRE_METHOD_ANNOUNCE_PEER,
    WBPXRE_METHOD_SAMPLE_INFOHASHES
} wbpxre_method_t;

/* BEP 51 support tracking */
typedef enum {
    WBPXRE_PROTOCOL_UNKNOWN = 0,
    WBPXRE_PROTOCOL_YES = 1,
    WBPXRE_PROTOCOL_NO = 2
} wbpxre_protocol_support_t;

/* DHT events for callbacks */
typedef enum {
    WBPXRE_EVENT_NONE = 0,
    WBPXRE_EVENT_VALUES = 1,           /* Peers discovered */
    WBPXRE_EVENT_SEARCH_DONE = 3,      /* Search completed */
    WBPXRE_EVENT_SAMPLES = 7,          /* BEP 51 samples */
    WBPXRE_EVENT_NODE_DISCOVERED = 10  /* New node found */
} wbpxre_event_t;

/* ============================================================================
 * Data Structures
 * ============================================================================ */

/* Public statistics structure */
typedef struct {
    uint64_t packets_sent;
    uint64_t packets_received;
    uint64_t queries_sent;
    uint64_t responses_received;
    uint64_t errors_received;
    uint64_t nodes_discovered;
    uint64_t infohashes_discovered;
    /* BEP 51 statistics */
    uint64_t bep51_queries_sent;
    uint64_t bep51_responses_received;
    uint64_t bep51_samples_received;
    /* Peer discovery statistics */
    uint64_t get_peers_queries_sent;
    uint64_t get_peers_responses_received;
} wbpxre_stats_t;

/* Node address */
typedef struct {
    struct sockaddr_in addr;
    char ip[INET_ADDRSTRLEN];
    uint16_t port;
} wbpxre_node_addr_t;

/* Routing table node with BEP 51 metadata */
typedef struct wbpxre_routing_node {
    uint8_t id[WBPXRE_NODE_ID_LEN];
    wbpxre_node_addr_t addr;
    time_t discovered_at;
    time_t last_responded_at;
    bool dropped;

    /* BEP 51 tracking */
    wbpxre_protocol_support_t bep51_support;
    int sampled_num;              /* Total hashes discovered from this node */
    int last_discovered_num;      /* Hashes in last query */
    int total_num;                /* Total hashes node reported having */
    time_t next_sample_time;      /* When to query again */

    /* Phase 4: Node quality tracking */
    int queries_sent;             /* Total queries sent to this node */
    int responses_received;       /* Total responses received from this node */

    /* Tree structure (for B-tree/AVL implementation) */
    struct wbpxre_routing_node *left;
    struct wbpxre_routing_node *right;
    int height;

    /* RCU support for lock-free reads */
    struct rcu_head rcu_head;     /* For deferred freeing */
} wbpxre_routing_node_t;

/* Routing table */
typedef struct {
    wbpxre_routing_node_t *root;         /* RCU-protected AVL tree root */
    pthread_mutex_t update_lock;         /* Serializes writers (RCU allows lock-free reads) */
    int node_count;                      /* Updated atomically */
    int max_nodes;
    /* Flat array for uniform iteration (eliminates BST traversal bias) */
    wbpxre_routing_node_t **all_nodes;  /* RCU-protected array of pointers to all nodes */
    int all_nodes_capacity;              /* Capacity of all_nodes array */
    int iteration_offset;                /* Rotating offset for fair iteration */

    /* Hash map for O(1) flat array index lookups */
    void *node_index_map;                /* node_index_map_entry_t* hash table (opaque to avoid exposing uthash) */
} wbpxre_routing_table_t;

/* ============================================================================
 * Dual Routing Table System
 * ============================================================================ */

/* Table states for dual routing */
typedef enum {
    DUAL_TABLE_STATE_ACTIVE,     /* Workers read from this table */
    DUAL_TABLE_STATE_FILLING,    /* New nodes inserted here */
    DUAL_TABLE_STATE_CLEARING,   /* Being cleared */
    DUAL_TABLE_STATE_READY       /* Cleared, waiting to become FILLING */
} dual_table_state_t;

/* Rotation states for dual routing */
typedef enum {
    ROTATION_STABLE,        /* Normal operation */
    ROTATION_TRANSITIONING, /* Switching active table */
    ROTATION_CLEARING       /* Clearing old table */
} dual_rotation_state_t;

/* Statistics for dual routing */
typedef struct {
    uint64_t total_rotations;
    uint64_t total_nodes_cleared;
    time_t last_rotation_time;
    int table_a_nodes;
    int table_b_nodes;
    dual_table_state_t table_a_state;
    dual_table_state_t table_b_state;
} dual_routing_stats_t;

/* Dual routing table controller */
typedef struct {
    /* The two routing tables */
    wbpxre_routing_table_t *table_a;
    wbpxre_routing_table_t *table_b;

    /* State tracking */
    dual_table_state_t table_a_state;
    dual_table_state_t table_b_state;
    dual_rotation_state_t rotation_state;

    /* Current active table (for workers to read) */
    wbpxre_routing_table_t *active_table;    /* RCU-protected pointer */
    wbpxre_routing_table_t *filling_table;   /* Current insert target */

    /* Capacity thresholds */
    int max_nodes_per_table;                 /* e.g., 30000 */
    double fill_start_threshold;             /* 0.80 (80%) */
    double switch_threshold;                 /* 0.20 (20%) */

    /* Synchronization */
    pthread_mutex_t rotation_lock;           /* Protects state transitions */
    pthread_rwlock_t active_table_lock;      /* Protects active_table pointer */

    /* Statistics */
    uint64_t total_rotations;
    uint64_t total_nodes_cleared;
    time_t last_rotation_time;
} dual_routing_controller_t;

/* ============================================================================
 * Triple Routing Table System
 * ============================================================================ */

/* Table states for triple routing */
typedef enum {
    TABLE_STATE_IDLE,      /* Empty, not in use (can be selected for filling) */
    TABLE_STATE_FILLING,   /* Receiving inserts from find_node workers */
    TABLE_STATE_STABLE,    /* Full, being read by sample/DHT workers */
    TABLE_STATE_CLEARING   /* Being cleared before transitioning to FILLING */
} triple_table_state_t;

/* Rotation progress state (atomic guard to prevent concurrent rotations) */
typedef enum {
    ROTATION_PROGRESS_IDLE = 0,        /* No rotation in progress */
    ROTATION_PROGRESS_IN_PROGRESS = 1  /* Rotation currently happening */
} rotation_progress_state_t;

/* Statistics for triple routing */
typedef struct {
    /* Bootstrap status */
    bool bootstrap_complete;
    uint32_t bootstrap_duration_sec;

    /* Rotation stats */
    uint64_t total_rotations;
    int stable_idx;           /* -1 if bootstrap incomplete */
    int filling_idx;
    uint32_t rotation_threshold;

    /* Table metrics */
    uint32_t stable_table_nodes;
    uint32_t filling_table_nodes;
    double fill_progress_pct;

    /* Detailed metrics */
    uint64_t total_nodes_cleared;
    uint64_t clearing_operations;
    time_t last_rotation_time;
    uint32_t nodes_at_rotation[3];
    double rotation_duration_ms[3];
    uint64_t stable_reads[3];

    /* Per-table state */
    triple_table_state_t table_states[3];
    uint32_t table_node_counts[3];
} triple_routing_stats_t;

/* Triple routing table controller */
typedef struct {
    /* The three routing tables */
    wbpxre_routing_table_t *tables[3];

    /* State of each table */
    triple_table_state_t states[3];

    /* Current table indices */
    int stable_idx;   /* Index of current STABLE table (-1 if none) */
    int filling_idx;  /* Index of current FILLING table */

    /* Rotation configuration */
    uint32_t rotation_threshold;  /* Node count to trigger rotation */
    int max_nodes_per_table;      /* Capacity per table */
    int rotation_time_sec;        /* Minimum time between rotations (seconds) */

    /* Bootstrap tracking */
    int bootstrap_complete;           /* 1 after first rotation, 0 otherwise (accessed atomically) */
    time_t bootstrap_start_time;      /* When bootstrap started */
    time_t first_rotation_time;       /* When first rotation completed */

    /* Per-table node IDs (one for each routing table) */
    uint8_t table_node_ids[3][WBPXRE_NODE_ID_LEN];

    /* DHT context for queue clearing during rotation */
    void *dht_context;  /* wbpxre_dht_t* (void* to avoid circular dependency) */

    /* Synchronization */
    pthread_mutex_t rotation_lock;  /* Protects state transitions */
    int rotation_in_progress;       /* Guard flag: ROTATION_PROGRESS_IDLE=0, ROTATION_PROGRESS_IN_PROGRESS=1 (accessed atomically) */
    time_t rotation_start_time;     /* When current rotation started (for monitoring) */

    /* Statistics */
    uint64_t rotation_count;          /* Total rotations performed */
    unsigned long long total_nodes_cleared;     /* Total nodes freed (accessed atomically for background clearing) */
    time_t last_rotation_time;        /* Timestamp of last rotation */
    uint32_t nodes_at_rotation[3];    /* Node count at each rotation */
    double rotation_duration_ms[3];   /* Fill time for each table */
    uint64_t stable_reads[3];         /* Reads from each table while STABLE */
    unsigned long long clearing_operations;     /* Total clear operations (accessed atomically for background clearing) */
} triple_routing_controller_t;

/* Peer info */
typedef struct {
    struct sockaddr_in addr;
    char ip[INET_ADDRSTRLEN];
    uint16_t port;
} wbpxre_peer_t;

/* Info hash with peers */
typedef struct {
    uint8_t info_hash[WBPXRE_INFO_HASH_LEN];
    wbpxre_peer_t *peers;
    int peer_count;
    int peer_capacity;
    time_t added_at;
} wbpxre_infohash_entry_t;

/* Info hash work item for get_peers pipeline */
typedef struct {
    uint8_t info_hash[WBPXRE_INFO_HASH_LEN];
    time_t added_at;
} wbpxre_infohash_work_t;

/* Pending query (for request/response matching) */
typedef struct wbpxre_pending_query {
    uint8_t transaction_id[WBPXRE_TRANSACTION_ID_LEN];
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    void *response_data;
    bool received;
    bool error;
    time_t deadline;
    struct wbpxre_pending_query *next;
    /* Reference counting for safe cleanup */
    _Atomic int ref_count;
    _Atomic bool freed;
} wbpxre_pending_query_t;

/* Work queue for pipeline */
typedef struct {
    void **items;
    int capacity;
    int size;
    int head;
    int tail;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    bool shutdown;
} wbpxre_work_queue_t;

/* DHT message structure */
typedef struct {
    uint32_t magic;              /* Magic number for validation (0 for stack-allocated, WBPXRE_MESSAGE_MAGIC for heap) */
    wbpxre_msg_type_t type;
    uint8_t transaction_id[WBPXRE_TRANSACTION_ID_LEN];

    /* For queries */
    wbpxre_method_t method;
    uint8_t id[WBPXRE_NODE_ID_LEN];
    uint8_t target[WBPXRE_NODE_ID_LEN];
    uint8_t info_hash[WBPXRE_INFO_HASH_LEN];

    /* For responses */
    uint8_t *nodes;              /* Compact node info */
    int nodes_len;
    uint8_t *values;             /* Compact peer info */
    int values_len;

    /* BEP 51 specific */
    uint8_t *samples;            /* Compact info_hash array */
    int samples_count;
    int total_num;
    int interval;

    /* Token for announce_peer */
    uint8_t *token;
    int token_len;
} wbpxre_message_t;

/* Callback function type */
typedef void (*wbpxre_callback_t)(void *closure, wbpxre_event_t event,
                                   const uint8_t *info_hash,
                                   const void *data, size_t data_len);

/* DHT configuration */
typedef struct {
    int port;
    uint8_t node_id[WBPXRE_NODE_ID_LEN];

    /* Worker counts */
    int ping_workers;
    int find_node_workers;
    int sample_infohashes_workers;
    int get_peers_workers;

    /* Timeouts (seconds) */
    int query_timeout;
    int tcp_connect_timeout;

    /* Intervals (seconds) */
    int target_rotation_interval;
    int reseed_interval;
    int old_node_threshold;

    /* Queue sizes */
    int discovered_nodes_capacity;
    int ping_queue_capacity;
    int find_node_queue_capacity;
    int sample_infohashes_capacity;

    /* Routing table configuration */
    int max_routing_table_nodes;     /* Maximum nodes in routing table (default: 90000) */
    uint32_t triple_routing_threshold;  /* Rotation threshold (default: 1500) */
    int triple_routing_rotation_time;   /* Minimum time between rotations in seconds (default: 60) */

    /* Callbacks */
    wbpxre_callback_t callback;
    void *callback_closure;
} wbpxre_config_t;

/* Main DHT context */
typedef struct {
    wbpxre_config_t config;

    /* Network */
    int udp_socket;
    struct sockaddr_in bind_addr;

    /* Triple routing table controller (replaces dual routing) */
    triple_routing_controller_t *routing_controller;

    /* Pending queries */
    wbpxre_pending_query_t *pending_queries[256];
    pthread_mutex_t pending_queries_mutex;

    /* Worker queues */
    wbpxre_work_queue_t *discovered_nodes;
    wbpxre_work_queue_t *nodes_for_find_node;
    wbpxre_work_queue_t *nodes_for_sample_infohashes;
    wbpxre_work_queue_t *infohashes_for_get_peers;

    /* Worker threads */
    pthread_t udp_reader_thread;
    pthread_t *find_node_worker_threads;
    pthread_t *sample_infohashes_worker_threads;
    pthread_t *get_peers_worker_threads;
    pthread_t discovered_nodes_dispatcher_thread;
    pthread_t target_rotation_thread;
    pthread_t bootstrap_thread;
    pthread_t sample_infohashes_feeder_thread;
    pthread_t find_node_feeder_thread;
    pthread_t get_peers_feeder_thread;

    /* Shared state */
    uint8_t sought_node_id[WBPXRE_NODE_ID_LEN];
    pthread_mutex_t sought_node_id_mutex;

    /* Control */
    bool running;
    pthread_mutex_t running_mutex;

    /* UDP Reader Readiness (Phase 1) */
    bool udp_reader_ready;
    pthread_cond_t udp_reader_ready_cond;
    pthread_mutex_t udp_reader_ready_mutex;

    /* Statistics */
    struct {
        uint64_t packets_sent;
        uint64_t packets_received;
        uint64_t queries_sent;
        uint64_t responses_received;
        uint64_t errors_received;
        uint64_t nodes_discovered;
        uint64_t infohashes_discovered;
        /* BEP 51 statistics */
        uint64_t bep51_queries_sent;
        uint64_t bep51_responses_received;
        uint64_t bep51_samples_received;
        /* Peer discovery statistics */
        uint64_t get_peers_queries_sent;
        uint64_t get_peers_responses_received;
    } stats;
    pthread_mutex_t stats_mutex;
} wbpxre_dht_t;

/* ============================================================================
 * Core API Functions
 * ============================================================================ */

/* Initialize DHT context */
wbpxre_dht_t *wbpxre_dht_init(const wbpxre_config_t *config);

/* Start DHT (spawn worker threads) */
int wbpxre_dht_start(wbpxre_dht_t *dht);

/* Stop DHT (graceful shutdown) */
int wbpxre_dht_stop(wbpxre_dht_t *dht);

/* Cleanup DHT context */
void wbpxre_dht_cleanup(wbpxre_dht_t *dht);

/* Hot-swap node ID without restarting DHT
 * Returns: 0 on success, -1 on error
 * Thread-safe: Yes
 * Side effects: All subsequent queries/responses use new ID
 *               Routing table is NOT reorganized (performance optimization)
 */

/* Bootstrap from known nodes */
int wbpxre_dht_bootstrap(wbpxre_dht_t *dht, const char *hostname, uint16_t port);

/* Insert node manually */
int wbpxre_dht_insert_node(wbpxre_dht_t *dht, const uint8_t *node_id,
                            const struct sockaddr_in *addr);

/* Queue info_hash for get_peers query
 * priority: if true, add to front of queue (for on-demand queries)
 *           if false, add to back of queue (for automatic discovery) */
int wbpxre_dht_query_peers(wbpxre_dht_t *dht, const uint8_t *info_hash, bool priority);

/* Get routing table stats */
int wbpxre_dht_nodes(wbpxre_dht_t *dht, int *good_return, int *dubious_return);

/* Get all nodes from routing table */
int wbpxre_dht_get_nodes(wbpxre_dht_t *dht, struct sockaddr_in *addrs, int *count);

/* Wait for UDP reader to be ready (Phase 1) */
int wbpxre_dht_wait_ready(wbpxre_dht_t *dht, int timeout_sec);

/* Test socket warmup (Phase 4) */
int wbpxre_dht_test_socket(wbpxre_dht_t *dht);

/* Get statistics */
int wbpxre_dht_get_stats(wbpxre_dht_t *dht, wbpxre_stats_t *stats_out);

/* Print statistics */
void wbpxre_dht_print_stats(wbpxre_dht_t *dht);

/* Clear sample_infohashes queue (used during rotation to prevent stale queries)
 * Returns number of items cleared
 */
int wbpxre_dht_clear_sample_queue(wbpxre_dht_t *dht);

/* ============================================================================
 * Protocol Functions (implemented in wbpxre_protocol.c)
 * ============================================================================ */

/* Send ping query */
int wbpxre_protocol_ping(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                         const uint8_t *my_node_id,
                         uint8_t *node_id_out);

/* Send find_node query */
int wbpxre_protocol_find_node(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                              const uint8_t *my_node_id,
                              const uint8_t *target_id,
                              wbpxre_routing_node_t **nodes_out, int *count_out);

/* Send get_peers query */
int wbpxre_protocol_get_peers(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                              const uint8_t *my_node_id,
                              const uint8_t *info_hash,
                              wbpxre_peer_t **peers_out, int *peer_count,
                              wbpxre_routing_node_t **nodes_out, int *node_count,
                              uint8_t *token_out, int *token_len);

/* Send sample_infohashes query (BEP 51) */
int wbpxre_protocol_sample_infohashes(wbpxre_dht_t *dht,
                                       const struct sockaddr_in *addr,
                                       const uint8_t *my_node_id,
                                       const uint8_t *target_id,
                                       uint8_t **hashes_out, int *hash_count,
                                       int *total_num_out, int *interval_out);

/* Encode DHT message to bencoded bytes */
int wbpxre_encode_message(const wbpxre_message_t *msg, uint8_t *buf, int buf_len);

/* Decode bencoded bytes to DHT message */
int wbpxre_decode_message(const uint8_t *buf, int buf_len, wbpxre_message_t *msg);

/* Parse compact node info */
int wbpxre_parse_compact_nodes(const uint8_t *data, int len,
                               wbpxre_routing_node_t **nodes_out);

/* Parse compact peer info */
int wbpxre_parse_compact_peers(const uint8_t *data, int len,
                               wbpxre_peer_t **peers_out);

/* Handle incoming query (responder) */
void wbpxre_handle_incoming_query(wbpxre_dht_t *dht, wbpxre_message_t *query,
                                   const struct sockaddr_in *from);

/* ============================================================================
 * Routing Table Functions (implemented in wbpxre_routing.c)
 * ============================================================================ */

/* Create routing table */
wbpxre_routing_table_t *wbpxre_routing_table_create(int max_nodes);

/* Destroy routing table */
void wbpxre_routing_table_destroy(wbpxre_routing_table_t *table);

/* Insert node into routing table */
int wbpxre_routing_table_insert(wbpxre_routing_table_t *table,
                                 const wbpxre_routing_node_t *node);

/* Get node count */
uint32_t wbpxre_routing_table_count(wbpxre_routing_table_t *table);

/* Find node by ID */
wbpxre_routing_node_t *wbpxre_routing_table_find(wbpxre_routing_table_t *table,
                                                  const uint8_t *node_id);

/* Check if node exists by ID (without allocating memory) */
bool wbpxre_routing_table_exists(wbpxre_routing_table_t *table,
                                  const uint8_t *node_id);

/* Get K closest nodes to target
 * Returns copies of nodes (caller must free each node in nodes_out array) */
int wbpxre_routing_table_get_closest(wbpxre_routing_table_t *table,
                                      const uint8_t *target,
                                      wbpxre_routing_node_t **nodes_out, int k);

/* Get nodes suitable for sample_infohashes
 * Returns copies of nodes (caller must free each node in nodes_out array)
 * current_node_id: Used for keyspace-aware filtering (prioritize close nodes) */
int wbpxre_routing_table_get_sample_candidates(wbpxre_routing_table_t *table,
                                                const uint8_t *current_node_id,
                                                wbpxre_routing_node_t **nodes_out,
                                                int n);

/* Get low-quality nodes (poor response rate)
 * Returns copies of nodes (caller must free each node in nodes_out array) */
int wbpxre_routing_table_get_low_quality_nodes(wbpxre_routing_table_t *table,
                                                 wbpxre_routing_node_t **nodes_out,
                                                 int n, double min_rate, int min_queries);

/* Get oldest nodes (sorted by last_responded_at, oldest first)
 * Returns copies of nodes (caller must free each node in nodes_out array)
 * Used for capacity-based eviction when table is full */
int wbpxre_routing_table_get_oldest_nodes(wbpxre_routing_table_t *table,
                                            wbpxre_routing_node_t **nodes_out,
                                            int n);

/* Get distant nodes (keyspace-aware eviction for rotation)
 * Returns nodes far from current_node_id, prioritized by composite score:
 * - XOR distance from current node_id (60%)
 * - Age since last response (20%)
 * - Response rate quality (20%)
 * Returns copies of nodes (caller must free each node in nodes_out array) */
int wbpxre_routing_table_get_distant_nodes(wbpxre_routing_table_t *table,
                                             const uint8_t *current_node_id,
                                             wbpxre_routing_node_t **nodes_out,
                                             int n);

/* Get distant nodes FAST (optimized for speed, 10-50x faster)
 * Uses first-byte XOR distance only (>128 = distant)
 * Sorts distant nodes by age and returns oldest X%
 * percentage: 0.0-1.0 (e.g., 0.5 = return 50% of distant nodes)
 * Returns copies of nodes (caller must free each node in nodes_out array) */
int wbpxre_routing_table_get_distant_nodes_fast(wbpxre_routing_table_t *table,
                                                  const uint8_t *current_node_id,
                                                  double percentage,
                                                  wbpxre_routing_node_t **nodes_out,
                                                  int max_out);

/* Get keyspace distribution statistics (close vs distant nodes)
 * Used for monitoring keyspace composition after rotation */
void wbpxre_routing_table_get_keyspace_distribution(wbpxre_routing_table_t *table,
                                                      const uint8_t *current_node_id,
                                                      int *close_nodes,
                                                      int *distant_nodes);

/* Update node after successful response */
void wbpxre_routing_table_update_node_responded(wbpxre_routing_table_t *table,
                                                 const uint8_t *node_id);

/* Phase 4: Track query sent to node */
void wbpxre_routing_table_update_node_queried(wbpxre_routing_table_t *table,
                                               const uint8_t *node_id);

/* Update node after sample_infohashes response */
void wbpxre_routing_table_update_sample_response(wbpxre_routing_table_t *table,
                                                  const uint8_t *node_id,
                                                  int discovered_num,
                                                  int total_num,
                                                  int interval);

/* Rebuild hash index to prevent uthash bucket array bloat
 * Clears and rebuilds the node_index_map from the current routing table
 * Returns number of nodes re-indexed */
int wbpxre_routing_table_rebuild_hash_index(wbpxre_routing_table_t *table);

/* ============================================================================
 * Dual Routing Controller Functions (implemented in wbpxre_routing.c)
 * ============================================================================ */

/* Create dual routing controller */
dual_routing_controller_t *dual_routing_controller_create(int max_nodes_per_table,
                                                           double fill_start_threshold,
                                                           double switch_threshold);

/* Destroy dual routing controller */
void dual_routing_controller_destroy(dual_routing_controller_t *controller);

/* Insert node (routes to filling table) */
int dual_routing_insert(dual_routing_controller_t *controller,
                        const wbpxre_routing_node_t *node);

/* Get active table for reading (RCU-protected)
 * Caller must be in RCU read-side critical section */
wbpxre_routing_table_t *dual_routing_get_active(dual_routing_controller_t *controller);

/* Check if node exists in either table */
bool dual_routing_exists(dual_routing_controller_t *controller,
                         const uint8_t *node_id);

/* Check and trigger rotation if thresholds met */
void dual_routing_check_rotation(dual_routing_controller_t *controller);

/* Force rotation (for testing/manual control) */
void dual_routing_force_rotation(dual_routing_controller_t *controller);

/* Update node metadata in both tables */
void dual_routing_update_node_responded(dual_routing_controller_t *controller,
                                         const uint8_t *node_id);

/* Update node as queried in both tables */
void dual_routing_update_node_queried(dual_routing_controller_t *controller,
                                       const uint8_t *node_id);

/* Update sample response in both tables */
void dual_routing_update_sample_response(dual_routing_controller_t *controller,
                                          const uint8_t *node_id,
                                          int discovered_num,
                                          int total_num,
                                          int interval);

/* Get statistics */
void dual_routing_get_stats(dual_routing_controller_t *controller,
                            dual_routing_stats_t *stats_out);

/* Get total node count across both tables */
int dual_routing_get_total_nodes(dual_routing_controller_t *controller);

/* ============================================================================
 * Triple Routing Controller Functions (implemented in wbpxre_routing.c)
 * ============================================================================ */

/* Create triple routing controller */
triple_routing_controller_t *triple_routing_controller_create(
    int max_nodes_per_table,
    uint32_t rotation_threshold,
    int rotation_time_sec
);

/* Destroy triple routing controller */
void triple_routing_controller_destroy(triple_routing_controller_t *ctrl);

/* Insert node into filling table (called by find_node workers) */
int triple_routing_insert_node(
    triple_routing_controller_t *ctrl,
    const wbpxre_routing_node_t *node
);

/* Get stable table for reading (called by sample/DHT workers)
 * Returns NULL if bootstrap incomplete */
wbpxre_routing_table_t *triple_routing_get_stable_table(
    triple_routing_controller_t *ctrl
);

/* Get filling table for reading (bootstrap fallback)
 * Returns NULL if controller is invalid */
wbpxre_routing_table_t *triple_routing_get_filling_table(
    triple_routing_controller_t *ctrl
);

/* Check if bootstrap is complete */
bool triple_routing_is_bootstrapped(triple_routing_controller_t *ctrl);

/* Update node metadata - responded to query */
int triple_routing_update_node_responded(
    triple_routing_controller_t *ctrl,
    const uint8_t *node_id
);

/* Update node metadata - we sent query */
int triple_routing_update_node_queried(
    triple_routing_controller_t *ctrl,
    const uint8_t *node_id
);

/* Update sample_infohashes response metadata */
int triple_routing_update_sample_response(
    triple_routing_controller_t *ctrl,
    const uint8_t *node_id,
    uint32_t sampled_num,
    uint64_t total_num,
    time_t next_sample_time
);

/* Get statistics */
void triple_routing_get_stats(
    triple_routing_controller_t *ctrl,
    triple_routing_stats_t *stats
);

/* Get node ID for stable table (used by sample/get_peers workers)
 * Returns NULL if bootstrap incomplete */
const uint8_t* triple_routing_get_stable_node_id(
    triple_routing_controller_t *ctrl
);

/* Get node ID for filling table (used by find_node workers) */
const uint8_t* triple_routing_get_filling_node_id(
    triple_routing_controller_t *ctrl
);

/* Set DHT context for queue clearing during rotation */
void triple_routing_set_dht_context(
    triple_routing_controller_t *ctrl,
    void *dht_context
);

/* ============================================================================
 * Worker Queue Functions (implemented in wbpxre_worker.c)
 * ============================================================================ */

/* Create work queue */
wbpxre_work_queue_t *wbpxre_queue_create(int capacity);

/* Destroy work queue */
void wbpxre_queue_destroy(wbpxre_work_queue_t *queue);

/* Push item to queue (blocking if full) */
int wbpxre_queue_push(wbpxre_work_queue_t *queue, void *item);

/* Try to push item to queue (non-blocking) */
bool wbpxre_queue_try_push(wbpxre_work_queue_t *queue, void *item);

/* Try to push item to front of queue (non-blocking, for priority items) */
bool wbpxre_queue_try_push_front(wbpxre_work_queue_t *queue, void *item);

/* Pop item from queue (blocking if empty) */
void *wbpxre_queue_pop(wbpxre_work_queue_t *queue);

/* Shutdown queue */
void wbpxre_queue_shutdown(wbpxre_work_queue_t *queue);

/* Clear all items from queue without shutdown */
void wbpxre_queue_clear(wbpxre_work_queue_t *queue);

/* ============================================================================
 * Pending Query Management Functions
 * ============================================================================ */

/* Create pending query */
wbpxre_pending_query_t *wbpxre_create_pending_query(const uint8_t *transaction_id,
                                                      int timeout_sec);

/* Free pending query */
void wbpxre_free_pending_query(wbpxre_pending_query_t *pq);

/* Register pending query */
int wbpxre_register_pending_query(wbpxre_dht_t *dht, wbpxre_pending_query_t *pq);

/* Wait for response */
int wbpxre_wait_for_response(wbpxre_pending_query_t *pq);

/* Find and remove pending query */
wbpxre_pending_query_t *wbpxre_find_and_remove_pending_query(wbpxre_dht_t *dht,
                                                               const uint8_t *transaction_id);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/* Generate random bytes */
void wbpxre_random_bytes(uint8_t *buf, int len);

/* Calculate XOR distance */
void wbpxre_xor_distance(const uint8_t *a, const uint8_t *b, uint8_t *result);

/* Compare distances */
int wbpxre_compare_distance(const uint8_t *dist1, const uint8_t *dist2);

/* Convert sockaddr to string */
const char *wbpxre_addr_to_string(const struct sockaddr_in *addr);

/* Hex dump for debugging */
void wbpxre_hex_dump(const uint8_t *data, int len, const char *label);

#ifdef __cplusplus
}
#endif

#endif /* WBPXRE_DHT_H */
