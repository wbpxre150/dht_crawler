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

/* Default configuration values */
#define WBPXRE_DEFAULT_QUERY_TIMEOUT 5
#define WBPXRE_DEFAULT_TARGET_ROTATION_INTERVAL 10
#define WBPXRE_DEFAULT_SAMPLE_INTERVAL 60
#define WBPXRE_DEFAULT_EMPTY_SAMPLE_PENALTY 300

/* Queue capacities (scaled for 50K routing table) */
#define WBPXRE_DISCOVERED_NODES_CAPACITY 50000
#define WBPXRE_PING_QUEUE_CAPACITY 2000
#define WBPXRE_FIND_NODE_QUEUE_CAPACITY 2000
#define WBPXRE_SAMPLE_INFOHASHES_CAPACITY 10000  /* Increased from 2000 to prevent queue saturation */
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
    /* Node health statistics */
    uint64_t nodes_dropped;
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
} wbpxre_routing_node_t;

/* Routing table */
typedef struct {
    wbpxre_routing_node_t *root;
    pthread_rwlock_t lock;
    int node_count;
    int max_nodes;
    /* Flat array for uniform iteration (eliminates BST traversal bias) */
    wbpxre_routing_node_t **all_nodes;  /* Array of pointers to all nodes */
    int all_nodes_capacity;              /* Capacity of all_nodes array */
    int iteration_offset;                /* Rotating offset for fair iteration */
} wbpxre_routing_table_t;

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
    int max_routing_table_nodes;  /* Maximum nodes in routing table (default: 10000) */

    /* Node health and verification */
    int node_verification_batch_size;  /* Nodes to verify per maintenance cycle (default: 100) */
    int max_node_age_sec;              /* Consider nodes old after this many seconds (default: 120) */
    int node_cleanup_interval_sec;     /* Clean dropped nodes every N seconds (default: 30) */
    double min_node_response_rate;     /* Evict nodes with response rate below this (default: 0.20) */
    int node_quality_min_queries;      /* Minimum queries before judging quality (default: 5) */

    /* Callbacks */
    wbpxre_callback_t callback;
    void *callback_closure;
} wbpxre_config_t;

/* Main DHT context */
typedef struct {
    wbpxre_config_t config;

    /* Node ID protection (for hot rotation) */
    pthread_rwlock_t node_id_lock;

    /* Network */
    int udp_socket;
    struct sockaddr_in bind_addr;

    /* Routing table */
    wbpxre_routing_table_t *routing_table;

    /* Pending queries */
    wbpxre_pending_query_t *pending_queries[256];
    pthread_mutex_t pending_queries_mutex;

    /* Worker queues */
    wbpxre_work_queue_t *discovered_nodes;
    wbpxre_work_queue_t *nodes_for_ping;
    wbpxre_work_queue_t *nodes_for_find_node;
    wbpxre_work_queue_t *nodes_for_sample_infohashes;
    wbpxre_work_queue_t *infohashes_for_get_peers;

    /* Worker threads */
    pthread_t udp_reader_thread;
    pthread_t *ping_worker_threads;
    pthread_t *find_node_worker_threads;
    pthread_t *sample_infohashes_worker_threads;
    pthread_t *get_peers_worker_threads;
    pthread_t discovered_nodes_dispatcher_thread;
    pthread_t target_rotation_thread;
    pthread_t bootstrap_thread;
    pthread_t sample_infohashes_feeder_thread;
    pthread_t find_node_feeder_thread;
    pthread_t ping_feeder_thread;
    pthread_t get_peers_feeder_thread;
    pthread_t maintenance_thread;

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
        /* Node health statistics */
        uint64_t nodes_dropped;
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
int wbpxre_dht_change_node_id(wbpxre_dht_t *dht, const uint8_t *new_node_id);

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

/* ============================================================================
 * Protocol Functions (implemented in wbpxre_protocol.c)
 * ============================================================================ */

/* Send ping query */
int wbpxre_protocol_ping(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                         uint8_t *node_id_out);

/* Send find_node query */
int wbpxre_protocol_find_node(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                              const uint8_t *target_id,
                              wbpxre_routing_node_t **nodes_out, int *count_out);

/* Send get_peers query */
int wbpxre_protocol_get_peers(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                              const uint8_t *info_hash,
                              wbpxre_peer_t **peers_out, int *peer_count,
                              wbpxre_routing_node_t **nodes_out, int *node_count,
                              uint8_t *token_out, int *token_len);

/* Send sample_infohashes query (BEP 51) */
int wbpxre_protocol_sample_infohashes(wbpxre_dht_t *dht,
                                       const struct sockaddr_in *addr,
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

/* Find node by ID */
wbpxre_routing_node_t *wbpxre_routing_table_find(wbpxre_routing_table_t *table,
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

/* Get old nodes that need verification (haven't responded recently)
 * Returns copies of nodes (caller must free each node in nodes_out array) */
int wbpxre_routing_table_get_old_nodes(wbpxre_routing_table_t *table,
                                        wbpxre_routing_node_t **nodes_out,
                                        int n, time_t threshold);

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

/* Drop node from routing table */
void wbpxre_routing_table_drop_node(wbpxre_routing_table_t *table,
                                     const uint8_t *node_id);

/* Batch cleanup of dropped nodes (Phase 3)
 * Returns number of nodes removed */
int wbpxre_routing_table_cleanup_dropped(wbpxre_routing_table_t *table);

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
