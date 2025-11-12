#ifndef METADATA_FETCHER_H
#define METADATA_FETCHER_H

#include "dht_crawler.h"
#include "config.h"
#include "database.h"
#include "infohash_queue.h"
#include "peer_store.h"
#include "connection_request_queue.h"
#include <uv.h>

/* BitTorrent protocol constants */
#define BT_PROTOCOL_STRING "BitTorrent protocol"
#define BT_PROTOCOL_LEN 19
#define BT_HANDSHAKE_SIZE 68
#define BT_RESERVED_BYTES_SIZE 8
#define BT_METADATA_PIECE_SIZE 16384  /* 16 KiB */
#define MAX_BT_MESSAGE_SIZE (32 * 1024 * 1024)  /* 32 MB max message */
#define MAX_METADATA_SIZE (100 * 1024 * 1024)   /* 100 MB max metadata */
#define CONNECTION_TIMEOUT_MS 6000               /* 6 second timeout (matches bitmagnet) */
#define MAX_PEERS_PER_INFOHASH 50                /* Max peers to try */

/* Message types */
#define BT_MSG_CHOKE 0
#define BT_MSG_UNCHOKE 1
#define BT_MSG_INTERESTED 2
#define BT_MSG_NOT_INTERESTED 3
#define BT_MSG_HAVE 4
#define BT_MSG_BITFIELD 5
#define BT_MSG_REQUEST 6
#define BT_MSG_PIECE 7
#define BT_MSG_CANCEL 8
#define BT_MSG_PORT 9
#define BT_MSG_EXTENDED 20

/* Extended message types (BEP 10) */
#define BT_EXT_HANDSHAKE 0
#define OUR_UT_METADATA_ID 1  /* Our extension ID for ut_metadata */

/* ut_metadata message types (BEP 9) */
#define UT_METADATA_REQUEST 0
#define UT_METADATA_DATA 1
#define UT_METADATA_REJECT 2

/* Peer connection states */
typedef enum {
    PEER_STATE_CONNECTING,
    PEER_STATE_HANDSHAKING,
    PEER_STATE_EXTENDED_HANDSHAKE,
    PEER_STATE_REQUESTING_METADATA,
    PEER_STATE_RECEIVING_METADATA,
    PEER_STATE_COMPLETE,
    PEER_STATE_FAILED
} peer_state_t;

/* Peer connection context */
typedef struct peer_connection {
    uv_tcp_t tcp;
    uv_connect_t connect_req;
    uv_timer_t timeout_timer;

    uint8_t info_hash[20];
    peer_state_t state;

    /* BitTorrent protocol state */
    uint8_t peer_id[20];
    uint8_t reserved[8];
    int ut_metadata_id;  /* Extension message ID */
    int metadata_size;   /* Total metadata size */

    /* Metadata assembly */
    uint8_t *metadata_buffer;
    int metadata_pieces_received;
    int total_pieces;
    uint8_t *pieces_received;  /* Bitmap of received pieces */

    /* Buffer for incoming data */
    char recv_buffer[65536];
    size_t recv_offset;

    /* Metadata fetcher reference */
    void *fetcher;

    /* Retry tracking */
    int retry_count;
    time_t start_time;

    /* Activity-based timeout tracking */
    time_t last_activity_time;      /* Timestamp of last data reception */
    time_t connection_start_time;   /* Timestamp when connection started */

    /* Async close handling */
    int closed;                /* Flag to prevent double-close */
    int handles_to_close;      /* Reference count for async close */
    int tcp_initialized;       /* Track if TCP handle initialized */
    int timer_initialized;     /* Track if timer handle initialized */
    char close_reason[64];     /* Reason for closing */

    struct peer_connection *next;
} peer_connection_t;

/* Forward declarations */
typedef struct worker_pool worker_pool_t;
typedef struct batch_writer batch_writer_t;

/* Retry queue entry */
typedef struct retry_entry {
    uint8_t info_hash[20];
    time_t next_retry_time;
    int retry_count;
    int consecutive_no_peers;  /* Track consecutive no-peer failures */
    struct retry_entry *next;
} retry_entry_t;

/* Retry queue structure */
typedef struct {
    retry_entry_t *head;
    retry_entry_t *tail;
    uv_mutex_t mutex;
    size_t count;
    size_t max_retries;        /* Max retry attempts (default: 3) */
    int retry_delays[3];       /* Backoff delays in seconds: [60, 300, 900] */
} retry_queue_t;

/* Infohash attempt tracking - aggregates per-connection failures to per-infohash outcomes */
typedef struct infohash_attempt {
    uint8_t info_hash[20];
    time_t first_attempt_time;
    time_t last_attempt_time;

    /* Peer queue for sequential attempts - try all available peers until success or exhaustion */
    struct sockaddr_storage *available_peers;
    socklen_t *available_peer_lens;
    int total_peer_count;
    int peers_tried;
    int max_concurrent_for_this_hash;  /* How many concurrent connections allowed */

    int total_connections_tried;
    int connections_failed;
    int connections_timeout;
    int handshake_failed;
    int no_metadata_support;
    int metadata_rejected;
    int hash_mismatch;
    int peer_count_at_start;
    peer_state_t furthest_state;  /* Track how far we got */
    int active_connections;       /* How many connections still active */
    struct infohash_attempt *next;
} infohash_attempt_t;

/* Failure classification for retry decisions */
typedef enum {
    FAILURE_RETRIABLE,      /* Should retry later */
    FAILURE_PERMANENT,      /* Don't retry */
    FAILURE_NO_PEERS        /* Special case: discard immediately */
} failure_classification_t;

/* Forward declaration - avoid including dht_manager.h to prevent circular dependencies */
struct dht_manager;

/* Metadata fetcher manager */
typedef struct {
    app_context_t *app_ctx;
    infohash_queue_t *queue;
    database_t *database;
    peer_store_t *peer_store;  /* Peer store for fetching peer addresses */
    struct dht_manager *dht_manager; /* DHT manager for re-querying peers on retry */

    /* Worker pool for concurrent fetching */
    worker_pool_t *worker_pool;
    uv_thread_t feeder_thread;
    int num_workers;
    uv_mutex_t mutex;

    /* Configuration */
    int max_concurrent_per_infohash;  /* Max connections per infohash (default: 5) */
    int max_global_connections;       /* Max global connections (default: 2000) */
    int connection_timeout_ms;        /* Idle timeout in milliseconds - resets on activity */
    int max_connection_lifetime_ms;   /* Max total connection time (0=unlimited) */

    /* Retry logic for info_hashes with no peers */
    retry_queue_t *retry_queue;
    uv_thread_t retry_thread;
    int retry_enabled;

    /* Batch writer for high-throughput database writes */
    batch_writer_t *batch_writer;

    /* Async handle for cross-thread communication */
    uv_async_t async_handle;

    /* Queue for connection requests from worker threads */
    connection_request_queue_t *conn_request_queue;

    /* Active connections */
    peer_connection_t *active_connections;
    int active_count;
    int max_concurrent;

    /* Async callback state management to prevent event loop starvation */
    int connection_queue_blocked;      /* Flag: requests were dropped due to capacity */

    /* Infohash attempt tracking - hash table for aggregating per-infohash failures */
    infohash_attempt_t *attempt_table[1024];  /* Hash table (1024 buckets) */
    uv_mutex_t attempt_table_mutex;

    /* Statistics - detailed failure tracking */
    uint64_t total_attempts;          /* Worker tasks processed */
    uint64_t no_peers_found;          /* Info_hashes with no peers in store */
    uint64_t connection_initiated;     /* TCP connections attempted */
    uint64_t connection_failed;        /* TCP connections that failed */
    uint64_t connection_timeout;       /* Connections that timed out */
    uint64_t handshake_failed;         /* Handshake failures */
    uint64_t no_metadata_support;      /* Peers without ut_metadata */
    uint64_t metadata_rejected;        /* Peers that rejected requests */
    uint64_t hash_mismatch;            /* SHA-1 verification failures */
    uint64_t total_fetched;            /* Successfully fetched */
    uint64_t total_failed;             /* Total failures (legacy, may be removed) */
    uint64_t retry_queue_added;        /* Info_hashes added to retry queue */
    uint64_t retry_attempts;           /* Retry attempts made */
    uint64_t retry_submitted;          /* Retries submitted to worker pool for fetch */
    uint64_t retry_abandoned;          /* Retries abandoned after max attempts */
    uint64_t retriable_failures;       /* Failures that were retried */
    uint64_t permanent_failures;       /* Failures that weren't retried */
    uint64_t no_peer_discards;         /* Infohashes discarded due to no peers */
    uint64_t discarded_failures;       /* All failures discarded when retry disabled */

    int running;
} metadata_fetcher_t;

/* Fetched metadata result */
typedef struct {
    uint8_t info_hash[20];
    char *name;
    int64_t total_size;
    file_info_t *files;
    int num_files;
    int32_t piece_length;
    int32_t num_pieces;
} metadata_result_t;

/* Function declarations */
int metadata_fetcher_init(metadata_fetcher_t *fetcher, app_context_t *app_ctx,
                          infohash_queue_t *queue, database_t *database,
                          peer_store_t *peer_store, crawler_config_t *config);
int metadata_fetcher_start(metadata_fetcher_t *fetcher);
void metadata_fetcher_stop(metadata_fetcher_t *fetcher);
void metadata_fetcher_cleanup(metadata_fetcher_t *fetcher);
void metadata_fetcher_set_dht_manager(metadata_fetcher_t *fetcher, struct dht_manager *dht_manager);
void metadata_fetcher_set_bloom_filter(metadata_fetcher_t *fetcher, bloom_filter_t *bloom, const char *bloom_path);

/* Internal functions */
void metadata_worker_thread(void *arg);
int fetch_metadata_from_dht(metadata_fetcher_t *fetcher, const uint8_t *info_hash);
void free_metadata_result(metadata_result_t *result);

#endif /* METADATA_FETCHER_H */
