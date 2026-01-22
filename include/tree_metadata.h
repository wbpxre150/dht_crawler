#ifndef TREE_METADATA_H
#define TREE_METADATA_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/socket.h>

/**
 * Tree Metadata Fetcher - Stage 5
 *
 * BEP9/10 metadata fetch implementation for thread trees.
 * Connects to peers via TCP and fetches torrent metadata.
 */

/* Forward declarations */
struct thread_tree;
struct batch_writer;

/**
 * Torrent metadata structure (simplified for tree architecture)
 */
typedef struct tree_torrent_metadata {
    uint8_t infohash[20];
    char *name;
    int64_t total_size;
    char **files;
    int64_t *file_sizes;
    int file_count;
} tree_torrent_metadata_t;

/**
 * Configuration for metadata fetching
 */
typedef struct tree_metadata_config {
    int tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */
    int metadata_timeout_ms;        /* Overall metadata fetch timeout (default: 30000) */
    int max_metadata_size;          /* Max metadata size in bytes (default: 10MB) */
    struct thread_tree *tree;       /* Tree pointer for connection tracking (optional) */
} tree_metadata_config_t;

/**
 * Shared state for incremental piece collection across peer attempts
 *
 * This structure preserves metadata pieces received from previous peer attempts,
 * enabling retry logic when partially complete. Allocated once by the worker
 * and reused across multiple peer attempts for the same infohash.
 */
typedef struct tree_metadata_state {
    /* Shared buffers (allocated once, preserved across peer attempts) */
    uint8_t *metadata_buffer;       /* Assembled metadata pieces */
    uint8_t *pieces_bitmap;         /* Track received pieces (1 bit per piece) */

    /* Progress tracking */
    int pieces_received;            /* Count of pieces we have */
    int total_pieces;               /* Total pieces needed */
    int metadata_size;              /* Total metadata size in bytes */
    uint8_t ut_metadata_id;         /* Extension ID (may vary per peer) */

    /* Retry logic */
    int *peer_attempts;             /* Track attempts per peer (0=untried, 1=first, 2=second) */
    int round;                      /* Current round (1=first attempt, 2=retry) */
    int max_rounds;                 /* Maximum rounds (2) */
} tree_metadata_state_t;

/**
 * Fetch metadata from a peer
 *
 * @param infohash The infohash to fetch metadata for
 * @param peer Peer address to connect to
 * @param config Fetch configuration (NULL for defaults)
 * @param shared_state Shared state for incremental piece collection (NULL for legacy behavior)
 * @return Allocated metadata structure on success, NULL on failure
 *
 * This function:
 * 1. TCP connects with timeout
 * 2. BitTorrent handshake with extension flag
 * 3. Extended handshake (BEP10)
 * 4. Request metadata pieces (BEP9) - only missing pieces if shared_state provided
 * 5. Assemble pieces, parse info dict
 * 6. Verify infohash matches SHA1 of assembled data
 * 7. Extract name, files, sizes
 *
 * When shared_state is provided, buffers are preserved across peer attempts to enable
 * incremental piece collection. The caller is responsible for freeing shared_state buffers.
 */
tree_torrent_metadata_t *tree_fetch_metadata_from_peer(
    const uint8_t *infohash,
    const struct sockaddr_storage *peer,
    tree_metadata_config_t *config,
    tree_metadata_state_t *shared_state
);

/**
 * Free metadata structure
 *
 * @param meta Metadata structure to free
 */
void tree_free_metadata(tree_torrent_metadata_t *meta);

/**
 * Metadata worker context
 */
typedef struct metadata_worker_ctx {
    struct thread_tree *tree;
    int worker_id;
} metadata_worker_ctx_t;

/**
 * Metadata worker thread function
 *
 * @param arg metadata_worker_ctx_t pointer (will be freed by worker)
 * @return NULL
 */
void *tree_metadata_worker_func(void *arg);

/**
 * Rate monitor context for metadata rate monitoring
 */
typedef struct rate_monitor_ctx {
    struct thread_tree *tree;
    double min_metadata_rate;
    int check_interval_sec;
    int grace_period_sec;
    int min_lifetime_sec;
    bool require_empty_queue;
} rate_monitor_ctx_t;

/**
 * Rate monitor thread function
 *
 * @param arg rate_monitor_ctx_t pointer (will be freed by monitor)
 * @return NULL
 */
void *tree_rate_monitor_func(void *arg);

#endif /* TREE_METADATA_H */
