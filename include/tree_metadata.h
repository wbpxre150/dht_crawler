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
} tree_metadata_config_t;

/**
 * Fetch metadata from a peer
 *
 * @param infohash The infohash to fetch metadata for
 * @param peer Peer address to connect to
 * @param config Fetch configuration (NULL for defaults)
 * @return Allocated metadata structure on success, NULL on failure
 *
 * This function:
 * 1. TCP connects with timeout
 * 2. BitTorrent handshake with extension flag
 * 3. Extended handshake (BEP10)
 * 4. Request metadata pieces (BEP9)
 * 5. Assemble pieces, parse info dict
 * 6. Verify infohash matches SHA1 of assembled data
 * 7. Extract name, files, sizes
 */
tree_torrent_metadata_t *tree_fetch_metadata_from_peer(
    const uint8_t *infohash,
    const struct sockaddr_storage *peer,
    tree_metadata_config_t *config
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
 * Rate monitor context
 */
typedef struct rate_monitor_ctx {
    struct thread_tree *tree;
    double min_metadata_rate;
    int check_interval_sec;
    int grace_period_sec;
    int min_lifetime_sec;           /* Minimum lifetime before rate checks apply */
    bool require_empty_queue;       /* Only shutdown if queue is empty */
} rate_monitor_ctx_t;

/**
 * Metadata worker thread function
 *
 * @param arg metadata_worker_ctx_t pointer (will be freed by worker)
 * @return NULL
 */
void *tree_metadata_worker_func(void *arg);

/**
 * Rate monitor thread function
 *
 * @param arg rate_monitor_ctx_t pointer (will be freed by monitor)
 * @return NULL
 */
void *tree_rate_monitor_func(void *arg);

#endif /* TREE_METADATA_H */
