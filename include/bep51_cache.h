#ifndef BEP51_CACHE_H
#define BEP51_CACHE_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <netinet/in.h>
#include "uthash.h"

/* Forward declarations */
struct shared_node_pool;

/**
 * BEP51 Cache Node
 * Represents a single DHT node that has proven BEP51 capability
 */
typedef struct bep51_cache_node {
    uint8_t node_id[20];              // 20-byte DHT node ID
    struct sockaddr_storage addr;     // Node address (IPv4/IPv6)
    UT_hash_handle hh;                // uthash handle for deduplication
} bep51_cache_node_t;

/**
 * BEP51 Cache
 * Thread-safe cache for BEP51-capable nodes with FIFO eviction
 */
typedef struct bep51_cache {
    bep51_cache_node_t *nodes_hash;   // uthash table for O(1) dedup
    bep51_cache_node_t **nodes_fifo;  // Circular buffer for FIFO eviction
    size_t capacity;                  // Maximum nodes
    size_t count;                     // Current node count
    size_t head_idx;                  // FIFO insertion index
    pthread_mutex_t lock;             // Thread-safe access
} bep51_cache_t;

/**
 * Create a new BEP51 cache
 * @param capacity Maximum number of nodes to cache
 * @return Pointer to created cache, or NULL on failure
 */
bep51_cache_t *bep51_cache_create(size_t capacity);

/**
 * Add a node to the cache (thread-safe, with deduplication and FIFO eviction)
 * @param cache The cache
 * @param node_id 20-byte DHT node ID
 * @param addr Socket address of the node
 * @return 0 on success, -1 on failure
 */
int bep51_cache_add_node(bep51_cache_t *cache,
                         const uint8_t node_id[20],
                         const struct sockaddr_storage *addr);

/**
 * Load cache from binary file
 * @param cache The cache to populate
 * @param path File path to load from
 * @return 0 on success, -1 on failure (strict validation)
 */
int bep51_cache_load_from_file(bep51_cache_t *cache, const char *path);

/**
 * Save cache to binary file (atomic write with temp file + rename)
 * @param cache The cache to save
 * @param path File path to save to
 * @return 0 on success, -1 on failure
 */
int bep51_cache_save_to_file(bep51_cache_t *cache, const char *path);

/**
 * Populate shared_node_pool from cache
 * @param cache The cache
 * @param pool The shared node pool to populate
 * @param max_nodes Maximum nodes to transfer
 * @return Number of nodes transferred
 */
int bep51_cache_populate_shared_pool(bep51_cache_t *cache,
                                     struct shared_node_pool *pool,
                                     int max_nodes);

/**
 * Get current node count in cache (thread-safe)
 * @param cache The cache
 * @return Number of nodes currently cached
 */
size_t bep51_cache_get_count(bep51_cache_t *cache);

/**
 * Destroy the cache and free all resources
 * @param cache The cache to destroy
 */
void bep51_cache_destroy(bep51_cache_t *cache);

#endif // BEP51_CACHE_H
