#ifndef SHARED_NODE_POOL_H
#define SHARED_NODE_POOL_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <netinet/in.h>

// Forward declaration from tree_routing.h
struct tree_node;

/**
 * Shared node pool for global bootstrap
 * Stores discovered DHT nodes that can be sampled by multiple thread trees
 */
typedef struct shared_node_pool {
    struct tree_node *nodes;   // Dynamic array of nodes
    size_t capacity;           // Maximum capacity (e.g., 5000)
    size_t count;              // Current number of nodes
    pthread_mutex_t lock;      // Thread-safe access
} shared_node_pool_t;

/**
 * Create a new shared node pool
 * @param capacity Maximum number of nodes to store
 * @return Pointer to created pool, or NULL on failure
 */
shared_node_pool_t *shared_node_pool_create(size_t capacity);

/**
 * Add a node to the shared pool (thread-safe, with deduplication)
 * @param pool The node pool
 * @param node_id 20-byte DHT node ID
 * @param addr Socket address of the node
 * @return 0 on success, -1 if pool is full or node is duplicate
 */
int shared_node_pool_add_node(shared_node_pool_t *pool,
                              const uint8_t node_id[20],
                              const struct sockaddr_storage *addr);

/**
 * Get random nodes from the pool (thread-safe)
 * @param pool The node pool
 * @param out Output array to store sampled nodes
 * @param count Number of nodes to sample
 * @return Number of nodes actually sampled (may be less than count if pool is small)
 */
int shared_node_pool_get_random(shared_node_pool_t *pool,
                                struct tree_node *out,
                                int count);

/**
 * Get current node count in the pool (thread-safe)
 * @param pool The node pool
 * @return Number of nodes currently in the pool
 */
size_t shared_node_pool_get_count(shared_node_pool_t *pool);

/**
 * Destroy the shared node pool and free resources
 * @param pool The node pool to destroy
 */
void shared_node_pool_destroy(shared_node_pool_t *pool);

#endif // SHARED_NODE_POOL_H
