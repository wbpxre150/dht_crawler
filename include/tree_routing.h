#ifndef TREE_ROUTING_H
#define TREE_ROUTING_H

#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "../lib/uthash/src/uthash.h"

/**
 * Private routing table for thread trees
 *
 * Design: Single routing table per thread tree with read-write mutex locking.
 * - Each thread tree has its own isolated routing table
 * - Read operations use read locks (allow concurrent readers)
 * - Write operations use write locks (exclusive access)
 * - No RCU, no triple buffering - simple and reliable
 */

/* BEP51 capability status for routing table nodes */
typedef enum {
    BEP51_UNKNOWN = 0,    /* Not yet tested */
    BEP51_CAPABLE = 1,    /* Confirmed BEP51 support */
    BEP51_INCAPABLE = 2   /* Confirmed no BEP51 support */
} bep51_status_t;

/* Node in the routing table */
typedef struct tree_node {
    uint8_t node_id[20];
    struct sockaddr_storage addr;
    time_t last_seen;
    int fail_count;
    bep51_status_t bep51_status;  /* BEP51 capability tracking */
    struct tree_node *next;  /* For bucket linked list */
    UT_hash_handle hh_flat;  /* uthash handle for flat index (fast iteration) */
} tree_node_t;

/* Bucket in the routing table (k-bucket) */
typedef struct tree_bucket {
    tree_node_t *nodes;
    int count;
    int max_nodes;  /* k = 8 typically */
} tree_bucket_t;

/* Hash map entry: node_id → pointer to tree_node_t in bucket */
typedef struct tree_node_hash_entry {
    uint8_t node_id[20];        /* Key (20 bytes) */
    tree_node_t *node_ptr;      /* Value: pointer to node in bucket linked list */
    UT_hash_handle hh;          /* uthash handle */
} tree_node_hash_entry_t;

/* Complete routing table */
typedef struct tree_routing_table {
    uint8_t our_node_id[20];
    tree_bucket_t buckets[160];  /* 160-bit address space */
    int total_nodes;
    pthread_rwlock_t rwlock;  /* Read-write lock: concurrent reads, exclusive writes */

    /* Hash map for O(1) lookups */
    tree_node_hash_entry_t *node_hash;  /* node_id → tree_node_t* (for updates/lookups) */

    /* Flat index for fast iteration (avoids bucket traversal) */
    tree_node_t *flat_index_head;  /* node_id → tree_node_t* (for iteration) */
} tree_routing_table_t;

/**
 * Create a new routing table
 * @param our_node_id Our 20-byte node ID
 * @return Pointer to routing table, or NULL on error
 */
tree_routing_table_t *tree_routing_create(const uint8_t *our_node_id);

/**
 * Destroy a routing table and free all memory
 * @param rt Routing table to destroy
 */
void tree_routing_destroy(tree_routing_table_t *rt);

/**
 * Add or update a node in the routing table.
 * Use this when we DIRECTLY received a response from this node.
 * Updates last_seen for existing nodes (proves node is alive).
 * @param rt Routing table
 * @param node_id 20-byte node ID to add
 * @param addr Address of the node
 * @return 0 on success, -1 on error
 */
int tree_routing_add_node(tree_routing_table_t *rt, const uint8_t *node_id,
                          const struct sockaddr_storage *addr);

/**
 * Add a node only if it doesn't already exist (no update if exists).
 * Use this for nodes we "heard about" in someone else's response.
 * Does NOT update last_seen for existing nodes (we didn't talk to them).
 * This allows LRU eviction to work properly for stale nodes.
 * @param rt Routing table
 * @param node_id 20-byte node ID to add
 * @param addr Address of the node
 * @return 0 on success (added or already exists), -1 on error
 */
int tree_routing_add_node_if_new(tree_routing_table_t *rt, const uint8_t *node_id,
                                  const struct sockaddr_storage *addr);

/**
 * Get closest nodes to a target ID
 * @param rt Routing table
 * @param target 20-byte target ID
 * @param out Array to fill with nodes (caller allocates)
 * @param count Max number of nodes to return
 * @return Number of nodes returned
 */
int tree_routing_get_closest(tree_routing_table_t *rt, const uint8_t *target,
                              tree_node_t *out, int count);

/**
 * Get random nodes from the routing table
 * @param rt Routing table
 * @param out Array to fill with nodes (caller allocates)
 * @param count Max number of nodes to return
 * @return Number of nodes returned
 */
int tree_routing_get_random_nodes(tree_routing_table_t *rt,
                                   tree_node_t *out, int count);

/**
 * Mark a node as failed (increment fail count, evict if too many failures)
 * @param rt Routing table
 * @param node_id 20-byte node ID to mark as failed
 */
void tree_routing_mark_failed(tree_routing_table_t *rt, const uint8_t *node_id);

/**
 * Get total node count
 * @param rt Routing table
 * @return Number of nodes in the routing table
 */
int tree_routing_get_count(tree_routing_table_t *rt);

/**
 * Set bucket capacity (for bootstrap mode)
 * @param rt Routing table
 * @param capacity New max_nodes per bucket (default 8, bootstrap 20)
 */
void tree_routing_set_bucket_capacity(tree_routing_table_t *rt, int capacity);

/**
 * Get random nodes that are known to be BEP51-capable.
 * Uses reservoir sampling like get_random_nodes() but filters to BEP51_CAPABLE only.
 * @param rt Routing table
 * @param out Array to fill with nodes (caller allocates)
 * @param count Max number of nodes to return
 * @return Number of nodes returned (may be less than count if not enough capable nodes)
 */
int tree_routing_get_random_bep51_nodes(tree_routing_table_t *rt,
                                         tree_node_t *out, int count);

/**
 * Mark a node as BEP51-capable (responded to sample_infohashes).
 * O(1) hash lookup by node_id.
 * @param rt Routing table
 * @param node_id 20-byte node ID to mark
 */
void tree_routing_mark_bep51_capable(tree_routing_table_t *rt,
                                      const uint8_t *node_id);

/**
 * Mark a node as BEP51-incapable (timeout/error on sample_infohashes).
 * O(1) hash lookup by node_id.
 * @param rt Routing table
 * @param node_id 20-byte node ID to mark
 */
void tree_routing_mark_bep51_incapable(tree_routing_table_t *rt,
                                        const uint8_t *node_id);

/**
 * Get count of BEP51-capable nodes in the routing table.
 * @param rt Routing table
 * @return Number of nodes with bep51_status == BEP51_CAPABLE
 */
int tree_routing_get_bep51_capable_count(tree_routing_table_t *rt);

#endif /* TREE_ROUTING_H */
