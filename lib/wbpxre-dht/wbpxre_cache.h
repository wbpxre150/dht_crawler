/*
 * wbpxre-dht: Per-Worker Routing Table Cache
 * Reduces lock contention by caching closest-K queries in thread-local storage
 */

#ifndef WBPXRE_CACHE_H
#define WBPXRE_CACHE_H

#include "wbpxre_dht.h"
#include <time.h>
#include <stdatomic.h>

/* Cache configuration */
#define WBPXRE_CACHE_SLOTS 128        /* Number of targets to cache */
#define WBPXRE_CACHE_NODES_PER_SLOT 8 /* K-closest nodes per target */
#define WBPXRE_CACHE_TTL_SEC 10       /* Cache entry lifetime */

/* Cached routing table entry */
typedef struct {
    uint8_t target[WBPXRE_NODE_ID_LEN];  /* Target node ID (cache key) */
    wbpxre_routing_node_t *nodes[WBPXRE_CACHE_NODES_PER_SLOT]; /* Cached nodes */
    int node_count;                       /* Number of valid nodes */
    time_t cached_at;                     /* When this was cached */
    time_t last_accessed;                 /* For LRU eviction */
    bool valid;                           /* Is this slot occupied? */
} wbpxre_cache_entry_t;

/* Thread-local routing table cache */
typedef struct wbpxre_routing_cache {
    wbpxre_cache_entry_t entries[WBPXRE_CACHE_SLOTS];

    /* Statistics */
    uint64_t hits;
    uint64_t misses;
    uint64_t evictions;
} wbpxre_routing_cache_t;

/* Forward declaration */
struct wbpxre_dht;

/* Cache operations */
wbpxre_routing_cache_t *wbpxre_cache_create(void);
void wbpxre_cache_destroy(wbpxre_routing_cache_t *cache);

/* Lookup cached nodes for target (returns -1 if miss or expired, otherwise number of nodes) */
int wbpxre_cache_lookup(wbpxre_routing_cache_t *cache,
                        const uint8_t *target,
                        wbpxre_routing_node_t **nodes_out,
                        int k,
                        struct wbpxre_dht *dht);

/* Insert nodes into cache */
void wbpxre_cache_insert(wbpxre_routing_cache_t *cache,
                         const uint8_t *target,
                         wbpxre_routing_node_t **nodes,
                         int node_count);

/* Clear expired entries */
void wbpxre_cache_cleanup_expired(wbpxre_routing_cache_t *cache);

/* Get cache statistics */
void wbpxre_cache_get_stats(wbpxre_routing_cache_t *cache,
                            uint64_t *hits, uint64_t *misses);

#endif /* WBPXRE_CACHE_H */
