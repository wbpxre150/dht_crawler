/*
 * wbpxre-dht: Per-Worker Routing Table Cache Implementation
 */

#include "wbpxre_cache.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

wbpxre_routing_cache_t *wbpxre_cache_create(void) {
    wbpxre_routing_cache_t *cache = calloc(1, sizeof(wbpxre_routing_cache_t));
    if (!cache) return NULL;

    /* Initialize all entries as invalid */
    for (int i = 0; i < WBPXRE_CACHE_SLOTS; i++) {
        cache->entries[i].valid = false;
        cache->entries[i].node_count = 0;
    }

    cache->hits = 0;
    cache->misses = 0;
    cache->evictions = 0;

    return cache;
}

void wbpxre_cache_destroy(wbpxre_routing_cache_t *cache) {
    if (!cache) return;

    /* Free all cached node copies */
    for (int i = 0; i < WBPXRE_CACHE_SLOTS; i++) {
        if (cache->entries[i].valid) {
            for (int j = 0; j < cache->entries[i].node_count; j++) {
                free(cache->entries[i].nodes[j]);
            }
        }
    }

    free(cache);
}

/* Simple hash function for target ID */
static uint32_t hash_target(const uint8_t *target) {
    uint32_t hash = 0;
    for (int i = 0; i < WBPXRE_NODE_ID_LEN; i++) {
        hash = hash * 31 + target[i];
    }
    return hash % WBPXRE_CACHE_SLOTS;
}

int wbpxre_cache_lookup(wbpxre_routing_cache_t *cache,
                        const uint8_t *target,
                        wbpxre_routing_node_t **nodes_out,
                        int k,
                        wbpxre_dht_t *dht) {
    if (!cache || !target || !nodes_out) return -1;

    uint32_t slot = hash_target(target);
    wbpxre_cache_entry_t *entry = &cache->entries[slot];

    /* Check if entry is valid and matches target */
    if (!entry->valid ||
        memcmp(entry->target, target, WBPXRE_NODE_ID_LEN) != 0) {
        cache->misses++;
        if (dht) atomic_fetch_add(&dht->cache_misses, 1);
        return -1; /* Cache miss */
    }

    /* Check if entry has expired */
    time_t now = time(NULL);
    if (now - entry->cached_at > WBPXRE_CACHE_TTL_SEC) {
        /* Expired - invalidate and return miss */
        entry->valid = false;
        for (int i = 0; i < entry->node_count; i++) {
            free(entry->nodes[i]);
            entry->nodes[i] = NULL;
        }
        entry->node_count = 0;
        cache->misses++;
        if (dht) atomic_fetch_add(&dht->cache_misses, 1);
        return -1;
    }

    /* Cache hit! */
    cache->hits++;
    if (dht) atomic_fetch_add(&dht->cache_hits, 1);
    entry->last_accessed = now;

    /* Copy cached nodes to output (allocate new copies) */
    int result_count = entry->node_count < k ? entry->node_count : k;
    for (int i = 0; i < result_count; i++) {
        nodes_out[i] = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(nodes_out[i], entry->nodes[i], sizeof(wbpxre_routing_node_t));
        nodes_out[i]->left = NULL;
        nodes_out[i]->right = NULL;
    }

    return result_count;
}

void wbpxre_cache_insert(wbpxre_routing_cache_t *cache,
                         const uint8_t *target,
                         wbpxre_routing_node_t **nodes,
                         int node_count) {
    if (!cache || !target || !nodes) return;

    uint32_t slot = hash_target(target);
    wbpxre_cache_entry_t *entry = &cache->entries[slot];

    /* If slot is occupied, evict old entry */
    if (entry->valid) {
        cache->evictions++;
        for (int i = 0; i < entry->node_count; i++) {
            free(entry->nodes[i]);
        }
    }

    /* Insert new entry */
    memcpy(entry->target, target, WBPXRE_NODE_ID_LEN);
    entry->node_count = node_count < WBPXRE_CACHE_NODES_PER_SLOT ?
                        node_count : WBPXRE_CACHE_NODES_PER_SLOT;

    /* Copy nodes (allocate new copies) */
    for (int i = 0; i < entry->node_count; i++) {
        entry->nodes[i] = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(entry->nodes[i], nodes[i], sizeof(wbpxre_routing_node_t));
        entry->nodes[i]->left = NULL;
        entry->nodes[i]->right = NULL;
    }

    time_t now = time(NULL);
    entry->cached_at = now;
    entry->last_accessed = now;
    entry->valid = true;
}

void wbpxre_cache_cleanup_expired(wbpxre_routing_cache_t *cache) {
    if (!cache) return;

    time_t now = time(NULL);

    for (int i = 0; i < WBPXRE_CACHE_SLOTS; i++) {
        wbpxre_cache_entry_t *entry = &cache->entries[i];

        if (entry->valid && now - entry->cached_at > WBPXRE_CACHE_TTL_SEC) {
            /* Expired - free and invalidate */
            for (int j = 0; j < entry->node_count; j++) {
                free(entry->nodes[j]);
                entry->nodes[j] = NULL;
            }
            entry->node_count = 0;
            entry->valid = false;
        }
    }
}

void wbpxre_cache_get_stats(wbpxre_routing_cache_t *cache,
                            uint64_t *hits, uint64_t *misses) {
    if (!cache) return;
    if (hits) *hits = cache->hits;
    if (misses) *misses = cache->misses;
}
