#ifndef SHADOW_ROUTING_TABLE_H
#define SHADOW_ROUTING_TABLE_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <sys/socket.h>
#include <uv.h>

/**
 * Shadow routing table for enhanced node tracking and BEP 51 support
 * 
 * Tracks extended metadata about DHT nodes including BEP 51 support,
 * response rates, and query scheduling to improve sample_infohashes performance.
 */

typedef struct {
    unsigned char id[20];
    struct sockaddr_storage addr;
    socklen_t addr_len;
    time_t last_seen;
    time_t last_responded;
    bool bep51_support;          /* Supports BEP 51 sample_infohashes */
    bool bep51_unsupported;      /* PHASE 3: Confirmed does NOT support BEP 51 */
    uint32_t bep51_failures;     /* PHASE 3: Failed BEP 51 query count */
    time_t next_sample_time;     /* Respect interval hints */
    uint32_t samples_discovered; /* Track quality */
    double response_rate;        /* Success rate */
    uint32_t queries_sent;
    uint32_t responses_received;
} node_metadata_t;

typedef struct shadow_routing_table shadow_routing_table_t;

/**
 * Initialize shadow routing table
 * @param capacity Maximum number of nodes to track
 * @param prune_interval_sec How often to prune dead nodes (seconds)
 * @param loop Event loop for timer
 * @return Pointer to shadow table, or NULL on error
 */
shadow_routing_table_t* shadow_table_init(size_t capacity, int prune_interval_sec, uv_loop_t *loop);

/**
 * Add or update node in shadow table
 * @param table Shadow routing table
 * @param node Node metadata
 * @return 0 on success, -1 on error
 */
int shadow_table_upsert(shadow_routing_table_t *table, const node_metadata_t *node);

/**
 * Mark node as responded (update statistics)
 * @param table Shadow routing table
 * @param node_id 20-byte node ID
 * @param samples_count Number of samples received (0 if not BEP 51 response)
 * @param interval Interval hint from BEP 51 response (0 if not applicable)
 * @return 0 on success, -1 if node not found
 */
int shadow_table_mark_responded(shadow_routing_table_t *table, const unsigned char *node_id,
                                uint32_t samples_count, int interval);

/**
 * Get best BEP 51 nodes for sampling
 * @param table Shadow routing table
 * @param out Output array for node metadata
 * @param count Maximum nodes to return
 * @return Number of nodes returned
 */
int shadow_table_get_best_bep51(shadow_routing_table_t *table, node_metadata_t **out, size_t count);

/**
 * Get best BEP 51 nodes for DHT cache persistence
 * Returns high-quality BEP51 nodes sorted by response rate for cache storage
 * @param table Shadow routing table
 * @param out Output array for node metadata pointers
 * @param count Maximum nodes to return
 * @return Number of nodes returned
 */
int shadow_table_get_bep51_for_cache(shadow_routing_table_t *table, node_metadata_t **out, size_t count);

/**
 * Get random nodes for queries
 * @param table Shadow routing table
 * @param out Output array for node metadata
 * @param count Maximum nodes to return
 * @return Number of nodes returned
 */
int shadow_table_get_random(shadow_routing_table_t *table, node_metadata_t **out, size_t count);

/**
 * Mark node as queried (track query sent)
 * @param table Shadow routing table
 * @param node_id 20-byte node ID
 * @return 0 on success, -1 if node not found
 */
int shadow_table_mark_queried(shadow_routing_table_t *table, const unsigned char *node_id);

/**
 * PHASE 3: Mark node as BEP 51 unsupported (after failed queries)
 * @param table Shadow routing table
 * @param node_id 20-byte node ID
 * @return 0 on success, -1 if node not found
 */
int shadow_table_mark_bep51_unsupported(shadow_routing_table_t *table, const unsigned char *node_id);

/**
 * PHASE 3: Increment BEP 51 failure count for node
 * @param table Shadow routing table
 * @param node_id 20-byte node ID
 * @return 0 on success, -1 if node not found
 */
int shadow_table_mark_bep51_failed(shadow_routing_table_t *table, const unsigned char *node_id);

/**
 * Get dubious nodes that need verification
 * Returns nodes that haven't responded recently or have low response rates
 * @param table Shadow routing table
 * @param out Output array for node metadata pointers
 * @param count Maximum nodes to return
 * @param max_age_sec Maximum age in seconds since last response (0 = no age filter)
 * @return Number of nodes returned
 */
int shadow_table_get_dubious(shadow_routing_table_t *table, node_metadata_t **out,
                             size_t count, int max_age_sec);

/**
 * Drop nodes with low response rates (< threshold after min_queries)
 * @param table Shadow routing table
 * @param min_queries Minimum queries before evaluating
 * @param min_response_rate Minimum response rate (0.0-1.0)
 * @return Number of nodes dropped
 */
int shadow_table_drop_low_quality(shadow_routing_table_t *table, uint32_t min_queries, double min_response_rate);

/**
 * Get shadow table statistics
 * @param table Shadow routing table
 * @param out_total Output: total nodes tracked
 * @param out_bep51_nodes Output: nodes with BEP 51 support
 * @param out_capacity Output: maximum capacity
 */
void shadow_table_stats(shadow_routing_table_t *table, size_t *out_total,
                       size_t *out_bep51_nodes, size_t *out_capacity);

/**
 * Save shadow table to file
 * @param table Shadow routing table
 * @param path File path
 * @return 0 on success, -1 on error
 */
int shadow_table_save(shadow_routing_table_t *table, const char *path);

/**
 * Load shadow table from file
 * @param path File path
 * @param prune_interval_sec Prune interval for timer
 * @param loop Event loop for timer
 * @return Pointer to shadow table, or NULL on error
 */
shadow_routing_table_t* shadow_table_load(const char *path, int prune_interval_sec, uv_loop_t *loop);

/**
 * Shutdown shadow table (stop timers)
 * @param table Shadow routing table
 */
void shadow_table_shutdown(shadow_routing_table_t *table);

/**
 * Cleanup and free shadow table
 * @param table Shadow routing table
 */
void shadow_table_cleanup(shadow_routing_table_t *table);

#endif /* SHADOW_ROUTING_TABLE_H */
