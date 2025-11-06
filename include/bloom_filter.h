#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Bloom filter wrapper for duplicate info_hash detection
 *
 * Provides 90% reduction in database queries by maintaining
 * an in-memory probabilistic data structure for seen hashes.
 *
 * Thread Safety: All functions are thread-safe and use internal
 * mutex synchronization. Safe for concurrent access from multiple threads.
 */

typedef struct bloom_filter bloom_filter_t;

/**
 * Initialize bloom filter
 * @param capacity Expected number of elements (e.g., 10000000)
 * @param error_rate Desired false positive rate (e.g., 0.001 for 0.1%)
 * @return Pointer to bloom filter, or NULL on error
 *
 * Thread-safe: Initializes internal mutex for synchronization
 */
bloom_filter_t* bloom_filter_init(uint64_t capacity, double error_rate);

/**
 * Add an info_hash to the bloom filter
 * @param filter Bloom filter instance
 * @param hash 20-byte info_hash
 *
 * Thread-safe: Uses internal mutex for synchronization
 */
void bloom_filter_add(bloom_filter_t *filter, const unsigned char *hash);

/**
 * Check if an info_hash might be in the set
 * @param filter Bloom filter instance
 * @param hash 20-byte info_hash
 * @return true if possibly in set (may be false positive), false if definitely not in set
 *
 * Thread-safe: Uses internal mutex for synchronization
 */
bool bloom_filter_check(bloom_filter_t *filter, const unsigned char *hash);

/**
 * Reset bloom filter (clear all entries)
 * @param filter Bloom filter instance
 *
 * Thread-safe: Uses internal mutex for synchronization
 */
void bloom_filter_reset(bloom_filter_t *filter);

/**
 * Get statistics about bloom filter
 * @param filter Bloom filter instance
 * @param out_capacity Output: configured capacity
 * @param out_error_rate Output: configured error rate
 * @param out_bytes Output: memory usage in bytes
 *
 * Thread-safe: Uses internal mutex for synchronization
 */
void bloom_filter_stats(bloom_filter_t *filter, uint64_t *out_capacity,
                       double *out_error_rate, uint64_t *out_bytes);

/**
 * Save bloom filter to file
 * @param filter Bloom filter instance
 * @param path File path
 * @return 0 on success, -1 on error
 *
 * Thread-safe: Uses internal mutex to ensure consistent snapshot.
 * Should only be called during shutdown after all worker threads have stopped.
 */
int bloom_filter_save(bloom_filter_t *filter, const char *path);

/**
 * Load bloom filter from file
 * @param path File path
 * @return Pointer to bloom filter, or NULL on error
 *
 * Thread-safe: Initializes internal mutex for the loaded filter
 */
bloom_filter_t* bloom_filter_load(const char *path);

/**
 * Cleanup and free bloom filter
 * @param filter Bloom filter instance
 *
 * Thread-safe: Should only be called after all other threads have stopped
 * accessing the filter. Destroys internal mutex.
 */
void bloom_filter_cleanup(bloom_filter_t *filter);

#endif /* BLOOM_FILTER_H */
