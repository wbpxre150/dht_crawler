#ifndef PEER_RETRY_TRACKER_H
#define PEER_RETRY_TRACKER_H

#include <stdint.h>
#include <pthread.h>
#include <time.h>

/* Retry tracking entry for a single info_hash */
typedef struct peer_retry_entry {
    uint8_t info_hash[20];
    int attempts_made;           /* Number of get_peers attempts so far */
    int peer_count;              /* Total peers discovered */
    int query_in_progress;       /* 1 if waiting for SEARCH_DONE */
    time_t last_attempt_time;    /* Timestamp of last attempt */
    time_t target_retry_time;    /* When this entry should be retried (0 if not scheduled) */
    time_t created_at;           /* When entry was created */
    size_t array_index;          /* Position in entries array */
    int valid;                   /* 1 if entry is valid, 0 if evicted/empty */
    struct peer_retry_entry *hash_next; /* Hash table chaining */
} peer_retry_entry_t;

/* Retry tracker store */
typedef struct {
    /* Hash table for O(1) lookups */
    peer_retry_entry_t **buckets;
    size_t bucket_count;

    /* Circular buffer for O(1) eviction */
    peer_retry_entry_t *entries;     /* Fixed-size array of entries */
    size_t max_entries;              /* Maximum entries (e.g., 50000) */
    size_t write_pos;                /* Next write position in circular buffer */
    size_t current_count;            /* Current number of valid entries */

    pthread_mutex_t mutex;

    /* Configuration */
    int max_attempts;            /* Max retry attempts (default: 3) */
    int min_peer_threshold;      /* Minimum peers before stopping retries (default: 10) */
    int retry_delay_ms;          /* Delay between retries (default: 500ms) */
    int max_age_sec;             /* Max age before cleanup (default: 60s) */

    /* Statistics */
    uint64_t total_entries;
    uint64_t retries_triggered;
    uint64_t success_first_try;
    uint64_t success_second_try;
    uint64_t success_third_try;
    uint64_t failed_all_attempts;
    uint64_t skipped_queue_full;
    uint64_t evicted_for_space;      /* Entries evicted to make room */
} peer_retry_tracker_t;

/* Function declarations */

/* Initialize retry tracker
 * bucket_count: Number of hash table buckets (recommended: 1009)
 * max_entries: Maximum entries in circular buffer (e.g., 50000)
 * max_attempts: Maximum retry attempts per info_hash (1-5)
 * min_peer_threshold: Minimum peers before stopping retries
 * retry_delay_ms: Delay between retry attempts in milliseconds
 * max_age_sec: Maximum age before cleanup
 */
peer_retry_tracker_t* peer_retry_tracker_init(size_t bucket_count,
                                               size_t max_entries,
                                               int max_attempts,
                                               int min_peer_threshold,
                                               int retry_delay_ms,
                                               int max_age_sec);

/* Create entry for new info_hash
 * Returns existing entry if already exists, new entry otherwise
 * If at capacity, evicts oldest entry automatically
 */
peer_retry_entry_t* peer_retry_entry_create(peer_retry_tracker_t *tracker,
                                             const uint8_t *info_hash);

/* Find existing entry by info_hash
 * Returns NULL if not found
 */
peer_retry_entry_t* peer_retry_entry_find(peer_retry_tracker_t *tracker,
                                           const uint8_t *info_hash);

/* Check if should retry for more peers
 * Returns 1 if should retry, 0 otherwise
 */
int peer_retry_should_retry(peer_retry_tracker_t *tracker,
                             const uint8_t *info_hash,
                             int current_peer_count);

/* Mark entry as complete and remove from tracker
 * Also updates statistics based on attempt count
 */
void peer_retry_mark_complete(peer_retry_tracker_t *tracker,
                               const uint8_t *info_hash,
                               int final_peer_count);

/* Cleanup old entries (older than max_age_sec)
 * Returns number of entries removed
 */
int peer_retry_cleanup_old(peer_retry_tracker_t *tracker);

/* Get entries that are ready for retry (target_retry_time <= now)
 * Fills info_hashes array with up to max_count ready entries
 * Returns actual number of entries found
 */
int peer_retry_get_ready_entries(peer_retry_tracker_t *tracker,
                                  uint8_t (*info_hashes)[20],
                                  int max_count);

/* Clear all active retry entries (used during node ID rotation)
 * Returns number of entries cleared
 */
int peer_retry_tracker_clear_all(peer_retry_tracker_t *tracker);

/* Print statistics */
void peer_retry_print_stats(peer_retry_tracker_t *tracker);

/* Cleanup and free tracker */
void peer_retry_tracker_cleanup(peer_retry_tracker_t *tracker);

#endif /* PEER_RETRY_TRACKER_H */
