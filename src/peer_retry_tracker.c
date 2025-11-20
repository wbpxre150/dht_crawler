#include "peer_retry_tracker.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>

/* Hash function for info_hash */
static size_t hash_info_hash(const uint8_t *info_hash, size_t bucket_count) {
    uint64_t h = 0;
    for (int i = 0; i < 8 && i < 20; i++) {
        h = (h << 8) | info_hash[i];
    }
    return h % bucket_count;
}

/* Compare two info_hashes */
static int hash_equal(const uint8_t *h1, const uint8_t *h2) {
    return memcmp(h1, h2, 20) == 0;
}

/* Remove entry from hash table (internal helper) */
static void remove_from_hash_table(peer_retry_tracker_t *tracker, peer_retry_entry_t *entry) {
    if (!entry->valid) return;

    size_t bucket_idx = hash_info_hash(entry->info_hash, tracker->bucket_count);
    peer_retry_entry_t **prev = &tracker->buckets[bucket_idx];
    peer_retry_entry_t *curr = tracker->buckets[bucket_idx];

    while (curr) {
        if (curr == entry) {
            *prev = curr->hash_next;
            entry->valid = 0;
            entry->hash_next = NULL;
            if (tracker->current_count > 0) {
                tracker->current_count--;
            }
            return;
        }
        prev = &curr->hash_next;
        curr = curr->hash_next;
    }
}

/* Initialize retry tracker */
peer_retry_tracker_t* peer_retry_tracker_init(size_t bucket_count,
                                               size_t max_entries,
                                               int max_attempts,
                                               int min_peer_threshold,
                                               int retry_delay_ms,
                                               int max_age_sec) {
    if (bucket_count == 0 || max_entries == 0 || max_attempts <= 0) {
        return NULL;
    }

    peer_retry_tracker_t *tracker = calloc(1, sizeof(peer_retry_tracker_t));
    if (!tracker) {
        return NULL;
    }

    /* Allocate hash table buckets */
    tracker->buckets = calloc(bucket_count, sizeof(peer_retry_entry_t*));
    if (!tracker->buckets) {
        free(tracker);
        return NULL;
    }

    /* Allocate circular buffer */
    tracker->entries = calloc(max_entries, sizeof(peer_retry_entry_t));
    if (!tracker->entries) {
        free(tracker->buckets);
        free(tracker);
        return NULL;
    }

    /* Initialize all entries as invalid */
    for (size_t i = 0; i < max_entries; i++) {
        tracker->entries[i].valid = 0;
        tracker->entries[i].array_index = i;
    }

    tracker->bucket_count = bucket_count;
    tracker->max_entries = max_entries;
    tracker->write_pos = 0;
    tracker->current_count = 0;
    tracker->max_attempts = max_attempts;
    tracker->min_peer_threshold = min_peer_threshold;
    tracker->retry_delay_ms = retry_delay_ms;
    tracker->max_age_sec = max_age_sec;

    if (pthread_mutex_init(&tracker->mutex, NULL) != 0) {
        free(tracker->entries);
        free(tracker->buckets);
        free(tracker);
        return NULL;
    }

    /* Initialize statistics */
    tracker->total_entries = 0;
    tracker->retries_triggered = 0;
    tracker->success_first_try = 0;
    tracker->success_second_try = 0;
    tracker->success_third_try = 0;
    tracker->failed_all_attempts = 0;
    tracker->skipped_queue_full = 0;
    tracker->evicted_for_space = 0;

    return tracker;
}

/* Create entry for new info_hash */
peer_retry_entry_t* peer_retry_entry_create(peer_retry_tracker_t *tracker,
                                             const uint8_t *info_hash) {
    if (!tracker || !info_hash) {
        return NULL;
    }

    pthread_mutex_lock(&tracker->mutex);

    /* Check if entry already exists in hash table */
    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t *existing = tracker->buckets[bucket_idx];

    while (existing) {
        if (existing->valid && hash_equal(existing->info_hash, info_hash)) {
            pthread_mutex_unlock(&tracker->mutex);
            return existing;  /* Already exists */
        }
        existing = existing->hash_next;
    }

    /* Get entry at current write position */
    peer_retry_entry_t *entry = &tracker->entries[tracker->write_pos];

    /* If entry at write_pos is valid, evict it first */
    if (entry->valid) {
        remove_from_hash_table(tracker, entry);
        tracker->evicted_for_space++;
    }

    /* Initialize new entry */
    memcpy(entry->info_hash, info_hash, 20);
    entry->attempts_made = 0;
    entry->peer_count = 0;
    entry->query_in_progress = 1;  /* First query is in progress */
    entry->last_attempt_time = time(NULL);
    entry->target_retry_time = 0;  /* Not scheduled for retry yet */
    entry->created_at = time(NULL);
    entry->array_index = tracker->write_pos;
    entry->valid = 1;

    /* Add to hash table */
    entry->hash_next = tracker->buckets[bucket_idx];
    tracker->buckets[bucket_idx] = entry;
    tracker->current_count++;
    tracker->total_entries++;

    /* Advance write position */
    tracker->write_pos = (tracker->write_pos + 1) % tracker->max_entries;

    pthread_mutex_unlock(&tracker->mutex);

    return entry;
}

/* Find existing entry by info_hash */
peer_retry_entry_t* peer_retry_entry_find(peer_retry_tracker_t *tracker,
                                           const uint8_t *info_hash) {
    if (!tracker || !info_hash) {
        return NULL;
    }

    pthread_mutex_lock(&tracker->mutex);

    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t *entry = tracker->buckets[bucket_idx];

    while (entry) {
        if (entry->valid && hash_equal(entry->info_hash, info_hash)) {
            pthread_mutex_unlock(&tracker->mutex);
            return entry;
        }
        entry = entry->hash_next;
    }

    pthread_mutex_unlock(&tracker->mutex);
    return NULL;
}

/* Check if should retry for more peers */
int peer_retry_should_retry(peer_retry_tracker_t *tracker,
                             const uint8_t *info_hash,
                             int current_peer_count) {
    if (!tracker || !info_hash) {
        return 0;
    }

    pthread_mutex_lock(&tracker->mutex);

    /* Find entry in hash table */
    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t *entry = tracker->buckets[bucket_idx];

    while (entry) {
        if (entry->valid && hash_equal(entry->info_hash, info_hash)) {
            break;
        }
        entry = entry->hash_next;
    }

    if (!entry) {
        pthread_mutex_unlock(&tracker->mutex);
        return 0;  /* Not tracked */
    }

    /* Check retry conditions */
    int should_retry = 0;

    /* Condition 1: Haven't exceeded max attempts */
    if (entry->attempts_made >= tracker->max_attempts) {
        should_retry = 0;
    }
    /* Condition 2: Haven't reached minimum peer threshold */
    else if (current_peer_count >= tracker->min_peer_threshold) {
        should_retry = 0;
    }
    /* Condition 3: Not already querying */
    else if (entry->query_in_progress) {
        should_retry = 0;
    }
    /* All conditions met - retry! */
    else {
        should_retry = 1;
    }

    pthread_mutex_unlock(&tracker->mutex);
    return should_retry;
}

/* Mark entry as complete and remove from tracker */
void peer_retry_mark_complete(peer_retry_tracker_t *tracker,
                               const uint8_t *info_hash,
                               int final_peer_count) {
    if (!tracker || !info_hash) {
        return;
    }

    pthread_mutex_lock(&tracker->mutex);

    /* Find entry in hash table */
    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t *entry = tracker->buckets[bucket_idx];

    while (entry) {
        if (entry->valid && hash_equal(entry->info_hash, info_hash)) {
            /* Update statistics based on attempt count */
            int attempts = entry->attempts_made + 1;  /* +1 for initial attempt */

            if (final_peer_count == 0) {
                tracker->failed_all_attempts++;
            } else if (attempts == 1) {
                tracker->success_first_try++;
            } else if (attempts == 2) {
                tracker->success_second_try++;
            } else if (attempts >= 3) {
                tracker->success_third_try++;
            }

            /* Remove from hash table (marks as invalid) */
            remove_from_hash_table(tracker, entry);

            pthread_mutex_unlock(&tracker->mutex);
            return;
        }
        entry = entry->hash_next;
    }

    pthread_mutex_unlock(&tracker->mutex);
}

/* Cleanup old entries */
int peer_retry_cleanup_old(peer_retry_tracker_t *tracker) {
    if (!tracker) {
        return 0;
    }

    time_t now = time(NULL);
    int removed = 0;

    pthread_mutex_lock(&tracker->mutex);

    /* Scan circular buffer for old entries */
    for (size_t i = 0; i < tracker->max_entries; i++) {
        peer_retry_entry_t *entry = &tracker->entries[i];

        if (entry->valid) {
            time_t age = now - entry->created_at;

            if (age > tracker->max_age_sec) {
                /* Remove from hash table */
                remove_from_hash_table(tracker, entry);
                removed++;
            }
        }
    }

    pthread_mutex_unlock(&tracker->mutex);

    return removed;
}

/* Get entries that are ready for retry */
int peer_retry_get_ready_entries(peer_retry_tracker_t *tracker,
                                  uint8_t (*info_hashes)[20],
                                  int max_count) {
    if (!tracker || !info_hashes || max_count <= 0) {
        return 0;
    }

    time_t now = time(NULL);
    int found = 0;

    pthread_mutex_lock(&tracker->mutex);

    /* Scan circular buffer for ready entries */
    for (size_t i = 0; i < tracker->max_entries && found < max_count; i++) {
        peer_retry_entry_t *entry = &tracker->entries[i];

        if (entry->valid) {
            /* Check if this entry is ready for retry */
            if (entry->target_retry_time > 0 && entry->target_retry_time <= now) {
                /* Copy info_hash to output array */
                memcpy(info_hashes[found], entry->info_hash, 20);
                found++;

                /* Clear target_retry_time so it won't be retried again until rescheduled */
                entry->target_retry_time = 0;
            }
        }
    }

    pthread_mutex_unlock(&tracker->mutex);

    return found;
}

/* Print statistics */
void peer_retry_print_stats(peer_retry_tracker_t *tracker) {
    if (!tracker) {
        return;
    }

    pthread_mutex_lock(&tracker->mutex);

    uint64_t total_completed = tracker->success_first_try +
                                tracker->success_second_try +
                                tracker->success_third_try +
                                tracker->failed_all_attempts;

    log_msg(LOG_INFO, "  Peer Retry Statistics:");
    log_msg(LOG_INFO, "    Active entries: %zu / %zu (%.1f%%)",
            tracker->current_count, tracker->max_entries,
            (100.0 * tracker->current_count) / tracker->max_entries);
    log_msg(LOG_INFO, "    Retries triggered: %llu", (unsigned long long)tracker->retries_triggered);
    log_msg(LOG_INFO, "    Evicted for space: %llu", (unsigned long long)tracker->evicted_for_space);

    if (total_completed > 0) {
        log_msg(LOG_INFO, "    Success on 1st try: %llu (%.1f%%)",
                (unsigned long long)tracker->success_first_try,
                (100.0 * tracker->success_first_try) / total_completed);
        log_msg(LOG_INFO, "    Success on 2nd try: %llu (%.1f%%)",
                (unsigned long long)tracker->success_second_try,
                (100.0 * tracker->success_second_try) / total_completed);
        log_msg(LOG_INFO, "    Success on 3rd+ try: %llu (%.1f%%)",
                (unsigned long long)tracker->success_third_try,
                (100.0 * tracker->success_third_try) / total_completed);
        log_msg(LOG_INFO, "    Failed all attempts: %llu (%.1f%%)",
                (unsigned long long)tracker->failed_all_attempts,
                (100.0 * tracker->failed_all_attempts) / total_completed);

        /* Calculate retry efficiency */
        uint64_t retry_successes = tracker->success_second_try + tracker->success_third_try;
        if (tracker->retries_triggered > 0) {
            log_msg(LOG_INFO, "    Retry efficiency: %.1f%% (successes after retry / total retries)",
                    (100.0 * retry_successes) / tracker->retries_triggered);
        }
    }

    if (tracker->skipped_queue_full > 0) {
        log_msg(LOG_INFO, "    Skipped (queue full): %llu",
                (unsigned long long)tracker->skipped_queue_full);
    }

    log_msg(LOG_INFO, "    Config: max_attempts=%d min_peers=%d delay=%dms max_entries=%zu",
            tracker->max_attempts, tracker->min_peer_threshold, tracker->retry_delay_ms,
            tracker->max_entries);

    pthread_mutex_unlock(&tracker->mutex);
}

/* Clear all active retry entries (used during node ID rotation)
 * Returns number of entries cleared
 */
int peer_retry_tracker_clear_all(peer_retry_tracker_t *tracker) {
    if (!tracker) {
        return 0;
    }

    int cleared = 0;

    pthread_mutex_lock(&tracker->mutex);

    /* Clear all hash table buckets */
    for (size_t i = 0; i < tracker->bucket_count; i++) {
        tracker->buckets[i] = NULL;
    }

    /* Invalidate all entries in circular buffer */
    for (size_t i = 0; i < tracker->max_entries; i++) {
        if (tracker->entries[i].valid) {
            tracker->entries[i].valid = 0;
            tracker->entries[i].hash_next = NULL;
            cleared++;
        }
    }

    /* Reset counters */
    tracker->current_count = 0;
    tracker->write_pos = 0;

    pthread_mutex_unlock(&tracker->mutex);

    return cleared;
}

/* Cleanup and free tracker */
void peer_retry_tracker_cleanup(peer_retry_tracker_t *tracker) {
    if (!tracker) {
        return;
    }

    pthread_mutex_lock(&tracker->mutex);

    /* Free circular buffer */
    free(tracker->entries);

    /* Free hash table buckets */
    free(tracker->buckets);

    pthread_mutex_unlock(&tracker->mutex);
    pthread_mutex_destroy(&tracker->mutex);
    free(tracker);
}
