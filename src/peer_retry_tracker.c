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

/* Initialize retry tracker */
peer_retry_tracker_t* peer_retry_tracker_init(size_t bucket_count,
                                               int max_attempts,
                                               int min_peer_threshold,
                                               int retry_delay_ms,
                                               int max_age_sec) {
    if (bucket_count == 0 || max_attempts <= 0) {
        return NULL;
    }

    peer_retry_tracker_t *tracker = calloc(1, sizeof(peer_retry_tracker_t));
    if (!tracker) {
        return NULL;
    }

    tracker->buckets = calloc(bucket_count, sizeof(peer_retry_entry_t*));
    if (!tracker->buckets) {
        free(tracker);
        return NULL;
    }

    tracker->bucket_count = bucket_count;
    tracker->max_attempts = max_attempts;
    tracker->min_peer_threshold = min_peer_threshold;
    tracker->retry_delay_ms = retry_delay_ms;
    tracker->max_age_sec = max_age_sec;

    if (pthread_mutex_init(&tracker->mutex, NULL) != 0) {
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

    return tracker;
}

/* Create entry for new info_hash */
peer_retry_entry_t* peer_retry_entry_create(peer_retry_tracker_t *tracker,
                                             const uint8_t *info_hash) {
    if (!tracker || !info_hash) {
        return NULL;
    }

    pthread_mutex_lock(&tracker->mutex);

    /* Check if entry already exists */
    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t *existing = tracker->buckets[bucket_idx];

    while (existing) {
        if (hash_equal(existing->info_hash, info_hash)) {
            pthread_mutex_unlock(&tracker->mutex);
            return existing;  /* Already exists */
        }
        existing = existing->next;
    }

    /* Create new entry */
    peer_retry_entry_t *entry = calloc(1, sizeof(peer_retry_entry_t));
    if (!entry) {
        pthread_mutex_unlock(&tracker->mutex);
        return NULL;
    }

    memcpy(entry->info_hash, info_hash, 20);
    entry->attempts_made = 0;
    entry->peer_count = 0;
    entry->query_in_progress = 1;  /* First query is in progress */
    entry->last_attempt_time = time(NULL);
    entry->target_retry_time = 0;  /* Not scheduled for retry yet */
    entry->created_at = time(NULL);

    /* Add to bucket chain */
    entry->next = tracker->buckets[bucket_idx];
    tracker->buckets[bucket_idx] = entry;
    tracker->total_entries++;

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
        if (hash_equal(entry->info_hash, info_hash)) {
            pthread_mutex_unlock(&tracker->mutex);
            return entry;
        }
        entry = entry->next;
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

    /* Find entry */
    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t *entry = tracker->buckets[bucket_idx];

    while (entry) {
        if (hash_equal(entry->info_hash, info_hash)) {
            break;
        }
        entry = entry->next;
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

    /* Find and remove entry */
    size_t bucket_idx = hash_info_hash(info_hash, tracker->bucket_count);
    peer_retry_entry_t **prev = &tracker->buckets[bucket_idx];
    peer_retry_entry_t *entry = tracker->buckets[bucket_idx];

    while (entry) {
        if (hash_equal(entry->info_hash, info_hash)) {
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

            /* Remove from chain */
            *prev = entry->next;
            free(entry);
            tracker->total_entries--;

            pthread_mutex_unlock(&tracker->mutex);
            return;
        }
        prev = &entry->next;
        entry = entry->next;
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

    for (size_t i = 0; i < tracker->bucket_count; i++) {
        peer_retry_entry_t **prev = &tracker->buckets[i];
        peer_retry_entry_t *entry = tracker->buckets[i];

        while (entry) {
            time_t age = now - entry->created_at;

            if (age > tracker->max_age_sec) {
                /* Remove expired entry */
                *prev = entry->next;
                peer_retry_entry_t *to_free = entry;
                entry = entry->next;
                free(to_free);
                tracker->total_entries--;
                removed++;
            } else {
                prev = &entry->next;
                entry = entry->next;
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

    /* Scan all buckets for ready entries */
    for (size_t i = 0; i < tracker->bucket_count && found < max_count; i++) {
        peer_retry_entry_t *entry = tracker->buckets[i];

        while (entry && found < max_count) {
            /* Check if this entry is ready for retry */
            if (entry->target_retry_time > 0 && entry->target_retry_time <= now) {
                /* Copy info_hash to output array */
                memcpy(info_hashes[found], entry->info_hash, 20);
                found++;

                /* Clear target_retry_time so it won't be retried again until rescheduled */
                entry->target_retry_time = 0;
            }

            entry = entry->next;
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
    log_msg(LOG_INFO, "    Active entries: %llu", (unsigned long long)tracker->total_entries);
    log_msg(LOG_INFO, "    Retries triggered: %llu", (unsigned long long)tracker->retries_triggered);

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

    log_msg(LOG_INFO, "    Config: max_attempts=%d min_peers=%d delay=%dms",
            tracker->max_attempts, tracker->min_peer_threshold, tracker->retry_delay_ms);

    pthread_mutex_unlock(&tracker->mutex);
}

/* Cleanup and free tracker */
void peer_retry_tracker_cleanup(peer_retry_tracker_t *tracker) {
    if (!tracker) {
        return;
    }

    pthread_mutex_lock(&tracker->mutex);

    /* Free all entries */
    for (size_t i = 0; i < tracker->bucket_count; i++) {
        peer_retry_entry_t *entry = tracker->buckets[i];
        while (entry) {
            peer_retry_entry_t *next = entry->next;
            free(entry);
            entry = next;
        }
    }

    free(tracker->buckets);
    pthread_mutex_unlock(&tracker->mutex);
    pthread_mutex_destroy(&tracker->mutex);
    free(tracker);
}
