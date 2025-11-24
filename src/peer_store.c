#include "peer_store.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

/* Hash function for info_hash -> bucket index */
static size_t hash_function(const uint8_t *info_hash, size_t bucket_count) {
    /* Simple hash: XOR first 8 bytes as uint64_t */
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

/* Check if peer_store is shutdown
 * Returns 0 on success, -1 if shutdown in progress */
static int peer_store_check_shutdown(peer_store_t *store) {
    if (!store) return -1;
    return store->shutdown ? -1 : 0;
}

/* Initialize peer store */
peer_store_t* peer_store_init(size_t max_hashes, size_t max_peers_per_hash,
                               int peer_expiry_seconds) {
    if (max_hashes == 0 || max_peers_per_hash == 0) {
        return NULL;
    }

    peer_store_t *store = calloc(1, sizeof(peer_store_t));
    if (!store) {
        return NULL;
    }

    /* Use prime number for bucket count to reduce collisions */
    store->bucket_count = max_hashes * 2 + 1;
    store->buckets = calloc(store->bucket_count, sizeof(hash_entry_t*));
    if (!store->buckets) {
        free(store);
        return NULL;
    }

    store->max_hashes = max_hashes;
    store->max_peers_per_hash = max_peers_per_hash;
    store->peer_expiry_seconds = peer_expiry_seconds;
    store->total_hashes = 0;
    store->total_peers = 0;
    store->shutdown = false;

    /* Initialize mutex */
    pthread_mutex_t *mutex = malloc(sizeof(pthread_mutex_t));
    if (!mutex) {
        free(store->buckets);
        free(store);
        return NULL;
    }

    if (pthread_mutex_init(mutex, NULL) != 0) {
        free(mutex);
        free(store->buckets);
        free(store);
        return NULL;
    }
    store->mutex = mutex;

    /* Initialize statistics */
    store->peers_added = 0;
    store->peers_expired = 0;
    store->hashes_evicted = 0;
    store->lookups_success = 0;
    store->lookups_miss = 0;

    return store;
}

/* Find hash entry in bucket chain */
static hash_entry_t* find_hash_entry(hash_entry_t *bucket, const uint8_t *info_hash) {
    hash_entry_t *entry = bucket;
    while (entry) {
        if (hash_equal(entry->info_hash, info_hash)) {
            return entry;
        }
        entry = entry->next;
    }
    return NULL;
}

/* Remove expired peers from an entry */
static void cleanup_expired_peers_in_entry(hash_entry_t *entry, time_t now,
                                            int expiry_seconds, uint64_t *expired_count) {
    if (!entry) return;

    peer_addr_t **prev = &entry->peers;
    peer_addr_t *peer = entry->peers;

    while (peer) {
        if (now - peer->discovered_at > expiry_seconds) {
            /* Expired - remove */
            *prev = peer->next;
            peer_addr_t *to_free = peer;
            peer = peer->next;
            free(to_free);
            entry->peer_count--;
            (*expired_count)++;
        } else {
            prev = &peer->next;
            peer = peer->next;
        }
    }
}

/* Evict least recently used hash entry */
static void evict_lru_entry(peer_store_t *store) {
    if (!store || store->total_hashes == 0) return;

    /* Find oldest entry across all buckets */
    hash_entry_t *oldest_entry = NULL;
    hash_entry_t **oldest_prev = NULL;
    time_t oldest_time = time(NULL);

    for (size_t i = 0; i < store->bucket_count; i++) {
        hash_entry_t **prev = &store->buckets[i];
        hash_entry_t *entry = store->buckets[i];

        while (entry) {
            if (entry->last_updated < oldest_time) {
                oldest_time = entry->last_updated;
                oldest_entry = entry;
                oldest_prev = prev;
            }
            prev = &entry->next;
            entry = entry->next;
        }
    }

    if (oldest_entry && oldest_prev) {
        /* Remove from chain */
        *oldest_prev = oldest_entry->next;

        /* Free all peers */
        peer_addr_t *peer = oldest_entry->peers;
        while (peer) {
            peer_addr_t *next = peer->next;
            free(peer);
            store->total_peers--;
            peer = next;
        }

        free(oldest_entry);
        store->total_hashes--;
        store->hashes_evicted++;
    }
}

/* Add a peer for an info_hash */
int peer_store_add_peer(peer_store_t *store, const uint8_t *info_hash,
                        const struct sockaddr *addr, socklen_t addr_len) {
    if (!store || !info_hash || !addr || addr_len == 0) {
        return -1;
    }

    /* Check if shutdown in progress */
    if (peer_store_check_shutdown(store) != 0) {
        return -1;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t*)store->mutex;
    pthread_mutex_lock(mutex);

    /* Double-check shutdown flag after acquiring lock */
    if (store->shutdown) {
        pthread_mutex_unlock(mutex);
        return -1;
    }

    /* Find or create hash entry */
    size_t bucket_idx = hash_function(info_hash, store->bucket_count);
    hash_entry_t *entry = find_hash_entry(store->buckets[bucket_idx], info_hash);

    if (!entry) {
        /* Check if we need to evict */
        if (store->total_hashes >= store->max_hashes) {
            evict_lru_entry(store);
        }

        /* Create new entry */
        entry = calloc(1, sizeof(hash_entry_t));
        if (!entry) {
            pthread_mutex_unlock(mutex);
            return -1;
        }

        memcpy(entry->info_hash, info_hash, 20);
        entry->peers = NULL;
        entry->peer_count = 0;
        entry->last_updated = time(NULL);

        /* Add to bucket chain */
        entry->next = store->buckets[bucket_idx];
        store->buckets[bucket_idx] = entry;
        store->total_hashes++;
    }

    /* Check if we already have too many peers for this hash */
    if (entry->peer_count >= (int)store->max_peers_per_hash) {
        /* Remove oldest peer */
        if (entry->peers) {
            peer_addr_t *old = entry->peers;
            entry->peers = old->next;
            free(old);
            entry->peer_count--;
            store->total_peers--;
        }
    }

    /* Check for duplicate peer address */
    peer_addr_t *existing = entry->peers;
    while (existing) {
        if (existing->addr_len == addr_len &&
            memcmp(&existing->addr, addr, addr_len) == 0) {
            /* Already have this peer, just update timestamp */
            existing->discovered_at = time(NULL);
            entry->last_updated = time(NULL);
            pthread_mutex_unlock(mutex);
            return 0;
        }
        existing = existing->next;
    }

    /* Add new peer */
    peer_addr_t *new_peer = calloc(1, sizeof(peer_addr_t));
    if (!new_peer) {
        pthread_mutex_unlock(mutex);
        return -1;
    }

    memcpy(&new_peer->addr, addr, addr_len);
    new_peer->addr_len = addr_len;
    new_peer->discovered_at = time(NULL);
    new_peer->next = entry->peers;
    entry->peers = new_peer;
    entry->peer_count++;
    entry->last_updated = time(NULL);

    store->total_peers++;
    store->peers_added++;

    pthread_mutex_unlock(mutex);
    return 0;
}

/* Get peers for an info_hash */
int peer_store_get_peers(peer_store_t *store, const uint8_t *info_hash,
                         struct sockaddr_storage *addrs, socklen_t *addr_lens,
                         int *count, int max_peers) {
    if (!store || !info_hash || !addrs || !addr_lens || !count || max_peers <= 0) {
        return -1;
    }

    /* Check if shutdown in progress */
    if (peer_store_check_shutdown(store) != 0) {
        *count = 0;
        return -1;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t*)store->mutex;
    pthread_mutex_lock(mutex);

    /* Double-check shutdown flag after acquiring lock */
    if (store->shutdown) {
        pthread_mutex_unlock(mutex);
        *count = 0;
        return -1;
    }

    /* Find hash entry */
    size_t bucket_idx = hash_function(info_hash, store->bucket_count);
    hash_entry_t *entry = find_hash_entry(store->buckets[bucket_idx], info_hash);

    if (!entry || entry->peer_count == 0) {
        *count = 0;
        store->lookups_miss++;
        pthread_mutex_unlock(mutex);
        return -1;
    }

    /* Cleanup expired peers first */
    time_t now = time(NULL);
    cleanup_expired_peers_in_entry(entry, now, store->peer_expiry_seconds,
                                   &store->peers_expired);

    if (entry->peer_count == 0) {
        *count = 0;
        store->lookups_miss++;
        pthread_mutex_unlock(mutex);
        return -1;
    }

    /* Copy peers to output array */
    int copied = 0;
    peer_addr_t *peer = entry->peers;
    while (peer && copied < max_peers) {
        memcpy(&addrs[copied], &peer->addr, sizeof(struct sockaddr_storage));
        addr_lens[copied] = peer->addr_len;
        copied++;
        peer = peer->next;
    }

    *count = copied;
    store->lookups_success++;

    pthread_mutex_unlock(mutex);
    return 0;
}

/* Get count of peers for an info_hash */
int peer_store_count_peers(peer_store_t *store, const uint8_t *info_hash) {
    if (!store || !info_hash) {
        return 0;
    }

    /* Check if shutdown in progress */
    if (peer_store_check_shutdown(store) != 0) {
        return 0;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t*)store->mutex;
    pthread_mutex_lock(mutex);

    /* Double-check shutdown flag after acquiring lock */
    if (store->shutdown) {
        pthread_mutex_unlock(mutex);
        return 0;
    }

    size_t bucket_idx = hash_function(info_hash, store->bucket_count);
    hash_entry_t *entry = find_hash_entry(store->buckets[bucket_idx], info_hash);

    int count = entry ? entry->peer_count : 0;

    pthread_mutex_unlock(mutex);
    return count;
}

/* Remove expired peers */
int peer_store_cleanup_expired(peer_store_t *store) {
    if (!store) {
        return 0;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t*)store->mutex;
    pthread_mutex_lock(mutex);

    uint64_t expired_before = store->peers_expired;
    time_t now = time(NULL);

    /* Iterate all buckets */
    for (size_t i = 0; i < store->bucket_count; i++) {
        hash_entry_t *entry = store->buckets[i];
        while (entry) {
            cleanup_expired_peers_in_entry(entry, now, store->peer_expiry_seconds,
                                          &store->peers_expired);
            entry = entry->next;
        }
    }

    int expired_count = (int)(store->peers_expired - expired_before);

    pthread_mutex_unlock(mutex);
    return expired_count;
}

/* Print statistics */
void peer_store_print_stats(peer_store_t *store) {
    if (!store) {
        return;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t*)store->mutex;
    pthread_mutex_lock(mutex);

    log_msg(LOG_DEBUG, "Peer Store Stats:");
    log_msg(LOG_DEBUG, "  Total hashes: %zu (max: %zu)",
            store->total_hashes, store->max_hashes);
    log_msg(LOG_DEBUG, "  Total peers: %zu (avg: %.1f per hash)",
            store->total_peers,
            store->total_hashes > 0 ? (double)store->total_peers / store->total_hashes : 0.0);
    log_msg(LOG_DEBUG, "  Peers added: %lu", store->peers_added);
    log_msg(LOG_DEBUG, "  Peers expired: %lu", store->peers_expired);
    log_msg(LOG_DEBUG, "  Hashes evicted: %lu", store->hashes_evicted);
    log_msg(LOG_DEBUG, "  Lookups: %lu success, %lu miss (%.1f%% hit rate)",
            store->lookups_success, store->lookups_miss,
            (store->lookups_success + store->lookups_miss) > 0 ?
            100.0 * store->lookups_success / (store->lookups_success + store->lookups_miss) : 0.0);

    pthread_mutex_unlock(mutex);
}

/* Initiate shutdown - set shutdown flag to prevent new operations */
void peer_store_shutdown(peer_store_t *store) {
    if (!store) {
        return;
    }

    /* Set shutdown flag WITHOUT locking to avoid deadlock
     * Rationale: We're only setting it once (false -> true) which is atomic on all modern CPUs
     * DHT worker threads check this flag and will exit operations quickly
     * This prevents deadlock where shutdown() blocks waiting for lock held by DHT callback */
    store->shutdown = true;
}

/* Cleanup and free peer store */
void peer_store_cleanup(peer_store_t *store) {
    if (!store) {
        return;
    }

    /* Safety: Check if already cleaned up */
    if (!store->buckets) {
        log_msg(LOG_WARN, "peer_store_cleanup called on already-cleaned store");
        return;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t*)store->mutex;

    pthread_mutex_lock(mutex);

    /* Set shutdown flag to prevent new operations */
    store->shutdown = true;

    pthread_mutex_unlock(mutex);

    /* Give in-flight operations time to complete and see shutdown flag
     * This prevents race conditions where threads are mid-operation */
    log_msg(LOG_DEBUG, "Waiting for in-flight peer_store operations to complete...");
    struct timespec sleep_time = {0, 100000000};  /* 100ms (0 sec, 100 million nanosec) */
    nanosleep(&sleep_time, NULL);

    pthread_mutex_lock(mutex);

    /* Re-verify shutdown flag (should still be true) */
    if (!store->shutdown) {
        log_msg(LOG_WARN, "peer_store shutdown flag was cleared during cleanup wait");
    }

    /* Free all entries and peers */
    for (size_t i = 0; i < store->bucket_count; i++) {
        hash_entry_t *entry = store->buckets[i];
        while (entry) {
            hash_entry_t *next_entry = entry->next;

            /* Free all peers in this entry */
            peer_addr_t *peer = entry->peers;
            while (peer) {
                peer_addr_t *next_peer = peer->next;
                free(peer);
                peer = next_peer;
            }

            free(entry);
            entry = next_entry;
        }
    }

    free(store->buckets);
    store->buckets = NULL;

    /* Unlock before destroying synchronization primitives */
    pthread_mutex_unlock(mutex);

    /* Destroy mutex */
    pthread_mutex_destroy(mutex);
    free(mutex);

    /* Free the store structure */
    free(store);
}
