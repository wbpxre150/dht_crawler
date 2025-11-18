#include "refresh_query.h"
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

/* Initialize query store */
refresh_query_store_t* refresh_query_store_init(size_t bucket_count, int timeout_sec) {
    refresh_query_store_t *store = calloc(1, sizeof(refresh_query_store_t));
    if (!store) return NULL;

    store->buckets = calloc(bucket_count, sizeof(refresh_query_t*));
    if (!store->buckets) {
        free(store);
        return NULL;
    }

    store->bucket_count = bucket_count;
    store->query_timeout_sec = timeout_sec;

    if (pthread_mutex_init(&store->mutex, NULL) != 0) {
        free(store->buckets);
        free(store);
        return NULL;
    }

    return store;
}

/* Increment reference count */
refresh_query_t* refresh_query_ref(refresh_query_t *query) {
    if (!query) return NULL;

    pthread_mutex_lock(&query->mutex);
    query->ref_count++;
    pthread_mutex_unlock(&query->mutex);

    return query;
}

/* Decrement reference count and free if zero */
void refresh_query_unref(refresh_query_t *query) {
    if (!query) return;

    pthread_mutex_lock(&query->mutex);
    query->ref_count--;
    int should_free = (query->ref_count <= 0);
    pthread_mutex_unlock(&query->mutex);

    if (should_free) {
        pthread_cond_destroy(&query->cond);
        pthread_mutex_destroy(&query->mutex);
        free(query);
    }
}

/* Create and register a new pending query */
refresh_query_t* refresh_query_create(refresh_query_store_t *store, const uint8_t *info_hash) {
    if (!store || !info_hash) return NULL;

    refresh_query_t *query = calloc(1, sizeof(refresh_query_t));
    if (!query) return NULL;

    memcpy(query->info_hash, info_hash, 20);
    query->peer_count = 0;
    query->complete = 0;
    query->timed_out = 0;
    query->ref_count = 1;  /* Initial reference for creator */
    query->created_at = time(NULL);

    if (pthread_mutex_init(&query->mutex, NULL) != 0) {
        free(query);
        return NULL;
    }

    if (pthread_cond_init(&query->cond, NULL) != 0) {
        pthread_mutex_destroy(&query->mutex);
        free(query);
        return NULL;
    }

    /* Add to hash table */
    pthread_mutex_lock(&store->mutex);

    size_t bucket_idx = hash_info_hash(info_hash, store->bucket_count);
    query->next = store->buckets[bucket_idx];
    store->buckets[bucket_idx] = query;

    pthread_mutex_unlock(&store->mutex);

    return query;
}

/* Find a pending query by info_hash */
refresh_query_t* refresh_query_find(refresh_query_store_t *store, const uint8_t *info_hash) {
    if (!store || !info_hash) return NULL;

    pthread_mutex_lock(&store->mutex);

    size_t bucket_idx = hash_info_hash(info_hash, store->bucket_count);
    refresh_query_t *query = store->buckets[bucket_idx];

    while (query) {
        if (hash_equal(query->info_hash, info_hash)) {
            pthread_mutex_unlock(&store->mutex);
            return query;
        }
        query = query->next;
    }

    pthread_mutex_unlock(&store->mutex);
    return NULL;
}

/* Wait for query completion (blocks until complete or timeout)
 * Returns peer_count, sets *timed_out if provided */
int refresh_query_wait(refresh_query_t *query, int timeout_sec, int *timed_out) {
    if (!query) return -1;

    pthread_mutex_lock(&query->mutex);

    if (query->complete) {
        int peer_count = query->peer_count;
        int was_timed_out = query->timed_out;
        pthread_mutex_unlock(&query->mutex);
        if (timed_out) *timed_out = was_timed_out;
        return peer_count;
    }

    /* Wait with timeout */
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_sec;

    int rc = pthread_cond_timedwait(&query->cond, &query->mutex, &ts);

    if (rc == 0) {
        /* Signaled - query complete */
        int peer_count = query->peer_count;
        int was_timed_out = query->timed_out;
        pthread_mutex_unlock(&query->mutex);
        if (timed_out) *timed_out = was_timed_out;
        return peer_count;
    } else {
        /* Timeout */
        query->timed_out = 1;
        int peer_count = query->peer_count;  /* Return partial count */
        pthread_mutex_unlock(&query->mutex);
        if (timed_out) *timed_out = 1;
        return peer_count;
    }
}

/* Update query with new peer count (called from DHT callback) */
void refresh_query_add_peers(refresh_query_store_t *store, const uint8_t *info_hash, int peer_count) {
    refresh_query_t *query = refresh_query_find(store, info_hash);
    if (!query) return;

    pthread_mutex_lock(&query->mutex);
    query->peer_count += peer_count;
    pthread_mutex_unlock(&query->mutex);
}

/* Mark query as complete (called from DHT callback on SEARCH_DONE) */
void refresh_query_complete(refresh_query_store_t *store, const uint8_t *info_hash) {
    refresh_query_t *query = refresh_query_find(store, info_hash);
    if (!query) return;

    pthread_mutex_lock(&query->mutex);
    query->complete = 1;
    pthread_cond_broadcast(&query->cond);  /* Wake up waiting thread */
    pthread_mutex_unlock(&query->mutex);
}

/* Remove query from store and decrement ref count */
void refresh_query_remove(refresh_query_store_t *store, const uint8_t *info_hash) {
    if (!store || !info_hash) return;

    pthread_mutex_lock(&store->mutex);

    size_t bucket_idx = hash_info_hash(info_hash, store->bucket_count);
    refresh_query_t **prev = &store->buckets[bucket_idx];
    refresh_query_t *query = store->buckets[bucket_idx];

    while (query) {
        if (hash_equal(query->info_hash, info_hash)) {
            *prev = query->next;
            pthread_mutex_unlock(&store->mutex);

            /* Decrement ref count - will free if count reaches zero */
            refresh_query_unref(query);
            return;
        }
        prev = &query->next;
        query = query->next;
    }

    pthread_mutex_unlock(&store->mutex);
}

/* Cleanup old timed-out queries (called periodically) */
void refresh_query_cleanup_old(refresh_query_store_t *store, int max_age_sec) {
    if (!store) return;

    pthread_mutex_lock(&store->mutex);
    time_t now = time(NULL);

    for (size_t i = 0; i < store->bucket_count; i++) {
        refresh_query_t **prev = &store->buckets[i];
        refresh_query_t *query = store->buckets[i];

        while (query) {
            if ((query->complete || query->timed_out) &&
                (now - query->created_at > max_age_sec)) {
                /* Remove old completed query */
                *prev = query->next;
                refresh_query_t *to_unref = query;
                query = query->next;

                /* Use unref instead of direct free */
                refresh_query_unref(to_unref);
            } else {
                prev = &query->next;
                query = query->next;
            }
        }
    }

    pthread_mutex_unlock(&store->mutex);
}

/* Cleanup store */
void refresh_query_store_cleanup(refresh_query_store_t *store) {
    if (!store) return;

    pthread_mutex_lock(&store->mutex);

    /* Free all queries using unref */
    for (size_t i = 0; i < store->bucket_count; i++) {
        refresh_query_t *query = store->buckets[i];
        while (query) {
            refresh_query_t *next = query->next;
            refresh_query_unref(query);
            query = next;
        }
    }

    free(store->buckets);
    pthread_mutex_unlock(&store->mutex);
    pthread_mutex_destroy(&store->mutex);
    free(store);
}
