#ifndef REFRESH_QUERY_H
#define REFRESH_QUERY_H

#include <stdint.h>
#include <pthread.h>
#include <time.h>

/* Single pending refresh query */
typedef struct refresh_query {
    uint8_t info_hash[20];
    int peer_count;              /* Accumulated peer count */
    int complete;                /* 1 when query finished */
    int timed_out;               /* 1 if query timed out */
    int ref_count;               /* Reference count for safe cleanup */
    time_t created_at;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    struct refresh_query *next;  /* For hash table chaining */
} refresh_query_t;

/* Thread-safe pending query store */
typedef struct {
    refresh_query_t **buckets;
    size_t bucket_count;
    pthread_mutex_t mutex;
    int query_timeout_sec;       /* Default timeout for queries */
} refresh_query_store_t;

/* Initialize query store */
refresh_query_store_t* refresh_query_store_init(size_t bucket_count, int timeout_sec);

/* Create and register a new pending query */
refresh_query_t* refresh_query_create(refresh_query_store_t *store, const uint8_t *info_hash);

/* Find a pending query by info_hash */
refresh_query_t* refresh_query_find(refresh_query_store_t *store, const uint8_t *info_hash);

/* Increment reference count (returns query for convenience) */
refresh_query_t* refresh_query_ref(refresh_query_t *query);

/* Decrement reference count and free if zero (called instead of direct free) */
void refresh_query_unref(refresh_query_t *query);

/* Wait for query completion (blocks until complete or timeout)
 * Returns peer_count, sets *timed_out if provided */
int refresh_query_wait(refresh_query_t *query, int timeout_sec, int *timed_out);

/* Update query with new peer count (called from DHT callback) */
void refresh_query_add_peers(refresh_query_store_t *store, const uint8_t *info_hash, int peer_count);

/* Mark query as complete (called from DHT callback on SEARCH_DONE) */
void refresh_query_complete(refresh_query_store_t *store, const uint8_t *info_hash);

/* Remove query from store by info_hash and decrement ref count
 * WARNING: If multiple queries exist for same info_hash, removes first match */
void refresh_query_remove(refresh_query_store_t *store, const uint8_t *info_hash);

/* Remove specific query from store by pointer and decrement ref count
 * This is the safe version - removes exactly the query you created */
void refresh_query_remove_ptr(refresh_query_store_t *store, refresh_query_t *query);

/* Cleanup old timed-out queries */
void refresh_query_cleanup_old(refresh_query_store_t *store, int max_age_sec);

/* Cleanup store */
void refresh_query_store_cleanup(refresh_query_store_t *store);

#endif /* REFRESH_QUERY_H */
