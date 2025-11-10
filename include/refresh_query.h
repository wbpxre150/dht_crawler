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

/* Wait for query completion (blocks until complete or timeout) */
int refresh_query_wait(refresh_query_t *query, int timeout_sec);

/* Update query with new peer count (called from DHT callback) */
void refresh_query_add_peers(refresh_query_store_t *store, const uint8_t *info_hash, int peer_count);

/* Mark query as complete (called from DHT callback on SEARCH_DONE) */
void refresh_query_complete(refresh_query_store_t *store, const uint8_t *info_hash);

/* Remove and free a query */
void refresh_query_remove(refresh_query_store_t *store, const uint8_t *info_hash);

/* Cleanup old timed-out queries */
void refresh_query_cleanup_old(refresh_query_store_t *store, int max_age_sec);

/* Cleanup store */
void refresh_query_store_cleanup(refresh_query_store_t *store);

#endif /* REFRESH_QUERY_H */
