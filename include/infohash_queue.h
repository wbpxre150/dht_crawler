#ifndef INFOHASH_QUEUE_H
#define INFOHASH_QUEUE_H

#include "dht_crawler.h"
#include "bloom_filter.h"
#include <uv.h>
#include <sys/socket.h>

/* Forward declaration - avoid circular dependency */
struct database;

/* Maximum peers to bundle with each infohash entry */
#define MAX_PEERS_PER_ENTRY 50

/* Info hash queue entry - self-contained work unit with peers */
typedef struct {
    uint8_t info_hash[SHA1_DIGEST_LENGTH];
    int64_t timestamp;
    struct sockaddr_storage peers[MAX_PEERS_PER_ENTRY];
    socklen_t peer_lens[MAX_PEERS_PER_ENTRY];
    int peer_count;
} infohash_entry_t;

/* Circular queue for info hashes */
typedef struct {
    infohash_entry_t *entries;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    uv_mutex_t mutex;
    uv_cond_t cond_not_empty;
    uv_cond_t cond_not_full;
    bloom_filter_t *bloom;
    struct database *db;
    uint64_t duplicates_filtered;
} infohash_queue_t;

/* Function declarations */
int infohash_queue_init(infohash_queue_t *queue, size_t capacity);
void infohash_queue_set_bloom(infohash_queue_t *queue, bloom_filter_t *bloom);
void infohash_queue_set_database(infohash_queue_t *queue, struct database *db);
int infohash_queue_push(infohash_queue_t *queue, const uint8_t *info_hash);
int infohash_queue_push_with_peers(infohash_queue_t *queue, const uint8_t *info_hash,
                                    struct sockaddr_storage *peers, socklen_t *peer_lens, int peer_count);
int infohash_queue_pop(infohash_queue_t *queue, uint8_t *info_hash);
int infohash_queue_pop_entry(infohash_queue_t *queue, infohash_entry_t *entry);
int infohash_queue_try_pop(infohash_queue_t *queue, uint8_t *info_hash, int timeout_ms);
int infohash_queue_try_pop_entry(infohash_queue_t *queue, infohash_entry_t *entry, int timeout_ms);
size_t infohash_queue_size(infohash_queue_t *queue);
size_t infohash_queue_capacity(infohash_queue_t *queue);
int infohash_queue_is_empty(infohash_queue_t *queue);
int infohash_queue_is_full(infohash_queue_t *queue);
uint64_t infohash_queue_get_duplicates(infohash_queue_t *queue);
void infohash_queue_cleanup(infohash_queue_t *queue);

#endif /* INFOHASH_QUEUE_H */
