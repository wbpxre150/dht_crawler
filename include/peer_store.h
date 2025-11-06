#ifndef PEER_STORE_H
#define PEER_STORE_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <time.h>

/* Peer store - thread-safe storage mapping info_hash -> peer addresses
 * Used to cache discovered peers for each torrent before metadata fetch
 */

/* Peer address entry */
typedef struct peer_addr {
    struct sockaddr_storage addr;
    socklen_t addr_len;
    time_t discovered_at;
    struct peer_addr *next;
} peer_addr_t;

/* Info hash entry with peer list */
typedef struct hash_entry {
    uint8_t info_hash[20];
    peer_addr_t *peers;
    int peer_count;
    time_t last_updated;
    struct hash_entry *next;
} hash_entry_t;

/* Peer store context */
typedef struct peer_store {
    hash_entry_t **buckets;
    size_t bucket_count;
    size_t max_hashes;
    size_t max_peers_per_hash;
    size_t total_hashes;
    size_t total_peers;
    int peer_expiry_seconds;

    /* Thread safety */
    void *mutex;  /* pthread_mutex_t */
    bool shutdown;  /* Shutdown flag to prevent new operations */

    /* Statistics */
    uint64_t peers_added;
    uint64_t peers_expired;
    uint64_t hashes_evicted;
    uint64_t lookups_success;
    uint64_t lookups_miss;
} peer_store_t;

/* Function declarations */

/* Initialize peer store
 * max_hashes: Maximum number of info_hashes to track
 * max_peers_per_hash: Maximum peers to store per info_hash
 * peer_expiry_seconds: Time after which peers expire
 */
peer_store_t* peer_store_init(size_t max_hashes, size_t max_peers_per_hash,
                               int peer_expiry_seconds);

/* Add a peer for an info_hash
 * Returns 0 on success, -1 on error
 */
int peer_store_add_peer(peer_store_t *store, const uint8_t *info_hash,
                        const struct sockaddr *addr, socklen_t addr_len);

/* Get peers for an info_hash
 * addrs: Output array of sockaddr pointers (caller must provide array)
 * count: Output - number of peers returned
 * max_peers: Maximum number of peers to return
 * Returns 0 on success (peers found), -1 if no peers found
 */
int peer_store_get_peers(peer_store_t *store, const uint8_t *info_hash,
                         struct sockaddr_storage *addrs, socklen_t *addr_lens,
                         int *count, int max_peers);

/* Get count of peers for an info_hash (without removing)
 * Returns peer count, or 0 if not found
 */
int peer_store_count_peers(peer_store_t *store, const uint8_t *info_hash);

/* Remove expired peers (internal maintenance)
 * Returns number of peers removed
 */
int peer_store_cleanup_expired(peer_store_t *store);

/* Print statistics */
void peer_store_print_stats(peer_store_t *store);

/* Initiate shutdown (set shutdown flag, prevent new operations)
 * Does NOT free resources - call peer_store_cleanup() after to free */
void peer_store_shutdown(peer_store_t *store);

/* Cleanup and free peer store */
void peer_store_cleanup(peer_store_t *store);

#endif /* PEER_STORE_H */
