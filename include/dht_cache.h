#ifndef DHT_CACHE_H
#define DHT_CACHE_H

#include <stdint.h>
#include <time.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define DHT_CACHE_FILE "data/dht_peers.cache"
#define DHT_CACHE_MAX_PEERS 200
#define DHT_CACHE_EXPIRY_DAYS 7

/* Cached peer entry - stores BEP51-capable nodes with full metadata
 * Only nodes with confirmed BEP51 support are cached for high-quality bootstrapping.
 */
typedef struct {
    unsigned char node_id[20];    /* 20-byte DHT node ID */
    struct sockaddr_storage addr; /* IP address and port */
    socklen_t addr_len;           /* Address length */
    time_t last_seen;             /* Last time this peer was good */
    bool bep51_support;           /* Confirmed BEP51 support flag */
} dht_cached_peer_t;

/* DHT cache context */
typedef struct {
    dht_cached_peer_t *peers;     /* Array of cached peers */
    size_t count;                 /* Number of cached peers */
    size_t capacity;              /* Maximum capacity */
    const char *cache_file;       /* Path to cache file */
    time_t last_save;             /* Last time cache was saved */
} dht_cache_t;

/* Function declarations */
int dht_cache_init(dht_cache_t *cache, const char *cache_file);
int dht_cache_load(dht_cache_t *cache);
int dht_cache_save(dht_cache_t *cache);
int dht_cache_add_peer(dht_cache_t *cache, const unsigned char *node_id, 
                       const struct sockaddr *addr, socklen_t addr_len, bool bep51_support);
void dht_cache_remove_expired(dht_cache_t *cache);
void dht_cache_get_stats(dht_cache_t *cache, size_t *out_total, size_t *out_bep51);
void dht_cache_cleanup(dht_cache_t *cache);

#endif /* DHT_CACHE_H */
