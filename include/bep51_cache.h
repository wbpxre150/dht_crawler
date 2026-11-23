#ifndef BEP51_CACHE_H
#define BEP51_CACHE_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <netinet/in.h>
#include "uthash.h"

/* Forward declarations */
struct tree_node;

/**
 * BEP51 Cache Node
 * Represents a single DHT node that has proven BEP51 capability
 */
typedef struct bep51_cache_node {
    uint8_t node_id[20];              // 20-byte DHT node ID
    struct sockaddr_storage addr;     // Node address (IPv4/IPv6)
    UT_hash_handle hh;                // uthash handle for deduplication
} bep51_cache_node_t;

/**
 * BEP51 Cache
 * Thread-safe cache for BEP51-capable nodes with FIFO eviction
 */
typedef struct bep51_cache {
    bep51_cache_node_t *nodes_hash;   // uthash table for O(1) dedup
    bep51_cache_node_t **nodes_fifo;  // Circular buffer for FIFO eviction
    size_t capacity;                  // Maximum nodes
    size_t count;                     // Current node count
    size_t head_idx;                  // FIFO insertion index
    pthread_mutex_t lock;             // Thread-safe access
} bep51_cache_t;

/**
 * Create a new BEP51 cache
 * @param capacity Maximum number of nodes to cache
 * @return Pointer to created cache, or NULL on failure
 */
bep51_cache_t *bep51_cache_create(size_t capacity);

/**
 * Add a node to the cache (thread-safe, with deduplication and FIFO eviction)
 * @param cache The cache
 * @param node_id 20-byte DHT node ID
 * @param addr Socket address of the node
 * @return 0 on success, -1 on failure
 */
int bep51_cache_add_node(bep51_cache_t *cache,
                         const uint8_t node_id[20],
                         const struct sockaddr_storage *addr);

/**
 * Load cache from binary file
 * @param cache The cache to populate
 * @param path File path to load from
 * @return 0 on success, -1 on failure (strict validation)
 */
int bep51_cache_load_from_file(bep51_cache_t *cache, const char *path);

/**
 * Save cache to binary file (atomic write with temp file + rename)
 * @param cache The cache to save
 * @param path File path to save to
 * @return 0 on success, -1 on failure
 */
int bep51_cache_save_to_file(bep51_cache_t *cache, const char *path);

/**
 * Fetch the published cache over HTTP(S) and install it.
 *
 * Downloads @p url to "<path>.download", validates it with the same magic and
 * sha256 checks bep51_cache_load_from_file() applies (against a scratch cache),
 * and only then renames it over @p path and merges it into @p cache. A failed,
 * truncated or corrupt download leaves the existing file and cache untouched, so
 * the caller keeps its cold-start fallback.
 *
 * @param cache       cache to populate on success
 * @param url         http:// or https:// URL of a .dat cache file
 * @param path        local cache path to replace
 * @param timeout_sec total deadline for the transfer; 0 = http_fetch default
 * @param ca_file     optional CA bundle override (NULL = system store)
 * @param ca_dir      optional hashed CA directory (NULL = none)
 * @return 0 on success, -1 otherwise
 */
int bep51_cache_fetch_remote(bep51_cache_t *cache, const char *url, const char *path,
                             int timeout_sec, const char *ca_file, const char *ca_dir);

/**
 * Outcome of one published-cache fetch.
 *
 * The numeric values are internal; the strings rendered by
 * bep51_cache_fetch_result_name() are the wire contract (/stats
 * bep51_cache_fetch.last_result), so a published block can never be blank.
 */
typedef enum {
    BEP51_FETCH_NOT_ATTEMPTED = 0, /*: no fetch has run in this process */
    BEP51_FETCH_OK,                /*: downloaded, validated, installed, loaded */
    BEP51_FETCH_DISABLED,          /*: the URL is empty, so nothing is ever fetched */
    BEP51_FETCH_HTTP_ERROR,        /*: the server answered and refused ("http_<status>") */
    BEP51_FETCH_SHA_MISMATCH,      /*: a 200 body failed the loader's magic+sha256 gate */
    BEP51_FETCH_LOAD_FAILED,       /*: validated, but could not be installed or loaded */
    BEP51_FETCH_NETWORK_ERROR      /*: no HTTP status at all (DNS, connect, TLS, deadline) */
} bep51_cache_fetch_result_t;

/**
 * One fetch's numbers, filled by bep51_cache_fetch_remote_ex().
 *
 * `bytes` is what the transfer wrote, so a 200 whose body then fails the sha256
 * gate still reports its full size; `nodes_loaded` is only set on success.
 */
typedef struct {
    bep51_cache_fetch_result_t result;
    int http_status;      /*: 0 = no response was received */
    long long bytes;      /*: body bytes received by the transfer */
    size_t nodes_loaded;  /*: nodes in the cache after a successful install */
    int duration_ms;      /*: wall time of the whole fetch, validation included */
    char error[192];      /*: human-readable reason when the result is not OK */
} bep51_cache_fetch_info_t;

/**
 * Render an outcome as its /stats string: "ok", "disabled",
 * "not_attempted", "sha_mismatch", "load_failed", "network_error" or
 * "http_<status>". Always writes a non-empty string into @p out.
 */
const char *bep51_cache_fetch_result_name(int result, int http_status,
                                          char *out, size_t out_len);

/**
 * Same as bep51_cache_fetch_remote(), plus the outcome in @p info (which may
 * be NULL). Callers that publish the result use this one; the plain wrapper
 * exists so the historical call sites keep their signature.
 */
int bep51_cache_fetch_remote_ex(bep51_cache_t *cache, const char *url, const char *path,
                                int timeout_sec, const char *ca_file, const char *ca_dir,
                                bep51_cache_fetch_info_t *info);

/**
 * Get current node count in cache (thread-safe)
 * @param cache The cache
 * @return Number of nodes currently cached
 */
size_t bep51_cache_get_count(bep51_cache_t *cache);

/**
 * Get random nodes from the cache (thread-safe)
 * @param cache The cache
 * @param out Output array to store sampled nodes (caller allocates)
 * @param count Number of nodes to sample
 * @return Number of nodes actually sampled
 */
int bep51_cache_get_random(bep51_cache_t *cache,
                           struct tree_node *out,
                           int count);

/**
 * Destroy the cache and free all resources
 * @param cache The cache to destroy
 */
void bep51_cache_destroy(bep51_cache_t *cache);

#endif // BEP51_CACHE_H
