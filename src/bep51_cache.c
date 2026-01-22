#include "bep51_cache.h"
#include "shared_node_pool.h"
#include "tree_routing.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <openssl/sha.h>

/* Binary file format constants */
#define BEP51_CACHE_MAGIC "BEP1"
#define BEP51_CACHE_VERSION 1
#define BEP51_CACHE_HEADER_SIZE 12
#define BEP51_CACHE_RECORD_SIZE 26
#define BEP51_CACHE_CHECKSUM_SIZE 32

/* File record format (26 bytes):
 * 20 bytes: node_id
 *  4 bytes: IPv4 address (network byte order)
 *  2 bytes: port (network byte order)
 */

bep51_cache_t *bep51_cache_create(size_t capacity) {
    if (capacity == 0) {
        log_msg(LOG_ERROR, "[bep51_cache] Cannot create cache with zero capacity");
        return NULL;
    }

    bep51_cache_t *cache = malloc(sizeof(bep51_cache_t));
    if (!cache) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to allocate cache structure");
        return NULL;
    }

    memset(cache, 0, sizeof(bep51_cache_t));
    cache->capacity = capacity;
    cache->count = 0;
    cache->head_idx = 0;
    cache->nodes_hash = NULL;

    /* Allocate FIFO array */
    cache->nodes_fifo = calloc(capacity, sizeof(bep51_cache_node_t *));
    if (!cache->nodes_fifo) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to allocate FIFO array for %zu nodes", capacity);
        free(cache);
        return NULL;
    }

    if (pthread_mutex_init(&cache->lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to initialize mutex");
        free(cache->nodes_fifo);
        free(cache);
        return NULL;
    }

    log_msg(LOG_DEBUG, "[bep51_cache] Created cache with capacity %zu", capacity);
    return cache;
}

int bep51_cache_add_node(bep51_cache_t *cache,
                         const uint8_t node_id[20],
                         const struct sockaddr_storage *addr) {
    if (!cache || !node_id || !addr) {
        return -1;
    }

    /* Only support IPv4 for now */
    if (addr->ss_family != AF_INET) {
        return -1;
    }

    struct sockaddr_in *sin = (struct sockaddr_in *)addr;
    uint32_t ip = ntohl(sin->sin_addr.s_addr);
    uint16_t port = ntohs(sin->sin_port);

    /* Sanity check: skip invalid IPs */
    if (ip == 0 || ip == 0xFFFFFFFF || port == 0) {
        return -1;
    }

    pthread_mutex_lock(&cache->lock);

    /* Check for duplicate using uthash */
    bep51_cache_node_t *existing = NULL;
    HASH_FIND(hh, cache->nodes_hash, node_id, 20, existing);

    if (existing) {
        /* Already cached, skip */
        pthread_mutex_unlock(&cache->lock);
        return 0;
    }

    /* Allocate new node */
    bep51_cache_node_t *new_node = malloc(sizeof(bep51_cache_node_t));
    if (!new_node) {
        pthread_mutex_unlock(&cache->lock);
        return -1;
    }

    memcpy(new_node->node_id, node_id, 20);
    memcpy(&new_node->addr, addr, sizeof(struct sockaddr_storage));

    /* FIFO eviction if cache is full */
    if (cache->count >= cache->capacity) {
        bep51_cache_node_t *evict = cache->nodes_fifo[cache->head_idx];
        if (evict) {
            HASH_DEL(cache->nodes_hash, evict);
            free(evict);
        }
    } else {
        cache->count++;
    }

    /* Add to FIFO and hash */
    cache->nodes_fifo[cache->head_idx] = new_node;
    HASH_ADD_KEYPTR(hh, cache->nodes_hash, new_node->node_id, 20, new_node);
    cache->head_idx = (cache->head_idx + 1) % cache->capacity;

    pthread_mutex_unlock(&cache->lock);
    return 0;
}

int bep51_cache_load_from_file(bep51_cache_t *cache, const char *path) {
    if (!cache || !path) {
        return -1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        log_msg(LOG_DEBUG, "[bep51_cache] Cache file not found: %s (first run?)", path);
        return -1;
    }

    log_msg(LOG_DEBUG, "[bep51_cache] Loading cache from %s", path);

    /* Read header */
    uint8_t header[BEP51_CACHE_HEADER_SIZE];
    if (fread(header, 1, BEP51_CACHE_HEADER_SIZE, fp) != BEP51_CACHE_HEADER_SIZE) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to read header");
        fclose(fp);
        return -1;
    }

    /* Validate magic */
    if (memcmp(header, BEP51_CACHE_MAGIC, 4) != 0) {
        log_msg(LOG_ERROR, "[bep51_cache] Invalid magic bytes");
        fclose(fp);
        return -1;
    }

    /* Parse version */
    uint32_t version;
    memcpy(&version, header + 4, 4);
    version = ntohl(version);

    if (version != BEP51_CACHE_VERSION) {
        log_msg(LOG_ERROR, "[bep51_cache] Unsupported version: %u (expected %u)",
                version, BEP51_CACHE_VERSION);
        fclose(fp);
        return -1;
    }

    /* Parse node count */
    uint32_t node_count;
    memcpy(&node_count, header + 8, 4);
    node_count = ntohl(node_count);

    log_msg(LOG_DEBUG, "[bep51_cache] Cache version %u, %u nodes", version, node_count);

    /* Validate file size */
    size_t expected_size = BEP51_CACHE_HEADER_SIZE +
                          (node_count * BEP51_CACHE_RECORD_SIZE) +
                          BEP51_CACHE_CHECKSUM_SIZE;

    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, BEP51_CACHE_HEADER_SIZE, SEEK_SET);

    if (file_size != (long)expected_size) {
        log_msg(LOG_ERROR, "[bep51_cache] File size mismatch: %ld bytes (expected %zu)",
                file_size, expected_size);
        fclose(fp);
        return -1;
    }

    /* Allocate buffer for records */
    size_t records_size = node_count * BEP51_CACHE_RECORD_SIZE;
    uint8_t *records = malloc(records_size);
    if (!records) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to allocate buffer for %u records", node_count);
        fclose(fp);
        return -1;
    }

    /* Read all records */
    if (fread(records, 1, records_size, fp) != records_size) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to read records");
        free(records);
        fclose(fp);
        return -1;
    }

    /* Read checksum */
    uint8_t stored_checksum[BEP51_CACHE_CHECKSUM_SIZE];
    if (fread(stored_checksum, 1, BEP51_CACHE_CHECKSUM_SIZE, fp) != BEP51_CACHE_CHECKSUM_SIZE) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to read checksum");
        free(records);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    /* Compute SHA-256 checksum of header + records */
    SHA256_CTX sha_ctx;
    uint8_t computed_checksum[BEP51_CACHE_CHECKSUM_SIZE];
    SHA256_Init(&sha_ctx);
    SHA256_Update(&sha_ctx, header, BEP51_CACHE_HEADER_SIZE);
    SHA256_Update(&sha_ctx, records, records_size);
    SHA256_Final(computed_checksum, &sha_ctx);

    /* Verify checksum */
    if (memcmp(stored_checksum, computed_checksum, BEP51_CACHE_CHECKSUM_SIZE) != 0) {
        log_msg(LOG_ERROR, "[bep51_cache] Checksum verification failed");
        free(records);
        return -1;
    }

    log_msg(LOG_DEBUG, "[bep51_cache] Checksum verified");

    /* Parse records and add to cache */
    int loaded = 0;
    for (uint32_t i = 0; i < node_count; i++) {
        uint8_t *rec = records + (i * BEP51_CACHE_RECORD_SIZE);

        uint8_t *node_id = rec;
        uint32_t ip;
        uint16_t port;

        memcpy(&ip, rec + 20, 4);
        memcpy(&port, rec + 24, 2);

        ip = ntohl(ip);
        port = ntohs(port);

        /* Sanity check */
        if (ip == 0 || ip == 0xFFFFFFFF || port == 0) {
            continue;
        }

        /* Convert to sockaddr_storage */
        struct sockaddr_storage addr;
        memset(&addr, 0, sizeof(addr));
        struct sockaddr_in *sin = (struct sockaddr_in *)&addr;
        sin->sin_family = AF_INET;
        sin->sin_addr.s_addr = htonl(ip);
        sin->sin_port = htons(port);

        /* Add to cache (bypasses mutex, we're in single-threaded init) */
        if (bep51_cache_add_node(cache, node_id, &addr) == 0) {
            loaded++;
        }
    }

    free(records);
    log_msg(LOG_DEBUG, "[bep51_cache] Loaded %d/%u nodes from cache", loaded, node_count);
    return 0;
}

int bep51_cache_save_to_file(bep51_cache_t *cache, const char *path) {
    if (!cache || !path) {
        return -1;
    }

    pthread_mutex_lock(&cache->lock);

    size_t count = cache->count;
    if (count == 0) {
        pthread_mutex_unlock(&cache->lock);
        log_msg(LOG_DEBUG, "[bep51_cache] Cache is empty, skipping save");
        return 0;
    }

    /* Create temp file path */
    char temp_path[1024];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);

    FILE *fp = fopen(temp_path, "wb");
    if (!fp) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to create temp file %s: %s",
                temp_path, strerror(errno));
        pthread_mutex_unlock(&cache->lock);
        return -1;
    }

    /* Build header */
    uint8_t header[BEP51_CACHE_HEADER_SIZE];
    memcpy(header, BEP51_CACHE_MAGIC, 4);

    uint32_t version = htonl(BEP51_CACHE_VERSION);
    memcpy(header + 4, &version, 4);

    uint32_t node_count = htonl((uint32_t)count);
    memcpy(header + 8, &node_count, 4);

    /* Write header */
    if (fwrite(header, 1, BEP51_CACHE_HEADER_SIZE, fp) != BEP51_CACHE_HEADER_SIZE) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to write header");
        fclose(fp);
        unlink(temp_path);
        pthread_mutex_unlock(&cache->lock);
        return -1;
    }

    /* Initialize SHA-256 context */
    SHA256_CTX sha_ctx;
    SHA256_Init(&sha_ctx);
    SHA256_Update(&sha_ctx, header, BEP51_CACHE_HEADER_SIZE);

    /* Write records */
    bep51_cache_node_t *node, *tmp;
    int written = 0;

    HASH_ITER(hh, cache->nodes_hash, node, tmp) {
        /* Only support IPv4 */
        if (node->addr.ss_family != AF_INET) {
            continue;
        }

        struct sockaddr_in *sin = (struct sockaddr_in *)&node->addr;
        uint32_t ip = htonl(ntohl(sin->sin_addr.s_addr));
        uint16_t port = htons(ntohs(sin->sin_port));

        /* Build record */
        uint8_t record[BEP51_CACHE_RECORD_SIZE];
        memcpy(record, node->node_id, 20);
        memcpy(record + 20, &ip, 4);
        memcpy(record + 24, &port, 2);

        /* Write and update checksum */
        if (fwrite(record, 1, BEP51_CACHE_RECORD_SIZE, fp) != BEP51_CACHE_RECORD_SIZE) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to write record %d", written);
            fclose(fp);
            unlink(temp_path);
            pthread_mutex_unlock(&cache->lock);
            return -1;
        }

        SHA256_Update(&sha_ctx, record, BEP51_CACHE_RECORD_SIZE);
        written++;
    }

    pthread_mutex_unlock(&cache->lock);

    /* Finalize checksum */
    uint8_t checksum[BEP51_CACHE_CHECKSUM_SIZE];
    SHA256_Final(checksum, &sha_ctx);

    /* Write checksum */
    if (fwrite(checksum, 1, BEP51_CACHE_CHECKSUM_SIZE, fp) != BEP51_CACHE_CHECKSUM_SIZE) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to write checksum");
        fclose(fp);
        unlink(temp_path);
        return -1;
    }

    fclose(fp);

    /* Atomic rename */
    if (rename(temp_path, path) != 0) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to rename %s to %s: %s",
                temp_path, path, strerror(errno));
        unlink(temp_path);
        return -1;
    }

    log_msg(LOG_DEBUG, "[bep51_cache] Saved %d nodes to %s", written, path);
    return 0;
}

int bep51_cache_populate_shared_pool(bep51_cache_t *cache,
                                     struct shared_node_pool *pool,
                                     int max_nodes) {
    if (!cache || !pool) {
        return 0;
    }

    pthread_mutex_lock(&cache->lock);

    int transferred = 0;
    bep51_cache_node_t *node, *tmp;

    HASH_ITER(hh, cache->nodes_hash, node, tmp) {
        if (transferred >= max_nodes) {
            break;
        }

        /* Add to shared pool */
        int rc = shared_node_pool_add_node(pool, node->node_id, &node->addr);
        if (rc == 0) {
            transferred++;
        }
    }

    pthread_mutex_unlock(&cache->lock);

    log_msg(LOG_DEBUG, "[bep51_cache] Transferred %d nodes to shared pool", transferred);
    return transferred;
}

size_t bep51_cache_get_count(bep51_cache_t *cache) {
    if (!cache) {
        return 0;
    }

    pthread_mutex_lock(&cache->lock);
    size_t count = cache->count;
    pthread_mutex_unlock(&cache->lock);

    return count;
}

int bep51_cache_get_random(bep51_cache_t *cache,
                           tree_node_t *out,
                           int count) {
    if (!cache || !out || count <= 0) {
        return 0;
    }

    pthread_mutex_lock(&cache->lock);

    size_t available = cache->count;
    if (available == 0) {
        pthread_mutex_unlock(&cache->lock);
        return 0;
    }

    int to_sample = (count < (int)available) ? count : (int)available;
    int sampled = 0;

    /* Iterate through FIFO buffer from random start */
    size_t start = rand() % cache->capacity;
    for (size_t i = 0; i < cache->capacity && sampled < to_sample; i++) {
        size_t idx = (start + i) % cache->capacity;
        bep51_cache_node_t *node = cache->nodes_fifo[idx];

        if (!node) {
            continue;  /* Empty slot in circular buffer */
        }

        /* Convert bep51_cache_node_t to tree_node_t */
        memcpy(out[sampled].node_id, node->node_id, 20);
        memcpy(&out[sampled].addr, &node->addr, sizeof(struct sockaddr_storage));
        out[sampled].last_seen = time(NULL);
        out[sampled].last_queried = 0;
        out[sampled].fail_count = 0;
        out[sampled].bep51_status = BEP51_CAPABLE;  /* Known BEP51-capable */
        out[sampled].next = NULL;
        sampled++;
    }

    pthread_mutex_unlock(&cache->lock);

    log_msg(LOG_DEBUG, "[bep51_cache] Sampled %d random nodes from cache", sampled);
    return sampled;
}

void bep51_cache_destroy(bep51_cache_t *cache) {
    if (!cache) {
        return;
    }

    pthread_mutex_lock(&cache->lock);

    /* Free all nodes in hash */
    bep51_cache_node_t *node, *tmp;
    HASH_ITER(hh, cache->nodes_hash, node, tmp) {
        HASH_DEL(cache->nodes_hash, node);
        free(node);
    }

    /* Free FIFO array */
    free(cache->nodes_fifo);

    size_t final_count = cache->count;
    pthread_mutex_unlock(&cache->lock);

    log_msg(LOG_DEBUG, "[bep51_cache] Destroyed cache (final count: %zu)", final_count);

    pthread_mutex_destroy(&cache->lock);
    free(cache);
}
