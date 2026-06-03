#define _DEFAULT_SOURCE  /* For usleep */

#include "bep51_cache.h"
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
#define BEP51_CACHE_VERSION 2
#define BEP51_CACHE_HEADER_SIZE 12
#define BEP51_CACHE_RECORD_SIZE 39
#define BEP51_CACHE_CHECKSUM_SIZE 32
#define BEP51_CACHE_FAMILY_IPV4 0x04
#define BEP51_CACHE_FAMILY_IPV6 0x06

/* File record format (39 bytes, version 2):
 * 20 bytes: node_id
 *  1 byte:  address family (0x04=IPv4, 0x06=IPv6)
 * 16 bytes: IP address (zero-padded to 16 bytes for IPv4)
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


/**
 * Normalize and validate an address for cache storage.
 * Accepts AF_INET, AF_INET6 (native and IPv4-mapped).
 * IPv4-mapped IPv6 is converted to native AF_INET.
 * Rejects all-zero / broadcast IPv4, all-zero IPv6, port 0.
 * @param in         Input socket address
 * @param out        Output normalized socket address
 * @param out_family On-disk family byte (0x04 or 0x06)
 * @param out_port   Port in host byte order
 * @return 0 on success, -1 on validation failure
 */
static int normalize_addr_for_cache(const struct sockaddr_storage *in,
                                     struct sockaddr_storage *out,
                                     uint8_t *out_family,
                                     uint16_t *out_port) {
    if (!in || !out || !out_family || !out_port) {
        return -1;
    }

    memset(out, 0, sizeof(*out));

    if (in->ss_family == AF_INET) {
        const struct sockaddr_in *sin = (const struct sockaddr_in *)in;
        uint32_t ip = ntohl(sin->sin_addr.s_addr);
        uint16_t port = ntohs(sin->sin_port);

        if (ip == 0 || ip == 0xFFFFFFFF || port == 0) {
            return -1;
        }

        memcpy(out, in, sizeof(struct sockaddr_in));
        *out_family = BEP51_CACHE_FAMILY_IPV4;
        *out_port = port;
        return 0;
    }

    if (in->ss_family == AF_INET6) {
        const struct sockaddr_in6 *sin6 = (const struct sockaddr_in6 *)in;
        uint16_t port = ntohs(sin6->sin6_port);

        if (port == 0) {
            return -1;
        }

        /* Reject all-zero IPv6 address (::) */
        const uint8_t *s6 = sin6->sin6_addr.s6_addr;
        int all_zero = 1;
        for (int i = 0; i < 16; i++) {
            if (s6[i] != 0) { all_zero = 0; break; }
        }
        if (all_zero) {
            return -1;
        }

        /* Check for IPv4-mapped IPv6 (::ffff:a.b.c.d) */
        if (IN6_IS_ADDR_V4MAPPED(&sin6->sin6_addr)) {
            uint32_t ipv4;
            memcpy(&ipv4, s6 + 12, 4);
            uint32_t ip = ntohl(ipv4);

            if (ip == 0 || ip == 0xFFFFFFFF) {
                return -1;
            }

            /* Store as native AF_INET */
            struct sockaddr_in *out_sin = (struct sockaddr_in *)out;
            out_sin->sin_family = AF_INET;
            out_sin->sin_addr.s_addr = ipv4;
            out_sin->sin_port = sin6->sin6_port;
            *out_family = BEP51_CACHE_FAMILY_IPV4;
            *out_port = port;
            return 0;
        }

        /* Native IPv6 */
        memcpy(out, in, sizeof(struct sockaddr_in6));
        *out_family = BEP51_CACHE_FAMILY_IPV6;
        *out_port = port;
        return 0;
    }

    return -1;
}
int bep51_cache_add_node(bep51_cache_t *cache,
                         const uint8_t node_id[20],
                         const struct sockaddr_storage *addr) {
    if (!cache || !node_id || !addr) {
        return -1;
    }

    /* Normalize and validate address */
    struct sockaddr_storage normalized;
    uint8_t addr_family;
    uint16_t addr_port;
    if (normalize_addr_for_cache(addr, &normalized, &addr_family, &addr_port) != 0) {
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
    memcpy(&new_node->addr, &normalized, sizeof(struct sockaddr_storage));

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

    /* Parse node count */
    uint32_t node_count;
    memcpy(&node_count, header + 8, 4);
    node_count = ntohl(node_count);

    log_msg(LOG_DEBUG, "[bep51_cache] Cache version %u, %u nodes", version, node_count);

    /* Get file size for validation */
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);

    if (version == 1) {
        /* --- v1 format: 26-byte records (IPv4 only), with 32-byte SHA-256 trailer --- */
        size_t v1_record_size = 26;
        size_t expected_size = BEP51_CACHE_HEADER_SIZE + (node_count * v1_record_size) + BEP51_CACHE_CHECKSUM_SIZE;

        if (file_size != (long)expected_size) {
            log_msg(LOG_ERROR, "[bep51_cache] v1 file size mismatch: %ld bytes (expected %zu)",
                    file_size, expected_size);
            fclose(fp);
            return -1;
        }

        size_t records_size = node_count * v1_record_size;
        uint8_t *records = malloc(records_size);
        if (!records) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to allocate buffer for %u v1 records", node_count);
            fclose(fp);
            return -1;
        }

        fseek(fp, BEP51_CACHE_HEADER_SIZE, SEEK_SET);
        if (fread(records, 1, records_size, fp) != records_size) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to read v1 records");
            free(records);
            fclose(fp);
            return -1;
        }

        uint8_t stored_checksum[BEP51_CACHE_CHECKSUM_SIZE];
        if (fread(stored_checksum, 1, BEP51_CACHE_CHECKSUM_SIZE, fp) != BEP51_CACHE_CHECKSUM_SIZE) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to read v1 checksum");
            free(records);
            fclose(fp);
            return -1;
        }
        fclose(fp);

        /* Verify SHA-256 over header + records */
        SHA256_CTX sha_ctx;
        uint8_t computed_checksum[BEP51_CACHE_CHECKSUM_SIZE];
        SHA256_Init(&sha_ctx);
        SHA256_Update(&sha_ctx, header, BEP51_CACHE_HEADER_SIZE);
        SHA256_Update(&sha_ctx, records, records_size);
        SHA256_Final(computed_checksum, &sha_ctx);

        if (memcmp(stored_checksum, computed_checksum, BEP51_CACHE_CHECKSUM_SIZE) != 0) {
            log_msg(LOG_ERROR, "[bep51_cache] v1 checksum verification failed");
            free(records);
            return -1;
        }

        /* Parse v1 records: 20 node_id + 4 IPv4 (network order) + 2 port (network order) */
        int loaded = 0;
        for (uint32_t i = 0; i < node_count; i++) {
            uint8_t *rec = records + (i * v1_record_size);
            uint8_t *node_id = rec;

            uint32_t ip_raw;
            memcpy(&ip_raw, rec + 20, 4);
            uint32_t ip = ntohl(ip_raw);

            uint16_t port_raw;
            memcpy(&port_raw, rec + 24, 2);
            uint16_t port = ntohs(port_raw);

            if (ip == 0 || ip == 0xFFFFFFFF || port == 0) {
                continue;
            }

            struct sockaddr_storage addr;
            memset(&addr, 0, sizeof(addr));
            struct sockaddr_in *sin = (struct sockaddr_in *)&addr;
            sin->sin_family = AF_INET;
            sin->sin_addr.s_addr = ip_raw;
            sin->sin_port = htons(port);

            if (bep51_cache_add_node(cache, node_id, &addr) == 0) {
                loaded++;
            }
        }

        free(records);
        log_msg(LOG_INFO, "[bep51_cache] Loaded %d/%u nodes from v1 cache; rewriting in v2 format", loaded, node_count);

        if (bep51_cache_save_to_file(cache, path) != 0) {
            log_msg(LOG_WARN, "[bep51_cache] Failed to rewrite v1 cache to v2 format");
        }

        return 0;
    }

    if (version == 2) {
        /* --- v2 format: 39-byte records, with or without 32-byte SHA-256 trailer --- */
        size_t expected_chk = BEP51_CACHE_HEADER_SIZE + (node_count * BEP51_CACHE_RECORD_SIZE) + BEP51_CACHE_CHECKSUM_SIZE;
        size_t expected_nochk = BEP51_CACHE_HEADER_SIZE + (node_count * BEP51_CACHE_RECORD_SIZE);

        int has_checksum;
        if (file_size == (long)expected_chk) {
            has_checksum = 1;
        } else if (file_size == (long)expected_nochk) {
            has_checksum = 0;
            log_msg(LOG_WARN, "[bep51_cache] v2 cache file lacks trailing SHA-256; loading without verification and rewriting in canonical form");
        } else {
            log_msg(LOG_ERROR, "[bep51_cache] v2 file size mismatch: %ld bytes (expected %zu with checksum, %zu without)",
                    file_size, expected_chk, expected_nochk);
            fclose(fp);
            return -1;
        }

        size_t records_size = node_count * BEP51_CACHE_RECORD_SIZE;
        uint8_t *records = malloc(records_size);
        if (!records) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to allocate buffer for %u records", node_count);
            fclose(fp);
            return -1;
        }

        fseek(fp, BEP51_CACHE_HEADER_SIZE, SEEK_SET);
        if (fread(records, 1, records_size, fp) != records_size) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to read records");
            free(records);
            fclose(fp);
            return -1;
        }

        if (has_checksum) {
            uint8_t stored_checksum[BEP51_CACHE_CHECKSUM_SIZE];
            if (fread(stored_checksum, 1, BEP51_CACHE_CHECKSUM_SIZE, fp) != BEP51_CACHE_CHECKSUM_SIZE) {
                log_msg(LOG_ERROR, "[bep51_cache] Failed to read checksum");
                free(records);
                fclose(fp);
                return -1;
            }
            fclose(fp);

            SHA256_CTX sha_ctx;
            uint8_t computed_checksum[BEP51_CACHE_CHECKSUM_SIZE];
            SHA256_Init(&sha_ctx);
            SHA256_Update(&sha_ctx, header, BEP51_CACHE_HEADER_SIZE);
            SHA256_Update(&sha_ctx, records, records_size);
            SHA256_Final(computed_checksum, &sha_ctx);

            if (memcmp(stored_checksum, computed_checksum, BEP51_CACHE_CHECKSUM_SIZE) != 0) {
                log_msg(LOG_ERROR, "[bep51_cache] Checksum verification failed");
                free(records);
                return -1;
            }

            log_msg(LOG_DEBUG, "[bep51_cache] Checksum verified");
        } else {
            fclose(fp);
        }

        /* Parse v2 records */
        int loaded = 0;
        for (uint32_t i = 0; i < node_count; i++) {
            uint8_t *rec = records + (i * BEP51_CACHE_RECORD_SIZE);

            uint8_t *node_id = rec;
            uint8_t family = rec[20];
            uint16_t port;
            memcpy(&port, rec + 37, 2);
            port = ntohs(port);

            if (port == 0) {
                continue;
            }

            struct sockaddr_storage addr;
            memset(&addr, 0, sizeof(addr));

            if (family == BEP51_CACHE_FAMILY_IPV4) {
                struct sockaddr_in *sin = (struct sockaddr_in *)&addr;
                sin->sin_family = AF_INET;
                uint32_t ip;
                memcpy(&ip, rec + 21 + 12, 4);
                sin->sin_addr.s_addr = ip;
                sin->sin_port = htons(port);
            } else if (family == BEP51_CACHE_FAMILY_IPV6) {
                struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&addr;
                sin6->sin6_family = AF_INET6;
                memcpy(&sin6->sin6_addr, rec + 21, 16);
                sin6->sin6_port = htons(port);
            } else {
                continue;
            }

            if (bep51_cache_add_node(cache, node_id, &addr) == 0) {
                loaded++;
            }
        }

        free(records);
        log_msg(LOG_DEBUG, "[bep51_cache] Loaded %d/%u nodes from cache", loaded, node_count);

        if (!has_checksum) {
            if (bep51_cache_save_to_file(cache, path) != 0) {
                log_msg(LOG_WARN, "[bep51_cache] Failed to rewrite cache file in canonical format");
            }
        }

        return 0;
    }

    /* Unknown version */
    log_msg(LOG_ERROR, "[bep51_cache] Unsupported version: %u (expected %u)",
            version, BEP51_CACHE_VERSION);
    fclose(fp);
    return -1;
}

static int write_record(FILE *fp, const bep51_cache_node_t *node) {
    uint8_t rec[BEP51_CACHE_RECORD_SIZE];
    memset(rec, 0, BEP51_CACHE_RECORD_SIZE);

    /* Copy node_id */
    memcpy(rec, node->node_id, 20);

    uint16_t port;
    if (node->addr.ss_family == AF_INET) {
        struct sockaddr_in *sin = (struct sockaddr_in *)&node->addr;
        rec[20] = BEP51_CACHE_FAMILY_IPV4;
        /* IPv4 address goes in the last 4 bytes of the 16-byte field */
        memcpy(rec + 21 + 12, &sin->sin_addr.s_addr, 4);
        port = sin->sin_port;
    } else if (node->addr.ss_family == AF_INET6) {
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&node->addr;
        rec[20] = BEP51_CACHE_FAMILY_IPV6;
        memcpy(rec + 21, &sin6->sin6_addr, 16);
        port = sin6->sin6_port;
    } else {
        return -1;
    }

    memcpy(rec + 37, &port, 2);

    if (fwrite(rec, 1, BEP51_CACHE_RECORD_SIZE, fp) != BEP51_CACHE_RECORD_SIZE) {
        return -1;
    }
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

    if (fwrite(header, 1, BEP51_CACHE_HEADER_SIZE, fp) != BEP51_CACHE_HEADER_SIZE) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to write header");
        fclose(fp);
        pthread_mutex_unlock(&cache->lock);
        unlink(temp_path);
        return -1;
    }

    /* Iterate FIFO and write records */
    int written = 0;
    for (size_t i = 0; i < cache->capacity; i++) {
        bep51_cache_node_t *node = cache->nodes_fifo[i];
        if (!node) {
            continue;
        }

        /* Write record using helper */
        if (write_record(fp, node) != 0) {
            log_msg(LOG_ERROR, "[bep51_cache] Failed to write node record");
            fclose(fp);
            pthread_mutex_unlock(&cache->lock);
            unlink(temp_path);
            return -1;
        }

        written++;
    }

    /* Compute SHA-256 checksum of header + records */
    SHA256_CTX sha_ctx;
    uint8_t checksum[BEP51_CACHE_CHECKSUM_SIZE];
    SHA256_Init(&sha_ctx);
    SHA256_Update(&sha_ctx, header, BEP51_CACHE_HEADER_SIZE);
    fseek(fp, BEP51_CACHE_HEADER_SIZE, SEEK_SET);
    /* Read and hash records */
    uint8_t *rec_buf = malloc(BEP51_CACHE_RECORD_SIZE);
    if (rec_buf) {
        while (fread(rec_buf, 1, BEP51_CACHE_RECORD_SIZE, fp) == BEP51_CACHE_RECORD_SIZE) {
            SHA256_Update(&sha_ctx, rec_buf, BEP51_CACHE_RECORD_SIZE);
        }
        free(rec_buf);
    }
    SHA256_Final(checksum, &sha_ctx);

    if (fwrite(checksum, 1, BEP51_CACHE_CHECKSUM_SIZE, fp) != BEP51_CACHE_CHECKSUM_SIZE) {
        log_msg(LOG_ERROR, "[bep51_cache] Failed to write checksum");
        fclose(fp);
        pthread_mutex_unlock(&cache->lock);
        unlink(temp_path);
        return -1;
    }

    fclose(fp);
    pthread_mutex_unlock(&cache->lock);

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
