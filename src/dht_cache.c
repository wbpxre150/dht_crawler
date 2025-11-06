#include "dht_cache.h"
#include "dht_crawler.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>

/* Cache file format version - incremented to 3 for BEP51-only format with node IDs */
#define DHT_CACHE_VERSION 3

/* Initialize DHT cache */
int dht_cache_init(dht_cache_t *cache, const char *cache_file) {
    if (!cache || !cache_file) {
        return -1;
    }

    memset(cache, 0, sizeof(dht_cache_t));
    cache->cache_file = cache_file;
    cache->capacity = DHT_CACHE_MAX_PEERS;
    cache->peers = calloc(cache->capacity, sizeof(dht_cached_peer_t));
    
    if (!cache->peers) {
        log_msg(LOG_ERROR, "Failed to allocate memory for peer cache");
        return -1;
    }

    cache->count = 0;
    cache->last_save = time(NULL);

    log_msg(LOG_DEBUG, "DHT cache initialized with capacity %zu", cache->capacity);
    return 0;
}

/* Load cached peers from file */
int dht_cache_load(dht_cache_t *cache) {
    if (!cache || !cache->cache_file) {
        return -1;
    }

    FILE *f = fopen(cache->cache_file, "rb");
    if (!f) {
        if (errno == ENOENT) {
            log_msg(LOG_INFO, "No cache file found, starting fresh");
            return 0;
        }
        log_msg(LOG_WARN, "Failed to open cache file: %s", strerror(errno));
        return -1;
    }

    /* Read version */
    uint32_t version;
    if (fread(&version, sizeof(version), 1, f) != 1) {
        log_msg(LOG_WARN, "Failed to read cache version");
        fclose(f);
        return -1;
    }

    if (version != DHT_CACHE_VERSION) {
        log_msg(LOG_INFO, "Old cache format detected (v%u), ignoring and will create new BEP51-only cache (v%u)", 
                version, DHT_CACHE_VERSION);
        fclose(f);
        return 0;  /* Not an error - just start fresh */
    }

    /* Read peer count */
    uint32_t count;
    if (fread(&count, sizeof(count), 1, f) != 1) {
        log_msg(LOG_WARN, "Failed to read peer count");
        fclose(f);
        return -1;
    }

    if (count > cache->capacity) {
        log_msg(LOG_WARN, "Cache contains too many peers (%u), limiting to %zu",
                count, cache->capacity);
        count = cache->capacity;
    }

    /* Read peers */
    cache->count = 0;
    time_t now = time(NULL);
    time_t expiry_threshold = now - (DHT_CACHE_EXPIRY_DAYS * 24 * 60 * 60);

    for (uint32_t i = 0; i < count; i++) {
        dht_cached_peer_t peer;
        
        /* Read node ID */
        if (fread(&peer.node_id, 20, 1, f) != 1) {
            log_msg(LOG_WARN, "Failed to read node_id at index %u", i);
            break;
        }
        
        /* Read address length */
        if (fread(&peer.addr_len, sizeof(peer.addr_len), 1, f) != 1) {
            log_msg(LOG_WARN, "Failed to read addr_len at index %u", i);
            break;
        }

        /* Validate address length */
        if (peer.addr_len > sizeof(peer.addr)) {
            log_msg(LOG_WARN, "Invalid addr_len %u at index %u, skipping", peer.addr_len, i);
            fseek(f, peer.addr_len + sizeof(time_t) + sizeof(bool), SEEK_CUR);
            continue;
        }

        /* Read address */
        if (fread(&peer.addr, peer.addr_len, 1, f) != 1) {
            log_msg(LOG_WARN, "Failed to read address at index %u", i);
            break;
        }

        /* Read last_seen */
        if (fread(&peer.last_seen, sizeof(peer.last_seen), 1, f) != 1) {
            log_msg(LOG_WARN, "Failed to read last_seen at index %u", i);
            break;
        }
        
        /* Read bep51_support */
        if (fread(&peer.bep51_support, sizeof(peer.bep51_support), 1, f) != 1) {
            log_msg(LOG_WARN, "Failed to read bep51_support at index %u", i);
            break;
        }

        /* Skip expired peers */
        if (peer.last_seen < expiry_threshold) {
            log_msg(LOG_DEBUG, "Skipping expired peer (age: %ld days)",
                    (now - peer.last_seen) / (24 * 60 * 60));
            continue;
        }
        
        /* Skip non-BEP51 peers */
        if (!peer.bep51_support) {
            log_msg(LOG_DEBUG, "Skipping non-BEP51 peer at index %u", i);
            continue;
        }

        /* Add to cache */
        if (cache->count < cache->capacity) {
            memcpy(&cache->peers[cache->count], &peer, sizeof(peer));
            cache->count++;
        }
    }

    fclose(f);
    log_msg(LOG_INFO, "Loaded %zu cached DHT peers from %s", cache->count, cache->cache_file);
    return 0;
}

/* Save cached peers to file */
int dht_cache_save(dht_cache_t *cache) {
    if (!cache || !cache->cache_file) {
        return -1;
    }

    /* Remove expired peers before saving */
    dht_cache_remove_expired(cache);

    if (cache->count == 0) {
        log_msg(LOG_DEBUG, "No peers to cache, skipping save");
        return 0;
    }

    /* Count BEP51 peers first */
    size_t bep51_count = 0;
    for (size_t i = 0; i < cache->count; i++) {
        if (cache->peers[i].bep51_support) {
            bep51_count++;
        }
    }
    
    /* Skip save if no BEP51 peers */
    if (bep51_count == 0) {
        log_msg(LOG_DEBUG, "No BEP51 peers to cache, skipping save");
        return 0;
    }

    /* Write to temporary file first */
    char temp_file[512];
    snprintf(temp_file, sizeof(temp_file), "%s.tmp", cache->cache_file);

    FILE *f = fopen(temp_file, "wb");
    if (!f) {
        log_msg(LOG_ERROR, "Failed to open temp cache file: %s", strerror(errno));
        return -1;
    }

    /* Write version */
    uint32_t version = DHT_CACHE_VERSION;
    if (fwrite(&version, sizeof(version), 1, f) != 1) {
        log_msg(LOG_ERROR, "Failed to write cache version");
        fclose(f);
        unlink(temp_file);
        return -1;
    }

    /* Write BEP51 peer count (not total count) */
    uint32_t count = bep51_count;
    if (fwrite(&count, sizeof(count), 1, f) != 1) {
        log_msg(LOG_ERROR, "Failed to write peer count");
        fclose(f);
        unlink(temp_file);
        return -1;
    }

    /* Write BEP51 peers only */
    for (size_t i = 0; i < cache->count; i++) {
        dht_cached_peer_t *peer = &cache->peers[i];
        
        /* Only write BEP51-capable nodes */
        if (!peer->bep51_support) {
            continue;
        }

        if (fwrite(&peer->node_id, 20, 1, f) != 1 ||
            fwrite(&peer->addr_len, sizeof(peer->addr_len), 1, f) != 1 ||
            fwrite(&peer->addr, peer->addr_len, 1, f) != 1 ||
            fwrite(&peer->last_seen, sizeof(peer->last_seen), 1, f) != 1 ||
            fwrite(&peer->bep51_support, sizeof(peer->bep51_support), 1, f) != 1) {
            log_msg(LOG_ERROR, "Failed to write peer at index %zu", i);
            fclose(f);
            unlink(temp_file);
            return -1;
        }
    }

    fclose(f);

    /* Atomically replace old cache file */
    if (rename(temp_file, cache->cache_file) != 0) {
        log_msg(LOG_ERROR, "Failed to rename cache file: %s", strerror(errno));
        unlink(temp_file);
        return -1;
    }

    cache->last_save = time(NULL);
    log_msg(LOG_INFO, "Saved %zu BEP51-capable DHT peers to cache (filtered from %zu total)", 
            bep51_count, cache->count);
    return 0;
}

/* Add a peer to cache */
int dht_cache_add_peer(dht_cache_t *cache, const unsigned char *node_id, 
                       const struct sockaddr *addr, socklen_t addr_len, bool bep51_support) {
    if (!cache || !node_id || !addr || addr_len == 0) {
        return -1;
    }
    
    /* Only cache BEP51-capable nodes */
    if (!bep51_support) {
        return 0;  /* Silently skip non-BEP51 nodes */
    }

    /* Check if peer already exists (by node ID) */
    for (size_t i = 0; i < cache->count; i++) {
        if (memcmp(cache->peers[i].node_id, node_id, 20) == 0) {
            /* Update existing peer's info */
            memcpy(&cache->peers[i].addr, addr, addr_len);
            cache->peers[i].addr_len = addr_len;
            cache->peers[i].last_seen = time(NULL);
            cache->peers[i].bep51_support = bep51_support;
            return 0;
        }
    }

    /* Add new peer */
    if (cache->count < cache->capacity) {
        dht_cached_peer_t *peer = &cache->peers[cache->count];
        memcpy(peer->node_id, node_id, 20);
        memcpy(&peer->addr, addr, addr_len);
        peer->addr_len = addr_len;
        peer->last_seen = time(NULL);
        peer->bep51_support = bep51_support;
        cache->count++;
        return 0;
    }

    /* Cache full - replace oldest peer */
    size_t oldest_idx = 0;
    time_t oldest_time = cache->peers[0].last_seen;
    
    for (size_t i = 1; i < cache->count; i++) {
        if (cache->peers[i].last_seen < oldest_time) {
            oldest_time = cache->peers[i].last_seen;
            oldest_idx = i;
        }
    }

    /* Replace oldest */
    dht_cached_peer_t *peer = &cache->peers[oldest_idx];
    memcpy(peer->node_id, node_id, 20);
    memcpy(&peer->addr, addr, addr_len);
    peer->addr_len = addr_len;
    peer->last_seen = time(NULL);
    peer->bep51_support = bep51_support;

    return 0;
}

/* Remove expired peers from cache */
void dht_cache_remove_expired(dht_cache_t *cache) {
    if (!cache) {
        return;
    }

    time_t now = time(NULL);
    time_t expiry_threshold = now - (DHT_CACHE_EXPIRY_DAYS * 24 * 60 * 60);
    size_t removed = 0;

    /* Compact array by removing expired entries */
    size_t write_idx = 0;
    for (size_t read_idx = 0; read_idx < cache->count; read_idx++) {
        if (cache->peers[read_idx].last_seen >= expiry_threshold) {
            if (write_idx != read_idx) {
                memcpy(&cache->peers[write_idx], &cache->peers[read_idx],
                       sizeof(dht_cached_peer_t));
            }
            write_idx++;
        } else {
            removed++;
        }
    }

    cache->count = write_idx;

    if (removed > 0) {
        log_msg(LOG_DEBUG, "Removed %zu expired peers from cache", removed);
    }
}

/* Get cache statistics */
void dht_cache_get_stats(dht_cache_t *cache, size_t *out_total, size_t *out_bep51) {
    if (!cache) {
        return;
    }
    
    if (out_total) {
        *out_total = cache->count;
    }
    
    if (out_bep51) {
        size_t bep51_count = 0;
        for (size_t i = 0; i < cache->count; i++) {
            if (cache->peers[i].bep51_support) {
                bep51_count++;
            }
        }
        *out_bep51 = bep51_count;
    }
}

/* Cleanup cache */
void dht_cache_cleanup(dht_cache_t *cache) {
    if (!cache) {
        return;
    }

    if (cache->peers) {
        free(cache->peers);
        cache->peers = NULL;
    }

    cache->count = 0;
    cache->capacity = 0;
}
