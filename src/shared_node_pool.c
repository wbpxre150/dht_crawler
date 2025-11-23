#include "shared_node_pool.h"
#include "tree_routing.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

shared_node_pool_t *shared_node_pool_create(size_t capacity) {
    if (capacity == 0) {
        log_msg(LOG_ERROR, "Cannot create node pool with zero capacity");
        return NULL;
    }

    shared_node_pool_t *pool = malloc(sizeof(shared_node_pool_t));
    if (!pool) {
        log_msg(LOG_ERROR, "Failed to allocate shared_node_pool_t");
        return NULL;
    }

    pool->nodes = calloc(capacity, sizeof(tree_node_t));
    if (!pool->nodes) {
        log_msg(LOG_ERROR, "Failed to allocate node array for %zu nodes", capacity);
        free(pool);
        return NULL;
    }

    pool->capacity = capacity;
    pool->count = 0;

    if (pthread_mutex_init(&pool->lock, NULL) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize mutex for shared node pool");
        free(pool->nodes);
        free(pool);
        return NULL;
    }

    log_msg(LOG_INFO, "Created shared node pool with capacity %zu", capacity);
    return pool;
}

int shared_node_pool_add_node(shared_node_pool_t *pool,
                              const uint8_t node_id[20],
                              const struct sockaddr_storage *addr) {
    if (!pool || !node_id || !addr) {
        return -1;
    }

    pthread_mutex_lock(&pool->lock);

    // Check if pool is full
    if (pool->count >= pool->capacity) {
        pthread_mutex_unlock(&pool->lock);
        return -1;
    }

    // Check for duplicates (simple linear search - acceptable for bootstrap phase)
    for (size_t i = 0; i < pool->count; i++) {
        if (memcmp(pool->nodes[i].node_id, node_id, 20) == 0) {
            // Duplicate found, skip
            pthread_mutex_unlock(&pool->lock);
            return -1;
        }
    }

    // Add new node
    tree_node_t *node = &pool->nodes[pool->count];
    memcpy(node->node_id, node_id, 20);
    memcpy(&node->addr, addr, sizeof(struct sockaddr_storage));
    node->last_seen = time(NULL);
    node->fail_count = 0;
    node->next = NULL;

    pool->count++;

    pthread_mutex_unlock(&pool->lock);
    return 0;
}

int shared_node_pool_get_random(shared_node_pool_t *pool,
                                tree_node_t *out,
                                int count) {
    if (!pool || !out || count <= 0) {
        return 0;
    }

    pthread_mutex_lock(&pool->lock);

    // Can't sample more than we have
    size_t available = pool->count;
    if (available == 0) {
        pthread_mutex_unlock(&pool->lock);
        return 0;
    }

    int to_sample = (count < (int)available) ? count : (int)available;

    // Simple random sampling without replacement
    // For small sample sizes relative to pool, use Fisher-Yates sampling
    if (to_sample <= (int)available / 2) {
        // Use hash set approach for small samples
        int sampled = 0;
        int attempts = 0;
        int max_attempts = to_sample * 10;  // Prevent infinite loop

        while (sampled < to_sample && attempts < max_attempts) {
            size_t idx = rand() % available;

            // Check if already sampled
            int duplicate = 0;
            for (int i = 0; i < sampled; i++) {
                if (memcmp(out[i].node_id, pool->nodes[idx].node_id, 20) == 0) {
                    duplicate = 1;
                    break;
                }
            }

            if (!duplicate) {
                memcpy(&out[sampled], &pool->nodes[idx], sizeof(tree_node_t));
                sampled++;
            }

            attempts++;
        }

        pthread_mutex_unlock(&pool->lock);
        return sampled;
    } else {
        // For large samples, just copy a contiguous block (simpler and faster)
        // Pick a random starting point
        size_t start = rand() % available;
        int sampled = 0;

        for (size_t i = 0; i < available && sampled < to_sample; i++) {
            size_t idx = (start + i) % available;
            memcpy(&out[sampled], &pool->nodes[idx], sizeof(tree_node_t));
            sampled++;
        }

        pthread_mutex_unlock(&pool->lock);
        return sampled;
    }
}

size_t shared_node_pool_get_count(shared_node_pool_t *pool) {
    if (!pool) {
        return 0;
    }

    pthread_mutex_lock(&pool->lock);
    size_t count = pool->count;
    pthread_mutex_unlock(&pool->lock);

    return count;
}

void shared_node_pool_destroy(shared_node_pool_t *pool) {
    if (!pool) {
        return;
    }

    pthread_mutex_lock(&pool->lock);
    size_t final_count = pool->count;
    pthread_mutex_unlock(&pool->lock);

    log_msg(LOG_INFO, "Destroying shared node pool (final count: %zu)", final_count);

    pthread_mutex_destroy(&pool->lock);
    free(pool->nodes);
    free(pool);
}
