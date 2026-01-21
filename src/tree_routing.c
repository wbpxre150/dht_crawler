#include "tree_routing.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#define K_BUCKET_SIZE 100  /* Increased to 100 to allow ~3000 nodes (100 * ~30 active buckets) */
#define MAX_FAIL_COUNT 3
#define MIN_NODE_AGE_FOR_EVICTION 60  /* Don't evict nodes younger than 60 seconds */

/* Calculate XOR distance between two node IDs */
static void xor_distance(const uint8_t *id1, const uint8_t *id2, uint8_t *out) {
    for (int i = 0; i < 20; i++) {
        out[i] = id1[i] ^ id2[i];
    }
}

/* Get bucket index for a node ID (0-159 based on leading zeros of XOR distance) */
static int get_bucket_index(const uint8_t *our_id, const uint8_t *node_id) {
    uint8_t dist[20];
    xor_distance(our_id, node_id, dist);

    for (int i = 0; i < 20; i++) {
        if (dist[i] == 0) continue;
        /* Find leading bit position in this byte */
        for (int bit = 7; bit >= 0; bit--) {
            if (dist[i] & (1 << bit)) {
                return 159 - (i * 8 + (7 - bit));
            }
        }
    }
    return 0;  /* Same ID (shouldn't happen) */
}

/* Compare XOR distances for sorting (reserved for future use) */
#if 0
static int compare_distance(const uint8_t *target, const uint8_t *id1, const uint8_t *id2) {
    uint8_t dist1[20], dist2[20];
    xor_distance(target, id1, dist1);
    xor_distance(target, id2, dist2);
    return memcmp(dist1, dist2, 20);
}
#endif

tree_routing_table_t *tree_routing_create(const uint8_t *our_node_id) {
    if (!our_node_id) {
        return NULL;
    }

    tree_routing_table_t *rt = calloc(1, sizeof(tree_routing_table_t));
    if (!rt) {
        return NULL;
    }

    memcpy(rt->our_node_id, our_node_id, 20);
    pthread_rwlock_init(&rt->rwlock, NULL);

    /* Initialize all buckets */
    for (int i = 0; i < 160; i++) {
        rt->buckets[i].nodes = NULL;
        rt->buckets[i].count = 0;
        rt->buckets[i].max_nodes = K_BUCKET_SIZE;
    }

    rt->total_nodes = 0;
    rt->node_hash = NULL;  /* Initialize uthash for lookups */
    rt->flat_index_head = NULL;  /* Initialize uthash for iteration */
    return rt;
}

void tree_routing_destroy(tree_routing_table_t *rt) {
    if (!rt) {
        return;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    /* Free hash map entries first (lookup index) */
    tree_node_hash_entry_t *hash_entry, *tmp_hash;
    HASH_ITER(hh, rt->node_hash, hash_entry, tmp_hash) {
        HASH_DEL(rt->node_hash, hash_entry);
        free(hash_entry);
    }

    /* Note: flat_index_head points to same nodes as buckets, so don't free twice.
     * Just clear the flat index structure (uthash handles) */
    tree_node_t *node, *tmp_node;
    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp_node) {
        HASH_DELETE(hh_flat, rt->flat_index_head, node);
        /* Don't free(node) here - we'll free from buckets below */
    }

    /* Free all nodes in all buckets (actual memory deallocation) */
    for (int i = 0; i < 160; i++) {
        tree_node_t *bucket_node = rt->buckets[i].nodes;
        while (bucket_node) {
            tree_node_t *next = bucket_node->next;
            free(bucket_node);
            bucket_node = next;
        }
    }

    pthread_rwlock_unlock(&rt->rwlock);
    pthread_rwlock_destroy(&rt->rwlock);
    free(rt);
}

int tree_routing_add_node(tree_routing_table_t *rt, const uint8_t *node_id,
                          const struct sockaddr_storage *addr) {
    if (!rt || !node_id || !addr) {
        return -1;
    }

    /* Don't add ourselves */
    if (memcmp(node_id, rt->our_node_id, 20) == 0) {
        return 0;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    int bucket_idx = get_bucket_index(rt->our_node_id, node_id);
    tree_bucket_t *bucket = &rt->buckets[bucket_idx];

    /* Check if node already exists using hash map (O(1) instead of O(n)) */
    tree_node_hash_entry_t *hash_entry = NULL;
    HASH_FIND(hh, rt->node_hash, node_id, 20, hash_entry);

    if (hash_entry) {
        /* Update existing node */
        tree_node_t *existing = hash_entry->node_ptr;
        memcpy(&existing->addr, addr, sizeof(struct sockaddr_storage));
        existing->last_seen = time(NULL);
        existing->fail_count = 0;
        pthread_rwlock_unlock(&rt->rwlock);
        return 0;
    }

    /* Check if bucket is full */
    if (bucket->count >= bucket->max_nodes) {
        /* Strategy 1: Try to evict a failed node first */
        tree_node_t **prev = &bucket->nodes;
        tree_node_t *curr = bucket->nodes;
        tree_node_t *evicted = NULL;

        while (curr) {
            if (curr->fail_count >= MAX_FAIL_COUNT) {
                *prev = curr->next;
                evicted = curr;

                /* Remove from hash map */
                tree_node_hash_entry_t *evict_hash_entry = NULL;
                HASH_FIND(hh, rt->node_hash, evicted->node_id, 20, evict_hash_entry);
                if (evict_hash_entry) {
                    HASH_DEL(rt->node_hash, evict_hash_entry);
                    free(evict_hash_entry);
                }

                /* Remove from flat index */
                HASH_DELETE(hh_flat, rt->flat_index_head, evicted);

                free(evicted);
                bucket->count--;
                rt->total_nodes--;
                break;
            }
            prev = &curr->next;
            curr = curr->next;
        }

        /* Strategy 2: If no failed nodes, evict least-recently-seen (LRU) node */
        if (bucket->count >= bucket->max_nodes) {
            tree_node_t **lru_prev = NULL;
            tree_node_t **prev_ptr = &bucket->nodes;
            tree_node_t *node = bucket->nodes;
            tree_node_t *lru_node = NULL;
            time_t oldest_time = 0;
            time_t now = time(NULL);

            /* Find LRU node that's old enough to evict (> MIN_NODE_AGE_FOR_EVICTION)
             * Optimization: Early exit after checking first 10 nodes if we found an evictable one
             * This reduces O(n) search cost when buckets are full */
            int nodes_checked = 0;
            int max_nodes_to_check = 10;  /* Don't search entire list if we found a candidate */

            while (node) {
                time_t node_age = now - node->last_seen;
                if (node_age >= MIN_NODE_AGE_FOR_EVICTION) {
                    if (lru_node == NULL || node->last_seen < oldest_time) {
                        oldest_time = node->last_seen;
                        lru_node = node;
                        lru_prev = prev_ptr;
                    }
                    /* Early exit: If we found an evictable node after checking first few nodes */
                    if (++nodes_checked >= max_nodes_to_check && lru_node != NULL) {
                        break;
                    }
                }
                prev_ptr = &node->next;
                node = node->next;
            }

            /* If no evictable node found (all nodes too fresh), don't add new node */
            if (lru_node == NULL) {
                static atomic_int thrash_warning_count = 0;
                int count = atomic_fetch_add(&thrash_warning_count, 1) + 1;
                if (count % 10000 == 0) {
                    log_msg(LOG_DEBUG, "Routing table: bucket %d full, all nodes too fresh to evict (warning #%d)",
                            bucket_idx, count);
                }
                pthread_rwlock_unlock(&rt->rwlock);
                return 0;  /* Not an error, just can't add right now */
            }

            /* Evict LRU node */
            if (lru_prev) {
                *lru_prev = lru_node->next;
            } else {
                bucket->nodes = lru_node->next;
            }

            /* Log LRU eviction periodically (every 1000 evictions) */
            static atomic_int lru_eviction_count = 0;
            int count = atomic_fetch_add(&lru_eviction_count, 1) + 1;
            if (count % 1000 == 0) {
                time_t age = now - oldest_time;
                log_msg(LOG_DEBUG, "Routing table: LRU eviction #%d, bucket %d full, evicted node age %ld sec",
                        count, bucket_idx, (long)age);
            }

            /* Remove from hash map */
            tree_node_hash_entry_t *lru_hash_entry = NULL;
            HASH_FIND(hh, rt->node_hash, lru_node->node_id, 20, lru_hash_entry);
            if (lru_hash_entry) {
                HASH_DEL(rt->node_hash, lru_hash_entry);
                free(lru_hash_entry);
            }

            /* Remove from flat index */
            HASH_DELETE(hh_flat, rt->flat_index_head, lru_node);

            free(lru_node);
            bucket->count--;
            rt->total_nodes--;
        }
    }

    /* Add new node */
    tree_node_t *new_node = calloc(1, sizeof(tree_node_t));
    if (!new_node) {
        pthread_rwlock_unlock(&rt->rwlock);
        return -1;
    }

    memcpy(new_node->node_id, node_id, 20);
    memcpy(&new_node->addr, addr, sizeof(struct sockaddr_storage));
    new_node->last_seen = time(NULL);
    new_node->fail_count = 0;
    new_node->next = bucket->nodes;
    bucket->nodes = new_node;
    bucket->count++;
    rt->total_nodes++;

    /* Add to hash map for O(1) future lookups */
    tree_node_hash_entry_t *new_hash_entry = malloc(sizeof(tree_node_hash_entry_t));
    if (new_hash_entry) {
        memcpy(new_hash_entry->node_id, node_id, 20);
        new_hash_entry->node_ptr = new_node;
        HASH_ADD(hh, rt->node_hash, node_id, 20, new_hash_entry);
    }

    /* Add to flat index for fast iteration (using separate hash handle) */
    HASH_ADD_KEYPTR(hh_flat, rt->flat_index_head, new_node->node_id, 20, new_node);

    pthread_rwlock_unlock(&rt->rwlock);
    return 0;
}

int tree_routing_add_node_if_new(tree_routing_table_t *rt, const uint8_t *node_id,
                                  const struct sockaddr_storage *addr) {
    if (!rt || !node_id || !addr) {
        return -1;
    }

    /* Don't add ourselves */
    if (memcmp(node_id, rt->our_node_id, 20) == 0) {
        return 0;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    int bucket_idx = get_bucket_index(rt->our_node_id, node_id);
    tree_bucket_t *bucket = &rt->buckets[bucket_idx];

    /* Check if node already exists - if so, do NOT update last_seen */
    tree_node_hash_entry_t *hash_entry = NULL;
    HASH_FIND(hh, rt->node_hash, node_id, 20, hash_entry);

    if (hash_entry) {
        /* Node already exists - do nothing (don't refresh last_seen) */
        pthread_rwlock_unlock(&rt->rwlock);
        return 0;
    }

    /* Check if bucket is full */
    if (bucket->count >= bucket->max_nodes) {
        /* Strategy 1: Try to evict a failed node first */
        tree_node_t **prev = &bucket->nodes;
        tree_node_t *curr = bucket->nodes;
        tree_node_t *evicted = NULL;

        while (curr) {
            if (curr->fail_count >= MAX_FAIL_COUNT) {
                *prev = curr->next;
                evicted = curr;

                /* Remove from hash map */
                tree_node_hash_entry_t *evict_hash_entry = NULL;
                HASH_FIND(hh, rt->node_hash, evicted->node_id, 20, evict_hash_entry);
                if (evict_hash_entry) {
                    HASH_DEL(rt->node_hash, evict_hash_entry);
                    free(evict_hash_entry);
                }

                /* Remove from flat index */
                HASH_DELETE(hh_flat, rt->flat_index_head, evicted);

                free(evicted);
                bucket->count--;
                rt->total_nodes--;
                break;
            }
            prev = &curr->next;
            curr = curr->next;
        }

        /* Strategy 2: If no failed nodes, evict least-recently-seen (LRU) node */
        if (bucket->count >= bucket->max_nodes) {
            tree_node_t **lru_prev = NULL;
            tree_node_t **prev_ptr = &bucket->nodes;
            tree_node_t *node = bucket->nodes;
            tree_node_t *lru_node = NULL;
            time_t oldest_time = 0;
            time_t now = time(NULL);

            /* Find LRU node that's old enough to evict */
            int nodes_checked = 0;
            int max_nodes_to_check = 10;

            while (node) {
                time_t node_age = now - node->last_seen;
                if (node_age >= MIN_NODE_AGE_FOR_EVICTION) {
                    if (lru_node == NULL || node->last_seen < oldest_time) {
                        oldest_time = node->last_seen;
                        lru_node = node;
                        lru_prev = prev_ptr;
                    }
                    if (++nodes_checked >= max_nodes_to_check && lru_node != NULL) {
                        break;
                    }
                }
                prev_ptr = &node->next;
                node = node->next;
            }

            /* If no evictable node found, don't add new node */
            if (lru_node == NULL) {
                pthread_rwlock_unlock(&rt->rwlock);
                return 0;
            }

            /* Evict LRU node */
            if (lru_prev) {
                *lru_prev = lru_node->next;
            } else {
                bucket->nodes = lru_node->next;
            }

            /* Remove from hash map */
            tree_node_hash_entry_t *lru_hash_entry = NULL;
            HASH_FIND(hh, rt->node_hash, lru_node->node_id, 20, lru_hash_entry);
            if (lru_hash_entry) {
                HASH_DEL(rt->node_hash, lru_hash_entry);
                free(lru_hash_entry);
            }

            /* Remove from flat index */
            HASH_DELETE(hh_flat, rt->flat_index_head, lru_node);

            free(lru_node);
            bucket->count--;
            rt->total_nodes--;
        }
    }

    /* Add new node */
    tree_node_t *new_node = calloc(1, sizeof(tree_node_t));
    if (!new_node) {
        pthread_rwlock_unlock(&rt->rwlock);
        return -1;
    }

    memcpy(new_node->node_id, node_id, 20);
    memcpy(&new_node->addr, addr, sizeof(struct sockaddr_storage));
    new_node->last_seen = time(NULL);
    new_node->fail_count = 0;
    new_node->next = bucket->nodes;
    bucket->nodes = new_node;
    bucket->count++;
    rt->total_nodes++;

    /* Add to hash map for O(1) future lookups */
    tree_node_hash_entry_t *new_hash_entry = malloc(sizeof(tree_node_hash_entry_t));
    if (new_hash_entry) {
        memcpy(new_hash_entry->node_id, node_id, 20);
        new_hash_entry->node_ptr = new_node;
        HASH_ADD(hh, rt->node_hash, node_id, 20, new_hash_entry);
    }

    /* Add to flat index for fast iteration */
    HASH_ADD_KEYPTR(hh_flat, rt->flat_index_head, new_node->node_id, 20, new_node);

    pthread_rwlock_unlock(&rt->rwlock);
    return 0;
}

int tree_routing_get_closest(tree_routing_table_t *rt, const uint8_t *target,
                              tree_node_t *out, int count) {
    if (!rt || !target || !out || count <= 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&rt->rwlock);

    /* K-smallest heap selection - O(N*log(K)) time, O(K) space
     * Maintain a sorted array of the K closest nodes seen so far.
     * This avoids allocating space for ALL nodes! */

    typedef struct {
        tree_node_t node;
        uint8_t distance[20];
    } node_with_dist_t;

    /* Stack-allocate small fixed buffer for K closest nodes (typically K=8) */
    node_with_dist_t closest[count];
    int num_closest = 0;

    tree_node_t *node, *tmp;
    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp) {
        uint8_t dist[20];
        xor_distance(target, node->node_id, dist);

        if (num_closest < count) {
            /* Fill initial array */
            memcpy(&closest[num_closest].node, node, sizeof(tree_node_t));
            closest[num_closest].node.next = NULL;
            memcpy(closest[num_closest].distance, dist, 20);
            num_closest++;

            /* If filled, sort once */
            if (num_closest == count) {
                /* Insertion sort on initial fill */
                for (int i = 1; i < num_closest; i++) {
                    node_with_dist_t temp = closest[i];
                    int j = i - 1;
                    while (j >= 0 && memcmp(closest[j].distance, temp.distance, 20) > 0) {
                        closest[j + 1] = closest[j];
                        j--;
                    }
                    closest[j + 1] = temp;
                }
            }
        } else {
            /* Check if this node is closer than furthest in our K-set */
            if (memcmp(dist, closest[count - 1].distance, 20) < 0) {
                /* Replace furthest, then insertion sort to maintain order */
                memcpy(&closest[count - 1].node, node, sizeof(tree_node_t));
                closest[count - 1].node.next = NULL;
                memcpy(closest[count - 1].distance, dist, 20);

                /* Bubble down the replaced element */
                int j = count - 2;
                while (j >= 0 && memcmp(closest[j].distance, closest[j + 1].distance, 20) > 0) {
                    node_with_dist_t temp = closest[j];
                    closest[j] = closest[j + 1];
                    closest[j + 1] = temp;
                    j--;
                }
            }
        }
    }

    /* Copy results to output */
    int result_count = num_closest;
    for (int i = 0; i < result_count; i++) {
        memcpy(&out[i], &closest[i].node, sizeof(tree_node_t));
    }

    pthread_rwlock_unlock(&rt->rwlock);
    return result_count;
}

int tree_routing_get_random_nodes(tree_routing_table_t *rt,
                                   tree_node_t *out, int count) {
    if (!rt || !out || count <= 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&rt->rwlock);

    if (rt->total_nodes == 0) {
        pthread_rwlock_unlock(&rt->rwlock);
        return 0;
    }

    /* Reservoir sampling algorithm - O(N) time, O(K) space
     * Instead of copying ALL nodes and shuffling, we iterate once and randomly select K nodes.
     * This avoids malloc of entire table! Much faster for large tables with small K. */

    int result_count = (rt->total_nodes < count) ? rt->total_nodes : count;
    int nodes_seen = 0;

    tree_node_t *node, *tmp;
    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp) {
        nodes_seen++;

        if (nodes_seen <= result_count) {
            /* Fill reservoir: copy first K nodes */
            memcpy(&out[nodes_seen - 1], node, sizeof(tree_node_t));
            out[nodes_seen - 1].next = NULL;
        } else {
            /* Randomly replace elements with decreasing probability */
            int j = rand() % nodes_seen;
            if (j < result_count) {
                memcpy(&out[j], node, sizeof(tree_node_t));
                out[j].next = NULL;
            }
        }
    }

    pthread_rwlock_unlock(&rt->rwlock);
    return result_count;
}

void tree_routing_mark_failed(tree_routing_table_t *rt, const uint8_t *node_id) {
    if (!rt || !node_id) {
        return;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    int bucket_idx = get_bucket_index(rt->our_node_id, node_id);
    tree_bucket_t *bucket = &rt->buckets[bucket_idx];

    /* O(1) hash lookup instead of O(n) linked list search */
    tree_node_hash_entry_t *hash_entry = NULL;
    HASH_FIND(hh, rt->node_hash, node_id, 20, hash_entry);

    if (!hash_entry) {
        pthread_rwlock_unlock(&rt->rwlock);
        return;  /* Node not found */
    }

    tree_node_t *node = hash_entry->node_ptr;
    node->fail_count++;

    if (node->fail_count >= MAX_FAIL_COUNT) {
        /* Evict node from bucket linked list */
        tree_node_t **prev = &bucket->nodes;
        tree_node_t *curr = bucket->nodes;
        while (curr) {
            if (curr == node) {
                *prev = curr->next;
                break;
            }
            prev = &curr->next;
            curr = curr->next;
        }

        /* Remove from hash map */
        HASH_DEL(rt->node_hash, hash_entry);
        free(hash_entry);

        /* Remove from flat index */
        HASH_DELETE(hh_flat, rt->flat_index_head, node);

        free(node);
        bucket->count--;
        rt->total_nodes--;
    }

    pthread_rwlock_unlock(&rt->rwlock);
}

int tree_routing_get_count(tree_routing_table_t *rt) {
    if (!rt) {
        return 0;
    }

    pthread_rwlock_rdlock(&rt->rwlock);
    int count = rt->total_nodes;
    pthread_rwlock_unlock(&rt->rwlock);
    return count;
}

void tree_routing_set_bucket_capacity(tree_routing_table_t *rt, int capacity) {
    if (!rt || capacity <= 0) {
        return;
    }

    pthread_rwlock_wrlock(&rt->rwlock);
    for (int i = 0; i < 160; i++) {
        rt->buckets[i].max_nodes = capacity;
    }
    pthread_rwlock_unlock(&rt->rwlock);
}

int tree_routing_get_random_bep51_nodes(tree_routing_table_t *rt,
                                         tree_node_t *out, int count,
                                         int cooldown_sec) {
    if (!rt || !out || count <= 0) {
        return 0;
    }

    time_t now = time(NULL);
    pthread_rwlock_wrlock(&rt->rwlock);  /* Write lock - we update last_queried */

    /* Count eligible BEP51-capable nodes (not on cooldown) */
    int eligible_count = 0;
    tree_node_t *node, *tmp;
    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp) {
        if (node->bep51_status == BEP51_CAPABLE &&
            (cooldown_sec <= 0 || (now - node->last_queried) >= cooldown_sec)) {
            eligible_count++;
        }
    }

    if (eligible_count == 0) {
        pthread_rwlock_unlock(&rt->rwlock);
        return 0;
    }

    /* Reservoir sampling - O(N) time, O(K) space
     * Only select nodes where bep51_status == BEP51_CAPABLE and not on cooldown */
    int result_count = (eligible_count < count) ? eligible_count : count;
    int eligible_seen = 0;

    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp) {
        if (node->bep51_status != BEP51_CAPABLE) {
            continue;  /* Skip non-capable nodes */
        }
        if (cooldown_sec > 0 && (now - node->last_queried) < cooldown_sec) {
            continue;  /* Skip nodes on cooldown */
        }

        eligible_seen++;

        if (eligible_seen <= result_count) {
            /* Fill reservoir: copy first K eligible nodes */
            memcpy(&out[eligible_seen - 1], node, sizeof(tree_node_t));
            out[eligible_seen - 1].next = NULL;
        } else {
            /* Randomly replace elements with decreasing probability */
            int j = rand() % eligible_seen;
            if (j < result_count) {
                memcpy(&out[j], node, sizeof(tree_node_t));
                out[j].next = NULL;
            }
        }
    }

    /* Mark selected nodes as queried to prevent immediate re-selection */
    for (int i = 0; i < result_count; i++) {
        tree_node_hash_entry_t *hash_entry = NULL;
        HASH_FIND(hh, rt->node_hash, out[i].node_id, 20, hash_entry);
        if (hash_entry) {
            hash_entry->node_ptr->last_queried = now;
        }
    }

    pthread_rwlock_unlock(&rt->rwlock);
    return result_count;
}

void tree_routing_mark_bep51_capable(tree_routing_table_t *rt,
                                      const uint8_t *node_id) {
    if (!rt || !node_id) {
        return;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    /* O(1) hash lookup by node_id */
    tree_node_hash_entry_t *hash_entry = NULL;
    HASH_FIND(hh, rt->node_hash, node_id, 20, hash_entry);

    if (hash_entry) {
        hash_entry->node_ptr->bep51_status = BEP51_CAPABLE;
        hash_entry->node_ptr->last_seen = time(NULL);  /* Node is responsive */
    }

    pthread_rwlock_unlock(&rt->rwlock);
}

void tree_routing_mark_bep51_incapable(tree_routing_table_t *rt,
                                        const uint8_t *node_id) {
    if (!rt || !node_id) {
        return;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    /* O(1) hash lookup by node_id */
    tree_node_hash_entry_t *hash_entry = NULL;
    HASH_FIND(hh, rt->node_hash, node_id, 20, hash_entry);

    if (hash_entry) {
        hash_entry->node_ptr->bep51_status = BEP51_INCAPABLE;
    }

    pthread_rwlock_unlock(&rt->rwlock);
}

int tree_routing_get_bep51_capable_count(tree_routing_table_t *rt) {
    if (!rt) {
        return 0;
    }

    pthread_rwlock_rdlock(&rt->rwlock);

    int count = 0;
    tree_node_t *node, *tmp;
    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp) {
        if (node->bep51_status == BEP51_CAPABLE) {
            count++;
        }
    }

    pthread_rwlock_unlock(&rt->rwlock);
    return count;
}
