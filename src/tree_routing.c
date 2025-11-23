#include "tree_routing.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>

#define K_BUCKET_SIZE 8
#define MAX_FAIL_COUNT 3

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
    return rt;
}

void tree_routing_destroy(tree_routing_table_t *rt) {
    if (!rt) {
        return;
    }

    pthread_rwlock_wrlock(&rt->rwlock);

    /* Free all nodes in all buckets */
    for (int i = 0; i < 160; i++) {
        tree_node_t *node = rt->buckets[i].nodes;
        while (node) {
            tree_node_t *next = node->next;
            free(node);
            node = next;
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

    /* Check if node already exists */
    tree_node_t *existing = bucket->nodes;
    while (existing) {
        if (memcmp(existing->node_id, node_id, 20) == 0) {
            /* Update existing node */
            memcpy(&existing->addr, addr, sizeof(struct sockaddr_storage));
            existing->last_seen = time(NULL);
            existing->fail_count = 0;
            pthread_rwlock_unlock(&rt->rwlock);
            return 0;
        }
        existing = existing->next;
    }

    /* Check if bucket is full */
    if (bucket->count >= bucket->max_nodes) {
        /* Try to evict a failed node */
        tree_node_t **prev = &bucket->nodes;
        tree_node_t *curr = bucket->nodes;
        while (curr) {
            if (curr->fail_count >= MAX_FAIL_COUNT) {
                *prev = curr->next;
                free(curr);
                bucket->count--;
                rt->total_nodes--;
                break;
            }
            prev = &curr->next;
            curr = curr->next;
        }

        /* If still full, don't add */
        if (bucket->count >= bucket->max_nodes) {
            pthread_rwlock_unlock(&rt->rwlock);
            return 0;  /* Not an error, just bucket full */
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

    pthread_rwlock_unlock(&rt->rwlock);
    return 0;
}

int tree_routing_get_closest(tree_routing_table_t *rt, const uint8_t *target,
                              tree_node_t *out, int count) {
    if (!rt || !target || !out || count <= 0) {
        return 0;
    }

    pthread_rwlock_rdlock(&rt->rwlock);

    /* Collect all nodes with their distances */
    typedef struct {
        tree_node_t node;
        uint8_t distance[20];
    } node_with_dist_t;

    node_with_dist_t *candidates = malloc(rt->total_nodes * sizeof(node_with_dist_t));
    if (!candidates) {
        pthread_rwlock_unlock(&rt->rwlock);
        return 0;
    }

    int num_candidates = 0;
    for (int i = 0; i < 160; i++) {
        tree_node_t *node = rt->buckets[i].nodes;
        while (node) {
            memcpy(&candidates[num_candidates].node, node, sizeof(tree_node_t));
            candidates[num_candidates].node.next = NULL;  /* Don't copy linked list pointer */
            xor_distance(target, node->node_id, candidates[num_candidates].distance);
            num_candidates++;
            node = node->next;
        }
    }

    /* Simple insertion sort by distance (good enough for small K) */
    for (int i = 1; i < num_candidates; i++) {
        node_with_dist_t temp = candidates[i];
        int j = i - 1;
        while (j >= 0 && memcmp(candidates[j].distance, temp.distance, 20) > 0) {
            candidates[j + 1] = candidates[j];
            j--;
        }
        candidates[j + 1] = temp;
    }

    /* Copy closest nodes to output */
    int result_count = (num_candidates < count) ? num_candidates : count;
    for (int i = 0; i < result_count; i++) {
        memcpy(&out[i], &candidates[i].node, sizeof(tree_node_t));
    }

    free(candidates);
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

    /* Collect all nodes into array */
    tree_node_t *all_nodes = malloc(rt->total_nodes * sizeof(tree_node_t));
    if (!all_nodes) {
        pthread_rwlock_unlock(&rt->rwlock);
        return 0;
    }

    int idx = 0;
    for (int i = 0; i < 160; i++) {
        tree_node_t *node = rt->buckets[i].nodes;
        while (node) {
            memcpy(&all_nodes[idx], node, sizeof(tree_node_t));
            all_nodes[idx].next = NULL;
            idx++;
            node = node->next;
        }
    }

    /* Fisher-Yates shuffle and take first 'count' */
    int result_count = (rt->total_nodes < count) ? rt->total_nodes : count;
    for (int i = 0; i < result_count; i++) {
        int j = i + (rand() % (idx - i));
        tree_node_t temp = all_nodes[i];
        all_nodes[i] = all_nodes[j];
        all_nodes[j] = temp;
        memcpy(&out[i], &all_nodes[i], sizeof(tree_node_t));
    }

    free(all_nodes);
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

    tree_node_t **prev = &bucket->nodes;
    tree_node_t *curr = bucket->nodes;
    while (curr) {
        if (memcmp(curr->node_id, node_id, 20) == 0) {
            curr->fail_count++;
            if (curr->fail_count >= MAX_FAIL_COUNT) {
                /* Evict node */
                *prev = curr->next;
                free(curr);
                bucket->count--;
                rt->total_nodes--;
            }
            pthread_rwlock_unlock(&rt->rwlock);
            return;
        }
        prev = &curr->next;
        curr = curr->next;
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
