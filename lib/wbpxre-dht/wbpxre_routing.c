/*
 * wbpxre-dht: Routing Table Implementation
 * Uses AVL tree for O(log n) operations
 * Tracks BEP 51 node metadata
 */

#include "wbpxre_dht.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <urcu.h>
#include <urcu/rculist.h>

/* ============================================================================
 * AVL Tree Operations
 * ============================================================================ */

static int max(int a, int b) {
    return (a > b) ? a : b;
}

static int height(wbpxre_routing_node_t *node) {
    return node ? node->height : 0;
}

static int balance_factor(wbpxre_routing_node_t *node) {
    return node ? height(node->left) - height(node->right) : 0;
}

static void update_height(wbpxre_routing_node_t *node) {
    if (node) {
        node->height = 1 + max(height(node->left), height(node->right));
    }
}

static wbpxre_routing_node_t *rotate_right(wbpxre_routing_node_t *y) {
    wbpxre_routing_node_t *x = y->left;
    wbpxre_routing_node_t *T2 = x->right;

    x->right = y;
    y->left = T2;

    update_height(y);
    update_height(x);

    return x;
}

static wbpxre_routing_node_t *rotate_left(wbpxre_routing_node_t *x) {
    wbpxre_routing_node_t *y = x->right;
    wbpxre_routing_node_t *T2 = y->left;

    y->left = x;
    x->right = T2;

    update_height(x);
    update_height(y);

    return y;
}

static wbpxre_routing_node_t *balance_tree(wbpxre_routing_node_t *node) {
    update_height(node);
    int balance = balance_factor(node);

    /* Left heavy */
    if (balance > 1) {
        if (balance_factor(node->left) < 0) {
            node->left = rotate_left(node->left);
        }
        return rotate_right(node);
    }

    /* Right heavy */
    if (balance < -1) {
        if (balance_factor(node->right) > 0) {
            node->right = rotate_right(node->right);
        }
        return rotate_left(node);
    }

    return node;
}

/* ============================================================================
 * Node Comparison (XOR distance)
 * ============================================================================ */

static int compare_node_ids(const uint8_t *id1, const uint8_t *id2) {
    return memcmp(id1, id2, WBPXRE_NODE_ID_LEN);
}

/* Forward declaration */
static wbpxre_routing_node_t *find_node_recursive(wbpxre_routing_node_t *root,
                                                   const uint8_t *node_id);

/* ============================================================================
 * Node Removal from AVL Tree
 * ============================================================================ */

static wbpxre_routing_node_t *find_min_node(wbpxre_routing_node_t *node) {
    while (node->left != NULL) {
        node = node->left;
    }
    return node;
}

static wbpxre_routing_node_t *remove_node_recursive(wbpxre_routing_node_t *root,
                                                      const uint8_t *node_id,
                                                      bool *removed) {
    if (!root) return NULL;

    int cmp = compare_node_ids(node_id, root->id);

    if (cmp < 0) {
        root->left = remove_node_recursive(root->left, node_id, removed);
    } else if (cmp > 0) {
        root->right = remove_node_recursive(root->right, node_id, removed);
    } else {
        /* Found the node to remove */
        *removed = true;

        /* Node with only one child or no child */
        if (root->left == NULL) {
            wbpxre_routing_node_t *temp = root->right;
            free(root);
            return temp;
        } else if (root->right == NULL) {
            wbpxre_routing_node_t *temp = root->left;
            free(root);
            return temp;
        }

        /* Node with two children: Get inorder successor (smallest in right subtree) */
        wbpxre_routing_node_t *temp = find_min_node(root->right);

        /* Copy successor's content to this node */
        memcpy(root->id, temp->id, WBPXRE_NODE_ID_LEN);
        memcpy(&root->addr, &temp->addr, sizeof(wbpxre_node_addr_t));
        root->last_responded_at = temp->last_responded_at;
        root->discovered_at = temp->discovered_at;
        root->dropped = temp->dropped;
        root->bep51_support = temp->bep51_support;
        root->sampled_num = temp->sampled_num;
        root->last_discovered_num = temp->last_discovered_num;
        root->total_num = temp->total_num;
        root->next_sample_time = temp->next_sample_time;
        root->queries_sent = temp->queries_sent;
        root->responses_received = temp->responses_received;

        /* Delete the inorder successor */
        root->right = remove_node_recursive(root->right, temp->id, removed);
    }

    if (!root) return NULL;

    /* Balance the tree */
    return balance_tree(root);
}

/* ============================================================================
 * LRU Node Finding (Phase 4: Quality-Based Eviction)
 * ============================================================================ */

static void find_lru_node_recursive(wbpxre_routing_node_t *root,
                                     wbpxre_routing_node_t **lru_node,
                                     time_t *oldest_time,
                                     double *worst_quality) {
    if (!root) return;

    /* Skip dropped nodes */
    if (!root->dropped) {
        bool should_evict = false;

        /* Priority 1: Low response rate (after at least 5 queries) */
        if (root->queries_sent >= 5) {
            double response_rate = (double)root->responses_received / root->queries_sent;
            if (response_rate < 0.20) {
                should_evict = true;
                if (response_rate < *worst_quality) {
                    *worst_quality = response_rate;
                    *oldest_time = root->last_responded_at;
                    *lru_node = root;
                }
            }
        }

        /* Priority 2: Old nodes (haven't responded in 5+ minutes) */
        if (!should_evict && *lru_node == NULL) {
            time_t now = time(NULL);
            if (root->last_responded_at > 0 && (now - root->last_responded_at) > 300) {
                should_evict = true;
                if (root->last_responded_at < *oldest_time) {
                    *oldest_time = root->last_responded_at;
                    *lru_node = root;
                }
            }
        }

        /* Priority 3: Oldest by last_responded_at (fallback) */
        if (!should_evict && *lru_node == NULL) {
            if (root->last_responded_at < *oldest_time) {
                *oldest_time = root->last_responded_at;
                *lru_node = root;
            }
        }
    }

    /* Recurse through tree */
    find_lru_node_recursive(root->left, lru_node, oldest_time, worst_quality);
    find_lru_node_recursive(root->right, lru_node, oldest_time, worst_quality);
}

static wbpxre_routing_node_t *find_lru_node(wbpxre_routing_table_t *table) {
    wbpxre_routing_node_t *lru_node = NULL;
    time_t oldest_time = time(NULL) + 1; /* Start with future time */
    double worst_quality = 1.0; /* Start with perfect quality */

    find_lru_node_recursive(table->root, &lru_node, &oldest_time, &worst_quality);

    return lru_node;
}

/* ============================================================================
 * Routing Table Operations
 * ============================================================================ */

wbpxre_routing_table_t *wbpxre_routing_table_create(int max_nodes) {
    wbpxre_routing_table_t *table = calloc(1, sizeof(wbpxre_routing_table_t));
    if (!table) return NULL;

    table->root = NULL;
    table->node_count = 0;
    table->max_nodes = max_nodes;

    /* Initialize update lock (only for writers - RCU allows lock-free reads) */
    pthread_mutex_init(&table->update_lock, NULL);

    /* Allocate flat array for uniform iteration */
    table->all_nodes_capacity = max_nodes > 0 ? max_nodes : 10000;
    table->all_nodes = calloc(table->all_nodes_capacity, sizeof(wbpxre_routing_node_t *));
    table->iteration_offset = 0;

    if (!table->all_nodes) {
        pthread_mutex_destroy(&table->update_lock);
        free(table);
        return NULL;
    }

    return table;
}

static void free_routing_tree(wbpxre_routing_node_t *node) {
    if (!node) return;
    free_routing_tree(node->left);
    free_routing_tree(node->right);
    free(node);
}

void wbpxre_routing_table_destroy(wbpxre_routing_table_t *table) {
    if (!table) return;

    /* Wait for all readers to finish (CRITICAL for RCU safety) */
    synchronize_rcu();

    /* Now safe to free all nodes */
    free_routing_tree(table->root);

    /* Free flat array */
    if (table->all_nodes) {
        free(table->all_nodes);
        table->all_nodes = NULL;
    }

    pthread_mutex_destroy(&table->update_lock);
    free(table);
}

static wbpxre_routing_node_t *insert_node_recursive(wbpxre_routing_node_t *root,
                                                     const wbpxre_routing_node_t *node,
                                                     bool *inserted) {
    /* Base case: create new node */
    if (!root) {
        wbpxre_routing_node_t *new_node = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(new_node, node, sizeof(wbpxre_routing_node_t));
        new_node->left = NULL;
        new_node->right = NULL;
        new_node->height = 1;
        *inserted = true;
        return new_node;
    }

    /* Compare node IDs */
    int cmp = compare_node_ids(node->id, root->id);

    if (cmp == 0) {
        /* Node already exists, update it */
        memcpy(&root->addr, &node->addr, sizeof(wbpxre_node_addr_t));
        root->last_responded_at = node->last_responded_at;
        *inserted = false;
        return root;
    } else if (cmp < 0) {
        root->left = insert_node_recursive(root->left, node, inserted);
    } else {
        root->right = insert_node_recursive(root->right, node, inserted);
    }

    /* Balance the tree */
    return balance_tree(root);
}

/* Helper: Add node pointer to flat array (must be called with write lock held) */
static void add_node_to_flat_array(wbpxre_routing_table_t *table, wbpxre_routing_node_t *node) {
    if (!table || !node || !table->all_nodes) return;

    /* Find first empty slot or append to end */
    for (int i = 0; i < table->all_nodes_capacity; i++) {
        if (table->all_nodes[i] == NULL) {
            table->all_nodes[i] = node;
            return;
        }
    }
}

/* Helper: Remove node pointer from flat array (must be called with write lock held) */
static void remove_node_from_flat_array(wbpxre_routing_table_t *table, const uint8_t *node_id) {
    if (!table || !node_id || !table->all_nodes) return;

    for (int i = 0; i < table->all_nodes_capacity; i++) {
        if (table->all_nodes[i] && memcmp(table->all_nodes[i]->id, node_id, WBPXRE_NODE_ID_LEN) == 0) {
            table->all_nodes[i] = NULL;
            return;
        }
    }
}

int wbpxre_routing_table_insert(wbpxre_routing_table_t *table,
                                 const wbpxre_routing_node_t *node) {
    if (!table || !node) return -1;

    pthread_mutex_lock(&table->update_lock);

    /* Check if node already exists (in which case we update, not insert) */
    wbpxre_routing_node_t *existing = find_node_recursive(table->root, node->id);

    if (!existing && table->max_nodes > 0 && table->node_count >= table->max_nodes) {
        /* At capacity and this is a new node - implement LRU eviction */
        wbpxre_routing_node_t *lru = find_lru_node(table);

        if (lru) {
            /* Save LRU node ID before removal */
            uint8_t lru_id[WBPXRE_NODE_ID_LEN];
            memcpy(lru_id, lru->id, WBPXRE_NODE_ID_LEN);

            /* Remove from flat array first */
            remove_node_from_flat_array(table, lru_id);

            /* Remove LRU node from tree */
            bool removed = false;
            table->root = remove_node_recursive(table->root, lru_id, &removed);

            if (removed) {
                table->node_count--;
            }
        }
    }

    bool inserted = false;
    table->root = insert_node_recursive(table->root, node, &inserted);

    if (inserted) {
        table->node_count++;

        /* Add to flat array for uniform iteration */
        wbpxre_routing_node_t *new_node = find_node_recursive(table->root, node->id);
        if (new_node) {
            add_node_to_flat_array(table, new_node);
        }
    }

    pthread_mutex_unlock(&table->update_lock);
    return 0;
}

static wbpxre_routing_node_t *find_node_recursive(wbpxre_routing_node_t *root,
                                                   const uint8_t *node_id) {
    if (!root) return NULL;

    int cmp = compare_node_ids(node_id, root->id);

    if (cmp == 0) {
        return root;
    } else if (cmp < 0) {
        return find_node_recursive(root->left, node_id);
    } else {
        return find_node_recursive(root->right, node_id);
    }
}

wbpxre_routing_node_t *wbpxre_routing_table_find(wbpxre_routing_table_t *table,
                                                  const uint8_t *node_id) {
    if (!table || !node_id) return NULL;

    rcu_read_lock();
    wbpxre_routing_node_t *node = find_node_recursive(table->root, node_id);
    rcu_read_unlock();

    return node;
}

/* ============================================================================
 * K-Closest Nodes Query
 * ============================================================================ */

typedef struct {
    wbpxre_routing_node_t *node;
    uint8_t distance[WBPXRE_NODE_ID_LEN];
} node_distance_t;

static int compare_node_distances(const void *a, const void *b) {
    const node_distance_t *nd1 = (const node_distance_t *)a;
    const node_distance_t *nd2 = (const node_distance_t *)b;
    return memcmp(nd1->distance, nd2->distance, WBPXRE_NODE_ID_LEN);
}

static void collect_all_nodes_recursive(wbpxre_routing_node_t *root,
                                        node_distance_t **array,
                                        int *count, int *capacity,
                                        const uint8_t *target) {
    if (!root) return;

    /* Expand array if needed */
    if (*count >= *capacity) {
        *capacity *= 2;
        *array = realloc(*array, sizeof(node_distance_t) * (*capacity));
    }

    /* Skip dropped nodes */
    if (root->dropped) {
        collect_all_nodes_recursive(root->left, array, count, capacity, target);
        collect_all_nodes_recursive(root->right, array, count, capacity, target);
        return;
    }

    /* Add this node */
    (*array)[*count].node = root;
    wbpxre_xor_distance(target, root->id, (*array)[*count].distance);
    (*count)++;

    /* Recurse */
    collect_all_nodes_recursive(root->left, array, count, capacity, target);
    collect_all_nodes_recursive(root->right, array, count, capacity, target);
}

int wbpxre_routing_table_get_closest(wbpxre_routing_table_t *table,
                                      const uint8_t *target,
                                      wbpxre_routing_node_t **nodes_out, int k) {
    if (!table || !target || !nodes_out || k <= 0) return 0;

    rcu_read_lock();

    /* Collect all nodes with distances */
    int capacity = 1000;
    int count = 0;
    node_distance_t *all_nodes = malloc(sizeof(node_distance_t) * capacity);

    collect_all_nodes_recursive(table->root, &all_nodes, &count, &capacity, target);

    /* Sort by distance */
    qsort(all_nodes, count, sizeof(node_distance_t), compare_node_distances);

    /* Allocate and copy top K nodes (to avoid data races after lock release) */
    int result_count = count < k ? count : k;
    for (int i = 0; i < result_count; i++) {
        /* Allocate a copy of the node to return to caller */
        wbpxre_routing_node_t *node_copy = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(node_copy, all_nodes[i].node, sizeof(wbpxre_routing_node_t));
        /* Clear tree pointers since this is a standalone copy */
        node_copy->left = NULL;
        node_copy->right = NULL;
        nodes_out[i] = node_copy;
    }

    free(all_nodes);
    rcu_read_unlock();

    return result_count;
}

/* ============================================================================
 * BEP 51 Sample Candidates
 * ============================================================================ */

/* Forward declaration of wbpxre_xor_distance (defined in wbpxre_dht.c) */
extern void wbpxre_xor_distance(const uint8_t *a, const uint8_t *b, uint8_t *result);

static bool is_sample_infohashes_candidate(wbpxre_routing_node_t *node,
                                            const uint8_t *current_node_id) {
    time_t now = time(NULL);

    /* Must not be dropped */
    if (node->dropped) return false;

    /* Must not explicitly not support BEP 51 */
    if (node->bep51_support == WBPXRE_PROTOCOL_NO) return false;

    /* Must respect interval (next_sample_time must have passed) */
    if (node->next_sample_time > now) return false;

    /* FIX #1: Keyspace distance filter
     * Calculate XOR distance between our node_id and this node
     * If first byte distance > 128 (opposite half of keyspace), apply stricter rules */
    uint8_t distance[WBPXRE_NODE_ID_LEN];
    wbpxre_xor_distance(current_node_id, node->id, distance);

    bool is_distant = distance[0] > 128;  /* First byte differs significantly */

    if (is_distant) {
        /* Distant nodes: stricter eligibility (60 seconds instead of 300) */
        if (node->last_responded_at > 0 && (now - node->last_responded_at) > 60) {
            return false;
        }
    } else {
        /* Close nodes: normal eligibility (300 seconds) */
        if (node->last_responded_at > 0 && (now - node->last_responded_at) > 300) {
            return false;
        }
    }

    return true;
}

/* OPTIMIZED: Use flat array iteration instead of BST traversal
 * This eliminates BST structure bias and provides uniform node selection
 * Critical for hot rotation where XOR distance changes but BST doesn't */
int wbpxre_routing_table_get_sample_candidates(wbpxre_routing_table_t *table,
                                                const uint8_t *current_node_id,
                                                wbpxre_routing_node_t **nodes_out,
                                                int n) {
    if (!table || !current_node_id || !nodes_out || n <= 0) return 0;

    rcu_read_lock();

    int count = 0;

    /* Rotate starting position for fairness across calls */
    int start_offset = table->iteration_offset;

    /* Iterate flat array starting from rotating offset */
    for (int i = 0; i < table->all_nodes_capacity && count < n; i++) {
        int idx = (start_offset + i) % table->all_nodes_capacity;
        wbpxre_routing_node_t *node = table->all_nodes[idx];

        if (!node) continue;  /* Empty slot */

        if (is_sample_infohashes_candidate(node, current_node_id)) {
            /* Copy node to avoid data races after lock release */
            wbpxre_routing_node_t *node_copy = malloc(sizeof(wbpxre_routing_node_t));
            memcpy(node_copy, node, sizeof(wbpxre_routing_node_t));
            /* Clear tree pointers since this is a standalone copy */
            node_copy->left = NULL;
            node_copy->right = NULL;
            nodes_out[count++] = node_copy;
        }
    }

    rcu_read_unlock();

    /* Update rotation offset for next call (rotate by 7 for better distribution) */
    pthread_mutex_lock(&table->update_lock);
    table->iteration_offset = (table->iteration_offset + 7) % table->all_nodes_capacity;
    pthread_mutex_unlock(&table->update_lock);

    return count;
}

/* ============================================================================
 * Get Low-Quality Nodes for Eviction
 * ============================================================================ */

static void collect_low_quality_nodes_recursive(wbpxre_routing_node_t *root,
                                                  wbpxre_routing_node_t **array,
                                                  int *count, int n,
                                                  double min_rate, int min_queries) {
    if (!root || *count >= n) return;

    /* Skip dropped nodes */
    if (root->dropped) {
        collect_low_quality_nodes_recursive(root->left, array, count, n, min_rate, min_queries);
        collect_low_quality_nodes_recursive(root->right, array, count, n, min_rate, min_queries);
        return;
    }

    /* Check if node has poor response rate */
    if (root->queries_sent >= min_queries) {
        double response_rate = (double)root->responses_received / (double)root->queries_sent;
        if (response_rate < min_rate) {
            array[(*count)++] = root;
        }
    }

    /* Recursively collect from subtrees */
    if (*count < n) {
        collect_low_quality_nodes_recursive(root->left, array, count, n, min_rate, min_queries);
    }
    if (*count < n) {
        collect_low_quality_nodes_recursive(root->right, array, count, n, min_rate, min_queries);
    }
}

int wbpxre_routing_table_get_low_quality_nodes(wbpxre_routing_table_t *table,
                                                 wbpxre_routing_node_t **nodes_out,
                                                 int n, double min_rate, int min_queries) {
    if (!table || !nodes_out || n <= 0) return 0;

    rcu_read_lock();

    /* Collect low-quality node pointers */
    wbpxre_routing_node_t *candidates[n];
    int count = 0;
    collect_low_quality_nodes_recursive(table->root, candidates, &count, n, min_rate, min_queries);

    /* Copy nodes to avoid data races after lock release */
    for (int i = 0; i < count; i++) {
        wbpxre_routing_node_t *node_copy = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(node_copy, candidates[i], sizeof(wbpxre_routing_node_t));
        /* Clear tree pointers since this is a standalone copy */
        node_copy->left = NULL;
        node_copy->right = NULL;
        nodes_out[i] = node_copy;
    }

    rcu_read_unlock();

    return count;
}

/* ============================================================================
 * Get Oldest Nodes (for capacity-based eviction)
 * ============================================================================ */

/* Helper to collect all nodes into array for age sorting */
static void collect_nodes_for_aging_recursive(wbpxre_routing_node_t *root,
                                                wbpxre_routing_node_t **array,
                                                int *count, int max) {
    if (!root || *count >= max) return;

    /* Skip dropped nodes */
    if (root->dropped) {
        collect_nodes_for_aging_recursive(root->left, array, count, max);
        collect_nodes_for_aging_recursive(root->right, array, count, max);
        return;
    }

    /* Add this node */
    array[(*count)++] = root;

    /* Recurse on children */
    collect_nodes_for_aging_recursive(root->left, array, count, max);
    collect_nodes_for_aging_recursive(root->right, array, count, max);
}

/* Comparison function for qsort - oldest first */
static int compare_nodes_by_age(const void *a, const void *b) {
    wbpxre_routing_node_t *node_a = *(wbpxre_routing_node_t **)a;
    wbpxre_routing_node_t *node_b = *(wbpxre_routing_node_t **)b;

    /* Nodes that never responded go first */
    if (node_a->last_responded_at == 0 && node_b->last_responded_at != 0) return -1;
    if (node_a->last_responded_at != 0 && node_b->last_responded_at == 0) return 1;
    if (node_a->last_responded_at == 0 && node_b->last_responded_at == 0) return 0;

    /* Otherwise sort by timestamp (oldest first) */
    if (node_a->last_responded_at < node_b->last_responded_at) return -1;
    if (node_a->last_responded_at > node_b->last_responded_at) return 1;
    return 0;
}

int wbpxre_routing_table_get_oldest_nodes(wbpxre_routing_table_t *table,
                                            wbpxre_routing_node_t **nodes_out,
                                            int n) {
    if (!table || !nodes_out || n <= 0) return 0;

    rcu_read_lock();

    /* Collect all nodes (scale limit with table size, cap at 50000) */
    int max_collect = table->node_count < 50000 ? table->node_count : 50000;
    wbpxre_routing_node_t **all_nodes = malloc(sizeof(wbpxre_routing_node_t *) * max_collect);
    if (!all_nodes) {
        rcu_read_unlock();
        return 0;
    }

    int count = 0;
    collect_nodes_for_aging_recursive(table->root, all_nodes, &count, max_collect);

    /* Sort by age (oldest first) */
    qsort(all_nodes, count, sizeof(wbpxre_routing_node_t *), compare_nodes_by_age);

    /* Copy the oldest N nodes */
    int to_return = count < n ? count : n;
    for (int i = 0; i < to_return; i++) {
        wbpxre_routing_node_t *node_copy = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(node_copy, all_nodes[i], sizeof(wbpxre_routing_node_t));
        /* Clear tree pointers since this is a standalone copy */
        node_copy->left = NULL;
        node_copy->right = NULL;
        nodes_out[i] = node_copy;
    }

    free(all_nodes);
    rcu_read_unlock();

    return to_return;
}

/* ============================================================================
 * Get Distant Nodes (Keyspace-Aware Eviction)
 * Returns nodes far from current node_id for eviction during rotation
 * ============================================================================ */

/* Composite scoring for keyspace-aware eviction:
 * - distance_score (60%): XOR distance from current node_id
 * - age_score (20%): Time since last response
 * - quality_score (20%): Response rate
 */
typedef struct {
    wbpxre_routing_node_t *node;
    uint8_t distance[WBPXRE_NODE_ID_LEN];
    double composite_score;
} node_distance_score_t;

static int compare_by_composite_score(const void *a, const void *b) {
    const node_distance_score_t *nd1 = (const node_distance_score_t *)a;
    const node_distance_score_t *nd2 = (const node_distance_score_t *)b;

    /* Higher score should be evicted first (reverse sort) */
    if (nd1->composite_score > nd2->composite_score) return -1;
    if (nd1->composite_score < nd2->composite_score) return 1;
    return 0;
}

/* Calculate eviction priority score (higher = more likely to evict) */
static double calculate_eviction_score(wbpxre_routing_node_t *node,
                                        const uint8_t *current_node_id) {
    time_t now = time(NULL);

    /* Distance score (0.0 = close, 1.0 = far)
     * Check if first byte differs significantly (>128 distance in first byte) */
    uint8_t distance[WBPXRE_NODE_ID_LEN];
    wbpxre_xor_distance(current_node_id, node->id, distance);

    /* Use first 8 bytes for distance metric (good enough approximation) */
    uint64_t dist_metric = 0;
    for (int i = 0; i < 8; i++) {
        dist_metric = (dist_metric << 8) | distance[i];
    }
    double distance_score = (double)dist_metric / (double)UINT64_MAX;

    /* Age score (0.0 = fresh, 1.0 = old) */
    double age_score = 0.0;
    if (node->last_responded_at > 0) {
        time_t age = now - node->last_responded_at;
        age_score = age > 300 ? 1.0 : (double)age / 300.0;  /* 5 minutes = max */
    } else {
        age_score = 1.0;  /* Never responded = old */
    }

    /* Quality score (0.0 = good, 1.0 = bad) */
    double quality_score = 0.0;
    if (node->queries_sent > 0) {
        double response_rate = (double)node->responses_received / (double)node->queries_sent;
        quality_score = 1.0 - response_rate;  /* Invert: low response = high score */
    } else {
        quality_score = 0.5;  /* Unknown quality = medium */
    }

    /* Weighted composite: distance (60%) + age (20%) + quality (20%) */
    return (distance_score * 0.60) + (age_score * 0.20) + (quality_score * 0.20);
}

int wbpxre_routing_table_get_distant_nodes(wbpxre_routing_table_t *table,
                                             const uint8_t *current_node_id,
                                             wbpxre_routing_node_t **nodes_out,
                                             int n) {
    if (!table || !current_node_id || !nodes_out || n <= 0) return 0;

    rcu_read_lock();

    /* Collect all nodes (scale limit with table size, cap at 50000) */
    int max_collect = table->node_count < 50000 ? table->node_count : 50000;
    node_distance_score_t *scored_nodes = malloc(sizeof(node_distance_score_t) * max_collect);
    if (!scored_nodes) {
        rcu_read_unlock();
        return 0;
    }

    /* Collect and score all nodes */
    wbpxre_routing_node_t **temp_nodes = malloc(sizeof(wbpxre_routing_node_t *) * max_collect);
    if (!temp_nodes) {
        free(scored_nodes);
        rcu_read_unlock();
        return 0;
    }

    int count = 0;
    collect_nodes_for_aging_recursive(table->root, temp_nodes, &count, max_collect);

    /* Calculate composite score for each node */
    for (int i = 0; i < count; i++) {
        scored_nodes[i].node = temp_nodes[i];
        wbpxre_xor_distance(current_node_id, temp_nodes[i]->id, scored_nodes[i].distance);
        scored_nodes[i].composite_score = calculate_eviction_score(temp_nodes[i], current_node_id);
    }

    /* Sort by composite score (highest first = most evictable) */
    qsort(scored_nodes, count, sizeof(node_distance_score_t), compare_by_composite_score);

    /* Copy the most evictable N nodes */
    int to_return = count < n ? count : n;
    for (int i = 0; i < to_return; i++) {
        wbpxre_routing_node_t *node_copy = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(node_copy, scored_nodes[i].node, sizeof(wbpxre_routing_node_t));
        /* Clear tree pointers since this is a standalone copy */
        node_copy->left = NULL;
        node_copy->right = NULL;
        nodes_out[i] = node_copy;
    }

    free(temp_nodes);
    free(scored_nodes);
    rcu_read_unlock();

    return to_return;
}

/* Get keyspace distribution statistics (close vs distant nodes)
 * Used for monitoring keyspace composition after rotation */
void wbpxre_routing_table_get_keyspace_distribution(wbpxre_routing_table_t *table,
                                                      const uint8_t *current_node_id,
                                                      int *close_nodes,
                                                      int *distant_nodes) {
    if (!table || !current_node_id || !close_nodes || !distant_nodes) return;

    *close_nodes = 0;
    *distant_nodes = 0;

    rcu_read_lock();

    /* Iterate all nodes and classify by distance */
    for (int i = 0; i < table->all_nodes_capacity; i++) {
        wbpxre_routing_node_t *node = table->all_nodes[i];
        if (!node || node->dropped) continue;

        /* Calculate XOR distance */
        uint8_t distance[WBPXRE_NODE_ID_LEN];
        wbpxre_xor_distance(current_node_id, node->id, distance);

        /* Classify by first byte distance */
        if (distance[0] > 128) {
            (*distant_nodes)++;
        } else {
            (*close_nodes)++;
        }
    }

    rcu_read_unlock();
}

/* ============================================================================
 * Node Updates
 * ============================================================================ */

void wbpxre_routing_table_update_node_responded(wbpxre_routing_table_t *table,
                                                 const uint8_t *node_id) {
    if (!table || !node_id) return;

    pthread_mutex_lock(&table->update_lock);

    wbpxre_routing_node_t *node = find_node_recursive(table->root, node_id);
    if (node) {
        node->last_responded_at = time(NULL);
        node->responses_received++;  /* Phase 4: Track response */
    }

    pthread_mutex_unlock(&table->update_lock);
}

/* Phase 4: Track query sent to node */
void wbpxre_routing_table_update_node_queried(wbpxre_routing_table_t *table,
                                               const uint8_t *node_id) {
    if (!table || !node_id) return;

    pthread_mutex_lock(&table->update_lock);

    wbpxre_routing_node_t *node = find_node_recursive(table->root, node_id);
    if (node) {
        node->queries_sent++;
    }

    pthread_mutex_unlock(&table->update_lock);
}

void wbpxre_routing_table_update_sample_response(wbpxre_routing_table_t *table,
                                                  const uint8_t *node_id,
                                                  int discovered_num,
                                                  int total_num,
                                                  int interval) {
    if (!table || !node_id) return;

    pthread_mutex_lock(&table->update_lock);

    wbpxre_routing_node_t *node = find_node_recursive(table->root, node_id);
    if (node) {
        time_t now = time(NULL);

        node->last_responded_at = now;
        node->bep51_support = WBPXRE_PROTOCOL_YES;
        node->sampled_num += discovered_num;
        node->last_discovered_num = discovered_num;
        node->total_num = total_num;

        /* If no new hashes, add 5 minute penalty */
        if (discovered_num == 0) {
            node->next_sample_time = now + interval + WBPXRE_DEFAULT_EMPTY_SAMPLE_PENALTY;
        } else {
            /* If getting results, use 60s instead of long intervals */
            if (interval > 300 && discovered_num > 0) {
                interval = WBPXRE_DEFAULT_SAMPLE_INTERVAL;
            }
            node->next_sample_time = now + interval;
        }
    }

    pthread_mutex_unlock(&table->update_lock);
}

void wbpxre_routing_table_drop_node(wbpxre_routing_table_t *table,
                                     const uint8_t *node_id) {
    if (!table || !node_id) return;

    pthread_mutex_lock(&table->update_lock);

    /* Mark as dropped first, then remove */
    wbpxre_routing_node_t *node = find_node_recursive(table->root, node_id);
    if (node) {
        node->dropped = true;

        /* Remove from flat array */
        remove_node_from_flat_array(table, node_id);

        /* Actually remove the node from the tree */
        bool removed = false;
        table->root = remove_node_recursive(table->root, node_id, &removed);

        if (removed) {
            table->node_count--;
        }
    }

    pthread_mutex_unlock(&table->update_lock);
}

/* Drop multiple nodes in a single operation (batch API)
 * More efficient than calling drop_node multiple times
 * Returns number of nodes actually dropped */
int wbpxre_routing_table_drop_nodes_batch(wbpxre_routing_table_t *table,
                                           const uint8_t node_ids[][WBPXRE_NODE_ID_LEN],
                                           int count) {
    if (!table || !node_ids || count <= 0) return 0;

    pthread_mutex_lock(&table->update_lock);

    int dropped = 0;

    for (int i = 0; i < count; i++) {
        /* Find and mark as dropped */
        wbpxre_routing_node_t *node = find_node_recursive(table->root, node_ids[i]);
        if (!node) continue;

        node->dropped = true;

        /* Remove from flat array */
        remove_node_from_flat_array(table, node_ids[i]);

        /* Remove from tree */
        bool removed = false;
        table->root = remove_node_recursive(table->root, node_ids[i], &removed);

        if (removed) {
            table->node_count--;
            dropped++;
        }
    }

    pthread_mutex_unlock(&table->update_lock);

    return dropped;
}

/* ============================================================================
 * Batch Cleanup of Dropped Nodes (Phase 3)
 * ============================================================================ */

static void collect_dropped_nodes_recursive(wbpxre_routing_node_t *root,
                                             uint8_t dropped_ids[][WBPXRE_NODE_ID_LEN],
                                             int *count, int max_count) {
    if (!root || *count >= max_count) return;

    /* Check if this node is dropped */
    if (root->dropped) {
        memcpy(dropped_ids[*count], root->id, WBPXRE_NODE_ID_LEN);
        (*count)++;
    }

    /* Recurse */
    collect_dropped_nodes_recursive(root->left, dropped_ids, count, max_count);
    collect_dropped_nodes_recursive(root->right, dropped_ids, count, max_count);
}

int wbpxre_routing_table_cleanup_dropped(wbpxre_routing_table_t *table) {
    if (!table) return 0;

    pthread_mutex_lock(&table->update_lock);

    /* Collect dropped node IDs (up to 1000 at a time) */
    uint8_t dropped_ids[1000][WBPXRE_NODE_ID_LEN];
    int dropped_count = 0;

    collect_dropped_nodes_recursive(table->root, dropped_ids, &dropped_count, 1000);

    /* Remove all dropped nodes */
    int removed_count = 0;
    for (int i = 0; i < dropped_count; i++) {
        /* Remove from flat array */
        remove_node_from_flat_array(table, dropped_ids[i]);

        bool removed = false;
        table->root = remove_node_recursive(table->root, dropped_ids[i], &removed);

        if (removed) {
            table->node_count--;
            removed_count++;
        }
    }

    pthread_mutex_unlock(&table->update_lock);

    return removed_count;
}

/* ============================================================================
 * Get Old Nodes for Verification
 * ============================================================================ */

static void collect_old_nodes_recursive(wbpxre_routing_node_t *root,
                                         wbpxre_routing_node_t **array,
                                         int *count, int n, time_t threshold) {
    if (!root || *count >= n) return;

    /* Skip dropped nodes */
    if (root->dropped) {
        collect_old_nodes_recursive(root->left, array, count, n, threshold);
        collect_old_nodes_recursive(root->right, array, count, n, threshold);
        return;
    }

    /* Check if node needs verification */
    if (root->last_responded_at == 0 || root->last_responded_at < threshold) {
        array[(*count)++] = root;
    }

    if (*count < n) {
        collect_old_nodes_recursive(root->left, array, count, n, threshold);
    }
    if (*count < n) {
        collect_old_nodes_recursive(root->right, array, count, n, threshold);
    }
}

int wbpxre_routing_table_get_old_nodes(wbpxre_routing_table_t *table,
                                        wbpxre_routing_node_t **nodes_out,
                                        int n, time_t threshold) {
    if (!table || !nodes_out || n <= 0) return 0;

    rcu_read_lock();

    /* Collect old node pointers */
    wbpxre_routing_node_t *candidates[n];
    int count = 0;
    collect_old_nodes_recursive(table->root, candidates, &count, n, threshold);

    /* Copy nodes to avoid data races after lock release */
    for (int i = 0; i < count; i++) {
        wbpxre_routing_node_t *node_copy = malloc(sizeof(wbpxre_routing_node_t));
        memcpy(node_copy, candidates[i], sizeof(wbpxre_routing_node_t));
        /* Clear tree pointers since this is a standalone copy */
        node_copy->left = NULL;
        node_copy->right = NULL;
        nodes_out[i] = node_copy;
    }

    rcu_read_unlock();

    return count;
}
