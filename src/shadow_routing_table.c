#include "shadow_routing_table.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct shadow_routing_table {
    node_metadata_t *nodes;
    size_t capacity;
    size_t size;

    uv_rwlock_t rwlock;
    uv_timer_t prune_timer;
    int prune_interval_sec;

    bool running;
    bool timer_closed;  /* Flag to track timer close completion */
};

/* Close callback for prune timer */
static void on_prune_timer_closed(uv_handle_t *handle) {
    shadow_routing_table_t *table = (shadow_routing_table_t *)handle->data;
    if (table) {
        table->timer_closed = true;
        log_msg(LOG_DEBUG, "Shadow table prune timer closed");
    }
}

/* Prune dead/unresponsive nodes */
static void prune_timer_cb(uv_timer_t *timer) {
    shadow_routing_table_t *table = (shadow_routing_table_t*)timer->data;
    
    uv_rwlock_wrlock(&table->rwlock);
    
    time_t now = time(NULL);
    size_t pruned = 0;
    size_t new_size = 0;
    
    for (size_t i = 0; i < table->size; i++) {
        node_metadata_t *node = &table->nodes[i];
        
        /* Prune nodes not seen in 10 minutes or with very low response rate */
        bool should_prune = (now - node->last_seen > 600) ||
                           (node->queries_sent > 10 && node->response_rate < 0.1);
        
        if (!should_prune) {
            if (new_size != i) {
                memcpy(&table->nodes[new_size], node, sizeof(node_metadata_t));
            }
            new_size++;
        } else {
            pruned++;
        }
    }
    
    table->size = new_size;
    
    uv_rwlock_wrunlock(&table->rwlock);
    
    if (pruned > 0) {
        log_msg(LOG_DEBUG, "Pruned %zu dead nodes from shadow table (now %zu/%zu)",
                pruned, new_size, table->capacity);
    }
}

shadow_routing_table_t* shadow_table_init(size_t capacity, int prune_interval_sec, uv_loop_t *loop) {
    if (capacity == 0 || !loop) {
        log_msg(LOG_ERROR, "Invalid shadow table parameters");
        return NULL;
    }
    
    shadow_routing_table_t *table = calloc(1, sizeof(shadow_routing_table_t));
    if (!table) {
        log_msg(LOG_ERROR, "Failed to allocate shadow routing table");
        return NULL;
    }
    
    table->capacity = capacity;
    table->size = 0;
    table->prune_interval_sec = prune_interval_sec;
    table->running = true;
    table->timer_closed = false;
    
    /* Allocate nodes array */
    table->nodes = calloc(capacity, sizeof(node_metadata_t));
    if (!table->nodes) {
        log_msg(LOG_ERROR, "Failed to allocate shadow table nodes");
        free(table);
        return NULL;
    }
    
    /* Initialize rwlock */
    if (uv_rwlock_init(&table->rwlock) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize shadow table rwlock");
        free(table->nodes);
        free(table);
        return NULL;
    }
    
    /* Initialize prune timer */
    if (uv_timer_init(loop, &table->prune_timer) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize prune timer");
        uv_rwlock_destroy(&table->rwlock);
        free(table->nodes);
        free(table);
        return NULL;
    }
    
    table->prune_timer.data = table;
    
    /* Start prune timer */
    if (prune_interval_sec > 0) {
        uint64_t interval_ms = prune_interval_sec * 1000;
        uv_timer_start(&table->prune_timer, prune_timer_cb, interval_ms, interval_ms);
    }
    
    log_msg(LOG_DEBUG, "Shadow routing table initialized: capacity=%zu, prune_interval=%ds",
            capacity, prune_interval_sec);
    
    return table;
}

int shadow_table_upsert(shadow_routing_table_t *table, const node_metadata_t *node) {
    if (!table || !node) {
        return -1;
    }
    
    uv_rwlock_wrlock(&table->rwlock);
    
    /* Search for existing node */
    for (size_t i = 0; i < table->size; i++) {
        if (memcmp(table->nodes[i].id, node->id, 20) == 0) {
            /* Update existing node */
            memcpy(&table->nodes[i], node, sizeof(node_metadata_t));
            uv_rwlock_wrunlock(&table->rwlock);
            return 0;
        }
    }
    
    /* Add new node if space available */
    if (table->size < table->capacity) {
        memcpy(&table->nodes[table->size], node, sizeof(node_metadata_t));
        table->size++;
        uv_rwlock_wrunlock(&table->rwlock);
        return 0;
    }
    
    /* Table full - replace worst node (lowest response rate) */
    size_t worst_idx = 0;
    double worst_rate = table->nodes[0].response_rate;
    
    for (size_t i = 1; i < table->size; i++) {
        if (table->nodes[i].response_rate < worst_rate) {
            worst_rate = table->nodes[i].response_rate;
            worst_idx = i;
        }
    }
    
    /* Only replace if new node is better (or we have no data on new node) */
    if (node->queries_sent == 0 || node->response_rate > worst_rate) {
        memcpy(&table->nodes[worst_idx], node, sizeof(node_metadata_t));
    }
    
    uv_rwlock_wrunlock(&table->rwlock);
    return 0;
}

int shadow_table_mark_responded(shadow_routing_table_t *table, const unsigned char *node_id,
                                uint32_t samples_count, int interval) {
    if (!table || !node_id) {
        return -1;
    }
    
    uv_rwlock_wrlock(&table->rwlock);
    
    for (size_t i = 0; i < table->size; i++) {
        if (memcmp(table->nodes[i].id, node_id, 20) == 0) {
            node_metadata_t *node = &table->nodes[i];
            
            node->last_responded = time(NULL);
            node->responses_received++;
            
            if (samples_count > 0) {
                node->bep51_support = true;
                node->samples_discovered += samples_count;
                
                if (interval > 0) {
                    node->next_sample_time = time(NULL) + interval;
                }
            }
            
            /* Update response rate */
            if (node->queries_sent > 0) {
                node->response_rate = (double)node->responses_received / node->queries_sent;
            }
            
            uv_rwlock_wrunlock(&table->rwlock);
            return 0;
        }
    }
    
    uv_rwlock_wrunlock(&table->rwlock);
    return -1;
}

/* PHASE 2: Mark node as queried (track query sent) */
int shadow_table_mark_queried(shadow_routing_table_t *table, const unsigned char *node_id) {
    if (!table || !node_id) {
        return -1;
    }

    uv_rwlock_wrlock(&table->rwlock);

    for (size_t i = 0; i < table->size; i++) {
        if (memcmp(table->nodes[i].id, node_id, 20) == 0) {
            node_metadata_t *node = &table->nodes[i];

            node->queries_sent++;

            /* Update response rate */
            if (node->queries_sent > 0) {
                node->response_rate = (double)node->responses_received / node->queries_sent;
            }

            uv_rwlock_wrunlock(&table->rwlock);
            return 0;
        }
    }

    uv_rwlock_wrunlock(&table->rwlock);
    return -1;  /* Node not found */
}

/* PHASE 2: Drop nodes with low response rates */
int shadow_table_drop_low_quality(shadow_routing_table_t *table, uint32_t min_queries, double min_response_rate) {
    if (!table) {
        return -1;
    }

    uv_rwlock_wrlock(&table->rwlock);

    size_t new_size = 0;
    size_t dropped = 0;

    for (size_t i = 0; i < table->size; i++) {
        node_metadata_t *node = &table->nodes[i];

        /* Drop nodes that have been queried enough times but have low response rate */
        bool should_drop = (node->queries_sent >= min_queries) &&
                          (node->response_rate < min_response_rate);

        if (!should_drop) {
            if (new_size != i) {
                memcpy(&table->nodes[new_size], node, sizeof(node_metadata_t));
            }
            new_size++;
        } else {
            dropped++;
        }
    }

    table->size = new_size;

    uv_rwlock_wrunlock(&table->rwlock);

    if (dropped > 0) {
        log_msg(LOG_INFO, "Dropped %zu low-quality nodes (<%d%% response rate after %u queries)",
                dropped, (int)(min_response_rate * 100), min_queries);
    }

    return dropped;
}

/* PHASE 3: Mark node as BEP 51 unsupported */
int shadow_table_mark_bep51_unsupported(shadow_routing_table_t *table, const unsigned char *node_id) {
    if (!table || !node_id) {
        return -1;
    }

    uv_rwlock_wrlock(&table->rwlock);

    for (size_t i = 0; i < table->size; i++) {
        if (memcmp(table->nodes[i].id, node_id, 20) == 0) {
            node_metadata_t *node = &table->nodes[i];

            node->bep51_unsupported = true;
            node->bep51_support = false;  /* Clear support flag if set */

            uv_rwlock_wrunlock(&table->rwlock);
            return 0;
        }
    }

    uv_rwlock_wrunlock(&table->rwlock);
    return -1;  /* Node not found */
}

/* PHASE 3: Increment BEP 51 failure count */
int shadow_table_mark_bep51_failed(shadow_routing_table_t *table, const unsigned char *node_id) {
    if (!table || !node_id) {
        return -1;
    }

    uv_rwlock_wrlock(&table->rwlock);

    for (size_t i = 0; i < table->size; i++) {
        if (memcmp(table->nodes[i].id, node_id, 20) == 0) {
            node_metadata_t *node = &table->nodes[i];

            node->bep51_failures++;

            /* After 3 failures, mark as unsupported */
            if (node->bep51_failures >= 3) {
                node->bep51_unsupported = true;
                node->bep51_support = false;
                log_msg(LOG_DEBUG, "Node marked as BEP 51 unsupported after %u failures",
                        node->bep51_failures);
            }

            uv_rwlock_wrunlock(&table->rwlock);
            return 0;
        }
    }

    uv_rwlock_wrunlock(&table->rwlock);
    return -1;  /* Node not found */
}

/* Get dubious nodes that need verification (BUGFIX) */
int shadow_table_get_dubious(shadow_routing_table_t *table, node_metadata_t **out,
                             size_t count, int max_age_sec) {
    if (!table || !out || count == 0) {
        return 0;
    }

    uv_rwlock_rdlock(&table->rwlock);

    time_t now = time(NULL);
    size_t found = 0;

    /* Find nodes that are "dubious" - haven't responded recently or never responded */
    for (size_t i = 0; i < table->size && found < count; i++) {
        node_metadata_t *node = &table->nodes[i];

        /* Node is dubious if:
         * 1. Never responded (last_responded == 0), OR
         * 2. Haven't responded within max_age_sec, OR
         * 3. Have low response rate (< 50% after at least 2 queries)
         */
        bool is_dubious = false;

        if (node->last_responded == 0) {
            /* Never responded - definitely dubious */
            is_dubious = true;
        } else if (max_age_sec > 0 && (now - node->last_responded) > max_age_sec) {
            /* Haven't responded recently */
            is_dubious = true;
        } else if (node->queries_sent >= 2 && node->response_rate < 0.5) {
            /* Low response rate after multiple queries */
            is_dubious = true;
        }

        if (is_dubious) {
            out[found++] = node;
        }
    }

    uv_rwlock_rdunlock(&table->rwlock);

    return found;
}

int shadow_table_get_best_bep51(shadow_routing_table_t *table, node_metadata_t **out, size_t count) {
    if (!table || !out || count == 0) {
        return 0;
    }

    uv_rwlock_rdlock(&table->rwlock);

    time_t now = time(NULL);
    size_t found = 0;

    /* PHASE 3: Find BEP 51 nodes that are ready for sampling
     * Skip nodes marked as unsupported (failed 3+ times) */
    for (size_t i = 0; i < table->size && found < count; i++) {
        node_metadata_t *node = &table->nodes[i];

        if (node->bep51_support &&
            !node->bep51_unsupported &&  /* PHASE 3: Skip unsupported nodes */
            now >= node->next_sample_time) {
            out[found++] = node;
        }
    }

    uv_rwlock_rdunlock(&table->rwlock);

    return found;
}

int shadow_table_get_bep51_for_cache(shadow_routing_table_t *table, node_metadata_t **out, size_t count) {
    if (!table || !out || count == 0) {
        return 0;
    }

    uv_rwlock_rdlock(&table->rwlock);

    /* Collect all BEP51 nodes with good response rates */
    size_t found = 0;
    time_t now = time(NULL);

    for (size_t i = 0; i < table->size && found < count; i++) {
        node_metadata_t *node = &table->nodes[i];

        /* PHASE 3: Only include BEP51 nodes that have been seen recently and not marked unsupported */
        if (node->bep51_support &&
            !node->bep51_unsupported &&  /* PHASE 3: Skip unsupported nodes */
            (now - node->last_seen) < 3600 &&
            node->response_rate > 0.5) {  /* At least 50% response rate */
            out[found++] = node;
        }
    }

    /* Sort by response rate (simple bubble sort, acceptable for small arrays) */
    if (found > 1) {
        for (size_t i = 0; i < found - 1; i++) {
            for (size_t j = 0; j < found - i - 1; j++) {
                if (out[j]->response_rate < out[j + 1]->response_rate) {
                    node_metadata_t *temp = out[j];
                    out[j] = out[j + 1];
                    out[j + 1] = temp;
                }
            }
        }
    }

    uv_rwlock_rdunlock(&table->rwlock);

    return found;
}

int shadow_table_get_random(shadow_routing_table_t *table, node_metadata_t **out, size_t count) {
    if (!table || !out || count == 0) {
        return 0;
    }
    
    uv_rwlock_rdlock(&table->rwlock);
    
    if (table->size == 0) {
        uv_rwlock_rdunlock(&table->rwlock);
        return 0;
    }
    
    size_t num_to_return = (count < table->size) ? count : table->size;
    
    /* Simple random selection */
    for (size_t i = 0; i < num_to_return; i++) {
        size_t idx = rand() % table->size;
        out[i] = &table->nodes[idx];
    }
    
    uv_rwlock_rdunlock(&table->rwlock);
    
    return num_to_return;
}

void shadow_table_stats(shadow_routing_table_t *table, size_t *out_total,
                       size_t *out_bep51_nodes, size_t *out_capacity) {
    if (!table) {
        return;
    }
    
    uv_rwlock_rdlock(&table->rwlock);
    
    if (out_total) {
        *out_total = table->size;
    }
    
    if (out_capacity) {
        *out_capacity = table->capacity;
    }
    
    if (out_bep51_nodes) {
        size_t bep51_count = 0;
        size_t bep51_unsupported_count = 0;  /* PHASE 3 */

        for (size_t i = 0; i < table->size; i++) {
            if (table->nodes[i].bep51_support && !table->nodes[i].bep51_unsupported) {
                bep51_count++;
            }
            if (table->nodes[i].bep51_unsupported) {
                bep51_unsupported_count++;
            }
        }

        *out_bep51_nodes = bep51_count;

        /* PHASE 3: Log unsupported count periodically */
        static time_t last_log = 0;
        time_t now = time(NULL);
        if (bep51_unsupported_count > 0 && now - last_log >= 300) {  /* Every 5 minutes */
            log_msg(LOG_INFO, "BEP 51 Stats: %zu supported, %zu unsupported (filtered)",
                    bep51_count, bep51_unsupported_count);
            last_log = now;
        }
    }
    
    uv_rwlock_rdunlock(&table->rwlock);
}

int shadow_table_save(shadow_routing_table_t *table, const char *path) {
    if (!table || !path) {
        return -1;
    }
    
    FILE *fp = fopen(path, "wb");
    if (!fp) {
        log_msg(LOG_ERROR, "Failed to open shadow table file for writing: %s", path);
        return -1;
    }
    
    uv_rwlock_rdlock(&table->rwlock);
    
    /* Write header */
    if (fwrite(&table->size, sizeof(size_t), 1, fp) != 1) {
        log_msg(LOG_ERROR, "Failed to write shadow table header");
        uv_rwlock_rdunlock(&table->rwlock);
        fclose(fp);
        return -1;
    }
    
    /* Write nodes */
    if (table->size > 0) {
        if (fwrite(table->nodes, sizeof(node_metadata_t), table->size, fp) != table->size) {
            log_msg(LOG_ERROR, "Failed to write shadow table data");
            uv_rwlock_rdunlock(&table->rwlock);
            fclose(fp);
            return -1;
        }
    }
    
    uv_rwlock_rdunlock(&table->rwlock);
    fclose(fp);
    
    log_msg(LOG_DEBUG, "Shadow table saved to %s (%zu nodes)", path, table->size);
    return 0;
}

shadow_routing_table_t* shadow_table_load(const char *path, int prune_interval_sec, uv_loop_t *loop) {
    if (!path || !loop) {
        return NULL;
    }
    
    FILE *fp = fopen(path, "rb");
    if (!fp) {
        log_msg(LOG_WARN, "Failed to open shadow table file for reading: %s", path);
        return NULL;
    }
    
    size_t size;
    if (fread(&size, sizeof(size_t), 1, fp) != 1) {
        log_msg(LOG_ERROR, "Failed to read shadow table header");
        fclose(fp);
        return NULL;
    }
    
    /* Create table with loaded capacity */
    shadow_routing_table_t *table = shadow_table_init(size * 2, prune_interval_sec, loop);
    if (!table) {
        fclose(fp);
        return NULL;
    }
    
    /* Read nodes */
    if (size > 0) {
        if (fread(table->nodes, sizeof(node_metadata_t), size, fp) != size) {
            log_msg(LOG_ERROR, "Failed to read shadow table data");
            shadow_table_cleanup(table);
            fclose(fp);
            return NULL;
        }
        table->size = size;
    }
    
    fclose(fp);
    log_msg(LOG_DEBUG, "Shadow table loaded from %s (%zu nodes)", path, size);
    return table;
}

void shadow_table_shutdown(shadow_routing_table_t *table) {
    /* Defensive NULL check */
    if (!table) {
        log_msg(LOG_DEBUG, "shadow_table_shutdown: table is NULL, nothing to shutdown");
        return;
    }

    log_msg(LOG_DEBUG, "Shadow table shutting down...");

    uv_rwlock_wrlock(&table->rwlock);
    table->running = false;
    uv_rwlock_wrunlock(&table->rwlock);

    /* Stop prune timer */
    uv_timer_stop(&table->prune_timer);
}

void shadow_table_cleanup(shadow_routing_table_t *table) {
    /* Defensive NULL check */
    if (!table) {
        log_msg(LOG_DEBUG, "shadow_table_cleanup: table is NULL, nothing to clean");
        return;
    }

    if (table->running) {
        shadow_table_shutdown(table);
    }

    /* Close timer handle with callback if not already closing */
    log_msg(LOG_DEBUG, "Closing shadow table prune timer...");
    if (!uv_is_closing((uv_handle_t*)&table->prune_timer)) {
        uv_close((uv_handle_t*)&table->prune_timer, on_prune_timer_closed);
    } else {
        /* If already closing, mark as closed to avoid wait loop */
        table->timer_closed = true;
    }

    /* Get the event loop from the timer handle */
    uv_loop_t *loop = uv_handle_get_loop((uv_handle_t*)&table->prune_timer);

    /* Run the event loop until timer is closed */
    log_msg(LOG_DEBUG, "Waiting for shadow table timer to close...");
    while (!table->timer_closed && uv_loop_alive(loop)) {
        uv_run(loop, UV_RUN_ONCE);
    }
    log_msg(LOG_DEBUG, "Shadow table timer closed");

    log_msg(LOG_DEBUG, "Shadow table cleaned up (%zu nodes)", table->size);

    /* Cleanup - only after timer is fully closed */
    uv_rwlock_destroy(&table->rwlock);
    free(table->nodes);
    free(table);
}
