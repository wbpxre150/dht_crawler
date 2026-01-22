#define _DEFAULT_SOURCE  /* For usleep */

#include "supervisor.h"
#include "dht_crawler.h"
#include "batch_writer.h"
#include "bloom_filter.h"
#include "shared_node_pool.h"
#include "bep51_cache.h"
#include "tree_socket.h"
#include "tree_dispatcher.h"
#include "tree_protocol.h"
#include "tree_routing.h"
#include "wbpxre_dht.h"
#include "keyspace.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>

/* Bootstrap nodes (reused from thread_tree.c) */
static const char *BOOTSTRAP_NODES[] = {
    "router.bittorrent.com",
    "dht.transmissionbt.com",
    "router.utorrent.com",
    NULL
};
static const int BOOTSTRAP_PORT = 6881;

/* Forward declaration of internal helpers */
static thread_tree_t *spawn_tree(supervisor_t *sup, int slot_index, thread_tree_t *old_tree);
static void *monitor_thread_func(void *arg);
static int global_bootstrap(supervisor_t *sup, int target_nodes, int timeout_sec, int num_workers);

supervisor_t *supervisor_create(supervisor_config_t *config) {
    if (!config || config->max_trees <= 0) {
        log_msg(LOG_ERROR, "[supervisor] Invalid config");
        return NULL;
    }

    supervisor_t *sup = calloc(1, sizeof(supervisor_t));
    if (!sup) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate supervisor");
        return NULL;
    }

    sup->max_trees = config->max_trees;
    sup->active_trees = 0;
    sup->next_tree_id = 1;
    sup->monitor_running = 0;

    /* Keyspace partitioning (default: enabled) */
    sup->use_keyspace_partitioning = config->use_keyspace_partitioning;
    sup->dht_port = config->dht_port;

    /* Store shared resources */
    sup->batch_writer = config->batch_writer;
    sup->bloom_filter = config->bloom_filter;
    sup->failure_bloom = NULL;  /* Will be initialized during start */
    sup->failure_bloom_path = "data/failure_bloom.dat";
    sup->shared_node_pool = NULL;  /* Will be created during bootstrap */
    sup->shared_socket = NULL;     /* Will be created if dht_port > 0 */
    sup->shared_dispatcher = NULL; /* Will be created if dht_port > 0 */

    /* Store bloom filter configuration */
    sup->failure_bloom_capacity = config->failure_bloom_capacity > 0 ? config->failure_bloom_capacity : 30000000;
    sup->bloom_error_rate = config->bloom_error_rate > 0 ? config->bloom_error_rate : 0.001;

    /* Store worker counts */
    sup->num_find_node_workers = config->num_find_node_workers > 0 ? config->num_find_node_workers : 10;
    sup->num_bep51_workers = config->num_bep51_workers;
    sup->num_get_peers_workers = config->num_get_peers_workers;
    sup->num_metadata_workers = config->num_metadata_workers;

    /* Stage 2 settings (Global Bootstrap - NEW) */
    sup->global_bootstrap_target = config->global_bootstrap_target > 0 ? config->global_bootstrap_target : 5000;
    sup->global_bootstrap_timeout_sec = config->global_bootstrap_timeout_sec > 0 ? config->global_bootstrap_timeout_sec : 60;
    sup->global_bootstrap_workers = config->global_bootstrap_workers > 0 ? config->global_bootstrap_workers : 50;
    sup->per_tree_sample_size = config->per_tree_sample_size > 0 ? config->per_tree_sample_size : 1000;

    /* Stage 3 settings (BEP51) */
    sup->infohash_queue_capacity = config->infohash_queue_capacity > 0 ? config->infohash_queue_capacity : 5000;
    sup->bep51_query_interval_ms = config->bep51_query_interval_ms >= 0 ? config->bep51_query_interval_ms : 10;
    sup->bep51_node_cooldown_sec = config->bep51_node_cooldown_sec > 0 ? config->bep51_node_cooldown_sec : 30;

    /* Stage 4 settings (get_peers) */
    sup->peers_queue_capacity = config->peers_queue_capacity > 0 ? config->peers_queue_capacity : 2000;
    sup->get_peers_timeout_ms = config->get_peers_timeout_ms > 0 ? config->get_peers_timeout_ms : 3000;

    /* Find_node throttling settings */
    sup->infohash_pause_threshold = config->infohash_pause_threshold > 0 ? config->infohash_pause_threshold : 2000;
    sup->infohash_resume_threshold = config->infohash_resume_threshold > 0 ? config->infohash_resume_threshold : 1000;

    /* Get_peers throttling settings */
    sup->peers_pause_threshold = config->peers_pause_threshold > 0 ? config->peers_pause_threshold : 2000;
    sup->peers_resume_threshold = config->peers_resume_threshold > 0 ? config->peers_resume_threshold : 1000;

    /* Stage 5 settings */
    sup->tcp_connect_timeout_ms = config->tcp_connect_timeout_ms > 0 ? config->tcp_connect_timeout_ms : 5000;

    /* Metadata rate-based respawn settings */
    sup->min_metadata_rate = config->min_metadata_rate >= 0 ? config->min_metadata_rate : 0.01;
    sup->rate_check_interval_sec = config->rate_check_interval_sec > 0 ? config->rate_check_interval_sec : 60;
    sup->rate_grace_period_sec = config->rate_grace_period_sec > 0 ? config->rate_grace_period_sec : 30;
    sup->min_lifetime_minutes = config->min_lifetime_minutes > 0 ? config->min_lifetime_minutes : 10;
    sup->require_empty_queue = config->require_empty_queue;

    /* Porn filter settings */
    sup->porn_filter_enabled = config->porn_filter_enabled;

    /* BEP51 cache settings */
    sup->bep51_cache_capacity = config->bep51_cache_capacity > 0 ? config->bep51_cache_capacity : 10000;
    sup->bep51_cache_submit_percent = config->bep51_cache_submit_percent > 0 ? config->bep51_cache_submit_percent : 5;
    strncpy(sup->bep51_cache_path, config->bep51_cache_path, sizeof(sup->bep51_cache_path) - 1);

    /* Initialize cumulative statistics */
    atomic_init(&sup->cumulative_metadata_count, 0);
    atomic_init(&sup->cumulative_first_strike_failures, 0);
    atomic_init(&sup->cumulative_second_strike_failures, 0);
    atomic_init(&sup->cumulative_filtered_count, 0);

    /* Initialize mutex */
    if (pthread_mutex_init(&sup->trees_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to init mutex");
        free(sup);
        return NULL;
    }

    /* Allocate tree array */
    sup->trees = calloc(sup->max_trees, sizeof(thread_tree_t *));
    if (!sup->trees) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate trees array");
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }

    /* Store respawn overlapping configuration */
    sup->respawn_spawn_threshold = config->respawn_spawn_threshold >= 0 ? config->respawn_spawn_threshold : 50;
    sup->respawn_drain_timeout_sec = config->respawn_drain_timeout_sec > 0 ? config->respawn_drain_timeout_sec : 120;
    sup->max_draining_trees = config->max_draining_trees > 0 ? config->max_draining_trees : 8;

    /* Allocate draining trees array */
    sup->draining_trees = calloc(sup->max_draining_trees, sizeof(draining_tree_t));
    if (!sup->draining_trees) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate draining trees array");
        free(sup->trees);
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }
    sup->draining_count = 0;
    if (pthread_mutex_init(&sup->draining_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to init draining_lock mutex");
        free(sup->draining_trees);
        free(sup->trees);
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }

    log_msg(LOG_DEBUG, "[supervisor] Created with max_trees=%d, bootstrap_target=%d, draining_slots=%d, spawn_threshold=%d, drain_timeout=%ds",
            sup->max_trees, sup->global_bootstrap_target, sup->max_draining_trees,
            sup->respawn_spawn_threshold, sup->respawn_drain_timeout_sec);

    return sup;
}

/**
 * Spawn a new tree for the given slot
 * @param sup Supervisor instance
 * @param slot_index Slot index (0 to max_trees-1), used as partition index
 * @param old_tree Previous tree in this slot (NULL for first spawn, non-NULL for respawn)
 * @return Pointer to new tree, or NULL on error
 */
static thread_tree_t *spawn_tree(supervisor_t *sup, int slot_index, thread_tree_t *old_tree) {
    tree_config_t config = {
        /* Keyspace partitioning */
        .use_keyspace_partitioning = sup->use_keyspace_partitioning,
        .partition_index = (uint32_t)slot_index,
        .num_partitions = (uint32_t)sup->max_trees,
        .dht_port = sup->dht_port,

        /* Stage 2 settings (Bootstrap) - using configurable find_node workers */
        .num_bootstrap_workers = sup->num_find_node_workers,
        .num_find_node_workers = sup->num_find_node_workers,  /* Continuous find_node workers */
        .bootstrap_timeout_sec = 0,  /* No longer used */
        .routing_threshold = 0,      /* No longer used */
        /* Worker counts */
        .num_bep51_workers = sup->num_bep51_workers,
        .num_get_peers_workers = sup->num_get_peers_workers,
        .num_metadata_workers = sup->num_metadata_workers,
        /* Stage 3 settings */
        .infohash_queue_capacity = sup->infohash_queue_capacity,
        .bep51_query_interval_ms = sup->bep51_query_interval_ms,
        .bep51_node_cooldown_sec = sup->bep51_node_cooldown_sec,
        /* Stage 4 settings */
        .peers_queue_capacity = sup->peers_queue_capacity,
        .get_peers_timeout_ms = sup->get_peers_timeout_ms,
        /* Find_node throttling settings */
        .infohash_pause_threshold = sup->infohash_pause_threshold,
        .infohash_resume_threshold = sup->infohash_resume_threshold,
        /* Get_peers throttling settings */
        .peers_pause_threshold = sup->peers_pause_threshold,
        .peers_resume_threshold = sup->peers_resume_threshold,
        /* Stage 5 settings */
        .tcp_connect_timeout_ms = sup->tcp_connect_timeout_ms,
        /* Metadata rate-based respawn settings */
        .min_metadata_rate = sup->min_metadata_rate,
        .rate_check_interval_sec = sup->rate_check_interval_sec,
        .rate_grace_period_sec = sup->rate_grace_period_sec,
        .min_lifetime_minutes = sup->min_lifetime_minutes,
        .require_empty_queue = sup->require_empty_queue,
        /* Porn filter settings */
        .porn_filter_enabled = sup->porn_filter_enabled,
        /* Shared resources */
        .batch_writer = sup->batch_writer,
        .bloom_filter = sup->bloom_filter,
        .failure_bloom = sup->failure_bloom,
        .supervisor_ctx = sup,
        .on_shutdown = supervisor_on_tree_shutdown,
        /* Shared socket/dispatcher from supervisor (NULL = create private) */
        .shared_socket = sup->shared_socket,
        .shared_dispatcher = sup->shared_dispatcher
    };

    uint32_t tree_id = sup->next_tree_id++;
    thread_tree_t *tree = thread_tree_create(tree_id, &config);
    if (!tree) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create tree %u", tree_id);
        return NULL;
    }

    /* Set supervisor backlink */
    tree->supervisor = sup;

    /* If respawning (old_tree != NULL), perturb the node ID to explore different neighborhood */
    if (old_tree && sup->use_keyspace_partitioning) {
        log_msg(LOG_DEBUG, "[supervisor] Respawning tree in slot %d, perturbing node ID within partition %u/%u",
                slot_index, tree->partition_index, tree->num_partitions);

        keyspace_perturb_node_id(old_tree->node_id,
                                tree->partition_index,
                                tree->num_partitions,
                                tree->node_id);

        /* Verify the new node ID is in the correct partition */
        if (!keyspace_verify_partition(tree->node_id, tree->partition_index, tree->num_partitions)) {
            log_msg(LOG_WARN, "[supervisor] Perturbed node ID failed partition verification");
        }
    }

    return tree;
}

/**
 * Move a tree from active array to draining list
 * Caller must hold trees_lock
 * Returns 0 on success, -1 if draining slots full
 */
static int move_to_draining(supervisor_t *sup, int slot_index) {
    thread_tree_t *tree = sup->trees[slot_index];
    if (!tree) {
        return -1;
    }

    pthread_mutex_lock(&sup->draining_lock);

    /* Check if draining slots available */
    if (sup->draining_count >= sup->max_draining_trees) {
        pthread_mutex_unlock(&sup->draining_lock);
        log_msg(LOG_WARN, "[tree %u] Cannot move to draining: %d/%d slots full",
                tree->tree_id, sup->draining_count, sup->max_draining_trees);
        return -1;
    }

    /* Add to draining list */
    sup->draining_trees[sup->draining_count].tree = tree;
    sup->draining_trees[sup->draining_count].drain_start_time = time(NULL);
    sup->draining_trees[sup->draining_count].original_slot = slot_index;
    sup->draining_count++;

    pthread_mutex_unlock(&sup->draining_lock);

    /* Clear from active array */
    sup->trees[slot_index] = NULL;
    sup->active_trees--;

    log_msg(LOG_INFO, "[tree %u] Moved to draining list (slot %d, draining=%d/%d, active_connections=%d)",
            tree->tree_id, slot_index, sup->draining_count, sup->max_draining_trees,
            (int)atomic_load(&tree->active_connections));

    return 0;
}

/**
 * Monitor draining trees and destroy when ready
 * Called from monitor thread
 */
static void monitor_draining_trees(supervisor_t *sup) {
    pthread_mutex_lock(&sup->draining_lock);

    /* Check each draining tree */
    for (int i = 0; i < sup->draining_count; i++) {
        draining_tree_t *dt = &sup->draining_trees[i];
        if (!dt->tree) {
            continue;
        }

        int active_conns = atomic_load(&dt->tree->active_connections);
        time_t now = time(NULL);
        double drain_time = difftime(now, dt->drain_start_time);

        bool should_destroy = false;
        const char *reason = NULL;

        /* Check destroy conditions */
        if (active_conns == 0) {
            should_destroy = true;
            reason = "all connections drained";
        } else if (drain_time >= sup->respawn_drain_timeout_sec) {
            should_destroy = true;
            reason = "drain timeout expired";
        }

        if (should_destroy) {
            log_msg(LOG_INFO, "[tree %u] Destroying draining tree (reason: %s, drain_time=%.0fs, final_connections=%d)",
                    dt->tree->tree_id, reason, drain_time, active_conns);

            /* Get stats before destruction */
            uint64_t metadata_count = atomic_load(&dt->tree->metadata_count);

            /* Release lock before blocking destruction */
            pthread_mutex_unlock(&sup->draining_lock);

            /* Destroy tree (may block briefly on pthread_join) */
            thread_tree_destroy(dt->tree);

            /* Re-acquire lock and remove from list */
            pthread_mutex_lock(&sup->draining_lock);

            /* Shift remaining draining trees down */
            for (int j = i; j < sup->draining_count - 1; j++) {
                sup->draining_trees[j] = sup->draining_trees[j + 1];
            }
            sup->draining_count--;

            /* Clear last slot */
            memset(&sup->draining_trees[sup->draining_count], 0, sizeof(draining_tree_t));

            log_msg(LOG_DEBUG, "Draining tree destroyed (metadata_count=%lu, remaining_draining=%d)",
                    (unsigned long)metadata_count, sup->draining_count);

            /* Adjust loop counter since we shifted array */
            i--;
        }
    }

    pthread_mutex_unlock(&sup->draining_lock);
}

/* Helper: resolve hostname to sockaddr_storage */
static int resolve_hostname(const char *hostname, int port, struct sockaddr_storage *addr) {
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);

    int ret = getaddrinfo(hostname, port_str, &hints, &res);
    if (ret != 0) {
        return -1;
    }

    memcpy(addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return 0;
}

/* Helper: generate random node ID */
static void generate_random_node_id(uint8_t *node_id) {
    srand((unsigned int)(time(NULL) ^ (uintptr_t)node_id));
    for (int i = 0; i < 20; i++) {
        node_id[i] = (uint8_t)(rand() % 256);
    }
}

/* Helper: generate random target */
static void generate_random_target(uint8_t *target) {
    for (int i = 0; i < 20; i++) {
        target[i] = (uint8_t)(rand() % 256);
    }
}

/* Bootstrap worker context */
typedef struct global_bootstrap_ctx {
    shared_node_pool_t *pool;
    tree_routing_table_t *routing_table;
    tree_socket_t *socket;
    uint8_t node_id[20];
    atomic_bool *shutdown_flag;
    int worker_id;
} global_bootstrap_ctx_t;

/* Global bootstrap worker: aggressively queries nodes */
static void *global_bootstrap_worker_func(void *arg) {
    global_bootstrap_ctx_t *ctx = (global_bootstrap_ctx_t *)arg;

    log_msg(LOG_DEBUG, "[global_bootstrap] Worker %d started", ctx->worker_id);

    while (!atomic_load(ctx->shutdown_flag)) {
        /* Get random nodes from routing table */
        tree_node_t nodes[8];
        int got = tree_routing_get_random_nodes(ctx->routing_table, nodes, 8);

        if (got == 0) {
            /* No nodes yet, wait a bit */
            usleep(100000);  /* 100ms */
            continue;
        }

        /* Query each node with a random target to explore keyspace */
        for (int i = 0; i < got; i++) {
            uint8_t random_target[20];
            generate_random_target(random_target);

            /* Build and send find_node query */
            /* Note: We can't use tree_send_find_node since we don't have a thread_tree */
            /* So we'll just send queries manually or reuse the socket */
            /* For simplicity, let's create a minimal send here */

            /* Actually, we need to build the bencode message - let's skip this for now */
            /* and just let the main loop handle everything */
            (void)random_target;  /* Suppress warning */
        }

        /* Rate limit */
        usleep(10000);  /* 10ms */
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[global_bootstrap] Worker %d exiting", ctx->worker_id);
    return NULL;
}

/**
 * Perform global bootstrap to collect nodes into shared pool
 * Uses wbpxre_dht for proven bootstrap implementation
 * @param sup Supervisor instance
 * @param target_nodes Target number of nodes to collect (e.g., 5000)
 * @param timeout_sec Maximum time to spend bootstrapping
 * @param num_workers Number of worker threads to use (unused, kept for API compatibility)
 * @return 0 on success (reached target or timeout), -1 on error
 */
static int global_bootstrap(supervisor_t *sup, int target_nodes, int timeout_sec, int num_workers) {
    log_msg(LOG_DEBUG, "[global_bootstrap] Starting (target: %d nodes, timeout: %ds, workers: %d)",
            target_nodes, timeout_sec, num_workers);

    time_t start_time = time(NULL);

    /* Create temporary wbpxre_dht instance for bootstrap */
    wbpxre_config_t config = {0};
    config.port = 0;  /* Ephemeral port */
    generate_random_node_id(config.node_id);
    config.max_routing_table_nodes = target_nodes * 3;  /* Allow plenty of space */
    config.triple_routing_threshold = target_nodes;

    /* Configure timeouts for parallel queries */
    config.query_timeout = 5;  /* 5 second timeout per query */

    /* Enable find_node workers for parallel bootstrap queries */
    config.ping_workers = 0;
    config.find_node_workers = num_workers > 0 ? num_workers : 20;  /* Default 20 parallel workers */
    config.sample_infohashes_workers = 0;  /* Not needed during bootstrap */
    config.get_peers_workers = 0;  /* Not needed during bootstrap */

    /* Configure queue capacities for bootstrap */
    config.discovered_nodes_capacity = 10000;
    config.find_node_queue_capacity = 5000;
    config.sample_infohashes_capacity = 100;  /* Small, not used */

    wbpxre_dht_t *bootstrap_dht = wbpxre_dht_init(&config);
    if (!bootstrap_dht) {
        log_msg(LOG_ERROR, "[global_bootstrap] Failed to initialize wbpxre_dht");
        return -1;
    }

    /* Start the DHT (this starts the UDP reader thread needed for synchronous queries) */
    if (wbpxre_dht_start(bootstrap_dht) != 0) {
        log_msg(LOG_ERROR, "[global_bootstrap] Failed to start wbpxre_dht");
        wbpxre_dht_cleanup(bootstrap_dht);
        return -1;
    }

    /* Wait for UDP reader to be ready */
    if (wbpxre_dht_wait_ready(bootstrap_dht, 5) != 0) {
        log_msg(LOG_ERROR, "[global_bootstrap] DHT not ready after 5 seconds");
        wbpxre_dht_stop(bootstrap_dht);
        wbpxre_dht_cleanup(bootstrap_dht);
        return -1;
    }

    log_msg(LOG_DEBUG, "[global_bootstrap] DHT started and ready");

    /* Bootstrap from known routers (synchronous) */
    int bootstrapped = 0;
    for (int i = 0; BOOTSTRAP_NODES[i] != NULL; i++) {
        log_msg(LOG_DEBUG, "[global_bootstrap] Bootstrapping from %s:%d",
                BOOTSTRAP_NODES[i], BOOTSTRAP_PORT);

        int rc = wbpxre_dht_bootstrap(bootstrap_dht, BOOTSTRAP_NODES[i], BOOTSTRAP_PORT);
        if (rc == 0) {
            bootstrapped++;
            log_msg(LOG_DEBUG, "[global_bootstrap] Successfully bootstrapped from %s",
                    BOOTSTRAP_NODES[i]);

            /* Early exit after 2 successful bootstraps */
            if (bootstrapped >= 2) {
                break;
            }
        } else {
            log_msg(LOG_WARN, "[global_bootstrap] Failed to bootstrap from %s",
                    BOOTSTRAP_NODES[i]);
        }
    }

    if (bootstrapped == 0) {
        log_msg(LOG_ERROR, "[global_bootstrap] Failed to bootstrap from any known routers");
        wbpxre_dht_stop(bootstrap_dht);
        wbpxre_dht_cleanup(bootstrap_dht);
        return -1;
    }

    /* Let worker threads discover nodes in parallel
     * The find_node_feeder and find_node_worker threads will automatically
     * query nodes from the routing table and discover new nodes */
    log_msg(LOG_DEBUG, "[global_bootstrap] Worker threads discovering nodes in parallel (workers: %d)",
            config.find_node_workers);

    int last_logged_count = 0;
    int poll_count = 0;

    while (1) {
        /* Check timeout */
        time_t elapsed = time(NULL) - start_time;
        if (elapsed > timeout_sec) {
            int good_nodes = 0, dubious_nodes = 0;
            wbpxre_dht_nodes(bootstrap_dht, &good_nodes, &dubious_nodes);
            log_msg(LOG_WARN, "[global_bootstrap] Timeout reached after %ld seconds (%d nodes discovered)",
                    elapsed, good_nodes);
            break;
        }

        /* Get current node count */
        int good_nodes = 0, dubious_nodes = 0;
        wbpxre_dht_nodes(bootstrap_dht, &good_nodes, &dubious_nodes);

        /* Log progress every 10 polls (2 seconds) */
        if (poll_count % 10 == 0 && good_nodes != last_logged_count) {
            log_msg(LOG_DEBUG, "[global_bootstrap] Progress: %d nodes discovered (%.1f sec elapsed)",
                    good_nodes, (double)elapsed);
            last_logged_count = good_nodes;
        }

        /* Check if we have enough nodes */
        if (good_nodes >= target_nodes) {
            log_msg(LOG_DEBUG, "[global_bootstrap] Reached target: %d nodes in %ld seconds",
                    good_nodes, elapsed);
            break;
        }

        /* Poll every 200ms to check progress */
        usleep(200000);
        poll_count++;
    }

    /* Extract nodes from wbpxre routing table and add to shared pool */
    log_msg(LOG_DEBUG, "[global_bootstrap] Extracting nodes from routing table to shared pool");

    tribuf_controller_t *ctrl = bootstrap_dht->routing_controller;
    wbpxre_routing_table_t *table = tribuf_get_readable_table(ctrl);

    if (!table) {
        table = tribuf_get_filling_table(ctrl);
    }

    int nodes_added = 0;
    if (table) {
        /* Get nodes from the table using get_sample_candidates (works like get_all) */
        wbpxre_routing_node_t *nodes[10000];
        const uint8_t *node_id = tribuf_get_filling_node_id(ctrl);

        if (node_id) {
            int count = wbpxre_routing_table_get_sample_candidates(table, node_id,
                                                                   nodes, 10000);

            log_msg(LOG_DEBUG, "[global_bootstrap] Extracted %d nodes from routing table", count);

            /* Add each node to the shared pool */
            for (int i = 0; i < count; i++) {
                struct sockaddr_storage addr;
                memset(&addr, 0, sizeof(addr));
                struct sockaddr_in *sin = (struct sockaddr_in *)&addr;

                /* Convert wbpxre_node_addr_t (ip string + port) to sockaddr_in */
                sin->sin_family = AF_INET;
                sin->sin_port = htons(nodes[i]->addr.port);

                /* Convert IP string to binary format */
                if (inet_pton(AF_INET, nodes[i]->addr.ip, &sin->sin_addr) != 1) {
                    /* Invalid IP address, skip this node */
                    log_msg(LOG_DEBUG, "[global_bootstrap] Skipping node with invalid IP: %s",
                            nodes[i]->addr.ip);
                    free(nodes[i]);
                    continue;
                }

                int rc = shared_node_pool_add_node(sup->shared_node_pool,
                                                   nodes[i]->id,
                                                   &addr);
                if (rc == 0) {
                    nodes_added++;
                }

                /* Free the node */
                free(nodes[i]);
            }
        }
    }

    size_t final_count = shared_node_pool_get_count(sup->shared_node_pool);
    time_t elapsed = time(NULL) - start_time;

    log_msg(LOG_DEBUG, "[global_bootstrap] Complete: added %d nodes to shared pool (%zu total) in %ld seconds",
            nodes_added, final_count, elapsed);

    /* Stop and cleanup */
    wbpxre_dht_stop(bootstrap_dht);
    wbpxre_dht_cleanup(bootstrap_dht);

    return (final_count >= 1000) ? 0 : -1;  /* Need at least 1000 nodes to be useful */
}

void supervisor_start(supervisor_t *sup) {
    if (!sup) {
        return;
    }

    log_msg(LOG_DEBUG, "[supervisor] Starting with %d trees", sup->max_trees);

    /* Initialize failure bloom filter for two-strike filtering */
    log_msg(LOG_DEBUG, "[supervisor] Initializing failure bloom filter (capacity: %llu, error rate: %.4f)",
            (unsigned long long)sup->failure_bloom_capacity, sup->bloom_error_rate);
    sup->failure_bloom = bloom_filter_init(sup->failure_bloom_capacity, sup->bloom_error_rate);
    if (!sup->failure_bloom) {
        log_msg(LOG_ERROR, "[supervisor] Failed to initialize failure bloom filter");
        return;
    }

    /* Try to load failure bloom filter from disk */
    bloom_filter_t *loaded_failure = bloom_filter_load(sup->failure_bloom_path);
    if (loaded_failure) {
        bloom_filter_cleanup(sup->failure_bloom);
        sup->failure_bloom = loaded_failure;
        log_msg(LOG_DEBUG, "[supervisor] Loaded failure bloom filter from disk");
    } else {
        log_msg(LOG_DEBUG, "[supervisor] Created new failure bloom filter (capacity: %llu, error rate: %.4f)",
                (unsigned long long)sup->failure_bloom_capacity, sup->bloom_error_rate);
    }

    /* Connect failure bloom to batch writer for periodic persistence */
    batch_writer_set_failure_bloom(sup->batch_writer, sup->failure_bloom, sup->failure_bloom_path);

    /* Create BEP51 cache */
    log_msg(LOG_DEBUG, "[supervisor] Creating BEP51 cache (capacity: %d)", sup->bep51_cache_capacity);
    sup->bep51_cache = bep51_cache_create(sup->bep51_cache_capacity);
    if (!sup->bep51_cache) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create BEP51 cache");
        bloom_filter_cleanup(sup->failure_bloom);
        return;
    }

    /* Create shared node pool for global bootstrap */
    sup->shared_node_pool = shared_node_pool_create(sup->global_bootstrap_target);
    if (!sup->shared_node_pool) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create shared node pool");
        bloom_filter_cleanup(sup->failure_bloom);
        bep51_cache_destroy(sup->bep51_cache);
        return;
    }

    /* Try to load cache and populate shared pool */
    log_msg(LOG_DEBUG, "[supervisor] Attempting to load BEP51 cache from %s", sup->bep51_cache_path);
    int cache_loaded = bep51_cache_load_from_file(sup->bep51_cache, sup->bep51_cache_path);

    if (cache_loaded == 0) {
        /* Cache loaded successfully, populate shared_node_pool */
        int populated = bep51_cache_populate_shared_pool(sup->bep51_cache,
                                                          sup->shared_node_pool,
                                                          sup->global_bootstrap_target);
        log_msg(LOG_DEBUG, "[supervisor] Populated %d nodes from cache into shared pool", populated);
    }

    size_t nodes_from_cache = shared_node_pool_get_count(sup->shared_node_pool);
    log_msg(LOG_DEBUG, "[supervisor] Shared pool has %zu nodes from cache", nodes_from_cache);

    /* Only run URL bootstrap if we don't have enough nodes from cache */
    if (nodes_from_cache < 1000) {
        log_msg(LOG_DEBUG, "[supervisor] Insufficient cache nodes (%zu), running URL bootstrap",
                nodes_from_cache);

        int rc = global_bootstrap(sup, sup->global_bootstrap_target,
                                 sup->global_bootstrap_timeout_sec,
                                 sup->global_bootstrap_workers);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[supervisor] Global bootstrap failed");
            /* Continue anyway with whatever nodes we got */
        }
    } else {
        log_msg(LOG_DEBUG, "[supervisor] Using %zu cached nodes, skipping URL bootstrap",
                nodes_from_cache);
    }

    size_t nodes_collected = shared_node_pool_get_count(sup->shared_node_pool);
    log_msg(LOG_DEBUG, "[supervisor] Bootstrap finished: %zu nodes available", nodes_collected);

    /* Create shared socket and dispatcher when using fixed port */
    if (sup->dht_port > 0) {
        sup->shared_socket = tree_socket_create(sup->dht_port);
        if (!sup->shared_socket) {
            log_msg(LOG_ERROR, "[supervisor] Failed to create shared socket on port %d", sup->dht_port);
            bloom_filter_cleanup(sup->failure_bloom);
            bep51_cache_destroy(sup->bep51_cache);
            shared_node_pool_destroy(sup->shared_node_pool);
            return;
        }

        sup->shared_dispatcher = tree_dispatcher_create(NULL, sup->shared_socket);
        if (!sup->shared_dispatcher) {
            log_msg(LOG_ERROR, "[supervisor] Failed to create shared dispatcher");
            tree_socket_destroy(sup->shared_socket);
            sup->shared_socket = NULL;
            bloom_filter_cleanup(sup->failure_bloom);
            bep51_cache_destroy(sup->bep51_cache);
            shared_node_pool_destroy(sup->shared_node_pool);
            return;
        }

        if (tree_dispatcher_start(sup->shared_dispatcher) != 0) {
            log_msg(LOG_ERROR, "[supervisor] Failed to start shared dispatcher");
            tree_dispatcher_destroy(sup->shared_dispatcher);
            tree_socket_destroy(sup->shared_socket);
            sup->shared_socket = NULL;
            sup->shared_dispatcher = NULL;
            bloom_filter_cleanup(sup->failure_bloom);
            bep51_cache_destroy(sup->bep51_cache);
            shared_node_pool_destroy(sup->shared_node_pool);
            return;
        }

        log_msg(LOG_INFO, "[supervisor] Shared socket/dispatcher ready on port %d",
                tree_socket_get_port(sup->shared_socket));
    }

    pthread_mutex_lock(&sup->trees_lock);

    /* Spawn all trees (they will sample from the shared pool) */
    for (int i = 0; i < sup->max_trees; i++) {
        sup->trees[i] = spawn_tree(sup, i, NULL);  /* NULL = first spawn, not a respawn */
        if (sup->trees[i]) {
            thread_tree_start(sup->trees[i]);
            sup->active_trees++;
        }
    }

    pthread_mutex_unlock(&sup->trees_lock);

    /* Start monitor thread */
    sup->monitor_running = 1;
    if (pthread_create(&sup->monitor_thread, NULL, monitor_thread_func, sup) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create monitor thread");
        sup->monitor_running = 0;
    }

    log_msg(LOG_DEBUG, "[supervisor] Started %d trees", sup->active_trees);
}

void supervisor_stop(supervisor_t *sup) {
    if (!sup) {
        return;
    }

    log_msg(LOG_DEBUG, "[supervisor] Stopping");

    /* Stop monitor thread */
    log_msg(LOG_DEBUG, "[supervisor] Stopping monitor thread...");
    sup->monitor_running = 0;

    /* Wait for monitor thread with timeout detection */
    if (sup->monitor_thread) {
        time_t start = time(NULL);
        pthread_join(sup->monitor_thread, NULL);
        sup->monitor_thread = 0;
        time_t elapsed = time(NULL) - start;

        if (elapsed > 2) {
            log_msg(LOG_WARN, "[supervisor] Monitor thread took %ld seconds to exit", elapsed);
        }
    }
    log_msg(LOG_DEBUG, "[supervisor] Monitor thread stopped");

    /* Sleep briefly to ensure monitor thread has fully released all locks
     * This prevents race where monitor thread is still holding trees_lock */
    usleep(50000);  /* 50ms grace period */

    /* Request shutdown on all trees */
    log_msg(LOG_DEBUG, "[supervisor] Requesting shutdown on all trees...");
    pthread_mutex_lock(&sup->trees_lock);
    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            thread_tree_request_shutdown(sup->trees[i], SHUTDOWN_REASON_SUPERVISOR);
        }
    }
    pthread_mutex_unlock(&sup->trees_lock);
    log_msg(LOG_DEBUG, "[supervisor] Shutdown requested on all trees");

    /* Wait for all trees to finish and destroy them */
    pthread_mutex_lock(&sup->trees_lock);
    thread_tree_t *trees_to_destroy[sup->max_trees];
    int num_trees = 0;

    /* Collect trees to destroy (under lock) */
    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            trees_to_destroy[num_trees++] = sup->trees[i];
            sup->trees[i] = NULL;
        }
    }
    sup->active_trees = 0;
    pthread_mutex_unlock(&sup->trees_lock);

    /* Destroy trees WITHOUT holding lock (pthread_join can block) */
    for (int i = 0; i < num_trees; i++) {
        /* Save tree_id before destroying (avoid use-after-free) */
        uint32_t tree_id = trees_to_destroy[i]->tree_id;

        log_msg(LOG_DEBUG, "[supervisor] Destroying tree %u...", tree_id);

        /* Track time to detect hanging threads */
        time_t start = time(NULL);
        thread_tree_destroy(trees_to_destroy[i]);
        time_t elapsed = time(NULL) - start;

        if (elapsed > 2) {
            log_msg(LOG_WARN, "[supervisor] Tree %u took %ld seconds to destroy (possible hang)",
                    tree_id, elapsed);
        }
        log_msg(LOG_DEBUG, "[supervisor] Tree destroyed");
    }

    /* Stop shared dispatcher AFTER all trees are destroyed */
    if (sup->shared_dispatcher) {
        log_msg(LOG_DEBUG, "[supervisor] Stopping shared dispatcher...");
        tree_dispatcher_stop(sup->shared_dispatcher);
        log_msg(LOG_DEBUG, "[supervisor] Shared dispatcher stopped");
    }

    log_msg(LOG_DEBUG, "[supervisor] Stopped");
}

void supervisor_destroy(supervisor_t *sup) {
    if (!sup) {
        return;
    }

    /* Ensure stopped */
    supervisor_stop(sup);

    /* Save failure bloom filter to disk */
    if (sup->failure_bloom && sup->failure_bloom_path) {
        log_msg(LOG_DEBUG, "[supervisor] Saving failure bloom filter to %s", sup->failure_bloom_path);
        if (bloom_filter_save(sup->failure_bloom, sup->failure_bloom_path) == 0) {
            log_msg(LOG_DEBUG, "[supervisor] Failure bloom filter saved successfully");
        } else {
            log_msg(LOG_ERROR, "[supervisor] Failed to save failure bloom filter to %s", sup->failure_bloom_path);
        }
    }

    /* Save BEP51 cache to disk */
    if (sup->bep51_cache) {
        log_msg(LOG_DEBUG, "[supervisor] Saving BEP51 cache to %s", sup->bep51_cache_path);
        if (bep51_cache_save_to_file(sup->bep51_cache, sup->bep51_cache_path) == 0) {
            log_msg(LOG_DEBUG, "[supervisor] BEP51 cache saved successfully");
        } else {
            log_msg(LOG_WARN, "[supervisor] Failed to save BEP51 cache");
        }
        bep51_cache_destroy(sup->bep51_cache);
        sup->bep51_cache = NULL;
    }

    /* Cleanup failure bloom filter */
    if (sup->failure_bloom) {
        bloom_filter_cleanup(sup->failure_bloom);
        sup->failure_bloom = NULL;
    }

    /* Cleanup shared node pool */
    if (sup->shared_node_pool) {
        shared_node_pool_destroy(sup->shared_node_pool);
        sup->shared_node_pool = NULL;
    }

    /* Cleanup shared dispatcher and socket */
    if (sup->shared_dispatcher) {
        tree_dispatcher_destroy(sup->shared_dispatcher);
        sup->shared_dispatcher = NULL;
    }
    if (sup->shared_socket) {
        tree_socket_destroy(sup->shared_socket);
        sup->shared_socket = NULL;
    }

    /* Destroy any remaining draining trees */
    if (sup->draining_trees) {
        pthread_mutex_lock(&sup->draining_lock);
        for (int i = 0; i < sup->draining_count; i++) {
            if (sup->draining_trees[i].tree) {
                log_msg(LOG_WARN, "[supervisor] Destroying draining tree %u (slot %d) during supervisor cleanup",
                        sup->draining_trees[i].tree->tree_id, sup->draining_trees[i].original_slot);
                thread_tree_destroy(sup->draining_trees[i].tree);
            }
        }
        pthread_mutex_unlock(&sup->draining_lock);
        pthread_mutex_destroy(&sup->draining_lock);
        free(sup->draining_trees);
    }

    pthread_mutex_destroy(&sup->trees_lock);
    free(sup->trees);
    free(sup);

    log_msg(LOG_DEBUG, "[supervisor] Destroyed");
}

void supervisor_on_tree_shutdown(thread_tree_t *tree) {
    if (!tree) {
        return;
    }
    /* Just mark for respawn - monitor thread handles destruction asynchronously.
     * This avoids the self-join deadlock that occurred when we tried to destroy
     * the tree (and join bootstrap_thread) from within the bootstrap thread itself. */
    atomic_store(&tree->needs_respawn, true);
}

static void *monitor_thread_func(void *arg) {
    supervisor_t *sup = (supervisor_t *)arg;

    log_msg(LOG_DEBUG, "[supervisor] Monitor thread started");

    while (sup->monitor_running) {
        /* Sleep in small chunks (1 second) to be responsive to shutdown */
        for (int i = 0; i < 10 && sup->monitor_running; i++) {
            struct timespec ts = {1, 0};
            nanosleep(&ts, NULL);
        }

        if (!sup->monitor_running) {
            break;
        }

        /* Check tree performance */
        pthread_mutex_lock(&sup->trees_lock);

        for (int i = 0; i < sup->max_trees; i++) {
            thread_tree_t *tree = sup->trees[i];

            /* Defensive: verify pointer looks valid before dereferencing
             * Detects use-after-free or corrupted pointers */
            if (!tree || (uintptr_t)tree < 0x1000 || (uintptr_t)tree > 0x7fffffffffff) {
                continue;
            }

            /* Further validation: check if tree_id looks reasonable
             * Corrupted pointers often have 0x00000000 or 0xffffffff values */
            uint32_t tree_id = tree->tree_id;
            if (tree_id == 0 || tree_id == 0xffffffff) {
                log_msg(LOG_WARN, "[supervisor] Detected corrupted tree pointer at slot %d (tree_id=%u), cleaning up",
                        i, tree_id);
                sup->trees[i] = NULL;  /* Clean up corruption */
                continue;
            }

            /* Check if tree needs respawn with overlapped spawning */
            if (atomic_load(&tree->needs_respawn)) {
                /* Check THIS specific tree's active connections (not global aggregate) */
                int this_tree_active_conns = atomic_load(&tree->active_connections);

                /* Check if THIS tree is ready to spawn its replacement */
                if (this_tree_active_conns <= sup->respawn_spawn_threshold) {
                    /* Check if draining slots available */
                    pthread_mutex_lock(&sup->draining_lock);
                    bool draining_available = (sup->draining_count < sup->max_draining_trees);
                    pthread_mutex_unlock(&sup->draining_lock);

                    if (!draining_available) {
                        /* Periodically log that we're waiting for draining slot */
                        static time_t last_log_time[256] = {0};  /* Assume max 256 trees */
                        time_t now = time(NULL);
                        if (i < 256 && difftime(now, last_log_time[i]) >= 10) {
                            log_msg(LOG_DEBUG, "[tree %u] Ready for respawn but draining slots full (%d/%d) - waiting",
                                    tree_id, sup->draining_count, sup->max_draining_trees);
                            last_log_time[i] = now;
                        }
                        continue;  /* Wait for draining slot to free up */
                    }

                    log_msg(LOG_INFO, "[tree %u] Respawning (this_tree_active_connections=%d <= threshold=%d)",
                            tree_id, this_tree_active_conns, sup->respawn_spawn_threshold);

                    /* Accumulate statistics from dying tree */
                    uint64_t tree_metadata = atomic_load(&tree->metadata_count);
                    uint64_t tree_filtered = atomic_load(&tree->filtered_count);
                    uint64_t tree_first_strike = atomic_load(&tree->first_strike_failures);
                    uint64_t tree_second_strike = atomic_load(&tree->second_strike_failures);

                    atomic_fetch_add(&sup->cumulative_metadata_count, tree_metadata);
                    atomic_fetch_add(&sup->cumulative_filtered_count, tree_filtered);
                    atomic_fetch_add(&sup->cumulative_first_strike_failures, tree_first_strike);
                    atomic_fetch_add(&sup->cumulative_second_strike_failures, tree_second_strike);

                    log_msg(LOG_DEBUG, "[tree %u] Accumulated stats: metadata=%lu, filtered=%lu, 1st_strike=%lu, 2nd_strike=%lu",
                            tree_id, (unsigned long)tree_metadata, (unsigned long)tree_filtered,
                            (unsigned long)tree_first_strike, (unsigned long)tree_second_strike);

                    /* Spawn replacement tree (with node ID perturbation if keyspace partitioning enabled) */
                    thread_tree_t *new_tree = spawn_tree(sup, i, tree);

                    if (!new_tree) {
                        log_msg(LOG_ERROR, "[tree %u] Failed to spawn replacement tree", tree_id);
                        continue;
                    }

                    /* Move old tree to draining list */
                    if (move_to_draining(sup, i) < 0) {
                        log_msg(LOG_ERROR, "[tree %u] Failed to move to draining, destroying immediately", tree_id);
                        /* Fallback: destroy immediately if can't move to draining */
                        pthread_mutex_unlock(&sup->trees_lock);
                        thread_tree_destroy(tree);
                        pthread_mutex_lock(&sup->trees_lock);
                        sup->trees[i] = NULL;
                        sup->active_trees--;
                    }

                    /* Start replacement tree immediately */
                    sup->trees[i] = new_tree;
                    thread_tree_start(new_tree);
                    sup->active_trees++;

                    log_msg(LOG_INFO, "[tree %u] Replacement tree started (slot=%d, draining_count=%d)",
                            new_tree->tree_id, i, sup->draining_count);
                } else {
                    /* Not ready yet - log periodically */
                    static time_t last_log_time[256] = {0};  /* Assume max 256 trees */
                    time_t now = time(NULL);
                    if (i < 256 && difftime(now, last_log_time[i]) >= 10) {
                        log_msg(LOG_DEBUG, "[tree %u] Waiting for respawn threshold (this_tree_active_connections=%d > %d)",
                                tree_id, this_tree_active_conns, sup->respawn_spawn_threshold);
                        last_log_time[i] = now;
                    }
                }

                continue;  /* Skip normal processing for this slot */
            }

            /* Copy values we need while holding lock to minimize window for corruption */
            tree_phase_t phase = atomic_load(&tree->current_phase);
            uint64_t metadata_count = atomic_load(&tree->metadata_count);

            /* TODO: Implement rate calculation in Stage 2 */
            /* For now, just log status */
            log_msg(LOG_DEBUG, "[supervisor] Tree %u phase=%s metadata=%lu",
                    tree_id,
                    thread_tree_phase_name(phase),
                    (unsigned long)metadata_count);
        }

        pthread_mutex_unlock(&sup->trees_lock);

        /* Monitor and destroy draining trees */
        monitor_draining_trees(sup);
    }

    log_msg(LOG_DEBUG, "[supervisor] Monitor thread exiting");
    return NULL;
}

void supervisor_stats(supervisor_t *sup, int *out_active_trees, uint64_t *out_total_metadata,
                     uint64_t *out_first_strike, uint64_t *out_second_strike) {
    if (!sup) {
        if (out_active_trees) *out_active_trees = 0;
        if (out_total_metadata) *out_total_metadata = 0;
        if (out_first_strike) *out_first_strike = 0;
        if (out_second_strike) *out_second_strike = 0;
        return;
    }

    /* Get cumulative counts from destroyed trees */
    uint64_t cumulative_metadata = atomic_load(&sup->cumulative_metadata_count);
    uint64_t cumulative_first_strike = atomic_load(&sup->cumulative_first_strike_failures);
    uint64_t cumulative_second_strike = atomic_load(&sup->cumulative_second_strike_failures);

    /* Add current active trees' counts */
    uint64_t active_metadata = 0;
    uint64_t active_first_strike = 0;
    uint64_t active_second_strike = 0;

    pthread_mutex_lock(&sup->trees_lock);

    if (out_active_trees) {
        *out_active_trees = sup->active_trees;
    }

    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            active_metadata += atomic_load(&sup->trees[i]->metadata_count);
            active_first_strike += atomic_load(&sup->trees[i]->first_strike_failures);
            active_second_strike += atomic_load(&sup->trees[i]->second_strike_failures);
        }
    }

    pthread_mutex_unlock(&sup->trees_lock);

    /* Return cumulative + active totals */
    if (out_total_metadata) {
        *out_total_metadata = cumulative_metadata + active_metadata;
    }
    if (out_first_strike) {
        *out_first_strike = cumulative_first_strike + active_first_strike;
    }
    if (out_second_strike) {
        *out_second_strike = cumulative_second_strike + active_second_strike;
    }
}

int supervisor_get_total_connections(supervisor_t *sup) {
    if (!sup) {
        return 0;
    }

    int total = 0;

    /* Count active tree connections */
    pthread_mutex_lock(&sup->trees_lock);
    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            total += atomic_load(&sup->trees[i]->active_connections);
        }
    }
    pthread_mutex_unlock(&sup->trees_lock);

    /* Count draining tree connections */
    pthread_mutex_lock(&sup->draining_lock);
    for (int i = 0; i < sup->draining_count; i++) {
        if (sup->draining_trees[i].tree) {
            total += atomic_load(&sup->draining_trees[i].tree->active_connections);
        }
    }
    pthread_mutex_unlock(&sup->draining_lock);

    return total;
}

/**
 * Get draining tree statistics (for debugging/monitoring)
 */
void supervisor_get_draining_stats(supervisor_t *sup, int *count, int *max_count, int *total_connections) {
    if (!sup || !count || !max_count || !total_connections) {
        return;
    }

    *max_count = sup->max_draining_trees;
    *total_connections = 0;

    pthread_mutex_lock(&sup->draining_lock);
    *count = sup->draining_count;
    for (int i = 0; i < sup->draining_count; i++) {
        if (sup->draining_trees[i].tree) {
            *total_connections += atomic_load(&sup->draining_trees[i].tree->active_connections);
        }
    }
    pthread_mutex_unlock(&sup->draining_lock);
}
