#define _DEFAULT_SOURCE
#include "dht_manager.h"
#include "dht_crawler.h"
#include "config.h"
#include "infohash_queue.h"
#include "dht_cache.h"
#include "shadow_routing_table.h"
#include "peer_store.h"
#include "metadata_fetcher.h"
#include "worker_pool.h"
#include "wbpxre_dht.h"
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include <time.h>

/* Bootstrap nodes - reliable DHT routers (Phase 5: Added more fallbacks) */
static const bootstrap_node_t bootstrap_nodes[] = {
    {"router.bittorrent.com", 6881},
    {"dht.transmissionbt.com", 6881},
    {"dht.libtorrent.org", 25401},
    {"dht.aelitis.com", 6881},
    {"router.bitcomet.com", 6881},
    {"dht.anacrolix.link", 42069},
};

#define NUM_BOOTSTRAP_NODES (sizeof(bootstrap_nodes) / sizeof(bootstrap_nodes[0]))

/* Global DHT manager for statistics tracking */
static dht_manager_t *g_dht_manager = NULL;

/* Forward declaration */
static int bootstrap_dht(dht_manager_t *mgr);
static void node_rotation_timer_cb(uv_timer_t *handle);

/* Close callback for timer handles */
static void on_timer_closed(uv_handle_t *handle) {
    dht_manager_t *mgr = (dht_manager_t *)handle->data;
    if (!mgr) return;

    pthread_mutex_lock(&mgr->close_tracker.mutex);
    mgr->close_tracker.handles_to_close--;
    log_msg(LOG_DEBUG, "Timer handle closed, %d handles remaining",
            mgr->close_tracker.handles_to_close);
    pthread_cond_signal(&mgr->close_tracker.cond);
    pthread_mutex_unlock(&mgr->close_tracker.mutex);
}

/* Helper: Format info_hash as hex string */
static void format_infohash(const uint8_t *info_hash, char *out, size_t out_len) {
    if (out_len < 41) return;
    for (int i = 0; i < 20; i++) {
        snprintf(out + i * 2, 3, "%02x", info_hash[i]);
    }
    out[40] = '\0';
}

/* wbpxre-dht callback - called when events occur */
void wbpxre_callback_wrapper(void *closure, wbpxre_event_t event,
                              const uint8_t *info_hash,
                              const void *data, size_t data_len) {
    dht_manager_t *mgr = (dht_manager_t *)closure;
    if (!mgr) {
        return;
    }

    /* SAFETY: If peer_store is NULL or shutdown, skip all operations
     * This prevents DHT worker threads from blocking on peer_store mutex during shutdown */
    if (!mgr->peer_store || mgr->peer_store->shutdown) {
        return;
    }

    switch (event) {
        case WBPXRE_EVENT_VALUES: {
            /* Peers found for an info_hash via get_peers query */
            if (info_hash && data && data_len >= sizeof(wbpxre_peer_t) && mgr->peer_store) {
                /* FIXED: data is wbpxre_peer_t array, not raw bytes */
                const wbpxre_peer_t *peer_array = (const wbpxre_peer_t *)data;
                int num_peers = data_len / sizeof(wbpxre_peer_t);
                int peers_added = 0;
                int valid_peers = 0;  /* NEW: Count valid peers */

                for (int i = 0; i < num_peers; i++) {
                    const wbpxre_peer_t *peer = &peer_array[i];

                    /* Validate peer address */
                    uint32_t ip_addr = ntohl(peer->addr.sin_addr.s_addr);

                    /* Skip invalid/private IPs */
                    if (ip_addr == 0 ||                           /* 0.0.0.0 */
                        (ip_addr & 0xFF000000) == 0x0A000000 ||   /* 10.x.x.x */
                        (ip_addr & 0xFFFF0000) == 0xC0A80000 ||   /* 192.168.x.x */
                        (ip_addr & 0xFFF00000) == 0xAC100000 ||   /* 172.16-31.x.x */
                        (ip_addr & 0xFF000000) == 0x7F000000) {   /* 127.x.x.x */
                        mgr->stats.peers_invalid_ip++;
                        continue;
                    }

                    /* Skip invalid ports */
                    if (peer->port == 0) {
                        mgr->stats.peers_invalid_port++;
                        continue;
                    }

                    valid_peers++;  /* NEW: Count valid peer */

                    /* Add valid peer to peer_store */
                    if (peer_store_add_peer(mgr->peer_store, info_hash,
                                          (struct sockaddr *)&peer->addr,
                                          sizeof(peer->addr)) == 0) {
                        peers_added++;
                        mgr->stats.total_peers_discovered++;
                    }
                }

                /* NEW: Update any pending refresh query */
                if (mgr->refresh_query_store && valid_peers > 0) {
                    refresh_query_add_peers(mgr->refresh_query_store, info_hash, valid_peers);
                }

                /* Track filtered peers */
                mgr->stats.peers_filtered = mgr->stats.peers_invalid_ip + mgr->stats.peers_invalid_port;

                if (peers_added > 0) {
                    mgr->stats.get_peers_responses++;
                    mgr->stats.info_hashes_with_peers++;

                    /* Add to queue for metadata fetching */
                    if (mgr->infohash_queue) {
                        infohash_queue_t *queue = (infohash_queue_t *)mgr->infohash_queue;
                        if (!infohash_queue_is_full(queue)) {
                            infohash_queue_push(queue, info_hash);
                        } else {
                            char hex[41];
                            format_infohash(info_hash, hex, sizeof(hex));
                            log_msg(LOG_WARN, "Infohash queue is full, dropping %s", hex);
                        }
                    }
                }
            }
            break;
        }

        case WBPXRE_EVENT_SEARCH_DONE:
            log_msg(LOG_DEBUG, "DHT event: search done");
            if (mgr->active_search_count > 0) {
                mgr->active_search_count--;
            }

            /* NEW: Mark refresh query as complete */
            if (info_hash && mgr->refresh_query_store) {
                refresh_query_complete(mgr->refresh_query_store, info_hash);
            }

            /* Track peer query completion */
            if (info_hash && mgr->peer_store) {
                int peer_count = peer_store_count_peers(mgr->peer_store, info_hash);
                if (peer_count > 0) {
                    /* Peers were already added to queue in WBPXRE_EVENT_VALUES
                     * Don't add duplicates here - just track statistics */
                    /* Note: info_hashes_with_peers incremented in WBPXRE_EVENT_VALUES */
                } else {
                    mgr->stats.info_hashes_no_peers++;
                }

                if (mgr->active_peer_queries > 0) {
                    mgr->active_peer_queries--;
                }
            }
            break;

        case WBPXRE_EVENT_SAMPLES: {
            /* BEP 51: sample_infohashes response */
            if (data && data_len > 0 && data_len % 20 == 0) {
                int num_samples = data_len / 20;
                const uint8_t *samples = (const uint8_t *)data;
                mgr->stats.bep51_responses_received++;
                mgr->stats.bep51_samples_received += num_samples;
                mgr->samples_since_rotation += num_samples;

                /* Mark node as responded in shadow table if we have node info */
                if (mgr->shadow_table && info_hash) {
                    shadow_table_mark_responded(mgr->shadow_table, info_hash,
                                               num_samples, 60);
                }

                /* Trigger peer queries for all samples */
                for (int i = 0; i < num_samples; i++) {
                    const uint8_t *hash = samples + (i * 20);
                    mgr->stats.infohashes_discovered++;

                    /* Query DHT for peers */
                    dht_manager_query_peers(mgr, hash);
                }
            }
            break;
        }

        case WBPXRE_EVENT_NODE_DISCOVERED: {
            /* New node discovered */
            if (data && data_len >= 26) {
                mgr->stats.nodes_from_queries++;
            }
            break;
        }

        default:
            log_msg(LOG_DEBUG, "DHT event: %d", event);
            break;
    }
}

/* Periodic statistics callback */
static void stats_timer_cb(uv_timer_t *handle) {
    dht_manager_t *mgr = (dht_manager_t *)handle->data;
    static int stats_interval = 0;
    static time_t last_bootstrap_check = 0;
    static time_t last_successful_bootstrap = 0;

    stats_interval++;

    /* Print statistics every 10 seconds */
    if (stats_interval >= 10) {
        dht_manager_print_stats(mgr);
        stats_interval = 0;
    }

    /* Update routing table stats from wbpxre-dht */
    int good = 0, dubious = 0;
    if (mgr->dht) {
        wbpxre_dht_nodes(mgr->dht, &good, &dubious);
        mgr->stats.nodes_in_jech_table = good + dubious;
    }

    /* Phase 6: Bootstrap health monitoring */
    time_t now = time(NULL);

    /* Check if we need to re-bootstrap */
    if (now - last_bootstrap_check >= 60) {  /* Check every 60 seconds */
        last_bootstrap_check = now;

        if (good < 10) {
            /* Critical: very few nodes */
            log_msg(LOG_WARN, "⚠ Bootstrap health check: Only %d nodes in routing table (critical!)",
                   good);
            log_msg(LOG_WARN, "⚠ Attempting automatic re-bootstrap...");

            if (bootstrap_dht(mgr) == 0) {
                log_msg(LOG_DEBUG, "✓ Automatic re-bootstrap successful");
                last_successful_bootstrap = now;
            } else {
                log_msg(LOG_ERROR, "✗ Automatic re-bootstrap failed");
            }
        } else if (good < 50) {
            /* Warning: fewer nodes than desired */
            log_msg(LOG_WARN, "⚠ Bootstrap health check: Only %d nodes in routing table (below target of 50)",
                   good);

            /* Only re-bootstrap if it's been a while since last successful bootstrap */
            if (now - last_successful_bootstrap > 600) {  /* 10 minutes */
                log_msg(LOG_DEBUG, "Attempting re-bootstrap to improve routing table health...");
                if (bootstrap_dht(mgr) == 0) {
                    last_successful_bootstrap = now;
                }
            }
        } else if (last_successful_bootstrap == 0) {
            /* First time we have enough nodes, record it */
            last_successful_bootstrap = now;
        }
    }

    /* Save cache periodically (every 300 seconds) */
    static int cache_save_counter = 0;
    cache_save_counter++;
    if (cache_save_counter >= 300 && mgr->cache.count > 0) {
        dht_cache_save(&mgr->cache);
        cache_save_counter = 0;
    }
}

/* Bootstrap the DHT by connecting to known routers */
static int bootstrap_dht(dht_manager_t *mgr) {
    int bootstrapped = 0;

    if (!mgr || !mgr->dht) {
        return -1;
    }

    log_msg(LOG_DEBUG, "Bootstrapping DHT from %d known routers...", NUM_BOOTSTRAP_NODES);

    /* Phase 3: Track timing for diagnostics */
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    /* Phase 5: Bootstrap from known routers with early exit on success */
    for (size_t i = 0; i < NUM_BOOTSTRAP_NODES; i++) {
        struct timespec node_start, node_end;
        clock_gettime(CLOCK_MONOTONIC, &node_start);

        log_msg(LOG_DEBUG, "Attempting to bootstrap from %s:%d...",
               bootstrap_nodes[i].host, bootstrap_nodes[i].port);

        int rc = wbpxre_dht_bootstrap(mgr->dht,
                                      bootstrap_nodes[i].host,
                                      bootstrap_nodes[i].port);

        clock_gettime(CLOCK_MONOTONIC, &node_end);
        double elapsed = (node_end.tv_sec - node_start.tv_sec) +
                        (node_end.tv_nsec - node_start.tv_nsec) / 1e9;

        if (rc == 0) {
            bootstrapped++;
            log_msg(LOG_DEBUG, "✓ Successfully bootstrapped from %s:%d (%.2fs)",
                   bootstrap_nodes[i].host, bootstrap_nodes[i].port, elapsed);
            mgr->stats.nodes_from_bootstrap++;

            /* Phase 5: Early exit after 2 successful bootstraps (good enough) */
            if (bootstrapped >= 2) {
                log_msg(LOG_DEBUG, "Sufficient bootstrap nodes contacted (%d), continuing...",
                       bootstrapped);
                break;
            }
        } else {
            log_msg(LOG_WARN, "✗ Failed to bootstrap from %s:%d after %.2fs (timeout or DNS failure)",
                   bootstrap_nodes[i].host, bootstrap_nodes[i].port, elapsed);
        }

        /* Small delay between bootstrap attempts to avoid overwhelming the network */
        if (i < NUM_BOOTSTRAP_NODES - 1) {  /* Don't sleep after last attempt */
            usleep(100000);  /* 100ms between attempts */
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double total_elapsed = (end_time.tv_sec - start_time.tv_sec) +
                          (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

    /* Insert cached peers into routing table */
    if (mgr->cache.count > 0) {
        log_msg(LOG_DEBUG, "Inserting %zu cached peers into routing table...", mgr->cache.count);

        for (size_t i = 0; i < mgr->cache.count; i++) {
            dht_cached_peer_t *peer = &mgr->cache.peers[i];
            wbpxre_dht_insert_node(mgr->dht, peer->node_id,
                                   (struct sockaddr_in *)&peer->addr);
            mgr->stats.cached_peers_pinged++;
        }

        log_msg(LOG_DEBUG, "Inserted %zu cached peers", mgr->cache.count);
    }

    /* Phase 3: Detailed bootstrap summary */
    if (bootstrapped > 0) {
        log_msg(LOG_DEBUG, "═══════════════════════════════════════════════════");
        log_msg(LOG_DEBUG, "Bootstrap Summary:");
        log_msg(LOG_DEBUG, "  ✓ Successful: %d/%d routers (%.1f%%)",
               bootstrapped, NUM_BOOTSTRAP_NODES,
               (bootstrapped * 100.0) / NUM_BOOTSTRAP_NODES);
        log_msg(LOG_DEBUG, "  ✓ Total time: %.2fs", total_elapsed);
        log_msg(LOG_DEBUG, "  ✓ Average: %.2fs per node", total_elapsed / NUM_BOOTSTRAP_NODES);

        /* Get current routing table stats */
        int good = 0, dubious = 0;
        wbpxre_dht_nodes(mgr->dht, &good, &dubious);
        log_msg(LOG_DEBUG, "  ✓ Routing table: %d nodes", good);
        log_msg(LOG_DEBUG, "═══════════════════════════════════════════════════");
    } else {
        log_msg(LOG_ERROR, "═══════════════════════════════════════════════════");
        log_msg(LOG_ERROR, "Bootstrap Failed!");
        log_msg(LOG_ERROR, "  ✗ Failed to contact any of %d bootstrap routers", NUM_BOOTSTRAP_NODES);
        log_msg(LOG_ERROR, "  ✗ Total time spent: %.2fs", total_elapsed);

        if (mgr->cache.count == 0) {
            log_msg(LOG_ERROR, "  ✗ No cached peers available");
            log_msg(LOG_ERROR, "  ✗ DHT will not function without bootstrap!");
        } else {
            log_msg(LOG_WARN, "  ⚠ Falling back to %zu cached peers", mgr->cache.count);
        }
        log_msg(LOG_ERROR, "═══════════════════════════════════════════════════");
    }

    return bootstrapped > 0 ? 0 : -1;
}

/* Initialize DHT manager */
int dht_manager_init(dht_manager_t *mgr, app_context_t *app_ctx, void *infohash_queue, void *config) {
    if (!mgr || !app_ctx) {
        return -1;
    }

    crawler_config_t *cfg = (crawler_config_t *)config;

    memset(mgr, 0, sizeof(dht_manager_t));
    mgr->app_ctx = app_ctx;
    mgr->infohash_queue = infohash_queue;
    mgr->crawler_config = config;  /* Store config pointer for rotation */
    mgr->stats.start_time = time(NULL);
    mgr->active_search_count = 0;
    mgr->samples_since_rotation = 0;
    g_dht_manager = mgr;

    /* Initialize close tracker */
    mgr->close_tracker.handles_to_close = 0;
    mgr->timers_initialized = false;  /* Timers not yet initialized */
    if (pthread_mutex_init(&mgr->close_tracker.mutex, NULL) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize close tracker mutex");
        return -1;
    }
    if (pthread_cond_init(&mgr->close_tracker.cond, NULL) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize close tracker condition variable");
        pthread_mutex_destroy(&mgr->close_tracker.mutex);
        return -1;
    }

    /* Initialize configuration with defaults */
    mgr->config.bucket_refresh_interval_sec = 90;
    mgr->config.neighbourhood_refresh_interval_sec = 60;
    mgr->config.wandering_search_interval_sec = 30;
    mgr->config.wandering_searches_per_cycle = 3;
    mgr->config.max_concurrent_searches = 15;
    mgr->config.routing_table_log_interval_sec = 60;
    mgr->config.min_good_nodes_threshold = 50;

    /* BEP 51 configuration */
    mgr->config.bep51_enabled = 1;
    mgr->config.bep51_query_rate = 60;
    mgr->config.bep51_target_rotation_interval = 10;
    mgr->config.bep51_respect_interval = 1;

    /* Peer discovery configuration */
    mgr->config.peer_discovery_enabled = 1;
    mgr->config.max_concurrent_peer_queries = 50;
    mgr->config.peer_query_timeout_sec = 30;
    mgr->active_peer_queries = 0;

    /* Node discovery configuration */
    mgr->config.node_discovery_enabled = 1;
    mgr->config.find_node_workers = 100;
    mgr->config.discovered_queue_capacity = 10000;
    mgr->config.bootstrap_reseed_interval_sec = 120;

    /* Node ID rotation configuration */
    mgr->node_rotation_enabled = 0;  /* Default: disabled */
    mgr->node_rotation_interval_sec = 300;
    mgr->node_rotation_drain_timeout_sec = 10;
    mgr->rotation_state = ROTATION_STATE_STABLE;  /* Initialize rotation state */
    mgr->rotation_phase_start = 0;
    if (cfg) {
        mgr->node_rotation_enabled = cfg->node_rotation_enabled;
        mgr->node_rotation_interval_sec = cfg->node_rotation_interval_sec;
        mgr->node_rotation_drain_timeout_sec = cfg->node_rotation_drain_timeout_sec;
        log_msg(LOG_DEBUG, "Node rotation config: enabled=%d interval=%d drain_timeout=%d",
                mgr->node_rotation_enabled, mgr->node_rotation_interval_sec,
                mgr->node_rotation_drain_timeout_sec);
    } else {
        log_msg(LOG_WARN, "No config provided to dht_manager_init - rotation disabled");
    }

    /* Initialize peer cache */
    if (dht_cache_init(&mgr->cache, DHT_CACHE_FILE) != 0) {
        log_msg(LOG_WARN, "Failed to initialize DHT cache, continuing without cache");
    } else {
        if (dht_cache_load(&mgr->cache) == 0 && mgr->cache.count > 0) {
            log_msg(LOG_DEBUG, "Loaded %zu cached DHT peers from %s",
                   mgr->cache.count, DHT_CACHE_FILE);
        }
    }

    /* Initialize peer store */
    mgr->peer_store = peer_store_init(10000, 100, 3600);
    if (!mgr->peer_store) {
        log_msg(LOG_ERROR, "Failed to create peer store");
        return -1;
    }
    log_msg(LOG_DEBUG, "Peer store created at address: %p", (void*)mgr->peer_store);

    /* Initialize shadow routing table for BEP 51 */
    if (cfg && cfg->shadow_table_enabled) {
        mgr->shadow_table = shadow_table_init(cfg->shadow_table_capacity,
                                               cfg->shadow_table_prune_interval,
                                               app_ctx->loop);
        if (!mgr->shadow_table) {
            log_msg(LOG_WARN, "Failed to create shadow routing table");
        } else {
            log_msg(LOG_DEBUG, "Created shadow routing table (capacity: %d)",
                   cfg->shadow_table_capacity);
        }
    }

    /* Build wbpxre-dht configuration */
    wbpxre_config_t dht_config = {0};
    dht_config.port = app_ctx->dht_port;
    memcpy(dht_config.node_id, app_ctx->node_id, WBPXRE_NODE_ID_LEN);

    /* Worker counts from config */
    if (cfg) {
        dht_config.ping_workers = cfg->wbpxre_ping_workers;
        dht_config.find_node_workers = cfg->wbpxre_find_node_workers;
        dht_config.sample_infohashes_workers = cfg->wbpxre_sample_infohashes_workers;
        dht_config.get_peers_workers = cfg->wbpxre_get_peers_workers;
        dht_config.query_timeout = cfg->wbpxre_query_timeout;
        dht_config.max_routing_table_nodes = cfg->max_routing_table_nodes;
        dht_config.node_verification_batch_size = cfg->node_verification_batch_size;
        dht_config.max_node_age_sec = cfg->max_node_age_sec;
        dht_config.node_cleanup_interval_sec = cfg->node_cleanup_interval_sec;
        dht_config.min_node_response_rate = cfg->min_node_response_rate;
        dht_config.node_quality_min_queries = cfg->node_quality_min_queries;
    } else {
        /* Fallback defaults */
        dht_config.ping_workers = 10;
        dht_config.find_node_workers = 20;
        dht_config.sample_infohashes_workers = 50;
        dht_config.get_peers_workers = 100;
        dht_config.query_timeout = 5;
        dht_config.max_routing_table_nodes = 10000;
        dht_config.node_verification_batch_size = 100;
        dht_config.max_node_age_sec = 120;
        dht_config.node_cleanup_interval_sec = 30;
        dht_config.min_node_response_rate = 0.20;
        dht_config.node_quality_min_queries = 5;
    }

    /* Callback setup */
    dht_config.callback = wbpxre_callback_wrapper;
    dht_config.callback_closure = mgr;

    /* Queue capacities - CRITICAL: Must be set or queues will have capacity 0! */
    dht_config.discovered_nodes_capacity = WBPXRE_DISCOVERED_NODES_CAPACITY;
    dht_config.ping_queue_capacity = WBPXRE_PING_QUEUE_CAPACITY;
    dht_config.find_node_queue_capacity = WBPXRE_FIND_NODE_QUEUE_CAPACITY;
    dht_config.sample_infohashes_capacity = WBPXRE_SAMPLE_INFOHASHES_CAPACITY;

    /* Initialize wbpxre-dht */
    mgr->dht = wbpxre_dht_init(&dht_config);
    if (!mgr->dht) {
        log_msg(LOG_ERROR, "Failed to initialize wbpxre-dht");
        peer_store_cleanup(mgr->peer_store);
        if (mgr->shadow_table) shadow_table_cleanup(mgr->shadow_table);
        return -1;
    }
    log_msg(LOG_DEBUG, "Initialized wbpxre-dht:");
    log_msg(LOG_DEBUG, "  - Port: %d", dht_config.port);
    log_msg(LOG_DEBUG, "  - Ping workers: %d", dht_config.ping_workers);
    log_msg(LOG_DEBUG, "  - Find node workers: %d", dht_config.find_node_workers);
    log_msg(LOG_DEBUG, "  - Sample infohashes workers: %d", dht_config.sample_infohashes_workers);
    log_msg(LOG_DEBUG, "  - Get peers workers: %d", dht_config.get_peers_workers);
    log_msg(LOG_DEBUG, "  - Query timeout: %d seconds", dht_config.query_timeout);
    log_msg(LOG_DEBUG, "  - Node verification batch size: %d", dht_config.node_verification_batch_size);

    /* Start wbpxre-dht (spawns worker threads) */
    int rc = wbpxre_dht_start(mgr->dht);
    if (rc < 0) {
        log_msg(LOG_ERROR, "Failed to start wbpxre-dht");
        wbpxre_dht_cleanup(mgr->dht);
        mgr->dht = NULL;
        peer_store_cleanup(mgr->peer_store);
        if (mgr->shadow_table) shadow_table_cleanup(mgr->shadow_table);
        return -1;
    }
    log_msg(LOG_DEBUG, "Started wbpxre-dht worker threads");

    /* Initialize statistics timer (every 1 second) */
    rc = uv_timer_init(app_ctx->loop, &mgr->bootstrap_reseed_timer);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize stats timer: %s", uv_strerror(rc));
        wbpxre_dht_stop(mgr->dht);
        wbpxre_dht_cleanup(mgr->dht);
        mgr->dht = NULL;
        peer_store_cleanup(mgr->peer_store);
        if (mgr->shadow_table) shadow_table_cleanup(mgr->shadow_table);
        return -1;
    }
    mgr->bootstrap_reseed_timer.data = mgr;
    mgr->timers_initialized = true;  /* Mark timers as initialized */

    /* Initialize refresh query store for HTTP API */
    mgr->refresh_query_store = refresh_query_store_init(1009, 10);  /* 1009 buckets, 10 sec timeout */
    if (!mgr->refresh_query_store) {
        log_msg(LOG_WARN, "Failed to initialize refresh query store");
    } else {
        log_msg(LOG_DEBUG, "Refresh query store initialized");
    }

    log_msg(LOG_DEBUG, "DHT manager initialized successfully");
    return 0;
}

/* Start DHT manager */
int dht_manager_start(dht_manager_t *mgr) {
    if (!mgr || !mgr->dht) {
        return -1;
    }

    /* Phase 1: Wait for UDP reader to be ready */
    log_msg(LOG_DEBUG, "Waiting for UDP reader thread to be ready...");
    if (wbpxre_dht_wait_ready(mgr->dht, 5) != 0) {
        log_msg(LOG_ERROR, "UDP reader thread failed to become ready!");
        return -1;
    }
    log_msg(LOG_DEBUG, "UDP reader thread is ready");

    /* Phase 4: Test socket with warmup */
    if (wbpxre_dht_test_socket(mgr->dht) == 0) {
        log_msg(LOG_DEBUG, "UDP socket test passed");
    } else {
        log_msg(LOG_WARN, "UDP socket test failed (non-critical)");
    }

    /* Phase 2: Bootstrap DHT with retry logic */
    int bootstrap_attempts = 0;
    int max_bootstrap_attempts = 3;
    int bootstrap_success = 0;

    while (bootstrap_attempts < max_bootstrap_attempts && !bootstrap_success) {
        bootstrap_attempts++;

        if (bootstrap_attempts > 1) {
            log_msg(LOG_DEBUG, "Bootstrap attempt %d/%d...",
                   bootstrap_attempts, max_bootstrap_attempts);
            sleep(2);  /* Wait 2 seconds between retry attempts */
        }

        if (bootstrap_dht(mgr) == 0) {
            bootstrap_success = 1;
            log_msg(LOG_DEBUG, "Bootstrap succeeded on attempt %d", bootstrap_attempts);
            break;
        }
    }

    if (!bootstrap_success) {
        /* Check if we have cached peers */
        if (mgr->cache.count > 0) {
            log_msg(LOG_WARN, "All bootstrap attempts failed, but %zu cached peers were loaded",
                   mgr->cache.count);
            log_msg(LOG_WARN, "DHT will attempt to function using cached peers");
        } else {
            log_msg(LOG_ERROR, "Bootstrap failed after %d attempts and no cached peers available!",
                   max_bootstrap_attempts);
            log_msg(LOG_ERROR, "DHT will not function properly without bootstrap nodes");
            return -1;
        }
    }

    /* Start statistics timer (1 second interval) */
    int rc = uv_timer_start(&mgr->bootstrap_reseed_timer, stats_timer_cb, 1000, 1000);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start stats timer: %s", uv_strerror(rc));
        return -1;
    }

    /* Initialize and start node rotation timer if enabled */
    log_msg(LOG_DEBUG, "Checking node rotation: enabled=%d", mgr->node_rotation_enabled);
    if (mgr->node_rotation_enabled) {
        log_msg(LOG_DEBUG, "Initializing node rotation timer...");
        rc = uv_timer_init(mgr->app_ctx->loop, &mgr->node_rotation_timer);
        if (rc != 0) {
            log_msg(LOG_ERROR, "Failed to initialize node rotation timer: %s", uv_strerror(rc));
            return -1;
        }
        mgr->node_rotation_timer.data = mgr;

        /* Start rotation timer (check every 10 seconds) */
        rc = uv_timer_start(&mgr->node_rotation_timer, node_rotation_timer_cb, 10000, 10000);
        if (rc != 0) {
            log_msg(LOG_ERROR, "Failed to start node rotation timer: %s", uv_strerror(rc));
            return -1;
        }

        log_msg(LOG_INFO, "✓ Node ID rotation ENABLED (interval: %d seconds)",
                mgr->node_rotation_interval_sec);
    } else {
        log_msg(LOG_INFO, "✗ Node ID rotation DISABLED");
    }

    log_msg(LOG_DEBUG, "DHT manager started successfully");
    return 0;
}

/* Stop DHT manager */
void dht_manager_stop(dht_manager_t *mgr) {
    if (!mgr) {
        return;
    }

    log_msg(LOG_DEBUG, "Stopping DHT manager...");

    /* Stop statistics timer */
    uv_timer_stop(&mgr->bootstrap_reseed_timer);

    /* Stop node rotation timer if enabled */
    if (mgr->node_rotation_enabled) {
        uv_timer_stop(&mgr->node_rotation_timer);
    }

    /* Step 1: Set peer_store shutdown flag FIRST
     * This causes DHT callbacks to exit early (see wbpxre_callback_wrapper)
     * preventing them from blocking on peer_store mutex */
    if (mgr->peer_store) {
        log_msg(LOG_INFO, "Setting peer_store shutdown flag...");
        peer_store_shutdown(mgr->peer_store);
        log_msg(LOG_INFO, "Peer_store shutdown flag set (DHT callbacks will now skip)");
    }

    /* Step 2: Stop wbpxre-dht (joins all worker threads)
     * Worker threads should exit quickly since callbacks skip when shutdown=true */
    if (mgr->dht) {
        log_msg(LOG_INFO, "Stopping wbpxre-dht (joining worker threads)...");
        wbpxre_dht_stop(mgr->dht);
        log_msg(LOG_INFO, "wbpxre-dht stopped successfully");
    }

    /* Step 3: Do NOT cleanup peer_store here - wait until dht_manager_cleanup
     * after metadata_fetcher_stop has joined all threads */
    log_msg(LOG_DEBUG, "DHT manager stopped (peer_store will be cleaned up later)");

    /* Save cache */
    if (mgr->cache.count > 0) {
        dht_cache_save(&mgr->cache);
        log_msg(LOG_DEBUG, "Saved %zu peers to DHT cache", mgr->cache.count);
    }

    log_msg(LOG_DEBUG, "DHT manager stopped");
}

/* Cleanup DHT manager */
void dht_manager_cleanup(dht_manager_t *mgr) {
    if (!mgr) {
        return;
    }

    log_msg(LOG_DEBUG, "Cleaning up DHT manager...");

    /* Only close timers if they were initialized */
    if (mgr->timers_initialized) {
        /* Set handle count for timer close tracking */
        pthread_mutex_lock(&mgr->close_tracker.mutex);
        mgr->close_tracker.handles_to_close = mgr->node_rotation_enabled ? 2 : 1;
        pthread_mutex_unlock(&mgr->close_tracker.mutex);

        /* Close timer handles with callback - must wait for close callback before freeing */
        log_msg(LOG_DEBUG, "Closing timer handles...");
        uv_close((uv_handle_t *)&mgr->bootstrap_reseed_timer, on_timer_closed);
        if (mgr->node_rotation_enabled) {
            uv_close((uv_handle_t *)&mgr->node_rotation_timer, on_timer_closed);
        }
    }

    /* Run the event loop to process close callbacks (only if timers were initialized) */
    if (mgr->timers_initialized) {
        log_msg(LOG_DEBUG, "Running event loop to process close callbacks...");
        uv_loop_t *loop = mgr->app_ctx->loop;

        pthread_mutex_lock(&mgr->close_tracker.mutex);
        while (mgr->close_tracker.handles_to_close > 0) {
            pthread_mutex_unlock(&mgr->close_tracker.mutex);

            /* Run one iteration of the event loop to process callbacks */
            if (uv_loop_alive(loop)) {
                uv_run(loop, UV_RUN_ONCE);
            } else {
                /* Loop is dead but handle not closed - shouldn't happen but be safe */
                log_msg(LOG_WARN, "Event loop dead but handle not closed, breaking wait");
                break;
            }

            pthread_mutex_lock(&mgr->close_tracker.mutex);
        }
        pthread_mutex_unlock(&mgr->close_tracker.mutex);
        log_msg(LOG_DEBUG, "All timer handles closed");
    }

    /* Cleanup wbpxre-dht (only after timer is fully closed) */
    if (mgr->dht) {
        wbpxre_dht_cleanup(mgr->dht);
        mgr->dht = NULL;
    }

    /* peer_store cleanup moved here from dht_manager_stop()
     * Safe now because metadata_fetcher_stop() has already been called */
    if (mgr->peer_store) {
        log_msg(LOG_DEBUG, "Cleaning up peer_store...");
        peer_store_cleanup(mgr->peer_store);
        mgr->peer_store = NULL;
    }

    /* Cleanup shadow table with NULL check */
    if (mgr->shadow_table) {
        log_msg(LOG_DEBUG, "Cleaning up shadow table...");
        shadow_table_cleanup(mgr->shadow_table);
        mgr->shadow_table = NULL;
    }

    /* Cleanup cache */
    dht_cache_cleanup(&mgr->cache);

    /* Cleanup refresh query store */
    if (mgr->refresh_query_store) {
        log_msg(LOG_DEBUG, "Cleaning up refresh query store...");
        refresh_query_store_cleanup(mgr->refresh_query_store);
        mgr->refresh_query_store = NULL;
    }

    /* Cleanup close tracker synchronization primitives */
    pthread_cond_destroy(&mgr->close_tracker.cond);
    pthread_mutex_destroy(&mgr->close_tracker.mutex);

    log_msg(LOG_DEBUG, "DHT manager cleanup complete");
}

/* Set metadata fetcher reference for statistics */
void dht_manager_set_metadata_fetcher(dht_manager_t *mgr, void *metadata_fetcher) {
    if (!mgr) {
        return;
    }
    mgr->metadata_fetcher = metadata_fetcher;
}

/* Query peers for an info_hash */
int dht_manager_query_peers(dht_manager_t *mgr, const uint8_t *info_hash) {
    if (!mgr || !mgr->dht || !info_hash) {
        return -1;
    }

    /* Queue the info_hash for the wbpxre-dht get_peers pipeline */
    int rc = wbpxre_dht_query_peers(mgr->dht, info_hash);

    if (rc == 0) {
        /* Track the request */
        mgr->stats.get_peers_queries_sent++;
        mgr->active_peer_queries++;
    }

    return rc;
}

/* Rotate node ID to explore different DHT keyspace */
int dht_manager_rotate_node_id(dht_manager_t *mgr) {
    if (!mgr || !mgr->dht) {
        return -1;
    }

    time_t now = time(NULL);

    /* Update statistics */
    if (mgr->stats.last_rotation_time > 0) {
        time_t time_since_rotation = now - mgr->stats.last_rotation_time;
        if (time_since_rotation > 0) {
            mgr->stats.samples_per_rotation = mgr->samples_since_rotation /
                (mgr->stats.node_rotations_performed > 0 ? mgr->stats.node_rotations_performed : 1);
        }
    }

    log_msg(LOG_INFO, "=== Node ID Rotation #%llu ===",
            (unsigned long long)(mgr->stats.node_rotations_performed + 1));
    log_msg(LOG_INFO, "  Samples collected since last rotation: %llu",
            (unsigned long long)mgr->samples_since_rotation);

    /* Generate new random node ID */
    uint8_t new_node_id[20];
    wbpxre_random_bytes(new_node_id, 20);

    char old_id_hex[41], new_id_hex[41];
    format_infohash((const uint8_t *)mgr->app_ctx->node_id, old_id_hex, sizeof(old_id_hex));
    format_infohash(new_node_id, new_id_hex, sizeof(new_id_hex));

    log_msg(LOG_INFO, "  Old node ID: %.16s...", old_id_hex);
    log_msg(LOG_INFO, "  New node ID: %.16s...", new_id_hex);

    /* Get routing table stats before rotation */
    int good_before = 0, dubious_before = 0;
    wbpxre_dht_nodes(mgr->dht, &good_before, &dubious_before);
    log_msg(LOG_INFO, "  Routing table before: %d nodes (%d good, %d dubious)",
            good_before + dubious_before, good_before, dubious_before);

    /* Stop wbpxre-dht (gracefully shutdown workers) */
    log_msg(LOG_DEBUG, "  Stopping DHT workers...");
    wbpxre_dht_stop(mgr->dht);

    /* Cleanup old DHT instance */
    log_msg(LOG_DEBUG, "  Cleaning up old DHT instance...");
    wbpxre_dht_cleanup(mgr->dht);
    mgr->dht = NULL;

    /* Update node ID in app context */
    memcpy(mgr->app_ctx->node_id, new_node_id, 20);

    /* Build new wbpxre-dht configuration with new node ID */
    wbpxre_config_t dht_config = {0};
    dht_config.port = mgr->app_ctx->dht_port;
    memcpy(dht_config.node_id, new_node_id, WBPXRE_NODE_ID_LEN);

    /* Worker counts from existing config */
    crawler_config_t *cfg = (crawler_config_t *)mgr->crawler_config;
    if (cfg) {
        dht_config.ping_workers = cfg->wbpxre_ping_workers;
        dht_config.find_node_workers = cfg->wbpxre_find_node_workers;
        dht_config.sample_infohashes_workers = cfg->wbpxre_sample_infohashes_workers;
        dht_config.get_peers_workers = cfg->wbpxre_get_peers_workers;
        dht_config.query_timeout = cfg->wbpxre_query_timeout;
        dht_config.max_routing_table_nodes = cfg->max_routing_table_nodes;
        dht_config.node_verification_batch_size = cfg->node_verification_batch_size;
        dht_config.max_node_age_sec = cfg->max_node_age_sec;
        dht_config.node_cleanup_interval_sec = cfg->node_cleanup_interval_sec;
        dht_config.min_node_response_rate = cfg->min_node_response_rate;
        dht_config.node_quality_min_queries = cfg->node_quality_min_queries;
    } else {
        /* Fallback defaults */
        dht_config.ping_workers = 10;
        dht_config.find_node_workers = 20;
        dht_config.sample_infohashes_workers = 50;
        dht_config.get_peers_workers = 100;
        dht_config.query_timeout = 5;
        dht_config.max_routing_table_nodes = 10000;
        dht_config.node_verification_batch_size = 100;
        dht_config.max_node_age_sec = 120;
        dht_config.node_cleanup_interval_sec = 30;
        dht_config.min_node_response_rate = 0.20;
        dht_config.node_quality_min_queries = 5;
    }

    /* Callback setup */
    dht_config.callback = wbpxre_callback_wrapper;
    dht_config.callback_closure = mgr;

    /* Queue capacities */
    dht_config.discovered_nodes_capacity = WBPXRE_DISCOVERED_NODES_CAPACITY;
    dht_config.ping_queue_capacity = WBPXRE_PING_QUEUE_CAPACITY;
    dht_config.find_node_queue_capacity = WBPXRE_FIND_NODE_QUEUE_CAPACITY;
    dht_config.sample_infohashes_capacity = WBPXRE_SAMPLE_INFOHASHES_CAPACITY;

    /* Initialize new wbpxre-dht instance */
    log_msg(LOG_DEBUG, "  Initializing new DHT instance...");
    mgr->dht = wbpxre_dht_init(&dht_config);
    if (!mgr->dht) {
        log_msg(LOG_ERROR, "  Failed to initialize new DHT instance!");
        return -1;
    }

    /* Start new DHT instance */
    log_msg(LOG_DEBUG, "  Starting new DHT instance...");
    int rc = wbpxre_dht_start(mgr->dht);
    if (rc < 0) {
        log_msg(LOG_ERROR, "  Failed to start new DHT instance!");
        wbpxre_dht_cleanup(mgr->dht);
        mgr->dht = NULL;
        return -1;
    }

    /* Wait for UDP reader to be ready */
    log_msg(LOG_DEBUG, "  Waiting for UDP reader...");
    if (wbpxre_dht_wait_ready(mgr->dht, 5) != 0) {
        log_msg(LOG_ERROR, "  UDP reader failed to become ready!");
        wbpxre_dht_stop(mgr->dht);
        wbpxre_dht_cleanup(mgr->dht);
        mgr->dht = NULL;
        return -1;
    }

    /* Re-bootstrap DHT with cached peers */
    log_msg(LOG_DEBUG, "  Re-bootstrapping DHT...");
    if (bootstrap_dht(mgr) != 0) {
        log_msg(LOG_WARN, "  Bootstrap failed after rotation (continuing anyway)");
    }

    /* Reset rotation counters */
    mgr->samples_since_rotation = 0;
    mgr->stats.last_rotation_time = now;
    mgr->stats.node_rotations_performed++;

    /* Get routing table stats after rotation */
    int good_after = 0, dubious_after = 0;
    wbpxre_dht_nodes(mgr->dht, &good_after, &dubious_after);
    log_msg(LOG_INFO, "  Routing table after: %d nodes (%d good, %d dubious)",
            good_after + dubious_after, good_after, dubious_after);
    log_msg(LOG_INFO, "=== Node ID Rotation Complete ===");

    return 0;
}

/* Hot rotation - preserves routing table and keeps workers running */
int dht_manager_rotate_node_id_hot(dht_manager_t *mgr) {
    if (!mgr || !mgr->dht) {
        return -1;
    }

    time_t now = time(NULL);

    switch (mgr->rotation_state) {
        case ROTATION_STATE_STABLE:
            /* Generate new ID and enter announcement phase */
            wbpxre_random_bytes(mgr->next_node_id, 20);
            mgr->rotation_state = ROTATION_STATE_ANNOUNCING;
            mgr->rotation_phase_start = now;

            char new_id_hex[41];
            format_infohash(mgr->next_node_id, new_id_hex, sizeof(new_id_hex));
            log_msg(LOG_INFO, "=== Node Rotation: Announcement Phase ===");
            log_msg(LOG_INFO, "  New ID generated: %.16s...", new_id_hex);
            log_msg(LOG_INFO, "  Announcing to network for 30 seconds...");

            /* Schedule transition in 30 seconds (handled by timer callback) */
            return 0;

        case ROTATION_STATE_ANNOUNCING:
            /* Switch to using new ID */
            if (wbpxre_dht_change_node_id(mgr->dht, mgr->next_node_id) != 0) {
                log_msg(LOG_ERROR, "Failed to swap node ID!");
                mgr->rotation_state = ROTATION_STATE_STABLE;
                return -1;
            }

            /* Update app context */
            memcpy(mgr->app_ctx->node_id, mgr->next_node_id, 20);
            mgr->rotation_state = ROTATION_STATE_TRANSITIONING;
            mgr->rotation_phase_start = now;

            /* Get routing table stats */
            int good = 0, dubious = 0;
            wbpxre_dht_nodes(mgr->dht, &good, &dubious);

            log_msg(LOG_INFO, "=== Node Rotation: Transition Phase ===");
            log_msg(LOG_INFO, "  Now using new ID for queries");
            log_msg(LOG_INFO, "  Routing table preserved: %d nodes (%d good, %d dubious)",
                    good + dubious, good, dubious);
            log_msg(LOG_INFO, "  Transitioning for 30 seconds...");

            /* Update statistics */
            if (mgr->stats.last_rotation_time > 0) {
                time_t time_since_rotation = now - mgr->stats.last_rotation_time;
                if (time_since_rotation > 0) {
                    mgr->stats.samples_per_rotation = mgr->samples_since_rotation /
                        (mgr->stats.node_rotations_performed > 0 ? mgr->stats.node_rotations_performed : 1);
                }
            }

            mgr->stats.node_rotations_performed++;
            mgr->samples_since_rotation = 0;
            return 0;

        case ROTATION_STATE_TRANSITIONING: {
            /* Complete transition */
            mgr->rotation_state = ROTATION_STATE_STABLE;
            mgr->stats.last_rotation_time = now;

            log_msg(LOG_INFO, "=== Node Rotation: Complete ===");
            log_msg(LOG_INFO, "  Fully migrated to new keyspace position");
            log_msg(LOG_INFO, "  No downtime - all workers kept running!");

            /* Post-rotation cleanup: Aggressively evict nodes from old keyspace
             * This creates room for new nodes from the current keyspace */
            int post_good = 0, post_dubious = 0;
            wbpxre_dht_nodes(mgr->dht, &post_good, &post_dubious);
            int current_nodes = post_good + post_dubious;

            /* If routing table is >60% full, evict distant nodes to make room */
            if (current_nodes > 24000) {  /* 60% of 40K target */
                log_msg(LOG_INFO, "=== Post-Rotation Cleanup ===");
                log_msg(LOG_INFO, "  Current nodes: %d", current_nodes);
                log_msg(LOG_INFO, "  Triggering aggressive keyspace cleanup...");

                /* Trigger happens automatically in maintenance thread via capacity-based eviction
                 * No explicit action needed here - just log that cleanup will happen */
                log_msg(LOG_INFO, "  Capacity-based eviction will reduce table to ~70%% in next maintenance cycle");
            }

            return 0;
        }
    }

    return 0;
}

/* Timer callback for node ID rotation */
static void node_rotation_timer_cb(uv_timer_t *handle) {
    dht_manager_t *mgr = (dht_manager_t *)handle->data;
    if (!mgr || !mgr->node_rotation_enabled) {
        return;
    }

    time_t now = time(NULL);

    switch (mgr->rotation_state) {
        case ROTATION_STATE_STABLE:
            /* Check if it's time to start rotation */
            if (mgr->stats.last_rotation_time == 0) {
                /* First rotation - initialize timer */
                mgr->stats.last_rotation_time = now;
                return;
            }

            if (now - mgr->stats.last_rotation_time >= mgr->node_rotation_interval_sec) {
                dht_manager_rotate_node_id_hot(mgr);
            }
            break;

        case ROTATION_STATE_ANNOUNCING:
            /* Advance to transition after 30 seconds */
            if (now - mgr->rotation_phase_start >= 30) {
                dht_manager_rotate_node_id_hot(mgr);
            }
            break;

        case ROTATION_STATE_TRANSITIONING:
            /* Complete transition after 30 seconds */
            if (now - mgr->rotation_phase_start >= 30) {
                dht_manager_rotate_node_id_hot(mgr);
            }
            break;
    }
}

/* Print DHT statistics */
void dht_manager_print_stats(dht_manager_t *mgr) {
    if (!mgr) {
        return;
    }

    time_t now = time(NULL);
    time_t uptime = now - mgr->stats.start_time;

    log_msg(LOG_INFO, "=== DHT Statistics (uptime: %ld seconds) ===", uptime);

    /* Get statistics from wbpxre-dht */
    if (mgr->dht) {
        wbpxre_stats_t wbpxre_stats;
        if (wbpxre_dht_get_stats(mgr->dht, &wbpxre_stats) == 0) {
            log_msg(LOG_INFO, "  Packets: sent=%llu received=%llu",
                    (unsigned long long)wbpxre_stats.packets_sent,
                    (unsigned long long)wbpxre_stats.packets_received);
            log_msg(LOG_INFO, "  Queries: sent=%llu",
                    (unsigned long long)wbpxre_stats.queries_sent);
            log_msg(LOG_INFO, "  Info hashes: discovered=%llu",
                    (unsigned long long)wbpxre_stats.infohashes_discovered);
            log_msg(LOG_INFO, "  BEP 51: queries=%llu responses=%llu samples=%llu",
                    (unsigned long long)wbpxre_stats.bep51_queries_sent,
                    (unsigned long long)wbpxre_stats.bep51_responses_received,
                    (unsigned long long)wbpxre_stats.bep51_samples_received);
            log_msg(LOG_INFO, "  Peers: discovered=%llu filtered=%llu (invalid_ip=%llu invalid_port=%llu)",
                    (unsigned long long)mgr->stats.total_peers_discovered,
                    (unsigned long long)mgr->stats.peers_filtered,
                    (unsigned long long)mgr->stats.peers_invalid_ip,
                    (unsigned long long)mgr->stats.peers_invalid_port);

            /* Get current routing table stats from wbpxre-dht */
            int good = 0, dubious = 0;
            wbpxre_dht_nodes(mgr->dht, &good, &dubious);
            log_msg(LOG_INFO, "  Routing table: nodes=%d",
                    good + dubious);
            log_msg(LOG_INFO, "  wbpxre-dht: good=%d dubious=%d total=%d",
                    good, dubious, good + dubious);

            /* Calculate and display nodes dropped in last interval */
            uint64_t current_dropped = wbpxre_stats.nodes_dropped;
            uint64_t dropped_delta = current_dropped - mgr->stats.last_nodes_dropped;
            log_msg(LOG_INFO, "  Nodes dropped: last_10s=%llu total=%llu",
                    (unsigned long long)dropped_delta,
                    (unsigned long long)current_dropped);
            mgr->stats.last_nodes_dropped = current_dropped;
        }
    }

    /* Print infohash queue statistics */
    if (mgr->infohash_queue) {
        infohash_queue_t *queue = (infohash_queue_t *)mgr->infohash_queue;
        size_t queue_size = infohash_queue_size(queue);
        size_t queue_capacity = infohash_queue_capacity(queue);
        int is_full = infohash_queue_is_full(queue);
        uint64_t duplicates_filtered = infohash_queue_get_duplicates(queue);

        log_msg(LOG_INFO, "  Infohash queue: size=%zu capacity=%zu full=%s duplicates_filtered=%llu",
                queue_size, queue_capacity, is_full ? "YES" : "no",
                (unsigned long long)duplicates_filtered);
    }

    /* Print node rotation statistics if enabled */
    if (mgr->node_rotation_enabled) {
        /* Get routing table node counts */
        int good = 0, dubious = 0;
        if (mgr->dht) {
            wbpxre_dht_nodes(mgr->dht, &good, &dubious);
        }

        /* Rotation state */
        const char *state_str =
            mgr->rotation_state == ROTATION_STATE_STABLE ? "STABLE" :
            mgr->rotation_state == ROTATION_STATE_ANNOUNCING ? "ANNOUNCING" :
            "TRANSITIONING";

        log_msg(LOG_INFO, "  Node ID rotation: HOT mode (state: %s)", state_str);
        log_msg(LOG_INFO, "    Current nodes: %d (no downtime!)", good);

        if (mgr->stats.node_rotations_performed > 0) {
            log_msg(LOG_INFO, "    Rotations: %llu (preserves routing table)",
                    (unsigned long long)mgr->stats.node_rotations_performed);
            log_msg(LOG_INFO, "    Samples: this_period=%llu avg=%llu",
                    (unsigned long long)mgr->samples_since_rotation,
                    (unsigned long long)mgr->stats.samples_per_rotation);
            if (mgr->stats.last_rotation_time > 0) {
                time_t time_since = now - mgr->stats.last_rotation_time;
                time_t time_until = mgr->node_rotation_interval_sec - time_since;
                if (time_until < 0) time_until = 0;
                log_msg(LOG_INFO, "    Last rotation: %ld seconds ago, next in: %ld seconds",
                        time_since, time_until);
            }
        } else {
            /* Before first rotation - show countdown */
            time_t time_until_first = mgr->node_rotation_interval_sec - (now - mgr->stats.start_time);
            if (time_until_first < 0) time_until_first = 0;
            log_msg(LOG_INFO, "    ENABLED (interval=%ds) - first rotation in %ld seconds",
                    mgr->node_rotation_interval_sec, time_until_first);
            log_msg(LOG_INFO, "    Samples: %llu",
                    (unsigned long long)mgr->samples_since_rotation);
        }
    }

    /* Print metadata fetcher statistics */
    if (mgr->metadata_fetcher) {
        metadata_fetcher_t *fetcher = (metadata_fetcher_t *)mgr->metadata_fetcher;

        /* Calculate total failures from detailed breakdown */
        uint64_t total_failed = fetcher->connection_failed +
                                fetcher->connection_timeout +
                                fetcher->handshake_failed +
                                fetcher->no_metadata_support +
                                fetcher->metadata_rejected +
                                fetcher->hash_mismatch;

        log_msg(LOG_INFO, "  Metadata: fetched=%llu failed=%llu (success_rate=%.1f%%)",
                (unsigned long long)fetcher->total_fetched,
                (unsigned long long)total_failed,
                fetcher->total_attempts > 0 ?
                    (100.0 * fetcher->total_fetched / fetcher->total_attempts) : 0.0);

        log_msg(LOG_INFO, "    Attempts: total=%llu no_peers=%llu connections=%llu",
                (unsigned long long)fetcher->total_attempts,
                (unsigned long long)fetcher->no_peers_found,
                (unsigned long long)fetcher->connection_initiated);

        log_msg(LOG_INFO, "    Failures: conn_failed=%llu timeout=%llu handshake=%llu no_support=%llu rejected=%llu hash_mismatch=%llu",
                (unsigned long long)fetcher->connection_failed,
                (unsigned long long)fetcher->connection_timeout,
                (unsigned long long)fetcher->handshake_failed,
                (unsigned long long)fetcher->no_metadata_support,
                (unsigned long long)fetcher->metadata_rejected,
                (unsigned long long)fetcher->hash_mismatch);

        /* Print retry queue statistics only if retry queue is enabled */
        if (fetcher->retry_queue) {
            log_msg(LOG_INFO, "    Retry queue: added=%llu attempts=%llu submitted=%llu abandoned=%llu current_size=%zu",
                    (unsigned long long)fetcher->retry_queue_added,
                    (unsigned long long)fetcher->retry_attempts,
                    (unsigned long long)fetcher->retry_submitted,
                    (unsigned long long)fetcher->retry_abandoned,
                    (unsigned long)fetcher->retry_queue->count);

            /* Print retry queue timing details if there are pending retries */
            if (fetcher->retry_queue->count > 0) {
                time_t now = time(NULL);
                time_t next_retry = 0;

                uv_mutex_lock(&fetcher->retry_queue->mutex);
                if (fetcher->retry_queue->head) {
                    next_retry = fetcher->retry_queue->head->next_retry_time;
                }
                uv_mutex_unlock(&fetcher->retry_queue->mutex);

                log_msg(LOG_INFO, "      Pending retries: %zu (next retry in: %ld seconds)",
                        fetcher->retry_queue->count,
                        next_retry > now ? (next_retry - now) : 0);
            }
        } else {
            log_msg(LOG_INFO, "    Retry queue: DISABLED (all failures discarded immediately)");
        }

        log_msg(LOG_INFO, "    Active: connections=%d max_per_infohash=%d max_global=%d",
                fetcher->active_count,
                fetcher->max_concurrent_per_infohash,
                fetcher->max_global_connections);

        /* Print worker pool statistics */
        if (fetcher->worker_pool) {
            size_t active_workers = 0, idle_workers = 0, queue_size = 0;
            uint64_t tasks_processed = 0;
            worker_pool_stats(fetcher->worker_pool, NULL, &queue_size, NULL, &tasks_processed,
                            &active_workers, &idle_workers);
            double utilization = fetcher->num_workers > 0 ?
                (100.0 * active_workers / fetcher->num_workers) : 0.0;
            log_msg(LOG_INFO, "    Worker pool: active=%zu idle=%zu queue=%zu utilization=%.1f%% tasks_processed=%llu",
                    active_workers, idle_workers, queue_size, utilization,
                    (unsigned long long)tasks_processed);
        }
    }
}
