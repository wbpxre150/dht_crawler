#define _DEFAULT_SOURCE
#include "dht_manager.h"
#include "dht_crawler.h"
#include "config.h"
#include "infohash_queue.h"
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
#include <urcu.h>

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

/* Forward declarations */
static int bootstrap_dht(dht_manager_t *mgr);
static void peer_retry_timer_cb(uv_timer_t *handle);
static void dual_routing_timer_cb(uv_timer_t *handle);

/* Structure to track pending timer closes */
typedef struct {
    int pending_closes;
    pthread_mutex_t mutex;
} timer_close_tracker_t;

/* Close callback for timers - decrements pending counter */
static void timer_close_cb(uv_handle_t *handle) {
    timer_close_tracker_t *tracker = (timer_close_tracker_t *)handle->data;
    if (tracker) {
        pthread_mutex_lock(&tracker->mutex);
        tracker->pending_closes--;
        log_msg(LOG_DEBUG, "Timer closed, pending: %d", tracker->pending_closes);
        pthread_mutex_unlock(&tracker->mutex);
    }
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

                    /* Hybrid approach: Allow immediate queue entry if we have enough peers
                     * This fixes queue starvation while maintaining the 10-peer quality threshold */

                    /* Get current total peer count from peer_store */
                    int total_peer_count = peer_store_count_peers(mgr->peer_store, info_hash);

                    /* Check if in retry tracking */
                    bool in_retry = false;
                    if (mgr->peer_retry_tracker) {
                        peer_retry_entry_t *entry = peer_retry_entry_find(mgr->peer_retry_tracker, info_hash);
                        in_retry = (entry != NULL);
                    }

                    /* Decision logic (FIXED):
                     * - If we have >= 10 peers: proceed to metadata fetcher immediately
                     * - If we have < 10 peers: WAIT for retry system to collect more peers via SEARCH_DONE
                     *
                     * This fixes the bug where info_hashes were queued with only 1 peer, causing
                     * low metadata fetch success rates (4% instead of expected 25-40%).
                     */
                    bool should_queue = false;

                    if (total_peer_count >= 10) {
                        /* Threshold met - proceed immediately */
                        should_queue = true;

                        /* If in retry system, mark as complete since we have enough peers */
                        if (in_retry && mgr->peer_retry_tracker) {
                            peer_retry_mark_complete(mgr->peer_retry_tracker, info_hash, total_peer_count);
                        }
                    }
                    /* else: < 10 peers - let SEARCH_DONE event trigger retry system to collect more */

                    if (should_queue && mgr->infohash_queue) {
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
            if (mgr->active_search_count > 0) {
                mgr->active_search_count--;
            }

            /* Mark refresh query as complete */
            if (info_hash && mgr->refresh_query_store) {
                refresh_query_complete(mgr->refresh_query_store, info_hash);
            }

            /* Check if we should retry for more peers */
            if (info_hash && mgr->peer_store && mgr->peer_retry_tracker) {
                int peer_count = peer_store_count_peers(mgr->peer_store, info_hash);

                /* Clear query_in_progress flag first - the query is done regardless of whether we retry */
                peer_retry_entry_t *entry = peer_retry_entry_find(mgr->peer_retry_tracker, info_hash);
                if (entry) {
                    pthread_mutex_lock(&mgr->peer_retry_tracker->mutex);
                    entry->query_in_progress = 0;
                    entry->peer_count = peer_count;
                    pthread_mutex_unlock(&mgr->peer_retry_tracker->mutex);
                }

                /* Check if retry is needed */
                if (peer_retry_should_retry(mgr->peer_retry_tracker, info_hash, peer_count)) {
                    /* Schedule retry after delay */
                    if (entry) {
                        pthread_mutex_lock(&mgr->peer_retry_tracker->mutex);
                        entry->attempts_made++;
                        entry->last_attempt_time = time(NULL);
                        mgr->peer_retry_tracker->retries_triggered++;
                        pthread_mutex_unlock(&mgr->peer_retry_tracker->mutex);

                        mgr->stats.peer_retries_triggered++;

                        /* Schedule retry by setting target_retry_time */
                        time_t target_time = time(NULL) + (mgr->peer_retry_tracker->retry_delay_ms / 1000);
                        entry->target_retry_time = target_time;
                    }
                } else {
                    /* No retry needed - add to queue for metadata fetching */
                    if (peer_count > 0 && mgr->infohash_queue) {
                        /* FIXED: Push to queue immediately after retry completion
                         * This ensures infohashes with peers reach the metadata fetcher */
                        infohash_queue_t *queue = (infohash_queue_t *)mgr->infohash_queue;
                        if (!infohash_queue_is_full(queue)) {
                            infohash_queue_push(queue, info_hash);
                        } else {
                            char hex[41];
                            format_infohash(info_hash, hex, sizeof(hex));
                            log_msg(LOG_WARN, "Infohash queue is full after retry completion, dropping %s", hex);
                        }
                    } else if (peer_count == 0) {
                        mgr->stats.info_hashes_no_peers++;
                    }

                    /* Mark as complete and remove from tracker */
                    peer_retry_mark_complete(mgr->peer_retry_tracker, info_hash, peer_count);
                }

                if (mgr->active_peer_queries > 0) {
                    mgr->active_peer_queries--;
                }
            }
            /* Fallback for when retry tracker is disabled */
            else if (info_hash && mgr->peer_store) {
                int peer_count = peer_store_count_peers(mgr->peer_store, info_hash);

                if (peer_count > 0 && mgr->infohash_queue) {
                    /* Push to queue for metadata fetching */
                    infohash_queue_t *queue = (infohash_queue_t *)mgr->infohash_queue;
                    if (!infohash_queue_is_full(queue)) {
                        infohash_queue_push(queue, info_hash);
                    } else {
                        char hex[41];
                        format_infohash(info_hash, hex, sizeof(hex));
                        log_msg(LOG_WARN, "Infohash queue is full, dropping %s", hex);
                    }
                } else if (peer_count == 0) {
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

                /* Trigger peer queries for all samples */
                for (int i = 0; i < num_samples; i++) {
                    const uint8_t *hash = samples + (i * 20);
                    mgr->stats.infohashes_discovered++;

                    /* Create retry tracker entry if enabled */
                    if (mgr->peer_retry_tracker) {
                        peer_retry_entry_create(mgr->peer_retry_tracker, hash);
                    }

                    /* Query DHT for peers (normal priority for automatic discovery) */
                    dht_manager_query_peers(mgr, hash, false);
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

    /* Peer store cleanup (every 300 seconds / 5 minutes) */
    static int cleanup_counter = 0;
    cleanup_counter++;  /* Timer fires every 1 second */

    if (cleanup_counter >= 300) {  /* Every 5 minutes */
        if (mgr->peer_store) {
            log_msg(LOG_DEBUG, "Running periodic peer store cleanup");
            int expired = peer_store_cleanup_expired(mgr->peer_store);
            if (expired > 0) {
                log_msg(LOG_DEBUG, "Peer store cleanup: %d expired peers removed", expired);
            }
        }
        cleanup_counter = 0;
    }

    /* Peer retry tracker cleanup (configurable interval) */
    static int peer_retry_cleanup_counter = 0;
    peer_retry_cleanup_counter++;  /* Timer fires every 1 second */

    if (mgr->peer_retry_tracker) {
        /* Default cleanup interval: 30 seconds */
        const int cleanup_interval_sec = 30;
        if (peer_retry_cleanup_counter >= cleanup_interval_sec) {
            int removed = peer_retry_cleanup_old(mgr->peer_retry_tracker);
            if (removed > 0) {
                log_msg(LOG_DEBUG, "Peer retry cleanup: %d stale entries removed", removed);
            }
            peer_retry_cleanup_counter = 0;
        }
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
        log_msg(LOG_ERROR, "  ✗ DHT will not function without bootstrap!");
        log_msg(LOG_ERROR, "═══════════════════════════════════════════════════");
    }

    return bootstrapped > 0 ? 0 : -1;
}

/* Initialize DHT manager */
int dht_manager_init(dht_manager_t *mgr, app_context_t *app_ctx, void *infohash_queue, void *config) {
    if (!mgr || !app_ctx) {
        return -1;
    }

    /* Register main thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    crawler_config_t *cfg = (crawler_config_t *)config;

    memset(mgr, 0, sizeof(dht_manager_t));
    mgr->app_ctx = app_ctx;
    mgr->infohash_queue = infohash_queue;
    mgr->stats.start_time = time(NULL);
    mgr->active_search_count = 0;
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

    /* Initialize peer store */
    mgr->peer_store = peer_store_init(1000, 500, 3600);
    if (!mgr->peer_store) {
        log_msg(LOG_ERROR, "Failed to create peer store");
        return -1;
    }
    log_msg(LOG_DEBUG, "Peer store created at address: %p", (void*)mgr->peer_store);

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
        dht_config.triple_routing_threshold = cfg->triple_routing_threshold;
    } else {
        /* Fallback defaults */
        dht_config.ping_workers = 10;
        dht_config.find_node_workers = 20;
        dht_config.sample_infohashes_workers = 50;
        dht_config.get_peers_workers = 100;
        dht_config.query_timeout = 5;
        dht_config.max_routing_table_nodes = 10000;
        dht_config.triple_routing_threshold = 1500;
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
        return -1;
    }
    log_msg(LOG_DEBUG, "Initialized wbpxre-dht:");
    log_msg(LOG_DEBUG, "  - Port: %d", dht_config.port);
    log_msg(LOG_DEBUG, "  - Ping workers: %d", dht_config.ping_workers);
    log_msg(LOG_DEBUG, "  - Find node workers: %d", dht_config.find_node_workers);
    log_msg(LOG_DEBUG, "  - Sample infohashes workers: %d", dht_config.sample_infohashes_workers);
    log_msg(LOG_DEBUG, "  - Get peers workers: %d", dht_config.get_peers_workers);
    log_msg(LOG_DEBUG, "  - Query timeout: %d seconds", dht_config.query_timeout);
    log_msg(LOG_DEBUG, "  - Triple routing threshold: %u", dht_config.triple_routing_threshold);

    /* Start wbpxre-dht (spawns worker threads) */
    int rc = wbpxre_dht_start(mgr->dht);
    if (rc < 0) {
        log_msg(LOG_ERROR, "Failed to start wbpxre-dht");
        wbpxre_dht_cleanup(mgr->dht);
        mgr->dht = NULL;
        peer_store_cleanup(mgr->peer_store);
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

    /* Initialize peer retry tracker */
    if (cfg && cfg->peer_retry_enabled) {
        /* Use bucket count ~2x max_entries for O(1) average lookup (load factor 0.5) */
        size_t bucket_count = (cfg->peer_retry_max_entries * 2) | 1;  /* Ensure odd for better distribution */
        mgr->peer_retry_tracker = peer_retry_tracker_init(
            bucket_count,  /* hash buckets sized for O(1) lookup */
            cfg->peer_retry_max_entries,  /* max entries in circular buffer */
            cfg->peer_retry_max_attempts,
            cfg->peer_retry_min_threshold,
            cfg->peer_retry_delay_ms,
            60  /* max age 60s */
        );
        if (!mgr->peer_retry_tracker) {
            log_msg(LOG_WARN, "Failed to initialize peer retry tracker");
        } else {
            log_msg(LOG_DEBUG, "Peer retry tracker initialized (max_entries=%d, max_attempts=%d, threshold=%d, delay=%dms)",
                    cfg->peer_retry_max_entries, cfg->peer_retry_max_attempts, cfg->peer_retry_min_threshold, cfg->peer_retry_delay_ms);

            /* Initialize peer retry timer (checks every 100ms for ready retries) */
            rc = uv_timer_init(app_ctx->loop, &mgr->peer_retry_timer);
            if (rc != 0) {
                log_msg(LOG_ERROR, "Failed to initialize peer retry timer: %s", uv_strerror(rc));
                peer_retry_tracker_cleanup(mgr->peer_retry_tracker);
                mgr->peer_retry_tracker = NULL;
            } else {
                mgr->peer_retry_timer.data = mgr;
                log_msg(LOG_DEBUG, "Peer retry timer initialized");
            }
        }
    } else {
        mgr->peer_retry_tracker = NULL;
        log_msg(LOG_DEBUG, "Peer retry disabled");
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
        log_msg(LOG_ERROR, "Bootstrap failed after %d attempts!",
               max_bootstrap_attempts);
        log_msg(LOG_ERROR, "DHT will not function properly without bootstrap nodes");
        return -1;
    }

    /* Start statistics timer (1 second interval) */
    int rc = uv_timer_start(&mgr->bootstrap_reseed_timer, stats_timer_cb, 1000, 1000);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start stats timer: %s", uv_strerror(rc));
        return -1;
    }

    /* Start peer retry timer if enabled */
    if (mgr->peer_retry_tracker) {
        rc = uv_timer_start(&mgr->peer_retry_timer, peer_retry_timer_cb, 100, 100);
        if (rc != 0) {
            log_msg(LOG_ERROR, "Failed to start peer retry timer: %s", uv_strerror(rc));
            return -1;
        }
        log_msg(LOG_DEBUG, "Peer retry timer started (checks every 100ms)");
    }

    /* Initialize and start dual routing check timer (every 5 seconds) */
    log_msg(LOG_DEBUG, "Initializing dual routing check timer...");
    rc = uv_timer_init(mgr->app_ctx->loop, &mgr->dual_routing_timer);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize dual routing timer: %s", uv_strerror(rc));
        return -1;
    }
    mgr->dual_routing_timer.data = mgr;

    /* Start timer (check every 5 seconds) */
    rc = uv_timer_start(&mgr->dual_routing_timer, dual_routing_timer_cb, 5000, 5000);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start dual routing timer: %s", uv_strerror(rc));
        return -1;
    }
    log_msg(LOG_DEBUG, "Dual routing timer started (checks every 5 seconds)");

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

    /* Stop peer retry timer if enabled */
    if (mgr->peer_retry_tracker) {
        uv_timer_stop(&mgr->peer_retry_timer);
    }

    /* Stop dual routing timer */
    uv_timer_stop(&mgr->dual_routing_timer);
    log_msg(LOG_DEBUG, "Dual routing timer stopped");

    /* Step 1: Set peer_store shutdown flag FIRST
     * This causes DHT callbacks to exit early (see wbpxre_callback_wrapper)
     * preventing them from blocking on peer_store mutex */
    if (mgr->peer_store) {
        peer_store_shutdown(mgr->peer_store);
    }

    /* Step 2: Stop wbpxre-dht (joins all worker threads)
     * Worker threads should exit quickly since callbacks skip when shutdown=true */
    if (mgr->dht) {
        wbpxre_dht_stop(mgr->dht);
    }

    /* Step 3: Do NOT cleanup peer_store here - wait until dht_manager_cleanup
     * after metadata_fetcher_stop has joined all threads */
    log_msg(LOG_DEBUG, "DHT manager stopped (peer_store will be cleaned up later)");

    log_msg(LOG_DEBUG, "DHT manager stopped");
}

/* Cleanup DHT manager */
void dht_manager_cleanup(dht_manager_t *mgr) {
    if (!mgr) {
        return;
    }

    log_msg(LOG_DEBUG, "Cleaning up DHT manager...");

    /* First, explicitly close only the timers we own */
    if (mgr->timers_initialized && mgr->app_ctx && mgr->app_ctx->loop) {
        log_msg(LOG_DEBUG, "Closing DHT manager timers...");
        uv_loop_t *loop = mgr->app_ctx->loop;

        /* Initialize close tracker */
        timer_close_tracker_t close_tracker;
        close_tracker.pending_closes = 0;
        pthread_mutex_init(&close_tracker.mutex, NULL);

        /* Close bootstrap_reseed_timer (always initialized) */
        if (!uv_is_closing((uv_handle_t *)&mgr->bootstrap_reseed_timer)) {
            mgr->bootstrap_reseed_timer.data = &close_tracker;
            close_tracker.pending_closes++;
            uv_close((uv_handle_t *)&mgr->bootstrap_reseed_timer, timer_close_cb);
            log_msg(LOG_DEBUG, "Closing bootstrap_reseed_timer");
        }

        /* Close peer_retry_timer (only if peer retry was enabled) */
        if (mgr->peer_retry_tracker && !uv_is_closing((uv_handle_t *)&mgr->peer_retry_timer)) {
            mgr->peer_retry_timer.data = &close_tracker;
            close_tracker.pending_closes++;
            uv_close((uv_handle_t *)&mgr->peer_retry_timer, timer_close_cb);
            log_msg(LOG_DEBUG, "Closing peer_retry_timer");
        }

        /* Close dual_routing_timer */
        if (!uv_is_closing((uv_handle_t *)&mgr->dual_routing_timer)) {
            mgr->dual_routing_timer.data = &close_tracker;
            close_tracker.pending_closes++;
            uv_close((uv_handle_t *)&mgr->dual_routing_timer, timer_close_cb);
            log_msg(LOG_DEBUG, "Closing dual_routing_timer");
        }

        /* Wait for all timer closes to complete */
        log_msg(LOG_DEBUG, "Waiting for %d timer(s) to close...", close_tracker.pending_closes);
        int iterations = 0;
        while (close_tracker.pending_closes > 0 && iterations < 20) {
            uv_run(loop, UV_RUN_NOWAIT);
            iterations++;
            if (iterations % 5 == 0) {
                pthread_mutex_lock(&close_tracker.mutex);
                int pending = close_tracker.pending_closes;
                pthread_mutex_unlock(&close_tracker.mutex);
                log_msg(LOG_DEBUG, "Still waiting for %d timer(s) to close (iteration %d)",
                        pending, iterations);
            }
        }

        pthread_mutex_lock(&close_tracker.mutex);
        int final_pending = close_tracker.pending_closes;
        pthread_mutex_unlock(&close_tracker.mutex);

        if (final_pending == 0) {
            log_msg(LOG_DEBUG, "All timers closed successfully");
        } else {
            log_msg(LOG_WARN, "Timeout waiting for timers to close (%d still pending)",
                    final_pending);
        }

        pthread_mutex_destroy(&close_tracker.mutex);
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

    /* Cleanup refresh query store */
    if (mgr->refresh_query_store) {
        log_msg(LOG_DEBUG, "Cleaning up refresh query store...");
        refresh_query_store_cleanup(mgr->refresh_query_store);
        mgr->refresh_query_store = NULL;
    }

    /* Cleanup peer retry tracker */
    if (mgr->peer_retry_tracker) {
        log_msg(LOG_DEBUG, "Cleaning up peer retry tracker...");
        peer_retry_print_stats(mgr->peer_retry_tracker);
        peer_retry_tracker_cleanup(mgr->peer_retry_tracker);
        mgr->peer_retry_tracker = NULL;
    }

    /* Cleanup close tracker synchronization primitives */
    pthread_cond_destroy(&mgr->close_tracker.cond);
    pthread_mutex_destroy(&mgr->close_tracker.mutex);

    /* Unregister main thread from RCU (REQUIRED before exit) */
    rcu_unregister_thread();

    log_msg(LOG_DEBUG, "DHT manager cleanup complete");
}

/* Set metadata fetcher reference for statistics */
void dht_manager_set_metadata_fetcher(dht_manager_t *mgr, void *metadata_fetcher) {
    if (!mgr) {
        return;
    }
    mgr->metadata_fetcher = metadata_fetcher;
}

/* Query peers for an info_hash
 *
 * DEDUPLICATION: This is the SINGLE point where bloom filter deduplication
 * happens for automatic discovery. All sample_infohashes pass through here
 * and are filtered before entering the get_peers queue.
 *
 * priority: if true, query bypasses bloom filter (for /refresh API)
 *           if false, query checks bloom filter + database first
 */
int dht_manager_query_peers(dht_manager_t *mgr, const uint8_t *info_hash, bool priority) {
    if (!mgr || !mgr->dht || !info_hash) {
        return -1;
    }

    /* DEDUPLICATION: Check bloom filter + database before queuing for get_peers
     * This prevents duplicates from saturating the get_peers queue.
     *
     * BYPASS for priority queries (explicit user /refresh requests) */
    if (!priority && mgr->infohash_queue) {
        infohash_queue_t *queue = (infohash_queue_t *)mgr->infohash_queue;

        /* Check bloom filter first (fast, probabilistic) */
        if (queue->bloom && bloom_filter_check(queue->bloom, info_hash)) {
            /* Bloom filter says "probably seen" - verify with database */
            if (queue->db && database_has_infohash((database_t *)queue->db, info_hash)) {
                /* Confirmed duplicate - skip get_peers entirely */
                uv_mutex_lock(&queue->mutex);
                queue->duplicates_filtered++;
                uv_mutex_unlock(&queue->mutex);

                return 0;  /* Success but filtered */
            }
        }
    }

    /* Queue the info_hash for the wbpxre-dht get_peers pipeline
     * Priority queries (e.g., from /refresh API) skip to front of queue */
    int rc = wbpxre_dht_query_peers(mgr->dht, info_hash, priority);

    if (rc == 0) {
        /* Track the request */
        mgr->stats.get_peers_queries_sent++;
        mgr->active_peer_queries++;

        if (priority) {
            char hex[41];
            format_infohash(info_hash, hex, sizeof(hex));
            log_msg(LOG_DEBUG, "Priority get_peers query for hash %s", hex);
        }
    }

    return rc;
}

/* Peer retry timer callback - periodically checks for retries that are ready */
static void peer_retry_timer_cb(uv_timer_t *handle) {
    dht_manager_t *mgr = (dht_manager_t *)handle->data;
    if (!mgr || !mgr->peer_retry_tracker) {
        return;
    }

    /* Get up to 100 ready retry entries */
    uint8_t ready_infohashes[100][20];
    int count = peer_retry_get_ready_entries(mgr->peer_retry_tracker, ready_infohashes, 100);

    /* Trigger retries for all ready entries */
    for (int i = 0; i < count; i++) {
        peer_retry_entry_t *entry = peer_retry_entry_find(mgr->peer_retry_tracker, ready_infohashes[i]);
        if (entry) {
            /* Mark query as in progress */
            pthread_mutex_lock(&mgr->peer_retry_tracker->mutex);
            entry->query_in_progress = 1;
            entry->last_attempt_time = time(NULL);
            pthread_mutex_unlock(&mgr->peer_retry_tracker->mutex);

            /* Trigger another get_peers query */
            dht_manager_query_peers(mgr, ready_infohashes[i], false);
        }
    }
}

/* Dual routing timer callback - periodically checks for table rotation */
static void dual_routing_timer_cb(uv_timer_t *handle) {
    dht_manager_t *mgr = (dht_manager_t *)handle->data;
    if (!mgr || !mgr->dht || !mgr->dht->routing_controller) {
        return;
    }

    /* Triple routing rotates automatically on insert - no periodic check needed */
    (void)mgr;  /* Suppress unused parameter warning */
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

            /* Print keyspace distribution statistics */
            int close_nodes = 0, distant_nodes = 0;
            wbpxre_routing_table_t *stable_table = triple_routing_get_stable_table(mgr->dht->routing_controller);
            const uint8_t *stable_node_id = triple_routing_get_stable_node_id(mgr->dht->routing_controller);
            if (stable_table && stable_node_id) {
                wbpxre_routing_table_get_keyspace_distribution(stable_table,
                                                                 stable_node_id,
                                                                 &close_nodes,
                                                                 &distant_nodes);
            }

            int total_nodes = close_nodes + distant_nodes;
            if (total_nodes > 0) {
                log_msg(LOG_INFO, "  Keyspace distribution: close=%d (%.1f%%) distant=%d (%.1f%%)",
                        close_nodes, (close_nodes * 100.0) / total_nodes,
                        distant_nodes, (distant_nodes * 100.0) / total_nodes);
            }

            /* Get and print triple routing table statistics */
            triple_routing_stats_t triple_stats;
            triple_routing_get_stats(mgr->dht->routing_controller, &triple_stats);

            /* Update mgr stats for HTTP API */
            mgr->stats.dual_routing_rotations = triple_stats.total_rotations;
            mgr->stats.dual_routing_nodes_cleared = triple_stats.total_nodes_cleared;

            if (!triple_stats.bootstrap_complete) {
                log_msg(LOG_INFO, "  Triple routing: BOOTSTRAP IN PROGRESS (%.1f%% to first rotation)",
                        triple_stats.fill_progress_pct);
                log_msg(LOG_INFO, "    Filling table: table_%d (%u / %u nodes)",
                        triple_stats.filling_idx, triple_stats.filling_table_nodes,
                        triple_stats.rotation_threshold);
            } else {
                log_msg(LOG_INFO, "  Triple routing: rotations=%llu nodes_cleared=%llu bootstrap_time=%us",
                        (unsigned long long)triple_stats.total_rotations,
                        (unsigned long long)triple_stats.total_nodes_cleared,
                        triple_stats.bootstrap_duration_sec);
                log_msg(LOG_INFO, "    Stable table: table_%d (%u nodes) | Filling table: table_%d (%u nodes, %.1f%% full)",
                        triple_stats.stable_idx, triple_stats.stable_table_nodes,
                        triple_stats.filling_idx, triple_stats.filling_table_nodes,
                        triple_stats.fill_progress_pct);
            }
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

    /* Print retry statistics */
    if (mgr->peer_retry_tracker) {
        peer_retry_print_stats(mgr->peer_retry_tracker);
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

        /* Print connection request queue statistics */
        if (fetcher->conn_request_queue) {
            size_t conn_queue_size = connection_request_queue_size(fetcher->conn_request_queue);
            double conn_queue_utilization = (100.0 * conn_queue_size / 4000.0);
            log_msg(LOG_INFO, "    Connection request queue: size=%zu capacity=4000 utilization=%.1f%%",
                    conn_queue_size, conn_queue_utilization);
        }
    }
}
