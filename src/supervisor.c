#define _DEFAULT_SOURCE  /* For usleep */

#include "supervisor.h"
#include "dht_crawler.h"
#include "batch_writer.h"
#include "bloom_filter.h"
#include "bep51_cache.h"
#include "tree_socket.h"
#include "tree_dispatcher.h"
#include "tree_protocol.h"
#include "tree_routing.h"
#include "keyspace.h"
#include "tree_infohash_queue.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>
#include <unistd.h>


/* Forward declaration of internal helpers */
static thread_tree_t *spawn_tree(supervisor_t *sup, int slot_index, thread_tree_t *old_tree, uint32_t partition_index);
static uint32_t choose_partition_for_respawn(supervisor_t *sup, thread_tree_t *tree, int slot_index);
static void *monitor_thread_func(void *arg);
static void *supervisor_bootstrap_worker_func(void *arg);
int supervisor_bootstrap(supervisor_t *sup);


/* Built-in DHT bootstrap routers for supervisor-level bootstrap */
static const char *BOOTSTRAP_HOSTS[] = {
    "router.bittorrent.com", "dht.transmissionbt.com", "dht.libtorrent.org",
    "dht.aelitis.com",       "router.bitcomet.com",     "dht.anacrolix.link",
    NULL
};
static const int BOOTSTRAP_PORTS[] = { 6881, 6881, 25401, 6881, 6881, 42069 };

/* Worker context for supervisor bootstrap iterative phase */
typedef struct {
    tree_routing_table_t *rt;
    tree_socket_t *sock;
    tree_dispatcher_t *dispatcher;
    tree_response_queue_t *queue;
    uint8_t node_id[20];
    atomic_bool *shutdown_flag;
    int worker_id;
} sup_bootstrap_worker_ctx_t;
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
    atomic_store(&sup->monitor_running, 0);

    /* Keyspace partitioning (default: enabled) */
    sup->use_keyspace_partitioning = config->use_keyspace_partitioning;
    sup->dht_port = config->dht_port;

    /* Store shared resources */
    sup->batch_writer = config->batch_writer;
    sup->bloom_filter = config->bloom_filter;
    sup->failure_bloom = NULL;  /* Will be initialized during start */
    sup->failure_bloom_path = "data/failure_bloom.dat";
      /* Will be created during bootstrap */
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

    /* Tree bootstrap settings */
    sup->tree_bootstrap_timeout_sec = config->tree_bootstrap_timeout_sec > 0 ? config->tree_bootstrap_timeout_sec : 30;

    /* Supervisor-level global bootstrap settings */
    sup->global_bootstrap_target = config->global_bootstrap_target > 0 ? config->global_bootstrap_target : 1000;
    sup->global_bootstrap_timeout_sec = config->global_bootstrap_timeout_sec > 0 ? config->global_bootstrap_timeout_sec : 60;
    sup->global_bootstrap_workers = config->global_bootstrap_workers > 0 ? config->global_bootstrap_workers : 20;

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
    sup->parallel_peers = config->parallel_peers > 0 ? config->parallel_peers : 2;

    /* Metadata rate-based respawn settings */
    sup->min_metadata_rate = config->min_metadata_rate >= 0 ? config->min_metadata_rate : 0.01;
    sup->dynamic_rate_margin = config->dynamic_rate_margin >= 0 ? config->dynamic_rate_margin : 0.02;
    sup->rate_check_interval_sec = config->rate_check_interval_sec > 0 ? config->rate_check_interval_sec : 60;
    sup->rate_grace_period_sec = config->rate_grace_period_sec > 0 ? config->rate_grace_period_sec : 30;
    sup->min_lifetime_minutes = config->min_lifetime_minutes > 0 ? config->min_lifetime_minutes : 10;
    sup->require_empty_queue = config->require_empty_queue;
    sup->rate_ema_alpha = (config->rate_ema_alpha > 0.0 && config->rate_ema_alpha <= 1.0) ? config->rate_ema_alpha : 0.3;

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
    atomic_init(&sup->cumulative_metadata_attempts, 0);

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

    /* Adaptive keyspace partitioning */
    sup->dead_partition_threshold = config->dead_partition_threshold > 0 ? config->dead_partition_threshold : 3;
    sup->max_trees_per_partition = config->max_trees_per_partition > 0 ? config->max_trees_per_partition : 4;
    sup->partition_stats = calloc(sup->max_trees, sizeof(partition_stats_t));
    if (!sup->partition_stats) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate partition_stats array");
        free(sup->trees);
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }
    sup->home_partitions = calloc(sup->max_trees, sizeof(uint32_t));
    if (!sup->home_partitions) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate home_partitions array");
        free(sup->partition_stats);
        free(sup->trees);
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
        free(sup->home_partitions);
        free(sup->partition_stats);
        free(sup->trees);
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }
    sup->draining_count = 0;
    if (pthread_mutex_init(&sup->draining_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to init draining_lock mutex");
        free(sup->draining_trees);
        free(sup->home_partitions);
        free(sup->partition_stats);
        free(sup->trees);
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }

    /* Allocate per-slot grace period tracking for dynamic rate respawn */
    sup->rate_below_since = calloc(sup->max_trees, sizeof(time_t));
    if (!sup->rate_below_since) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate rate_below_since array");
        pthread_mutex_destroy(&sup->draining_lock);
        free(sup->draining_trees);
        free(sup->home_partitions);
        free(sup->partition_stats);
        free(sup->trees);
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }

    log_msg(LOG_DEBUG, "[supervisor] Created with max_trees=%d, bootstrap_timeout=%ds, draining_slots=%d, spawn_threshold=%d, drain_timeout=%ds",
            sup->max_trees, sup->tree_bootstrap_timeout_sec, sup->max_draining_trees,
            sup->respawn_spawn_threshold, sup->respawn_drain_timeout_sec);

    return sup;
}

/**
 * Spawn a new tree for the given slot
 * @param sup Supervisor instance
 * @param slot_index Slot index (0 to max_trees-1)
 * @param old_tree Previous tree in this slot (NULL for first spawn, non-NULL for respawn with perturbation)
 * @param partition_index Keyspace partition to assign this tree to
 * @return Pointer to new tree, or NULL on error
 */
/* Supervisor bootstrap worker: iteratively discover nodes by querying random RT entries */
static void *supervisor_bootstrap_worker_func(void *arg) {
    sup_bootstrap_worker_ctx_t *ctx = (sup_bootstrap_worker_ctx_t *)arg;
    tree_routing_table_t *rt = ctx->rt;
    tree_socket_t *sock = ctx->sock;
    tree_dispatcher_t *dispatcher = ctx->dispatcher;
    tree_response_queue_t *q = ctx->queue;
    atomic_bool *shutdown_flag = ctx->shutdown_flag;

    unsigned char my_id[20];
    FILE *ur = fopen("/dev/urandom", "rb");
    if (ur) { fread(my_id, 1, 20, ur); fclose(ur); }
    else { for (int i = 0; i < 20; i++) my_id[i] = (unsigned char)(rand() % 256); }

    while (!atomic_load(shutdown_flag)) {
        /* Pick a random node from routing table to query */
        tree_node_t node;
        if (tree_routing_get_random_nodes(rt, &node, 1) < 1) {
            usleep(10000);
            continue;
        }

        /* Generate random target */
        uint8_t target[20];
        for (int i = 0; i < 20; i++) target[i] = (unsigned char)(rand() % 256);

        /* Generate TID and register */
        uint8_t tid[4];
        int tid_len = tree_protocol_gen_tid(tid);
        tree_dispatcher_register_tid(dispatcher, tid, tid_len, q);

        /* Send find_node */
        uint8_t send_tid[4];
        int send_tid_len = tree_protocol_gen_tid(send_tid);
        tree_dispatcher_register_tid(dispatcher, send_tid, send_tid_len, q);
        int send_rc = tree_send_find_node(NULL, sock, send_tid, send_tid_len, target, &node.addr);
        tree_dispatcher_unregister_tid(dispatcher, send_tid, send_tid_len);
        if (send_rc != 0) {
            tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);
            usleep(10000);
            continue;
        }

        /* Wait for response */
        tree_response_t response_pkt;
        if (tree_response_queue_pop(q, &response_pkt, 1500) == 0) {
            uint8_t *payload = response_pkt.data;
            int payload_len = response_pkt.len;
            if (payload_len >= 26) {
                /* Parse find_node response for node list */
                if (payload[0] == 0 && payload[1] == 0 && payload[2] == 'o' && payload[3] == 'k') {
                    const uint8_t *nodes_ptr = payload + 26;
                    int remaining = payload_len - 26;
                    if (remaining > 4) {
                        int nodes_data_len = nodes_ptr[0] * 256 + nodes_ptr[1];
                        if (nodes_data_len > 0 && nodes_data_len + 4 <= remaining) {
                            int ip_len = nodes_ptr[2];
                            int data_offset = 4 + ip_len;
                            if (data_offset + 4 <= nodes_data_len) {
                                uint16_t port = (nodes_ptr[data_offset] << 8) | nodes_ptr[data_offset + 1];
                                const uint8_t *nid_ptr = nodes_ptr + data_offset + 2;
                                int nid_remaining = nodes_data_len - data_offset - 2;

                                struct sockaddr_in sin;
                                memset(&sin, 0, sizeof(sin));
                                sin.sin_family = AF_INET;
                                sin.sin_port = htons(port);
                                if (ip_len == 4) {
                                    memcpy(&sin.sin_addr, nodes_ptr + 2, 4);
                                }

                                while (nid_remaining >= 24) {
                                    uint8_t nid[20];
                                    memcpy(nid, nid_ptr, 20);
                                    nid_remaining -= 24;
                                    nid_ptr += 24;

                                    tree_node_t tn;
                                    memcpy(tn.node_id, nid, 20);
                                    tn.addr = (struct sockaddr_storage){0};
                                    memcpy(&tn.addr, &sin, sizeof(sin));
                                    tn.last_seen = time(NULL);
                                    tn.last_queried = 0;
                                    tn.fail_count = 0;
                                    tn.bep51_status = BEP51_UNKNOWN;
                                    tn.next = NULL;
                                    tree_routing_add_node(rt, nid, &tn.addr);
                                }
                            }
                        }
                    }
                }
            }
        }

        tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);
        usleep(10000);  /* 10ms */
    }

    tree_response_queue_destroy(q);
    free(ctx);
    return NULL;
}

/* Phase A: resolve bootstrap hosts and query them for nodes */
static void bootstrap_phase_a_url_lookup(tree_routing_table_t *rt,
                                          tree_socket_t *sock,
                                          tree_dispatcher_t *dispatcher,
                                          time_t deadline) {
    int bootstrapped = 0;
    for (int i = 0; BOOTSTRAP_HOSTS[i] != NULL && time(NULL) < deadline; i++) {
        struct addrinfo hints, *res;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_DGRAM;
        char port_str[16];
        snprintf(port_str, sizeof(port_str), "%d", BOOTSTRAP_PORTS[i]);
        int ret = getaddrinfo(BOOTSTRAP_HOSTS[i], port_str, &hints, &res);
        if (ret != 0) {
            log_msg(LOG_WARN, "[supervisor_bootstrap] DNS failed for %s", BOOTSTRAP_HOSTS[i]);
            continue;
        }
        struct sockaddr_storage addr;
        memcpy(&addr, res->ai_addr, res->ai_addrlen);
        freeaddrinfo(res);

        uint8_t target[20];
        for (int j = 0; j < 20; j++) target[j] = (unsigned char)(rand() % 256);

        uint8_t tid[4];
        int tid_len = tree_protocol_gen_tid(tid);
        tree_response_queue_t *q = tree_response_queue_create(4);
        tree_dispatcher_register_tid(dispatcher, tid, tid_len, q);

        int send_rc = tree_send_find_node(NULL, sock, tid, tid_len, target, &addr);

        tree_response_t response_pkt;
        if (send_rc == 0 && tree_response_queue_pop(q, &response_pkt, 1500) == 0) {
            uint8_t *payload = response_pkt.data;
            int payload_len = response_pkt.len;
            if (payload_len >= 26 && payload[0] == 0 && payload[1] == 0 && payload[2] == 'o' && payload[3] == 'k') {
                const uint8_t *nodes_ptr = payload + 26;
                int remaining = payload_len - 26;
                if (remaining > 4) {
                    int nodes_data_len = nodes_ptr[0] * 256 + nodes_ptr[1];
                    if (nodes_data_len > 0 && nodes_data_len + 4 <= remaining) {
                        int ip_len = nodes_ptr[2];
                        int data_offset = 4 + ip_len;
                        if (data_offset + 4 <= nodes_data_len) {
                            uint16_t port = (nodes_ptr[data_offset] << 8) | nodes_ptr[data_offset + 1];
                            const uint8_t *nid_ptr = nodes_ptr + data_offset + 2;
                            int nid_remaining = nodes_data_len - data_offset - 2;

                            struct sockaddr_in sin;
                            memset(&sin, 0, sizeof(sin));
                            sin.sin_family = AF_INET;
                            sin.sin_port = htons(port);
                            if (ip_len == 4) {
                                memcpy(&sin.sin_addr, nodes_ptr + 2, 4);
                            }

                            while (nid_remaining >= 24) {
                                uint8_t nid[20];
                                memcpy(nid, nid_ptr, 20);
                                nid_remaining -= 24;
                                nid_ptr += 24;

                                tree_node_t tn;
                                memcpy(tn.node_id, nid, 20);
                                tn.addr = (struct sockaddr_storage){0};
                                memcpy(&tn.addr, &sin, sizeof(sin));
                                tn.last_seen = time(NULL);
                                tn.last_queried = 0;
                                tn.fail_count = 0;
                                tn.bep51_status = BEP51_UNKNOWN;
                                tn.next = NULL;
                                tree_routing_add_node(rt, nid, &tn.addr);
                            }
                        }
                    }
                }
            }
        }

        tree_response_queue_destroy(q);
        tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);
        bootstrapped++;
        if (tree_routing_get_count(rt) >= 100 || bootstrapped >= 2) break;
    }
    if (tree_routing_get_count(rt) < 100) {
        log_msg(LOG_WARN, "[supervisor_bootstrap] URL bootstrap got only %d nodes; relying on iterative workers",
                tree_routing_get_count(rt));
    }
}

/* Main supervisor bootstrap function */
int supervisor_bootstrap(supervisor_t *sup) {
    int target = sup->global_bootstrap_target > 0 ? sup->global_bootstrap_target : 1000;
    int timeout_s = sup->global_bootstrap_timeout_sec > 0 ? sup->global_bootstrap_timeout_sec : 60;
    int nworkers = sup->global_bootstrap_workers > 0 ? sup->global_bootstrap_workers : 20;

    log_msg(LOG_INFO, "Supervisor bootstrap starting (target=%d, timeout=%ds, workers=%d)",
            target, timeout_s, nworkers);

    unsigned char our_id[20];
    FILE *ur = fopen("/dev/urandom", "rb");
    if (ur) { fread(our_id, 1, 20, ur); fclose(ur); }
    else { for (int i = 0; i < 20; i++) our_id[i] = (unsigned char)(rand() % 256); }
    sup->bootstrap_routing_table = tree_routing_create(our_id);
    tree_routing_set_bucket_capacity((tree_routing_table_t *)sup->bootstrap_routing_table, 20);

    sup->bootstrap_socket = tree_socket_create(0);
    if (!sup->bootstrap_socket) {
        log_msg(LOG_ERROR, "[supervisor_bootstrap] Failed to create ephemeral socket");
        tree_routing_destroy((tree_routing_table_t *)sup->bootstrap_routing_table);
        sup->bootstrap_routing_table = NULL;
        return -1;
    }

    sup->bootstrap_dispatcher = tree_dispatcher_create(NULL, (tree_socket_t *)sup->bootstrap_socket);
    if (!sup->bootstrap_dispatcher) {
        log_msg(LOG_ERROR, "[supervisor_bootstrap] Failed to create dispatcher");
        tree_socket_destroy((tree_socket_t *)sup->bootstrap_socket);
        tree_routing_destroy((tree_routing_table_t *)sup->bootstrap_routing_table);
        sup->bootstrap_socket = NULL;
        sup->bootstrap_routing_table = NULL;
        return -1;
    }
    tree_dispatcher_start((tree_dispatcher_t *)sup->bootstrap_dispatcher);

    time_t start = time(NULL);
    time_t deadline = start + timeout_s;
    tree_routing_table_t *rt = (tree_routing_table_t *)sup->bootstrap_routing_table;

    bootstrap_phase_a_url_lookup(rt, (tree_socket_t *)sup->bootstrap_socket,
                                 (tree_dispatcher_t *)sup->bootstrap_dispatcher, deadline);

    atomic_bool shutdown_flag = false;
    sup->bootstrap_worker_count = nworkers;
    sup->bootstrap_workers = calloc(nworkers, sizeof(pthread_t));
    if (!sup->bootstrap_workers) {
        log_msg(LOG_ERROR, "[supervisor_bootstrap] Failed to allocate worker array");
        goto cleanup;
    }

    for (int i = 0; i < nworkers; i++) {
        sup_bootstrap_worker_ctx_t *ctx = malloc(sizeof(sup_bootstrap_worker_ctx_t));
        if (!ctx) { nworkers = i; break; }
        ctx->rt = rt;
        ctx->sock = (tree_socket_t *)sup->bootstrap_socket;
        ctx->dispatcher = (tree_dispatcher_t *)sup->bootstrap_dispatcher;
        ctx->queue = tree_response_queue_create(4);
        ctx->shutdown_flag = &shutdown_flag;
        ctx->worker_id = i;
        if (!ctx->queue) { free(ctx); nworkers = i; break; }
        int rc = pthread_create(&sup->bootstrap_workers[i], NULL,
                                supervisor_bootstrap_worker_func, ctx);
        if (rc != 0) { tree_response_queue_destroy(ctx->queue); free(ctx); nworkers = i; break; }
    }

    while (time(NULL) < deadline) {
        int n = tree_routing_get_count(rt);
        if (n >= target) break;
        usleep(200000);
    }

    atomic_store(&shutdown_flag, true);
    for (int i = 0; i < nworkers; i++) {
        if (sup->bootstrap_workers[i]) pthread_join(sup->bootstrap_workers[i], NULL);
    }
    free(sup->bootstrap_workers);
    sup->bootstrap_workers = NULL;
    sup->bootstrap_worker_count = 0;

    int final_count = tree_routing_get_count(rt);
    int elapsed = (int)(time(NULL) - start);
    log_msg(LOG_INFO, "Supervisor bootstrap: %d nodes in %ds", final_count, elapsed);

    int added_to_cache = 0;
    tree_node_t *node, *tmp;
    HASH_ITER(hh_flat, rt->flat_index_head, node, tmp) {
        if (bep51_cache_add_node(sup->bep51_cache, node->node_id, &node->addr) == 0) {
            added_to_cache++;
        }
    }
    log_msg(LOG_INFO, "Supervisor bootstrap: %d nodes submitted to BEP51 cache", added_to_cache);

    if (final_count < 100) {
        log_msg(LOG_WARN, "[supervisor_bootstrap] Only %d nodes collected; per-tree URL bootstrap may still be needed", final_count);
    }

cleanup:
    tree_dispatcher_stop((tree_dispatcher_t *)sup->bootstrap_dispatcher);
    tree_dispatcher_destroy((tree_dispatcher_t *)sup->bootstrap_dispatcher);
    tree_socket_destroy((tree_socket_t *)sup->bootstrap_socket);
    tree_routing_destroy((tree_routing_table_t *)sup->bootstrap_routing_table);
    sup->bootstrap_dispatcher = NULL;
    sup->bootstrap_socket = NULL;
    sup->bootstrap_routing_table = NULL;

    return (final_count >= 100) ? 0 : -1;
}


static thread_tree_t *spawn_tree(supervisor_t *sup, int slot_index, thread_tree_t *old_tree, uint32_t partition_index) {
    tree_config_t config = {
        /* Keyspace partitioning */
        .use_keyspace_partitioning = sup->use_keyspace_partitioning,
        .partition_index = partition_index,
        .num_partitions = (uint32_t)sup->max_trees,
        .dht_port = sup->dht_port,

        .num_find_node_workers = sup->num_find_node_workers,  /* Continuous find_node workers */
        .find_node_target_nodes = sup->bep51_cache_capacity,
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
        .parallel_peers = sup->parallel_peers,
        /* Metadata rate-based respawn settings */
        .min_metadata_rate = sup->min_metadata_rate,
        .rate_check_interval_sec = sup->rate_check_interval_sec,
        .rate_grace_period_sec = sup->rate_grace_period_sec,
        .min_lifetime_minutes = sup->min_lifetime_minutes,
        .require_empty_queue = sup->require_empty_queue,
        .ema_alpha = sup->rate_ema_alpha,
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

    /* Set supervisor backlink and home partition */
    tree->supervisor = sup;
    tree->home_partition = sup->home_partitions[slot_index];

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
 * Choose which partition to use when respawning a tree.
 * Updates partition_stats based on the dying tree's performance.
 *
 * When a partition is "dead" (N consecutive zero-rate respawns):
 * - If the tree is away from home: return to home partition (reset its death counter)
 * - If the tree is at home: migrate to the partition with highest live metadata rate
 *
 * Caller must hold trees_lock.
 *
 * @param sup Supervisor instance
 * @param tree The dying tree being respawned
 * @param slot_index Slot index in the trees array
 * @return Partition index to use for the new tree
 */
static uint32_t choose_partition_for_respawn(supervisor_t *sup, thread_tree_t *tree, int slot_index) {
    uint32_t old_partition = tree->partition_index;

    /* Check if this tree's metadata rate was effectively zero at shutdown.
     * We use metadata_rate (computed by the rate monitor each check interval)
     * rather than lifetime metadata_count, because a tree that fetched 50 metadata
     * early on but then dropped to 0 rate should count as a "zero rate" respawn
     * for partition death detection. The rate monitor already confirmed the rate
     * was below min_metadata_rate before triggering the shutdown. */
    bool was_zero_rate = (tree->metadata_rate < sup->min_metadata_rate);

    if (was_zero_rate) {
        sup->partition_stats[old_partition].dead_consecutive++;
        log_msg(LOG_DEBUG, "[supervisor] Partition %u: zero-rate respawn #%d (tree had rate=%.4f)",
                old_partition, sup->partition_stats[old_partition].dead_consecutive,
                tree->metadata_rate);
    } else {
        sup->partition_stats[old_partition].dead_consecutive = 0;
    }

    /* Check if partition is dead */
    if (sup->partition_stats[old_partition].dead_consecutive < sup->dead_partition_threshold) {
        /* Partition is still viable, stay */
        return old_partition;
    }

    log_msg(LOG_DEBUG, "[supervisor] Partition %u is dead (%d consecutive low-rate respawns)",
            old_partition, sup->partition_stats[old_partition].dead_consecutive);

    /* If tree is away from home, return home first */
    uint32_t home = sup->home_partitions[slot_index];
    if (old_partition != home) {
        /* Reset home's death counter to give it a fresh chance */
        sup->partition_stats[home].dead_consecutive = 0;
        log_msg(LOG_DEBUG, "[supervisor] Returning slot %d from partition %u to home partition %u",
                slot_index, old_partition, home);
        return home;
    }

    /* Tree is at home and home is dead — migrate to partition with highest live rate */

    /* Compute average metadata rate per partition from active trees */
    double partition_rates[sup->max_trees];
    memset(partition_rates, 0, sup->max_trees * sizeof(double));
    for (int s = 0; s < sup->max_trees; s++) {
        if (sup->trees[s]) {
            uint32_t p = sup->trees[s]->partition_index;
            partition_rates[p] += sup->trees[s]->metadata_rate;
        }
    }
    for (int p = 0; p < sup->max_trees; p++) {
        int count = sup->partition_stats[p].current_tree_count;
        if (count > 0) {
            partition_rates[p] /= count;
        }
    }

    /* Find the best alternative partition by live rate */
    uint32_t best_partition = old_partition;
    double best_rate = 0.0;
    bool found_alternative = false;

    for (int p = 0; p < sup->max_trees; p++) {
        if ((uint32_t)p == old_partition) {
            continue;
        }

        /* Skip dead partitions */
        if (sup->partition_stats[p].dead_consecutive >= sup->dead_partition_threshold) {
            continue;
        }

        /* Skip partitions at capacity */
        if (sup->partition_stats[p].current_tree_count >= sup->max_trees_per_partition) {
            continue;
        }

        /* Pick partition with highest live metadata rate */
        if (!found_alternative || partition_rates[p] > best_rate) {
            best_partition = (uint32_t)p;
            best_rate = partition_rates[p];
            found_alternative = true;
        }
    }

    if (found_alternative && best_partition != old_partition) {
        log_msg(LOG_DEBUG, "[supervisor] Migrating slot %d from home partition %u to partition %u "
                "(target avg rate=%.4f, %d trees)",
                slot_index, old_partition, best_partition,
                best_rate,
                sup->partition_stats[best_partition].current_tree_count);
    } else {
        log_msg(LOG_WARN, "[supervisor] No better partition found for migration from partition %u, staying",
                old_partition);
    }

    return best_partition;
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
    sup->draining_trees[sup->draining_count].drain_start = time(NULL);
    sup->draining_trees[sup->draining_count].original_slot = slot_index;
    sup->draining_count++;

    pthread_mutex_unlock(&sup->draining_lock);

    /* Clear from active array */
    sup->trees[slot_index] = NULL;
    sup->active_trees--;

    log_msg(LOG_DEBUG, "[tree %u] Moved to draining list (slot %d, draining=%d/%d, active_connections=%d)",
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
        double drain_time = difftime(now, dt->drain_start);

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
            log_msg(LOG_DEBUG, "[tree %u] Destroying draining tree (reason: %s, drain_time=%.0fs, final_connections=%d)",
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

    /* Try to load BEP51 cache for fallback bootstrap */
    log_msg(LOG_DEBUG, "[supervisor] Attempting to load BEP51 cache from %s", sup->bep51_cache_path);
    int cache_loaded = bep51_cache_load_from_file(sup->bep51_cache, sup->bep51_cache_path);
    if (cache_loaded == 0) {
        log_msg(LOG_DEBUG, "[supervisor] Cache loaded successfully: %zu nodes", bep51_cache_get_count(sup->bep51_cache));
    }

    /* Supervisor-level bootstrap: if cache is cold (fresh data dir), run
     * a parallel URL bootstrap to collect >= 1000 nodes, then persist
     * them to the cache. On warm restart the cache has enough nodes
     * and this step is skipped entirely. */
    if (bep51_cache_get_count(sup->bep51_cache) < 100) {
        log_msg(LOG_INFO, "BEP51 cache cold (%zu nodes), running supervisor bootstrap",
                bep51_cache_get_count(sup->bep51_cache));
        if (supervisor_bootstrap(sup) != 0) {
            log_msg(LOG_WARN, "Supervisor bootstrap failed; per-tree URL bootstrap will still run");
        }
    } else {
        log_msg(LOG_INFO, "BEP51 cache warm (%zu nodes), skipping supervisor bootstrap",
                bep51_cache_get_count(sup->bep51_cache));
    }

    /* Create shared socket and dispatcher when using fixed port */
    if (sup->dht_port > 0) {
        sup->shared_socket = tree_socket_create(sup->dht_port);
        if (!sup->shared_socket) {
            log_msg(LOG_ERROR, "[supervisor] Failed to create shared socket on port %d", sup->dht_port);
            bloom_filter_cleanup(sup->failure_bloom);
            bep51_cache_destroy(sup->bep51_cache);
            return;
        }

        sup->shared_dispatcher = tree_dispatcher_create(NULL, sup->shared_socket);
        if (!sup->shared_dispatcher) {
            log_msg(LOG_ERROR, "[supervisor] Failed to create shared dispatcher");
            tree_socket_destroy(sup->shared_socket);
            sup->shared_socket = NULL;
            bloom_filter_cleanup(sup->failure_bloom);
            bep51_cache_destroy(sup->bep51_cache);
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
            return;
        }

        log_msg(LOG_INFO, "[supervisor] Shared socket/dispatcher ready on port %d",
                tree_socket_get_port(sup->shared_socket));
    }

    pthread_mutex_lock(&sup->trees_lock);

    /* Spawn all trees (they will sample from the shared pool) */
    for (int i = 0; i < sup->max_trees; i++) {
        sup->home_partitions[i] = (uint32_t)i;  /* Each slot's home is its own index */
        sup->trees[i] = spawn_tree(sup, i, NULL, (uint32_t)i);  /* NULL = first spawn, partition = slot */
        if (sup->trees[i]) {
            thread_tree_start(sup->trees[i]);
            sup->active_trees++;
            sup->partition_stats[i].current_tree_count++;
        }
    }

    pthread_mutex_unlock(&sup->trees_lock);

    /* Start monitor thread */
    atomic_store(&sup->monitor_running, 1);
    if (pthread_create(&sup->monitor_thread, NULL, monitor_thread_func, sup) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create monitor thread");
        atomic_store(&sup->monitor_running, 0);
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
    atomic_store(&sup->monitor_running, 0);

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

    /* Destroy any remaining draining trees BEFORE shared resources.
     * Draining trees still have worker threads running that may access
     * the bep51_cache, bloom filters, and shared node pool.  Joining
     * them here prevents use-after-free when those resources are freed
     * below. */
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
        sup->draining_trees = NULL;
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


    /* Cleanup shared dispatcher and socket */
    if (sup->shared_dispatcher) {
        tree_dispatcher_destroy(sup->shared_dispatcher);
        sup->shared_dispatcher = NULL;
    }
    if (sup->shared_socket) {
        tree_socket_destroy(sup->shared_socket);
        sup->shared_socket = NULL;
    }

    pthread_mutex_destroy(&sup->trees_lock);
    free(sup->home_partitions);
    free(sup->partition_stats);
    free(sup->rate_below_since);
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

    while (atomic_load(&sup->monitor_running)) {
        /* Sleep in small chunks (1 second) to be responsive to shutdown */
        for (int i = 0; i < 10 && atomic_load(&sup->monitor_running); i++) {
            struct timespec ts = {1, 0};
            nanosleep(&ts, NULL);
        }

        if (!atomic_load(&sup->monitor_running)) {
            break;
        }

        /* Check tree performance */
        pthread_mutex_lock(&sup->trees_lock);

        /* Compute dynamic respawn threshold for this cycle:
         *   dynamic_threshold = max(min_metadata_rate, (total_rate / N) - dynamic_rate_margin)
         * where N = active non-draining trees past their immunity period. */
        double total_rate = 0.0;
        int active_rate_count = 0;
        time_t now_thresh = time(NULL);
        for (int j = 0; j < sup->max_trees; j++) {
            thread_tree_t *t = sup->trees[j];
            if (!t || (uintptr_t)t < 0x1000) continue;
            if (atomic_load(&t->needs_respawn)) continue;
            double age = difftime(now_thresh, t->creation_time);
            if (age < (double)t->min_lifetime_sec) continue;
            total_rate += t->metadata_rate;
            active_rate_count++;
        }
        double dynamic_threshold = sup->min_metadata_rate;
        if (active_rate_count > 0) {
            double avg = total_rate / active_rate_count;
            double computed = avg - sup->dynamic_rate_margin;
            if (computed > sup->min_metadata_rate) {
                dynamic_threshold = computed;
            }
        }
        log_msg(LOG_DEBUG, "[supervisor] Dynamic threshold: %.4f/s (avg=%.4f, N=%d, margin=%.4f, floor=%.4f)",
                dynamic_threshold,
                active_rate_count > 0 ? total_rate / active_rate_count : 0.0,
                active_rate_count, sup->dynamic_rate_margin, sup->min_metadata_rate);

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

                    log_msg(LOG_DEBUG, "[tree %u] Respawning (this_tree_active_connections=%d <= threshold=%d)",
                            tree_id, this_tree_active_conns, sup->respawn_spawn_threshold);

                    /* Accumulate statistics from dying tree */
                    uint64_t tree_metadata = atomic_load(&tree->metadata_count);
                    uint64_t tree_filtered = atomic_load(&tree->filtered_count);
                    uint64_t tree_first_strike = atomic_load(&tree->first_strike_failures);
                    uint64_t tree_second_strike = atomic_load(&tree->second_strike_failures);
                    uint64_t tree_attempts = atomic_load(&tree->metadata_attempts);

                    atomic_fetch_add(&sup->cumulative_metadata_count, tree_metadata);
                    atomic_fetch_add(&sup->cumulative_filtered_count, tree_filtered);
                    atomic_fetch_add(&sup->cumulative_first_strike_failures, tree_first_strike);
                    atomic_fetch_add(&sup->cumulative_second_strike_failures, tree_second_strike);
                    atomic_fetch_add(&sup->cumulative_metadata_attempts, tree_attempts);

                    log_msg(LOG_DEBUG, "[tree %u] Accumulated stats: metadata=%lu, filtered=%lu, 1st_strike=%lu, 2nd_strike=%lu",
                            tree_id, (unsigned long)tree_metadata, (unsigned long)tree_filtered,
                            (unsigned long)tree_first_strike, (unsigned long)tree_second_strike);

                    /* Choose partition for respawn (may migrate or return home) */
                    uint32_t new_partition = choose_partition_for_respawn(sup, tree, i);
                    uint32_t old_partition = tree->partition_index;
                    sup->partition_stats[old_partition].current_tree_count--;

                    /* If migrating to a new partition, don't perturb from old tree (generate fresh ID) */
                    thread_tree_t *perturb_from = (new_partition == old_partition) ? tree : NULL;
                    thread_tree_t *new_tree = spawn_tree(sup, i, perturb_from, new_partition);

                    if (!new_tree) {
                        log_msg(LOG_ERROR, "[tree %u] Failed to spawn replacement tree", tree_id);
                        sup->partition_stats[old_partition].current_tree_count++;  /* Restore count */
                        continue;
                    }

                    sup->partition_stats[new_partition].current_tree_count++;

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
                    sup->rate_below_since[i] = 0;  /* Reset grace period for new tree */
                    thread_tree_start(new_tree);
                    sup->active_trees++;

                    log_msg(LOG_DEBUG, "[tree %u] Replacement tree started (slot=%d, draining_count=%d)",
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

            log_msg(LOG_DEBUG, "[supervisor] Tree %u phase=%s metadata=%lu ema=%.4f/s",
                    tree_id,
                    thread_tree_phase_name(phase),
                    (unsigned long)metadata_count,
                    tree->metadata_rate);

            /* Skip rate check if tree is still in immunity period */
            time_t now_check = time(NULL);
            double tree_age = difftime(now_check, tree->creation_time);
            if (tree_age < (double)tree->min_lifetime_sec) {
                sup->rate_below_since[i] = 0;
                continue;
            }

            /* Skip rate check if tree has not yet computed its first EMA (rate == 0.0 at init) */
            if (tree->metadata_rate == 0.0 && metadata_count == 0) {
                continue;
            }

            /* Check rate against dynamic threshold */
            double ema_rate = tree->metadata_rate;
            if (ema_rate < dynamic_threshold) {
                /* Optionally wait for empty infohash queue before respawning */
                if (sup->require_empty_queue) {
                    int queue_size = tree_infohash_queue_count(tree->infohash_queue);
                    if (queue_size > 0) {
                        log_msg(LOG_DEBUG, "[tree %u] EMA %.4f < threshold %.4f but queue not empty (%d) - waiting",
                                tree_id, ema_rate, dynamic_threshold, queue_size);
                        sup->rate_below_since[i] = 0;
                        continue;
                    }
                }

                if (sup->rate_below_since[i] == 0) {
                    sup->rate_below_since[i] = now_check;
                    log_msg(LOG_DEBUG, "[tree %u] EMA rate %.4f/s < dynamic threshold %.4f/s - grace period starts (%ds)",
                            tree_id, ema_rate, dynamic_threshold, sup->rate_grace_period_sec);
                } else {
                    double below_duration = difftime(now_check, sup->rate_below_since[i]);
                    if (below_duration >= (double)sup->rate_grace_period_sec) {
                        log_msg(LOG_DEBUG, "[tree %u] EMA rate %.4f/s < threshold %.4f/s for %.0fs - requesting respawn",
                                tree_id, ema_rate, dynamic_threshold, below_duration);
                        if (!atomic_load(&tree->shutdown_requested)) {
                            thread_tree_request_shutdown(tree, SHUTDOWN_REASON_RATE_BASED);
                        }
                    } else {
                        log_msg(LOG_DEBUG, "[tree %u] EMA rate %.4f/s < threshold %.4f/s for %.0fs / %ds",
                                tree_id, ema_rate, dynamic_threshold, below_duration, sup->rate_grace_period_sec);
                    }
                }
            } else {
                if (sup->rate_below_since[i] != 0) {
                    log_msg(LOG_DEBUG, "[tree %u] EMA rate %.4f/s recovered above threshold %.4f/s",
                            tree_id, ema_rate, dynamic_threshold);
                }
                sup->rate_below_since[i] = 0;
            }
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
