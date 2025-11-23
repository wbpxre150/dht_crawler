#define _DEFAULT_SOURCE  /* For usleep */

#include "thread_tree.h"
#include "tree_routing.h"
#include "tree_socket.h"
#include "tree_protocol.h"
#include "tree_infohash_queue.h"
#include "tree_peers_queue.h"
#include "tree_metadata.h"
#include "bloom_filter.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>

/* Bootstrap nodes */
static const char *BOOTSTRAP_NODES[] = {
    "router.bittorrent.com",
    "dht.transmissionbt.com",
    "router.utorrent.com",
    NULL
};
static const int BOOTSTRAP_PORT = 6881;

/* Generate random node ID for DHT identity */
static void generate_random_node_id(uint8_t *node_id) {
    /* Seed with time and random */
    srand((unsigned int)(time(NULL) ^ (uintptr_t)node_id));
    for (int i = 0; i < 20; i++) {
        node_id[i] = (uint8_t)(rand() % 256);
    }
}

const char *thread_tree_phase_name(tree_phase_t phase) {
    switch (phase) {
        case TREE_PHASE_BOOTSTRAP:     return "BOOTSTRAP";
        case TREE_PHASE_BEP51:         return "BEP51";
        case TREE_PHASE_GET_PEERS:     return "GET_PEERS";
        case TREE_PHASE_METADATA:      return "METADATA";
        case TREE_PHASE_SHUTTING_DOWN: return "SHUTTING_DOWN";
        default:                       return "UNKNOWN";
    }
}

/* Resolve hostname to sockaddr_storage */
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

/* BEP51 worker context */
typedef struct bep51_worker_ctx {
    thread_tree_t *tree;
    int worker_id;
} bep51_worker_ctx_t;

/* Generate random 20-byte target for BEP51 queries */
static void generate_random_target(uint8_t *target) {
    for (int i = 0; i < 20; i++) {
        target[i] = (uint8_t)(rand() % 256);
    }
}

/* Forward declarations */
static void tree_start_get_peers_workers(thread_tree_t *tree);
static void tree_start_metadata_workers(thread_tree_t *tree);
static void tree_start_rate_monitor(thread_tree_t *tree);

/* Bootstrap find_node worker context */
typedef struct bootstrap_worker_ctx {
    thread_tree_t *tree;
    int worker_id;
} bootstrap_worker_ctx_t;

/* Bootstrap find_node worker: continuously queries nodes from routing table */
static void *bootstrap_find_node_worker_func(void *arg) {
    bootstrap_worker_ctx_t *ctx = (bootstrap_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;

    log_msg(LOG_DEBUG, "[tree %u] Bootstrap worker %d started", tree->tree_id, worker_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;

    while (!atomic_load(&tree->shutdown_requested) && tree->current_phase == TREE_PHASE_BOOTSTRAP) {
        /* Get random nodes from routing table */
        tree_node_t nodes[8];
        int got = tree_routing_get_random_nodes(rt, nodes, 8);

        if (got == 0) {
            /* No nodes yet, wait a bit */
            usleep(100000);  /* 100ms */
            continue;
        }

        /* Query each node with a random target to explore keyspace */
        for (int i = 0; i < got; i++) {
            uint8_t random_target[20];
            generate_random_target(random_target);
            tree_send_find_node(tree, sock, random_target, &nodes[i].addr);
        }

        /* Rate limit: ~10ms per query batch */
        usleep(10000);
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] Bootstrap worker %d exiting", tree->tree_id, worker_id);
    return NULL;
}

/* BEP51 worker thread function */
static void *bep51_worker_func(void *arg) {
    bep51_worker_ctx_t *ctx = (bep51_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;

    log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d started", tree->tree_id, worker_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;
    uint8_t recv_buf[2048];
    bool first_infohash_found = false;

    while (!atomic_load(&tree->shutdown_requested)) {
        /* 1. Get random node from routing table */
        tree_node_t node;
        if (tree_routing_get_random_nodes(rt, &node, 1) < 1) {
            usleep(100000);  /* 100ms backoff if no nodes */
            continue;
        }

        /* 2. Send sample_infohashes to node */
        uint8_t target[20];
        generate_random_target(target);
        tree_send_sample_infohashes(tree, sock, target, &node.addr);

        /* 3. Wait for response with timeout */
        struct sockaddr_storage from;
        int recv_len = tree_socket_recv(sock, recv_buf, sizeof(recv_buf), &from, 500);

        if (recv_len > 0) {
            /* 4. Parse response, extract infohashes */
            tree_sample_response_t response;
            if (tree_handle_sample_infohashes_response(tree, recv_buf, recv_len, &from, &response) == 0) {
                /* 5. Push infohashes to queue (with bloom check) */
                for (int i = 0; i < response.infohash_count; i++) {
                    /* Check bloom filter (shared, thread-safe) */
                    if (tree->shared_bloom && bloom_filter_check(tree->shared_bloom, response.infohashes[i])) {
                        continue;  /* Already seen */
                    }

                    /* Try to push to queue (non-blocking) */
                    if (tree_infohash_queue_try_push(tree->infohash_queue, response.infohashes[i]) == 0) {
                        if (!first_infohash_found) {
                            first_infohash_found = true;
                            /* Trigger get_peers phase (Stage 4 placeholder) */
                            if (tree->current_phase == TREE_PHASE_BEP51) {
                                tree->current_phase = TREE_PHASE_GET_PEERS;
                                tree_start_get_peers_workers(tree);
                            }
                        }
                    }
                }

                /* 6. Update routing table with new nodes from response */
                for (int i = 0; i < response.node_count; i++) {
                    tree_routing_add_node(rt, response.nodes[i], &response.addrs[i]);
                }
            }
        }

        /* Rate limit */
        if (tree->bep51_query_interval_ms > 0) {
            usleep(tree->bep51_query_interval_ms * 1000);
        }
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d exiting", tree->tree_id, worker_id);
    return NULL;
}

/* Start BEP51 workers */
static void tree_start_bep51_workers(thread_tree_t *tree) {
    for (int i = 0; i < tree->num_bep51_workers; i++) {
        bep51_worker_ctx_t *ctx = malloc(sizeof(bep51_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate BEP51 worker context", tree->tree_id);
            continue;
        }
        ctx->tree = tree;
        ctx->worker_id = i;

        int rc = pthread_create(&tree->bep51_threads[i], NULL, bep51_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create BEP51 worker %d: %d", tree->tree_id, i, rc);
            free(ctx);
        }
    }
}

/* Get_peers worker context (Stage 4) */
typedef struct get_peers_worker_ctx {
    thread_tree_t *tree;
    int worker_id;
} get_peers_worker_ctx_t;

/* Get_peers worker thread function (Stage 4) */
static void *get_peers_worker_func(void *arg) {
    get_peers_worker_ctx_t *ctx = (get_peers_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;

    log_msg(LOG_DEBUG, "[tree %u] get_peers worker %d started", tree->tree_id, worker_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;
    tree_peers_queue_t *peers_queue = (tree_peers_queue_t *)tree->peers_queue;
    uint8_t recv_buf[2048];
    bool first_peers_found = false;

    while (!atomic_load(&tree->shutdown_requested)) {
        /* 1. Pop infohash from queue (with timeout) */
        uint8_t infohash[20];
        if (tree_infohash_queue_pop(tree->infohash_queue, infohash, 1000) < 0) {
            /* Check for shutdown */
            if (atomic_load(&tree->shutdown_requested)) {
                break;
            }
            continue;
        }

        /* 2. Get closest nodes from routing table */
        tree_node_t closest[8];
        int n = tree_routing_get_closest(rt, infohash, closest, 8);
        if (n == 0) {
            /* No nodes to query, skip this infohash */
            continue;
        }

        /* 3. Query nodes for peers (iterative lookup) */
        peer_entry_t result = {0};
        memcpy(result.infohash, infohash, 20);

        /* Simple iterative lookup: query initial nodes, then closer nodes from responses */
        int max_iterations = 3;
        for (int iter = 0; iter < max_iterations && result.peer_count < MAX_PEERS_PER_ENTRY; iter++) {
            int queries_this_round = (iter == 0) ? n : 0;

            /* Query each node */
            for (int i = 0; i < n && result.peer_count < MAX_PEERS_PER_ENTRY; i++) {
                tree_send_get_peers(tree, sock, infohash, &closest[i].addr);

                /* Wait for response with timeout */
                struct sockaddr_storage from;
                int recv_len = tree_socket_recv(sock, recv_buf, sizeof(recv_buf), &from, 500);

                if (recv_len > 0) {
                    tree_get_peers_response_t response;
                    if (tree_handle_get_peers_response(tree, recv_buf, recv_len, &from, &response) == 0) {
                        /* Collect peers */
                        for (int p = 0; p < response.peer_count && result.peer_count < MAX_PEERS_PER_ENTRY; p++) {
                            memcpy(&result.peers[result.peer_count], &response.peers[p],
                                   sizeof(struct sockaddr_storage));
                            result.peer_count++;
                        }

                        /* Add closer nodes for next iteration */
                        for (int ni = 0; ni < response.node_count && queries_this_round < 8; ni++) {
                            tree_routing_add_node(rt, response.nodes[ni], &response.node_addrs[ni]);
                            /* Could track these for next iteration, but keeping it simple */
                        }
                    }
                }
            }

            /* If we found peers, we're done */
            if (result.peer_count > 0) {
                break;
            }

            /* Get fresh closest nodes for next iteration */
            n = tree_routing_get_closest(rt, infohash, closest, 8);
            if (n == 0) break;
        }

        /* 4. If peers found, push to peers queue */
        if (result.peer_count > 0) {
            if (tree_peers_queue_try_push(peers_queue, &result) == 0) {
                /* Trigger metadata phase on first peers */
                if (!first_peers_found) {
                    first_peers_found = true;
                    if (tree->current_phase == TREE_PHASE_GET_PEERS) {
                        tree->current_phase = TREE_PHASE_METADATA;
                        tree_start_metadata_workers(tree);
                    }
                }
            }
        }
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] get_peers worker %d exiting", tree->tree_id, worker_id);
    return NULL;
}

/* Start get_peers workers (Stage 4) */
static void tree_start_get_peers_workers(thread_tree_t *tree) {
    log_msg(LOG_INFO, "[tree %u] Starting %d get_peers workers", tree->tree_id, tree->num_get_peers_workers);

    for (int i = 0; i < tree->num_get_peers_workers; i++) {
        get_peers_worker_ctx_t *ctx = malloc(sizeof(get_peers_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate get_peers worker context", tree->tree_id);
            continue;
        }
        ctx->tree = tree;
        ctx->worker_id = i;

        int rc = pthread_create(&tree->get_peers_threads[i], NULL, get_peers_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create get_peers worker %d: %d", tree->tree_id, i, rc);
            free(ctx);
        }
    }
}

/* Stage 5: Start metadata workers */
static void tree_start_metadata_workers(thread_tree_t *tree) {
    log_msg(LOG_INFO, "[tree %u] Starting %d metadata workers", tree->tree_id, tree->num_metadata_workers);

    for (int i = 0; i < tree->num_metadata_workers; i++) {
        metadata_worker_ctx_t *ctx = malloc(sizeof(metadata_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate metadata worker context", tree->tree_id);
            continue;
        }
        ctx->tree = tree;
        ctx->worker_id = i;

        int rc = pthread_create(&tree->metadata_threads[i], NULL, tree_metadata_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create metadata worker %d: %d", tree->tree_id, i, rc);
            free(ctx);
        }
    }

    /* Also start the rate monitor thread */
    tree_start_rate_monitor(tree);
}

/* OLD placeholder for metadata workers - disabled, kept for reference */
#if 0
static void tree_start_metadata_workers_OLD(thread_tree_t *tree) {
    log_msg(LOG_INFO, "[tree %u] metadata workers not yet implemented (Stage 5)", tree->tree_id);
    /* TODO: Implement in Stage 5 */
    (void)tree;
}
#endif

/* Stage 5: Start rate monitor thread */
static void tree_start_rate_monitor(thread_tree_t *tree) {
    rate_monitor_ctx_t *ctx = malloc(sizeof(rate_monitor_ctx_t));
    if (!ctx) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate rate monitor context", tree->tree_id);
        return;
    }

    ctx->tree = tree;
    ctx->min_metadata_rate = tree->min_metadata_rate;
    ctx->check_interval_sec = tree->rate_check_interval_sec;
    ctx->grace_period_sec = tree->rate_grace_period_sec;

    int rc = pthread_create(&tree->rate_monitor_thread, NULL, tree_rate_monitor_func, ctx);
    if (rc != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create rate monitor thread: %d", tree->tree_id, rc);
        free(ctx);
    } else {
        log_msg(LOG_INFO, "[tree %u] Rate monitor started (min_rate=%.2f/s)",
                tree->tree_id, tree->min_metadata_rate);
    }
}

/* OLD placeholder for get_peers workers - disabled, kept for reference */
#if 0
static void tree_start_get_peers_workers_OLD(thread_tree_t *tree) {
    log_msg(LOG_INFO, "[tree %u] get_peers workers not yet implemented (Stage 4)", tree->tree_id);
    /* TODO: Implement in Stage 4 */
    (void)tree;
}
#endif

/* Start BEP51 phase */
static void tree_start_bep51_phase(thread_tree_t *tree) {
    log_msg(LOG_INFO, "[tree %u] Transitioning to BEP51 phase with %d nodes",
            tree->tree_id, tree_routing_get_count(tree->routing_table));
    tree->current_phase = TREE_PHASE_BEP51;

    /* Start BEP51 worker threads */
    tree_start_bep51_workers(tree);
}

/* Bootstrap thread function */
static void *bootstrap_thread_func(void *arg) {
    thread_tree_t *tree = (thread_tree_t *)arg;

    log_msg(LOG_INFO, "[tree %u] Bootstrap thread started", tree->tree_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;

    if (!rt || !sock) {
        log_msg(LOG_ERROR, "[tree %u] Missing routing table or socket", tree->tree_id);
        goto shutdown;
    }

    /* Increase bucket capacity during bootstrap to allow more nodes per bucket
     * This helps overcome the issue where bootstrap discovers nodes in similar
     * keyspace regions that fill only a few buckets. Default k=8, bootstrap k=20 */
    tree_routing_set_bucket_capacity(rt, 20);
    log_msg(LOG_DEBUG, "[tree %u] Bootstrap mode: increased bucket capacity to 20", tree->tree_id);

    /* Start bootstrap find_node worker threads to continuously query routing table */
    log_msg(LOG_INFO, "[tree %u] Starting %d bootstrap find_node workers",
            tree->tree_id, tree->num_bootstrap_workers);
    for (int i = 0; i < tree->num_bootstrap_workers; i++) {
        bootstrap_worker_ctx_t *ctx = malloc(sizeof(bootstrap_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate bootstrap worker context", tree->tree_id);
            continue;
        }
        ctx->tree = tree;
        ctx->worker_id = i;

        int rc = pthread_create(&tree->bootstrap_workers[i], NULL, bootstrap_find_node_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create bootstrap worker %d: %d",
                    tree->tree_id, i, rc);
            free(ctx);
        }
    }

    /* Query bootstrap nodes */
    for (int i = 0; BOOTSTRAP_NODES[i] != NULL && !atomic_load(&tree->shutdown_requested); i++) {
        struct sockaddr_storage addr;
        if (resolve_hostname(BOOTSTRAP_NODES[i], BOOTSTRAP_PORT, &addr) == 0) {
            log_msg(LOG_DEBUG, "[tree %u] Sending find_node to %s",
                    tree->tree_id, BOOTSTRAP_NODES[i]);
            tree_send_find_node(tree, sock, tree->node_id, &addr);
        }
    }

    /* Bootstrap loop: query and process responses */
    time_t start_time = time(NULL);
    uint8_t recv_buf[2048];
    int queries_sent = 0;
    int last_node_count = 0;
    int stall_counter = 0;       /* Stall detection counter (tree-local, not static) */
    int loop_iterations = 0;     /* Track loop iterations for proactive queries */

    while (!atomic_load(&tree->shutdown_requested)) {
        loop_iterations++;
        /* Check timeout */
        time_t elapsed = time(NULL) - start_time;
        if (elapsed > tree->bootstrap_timeout_sec && tree_routing_get_count(rt) < 10) {
            log_msg(LOG_WARN, "[tree %u] Bootstrap timeout with only %d nodes",
                    tree->tree_id, tree_routing_get_count(rt));
            /* Continue anyway, maybe we'll get more nodes */
        }

        /* Check if we've reached the threshold */
        int node_count = tree_routing_get_count(rt);
        if (node_count >= tree->routing_threshold) {
            log_msg(LOG_INFO, "[tree %u] Reached routing threshold (%d nodes)",
                    tree->tree_id, node_count);
            /* Reset bucket capacity to normal k=8 */
            tree_routing_set_bucket_capacity(rt, 8);
            log_msg(LOG_DEBUG, "[tree %u] Bootstrap complete: reset bucket capacity to 8", tree->tree_id);
            tree_start_bep51_phase(tree);
            break;
        }

        /* Log progress periodically */
        if (node_count % 50 == 0 && node_count != last_node_count) {
            log_msg(LOG_DEBUG, "[tree %u] Bootstrap progress: %d/%d nodes",
                    tree->tree_id, node_count, tree->routing_threshold);
        }

        /* Detect stalled bootstrap - aggressive mode disabled per user request */
        /* Just let stall_counter track stalls without triggering aggressive queries */
        if (node_count == last_node_count && node_count < tree->routing_threshold && node_count > 0) {
            stall_counter++;
            /* Aggressive mode disabled - let normal bootstrap queries handle it */
            if (stall_counter >= 10 && (stall_counter % 50) == 0) {
                /* Log stall status periodically, but don't flood the network */
                log_msg(LOG_DEBUG, "[tree %u] Bootstrap progress slow at %d nodes (stalled %d iterations)",
                        tree->tree_id, node_count, stall_counter);
            }
        } else if (node_count != last_node_count) {
            stall_counter = 0;
        }

        /* Retransmit bootstrap queries periodically if we have zero nodes */
        if (node_count == 0 && (loop_iterations % 20) == 0) {  /* Every ~1 second (20 * 50ms) */
            log_msg(LOG_DEBUG, "[tree %u] No nodes yet, retrying bootstrap nodes (iteration %d)",
                    tree->tree_id, loop_iterations);
            for (int i = 0; BOOTSTRAP_NODES[i] != NULL; i++) {
                struct sockaddr_storage addr;
                if (resolve_hostname(BOOTSTRAP_NODES[i], BOOTSTRAP_PORT, &addr) == 0) {
                    tree_send_find_node(tree, sock, tree->node_id, &addr);
                    queries_sent++;
                }
            }
        }

        /* PROACTIVE QUERYING: Query random nodes every N iterations to explore keyspace */
        /* During bootstrap, be VERY aggressive to fill empty buckets across entire keyspace */
        /* Use RANDOM targets to discover nodes in different keyspace regions */
        int query_frequency = (node_count < tree->routing_threshold) ? 2 : 10;  /* Every 2 iterations during bootstrap */
        int nodes_to_query = (node_count < tree->routing_threshold) ? 16 : 8;   /* Query more nodes during bootstrap */

        if (loop_iterations % query_frequency == 0 && node_count > 10) {
            tree_node_t random_nodes[16];
            int got = tree_routing_get_random_nodes(rt, random_nodes, nodes_to_query);
            for (int i = 0; i < got && queries_sent < 3000; i++) {
                uint8_t random_target[20];
                generate_random_target(random_target);
                tree_send_find_node(tree, sock, random_target, &random_nodes[i].addr);
                queries_sent++;
            }
        }

        /* Receive and process responses (reduced timeout from 100ms to 50ms for faster iterations) */
        struct sockaddr_storage from;
        int recv_len = tree_socket_recv(sock, recv_buf, sizeof(recv_buf), &from, 50);

        if (recv_len > 0) {
            tree_find_node_response_t response;
            memset(&response, 0, sizeof(response));

            tree_response_type_t resp_type = tree_handle_response(
                tree, recv_buf, recv_len, &from, &response);

            if (resp_type == TREE_RESP_FIND_NODE && response.node_count > 0) {
                /* Add discovered nodes to routing table */
                for (int i = 0; i < response.node_count; i++) {
                    tree_routing_add_node(rt, response.nodes[i], &response.addrs[i]);
                }

                /* Query some of the new nodes for more nodes (increased from 3 to 8) */
                /* Use random targets to explore different parts of the keyspace */
                int to_query = (response.node_count < 8) ? response.node_count : 8;
                for (int i = 0; i < to_query && queries_sent < 2000; i++) {
                    uint8_t random_target[20];
                    generate_random_target(random_target);
                    tree_send_find_node(tree, sock, random_target, &response.addrs[i]);
                    queries_sent++;
                }
            }
        }

        /* Response-based random node queries: more aggressive after initial bootstrap */
        /* After 50 nodes, query every 3rd response. Before 50, query every 5th to avoid overwhelming network */
        /* Use random targets for keyspace exploration */
        int query_interval = (node_count >= 50) ? 3 : 5;
        if (queries_sent % query_interval == 0 && node_count > 0) {
            tree_node_t random_nodes[8];
            int got = tree_routing_get_random_nodes(rt, random_nodes, 8);
            for (int i = 0; i < got && queries_sent < 3000; i++) {
                uint8_t random_target[20];
                generate_random_target(random_target);
                tree_send_find_node(tree, sock, random_target, &random_nodes[i].addr);
                queries_sent++;
            }
        }

        /* Update last_node_count at end of iteration for accurate stall detection */
        last_node_count = node_count;
    }

    /* BEP51 phase loop (placeholder - wait until shutdown) */
    while (!atomic_load(&tree->shutdown_requested) && tree->current_phase == TREE_PHASE_BEP51) {
        struct timespec ts = {0, 100000000};  /* 100ms */
        nanosleep(&ts, NULL);
    }

shutdown:
    log_msg(LOG_INFO, "[tree %u] Bootstrap thread exiting (final: %d nodes)",
            tree->tree_id, tree->routing_table ? tree_routing_get_count(tree->routing_table) : 0);

    /* Notify supervisor of shutdown completion */
    if (tree->on_shutdown) {
        tree->on_shutdown(tree);
    }

    return NULL;
}

thread_tree_t *thread_tree_create(uint32_t tree_id, tree_config_t *config) {
    if (!config) {
        log_msg(LOG_ERROR, "[tree %u] NULL config provided", tree_id);
        return NULL;
    }

    thread_tree_t *tree = calloc(1, sizeof(thread_tree_t));
    if (!tree) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate thread tree", tree_id);
        return NULL;
    }

    tree->tree_id = tree_id;
    generate_random_node_id(tree->node_id);

    /* Stage 2 config */
    tree->bootstrap_timeout_sec = config->bootstrap_timeout_sec > 0 ? config->bootstrap_timeout_sec : 30;
    tree->routing_threshold = config->routing_threshold > 0 ? config->routing_threshold : 500;

    /* Stage 3 config */
    tree->infohash_queue_capacity = config->infohash_queue_capacity > 0 ? config->infohash_queue_capacity : 5000;
    tree->bep51_query_interval_ms = config->bep51_query_interval_ms >= 0 ? config->bep51_query_interval_ms : 10;
    tree->shared_bloom = config->bloom_filter;

    /* Stage 4 config */
    tree->peers_queue_capacity = config->peers_queue_capacity > 0 ? config->peers_queue_capacity : 2000;
    tree->get_peers_timeout_ms = config->get_peers_timeout_ms > 0 ? config->get_peers_timeout_ms : 3000;

    /* Stage 5 config */
    tree->min_metadata_rate = config->min_metadata_rate > 0 ? config->min_metadata_rate : 0.5;
    tree->rate_check_interval_sec = config->rate_check_interval_sec > 0 ? config->rate_check_interval_sec : 10;
    tree->rate_grace_period_sec = config->rate_grace_period_sec > 0 ? config->rate_grace_period_sec : 30;
    tree->tcp_connect_timeout_ms = config->tcp_connect_timeout_ms > 0 ? config->tcp_connect_timeout_ms : 5000;
    tree->shared_batch_writer = config->batch_writer;

    /* Initialize thread counts from config */
    tree->num_bootstrap_workers = config->num_bootstrap_workers > 0 ? config->num_bootstrap_workers : 10;
    tree->num_bep51_workers = config->num_bep51_workers;
    tree->num_get_peers_workers = config->num_get_peers_workers;
    tree->num_metadata_workers = config->num_metadata_workers;

    /* Supervisor callbacks */
    tree->on_shutdown = config->on_shutdown;
    tree->supervisor_ctx = config->supervisor_ctx;

    /* Initialize phase and shutdown flag */
    tree->current_phase = TREE_PHASE_BOOTSTRAP;
    atomic_store(&tree->shutdown_requested, false);

    /* Initialize statistics */
    atomic_store(&tree->metadata_count, 0);
    atomic_store(&tree->last_metadata_time, 0);
    tree->metadata_rate = 0.0;

    /* Allocate thread handle arrays */
    if (tree->num_bootstrap_workers > 0) {
        tree->bootstrap_workers = calloc(tree->num_bootstrap_workers, sizeof(pthread_t));
        if (!tree->bootstrap_workers) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate bootstrap workers", tree_id);
            thread_tree_destroy(tree);
            return NULL;
        }
    }

    if (tree->num_bep51_workers > 0) {
        tree->bep51_threads = calloc(tree->num_bep51_workers, sizeof(pthread_t));
        if (!tree->bep51_threads) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate bep51 threads", tree_id);
            thread_tree_destroy(tree);
            return NULL;
        }
    }

    if (tree->num_get_peers_workers > 0) {
        tree->get_peers_threads = calloc(tree->num_get_peers_workers, sizeof(pthread_t));
        if (!tree->get_peers_threads) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate get_peers threads", tree_id);
            thread_tree_destroy(tree);
            return NULL;
        }
    }

    if (tree->num_metadata_workers > 0) {
        tree->metadata_threads = calloc(tree->num_metadata_workers, sizeof(pthread_t));
        if (!tree->metadata_threads) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate metadata threads", tree_id);
            thread_tree_destroy(tree);
            return NULL;
        }
    }

    /* Stage 2: Initialize private routing table */
    tree->routing_table = tree_routing_create(tree->node_id);
    if (!tree->routing_table) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create routing table", tree_id);
        thread_tree_destroy(tree);
        return NULL;
    }

    /* Stage 2: Create private UDP socket (port 0 = random) */
    tree->socket = tree_socket_create(0);
    if (!tree->socket) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create socket", tree_id);
        thread_tree_destroy(tree);
        return NULL;
    }

    /* Stage 3: Create private infohash queue */
    tree->infohash_queue = tree_infohash_queue_create(tree->infohash_queue_capacity);
    if (!tree->infohash_queue) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create infohash queue", tree_id);
        thread_tree_destroy(tree);
        return NULL;
    }

    /* Stage 4: Create private peers queue */
    tree->peers_queue = tree_peers_queue_create(tree->peers_queue_capacity);
    if (!tree->peers_queue) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create peers queue", tree_id);
        thread_tree_destroy(tree);
        return NULL;
    }

    int port = tree_socket_get_port(tree->socket);
    log_msg(LOG_INFO, "[tree %u] Created with node_id %02x%02x%02x%02x... on port %d",
            tree_id, tree->node_id[0], tree->node_id[1], tree->node_id[2], tree->node_id[3], port);

    return tree;
}

void thread_tree_destroy(thread_tree_t *tree) {
    if (!tree) {
        return;
    }

    log_msg(LOG_INFO, "[tree %u] Destroying", tree->tree_id);

    /* Request shutdown if not already done */
    thread_tree_request_shutdown(tree);

    /* Join bootstrap thread */
    log_msg(LOG_DEBUG, "[tree %u] Joining bootstrap thread...", tree->tree_id);
    if (tree->bootstrap_thread) {
        pthread_join(tree->bootstrap_thread, NULL);
    }
    log_msg(LOG_DEBUG, "[tree %u] Bootstrap thread joined", tree->tree_id);

    /* Join bootstrap workers */
    log_msg(LOG_DEBUG, "[tree %u] Joining %d bootstrap workers...", tree->tree_id, tree->num_bootstrap_workers);
    if (tree->bootstrap_workers) {
        for (int i = 0; i < tree->num_bootstrap_workers; i++) {
            if (tree->bootstrap_workers[i]) {
                pthread_join(tree->bootstrap_workers[i], NULL);
            }
        }
        free(tree->bootstrap_workers);
    }
    log_msg(LOG_DEBUG, "[tree %u] Bootstrap workers joined", tree->tree_id);

    /* Join BEP51 workers */
    log_msg(LOG_DEBUG, "[tree %u] Joining %d BEP51 workers...", tree->tree_id, tree->num_bep51_workers);
    if (tree->bep51_threads) {
        for (int i = 0; i < tree->num_bep51_workers; i++) {
            if (tree->bep51_threads[i]) {
                pthread_join(tree->bep51_threads[i], NULL);
            }
        }
        free(tree->bep51_threads);
    }
    log_msg(LOG_DEBUG, "[tree %u] BEP51 workers joined", tree->tree_id);

    /* Join get_peers workers */
    log_msg(LOG_DEBUG, "[tree %u] Joining %d get_peers workers...", tree->tree_id, tree->num_get_peers_workers);
    if (tree->get_peers_threads) {
        for (int i = 0; i < tree->num_get_peers_workers; i++) {
            if (tree->get_peers_threads[i]) {
                pthread_join(tree->get_peers_threads[i], NULL);
            }
        }
        free(tree->get_peers_threads);
    }
    log_msg(LOG_DEBUG, "[tree %u] get_peers workers joined", tree->tree_id);

    /* Join metadata workers */
    log_msg(LOG_DEBUG, "[tree %u] Joining %d metadata workers...", tree->tree_id, tree->num_metadata_workers);
    if (tree->metadata_threads) {
        for (int i = 0; i < tree->num_metadata_workers; i++) {
            if (tree->metadata_threads[i]) {
                pthread_join(tree->metadata_threads[i], NULL);
            }
        }
        free(tree->metadata_threads);
    }
    log_msg(LOG_DEBUG, "[tree %u] Metadata workers joined", tree->tree_id);

    /* Stage 5: Join rate monitor thread */
    log_msg(LOG_DEBUG, "[tree %u] Joining rate monitor thread...", tree->tree_id);
    if (tree->rate_monitor_thread) {
        pthread_join(tree->rate_monitor_thread, NULL);
    }
    log_msg(LOG_DEBUG, "[tree %u] Rate monitor thread joined", tree->tree_id);

    /* Stage 2: Cleanup private data structures */
    if (tree->routing_table) {
        tree_routing_destroy(tree->routing_table);
    }
    if (tree->socket) {
        tree_socket_destroy(tree->socket);
    }

    /* Stage 3: Cleanup infohash queue */
    if (tree->infohash_queue) {
        tree_infohash_queue_destroy(tree->infohash_queue);
    }

    /* Stage 4: Cleanup peers queue */
    if (tree->peers_queue) {
        tree_peers_queue_destroy(tree->peers_queue);
    }

    log_msg(LOG_INFO, "[tree %u] Destroyed (metadata_count=%lu)",
            tree->tree_id, (unsigned long)atomic_load(&tree->metadata_count));

    free(tree);
}

void thread_tree_start(thread_tree_t *tree) {
    if (!tree) {
        return;
    }

    log_msg(LOG_INFO, "[tree %u] Starting (phase: %s)",
            tree->tree_id, thread_tree_phase_name(tree->current_phase));

    /* Spawn bootstrap thread */
    int rc = pthread_create(&tree->bootstrap_thread, NULL, bootstrap_thread_func, tree);
    if (rc != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create bootstrap thread: %d", tree->tree_id, rc);
        return;
    }
}

void thread_tree_request_shutdown(thread_tree_t *tree) {
    if (!tree) {
        return;
    }

    bool expected = false;
    if (atomic_compare_exchange_strong(&tree->shutdown_requested, &expected, true)) {
        log_msg(LOG_INFO, "[tree %u] Shutdown requested", tree->tree_id);
        tree->current_phase = TREE_PHASE_SHUTTING_DOWN;

        /* Stage 3: Signal infohash queue to wake up waiting threads */
        if (tree->infohash_queue) {
            tree_infohash_queue_signal_shutdown(tree->infohash_queue);
        }

        /* Stage 4: Signal peers queue to wake up waiting threads */
        if (tree->peers_queue) {
            tree_peers_queue_signal_shutdown(tree->peers_queue);
        }
    }
}
