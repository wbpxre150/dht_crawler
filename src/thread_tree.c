#define _DEFAULT_SOURCE  /* For usleep */

#include "thread_tree.h"
#include "tree_routing.h"
#include "tree_socket.h"
#include "tree_protocol.h"
#include "tree_infohash_queue.h"
#include "tree_peers_queue.h"
#include "tree_metadata.h"
#include "tree_dispatcher.h"
#include "tree_response_queue.h"
#include "bloom_filter.h"
#include "shared_node_pool.h"
#include "supervisor.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/socket.h>

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

/* BEP51 worker context */
typedef struct bep51_worker_ctx {
    thread_tree_t *tree;
    int worker_id;
    tree_response_queue_t *response_queue;  /* Per-worker response queue */
} bep51_worker_ctx_t;

/* Generate random 20-byte target for BEP51 queries */
static void generate_random_target(uint8_t *target) {
    for (int i = 0; i < 20; i++) {
        target[i] = (uint8_t)(rand() % 256);
    }
}

/* Forward declarations */
static void tree_start_find_node_workers(thread_tree_t *tree);
static void tree_start_get_peers_workers(thread_tree_t *tree);
static void tree_start_metadata_workers(thread_tree_t *tree);
static void tree_start_rate_monitor(thread_tree_t *tree);

/* find_node worker context */
typedef struct find_node_worker_ctx {
    thread_tree_t *tree;
    int worker_id;
    tree_response_queue_t *response_queue;  /* Per-worker response queue */
} find_node_worker_ctx_t;

/* find_node worker thread function - NEW DISPATCHER PATTERN */
static void *find_node_worker_func(void *arg) {
    find_node_worker_ctx_t *ctx = (find_node_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;
    tree_response_queue_t *my_queue = ctx->response_queue;

    log_msg(LOG_DEBUG, "[tree %u] ===== find_node worker %d STARTED =====", tree->tree_id, worker_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;
    tree_dispatcher_t *dispatcher = tree->dispatcher;
    static int query_count = 0;

    /* DEBUG: Log socket info */
    int sock_fd = sock ? sock->fd : -1;
    int sock_port = tree_socket_get_port(sock);
    log_msg(LOG_DEBUG, "[tree %u] find_node worker %d: socket_fd=%d, port=%d",
            tree->tree_id, worker_id, sock_fd, sock_port);

    unsigned long queries_sent = 0;
    unsigned long responses_received = 0;
    unsigned long timeouts = 0;

    while (!atomic_load(&tree->shutdown_requested)) {
        /* 0. Check if we should pause (infohash queue is full enough) */
        if (atomic_load(&tree->discovery_paused)) {
            log_msg(LOG_DEBUG, "[tree %u] find_node worker %d PAUSED (queue >= %d)",
                    tree->tree_id, worker_id, tree->infohash_pause_threshold);

            /* Wait on condition variable until resumed */
            pthread_mutex_lock(&tree->throttle_lock);
            while (atomic_load(&tree->discovery_paused) &&
                   !atomic_load(&tree->shutdown_requested)) {
                pthread_cond_wait(&tree->throttle_resume, &tree->throttle_lock);
            }
            pthread_mutex_unlock(&tree->throttle_lock);

            log_msg(LOG_DEBUG, "[tree %u] find_node worker %d RESUMED (queue < %d)",
                    tree->tree_id, worker_id, tree->infohash_resume_threshold);

            if (atomic_load(&tree->shutdown_requested)) break;
        }

        /* 1. Check routing table size and throttle aggressively when full */
        int current_nodes = tree_routing_get_count(rt);
        int target_nodes = 1500;  /* Target from triple_routing_threshold config */

        if (current_nodes >= target_nodes) {
            /* Routing table is full (>= 1500 nodes) - aggressive throttle */
            /* Sleep 10 seconds, then do minimal maintenance (1 query) */
            usleep(10000000);  /* 10 seconds */

            /* After sleep, do just 1 maintenance query then go back to sleep */
            tree_node_t nodes[1];
            int n = tree_routing_get_random_nodes(rt, nodes, 1);
            if (n < 1) {
                continue;  /* No nodes, try again after next sleep */
            }

            /* Single maintenance query */
            uint8_t target[20];
            generate_random_target(target);
            uint8_t tid[4];
            int tid_len = tree_protocol_gen_tid(tid);
            tree_dispatcher_register_tid(dispatcher, tid, tid_len, my_queue);
            tree_send_find_node(tree, sock, tid, tid_len, target, &nodes[0].addr);
            queries_sent++;

            tree_response_t response_pkt;
            tree_response_queue_pop(my_queue, &response_pkt, 500);
            tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);

            continue;  /* Go back to sleep check */

        } else if (current_nodes >= (int)(target_nodes * 0.9)) {
            /* 90-100% full (1350-1500 nodes) - heavy throttle */
            usleep(2000000);  /* 2 seconds between query batches */
        } else if (current_nodes >= (int)(target_nodes * 0.7)) {
            /* 70-90% full (1050-1350 nodes) - moderate throttle */
            usleep(500000);  /* 500ms between query batches */
        }
        /* else: < 70% full (< 1050 nodes) - aggressive discovery mode, no throttle */

        /* 1. Get random nodes from routing table */
        tree_node_t nodes[3];
        int n = tree_routing_get_random_nodes(rt, nodes, 3);

        if (n < 1) {
            /* DEBUG: Log when we have no nodes */
            if (queries_sent == 0) {
                log_msg(LOG_WARN, "[tree %u] find_node worker %d: NO NODES in routing table (count=%d)",
                        tree->tree_id, worker_id, tree_routing_get_count(rt));
            }
            usleep(100000);  /* 100ms backoff if no nodes */
            continue;
        }

        /* 2. Query each node with find_node */
        for (int i = 0; i < n && !atomic_load(&tree->shutdown_requested); i++) {
            /* Generate random target for broad keyspace coverage */
            uint8_t target[20];
            generate_random_target(target);

            /* Generate TID and register with dispatcher */
            uint8_t tid[4];
            int tid_len = tree_protocol_gen_tid(tid);
            tree_dispatcher_register_tid(dispatcher, tid, tid_len, my_queue);

            /* DEBUG: Log node we're querying */
            char node_ip[INET6_ADDRSTRLEN] = {0};
            uint16_t node_port = 0;
            if (nodes[i].addr.ss_family == AF_INET) {
                struct sockaddr_in *sin = (struct sockaddr_in *)&nodes[i].addr;
                inet_ntop(AF_INET, &sin->sin_addr, node_ip, sizeof(node_ip));
                node_port = ntohs(sin->sin_port);
            }

            /* Send find_node query with our TID */
            int send_result = tree_send_find_node(tree, sock, tid, tid_len, target, &nodes[i].addr);
            queries_sent++;

            /* DEBUG: Log every send */
            if (queries_sent <= 10 || queries_sent % 100 == 0) {
                log_msg(LOG_DEBUG, "[tree %u] find_node worker %d: SENT query #%lu (TID=%02x%02x%02x) to %s:%u (result=%d)",
                        tree->tree_id, worker_id, queries_sent, tid[0], tid[1], tid[2],
                        node_ip, node_port, send_result);
            }

            /* 3. Wait for response on personal queue (500ms timeout) */
            tree_response_t response_pkt;
            int rc = tree_response_queue_pop(my_queue, &response_pkt, 500);

            /* Unregister TID immediately after receiving response (or timeout) */
            tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);

            if (rc == 0) {
                responses_received++;
                /* DEBUG: Log response receipt */
                if (responses_received <= 10 || responses_received % 100 == 0) {
                    log_msg(LOG_DEBUG, "[tree %u] find_node worker %d: RECEIVED response #%lu (TID=%02x%02x%02x)",
                            tree->tree_id, worker_id, responses_received, tid[0], tid[1], tid[2]);
                }
            } else {
                timeouts++;
                /* DEBUG: Log timeout */
                if (timeouts <= 10 || timeouts % 100 == 0) {
                    log_msg(LOG_DEBUG, "[tree %u] find_node worker %d: TIMEOUT #%lu (TID=%02x%02x%02x)",
                            tree->tree_id, worker_id, timeouts, tid[0], tid[1], tid[2]);
                }
            }

            if (rc == 0) {
                /* 4. Parse response and insert discovered nodes */
                tree_find_node_response_t response;
                if (tree_handle_response(tree, response_pkt.data, response_pkt.len,
                                        &response_pkt.from, &response) == TREE_RESP_FIND_NODE) {
                    /* Add all discovered nodes to routing table */
                    for (int j = 0; j < response.node_count; j++) {
                        tree_routing_add_node(rt, response.nodes[j], &response.addrs[j]);
                    }

                    /* Log discovery occasionally (every 100 queries) with routing table stats */
                    if (++query_count % 100 == 0) {
                        int rt_count = tree_routing_get_count(rt);
                        log_msg(LOG_DEBUG, "[tree %u] find_node worker %d: discovered %d nodes (total: %d, target: ~1500)",
                                tree->tree_id, worker_id, response.node_count, rt_count);
                    }
                }
            }

            /* No rate limit here - throttling is adaptive at top of loop based on routing table size
             * < 70% full (< 1050 nodes): no delay - aggressive discovery
             * 70-90% full: 500ms delay between batches
             * 90-100% full: 2s delay between batches
             * >= 100% full: 10s delay, single maintenance query */
        }
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] find_node worker %d exiting", tree->tree_id, worker_id);
    return NULL;
}

/* Throttle monitor context */
typedef struct throttle_monitor_ctx {
    thread_tree_t *tree;
} throttle_monitor_ctx_t;

/* Throttle monitor thread - checks infohash queue size and pauses/resumes discovery workers (find_node + BEP51) */
static void *throttle_monitor_func(void *arg) {
    throttle_monitor_ctx_t *ctx = (throttle_monitor_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;

    log_msg(LOG_DEBUG, "[tree %u] Throttle monitor started", tree->tree_id);

    bool currently_paused = false;

    while (!atomic_load(&tree->shutdown_requested)) {
        /* Check infohash queue size */
        int queue_size = tree_infohash_queue_count(tree->infohash_queue);

        if (!currently_paused && queue_size >= tree->infohash_pause_threshold) {
            /* Pause discovery workers (find_node + BEP51) */
            log_msg(LOG_INFO, "[tree %u] PAUSING discovery workers (find_node + BEP51) (queue=%d >= %d)",
                    tree->tree_id, queue_size, tree->infohash_pause_threshold);

            pthread_mutex_lock(&tree->throttle_lock);
            atomic_store(&tree->discovery_paused, true);
            pthread_mutex_unlock(&tree->throttle_lock);

            currently_paused = true;

        } else if (currently_paused && queue_size < tree->infohash_resume_threshold) {
            /* Resume discovery workers (find_node + BEP51) */
            log_msg(LOG_INFO, "[tree %u] RESUMING discovery workers (find_node + BEP51) (queue=%d < %d)",
                    tree->tree_id, queue_size, tree->infohash_resume_threshold);

            pthread_mutex_lock(&tree->throttle_lock);
            atomic_store(&tree->discovery_paused, false);
            pthread_cond_broadcast(&tree->throttle_resume);  /* Wake all workers */
            pthread_mutex_unlock(&tree->throttle_lock);

            currently_paused = false;
        }

        /* Check every 2 seconds (responsive but not too aggressive) */
        usleep(2000000);
    }

    /* On shutdown, resume all workers so they can exit cleanly */
    if (currently_paused) {
        pthread_mutex_lock(&tree->throttle_lock);
        atomic_store(&tree->discovery_paused, false);
        pthread_cond_broadcast(&tree->throttle_resume);
        pthread_mutex_unlock(&tree->throttle_lock);
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] Throttle monitor exiting", tree->tree_id);
    return NULL;
}

/* Start throttle monitor */
static void tree_start_throttle_monitor(thread_tree_t *tree) {
    log_msg(LOG_DEBUG, "[tree %u] Starting throttle monitor", tree->tree_id);

    throttle_monitor_ctx_t *ctx = malloc(sizeof(throttle_monitor_ctx_t));
    if (!ctx) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate throttle monitor context", tree->tree_id);
        return;
    }
    ctx->tree = tree;

    if (pthread_create(&tree->throttle_monitor_thread, NULL, throttle_monitor_func, ctx) != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create throttle monitor thread", tree->tree_id);
        free(ctx);
    }
}

/* Start find_node workers */
static void tree_start_find_node_workers(thread_tree_t *tree) {
    if (tree->num_find_node_workers <= 0) {
        log_msg(LOG_DEBUG, "[tree %u] find_node workers disabled (count=0)", tree->tree_id);
        return;
    }

    log_msg(LOG_DEBUG, "[tree %u] Starting %d find_node workers", tree->tree_id, tree->num_find_node_workers);

    for (int i = 0; i < tree->num_find_node_workers; i++) {
        find_node_worker_ctx_t *ctx = malloc(sizeof(find_node_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate find_node worker context", tree->tree_id);
            continue;
        }
        ctx->tree = tree;
        ctx->worker_id = i;

        /* Create per-worker response queue (capacity 10 = buffer up to 10 responses) */
        ctx->response_queue = tree_response_queue_create(10);
        if (!ctx->response_queue) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create find_node worker %d response queue", tree->tree_id, i);
            free(ctx);
            continue;
        }

        int rc = pthread_create(&tree->find_node_workers[i], NULL, find_node_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create find_node worker %d: %d", tree->tree_id, i, rc);
            tree_response_queue_destroy(ctx->response_queue);
            free(ctx);
        }
    }
}

/* BEP51 worker thread function - NEW DISPATCHER PATTERN */
static void *bep51_worker_func(void *arg) {
    bep51_worker_ctx_t *ctx = (bep51_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;
    tree_response_queue_t *my_queue = ctx->response_queue;

    log_msg(LOG_DEBUG, "[tree %u] ===== BEP51 worker %d STARTED =====", tree->tree_id, worker_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;
    tree_dispatcher_t *dispatcher = tree->dispatcher;
    bool first_infohash_found = false;

    /* DEBUG: Log socket info */
    int sock_fd = sock ? sock->fd : -1;
    int sock_port = tree_socket_get_port(sock);
    log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d: socket_fd=%d, port=%d",
            tree->tree_id, worker_id, sock_fd, sock_port);

    unsigned long queries_sent = 0;
    unsigned long responses_received = 0;
    unsigned long timeouts = 0;

    while (!atomic_load(&tree->shutdown_requested)) {
        /* 0. Check if we should pause (infohash queue is full enough) */
        if (atomic_load(&tree->discovery_paused)) {
            log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d PAUSED (queue >= %d)",
                    tree->tree_id, worker_id, tree->infohash_pause_threshold);

            /* Wait on condition variable until resumed */
            pthread_mutex_lock(&tree->throttle_lock);
            while (atomic_load(&tree->discovery_paused) &&
                   !atomic_load(&tree->shutdown_requested)) {
                pthread_cond_wait(&tree->throttle_resume, &tree->throttle_lock);
            }
            pthread_mutex_unlock(&tree->throttle_lock);

            log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d RESUMED (queue < %d)",
                    tree->tree_id, worker_id, tree->infohash_resume_threshold);

            if (atomic_load(&tree->shutdown_requested)) break;
        }

        /* 1. Get random node from routing table */
        tree_node_t node;
        if (tree_routing_get_random_nodes(rt, &node, 1) < 1) {
            /* DEBUG: Log when we have no nodes */
            if (queries_sent == 0) {
                log_msg(LOG_WARN, "[tree %u] BEP51 worker %d: NO NODES in routing table (count=%d)",
                        tree->tree_id, worker_id, tree_routing_get_count(rt));
            }
            usleep(100000);  /* 100ms backoff if no nodes */
            continue;
        }

        /* 2. Generate TID and register with dispatcher BEFORE sending */
        uint8_t tid[4];
        int tid_len = tree_protocol_gen_tid(tid);
        tree_dispatcher_register_tid(dispatcher, tid, tid_len, my_queue);

        /* DEBUG: Log node we're querying */
        char node_ip[INET6_ADDRSTRLEN] = {0};
        uint16_t node_port = 0;
        if (node.addr.ss_family == AF_INET) {
            struct sockaddr_in *sin = (struct sockaddr_in *)&node.addr;
            inet_ntop(AF_INET, &sin->sin_addr, node_ip, sizeof(node_ip));
            node_port = ntohs(sin->sin_port);
        }

        /* 3. Send sample_infohashes to node with our TID */
        uint8_t target[20];
        generate_random_target(target);
        int send_result = tree_send_sample_infohashes(tree, sock, tid, tid_len, target, &node.addr);
        queries_sent++;

        /* DEBUG: Log every send */
        if (queries_sent <= 10 || queries_sent % 100 == 0) {
            log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d: SENT query #%lu (TID=%02x%02x%02x) to %s:%u (result=%d)",
                    tree->tree_id, worker_id, queries_sent, tid[0], tid[1], tid[2],
                    node_ip, node_port, send_result);
        }

        /* 4. Wait for response on personal queue (1000ms timeout) */
        tree_response_t response_pkt;
        int rc = tree_response_queue_pop(my_queue, &response_pkt, 1000);

        /* Unregister TID immediately after receiving response (or timeout) */
        tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);

        if (rc == 0) {
            responses_received++;
            /* DEBUG: Log response receipt */
            if (responses_received <= 10 || responses_received % 100 == 0) {
                log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d: RECEIVED response #%lu (TID=%02x%02x%02x)",
                        tree->tree_id, worker_id, responses_received, tid[0], tid[1], tid[2]);
            }
        } else {
            timeouts++;
            /* DEBUG: Log timeout */
            if (timeouts <= 10 || timeouts % 100 == 0) {
                log_msg(LOG_DEBUG, "[tree %u] BEP51 worker %d: TIMEOUT #%lu (TID=%02x%02x%02x)",
                        tree->tree_id, worker_id, timeouts, tid[0], tid[1], tid[2]);
            }
        }

        if (rc == 0) {
            /* 5. Parse response, extract infohashes */
            tree_sample_response_t response;
            if (tree_handle_sample_infohashes_response(tree, response_pkt.data, response_pkt.len,
                                                       &response_pkt.from, &response) == 0) {
                /* 6. Push infohashes to queue (with bloom check) */
                for (int i = 0; i < response.infohash_count; i++) {
                    /* Check bloom filter (shared, thread-safe) */
                    if (tree->shared_bloom && bloom_filter_check(tree->shared_bloom, response.infohashes[i])) {
                        continue;  /* Already seen */
                    }

                    /* Try to push to queue (non-blocking) */
                    if (tree_infohash_queue_try_push(tree->infohash_queue, response.infohashes[i]) == 0) {
                        if (!first_infohash_found) {
                            first_infohash_found = true;
                            /* Trigger get_peers phase (Stage 4) */
                            if (tree->current_phase == TREE_PHASE_BEP51) {
                                tree->current_phase = TREE_PHASE_GET_PEERS;
                                tree_start_get_peers_workers(tree);
                            }
                        }
                    }
                }

                /* 7. Update routing table with new nodes from response */
                for (int i = 0; i < response.node_count; i++) {
                    tree_routing_add_node(rt, response.nodes[i], &response.addrs[i]);
                }
            }
        }
        /* If timeout (rc != 0), just continue to next query */

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

        /* Create per-worker response queue (capacity 10 = buffer up to 10 responses) */
        ctx->response_queue = tree_response_queue_create(10);
        if (!ctx->response_queue) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create BEP51 worker %d response queue", tree->tree_id, i);
            free(ctx);
            continue;
        }

        int rc = pthread_create(&tree->bep51_threads[i], NULL, bep51_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create BEP51 worker %d: %d", tree->tree_id, i, rc);
            tree_response_queue_destroy(ctx->response_queue);
            free(ctx);
        }
    }
}

/* Get_peers worker context (Stage 4) */
typedef struct get_peers_worker_ctx {
    thread_tree_t *tree;
    int worker_id;
    tree_response_queue_t *response_queue;  /* Per-worker response queue */
} get_peers_worker_ctx_t;

/* Get_peers worker thread function (Stage 4) - NEW DISPATCHER PATTERN */
static void *get_peers_worker_func(void *arg) {
    get_peers_worker_ctx_t *ctx = (get_peers_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;
    tree_response_queue_t *my_queue = ctx->response_queue;

    log_msg(LOG_DEBUG, "[tree %u] get_peers worker %d started", tree->tree_id, worker_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;
    tree_peers_queue_t *peers_queue = (tree_peers_queue_t *)tree->peers_queue;
    tree_dispatcher_t *dispatcher = tree->dispatcher;
    bool first_peers_found = false;

    unsigned long queries_sent = 0;
    unsigned long responses_received = 0;
    unsigned long timeouts = 0;

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
                /* Generate TID for this query */
                uint8_t tid[4];
                int tid_len = tree_protocol_gen_tid(tid);

                /* Register TID with dispatcher BEFORE sending */
                tree_dispatcher_register_tid(dispatcher, tid, tid_len, my_queue);

                /* Send get_peers query */
                tree_send_get_peers(tree, sock, tid, tid_len, infohash, &closest[i].addr);
                queries_sent++;

                /* Wait for response on personal queue (500ms timeout, increased from 100ms) */
                tree_response_t response_pkt;
                int pop_result = tree_response_queue_pop(my_queue, &response_pkt, 500);

                /* Unregister TID immediately after receiving (or timeout) */
                tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);

                if (pop_result == 0) {
                    /* Got a response! */
                    responses_received++;

                    tree_get_peers_response_t response;
                    if (tree_handle_get_peers_response(tree, response_pkt.data, response_pkt.len,
                                                       &response_pkt.from, &response) == 0) {
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
                } else {
                    /* Timeout or error */
                    timeouts++;
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

    /* Log worker statistics on exit */
    log_msg(LOG_INFO, "[tree %u] get_peers worker %d exiting: queries=%lu, responses=%lu, timeouts=%lu (%.1f%% success)",
            tree->tree_id, worker_id, queries_sent, responses_received, timeouts,
            queries_sent > 0 ? (100.0 * responses_received / queries_sent) : 0.0);

    free(ctx);
    return NULL;
}

/* Start get_peers workers (Stage 4) - NEW DISPATCHER PATTERN */
static void tree_start_get_peers_workers(thread_tree_t *tree) {
    log_msg(LOG_DEBUG, "[tree %u] Starting %d get_peers workers with dispatcher pattern",
            tree->tree_id, tree->num_get_peers_workers);

    for (int i = 0; i < tree->num_get_peers_workers; i++) {
        /* Create per-worker response queue (capacity 10 for buffering burst responses) */
        tree_response_queue_t *response_queue = tree_response_queue_create(10);
        if (!response_queue) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create response queue for get_peers worker %d",
                    tree->tree_id, i);
            continue;
        }

        get_peers_worker_ctx_t *ctx = malloc(sizeof(get_peers_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate get_peers worker context", tree->tree_id);
            tree_response_queue_destroy(response_queue);
            continue;
        }
        ctx->tree = tree;
        ctx->worker_id = i;
        ctx->response_queue = response_queue;

        int rc = pthread_create(&tree->get_peers_threads[i], NULL, get_peers_worker_func, ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "[tree %u] Failed to create get_peers worker %d: %d", tree->tree_id, i, rc);
            tree_response_queue_destroy(response_queue);
            free(ctx);
        }
    }
}

/* Stage 5: Start metadata workers */
static void tree_start_metadata_workers(thread_tree_t *tree) {
    log_msg(LOG_DEBUG, "[tree %u] Starting %d metadata workers", tree->tree_id, tree->num_metadata_workers);

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
    log_msg(LOG_DEBUG, "[tree %u] metadata workers not yet implemented (Stage 5)", tree->tree_id);
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
    ctx->min_lifetime_sec = tree->min_lifetime_sec;
    ctx->require_empty_queue = tree->require_empty_queue;

    int rc = pthread_create(&tree->rate_monitor_thread, NULL, tree_rate_monitor_func, ctx);
    if (rc != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create rate monitor thread: %d", tree->tree_id, rc);
        free(ctx);
    } else {
        log_msg(LOG_DEBUG, "[tree %u] Rate monitor started (min_rate=%.2f/s)",
                tree->tree_id, tree->min_metadata_rate);
    }
}

/* OLD placeholder for get_peers workers - disabled, kept for reference */
#if 0
static void tree_start_get_peers_workers_OLD(thread_tree_t *tree) {
    log_msg(LOG_DEBUG, "[tree %u] get_peers workers not yet implemented (Stage 4)", tree->tree_id);
    /* TODO: Implement in Stage 4 */
    (void)tree;
}
#endif

/* Start BEP51 phase */
static void tree_start_bep51_phase(thread_tree_t *tree) {
    log_msg(LOG_DEBUG, "[tree %u] Transitioning to BEP51 phase with %d nodes",
            tree->tree_id, tree_routing_get_count(tree->routing_table));
    tree->current_phase = TREE_PHASE_BEP51;

    /* Start BEP51 worker threads */
    tree_start_bep51_workers(tree);
}

/* Bootstrap thread function - NEW SIMPLIFIED VERSION using shared node pool */
static void *bootstrap_thread_func(void *arg) {
    thread_tree_t *tree = (thread_tree_t *)arg;

    log_msg(LOG_DEBUG, "[tree %u] Bootstrap thread started (using shared node pool)", tree->tree_id);

    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;

    if (!rt) {
        log_msg(LOG_ERROR, "[tree %u] Missing routing table", tree->tree_id);
        goto shutdown;
    }

    /* Check if supervisor and shared_node_pool are available */
    if (!tree->supervisor || !tree->supervisor->shared_node_pool) {
        log_msg(LOG_ERROR, "[tree %u] No shared node pool available, cannot bootstrap", tree->tree_id);
        goto shutdown;
    }

    shared_node_pool_t *pool = tree->supervisor->shared_node_pool;
    int sample_size = tree->supervisor->per_tree_sample_size;

    /* Sample random nodes from the shared pool */
    tree_node_t *sampled_nodes = malloc(sample_size * sizeof(tree_node_t));
    if (!sampled_nodes) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate memory for sampled nodes", tree->tree_id);
        goto shutdown;
    }

    int got = shared_node_pool_get_random(pool, sampled_nodes, sample_size);
    if (got < 100) {
        log_msg(LOG_ERROR, "[tree %u] Insufficient nodes in shared pool: got %d, need at least 100",
                tree->tree_id, got);
        free(sampled_nodes);
        goto shutdown;
    }

    log_msg(LOG_DEBUG, "[tree %u] Sampled %d nodes from shared pool", tree->tree_id, got);

    /* Add sampled nodes to routing table */
    for (int i = 0; i < got; i++) {
        tree_routing_add_node(rt, sampled_nodes[i].node_id, &sampled_nodes[i].addr);
    }

    free(sampled_nodes);

    int final_count = tree_routing_get_count(rt);
    log_msg(LOG_DEBUG, "[tree %u] Bootstrap complete: %d nodes in routing table",
            tree->tree_id, final_count);

    /* Start find_node workers to continuously discover new nodes */
    tree_start_find_node_workers(tree);

    /* Start throttle monitor to control find_node worker pausing */
    tree_start_throttle_monitor(tree);

    /* Transition to BEP51 phase to discover infohashes */
    log_msg(LOG_DEBUG, "[tree %u] Transitioning to BEP51 phase (sample_infohashes)", tree->tree_id);
    tree->current_phase = TREE_PHASE_BEP51;
    tree_start_bep51_workers(tree);

    /* Wait for shutdown */
    while (!atomic_load(&tree->shutdown_requested)) {
        struct timespec ts = {1, 0};  /* 1 second */
        nanosleep(&ts, NULL);
    }

shutdown:
    log_msg(LOG_DEBUG, "[tree %u] Bootstrap thread exiting (final: %d nodes)",
            tree->tree_id, tree->routing_table ? tree_routing_get_count(tree->routing_table) : 0);

    /* Notify supervisor of shutdown completion
     * ONLY if we're NOT in global shutdown (avoid deadlock on trees_lock)
     * During shutdown, supervisor already knows it's destroying the tree */
    if (tree->on_shutdown && tree->current_phase != TREE_PHASE_SHUTTING_DOWN) {
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
    tree->min_lifetime_sec = (config->min_lifetime_minutes > 0 ? config->min_lifetime_minutes : 10) * 60;
    tree->require_empty_queue = config->require_empty_queue;
    tree->shared_batch_writer = config->batch_writer;

    /* Initialize thread counts from config */
    tree->num_bootstrap_workers = config->num_bootstrap_workers > 0 ? config->num_bootstrap_workers : 10;
    tree->num_find_node_workers = config->num_find_node_workers > 0 ? config->num_find_node_workers : 30;
    tree->num_bep51_workers = config->num_bep51_workers;
    tree->num_get_peers_workers = config->num_get_peers_workers;
    tree->num_metadata_workers = config->num_metadata_workers;

    /* Supervisor callbacks */
    tree->on_shutdown = config->on_shutdown;
    tree->supervisor_ctx = config->supervisor_ctx;

    /* Initialize phase and shutdown flag */
    tree->current_phase = TREE_PHASE_BOOTSTRAP;
    atomic_store(&tree->shutdown_requested, false);

    /* Initialize discovery throttling (find_node + BEP51) */
    atomic_store(&tree->discovery_paused, false);
    pthread_mutex_init(&tree->throttle_lock, NULL);
    pthread_cond_init(&tree->throttle_resume, NULL);
    tree->infohash_pause_threshold = config->infohash_pause_threshold > 0 ? config->infohash_pause_threshold : 2000;
    tree->infohash_resume_threshold = config->infohash_resume_threshold > 0 ? config->infohash_resume_threshold : 1000;

    /* Initialize statistics */
    atomic_store(&tree->metadata_count, 0);
    atomic_store(&tree->last_metadata_time, 0);
    tree->metadata_rate = 0.0;

    /* Initialize lifecycle tracking */
    tree->creation_time = time(NULL);

    /* Allocate thread handle arrays */
    if (tree->num_bootstrap_workers > 0) {
        tree->bootstrap_workers = calloc(tree->num_bootstrap_workers, sizeof(pthread_t));
        if (!tree->bootstrap_workers) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate bootstrap workers", tree_id);
            thread_tree_destroy(tree);
            return NULL;
        }
    }

    if (tree->num_find_node_workers > 0) {
        tree->find_node_workers = calloc(tree->num_find_node_workers, sizeof(pthread_t));
        if (!tree->find_node_workers) {
            log_msg(LOG_ERROR, "[tree %u] Failed to allocate find_node workers", tree_id);
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

    /* Create UDP response dispatcher */
    tree->dispatcher = tree_dispatcher_create(tree, (tree_socket_t *)tree->socket);
    if (!tree->dispatcher) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create dispatcher", tree_id);
        thread_tree_destroy(tree);
        return NULL;
    }

    int port = tree_socket_get_port(tree->socket);
    log_msg(LOG_DEBUG, "[tree %u] Created with node_id %02x%02x%02x%02x... on port %d",
            tree_id, tree->node_id[0], tree->node_id[1], tree->node_id[2], tree->node_id[3], port);

    return tree;
}

void thread_tree_destroy(thread_tree_t *tree) {
    if (!tree) {
        return;
    }

    log_msg(LOG_DEBUG, "[tree %u] Destroying", tree->tree_id);

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

    /* Join find_node workers */
    log_msg(LOG_DEBUG, "[tree %u] Joining %d find_node workers...", tree->tree_id, tree->num_find_node_workers);
    if (tree->find_node_workers) {
        for (int i = 0; i < tree->num_find_node_workers; i++) {
            if (tree->find_node_workers[i]) {
                pthread_join(tree->find_node_workers[i], NULL);
            }
        }
        free(tree->find_node_workers);
    }
    log_msg(LOG_DEBUG, "[tree %u] find_node workers joined", tree->tree_id);

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

    /* Join throttle monitor thread */
    log_msg(LOG_DEBUG, "[tree %u] Joining throttle monitor thread...", tree->tree_id);
    if (tree->throttle_monitor_thread) {
        pthread_join(tree->throttle_monitor_thread, NULL);
    }
    log_msg(LOG_DEBUG, "[tree %u] Throttle monitor thread joined", tree->tree_id);

    /* Close socket BEFORE cleanup to unblock any pending recv() calls
     * This is critical - workers may be blocked in poll() */
    log_msg(LOG_DEBUG, "[tree %u] Closing socket to unblock workers...", tree->tree_id);
    if (tree->socket) {
        /* Close the file descriptor but don't free the structure yet */
        tree_socket_t *sock = (tree_socket_t *)tree->socket;
        if (sock->fd >= 0) {
            shutdown(sock->fd, SHUT_RDWR);  /* Unblock pending recv/send */
            close(sock->fd);
            sock->fd = -1;
        }
    }
    log_msg(LOG_DEBUG, "[tree %u] Socket closed", tree->tree_id);

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

    /* Cleanup dispatcher */
    if (tree->dispatcher) {
        tree_dispatcher_destroy(tree->dispatcher);
    }

    /* Cleanup throttling synchronization primitives */
    pthread_mutex_destroy(&tree->throttle_lock);
    pthread_cond_destroy(&tree->throttle_resume);

    log_msg(LOG_DEBUG, "[tree %u] Destroyed (metadata_count=%lu)",
            tree->tree_id, (unsigned long)atomic_load(&tree->metadata_count));

    free(tree);
}

void thread_tree_start(thread_tree_t *tree) {
    if (!tree) {
        return;
    }

    log_msg(LOG_DEBUG, "[tree %u] Starting (phase: %s)",
            tree->tree_id, thread_tree_phase_name(tree->current_phase));

    /* Start UDP response dispatcher FIRST before any workers send queries */
    if (tree_dispatcher_start(tree->dispatcher) != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to start dispatcher", tree->tree_id);
        return;
    }

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
        log_msg(LOG_DEBUG, "[tree %u] Shutdown requested", tree->tree_id);
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
