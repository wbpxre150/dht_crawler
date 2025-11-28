#define _DEFAULT_SOURCE  /* For usleep */

#include "refresh_thread.h"
#include "refresh_request_queue.h"
#include "tree_routing.h"
#include "tree_socket.h"
#include "tree_protocol.h"
#include "refresh_protocol.h"
#include "refresh_dispatcher.h"
#include "tree_response_queue.h"
#include "shared_node_pool.h"
#include "refresh_query.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>

/* Generate random node ID for DHT identity */
static void generate_random_node_id(uint8_t *node_id) {
    FILE *urandom = fopen("/dev/urandom", "rb");
    if (urandom) {
        fread(node_id, 1, 20, urandom);
        fclose(urandom);
    } else {
        /* Fallback to rand() */
        srand((unsigned int)(time(NULL) ^ (uintptr_t)node_id));
        for (int i = 0; i < 20; i++) {
            node_id[i] = (uint8_t)(rand() % 256);
        }
    }
}

/* Worker context structures */
typedef struct ping_worker_ctx {
    refresh_thread_t *thread;
    int worker_id;
} ping_worker_ctx_t;

typedef struct find_node_worker_ctx {
    refresh_thread_t *thread;
    int worker_id;
    tree_response_queue_t *response_queue;
} find_node_worker_ctx_t;

typedef struct get_peers_worker_ctx {
    refresh_thread_t *thread;
    int worker_id;
    tree_response_queue_t *response_queue;
} get_peers_worker_ctx_t;

/* Candidate node for iterative get_peers lookup */
typedef struct candidate_node {
    uint8_t node_id[20];
    struct sockaddr_storage addr;
    uint8_t distance[20];  /* XOR distance to target infohash */
    bool queried;          /* Has this node been queried yet? */
} candidate_node_t;

/* Candidate list for managing iterative lookup state */
typedef struct candidate_list {
    candidate_node_t *nodes;
    int count;
    int capacity;
} candidate_list_t;

/* Forward declarations */
static void *bootstrap_thread_func(void *arg);
static void *ping_worker_func(void *arg);
static void *find_node_worker_func(void *arg);
static void *get_peers_worker_func(void *arg);

/* ========================================================================== */
/*                          LIFECYCLE MANAGEMENT                               */
/* ========================================================================== */

refresh_thread_t *refresh_thread_create(const refresh_thread_config_t *config,
                                       shared_node_pool_t *shared_pool,
                                       refresh_query_store_t *query_store) {
    if (!config || !shared_pool || !query_store) {
        log_msg(LOG_ERROR, "refresh_thread_create: invalid arguments");
        return NULL;
    }

    refresh_thread_t *thread = malloc(sizeof(refresh_thread_t));
    if (!thread) {
        log_msg(LOG_ERROR, "refresh_thread_create: malloc failed");
        return NULL;
    }

    memset(thread, 0, sizeof(refresh_thread_t));
    thread->config = *config;
    thread->shared_node_pool = shared_pool;
    thread->refresh_query_store = query_store;

    /* Generate random node ID */
    generate_random_node_id(thread->node_id);

    /* Create routing table */
    thread->routing_table = tree_routing_create(thread->node_id);
    if (!thread->routing_table) {
        log_msg(LOG_ERROR, "refresh_thread_create: failed to create routing table");
        free(thread);
        return NULL;
    }

    /* Create UDP socket (port 0 = auto-assign) */
    thread->socket = tree_socket_create(0);
    if (!thread->socket) {
        log_msg(LOG_ERROR, "refresh_thread_create: failed to create socket");
        tree_routing_destroy(thread->routing_table);
        free(thread);
        return NULL;
    }

    /* Create dispatcher (standalone refresh dispatcher) */
    thread->dispatcher = refresh_dispatcher_create(thread->socket);
    if (!thread->dispatcher) {
        log_msg(LOG_ERROR, "refresh_thread_create: failed to create dispatcher");
        tree_socket_destroy(thread->socket);
        tree_routing_destroy(thread->routing_table);
        free(thread);
        return NULL;
    }

    /* Create request queue */
    thread->request_queue = refresh_request_queue_create(config->request_queue_capacity);
    if (!thread->request_queue) {
        log_msg(LOG_ERROR, "refresh_thread_create: failed to create request queue");
        refresh_dispatcher_destroy(thread->dispatcher);
        tree_socket_destroy(thread->socket);
        tree_routing_destroy(thread->routing_table);
        free(thread);
        return NULL;
    }

    /* Allocate worker thread arrays */
    if (config->ping_worker_count > 0) {
        thread->ping_workers = malloc(sizeof(pthread_t) * config->ping_worker_count);
        if (!thread->ping_workers) {
            log_msg(LOG_ERROR, "refresh_thread_create: failed to allocate ping workers");
            refresh_request_queue_destroy(thread->request_queue);
            refresh_dispatcher_destroy(thread->dispatcher);
            tree_socket_destroy(thread->socket);
            tree_routing_destroy(thread->routing_table);
            free(thread);
            return NULL;
        }
        memset(thread->ping_workers, 0, sizeof(pthread_t) * config->ping_worker_count);
    }

    if (config->find_node_worker_count > 0) {
        thread->find_node_workers = malloc(sizeof(pthread_t) * config->find_node_worker_count);
        if (!thread->find_node_workers) {
            log_msg(LOG_ERROR, "refresh_thread_create: failed to allocate find_node workers");
            free(thread->ping_workers);
            refresh_request_queue_destroy(thread->request_queue);
            refresh_dispatcher_destroy(thread->dispatcher);
            tree_socket_destroy(thread->socket);
            tree_routing_destroy(thread->routing_table);
            free(thread);
            return NULL;
        }
        memset(thread->find_node_workers, 0, sizeof(pthread_t) * config->find_node_worker_count);
    }

    if (config->get_peers_worker_count > 0) {
        thread->get_peers_workers = malloc(sizeof(pthread_t) * config->get_peers_worker_count);
        if (!thread->get_peers_workers) {
            log_msg(LOG_ERROR, "refresh_thread_create: failed to allocate get_peers workers");
            free(thread->find_node_workers);
            free(thread->ping_workers);
            refresh_request_queue_destroy(thread->request_queue);
            refresh_dispatcher_destroy(thread->dispatcher);
            tree_socket_destroy(thread->socket);
            tree_routing_destroy(thread->routing_table);
            free(thread);
            return NULL;
        }
        memset(thread->get_peers_workers, 0, sizeof(pthread_t) * config->get_peers_worker_count);
    }

    /* Initialize synchronization */
    atomic_init(&thread->shutdown_requested, false);
    atomic_init(&thread->initialized, false);
    pthread_mutex_init(&thread->lock, NULL);

    log_msg(LOG_INFO, "Refresh thread created with %d find_node workers, %d ping workers, %d get_peers workers",
            config->find_node_worker_count, config->ping_worker_count, config->get_peers_worker_count);

    return thread;
}

int refresh_thread_start(refresh_thread_t *thread) {
    if (!thread) {
        return -1;
    }

    /* Start dispatcher */
    if (refresh_dispatcher_start(thread->dispatcher) < 0) {
        log_msg(LOG_ERROR, "refresh_thread_start: failed to start dispatcher");
        return -1;
    }

    /* Start bootstrap thread */
    if (pthread_create(&thread->bootstrap_thread, NULL, bootstrap_thread_func, thread) != 0) {
        log_msg(LOG_ERROR, "refresh_thread_start: failed to create bootstrap thread");
        return -1;
    }

    /* Start get_peers workers */
    for (int i = 0; i < thread->config.get_peers_worker_count; i++) {
        get_peers_worker_ctx_t *ctx = malloc(sizeof(get_peers_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "refresh_thread_start: failed to allocate get_peers worker context");
            continue;
        }
        ctx->thread = thread;
        ctx->worker_id = i;
        ctx->response_queue = tree_response_queue_create(10);
        if (!ctx->response_queue) {
            log_msg(LOG_ERROR, "refresh_thread_start: failed to create response queue for get_peers worker %d", i);
            free(ctx);
            continue;
        }

        if (pthread_create(&thread->get_peers_workers[i], NULL, get_peers_worker_func, ctx) != 0) {
            log_msg(LOG_ERROR, "refresh_thread_start: failed to create get_peers worker %d", i);
            tree_response_queue_destroy(ctx->response_queue);
            free(ctx);
        }
    }

    log_msg(LOG_INFO, "Refresh thread started, waiting for bootstrap");

    return 0;
}

void refresh_thread_request_shutdown(refresh_thread_t *thread) {
    if (!thread) {
        return;
    }

    log_msg(LOG_INFO, "Requesting refresh thread shutdown");
    atomic_store(&thread->shutdown_requested, true);
    refresh_request_queue_shutdown(thread->request_queue);
}

void refresh_thread_destroy(refresh_thread_t *thread) {
    if (!thread) {
        return;
    }

    log_msg(LOG_DEBUG, "Destroying refresh thread");

    /* Join bootstrap thread */
    if (thread->bootstrap_thread) {
        pthread_join(thread->bootstrap_thread, NULL);
    }

    /* Join worker threads */
    for (int i = 0; i < thread->config.ping_worker_count; i++) {
        if (thread->ping_workers[i]) {
            pthread_join(thread->ping_workers[i], NULL);
        }
    }

    for (int i = 0; i < thread->config.find_node_worker_count; i++) {
        if (thread->find_node_workers[i]) {
            pthread_join(thread->find_node_workers[i], NULL);
        }
    }

    for (int i = 0; i < thread->config.get_peers_worker_count; i++) {
        if (thread->get_peers_workers[i]) {
            pthread_join(thread->get_peers_workers[i], NULL);
        }
    }

    /* Destroy components */
    if (thread->dispatcher) {
        refresh_dispatcher_destroy(thread->dispatcher);
    }
    if (thread->request_queue) {
        refresh_request_queue_destroy(thread->request_queue);
    }
    if (thread->socket) {
        tree_socket_destroy(thread->socket);
    }
    if (thread->routing_table) {
        tree_routing_destroy(thread->routing_table);
    }

    /* Free worker arrays */
    free(thread->ping_workers);
    free(thread->find_node_workers);
    free(thread->get_peers_workers);

    pthread_mutex_destroy(&thread->lock);
    free(thread);

    log_msg(LOG_INFO, "Refresh thread destroyed");
}

int refresh_thread_submit_request(refresh_thread_t *thread,
                                  const uint8_t *infohash) {
    if (!thread || !infohash) {
        return -1;
    }

    return refresh_request_queue_push(thread->request_queue, infohash, 1000);
}

/* ========================================================================== */
/*                          BOOTSTRAP THREAD                                  */
/* ========================================================================== */

static void *bootstrap_thread_func(void *arg) {
    refresh_thread_t *thread = (refresh_thread_t *)arg;
    tree_node_t sampled_nodes[1000];
    int sample_size = thread->config.bootstrap_sample_size;

    log_msg(LOG_INFO, "Refresh thread bootstrap starting");

    /* Wait for shared node pool to have sufficient nodes */
    while (!atomic_load(&thread->shutdown_requested)) {
        int pool_size = (int)shared_node_pool_get_count(thread->shared_node_pool);
        if (pool_size >= sample_size) {
            break;
        }
        log_msg(LOG_DEBUG, "Refresh thread waiting for shared pool (%d/%d nodes)",
                pool_size, sample_size);
        sleep(1);
    }

    if (atomic_load(&thread->shutdown_requested)) {
        log_msg(LOG_INFO, "Refresh thread bootstrap aborted (shutdown requested)");
        return NULL;
    }

    /* Sample nodes from shared pool */
    int got = shared_node_pool_get_random(thread->shared_node_pool, sampled_nodes, sample_size);
    if (got < 100) {
        log_msg(LOG_ERROR, "Refresh thread bootstrap failed: insufficient nodes (%d/%d)",
                got, sample_size);
        return NULL;
    }

    log_msg(LOG_INFO, "Refresh thread bootstrap: sampled %d nodes from shared pool", got);

    /* Add nodes to routing table */
    for (int i = 0; i < got; i++) {
        tree_routing_add_node(thread->routing_table, sampled_nodes[i].node_id, &sampled_nodes[i].addr);
    }

    /* Set bucket capacity to normal mode (8) */
    tree_routing_set_bucket_capacity(thread->routing_table, 8);

    log_msg(LOG_INFO, "Refresh thread routing table populated with %d nodes", got);

    /* Start find_node workers */
    for (int i = 0; i < thread->config.find_node_worker_count; i++) {
        find_node_worker_ctx_t *ctx = malloc(sizeof(find_node_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "Failed to allocate find_node worker context");
            continue;
        }
        ctx->thread = thread;
        ctx->worker_id = i;
        ctx->response_queue = tree_response_queue_create(10);
        if (!ctx->response_queue) {
            log_msg(LOG_ERROR, "Failed to create response queue for find_node worker %d", i);
            free(ctx);
            continue;
        }

        if (pthread_create(&thread->find_node_workers[i], NULL, find_node_worker_func, ctx) != 0) {
            log_msg(LOG_ERROR, "Failed to create find_node worker %d", i);
            tree_response_queue_destroy(ctx->response_queue);
            free(ctx);
        }
    }

    /* Start ping workers */
    for (int i = 0; i < thread->config.ping_worker_count; i++) {
        ping_worker_ctx_t *ctx = malloc(sizeof(ping_worker_ctx_t));
        if (!ctx) {
            log_msg(LOG_ERROR, "Failed to allocate ping worker context");
            continue;
        }
        ctx->thread = thread;
        ctx->worker_id = i;

        if (pthread_create(&thread->ping_workers[i], NULL, ping_worker_func, ctx) != 0) {
            log_msg(LOG_ERROR, "Failed to create ping worker %d", i);
            free(ctx);
        }
    }

    /* Mark as initialized */
    atomic_store(&thread->initialized, true);
    log_msg(LOG_INFO, "Refresh thread bootstrap complete, ready to process requests");

    return NULL;
}

/* ========================================================================== */
/*                          PING WORKER                                       */
/* ========================================================================== */

static void *ping_worker_func(void *arg) {
    ping_worker_ctx_t *ctx = (ping_worker_ctx_t *)arg;
    refresh_thread_t *thread = ctx->thread;

    log_msg(LOG_DEBUG, "Refresh thread ping worker %d started", ctx->worker_id);

    while (!atomic_load(&thread->shutdown_requested)) {
        /* Sleep for 30 seconds between pings */
        for (int i = 0; i < 30 && !atomic_load(&thread->shutdown_requested); i++) {
            sleep(1);
        }

        if (atomic_load(&thread->shutdown_requested)) {
            break;
        }

        /* Get a random node from routing table */
        tree_node_t node;
        if (tree_routing_get_random_nodes(thread->routing_table, &node, 1) < 1) {
            continue;
        }

        /* Send ping query */
        refresh_send_ping(thread->node_id, thread->socket, &node.addr);
    }

    log_msg(LOG_DEBUG, "Refresh thread ping worker %d stopped", ctx->worker_id);
    free(ctx);
    return NULL;
}

/* ========================================================================== */
/*                          FIND_NODE WORKER                                  */
/* ========================================================================== */

static void *find_node_worker_func(void *arg) {
    find_node_worker_ctx_t *ctx = (find_node_worker_ctx_t *)arg;
    refresh_thread_t *thread = ctx->thread;
    tree_response_queue_t *my_queue = ctx->response_queue;

    log_msg(LOG_DEBUG, "Refresh thread find_node worker %d started", ctx->worker_id);

    while (!atomic_load(&thread->shutdown_requested)) {
        /* Adaptive throttling based on routing table size */
        int current_size = tree_routing_get_count(thread->routing_table);
        int target_size = thread->config.routing_table_target;

        if (current_size < (int)(target_size * 0.7)) {
            /* Less than 70%: aggressive discovery, no delay */
        } else if (current_size < (int)(target_size * 0.9)) {
            /* 70-90%: moderate, 500ms delay */
            usleep(500000);
        } else if (current_size < target_size) {
            /* 90-100%: slow, 2s delay */
            sleep(2);
        } else {
            /* Full: maintenance mode, 10s delay */
            sleep(10);
        }

        /* Get random node to query */
        tree_node_t nodes[1];
        if (tree_routing_get_random_nodes(thread->routing_table, nodes, 1) < 1) {
            usleep(100000);
            continue;
        }

        /* Generate random target */
        uint8_t target[20];
        for (int i = 0; i < 20; i++) {
            target[i] = (uint8_t)(rand() % 256);
        }

        /* Generate TID and register */
        uint8_t tid[4];
        int tid_len = tree_protocol_gen_tid(tid);
        refresh_dispatcher_register_tid(thread->dispatcher, tid, tid_len, my_queue);

        /* Send find_node query */
        refresh_send_find_node(thread->node_id, thread->socket, tid, tid_len, target, &nodes[0].addr);

        /* Wait for response */
        tree_response_t response_pkt;
        if (tree_response_queue_pop(my_queue, &response_pkt, 500) == 0) {
            /* Parse response */
            refresh_find_node_response_t response;
            if (refresh_parse_find_node_response(response_pkt.data, response_pkt.len,
                                                 &response_pkt.from, &response) == 0) {
                /* Add sender if present */
                if (response.has_sender_id) {
                    tree_routing_add_node(thread->routing_table, response.sender_id,
                                        &response_pkt.from);
                }

                /* Add discovered nodes to routing table */
                for (int i = 0; i < response.node_count; i++) {
                    tree_routing_add_node(thread->routing_table, response.nodes[i], &response.addrs[i]);
                }
            }
        }

        refresh_dispatcher_unregister_tid(thread->dispatcher, tid, tid_len);
    }

    log_msg(LOG_DEBUG, "Refresh thread find_node worker %d stopped", ctx->worker_id);
    tree_response_queue_destroy(my_queue);
    free(ctx);
    return NULL;
}

/* ========================================================================== */
/*                  CANDIDATE LIST MANAGEMENT (for iterative lookup)         */
/* ========================================================================== */

/* Calculate XOR distance between node_id and target */
static void calculate_distance(const uint8_t *node_id, const uint8_t *target, uint8_t *out) {
    for (int i = 0; i < 20; i++) {
        out[i] = node_id[i] ^ target[i];
    }
}

/* Compare two distances (for qsort) - returns <0 if d1 < d2, >0 if d1 > d2 */
static int compare_distances(const void *a, const void *b) {
    const candidate_node_t *n1 = (const candidate_node_t *)a;
    const candidate_node_t *n2 = (const candidate_node_t *)b;
    return memcmp(n1->distance, n2->distance, 20);
}

/* Create a new candidate list with initial capacity */
static candidate_list_t *candidate_list_create(int initial_capacity) {
    candidate_list_t *list = malloc(sizeof(candidate_list_t));
    if (!list) {
        return NULL;
    }

    list->nodes = calloc(initial_capacity, sizeof(candidate_node_t));
    if (!list->nodes) {
        free(list);
        return NULL;
    }

    list->count = 0;
    list->capacity = initial_capacity;
    return list;
}

/* Free candidate list resources */
static void candidate_list_destroy(candidate_list_t *list) {
    if (!list) {
        return;
    }
    free(list->nodes);
    free(list);
}

/* Add a node to candidate list (no duplicates, maintains sorted order by distance) */
static int candidate_list_add(candidate_list_t *list, const uint8_t *node_id,
                               const struct sockaddr_storage *addr,
                               const uint8_t *target_infohash) {
    if (!list || !node_id || !addr) {
        return -1;
    }

    /* Check for duplicates (O(n) scan - acceptable for small lists) */
    for (int i = 0; i < list->count; i++) {
        if (memcmp(list->nodes[i].node_id, node_id, 20) == 0) {
            return 0;  /* Already have this node */
        }
    }

    /* Expand capacity if needed (double size) */
    if (list->count >= list->capacity) {
        int new_capacity = list->capacity * 2;
        candidate_node_t *new_nodes = realloc(list->nodes,
                                               new_capacity * sizeof(candidate_node_t));
        if (!new_nodes) {
            return -1;
        }
        list->nodes = new_nodes;
        list->capacity = new_capacity;
    }

    /* Add new candidate */
    candidate_node_t *node = &list->nodes[list->count];
    memcpy(node->node_id, node_id, 20);
    memcpy(&node->addr, addr, sizeof(struct sockaddr_storage));
    calculate_distance(node_id, target_infohash, node->distance);
    node->queried = false;
    list->count++;

    /* Sort by distance to maintain closest-first order */
    qsort(list->nodes, list->count, sizeof(candidate_node_t), compare_distances);

    return 0;
}

/* Get up to 'count' unqueried candidates closest to target
 * Returns number of candidates found */
static int candidate_list_get_unqueried(candidate_list_t *list,
                                         candidate_node_t **out,
                                         int count) {
    if (!list || !out || count <= 0) {
        return 0;
    }

    int found = 0;
    for (int i = 0; i < list->count && found < count; i++) {
        if (!list->nodes[i].queried) {
            out[found++] = &list->nodes[i];
        }
    }

    return found;
}

/* ========================================================================== */
/*                          GET_PEERS WORKER                                  */
/* ========================================================================== */

static void *get_peers_worker_func(void *arg) {
    get_peers_worker_ctx_t *ctx = (get_peers_worker_ctx_t *)arg;
    refresh_thread_t *thread = ctx->thread;
    tree_response_queue_t *my_queue = ctx->response_queue;

    log_msg(LOG_DEBUG, "Refresh thread get_peers worker %d started", ctx->worker_id);

    while (!atomic_load(&thread->shutdown_requested)) {
        /* Pop request from queue */
        refresh_request_t request;
        if (refresh_request_queue_pop(thread->request_queue, &request, 1000) < 0) {
            continue;
        }

        /* Verify bootstrap complete */
        if (!atomic_load(&thread->initialized)) {
            log_msg(LOG_ERROR, "Refresh request received before bootstrap complete");
            refresh_query_complete(thread->refresh_query_store, request.infohash);
            continue;
        }

        log_msg(LOG_DEBUG, "[get_peers] Starting iterative lookup for infohash");

        /* STEP 1: Initialize candidate list from routing table */
        candidate_list_t *candidates = candidate_list_create(50);
        if (!candidates) {
            log_msg(LOG_ERROR, "[get_peers] Failed to create candidate list");
            refresh_query_complete(thread->refresh_query_store, request.infohash);
            continue;
        }

        /* Seed with K closest nodes from routing table */
        tree_node_t initial_nodes[8];
        int initial_count = tree_routing_get_closest(thread->routing_table,
                                                     request.infohash,
                                                     initial_nodes, 8);

        if (initial_count == 0) {
            log_msg(LOG_ERROR, "[get_peers] No nodes in routing table");
            candidate_list_destroy(candidates);
            refresh_query_complete(thread->refresh_query_store, request.infohash);
            continue;
        }

        for (int i = 0; i < initial_count; i++) {
            candidate_list_add(candidates, initial_nodes[i].node_id,
                              &initial_nodes[i].addr, request.infohash);
        }

        log_msg(LOG_DEBUG, "[get_peers] Initialized with %d candidates from routing table",
                initial_count);

        /* STEP 2: Iterative lookup loop */
        int total_peers = 0;
        int total_queries = 0;
        const int MAX_ITERATIONS = thread->config.max_iterations;
        const int ALPHA = 3;  /* Standard Kademlia concurrency factor */
        const int MIN_PEERS = 10; /* Minimum peers before stopping */

        for (int iter = 0; iter < MAX_ITERATIONS && total_peers < MIN_PEERS; iter++) {
            /* Get ALPHA unqueried candidates closest to target */
            candidate_node_t *to_query[ALPHA];
            int num_to_query = candidate_list_get_unqueried(candidates, to_query, ALPHA);

            if (num_to_query == 0) {
                log_msg(LOG_DEBUG, "[get_peers] No unqueried candidates, converged");
                break;
            }

            log_msg(LOG_DEBUG, "[get_peers] Iteration %d: querying %d candidates",
                    iter, num_to_query);

            int new_candidates_added = 0;

            /* Query each candidate */
            for (int i = 0; i < num_to_query; i++) {
                candidate_node_t *candidate = to_query[i];

                /* Generate TID and register */
                uint8_t tid[4];
                int tid_len = tree_protocol_gen_tid(tid);
                refresh_dispatcher_register_tid(thread->dispatcher, tid, tid_len, my_queue);

                /* Send get_peers query */
                refresh_send_get_peers(thread->node_id, thread->socket, tid, tid_len,
                                      request.infohash, &candidate->addr);
                total_queries++;

                /* Mark as queried immediately (don't retry on timeout) */
                candidate->queried = true;

                /* Wait for response */
                tree_response_t response_pkt;
                if (tree_response_queue_pop(my_queue, &response_pkt,
                                           thread->config.get_peers_timeout_ms) == 0) {
                    refresh_get_peers_response_t response;
                    if (refresh_parse_get_peers_response(response_pkt.data, response_pkt.len,
                                                         &response_pkt.from, &response) == 0) {
                        /* Add responding node to routing table (for long-term DHT health) */
                        if (response.has_sender_id) {
                            tree_routing_add_node(thread->routing_table, response.sender_id,
                                                &response_pkt.from);
                        }

                        /* Found peers! */
                        if (response.peer_count > 0) {
                            total_peers += response.peer_count;
                            refresh_query_add_peers(thread->refresh_query_store,
                                                  request.infohash, response.peer_count);

                            log_msg(LOG_INFO, "[get_peers] Found %d peers (total: %d) on iteration %d",
                                    response.peer_count, total_peers, iter);
                        }

                        /* Add returned nodes to candidate list (NOT routing table) */
                        for (int j = 0; j < response.node_count; j++) {
                            if (candidate_list_add(candidates, response.nodes[j],
                                                  &response.node_addrs[j],
                                                  request.infohash) == 0) {
                                new_candidates_added++;
                            }
                        }
                    }
                }

                refresh_dispatcher_unregister_tid(thread->dispatcher, tid, tid_len);

                /* Stop querying this iteration if we have enough peers */
                if (total_peers >= MIN_PEERS) {
                    break;
                }
            }

            log_msg(LOG_DEBUG, "[get_peers] Iteration %d complete: %d peers, %d new candidates",
                    iter, total_peers, new_candidates_added);

            /* Stop if no new candidates discovered (converged) */
            if (new_candidates_added == 0) {
                log_msg(LOG_DEBUG, "[get_peers] Converged - no new candidates");
                break;
            }
        }

        log_msg(LOG_INFO, "[get_peers] Query complete: %d peers found after %d queries",
                total_peers, total_queries);

        /* Cleanup */
        candidate_list_destroy(candidates);

        /* Mark query complete */
        refresh_query_complete(thread->refresh_query_store, request.infohash);
    }

    log_msg(LOG_DEBUG, "Refresh thread get_peers worker %d stopped", ctx->worker_id);
    tree_response_queue_destroy(my_queue);
    free(ctx);
    return NULL;
}
