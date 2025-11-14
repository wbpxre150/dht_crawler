/*
 * wbpxre-dht: Core DHT implementation
 */

#define _DEFAULT_SOURCE
#include "wbpxre_dht.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/time.h>
#include <urcu.h>
#include <urcu/rculist.h>

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

void wbpxre_random_bytes(uint8_t *buf, int len) {
    FILE *f = fopen("/dev/urandom", "rb");
    if (f) {
        fread(buf, 1, len, f);
        fclose(f);
    } else {
        /* Fallback to poor randomness */
        for (int i = 0; i < len; i++) {
            buf[i] = rand() & 0xFF;
        }
    }
}

void wbpxre_xor_distance(const uint8_t *a, const uint8_t *b, uint8_t *result) {
    for (int i = 0; i < WBPXRE_NODE_ID_LEN; i++) {
        result[i] = a[i] ^ b[i];
    }
}

int wbpxre_compare_distance(const uint8_t *dist1, const uint8_t *dist2) {
    return memcmp(dist1, dist2, WBPXRE_NODE_ID_LEN);
}

const char *wbpxre_addr_to_string(const struct sockaddr_in *addr) {
    static char buf[32];
    snprintf(buf, sizeof(buf), "%s:%d",
             inet_ntoa(addr->sin_addr),
             ntohs(addr->sin_port));
    return buf;
}

void wbpxre_hex_dump(const uint8_t *data, int len, const char *label) {
    printf("%s (%d bytes): ", label, len);
    for (int i = 0; i < len && i < 40; i++) {
        printf("%02x", data[i]);
    }
    if (len > 40) printf("...");
    printf("\n");
}

/* ============================================================================
 * Pending Query Management
 * ============================================================================ */

wbpxre_pending_query_t *wbpxre_create_pending_query(const uint8_t *transaction_id,
                                                      int timeout_sec) {
    wbpxre_pending_query_t *pq = calloc(1, sizeof(wbpxre_pending_query_t));
    if (!pq) return NULL;

    memcpy(pq->transaction_id, transaction_id, WBPXRE_TRANSACTION_ID_LEN);
    pthread_mutex_init(&pq->mutex, NULL);
    pthread_cond_init(&pq->cond, NULL);
    pq->received = false;
    pq->error = false;
    pq->deadline = time(NULL) + timeout_sec;
    pq->response_data = NULL;
    pq->next = NULL;

    return pq;
}

/* Free dynamically allocated fields within a wbpxre_message_t */
static void free_message_fields(wbpxre_message_t *msg) {
    if (!msg) return;

    if (msg->nodes) {
        free(msg->nodes);
        msg->nodes = NULL;
    }
    if (msg->values) {
        free(msg->values);
        msg->values = NULL;
    }
    if (msg->samples) {
        free(msg->samples);
        msg->samples = NULL;
    }
    if (msg->token) {
        free(msg->token);
        msg->token = NULL;
    }
}

void wbpxre_free_pending_query(wbpxre_pending_query_t *pq) {
    if (!pq) return;
    pthread_mutex_destroy(&pq->mutex);
    pthread_cond_destroy(&pq->cond);

    /* Free nested dynamic fields before freeing response_data */
    if (pq->response_data) {
        wbpxre_message_t *msg = (wbpxre_message_t *)pq->response_data;
        free_message_fields(msg);
        free(pq->response_data);
    }

    free(pq);
}

int wbpxre_register_pending_query(wbpxre_dht_t *dht, wbpxre_pending_query_t *pq) {
    uint8_t hash = pq->transaction_id[0];

    pthread_mutex_lock(&dht->pending_queries_mutex);

    /* Add to hash bucket */
    pq->next = dht->pending_queries[hash];
    dht->pending_queries[hash] = pq;

    pthread_mutex_unlock(&dht->pending_queries_mutex);
    return 0;
}

wbpxre_pending_query_t *wbpxre_find_and_remove_pending_query(wbpxre_dht_t *dht,
                                                               const uint8_t *transaction_id) {
    uint8_t hash = transaction_id[0];

    pthread_mutex_lock(&dht->pending_queries_mutex);

    wbpxre_pending_query_t *prev = NULL;
    wbpxre_pending_query_t *curr = dht->pending_queries[hash];

    while (curr) {
        if (memcmp(curr->transaction_id, transaction_id, WBPXRE_TRANSACTION_ID_LEN) == 0) {
            /* Found it, remove from list */
            if (prev) {
                prev->next = curr->next;
            } else {
                dht->pending_queries[hash] = curr->next;
            }
            pthread_mutex_unlock(&dht->pending_queries_mutex);
            return curr;
        }
        prev = curr;
        curr = curr->next;
    }

    pthread_mutex_unlock(&dht->pending_queries_mutex);
    return NULL;
}

int wbpxre_wait_for_response(wbpxre_pending_query_t *pq) {
    pthread_mutex_lock(&pq->mutex);

    while (!pq->received) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec = pq->deadline;

        int rc = pthread_cond_timedwait(&pq->cond, &pq->mutex, &ts);
        if (rc == ETIMEDOUT) {
            /* Double-check that we actually timed out */
            if (!pq->received) {
                pthread_mutex_unlock(&pq->mutex);
                return -1;
            }
            /* Signal arrived just before/during timeout, treat as success */
            break;
        }
    }

    int result = pq->error ? -1 : 0;
    pthread_mutex_unlock(&pq->mutex);
    return result;
}

static void signal_pending_query(wbpxre_pending_query_t *pq, void *response_data, bool error) {
    pthread_mutex_lock(&pq->mutex);
    pq->response_data = response_data;
    pq->received = true;
    pq->error = error;
    pthread_cond_signal(&pq->cond);
    pthread_mutex_unlock(&pq->mutex);
}

/* ============================================================================
 * UDP Socket Management
 * ============================================================================ */

static int create_udp_socket(int port) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return -1;
    }

    /* Set non-blocking */
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    /* Set SO_REUSEADDR */
    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    /* Bind */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(sock);
        return -1;
    }

    /* Phase 3: Log socket information for diagnostics */
    struct sockaddr_in bound_addr;
    socklen_t addr_len = sizeof(bound_addr);
    if (getsockname(sock, (struct sockaddr *)&bound_addr, &addr_len) == 0) {
        #ifdef DEBUG_PROTOCOL
        fprintf(stderr, "DEBUG: UDP socket bound to %s:%d (fd=%d)\n",
               inet_ntoa(bound_addr.sin_addr), ntohs(bound_addr.sin_port), sock);
        #endif
    }

    return sock;
}

/* ============================================================================
 * UDP Reader Thread
 * ============================================================================ */

static void *udp_reader_thread_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    uint8_t buf[WBPXRE_MAX_UDP_PACKET];

    /* Signal that UDP reader is ready (Phase 1) */
    pthread_mutex_lock(&dht->udp_reader_ready_mutex);
    dht->udp_reader_ready = true;
    pthread_cond_broadcast(&dht->udp_reader_ready_cond);
    pthread_mutex_unlock(&dht->udp_reader_ready_mutex);

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: UDP reader thread is ready and waiting for packets\n");
    #endif

    while (dht->running) {
        struct sockaddr_in from_addr;
        socklen_t from_len = sizeof(from_addr);

        int n = recvfrom(dht->udp_socket, buf, sizeof(buf), 0,
                         (struct sockaddr *)&from_addr, &from_len);

        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000);  /* Sleep 1ms */
                continue;
            }
            perror("recvfrom");
            continue;
        }

        if (n == 0) continue;

        /* Update stats */
        pthread_mutex_lock(&dht->stats_mutex);
        dht->stats.packets_received++;
        pthread_mutex_unlock(&dht->stats_mutex);

        /* Debug: dump first 100 bytes of received packet */
        #ifdef DEBUG_PROTOCOL
        fprintf(stderr, "DEBUG: Received packet (%d bytes) from %s:%d\n", n,
               inet_ntoa(from_addr.sin_addr), ntohs(from_addr.sin_port));
        fprintf(stderr, "  ");
        for (int i = 0; i < n && i < 100; i++) {
            fprintf(stderr, "%02x", buf[i]);
        }
        fprintf(stderr, "\n");
        #endif

        /* Decode message */
        wbpxre_message_t msg;
        memset(&msg, 0, sizeof(msg));

        if (wbpxre_decode_message(buf, n, &msg) < 0) {
            /* Invalid message, skip */
            #ifdef DEBUG_PROTOCOL
            fprintf(stderr, "DEBUG: Failed to decode message\n");
            #endif
            continue;
        }

        #ifdef DEBUG_PROTOCOL
        fprintf(stderr, "DEBUG: Decoded message type=%c (%d) transaction_id=%02x%02x\n",
               msg.type ? msg.type : '?', msg.type,
               msg.transaction_id[0], msg.transaction_id[1]);
        fprintf(stderr, "DEBUG: Transaction ID bytes: [0]=%02x [1]=%02x\n",
               (unsigned char)msg.transaction_id[0], (unsigned char)msg.transaction_id[1]);
        #endif

        /* Handle based on message type */
        if (msg.type == WBPXRE_MSG_RESPONSE) {
            /* Match to pending query */
            wbpxre_pending_query_t *pq = wbpxre_find_and_remove_pending_query(dht, msg.transaction_id);
            if (pq) {
                /* Copy response data */
                wbpxre_message_t *response_copy = malloc(sizeof(wbpxre_message_t));
                memcpy(response_copy, &msg, sizeof(wbpxre_message_t));

                /* Allocate copies of dynamic data */
                if (msg.nodes && msg.nodes_len > 0) {
                    response_copy->nodes = malloc(msg.nodes_len);
                    memcpy(response_copy->nodes, msg.nodes, msg.nodes_len);
                }
                if (msg.values && msg.values_len > 0) {
                    response_copy->values = malloc(msg.values_len);
                    memcpy(response_copy->values, msg.values, msg.values_len);
                }
                if (msg.samples && msg.samples_count > 0) {
                    int samples_len = msg.samples_count * WBPXRE_INFO_HASH_LEN;
                    response_copy->samples = malloc(samples_len);
                    memcpy(response_copy->samples, msg.samples, samples_len);
                }
                if (msg.token && msg.token_len > 0) {
                    response_copy->token = malloc(msg.token_len);
                    memcpy(response_copy->token, msg.token, msg.token_len);
                }

                /* Ensure NULL pointers for zero-length fields to prevent dangling pointers */
                if (msg.nodes_len == 0) response_copy->nodes = NULL;
                if (msg.values_len == 0) response_copy->values = NULL;
                if (msg.samples_count == 0) response_copy->samples = NULL;
                if (msg.token_len == 0) response_copy->token = NULL;

                signal_pending_query(pq, response_copy, false);
                /* Note: pq will be freed by the waiting thread, not here */

                pthread_mutex_lock(&dht->stats_mutex);
                dht->stats.responses_received++;
                pthread_mutex_unlock(&dht->stats_mutex);
            }
        } else if (msg.type == WBPXRE_MSG_ERROR) {
            /* Error response */
            wbpxre_pending_query_t *pq = wbpxre_find_and_remove_pending_query(dht, msg.transaction_id);
            if (pq) {
                signal_pending_query(pq, NULL, true);
                /* Note: pq will be freed by the waiting thread, not here */

                pthread_mutex_lock(&dht->stats_mutex);
                dht->stats.errors_received++;
                pthread_mutex_unlock(&dht->stats_mutex);
            }
        } else if (msg.type == WBPXRE_MSG_QUERY) {
            /* Incoming query - handle with responder */
            wbpxre_handle_incoming_query(dht, &msg, &from_addr);
        }

        /* Free dynamically allocated message fields */
        if (msg.nodes) free(msg.nodes);
        if (msg.values) free(msg.values);
        if (msg.samples) free(msg.samples);
        if (msg.token) free(msg.token);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* ============================================================================
 * Bootstrap
 * ============================================================================ */

int wbpxre_dht_bootstrap(wbpxre_dht_t *dht, const char *hostname, uint16_t port) {
    /* Use getaddrinfo instead of deprecated gethostbyname */
    struct addrinfo hints, *res, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;  /* IPv4 only for now */
    hints.ai_socktype = SOCK_DGRAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%u", port);

    int rc = getaddrinfo(hostname, port_str, &hints, &res);
    if (rc != 0) {
        fprintf(stderr, "Failed to resolve %s: %s\n", hostname, gai_strerror(rc));
        return -1;
    }

    struct sockaddr_in addr;
    int success = 0;

    /* Try each address returned */
    for (rp = res; rp != NULL; rp = rp->ai_next) {
        if (rp->ai_family != AF_INET) continue;

        memcpy(&addr, rp->ai_addr, sizeof(addr));

        /* Send ping to bootstrap node */
        uint8_t node_id[WBPXRE_NODE_ID_LEN];
        if (wbpxre_protocol_ping(dht, &addr, node_id) == 0) {
            /* Insert into routing table */
            wbpxre_routing_node_t node;
            memset(&node, 0, sizeof(node));
            memcpy(node.id, node_id, WBPXRE_NODE_ID_LEN);
            node.addr.addr = addr;
            inet_ntop(AF_INET, &addr.sin_addr, node.addr.ip, sizeof(node.addr.ip));
            node.addr.port = ntohs(addr.sin_port);
            node.discovered_at = time(NULL);
            node.last_responded_at = time(NULL);

            wbpxre_routing_table_insert(dht->routing_table, &node);

            /* Read node ID with lock protection */
            uint8_t local_node_id[WBPXRE_NODE_ID_LEN];
            pthread_rwlock_rdlock(&dht->node_id_lock);
            memcpy(local_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
            pthread_rwlock_unlock(&dht->node_id_lock);

            /* Follow up with find_node to discover more nodes */
            wbpxre_routing_node_t *found_nodes = NULL;
            int found_count = 0;
            if (wbpxre_protocol_find_node(dht, &addr, local_node_id,
                                         &found_nodes, &found_count) == 0) {
                /* Insert all discovered nodes */
                for (int i = 0; i < found_count; i++) {
                    /* Make a copy to avoid use-after-free if insert holds reference */
                    wbpxre_routing_node_t node_copy = found_nodes[i];
                    wbpxre_routing_table_insert(dht->routing_table, &node_copy);
                }
                if (found_count > 0) {
                    fprintf(stderr, "Bootstrap: Discovered %d nodes from %s:%d\n",
                           found_count, hostname, port);
                }
                /* Free allocated nodes */
                if (found_nodes) {
                    free(found_nodes);
                }
            }

            success = 1;
            break;  /* Success, stop trying other addresses */
        }
    }

    freeaddrinfo(res);
    return success ? 0 : -1;
}

/* ============================================================================
 * Main DHT API
 * ============================================================================ */

wbpxre_dht_t *wbpxre_dht_init(const wbpxre_config_t *config) {
    /* Initialize RCU library (MUST be called before any RCU operations) */
    rcu_init();

    wbpxre_dht_t *dht = calloc(1, sizeof(wbpxre_dht_t));
    if (!dht) return NULL;

    /* Copy config */
    memcpy(&dht->config, config, sizeof(wbpxre_config_t));

    /* Create UDP socket */
    dht->udp_socket = create_udp_socket(config->port);
    if (dht->udp_socket < 0) {
        free(dht);
        return NULL;
    }

    /* Initialize routing table */
    int max_nodes = config->max_routing_table_nodes > 0 ? config->max_routing_table_nodes : 10000;
    dht->routing_table = wbpxre_routing_table_create(max_nodes);
    if (!dht->routing_table) {
        close(dht->udp_socket);
        free(dht);
        return NULL;
    }

    /* Initialize mutexes and locks */
    pthread_rwlock_init(&dht->node_id_lock, NULL);
    pthread_mutex_init(&dht->pending_queries_mutex, NULL);
    pthread_mutex_init(&dht->sought_node_id_mutex, NULL);
    pthread_mutex_init(&dht->running_mutex, NULL);
    pthread_mutex_init(&dht->stats_mutex, NULL);
    pthread_mutex_init(&dht->udp_reader_ready_mutex, NULL);
    pthread_cond_init(&dht->udp_reader_ready_cond, NULL);

    /* Initialize pending queries hash table */
    memset(dht->pending_queries, 0, sizeof(dht->pending_queries));

    /* Generate initial sought node ID */
    wbpxre_random_bytes(dht->sought_node_id, WBPXRE_NODE_ID_LEN);

    /* Create work queues */
    dht->discovered_nodes = wbpxre_queue_create(config->discovered_nodes_capacity);
    dht->nodes_for_ping = wbpxre_queue_create(config->ping_queue_capacity);
    dht->nodes_for_find_node = wbpxre_queue_create(config->find_node_queue_capacity);
    dht->nodes_for_sample_infohashes = wbpxre_queue_create(config->sample_infohashes_capacity);
    dht->infohashes_for_get_peers = wbpxre_queue_create(WBPXRE_GET_PEERS_CAPACITY);

    dht->running = false;
    dht->udp_reader_ready = false;

    return dht;
}

/* ============================================================================
 * Worker Thread Functions
 * ============================================================================ */

/* Target rotation thread - rotates the sought node ID every 10 seconds */
static void *target_rotation_thread_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        sleep(10);  /* Rotate every 10 seconds */

        if (!dht->running) break;

        /* Generate new random target */
        pthread_mutex_lock(&dht->sought_node_id_mutex);
        wbpxre_random_bytes(dht->sought_node_id, WBPXRE_NODE_ID_LEN);
        pthread_mutex_unlock(&dht->sought_node_id_mutex);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Ping worker thread */
static void *ping_worker_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Pop node from ping queue (blocking) */
        wbpxre_routing_node_t *node = (wbpxre_routing_node_t *)wbpxre_queue_pop(dht->nodes_for_ping);
        if (!node) break;  /* Queue shutdown */

        /* Send ping */
        uint8_t response_id[WBPXRE_NODE_ID_LEN];
        int rc = wbpxre_protocol_ping(dht, &node->addr.addr, response_id);

        if (rc == 0) {
            /* Success -> update lastRespondedAt */
            wbpxre_routing_table_update_node_responded(dht->routing_table, node->id);
        } else {
            /* Failed -> drop node */
            wbpxre_routing_table_drop_node(dht->routing_table, node->id);

            /* Update dropped nodes statistics */
            pthread_mutex_lock(&dht->stats_mutex);
            dht->stats.nodes_dropped++;
            pthread_mutex_unlock(&dht->stats_mutex);
        }

        free(node);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Find node worker thread */
static void *find_node_worker_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Pop node from find_node queue (blocking) */
        wbpxre_routing_node_t *node = (wbpxre_routing_node_t *)wbpxre_queue_pop(dht->nodes_for_find_node);
        if (!node) break;  /* Queue shutdown */

        /* Get current target */
        pthread_mutex_lock(&dht->sought_node_id_mutex);
        uint8_t target[WBPXRE_NODE_ID_LEN];
        memcpy(target, dht->sought_node_id, WBPXRE_NODE_ID_LEN);
        pthread_mutex_unlock(&dht->sought_node_id_mutex);

        /* Send find_node query */
        wbpxre_routing_node_t *found_nodes = NULL;
        int found_count = 0;
        int rc = wbpxre_protocol_find_node(dht, &node->addr.addr, target,
                                          &found_nodes, &found_count);

        if (rc == 0) {
            /* Success -> update lastRespondedAt */
            wbpxre_routing_table_update_node_responded(dht->routing_table, node->id);

            /* Insert discovered nodes into routing table and discovered_nodes queue */
            for (int i = 0; i < found_count; i++) {
                /* Insert into routing table */
                wbpxre_routing_node_t node_copy = found_nodes[i];
                wbpxre_routing_table_insert(dht->routing_table, &node_copy);

                /* Try to push to discovered_nodes queue (non-blocking) */
                wbpxre_routing_node_t *discovered_copy = malloc(sizeof(wbpxre_routing_node_t));
                if (discovered_copy) {
                    memcpy(discovered_copy, &found_nodes[i], sizeof(wbpxre_routing_node_t));
                    discovered_copy->left = NULL;
                    discovered_copy->right = NULL;
                    if (!wbpxre_queue_try_push(dht->discovered_nodes, discovered_copy)) {
                        free(discovered_copy);  /* Queue full */
                    }
                }
            }

            if (found_nodes) free(found_nodes);
        } else {
            /* Failed -> drop node */
            wbpxre_routing_table_drop_node(dht->routing_table, node->id);
        }

        free(node);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Sample infohashes worker thread */
static void *sample_infohashes_worker_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Pop node from sample_infohashes queue (blocking) */
        wbpxre_routing_node_t *node = (wbpxre_routing_node_t *)wbpxre_queue_pop(dht->nodes_for_sample_infohashes);
        if (!node) break;  /* Queue shutdown */

        /* Check if node is eligible (respects interval) */
        time_t now = time(NULL);
        if (node->next_sample_time > now) {
            free(node);
            continue;  /* Too soon, skip */
        }

        /* Get current target */
        pthread_mutex_lock(&dht->sought_node_id_mutex);
        uint8_t target[WBPXRE_NODE_ID_LEN];
        memcpy(target, dht->sought_node_id, WBPXRE_NODE_ID_LEN);
        pthread_mutex_unlock(&dht->sought_node_id_mutex);

        /* Track query attempt BEFORE sending (so failed nodes have queries_sent > 0) */
        wbpxre_routing_table_update_node_queried(dht->routing_table, node->id);

        /* Send sample_infohashes query */
        uint8_t *hashes = NULL;
        int hash_count = 0, total_num = 0, interval = 0;

        int rc = wbpxre_protocol_sample_infohashes(dht, &node->addr.addr, target,
                                                    &hashes, &hash_count, &total_num, &interval);

        /* Count query attempt */
        pthread_mutex_lock(&dht->stats_mutex);
        dht->stats.bep51_queries_sent++;
        pthread_mutex_unlock(&dht->stats_mutex);

        if (rc == 0) {
            /* Success -> update node metadata */
            wbpxre_routing_table_update_node_responded(dht->routing_table, node->id);
            wbpxre_routing_table_update_sample_response(dht->routing_table, node->id,
                                                        hash_count, total_num, interval);

            /* Update stats */
            pthread_mutex_lock(&dht->stats_mutex);
            dht->stats.bep51_responses_received++;
            dht->stats.bep51_samples_received += hash_count;
            dht->stats.infohashes_discovered += hash_count;
            pthread_mutex_unlock(&dht->stats_mutex);

            /* Invoke callback with discovered hashes */
            if (hash_count > 0 && dht->config.callback) {
                dht->config.callback(dht->config.callback_closure, WBPXRE_EVENT_SAMPLES,
                                    NULL, hashes, hash_count * WBPXRE_INFO_HASH_LEN);
            }

            if (hashes) free(hashes);
        }

        free(node);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Get peers worker thread */
static void *get_peers_worker_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Pop info_hash from get_peers queue (blocking) */
        wbpxre_infohash_work_t *work = (wbpxre_infohash_work_t *)wbpxre_queue_pop(dht->infohashes_for_get_peers);
        if (!work) break;  /* Queue shutdown */

        /* Get K=8 closest nodes from routing table */
        wbpxre_routing_node_t *nodes[8];
        int node_count = wbpxre_routing_table_get_closest(dht->routing_table, work->info_hash, nodes, 8);

        if (node_count > 0) {
            /* Query each node for peers */
            for (int i = 0; i < node_count; i++) {
                if (!dht->running) {
                    /* Free remaining nodes */
                    for (int j = i; j < node_count; j++) {
                        free(nodes[j]);
                    }
                    break;
                }

                /* Send get_peers query */
                wbpxre_peer_t *peers = NULL;
                int peer_count = 0;
                wbpxre_routing_node_t *returned_nodes = NULL;
                int returned_node_count = 0;
                uint8_t token[256];
                int token_len = 0;

                int rc = wbpxre_protocol_get_peers(dht, &nodes[i]->addr.addr, work->info_hash,
                                                    &peers, &peer_count,
                                                    &returned_nodes, &returned_node_count,
                                                    token, &token_len);

                /* Count query attempt */
                pthread_mutex_lock(&dht->stats_mutex);
                dht->stats.get_peers_queries_sent++;
                pthread_mutex_unlock(&dht->stats_mutex);

                if (rc == 0) {
                    /* Success -> update node as responsive */
                    wbpxre_routing_table_update_node_responded(dht->routing_table, nodes[i]->id);

                    /* Update stats */
                    pthread_mutex_lock(&dht->stats_mutex);
                    dht->stats.get_peers_responses_received++;
                    pthread_mutex_unlock(&dht->stats_mutex);

                    /* Invoke callback with discovered peers */
                    if (peer_count > 0 && dht->config.callback) {
                        dht->config.callback(dht->config.callback_closure, WBPXRE_EVENT_VALUES,
                                            work->info_hash, peers, peer_count * sizeof(wbpxre_peer_t));
                    }

                    /* Add returned nodes to routing table */
                    for (int j = 0; j < returned_node_count; j++) {
                        wbpxre_routing_table_insert(dht->routing_table, &returned_nodes[j]);
                    }

                    /* Free the arrays (not individual elements, they're stack-allocated structs in the array) */
                    if (peers) free(peers);
                    if (returned_nodes) free(returned_nodes);

                    /* OPTIMIZATION: Continue querying all K=8 nodes instead of stopping after first peer.
                     * This maximizes peer discovery per query, improving metadata fetch success rates.
                     * The retry system will collect peers from multiple queries to reach the 10+ threshold.
                     *
                     * Old behavior: Stop after finding 1 peer → ~1-3 peers per query
                     * New behavior: Query all nodes → ~3-8 peers per query
                     */
                } else {
                    /* Failed -> drop node */
                    wbpxre_routing_table_drop_node(dht->routing_table, nodes[i]->id);
                }

                /* Free current node */
                free(nodes[i]);
            }
        }

        /* Notify that get_peers search is complete (whether peers found or not) */
        if (dht->config.callback) {
            dht->config.callback(dht->config.callback_closure, WBPXRE_EVENT_SEARCH_DONE,
                                work->info_hash, NULL, 0);
        }

        free(work);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Get peers feeder thread - not needed as we queue info_hashes on demand */
static void *get_peers_feeder_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    /* This thread is a placeholder for future optimizations
     * Currently, info_hashes are queued directly via wbpxre_dht_query_peers()
     * In the future, this could implement retry logic or smart scheduling */

    while (dht->running) {
        sleep(10);
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Discovered nodes dispatcher thread */
static void *discovered_nodes_dispatcher_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Pop from discovered_nodes queue */
        wbpxre_routing_node_t *node = (wbpxre_routing_node_t *)wbpxre_queue_pop(dht->discovered_nodes);
        if (!node) break;  /* Queue shutdown */

        /* Check if already in routing table */
        wbpxre_routing_node_t *existing = wbpxre_routing_table_find(dht->routing_table, node->id);
        if (existing) {
            free(node);
            continue;  /* Already known */
        }

        /* Try to dispatch to one of the pipelines (non-blocking) */
        if (wbpxre_queue_try_push(dht->nodes_for_find_node, node)) {
            continue;
        } else if (wbpxre_queue_try_push(dht->nodes_for_sample_infohashes, node)) {
            continue;
        } else if (wbpxre_queue_try_push(dht->nodes_for_ping, node)) {
            continue;
        } else {
            free(node);  /* All queues full, discard */
        }
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Sample infohashes feeder thread - continuously feeds nodes to the worker queue */
static void *sample_infohashes_feeder_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    static time_t last_queue_full_warn = 0;

    while (dht->running) {
        /* Get current node ID for keyspace-aware filtering */
        uint8_t current_node_id[WBPXRE_NODE_ID_LEN];
        pthread_rwlock_rdlock(&dht->node_id_lock);
        memcpy(current_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
        pthread_rwlock_unlock(&dht->node_id_lock);

        /* Get up to 200 nodes suitable for sample_infohashes (increased from 60) */
        wbpxre_routing_node_t *candidates[200];
        int count = wbpxre_routing_table_get_sample_candidates(dht->routing_table, current_node_id, candidates, 200);

        /* Feed them to worker queue */
        int dropped = 0;
        for (int i = 0; i < count; i++) {
            if (!dht->running) {
                /* Free remaining candidates */
                for (int j = i; j < count; j++) {
                    free(candidates[j]);
                }
                break;
            }

            /* Try to push (non-blocking to avoid deadlock) */
            if (!wbpxre_queue_try_push(dht->nodes_for_sample_infohashes, candidates[i])) {
                free(candidates[i]);  /* Queue full, discard */
                dropped++;
            }
        }

        /* Log queue saturation warning (throttled to once per 10 seconds) */
        if (dropped > 0) {
            time_t now = time(NULL);
            if (now - last_queue_full_warn >= 10) {
                fprintf(stderr, "WARNING: sample_infohashes queue full, dropped %d/%d nodes\n", dropped, count);
                last_queue_full_warn = now;
            }
        }

        sleep(1);  /* Run every second */
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Ping feeder thread - continuously feeds old nodes for verification */
static void *ping_feeder_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Get old nodes that haven't responded in last 60 seconds (or never) */
        time_t threshold = time(NULL) - 60;
        wbpxre_routing_node_t *candidates[20];
        int count = wbpxre_routing_table_get_old_nodes(dht->routing_table, candidates, 20, threshold);

        /* Feed them to worker queue */
        for (int i = 0; i < count; i++) {
            if (!dht->running) {
                /* Free remaining candidates */
                for (int j = i; j < count; j++) {
                    free(candidates[j]);
                }
                break;
            }

            /* Try to push (non-blocking) */
            if (!wbpxre_queue_try_push(dht->nodes_for_ping, candidates[i])) {
                free(candidates[i]);  /* Queue full, discard */
            }
        }

        sleep(10);  /* Run every 10 seconds */
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

/* Find node feeder thread - continuously feeds nodes for find_node queries */
static void *find_node_feeder_func(void *arg) {
    wbpxre_dht_t *dht = (wbpxre_dht_t *)arg;

    /* Register this thread with RCU (REQUIRED before rcu_read_lock) */
    rcu_register_thread();

    while (dht->running) {
        /* Get current target */
        pthread_mutex_lock(&dht->sought_node_id_mutex);
        uint8_t target[WBPXRE_NODE_ID_LEN];
        memcpy(target, dht->sought_node_id, WBPXRE_NODE_ID_LEN);
        pthread_mutex_unlock(&dht->sought_node_id_mutex);

        /* Get up to 10 nodes closest to target */
        wbpxre_routing_node_t *candidates[10];
        int count = wbpxre_routing_table_get_closest(dht->routing_table, target, candidates, 10);

        /* Feed them to worker queue */
        for (int i = 0; i < count; i++) {
            if (!dht->running) {
                /* Free remaining candidates */
                for (int j = i; j < count; j++) {
                    free(candidates[j]);
                }
                break;
            }

            /* Try to push (non-blocking) */
            if (!wbpxre_queue_try_push(dht->nodes_for_find_node, candidates[i])) {
                free(candidates[i]);  /* Queue full, discard */
            }
        }

        sleep(5);  /* Run every 5 seconds */
    }

    /* Unregister thread before exit (REQUIRED) */
    rcu_unregister_thread();
    return NULL;
}

int wbpxre_dht_start(wbpxre_dht_t *dht) {
    if (!dht) return -1;

    dht->running = true;

    /* Start UDP reader thread */
    pthread_create(&dht->udp_reader_thread, NULL, udp_reader_thread_func, dht);

    /* Start target rotation thread */
    pthread_create(&dht->target_rotation_thread, NULL, target_rotation_thread_func, dht);

    /* Allocate worker thread arrays */
    dht->ping_worker_threads = calloc(dht->config.ping_workers, sizeof(pthread_t));
    dht->find_node_worker_threads = calloc(dht->config.find_node_workers, sizeof(pthread_t));
    dht->sample_infohashes_worker_threads = calloc(dht->config.sample_infohashes_workers, sizeof(pthread_t));
    dht->get_peers_worker_threads = calloc(dht->config.get_peers_workers, sizeof(pthread_t));

    /* Start ping workers */
    for (int i = 0; i < dht->config.ping_workers; i++) {
        pthread_create(&dht->ping_worker_threads[i], NULL, ping_worker_func, dht);
    }

    /* Start find_node workers */
    for (int i = 0; i < dht->config.find_node_workers; i++) {
        pthread_create(&dht->find_node_worker_threads[i], NULL, find_node_worker_func, dht);
    }

    /* Start sample_infohashes workers */
    for (int i = 0; i < dht->config.sample_infohashes_workers; i++) {
        pthread_create(&dht->sample_infohashes_worker_threads[i], NULL, sample_infohashes_worker_func, dht);
    }

    /* Start get_peers workers */
    for (int i = 0; i < dht->config.get_peers_workers; i++) {
        pthread_create(&dht->get_peers_worker_threads[i], NULL, get_peers_worker_func, dht);
    }

    /* Start discovered nodes dispatcher */
    pthread_create(&dht->discovered_nodes_dispatcher_thread, NULL, discovered_nodes_dispatcher_func, dht);

    /* Start feeder threads */
    pthread_create(&dht->sample_infohashes_feeder_thread, NULL, sample_infohashes_feeder_func, dht);
    pthread_create(&dht->find_node_feeder_thread, NULL, find_node_feeder_func, dht);
    pthread_create(&dht->ping_feeder_thread, NULL, ping_feeder_func, dht);
    pthread_create(&dht->get_peers_feeder_thread, NULL, get_peers_feeder_func, dht);

    return 0;
}

int wbpxre_dht_stop(wbpxre_dht_t *dht) {
    if (!dht) return -1;

    pthread_mutex_lock(&dht->running_mutex);
    dht->running = false;
    pthread_mutex_unlock(&dht->running_mutex);

    /* Shutdown queues to wake up blocking workers */
    wbpxre_queue_shutdown(dht->discovered_nodes);
    wbpxre_queue_shutdown(dht->nodes_for_ping);
    wbpxre_queue_shutdown(dht->nodes_for_find_node);
    wbpxre_queue_shutdown(dht->nodes_for_sample_infohashes);
    wbpxre_queue_shutdown(dht->infohashes_for_get_peers);

    /* Wait for worker threads */
    if (dht->ping_worker_threads) {
        for (int i = 0; i < dht->config.ping_workers; i++) {
            pthread_join(dht->ping_worker_threads[i], NULL);
        }
        free(dht->ping_worker_threads);
        dht->ping_worker_threads = NULL;
    }

    if (dht->find_node_worker_threads) {
        for (int i = 0; i < dht->config.find_node_workers; i++) {
            pthread_join(dht->find_node_worker_threads[i], NULL);
        }
        free(dht->find_node_worker_threads);
        dht->find_node_worker_threads = NULL;
    }

    if (dht->sample_infohashes_worker_threads) {
        for (int i = 0; i < dht->config.sample_infohashes_workers; i++) {
            pthread_join(dht->sample_infohashes_worker_threads[i], NULL);
        }
        free(dht->sample_infohashes_worker_threads);
        dht->sample_infohashes_worker_threads = NULL;
    }

    if (dht->get_peers_worker_threads) {
        for (int i = 0; i < dht->config.get_peers_workers; i++) {
            pthread_join(dht->get_peers_worker_threads[i], NULL);
        }
        free(dht->get_peers_worker_threads);
        dht->get_peers_worker_threads = NULL;
    }

    /* Wait for dispatcher and feeder threads */
    pthread_join(dht->discovered_nodes_dispatcher_thread, NULL);
    pthread_join(dht->sample_infohashes_feeder_thread, NULL);
    pthread_join(dht->find_node_feeder_thread, NULL);
    pthread_join(dht->ping_feeder_thread, NULL);
    pthread_join(dht->get_peers_feeder_thread, NULL);

    /* Close socket BEFORE joining UDP reader thread to unblock recvfrom()
     * This fixes hang where udp_reader_thread is blocked in recvfrom() */
    if (dht->udp_socket >= 0) {
        shutdown(dht->udp_socket, SHUT_RDWR);  /* Unblock any pending recv/send */
        close(dht->udp_socket);
        dht->udp_socket = -1;
    }

    /* Wait for other threads (UDP reader should now exit quickly) */
    pthread_join(dht->udp_reader_thread, NULL);
    pthread_join(dht->target_rotation_thread, NULL);

    return 0;
}

void wbpxre_dht_cleanup(wbpxre_dht_t *dht) {
    if (!dht) return;

    /* Socket already closed in wbpxre_dht_stop() to unblock UDP reader thread
     * Just skip if already closed */
    if (dht->udp_socket >= 0) {
        close(dht->udp_socket);
        dht->udp_socket = -1;
    }

    /* Wait for all pending RCU callbacks to complete (CRITICAL!) */
    rcu_barrier();

    /* Destroy routing table */
    if (dht->routing_table) {
        wbpxre_routing_table_destroy(dht->routing_table);
    }

    /* Destroy queues */
    if (dht->discovered_nodes) wbpxre_queue_destroy(dht->discovered_nodes);
    if (dht->nodes_for_ping) wbpxre_queue_destroy(dht->nodes_for_ping);
    if (dht->nodes_for_find_node) wbpxre_queue_destroy(dht->nodes_for_find_node);
    if (dht->nodes_for_sample_infohashes) wbpxre_queue_destroy(dht->nodes_for_sample_infohashes);
    if (dht->infohashes_for_get_peers) wbpxre_queue_destroy(dht->infohashes_for_get_peers);

    /* Destroy mutexes and locks */
    pthread_rwlock_destroy(&dht->node_id_lock);
    pthread_mutex_destroy(&dht->pending_queries_mutex);
    pthread_mutex_destroy(&dht->sought_node_id_mutex);
    pthread_mutex_destroy(&dht->running_mutex);
    pthread_mutex_destroy(&dht->stats_mutex);
    pthread_mutex_destroy(&dht->udp_reader_ready_mutex);
    pthread_cond_destroy(&dht->udp_reader_ready_cond);

    /* Free pending queries */
    for (int i = 0; i < 256; i++) {
        wbpxre_pending_query_t *pq = dht->pending_queries[i];
        while (pq) {
            wbpxre_pending_query_t *next = pq->next;
            wbpxre_free_pending_query(pq);
            pq = next;
        }
    }

    free(dht);
}

int wbpxre_dht_change_node_id(wbpxre_dht_t *dht, const uint8_t *new_node_id) {
    if (!dht || !new_node_id) {
        return -1;
    }

    /* Acquire write lock (blocks until all readers finish) */
    pthread_rwlock_wrlock(&dht->node_id_lock);

    /* Atomic swap */
    memcpy(dht->config.node_id, new_node_id, WBPXRE_NODE_ID_LEN);

    /* Release lock */
    pthread_rwlock_unlock(&dht->node_id_lock);

    return 0;
}

int wbpxre_dht_insert_node(wbpxre_dht_t *dht, const uint8_t *node_id,
                            const struct sockaddr_in *addr) {
    wbpxre_routing_node_t node;
    memset(&node, 0, sizeof(node));
    memcpy(node.id, node_id, WBPXRE_NODE_ID_LEN);
    node.addr.addr = *addr;
    inet_ntop(AF_INET, &addr->sin_addr, node.addr.ip, sizeof(node.addr.ip));
    node.addr.port = ntohs(addr->sin_port);
    node.discovered_at = time(NULL);
    node.last_responded_at = time(NULL);

    return wbpxre_routing_table_insert(dht->routing_table, &node);
}

int wbpxre_dht_query_peers(wbpxre_dht_t *dht, const uint8_t *info_hash, bool priority) {
    if (!dht || !info_hash || !dht->infohashes_for_get_peers) {
        return -1;
    }

    /* Create work item */
    wbpxre_infohash_work_t *work = malloc(sizeof(wbpxre_infohash_work_t));
    if (!work) return -1;

    memcpy(work->info_hash, info_hash, WBPXRE_INFO_HASH_LEN);
    work->added_at = time(NULL);

    /* Try to push to queue (non-blocking to avoid blocking DHT callback)
     * Use front insertion for priority queries (e.g., on-demand /refresh requests) */
    bool success = priority ?
        wbpxre_queue_try_push_front(dht->infohashes_for_get_peers, work) :
        wbpxre_queue_try_push(dht->infohashes_for_get_peers, work);

    if (!success) {
        free(work);
        return -1;  /* Queue full */
    }

    return 0;
}

int wbpxre_dht_nodes(wbpxre_dht_t *dht, int *good_return, int *dubious_return) {
    if (!dht || !dht->routing_table) return -1;

    /* With RCU, node_count is updated atomically and can be read without locks */
    int total = dht->routing_table->node_count;

    if (good_return) *good_return = total;
    if (dubious_return) *dubious_return = 0;

    return 0;
}

/* Wait for UDP reader to be ready (Phase 1) */
int wbpxre_dht_wait_ready(wbpxre_dht_t *dht, int timeout_sec) {
    if (!dht) return -1;

    pthread_mutex_lock(&dht->udp_reader_ready_mutex);

    if (dht->udp_reader_ready) {
        pthread_mutex_unlock(&dht->udp_reader_ready_mutex);
        return 0;
    }

    /* Wait with timeout */
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += timeout_sec;

    int rc = pthread_cond_timedwait(&dht->udp_reader_ready_cond,
                                    &dht->udp_reader_ready_mutex, &ts);

    bool ready = dht->udp_reader_ready;
    pthread_mutex_unlock(&dht->udp_reader_ready_mutex);

    if (rc == ETIMEDOUT) {
        fprintf(stderr, "ERROR: UDP reader thread did not become ready within %d seconds\n",
                timeout_sec);
        return -1;
    }

    return ready ? 0 : -1;
}

/* Test socket warmup (Phase 4) - send a test packet to verify UDP stack is working */
int wbpxre_dht_test_socket(wbpxre_dht_t *dht) {
    if (!dht) return -1;

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: Testing UDP socket with loopback ping...\n");
    #endif

    /* Try to ping localhost */
    struct sockaddr_in localhost;
    memset(&localhost, 0, sizeof(localhost));
    localhost.sin_family = AF_INET;
    localhost.sin_addr.s_addr = inet_addr("127.0.0.1");
    localhost.sin_port = htons(dht->config.port);

    /* Send a ping to ourselves */
    wbpxre_message_t msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = WBPXRE_MSG_QUERY;
    msg.method = WBPXRE_METHOD_PING;
    wbpxre_random_bytes(msg.transaction_id, WBPXRE_TRANSACTION_ID_LEN);

    /* Read node ID with lock protection */
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(msg.id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    uint8_t buf[WBPXRE_MAX_UDP_PACKET];
    int len = wbpxre_encode_message(&msg, buf, sizeof(buf));

    if (len < 0) {
        fprintf(stderr, "ERROR: Failed to encode test message\n");
        return -1;
    }

    /* Send packet */
    int sent = sendto(dht->udp_socket, buf, len, 0,
                      (struct sockaddr *)&localhost, sizeof(localhost));

    if (sent < 0) {
        // Silently ignore sendto errors during loopback test
        fprintf(stderr, "ERROR: Failed to send loopback test packet\n");
        return -1;
    }

    /* Wait a bit for the packet to be received */
    usleep(10000);  /* 10ms */

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: Socket test completed (sent %d bytes)\n", sent);
    fprintf(stderr, "DEBUG: UDP stack appears to be working\n");
    #endif

    return 0;
}

int wbpxre_dht_get_stats(wbpxre_dht_t *dht, wbpxre_stats_t *stats_out) {
    if (!dht || !stats_out) {
        return -1;
    }

    pthread_mutex_lock(&dht->stats_mutex);
    stats_out->packets_sent = dht->stats.packets_sent;
    stats_out->packets_received = dht->stats.packets_received;
    stats_out->queries_sent = dht->stats.queries_sent;
    stats_out->responses_received = dht->stats.responses_received;
    stats_out->errors_received = dht->stats.errors_received;
    stats_out->nodes_discovered = dht->stats.nodes_discovered;
    stats_out->infohashes_discovered = dht->stats.infohashes_discovered;
    stats_out->bep51_queries_sent = dht->stats.bep51_queries_sent;
    stats_out->bep51_responses_received = dht->stats.bep51_responses_received;
    stats_out->bep51_samples_received = dht->stats.bep51_samples_received;
    stats_out->get_peers_queries_sent = dht->stats.get_peers_queries_sent;
    stats_out->get_peers_responses_received = dht->stats.get_peers_responses_received;
    stats_out->nodes_dropped = dht->stats.nodes_dropped;
    stats_out->nodes_dropped_unresponsive = dht->stats.nodes_dropped_unresponsive;
    stats_out->aggressive_prune_triggers = dht->stats.aggressive_prune_triggers;
    pthread_mutex_unlock(&dht->stats_mutex);

    return 0;
}

void wbpxre_dht_print_stats(wbpxre_dht_t *dht) {
    if (!dht) return;

    pthread_mutex_lock(&dht->stats_mutex);
    printf("\n=== wbpxre-dht Statistics ===\n");
    printf("Packets sent:        %lu\n", dht->stats.packets_sent);
    printf("Packets received:    %lu\n", dht->stats.packets_received);
    printf("Queries sent:        %lu\n", dht->stats.queries_sent);
    printf("Responses received:  %lu\n", dht->stats.responses_received);
    printf("Errors received:     %lu\n", dht->stats.errors_received);
    printf("Nodes discovered:    %lu\n", dht->stats.nodes_discovered);
    printf("InfoHashes discovered: %lu\n", dht->stats.infohashes_discovered);
    printf("BEP 51 queries sent: %lu\n", dht->stats.bep51_queries_sent);
    printf("BEP 51 responses:    %lu\n", dht->stats.bep51_responses_received);
    printf("BEP 51 samples:      %lu\n", dht->stats.bep51_samples_received);
    printf("get_peers queries:   %lu\n", dht->stats.get_peers_queries_sent);
    printf("get_peers responses: %lu\n", dht->stats.get_peers_responses_received);
    pthread_mutex_unlock(&dht->stats_mutex);

    int good = 0, dubious = 0;
    wbpxre_dht_nodes(dht, &good, &dubious);
    printf("Routing table nodes: %d\n", good);
    printf("=============================\n\n");
}
