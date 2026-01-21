#define _DEFAULT_SOURCE  /* For usleep */

#include "tree_dispatcher.h"
#include "thread_tree.h"
#include "tree_protocol.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <arpa/inet.h>

/* TID comparison helper for uthash
 * NOTE: uthash requires exact key length match, so we use variable-length keys */

/* Dispatcher thread function */
static void *dispatcher_thread_func(void *arg) {
    tree_dispatcher_t *dispatcher = (tree_dispatcher_t *)arg;
    tree_socket_t *sock = dispatcher->socket;
    uint8_t recv_buf[MAX_RESPONSE_SIZE];

    /* DEBUG: Log socket details at startup */
    int sock_fd = sock ? sock->fd : -1;
    int sock_port = tree_socket_get_port(sock);
    uint32_t tree_id = dispatcher->tree ? dispatcher->tree->tree_id : 0;
    const char *prefix = dispatcher->tree ? "" : "shared ";

    log_msg(LOG_DEBUG, "[%stree %u] ===== DISPATCHER THREAD STARTED ===== (socket_fd=%d, port=%d)",
            prefix, tree_id, sock_fd, sock_port);

    time_t last_cleanup = time(NULL);
    time_t last_stats = time(NULL);
    const int CLEANUP_INTERVAL = 30;  /* Cleanup stale TIDs every 30 seconds */
    const int STATS_INTERVAL = 10;    /* Log stats every 10 seconds */
    const int TID_TIMEOUT = 10;       /* Remove TID registrations older than 10 seconds */

    unsigned long loop_count = 0;
    unsigned long timeout_count = 0;

    while (atomic_load(&dispatcher->running)) {
        loop_count++;

        /* Receive UDP packet (100ms timeout for responsiveness) */
        struct sockaddr_storage from;
        int recv_len = tree_socket_recv(sock, recv_buf, sizeof(recv_buf), &from, 100);

        if (recv_len <= 0) {
            timeout_count++;

            /* DEBUG: Log periodic timeout info */
            if (timeout_count % 100 == 1) {
                log_msg(LOG_DEBUG, "[%stree %u] Dispatcher: %lu consecutive timeouts (recv_len=%d, errno=%d)",
                        prefix, tree_id, timeout_count, recv_len, errno);
            }

            /* Timeout or error - check for cleanup */
            time_t now = time(NULL);
            if (now - last_cleanup >= CLEANUP_INTERVAL) {
                int removed = tree_dispatcher_cleanup_stale_tids(dispatcher, TID_TIMEOUT);
                if (removed > 0) {
                    log_msg(LOG_DEBUG, "[%stree %u] Dispatcher cleaned up %d stale TID registrations",
                            prefix, tree_id, removed);
                }
                last_cleanup = now;
            }

            /* DEBUG: Log periodic statistics */
            if (now - last_stats >= STATS_INTERVAL) {
                unsigned long total = atomic_load(&dispatcher->total_responses);
                unsigned long routed = atomic_load(&dispatcher->routed_responses);
                unsigned long dropped = atomic_load(&dispatcher->dropped_responses);
                unsigned long queue_full = atomic_load(&dispatcher->queue_full_drops);

                log_msg(LOG_DEBUG, "[%stree %u] DISPATCHER STATS: loops=%lu, timeouts=%lu, packets=%lu, routed=%lu, dropped=%lu, queue_full=%lu",
                        prefix, tree_id, loop_count, timeout_count, total, routed, dropped, queue_full);
                last_stats = now;
            }

            continue;
        }

        /* DEBUG: Packet received! */
        timeout_count = 0;  /* Reset timeout counter */
        char from_ip[INET6_ADDRSTRLEN] = {0};
        uint16_t from_port = 0;
        if (from.ss_family == AF_INET) {
            struct sockaddr_in *sin = (struct sockaddr_in *)&from;
            inet_ntop(AF_INET, &sin->sin_addr, from_ip, sizeof(from_ip));
            from_port = ntohs(sin->sin_port);
        }

        log_msg(LOG_DEBUG, "[%stree %u] Dispatcher RECEIVED packet: len=%d, from=%s:%u",
                prefix, tree_id, recv_len, from_ip, from_port);

        atomic_fetch_add(&dispatcher->total_responses, 1);

        /* Extract TID from response */
        uint8_t tid[MAX_TID_SIZE];
        int tid_len;
        if (tree_protocol_extract_tid(recv_buf, recv_len, tid, &tid_len) != 0) {
            /* Failed to extract TID - might be malformed packet or query (not response) */
            log_msg(LOG_DEBUG, "[%stree %u] Dispatcher: Failed to extract TID from packet (len=%d)",
                    prefix, tree_id, recv_len);
            atomic_fetch_add(&dispatcher->dropped_responses, 1);
            continue;
        }

        /* DEBUG: Log TID extraction success */
        log_msg(LOG_DEBUG, "[%stree %u] Dispatcher: Extracted TID (len=%d): %02x %02x %02x",
                prefix, tree_id, tid_len, tid[0], tid[1], tid[2]);

        /* Find response queue for this TID using uthash O(1) lookup */
        pthread_rwlock_rdlock(&dispatcher->tid_map_lock);

        tid_registration_t *reg = NULL;
        tree_response_queue_t *target_queue = NULL;

        /* uthash lookup with variable-length key */
        HASH_FIND(hh, dispatcher->tid_map_head, tid, tid_len, reg);
        if (reg) {
            target_queue = reg->response_queue;
        }

        pthread_rwlock_unlock(&dispatcher->tid_map_lock);

        if (!target_queue) {
            /* No registered worker for this TID - might be response to old query */
            log_msg(LOG_DEBUG, "[%stree %u] Dispatcher: No registered worker for TID %02x%02x%02x",
                    prefix, tree_id, tid[0], tid[1], tid[2]);
            atomic_fetch_add(&dispatcher->dropped_responses, 1);
            continue;
        }

        /* DEBUG: Successfully found target queue */
        log_msg(LOG_DEBUG, "[%stree %u] Dispatcher: Routing to worker queue (TID=%02x%02x%02x)",
                prefix, tree_id, tid[0], tid[1], tid[2]);

        /* Route response to worker queue */
        tree_response_t response;
        if (recv_len > (int)sizeof(response.data)) {
            recv_len = sizeof(response.data);
        }
        memcpy(response.data, recv_buf, recv_len);
        response.len = recv_len;
        memcpy(&response.from, &from, sizeof(struct sockaddr_storage));

        if (tree_response_queue_try_push(target_queue, &response) == 0) {
            atomic_fetch_add(&dispatcher->routed_responses, 1);
            log_msg(LOG_DEBUG, "[%stree %u] Dispatcher: Successfully routed response to worker",
                    prefix, tree_id);
        } else {
            /* Worker queue full - drop response */
            log_msg(LOG_WARN, "[%stree %u] Dispatcher: Worker queue FULL, dropping response",
                    prefix, tree_id);
            atomic_fetch_add(&dispatcher->queue_full_drops, 1);
        }
    }

    log_msg(LOG_DEBUG, "[%stree %u] Dispatcher thread exiting (total=%lu, routed=%lu, dropped=%lu, queue_full=%lu)",
            prefix, tree_id,
            (unsigned long)atomic_load(&dispatcher->total_responses),
            (unsigned long)atomic_load(&dispatcher->routed_responses),
            (unsigned long)atomic_load(&dispatcher->dropped_responses),
            (unsigned long)atomic_load(&dispatcher->queue_full_drops));

    return NULL;
}

tree_dispatcher_t *tree_dispatcher_create(struct thread_tree *tree, tree_socket_t *socket) {
    /* Socket required, tree can be NULL for shared dispatcher */
    if (!socket) {
        log_msg(LOG_ERROR, "[dispatcher] NULL socket");
        return NULL;
    }

    tree_dispatcher_t *dispatcher = calloc(1, sizeof(tree_dispatcher_t));
    if (!dispatcher) {
        log_msg(LOG_ERROR, "[%sdispatcher] Failed to allocate dispatcher",
                tree ? "" : "shared ");
        return NULL;
    }

    dispatcher->tree = tree;
    dispatcher->socket = socket;

    /* Initialize TID map (uthash - starts empty) */
    dispatcher->tid_map_head = NULL;  /* uthash: NULL = empty hash table */

    if (pthread_rwlock_init(&dispatcher->tid_map_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[%sdispatcher] Failed to init TID map lock",
                tree ? "" : "shared ");
        free(dispatcher);
        return NULL;
    }

    atomic_store(&dispatcher->running, false);
    atomic_store(&dispatcher->total_responses, 0);
    atomic_store(&dispatcher->routed_responses, 0);
    atomic_store(&dispatcher->dropped_responses, 0);
    atomic_store(&dispatcher->queue_full_drops, 0);

    if (tree) {
        log_msg(LOG_DEBUG, "[tree %u] Dispatcher created", tree->tree_id);
    } else {
        log_msg(LOG_DEBUG, "[shared dispatcher] Created");
    }

    return dispatcher;
}

int tree_dispatcher_start(tree_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return -1;
    }

    atomic_store(&dispatcher->running, true);

    int rc = pthread_create(&dispatcher->dispatcher_thread, NULL, dispatcher_thread_func, dispatcher);
    if (rc != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to create dispatcher thread: %d",
                dispatcher->tree ? dispatcher->tree->tree_id : 0, rc);
        atomic_store(&dispatcher->running, false);
        return -1;
    }

    log_msg(LOG_DEBUG, "[tree %u] Dispatcher thread started",
            dispatcher->tree ? dispatcher->tree->tree_id : 0);

    return 0;
}

void tree_dispatcher_stop(tree_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }

    atomic_store(&dispatcher->running, false);

    if (dispatcher->dispatcher_thread) {
        pthread_join(dispatcher->dispatcher_thread, NULL);
        dispatcher->dispatcher_thread = 0;
    }

    log_msg(LOG_DEBUG, "[tree %u] Dispatcher thread stopped",
            dispatcher->tree ? dispatcher->tree->tree_id : 0);
}

void tree_dispatcher_destroy(tree_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }

    tree_dispatcher_stop(dispatcher);

    /* Free all TID registrations using uthash iteration */
    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    tid_registration_t *reg, *tmp;
    HASH_ITER(hh, dispatcher->tid_map_head, reg, tmp) {
        HASH_DEL(dispatcher->tid_map_head, reg);
        free(reg);
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);
    pthread_rwlock_destroy(&dispatcher->tid_map_lock);

    free(dispatcher);

    log_msg(LOG_DEBUG, "[dispatcher] Destroyed");
}

int tree_dispatcher_register_tid(tree_dispatcher_t *dispatcher,
                                  const uint8_t *tid, int tid_len,
                                  tree_response_queue_t *response_queue) {
    if (!dispatcher || !tid || tid_len <= 0 || tid_len > MAX_TID_SIZE || !response_queue) {
        return -1;
    }

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    /* Check for duplicate TID using uthash O(1) lookup */
    tid_registration_t *existing = NULL;
    HASH_FIND(hh, dispatcher->tid_map_head, tid, tid_len, existing);

    if (existing) {
        /* TID already registered - this is a bug, but update it anyway */
        log_msg(LOG_WARN, "[tree %u] TID already registered, updating",
                dispatcher->tree ? dispatcher->tree->tree_id : 0);
        existing->response_queue = response_queue;
        existing->registered_at = time(NULL);
        pthread_rwlock_unlock(&dispatcher->tid_map_lock);
        return 0;
    }

    /* Create new registration */
    tid_registration_t *reg = malloc(sizeof(tid_registration_t));
    if (!reg) {
        pthread_rwlock_unlock(&dispatcher->tid_map_lock);
        return -1;
    }

    memcpy(reg->tid, tid, tid_len);
    reg->tid_len = tid_len;
    reg->response_queue = response_queue;
    reg->registered_at = time(NULL);

    /* Add to uthash table (variable-length key) */
    HASH_ADD_KEYPTR(hh, dispatcher->tid_map_head, reg->tid, reg->tid_len, reg);

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);

    return 0;
}

void tree_dispatcher_unregister_tid(tree_dispatcher_t *dispatcher,
                                    const uint8_t *tid, int tid_len) {
    if (!dispatcher || !tid || tid_len <= 0) {
        return;
    }

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    /* Find and delete registration using uthash O(1) operations */
    tid_registration_t *reg = NULL;
    HASH_FIND(hh, dispatcher->tid_map_head, tid, tid_len, reg);

    if (reg) {
        HASH_DEL(dispatcher->tid_map_head, reg);
        free(reg);
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);
}

int tree_dispatcher_cleanup_stale_tids(tree_dispatcher_t *dispatcher, int timeout_sec) {
    if (!dispatcher || timeout_sec <= 0) {
        return 0;
    }

    time_t now = time(NULL);
    int removed = 0;

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    /* Iterate all registrations using uthash and remove stale ones */
    tid_registration_t *reg, *tmp;
    HASH_ITER(hh, dispatcher->tid_map_head, reg, tmp) {
        if ((now - reg->registered_at) > timeout_sec) {
            HASH_DEL(dispatcher->tid_map_head, reg);
            free(reg);
            removed++;
        }
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);

    return removed;
}
