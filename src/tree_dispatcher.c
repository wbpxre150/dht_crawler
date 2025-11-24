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

/* Hash function for TID (simple sum mod capacity) */
static int tid_hash(const uint8_t *tid, int tid_len, int capacity) {
    if (!tid || tid_len <= 0 || capacity <= 0) {
        return 0;
    }

    int hash = 0;
    for (int i = 0; i < tid_len; i++) {
        hash += tid[i];
    }
    return hash % capacity;
}

/* Dispatcher thread function */
static void *dispatcher_thread_func(void *arg) {
    tree_dispatcher_t *dispatcher = (tree_dispatcher_t *)arg;
    tree_socket_t *sock = dispatcher->socket;
    uint8_t recv_buf[MAX_RESPONSE_SIZE];

    /* DEBUG: Log socket details at startup */
    int sock_fd = sock ? sock->fd : -1;
    int sock_port = tree_socket_get_port(sock);
    uint32_t tree_id = dispatcher->tree ? dispatcher->tree->tree_id : 0;

    log_msg(LOG_DEBUG, "[tree %u] ===== DISPATCHER THREAD STARTED ===== (socket_fd=%d, port=%d)",
            tree_id, sock_fd, sock_port);

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
                log_msg(LOG_DEBUG, "[tree %u] Dispatcher: %lu consecutive timeouts (recv_len=%d, errno=%d)",
                        tree_id, timeout_count, recv_len, errno);
            }

            /* Timeout or error - check for cleanup */
            time_t now = time(NULL);
            if (now - last_cleanup >= CLEANUP_INTERVAL) {
                int removed = tree_dispatcher_cleanup_stale_tids(dispatcher, TID_TIMEOUT);
                if (removed > 0) {
                    log_msg(LOG_DEBUG, "[tree %u] Dispatcher cleaned up %d stale TID registrations",
                            tree_id, removed);
                }
                last_cleanup = now;
            }

            /* DEBUG: Log periodic statistics */
            if (now - last_stats >= STATS_INTERVAL) {
                unsigned long total = atomic_load(&dispatcher->total_responses);
                unsigned long routed = atomic_load(&dispatcher->routed_responses);
                unsigned long dropped = atomic_load(&dispatcher->dropped_responses);
                unsigned long queue_full = atomic_load(&dispatcher->queue_full_drops);

                log_msg(LOG_DEBUG, "[tree %u] DISPATCHER STATS: loops=%lu, timeouts=%lu, packets=%lu, routed=%lu, dropped=%lu, queue_full=%lu",
                        tree_id, loop_count, timeout_count, total, routed, dropped, queue_full);
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

        log_msg(LOG_DEBUG, "[tree %u] Dispatcher RECEIVED packet: len=%d, from=%s:%u",
                tree_id, recv_len, from_ip, from_port);

        atomic_fetch_add(&dispatcher->total_responses, 1);

        /* Extract TID from response */
        uint8_t tid[MAX_TID_SIZE];
        int tid_len;
        if (tree_protocol_extract_tid(recv_buf, recv_len, tid, &tid_len) != 0) {
            /* Failed to extract TID - might be malformed packet or query (not response) */
            log_msg(LOG_DEBUG, "[tree %u] Dispatcher: Failed to extract TID from packet (len=%d)",
                    tree_id, recv_len);
            atomic_fetch_add(&dispatcher->dropped_responses, 1);
            continue;
        }

        /* DEBUG: Log TID extraction success */
        log_msg(LOG_DEBUG, "[tree %u] Dispatcher: Extracted TID (len=%d): %02x %02x %02x",
                tree_id, tid_len, tid[0], tid[1], tid[2]);

        /* Find response queue for this TID */
        pthread_rwlock_rdlock(&dispatcher->tid_map_lock);

        int hash_idx = tid_hash(tid, tid_len, dispatcher->tid_map_capacity);
        tid_registration_t *reg = dispatcher->tid_map[hash_idx];

        tree_response_queue_t *target_queue = NULL;
        while (reg) {
            if (reg->tid_len == tid_len && memcmp(reg->tid, tid, tid_len) == 0) {
                target_queue = reg->response_queue;
                break;
            }
            reg = reg->next;
        }

        pthread_rwlock_unlock(&dispatcher->tid_map_lock);

        if (!target_queue) {
            /* No registered worker for this TID - might be response to old query */
            log_msg(LOG_DEBUG, "[tree %u] Dispatcher: No registered worker for TID %02x%02x%02x (hash=%d)",
                    tree_id, tid[0], tid[1], tid[2], hash_idx);
            atomic_fetch_add(&dispatcher->dropped_responses, 1);
            continue;
        }

        /* DEBUG: Successfully found target queue */
        log_msg(LOG_DEBUG, "[tree %u] Dispatcher: Routing to worker queue (TID=%02x%02x%02x)",
                tree_id, tid[0], tid[1], tid[2]);

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
            log_msg(LOG_DEBUG, "[tree %u] Dispatcher: Successfully routed response to worker",
                    tree_id);
        } else {
            /* Worker queue full - drop response */
            log_msg(LOG_WARN, "[tree %u] Dispatcher: Worker queue FULL, dropping response",
                    tree_id);
            atomic_fetch_add(&dispatcher->queue_full_drops, 1);
        }
    }

    log_msg(LOG_DEBUG, "[tree %u] Dispatcher thread exiting (total=%lu, routed=%lu, dropped=%lu, queue_full=%lu)",
            dispatcher->tree ? dispatcher->tree->tree_id : 0,
            (unsigned long)atomic_load(&dispatcher->total_responses),
            (unsigned long)atomic_load(&dispatcher->routed_responses),
            (unsigned long)atomic_load(&dispatcher->dropped_responses),
            (unsigned long)atomic_load(&dispatcher->queue_full_drops));

    return NULL;
}

tree_dispatcher_t *tree_dispatcher_create(struct thread_tree *tree, tree_socket_t *socket) {
    if (!tree || !socket) {
        log_msg(LOG_ERROR, "[dispatcher] Invalid arguments to create");
        return NULL;
    }

    tree_dispatcher_t *dispatcher = calloc(1, sizeof(tree_dispatcher_t));
    if (!dispatcher) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate dispatcher", tree->tree_id);
        return NULL;
    }

    dispatcher->tree = tree;
    dispatcher->socket = socket;

    /* Initialize TID map (simple hash table with chaining) */
    dispatcher->tid_map_capacity = 1024;  /* Support up to ~1000 concurrent queries */
    dispatcher->tid_map = calloc(dispatcher->tid_map_capacity, sizeof(tid_registration_t *));
    if (!dispatcher->tid_map) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate TID map", tree->tree_id);
        free(dispatcher);
        return NULL;
    }

    if (pthread_rwlock_init(&dispatcher->tid_map_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[tree %u] Failed to init TID map lock", tree->tree_id);
        free(dispatcher->tid_map);
        free(dispatcher);
        return NULL;
    }

    atomic_store(&dispatcher->running, false);
    atomic_store(&dispatcher->total_responses, 0);
    atomic_store(&dispatcher->routed_responses, 0);
    atomic_store(&dispatcher->dropped_responses, 0);
    atomic_store(&dispatcher->queue_full_drops, 0);

    log_msg(LOG_DEBUG, "[tree %u] Dispatcher created", tree->tree_id);

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

    /* Free all TID registrations */
    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    for (int i = 0; i < dispatcher->tid_map_capacity; i++) {
        tid_registration_t *reg = dispatcher->tid_map[i];
        while (reg) {
            tid_registration_t *next = reg->next;
            free(reg);
            reg = next;
        }
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);
    pthread_rwlock_destroy(&dispatcher->tid_map_lock);

    free(dispatcher->tid_map);
    free(dispatcher);

    log_msg(LOG_DEBUG, "[dispatcher] Destroyed");
}

int tree_dispatcher_register_tid(tree_dispatcher_t *dispatcher,
                                  const uint8_t *tid, int tid_len,
                                  tree_response_queue_t *response_queue) {
    if (!dispatcher || !tid || tid_len <= 0 || tid_len > MAX_TID_SIZE || !response_queue) {
        return -1;
    }

    tid_registration_t *reg = malloc(sizeof(tid_registration_t));
    if (!reg) {
        return -1;
    }

    memcpy(reg->tid, tid, tid_len);
    reg->tid_len = tid_len;
    reg->response_queue = response_queue;
    reg->registered_at = time(NULL);
    reg->next = NULL;

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    int hash_idx = tid_hash(tid, tid_len, dispatcher->tid_map_capacity);

    /* Check for duplicate TID (should be rare with good TID generation) */
    tid_registration_t *existing = dispatcher->tid_map[hash_idx];
    while (existing) {
        if (existing->tid_len == tid_len && memcmp(existing->tid, tid, tid_len) == 0) {
            /* TID already registered - this is a bug, but update it anyway */
            log_msg(LOG_WARN, "[tree %u] TID already registered, updating",
                    dispatcher->tree ? dispatcher->tree->tree_id : 0);
            existing->response_queue = response_queue;
            existing->registered_at = time(NULL);
            pthread_rwlock_unlock(&dispatcher->tid_map_lock);
            free(reg);
            return 0;
        }
        existing = existing->next;
    }

    /* Insert at head of chain */
    reg->next = dispatcher->tid_map[hash_idx];
    dispatcher->tid_map[hash_idx] = reg;

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);

    return 0;
}

void tree_dispatcher_unregister_tid(tree_dispatcher_t *dispatcher,
                                    const uint8_t *tid, int tid_len) {
    if (!dispatcher || !tid || tid_len <= 0) {
        return;
    }

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    int hash_idx = tid_hash(tid, tid_len, dispatcher->tid_map_capacity);

    tid_registration_t **prev = &dispatcher->tid_map[hash_idx];
    tid_registration_t *curr = dispatcher->tid_map[hash_idx];

    while (curr) {
        if (curr->tid_len == tid_len && memcmp(curr->tid, tid, tid_len) == 0) {
            *prev = curr->next;
            free(curr);
            pthread_rwlock_unlock(&dispatcher->tid_map_lock);
            return;
        }
        prev = &curr->next;
        curr = curr->next;
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

    for (int i = 0; i < dispatcher->tid_map_capacity; i++) {
        tid_registration_t **prev = &dispatcher->tid_map[i];
        tid_registration_t *curr = dispatcher->tid_map[i];

        while (curr) {
            if ((now - curr->registered_at) > timeout_sec) {
                *prev = curr->next;
                free(curr);
                curr = *prev;
                removed++;
            } else {
                prev = &curr->next;
                curr = curr->next;
            }
        }
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);

    return removed;
}
