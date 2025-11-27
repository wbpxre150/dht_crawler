#define _DEFAULT_SOURCE

#include "refresh_dispatcher.h"
#include "tree_socket.h"
#include "tree_response_queue.h"
#include "protocol_utils.h"
#include "tree_protocol.h"
#include "dht_crawler.h"
#include "uthash.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* TID registration entry for uthash */
typedef struct refresh_tid_registration {
    uint8_t tid[REFRESH_MAX_TID_SIZE];
    unsigned int tid_len;
    tree_response_queue_t *queue;
    time_t registered_at;
    UT_hash_handle hh;
} refresh_tid_registration_t;

/* Dispatcher thread function */
static void *refresh_dispatcher_thread_func(void *arg) {
    refresh_dispatcher_t *dispatcher = (refresh_dispatcher_t *)arg;
    tree_socket_t *sock = dispatcher->socket;
    uint8_t recv_buf[REFRESH_MAX_RESPONSE_SIZE];

    log_msg(LOG_DEBUG, "[refresh_dispatcher] Thread started");

    time_t last_cleanup = time(NULL);
    const int CLEANUP_INTERVAL = 30;
    const int TID_TIMEOUT = 10;

    while (atomic_load(&dispatcher->running)) {
        /* Receive UDP packet with 100ms timeout */
        struct sockaddr_storage from;
        int recv_len = tree_socket_recv(sock, recv_buf, sizeof(recv_buf), &from, 100);

        if (recv_len <= 0) {
            /* Timeout or error - check for cleanup */
            time_t now = time(NULL);
            if (now - last_cleanup >= CLEANUP_INTERVAL) {
                pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

                refresh_tid_registration_t *reg, *tmp;
                int removed = 0;
                HASH_ITER(hh, dispatcher->tid_map_head, reg, tmp) {
                    if (now - reg->registered_at > TID_TIMEOUT) {
                        HASH_DEL(dispatcher->tid_map_head, reg);
                        free(reg);
                        removed++;
                    }
                }

                pthread_rwlock_unlock(&dispatcher->tid_map_lock);

                if (removed > 0) {
                    log_msg(LOG_DEBUG, "[refresh_dispatcher] Cleaned up %d stale TID registrations", removed);
                }
                last_cleanup = now;
            }
            continue;
        }

        atomic_fetch_add(&dispatcher->total_responses, 1);

        /* Extract TID from response */
        uint8_t tid[REFRESH_MAX_TID_SIZE];
        int tid_len;
        if (tree_protocol_extract_tid(recv_buf, recv_len, tid, &tid_len) != 0) {
            atomic_fetch_add(&dispatcher->dropped_responses, 1);
            continue;
        }

        /* Look up TID registration */
        pthread_rwlock_rdlock(&dispatcher->tid_map_lock);

        refresh_tid_registration_t *reg = NULL;
        HASH_FIND(hh, dispatcher->tid_map_head, tid, tid_len, reg);

        if (reg && reg->queue) {
            tree_response_queue_t *queue = reg->queue;
            pthread_rwlock_unlock(&dispatcher->tid_map_lock);

            /* Build response structure and push to queue */
            tree_response_t response;
            response.len = recv_len;
            memcpy(response.data, recv_buf, recv_len);
            memcpy(&response.from, &from, sizeof(from));

            if (tree_response_queue_try_push(queue, &response) == 0) {
                atomic_fetch_add(&dispatcher->routed_responses, 1);
            } else {
                atomic_fetch_add(&dispatcher->dropped_responses, 1);
            }
        } else {
            pthread_rwlock_unlock(&dispatcher->tid_map_lock);
            atomic_fetch_add(&dispatcher->dropped_responses, 1);
        }
    }

    log_msg(LOG_DEBUG, "[refresh_dispatcher] Thread exiting (total=%lu, routed=%lu, dropped=%lu)",
            (unsigned long)atomic_load(&dispatcher->total_responses),
            (unsigned long)atomic_load(&dispatcher->routed_responses),
            (unsigned long)atomic_load(&dispatcher->dropped_responses));

    return NULL;
}

refresh_dispatcher_t *refresh_dispatcher_create(tree_socket_t *socket) {
    if (!socket) {
        log_msg(LOG_ERROR, "[refresh_dispatcher] Cannot create dispatcher with NULL socket");
        return NULL;
    }

    refresh_dispatcher_t *dispatcher = calloc(1, sizeof(refresh_dispatcher_t));
    if (!dispatcher) {
        log_msg(LOG_ERROR, "[refresh_dispatcher] Failed to allocate dispatcher");
        return NULL;
    }

    dispatcher->socket = socket;
    dispatcher->tid_map_head = NULL;

    if (pthread_rwlock_init(&dispatcher->tid_map_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[refresh_dispatcher] Failed to init TID map lock");
        free(dispatcher);
        return NULL;
    }

    atomic_store(&dispatcher->running, false);
    atomic_store(&dispatcher->total_responses, 0);
    atomic_store(&dispatcher->routed_responses, 0);
    atomic_store(&dispatcher->dropped_responses, 0);

    log_msg(LOG_DEBUG, "[refresh_dispatcher] Created");

    return dispatcher;
}

int refresh_dispatcher_start(refresh_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return -1;
    }

    atomic_store(&dispatcher->running, true);

    int rc = pthread_create(&dispatcher->dispatcher_thread, NULL,
                           refresh_dispatcher_thread_func, dispatcher);
    if (rc != 0) {
        log_msg(LOG_ERROR, "[refresh_dispatcher] Failed to create thread: %d", rc);
        atomic_store(&dispatcher->running, false);
        return -1;
    }

    log_msg(LOG_DEBUG, "[refresh_dispatcher] Thread started");
    return 0;
}

void refresh_dispatcher_stop(refresh_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }

    atomic_store(&dispatcher->running, false);

    if (dispatcher->dispatcher_thread) {
        pthread_join(dispatcher->dispatcher_thread, NULL);
        dispatcher->dispatcher_thread = 0;
    }

    log_msg(LOG_DEBUG, "[refresh_dispatcher] Thread stopped");
}

void refresh_dispatcher_destroy(refresh_dispatcher_t *dispatcher) {
    if (!dispatcher) {
        return;
    }

    refresh_dispatcher_stop(dispatcher);

    /* Free all TID registrations */
    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    refresh_tid_registration_t *reg, *tmp;
    HASH_ITER(hh, dispatcher->tid_map_head, reg, tmp) {
        HASH_DEL(dispatcher->tid_map_head, reg);
        free(reg);
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);

    pthread_rwlock_destroy(&dispatcher->tid_map_lock);
    free(dispatcher);

    log_msg(LOG_DEBUG, "[refresh_dispatcher] Destroyed");
}

int refresh_dispatcher_register_tid(refresh_dispatcher_t *dispatcher,
                                    const uint8_t *tid, int tid_len,
                                    tree_response_queue_t *queue) {
    if (!dispatcher || !tid || tid_len <= 0 || tid_len > REFRESH_MAX_TID_SIZE || !queue) {
        return -1;
    }

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    /* Check if already registered */
    refresh_tid_registration_t *existing = NULL;
    HASH_FIND(hh, dispatcher->tid_map_head, tid, tid_len, existing);

    if (existing) {
        /* Update existing registration */
        existing->queue = queue;
        existing->registered_at = time(NULL);
        pthread_rwlock_unlock(&dispatcher->tid_map_lock);
        return 0;
    }

    /* Create new registration */
    refresh_tid_registration_t *reg = calloc(1, sizeof(refresh_tid_registration_t));
    if (!reg) {
        pthread_rwlock_unlock(&dispatcher->tid_map_lock);
        return -1;
    }

    memcpy(reg->tid, tid, tid_len);
    reg->tid_len = tid_len;
    reg->queue = queue;
    reg->registered_at = time(NULL);

    HASH_ADD_KEYPTR(hh, dispatcher->tid_map_head, reg->tid, reg->tid_len, reg);

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);

    return 0;
}

void refresh_dispatcher_unregister_tid(refresh_dispatcher_t *dispatcher,
                                       const uint8_t *tid, int tid_len) {
    if (!dispatcher || !tid || tid_len <= 0 || tid_len > REFRESH_MAX_TID_SIZE) {
        return;
    }

    pthread_rwlock_wrlock(&dispatcher->tid_map_lock);

    refresh_tid_registration_t *reg = NULL;
    HASH_FIND(hh, dispatcher->tid_map_head, tid, tid_len, reg);

    if (reg) {
        HASH_DEL(dispatcher->tid_map_head, reg);
        free(reg);
    }

    pthread_rwlock_unlock(&dispatcher->tid_map_lock);
}
