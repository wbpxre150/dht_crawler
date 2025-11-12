#define _DEFAULT_SOURCE  /* For usleep() */
#include "metadata_fetcher.h"
#include "dht_crawler.h"
#include "dht_manager.h"
#include "config.h"
#include "bencode_util.h"
#include "worker_pool.h"
#include "batch_writer.h"
#include "connection_request_queue.h"
#include <openssl/sha.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include "../lib/bencode-c/bencode.h"

/* Task structure for worker pool */
typedef struct {
    uint8_t info_hash[20];
    metadata_fetcher_t *fetcher;
} fetch_task_t;

/* Forward declarations */
static void metadata_worker_fn(void *task_data, void *closure);
static void on_connection_request(uv_async_t *handle);
static void on_tcp_connect(uv_connect_t *req, int status);
static void on_tcp_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);
static void on_tcp_write(uv_write_t *req, int status);
static void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf);
static void on_timeout(uv_timer_t *handle);
static void on_handle_closed(uv_handle_t *handle);
static void close_peer_connection_locked(peer_connection_t *peer, const char *reason);
static void close_peer_connection(peer_connection_t *peer, const char *reason);
static peer_connection_t* create_peer_connection(metadata_fetcher_t *fetcher,
                                                  const uint8_t *info_hash,
                                                  struct sockaddr *addr);

/* Retry queue functions */
static retry_queue_t* retry_queue_init(size_t max_retries);
static void retry_queue_cleanup(retry_queue_t *queue);
static int retry_queue_add(retry_queue_t *queue, const uint8_t *info_hash, int retry_count);
static void retry_thread_fn(void *arg);

/* Infohash attempt tracking functions */
static uint32_t hash_infohash(const uint8_t *info_hash);
static infohash_attempt_t* lookup_attempt(metadata_fetcher_t *fetcher, const uint8_t *info_hash);
static infohash_attempt_t* create_or_get_attempt(metadata_fetcher_t *fetcher, const uint8_t *info_hash, int peer_count);
static infohash_attempt_t* create_or_get_attempt_with_peers(metadata_fetcher_t *fetcher, const uint8_t *info_hash,
                                                             struct sockaddr_storage *peers, socklen_t *peer_lens, int peer_count);
static void start_next_peer_connections(metadata_fetcher_t *fetcher, infohash_attempt_t *attempt);
static void remove_attempt(metadata_fetcher_t *fetcher, const uint8_t *info_hash);
static void update_attempt_statistics(infohash_attempt_t *attempt, peer_connection_t *peer, const char *close_reason);
static failure_classification_t classify_failure(infohash_attempt_t *attempt);
static void handle_infohash_failure(metadata_fetcher_t *fetcher, infohash_attempt_t *attempt);

static int send_handshake(peer_connection_t *peer);
static int send_extended_handshake(peer_connection_t *peer);
static int send_metadata_request(peer_connection_t *peer, int piece);
static int process_handshake(peer_connection_t *peer, const uint8_t *data, size_t len);
static int process_message(peer_connection_t *peer, const uint8_t *data, size_t len);
static int parse_metadata_piece(peer_connection_t *peer, const uint8_t *data, size_t len);
static int verify_and_parse_metadata(peer_connection_t *peer);

/* Helper: format info_hash as hex */
static void format_infohash_hex(const uint8_t *hash, char *out) {
    for (int i = 0; i < 20; i++) {
        snprintf(out + i * 2, 3, "%02x", hash[i]);
    }
}

/* Async callback - handles connection requests from worker threads */
static void on_connection_request(uv_async_t *handle) {
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)handle->data;

    if (!fetcher) {
        return;
    }

    /* Process all pending connection requests */
    connection_request_t *req;
    while ((req = connection_request_queue_try_pop(fetcher->conn_request_queue)) != NULL) {
        /* Check global connection limit */
        uv_mutex_lock(&fetcher->mutex);
        int current_active = fetcher->active_count;
        uv_mutex_unlock(&fetcher->mutex);

        if (current_active >= fetcher->max_global_connections) {
            free(req);
            continue;
        }

        /* Create or get infohash attempt entry with peer queue */
        infohash_attempt_t *attempt = create_or_get_attempt_with_peers(
            fetcher, req->info_hash, req->peers, req->peer_lens, req->peer_count
        );
        if (!attempt) {
            log_msg(LOG_ERROR, "Failed to create attempt entry");
            free(req);
            continue;
        }

        /* Start initial batch of connections using the new peer queue mechanism */
        start_next_peer_connections(fetcher, attempt);

        free(req);
    }
}

/* Initialize metadata fetcher */
int metadata_fetcher_init(metadata_fetcher_t *fetcher, app_context_t *app_ctx,
                          infohash_queue_t *queue, database_t *database,
                          peer_store_t *peer_store, crawler_config_t *config) {
    if (!fetcher || !app_ctx || !queue || !database || !peer_store || !config) {
        return -1;
    }

    memset(fetcher, 0, sizeof(metadata_fetcher_t));
    fetcher->app_ctx = app_ctx;
    fetcher->queue = queue;
    fetcher->database = database;
    fetcher->peer_store = peer_store;
    fetcher->max_concurrent = 5;  /* Kept for backward compatibility */
    fetcher->running = 0;

    /* Load config values from provided config */
    fetcher->max_concurrent_per_infohash = config->concurrent_peers_per_torrent;
    fetcher->max_global_connections = config->max_concurrent_connections;
    fetcher->connection_timeout_ms = config->connection_timeout_sec * 1000;  /* Convert seconds to ms */
    fetcher->retry_enabled = config->retry_enabled;

    /* Get worker count from config */
    fetcher->num_workers = config->metadata_workers;

    if (uv_mutex_init(&fetcher->mutex) != 0) {
        return -1;
    }

    /* Initialize attempt table mutex */
    if (uv_mutex_init(&fetcher->attempt_table_mutex) != 0) {
        uv_mutex_destroy(&fetcher->mutex);
        return -1;
    }

    /* Initialize worker pool with 10x queue capacity */
    size_t queue_capacity = fetcher->num_workers * 10;
    fetcher->worker_pool = worker_pool_init(fetcher->num_workers, queue_capacity,
                                           metadata_worker_fn, fetcher);
    if (!fetcher->worker_pool) {
        log_msg(LOG_ERROR, "Failed to initialize worker pool");
        uv_mutex_destroy(&fetcher->mutex);
        return -1;
    }

    /* Initialize batch writer for high-throughput database writes
     * Use config values for batch size and flush interval */
    fetcher->batch_writer = batch_writer_init(database, config->batch_size,
                                               config->flush_interval, app_ctx->loop);
    if (!fetcher->batch_writer) {
        log_msg(LOG_ERROR, "Failed to initialize batch writer");
        worker_pool_cleanup(fetcher->worker_pool);
        uv_mutex_destroy(&fetcher->mutex);
        return -1;
    }

    /* Initialize connection request queue (capacity: 1000) */
    fetcher->conn_request_queue = connection_request_queue_init(1000);
    if (!fetcher->conn_request_queue) {
        log_msg(LOG_ERROR, "Failed to initialize connection request queue");
        batch_writer_cleanup(fetcher->batch_writer);
        worker_pool_cleanup(fetcher->worker_pool);
        uv_mutex_destroy(&fetcher->mutex);
        return -1;
    }

    /* Initialize async handle (MUST be on main thread) */
    if (uv_async_init(app_ctx->loop, &fetcher->async_handle, on_connection_request) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize async handle");
        connection_request_queue_cleanup(fetcher->conn_request_queue);
        batch_writer_cleanup(fetcher->batch_writer);
        worker_pool_cleanup(fetcher->worker_pool);
        uv_mutex_destroy(&fetcher->mutex);
        return -1;
    }
    fetcher->async_handle.data = fetcher;

    /* Initialize retry queue (max 3 retries) - only if retry is enabled */
    if (fetcher->retry_enabled) {
        fetcher->retry_queue = retry_queue_init(3);
        if (!fetcher->retry_queue) {
            log_msg(LOG_ERROR, "Failed to initialize retry queue");
            uv_close((uv_handle_t *)&fetcher->async_handle, NULL);
            connection_request_queue_cleanup(fetcher->conn_request_queue);
            batch_writer_cleanup(fetcher->batch_writer);
            worker_pool_cleanup(fetcher->worker_pool);
            uv_mutex_destroy(&fetcher->mutex);
            return -1;
        }
        log_msg(LOG_INFO, "Retry queue initialized (max retries: 3)");
    } else {
        fetcher->retry_queue = NULL;
        log_msg(LOG_INFO, "Retry queue DISABLED - all failures will be discarded immediately");
    }

    log_msg(LOG_INFO, "Metadata fetcher initialized:");
    log_msg(LOG_INFO, "  Workers: %d", fetcher->num_workers);
    log_msg(LOG_INFO, "  Queue capacity: %zu", queue_capacity);
    log_msg(LOG_INFO, "  Max concurrent per infohash: %d", fetcher->max_concurrent_per_infohash);
    log_msg(LOG_INFO, "  Max global connections: %d", fetcher->max_global_connections);
    log_msg(LOG_INFO, "  Connection timeout: %d ms", fetcher->connection_timeout_ms);
    log_msg(LOG_INFO, "  Retry enabled: %s", fetcher->retry_enabled ? "yes" : "no");
    return 0;
}

/* Worker function - processes individual fetch tasks (THREAD-SAFE VERSION) */
static void metadata_worker_fn(void *task_data, void *closure) {
    fetch_task_t *task = (fetch_task_t *)task_data;
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)closure;

    if (!task || !fetcher) {
        if (task) free(task);
        return;
    }

    /* NOTE: Bloom filter + database deduplication already happened in dht_manager_query_peers()
     * before the info_hash entered the get_peers queue. No need to check database again here.
     * See src/dht_manager.c:979-1003 for the single deduplication point. */

    /* Get peers from peer store (thread-safe) */
    struct sockaddr_storage peers[MAX_PEERS_PER_REQUEST];
    socklen_t peer_lens[MAX_PEERS_PER_REQUEST];
    int peer_count = 0;

    if (peer_store_get_peers(fetcher->peer_store, task->info_hash,
                            peers, peer_lens, &peer_count, MAX_PEERS_PER_REQUEST) != 0 || peer_count == 0) {
        /* No peers found - discard immediately (don't retry) */
        uv_mutex_lock(&fetcher->mutex);
        fetcher->no_peers_found++;
        uv_mutex_unlock(&fetcher->mutex);

        /* Create attempt entry with 0 peers to track this as FAILURE_NO_PEERS */
        infohash_attempt_t *attempt = create_or_get_attempt(fetcher, task->info_hash, 0);
        if (attempt) {
            /* Mark as having no peers and handle immediately */
            handle_infohash_failure(fetcher, attempt);
            remove_attempt(fetcher, task->info_hash);
        }

        free(task);
        return;
    }

    /* Create connection request */
    connection_request_t *req = (connection_request_t *)malloc(sizeof(connection_request_t));
    if (!req) {
        log_msg(LOG_ERROR, "Failed to allocate connection request");
        free(task);
        return;
    }

    memcpy(req->info_hash, task->info_hash, 20);
    memcpy(req->peers, peers, sizeof(struct sockaddr_storage) * peer_count);
    memcpy(req->peer_lens, peer_lens, sizeof(socklen_t) * peer_count);
    req->peer_count = peer_count;

    /* Enqueue to connection request queue */
    if (connection_request_queue_push(fetcher->conn_request_queue, req) != 0) {
        free(req);
        free(task);
        return;
    }

    /* Signal main thread via uv_async_send (THREAD-SAFE) */
    uv_async_send(&fetcher->async_handle);

    /* Update statistics */
    uv_mutex_lock(&fetcher->mutex);
    fetcher->total_attempts++;
    uv_mutex_unlock(&fetcher->mutex);

    free(task);
}

/* Queue feeder thread - pulls from infohash queue and submits to worker pool */
static void queue_feeder_thread(void *arg) {
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)arg;

    log_msg(LOG_DEBUG, "Queue feeder thread started");
    
    while (fetcher->running) {
        uint8_t info_hash[20];
        
        /* Try to get an info_hash from queue with timeout */
        if (infohash_queue_try_pop(fetcher->queue, info_hash, 1000) == 0) {
            /* Create task for worker pool */
            fetch_task_t *task = (fetch_task_t *)malloc(sizeof(fetch_task_t));
            if (!task) {
                log_msg(LOG_ERROR, "Failed to allocate fetch task");
                continue;
            }
            
            memcpy(task->info_hash, info_hash, 20);
            task->fetcher = fetcher;
            
            /* Try to submit to worker pool (non-blocking) */
            if (worker_pool_try_submit(fetcher->worker_pool, task) != 0) {
                /* Worker pool queue full - put back in infohash queue for later */
                if (infohash_queue_push(fetcher->queue, info_hash) != 0) {
                    log_msg(LOG_WARN, "Both worker pool and infohash queue full, dropping task");
                }
                free(task);
            }
        }
    }
    
    log_msg(LOG_DEBUG, "Queue feeder thread stopped");
}

/* Start metadata fetcher */
int metadata_fetcher_start(metadata_fetcher_t *fetcher) {
    if (!fetcher || fetcher->running) {
        return -1;
    }

    fetcher->running = 1;

    /* Start queue feeder thread */
    if (uv_thread_create(&fetcher->feeder_thread, queue_feeder_thread, fetcher) != 0) {
        log_msg(LOG_ERROR, "Failed to create queue feeder thread");
        fetcher->running = 0;
        return -1;
    }

    /* Start retry thread if retry is enabled */
    if (fetcher->retry_enabled && fetcher->retry_queue) {
        if (uv_thread_create(&fetcher->retry_thread, retry_thread_fn, fetcher) != 0) {
            log_msg(LOG_ERROR, "Failed to create retry thread");
            fetcher->running = 0;
            uv_thread_join(&fetcher->feeder_thread);
            return -1;
        }
        log_msg(LOG_INFO, "Retry thread started");
    } else {
        log_msg(LOG_INFO, "Retry thread NOT started (retry disabled)");
    }

    log_msg(LOG_DEBUG, "Metadata fetcher started (%d workers)", fetcher->num_workers);
    return 0;
}

/* Stop metadata fetcher */
void metadata_fetcher_stop(metadata_fetcher_t *fetcher) {
    if (!fetcher || !fetcher->running) {
        return;
    }

    log_msg(LOG_DEBUG, "Stopping metadata fetcher...");
    fetcher->running = 0;

    /* Shutdown worker pool (waits for tasks to complete) */
    if (fetcher->worker_pool) {
        worker_pool_shutdown(fetcher->worker_pool);
    }

    /* Wait for feeder thread */
    uv_thread_join(&fetcher->feeder_thread);

    /* Wait for retry thread if enabled */
    if (fetcher->retry_enabled && fetcher->retry_queue) {
        uv_thread_join(&fetcher->retry_thread);
        log_msg(LOG_DEBUG, "Retry thread joined");
    }

    /* Shutdown batch writer and flush any pending writes */
    if (fetcher->batch_writer) {
        batch_writer_shutdown(fetcher->batch_writer);
    }

    log_msg(LOG_DEBUG, "Metadata fetcher stopped");
}

/* Cleanup metadata fetcher */
/* Set DHT manager reference for peer refresh on retry */
void metadata_fetcher_set_dht_manager(metadata_fetcher_t *fetcher, struct dht_manager *dht_manager) {
    if (fetcher) {
        fetcher->dht_manager = dht_manager;
    }
}

/* Set bloom filter for persistence after batch writes */
void metadata_fetcher_set_bloom_filter(metadata_fetcher_t *fetcher, bloom_filter_t *bloom, const char *bloom_path) {
    if (fetcher && fetcher->batch_writer) {
        batch_writer_set_bloom(fetcher->batch_writer, bloom, bloom_path);
    }
}

void metadata_fetcher_cleanup(metadata_fetcher_t *fetcher) {
    if (!fetcher) {
        return;
    }

    /* Close all active connections */
    peer_connection_t *peer = fetcher->active_connections;
    while (peer) {
        peer_connection_t *next = peer->next;
        close_peer_connection(peer, "cleanup");
        peer = next;
    }

    /* Close async handle */
    uv_close((uv_handle_t *)&fetcher->async_handle, NULL);

    /* Cleanup connection request queue */
    if (fetcher->conn_request_queue) {
        connection_request_queue_cleanup(fetcher->conn_request_queue);
        fetcher->conn_request_queue = NULL;
    }

    /* Cleanup batch writer (will flush any remaining items) */
    if (fetcher->batch_writer) {
        size_t batch_size, batch_capacity;
        uint64_t total_written, total_flushes;
        batch_writer_stats(fetcher->batch_writer, &batch_size, &batch_capacity,
                          &total_written, &total_flushes);
        log_msg(LOG_INFO, "Batch writer stats: %lu written, %lu flushes, %zu pending",
                total_written, total_flushes, batch_size);
        batch_writer_cleanup(fetcher->batch_writer);
        fetcher->batch_writer = NULL;
    }

    /* Cleanup worker pool */
    if (fetcher->worker_pool) {
        worker_pool_cleanup(fetcher->worker_pool);
        fetcher->worker_pool = NULL;
    }

    /* Cleanup retry queue */
    if (fetcher->retry_queue) {
        log_msg(LOG_INFO, "Retry queue status: %zu pending retries", fetcher->retry_queue->count);
        retry_queue_cleanup(fetcher->retry_queue);
        fetcher->retry_queue = NULL;
    }

    /* Cleanup attempt table */
    for (int i = 0; i < 1024; i++) {
        infohash_attempt_t *attempt = fetcher->attempt_table[i];
        while (attempt) {
            infohash_attempt_t *next = attempt->next;
            free(attempt);
            attempt = next;
        }
        fetcher->attempt_table[i] = NULL;
    }

    uv_mutex_destroy(&fetcher->attempt_table_mutex);
    uv_mutex_destroy(&fetcher->mutex);

    /* Print detailed statistics breakdown */
    log_msg(LOG_INFO, "=== Metadata Fetcher Statistics ===");
    log_msg(LOG_INFO, "  Total attempts: %llu", (unsigned long long)fetcher->total_attempts);
    log_msg(LOG_INFO, "  No peers found: %llu (%.1f%%)",
            (unsigned long long)fetcher->no_peers_found,
            fetcher->total_attempts > 0 ? (fetcher->no_peers_found * 100.0 / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "  Connections initiated: %llu (avg %.1f per attempt)",
            (unsigned long long)fetcher->connection_initiated,
            fetcher->total_attempts > 0 ? ((double)fetcher->connection_initiated / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "  Connection failures: %llu (%.1f per attempt, %.1f%% of connections)",
            (unsigned long long)fetcher->connection_failed,
            fetcher->total_attempts > 0 ? ((double)fetcher->connection_failed / fetcher->total_attempts) : 0.0,
            fetcher->connection_initiated > 0 ? (100.0 * fetcher->connection_failed / fetcher->connection_initiated) : 0.0);
    log_msg(LOG_INFO, "  Connection timeouts: %llu (%.1f per attempt, %.1f%% of connections)",
            (unsigned long long)fetcher->connection_timeout,
            fetcher->total_attempts > 0 ? ((double)fetcher->connection_timeout / fetcher->total_attempts) : 0.0,
            fetcher->connection_initiated > 0 ? (100.0 * fetcher->connection_timeout / fetcher->connection_initiated) : 0.0);
    log_msg(LOG_INFO, "  Handshake failures: %llu (%.1f%%)",
            (unsigned long long)fetcher->handshake_failed,
            fetcher->total_attempts > 0 ? (fetcher->handshake_failed * 100.0 / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "  No metadata support: %llu (%.1f%%)",
            (unsigned long long)fetcher->no_metadata_support,
            fetcher->total_attempts > 0 ? (fetcher->no_metadata_support * 100.0 / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "  Metadata rejected: %llu (%.1f%%)",
            (unsigned long long)fetcher->metadata_rejected,
            fetcher->total_attempts > 0 ? (fetcher->metadata_rejected * 100.0 / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "  Hash mismatches: %llu (%.1f%%)",
            (unsigned long long)fetcher->hash_mismatch,
            fetcher->total_attempts > 0 ? (fetcher->hash_mismatch * 100.0 / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "  Successfully fetched: %llu (%.1f%%)",
            (unsigned long long)fetcher->total_fetched,
            fetcher->total_attempts > 0 ? (fetcher->total_fetched * 100.0 / fetcher->total_attempts) : 0.0);
    log_msg(LOG_INFO, "");
    log_msg(LOG_INFO, "  Failure Breakdown:");
    if (fetcher->retry_enabled) {
        log_msg(LOG_INFO, "    Retriable failures: %llu (added to retry queue)",
                (unsigned long long)fetcher->retriable_failures);
        log_msg(LOG_INFO, "    Permanent failures: %llu (not retried)",
                (unsigned long long)fetcher->permanent_failures);
        log_msg(LOG_INFO, "    No peers (discarded): %llu",
                (unsigned long long)fetcher->no_peer_discards);
    } else {
        log_msg(LOG_INFO, "    All failures discarded: %llu (retry disabled)",
                (unsigned long long)fetcher->discarded_failures);
    }
    log_msg(LOG_INFO, "");
    if (fetcher->retry_enabled) {
        log_msg(LOG_INFO, "  Retry Statistics:");
        log_msg(LOG_INFO, "    Added to retry queue: %llu", (unsigned long long)fetcher->retry_queue_added);
        log_msg(LOG_INFO, "    Retry attempts: %llu", (unsigned long long)fetcher->retry_attempts);
        log_msg(LOG_INFO, "    Retry submitted for fetch: %llu (%.1f%%)",
                (unsigned long long)fetcher->retry_submitted,
                fetcher->retry_attempts > 0 ? (fetcher->retry_submitted * 100.0 / fetcher->retry_attempts) : 0.0);
        log_msg(LOG_INFO, "    Abandoned after max retries: %llu", (unsigned long long)fetcher->retry_abandoned);
        log_msg(LOG_INFO, "");
    } else {
        log_msg(LOG_INFO, "  Retry Statistics: N/A (retry disabled)");
        log_msg(LOG_INFO, "");
    }
    log_msg(LOG_INFO, "===================================");
}

/* Create a new peer connection */
static peer_connection_t* create_peer_connection(metadata_fetcher_t *fetcher,
                                                  const uint8_t *info_hash,
                                                  struct sockaddr *addr) {
    peer_connection_t *peer = (peer_connection_t *)calloc(1, sizeof(peer_connection_t));
    if (!peer) {
        log_msg(LOG_ERROR, "Failed to allocate peer connection");
        return NULL;
    }

    /* Initialize structure */
    memcpy(peer->info_hash, info_hash, 20);
    peer->state = PEER_STATE_CONNECTING;
    peer->fetcher = fetcher;
    peer->closed = 0;
    peer->handles_to_close = 2;  /* TCP + timer */
    peer->tcp_initialized = 0;
    peer->timer_initialized = 0;

    /* Generate random peer ID */
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        read(fd, peer->peer_id, 20);
        close(fd);
    } else {
        /* Fallback to pseudo-random */
        for (int i = 0; i < 20; i++) {
            peer->peer_id[i] = rand() & 0xFF;
        }
    }

    /* Initialize timeout timer FIRST (before TCP handle to avoid FD leak on failure) */
    if (uv_timer_init(fetcher->app_ctx->loop, &peer->timeout_timer) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize timer");
        /* No handles opened yet, safe to just free */
        free(peer);
        return NULL;
    }
    peer->timeout_timer.data = peer;
    peer->timer_initialized = 1;

    /* Start timeout timer using configured timeout */
    uv_timer_start(&peer->timeout_timer, on_timeout, fetcher->connection_timeout_ms, 0);

    /* Initialize TCP handle (deferred until after timer to minimize FD lifetime) */
    if (uv_tcp_init(fetcher->app_ctx->loop, &peer->tcp) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize TCP handle");
        /* Only timer needs closing */
        peer->handles_to_close = 1;
        uv_close((uv_handle_t *)&peer->timeout_timer, on_handle_closed);
        return NULL;
    }
    peer->tcp.data = peer;
    peer->tcp_initialized = 1;

    /* Apply TCP socket optimizations (bitmagnet approach) */
    uv_os_fd_t socket_fd;
    if (uv_fileno((uv_handle_t *)&peer->tcp, &socket_fd) == 0) {
        /* 1. Set SO_LINGER to discard unsent data on close */
        struct linger ling = {1, 0};
        if (setsockopt(socket_fd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling)) != 0) {
            log_msg(LOG_DEBUG, "Failed to set SO_LINGER: %s", strerror(errno));
        }

        /* 2. Set TCP_NODELAY to disable Nagle's algorithm */
        int flag = 1;
        if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) != 0) {
            log_msg(LOG_DEBUG, "Failed to set TCP_NODELAY: %s", strerror(errno));
        }

        /* 3. Set TCP_QUICKACK for faster ACKs (Linux only) */
        #ifdef TCP_QUICKACK
        flag = 1;
        setsockopt(socket_fd, IPPROTO_TCP, TCP_QUICKACK, &flag, sizeof(flag));
        #endif
    }

    /* Set up connect request */
    peer->connect_req.data = peer;

    /* Initiate TCP connection */
    int rc = uv_tcp_connect(&peer->connect_req, &peer->tcp, addr, on_tcp_connect);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initiate connection: %s", uv_strerror(rc));
        /* Update statistics: connection failed */
        uv_mutex_lock(&fetcher->mutex);
        fetcher->connection_failed++;
        uv_mutex_unlock(&fetcher->mutex);
        close_peer_connection(peer, "connect initiation failed");
        return NULL;
    }

    /* Update statistics: connection initiated */
    uv_mutex_lock(&fetcher->mutex);
    fetcher->connection_initiated++;
    uv_mutex_unlock(&fetcher->mutex);

    /* Add to active connections list */
    uv_mutex_lock(&fetcher->mutex);
    peer->next = fetcher->active_connections;
    fetcher->active_connections = peer;
    fetcher->active_count++;
    uv_mutex_unlock(&fetcher->mutex);

    return peer;
}

/* Fetch metadata from DHT for a specific info_hash */
int fetch_metadata_from_dht(metadata_fetcher_t *fetcher, const uint8_t *info_hash) {
    if (!fetcher || !info_hash || !fetcher->peer_store) {
        return -1;
    }

    /* Get peers from peer store */
    struct sockaddr_storage peers[MAX_PEERS_PER_INFOHASH];
    socklen_t peer_lens[MAX_PEERS_PER_INFOHASH];
    int peer_count = 0;

    if (peer_store_get_peers(fetcher->peer_store, info_hash,
                            peers, peer_lens, &peer_count, MAX_PEERS_PER_INFOHASH) != 0 || peer_count == 0) {
        /* No peers found */
        uv_mutex_lock(&fetcher->mutex);
        fetcher->no_peers_found++;
        uv_mutex_unlock(&fetcher->mutex);

        return -1;
    }

    char hex[41];
    format_infohash_hex(info_hash, hex);
    log_msg(LOG_DEBUG, "Found %d peers for %s, attempting metadata fetch", peer_count, hex);

    /* Try to connect to multiple peers concurrently (up to 3) */
    int max_concurrent = (peer_count < 3) ? peer_count : 3;
    int connections_made = 0;

    for (int i = 0; i < peer_count && connections_made < max_concurrent; i++) {
        /* Create peer connection */
        peer_connection_t *peer = create_peer_connection(fetcher, info_hash,
                                                         (struct sockaddr *)&peers[i]);
        if (peer) {
            connections_made++;
        }
    }

    if (connections_made == 0) {
        log_msg(LOG_WARN, "Failed to initiate any connections for %s", hex);
        return -1;
    }

    /* Connections are now active and will proceed asynchronously
     * Success/failure will be handled in callbacks
     * For now, return success to indicate we attempted the fetch */
    return 0;
}

/* Send BitTorrent handshake */
static int send_handshake(peer_connection_t *peer) {
    uint8_t handshake[BT_HANDSHAKE_SIZE];
    size_t offset = 0;

    /* Protocol string length */
    handshake[offset++] = BT_PROTOCOL_LEN;

    /* Protocol string */
    memcpy(handshake + offset, BT_PROTOCOL_STRING, BT_PROTOCOL_LEN);
    offset += BT_PROTOCOL_LEN;

    /* Reserved bytes - set DHT (bit 0) and extension (bit 20) bits */
    memset(handshake + offset, 0, 8);
    handshake[offset + 5] = 0x10;  /* ExtensionBitLtep (bit 20) */
    handshake[offset + 7] = 0x01;  /* ExtensionBitDht (bit 0) */
    offset += 8;

    /* Info hash */
    memcpy(handshake + offset, peer->info_hash, 20);
    offset += 20;

    /* Peer ID - generate random */
    memcpy(handshake + offset, peer->peer_id, 20);
    offset += 20;

    /* Send handshake */
    uv_buf_t buf = uv_buf_init((char *)handshake, BT_HANDSHAKE_SIZE);
    uv_write_t *req = (uv_write_t *)malloc(sizeof(uv_write_t));
    if (!req) {
        return -1;
    }

    int rc = uv_write(req, (uv_stream_t *)&peer->tcp, &buf, 1, on_tcp_write);
    if (rc != 0) {
        free(req);
        return -1;
    }

    peer->state = PEER_STATE_HANDSHAKING;
    return 0;
}

/* Send extended handshake (BEP 10) */
static int send_extended_handshake(peer_connection_t *peer) {
    /* Build extended handshake message */
    char msg[256];
    size_t offset = 0;

    /* Message length (will fill in later) */
    offset += 4;

    /* Extended message ID */
    msg[offset++] = BT_MSG_EXTENDED;

    /* Extended handshake ID */
    msg[offset++] = BT_EXT_HANDSHAKE;

    /* Bencode dictionary */
    bencode_encode_dict_start(msg, &offset, sizeof(msg));

    /* m dictionary with ut_metadata */
    bencode_encode_string(msg, &offset, sizeof(msg), "m", 1);
    bencode_encode_dict_start(msg, &offset, sizeof(msg));
    bencode_encode_string(msg, &offset, sizeof(msg), "ut_metadata", 11);
    bencode_encode_int(msg, &offset, sizeof(msg), OUR_UT_METADATA_ID);
    bencode_encode_dict_end(msg, &offset, sizeof(msg));

    bencode_encode_dict_end(msg, &offset, sizeof(msg));

    /* Fill in message length */
    uint32_t msg_len = htonl(offset - 4);
    memcpy(msg, &msg_len, 4);

    /* Send message */
    uv_buf_t buf = uv_buf_init(msg, offset);
    uv_write_t *req = (uv_write_t *)malloc(sizeof(uv_write_t));
    if (!req) {
        return -1;
    }

    int rc = uv_write(req, (uv_stream_t *)&peer->tcp, &buf, 1, on_tcp_write);
    if (rc != 0) {
        free(req);
        return -1;
    }

    peer->state = PEER_STATE_EXTENDED_HANDSHAKE;
    return 0;
}

/* Send metadata request (BEP 9) */
static int send_metadata_request(peer_connection_t *peer, int piece) {
    char msg[128];
    size_t offset = 0;

    /* Message length */
    offset += 4;

    /* Extended message */
    msg[offset++] = BT_MSG_EXTENDED;

    /* ut_metadata ID */
    msg[offset++] = peer->ut_metadata_id;

    /* Bencode dictionary */
    bencode_encode_dict_start(msg, &offset, sizeof(msg));
    bencode_encode_string(msg, &offset, sizeof(msg), "msg_type", 8);
    bencode_encode_int(msg, &offset, sizeof(msg), UT_METADATA_REQUEST);
    bencode_encode_string(msg, &offset, sizeof(msg), "piece", 5);
    bencode_encode_int(msg, &offset, sizeof(msg), piece);
    bencode_encode_dict_end(msg, &offset, sizeof(msg));

    /* Fill in length */
    uint32_t msg_len = htonl(offset - 4);
    memcpy(msg, &msg_len, 4);

    /* Send */
    uv_buf_t buf = uv_buf_init(msg, offset);
    uv_write_t *req = (uv_write_t *)malloc(sizeof(uv_write_t));
    if (!req) {
        return -1;
    }

    int rc = uv_write(req, (uv_stream_t *)&peer->tcp, &buf, 1, on_tcp_write);
    if (rc != 0) {
        free(req);
        return -1;
    }

    return 0;
}

/* TCP write callback */
static void on_tcp_write(uv_write_t *req, int status) {
    (void)status;
    /* Note: This callback doesn't access the peer structure, so it's safe
     * even if called after the peer is freed. We just free the write request. */
    free(req);
}

/* Allocate read buffer */
static void alloc_buffer(uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
    peer_connection_t *peer = (peer_connection_t *)handle->data;
    (void)suggested_size;

    /* Check if peer is NULL (should not happen) or closing */
    if (!peer) {
        buf->base = NULL;
        buf->len = 0;
        if (handle && !uv_is_closing(handle)) {
            uv_read_stop((uv_stream_t*)handle);
        }
        return;
    }

    /* Validate peer pointer by checking if tcp_initialized flag is set */
    if (!peer->tcp_initialized) {
        log_msg(LOG_ERROR, "alloc_buffer: peer->tcp_initialized is 0! Possible use-after-free.");
        log_msg(LOG_ERROR, "  peer=%p handle=%p", (void*)peer, (void*)handle);
        buf->base = NULL;
        buf->len = 0;
        if (handle && !uv_is_closing(handle)) {
            uv_read_stop((uv_stream_t*)handle);
        }
        return;
    }

    if (peer->closed) {
        /* Peer is closing/closed - don't allocate buffer */
        buf->base = NULL;
        buf->len = 0;
        /* Stop reading to prevent further callbacks */
        if (handle && !uv_is_closing(handle)) {
            uv_read_stop((uv_stream_t*)handle);
        }
        return;
    }

    /* Check if buffer is full */
    if (peer->recv_offset >= sizeof(peer->recv_buffer)) {
        log_msg(LOG_WARN, "Receive buffer full, closing connection");
        close_peer_connection(peer, "receive buffer full");
        buf->base = NULL;
        buf->len = 0;
        return;
    }

    buf->base = peer->recv_buffer + peer->recv_offset;
    buf->len = sizeof(peer->recv_buffer) - peer->recv_offset;
}

/* TCP read callback */
static void on_tcp_read(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    peer_connection_t *peer = (peer_connection_t *)stream->data;
    (void)buf;

    /* Check if peer is NULL (should not happen) or closing */
    if (!peer) {
        log_msg(LOG_WARN, "on_tcp_read: peer data is NULL!");
        if (stream && !uv_is_closing((uv_handle_t*)stream)) {
            uv_read_stop(stream);
        }
        return;
    }

    /* Validate peer pointer by checking if tcp_initialized flag is set
     * This helps detect use-after-free or corrupted pointers */
    if (!peer->tcp_initialized) {
        log_msg(LOG_ERROR, "on_tcp_read: peer->tcp_initialized is 0! Possible use-after-free or corruption.");
        log_msg(LOG_ERROR, "  peer=%p stream=%p nread=%zd", (void*)peer, (void*)stream, nread);
        if (stream && !uv_is_closing((uv_handle_t*)stream)) {
            uv_read_stop(stream);
        }
        return;
    }

    if (peer->closed) {
        /* Peer is closing - stop reading to prevent further callbacks */
        if (stream && !uv_is_closing((uv_handle_t*)stream)) {
            uv_read_stop(stream);
        }
        return;  /* Already closed */
    }

    if (nread < 0) {
        close_peer_connection(peer, "read error");
        return;
    }

    if (nread == 0) {
        return;
    }

    /* Validate that nread doesn't exceed buffer capacity */
    if ((size_t)nread > sizeof(peer->recv_buffer) - peer->recv_offset) {
        log_msg(LOG_ERROR, "nread=%zd would overflow buffer (offset=%zu size=%zu)",
                nread, peer->recv_offset, sizeof(peer->recv_buffer));
        close_peer_connection(peer, "buffer overflow in on_tcp_read");
        return;
    }

    peer->recv_offset += (size_t)nread;

    /* Process received data based on state */
    if (peer->state == PEER_STATE_HANDSHAKING) {
        if (peer->recv_offset >= BT_HANDSHAKE_SIZE) {
            process_handshake(peer, (uint8_t *)peer->recv_buffer, peer->recv_offset);
        }
    } else if (peer->state >= PEER_STATE_EXTENDED_HANDSHAKE) {
        /* Process BitTorrent messages */
        while (peer->recv_offset >= 4 && !peer->closed) {
            uint32_t msg_len;
            memcpy(&msg_len, peer->recv_buffer, 4);
            msg_len = ntohl(msg_len);

            /* Validate message length to prevent buffer overflow */
            if (msg_len > MAX_BT_MESSAGE_SIZE) {
                char err[64];
                snprintf(err, sizeof(err), "message too large: %u", msg_len);
                close_peer_connection(peer, err);
                return;
            }

            /* Check for integer overflow */
            if (msg_len + 4 < msg_len) {
                close_peer_connection(peer, "integer overflow in message length");
                return;
            }

            /* Check if message exceeds buffer capacity */
            if (msg_len + 4 > sizeof(peer->recv_buffer)) {
                close_peer_connection(peer, "message exceeds buffer capacity");
                return;
            }

            /* Check if we have the complete message */
            if (peer->recv_offset >= msg_len + 4) {
                process_message(peer, (uint8_t *)peer->recv_buffer + 4, msg_len);

                if (peer->closed) {
                    return;  /* Connection closed during processing */
                }

                /* Validate buffer size before memmove */
                if (peer->recv_offset < msg_len + 4) {
                    close_peer_connection(peer, "buffer underflow");
                    return;
                }

                /* Additional safety check: ensure source pointer is within buffer */
                if (msg_len + 4 > sizeof(peer->recv_buffer)) {
                    close_peer_connection(peer, "message size exceeds buffer in memmove");
                    return;
                }

                /* Shift buffer */
                size_t remaining = peer->recv_offset - msg_len - 4;
                if (remaining > 0) {
                    /* Ensure we're not reading past the buffer end */
                    if (msg_len + 4 + remaining > sizeof(peer->recv_buffer)) {
                        log_msg(LOG_ERROR, "FATAL: memmove would read past buffer: msg_len=%u remaining=%zu",
                                msg_len, remaining);
                        close_peer_connection(peer, "memmove would read past buffer");
                        return;
                    }
                    memmove(peer->recv_buffer, peer->recv_buffer + msg_len + 4, remaining);
                }
                peer->recv_offset -= msg_len + 4;
            } else {
                break;
            }
        }
    }
}

/* Process handshake response */
static int process_handshake(peer_connection_t *peer, const uint8_t *data, size_t len) {
    if (peer->closed) {
        return -1;
    }

    if (len < BT_HANDSHAKE_SIZE) {
        return -1;
    }

    /* Verify protocol string */
    if (data[0] != BT_PROTOCOL_LEN ||
        memcmp(data + 1, BT_PROTOCOL_STRING, BT_PROTOCOL_LEN) != 0) {
        close_peer_connection(peer, "invalid protocol");
        return -1;
    }

    /* Check extension support */
    if ((data[25] & 0x10) == 0) {
        /* Update statistics: no metadata support */
        metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
        if (fetcher) {
            uv_mutex_lock(&fetcher->mutex);
            fetcher->no_metadata_support++;
            uv_mutex_unlock(&fetcher->mutex);
        }
        close_peer_connection(peer, "no extension support");
        return -1;
    }

    /* Verify info_hash */
    if (memcmp(data + 28, peer->info_hash, 20) != 0) {
        /* Update statistics: handshake failed */
        metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
        if (fetcher) {
            uv_mutex_lock(&fetcher->mutex);
            fetcher->handshake_failed++;
            uv_mutex_unlock(&fetcher->mutex);
        }
        close_peer_connection(peer, "info_hash mismatch");
        return -1;
    }

    /* Send extended handshake */
    if (send_extended_handshake(peer) != 0) {
        close_peer_connection(peer, "failed to send extended handshake");
        return -1;
    }

    /* Validate buffer size before memmove */
    if (peer->recv_offset < BT_HANDSHAKE_SIZE) {
        close_peer_connection(peer, "buffer underflow in handshake");
        return -1;
    }

    /* Shift buffer */
    size_t remaining = peer->recv_offset - BT_HANDSHAKE_SIZE;
    if (remaining > 0) {
        memmove(peer->recv_buffer, peer->recv_buffer + BT_HANDSHAKE_SIZE, remaining);
    }
    peer->recv_offset = remaining;

    return 0;
}

/* Process BitTorrent message */
static int process_message(peer_connection_t *peer, const uint8_t *data, size_t len) {
    if (peer->closed) {
        return -1;
    }

    if (len == 0) {
        /* Keep-alive */
        return 0;
    }

    uint8_t msg_type = data[0];

    if (msg_type == BT_MSG_EXTENDED && len > 1) {
        uint8_t ext_id = data[1];

        if (ext_id == BT_EXT_HANDSHAKE) {
            /* Parse extended handshake response using bencode-c */
            struct bencode b;
            bencode_init(&b, data + 2, len - 2);

            int found_ut_metadata = 0;
            int found_metadata_size = 0;
            int in_m_dict = 0;

            /* Track the last key we saw when parsing dict */
            char last_dict_key[256] = {0};
            size_t last_dict_key_len = 0;
            int expecting_key = 0;  /* In a dict, are we expecting a key next? */
            int expecting_value = 0;  /* In a dict, did we just see a key and expecting its value? */

            int token;
            int depth = 0;  /* Track nesting depth */

            while ((token = bencode_next(&b)) > 0) {
                if (token == BENCODE_DICT_BEGIN) {
                    depth++;

                    /* Check if this dict is the value for key "m" */
                    if (expecting_value && depth == 2 &&
                        last_dict_key_len == 1 && memcmp(last_dict_key, "m", 1) == 0) {
                        in_m_dict = 1;
                    }

                    expecting_key = 1;  /* After DICT_BEGIN, we expect a key */
                    expecting_value = 0;
                    last_dict_key_len = 0;

                } else if (token == BENCODE_DICT_END) {
                    if (in_m_dict && depth == 2) {
                        in_m_dict = 0;
                    }
                    depth--;
                    expecting_key = (depth > 0) ? 1 : 0;  /* After DICT_END, if still in dict, expect key */
                    expecting_value = 0;

                } else if (token == BENCODE_STRING) {
                    if (expecting_key) {
                        /* This STRING is a dict key */
                        last_dict_key_len = b.toklen < sizeof(last_dict_key) ? b.toklen : sizeof(last_dict_key) - 1;
                        memcpy(last_dict_key, b.tok, last_dict_key_len);
                        last_dict_key[last_dict_key_len] = '\0';
                        expecting_key = 0;
                        expecting_value = 1;
                    } else {
                        /* This STRING is a value, not a key */
                        expecting_key = (depth > 0) ? 1 : 0;
                        expecting_value = 0;
                    }

                } else if (token == BENCODE_INTEGER) {
                    /* INTEGER tokens in dict context are always values (never keys)
                     * Stack-based checks are sufficient to identify the correct values */

                    /* Check for ut_metadata in m dict */
                    if (in_m_dict && b.size > 0 && b.stack[b.size - 1].key != NULL &&
                        b.stack[b.size - 1].keylen == 11 &&
                        memcmp(b.stack[b.size - 1].key, "ut_metadata", 11) == 0) {
                        peer->ut_metadata_id = (uint8_t)atoi((const char *)b.tok);
                        found_ut_metadata = 1;
                    }
                    /* Check for metadata_size in root dict */
                    else if (depth == 1 && b.size > 0 && b.stack[b.size - 1].key != NULL &&
                             b.stack[b.size - 1].keylen == 13 &&
                             memcmp(b.stack[b.size - 1].key, "metadata_size", 13) == 0) {
                        peer->metadata_size = atoi((const char *)b.tok);
                        found_metadata_size = 1;
                    }

                    expecting_key = (depth > 0) ? 1 : 0;
                    expecting_value = 0;

                } else {
                    /* LIST_BEGIN, LIST_END, etc - reset state */
                    expecting_key = 0;
                    expecting_value = 0;
                }
            }

            bencode_free(&b);

            /* Validate that we got the required fields */
            if (!found_ut_metadata) {
                /* Update statistics: no metadata support */
                metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
                if (fetcher) {
                    uv_mutex_lock(&fetcher->mutex);
                    fetcher->no_metadata_support++;
                    uv_mutex_unlock(&fetcher->mutex);
                }
                close_peer_connection(peer, "peer doesn't support ut_metadata");
                return -1;
            }

            if (!found_metadata_size || peer->metadata_size <= 0) {
                close_peer_connection(peer, "invalid or missing metadata_size");
                return -1;
            }

            if (peer->metadata_size > MAX_METADATA_SIZE) {
                close_peer_connection(peer, "metadata too large");
                return -1;
            }

            /* Calculate total pieces */
            peer->total_pieces = (peer->metadata_size + BT_METADATA_PIECE_SIZE - 1) /
                                 BT_METADATA_PIECE_SIZE;

            /* Allocate buffers */
            peer->metadata_buffer = (uint8_t *)malloc(peer->metadata_size);
            if (!peer->metadata_buffer) {
                close_peer_connection(peer, "out of memory for metadata buffer");
                return -1;
            }

            int bitmap_size = (peer->total_pieces + 7) / 8;
            peer->pieces_received = (uint8_t *)calloc(bitmap_size, 1);
            if (!peer->pieces_received) {
                close_peer_connection(peer, "out of memory for pieces bitmap");
                return -1;
            }

            /* Request ALL metadata pieces immediately (parallel requesting - bitmagnet approach) */
            peer->state = PEER_STATE_REQUESTING_METADATA;
            for (int piece = 0; piece < peer->total_pieces; piece++) {
                send_metadata_request(peer, piece);
            }
        } else if (ext_id == OUR_UT_METADATA_ID) {
            /* Metadata piece (peer sends using OUR extension ID) */
            parse_metadata_piece(peer, data + 2, len - 2);
        }
    }

    return 0;
}

/* Find next missing metadata piece */
static int find_next_missing_piece(peer_connection_t *peer) {
    for (int i = 0; i < peer->total_pieces; i++) {
        int byte_idx = i / 8;
        int bit_idx = i % 8;
        if (!(peer->pieces_received[byte_idx] & (1 << bit_idx))) {
            return i;
        }
    }
    return -1;  /* All pieces received */
}

/* Parse metadata piece */
static int parse_metadata_piece(peer_connection_t *peer, const uint8_t *data, size_t len) {
    if (peer->closed) {
        return -1;
    }

    /* Parse bencode dictionary header */
    struct bencode b;
    bencode_init(&b, data, len);

    int msg_type = -1;
    int piece_index = -1;
    const uint8_t *piece_data = NULL;
    size_t piece_data_len = 0;

    int token;
    int dict_depth = 0;
    size_t dict_end_pos = 0;

    /* Track current key manually (like extended handshake parser) */
    const char *current_key = NULL;
    size_t current_key_len = 0;
    int expecting_value = 0;

    while ((token = bencode_next(&b)) > 0) {
        if (token == BENCODE_DICT_BEGIN) {
            dict_depth++;
            expecting_value = 0;
        } else if (token == BENCODE_DICT_END) {
            dict_depth--;
            if (dict_depth == 0) {
                /* End of header dictionary, rest is piece data */
                dict_end_pos = (const uint8_t *)b.buf - data;
                break;
            }
            expecting_value = 0;
        } else if (token == BENCODE_STRING && dict_depth == 1 && !expecting_value) {
            /* This is a key in the top-level dict */
            current_key = (const char *)b.tok;
            current_key_len = b.toklen;
            expecting_value = 1;
        } else if (token == BENCODE_INTEGER && dict_depth == 1 && expecting_value) {
            /* This is a value for the current key */
            int value = atoi((const char *)b.tok);

            if (current_key_len == 8 && memcmp(current_key, "msg_type", 8) == 0) {
                msg_type = value;
            } else if (current_key_len == 5 && memcmp(current_key, "piece", 5) == 0) {
                piece_index = value;
            }

            expecting_value = 0;
        } else {
            /* Other tokens (strings as values, nested structures, etc) */
            expecting_value = 0;
        }
    }

    bencode_free(&b);

    /* Check message type */
    if (msg_type == UT_METADATA_REJECT) {
        /* Update statistics: metadata rejected */
        metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
        if (fetcher) {
            uv_mutex_lock(&fetcher->mutex);
            fetcher->metadata_rejected++;
            uv_mutex_unlock(&fetcher->mutex);
        }
        log_msg(LOG_WARN, "Peer rejected metadata request");
        close_peer_connection(peer, "metadata rejected");
        return -1;
    }

    if (msg_type != UT_METADATA_DATA) {
        log_msg(LOG_WARN, "Unexpected metadata message type: %d (expected %d=UT_METADATA_DATA)",
                msg_type, UT_METADATA_DATA);
        return -1;
    }

    /* Validate piece index */
    if (piece_index < 0 || piece_index >= peer->total_pieces) {
        log_msg(LOG_WARN, "Invalid piece index: %d (total_pieces=%d)", piece_index, peer->total_pieces);
        close_peer_connection(peer, "invalid piece index");
        return -1;
    }

    /* Extract piece data (everything after the bencode dictionary) */
    if (dict_end_pos < len) {
        piece_data = data + dict_end_pos;
        piece_data_len = len - dict_end_pos;
    } else {
        log_msg(LOG_WARN, "No piece data in metadata message (dict_end_pos=%zu >= len=%zu)",
                dict_end_pos, len);
        return -1;
    }

    /* Calculate expected piece size */
    size_t offset = (size_t)piece_index * BT_METADATA_PIECE_SIZE;
    size_t expected_size = BT_METADATA_PIECE_SIZE;
    if (offset + expected_size > (size_t)peer->metadata_size) {
        expected_size = (size_t)peer->metadata_size - offset;
    }

    /* Validate piece data length */
    if (piece_data_len != expected_size) {
        log_msg(LOG_WARN, "Piece data length mismatch: expected %zu, got %zu (piece %d, metadata_size=%d)",
               expected_size, piece_data_len, piece_index, peer->metadata_size);
        close_peer_connection(peer, "piece size mismatch");
        return -1;
    }

    /* Check if piece already received */
    int byte_idx = piece_index / 8;
    int bit_idx = piece_index % 8;
    if (peer->pieces_received[byte_idx] & (1 << bit_idx)) {
        return 0;
    }

    /* Copy piece data to buffer */
    memcpy(peer->metadata_buffer + offset, piece_data, piece_data_len);

    /* Mark piece as received */
    peer->pieces_received[byte_idx] |= (1 << bit_idx);
    peer->metadata_pieces_received++;

    /* Check if all pieces received */
    if (peer->metadata_pieces_received >= peer->total_pieces) {
        return verify_and_parse_metadata(peer);
    }

    /* All pieces already requested upfront - no sequential requesting needed */
    return 0;
}

/* Verify and parse complete metadata */
static int verify_and_parse_metadata(peer_connection_t *peer) {
    if (peer->closed) {
        return -1;
    }

    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;

    /* Step 1: Verify SHA-1 hash */
    unsigned char computed_hash[20];
    SHA1(peer->metadata_buffer, peer->metadata_size, computed_hash);

    if (memcmp(computed_hash, peer->info_hash, 20) != 0) {
        /* Update statistics: hash mismatch */
        uv_mutex_lock(&fetcher->mutex);
        fetcher->hash_mismatch++;
        uv_mutex_unlock(&fetcher->mutex);

        char hex[41];
        format_infohash_hex(peer->info_hash, hex);
        log_msg(LOG_ERROR, "SHA-1 verification failed for %s", hex);
        close_peer_connection(peer, "hash verification failed");
        return -1;
    }

    /* Step 2: Parse info dictionary using bencode-c */
    struct bencode b;
    bencode_init(&b, peer->metadata_buffer, peer->metadata_size);

    torrent_metadata_t torrent;
    memset(&torrent, 0, sizeof(torrent));
    memcpy(torrent.info_hash, peer->info_hash, 20);

    char *name = NULL;
    int64_t piece_length = 0;
    int64_t single_file_length = -1;
    const uint8_t *pieces_data = NULL;
    size_t pieces_len = 0;

    /* For multi-file mode */
    typedef struct {
        char *path;
        int64_t size;
    } temp_file_t;
    temp_file_t *files = NULL;
    int num_files = 0;
    int files_capacity = 0;

    int in_files_list = 0;
    int in_file_dict = 0;
    int in_path_list = 0;
    char path_buffer[1024] = {0};
    int64_t current_file_length = 0;

    int token;
    while ((token = bencode_next(&b)) > 0) {
        /* Note: Removed BENCODE_IS_VALUE checks - they return false when value arrives.
         * Instead, check if b.size > 0 (we're in a dict) and check the key directly. */
        if (token == BENCODE_STRING) {
            if (b.size > 0 && b.stack[b.size - 1].key != NULL &&
                b.stack[b.size - 1].keylen == 4 &&
                memcmp(b.stack[b.size - 1].key, "name", 4) == 0) {
                /* Torrent name */
                name = (char *)malloc(b.toklen + 1);
                if (name) {
                    memcpy(name, b.tok, b.toklen);
                    name[b.toklen] = '\0';
                }
            } else if (b.size > 0 && b.stack[b.size - 1].key != NULL &&
                      b.stack[b.size - 1].keylen == 6 &&
                      memcmp(b.stack[b.size - 1].key, "pieces", 6) == 0) {
                /* Pieces hash */
                pieces_data = (const uint8_t *)b.tok;
                pieces_len = b.toklen;
            } else if (in_path_list) {
                /* Path component in multi-file mode */
                if (strlen(path_buffer) > 0) {
                    strncat(path_buffer, "/", sizeof(path_buffer) - strlen(path_buffer) - 1);
                }
                strncat(path_buffer, (const char *)b.tok,
                       b.toklen < sizeof(path_buffer) - strlen(path_buffer) - 1 ?
                       b.toklen : sizeof(path_buffer) - strlen(path_buffer) - 1);
            }
        } else if (token == BENCODE_INTEGER) {
            if (b.size > 0 && b.stack[b.size - 1].key != NULL &&
                b.stack[b.size - 1].keylen == 12 &&
                memcmp(b.stack[b.size - 1].key, "piece length", 12) == 0) {
                /* Piece length */
                piece_length = atoll((const char *)b.tok);
            } else if (b.size > 0 && b.stack[b.size - 1].key != NULL &&
                      b.stack[b.size - 1].keylen == 6 &&
                      memcmp(b.stack[b.size - 1].key, "length", 6) == 0) {
                if (in_file_dict) {
                    /* File length in multi-file mode */
                    current_file_length = atoll((const char *)b.tok);
                } else {
                    /* Single file length */
                    single_file_length = atoll((const char *)b.tok);
                }
            }
        } else if (token == BENCODE_LIST_BEGIN) {
            /* When BENCODE_LIST_BEGIN is encountered, the list has been pushed onto the stack.
             * The key for this list is in the PARENT dictionary (stack[size-2]), not the current level.
             * Check b.size >= 2 to ensure we have a parent dict. */
            if (b.size >= 2 && b.stack[b.size - 2].key != NULL &&
                b.stack[b.size - 2].keylen == 5 &&
                memcmp(b.stack[b.size - 2].key, "files", 5) == 0) {
                in_files_list = 1;
            } else if (in_file_dict && b.size >= 2 && b.stack[b.size - 2].key != NULL &&
                      b.stack[b.size - 2].keylen == 4 &&
                      memcmp(b.stack[b.size - 2].key, "path", 4) == 0) {
                in_path_list = 1;
                path_buffer[0] = '\0';
            }
        } else if (token == BENCODE_LIST_END) {
            if (in_path_list) {
                in_path_list = 0;
            } else if (in_files_list && !in_file_dict) {
                in_files_list = 0;
            }
        } else if (token == BENCODE_DICT_BEGIN) {
            if (in_files_list) {
                in_file_dict = 1;
                path_buffer[0] = '\0';
                current_file_length = 0;
            }
        } else if (token == BENCODE_DICT_END) {
            if (in_file_dict) {
                /* Save file info */
                if (num_files >= files_capacity) {
                    files_capacity = files_capacity == 0 ? 16 : files_capacity * 2;
                    files = (temp_file_t *)realloc(files, files_capacity * sizeof(temp_file_t));
                }
                if (files && strlen(path_buffer) > 0) {
                    files[num_files].path = strdup(path_buffer);
                    files[num_files].size = current_file_length;
                    num_files++;
                }
                in_file_dict = 0;
            }
        }
    }

    bencode_free(&b);

    /* Validate required fields */
    if (!name || !pieces_data || piece_length == 0) {
        log_msg(LOG_ERROR, "Missing required fields in info dictionary (name=%s, pieces=%s, piece_length=%ld)",
                name ? "present" : "missing",
                pieces_data ? "present" : "missing",
                (long)piece_length);
        if (name) free(name);
        if (files) {
            for (int i = 0; i < num_files; i++) {
                free(files[i].path);
            }
            free(files);
        }
        close_peer_connection(peer, "invalid info dict");
        return -1;
    }

    /* Populate torrent metadata */
    torrent.name = name;
    torrent.piece_length = (int32_t)piece_length;
    torrent.num_pieces = pieces_len / 20;
    torrent.added_timestamp = time(NULL);
    torrent.last_seen = time(NULL);

    /* Query peer store for total peer count at time of metadata fetch */
    int peer_count = 0;
    if (peer->fetcher) {
        metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
        if (fetcher->peer_store) {
            peer_count = peer_store_count_peers(fetcher->peer_store, peer->info_hash);
        }
    }
    torrent.total_peers = peer_count;

    /* Convert temp files to file_info_t and calculate total size */
    file_info_t *file_info = NULL;
    if (single_file_length >= 0) {
        /* Single file mode - create single file entry */
        torrent.size_bytes = single_file_length;
        file_info = malloc(sizeof(file_info_t));
        if (file_info) {
            file_info[0].path = strdup(name);  /* Use torrent name as filename */
            file_info[0].size_bytes = single_file_length;
            file_info[0].file_index = 0;
            torrent.files = file_info;
            torrent.num_files = 1;
        } else {
            torrent.files = NULL;
            torrent.num_files = 0;
        }
    } else if (num_files > 0) {
        /* Multi-file mode - convert temp_file_t to file_info_t */
        torrent.size_bytes = 0;
        file_info = malloc(num_files * sizeof(file_info_t));
        if (file_info) {
            for (int i = 0; i < num_files; i++) {
                file_info[i].path = files[i].path;  /* Transfer ownership */
                file_info[i].size_bytes = files[i].size;
                file_info[i].file_index = i;
                torrent.size_bytes += files[i].size;
            }
            torrent.files = file_info;
            torrent.num_files = num_files;
        } else {
            /* Allocation failed - clean up temp files */
            for (int i = 0; i < num_files; i++) {
                free(files[i].path);
            }
            torrent.files = NULL;
            torrent.num_files = 0;
        }
        free(files);  /* Free the temp array (paths transferred to file_info) */
        files = NULL;  /* Prevent double-free below */
    } else {
        log_msg(LOG_ERROR, "No file information found");
        free(name);
        if (files) {
            for (int i = 0; i < num_files; i++) {
                free(files[i].path);
            }
            free(files);
        }
        close_peer_connection(peer, "no files");
        return -1;
    }

    log_msg(LOG_DEBUG, "Parsed torrent: %s (%lld bytes, %d pieces, %d files)",
           name, (long long)torrent.size_bytes, torrent.num_pieces,
           torrent.num_files);

    /* Add to batch writer - batch writer will make deep copies */
    int rc = batch_writer_add(fetcher->batch_writer, &torrent);
    if (rc != 0) {
        char hex[41];
        format_infohash_hex(peer->info_hash, hex);
        log_msg(LOG_ERROR, "Failed to add torrent %s to batch writer (name: %s, rc=%d)",
                hex, name, rc);
    } else {
        char hex[41];
        format_infohash_hex(peer->info_hash, hex);
        log_msg(LOG_DEBUG, "Successfully added torrent %s to batch writer", hex);
    }

    /* Cleanup - free the torrent structure fields */
    free(name);
    if (torrent.files) {
        for (int32_t i = 0; i < torrent.num_files; i++) {
            if (torrent.files[i].path) {
                free(torrent.files[i].path);
            }
        }
        free(torrent.files);
    }
    /* files array already freed or ownership transferred above */

    /* Update statistics */
    uv_mutex_lock(&fetcher->mutex);
    fetcher->total_fetched++;
    uv_mutex_unlock(&fetcher->mutex);

    /* Close all other connections for this info_hash */
    uv_mutex_lock(&fetcher->mutex);
    peer_connection_t *p = fetcher->active_connections;
    while (p) {
        peer_connection_t *next = p->next;
        if (p != peer && memcmp(p->info_hash, peer->info_hash, 20) == 0 && !p->closed) {
            /* Use locked version since we already hold the mutex */
            close_peer_connection_locked(p, "already fetched");
        }
        p = next;
    }
    uv_mutex_unlock(&fetcher->mutex);

    peer->state = PEER_STATE_COMPLETE;
    close_peer_connection(peer, "complete");
    return rc;
}

/* Timeout callback */
static void on_timeout(uv_timer_t *handle) {
    peer_connection_t *peer = (peer_connection_t *)handle->data;

    /* Check if peer is NULL (should not happen) or closing */
    if (!peer) {
        if (handle && !uv_is_closing((uv_handle_t*)handle)) {
            uv_timer_stop(handle);
        }
        return;
    }

    if (peer->closed) {
        /* Peer already closing - stop timer and return */
        if (handle && !uv_is_closing((uv_handle_t*)handle)) {
            uv_timer_stop(handle);
        }
        return;  /* Already closed */
    }

    /* Update statistics: connection timeout */
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
    if (fetcher) {
        uv_mutex_lock(&fetcher->mutex);
        fetcher->connection_timeout++;
        uv_mutex_unlock(&fetcher->mutex);
    }

    close_peer_connection(peer, "timeout");
}

/* TCP connect callback */
static void on_tcp_connect(uv_connect_t *req, int status) {
    peer_connection_t *peer = (peer_connection_t *)req->data;

    /* Check if peer is NULL (should not happen) or closing */
    if (!peer) {
        return;
    }

    if (peer->closed) {
        /* Peer closed during connection attempt */
        return;  /* Already closed */
    }

    if (status != 0) {
        /* Update statistics: connection failed */
        metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
        if (fetcher) {
            uv_mutex_lock(&fetcher->mutex);
            fetcher->connection_failed++;
            uv_mutex_unlock(&fetcher->mutex);
        }
        close_peer_connection(peer, "connect failed");
        return;
    }

    /* Start reading */
    int rc = uv_read_start((uv_stream_t *)&peer->tcp, alloc_buffer, on_tcp_read);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start reading: %s", uv_strerror(rc));
        close_peer_connection(peer, "read start failed");
        return;
    }

    /* Send handshake */
    if (send_handshake(peer) != 0) {
        close_peer_connection(peer, "failed to send handshake");
    }
}

/* Callback when a handle is closed - properly frees peer when all handles closed */
static void on_handle_closed(uv_handle_t *handle) {
    peer_connection_t *peer = (peer_connection_t *)handle->data;
    if (!peer) {
        /* This can happen if we already freed the peer or if data was set to NULL.
         * This is not necessarily an error - it can occur during normal shutdown. */
        return;
    }

    /* Sanity check: peer should be marked as closed */
    if (!peer->closed) {
        log_msg(LOG_WARN, "on_handle_closed called but peer->closed is not set!");
    }

    /* NULL out this handle's data pointer immediately to prevent any late callbacks
     * from accessing the peer structure after it's freed */
    handle->data = NULL;

    peer->handles_to_close--;

    /* Only free when all handles are closed */
    if (peer->handles_to_close == 0) {

        /* Clear initialization flags to help detect use-after-free */
        peer->tcp_initialized = 0;
        peer->timer_initialized = 0;

        /* Free buffers */
        if (peer->metadata_buffer) {
            free(peer->metadata_buffer);
        }
        if (peer->pieces_received) {
            free(peer->pieces_received);
        }

        /* Finally free the peer structure */
        free(peer);
    } else if (peer->handles_to_close < 0) {
        log_msg(LOG_ERROR, "BUG: handles_to_close went negative (%d)! reason: %s",
                peer->handles_to_close, peer->close_reason);
    }
}

/* Close peer connection with proper async handling (internal, assumes mutex held) */
static void close_peer_connection_locked(peer_connection_t *peer, const char *reason) {
    if (!peer || peer->closed) {
        return;  /* Already closing or closed */
    }

    /* Stop reading immediately to prevent callbacks with freed memory */
    if (peer->tcp_initialized && !uv_is_closing((uv_handle_t*)&peer->tcp)) {
        uv_read_stop((uv_stream_t*)&peer->tcp);
    }

    peer->closed = 1;
    snprintf(peer->close_reason, sizeof(peer->close_reason), "%s", reason);

    /* NOTE: We do NOT set tcp.data/timeout_timer.data to NULL here because
     * on_handle_closed() needs access to the peer structure to free it properly.
     * Instead, we rely on the peer->closed flag to prevent access in other callbacks. */

    /* Remove from active list (caller must hold fetcher->mutex) */
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
    if (fetcher) {
        peer_connection_t **p = &fetcher->active_connections;
        while (*p) {
            if (*p == peer) {
                *p = peer->next;
                fetcher->active_count--;
                break;
            }
            p = &(*p)->next;
        }

        /* Update attempt entry and check if all connections for this infohash are done */
        /* IMPORTANT: Must release fetcher->mutex before calling handle_infohash_failure
         * to avoid deadlock, since handle_infohash_failure acquires fetcher->mutex */
        uint8_t info_hash_copy[20];
        memcpy(info_hash_copy, peer->info_hash, 20);
        int should_handle_failure = 0;

        uv_mutex_lock(&fetcher->attempt_table_mutex);
        infohash_attempt_t *attempt = lookup_attempt(fetcher, peer->info_hash);
        if (attempt) {
            attempt->active_connections--;

            /* Update failure statistics based on close reason (unless successful) */
            if (peer->state != PEER_STATE_COMPLETE) {
                update_attempt_statistics(attempt, peer, reason);

                /* If connection failed and there are more peers to try, start next peer */
                if (attempt->peers_tried < attempt->total_peer_count) {
                    uv_mutex_unlock(&fetcher->attempt_table_mutex);
                    uv_mutex_unlock(&fetcher->mutex);

                    /* Try next peer immediately to maintain concurrent connections */
                    start_next_peer_connections(fetcher, attempt);

                    uv_mutex_lock(&fetcher->mutex);
                    uv_mutex_lock(&fetcher->attempt_table_mutex);

                    /* Re-lookup attempt in case it was modified */
                    attempt = lookup_attempt(fetcher, peer->info_hash);
                }
            }

            /* If this was the last connection for this infohash */
            if (attempt && attempt->active_connections == 0) {
                /* Only handle failure if we didn't successfully fetch metadata */
                if (peer->state != PEER_STATE_COMPLETE) {
                    /* Check if ANY connection for this infohash succeeded */
                    int already_fetched = database_check_exists(fetcher->database, peer->info_hash);
                    if (!already_fetched) {
                        /* Only mark as failure if we've exhausted all peers */
                        if (attempt->peers_tried >= attempt->total_peer_count) {
                            should_handle_failure = 1;
                        }
                    }
                }
            }
        }
        uv_mutex_unlock(&fetcher->attempt_table_mutex);

        /* Release fetcher->mutex before calling handle_infohash_failure to avoid deadlock */
        if (should_handle_failure) {
            uv_mutex_unlock(&fetcher->mutex);

            /* Re-acquire attempt_table_mutex to get the attempt */
            uv_mutex_lock(&fetcher->attempt_table_mutex);
            attempt = lookup_attempt(fetcher, info_hash_copy);
            if (attempt && attempt->active_connections == 0) {
                handle_infohash_failure(fetcher, attempt);
                uv_mutex_unlock(&fetcher->attempt_table_mutex);
                remove_attempt(fetcher, info_hash_copy);
            } else {
                uv_mutex_unlock(&fetcher->attempt_table_mutex);
            }

            uv_mutex_lock(&fetcher->mutex);
        } else {
            /* No failure to handle, but may need to remove attempt entry on success */
            /* Re-acquire attempt_table_mutex to safely check and remove */
            uv_mutex_lock(&fetcher->attempt_table_mutex);
            attempt = lookup_attempt(fetcher, info_hash_copy);
            if (attempt && attempt->active_connections == 0) {
                uv_mutex_unlock(&fetcher->attempt_table_mutex);
                /* Remove attempt entry (successful or no more connections) */
                remove_attempt(fetcher, info_hash_copy);
            } else {
                uv_mutex_unlock(&fetcher->attempt_table_mutex);
            }
        }
    }

    /* Stop and close handles asynchronously */
    if (peer->timer_initialized) {
        uv_timer_stop(&peer->timeout_timer);
        uv_close((uv_handle_t *)&peer->timeout_timer, on_handle_closed);
    } else {
        peer->handles_to_close--;  /* Wasn't initialized, don't wait for it */
    }

    if (peer->tcp_initialized) {
        uv_close((uv_handle_t *)&peer->tcp, on_handle_closed);
    } else {
        peer->handles_to_close--;  /* Wasn't initialized, don't wait for it */
    }

    /* CRITICAL FIX: Peer is ONLY freed in on_handle_closed() when all handles are closed.
     * This ensures the peer structure remains valid until libuv guarantees no more
     * callbacks will fire. This fixes the race condition where on_tcp_read() could be
     * called with freed memory if the peer was freed here while a read was in progress. */
}

/* Close peer connection with proper async handling (public API) */
static void close_peer_connection(peer_connection_t *peer, const char *reason) {
    if (!peer || peer->closed) {
        return;  /* Already closing or closed */
    }

    /* Acquire mutex and call locked version */
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)peer->fetcher;
    if (fetcher) {
        uv_mutex_lock(&fetcher->mutex);
        close_peer_connection_locked(peer, reason);
        uv_mutex_unlock(&fetcher->mutex);
    } else {
        /* No fetcher, just close directly without mutex */
        close_peer_connection_locked(peer, reason);
    }
}

/* Initialize retry queue */
static retry_queue_t* retry_queue_init(size_t max_retries) {
    retry_queue_t *queue = (retry_queue_t *)calloc(1, sizeof(retry_queue_t));
    if (!queue) {
        return NULL;
    }

    queue->head = NULL;
    queue->tail = NULL;
    queue->count = 0;
    queue->max_retries = max_retries;

    /* Set exponential backoff delays (bitmagnet-inspired): 30s, 120s (2min), 300s (5min) */
    queue->retry_delays[0] = 30;    /* 30 seconds (was 120s) */
    queue->retry_delays[1] = 120;   /* 2 minutes (was 600s/10min) */
    queue->retry_delays[2] = 300;   /* 5 minutes (was 1800s/30min) */

    if (uv_mutex_init(&queue->mutex) != 0) {
        free(queue);
        return NULL;
    }

    return queue;
}

/* Cleanup retry queue */
static void retry_queue_cleanup(retry_queue_t *queue) {
    if (!queue) {
        return;
    }

    /* Free all entries */
    retry_entry_t *entry = queue->head;
    while (entry) {
        retry_entry_t *next = entry->next;
        free(entry);
        entry = next;
    }

    uv_mutex_destroy(&queue->mutex);
    free(queue);
}

/* Add info_hash to retry queue */
static int retry_queue_add(retry_queue_t *queue, const uint8_t *info_hash, int retry_count) {
    if (!queue || !info_hash || retry_count >= (int)queue->max_retries) {
        return -1;
    }

    retry_entry_t *entry = (retry_entry_t *)calloc(1, sizeof(retry_entry_t));
    if (!entry) {
        return -1;
    }

    memcpy(entry->info_hash, info_hash, 20);
    entry->retry_count = retry_count;
    entry->consecutive_no_peers = 0;

    /* Calculate next retry time based on retry count with jitter to prevent thundering herd */
    time_t delay = queue->retry_delays[retry_count];
    int jitter = rand() % (delay / 2);  /* Random jitter up to 50% of delay */
    entry->next_retry_time = time(NULL) + delay + jitter;
    entry->next = NULL;

    uv_mutex_lock(&queue->mutex);

    /* Add to tail of queue */
    if (queue->tail) {
        queue->tail->next = entry;
    } else {
        queue->head = entry;
    }
    queue->tail = entry;
    queue->count++;

    uv_mutex_unlock(&queue->mutex);

    return 0;
}

/* Retry thread - periodically checks retry queue and reissues get_peers queries */
static void retry_thread_fn(void *arg) {
    metadata_fetcher_t *fetcher = (metadata_fetcher_t *)arg;

    log_msg(LOG_DEBUG, "Retry thread started");

    while (fetcher->running) {
        time_t now = time(NULL);

        uv_mutex_lock(&fetcher->retry_queue->mutex);

        retry_entry_t **prev_ptr = &fetcher->retry_queue->head;
        retry_entry_t *entry = fetcher->retry_queue->head;

        while (entry) {
            if (now >= entry->next_retry_time) {
                /* Time to retry this info_hash */
                retry_entry_t *to_retry = entry;

                /* Remove from queue */
                *prev_ptr = entry->next;
                if (entry == fetcher->retry_queue->tail) {
                    fetcher->retry_queue->tail = NULL;
                }
                entry = entry->next;
                fetcher->retry_queue->count--;

                uv_mutex_unlock(&fetcher->retry_queue->mutex);

                /* Update statistics */
                uv_mutex_lock(&fetcher->mutex);
                fetcher->retry_attempts++;
                uv_mutex_unlock(&fetcher->mutex);

                /* Re-query DHT for fresh peers before retry (critical fix) */
                if (fetcher->dht_manager) {
                    dht_manager_query_peers((dht_manager_t *)fetcher->dht_manager, to_retry->info_hash, false);
                    /* Small delay to let DHT responses populate peer_store */
                    usleep(500000);  /* 500ms - balance between freshness and latency */
                }

                /* Check if peers are now available */
                struct sockaddr_storage peers[MAX_PEERS_PER_REQUEST];
                socklen_t peer_lens[MAX_PEERS_PER_REQUEST];
                int peer_count = 0;

                if (peer_store_get_peers(fetcher->peer_store, to_retry->info_hash,
                                        peers, peer_lens, &peer_count, MAX_PEERS_PER_REQUEST) == 0 && peer_count > 0) {
                    /* Peers found! Reset consecutive no-peer counter and submit */
                    to_retry->consecutive_no_peers = 0;

                    /* Create task for worker pool (REUSE existing structure) */
                    fetch_task_t *task = (fetch_task_t *)malloc(sizeof(fetch_task_t));
                    if (!task) {
                        log_msg(LOG_ERROR, "Failed to allocate fetch task for retry");
                        free(to_retry);
                        uv_mutex_lock(&fetcher->retry_queue->mutex);
                        continue;
                    }

                    memcpy(task->info_hash, to_retry->info_hash, 20);
                    task->fetcher = fetcher;

                    /* Submit to worker pool (REUSE existing submission mechanism) */
                    if (worker_pool_submit(fetcher->worker_pool, task) == 0) {
                        /* Successfully submitted for metadata fetch */
                        uv_mutex_lock(&fetcher->mutex);
                        fetcher->retry_submitted++;  // New counter: retries submitted to worker pool
                        uv_mutex_unlock(&fetcher->mutex);
                    } else {
                        /* Worker pool full - try again next cycle */
                        log_msg(LOG_WARN, "Worker pool full, re-queuing retry for next cycle");
                        retry_queue_add(fetcher->retry_queue, to_retry->info_hash, to_retry->retry_count);
                        free(task);
                    }

                    free(to_retry);
                } else {
                    /* Still no peers - increment consecutive no-peer counter */
                    to_retry->consecutive_no_peers++;

                    /* Abandon if 3+ consecutive no-peer failures */
                    if (to_retry->consecutive_no_peers >= 3) {
                        uv_mutex_lock(&fetcher->mutex);
                        fetcher->retry_abandoned++;
                        uv_mutex_unlock(&fetcher->mutex);
                        free(to_retry);
                    }
                    /* Abandon if max retries reached */
                    else if (to_retry->retry_count + 1 >= (int)fetcher->retry_queue->max_retries) {
                        uv_mutex_lock(&fetcher->mutex);
                        fetcher->retry_abandoned++;
                        uv_mutex_unlock(&fetcher->mutex);
                        free(to_retry);
                    }
                    /* Otherwise add back to retry queue with incremented count */
                    else {
                        retry_queue_add(fetcher->retry_queue, to_retry->info_hash, to_retry->retry_count + 1);
                        free(to_retry);
                    }
                }

                uv_mutex_lock(&fetcher->retry_queue->mutex);
            } else {
                /* Not time yet, move to next */
                prev_ptr = &entry->next;
                entry = entry->next;
            }
        }

        uv_mutex_unlock(&fetcher->retry_queue->mutex);

        /* Sleep for 10 seconds between scans */
        sleep(10);
    }

    log_msg(LOG_DEBUG, "Retry thread stopped");
}

/* Hash function for infohash (simple but effective) */
static uint32_t hash_infohash(const uint8_t *info_hash) {
    uint32_t hash = 0;
    for (int i = 0; i < 20; i++) {
        hash = hash * 31 + info_hash[i];
    }
    return hash % 1024;
}

/* Lookup attempt entry (caller must hold attempt_table_mutex) */
static infohash_attempt_t* lookup_attempt(metadata_fetcher_t *fetcher, const uint8_t *info_hash) {
    uint32_t bucket = hash_infohash(info_hash);
    infohash_attempt_t *attempt = fetcher->attempt_table[bucket];

    while (attempt) {
        if (memcmp(attempt->info_hash, info_hash, 20) == 0) {
            return attempt;
        }
        attempt = attempt->next;
    }

    return NULL;
}

/* Create or get existing attempt entry (thread-safe) */
static infohash_attempt_t* create_or_get_attempt(metadata_fetcher_t *fetcher, const uint8_t *info_hash, int peer_count) {
    uv_mutex_lock(&fetcher->attempt_table_mutex);

    /* Check if entry already exists */
    infohash_attempt_t *attempt = lookup_attempt(fetcher, info_hash);
    if (attempt) {
        /* Update peer count if higher */
        if (peer_count > attempt->peer_count_at_start) {
            attempt->peer_count_at_start = peer_count;
        }
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return attempt;
    }

    /* Create new entry */
    attempt = (infohash_attempt_t *)calloc(1, sizeof(infohash_attempt_t));
    if (!attempt) {
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return NULL;
    }

    memcpy(attempt->info_hash, info_hash, 20);
    attempt->first_attempt_time = time(NULL);
    attempt->last_attempt_time = time(NULL);
    attempt->peer_count_at_start = peer_count;
    attempt->furthest_state = PEER_STATE_CONNECTING;
    attempt->active_connections = 0;

    /* Insert into hash table */
    uint32_t bucket = hash_infohash(info_hash);
    attempt->next = fetcher->attempt_table[bucket];
    fetcher->attempt_table[bucket] = attempt;

    uv_mutex_unlock(&fetcher->attempt_table_mutex);
    return attempt;
}

/* Create or get existing attempt entry with peer queue (thread-safe) */
static infohash_attempt_t* create_or_get_attempt_with_peers(
    metadata_fetcher_t *fetcher,
    const uint8_t *info_hash,
    struct sockaddr_storage *peers,
    socklen_t *peer_lens,
    int peer_count
) {
    if (!fetcher || !info_hash || !peers || !peer_lens || peer_count <= 0) {
        return NULL;
    }

    uv_mutex_lock(&fetcher->attempt_table_mutex);

    /* Check if entry already exists */
    infohash_attempt_t *attempt = lookup_attempt(fetcher, info_hash);
    if (attempt) {
        /* Entry exists - don't overwrite peer queue, just return it */
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return attempt;
    }

    /* Create new entry */
    attempt = (infohash_attempt_t *)calloc(1, sizeof(infohash_attempt_t));
    if (!attempt) {
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return NULL;
    }

    memcpy(attempt->info_hash, info_hash, 20);
    attempt->first_attempt_time = time(NULL);
    attempt->last_attempt_time = time(NULL);

    /* Allocate and copy peer list */
    attempt->available_peers = malloc(sizeof(struct sockaddr_storage) * peer_count);
    attempt->available_peer_lens = malloc(sizeof(socklen_t) * peer_count);

    if (!attempt->available_peers || !attempt->available_peer_lens) {
        free(attempt->available_peers);
        free(attempt->available_peer_lens);
        free(attempt);
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return NULL;
    }

    memcpy(attempt->available_peers, peers, sizeof(struct sockaddr_storage) * peer_count);
    memcpy(attempt->available_peer_lens, peer_lens, sizeof(socklen_t) * peer_count);
    attempt->total_peer_count = peer_count;
    attempt->peers_tried = 0;
    attempt->max_concurrent_for_this_hash = fetcher->max_concurrent_per_infohash;
    attempt->peer_count_at_start = peer_count;
    attempt->furthest_state = PEER_STATE_CONNECTING;
    attempt->active_connections = 0;

    /* Insert into hash table */
    uint32_t bucket = hash_infohash(info_hash);
    attempt->next = fetcher->attempt_table[bucket];
    fetcher->attempt_table[bucket] = attempt;

    uv_mutex_unlock(&fetcher->attempt_table_mutex);
    return attempt;
}

/* Try to start next peer connection(s) for an infohash - maintains concurrent connections */
static void start_next_peer_connections(metadata_fetcher_t *fetcher, infohash_attempt_t *attempt) {
    if (!fetcher || !attempt) {
        return;
    }

    uv_mutex_lock(&fetcher->attempt_table_mutex);

    /* Calculate how many more connections we can start */
    int active = attempt->active_connections;
    int max_concurrent = attempt->max_concurrent_for_this_hash;
    int peers_remaining = attempt->total_peer_count - attempt->peers_tried;

    int to_start = max_concurrent - active;
    if (to_start > peers_remaining) {
        to_start = peers_remaining;
    }

    if (to_start <= 0) {
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return;
    }

    /* Check global connection limit */
    uv_mutex_lock(&fetcher->mutex);
    int current_global = fetcher->active_count;
    int global_remaining = fetcher->max_global_connections - current_global;
    uv_mutex_unlock(&fetcher->mutex);

    if (to_start > global_remaining) {
        to_start = global_remaining;
    }

    if (to_start <= 0) {
        uv_mutex_unlock(&fetcher->attempt_table_mutex);
        return;
    }

    /* Collect peers to connect to while holding lock */
    struct sockaddr_storage peer_addrs[20];  /* Max concurrent per hash is 20 */
    int peers_to_connect = 0;

    for (int i = 0; i < to_start && attempt->peers_tried < attempt->total_peer_count; i++) {
        int peer_idx = attempt->peers_tried;
        peer_addrs[peers_to_connect] = attempt->available_peers[peer_idx];
        attempt->peers_tried++;
        peers_to_connect++;
    }

    uint8_t info_hash_copy[20];
    memcpy(info_hash_copy, attempt->info_hash, 20);

    uv_mutex_unlock(&fetcher->attempt_table_mutex);

    /* Start connections without holding lock */
    for (int i = 0; i < peers_to_connect; i++) {
        peer_connection_t *peer = create_peer_connection(
            fetcher, info_hash_copy, (struct sockaddr *)&peer_addrs[i]
        );

        if (peer) {
            uv_mutex_lock(&fetcher->attempt_table_mutex);
            /* Re-lookup attempt in case it was removed */
            infohash_attempt_t *current_attempt = lookup_attempt(fetcher, info_hash_copy);
            if (current_attempt) {
                current_attempt->active_connections++;
            }
            uv_mutex_unlock(&fetcher->attempt_table_mutex);
        }
    }
}

/* Remove attempt entry (thread-safe) */
static void remove_attempt(metadata_fetcher_t *fetcher, const uint8_t *info_hash) {
    uv_mutex_lock(&fetcher->attempt_table_mutex);

    uint32_t bucket = hash_infohash(info_hash);
    infohash_attempt_t **prev_ptr = &fetcher->attempt_table[bucket];
    infohash_attempt_t *attempt = fetcher->attempt_table[bucket];

    while (attempt) {
        if (memcmp(attempt->info_hash, info_hash, 20) == 0) {
            *prev_ptr = attempt->next;

            /* Free peer queue arrays if allocated */
            if (attempt->available_peers) {
                free(attempt->available_peers);
            }
            if (attempt->available_peer_lens) {
                free(attempt->available_peer_lens);
            }
            free(attempt);
            uv_mutex_unlock(&fetcher->attempt_table_mutex);
            return;
        }
        prev_ptr = &attempt->next;
        attempt = attempt->next;
    }

    uv_mutex_unlock(&fetcher->attempt_table_mutex);
}

/* Update attempt statistics based on peer connection close reason */
static void update_attempt_statistics(infohash_attempt_t *attempt, peer_connection_t *peer, const char *close_reason) {
    if (!attempt || !peer || !close_reason) {
        return;
    }

    attempt->last_attempt_time = time(NULL);
    attempt->total_connections_tried++;

    /* Track furthest state reached */
    if (peer->state > attempt->furthest_state) {
        attempt->furthest_state = peer->state;
    }

    /* Classify failure reason */
    if (strstr(close_reason, "timeout")) {
        attempt->connections_timeout++;
    } else if (strstr(close_reason, "connect") || strstr(close_reason, "connection")) {
        attempt->connections_failed++;
    } else if (strstr(close_reason, "handshake") || strstr(close_reason, "info_hash mismatch")) {
        attempt->handshake_failed++;
    } else if (strstr(close_reason, "no extension support") || strstr(close_reason, "ut_metadata")) {
        attempt->no_metadata_support++;
    } else if (strstr(close_reason, "rejected")) {
        attempt->metadata_rejected++;
    } else if (strstr(close_reason, "hash")) {
        attempt->hash_mismatch++;
    }
}

/* Classify failure for retry decision */
static failure_classification_t classify_failure(infohash_attempt_t *attempt) {
    if (!attempt) {
        return FAILURE_PERMANENT;
    }

    /* No peers at all: discard */
    if (attempt->peer_count_at_start == 0) {
        return FAILURE_NO_PEERS;
    }

    /* All peers rejected or don't support metadata: permanent */
    if (attempt->no_metadata_support >= attempt->total_connections_tried) {
        return FAILURE_PERMANENT;
    }

    /* Hash mismatch: permanent (corrupted data) */
    if (attempt->hash_mismatch > 0) {
        return FAILURE_PERMANENT;
    }

    /* Timeouts or connection failures: retriable */
    if (attempt->connections_timeout > 0 || attempt->connections_failed > 0) {
        return FAILURE_RETRIABLE;
    }

    /* Handshake failures: retriable (might be temporary peer issues) */
    if (attempt->handshake_failed > 0) {
        return FAILURE_RETRIABLE;
    }

    /* Default: don't retry */
    return FAILURE_PERMANENT;
}

/* Handle infohash-level failure and potentially add to retry queue */
static void handle_infohash_failure(metadata_fetcher_t *fetcher, infohash_attempt_t *attempt) {
    if (!fetcher || !attempt) {
        return;
    }

    /* If retry is disabled, discard all failures immediately */
    if (!fetcher->retry_enabled) {
        uv_mutex_lock(&fetcher->mutex);
        fetcher->discarded_failures++;
        uv_mutex_unlock(&fetcher->mutex);
        return;
    }

    /* Retry is enabled - classify and handle accordingly */
    failure_classification_t classification = classify_failure(attempt);

    switch (classification) {
        case FAILURE_RETRIABLE:
            /* Add to retry queue */
            if (fetcher->retry_queue) {
                if (retry_queue_add(fetcher->retry_queue, attempt->info_hash, 0) == 0) {
                    uv_mutex_lock(&fetcher->mutex);
                    fetcher->retry_queue_added++;
                    fetcher->retriable_failures++;
                    uv_mutex_unlock(&fetcher->mutex);
                }
            }
            break;

        case FAILURE_PERMANENT:
            /* Discard permanently */
            uv_mutex_lock(&fetcher->mutex);
            fetcher->permanent_failures++;
            uv_mutex_unlock(&fetcher->mutex);
            break;

        case FAILURE_NO_PEERS:
            /* Discard immediately */
            uv_mutex_lock(&fetcher->mutex);
            fetcher->no_peer_discards++;
            uv_mutex_unlock(&fetcher->mutex);
            break;
    }
}

/* Free metadata result */
void free_metadata_result(metadata_result_t *result) {
    if (!result) {
        return;
    }

    if (result->name) {
        free(result->name);
    }

    if (result->files) {
        for (int i = 0; i < result->num_files; i++) {
            if (result->files[i].path) {
                free((void *)result->files[i].path);
            }
        }
        free(result->files);
    }

    free(result);
}
