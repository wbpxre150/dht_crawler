#include "dht_crawler.h"
#include "dht_manager.h"
#include "database.h"
#include "infohash_queue.h"
#include "metadata_fetcher.h"
#include "http_api.h"
#include "bloom_filter.h"
#include "porn_filter.h"
#include "torrent_search.h"
#include "config.h"
#include "supervisor.h"
#include "batch_writer.h"
#include "refresh_thread.h"
#include <signal.h>
#include <unistd.h>
#include <stdarg.h>

/* Global application context */
static app_context_t g_app_ctx;
static dht_manager_t g_dht_mgr;  /* Single DHT instance (old architecture) */
static database_t g_database;
static infohash_queue_t g_queue;
static metadata_fetcher_t g_fetcher;
static http_api_t g_http_api;
static bloom_filter_t *g_bloom = NULL;

/* Stage 6: Thread tree architecture globals */
static supervisor_t *g_supervisor = NULL;
static batch_writer_t *g_batch_writer = NULL;  /* Shared batch writer for thread trees */
static refresh_thread_t *g_refresh_thread = NULL;  /* Refresh thread for /refresh endpoint */

/* Signal handler for graceful shutdown */
static void signal_handler(int signum) {
    log_msg(LOG_DEBUG, "Received signal %d, shutting down...", signum);
    g_app_ctx.running = 0;
    
    /* Stop the event loop to break out of UV_RUN_DEFAULT */
    if (g_app_ctx.loop) {
        uv_stop(g_app_ctx.loop);
    }
}

/* Logging function with timestamps */
void log_msg(log_level_t level, const char *format, ...) {
    if (level < g_app_ctx.log_level) {
        return;
    }

    const char *level_str[] = {"DEBUG", "INFO", "WARN", "ERROR"};
    time_t now = time(NULL);
    struct tm tm_info_buf;
    struct tm *tm_info = localtime_r(&now, &tm_info_buf);  /* Thread-safe version */
    char time_buf[64];

    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);

    fprintf(stderr, "[%s] [%s] ", time_buf, level_str[level]);

    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);

    fprintf(stderr, "\n");
    fflush(stderr);
}

/* Initialize application context */
void init_app_context(app_context_t *ctx) {
    memset(ctx, 0, sizeof(app_context_t));
    ctx->running = 1;
    ctx->log_level = LOG_INFO;
    ctx->dht_port = DHT_PORT;
    ctx->db_path = "data/torrents.db";
    ctx->start_time = time(NULL);

    /* Generate random node ID */
    FILE *urandom = fopen("/dev/urandom", "rb");
    if (urandom) {
        fread(ctx->node_id, 1, NODE_ID_LENGTH, urandom);
        fclose(urandom);
    }

    /* Create libuv event loop */
    ctx->loop = uv_default_loop();
}

/* Cleanup application context */
void cleanup_app_context(app_context_t *ctx) {
    if (ctx->loop) {
        uv_loop_close(ctx->loop);
        ctx->loop = NULL;
    }
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;
    int rc;

    log_msg(LOG_DEBUG, "DHT Crawler v%s starting...", DHT_CRAWLER_VERSION);

    /* Initialize application context */
    init_app_context(&g_app_ctx);

    /* Load configuration from config.ini */
    crawler_config_t config;
    config_init_defaults(&config);
    if (config_load_file(&config, "config.ini") == 0) {
        /* Apply log level from config */
        g_app_ctx.log_level = config.log_level;
        log_msg(LOG_DEBUG, "Loaded configuration from config.ini (log_level=%d)", config.log_level);
    } else {
        log_msg(LOG_WARN, "Could not load config.ini, using defaults");
    }

    /* Set up signal handlers */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Initialize infohash queue */
    log_msg(LOG_DEBUG, "Initializing infohash queue (capacity: %d)...", INFOHASH_QUEUE_SIZE);
    rc = infohash_queue_init(&g_queue, INFOHASH_QUEUE_SIZE);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize infohash queue: %d", rc);
        return 1;
    }

    /* Initialize bloom filter for duplicate detection */
    if (config.bloom_enabled) {
        log_msg(LOG_DEBUG, "Initializing bloom filter (capacity: %lu, error rate: %.3f%%)...",
                config.bloom_capacity, config.bloom_error_rate * 100.0);
        g_bloom = bloom_filter_init(config.bloom_capacity, config.bloom_error_rate);
        if (!g_bloom) {
            log_msg(LOG_ERROR, "Failed to initialize bloom filter");
            infohash_queue_cleanup(&g_queue);
            return 1;
        }

        /* Try to load existing bloom filter if persistence is enabled */
        if (config.bloom_persist) {
            bloom_filter_t *loaded_bloom = bloom_filter_load(config.bloom_path);
            if (loaded_bloom) {
                /* Check if loaded filter has same capacity as configured */
                uint64_t loaded_capacity = 0;
                double loaded_error_rate = 0.0;
                bloom_filter_stats(loaded_bloom, &loaded_capacity, &loaded_error_rate, NULL);

                if (loaded_capacity == config.bloom_capacity &&
                    loaded_error_rate == config.bloom_error_rate) {
                    bloom_filter_cleanup(g_bloom);
                    g_bloom = loaded_bloom;
                } else {
                    log_msg(LOG_WARN, "Bloom filter config mismatch - loaded: capacity=%lu error=%.3f%%, "
                            "config: capacity=%lu error=%.3f%%. Starting fresh.",
                            loaded_capacity, loaded_error_rate * 100.0,
                            config.bloom_capacity, config.bloom_error_rate * 100.0);
                    bloom_filter_cleanup(loaded_bloom);
                    /* Keep the newly initialized filter with correct config */
                }
            }
        }
    }

    /* Initialize pornography content filter (if enabled) */
    if (config.porn_filter_enabled) {
        log_msg(LOG_DEBUG, "Initializing porn filter (keyword file: %s)...", config.porn_filter_keyword_file);
        rc = porn_filter_init(config.porn_filter_keyword_file);
        if (rc != 0) {
            log_msg(LOG_WARN, "Failed to initialize porn filter, continuing without filtering");
        } else {
            /* Set thresholds from config */
            porn_filter_set_thresholds(config.porn_filter_keyword_threshold,
                                      config.porn_filter_regex_threshold,
                                      config.porn_filter_heuristic_threshold);
            log_msg(LOG_DEBUG, "Porn filter enabled (thresholds: keyword=%d, regex=%d, heuristic=%d)",
                    config.porn_filter_keyword_threshold,
                    config.porn_filter_regex_threshold,
                    config.porn_filter_heuristic_threshold);
        }
    } else {
        log_msg(LOG_DEBUG, "Porn filter disabled");
    }

    /* Initialize torrent search module for title extraction */
    log_msg(LOG_DEBUG, "Initializing torrent search module...");
    rc = torrent_search_init("torrent_search_keywords.txt");
    if (rc != 0) {
        log_msg(LOG_WARN, "Failed to initialize torrent search module, title extraction will be basic");
    }

    /* Initialize database */
    log_msg(LOG_DEBUG, "Initializing database: %s", g_app_ctx.db_path);
    rc = database_init(&g_database, g_app_ctx.db_path, &g_app_ctx);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize database: %d", rc);
        porn_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Create database schema */
    rc = database_create_schema(&g_database);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to create database schema: %d", rc);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Connect bloom filter to database for write tracking (CRITICAL for correctness)
     * Bloom filter is updated ONLY after successful database writes, not at discovery time.
     * This prevents data loss from failed metadata fetches by allowing retries. */
    if (g_bloom) {
        database_set_bloom(&g_database, g_bloom);
    }

    /*******************************************************************
     * Stage 6: Branch based on architecture selection
     *******************************************************************/
    if (config.use_thread_trees) {
        /*******************************************************************
         * NEW ARCHITECTURE: Thread Tree Supervisor
         *******************************************************************/
        log_msg(LOG_DEBUG, "=== Starting Thread Tree Architecture ===");

        /* Create shared batch writer for all trees */
        g_batch_writer = batch_writer_init(&g_database, config.batch_size,
                                            config.flush_interval, g_app_ctx.loop);
        if (!g_batch_writer) {
            log_msg(LOG_ERROR, "Failed to create batch writer");
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        /* Connect bloom filter to batch writer */
        if (g_bloom && config.bloom_persist) {
            batch_writer_set_bloom(g_batch_writer, g_bloom, config.bloom_path);
        }

        /* Create supervisor config */
        supervisor_config_t sup_config = {
            .max_trees = config.num_trees,
            .use_keyspace_partitioning = config.use_keyspace_partitioning,
            .batch_writer = g_batch_writer,
            .bloom_filter = g_bloom,
            .num_find_node_workers = config.tree_find_node_workers,
            .num_bep51_workers = config.tree_bep51_workers,
            .num_get_peers_workers = config.tree_get_peers_workers,
            .num_metadata_workers = config.tree_metadata_workers,
            /* Stage 2 settings (Global Bootstrap - NEW) */
            .global_bootstrap_target = config.global_bootstrap_target,
            .global_bootstrap_timeout_sec = config.global_bootstrap_timeout_sec,
            .global_bootstrap_workers = config.global_bootstrap_workers,
            .per_tree_sample_size = config.per_tree_sample_size,
            /* Stage 3 settings (BEP51) */
            .infohash_queue_capacity = config.tree_infohash_queue_capacity,
            .bep51_query_interval_ms = config.tree_bep51_query_interval_ms,
            /* Stage 4 settings (get_peers) */
            .peers_queue_capacity = config.tree_peers_queue_capacity,
            .get_peers_timeout_ms = config.tree_get_peers_timeout_ms,
            /* Find_node throttling settings */
            .infohash_pause_threshold = config.tree_infohash_pause_threshold,
            .infohash_resume_threshold = config.tree_infohash_resume_threshold,
            /* Get_peers throttling settings */
            .peers_pause_threshold = config.tree_peers_pause_threshold,
            .peers_resume_threshold = config.tree_peers_resume_threshold,
            /* Stage 5 settings */
            .tcp_connect_timeout_ms = config.tree_tcp_connect_timeout_ms,
            /* Bloom-based respawn settings */
            .max_bloom_duplicate_rate = config.tree_max_bloom_duplicate_rate,
            .bloom_check_interval_sec = config.tree_bloom_check_interval_sec,
            .bloom_check_sample_size = config.tree_bloom_check_sample_size,
            .bloom_grace_period_sec = config.tree_bloom_grace_period_sec,
            .bloom_min_lifetime_minutes = config.tree_bloom_min_lifetime_minutes
        };

        g_supervisor = supervisor_create(&sup_config);
        if (!g_supervisor) {
            log_msg(LOG_ERROR, "Failed to create supervisor");
            batch_writer_cleanup(g_batch_writer);
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        /* Start supervisor (spawns all trees) */
        supervisor_start(g_supervisor);

        /* Create and start refresh thread */
        log_msg(LOG_DEBUG, "Creating refresh thread...");
        refresh_thread_config_t refresh_config = {
            .bootstrap_sample_size = config.refresh_bootstrap_sample_size,
            .routing_table_target = config.refresh_routing_table_target,
            .ping_worker_count = config.refresh_ping_workers,
            .find_node_worker_count = config.refresh_find_node_workers,
            .get_peers_worker_count = config.refresh_get_peers_workers,
            .request_queue_capacity = config.refresh_request_queue_capacity,
            .get_peers_timeout_ms = config.refresh_get_peers_timeout_ms,
            .max_iterations = config.refresh_max_iterations
        };

        g_refresh_thread = refresh_thread_create(&refresh_config,
                                                  g_supervisor->shared_node_pool,
                                                  g_dht_mgr.refresh_query_store);
        if (!g_refresh_thread) {
            log_msg(LOG_ERROR, "Failed to create refresh thread");
            supervisor_stop(g_supervisor);
            supervisor_destroy(g_supervisor);
            batch_writer_cleanup(g_batch_writer);
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        rc = refresh_thread_start(g_refresh_thread);
        if (rc != 0) {
            log_msg(LOG_ERROR, "Failed to start refresh thread");
            refresh_thread_destroy(g_refresh_thread);
            supervisor_stop(g_supervisor);
            supervisor_destroy(g_supervisor);
            batch_writer_cleanup(g_batch_writer);
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        /* Initialize HTTP API (minimal - no DHT manager in thread tree mode) */
        log_msg(LOG_DEBUG, "Initializing HTTP API for thread tree mode...");
        rc = http_api_init(&g_http_api, &g_app_ctx, &g_database, NULL,
                           g_batch_writer, NULL, HTTP_API_PORT);
        if (rc != 0) {
            log_msg(LOG_ERROR, "Failed to initialize HTTP API: %d", rc);
            refresh_thread_request_shutdown(g_refresh_thread);
            refresh_thread_destroy(g_refresh_thread);
            supervisor_stop(g_supervisor);
            supervisor_destroy(g_supervisor);
            batch_writer_cleanup(g_batch_writer);
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        /* Set supervisor reference for stats */
        http_api_set_supervisor(&g_http_api, g_supervisor);

        /* Set refresh thread for /refresh endpoint */
        http_api_set_refresh_thread(&g_http_api, g_refresh_thread);

        /* Start HTTP API */
        log_msg(LOG_DEBUG, "Starting HTTP API server on port %d...", HTTP_API_PORT);
        rc = http_api_start(&g_http_api);
        if (rc != 0) {
            log_msg(LOG_ERROR, "Failed to start HTTP API: %d", rc);
            supervisor_stop(g_supervisor);
            supervisor_destroy(g_supervisor);
            http_api_cleanup(&g_http_api);
            batch_writer_cleanup(g_batch_writer);
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        log_msg(LOG_DEBUG, "DHT crawler (thread tree mode) is running.");
        log_msg(LOG_DEBUG, "  Trees: %d, Workers per tree: BEP51=%d, get_peers=%d, metadata=%d",
                config.num_trees, config.tree_bep51_workers,
                config.tree_get_peers_workers, config.tree_metadata_workers);
        log_msg(LOG_DEBUG, "HTTP API available at http://localhost:%d/", HTTP_API_PORT);
        log_msg(LOG_DEBUG, "Press Ctrl+C to stop.");

        /* Wait for shutdown signal (blocking) */
        while (g_app_ctx.running) {
            sleep(1);
        }

        /* Graceful shutdown sequence for thread tree mode */
        log_msg(LOG_DEBUG, "=== Beginning thread tree shutdown sequence ===");

        log_msg(LOG_DEBUG, "Step 1: Stopping HTTP API...");
        http_api_stop(&g_http_api);

        log_msg(LOG_DEBUG, "Step 2: Stopping refresh thread...");
        if (g_refresh_thread) {
            refresh_thread_request_shutdown(g_refresh_thread);
        }

        log_msg(LOG_DEBUG, "Step 3: Stopping supervisor (stops all trees)...");
        supervisor_stop(g_supervisor);

        log_msg(LOG_DEBUG, "Step 4: Flushing batch writer...");
        batch_writer_flush(g_batch_writer);

        log_msg(LOG_DEBUG, "=== Thread tree shutdown complete, beginning cleanup ===");

        /* Save bloom filter */
        if (g_bloom && config.bloom_persist) {
            log_msg(LOG_DEBUG, "Saving bloom filter to %s...", config.bloom_path);
            if (bloom_filter_save(g_bloom, config.bloom_path) == 0) {
                log_msg(LOG_DEBUG, "Bloom filter saved successfully");
            } else {
                log_msg(LOG_WARN, "Failed to save bloom filter");
            }
        }

        /* Cleanup */
        http_api_cleanup(&g_http_api);
        if (g_refresh_thread) {
            refresh_thread_destroy(g_refresh_thread);
        }
        supervisor_destroy(g_supervisor);
        batch_writer_cleanup(g_batch_writer);
        database_cleanup(&g_database);
        torrent_search_cleanup();
        porn_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        cleanup_app_context(&g_app_ctx);

        log_msg(LOG_DEBUG, "DHT Crawler (thread tree mode) stopped.");
        return 0;
    }

    /*******************************************************************
     * OLD ARCHITECTURE: Single DHT instance with metadata fetcher
     * (Kept for backward compatibility - set use_thread_trees=0)
     *******************************************************************/
    log_msg(LOG_DEBUG, "=== Starting Old Architecture (use_thread_trees=0) ===");

    /* Connect bloom filter and database to queue for read-only duplicate checking */
    if (g_bloom) {
        infohash_queue_set_bloom(&g_queue, g_bloom);
        infohash_queue_set_database(&g_queue, (struct database *)&g_database);
        log_msg(LOG_DEBUG, "Bloom filter duplicate detection enabled");
    } else {
        /* If bloom filter is disabled, still need to connect database for duplicate checking */
        infohash_queue_set_database(&g_queue, (struct database *)&g_database);
        log_msg(LOG_DEBUG, "Bloom filter disabled - using database-only duplicate detection");
    }

    /* Initialize DHT manager */
    log_msg(LOG_DEBUG, "Initializing DHT manager...");
    rc = dht_manager_init(&g_dht_mgr, &g_app_ctx, &g_queue, &config);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize DHT manager: %d", rc);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Initialize metadata fetcher */
    log_msg(LOG_DEBUG, "Initializing metadata fetcher...");

    rc = metadata_fetcher_init(&g_fetcher, &g_app_ctx, &g_queue, &g_database, &config);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize metadata fetcher: %d", rc);
        dht_manager_cleanup(&g_dht_mgr);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Set metadata fetcher reference in DHT manager for statistics */
    dht_manager_set_metadata_fetcher(&g_dht_mgr, &g_fetcher);

    /* Connect bloom filter to batch writer for persistence after each batch write
     * This ensures bloom filter on disk stays synchronized with database */
    if (g_bloom && config.bloom_persist) {
        metadata_fetcher_set_bloom_filter(&g_fetcher, g_bloom, config.bloom_path);
    }

    /* Start DHT manager */
    log_msg(LOG_DEBUG, "Starting DHT crawler...");
    rc = dht_manager_start(&g_dht_mgr);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start DHT manager: %d", rc);
        dht_manager_cleanup(&g_dht_mgr);
        metadata_fetcher_cleanup(&g_fetcher);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Start metadata fetcher */
    log_msg(LOG_DEBUG, "Starting metadata fetcher...");
    rc = metadata_fetcher_start(&g_fetcher);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start metadata fetcher: %d", rc);
        dht_manager_stop(&g_dht_mgr);
        dht_manager_cleanup(&g_dht_mgr);
        metadata_fetcher_cleanup(&g_fetcher);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Initialize HTTP API */
    log_msg(LOG_DEBUG, "Initializing HTTP API...");
    rc = http_api_init(&g_http_api, &g_app_ctx, &g_database, &g_dht_mgr,
                       g_fetcher.batch_writer, &g_fetcher, HTTP_API_PORT);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize HTTP API: %d", rc);
        dht_manager_stop(&g_dht_mgr);
        metadata_fetcher_stop(&g_fetcher);
        dht_manager_cleanup(&g_dht_mgr);
        metadata_fetcher_cleanup(&g_fetcher);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    /* Start HTTP API */
    log_msg(LOG_DEBUG, "Starting HTTP API server on port %d...", HTTP_API_PORT);
    rc = http_api_start(&g_http_api);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to start HTTP API: %d", rc);
        dht_manager_stop(&g_dht_mgr);
        metadata_fetcher_stop(&g_fetcher);
        dht_manager_cleanup(&g_dht_mgr);
        metadata_fetcher_cleanup(&g_fetcher);
        http_api_cleanup(&g_http_api);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    log_msg(LOG_DEBUG, "DHT crawler is running.");
    log_msg(LOG_DEBUG, "HTTP API available at http://localhost:%d/", HTTP_API_PORT);
    log_msg(LOG_DEBUG, "Press Ctrl+C to stop.");

    /* Main event loop - blocks until uv_stop() is called or no active handles remain */
    log_msg(LOG_DEBUG, "Starting main event loop (loop=%p)", g_app_ctx.loop);
    uv_run(g_app_ctx.loop, UV_RUN_DEFAULT);
    log_msg(LOG_DEBUG, "Main event loop exited");

    /* Cleanup */
    log_msg(LOG_DEBUG, "=== Beginning graceful shutdown sequence ===");

    log_msg(LOG_DEBUG, "Step 1: Stopping HTTP API...");
    http_api_stop(&g_http_api);

    log_msg(LOG_DEBUG, "Step 2: Stopping metadata fetcher (joins all worker threads)...");
    metadata_fetcher_stop(&g_fetcher);

    log_msg(LOG_DEBUG, "Step 3: Stopping DHT manager (stops DHT network participation)...");
    dht_manager_stop(&g_dht_mgr);

    log_msg(LOG_DEBUG, "=== Shutdown sequence complete, beginning cleanup ===");

    /* Save bloom filter statistics and persist to disk */
    if (g_bloom) {
        /* Save bloom filter to disk if persistence is enabled */
        if (config.bloom_persist) {
            log_msg(LOG_DEBUG, "Saving bloom filter to %s...", config.bloom_path);
            if (bloom_filter_save(g_bloom, config.bloom_path) == 0) {
                log_msg(LOG_DEBUG, "Bloom filter saved successfully");
            } else {
                log_msg(LOG_WARN, "Failed to save bloom filter");
            }
        }
    }

    /* Now proceed with cleanup - all handles are guaranteed to be closed */
    http_api_cleanup(&g_http_api);
    dht_manager_cleanup(&g_dht_mgr);
    metadata_fetcher_cleanup(&g_fetcher);
    database_cleanup(&g_database);
    torrent_search_cleanup();
    porn_filter_cleanup();
    bloom_filter_cleanup(g_bloom);
    infohash_queue_cleanup(&g_queue);
    cleanup_app_context(&g_app_ctx);

    log_msg(LOG_DEBUG, "DHT Crawler stopped.");
    return 0;
}
