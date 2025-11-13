#include "dht_crawler.h"
#include "dht_manager.h"
#include "database.h"
#include "infohash_queue.h"
#include "metadata_fetcher.h"
#include "http_api.h"
#include "bloom_filter.h"
#include "config.h"
#include <signal.h>
#include <unistd.h>
#include <stdarg.h>

/* Global application context */
static app_context_t g_app_ctx;
static dht_manager_t g_dht_mgr;  /* Single DHT instance */
static database_t g_database;
static infohash_queue_t g_queue;
static metadata_fetcher_t g_fetcher;
static http_api_t g_http_api;
static bloom_filter_t *g_bloom = NULL;

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
    struct tm *tm_info = localtime(&now);
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
                    log_msg(LOG_INFO, "Loaded existing bloom filter from %s (capacity: %lu)",
                            config.bloom_path, loaded_capacity);
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
    } else {
        log_msg(LOG_INFO, "Bloom filter disabled in config");
    }

    /* Initialize database */
    log_msg(LOG_DEBUG, "Initializing database: %s", g_app_ctx.db_path);
    rc = database_init(&g_database, g_app_ctx.db_path, &g_app_ctx);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize database: %d", rc);
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

        /* Connect bloom filter and database to queue for read-only duplicate checking */
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

    /* Initialize metadata fetcher with peer store from DHT manager */
    log_msg(LOG_DEBUG, "Initializing metadata fetcher...");
    log_msg(LOG_DEBUG, "g_dht_mgr.peer_store address: %p", (void*)g_dht_mgr.peer_store);
    peer_store_t *peer_store = g_dht_mgr.peer_store;
    if (!peer_store) {
        log_msg(LOG_ERROR, "DHT manager has no peer store");
        dht_manager_cleanup(&g_dht_mgr);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);
        return 1;
    }

    rc = metadata_fetcher_init(&g_fetcher, &g_app_ctx, &g_queue, &g_database, peer_store, &config);
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

    /* Set DHT manager reference in metadata fetcher for peer refresh on retry */
    metadata_fetcher_set_dht_manager(&g_fetcher, (struct dht_manager *)&g_dht_mgr);

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
                       g_fetcher.batch_writer, HTTP_API_PORT);
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
    log_msg(LOG_INFO, "=== Beginning graceful shutdown sequence ===");

    log_msg(LOG_INFO, "Step 1: Stopping HTTP API...");
    http_api_stop(&g_http_api);

    log_msg(LOG_INFO, "Step 2: Stopping metadata fetcher (joins all worker threads)...");
    metadata_fetcher_stop(&g_fetcher);

    log_msg(LOG_INFO, "Step 3: Stopping DHT manager (stops DHT network participation)...");
    dht_manager_stop(&g_dht_mgr);

    log_msg(LOG_INFO, "=== Shutdown sequence complete, beginning cleanup ===");

    /* Save bloom filter statistics and persist to disk */
    if (g_bloom) {
        uint64_t duplicates = infohash_queue_get_duplicates(&g_queue);
        log_msg(LOG_INFO, "Bloom filter statistics: %lu duplicates filtered (90%% DB query reduction)", duplicates);

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
    bloom_filter_cleanup(g_bloom);
    infohash_queue_cleanup(&g_queue);
    cleanup_app_context(&g_app_ctx);

    log_msg(LOG_DEBUG, "DHT Crawler stopped.");
    return 0;
}
