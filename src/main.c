#include "dht_crawler.h"
#include "dht_manager.h"
#include "database.h"
#include "infohash_queue.h"
#include "metadata_fetcher.h"
#include "http_api.h"
#include "bloom_filter.h"
#include "porn_filter.h"
#include "porn_filter_cleanup.h"
#include "language_filter.h"
#include "torrent_search.h"
#include "config.h"
#include "supervisor.h"
#include "batch_writer.h"
#include "refresh_thread.h"
#include "refresh_query.h"
#include <signal.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/resource.h>
#include <errno.h>
#include <limits.h>
#include <libgen.h>

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
static refresh_query_store_t *g_refresh_query_store = NULL;  /* Shared refresh query store */

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

/* Ensure directory exists, create if needed */
static int ensure_directory_exists(const char *path) {
    struct stat st;

    /* Check if path exists */
    if (stat(path, &st) == 0) {
        /* Path exists, check if it's a directory */
        if (S_ISDIR(st.st_mode)) {
            return 0;  /* Success - directory exists */
        } else {
            log_msg(LOG_ERROR, "Path exists but is not a directory: %s", path);
            return -1;
        }
    }

    /* Path doesn't exist, try to create it */
    if (mkdir(path, 0755) == 0) {
        log_msg(LOG_DEBUG, "Created directory: %s", path);
        return 0;
    }

    /* Failed to create directory */
    if (errno == EEXIST) {
        /* Race condition - another thread/process created it */
        return 0;
    }

    log_msg(LOG_ERROR, "Failed to create directory %s (errno=%d: %s)",
            path, errno, strerror(errno));
    return -1;
}

/* Ensure parent directory of a file path exists */
static int ensure_parent_directory_exists(const char *filepath) {
    char *path_copy = strdup(filepath);
    if (!path_copy) {
        log_msg(LOG_ERROR, "Failed to allocate memory for path copy");
        return -1;
    }

    char *dir = dirname(path_copy);
    int ret = ensure_directory_exists(dir);
    free(path_copy);
    return ret;
}

/* Check that the filesystem containing tmp_path has enough free space to hold
 * a copy of db_path (plus its WAL file if present).
 * Returns 0 if space is sufficient, -1 if not. On statvfs failure the check
 * is skipped with a warning and 0 is returned so the operation can proceed. */
static int check_disk_space(const char *db_path, const char *tmp_path) {
    struct stat st;
    off_t required = 0;

    if (stat(db_path, &st) == 0)
        required += st.st_size;

    char wal_path[PATH_MAX];
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    if (stat(wal_path, &st) == 0)
        required += st.st_size;

    char *tmp_copy = strdup(tmp_path);
    if (!tmp_copy) return 0;
    char *dir = dirname(tmp_copy);

    struct statvfs vfs;
    int rc = statvfs(dir, &vfs);
    free(tmp_copy);

    if (rc != 0) {
        log_msg(LOG_WARN, "Could not check available disk space: %s", strerror(errno));
        return 0;
    }

    uint64_t available = (uint64_t)vfs.f_bavail * (uint64_t)vfs.f_bsize;
    log_msg(LOG_INFO, "Disk space: need %lld bytes, have %llu bytes available",
            (long long)required, (unsigned long long)available);

    if (available < (uint64_t)required) {
        log_msg(LOG_ERROR, "Insufficient disk space for operation: need %lld bytes, only %llu available",
                (long long)required, (unsigned long long)available);
        return -1;
    }
    return 0;
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
    int rc;
    bool rebuild_bloom = false;
    bool porn_filter_update = false;
    bool compact_db = false;
    bool check_db = false;
    bool recover_db = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [OPTIONS]\n\n", argv[0]);
            printf("Options:\n");
            printf("  --rebuild-bloom-filter   Rebuild the bloom filter from the existing database\n");
            printf("  --porn-filter-update     Re-scan the database and remove entries matching the porn filter\n");
            printf("  --compact                Compact the database by writing a fresh copy and replacing the original\n");
            printf("                           Can be used alone or combined with --porn-filter-update\n");
            printf("  --check-database         Run an integrity check on the database and report any errors\n");
            printf("  --recover-database       Recover the database by copying all readable rows to a fresh\n");
            printf("                           database, then replacing the original\n");
            printf("  --help, -h               Show this help message and exit\n");
            return 0;
        } else if (strcmp(argv[i], "--rebuild-bloom-filter") == 0) {
            rebuild_bloom = true;
        } else if (strcmp(argv[i], "--porn-filter-update") == 0) {
            porn_filter_update = true;
        } else if (strcmp(argv[i], "--compact") == 0) {
            compact_db = true;
        } else if (strcmp(argv[i], "--check-database") == 0) {
            check_db = true;
        } else if (strcmp(argv[i], "--recover-database") == 0) {
            recover_db = true;
        }
    }

    /* Increase file descriptor limit to support max_concurrent_connections
     * The default soft limit is often 1024, which is too low for high-concurrency crawling.
     * We raise it to the hard limit (typically 524288 on modern systems). */
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        rlim_t old_soft = rlim.rlim_cur;
        /* Set soft limit to hard limit */
        rlim.rlim_cur = rlim.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &rlim) == 0) {
            log_msg(LOG_DEBUG, "Increased file descriptor limit from %lu to %lu",
                   (unsigned long)old_soft, (unsigned long)rlim.rlim_cur);
        } else {
            log_msg(LOG_WARN, "Failed to increase file descriptor limit: %s", strerror(errno));
        }
    } else {
        log_msg(LOG_WARN, "Failed to get file descriptor limit: %s", strerror(errno));
    }

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

    /* Rebuild bloom filter from database if requested */
    if (rebuild_bloom) {
        if (!config.bloom_enabled) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: bloom filter is disabled in config.ini");
            return 1;
        }
        if (!config.bloom_persist) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: bloom filter persistence is disabled in config.ini (bloom_persist=0)");
            return 1;
        }
        if (ensure_parent_directory_exists(config.bloom_path) != 0) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to create bloom filter directory");
            return 1;
        }

        log_msg(LOG_INFO, "Rebuilding bloom filter from database %s ...", g_app_ctx.db_path);
        bloom_filter_t *bloom = bloom_filter_init(config.bloom_capacity, config.bloom_error_rate);
        if (!bloom) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to allocate bloom filter");
            return 1;
        }

        int added = database_rebuild_bloom(g_app_ctx.db_path, bloom);
        if (added < 0) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to read database");
            bloom_filter_cleanup(bloom);
            return 1;
        }

        log_msg(LOG_INFO, "Added %d infohashes to bloom filter", added);

        if (bloom_filter_save(bloom, config.bloom_path) != 0) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to save bloom filter to %s", config.bloom_path);
            bloom_filter_cleanup(bloom);
            return 1;
        }

        bloom_filter_cleanup(bloom);
        log_msg(LOG_INFO, "Bloom filter rebuilt and saved to %s.", config.bloom_path);
        return 0;
    }

    /*******************************************************************
     * --check-database: run integrity_check PRAGMA and report results.
     *******************************************************************/
    if (check_db) {
        log_msg(LOG_INFO, "Checking database integrity: %s", g_app_ctx.db_path);
        int nerrors = database_integrity_check(g_app_ctx.db_path);
        cleanup_app_context(&g_app_ctx);
        if (nerrors < 0) {
            log_msg(LOG_ERROR, "--check-database: failed to open or query database.");
            return 1;
        }
        if (nerrors == 0) {
            log_msg(LOG_INFO, "--check-database: database is clean.");
            return 0;
        }
        log_msg(LOG_ERROR, "--check-database: %d integrity error(s) found.", nerrors);
        return 1;
    }

    /*******************************************************************
     * --recover-database: copy all readable rows into a fresh database,
     * then replace the original.
     *******************************************************************/
    if (recover_db) {
        char tmp_path[PATH_MAX];
        snprintf(tmp_path, sizeof(tmp_path), "%s.recover.tmp", g_app_ctx.db_path);

        if (check_disk_space(g_app_ctx.db_path, tmp_path) != 0) {
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        unlink(tmp_path);

        log_msg(LOG_INFO, "Recovering database %s into %s ...", g_app_ctx.db_path, tmp_path);
        int nrows = database_recover(g_app_ctx.db_path, tmp_path);

        cleanup_app_context(&g_app_ctx);

        if (nrows < 0) {
            log_msg(LOG_ERROR, "--recover-database: recovery failed; original database unchanged.");
            unlink(tmp_path);
            return 1;
        }

        if (rename(tmp_path, g_app_ctx.db_path) != 0) {
            log_msg(LOG_ERROR, "--recover-database: failed to replace database: %s", strerror(errno));
            log_msg(LOG_ERROR, "Recovered copy left at %s", tmp_path);
            return 1;
        }

        log_msg(LOG_INFO, "--recover-database: complete. %d torrents recovered.", nrows);
        return 0;
    }

    /* Set up signal handlers */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Ensure data directory exists for database and bloom filter */
    log_msg(LOG_DEBUG, "Ensuring data directory exists...");
    if (ensure_parent_directory_exists(g_app_ctx.db_path) != 0) {
        log_msg(LOG_ERROR, "Failed to create data directory for database");
        return 1;
    }

    /* Initialize infohash queue */
    log_msg(LOG_DEBUG, "Initializing infohash queue (capacity: %d)...", INFOHASH_QUEUE_SIZE);
    rc = infohash_queue_init(&g_queue, INFOHASH_QUEUE_SIZE);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize infohash queue: %d", rc);
        return 1;
    }

    /* Initialize bloom filter for duplicate detection */
    if (config.bloom_enabled) {
        /* Ensure bloom filter directory exists if persistence is enabled */
        if (config.bloom_persist) {
            if (ensure_parent_directory_exists(config.bloom_path) != 0) {
                log_msg(LOG_ERROR, "Failed to create directory for bloom filter");
                infohash_queue_cleanup(&g_queue);
                return 1;
            }
        }

        {
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
    }

    /* Initialize pornography content filter (if enabled) */
    if (config.porn_filter_enabled) {
        log_msg(LOG_DEBUG, "Initializing porn filter (keyword file: %s)...", config.porn_filter_keyword_file);
        rc = porn_filter_init(config.porn_filter_keyword_file);
        if (rc != 0) {
            log_msg(LOG_WARN, "Failed to initialize porn filter, continuing without filtering");
        } else {
            log_msg(LOG_DEBUG, "Porn filter enabled");
        }
    } else {
        log_msg(LOG_DEBUG, "Porn filter disabled");
    }

    /* Initialize language filter (independent of porn filter) */
    language_filter_init(config.language_filter_non_latin_threshold);

    /* Initialize torrent search module for title extraction */
    log_msg(LOG_DEBUG, "Initializing torrent search module...");
    rc = torrent_search_init("torrent_search_keywords.txt");
    if (rc != 0) {
        log_msg(LOG_WARN, "Failed to initialize torrent search module, title extraction will be basic");
    }

    /* Integrity check: auto-recover if database is corrupted from a previous crash.
     * Skip if file doesn't exist yet (fresh install). */
    if (access(g_app_ctx.db_path, F_OK) == 0) {
        int nerrors = database_integrity_check(g_app_ctx.db_path);
        /* nerrors > 0: counted integrity errors; nerrors == -1: query itself failed (severe corruption) */
        if (nerrors != 0) {
            log_msg(LOG_WARN, "Database integrity check failed (result=%d) — attempting auto-recovery", nerrors);
            char tmp_path[512];
            snprintf(tmp_path, sizeof(tmp_path), "%s.recover.tmp", g_app_ctx.db_path);
            int nrows = database_recover(g_app_ctx.db_path, tmp_path);
            if (nrows < 0) {
                log_msg(LOG_ERROR, "Auto-recovery failed; starting with corrupted database");
                unlink(tmp_path);
            } else {
                if (rename(tmp_path, g_app_ctx.db_path) != 0) {
                    log_msg(LOG_ERROR, "Auto-recovery: failed to replace database: %s", strerror(errno));
                    unlink(tmp_path);
                } else {
                    log_msg(LOG_WARN, "Auto-recovery complete: %d torrents recovered", nrows);
                }
            }
        }
    }

    /* Initialize database */
    log_msg(LOG_DEBUG, "Initializing database: %s", g_app_ctx.db_path);
    rc = database_init(&g_database, g_app_ctx.db_path, &g_app_ctx);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize database: %d", rc);
        porn_filter_cleanup();
        language_filter_cleanup();
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
     * --porn-filter-update maintenance mode: scan DB and remove rows
     * the current filter would have rejected, then exit.
     *******************************************************************/
    if (porn_filter_update) {
        if (!config.porn_filter_enabled) {
            log_msg(LOG_ERROR, "--porn-filter-update requires porn_filter_enabled=1 in config.ini. Aborting.");
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            infohash_queue_cleanup(&g_queue);
            return 1;
        }
        if (porn_filter_get_keyword_count() < 0) {
            log_msg(LOG_ERROR, "--porn-filter-update: porn filter failed to initialize earlier. Aborting.");
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            infohash_queue_cleanup(&g_queue);
            return 1;
        }

        int rrc = porn_filter_cleanup_run(&g_database, g_app_ctx.db_path);

        if (rrc != 0 || !compact_db) {
            database_cleanup(&g_database);
            torrent_search_cleanup();
            porn_filter_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            infohash_queue_cleanup(&g_queue);
            cleanup_app_context(&g_app_ctx);
            return rrc;
        }
    }

    /*******************************************************************
     * --compact: write a fresh compacted copy, replace original, exit.
     *******************************************************************/
    if (compact_db) {
        char tmp_path[PATH_MAX];
        snprintf(tmp_path, sizeof(tmp_path), "%s.compact.tmp", g_app_ctx.db_path);

        if (check_disk_space(g_app_ctx.db_path, tmp_path) != 0) {
            database_cleanup(&g_database);
            torrent_search_cleanup();
            porn_filter_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            infohash_queue_cleanup(&g_queue);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        /* Remove any leftover temp file from a previous failed run */
        unlink(tmp_path);

        log_msg(LOG_INFO, "Compacting database into %s ...", tmp_path);
        int crc = database_vacuum_into(&g_database, tmp_path);

        database_cleanup(&g_database);
        torrent_search_cleanup();
        porn_filter_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        infohash_queue_cleanup(&g_queue);

        if (crc != 0) {
            log_msg(LOG_ERROR, "Compaction failed; original database unchanged.");
            unlink(tmp_path);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        if (rename(tmp_path, g_app_ctx.db_path) != 0) {
            log_msg(LOG_ERROR, "Failed to replace database with compacted copy: %s", strerror(errno));
            log_msg(LOG_ERROR, "Compacted copy left at %s", tmp_path);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        log_msg(LOG_INFO, "Database compaction complete.");
        cleanup_app_context(&g_app_ctx);
        return 0;
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

        /* Configure daily backup */
        if (config.backup_enabled) {
            batch_writer_set_backup(g_batch_writer, config.db_path, config.backup_path);
        }

        /* Create supervisor config */
        supervisor_config_t sup_config = {
            .max_trees = config.num_trees,
            .use_keyspace_partitioning = config.use_keyspace_partitioning,
            .dht_port = config.dht_port,
            .batch_writer = g_batch_writer,
            .bloom_filter = g_bloom,
            /* Bloom filter settings for failure tracking */
            .failure_bloom_capacity = config.failure_bloom_capacity,
            .bloom_error_rate = config.bloom_error_rate,
            .num_find_node_workers = config.tree_find_node_workers,
            .num_bep51_workers = config.tree_bep51_workers,
            .num_get_peers_workers = config.tree_get_peers_workers,
            .num_metadata_workers = config.tree_metadata_workers,
            /* Stage 2 settings (Global Bootstrap - NEW) */
            .global_bootstrap_target = config.global_bootstrap_target,
            .global_bootstrap_timeout_sec = config.global_bootstrap_timeout_sec,
            .global_bootstrap_workers = config.global_bootstrap_workers,
            .per_tree_sample_size = config.per_tree_sample_size,
            /* BEP51 cache settings */
            .bep51_cache_capacity = config.bep51_cache_capacity,
            .bep51_cache_submit_percent = config.bep51_cache_submit_percent,
            /* Stage 3 settings (BEP51) */
            .infohash_queue_capacity = config.tree_infohash_queue_capacity,
            .bep51_query_interval_ms = config.tree_bep51_query_interval_ms,
            .bep51_node_cooldown_sec = config.bep51_node_cooldown_sec,
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
            .parallel_peers = config.tree_parallel_peers,
            /* Metadata rate-based respawn settings */
            .min_metadata_rate = config.min_metadata_rate,
            .dynamic_rate_margin = config.dynamic_rate_margin,
            .rate_check_interval_sec = config.tree_rate_check_interval_sec,
            .rate_grace_period_sec = config.tree_rate_grace_period_sec,
            .min_lifetime_minutes = config.tree_min_lifetime_minutes,
            .require_empty_queue = config.tree_require_empty_queue,
            .rate_ema_alpha = config.tree_rate_ema_alpha,
            /* Respawn overlapping configuration */
            .respawn_spawn_threshold = config.respawn_spawn_threshold,
            .respawn_drain_timeout_sec = config.respawn_drain_timeout_sec,
            .max_draining_trees = config.max_draining_trees,
            /* Porn filter settings */
            .porn_filter_enabled = config.porn_filter_enabled,
            /* Adaptive keyspace partitioning */
            .dead_partition_threshold = config.dead_partition_threshold,
            .max_trees_per_partition = config.max_trees_per_partition
        };
        /* Copy BEP51 cache path (can't use string literal in struct initializer) */
        strncpy(sup_config.bep51_cache_path, config.bep51_cache_path, sizeof(sup_config.bep51_cache_path) - 1);

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

        /* Create shared refresh query store for HTTP API */
        log_msg(LOG_DEBUG, "Creating refresh query store...");
        g_refresh_query_store = refresh_query_store_init(1009, 10);
        if (!g_refresh_query_store) {
            log_msg(LOG_ERROR, "Failed to create refresh query store");
            supervisor_stop(g_supervisor);
            supervisor_destroy(g_supervisor);
            batch_writer_cleanup(g_batch_writer);
            database_cleanup(&g_database);
            bloom_filter_cleanup(g_bloom);
            return 1;
        }

        /* Create and start refresh thread */
        log_msg(LOG_DEBUG, "Creating refresh thread...");
        refresh_thread_config_t refresh_config = {
            .dht_port = 0,  /* Use ephemeral port (not 6881) to avoid conflict with shared socket */
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
                                                  g_refresh_query_store);
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

        /* Set refresh query store for /refresh endpoint */
        http_api_set_refresh_query_store(&g_http_api, g_refresh_query_store);

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
        batch_writer_inhibit_backup(g_batch_writer);
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

        /* Cleanup - cleanup_app_context must be called before batch_writer_cleanup
         * to avoid use-after-free in uv_loop_close() */
        http_api_cleanup(&g_http_api);
        if (g_refresh_thread) {
            refresh_thread_destroy(g_refresh_thread);
        }
        supervisor_destroy(g_supervisor);
        cleanup_app_context(&g_app_ctx);
        batch_writer_cleanup(g_batch_writer);
        if (g_refresh_query_store) {
            refresh_query_store_cleanup(g_refresh_query_store);
        }
        database_cleanup(&g_database);
        torrent_search_cleanup();
        porn_filter_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);

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
    language_filter_cleanup();
    bloom_filter_cleanup(g_bloom);
    infohash_queue_cleanup(&g_queue);
    cleanup_app_context(&g_app_ctx);

    log_msg(LOG_DEBUG, "DHT Crawler stopped.");
    return 0;
}
