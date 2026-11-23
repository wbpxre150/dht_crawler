#include "dht_crawler.h"
#include "database.h"
#include "http_api.h"
#include "bloom_filter.h"
#include "inflight_set.h"
#include "language_filter.h"
#include "classifier.h"
#include "hard_block.h"
#include "category_filter_cleanup.h"
#include "torrent_search.h"
#include "config.h"
#include "udp_rate_limit.h"
#include "supervisor.h"
#include "batch_writer.h"
#include "refresh_thread.h"
#include "refresh_query.h"
#include "adult_backfill.h"
#include "classify_backfill.h"
#include "classify_backfill_daemon.h"
#include "movie_kind.h"
#include "movie_backfill.h"
#include "movie_backfill_daemon.h"
#include "maintenance_progress.h"
#include "model_fetch.h"
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
static database_t g_database;
static http_api_t g_http_api;
static bloom_filter_t *g_bloom = NULL;
static inflight_set_t *g_inflight = NULL;   /* Shared in-flight claim set (NULL = dedup disabled) */

/* In-flight claim set sizing: one slot per concurrently in-flight infohash
 * (queue backlog + get_peers lookups + metadata fetches across all trees).
 * 262144 slots at 32 bytes is 8 MiB, which is noise next to the two 54 MB bloom
 * filters, and it clears itself at 3/4 full rather than losing claims. */
#define INFLIGHT_SET_CAPACITY 262144
/* Backstop for a claim whose worker never resolved it (a thread torn down
 * mid-fetch). Trees normally hand their claims back in thread_tree_destroy(). */
#define INFLIGHT_SET_TTL_SEC 900

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
    bool compact_db = false;
    bool check_db = false;
    bool recover_db = false;
    const char *restore_db_path = NULL;
    const char *rebuild_fts_db_path = NULL;
    bool clear_failure_filter = false;
    bool fetch_bep51_cache = false;
    bool fetch_models = false;
    const char *models_base = NULL;
    const char *models_dir = NULL;
    bool models_force = false;
    bool category_filter_update = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            printf("Usage: %s [OPTIONS]\n\n", argv[0]);
            printf("Options:\n");
            printf("  --rebuild-bloom-filter   Rebuild the bloom filter from the existing database\n");
            printf("  --category-filter-update Remove torrents outside save_categories/save_tv_kinds\n");
            printf("                           and adult-tagged torrents from the database\n");
            printf("  --compact                Compact the database by writing a fresh copy and replacing the original\n");
            printf("  --check-database         Run an integrity check on the database and report any errors\n");
            printf("  --recover-database       Recover the database by copying all readable rows to a fresh\n");
            printf("                           database, then replacing the original\n");
            printf("  --restore-database <sql> Replay an uncompressed SQL dump into a fresh database\n");
            printf("                           and replace the configured database with it\n");
            printf("  --rebuild-fts <db>       Create the FTS5 search index in an existing database\n");
            printf("                           and rebuild it with the bundled SQLite\n");
            printf("  --clear-failure-filter   Delete the persisted failure bloom filter (data/failure_bloom.dat)\n");
            printf("  --fetch-bep51-cache      Download bep51_cache_url into bep51_cache_path and exit\n");
            printf("  --fetch-models           Download the classifier weights into ./models and exit\n");
            printf("                           (--models-base <url>, --models-dir <dir>, --force)\n");
            printf("  --help, -h               Show this help message and exit\n");
            return 0;
        } else if (strcmp(argv[i], "--rebuild-bloom-filter") == 0) {
            rebuild_bloom = true;
        } else if (strcmp(argv[i], "--category-filter-update") == 0) {
            category_filter_update = true;
        } else if (strcmp(argv[i], "--compact") == 0) {
            compact_db = true;
        } else if (strcmp(argv[i], "--check-database") == 0) {
            check_db = true;
        } else if (strcmp(argv[i], "--recover-database") == 0) {
            recover_db = true;
        } else if (strcmp(argv[i], "--restore-database") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--restore-database requires a <dump.sql> argument");
                return 1;
            }
            restore_db_path = argv[++i];
        } else if (strcmp(argv[i], "--rebuild-fts") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--rebuild-fts requires a <database> argument");
                return 1;
            }
            rebuild_fts_db_path = argv[++i];
        } else if (strcmp(argv[i], "--clear-failure-filter") == 0) {
            clear_failure_filter = true;
        } else if (strcmp(argv[i], "--fetch-bep51-cache") == 0) {
            fetch_bep51_cache = true;
        } else if (strcmp(argv[i], "--fetch-models") == 0) {
            fetch_models = true;
        } else if (strcmp(argv[i], "--models-base") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--models-base requires a <url> argument");
                return 1;
            }
            models_base = argv[++i];
        } else if (strcmp(argv[i], "--models-dir") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--models-dir requires a <dir> argument");
                return 1;
            }
            models_dir = argv[++i];
        } else if (strcmp(argv[i], "--force") == 0) {
            /* Only --fetch-models consumes it; see the dispatch below. */
            models_force = true;
        }
    }

    /* --rebuild-docs <db>: backfill the schema-v2 torrent-level word index
     * (torrent_docs) of an existing database from its base tables. One-shot
     * maintenance, dispatched before the daemon initialises anything, so it
     * needs no configuration, no app context and no cleanup call. Scanned in
     * its own loop (rather than in the argument loop above) to keep that
     * loop's shape unchanged.
     *
     * --print-schema: write the schema DDL to stdout, one CREATE statement
     * per line, and exit 0. Read-only and side-effect free; the host database
     * rebuild script uses it so the target schema is the crawler's own.
     * Declared here rather than with the other flags above because that
     * declaration block is context for the build patch stack.
     *
     * --classify-backfill <db> [--write] [--classify-backfill-limit <n>]:
     * re-classify the torrent_class rows of <db> with the model generation the
     * loaded manifest describes. Resumable and upsert-only: no row is ever
     * removed, and an interrupted pass continues from the resume key in
     * classifier_meta. Dry run by default - it only counts the candidates;
     * --write commits the rows. --classify-backfill-limit <n> stops after n
     * candidates (0 = unlimited), for smoke tests. Dispatched after
     * classifier_init(), because a backfill without loaded models can only
     * report DISABLED, but before database_init(), so it opens <db> alone and
     * never touches the configured daemon database.
     *
     * Usage is documented here, not in --help, on purpose: the help printf
     * block is patch-anchor territory for the build patch stack. */
    bool print_schema = false;
    const char *rebuild_docs_db_path = NULL;
    const char *classify_backfill_db = NULL;
    long long classify_backfill_limit = 0;
    bool classify_backfill_write = false;
    const char *adult_backfill_db = NULL;
    long long adult_backfill_limit = 0;
    const char *movie_backfill_db = NULL;
    long long movie_backfill_limit = 0;
    bool movie_backfill_dry_run = false;
    bool movie_backfill_reset_flag = false;
    const char *movie_bench_db = NULL;
    int movie_bench_rows = 0;
    bool dump_masks = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--rebuild-docs") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--rebuild-docs requires a <database> argument");
                return 1;
            }
            rebuild_docs_db_path = argv[++i];
        } else if (strcmp(argv[i], "--print-schema") == 0) {
            print_schema = true;
        } else if (strcmp(argv[i], "--classify-backfill") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--classify-backfill requires a <database> argument");
                return 1;
            }
            classify_backfill_db = argv[++i];
        } else if (strcmp(argv[i], "--classify-backfill-limit") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--classify-backfill-limit requires a <rows> argument");
                return 1;
            }
            const char *limit_arg = argv[++i];
            char *limit_end = NULL;
            errno = 0;
            long long limit_rows = strtoll(limit_arg, &limit_end, 10);
            if (errno != 0 || limit_end == limit_arg || *limit_end != '\0' || limit_rows < 0) {
                log_msg(LOG_ERROR, "--classify-backfill-limit requires a non-negative integer, got '%s'",
                        limit_arg);
                return 1;
            }
            classify_backfill_limit = limit_rows;
        } else if (strcmp(argv[i], "--adult-backfill") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--adult-backfill requires a <database> argument");
                return 1;
            }
            adult_backfill_db = argv[++i];
        } else if (strcmp(argv[i], "--adult-backfill-limit") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--adult-backfill-limit requires a <rows> argument");
                return 1;
            }
            const char *limit_arg = argv[++i];
            char *limit_end = NULL;
            errno = 0;
            long long limit_rows = strtoll(limit_arg, &limit_end, 10);
            if (errno != 0 || limit_end == limit_arg || *limit_end != '\0' || limit_rows < 0) {
                log_msg(LOG_ERROR,
                        "--adult-backfill-limit requires a non-negative integer, got '%s'",
                        limit_arg);
                return 1;
            }
            adult_backfill_limit = limit_rows;
        } else if (strcmp(argv[i], "--write") == 0) {
            classify_backfill_write = true;
        } else if (strcmp(argv[i], "--movie-kind-backfill") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--movie-kind-backfill requires a <database> argument");
                return 1;
            }
            movie_backfill_db = argv[++i];
        } else if (strcmp(argv[i], "--movie-kind-bench") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--movie-kind-bench requires a <database> argument");
                return 1;
            }
            movie_bench_db = argv[++i];
            /* Optional <rows> operand: a bare non-negative integer. */
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                const char *rows_arg = argv[i + 1];
                char *rows_end = NULL;
                errno = 0;
                long long rows = strtoll(rows_arg, &rows_end, 10);
                if (errno != 0 || rows_end == rows_arg || *rows_end != '\0' || rows <= 0 ||
                    rows > 1000000) {
                    log_msg(LOG_ERROR,
                            "--movie-kind-bench takes a positive <rows> count, got '%s'",
                            rows_arg);
                    return 1;
                }
                movie_bench_rows = (int)rows;
                i++;
            }
        } else if (strcmp(argv[i], "--dry-run") == 0) {
            movie_backfill_dry_run = true;
        } else if (strcmp(argv[i], "--limit") == 0) {
            if (i + 1 >= argc) {
                log_msg(LOG_ERROR, "--limit requires a <rows> argument");
                return 1;
            }
            const char *limit_arg = argv[++i];
            char *limit_end = NULL;
            errno = 0;
            long long limit_rows = strtoll(limit_arg, &limit_end, 10);
            if (errno != 0 || limit_end == limit_arg || *limit_end != '\0' || limit_rows < 0) {
                log_msg(LOG_ERROR, "--limit requires a non-negative integer, got '%s'",
                        limit_arg);
                return 1;
            }
            movie_backfill_limit = limit_rows;
        } else if (strcmp(argv[i], "--dump-masks") == 0) {
            dump_masks = true;
        } else if (strcmp(argv[i], "--reset") == 0) {
            /* --movie-kind-backfill <db> --reset: clear the rows a broken pass
             * labelled (stage 10). See include/movie_backfill.h. */
            movie_backfill_reset_flag = true;
        }
    }

    /* --fetch-models [--models-base <url>] [--models-dir <dir>] [--force]:
     * download the classifier weights this binary serves with, then exit.
     *
     * Dispatched before any configuration or database work: it needs neither,
     * and it must be cheap to call on every process start - a tree whose files
     * already verify costs a handful of sha256s and no network at all. Progress
     * goes to stderr and nothing to stdout, so a caller can capture stdout.
     * Exit 0 means both generations are present and verified (including the
     * already-present no-op case); 1 means something failed. */
    if (fetch_models) {
        model_fetch_options_t models_opts;
        memset(&models_opts, 0, sizeof(models_opts));
        models_opts.base = models_base;
        models_opts.dir = models_dir;
        models_opts.force = models_force;
        models_opts.log = stderr;
        return model_fetch_run(&models_opts) == 0 ? 0 : 1;
    }
    if (print_schema) {
        database_print_schema(stdout);
        return 0;
    }

    if (rebuild_docs_db_path) {
        log_msg(LOG_INFO, "Rebuilding torrent_docs of %s ...", rebuild_docs_db_path);
        int docs_rc = database_rebuild_docs(rebuild_docs_db_path);
        if (docs_rc != 0) {
            log_msg(LOG_ERROR, "--rebuild-docs failed; database left unchanged.");
            return 1;
        }
        return 0;
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
    /* Apply the database path from config. config is stack-local and lives
     * for the whole of main(), and cleanup_app_context() never frees
     * db_path, so pointing at config.db_path is safe. When config.ini is
     * missing or has no db_path key, config_init_defaults() leaves it at
     * "data/torrents.db", preserving the previous default behaviour. */
    g_app_ctx.db_path = config.db_path;
    log_msg(LOG_DEBUG, "Using database path: %s", g_app_ctx.db_path);
    /* Configure the process-wide UDP send rate limiter before any thread
     * that sends DHT UDP packets (trees, refresh thread) is spawned. */
    udp_rate_limit_init((uint32_t)config.udp_rate_limit_pps);

    /* Apply the process nice level before any thread is spawned so that
     * every thread in the process inherits it. */
    if (config.nice_level != 0) {
        if (setpriority(PRIO_PROCESS, 0, config.nice_level) == 0) {
            log_msg(LOG_INFO, "Process nice level set to %d", config.nice_level);
        } else {
            log_msg(LOG_WARN, "Failed to set nice level to %d: %s", config.nice_level, strerror(errno));
        }
    }

    /* Rebuild bloom filter from database if requested */
    if (rebuild_bloom) {
        if (!config.bloom_enabled) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: bloom filter is disabled in config.ini");
            mprog_terminal("rebuild-bloom-filter", 0, 1, NULL);
            return 1;
        }
        if (!config.bloom_persist) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: bloom filter persistence is disabled in config.ini (bloom_persist=0)");
            mprog_terminal("rebuild-bloom-filter", 0, 1, NULL);
            return 1;
        }
        if (ensure_parent_directory_exists(config.bloom_path) != 0) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to create bloom filter directory");
            mprog_terminal("rebuild-bloom-filter", 0, 1, NULL);
            return 1;
        }

        log_msg(LOG_INFO, "Rebuilding bloom filter from database %s ...", g_app_ctx.db_path);
        bloom_filter_t *bloom = bloom_filter_init(config.bloom_capacity, config.bloom_error_rate);
        if (!bloom) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to allocate bloom filter");
            mprog_terminal("rebuild-bloom-filter", 0, 1, NULL);
            return 1;
        }

        /* Count first, so the very first [MPROG] line already carries the percent
         * base; database_rebuild_bloom counts the same rows again for its own
         * scan phase (an index-only COUNT(*), milliseconds either way). */
        long long bloom_total = database_count_torrents(g_app_ctx.db_path);
        mprog_emit("rebuild-bloom-filter", "count", 0, bloom_total, NULL);

        int added = database_rebuild_bloom(g_app_ctx.db_path, bloom);
        if (added < 0) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to read database");
            bloom_filter_cleanup(bloom);
            mprog_terminal("rebuild-bloom-filter", 0, 1, NULL);
            return 1;
        }

        log_msg(LOG_INFO, "Added %d infohashes to bloom filter", added);

        mprog_emit("rebuild-bloom-filter", "save", -1, 0, NULL);
        if (bloom_filter_save(bloom, config.bloom_path) != 0) {
            log_msg(LOG_ERROR, "--rebuild-bloom-filter: failed to save bloom filter to %s", config.bloom_path);
            bloom_filter_cleanup(bloom);
            mprog_terminal("rebuild-bloom-filter", 0, 1, NULL);
            return 1;
        }

        bloom_filter_cleanup(bloom);
        log_msg(LOG_INFO, "Bloom filter rebuilt and saved to %s.", config.bloom_path);
        {
            char bloom_extra[64];
            snprintf(bloom_extra, sizeof(bloom_extra), "added=%d", added);
            mprog_terminal("rebuild-bloom-filter", 1, 0, bloom_extra);
        }
        return 0;
    }

    /* Refresh the published BEP51 cache without starting the crawler: the same
     * fetch the supervisor runs on a cold start, usable from cron/CI. */
    if (fetch_bep51_cache) {
        if (config.bep51_cache_url[0] == '\0') {
            log_msg(LOG_ERROR, "--fetch-bep51-cache: bep51_cache_url is empty in config.ini");
            return 1;
        }
        bep51_cache_t *cache = bep51_cache_create(config.bep51_cache_capacity);
        if (!cache) {
            log_msg(LOG_ERROR, "--fetch-bep51-cache: cannot create the cache");
            return 1;
        }
        bep51_cache_fetch_info_t fetch;
        int frc = bep51_cache_fetch_remote_ex(cache, config.bep51_cache_url, config.bep51_cache_path,
                                              config.bep51_cache_fetch_timeout_sec,
                                              config.bep51_cache_ca_file[0] ? config.bep51_cache_ca_file : NULL,
                                              config.bep51_cache_ca_dir[0] ? config.bep51_cache_ca_dir : NULL,
                                              &fetch);
        /* No supervisor exists in this path, so the one-shot publishes its own
         * single-shot numbers on this line instead of into /stats - the same
         * verdict string the daemon's bep51_cache_fetch.last_result carries. */
        char verdict[32];
        bep51_cache_fetch_result_name((int)fetch.result, fetch.http_status, verdict, sizeof(verdict));
        if (frc == 0) {
            log_msg(LOG_INFO, "--fetch-bep51-cache: %s, %zu nodes in %s (http %d, %lld bytes, %d ms)",
                    verdict, bep51_cache_get_count(cache), config.bep51_cache_path,
                    fetch.http_status, fetch.bytes, fetch.duration_ms);
        } else {
            log_msg(LOG_ERROR,
                    "--fetch-bep51-cache: failed (%s%s%s; http %d, %lld bytes, %d ms); "
                    "existing cache left untouched",
                    verdict, fetch.error[0] ? ": " : "", fetch.error,
                    fetch.http_status, fetch.bytes, fetch.duration_ms);
        }
        bep51_cache_destroy(cache);
        return frc == 0 ? 0 : 1;
    }

    /* Remove the two-strike failure bloom filter so infohashes that failed
     * metadata fetch (and were suppressed from retry) become eligible again.
     * The crawler re-creates an empty filter on its next start. */
    if (clear_failure_filter) {
        /* Phase-only by nature: one unlink(), so there is no denominator to
         * report - the outcome rides on the terminal line as removed=1|0. */
        mprog_emit("clear-failure-filter", "unlink", -1, 0, NULL);
        if (unlink("data/failure_bloom.dat") == 0) {
            log_msg(LOG_INFO, "--clear-failure-filter: removed data/failure_bloom.dat");
            mprog_terminal("clear-failure-filter", 1, 0, "removed=1");
            return 0;
        }
        if (errno == ENOENT) {
            log_msg(LOG_INFO, "--clear-failure-filter: data/failure_bloom.dat not present; nothing to remove");
            mprog_terminal("clear-failure-filter", 1, 0, "removed=0");
            return 0;
        }
        log_msg(LOG_ERROR, "--clear-failure-filter: failed to remove data/failure_bloom.dat: %s", strerror(errno));
        mprog_terminal("clear-failure-filter", 0, 1, "removed=0");
        return 1;
    }

    /*******************************************************************
     * --check-database: run integrity_check PRAGMA and report results.
     *******************************************************************/
    if (check_db) {
        log_msg(LOG_INFO, "Checking database integrity: %s", g_app_ctx.db_path);
        /* Phase-only: PRAGMA integrity_check reports a verdict at the end and
         * nothing incremental, so there is no honest denominator to show. */
        mprog_emit("check-database", "integrity_check", -1, 0, NULL);
        int nerrors = database_integrity_check(g_app_ctx.db_path);
        cleanup_app_context(&g_app_ctx);
        if (nerrors < 0) {
            log_msg(LOG_ERROR, "--check-database: failed to open or query database.");
            mprog_terminal("check-database", 0, 1, NULL);
            return 1;
        }
        if (nerrors == 0) {
            log_msg(LOG_INFO, "--check-database: database is clean.");
            mprog_terminal("check-database", 1, 0, "errors=0");
            return 0;
        }
        log_msg(LOG_ERROR, "--check-database: %d integrity error(s) found.", nerrors);
        {
            char check_extra[64];
            snprintf(check_extra, sizeof(check_extra), "errors=%d", nerrors);
            mprog_terminal("check-database", 0, 1, check_extra);
        }
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
            mprog_terminal("recover-database", 0, 1, NULL);
            return 1;
        }

        unlink(tmp_path);

        log_msg(LOG_INFO, "Recovering database %s into %s ...", g_app_ctx.db_path, tmp_path);
        /* The three copy phases emit their own lines from database_recover(). */
        int nrows = database_recover(g_app_ctx.db_path, tmp_path);

        cleanup_app_context(&g_app_ctx);

        if (nrows < 0) {
            log_msg(LOG_ERROR, "--recover-database: recovery failed; original database unchanged.");
            unlink(tmp_path);
            mprog_terminal("recover-database", 0, 1, NULL);
            return 1;
        }

        mprog_emit("recover-database", "verify", -1, 0, NULL);
        int chk = database_integrity_check(tmp_path);
        if (chk != 0) {
            log_msg(LOG_ERROR, "--recover-database: recovered database failed integrity check (%d errors); aborting.", chk);
            unlink(tmp_path);
            {
                char recover_extra[64];
                snprintf(recover_extra, sizeof(recover_extra), "errors=%d", chk);
                mprog_terminal("recover-database", 0, 1, recover_extra);
            }
            return 1;
        }

        /* Remove stale WAL/SHM from the original (corrupted) database before rename */
        char wal_path[PATH_MAX], shm_path[PATH_MAX];
        snprintf(wal_path, sizeof(wal_path), "%s-wal", g_app_ctx.db_path);
        snprintf(shm_path, sizeof(shm_path), "%s-shm", g_app_ctx.db_path);
        unlink(wal_path);
        unlink(shm_path);

        mprog_emit("recover-database", "rename", -1, 0, NULL);
        if (rename(tmp_path, g_app_ctx.db_path) != 0) {
            log_msg(LOG_ERROR, "--recover-database: failed to replace database: %s", strerror(errno));
            log_msg(LOG_ERROR, "Recovered copy left at %s", tmp_path);
            mprog_terminal("recover-database", 0, 1, NULL);
            return 1;
        }

        /* Clean up any WAL/SHM left by the tmp database */
        char tmp_wal[PATH_MAX], tmp_shm[PATH_MAX];
        snprintf(tmp_wal, sizeof(tmp_wal), "%s-wal", tmp_path);
        snprintf(tmp_shm, sizeof(tmp_shm), "%s-shm", tmp_path);
        unlink(tmp_wal);
        unlink(tmp_shm);

        log_msg(LOG_INFO, "--recover-database: complete. %d torrents recovered.", nrows);
        {
            char recover_extra[64];
            snprintf(recover_extra, sizeof(recover_extra), "torrents=%d", nrows);
            mprog_terminal("recover-database", 1, 0, recover_extra);
        }
        return 0;
    }

    /*******************************************************************
     * --restore-database <dump.sql>: replay the dump into a fresh
     * database, then replace the configured one. The dump is plain
     * (uncompressed) SQL in the incremental-dump format; the C side is
     * deliberately dumb — no decompression, no chain knowledge.
     *******************************************************************/
    if (restore_db_path) {
        char tmp_path[PATH_MAX];
        snprintf(tmp_path, sizeof(tmp_path), "%s.restore.tmp", g_app_ctx.db_path);

        if (check_disk_space(g_app_ctx.db_path, tmp_path) != 0) {
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        unlink(tmp_path);

        log_msg(LOG_INFO, "Restoring database %s from SQL dump %s ...",
                g_app_ctx.db_path, restore_db_path);
        int nstmts = database_restore(restore_db_path, tmp_path);

        cleanup_app_context(&g_app_ctx);

        if (nstmts < 0) {
            log_msg(LOG_ERROR, "--restore-database: restore failed; original database unchanged.");
            unlink(tmp_path);
            return 1;
        }

        int chk = database_integrity_check(tmp_path);
        if (chk != 0) {
            log_msg(LOG_ERROR, "--restore-database: restored database failed integrity check (%d errors); aborting.", chk);
            unlink(tmp_path);
            return 1;
        }

        /* Remove stale WAL/SHM from the original database before rename */
        char wal_path[PATH_MAX], shm_path[PATH_MAX];
        snprintf(wal_path, sizeof(wal_path), "%s-wal", g_app_ctx.db_path);
        snprintf(shm_path, sizeof(shm_path), "%s-shm", g_app_ctx.db_path);
        unlink(wal_path);
        unlink(shm_path);

        if (rename(tmp_path, g_app_ctx.db_path) != 0) {
            log_msg(LOG_ERROR, "--restore-database: failed to replace database: %s", strerror(errno));
            log_msg(LOG_ERROR, "Restored copy left at %s", tmp_path);
            return 1;
        }

        /* Clean up any WAL/SHM left by the tmp database */
        char tmp_wal[PATH_MAX], tmp_shm[PATH_MAX];
        snprintf(tmp_wal, sizeof(tmp_wal), "%s-wal", tmp_path);
        snprintf(tmp_shm, sizeof(tmp_shm), "%s-shm", tmp_path);
        unlink(tmp_wal);
        unlink(tmp_shm);

        log_msg(LOG_INFO, "--restore-database: complete. %d statements replayed.", nstmts);
        return 0;
    }

    /*******************************************************************
     * --rebuild-fts <db>: create the FTS5 tables + triggers in an
     * existing database, rebuild the external-content torrent_search
     * index and backfill the contentless torrent_docs index with the
     * bundled SQLite, which always has FTS5 compiled in (unlike the
     * platform SQLite). The restore path bulk-imports the base tables
     * from the Kotlin side first, then calls this on the TEMP database
     * before swapping it in, so a restore never leaves either search
     * index silently empty.
     *******************************************************************/
    if (rebuild_fts_db_path) {
        log_msg(LOG_INFO, "Rebuilding FTS indexes of %s ...", rebuild_fts_db_path);
        int fts_rc = database_rebuild_fts(rebuild_fts_db_path);

        cleanup_app_context(&g_app_ctx);

        if (fts_rc != 0) {
            log_msg(LOG_ERROR, "--rebuild-fts: FTS rebuild failed; database left unchanged.");
            return 1;
        }

        log_msg(LOG_INFO, "--rebuild-fts: complete.");
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


    /* Initialize bloom filter for duplicate detection */
    if (config.bloom_enabled) {
        /* Ensure bloom filter directory exists if persistence is enabled */
        if (config.bloom_persist) {
            if (ensure_parent_directory_exists(config.bloom_path) != 0) {
                log_msg(LOG_ERROR, "Failed to create directory for bloom filter");
                return 1;
            }
        }

        {
            log_msg(LOG_DEBUG, "Initializing bloom filter (capacity: %lu, error rate: %.3f%%)...",
                    config.bloom_capacity, config.bloom_error_rate * 100.0);
            g_bloom = bloom_filter_init(config.bloom_capacity, config.bloom_error_rate);
            if (!g_bloom) {
                log_msg(LOG_ERROR, "Failed to initialize bloom filter");
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

    /* Shared in-flight claim set (include/inflight_set.h). Non-fatal: a NULL set
     * makes every claim/release a no-op, which is exactly the pre-existing
     * duplicate-fetch behaviour - it never suppresses or loses work. */
    g_inflight = inflight_set_create(INFLIGHT_SET_CAPACITY, INFLIGHT_SET_TTL_SEC);
    if (!g_inflight) {
        log_msg(LOG_WARN, "In-flight claim set unavailable: concurrent duplicate "
                          "metadata fetches will not be suppressed");
    }

    /* Initialize the always-on hard block list (Stage 6). Non-fatal: a missing
     * file only disables the list, and hard_block_init() logs the warning. */
    hard_block_init(config.hard_block_file);

    /* Initialize the language filter */
    language_filter_init(config.language_filter_non_latin_threshold);

    /* Initialize ingest-time classifier (Stage 5, tag mode). Non-fatal: without
     * a model directory it falls back to parser-only classification. */
    if (config.classifier_enabled) {
        classifier_init(config.classifier_model_dir, config.adult_hide_threshold);
    } else {
        log_msg(LOG_DEBUG, "Classifier disabled");
    }

    /* Publish the active adult mode for /stats, even when the classifier is off:
     * it is what the operator wrote, and it decides whether an adult-tagged row
     * is stored (tag) or discarded (drop). Set once, before any worker thread. */
    classifier_set_adult_mode(config.adult_mode);

    /* Movie-kind (v9) classifier: manifest/sha256 gated and lazy - the 152.8 MiB
     * backbone is only mapped by the first classify, so a crawler that never
     * backfills never pays for it. Non-fatal: a phone without the v9 assets
     * leaves the module disabled, and the backfill daemon then reports
     * "disabled" without touching the database. */
    if (config.movie_kind_enabled) {
        if (movie_kind_init(config.movie_kind_model_dir) == 0) {
            movie_kind_set_threads(config.movie_kind_threads);
        }
    } else {
        log_msg(LOG_DEBUG, "Movie-kind classifier disabled");
    }

    /* --classify-backfill <db>: one-shot maintenance over the database named on
     * the command line. Dispatched here on purpose: after classifier_init(), so
     * the models a backfill needs are loaded, but before database_init(), so the
     * configured daemon database is never opened and classifier_write_meta()
     * never runs against it. The pass works purely on the flag's path, through
     * its own database handle. */
    if (classify_backfill_db) {
        classify_backfill_opts_t bf_opts = {
            .target_clf_ver = 0,  /* whatever the loaded manifest describes */
            .chunk_ids = 0,       /* engine defaults */
            .batch_rows = 0,
            .dry_run = classify_backfill_write ? 0 : 1,
            .limit_rows = classify_backfill_limit,
        };
        classify_backfill_progress_t bf_progress;
        database_t bf_db;

        memset(&bf_progress, 0, sizeof(bf_progress));
        log_msg(LOG_INFO, "classify-backfill: %s %s ...",
                classify_backfill_write ? "write pass on" : "dry run on", classify_backfill_db);

        rc = database_init(&bf_db, classify_backfill_db, &g_app_ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "classify-backfill: cannot open %s: %d",
                    classify_backfill_db, rc);
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        rc = database_create_schema(&bf_db);
        if (rc != 0) {
            log_msg(LOG_ERROR, "classify-backfill: cannot create the schema of %s: %d",
                    classify_backfill_db, rc);
            database_cleanup(&bf_db);
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        int bf_rc = classify_backfill_run(&bf_db, &bf_opts, &bf_progress);
        log_msg(bf_rc == 0 ? LOG_INFO : LOG_ERROR,
                "classify-backfill: state=%s target_clf_ver=%lld total=%lld done=%lld "
                "updated=%lld inserted=%lld skipped=%lld failed=%lld last_id=%lld",
                classify_backfill_state_name(bf_progress.state),
                bf_progress.target_clf_ver, bf_progress.rows_total,
                bf_progress.rows_done, bf_progress.rows_updated,
                bf_progress.rows_inserted, bf_progress.rows_skipped,
                bf_progress.rows_failed, bf_progress.last_id);

        database_cleanup(&bf_db);
        /* Teardown for everything initialised so far. No porn filter cleanup:
         * remove-porn-filter.patch removes both the filter and its sources. */
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        cleanup_app_context(&g_app_ctx);
        /* DISABLED and FAILED must both fail loudly: a backfill that cannot run
         * must never look like a success. */
        return (bf_rc == 0) ? 0 : 1;
    }

    /* --adult-backfill <db> [--write] [--adult-backfill-limit <n>]: one-shot
     * maintenance over the database named on the command line, dispatched for
     * exactly the reasons --classify-backfill is (after classifier_init(), so
     * the adult student is loaded; before database_init(), so the configured
     * daemon database is never opened). Dry run by default: without --write it
     * runs the SAME two-stage token gate the write pass runs - no student, no
     * UPDATE, no commit - and reports the predicate count (total) plus how many
     * of those rows the gate rejected (gated_out). The gated-in candidate set,
     * i.e. the rows the write pass would score, is total - gated_out; `total`
     * alone is NOT that number. */
    if (adult_backfill_db) {
        adult_backfill_opts_t ab_opts = {
            .target_adult_clf_ver = 0, /* whatever the loaded manifest describes */
            .chunk_ids = 0,            /* engine defaults */
            .batch_rows = 0,
            .dry_run = classify_backfill_write ? 0 : 1,
            .limit_rows = adult_backfill_limit,
        };
        adult_backfill_progress_t ab_progress;
        database_t ab_db;

        memset(&ab_progress, 0, sizeof(ab_progress));
        log_msg(LOG_INFO, "adult-backfill: %s %s ...",
                classify_backfill_write ? "write pass on" : "dry run on", adult_backfill_db);

        rc = database_init(&ab_db, adult_backfill_db, &g_app_ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "adult-backfill: cannot open %s: %d", adult_backfill_db, rc);
            mprog_terminal("adult-backfill", 0, 1, NULL);
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        rc = database_create_schema(&ab_db);
        if (rc != 0) {
            log_msg(LOG_ERROR, "adult-backfill: cannot create the schema of %s: %d",
                    adult_backfill_db, rc);
            mprog_terminal("adult-backfill", 0, 1, NULL);
            database_cleanup(&ab_db);
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        int ab_rc = adult_backfill_run(&ab_db, &ab_opts, &ab_progress);
        log_msg(ab_rc == 0 ? LOG_INFO : LOG_ERROR,
                "adult-backfill: state=%s target=%lld total=%lld gated_out=%lld "
                "scored=%lld updated=%lld failed=%lld last_id=%lld",
                adult_backfill_state_name(ab_progress.state),
                ab_progress.target_adult_clf_ver, ab_progress.rows_total,
                ab_progress.rows_gated_out, ab_progress.rows_scored,
                ab_progress.rows_updated, ab_progress.rows_failed, ab_progress.last_id);

        /* Terminal [MPROG] line, so the app's maintenance status finalises from
         * the same stream as every other one-shot (the counters ride the
         * extra k=v tail; parseMaintenanceProgress keeps unknown keys as
         * extras). */
        {
            char ab_extra[192];
            snprintf(ab_extra, sizeof ab_extra,
                     "total=%lld gated_out=%lld scored=%lld updated=%lld failed=%lld",
                     ab_progress.rows_total, ab_progress.rows_gated_out,
                     ab_progress.rows_scored, ab_progress.rows_updated,
                     ab_progress.rows_failed);
            mprog_terminal("adult-backfill", ab_rc == 0, ab_rc == 0 ? 0 : 1, ab_extra);
        }

        database_cleanup(&ab_db);
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        cleanup_app_context(&g_app_ctx);
        /* DISABLED and FAILED must both fail loudly: a backfill that cannot run
         * must never look like a success. */
        return (ab_rc == 0) ? 0 : 1;
    }

    /* --movie-kind-backfill <db> --reset: the stage-10 one-shot that clears the
     * movie columns of every row a broken pass labelled, plus the resume and
     * completion meta keys (include/movie_backfill.h). Dispatched before the
     * pass below because it is a pure UPDATE - it needs no model at all - and
     * it must be run with the pass stopped. */
    if (movie_backfill_db && movie_backfill_reset_flag) {
        long long rows_reset = 0;
        int reset_rc = movie_backfill_reset(movie_backfill_db, &rows_reset);
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        cleanup_app_context(&g_app_ctx);
        /* A reset that cannot run must never look like a success. */
        return (reset_rc == 0) ? 0 : 1;
    }

    /* --movie-kind-backfill <db> [--dry-run] [--limit N] [--dump-masks]:
     * one-shot maintenance over the database named on the command line,
     * dispatched exactly like --classify-backfill above - after
     * movie_kind_init(), so the v9 manifest is verified, but before
     * database_init(), so the configured daemon database is never opened. Write
     * mode by default; --dry-run only counts the movie+adult=0 candidates and
     * prints the count; --dump-masks echoes "<mask> <hex(name)>" per labelled
     * row so the real pass's output can be observed (the bench's format). */
    if (movie_backfill_db) {
        movie_backfill_opts_t mb_opts = {
            .target_clf_ver = 0, /* whatever the loaded manifest describes */
            .chunk_ids = 0,      /* engine defaults */
            .batch_rows = 0,
            .dry_run = movie_backfill_dry_run ? 1 : 0,
            .limit_rows = movie_backfill_limit,
            .thermal_backoff_c = config.movie_kind_thermal_backoff_c,
            .dump_masks = dump_masks ? 1 : 0,
        };
        movie_backfill_progress_t mb_progress;
        database_t mb_db;

        memset(&mb_progress, 0, sizeof(mb_progress));
        log_msg(LOG_INFO, "movie-kind-backfill: %s %s ...",
                movie_backfill_dry_run ? "dry run on" : "write pass on", movie_backfill_db);

        rc = database_init(&mb_db, movie_backfill_db, &g_app_ctx);
        if (rc != 0) {
            log_msg(LOG_ERROR, "movie-kind-backfill: cannot open %s: %d", movie_backfill_db,
                    rc);
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        rc = database_create_schema(&mb_db);
        if (rc != 0) {
            log_msg(LOG_ERROR, "movie-kind-backfill: cannot create the schema of %s: %d",
                    movie_backfill_db, rc);
            database_cleanup(&mb_db);
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }

        int mb_rc = movie_backfill_run(&mb_db, &mb_opts, &mb_progress);
        log_msg(mb_rc == 0 ? LOG_INFO : LOG_ERROR,
                "movie-kind-backfill: state=%s target_clf_ver=%lld total=%lld done=%lld "
                "updated=%lld skipped=%lld failed=%lld thermal_pauses=%lld last_id=%lld",
                movie_backfill_state_name(mb_progress.state), mb_progress.target_clf_ver,
                mb_progress.rows_total, mb_progress.rows_done, mb_progress.rows_updated,
                mb_progress.rows_skipped, mb_progress.rows_failed,
                mb_progress.thermal_pauses, mb_progress.last_id);
        if (movie_backfill_dry_run) {
            /* The candidate count is the point of a dry run, so it goes to
             * stdout as well as the log. */
            printf("movie-kind-backfill: dry run, %lld candidate(s) at generation %lld, "
                   "nothing written\n",
                   mb_progress.rows_total, mb_progress.target_clf_ver);
            fflush(stdout);
        }

        database_cleanup(&mb_db);
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        cleanup_app_context(&g_app_ctx);
        /* DISABLED and a failed pass must both fail loudly: a backfill that
         * cannot run must never look like a success. */
        return (mb_rc == 0) ? 0 : 1;
    }

    /* --movie-kind-bench <db> [N] [--dump-masks]: measure the v9 encode pipeline
     * over the first N movie+adult=0 names. Read-only (the bench opens the
     * database SQLITE_OPEN_READONLY) and dispatched here for the same reason as
     * the backfill above: the model must be initialised, the daemon database must
     * not be opened. */
    if (movie_bench_db) {
        int bench_rc = movie_backfill_bench(movie_bench_db, movie_bench_rows,
                                            config.movie_kind_threads,
                                            dump_masks ? 1 : 0);
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        cleanup_app_context(&g_app_ctx);
        return (bench_rc == 0) ? 0 : 1;
    }

    /* Initialize torrent search module for title extraction */
    log_msg(LOG_DEBUG, "Initializing torrent search module...");
    rc = torrent_search_init("torrent_search_keywords.txt");
    if (rc != 0) {
        log_msg(LOG_WARN, "Failed to initialize torrent search module, title extraction will be basic");
    }

    /* The database is never scanned at startup; integrity is checked only
     * when the user explicitly asks for it via --check-database or
     * --recover-database. */

    /* Initialize database */
    log_msg(LOG_DEBUG, "Initializing database: %s", g_app_ctx.db_path);
    rc = database_init(&g_database, g_app_ctx.db_path, &g_app_ctx);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to initialize database: %d", rc);
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        return 1;
    }

    /* Create database schema */
    rc = database_create_schema(&g_database);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Failed to create database schema: %d", rc);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        return 1;
    }

    /* Stage 5: the classifier was initialised before the database existed (it
     * reads the model manifest, not the DB), so its provenance rows land here,
     * right after classifier_meta is guaranteed to exist. */
    classifier_write_meta(g_database.db);

    /* Connect bloom filter to database for write tracking (CRITICAL for correctness)
     * Bloom filter is updated ONLY after successful database writes, not at discovery time.
     * This prevents data loss from failed metadata fetches by allowing retries. */
    if (g_bloom) {
        database_set_bloom(&g_database, g_bloom);
    }

    /*******************************************************************
     * --category-filter-update: delete the rows the current
     * save_categories / save_tv_kinds selection rejects plus every
     * adult-tagged row, then exit. No compaction: a VACUUM INTO of a large
     * database takes long enough that Android's lowmemorykiller reaps the
     * app mid-run, and an explicit --compact is still available. The flag
     * never prompts: the app runs it with no TTY.
     *******************************************************************/
    if (category_filter_update) {
        int cfrc = category_filter_update_run(&g_database, g_app_ctx.db_path,
                                             config.save_category_mask,
                                             config.save_tv_kind_mask);
        database_cleanup(&g_database);
        torrent_search_cleanup();
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);
        if (cfrc != 0) {
            log_msg(LOG_ERROR, "category filter update failed (%d); the database was left in place", cfrc);
            cleanup_app_context(&g_app_ctx);
            return 1;
        }
        log_msg(LOG_INFO, "category filter update complete");
        cleanup_app_context(&g_app_ctx);
        return 0;
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
            hard_block_cleanup();
            language_filter_cleanup();
            bloom_filter_cleanup(g_bloom);
            cleanup_app_context(&g_app_ctx);
        }

        /* Remove any leftover temp file from a previous failed run */
        unlink(tmp_path);

        log_msg(LOG_INFO, "Compacting database into %s ...", tmp_path);
        int crc = database_vacuum_into(&g_database, tmp_path);

        database_cleanup(&g_database);
        torrent_search_cleanup();
        hard_block_cleanup();
        language_filter_cleanup();
        bloom_filter_cleanup(g_bloom);

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
    
    /* Throttle bloom-filter persistence: the in-memory adds still happen on
     * every flush, but the 2x53.9 MB disk write runs at most once per interval. */
    batch_writer_set_bloom_persist_interval(g_batch_writer, config.bloom_persist_interval_sec);

    /* Connect bloom filter to batch writer */
    if (g_bloom && config.bloom_persist) {
        batch_writer_set_bloom(g_batch_writer, g_bloom, config.bloom_path);
    }
    
    /* Configure daily backup */
    if (config.backup_enabled) {
        batch_writer_set_backup(g_batch_writer, config.db_path, config.backup_path);
    }
    
    /* Configure SSH incremental backup */
    if (config.ssh_backup_enabled && config.ssh_host[0]) {
        batch_writer_set_ssh_backup(g_batch_writer,
            config.ssh_host, config.ssh_user,
            config.ssh_dest_path, config.ssh_key_path,
            config.ssh_bookmark_path);
    }
    
    /* Configure rclone incremental backup */
    if (config.rclone_backup_enabled && config.rclone_remote[0]) {
        batch_writer_set_rclone_backup(g_batch_writer,
            config.rclone_remote,
            config.rclone_dest_path,
            config.rclone_bookmark_path);
    }
    
    /* Create supervisor config */
    supervisor_config_t sup_config = {
        .max_trees = config.num_trees,
        .use_keyspace_partitioning = config.use_keyspace_partitioning,
        .batch_writer = g_batch_writer,
        .bloom_filter = g_bloom,
        .inflight = g_inflight,
        .dht_port = config.dht_port,
        /* Bloom filter settings for failure tracking */
        .failure_bloom_capacity = config.failure_bloom_capacity,
        .failure_strike_count = config.failure_strike_count,
        .bloom_error_rate = config.bloom_error_rate,
        .num_find_node_workers = config.tree_find_node_workers,
        .num_bep51_workers = config.tree_bep51_workers,
        .num_get_peers_workers = config.tree_get_peers_workers,
        .num_metadata_workers = config.tree_metadata_workers,
        /* Tree bootstrap settings */
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
        .get_peers_deadline_ms = config.tree_get_peers_deadline_ms,
        .get_peers_fanout = config.tree_get_peers_fanout,
        .get_peers_max_queries = config.tree_get_peers_max_queries,
        .get_peers_round_idle_ms = config.tree_get_peers_round_idle_ms,
        .get_peers_penalize_misses = config.tree_get_peers_penalize_misses,
        .get_peers_slot_budget_ms = config.tree_get_peers_slot_budget_ms,
        .get_peers_finish_on_peers = config.tree_get_peers_finish_on_peers,
        /* Stage 5 settings */
        .tcp_connect_timeout_ms = config.tree_tcp_connect_timeout_ms,
        .parallel_peers = config.tree_parallel_peers,
        .fresh_peer_retries = config.tree_metadata_fresh_peer_retries,
        .metadata_timeout_ms = config.tree_metadata_timeout_ms,
        .metadata_piece_idle_ms = config.tree_metadata_piece_idle_ms,
        .metadata_peer_blacklist_ttl_sec = config.tree_metadata_peer_blacklist_ttl_sec,
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
        /* Supervisor-level global bootstrap */
        .global_bootstrap_target = config.global_bootstrap_target,
        .global_bootstrap_timeout_sec = config.global_bootstrap_timeout_sec,
        .global_bootstrap_workers = config.global_bootstrap_workers,
        /* Stage 6 adult handling (adult_mode is copied after the literal) */
        /* "Categories to save" (Stage: save_categories/save_tv_kinds) */
        .save_category_mask = config.save_category_mask,
        .save_tv_kind_mask = config.save_tv_kind_mask,
        /* Adaptive keyspace partitioning */
        .dead_partition_threshold = config.dead_partition_threshold,
        .max_trees_per_partition = config.max_trees_per_partition
    };
    /* Copy BEP51 cache path (can't use string literal in struct initializer) */
    strncpy(sup_config.bep51_cache_path, config.bep51_cache_path, sizeof(sup_config.bep51_cache_path) - 1);
    strncpy(sup_config.bep51_cache_url, config.bep51_cache_url, sizeof(sup_config.bep51_cache_url) - 1);
    sup_config.bep51_cache_fetch_timeout_sec = config.bep51_cache_fetch_timeout_sec;
    strncpy(sup_config.bep51_cache_ca_file, config.bep51_cache_ca_file, sizeof(sup_config.bep51_cache_ca_file) - 1);
    strncpy(sup_config.bep51_cache_ca_dir, config.bep51_cache_ca_dir, sizeof(sup_config.bep51_cache_ca_dir) - 1);
    strncpy(sup_config.adult_mode, config.adult_mode, sizeof(sup_config.adult_mode) - 1);
    
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
    if (g_supervisor->active_trees == 0) {
        log_msg(LOG_ERROR, "Supervisor started 0 trees (bootstrap failed); aborting");
        batch_writer_cleanup(g_batch_writer);
        database_cleanup(&g_database);
        bloom_filter_cleanup(g_bloom);
        return 1;
    }
    
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
                                              g_refresh_query_store,
                                              g_supervisor->bep51_cache);
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
    rc = http_api_init(&g_http_api, &g_app_ctx, &g_database, g_batch_writer,
                       config.http_port, config.http_bind);
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

    /* Set retry count for /refresh endpoint */
    g_http_api.refresh_retry_count = config.refresh_retry_count;

    /* Start HTTP API */
    log_msg(LOG_DEBUG, "Starting HTTP API server (port %d, bind %s)...", config.http_port, config.http_bind);
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
    
    /* Start the classify-backfill thread: the DB, the classifier and the HTTP
     * API are all up by now, so /stats can serve its object from the first
     * request. Non-blocking - the thread takes its own first decision. */
    /* Which pass that thread runs is a config decision, read once here so the
     * thread never has to look at the config. */
    classify_backfill_daemon_set_mode(config.classify_backfill_mode);
    classify_backfill_daemon_start(&g_app_ctx, &g_database);

    /* The movie-kind backfill thread is deliberately NOT started: production
     * classifies movies inline at ingest (tree_metadata_worker_func), so there
     * is nothing left for a pass to walk. The engine, the --movie-kind-backfill
     * / --reset / --movie-kind-bench CLI and its tests stay compiled, and
     * /stats movie_kind_backfill stays permanently "idle". The shutdown and
     * destroy calls below are no-ops for a thread that never started
     * (src/movie_backfill_daemon.c). */
    
    log_msg(LOG_DEBUG, "DHT crawler (thread tree mode) is running.");
    log_msg(LOG_DEBUG, "  Trees: %d, Workers per tree: BEP51=%d, get_peers=%d, metadata=%d",
            config.num_trees, config.tree_bep51_workers,
            config.tree_get_peers_workers, config.tree_metadata_workers);
    log_msg(LOG_DEBUG, "HTTP API available at http://localhost:%d/", config.http_port);
    log_msg(LOG_DEBUG, "Press Ctrl+C to stop.");
    
    /* Wait for shutdown signal (blocking) */
    while (g_app_ctx.running) {
        sleep(1);
    }
    
    /* Graceful shutdown sequence for thread tree mode */
    log_msg(LOG_DEBUG, "=== Beginning thread tree shutdown sequence ===");
    /* Release any thread blocked in udp_rate_limit_acquire() so shutdown
     * cannot deadlock waiting for a token. */
    udp_rate_limit_shutdown();
    
    /* Stop the classify-backfill thread first: the request is asynchronous
     * (stop flag + cooperative engine stop), so an in-flight pass is bounded to
     * the batch it is in and the join below cannot hang. */
    classify_backfill_daemon_request_shutdown();

    /* Same cooperative stop for the movie-kind thread (it interrupts a thermal
     * pause as well as a batch). */
    movie_backfill_daemon_request_shutdown();
    
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
    /* Join the backfill thread after http_api_stop() and before
     * database_cleanup(): an in-flight batch must commit before the DB closes. */
    classify_backfill_daemon_destroy();

    /* Same rule for the movie-kind thread: join before the database closes. */
    movie_backfill_daemon_destroy();
    supervisor_destroy(g_supervisor);
    cleanup_app_context(&g_app_ctx);
    batch_writer_cleanup(g_batch_writer);
    if (g_refresh_query_store) {
        refresh_query_store_cleanup(g_refresh_query_store);
    }
    database_cleanup(&g_database);
    torrent_search_cleanup();
    hard_block_cleanup();
    language_filter_cleanup();
    bloom_filter_cleanup(g_bloom);

    if (g_inflight) {
        inflight_stats_t inflight_stats;
        inflight_set_stats(g_inflight, &inflight_stats);
        log_msg(LOG_INFO,
                "In-flight set: claimed=%llu refused=%llu released=%llu expired=%llu "
                "tree_released=%llu clears=%llu still_held=%llu",
                (unsigned long long)inflight_stats.claimed,
                (unsigned long long)inflight_stats.refused,
                (unsigned long long)inflight_stats.released,
                (unsigned long long)inflight_stats.expired,
                (unsigned long long)inflight_stats.tree_released,
                (unsigned long long)inflight_stats.clears,
                (unsigned long long)inflight_stats.held);
    }
    inflight_set_destroy(g_inflight);
    g_inflight = NULL;

    log_msg(LOG_DEBUG, "DHT Crawler (thread tree mode) stopped.");
    return 0;
}