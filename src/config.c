#include "config.h"
#include "dht_crawler.h"
#include "category_filter_cleanup.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

/*
 * Parse a comma-separated save_categories / save_tv_kinds list into a bitmask.
 * Entries are taxonomy names (case-insensitive) or, as a convenience, decimal
 * indices. Unknown entries are logged and ignored; an empty list — or one with
 * no valid entry at all — means "save everything", i.e. every bit set, which
 * is what keeps the filter backward compatible with old configs.
 */
static uint16_t parse_taxonomy_mask(const char *value, int count, uint16_t all,
                                    int (*parse_one)(const char *),
                                    const char *key_name) {
    uint16_t mask = 0;
    char buf[512];

    snprintf(buf, sizeof(buf), "%s", value);

    char *cursor = buf;
    while (cursor && *cursor) {
        char *comma = strchr(cursor, ',');
        if (comma) *comma = '\0';

        int index = parse_one(cursor);
        if (index >= 0 && index < count) {
            mask |= (uint16_t)(1u << index);
        } else {
            log_msg(LOG_WARN, "Unknown %s entry '%s' ignored", key_name, cursor);
        }

        cursor = comma ? comma + 1 : NULL;
    }

    if (mask == 0) {
        return all;
    }
    return mask;
}
/* Initialize config with default values */
void config_init_defaults(crawler_config_t *config) {
    if (!config) {
        return;
    }

    memset(config, 0, sizeof(crawler_config_t));
    
    /* DHT defaults */
    config->dht_port = 6881;
    config->udp_rate_limit_pps = 2500;              /* Outbound DHT UDP packets/sec (0=disabled) */

    /* Database defaults */
    strncpy(config->db_path, "data/torrents.db", sizeof(config->db_path) - 1);
    
    /* Logging defaults */
    config->log_level = 1;  /* INFO */

    /* HTTP API defaults */
    config->http_port = 8080;
    strncpy(config->http_bind, "0.0.0.0", sizeof(config->http_bind) - 1);

    /* Process defaults */
    config->nice_level = 0;
    
    /* Phase 2: Bloom Filter defaults */
    config->bloom_enabled = 1;
    config->bloom_capacity = 10000000;
    config->failure_bloom_capacity = 30000000;  /* 30M entries (same as main bloom) */
    config->failure_strike_count = 2;           /* Two-strike policy */
    config->bloom_error_rate = 0.001;
    config->bloom_persist = 1;
    config->bloom_persist_interval_sec = 300;   /* min seconds between bloom-filter disk saves (0=every flush) */
    strncpy(config->bloom_path, "data/bloom.dat", sizeof(config->bloom_path) - 1);

    /* Metadata fetcher defaults */
    config->max_concurrent_connections = 2000;
    config->tcp_connect_timeout_sec = 10;  /* 10s TCP connection timeout */
    config->connection_timeout_sec = 30;  /* 30s idle timeout - resets on activity */
    config->max_connection_lifetime_sec = 300;  /* 5 minutes max total connection time */

    /* Phase 5: Batch Writer defaults */
    config->batch_size = 1000;
    config->flush_interval = 60;

    /* Daily backup defaults */
    config->backup_enabled = 0;
    strncpy(config->backup_path,
            "/home/adam/external_hdd/dht_crawler_backup/torrent.db.bak",
            sizeof(config->backup_path) - 1);

    /* SSH incremental backup defaults */
    config->ssh_backup_enabled = 0;
    config->ssh_host[0]        = '\0';
    config->ssh_user[0]        = '\0';
    strncpy(config->ssh_dest_path,     "/backups/dht",             sizeof(config->ssh_dest_path)     - 1);
    config->ssh_key_path[0]    = '\0';
    strncpy(config->ssh_bookmark_path, "data/ssh_backup_ids.txt",  sizeof(config->ssh_bookmark_path) - 1);

    /* rclone incremental backup defaults */
    config->rclone_backup_enabled = 0;
    config->rclone_remote[0]      = '\0';
    config->rclone_dest_path[0]   = '\0';
    strncpy(config->rclone_bookmark_path, "data/rclone_backup_ids.txt", sizeof(config->rclone_bookmark_path) - 1);

    /* Supervisor-level global bootstrap defaults */
    config->global_bootstrap_target = 200;
    config->global_bootstrap_timeout_sec = 600;
    config->global_bootstrap_workers = 20;

    /* Language filter defaults 
    config->language_filter_non_latin_threshold = 33; /* Filter if >33% non-Latin characters */

    /* Ingest-time classifier defaults (Stage 5, tag mode) */
    config->classifier_enabled = 1;                 /* On by default; tag mode only */
    strncpy(config->classifier_model_dir, "models/v1", sizeof(config->classifier_model_dir) - 1);

    /* Stage 6 adult handling defaults (calibrated values from models/v1/manifest.json) */
    strncpy(config->adult_mode, "tag", sizeof(config->adult_mode) - 1);
    config->adult_hide_threshold = 0.917039;        /* 0 = take the threshold from the manifest */
    strncpy(config->hard_block_file, "hard_block.txt", sizeof(config->hard_block_file) - 1);
    /* The C default stays "all": a direct binary user sees the historical
     * behaviour. The Android app pins "off" and drives the adult pass from
     * the maintenance action. */
    strncpy(config->classify_backfill_mode, "all", sizeof(config->classify_backfill_mode) - 1);

    /* Movie-kind defaults (movie-kind Stage B): v10 ships in models/v10, 4 threads
     * is the measured steady state, and 82 °C is the backfill engine's pause
     * threshold. */
    config->movie_kind_enabled = 1;
    strncpy(config->movie_kind_model_dir, "models/v10", sizeof(config->movie_kind_model_dir) - 1);
    config->movie_kind_threads = 4;
    config->movie_kind_thermal_backoff_c = 82;

    /* "Categories to save" defaults: every bit set, so nothing is filtered at
     * ingest until save_categories / save_tv_kinds narrow the selection. */
    config->save_category_mask = CATEGORY_FILTER_ALL_CATEGORIES;
    config->save_tv_kind_mask = CATEGORY_FILTER_ALL_TV_KINDS;

    /* Thread tree defaults (Stage 1) */
    config->num_trees = 4;                          /* 4 concurrent thread trees */

    /* Thread tree Stage 2 defaults (find_node/bootstrap) */
    config->tree_find_node_workers = 10;            /* 10 find_node workers per tree */

    /* Thread tree Stage 3 defaults (BEP51) */
    config->tree_bep51_workers = 10;                /* 10 BEP51 workers per tree */
    config->tree_infohash_queue_capacity = 5000;    /* 5000 infohash queue capacity */
    config->tree_bep51_query_interval_ms = 10;      /* 10ms between BEP51 queries */

    /* Thread tree Stage 4 defaults (get_peers) */
    config->tree_get_peers_workers = 8;             /* 8 get_peers workers per tree */
    config->tree_peers_queue_capacity = 2000;       /* 2000 peers queue capacity */
    config->tree_get_peers_timeout_ms = 1500;       /* 1.5s get_peers timeout */
    config->tree_get_peers_deadline_ms = 6000;      /* 6s get_peers lookup deadline (500-8000) */
    config->tree_get_peers_fanout = 8;              /* 8 concurrent get_peers queries per round (2-12) */
    config->tree_get_peers_max_queries = 16;        /* 16 get_peers packets per infohash (8-32, >= fanout) */

    /* Thread tree Stage 5 defaults (metadata) */
    config->tree_metadata_workers = 2;              /* 2 metadata workers per tree */
    config->tree_tcp_connect_timeout_ms = 2000;     /* 2 second TCP connect timeout */
    config->tree_parallel_peers = 2;                /* 2 parallel peer connections per infohash */
    config->tree_metadata_fresh_peer_retries = 1;   /* 1 fresh-peer re-lookup before a strike (0 = off) */
    config->tree_metadata_timeout_ms = 60000;       /* 60s metadata fetch deadline */

    /* Thread tree metadata rate-based respawn defaults */
    config->min_metadata_rate = 0.01;                  /* Floor for dynamic threshold */
    config->dynamic_rate_margin = 0.02;                /* Margin from per-tree average */
    config->tree_rate_check_interval_sec = 60;         /* Check every 60 seconds */
    config->tree_rate_grace_period_sec = 30;           /* 30 second grace period */
    config->tree_min_lifetime_minutes = 10;            /* 10 minute minimum lifetime */
    config->tree_require_empty_queue = 1;              /* Require empty queue before respawn */
    config->tree_rate_ema_alpha = 0.3;                 /* EMA smoothing alpha (default: 0.3) */

    /* Tree respawn overlapping defaults */
    config->respawn_spawn_threshold = 50;              /* Spawn replacement at 50 connections */
    config->respawn_drain_timeout_sec = 120;           /* Force destroy after 120 seconds */
    config->max_draining_trees = 8;                    /* Max 8 draining trees simultaneously */

    /* Thread tree mode toggle - stable and production-ready */
    config->use_thread_trees = 1;                   /* 1=thread tree architecture */

    /* Keyspace partitioning - enabled by default */
    config->use_keyspace_partitioning = 1;          /* 1=enabled (evenly distributed node IDs) */
    config->dead_partition_threshold = 3;            /* 3 consecutive zero-metadata respawns before migration */
    config->max_trees_per_partition = 4;             /* Max 4 trees in one partition */

    /* Refresh thread defaults (for /refresh HTTP endpoint) */
    config->refresh_retry_count = 1;               /* Retry once when /refresh returns 0 peers */
    config->refresh_bootstrap_sample_size = 1000;   /* Sample 1000 nodes from shared pool */
    config->refresh_routing_table_target = 500;     /* Target 500 nodes in routing table */
    config->refresh_ping_workers = 1;               /* 1 ping worker */
    config->refresh_find_node_workers = 1;          /* 1 find_node worker */
    config->refresh_get_peers_workers = 1;          /* 1 get_peers worker */
    config->refresh_request_queue_capacity = 100;   /* 100 request queue capacity */
    config->refresh_get_peers_timeout_ms = 500;     /* 500ms get_peers timeout */
    config->refresh_max_iterations = 3;             /* 3 iterations max */

    /* BEP51 cache defaults */
    strncpy(config->bep51_cache_path, "data/bep51_cache.dat", sizeof(config->bep51_cache_path) - 1);
    config->bep51_cache_capacity = 10000;           /* 10000 nodes max */
    config->bep51_cache_submit_percent = 5;         /* 5% submission rate */
    config->bep51_node_cooldown_sec = 30;           /* 30 second cooldown per node */
}

/* Load config from INI-style file */
int config_load_file(crawler_config_t *config, const char *config_file) {
    if (!config || !config_file) {
        return -1;
    }

    FILE *f = fopen(config_file, "r");
    if (!f) {
        log_msg(LOG_DEBUG, "Config file not found: %s (using defaults)", config_file);
        return -1;
    }

    char line[512];
    int line_num = 0;

    while (fgets(line, sizeof(line), f)) {
        line_num++;
        
        /* Skip comments and empty lines */
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') {
            continue;
        }

        /* Remove trailing newline */
        size_t len = strlen(line);
        if (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) {
            line[len-1] = '\0';
            if (len > 1 && line[len-2] == '\r') {
                line[len-2] = '\0';
            }
        }

        /* Parse key=value */
        char *equals = strchr(line, '=');
        if (!equals) {
            continue;
        }

        *equals = '\0';
        char *key = line;
        char *value = equals + 1;

        /* Trim leading whitespace */
        while (*key == ' ' || *key == '\t') key++;
        while (*value == ' ' || *value == '\t') value++;

        /* Trim trailing whitespace from key */
        char *key_end = key + strlen(key) - 1;
        while (key_end > key && (*key_end == ' ' || *key_end == '\t')) {
            *key_end = '\0';
            key_end--;
        }

        /* Parse settings */
        if (strcmp(key, "dht_port") == 0) {
            config->dht_port = atoi(value);
        } else if (strcmp(key, "db_path") == 0) {
            strncpy(config->db_path, value, sizeof(config->db_path) - 1);
        } else if (strcmp(key, "log_level") == 0) {
            if (strcmp(value, "DEBUG") == 0 || strcmp(value, "0") == 0) {
                config->log_level = 0;
            } else if (strcmp(value, "INFO") == 0 || strcmp(value, "1") == 0) {
                config->log_level = 1;
            } else if (strcmp(value, "WARN") == 0 || strcmp(value, "2") == 0) {
                config->log_level = 2;
            } else if (strcmp(value, "ERROR") == 0 || strcmp(value, "3") == 0) {
                config->log_level = 3;
            }
        }
        /* HTTP API settings */
        else if (strcmp(key, "http_port") == 0) {
            config->http_port = atoi(value);
            if (config->http_port < 1) config->http_port = 1;
            if (config->http_port > 65535) config->http_port = 65535;
        } else if (strcmp(key, "http_bind") == 0) {
            strncpy(config->http_bind, value, sizeof(config->http_bind) - 1);
        }
        /* Process settings */
        else if (strcmp(key, "nice_level") == 0) {
            config->nice_level = atoi(value);
            if (config->nice_level < -20) config->nice_level = -20;
            if (config->nice_level > 19) config->nice_level = 19;
        }
        /* UDP send rate limiting (Stage 2) */
        else if (strcmp(key, "udp_rate_limit_pps") == 0) {
            config->udp_rate_limit_pps = atoi(value);
            if (config->udp_rate_limit_pps != 0) {
                if (config->udp_rate_limit_pps < 100) config->udp_rate_limit_pps = 100;
                if (config->udp_rate_limit_pps > 20000) config->udp_rate_limit_pps = 20000;
            }
        }
        /* Phase 2: Bloom Filter settings */
        else if (strcmp(key, "bloom_enabled") == 0) {
            config->bloom_enabled = atoi(value);
        } else if (strcmp(key, "bloom_capacity") == 0) {
            config->bloom_capacity = strtoull(value, NULL, 10);
        } else if (strcmp(key, "failure_bloom_capacity") == 0) {
            config->failure_bloom_capacity = strtoull(value, NULL, 10);
        } else if (strcmp(key, "failure_strike_count") == 0) {
            config->failure_strike_count = atoi(value);
            if (config->failure_strike_count < 1) config->failure_strike_count = 1;
            if (config->failure_strike_count > 2) config->failure_strike_count = 2;
        } else if (strcmp(key, "bloom_error_rate") == 0) {
            config->bloom_error_rate = atof(value);
        } else if (strcmp(key, "bloom_persist") == 0) {
            config->bloom_persist = atoi(value);
        } else if (strcmp(key, "bloom_persist_interval_sec") == 0) {
            config->bloom_persist_interval_sec = atoi(value);
            if (config->bloom_persist_interval_sec < 0) {
                log_msg(LOG_WARN, "bloom_persist_interval_sec %d out of range (0, or 30-3600); clamping to 0",
                        config->bloom_persist_interval_sec);
                config->bloom_persist_interval_sec = 0;
            }
            if (config->bloom_persist_interval_sec > 0 && config->bloom_persist_interval_sec < 30) {
                log_msg(LOG_WARN, "bloom_persist_interval_sec %d out of range (0, or 30-3600); clamping to 30",
                        config->bloom_persist_interval_sec);
                config->bloom_persist_interval_sec = 30;
            }
            if (config->bloom_persist_interval_sec > 3600) {
                log_msg(LOG_WARN, "bloom_persist_interval_sec %d out of range (0, or 30-3600); clamping to 3600",
                        config->bloom_persist_interval_sec);
                config->bloom_persist_interval_sec = 3600;
            }
        } else if (strcmp(key, "bloom_path") == 0) {
            strncpy(config->bloom_path, value, sizeof(config->bloom_path) - 1);
        }
        /* Phase 5: Batch Writer settings */
        else if (strcmp(key, "batch_size") == 0) {
            config->batch_size = atoi(value);
        } else if (strcmp(key, "flush_interval") == 0) {
            config->flush_interval = atoi(value);
        } else if (strcmp(key, "backup_enabled") == 0) {
            config->backup_enabled = atoi(value);
        } else if (strcmp(key, "backup_path") == 0) {
            strncpy(config->backup_path, value, sizeof(config->backup_path) - 1);
        } else if (strcmp(key, "ssh_backup_enabled") == 0) {
            config->ssh_backup_enabled = atoi(value);
        } else if (strcmp(key, "ssh_host") == 0) {
            strncpy(config->ssh_host, value, sizeof(config->ssh_host) - 1);
        } else if (strcmp(key, "ssh_user") == 0) {
            strncpy(config->ssh_user, value, sizeof(config->ssh_user) - 1);
        } else if (strcmp(key, "ssh_dest_path") == 0) {
            strncpy(config->ssh_dest_path, value, sizeof(config->ssh_dest_path) - 1);
        } else if (strcmp(key, "ssh_key_path") == 0) {
            strncpy(config->ssh_key_path, value, sizeof(config->ssh_key_path) - 1);
        } else if (strcmp(key, "ssh_bookmark_path") == 0) {
            strncpy(config->ssh_bookmark_path, value, sizeof(config->ssh_bookmark_path) - 1);
        } else if (strcmp(key, "rclone_backup_enabled") == 0) {
            config->rclone_backup_enabled = atoi(value);
        } else if (strcmp(key, "rclone_remote") == 0) {
            strncpy(config->rclone_remote, value, sizeof(config->rclone_remote) - 1);
        } else if (strcmp(key, "rclone_dest_path") == 0) {
            strncpy(config->rclone_dest_path, value, sizeof(config->rclone_dest_path) - 1);
        } else if (strcmp(key, "rclone_bookmark_path") == 0) {
            strncpy(config->rclone_bookmark_path, value, sizeof(config->rclone_bookmark_path) - 1);
        }
        /* Metadata fetcher settings */
        else if (strcmp(key, "max_concurrent_connections") == 0) {
            config->max_concurrent_connections = atoi(value);
        } else if (strcmp(key, "tcp_connect_timeout_sec") == 0) {
            config->tcp_connect_timeout_sec = atoi(value);
        } else if (strcmp(key, "connection_timeout_sec") == 0) {
            config->connection_timeout_sec = atoi(value);
        } else if (strcmp(key, "max_connection_lifetime_sec") == 0) {
            config->max_connection_lifetime_sec = atoi(value);
        }
        /* Language filter settings */
        else if (strcmp(key, "language_filter_non_latin_threshold") == 0 ||
                 strcmp(key, "porn_filter_non_latin_threshold") == 0) {
            config->language_filter_non_latin_threshold = atoi(value);
            if (config->language_filter_non_latin_threshold < 0) config->language_filter_non_latin_threshold = 0;
            if (config->language_filter_non_latin_threshold > 100) config->language_filter_non_latin_threshold = 100;
        }
        /* Ingest-time classifier settings (Stage 5, tag mode) */
        else if (strcmp(key, "classifier_enabled") == 0) {
            config->classifier_enabled = atoi(value);
        } else if (strcmp(key, "classifier_model_dir") == 0) {
            strncpy(config->classifier_model_dir, value, sizeof(config->classifier_model_dir) - 1);
        }
        /* Movie-kind settings (movie-kind Stage B) */
        else if (strcmp(key, "movie_kind_enabled") == 0) {
            config->movie_kind_enabled = atoi(value);
        } else if (strcmp(key, "movie_kind_model_dir") == 0) {
            strncpy(config->movie_kind_model_dir, value, sizeof(config->movie_kind_model_dir) - 1);
        } else if (strcmp(key, "movie_kind_threads") == 0) {
            config->movie_kind_threads = atoi(value);
            if (config->movie_kind_threads < 1) config->movie_kind_threads = 1;
            if (config->movie_kind_threads > 8) config->movie_kind_threads = 8;
        } else if (strcmp(key, "movie_kind_thermal_backoff_c") == 0) {
            config->movie_kind_thermal_backoff_c = atoi(value);
            if (config->movie_kind_thermal_backoff_c < 0) config->movie_kind_thermal_backoff_c = 0;
        }
        /* Stage 6 adult handling settings */
        else if (strcmp(key, "adult_mode") == 0) {
            if (strcmp(value, "tag") == 0 || strcmp(value, "drop") == 0) {
                strncpy(config->adult_mode, value, sizeof(config->adult_mode) - 1);
            } else {
                log_msg(LOG_WARN, "Invalid adult_mode '%s' (expected tag|drop); keeping '%s'",
                        value, config->adult_mode);
            }
        } else if (strcmp(key, "adult_hide_threshold") == 0) {
            double threshold = strtod(value, NULL);
            if (threshold < 0.0) threshold = 0.0;
            if (threshold > 1.0) threshold = 1.0;
            config->adult_hide_threshold = threshold;
        } else if (strcmp(key, "classify_backfill_mode") == 0) {
            /* Same shape as adult_mode: enum string, case-sensitive, invalid
             * values warn and keep the previous one. */
            if (strcmp(value, "all") == 0 || strcmp(value, "adult") == 0 ||
                strcmp(value, "off") == 0) {
                strncpy(config->classify_backfill_mode, value,
                        sizeof(config->classify_backfill_mode) - 1);
            } else {
                log_msg(LOG_WARN,
                        "Invalid classify_backfill_mode '%s' (expected all|adult|off); "
                        "keeping '%s'",
                        value, config->classify_backfill_mode);
            }
        } else if (strcmp(key, "hard_block_file") == 0) {
            strncpy(config->hard_block_file, value, sizeof(config->hard_block_file) - 1);
        /* "Categories to save": comma-separated taxonomy names/indices; an
         * empty list means "save everything". */
        } else if (strcmp(key, "save_categories") == 0) {
            config->save_category_mask =
                parse_taxonomy_mask(value, CATEGORY_FILTER_NUM_CATEGORIES,
                                    CATEGORY_FILTER_ALL_CATEGORIES,
                                    category_filter_category_parse, key);
        } else if (strcmp(key, "save_tv_kinds") == 0) {
            config->save_tv_kind_mask =
                parse_taxonomy_mask(value, CATEGORY_FILTER_NUM_TV_KINDS,
                                    CATEGORY_FILTER_ALL_TV_KINDS,
                                    category_filter_tv_kind_parse, key);
        }
        /* Thread tree settings (Stage 1) */
        else if (strcmp(key, "num_trees") == 0) {
            config->num_trees = atoi(value);
            if (config->num_trees < 1) config->num_trees = 1;
            if (config->num_trees > 64) config->num_trees = 64;
        }
        /* Supervisor-level global bootstrap settings */
        else if (strcmp(key, "global_bootstrap_target") == 0) {
            config->global_bootstrap_target = atoi(value);
            if (config->global_bootstrap_target < 200) config->global_bootstrap_target = 200;
            if (config->global_bootstrap_target > 10000) config->global_bootstrap_target = 10000;
        } else if (strcmp(key, "global_bootstrap_timeout_sec") == 0) {
            config->global_bootstrap_timeout_sec = atoi(value);
            if (config->global_bootstrap_timeout_sec < 10) config->global_bootstrap_timeout_sec = 10;
            if (config->global_bootstrap_timeout_sec > 300) config->global_bootstrap_timeout_sec = 300;
        } else if (strcmp(key, "global_bootstrap_workers") == 0) {
            config->global_bootstrap_workers = atoi(value);
            if (config->global_bootstrap_workers < 1) config->global_bootstrap_workers = 1;
            if (config->global_bootstrap_workers > 50) config->global_bootstrap_workers = 50;
        }
        /* Thread tree Stage 2 settings (find_node/bootstrap) */
        else if (strcmp(key, "tree_find_node_workers") == 0) {
            config->tree_find_node_workers = atoi(value);
            if (config->tree_find_node_workers < 1) config->tree_find_node_workers = 1;
            if (config->tree_find_node_workers > 100) config->tree_find_node_workers = 100;
        }
        /* Thread tree Stage 3 settings (BEP51) */
        else if (strcmp(key, "tree_bep51_workers") == 0) {
            config->tree_bep51_workers = atoi(value);
            if (config->tree_bep51_workers < 1) config->tree_bep51_workers = 1;
            if (config->tree_bep51_workers > 100) config->tree_bep51_workers = 100;
        } else if (strcmp(key, "tree_infohash_queue_capacity") == 0) {
            config->tree_infohash_queue_capacity = atoi(value);
            if (config->tree_infohash_queue_capacity < 100) config->tree_infohash_queue_capacity = 100;
        } else if (strcmp(key, "tree_bep51_query_interval_ms") == 0) {
            config->tree_bep51_query_interval_ms = atoi(value);
            if (config->tree_bep51_query_interval_ms < 0) config->tree_bep51_query_interval_ms = 0;
        }
        /* Thread tree Stage 4 settings (get_peers) */
        else if (strcmp(key, "tree_get_peers_workers") == 0) {
            config->tree_get_peers_workers = atoi(value);
            if (config->tree_get_peers_workers < 1) config->tree_get_peers_workers = 1;
            if (config->tree_get_peers_workers > 1000) config->tree_get_peers_workers = 1000;
        } else if (strcmp(key, "tree_peers_queue_capacity") == 0) {
            config->tree_peers_queue_capacity = atoi(value);
            if (config->tree_peers_queue_capacity < 100) config->tree_peers_queue_capacity = 100;
        } else if (strcmp(key, "tree_get_peers_timeout_ms") == 0) {
            config->tree_get_peers_timeout_ms = atoi(value);
            if (config->tree_get_peers_timeout_ms < 100) config->tree_get_peers_timeout_ms = 100;
        } else if (strcmp(key, "tree_get_peers_deadline_ms") == 0) {
            config->tree_get_peers_deadline_ms = atoi(value);
            if (config->tree_get_peers_deadline_ms < 500) {
                log_msg(LOG_WARN, "tree_get_peers_deadline_ms %d out of range (500-8000); clamping to 500",
                        config->tree_get_peers_deadline_ms);
                config->tree_get_peers_deadline_ms = 500;
            }
            if (config->tree_get_peers_deadline_ms > 8000) {
                log_msg(LOG_WARN, "tree_get_peers_deadline_ms %d out of range (500-8000); clamping to 8000",
                        config->tree_get_peers_deadline_ms);
                config->tree_get_peers_deadline_ms = 8000;
            }
        } else if (strcmp(key, "tree_get_peers_fanout") == 0) {
            config->tree_get_peers_fanout = atoi(value);
            if (config->tree_get_peers_fanout < 2) {
                log_msg(LOG_WARN, "tree_get_peers_fanout %d out of range (2-12); clamping to 2",
                        config->tree_get_peers_fanout);
                config->tree_get_peers_fanout = 2;
            }
            if (config->tree_get_peers_fanout > 12) {
                log_msg(LOG_WARN, "tree_get_peers_fanout %d out of range (2-12); clamping to 12",
                        config->tree_get_peers_fanout);
                config->tree_get_peers_fanout = 12;
            }
        } else if (strcmp(key, "tree_get_peers_max_queries") == 0) {
            config->tree_get_peers_max_queries = atoi(value);
            if (config->tree_get_peers_max_queries < 8) {
                log_msg(LOG_WARN, "tree_get_peers_max_queries %d out of range (8-32); clamping to 8",
                        config->tree_get_peers_max_queries);
                config->tree_get_peers_max_queries = 8;
            }
            if (config->tree_get_peers_max_queries > 32) {
                log_msg(LOG_WARN, "tree_get_peers_max_queries %d out of range (8-32); clamping to 32",
                        config->tree_get_peers_max_queries);
                config->tree_get_peers_max_queries = 32;
            }
        }
        /* Thread tree Stage 5 settings (metadata) */
        else if (strcmp(key, "tree_metadata_workers") == 0) {
            config->tree_metadata_workers = atoi(value);
            if (config->tree_metadata_workers < 1) config->tree_metadata_workers = 1;
        } else if (strcmp(key, "tree_tcp_connect_timeout_ms") == 0) {
            config->tree_tcp_connect_timeout_ms = atoi(value);
            if (config->tree_tcp_connect_timeout_ms < 1000) config->tree_tcp_connect_timeout_ms = 1000;
        } else if (strcmp(key, "tree_parallel_peers") == 0) {
            config->tree_parallel_peers = atoi(value);
            if (config->tree_parallel_peers < 1) config->tree_parallel_peers = 1;
            if (config->tree_parallel_peers > 10) config->tree_parallel_peers = 10;
        } else if (strcmp(key, "tree_metadata_fresh_peer_retries") == 0) {
            config->tree_metadata_fresh_peer_retries = atoi(value);
            if (config->tree_metadata_fresh_peer_retries < 0) config->tree_metadata_fresh_peer_retries = 0;
            if (config->tree_metadata_fresh_peer_retries > 3) config->tree_metadata_fresh_peer_retries = 3;
        } else if (strcmp(key, "tree_metadata_timeout_ms") == 0) {
            config->tree_metadata_timeout_ms = atoi(value);
            if (config->tree_metadata_timeout_ms < 5000) config->tree_metadata_timeout_ms = 5000;
            if (config->tree_metadata_timeout_ms > 600000) config->tree_metadata_timeout_ms = 600000;
        }
        /* Thread tree metadata rate-based respawn settings */
        else if (strcmp(key, "min_metadata_rate") == 0) {
            config->min_metadata_rate = atof(value);
            if (config->min_metadata_rate < 0.0) config->min_metadata_rate = 0.0;
        } else if (strcmp(key, "dynamic_rate_margin") == 0) {
            config->dynamic_rate_margin = atof(value);
            if (config->dynamic_rate_margin < 0.0) config->dynamic_rate_margin = 0.0;
        } else if (strcmp(key, "tree_rate_check_interval_sec") == 0) {
            config->tree_rate_check_interval_sec = atoi(value);
            if (config->tree_rate_check_interval_sec < 10) config->tree_rate_check_interval_sec = 10;
        } else if (strcmp(key, "tree_rate_grace_period_sec") == 0) {
            config->tree_rate_grace_period_sec = atoi(value);
            if (config->tree_rate_grace_period_sec < 10) config->tree_rate_grace_period_sec = 10;
        } else if (strcmp(key, "tree_min_lifetime_minutes") == 0) {
            config->tree_min_lifetime_minutes = atoi(value);
            if (config->tree_min_lifetime_minutes < 0) config->tree_min_lifetime_minutes = 0;
        } else if (strcmp(key, "tree_require_empty_queue") == 0) {
            config->tree_require_empty_queue = atoi(value);
        } else if (strcmp(key, "tree_rate_ema_alpha") == 0) {
            config->tree_rate_ema_alpha = atof(value);
            if (config->tree_rate_ema_alpha <= 0.0) config->tree_rate_ema_alpha = 0.01;
            if (config->tree_rate_ema_alpha > 1.0) config->tree_rate_ema_alpha = 1.0;
        }
        /* Tree respawn overlapping configuration */
        else if (strcmp(key, "respawn_spawn_threshold") == 0) {
            config->respawn_spawn_threshold = atoi(value);
            if (config->respawn_spawn_threshold < 0) config->respawn_spawn_threshold = 0;
        } else if (strcmp(key, "respawn_drain_timeout_sec") == 0) {
            config->respawn_drain_timeout_sec = atoi(value);
            if (config->respawn_drain_timeout_sec < 10) config->respawn_drain_timeout_sec = 10;  /* Minimum 10s */
        } else if (strcmp(key, "max_draining_trees") == 0) {
            config->max_draining_trees = atoi(value);
            if (config->max_draining_trees < 1) config->max_draining_trees = 1;
            if (config->max_draining_trees > 32) config->max_draining_trees = 32;  /* Can't drain more than total trees */
        }
        /* Thread tree mode toggle */
        else if (strcmp(key, "use_thread_trees") == 0) {
            config->use_thread_trees = atoi(value);
        }
        /* Keyspace partitioning toggle */
        else if (strcmp(key, "use_keyspace_partitioning") == 0) {
            config->use_keyspace_partitioning = atoi(value);
        } else if (strcmp(key, "dead_partition_threshold") == 0) {
            config->dead_partition_threshold = atoi(value);
            if (config->dead_partition_threshold < 1) config->dead_partition_threshold = 1;
            if (config->dead_partition_threshold > 20) config->dead_partition_threshold = 20;
        } else if (strcmp(key, "max_trees_per_partition") == 0) {
            config->max_trees_per_partition = atoi(value);
            if (config->max_trees_per_partition < 1) config->max_trees_per_partition = 1;
            if (config->max_trees_per_partition > 64) config->max_trees_per_partition = 64;
        }
        /* Refresh thread settings */
        else if (strcmp(key, "refresh_retry_count") == 0) {
            config->refresh_retry_count = atoi(value);
            if (config->refresh_retry_count < 0) config->refresh_retry_count = 0;
            if (config->refresh_retry_count > 5) config->refresh_retry_count = 5;
        } else if (strcmp(key, "refresh_bootstrap_sample_size") == 0) {
            config->refresh_bootstrap_sample_size = atoi(value);
            if (config->refresh_bootstrap_sample_size < 100) config->refresh_bootstrap_sample_size = 100;
            if (config->refresh_bootstrap_sample_size > 10000) config->refresh_bootstrap_sample_size = 10000;
        } else if (strcmp(key, "refresh_routing_table_target") == 0) {
            config->refresh_routing_table_target = atoi(value);
            if (config->refresh_routing_table_target < 50) config->refresh_routing_table_target = 50;
            if (config->refresh_routing_table_target > 2000) config->refresh_routing_table_target = 2000;
        } else if (strcmp(key, "refresh_ping_workers") == 0) {
            config->refresh_ping_workers = atoi(value);
            if (config->refresh_ping_workers < 0) config->refresh_ping_workers = 0;
            if (config->refresh_ping_workers > 10) config->refresh_ping_workers = 10;
        } else if (strcmp(key, "refresh_find_node_workers") == 0) {
            config->refresh_find_node_workers = atoi(value);
            if (config->refresh_find_node_workers < 1) config->refresh_find_node_workers = 1;
            if (config->refresh_find_node_workers > 10) config->refresh_find_node_workers = 10;
        } else if (strcmp(key, "refresh_get_peers_workers") == 0) {
            config->refresh_get_peers_workers = atoi(value);
            if (config->refresh_get_peers_workers < 1) config->refresh_get_peers_workers = 1;
            if (config->refresh_get_peers_workers > 10) config->refresh_get_peers_workers = 10;
        } else if (strcmp(key, "refresh_request_queue_capacity") == 0) {
            config->refresh_request_queue_capacity = atoi(value);
            if (config->refresh_request_queue_capacity < 10) config->refresh_request_queue_capacity = 10;
        } else if (strcmp(key, "refresh_get_peers_timeout_ms") == 0) {
            config->refresh_get_peers_timeout_ms = atoi(value);
            if (config->refresh_get_peers_timeout_ms < 100) config->refresh_get_peers_timeout_ms = 100;
        } else if (strcmp(key, "refresh_max_iterations") == 0) {
            config->refresh_max_iterations = atoi(value);
            if (config->refresh_max_iterations < 1) config->refresh_max_iterations = 1;
            if (config->refresh_max_iterations > 10) config->refresh_max_iterations = 10;
        }
        /* BEP51 cache settings */
        else if (strcmp(key, "bep51_cache_path") == 0) {
            strncpy(config->bep51_cache_path, value, sizeof(config->bep51_cache_path) - 1);
        } else if (strcmp(key, "bep51_cache_capacity") == 0) {
            config->bep51_cache_capacity = atoi(value);
            if (config->bep51_cache_capacity < 1000) config->bep51_cache_capacity = 1000;
            if (config->bep51_cache_capacity > 100000) config->bep51_cache_capacity = 100000;
        } else if (strcmp(key, "bep51_cache_submit_percent") == 0) {
            config->bep51_cache_submit_percent = atoi(value);
            if (config->bep51_cache_submit_percent < 1) config->bep51_cache_submit_percent = 1;
            if (config->bep51_cache_submit_percent > 100) config->bep51_cache_submit_percent = 100;
        } else if (strcmp(key, "bep51_node_cooldown_sec") == 0) {
            config->bep51_node_cooldown_sec = atoi(value);
            if (config->bep51_node_cooldown_sec < 0) config->bep51_node_cooldown_sec = 0;
        }
    }

    /* Cross-key invariant: a lookup can never issue more rounds than the
     * per-infohash packet budget, so max_queries must be at least fanout.
     * Applied once after the whole file is parsed, so the result does not
     * depend on the order the two keys appear in config.ini. */
    if (config->tree_get_peers_max_queries < config->tree_get_peers_fanout) {
        log_msg(LOG_WARN, "tree_get_peers_max_queries %d below fanout %d; raised to %d",
                config->tree_get_peers_max_queries, config->tree_get_peers_fanout,
                config->tree_get_peers_fanout);
        config->tree_get_peers_max_queries = config->tree_get_peers_fanout;
    }

    fclose(f);
    log_msg(LOG_DEBUG, "Loaded configuration from %s", config_file);
    return 0;
}

/* Parse command-line arguments */
int config_parse_args(crawler_config_t *config, int argc, char *argv[]) {
    if (!config) {
        return -1;
    }

    static struct option long_options[] = {
        {"port",        required_argument, 0, 'p'},
        {"db-path",     required_argument, 0, 'd'},
        {"log-level",   required_argument, 0, 'l'},
        {"config",      required_argument, 0, 'c'},
        {"help",        no_argument,       0, '?'},
        {"classify-backfill-mode", required_argument, 0, 'B'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;

    while ((opt = getopt_long(argc, argv, "p:d:l:c?B:", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'p':
                config->dht_port = atoi(optarg);
                break;
            case 'd':
                strncpy(config->db_path, optarg, sizeof(config->db_path) - 1);
                break;
            case 'l':
                if (strcmp(optarg, "DEBUG") == 0) {
                    config->log_level = 0;
                } else if (strcmp(optarg, "INFO") == 0) {
                    config->log_level = 1;
                } else if (strcmp(optarg, "WARN") == 0) {
                    config->log_level = 2;
                } else if (strcmp(optarg, "ERROR") == 0) {
                    config->log_level = 3;
                } else {
                    config->log_level = atoi(optarg);
                }
                break;
            case 'c':
                /* Config file is loaded separately */
                break;
            case 'B':
                /* Same validation as the config-file key, so the CLI can
                 * neither widen the enum nor silently ignore a typo. */
                if (strcmp(optarg, "all") == 0 || strcmp(optarg, "adult") == 0 ||
                    strcmp(optarg, "off") == 0) {
                    strncpy(config->classify_backfill_mode, optarg,
                            sizeof(config->classify_backfill_mode) - 1);
                } else {
                    fprintf(stderr,
                            "--classify-backfill-mode takes all|adult|off, got '%s'\n",
                            optarg);
                    return -1;
                }
                break;
            case '?':
            default:
                fprintf(stderr, "Usage: %s [OPTIONS]\n", argv[0]);
                fprintf(stderr, "Options:\n");
                fprintf(stderr, "  -p, --port PORT          DHT port (default: 6881)\n");
                fprintf(stderr, "  -d, --db-path PATH       Database path (default: data/torrents.db)\n");
                fprintf(stderr, "  -l, --log-level LEVEL    Log level: DEBUG|INFO|WARN|ERROR (default: INFO)\n");
                fprintf(stderr, "  -c, --config FILE        Config file path (default: config.ini)\n");
                fprintf(stderr, "      --classify-backfill-mode MODE  all|adult|off (default: all)\n");
                fprintf(stderr, "  -?, --help               Show this help message\n");
                return -1;
        }
    }

    return 0;
}

/* Print current configuration */
void config_print(const crawler_config_t *config) {
    if (!config) {
        return;
    }

    const char *log_levels[] = {"DEBUG", "INFO", "WARN", "ERROR"};

    log_msg(LOG_DEBUG, "=== DHT Crawler Configuration ===");
    log_msg(LOG_DEBUG, "DHT Settings:");
    log_msg(LOG_DEBUG, "  Port: %d", config->dht_port);
    log_msg(LOG_DEBUG, "  get_peers: deadline_ms=%d fanout=%d max_queries=%d",
            config->tree_get_peers_deadline_ms, config->tree_get_peers_fanout,
            config->tree_get_peers_max_queries);
    log_msg(LOG_DEBUG, "Database Settings:");
    log_msg(LOG_DEBUG, "  Path: %s", config->db_path);
    log_msg(LOG_DEBUG, "Logging:");
    log_msg(LOG_DEBUG, "  Level: %s", log_levels[config->log_level]);
    log_msg(LOG_DEBUG, "Classifier Settings:");
    log_msg(LOG_DEBUG, "  classify_backfill_mode: %s",
            config->classify_backfill_mode[0] ? config->classify_backfill_mode : "all");
    log_msg(LOG_DEBUG, "Bloom persist interval: %d sec", config->bloom_persist_interval_sec);
}

