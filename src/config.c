#include "config.h"
#include "dht_crawler.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

/* Initialize config with default values */
void config_init_defaults(crawler_config_t *config) {
    if (!config) {
        return;
    }

    memset(config, 0, sizeof(crawler_config_t));
    
    /* DHT defaults */
    config->dht_port = 6881;
    config->max_concurrent_searches = 100;
    config->searches_per_batch = 10;
    config->search_interval_ms = 200;
    
    /* Cache defaults */
    config->cache_enabled = 1;
    config->cache_max_peers = 200;
    config->cache_save_interval_sec = 300;
    
    /* Discovery defaults */
    config->targeted_search_percentage = 30;
    
    /* HTTP defaults */
    config->http_port = 8080;
    
    /* Database defaults */
    strncpy(config->db_path, "data/torrents.db", sizeof(config->db_path) - 1);
    
    /* Logging defaults */
    config->log_level = 1;  /* INFO */
    
    /* Phase 2: Bloom Filter defaults */
    config->bloom_enabled = 1;
    config->bloom_capacity = 10000000;
    config->bloom_error_rate = 0.001;
    config->bloom_persist = 1;
    strncpy(config->bloom_path, "data/bloom.dat", sizeof(config->bloom_path) - 1);
    
    /* Phase 3: Active Exploration defaults */
    config->exploration_enabled = 1;
    config->target_rotation_interval = 10;
    config->find_node_rate = 10;
    
    /* Phase 4: Worker Pool defaults */
    config->scaling_factor = 10;
    config->metadata_workers = 100;

    /* Metadata fetcher defaults */
    config->concurrent_peers_per_torrent = 5;
    config->max_concurrent_connections = 2000;
    config->connection_timeout_sec = 10;
    config->max_metadata_size_mb = 100;
    config->max_retry_attempts = 3;
    config->retry_delay_sec = 120;
    config->retry_enabled = 0;  /* Disabled by default - focus on new discoveries */

    /* Phase 5: Batch Writer defaults */
    config->batch_writes_enabled = 1;
    config->batch_size = 1000;
    config->flush_interval = 60;
    
    /* Phase 6: Shadow Table defaults */
    config->shadow_table_enabled = 1;
    config->shadow_table_capacity = 10000;
    config->shadow_table_prune_interval = 600;
    config->shadow_table_persist = 1;
    strncpy(config->shadow_table_path, "data/shadow_table.dat", sizeof(config->shadow_table_path) - 1);

    /* Phase 7: Active Ping Verification defaults (BUGFIX) */
    config->ping_verification_enabled = 1;
    config->ping_verification_interval = 5;  /* Ping dubious nodes every 5 seconds */
    config->ping_max_dubious_per_cycle = 100;  /* Ping up to 100 nodes per cycle */
    config->ping_dubious_age_threshold = 120;  /* Nodes dubious if no response in 2 minutes */

    /* wbpxre-dht defaults */
    config->wbpxre_ping_workers = 10;
    config->wbpxre_find_node_workers = 20;
    config->wbpxre_sample_infohashes_workers = 50;
    config->wbpxre_get_peers_workers = 100;
    config->wbpxre_query_timeout = 5;
    config->max_routing_table_nodes = 10000;  /* Default: 10000 nodes */

    /* Node health and pruning defaults */
    config->max_node_age_sec = 120;                    /* 2 minutes */
    config->node_verification_batch_size = 100;        /* 100 nodes per cycle */
    config->node_cleanup_interval_sec = 30;            /* 30 seconds */
    config->min_node_response_rate = 0.20;             /* 20% minimum response rate */
    config->node_quality_min_queries = 5;              /* 5 queries minimum */

    /* Node ID rotation defaults */
    config->node_rotation_enabled = 1;
    config->node_rotation_interval_sec = 300;  /* 5 minutes */
    config->node_rotation_drain_timeout_sec = 10;

    /* Peer discovery retry defaults */
    config->peer_retry_enabled = 1;
    config->peer_retry_max_attempts = 3;
    config->peer_retry_min_threshold = 10;
    config->peer_retry_delay_ms = 500;
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
        } else if (strcmp(key, "max_concurrent_searches") == 0) {
            config->max_concurrent_searches = atoi(value);
        } else if (strcmp(key, "searches_per_batch") == 0) {
            config->searches_per_batch = atoi(value);
        } else if (strcmp(key, "search_interval_ms") == 0) {
            config->search_interval_ms = atoi(value);
        } else if (strcmp(key, "cache_enabled") == 0) {
            config->cache_enabled = atoi(value);
        } else if (strcmp(key, "cache_max_peers") == 0) {
            config->cache_max_peers = atoi(value);
        } else if (strcmp(key, "cache_save_interval_sec") == 0) {
            config->cache_save_interval_sec = atoi(value);
        } else if (strcmp(key, "targeted_search_percentage") == 0) {
            config->targeted_search_percentage = atoi(value);
        } else if (strcmp(key, "http_port") == 0) {
            config->http_port = atoi(value);
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
        /* Phase 2: Bloom Filter settings */
        else if (strcmp(key, "bloom_enabled") == 0) {
            config->bloom_enabled = atoi(value);
        } else if (strcmp(key, "bloom_capacity") == 0) {
            config->bloom_capacity = strtoull(value, NULL, 10);
        } else if (strcmp(key, "bloom_error_rate") == 0) {
            config->bloom_error_rate = atof(value);
        } else if (strcmp(key, "bloom_persist") == 0) {
            config->bloom_persist = atoi(value);
        } else if (strcmp(key, "bloom_path") == 0) {
            strncpy(config->bloom_path, value, sizeof(config->bloom_path) - 1);
        }
        /* Phase 3: Active Exploration settings */
        else if (strcmp(key, "exploration_enabled") == 0) {
            config->exploration_enabled = atoi(value);
        } else if (strcmp(key, "target_rotation_interval") == 0) {
            config->target_rotation_interval = atoi(value);
        } else if (strcmp(key, "find_node_rate") == 0) {
            config->find_node_rate = atoi(value);
        }
        /* Phase 4: Worker Pool settings */
        else if (strcmp(key, "scaling_factor") == 0) {
            config->scaling_factor = atoi(value);
        } else if (strcmp(key, "metadata_workers") == 0) {
            config->metadata_workers = atoi(value);
        }
        /* Phase 5: Batch Writer settings */
        else if (strcmp(key, "batch_writes_enabled") == 0) {
            config->batch_writes_enabled = atoi(value);
        } else if (strcmp(key, "batch_size") == 0) {
            config->batch_size = atoi(value);
        } else if (strcmp(key, "flush_interval") == 0) {
            config->flush_interval = atoi(value);
        }
        /* Phase 6: Shadow Table settings */
        else if (strcmp(key, "shadow_table_enabled") == 0) {
            config->shadow_table_enabled = atoi(value);
        } else if (strcmp(key, "shadow_table_capacity") == 0) {
            config->shadow_table_capacity = atoi(value);
        } else if (strcmp(key, "shadow_table_prune_interval") == 0) {
            config->shadow_table_prune_interval = atoi(value);
        } else if (strcmp(key, "shadow_table_persist") == 0) {
            config->shadow_table_persist = atoi(value);
        } else if (strcmp(key, "shadow_table_path") == 0) {
            strncpy(config->shadow_table_path, value, sizeof(config->shadow_table_path) - 1);
        }
        /* Phase 7: Ping Verification settings (BUGFIX) */
        else if (strcmp(key, "ping_verification_enabled") == 0) {
            config->ping_verification_enabled = atoi(value);
        } else if (strcmp(key, "ping_verification_interval") == 0) {
            config->ping_verification_interval = atoi(value);
        } else if (strcmp(key, "ping_max_dubious_per_cycle") == 0) {
            config->ping_max_dubious_per_cycle = atoi(value);
        } else if (strcmp(key, "ping_dubious_age_threshold") == 0) {
            config->ping_dubious_age_threshold = atoi(value);
        }
        /* Metadata fetcher settings */
        else if (strcmp(key, "concurrent_peers_per_torrent") == 0) {
            config->concurrent_peers_per_torrent = atoi(value);
        } else if (strcmp(key, "max_concurrent_connections") == 0) {
            config->max_concurrent_connections = atoi(value);
        } else if (strcmp(key, "connection_timeout_sec") == 0) {
            config->connection_timeout_sec = atoi(value);
        } else if (strcmp(key, "max_metadata_size_mb") == 0) {
            config->max_metadata_size_mb = atoi(value);
        } else if (strcmp(key, "max_retry_attempts") == 0) {
            config->max_retry_attempts = atoi(value);
        } else if (strcmp(key, "retry_delay_sec") == 0) {
            config->retry_delay_sec = atoi(value);
        } else if (strcmp(key, "retry_enabled") == 0) {
            config->retry_enabled = atoi(value);
        }
        /* wbpxre-dht settings */
        else if (strcmp(key, "wbpxre_ping_workers") == 0) {
            config->wbpxre_ping_workers = atoi(value);
        } else if (strcmp(key, "wbpxre_find_node_workers") == 0) {
            config->wbpxre_find_node_workers = atoi(value);
        } else if (strcmp(key, "wbpxre_sample_infohashes_workers") == 0) {
            config->wbpxre_sample_infohashes_workers = atoi(value);
        } else if (strcmp(key, "wbpxre_get_peers_workers") == 0) {
            config->wbpxre_get_peers_workers = atoi(value);
        } else if (strcmp(key, "wbpxre_query_timeout") == 0) {
            config->wbpxre_query_timeout = atoi(value);
        } else if (strcmp(key, "max_routing_table_nodes") == 0) {
            config->max_routing_table_nodes = atoi(value);
        }
        /* Node health and pruning settings */
        else if (strcmp(key, "max_node_age_sec") == 0) {
            config->max_node_age_sec = atoi(value);
        } else if (strcmp(key, "node_verification_batch_size") == 0) {
            config->node_verification_batch_size = atoi(value);
        } else if (strcmp(key, "node_cleanup_interval_sec") == 0) {
            config->node_cleanup_interval_sec = atoi(value);
        } else if (strcmp(key, "min_node_response_rate") == 0) {
            config->min_node_response_rate = atof(value);
        } else if (strcmp(key, "node_quality_min_queries") == 0) {
            config->node_quality_min_queries = atoi(value);
        }
        /* Node ID rotation settings */
        else if (strcmp(key, "node_rotation_enabled") == 0) {
            config->node_rotation_enabled = atoi(value);
        } else if (strcmp(key, "node_rotation_interval_sec") == 0) {
            config->node_rotation_interval_sec = atoi(value);
        } else if (strcmp(key, "node_rotation_drain_timeout_sec") == 0) {
            config->node_rotation_drain_timeout_sec = atoi(value);
        }
        /* Peer discovery retry settings */
        else if (strcmp(key, "peer_retry_enabled") == 0) {
            config->peer_retry_enabled = atoi(value);
        } else if (strcmp(key, "peer_retry_max_attempts") == 0) {
            config->peer_retry_max_attempts = atoi(value);
        } else if (strcmp(key, "peer_retry_min_threshold") == 0) {
            config->peer_retry_min_threshold = atoi(value);
        } else if (strcmp(key, "peer_retry_delay_ms") == 0) {
            config->peer_retry_delay_ms = atoi(value);
        }
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
        {"http-port",   required_argument, 0, 'h'},
        {"db-path",     required_argument, 0, 'd'},
        {"log-level",   required_argument, 0, 'l'},
        {"config",      required_argument, 0, 'c'},
        {"aggressive",  no_argument,       0, 'a'},
        {"help",        no_argument,       0, '?'},
        {0, 0, 0, 0}
    };

    int opt;
    int option_index = 0;

    while ((opt = getopt_long(argc, argv, "p:h:d:l:c:a?", long_options, &option_index)) != -1) {
        switch (opt) {
            case 'p':
                config->dht_port = atoi(optarg);
                break;
            case 'h':
                config->http_port = atoi(optarg);
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
            case 'a':
                /* Aggressive mode: more searches */
                config->searches_per_batch = 20;
                config->search_interval_ms = 100;
                config->max_concurrent_searches = 200;
                log_msg(LOG_DEBUG, "Aggressive mode enabled");
                break;
            case '?':
            default:
                fprintf(stderr, "Usage: %s [OPTIONS]\n", argv[0]);
                fprintf(stderr, "Options:\n");
                fprintf(stderr, "  -p, --port PORT          DHT port (default: 6881)\n");
                fprintf(stderr, "  -h, --http-port PORT     HTTP API port (default: 8080)\n");
                fprintf(stderr, "  -d, --db-path PATH       Database path (default: data/torrents.db)\n");
                fprintf(stderr, "  -l, --log-level LEVEL    Log level: DEBUG|INFO|WARN|ERROR (default: INFO)\n");
                fprintf(stderr, "  -c, --config FILE        Config file path (default: config.ini)\n");
                fprintf(stderr, "  -a, --aggressive         Enable aggressive crawling mode\n");
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
    log_msg(LOG_DEBUG, "  Max Concurrent Searches: %d", config->max_concurrent_searches);
    log_msg(LOG_DEBUG, "  Searches Per Batch: %d", config->searches_per_batch);
    log_msg(LOG_DEBUG, "  Search Interval: %dms", config->search_interval_ms);
    log_msg(LOG_DEBUG, "Cache Settings:");
    log_msg(LOG_DEBUG, "  Enabled: %s", config->cache_enabled ? "yes" : "no");
    log_msg(LOG_DEBUG, "  Max Peers: %d", config->cache_max_peers);
    log_msg(LOG_DEBUG, "  Save Interval: %ds", config->cache_save_interval_sec);
    log_msg(LOG_DEBUG, "Discovery Settings:");
    log_msg(LOG_DEBUG, "  Targeted Search Percentage: %d%%", config->targeted_search_percentage);
    log_msg(LOG_DEBUG, "HTTP API Settings:");
    log_msg(LOG_DEBUG, "  Port: %d", config->http_port);
    log_msg(LOG_DEBUG, "Database Settings:");
    log_msg(LOG_DEBUG, "  Path: %s", config->db_path);
    log_msg(LOG_DEBUG, "Logging:");
    log_msg(LOG_DEBUG, "  Level: %s", log_levels[config->log_level]);
}
