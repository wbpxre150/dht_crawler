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

    /* Phase 4: Worker Pool defaults */
    config->scaling_factor = 10;
    config->metadata_workers = 100;

    /* Metadata fetcher defaults */
    config->max_concurrent_connections = 2000;
    config->tcp_connect_timeout_sec = 10;  /* 10s TCP connection timeout */
    config->connection_timeout_sec = 30;  /* 30s idle timeout - resets on activity */
    config->max_connection_lifetime_sec = 300;  /* 5 minutes max total connection time */
    config->max_metadata_size_mb = 100;
    config->max_retry_attempts = 3;
    config->retry_delay_sec = 120;

    /* Phase 5: Batch Writer defaults */
    config->batch_writes_enabled = 1;
    config->batch_size = 1000;
    config->flush_interval = 60;

    /* wbpxre-dht defaults */
    config->wbpxre_ping_workers = 10;
    config->wbpxre_find_node_workers = 20;
    config->wbpxre_sample_infohashes_workers = 50;
    config->wbpxre_get_peers_workers = 100;
    config->wbpxre_query_timeout = 5;
    config->max_routing_table_nodes = 10000;  /* Default: 10000 nodes */

    /* Peer discovery retry defaults */
    config->peer_retry_enabled = 1;
    config->peer_retry_max_attempts = 3;
    config->peer_retry_min_threshold = 10;
    config->peer_retry_delay_ms = 500;
    config->peer_retry_cleanup_interval_sec = 10;
    config->peer_retry_max_entries = 50000;

    /* Triple routing table defaults */
    config->triple_routing_threshold = 1500;        /* Rotate when filling table reaches this count */
    config->triple_routing_rotation_time = 60;      /* Minimum time between rotations (seconds) */

    /* Pornography content filter defaults */
    config->porn_filter_enabled = 0;                /* Disabled by default */
    strncpy(config->porn_filter_keyword_file, "porn_filter_keywords.txt", sizeof(config->porn_filter_keyword_file) - 1);
    config->porn_filter_keyword_threshold = 8;      /* Min weight for keyword match */
    config->porn_filter_regex_threshold = 9;        /* Min weight for regex match */
    config->porn_filter_heuristic_threshold = 5;    /* Min score for heuristic match */

    /* Thread tree defaults (Stage 1) */
    config->num_trees = 4;                          /* 4 concurrent thread trees */
    config->min_metadata_rate = 0.5;                /* Min 0.5 metadata/sec before restart */

    /* Thread tree Stage 2 defaults */
    config->tree_bootstrap_timeout_sec = 30;        /* 30 second bootstrap timeout */
    config->tree_routing_threshold = 200;           /* 200 nodes before BEP51 phase (lowered from 500) */

    /* Thread tree Stage 3 defaults (BEP51) */
    config->tree_bep51_workers = 10;                /* 10 BEP51 workers per tree */
    config->tree_infohash_queue_capacity = 5000;    /* 5000 infohash queue capacity */
    config->tree_bep51_query_interval_ms = 10;      /* 10ms between BEP51 queries */

    /* Thread tree Stage 4 defaults (get_peers) */
    config->tree_get_peers_workers = 500;           /* 500 get_peers workers per tree */
    config->tree_peers_queue_capacity = 2000;       /* 2000 peers queue capacity */
    config->tree_get_peers_timeout_ms = 3000;       /* 3 second get_peers timeout */

    /* Thread tree Stage 5 defaults (metadata) */
    config->tree_metadata_workers = 2;              /* 2 metadata workers per tree */
    config->tree_rate_check_interval_sec = 10;      /* 10 second rate check interval */
    config->tree_rate_grace_period_sec = 30;        /* 30 second grace period */
    config->tree_tcp_connect_timeout_ms = 5000;     /* 5 second TCP connect timeout */

    /* Thread tree mode toggle - disabled by default for safety */
    config->use_thread_trees = 0;                   /* 0=old architecture */
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
        /* Metadata fetcher settings */
        else if (strcmp(key, "max_concurrent_connections") == 0) {
            config->max_concurrent_connections = atoi(value);
        } else if (strcmp(key, "tcp_connect_timeout_sec") == 0) {
            config->tcp_connect_timeout_sec = atoi(value);
        } else if (strcmp(key, "connection_timeout_sec") == 0) {
            config->connection_timeout_sec = atoi(value);
        } else if (strcmp(key, "max_connection_lifetime_sec") == 0) {
            config->max_connection_lifetime_sec = atoi(value);
        } else if (strcmp(key, "max_metadata_size_mb") == 0) {
            config->max_metadata_size_mb = atoi(value);
        } else if (strcmp(key, "max_retry_attempts") == 0) {
            config->max_retry_attempts = atoi(value);
        } else if (strcmp(key, "retry_delay_sec") == 0) {
            config->retry_delay_sec = atoi(value);
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
        /* Peer discovery retry settings */
        else if (strcmp(key, "peer_retry_enabled") == 0) {
            config->peer_retry_enabled = atoi(value);
        } else if (strcmp(key, "peer_retry_max_attempts") == 0) {
            config->peer_retry_max_attempts = atoi(value);
        } else if (strcmp(key, "peer_retry_min_threshold") == 0) {
            config->peer_retry_min_threshold = atoi(value);
        } else if (strcmp(key, "peer_retry_delay_ms") == 0) {
            config->peer_retry_delay_ms = atoi(value);
        } else if (strcmp(key, "peer_retry_cleanup_interval_sec") == 0) {
            config->peer_retry_cleanup_interval_sec = atoi(value);
            if (config->peer_retry_cleanup_interval_sec < 1) config->peer_retry_cleanup_interval_sec = 1;
        } else if (strcmp(key, "peer_retry_max_entries") == 0) {
            config->peer_retry_max_entries = atoi(value);
            if (config->peer_retry_max_entries < 1000) config->peer_retry_max_entries = 1000;
        }
        /* Triple routing table settings */
        else if (strcmp(key, "triple_routing_threshold") == 0) {
            config->triple_routing_threshold = (uint32_t)atoi(value);
            /* Minimum threshold of 100 nodes */
            if (config->triple_routing_threshold < 100) config->triple_routing_threshold = 100;
        }
        else if (strcmp(key, "triple_routing_rotation_time") == 0) {
            config->triple_routing_rotation_time = atoi(value);
            /* Minimum rotation time of 10 seconds */
            if (config->triple_routing_rotation_time < 10) config->triple_routing_rotation_time = 10;
        }
        /* Pornography content filter settings */
        else if (strcmp(key, "porn_filter_enabled") == 0) {
            config->porn_filter_enabled = atoi(value);
        } else if (strcmp(key, "porn_filter_keyword_file") == 0) {
            strncpy(config->porn_filter_keyword_file, value, sizeof(config->porn_filter_keyword_file) - 1);
        } else if (strcmp(key, "porn_filter_keyword_threshold") == 0) {
            config->porn_filter_keyword_threshold = atoi(value);
            if (config->porn_filter_keyword_threshold < 1) config->porn_filter_keyword_threshold = 1;
            if (config->porn_filter_keyword_threshold > 10) config->porn_filter_keyword_threshold = 10;
        } else if (strcmp(key, "porn_filter_regex_threshold") == 0) {
            config->porn_filter_regex_threshold = atoi(value);
            if (config->porn_filter_regex_threshold < 1) config->porn_filter_regex_threshold = 1;
            if (config->porn_filter_regex_threshold > 10) config->porn_filter_regex_threshold = 10;
        } else if (strcmp(key, "porn_filter_heuristic_threshold") == 0) {
            config->porn_filter_heuristic_threshold = atoi(value);
            if (config->porn_filter_heuristic_threshold < 0) config->porn_filter_heuristic_threshold = 0;
            if (config->porn_filter_heuristic_threshold > 20) config->porn_filter_heuristic_threshold = 20;
        }
        /* Thread tree settings (Stage 1) */
        else if (strcmp(key, "num_trees") == 0) {
            config->num_trees = atoi(value);
            if (config->num_trees < 1) config->num_trees = 1;
            if (config->num_trees > 64) config->num_trees = 64;
        } else if (strcmp(key, "min_metadata_rate") == 0) {
            config->min_metadata_rate = atof(value);
            if (config->min_metadata_rate < 0.0) config->min_metadata_rate = 0.0;
        }
        /* Thread tree Stage 2 settings */
        else if (strcmp(key, "tree_bootstrap_timeout_sec") == 0) {
            config->tree_bootstrap_timeout_sec = atoi(value);
            if (config->tree_bootstrap_timeout_sec < 5) config->tree_bootstrap_timeout_sec = 5;
        } else if (strcmp(key, "tree_routing_threshold") == 0) {
            config->tree_routing_threshold = atoi(value);
            if (config->tree_routing_threshold < 10) config->tree_routing_threshold = 10;
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
        }
        /* Thread tree Stage 5 settings (metadata) */
        else if (strcmp(key, "tree_metadata_workers") == 0) {
            config->tree_metadata_workers = atoi(value);
            if (config->tree_metadata_workers < 1) config->tree_metadata_workers = 1;
            if (config->tree_metadata_workers > 50) config->tree_metadata_workers = 50;
        } else if (strcmp(key, "tree_rate_check_interval_sec") == 0) {
            config->tree_rate_check_interval_sec = atoi(value);
            if (config->tree_rate_check_interval_sec < 5) config->tree_rate_check_interval_sec = 5;
        } else if (strcmp(key, "tree_rate_grace_period_sec") == 0) {
            config->tree_rate_grace_period_sec = atoi(value);
            if (config->tree_rate_grace_period_sec < 10) config->tree_rate_grace_period_sec = 10;
        } else if (strcmp(key, "tree_tcp_connect_timeout_ms") == 0) {
            config->tree_tcp_connect_timeout_ms = atoi(value);
            if (config->tree_tcp_connect_timeout_ms < 1000) config->tree_tcp_connect_timeout_ms = 1000;
        }
        /* Thread tree mode toggle */
        else if (strcmp(key, "use_thread_trees") == 0) {
            config->use_thread_trees = atoi(value);
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
                /* Aggressive mode (placeholder - no longer used) */
                log_msg(LOG_DEBUG, "Aggressive mode flag ignored (deprecated)");
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
    log_msg(LOG_DEBUG, "Discovery Settings:");
    log_msg(LOG_DEBUG, "  Targeted Search Percentage: %d%%", config->targeted_search_percentage);
    log_msg(LOG_DEBUG, "HTTP API Settings:");
    log_msg(LOG_DEBUG, "  Port: %d", config->http_port);
    log_msg(LOG_DEBUG, "Database Settings:");
    log_msg(LOG_DEBUG, "  Path: %s", config->db_path);
    log_msg(LOG_DEBUG, "Logging:");
    log_msg(LOG_DEBUG, "  Level: %s", log_levels[config->log_level]);
}
