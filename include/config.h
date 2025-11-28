#ifndef CONFIG_H
#define CONFIG_H

#include <stdint.h>

/* Configuration structure */
typedef struct {
    /* DHT settings */
    int dht_port;

    /* Discovery settings */
    int targeted_search_percentage;  /* 0-100 */
    
    /* HTTP API settings */
    int http_port;
    
    /* Database settings */
    char db_path[512];
    
    /* Logging */
    int log_level;  /* 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR */
    
    /* Phase 2: Bloom Filter settings */
    int bloom_enabled;
    uint64_t bloom_capacity;
    double bloom_error_rate;
    int bloom_persist;
    char bloom_path[512];
    
    /* Phase 4: Worker Pool settings */
    int scaling_factor;
    int metadata_workers;

    /* Metadata fetcher settings */
    int max_concurrent_connections;
    int tcp_connect_timeout_sec;        /* TCP connection establishment timeout */
    int connection_timeout_sec;         /* Idle timeout - resets on activity */
    int max_connection_lifetime_sec;    /* Max total connection time (0=unlimited) */
    int max_metadata_size_mb;
    int max_retry_attempts;
    int retry_delay_sec;

    /* Phase 5: Batch Writer settings */
    int batch_writes_enabled;
    int batch_size;
    int flush_interval;              /* seconds */

    /* wbpxre-dht settings */
    int wbpxre_ping_workers;
    int wbpxre_find_node_workers;
    int wbpxre_sample_infohashes_workers;
    int wbpxre_get_peers_workers;
    int wbpxre_query_timeout;
    int max_routing_table_nodes;         /* Maximum nodes in routing table */

    /* Peer discovery retry settings */
    int peer_retry_enabled;              /* Enable multi-retry peer discovery */
    int peer_retry_max_attempts;         /* Max get_peers retries (1-5, default: 3) */
    int peer_retry_min_threshold;        /* Min peers before stopping retries (default: 10) */
    int peer_retry_delay_ms;             /* Delay between retries in ms (default: 500) */
    int peer_retry_cleanup_interval_sec; /* How often to cleanup old entries (default: 10) */
    int peer_retry_max_entries;          /* Max entries in tracker before eviction (default: 50000) */

    /* Triple routing table settings */
    uint32_t triple_routing_threshold;    /* Node count to trigger rotation (default: 1500) */
    int triple_routing_rotation_time;     /* Minimum time between rotations in seconds (default: 60) */

    /* Pornography content filter settings */
    int porn_filter_enabled;             /* Enable pornography content filter (0=disabled, 1=enabled) */
    char porn_filter_keyword_file[512];  /* Path to keyword file */
    int porn_filter_keyword_threshold;   /* Min weight for keyword match (1-10, default: 8) */
    int porn_filter_regex_threshold;     /* Min weight for regex match (1-10, default: 9) */
    int porn_filter_heuristic_threshold; /* Min score for heuristic match (0-20, default: 5) */

    /* Thread tree settings (Stage 1) */
    int num_trees;                       /* Number of concurrent thread trees (default: 4) */

    /* Thread tree Stage 2 settings (Global Bootstrap - NEW) */
    int global_bootstrap_target;         /* Target nodes for shared pool (default: 5000) */
    int global_bootstrap_timeout_sec;    /* Global bootstrap timeout (default: 60) */
    int global_bootstrap_workers;        /* Bootstrap worker threads (default: 50) */
    int per_tree_sample_size;            /* Nodes each tree samples from pool (default: 1000) */

    /* Thread tree Stage 2 settings (find_node/bootstrap) */
    int tree_find_node_workers;          /* find_node workers per tree for bootstrap (default: 10) */

    /* Thread tree Stage 3 settings (BEP51) */
    int tree_bep51_workers;              /* BEP51 workers per tree (default: 10) */
    int tree_infohash_queue_capacity;    /* Infohash queue size per tree (default: 5000) */
    int tree_bep51_query_interval_ms;    /* Delay between BEP51 queries (default: 10) */

    /* Thread tree Stage 4 settings (get_peers) */
    int tree_get_peers_workers;          /* get_peers workers per tree (default: 500) */
    int tree_peers_queue_capacity;       /* Peers queue size per tree (default: 2000) */
    int tree_get_peers_timeout_ms;       /* get_peers response timeout (default: 3000) */

    /* Find_node throttling settings */
    int tree_infohash_pause_threshold;   /* Queue size to pause find_node workers (default: 2000) */
    int tree_infohash_resume_threshold;  /* Queue size to resume find_node workers (default: 1000) */

    /* Get_peers throttling settings */
    int tree_peers_pause_threshold;      /* Peers queue size to pause get_peers workers (default: 2000) */
    int tree_peers_resume_threshold;     /* Peers queue size to resume get_peers workers (default: 1000) */

    /* Thread tree Stage 5 settings (metadata) */
    int tree_metadata_workers;           /* Metadata workers per tree (default: 2) */
    int tree_tcp_connect_timeout_ms;     /* TCP connect timeout (default: 5000) */

    /* Thread tree bloom-based respawn settings */
    double tree_max_bloom_duplicate_rate;   /* Max bloom duplicate rate before respawn (default: 0.70) */
    int tree_bloom_check_interval_sec;      /* Bloom rate check interval (default: 60) */
    int tree_bloom_check_sample_size;       /* Min samples before check (default: 100) */
    int tree_bloom_grace_period_sec;        /* Grace period before respawn (default: 120) */
    int tree_bloom_min_lifetime_minutes;    /* Min lifetime before bloom checks (default: 10) */

    /* Thread tree mode toggle */
    int use_thread_trees;                /* 0=old architecture, 1=new thread tree architecture */

    /* Keyspace partitioning settings */
    int use_keyspace_partitioning;       /* Enable keyspace partitioning (0=random node IDs, 1=partitioned, default: 1) */

    /* Refresh thread settings (for /refresh HTTP endpoint) */
    int refresh_bootstrap_sample_size;   /* Nodes to sample from shared pool (default: 1000) */
    int refresh_routing_table_target;    /* Target routing table size (default: 500) */
    int refresh_ping_workers;            /* Ping workers (default: 1) */
    int refresh_find_node_workers;       /* Find_node workers (default: 1) */
    int refresh_get_peers_workers;       /* Get_peers workers (default: 1) */
    int refresh_request_queue_capacity;  /* Request queue capacity (default: 100) */
    int refresh_get_peers_timeout_ms;    /* Get_peers response timeout (default: 500) */
    int refresh_max_iterations;          /* Max get_peers iterations (default: 3) */

    /* BEP51 cache settings */
    char bep51_cache_path[512];          /* Cache file path (default: "data/bep51_cache.dat") */
    int bep51_cache_capacity;            /* Max nodes in cache (default: 10000) */
    int bep51_cache_submit_percent;      /* % of BEP51 responses to cache (default: 5) */
} crawler_config_t;

/* Initialize config with default values */
void config_init_defaults(crawler_config_t *config);

/* Load config from file (returns 0 on success, -1 on error) */
int config_load_file(crawler_config_t *config, const char *config_file);

/* Parse command-line arguments (returns 0 on success, -1 on error) */
int config_parse_args(crawler_config_t *config, int argc, char *argv[]);

/* Print current configuration */
void config_print(const crawler_config_t *config);

#endif /* CONFIG_H */
