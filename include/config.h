#ifndef CONFIG_H
#define CONFIG_H

#include <stdint.h>

/* Configuration structure */
typedef struct {
    /* DHT settings */
    int dht_port;
    int max_concurrent_searches;
    int searches_per_batch;
    int search_interval_ms;
    
    /* Cache settings */
    int cache_enabled;
    int cache_max_peers;
    int cache_save_interval_sec;
    
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
    
    /* Phase 3: Active Exploration settings */
    int exploration_enabled;
    int target_rotation_interval;    /* seconds */
    int find_node_rate;              /* queries per second */
    
    /* Phase 4: Worker Pool settings */
    int scaling_factor;
    int metadata_workers;

    /* Metadata fetcher settings */
    int concurrent_peers_per_torrent;
    int max_concurrent_connections;
    int connection_timeout_sec;         /* Idle timeout - resets on activity */
    int max_connection_lifetime_sec;    /* Max total connection time (0=unlimited) */
    int max_metadata_size_mb;
    int max_retry_attempts;
    int retry_delay_sec;
    int retry_enabled;  /* 0=disabled, 1=enabled */

    /* Phase 5: Batch Writer settings */
    int batch_writes_enabled;
    int batch_size;
    int flush_interval;              /* seconds */
    
    /* Phase 6: Shadow Routing Table settings */
    int shadow_table_enabled;
    int shadow_table_capacity;
    int shadow_table_prune_interval; /* seconds */
    int shadow_table_persist;
    char shadow_table_path[512];

    /* Phase 7: Active Ping Verification settings (BUGFIX) */
    int ping_verification_enabled;
    int ping_verification_interval; /* seconds */
    int ping_max_dubious_per_cycle; /* max dubious nodes to ping per cycle */
    int ping_dubious_age_threshold; /* seconds since last response */

    /* wbpxre-dht settings */
    int wbpxre_ping_workers;
    int wbpxre_find_node_workers;
    int wbpxre_sample_infohashes_workers;
    int wbpxre_get_peers_workers;
    int wbpxre_query_timeout;
    int max_routing_table_nodes;         /* Maximum nodes in routing table */

    /* Node health and pruning settings */
    int maintenance_thread_enabled;      /* Enable maintenance thread (0=disabled, 1=enabled) */
    int max_node_age_sec;                /* Consider nodes old after this many seconds */
    int node_verification_batch_size;    /* Verify this many old nodes per cycle */
    int node_cleanup_interval_sec;       /* Clean dropped nodes every N seconds */
    double min_node_response_rate;       /* Evict nodes with response rate below this */
    int node_quality_min_queries;        /* Minimum queries before judging quality */

    /* Node ID rotation settings */
    int node_rotation_enabled;           /* Enable periodic node ID rotation */
    int node_rotation_interval_sec;      /* Rotation interval in seconds */
    int node_rotation_drain_timeout_sec; /* Time to wait for in-flight operations */
    int rotation_phase_duration_sec;     /* Duration of ANNOUNCING and TRANSITIONING phases */
    int clear_sample_queue_on_rotation;  /* Clear sample_infohashes queue after rotation */

    /* Peer discovery retry settings */
    int peer_retry_enabled;              /* Enable multi-retry peer discovery */
    int peer_retry_max_attempts;         /* Max get_peers retries (1-5, default: 3) */
    int peer_retry_min_threshold;        /* Min peers before stopping retries (default: 10) */
    int peer_retry_delay_ms;             /* Delay between retries in ms (default: 500) */

    /* BEP51-focused pruning settings */
    int bep51_pruning_enabled;           /* Enable BEP51-focused node pruning */
    int bep51_pruning_interval_sec;      /* How often to check and prune (default: 30) */
    double bep51_pruning_min_capacity;   /* Min capacity ratio (0.0-1.0) before pruning (default: 0.0) */

    /* Async node pruning settings (timer-based) */
    int async_pruning_enabled;           /* Enable async pruning */
    int async_pruning_interval_sec;      /* How often to run pruning (seconds) */
    int async_pruning_target_nodes;      /* Target final node count after pruning */
    double async_pruning_distant_percent; /* Percentage of distant nodes to remove (0.0-100.0) */
    double async_pruning_old_percent;    /* Percentage of old nodes to remove (0.0-100.0) */
    int async_pruning_batch_size;        /* Nodes per batch for drop operations */
    int async_pruning_log_interval;      /* Log progress every N nodes */
    int async_pruning_workers;           /* Number of worker threads (default: 4) */
    int async_pruning_delete_chunk_size; /* Nodes per delete chunk (default: 100) */
    double async_pruning_min_capacity_percent; /* Min capacity % before pruning activates (0.0-100.0) */
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
