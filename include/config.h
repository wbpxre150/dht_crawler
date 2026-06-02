#ifndef CONFIG_H
#define CONFIG_H

#include <stdint.h>

/* Configuration structure */
typedef struct {
    /*
     * Live fields. The 22 dead fields removed in Stage 1 are documented in
     * CONFIG_INI.md (one-pager post-Stage 6). When auditing this struct, the
     * consumers are:
     *   - main.c (dht_port, db_path, log_level, bloom_*, batch_size, backup_*,
     *     ssh_*, rclone_*, porn_filter_*, language_filter_*)
     *   - supervisor.c (dht_port, num_trees, tree_*, bep51_*, refresh_*,
     *     respawn_*, min_metadata_rate, dynamic_rate_margin, etc.)
     *   - tree_*.c (tree_* via supervisor config)
     *   - tree_metadata.c (tree_tcp_connect_timeout_ms, tree_parallel_peers)
     *   - batch_writer.c (batch_size, flush_interval, backup_path)
     *   - refresh_thread.c (refresh_*)
     *   - language_filter.c (language_filter_non_latin_threshold)
     *   - porn_filter.c (porn_filter_enabled, porn_filter_keyword_file)
     *   - failure_bloom filter (failure_bloom_capacity)
     */

    /* DHT settings */
    int dht_port;

    /* Database settings */
    char db_path[512];

    /* Logging */
    int log_level;  /* 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR */

    /* Phase 2: Bloom Filter settings */
    int bloom_enabled;
    uint64_t bloom_capacity;
    uint64_t failure_bloom_capacity;  /* NEW: Capacity for failure bloom filter */
    double bloom_error_rate;
    int bloom_persist;
    char bloom_path[512];

    /* Metadata fetcher settings */
    int max_concurrent_connections;
    int tcp_connect_timeout_sec;        /* TCP connection establishment timeout */
    int connection_timeout_sec;         /* Idle timeout - resets on activity */
    int max_connection_lifetime_sec;    /* Max total connection time (0=unlimited) */

    /* Phase 5: Batch Writer settings */
    int batch_size;
    int flush_interval;              /* seconds */

    /* Daily backup settings */
    int backup_enabled;
    char backup_path[1024];          /* Destination path; %DATE% replaced with YYYY-MM-DD */

    /* SSH incremental backup */
    int  ssh_backup_enabled;
    char ssh_host[256];
    char ssh_user[128];
    char ssh_dest_path[1024];
    char ssh_key_path[512];
    char ssh_bookmark_path[1024];

    /* rclone incremental backup */
    int  rclone_backup_enabled;
    char rclone_remote[256];         /* rclone remote name, e.g. "r2" */
    char rclone_dest_path[1024];     /* bucket path, e.g. "dht-backup/dht_crawler_backup" */
    char rclone_bookmark_path[1024]; /* local bookmark file */

    /* Pornography content filter settings */
    int porn_filter_enabled;             /* Enable pornography content filter (0=disabled, 1=enabled) */
    char porn_filter_keyword_file[512];  /* Path to keyword file */

    /* Language filter settings (independent of porn filter) */
    int language_filter_non_latin_threshold; /* Filter torrents where non-Latin chars exceed this % (0=disabled, default: 33) */

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
    int tree_parallel_peers;             /* Parallel peer connections per infohash (default: 2) */

    /* Thread tree metadata rate-based respawn settings */
    double min_metadata_rate;               /* Floor for dynamic threshold (default: 0.01) */
    double dynamic_rate_margin;             /* Margin subtracted from per-tree average (default: 0.02) */
    int tree_rate_check_interval_sec;       /* Rate check interval (default: 60) */
    int tree_rate_grace_period_sec;         /* Grace period before respawn (default: 30) */
    int tree_min_lifetime_minutes;          /* Min lifetime before rate checks (default: 10) */
    int tree_require_empty_queue;           /* Only respawn if queue empty (default: 1) */
    double tree_rate_ema_alpha;             /* EMA smoothing alpha for metadata rate (default: 0.3) */

    /* Tree respawn overlapping configuration */
    int respawn_spawn_threshold;            /* Spawn replacement when connections drop below this (default: 50) */
    int respawn_drain_timeout_sec;          /* Force destroy draining tree after this timeout (default: 120) */
    int max_draining_trees;                 /* Maximum trees allowed in draining state (default: 8) */

    /* Thread tree mode toggle (kept for build compatibility; removed by main.c branch in Stage 3) */
    int use_thread_trees;                /* 0=old architecture, 1=new thread tree architecture */

    /* Keyspace partitioning settings */
    int use_keyspace_partitioning;       /* Enable keyspace partitioning (0=random node IDs, 1=partitioned, default: 1) */
    int dead_partition_threshold;        /* Consecutive zero-metadata respawns before migration (default: 3) */
    int max_trees_per_partition;         /* Max trees allowed in one partition (default: 4) */

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
    int bep51_node_cooldown_sec;         /* Cooldown between queries to same node (default: 30) */
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
