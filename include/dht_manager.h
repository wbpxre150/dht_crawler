#ifndef DHT_MANAGER_H
#define DHT_MANAGER_H

#include "dht_crawler.h"
#include "dht_cache.h"
#include "shadow_routing_table.h"
#include "peer_store.h"
#include "discovered_nodes.h"
#include "wbpxre_dht.h"
#include "refresh_query.h"
#include "peer_retry_tracker.h"
#include <uv.h>
#include <stdbool.h>

/* DHT statistics */
typedef struct {
    uint64_t packets_received;
    uint64_t packets_sent;
    uint64_t packets_dropped;
    uint64_t queries_received;
    uint64_t responses_received;
    uint64_t errors_received;
    uint64_t announce_peer_received;
    uint64_t get_peers_received;
    uint64_t find_node_received;
    uint64_t ping_received;
    uint64_t infohashes_discovered;
    uint64_t active_searches;
    uint64_t cached_peers_pinged;     /* Number of cached peers pinged on bootstrap */
    uint64_t bucket_refreshes_sent;   /* Bucket refresh searches */
    uint64_t neighbourhood_searches_sent; /* Neighbourhood maintenance searches */
    uint64_t wandering_searches_sent; /* Random keyspace exploration searches */
    uint64_t stale_nodes_pinged;      /* Stale node verification pings */
    uint64_t responses_sent;          /* Responses to queries (good citizenship) */
    /* BEP 51 statistics */
    uint64_t bep51_queries_sent;      /* BEP 51 sample_infohashes queries sent */
    uint64_t bep51_responses_received; /* BEP 51 responses received */
    uint64_t bep51_samples_received;  /* Total sample hashes received from BEP 51 */
    /* Peer discovery statistics */
    uint64_t get_peers_queries_sent;  /* Active get_peers queries sent */
    uint64_t get_peers_responses;     /* get_peers responses received */
    uint64_t total_peers_discovered;  /* Total peers found via get_peers */
    uint64_t info_hashes_with_peers;  /* Info hashes that found at least one peer */
    uint64_t info_hashes_no_peers;    /* Info hashes with no peers found */
    /* Peer validation statistics */
    uint64_t peers_invalid_ip;        /* Peers with invalid/private IPs */
    uint64_t peers_invalid_port;      /* Peers with port 0 */
    uint64_t peers_filtered;          /* Total peers filtered out */
    time_t start_time;
    time_t last_routing_table_log;    /* Last time we logged routing table stats */
    /* Node discovery statistics */
    uint64_t nodes_from_queries;      /* Nodes discovered from outgoing queries */
    uint64_t nodes_from_incoming;     /* Nodes discovered from incoming requests */
    uint64_t nodes_from_bootstrap;    /* Nodes discovered from bootstrap */
    uint64_t nodes_deduped;           /* Duplicate nodes filtered */
    uint64_t find_node_sent;          /* find_node queries sent */
    uint64_t find_node_success;       /* find_node successful responses */
    uint64_t find_node_timeout;       /* find_node query timeouts */
    uint64_t nodes_in_shadow_table;   /* Current nodes in shadow table */
    uint64_t nodes_in_jech_table;     /* Current nodes in jech/dht table */
    uint64_t nodes_expired;           /* Nodes expired due to inactivity */
    uint64_t nodes_pruned;            /* Nodes pruned from tables */
    /* Phase 7: Active Ping Verification statistics (BUGFIX) */
    uint64_t ping_verification_cycles;   /* Number of verification cycles run */
    uint64_t dubious_nodes_pinged;       /* Total dubious nodes pinged */
    uint64_t dubious_ping_responses;     /* Responses received from dubious pings */
    uint64_t nodes_promoted_to_good;     /* Nodes that went from dubious to good */
    uint64_t discovery_pings_sent;       /* Pings sent immediately on node discovery */
    uint64_t discovery_ping_responses;   /* Responses to discovery pings */
    /* Node ID rotation statistics */
    uint64_t node_rotations_performed;   /* Total rotations performed */
    uint64_t samples_per_rotation;       /* Average samples per rotation period */
    time_t last_rotation_time;           /* Last rotation timestamp */
    /* Node pruning statistics */
    uint64_t last_nodes_dropped;         /* Nodes dropped at last stats print (for delta calculation) */
    /* Peer retry statistics */
    uint64_t peer_retries_triggered;     /* Number of retry attempts triggered */
} dht_stats_t;

/* DHT configuration */
typedef struct {
    int bucket_refresh_interval_sec;      /* How often to refresh stale buckets */
    int neighbourhood_refresh_interval_sec; /* How often to search near our ID */
    int wandering_search_interval_sec;    /* How often to do random searches */
    int wandering_searches_per_cycle;     /* Number of random searches per cycle */
    int max_concurrent_searches;          /* Maximum concurrent active searches */
    int routing_table_log_interval_sec;   /* How often to log routing table health */
    int min_good_nodes_threshold;         /* Alert if good nodes drop below this */
    /* BEP 51 configuration */
    int bep51_enabled;                    /* Enable BEP 51 sample_infohashes queries */
    int bep51_query_rate;                 /* Queries per second (nodes to query) */
    int bep51_target_rotation_interval;   /* Target ID rotation interval in seconds */
    int bep51_respect_interval;           /* Respect interval hints from nodes */
    /* Peer discovery configuration */
    int peer_discovery_enabled;           /* Enable active peer discovery via get_peers */
    int max_concurrent_peer_queries;      /* Maximum concurrent get_peers queries */
    int peer_query_timeout_sec;           /* Timeout for get_peers queries */
    /* Node discovery configuration */
    int node_discovery_enabled;           /* Enable aggressive node discovery */
    int find_node_workers;                /* Number of find_node worker threads */
    int discovered_queue_capacity;        /* Discovered nodes queue capacity */
    int bootstrap_reseed_interval_sec;    /* How often to reseed bootstrap nodes */
    int old_node_check_interval_sec;      /* How often to check for old nodes */
    int old_node_threshold_sec;           /* Age threshold for old nodes */
    int max_routing_table_nodes;          /* Maximum nodes in routing table */
    /* Phase 7: Active Ping Verification configuration (BUGFIX) */
    int ping_verification_enabled;        /* Enable active ping verification */
    int ping_verification_interval;       /* How often to ping dubious nodes (seconds) */
    int ping_max_dubious_per_cycle;       /* Max dubious nodes to ping per cycle */
    int ping_dubious_age_threshold;       /* Seconds since last response to be dubious */
    int ping_on_discovery_enabled;        /* Ping nodes immediately when discovered */
    int discovery_ping_probability;       /* Probability (0-100) of pinging discovered node */
} dht_config_t;

/* Close callback tracking for proper libuv handle cleanup */
typedef struct {
    int handles_to_close;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} close_tracker_t;

/* Rotation state machine */
typedef enum {
    ROTATION_STATE_STABLE,          // Normal operation
    ROTATION_STATE_ANNOUNCING,      // Telling network about new ID
    ROTATION_STATE_TRANSITIONING    // Using new ID
} rotation_state_t;

/* DHT manager context */
typedef struct {
    app_context_t *app_ctx;
    wbpxre_dht_t *dht;                /* wbpxre-dht handle */
    dht_stats_t stats;
    dht_config_t config;              /* Configuration parameters */
    void *infohash_queue;  /* Pointer to global queue */
    dht_cache_t cache;                /* Peer cache */
    int active_search_count;          /* Number of active searches */
    time_t last_bucket_refresh;       /* Last bucket refresh time */
    time_t last_neighbourhood_search; /* Last neighbourhood search time */
    time_t last_wandering_search;     /* Last wandering search time */
    /* BEP 51 state */
    unsigned char bep51_target[20];   /* Current target ID for sample_infohashes */
    time_t last_target_rotation;      /* Last time we rotated the target */
    time_t last_bep51_query;          /* Last time we sent BEP 51 queries */
    /* Shadow routing table */
    shadow_routing_table_t *shadow_table; /* Enhanced node tracking for BEP 51 */
    /* Peer discovery state */
    peer_store_t *peer_store;         /* Storage for discovered peers */
    int active_peer_queries;          /* Current number of active get_peers queries */
    /* Node discovery state */
    discovered_nodes_queue_t discovered_nodes; /* Queue for discovered nodes */
    void *find_node_worker_pool;      /* Worker pool for find_node queries */
    uv_timer_t bootstrap_reseed_timer; /* Timer for bootstrap reseeding */
    uv_timer_t old_node_timer;        /* Timer for old node maintenance */
    /* Phase 7: Active Ping Verification state (BUGFIX) */
    uv_timer_t ping_verification_timer; /* Timer for periodic dubious node pinging */
    time_t last_ping_verification;    /* Last time we ran ping verification */
    /* Node ID rotation state */
    uv_timer_t node_rotation_timer;   /* Timer for periodic node ID rotation */
    int node_rotation_enabled;        /* Is rotation enabled? */
    int node_rotation_interval_sec;   /* Rotation interval in seconds */
    int node_rotation_drain_timeout_sec; /* Drain timeout in seconds */
    uint64_t samples_since_rotation;  /* Samples collected since last rotation */
    void *crawler_config;             /* Pointer to crawler_config_t for rotation */
    /* Hot rotation state */
    rotation_state_t rotation_state;  /* Current rotation state */
    uint8_t next_node_id[20];         /* Next node ID (during rotation) */
    time_t rotation_phase_start;      /* When current phase started */
    /* Metadata fetcher reference for statistics */
    void *metadata_fetcher;           /* Pointer to metadata_fetcher_t */
    /* Refresh query tracking for HTTP API */
    refresh_query_store_t *refresh_query_store;
    /* Peer retry tracking */
    peer_retry_tracker_t *peer_retry_tracker;
    /* Close tracker for safe shutdown */
    close_tracker_t close_tracker;
    /* Initialization state flags */
    bool timers_initialized;          /* Track if timers were initialized */
} dht_manager_t;

/* Bootstrap nodes */
typedef struct {
    const char *host;
    int port;
} bootstrap_node_t;

/* Function declarations */
int dht_manager_init(dht_manager_t *mgr, app_context_t *app_ctx, void *infohash_queue, void *config);
int dht_manager_start(dht_manager_t *mgr);
void dht_manager_stop(dht_manager_t *mgr);
void dht_manager_cleanup(dht_manager_t *mgr);
void dht_manager_print_stats(dht_manager_t *mgr);
void dht_manager_set_metadata_fetcher(dht_manager_t *mgr, void *metadata_fetcher);

/* Peer discovery functions
 * priority: if true, query is processed immediately (for /refresh API)
 *           if false, query is queued normally (for automatic discovery) */
int dht_manager_query_peers(dht_manager_t *mgr, const uint8_t *info_hash, bool priority);

/* Node ID rotation functions */
int dht_manager_rotate_node_id(dht_manager_t *mgr);

/* wbpxre-dht callback wrapper */
void wbpxre_callback_wrapper(void *closure, wbpxre_event_t event,
                              const uint8_t *info_hash,
                              const void *data, size_t data_len);

#endif /* DHT_MANAGER_H */
