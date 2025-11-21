#ifndef DHT_MANAGER_H
#define DHT_MANAGER_H

#include "dht_crawler.h"
#include "discovered_nodes.h"
#include "wbpxre_dht.h"
#include "refresh_query.h"
#include "peer_retry_tracker.h"
#include "infohash_queue.h"
#include <uv.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

/* DHT statistics */
typedef struct {
    time_t start_time;
    time_t last_routing_table_log;    /* Last time we logged routing table stats */
    /* Node discovery statistics (main thread only) */
    uint64_t nodes_from_incoming;     /* Nodes discovered from incoming requests */
    uint64_t nodes_from_bootstrap;    /* Nodes discovered from bootstrap */
    uint64_t nodes_deduped;           /* Duplicate nodes filtered */
    uint64_t find_node_sent;          /* find_node queries sent */
    uint64_t find_node_success;       /* find_node successful responses */
    uint64_t find_node_timeout;       /* find_node query timeouts */
    uint64_t nodes_in_jech_table;     /* Current nodes in jech/dht table */
    /* Dual routing table statistics (timer callback only) */
    uint64_t dual_routing_rotations;     /* Total dual routing table rotations */
    uint64_t dual_routing_nodes_cleared; /* Total nodes cleared during rotations */
    /* Queue clearing statistics (for rotation synchronization) */
    uint64_t peer_retry_entries_cleared; /* Peer retry entries cleared on rotation */
    uint64_t total_rotation_clears;      /* Total rotation clear events */
} dht_stats_t;

/* DHT configuration */
typedef struct {
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
    int max_routing_table_nodes;          /* Maximum nodes in routing table */
} dht_config_t;

/* Close callback tracking for proper libuv handle cleanup */
typedef struct {
    int handles_to_close;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} close_tracker_t;

/* DHT manager context */
typedef struct {
    app_context_t *app_ctx;
    wbpxre_dht_t *dht;                /* wbpxre-dht handle */
    dht_stats_t stats;
    dht_config_t config;              /* Configuration parameters */
    void *infohash_queue;  /* Pointer to global queue */
    int active_search_count;          /* Number of active searches */
    time_t last_bucket_refresh;       /* Last bucket refresh time */
    time_t last_neighbourhood_search; /* Last neighbourhood search time */
    time_t last_wandering_search;     /* Last wandering search time */
    /* BEP 51 state */
    unsigned char bep51_target[20];   /* Current target ID for sample_infohashes */
    time_t last_target_rotation;      /* Last time we rotated the target */
    time_t last_bep51_query;          /* Last time we sent BEP 51 queries */
    /* Node discovery state */
    discovered_nodes_queue_t discovered_nodes; /* Queue for discovered nodes */
    void *find_node_worker_pool;      /* Worker pool for find_node queries */
    uv_timer_t bootstrap_reseed_timer; /* Timer for bootstrap reseeding */
    /* Metadata fetcher reference for statistics */
    void *metadata_fetcher;           /* Pointer to metadata_fetcher_t */
    /* Refresh query tracking for HTTP API */
    refresh_query_store_t *refresh_query_store;
    /* Peer retry tracking */
    peer_retry_tracker_t *peer_retry_tracker;
    uv_timer_t peer_retry_timer;      /* Timer for periodic retry checking */
    /* Dual routing table rotation check timer */
    uv_timer_t dual_routing_timer;    /* Timer for periodic dual_routing_check_rotation */
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

/* wbpxre-dht callback wrapper */
void wbpxre_callback_wrapper(void *closure, wbpxre_event_t event,
                              const uint8_t *info_hash,
                              const void *data, size_t data_len);

#endif /* DHT_MANAGER_H */
