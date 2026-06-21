#ifndef HTTP_API_H
#define HTTP_API_H

#include "dht_crawler.h"
#include "database.h"
#include "refresh_query.h"

/* Forward declarations */
struct batch_writer;
struct supervisor;
struct refresh_thread;

/* HTTP server configuration */
#define HTTP_API_PORT 8080
#define HTTP_API_MAX_RESULTS 50

/* HTTP API manager */
typedef struct {
    void *mg_ctx;  /* CivetWeb context */
    app_context_t *app_ctx;
    database_t *database;
    struct batch_writer *batch_writer;
    struct supervisor *supervisor;  /* Stage 6: Thread tree supervisor */
    struct refresh_thread *refresh_thread;  /* Refresh thread for /refresh endpoint */
    refresh_query_store_t *refresh_query_store;  /* Direct access for /refresh endpoint */
    int refresh_retry_count;  /* Number of retries when /refresh returns 0 peers (default: 1) */
    int port;
    int running;
} http_api_t;

/* Search result structure */
typedef struct {
    uint8_t info_hash[20];
    char *name;
    int64_t size_bytes;
    int32_t total_peers;
    int64_t added_timestamp;
    int num_files;
    char **file_paths;
    int64_t *file_sizes;
} search_result_t;

/* Function declarations */
int http_api_init(http_api_t *api, app_context_t *app_ctx, database_t *database,
                  struct batch_writer *batch_writer, int port);
int http_api_start(http_api_t *api);
void http_api_stop(http_api_t *api);
void http_api_cleanup(http_api_t *api);

/* Stage 6: Set supervisor for thread tree mode stats */
void http_api_set_supervisor(http_api_t *api, struct supervisor *supervisor);

/* Set refresh thread for /refresh endpoint */
void http_api_set_refresh_thread(http_api_t *api, struct refresh_thread *refresh_thread);

/* Set refresh query store for /refresh endpoint */
void http_api_set_refresh_query_store(http_api_t *api, refresh_query_store_t *query_store);

/* Search functions */
int search_torrents(database_t *db, const char *query, int offset, search_result_t **results, int *count, int *total_count);
void free_search_results(search_result_t *results, int count);

#endif /* HTTP_API_H */
