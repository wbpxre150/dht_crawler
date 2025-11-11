#ifndef HTTP_API_H
#define HTTP_API_H

#include "dht_crawler.h"
#include "database.h"
#include "dht_manager.h"

/* Forward declaration */
typedef struct batch_writer batch_writer_t;

/* HTTP server configuration */
#define HTTP_API_PORT 8080
#define HTTP_API_MAX_RESULTS 50

/* HTTP API manager */
typedef struct {
    void *mg_ctx;  /* CivetWeb context */
    app_context_t *app_ctx;
    database_t *database;
    dht_manager_t *dht_manager;
    batch_writer_t *batch_writer;
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
                  dht_manager_t *dht_manager, batch_writer_t *batch_writer, int port);
int http_api_start(http_api_t *api);
void http_api_stop(http_api_t *api);
void http_api_cleanup(http_api_t *api);

/* Search functions */
int search_torrents(database_t *db, const char *query, search_result_t **results, int *count);
void free_search_results(search_result_t *results, int count);

#endif /* HTTP_API_H */
