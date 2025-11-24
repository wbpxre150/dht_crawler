#include "http_api.h"
#include "dht_crawler.h"
#include "dht_manager.h"
#include "refresh_query.h"
#include "batch_writer.h"
#include "metadata_fetcher.h"
#include "porn_filter.h"
#include "torrent_search.h"
#include "supervisor.h"
#include "thread_tree.h"
#include <civetweb.h>

/* Forward declarations for tree queue functions to avoid type conflicts */
int tree_routing_get_count(void *rt);
int tree_infohash_queue_count(void *q);
int tree_peers_queue_count(void *q);
#include <cJSON.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

/* Forward declarations */
static int search_handler(struct mg_connection *conn, void *cbdata);
static int stats_handler(struct mg_connection *conn, void *cbdata);
static int root_handler(struct mg_connection *conn, void *cbdata);
static int refresh_handler(struct mg_connection *conn, void *cbdata);
static char* url_decode(const char *str);
static char* url_encode(const char *str);
static char* generate_search_results_html(search_result_t *results, int count, const char *query, int page, int total_count);

/* Helper: Format info_hash as hex */
static void format_hex(const uint8_t *data, size_t len, char *out) {
    for (size_t i = 0; i < len; i++) {
        snprintf(out + i * 2, 3, "%02x", data[i]);
    }
}

/* Helper: Format size as human readable */
static void format_size(int64_t bytes, char *out, size_t out_len) {
    const char *units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit = 0;
    double size = (double)bytes;

    while (size >= 1024 && unit < 4) {
        size /= 1024;
        unit++;
    }

    snprintf(out, out_len, "%.2f %s", size, units[unit]);
}

/* Initialize HTTP API */
int http_api_init(http_api_t *api, app_context_t *app_ctx, database_t *database,
                  dht_manager_t *dht_manager, batch_writer_t *batch_writer,
                  metadata_fetcher_t *metadata_fetcher, int port) {
    /* dht_manager can be NULL in thread tree mode */
    if (!api || !app_ctx || !database) {
        return -1;
    }

    memset(api, 0, sizeof(http_api_t));
    api->app_ctx = app_ctx;
    api->database = database;
    api->dht_manager = dht_manager;
    api->batch_writer = batch_writer;
    api->metadata_fetcher = metadata_fetcher;
    api->port = port;
    api->running = 0;

    log_msg(LOG_DEBUG, "HTTP API initialized on port %d", port);
    return 0;
}

/* Start HTTP API server */
int http_api_start(http_api_t *api) {
    if (!api || api->running) {
        return -1;
    }

    /* CivetWeb options */
    const char *options[] = {
        "listening_ports", "8080",
        "num_threads", "2",
        "request_timeout_ms", "15000",
        NULL
    };

    /* Start CivetWeb */
    api->mg_ctx = mg_start(NULL, api, options);
    if (!api->mg_ctx) {
        log_msg(LOG_ERROR, "Failed to start HTTP server");
        return -1;
    }

    /* Register handlers */
    mg_set_request_handler(api->mg_ctx, "/", root_handler, api);
    mg_set_request_handler(api->mg_ctx, "/search", search_handler, api);
    mg_set_request_handler(api->mg_ctx, "/stats", stats_handler, api);
    mg_set_request_handler(api->mg_ctx, "/refresh", refresh_handler, api);

    api->running = 1;
    log_msg(LOG_DEBUG, "HTTP API server started on port %d", api->port);
    return 0;
}

/* Stop HTTP API server */
void http_api_stop(http_api_t *api) {
    if (!api || !api->running) {
        return;
    }

    if (api->mg_ctx) {
        mg_stop(api->mg_ctx);
        api->mg_ctx = NULL;
    }

    api->running = 0;
    log_msg(LOG_DEBUG, "HTTP API server stopped");
}

/* Cleanup HTTP API */
void http_api_cleanup(http_api_t *api) {
    if (!api) {
        return;
    }

    if (api->running) {
        http_api_stop(api);
    }

    log_msg(LOG_DEBUG, "HTTP API cleaned up");
}

/* Stage 6: Set supervisor for thread tree mode */
void http_api_set_supervisor(http_api_t *api, struct supervisor *supervisor) {
    if (api) {
        api->supervisor = supervisor;
    }
}

/* Root handler - Google-like search interface */
static int root_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get database statistics */
    sqlite3_stmt *stmt;
    int torrent_count = 0;
    int file_count = 0;
    size_t hourly_count = 0;

    const char *count_sql = "SELECT COUNT(*) FROM torrents";
    if (sqlite3_prepare_v2(api->database->db, count_sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            torrent_count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    const char *files_sql = "SELECT COUNT(*) FROM torrent_files";
    if (sqlite3_prepare_v2(api->database->db, files_sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            file_count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    /* Get hourly count from batch writer */
    if (api->batch_writer) {
        hourly_count = batch_writer_get_hourly_count(api->batch_writer);
    }

    /* Calculate uptime */
    time_t uptime_seconds = time(NULL) - api->app_ctx->start_time;
    int uptime_hours = (int)(uptime_seconds / 3600);
    int uptime_minutes = (int)((uptime_seconds % 3600) / 60);

    /* Format numbers with commas */
    char torrent_count_str[32];
    char file_count_str[32];
    char hourly_count_str[32];
    char uptime_str[32];

    if (torrent_count >= 1000000) {
        snprintf(torrent_count_str, sizeof(torrent_count_str), "%d,%03d,%03d",
                 torrent_count / 1000000, (torrent_count / 1000) % 1000, torrent_count % 1000);
    } else if (torrent_count >= 1000) {
        snprintf(torrent_count_str, sizeof(torrent_count_str), "%d,%03d",
                 torrent_count / 1000, torrent_count % 1000);
    } else {
        snprintf(torrent_count_str, sizeof(torrent_count_str), "%d", torrent_count);
    }

    if (file_count >= 1000000) {
        snprintf(file_count_str, sizeof(file_count_str), "%d,%03d,%03d",
                 file_count / 1000000, (file_count / 1000) % 1000, file_count % 1000);
    } else if (file_count >= 1000) {
        snprintf(file_count_str, sizeof(file_count_str), "%d,%03d",
                 file_count / 1000, file_count % 1000);
    } else {
        snprintf(file_count_str, sizeof(file_count_str), "%d", file_count);
    }

    if (hourly_count >= 1000000) {
        snprintf(hourly_count_str, sizeof(hourly_count_str), "%zu,%03zu,%03zu",
                 hourly_count / 1000000, (hourly_count / 1000) % 1000, hourly_count % 1000);
    } else if (hourly_count >= 1000) {
        snprintf(hourly_count_str, sizeof(hourly_count_str), "%zu,%03zu",
                 hourly_count / 1000, hourly_count % 1000);
    } else {
        snprintf(hourly_count_str, sizeof(hourly_count_str), "%zu", hourly_count);
    }

    snprintf(uptime_str, sizeof(uptime_str), "%d:%02d", uptime_hours, uptime_minutes);

    /* Build HTML */
    char html[4096];
    snprintf(html, sizeof(html),
        "<!DOCTYPE html>\n"
        "<html>\n"
        "<head>\n"
        "  <meta charset='UTF-8'>\n"
        "  <meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
        "  <title>DHT Crawler</title>\n"
        "  <style>\n"
        "    * { box-sizing: border-box; margin: 0; padding: 0; }\n"
        "    body {\n"
        "      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;\n"
        "      background: #f5f5f5;\n"
        "      color: #333;\n"
        "      min-height: 100vh;\n"
        "      display: flex;\n"
        "      align-items: center;\n"
        "      justify-content: center;\n"
        "      padding: 20px;\n"
        "    }\n"
        "    .container {\n"
        "      text-align: center;\n"
        "      max-width: 600px;\n"
        "      width: 100%%;\n"
        "    }\n"
        "    .logo {\n"
        "      font-size: 64px;\n"
        "      font-weight: 300;\n"
        "      color: #1a73e8;\n"
        "      margin-bottom: 32px;\n"
        "      letter-spacing: -1px;\n"
        "    }\n"
        "    .search-box {\n"
        "      background: white;\n"
        "      border-radius: 24px;\n"
        "      box-shadow: 0 2px 8px rgba(0,0,0,0.1);\n"
        "      padding: 20px;\n"
        "      margin-bottom: 24px;\n"
        "    }\n"
        "    .search-form {\n"
        "      display: flex;\n"
        "      flex-direction: column;\n"
        "      gap: 16px;\n"
        "    }\n"
        "    .search-form input[type='text'] {\n"
        "      width: 100%%;\n"
        "      padding: 16px 20px;\n"
        "      border: 2px solid #ddd;\n"
        "      border-radius: 24px;\n"
        "      font-size: 16px;\n"
        "      transition: border-color 0.2s;\n"
        "    }\n"
        "    .search-form input[type='text']:focus {\n"
        "      outline: none;\n"
        "      border-color: #1a73e8;\n"
        "    }\n"
        "    .btn {\n"
        "      padding: 14px 32px;\n"
        "      background: #1a73e8;\n"
        "      color: white;\n"
        "      border: none;\n"
        "      border-radius: 24px;\n"
        "      font-size: 16px;\n"
        "      font-weight: 500;\n"
        "      cursor: pointer;\n"
        "      transition: background 0.2s, transform 0.1s;\n"
        "    }\n"
        "    .btn:hover { background: #1557b0; }\n"
        "    .btn:active { transform: scale(0.98); }\n"
        "    .stats {\n"
        "      color: #5f6368;\n"
        "      font-size: 14px;\n"
        "      line-height: 1.8;\n"
        "    }\n"
        "    @media (max-width: 640px) {\n"
        "      .logo { font-size: 48px; margin-bottom: 24px; }\n"
        "      .search-box { border-radius: 16px; padding: 16px; }\n"
        "      .search-form input[type='text'] { padding: 14px 16px; }\n"
        "      .btn { padding: 12px 24px; }\n"
        "    }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <div class='container'>\n"
        "    <div class='logo'>DHT Crawler</div>\n"
        "    <div class='search-box'>\n"
        "      <form class='search-form' action='/search' method='get'>\n"
        "        <input type='text' name='q' placeholder='Search torrents...' autofocus required>\n"
        "        <input type='hidden' name='format' value='html'>\n"
        "        <button type='submit' class='btn'>Search</button>\n"
        "      </form>\n"
        "    </div>\n"
        "    <div class='stats'>\n"
        "      <div>%s torrents indexed</div>\n"
        "      <div>%s files indexed</div>\n"
        "      <div>%s found in last hour</div>\n"
        "      <div>Uptime: %s</div>\n"
        "    </div>\n"
        "  </div>\n"
        "</body>\n"
        "</html>",
        torrent_count_str, file_count_str, hourly_count_str, uptime_str);

    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html\r\n"
              "Content-Length: %d\r\n"
              "\r\n"
              "%s",
              (int)strlen(html), html);

    return 200;
}

/* Stats handler - Return JSON statistics */
static int stats_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get database statistics */
    sqlite3_stmt *stmt;
    int torrent_count = 0;
    int file_count = 0;
    size_t hourly_count = 0;

    const char *count_sql = "SELECT COUNT(*) FROM torrents";
    if (sqlite3_prepare_v2(api->database->db, count_sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            torrent_count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    const char *files_sql = "SELECT COUNT(*) FROM torrent_files";
    if (sqlite3_prepare_v2(api->database->db, files_sql, -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            file_count = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    /* Get hourly count from batch writer */
    if (api->batch_writer) {
        hourly_count = batch_writer_get_hourly_count(api->batch_writer);
    }

    /* Get metadata fetcher statistics */
    metadata_fetcher_stats_t metadata_stats = {0};
    if (api->metadata_fetcher) {
        metadata_fetcher_get_stats(api->metadata_fetcher, &metadata_stats);
    }

    /* Build JSON response */
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "version", DHT_CRAWLER_VERSION);
    cJSON_AddNumberToObject(root, "torrents_indexed", torrent_count);
    cJSON_AddNumberToObject(root, "files_indexed", file_count);
    cJSON_AddNumberToObject(root, "torrents_last_hour", (double)hourly_count);
    cJSON_AddNumberToObject(root, "timestamp", time(NULL));

    /* Calculate and format uptime */
    time_t uptime_seconds = time(NULL) - api->app_ctx->start_time;
    int uptime_hours = (int)(uptime_seconds / 3600);
    int uptime_minutes = (int)((uptime_seconds % 3600) / 60);
    char uptime_str[32];
    snprintf(uptime_str, sizeof(uptime_str), "%d:%02d", uptime_hours, uptime_minutes);
    cJSON_AddStringToObject(root, "uptime", uptime_str);

    /* Add DHT/dual routing statistics */
    if (api->dht_manager) {
        cJSON *dht = cJSON_CreateObject();
        cJSON_AddNumberToObject(dht, "dual_routing_rotations", (double)api->dht_manager->stats.dual_routing_rotations);
        cJSON_AddNumberToObject(dht, "dual_routing_nodes_cleared", (double)api->dht_manager->stats.dual_routing_nodes_cleared);
        cJSON_AddItemToObject(root, "dht", dht);
    }

    /* Add metadata fetcher statistics */
    if (api->metadata_fetcher) {
        cJSON *metadata = cJSON_CreateObject();
        cJSON_AddNumberToObject(metadata, "total_attempts", (double)metadata_stats.total_attempts);
        cJSON_AddNumberToObject(metadata, "no_peers_found", (double)metadata_stats.no_peers_found);
        cJSON_AddNumberToObject(metadata, "connections_initiated", (double)metadata_stats.connection_initiated);
        cJSON_AddNumberToObject(metadata, "connections_failed", (double)metadata_stats.connection_failed);
        cJSON_AddNumberToObject(metadata, "connections_timeout", (double)metadata_stats.connection_timeout);
        cJSON_AddNumberToObject(metadata, "handshake_failed", (double)metadata_stats.handshake_failed);
        cJSON_AddNumberToObject(metadata, "no_metadata_support", (double)metadata_stats.no_metadata_support);
        cJSON_AddNumberToObject(metadata, "metadata_rejected", (double)metadata_stats.metadata_rejected);
        cJSON_AddNumberToObject(metadata, "hash_mismatch", (double)metadata_stats.hash_mismatch);
        cJSON_AddNumberToObject(metadata, "fetched", (double)metadata_stats.total_fetched);
        cJSON_AddNumberToObject(metadata, "filtered", (double)metadata_stats.filtered_count);
        cJSON_AddNumberToObject(metadata, "active_connections", metadata_stats.active_count);

        /* Calculate success rate */
        double success_rate = 0.0;
        if (metadata_stats.total_attempts > 0) {
            success_rate = (metadata_stats.total_fetched * 100.0) / metadata_stats.total_attempts;
        }
        cJSON_AddNumberToObject(metadata, "success_rate_percent", success_rate);

        /* Calculate filter rate */
        double filter_rate = 0.0;
        if (metadata_stats.total_attempts > 0) {
            filter_rate = (metadata_stats.filtered_count * 100.0) / metadata_stats.total_attempts;
        }
        cJSON_AddNumberToObject(metadata, "filter_rate_percent", filter_rate);

        /* Calculate timeout rate */
        double timeout_rate = 0.0;
        if (metadata_stats.connection_initiated > 0) {
            timeout_rate = (metadata_stats.connection_timeout * 100.0) / metadata_stats.connection_initiated;
        }
        cJSON_AddNumberToObject(metadata, "timeout_rate_percent", timeout_rate);

        cJSON_AddItemToObject(root, "metadata_fetcher", metadata);
    }

    /* Add porn filter statistics */
    porn_filter_stats_t pf_stats;
    porn_filter_get_stats(&pf_stats);
    if (pf_stats.total_checked > 0) {
        cJSON *porn_filter = cJSON_CreateObject();
        cJSON_AddNumberToObject(porn_filter, "total_checked", (double)pf_stats.total_checked);
        cJSON_AddNumberToObject(porn_filter, "total_filtered", (double)pf_stats.total_filtered);
        cJSON_AddNumberToObject(porn_filter, "filtered_by_keyword", (double)pf_stats.filtered_by_keyword);
        cJSON_AddNumberToObject(porn_filter, "filtered_by_regex", (double)pf_stats.filtered_by_regex);
        cJSON_AddNumberToObject(porn_filter, "filtered_by_heuristic", (double)pf_stats.filtered_by_heuristic);

        /* Calculate filter rate */
        double filter_rate = 0.0;
        if (pf_stats.total_checked > 0) {
            filter_rate = (pf_stats.total_filtered * 100.0) / pf_stats.total_checked;
        }
        cJSON_AddNumberToObject(porn_filter, "filter_rate_percent", filter_rate);

        cJSON_AddItemToObject(root, "porn_filter", porn_filter);
    }

    /* Stage 6: Add supervisor/thread tree statistics */
    if (api->supervisor) {
        int active_trees = 0;
        uint64_t total_metadata = 0;
        supervisor_stats(api->supervisor, &active_trees, &total_metadata);

        cJSON *supervisor_json = cJSON_CreateObject();
        cJSON_AddNumberToObject(supervisor_json, "active_trees", active_trees);
        cJSON_AddNumberToObject(supervisor_json, "max_trees", api->supervisor->max_trees);
        cJSON_AddNumberToObject(supervisor_json, "total_trees_spawned", (double)api->supervisor->next_tree_id - 1);
        cJSON_AddItemToObject(root, "supervisor", supervisor_json);

        /* Add per-tree stats */
        cJSON *trees_array = cJSON_CreateArray();
        pthread_mutex_lock(&api->supervisor->trees_lock);
        for (int i = 0; i < api->supervisor->max_trees; i++) {
            thread_tree_t *tree = api->supervisor->trees[i];
            if (tree) {
                cJSON *tree_json = cJSON_CreateObject();
                cJSON_AddNumberToObject(tree_json, "tree_id", tree->tree_id);
                cJSON_AddStringToObject(tree_json, "phase", thread_tree_phase_name(tree->current_phase));
                cJSON_AddNumberToObject(tree_json, "metadata_count", (double)atomic_load(&tree->metadata_count));
                cJSON_AddNumberToObject(tree_json, "metadata_rate", tree->metadata_rate);
                cJSON_AddBoolToObject(tree_json, "find_node_paused", atomic_load(&tree->find_node_paused));

                /* Add queue sizes for monitoring and tuning */
                cJSON *queues = cJSON_CreateObject();

                /* Routing table node count */
                int node_count = 0;
                if (tree->routing_table) {
                    node_count = tree_routing_get_count(tree->routing_table);
                }
                cJSON_AddNumberToObject(queues, "routing_table_nodes", node_count);

                /* Sample infohash queue (before get_peers) */
                int infohash_queue_size = 0;
                int infohash_queue_capacity = tree->infohash_queue_capacity;
                if (tree->infohash_queue) {
                    infohash_queue_size = tree_infohash_queue_count(tree->infohash_queue);
                }
                cJSON_AddNumberToObject(queues, "infohash_queue_size", infohash_queue_size);
                cJSON_AddNumberToObject(queues, "infohash_queue_capacity", infohash_queue_capacity);

                /* Peers queue (for metadata fetchers) */
                int peers_queue_size = 0;
                int peers_queue_capacity = tree->peers_queue_capacity;
                if (tree->peers_queue) {
                    peers_queue_size = tree_peers_queue_count(tree->peers_queue);
                }
                cJSON_AddNumberToObject(queues, "peers_queue_size", peers_queue_size);
                cJSON_AddNumberToObject(queues, "peers_queue_capacity", peers_queue_capacity);

                /* Throttling thresholds */
                cJSON_AddNumberToObject(queues, "pause_threshold", tree->infohash_pause_threshold);
                cJSON_AddNumberToObject(queues, "resume_threshold", tree->infohash_resume_threshold);

                cJSON_AddItemToObject(tree_json, "queues", queues);
                cJSON_AddItemToArray(trees_array, tree_json);
            }
        }
        pthread_mutex_unlock(&api->supervisor->trees_lock);
        cJSON_AddItemToObject(root, "trees", trees_array);

        /* Add aggregate stats */
        cJSON *aggregate = cJSON_CreateObject();
        cJSON_AddNumberToObject(aggregate, "total_metadata_fetched", (double)total_metadata);
        cJSON_AddItemToObject(root, "aggregate", aggregate);
    }

    char *json = cJSON_Print(root);
    cJSON_Delete(root);

    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: application/json\r\n"
              "Content-Length: %d\r\n"
              "Access-Control-Allow-Origin: *\r\n"
              "\r\n"
              "%s",
              (int)strlen(json), json);

    free(json);
    return 200;
}

/* Refresh handler - Query DHT network for live peer count */
static int refresh_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get info_hash parameter */
    char hash_buf[256] = {0};
    const struct mg_request_info *ri = mg_get_request_info(conn);

    int hash_len = mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
                               "hash", hash_buf, sizeof(hash_buf));

    if (hash_len <= 0 || strlen(hash_buf) != 40) {
        const char *error = "{\"error\":\"Missing or invalid hash parameter (expected 40 hex chars)\"}";
        mg_printf(conn,
                  "HTTP/1.1 400 Bad Request\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 400;
    }

    /* Convert hex string to binary */
    uint8_t info_hash[20];
    for (int i = 0; i < 20; i++) {
        sscanf(hash_buf + i * 2, "%2hhx", &info_hash[i]);
    }

    /* Verify torrent exists in database */
    const char *check_sql = "SELECT 1 FROM torrents WHERE info_hash = ? LIMIT 1";
    sqlite3_stmt *check_stmt;
    int exists = 0;
    if (sqlite3_prepare_v2(api->database->db, check_sql, -1, &check_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_blob(check_stmt, 1, info_hash, 20, SQLITE_STATIC);
        if (sqlite3_step(check_stmt) == SQLITE_ROW) {
            exists = 1;
        }
        sqlite3_finalize(check_stmt);
    }

    if (!exists) {
        const char *error = "{\"error\":\"Torrent not found in database\"}";
        mg_printf(conn,
                  "HTTP/1.1 404 Not Found\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 404;
    }

    /* Check DHT manager availability */
    if (!api->dht_manager || !api->dht_manager->refresh_query_store) {
        const char *error = "{\"error\":\"DHT query system not available\"}";
        mg_printf(conn,
                  "HTTP/1.1 503 Service Unavailable\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 503;
    }

    /* Create pending query */
    refresh_query_t *query = refresh_query_create(api->dht_manager->refresh_query_store, info_hash);
    if (!query) {
        const char *error = "{\"error\":\"Failed to create DHT query\"}";
        mg_printf(conn,
                  "HTTP/1.1 500 Internal Server Error\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 500;
    }

    /* Trigger DHT get_peers query with PRIORITY (skip to front of queue) */
    int rc = dht_manager_query_peers(api->dht_manager, info_hash, true);
    if (rc != 0) {
        log_msg(LOG_WARN, "Failed to trigger priority DHT query for refresh");
    }

    /* Wait for DHT responses (blocks for up to 8 seconds)
     * timed_out is returned safely via output parameter to avoid use-after-free */
    int timed_out = 0;
    int peer_count = refresh_query_wait(query, 8, &timed_out);
    int retry_attempted = 0;

    /* Retry logic: If zero peers found, try one more time immediately */
    if (peer_count == 0) {
        log_msg(LOG_DEBUG, "First query returned 0 peers for %s, retrying...", hash_buf);

        /* Remove old query from store by pointer (critical for thread safety) */
        refresh_query_remove_ptr(api->dht_manager->refresh_query_store, query);

        /* Create new query for retry */
        query = refresh_query_create(api->dht_manager->refresh_query_store, info_hash);
        if (query) {
            /* Trigger second priority query */
            rc = dht_manager_query_peers(api->dht_manager, info_hash, true);
            if (rc == 0) {
                /* Wait for retry results (up to 8 seconds) */
                peer_count = refresh_query_wait(query, 8, &timed_out);
                retry_attempted = 1;

                if (peer_count > 0) {
                    log_msg(LOG_DEBUG, "Retry successful: found %d peers for %s", peer_count, hash_buf);
                } else {
                    log_msg(LOG_DEBUG, "Retry also returned 0 peers for %s", hash_buf);
                }
            }
        }
    }

    /* Remove query from store by pointer (critical for thread safety) */
    refresh_query_remove_ptr(api->dht_manager->refresh_query_store, query);

    /* Build JSON response */
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "info_hash", hash_buf);
    cJSON_AddNumberToObject(root, "total_peers", peer_count);
    cJSON_AddBoolToObject(root, "live_query", 1);
    cJSON_AddBoolToObject(root, "timed_out", timed_out);
    cJSON_AddBoolToObject(root, "retry_attempted", retry_attempted);
    cJSON_AddNumberToObject(root, "timestamp", time(NULL));

    if (timed_out && peer_count == 0) {
        cJSON_AddStringToObject(root, "warning",
            retry_attempted ?
            "Both queries timed out with no peers - torrent may have no active peers" :
            "Query timed out with no peers found - torrent may have no active peers");
    } else if (timed_out) {
        cJSON_AddStringToObject(root, "warning",
            "Query timed out - partial peer count returned");
    } else if (retry_attempted && peer_count > 0) {
        cJSON_AddStringToObject(root, "info",
            "Peers found on retry attempt");
    }

    char *json = cJSON_Print(root);
    cJSON_Delete(root);

    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: application/json\r\n"
              "Content-Length: %d\r\n"
              "Access-Control-Allow-Origin: *\r\n"
              "\r\n"
              "%s",
              (int)strlen(json), json);

    free(json);
    return 200;
}

/* Search handler - Perform FTS5 search and return JSON or HTML */
static int search_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get query parameter */
    char query_buf[256] = {0};
    char format_buf[16] = {0};
    char page_buf[16] = {0};
    const struct mg_request_info *ri = mg_get_request_info(conn);

    int query_len = mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
                                "q", query_buf, sizeof(query_buf));

    /* Get format parameter (default: json) */
    mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
               "format", format_buf, sizeof(format_buf));

    /* Get page parameter (default: 1) */
    mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
               "page", page_buf, sizeof(page_buf));

    int want_html = (strcmp(format_buf, "html") == 0);

    /* Parse page number (1-indexed, default to 1) */
    int page = 1;
    if (strlen(page_buf) > 0) {
        page = atoi(page_buf);
        if (page < 1) page = 1;
    }
    int offset = (page - 1) * HTTP_API_MAX_RESULTS;

    if (query_len <= 0 || strlen(query_buf) == 0) {
        const char *error = "{\"error\":\"Missing query parameter 'q'\"}";
        mg_printf(conn,
                  "HTTP/1.1 400 Bad Request\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 400;
    }

    char *query = url_decode(query_buf);
    log_msg(LOG_DEBUG, "Search query: %s (format: %s, page: %d)", query, want_html ? "html" : "json", page);

    /* Search database */
    search_result_t *results = NULL;
    int count = 0;
    int total_count = 0;
    int rc = search_torrents(api->database, query, offset, &results, &count, &total_count);
    free(query);

    if (rc != 0) {
        const char *error = "{\"error\":\"Database search failed\"}";
        mg_printf(conn,
                  "HTTP/1.1 500 Internal Server Error\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 500;
    }

    /* Return HTML if requested */
    if (want_html) {
        if (count == 0) {
            /* No results - show empty state */
            char empty_html[2048];
            if (total_count > 0 && page > 1) {
                /* Page beyond results - offer to go back */
                char *encoded_query = url_encode(query_buf);
                snprintf(empty_html, sizeof(empty_html),
                    "<!DOCTYPE html>\n"
                    "<html>\n"
                    "<head>\n"
                    "  <meta charset='UTF-8'>\n"
                    "  <meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
                    "  <title>No More Results</title>\n"
                    "  <style>\n"
                    "    body { font-family: Arial, sans-serif; padding: 20px; text-align: center; }\n"
                    "    .message { margin: 50px 0; color: #666; }\n"
                    "    a { color: #1a73e8; text-decoration: none; }\n"
                    "  </style>\n"
                    "</head>\n"
                    "<body>\n"
                    "  <h1>No More Results</h1>\n"
                    "  <p class='message'>Page %d is beyond the available results (%d total)</p>\n"
                    "  <a href='/search?q=%s&format=html&page=1'>Back to First Page</a>\n"
                    "</body>\n"
                    "</html>", page, total_count, encoded_query);
                free(encoded_query);
            } else {
                snprintf(empty_html, sizeof(empty_html),
                    "<!DOCTYPE html>\n"
                    "<html>\n"
                    "<head>\n"
                    "  <meta charset='UTF-8'>\n"
                    "  <meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
                    "  <title>No Results</title>\n"
                    "  <style>\n"
                    "    body { font-family: Arial, sans-serif; padding: 20px; text-align: center; }\n"
                    "    .message { margin: 50px 0; color: #666; }\n"
                    "    a { color: #1a73e8; text-decoration: none; }\n"
                    "  </style>\n"
                    "</head>\n"
                    "<body>\n"
                    "  <h1>No Results Found</h1>\n"
                    "  <p class='message'>No torrents matched your query: \"%s\"</p>\n"
                    "  <a href='/'>Back to Search</a>\n"
                    "</body>\n"
                    "</html>", query_buf);
            }

            mg_printf(conn,
                      "HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/html\r\n"
                      "Content-Length: %d\r\n"
                      "\r\n"
                      "%s",
                      (int)strlen(empty_html), empty_html);

            free_search_results(results, count);
            return 200;
        }

        char *html = generate_search_results_html(results, count, query_buf, page, total_count);
        if (!html) {
            const char *error = "{\"error\":\"Failed to generate HTML\"}";
            mg_printf(conn,
                      "HTTP/1.1 500 Internal Server Error\r\n"
                      "Content-Type: application/json\r\n"
                      "Content-Length: %d\r\n"
                      "\r\n"
                      "%s",
                      (int)strlen(error), error);
            free_search_results(results, count);
            return 500;
        }

        mg_printf(conn,
                  "HTTP/1.1 200 OK\r\n"
                  "Content-Type: text/html\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(html), html);

        free(html);
        free_search_results(results, count);
        return 200;
    }

    /* Build JSON response */
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "count", count);
    cJSON_AddNumberToObject(root, "total_count", total_count);
    cJSON_AddNumberToObject(root, "page", page);
    cJSON_AddNumberToObject(root, "limit", HTTP_API_MAX_RESULTS);
    cJSON_AddBoolToObject(root, "has_more", (offset + count) < total_count);
    cJSON_AddNumberToObject(root, "timestamp", time(NULL));

    cJSON *results_array = cJSON_CreateArray();
    for (int i = 0; i < count; i++) {
        cJSON *item = cJSON_CreateObject();

        char hex[41];
        format_hex(results[i].info_hash, 20, hex);
        cJSON_AddStringToObject(item, "info_hash", hex);
        cJSON_AddStringToObject(item, "name", results[i].name);

        char size_str[64];
        format_size(results[i].size_bytes, size_str, sizeof(size_str));
        cJSON_AddStringToObject(item, "size", size_str);
        cJSON_AddNumberToObject(item, "size_bytes", results[i].size_bytes);
        cJSON_AddNumberToObject(item, "total_peers", results[i].total_peers);
        cJSON_AddNumberToObject(item, "added", results[i].added_timestamp);
        cJSON_AddNumberToObject(item, "num_files", results[i].num_files);

        /* Add files array */
        if (results[i].num_files > 0 && results[i].file_paths) {
            cJSON *files_array = cJSON_CreateArray();
            for (int j = 0; j < results[i].num_files && j < 10; j++) {  /* Limit to 10 files */
                cJSON *file = cJSON_CreateObject();
                cJSON_AddStringToObject(file, "path", results[i].file_paths[j]);

                char file_size_str[64];
                format_size(results[i].file_sizes[j], file_size_str, sizeof(file_size_str));
                cJSON_AddStringToObject(file, "size", file_size_str);
                cJSON_AddNumberToObject(file, "size_bytes", results[i].file_sizes[j]);

                cJSON_AddItemToArray(files_array, file);
            }
            cJSON_AddItemToObject(item, "files", files_array);
        }

        cJSON_AddItemToArray(results_array, item);
    }
    cJSON_AddItemToObject(root, "results", results_array);

    char *json = cJSON_Print(root);
    cJSON_Delete(root);

    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: application/json\r\n"
              "Content-Length: %d\r\n"
              "Access-Control-Allow-Origin: *\r\n"
              "\r\n"
              "%s",
              (int)strlen(json), json);

    free(json);
    free_search_results(results, count);

    return 200;
}

/* Search torrents using FTS5 */
int search_torrents(database_t *db, const char *query, int offset, search_result_t **results, int *count, int *total_count) {
    if (!db || !query || !results || !count || !total_count) {
        return -1;
    }

    *results = NULL;
    *count = 0;
    *total_count = 0;

    /* First, get total count of matching results */
    const char *count_sql =
        "SELECT COUNT(DISTINCT t.id) "
        "FROM torrents t "
        "WHERE t.id IN ("
        "    SELECT rowid FROM torrent_search WHERE name MATCH ?"
        "    UNION"
        "    SELECT DISTINCT tf.torrent_id FROM torrent_files tf "
        "    WHERE tf.id IN (SELECT rowid FROM file_search WHERE filename MATCH ?)"
        ")";

    sqlite3_stmt *count_stmt;
    int rc = sqlite3_prepare_v2(db->db, count_sql, -1, &count_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare count query: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    sqlite3_bind_text(count_stmt, 1, query, -1, SQLITE_STATIC);
    sqlite3_bind_text(count_stmt, 2, query, -1, SQLITE_STATIC);

    if (sqlite3_step(count_stmt) == SQLITE_ROW) {
        *total_count = sqlite3_column_int(count_stmt, 0);
    }
    sqlite3_finalize(count_stmt);

    /* Build FTS5 search query - searches both torrent names and file names
     * Uses subqueries to properly utilize FTS5 MATCH function, then UNION to combine results */
    const char *sql =
        "SELECT DISTINCT t.info_hash, t.name, t.size_bytes, t.total_peers, "
        "       t.added_timestamp, COUNT(f.id) as file_count "
        "FROM torrents t "
        "LEFT JOIN torrent_files f ON t.id = f.torrent_id "
        "WHERE t.id IN ("
        "    SELECT rowid FROM torrent_search WHERE name MATCH ?"
        "    UNION"
        "    SELECT DISTINCT tf.torrent_id FROM torrent_files tf "
        "    WHERE tf.id IN (SELECT rowid FROM file_search WHERE filename MATCH ?)"
        ") "
        "GROUP BY t.id "
        "ORDER BY t.total_peers DESC, t.added_timestamp DESC "
        "LIMIT ? OFFSET ?";

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare search query: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    /* Bind query parameter twice (once for torrent names, once for file paths) */
    sqlite3_bind_text(stmt, 1, query, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, query, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, HTTP_API_MAX_RESULTS);
    sqlite3_bind_int(stmt, 4, offset);

    /* Allocate results array */
    search_result_t *res = (search_result_t *)calloc(HTTP_API_MAX_RESULTS, sizeof(search_result_t));
    if (!res) {
        sqlite3_finalize(stmt);
        return -1;
    }

    int i = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && i < HTTP_API_MAX_RESULTS) {
        /* Extract torrent data */
        memcpy(res[i].info_hash, sqlite3_column_blob(stmt, 0), 20);
        res[i].name = strdup((const char *)sqlite3_column_text(stmt, 1));
        res[i].size_bytes = sqlite3_column_int64(stmt, 2);
        res[i].total_peers = sqlite3_column_int(stmt, 3);
        res[i].added_timestamp = sqlite3_column_int64(stmt, 4);
        res[i].num_files = sqlite3_column_int(stmt, 5);

        /* Get file listings for this torrent - reconstruct paths from prefix + filename */
        if (res[i].num_files > 0) {
            const char *files_sql =
                "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename) as path, "
                "       tf.size_bytes "
                "FROM torrent_files tf "
                "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
                "WHERE tf.torrent_id = (SELECT id FROM torrents WHERE info_hash = ?) "
                "ORDER BY tf.file_index LIMIT 10";

            sqlite3_stmt *files_stmt;
            if (sqlite3_prepare_v2(db->db, files_sql, -1, &files_stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_blob(files_stmt, 1, res[i].info_hash, 20, SQLITE_STATIC);

                int file_count = 0;
                res[i].file_paths = (char **)calloc(10, sizeof(char *));
                res[i].file_sizes = (int64_t *)calloc(10, sizeof(int64_t));

                while (sqlite3_step(files_stmt) == SQLITE_ROW && file_count < 10) {
                    res[i].file_paths[file_count] = strdup((const char *)sqlite3_column_text(files_stmt, 0));
                    res[i].file_sizes[file_count] = sqlite3_column_int64(files_stmt, 1);
                    file_count++;
                }

                sqlite3_finalize(files_stmt);
            }
        }

        i++;
    }

    sqlite3_finalize(stmt);

    *results = res;
    *count = i;

    log_msg(LOG_DEBUG, "Search found %d results (offset %d, total %d) for query: %s", i, offset, *total_count, query);
    return 0;
}

/* Free search results */
void free_search_results(search_result_t *results, int count) {
    if (!results) {
        return;
    }

    for (int i = 0; i < count; i++) {
        if (results[i].name) {
            free(results[i].name);
        }

        if (results[i].file_paths) {
            for (int j = 0; j < 10; j++) {
                if (results[i].file_paths[j]) {
                    free(results[i].file_paths[j]);
                }
            }
            free(results[i].file_paths);
        }

        if (results[i].file_sizes) {
            free(results[i].file_sizes);
        }
    }

    free(results);
}

/* URL decode helper */
static char* url_decode(const char *str) {
    if (!str) return NULL;

    size_t len = strlen(str);
    char *decoded = (char *)malloc(len + 1);
    if (!decoded) return NULL;

    size_t i = 0, j = 0;
    while (i < len) {
        if (str[i] == '%' && i + 2 < len) {
            int value;
            sscanf(str + i + 1, "%2x", &value);
            decoded[j++] = (char)value;
            i += 3;
        } else if (str[i] == '+') {
            decoded[j++] = ' ';
            i++;
        } else {
            decoded[j++] = str[i++];
        }
    }
    decoded[j] = '\0';

    return decoded;
}

/* URL encode helper - encodes string for use in magnet links */
static char* url_encode(const char *str) {
    if (!str) return NULL;

    size_t len = strlen(str);
    /* Worst case: each character becomes %XX (3x expansion) */
    char *encoded = (char *)malloc(len * 3 + 1);
    if (!encoded) return NULL;

    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)str[i];

        /* Safe characters: alphanumeric, dash, underscore, period, tilde */
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' ||
            c == '.' || c == '~') {
            encoded[j++] = c;
        } else {
            /* Encode as %XX */
            snprintf(encoded + j, 4, "%%%02X", c);
            j += 3;
        }
    }
    encoded[j] = '\0';

    return encoded;
}

/* Generate HTML page for search results */
static char* generate_search_results_html(search_result_t *results, int count, const char *query, int page, int total_count) {
    if (!results || count <= 0) {
        return NULL;
    }

    /* Calculate pagination info */
    int total_pages = (total_count + HTTP_API_MAX_RESULTS - 1) / HTTP_API_MAX_RESULTS;
    int has_prev = page > 1;
    int has_next = page < total_pages;

    /* Build HTML dynamically */
    size_t capacity = 1024 * 100;  /* Start with 100KB */
    char *html = (char *)malloc(capacity);
    if (!html) return NULL;

    size_t offset = 0;

    /* Helper macro to append string safely */
    #define APPEND(fmt, ...) do { \
        int needed = snprintf(html + offset, capacity - offset, fmt, ##__VA_ARGS__); \
        if (needed < 0) { free(html); return NULL; } \
        if ((size_t)needed >= capacity - offset) { \
            capacity *= 2; \
            char *new_html = (char *)realloc(html, capacity); \
            if (!new_html) { free(html); return NULL; } \
            html = new_html; \
            needed = snprintf(html + offset, capacity - offset, fmt, ##__VA_ARGS__); \
        } \
        offset += needed; \
    } while(0)

    /* HTML header with CSS */
    APPEND("<!DOCTYPE html>\n"
           "<html>\n"
           "<head>\n"
           "  <meta charset='UTF-8'>\n"
           "  <meta name='viewport' content='width=device-width, initial-scale=1.0'>\n"
           "  <title>Search Results: %s</title>\n"
           "  <style>\n"
           "    * { box-sizing: border-box; margin: 0; padding: 0; }\n"
           "    body {\n"
           "      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;\n"
           "      background: #f5f5f5;\n"
           "      color: #333;\n"
           "      padding: 16px;\n"
           "      font-size: 16px;\n"
           "      line-height: 1.5;\n"
           "    }\n"
           "    .container {\n"
           "      max-width: 900px;\n"
           "      margin: 0 auto;\n"
           "    }\n"
           "    .header {\n"
           "      background: white;\n"
           "      padding: 20px;\n"
           "      border-radius: 8px;\n"
           "      margin-bottom: 16px;\n"
           "      box-shadow: 0 1px 3px rgba(0,0,0,0.1);\n"
           "    }\n"
           "    h1 {\n"
           "      font-size: 24px;\n"
           "      margin-bottom: 12px;\n"
           "      color: #1a73e8;\n"
           "    }\n"
           "    .search-form {\n"
           "      display: flex;\n"
           "      gap: 8px;\n"
           "      flex-wrap: wrap;\n"
           "    }\n"
           "    .search-form input[type='text'] {\n"
           "      flex: 1;\n"
           "      min-width: 200px;\n"
           "      padding: 12px 16px;\n"
           "      border: 2px solid #ddd;\n"
           "      border-radius: 6px;\n"
           "      font-size: 16px;\n"
           "    }\n"
           "    .search-form input[type='text']:focus {\n"
           "      outline: none;\n"
           "      border-color: #1a73e8;\n"
           "    }\n"
           "    .btn {\n"
           "      padding: 12px 24px;\n"
           "      background: #1a73e8;\n"
           "      color: white;\n"
           "      border: none;\n"
           "      border-radius: 6px;\n"
           "      font-size: 16px;\n"
           "      cursor: pointer;\n"
           "      min-height: 44px;\n"
           "      font-weight: 500;\n"
           "    }\n"
           "    .btn:hover { background: #1557b0; }\n"
           "    .btn:active { transform: scale(0.98); }\n"
           "    .btn-small {\n"
           "      padding: 8px 12px;\n"
           "      font-size: 14px;\n"
           "      min-height: 36px;\n"
           "      background: #f1f3f4;\n"
           "      color: #333;\n"
           "      border: 1px solid #ddd;\n"
           "    }\n"
           "    .btn-small:hover { background: #e8eaed; }\n"
           "    .results-info {\n"
           "      margin-bottom: 16px;\n"
           "      color: #5f6368;\n"
           "      font-size: 14px;\n"
           "    }\n"
           "    .torrent-card {\n"
           "      background: white;\n"
           "      border-radius: 8px;\n"
           "      margin-bottom: 12px;\n"
           "      box-shadow: 0 1px 3px rgba(0,0,0,0.1);\n"
           "      overflow: hidden;\n"
           "      transition: box-shadow 0.2s;\n"
           "    }\n"
           "    .torrent-card:hover {\n"
           "      box-shadow: 0 2px 8px rgba(0,0,0,0.15);\n"
           "    }\n"
           "    .torrent-header {\n"
           "      padding: 16px;\n"
           "      cursor: pointer;\n"
           "      user-select: none;\n"
           "    }\n"
           "    .torrent-name {\n"
           "      font-size: 18px;\n"
           "      font-weight: 500;\n"
           "      margin-bottom: 8px;\n"
           "      color: #1a73e8;\n"
           "      word-wrap: break-word;\n"
           "    }\n"
           "    .torrent-name a {\n"
           "      color: inherit;\n"
           "      text-decoration: none;\n"
           "    }\n"
           "    .torrent-name a:hover {\n"
           "      text-decoration: underline;\n"
           "    }\n"
           "    .torrent-meta {\n"
           "      display: flex;\n"
           "      gap: 12px;\n"
           "      flex-wrap: wrap;\n"
           "      align-items: center;\n"
           "      font-size: 14px;\n"
           "      color: #5f6368;\n"
           "    }\n"
           "    .peer-count {\n"
           "      font-weight: 600;\n"
           "      padding: 4px 8px;\n"
           "      border-radius: 4px;\n"
           "      background: #e8f5e9;\n"
           "      color: #2e7d32;\n"
           "    }\n"
           "    .peer-count.low { background: #f5f5f5; color: #666; }\n"
           "    .peer-count.medium { background: #fff8e1; color: #f57f17; }\n"
           "    .refresh-btn {\n"
           "      background: none;\n"
           "      border: none;\n"
           "      font-size: 18px;\n"
           "      cursor: pointer;\n"
           "      padding: 4px 8px;\n"
           "      border-radius: 4px;\n"
           "      min-height: 32px;\n"
           "      min-width: 32px;\n"
           "      transition: background 0.2s;\n"
           "    }\n"
           "    .refresh-btn:hover { background: #f1f3f4; }\n"
           "    .refresh-btn.loading { animation: spin 1s linear infinite; }\n"
           "    @keyframes spin {\n"
           "      from { transform: rotate(0deg); }\n"
           "      to { transform: rotate(360deg); }\n"
           "    }\n"
           "    .copy-btn {\n"
           "      background: none;\n"
           "      border: none;\n"
           "      font-size: 18px;\n"
           "      cursor: pointer;\n"
           "      padding: 4px 8px;\n"
           "      border-radius: 4px;\n"
           "      min-height: 32px;\n"
           "      min-width: 32px;\n"
           "      transition: background 0.2s, transform 0.1s;\n"
           "    }\n"
           "    .copy-btn:hover { background: #f1f3f4; }\n"
           "    .copy-btn:active { transform: scale(0.95); }\n"
           "    .copy-btn.copied {\n"
           "      background: #e8f5e9;\n"
           "      color: #2e7d32;\n"
           "    }\n"
           "    .file-list {\n"
           "      border-top: 1px solid #e0e0e0;\n"
           "      padding: 16px;\n"
           "      background: #fafafa;\n"
           "      display: none;\n"
           "    }\n"
           "    .file-list.show { display: block; }\n"
           "    .file-list ul { list-style: none; }\n"
           "    .file-list li {\n"
           "      padding: 8px 0;\n"
           "      border-bottom: 1px solid #e0e0e0;\n"
           "      font-size: 14px;\n"
           "      display: flex;\n"
           "      justify-content: space-between;\n"
           "      gap: 12px;\n"
           "      word-break: break-word;\n"
           "    }\n"
           "    .file-list li:last-child { border-bottom: none; }\n"
           "    .file-path { flex: 1; color: #333; }\n"
           "    .file-size { color: #5f6368; white-space: nowrap; }\n"
           "    .no-files { color: #999; font-style: italic; }\n"
           "    @media (max-width: 640px) {\n"
           "      .refresh-btn, .copy-btn {\n"
           "        min-height: 44px;\n"
           "        min-width: 44px;\n"
           "        font-size: 20px;\n"
           "      }\n"
           "    }\n"
           "    .pagination {\n"
           "      display: flex;\n"
           "      justify-content: center;\n"
           "      align-items: center;\n"
           "      gap: 16px;\n"
           "      margin: 24px 0;\n"
           "      padding: 16px;\n"
           "      background: white;\n"
           "      border-radius: 8px;\n"
           "      box-shadow: 0 1px 3px rgba(0,0,0,0.1);\n"
           "    }\n"
           "    .pagination a, .pagination span {\n"
           "      padding: 10px 20px;\n"
           "      border-radius: 6px;\n"
           "      text-decoration: none;\n"
           "      font-weight: 500;\n"
           "      min-height: 44px;\n"
           "      display: flex;\n"
           "      align-items: center;\n"
           "    }\n"
           "    .pagination a {\n"
           "      background: #1a73e8;\n"
           "      color: white;\n"
           "    }\n"
           "    .pagination a:hover {\n"
           "      background: #1557b0;\n"
           "    }\n"
           "    .pagination .disabled {\n"
           "      background: #e0e0e0;\n"
           "      color: #999;\n"
           "      cursor: not-allowed;\n"
           "    }\n"
           "    .pagination .page-info {\n"
           "      color: #5f6368;\n"
           "      font-size: 14px;\n"
           "    }\n"
           "  </style>\n"
           "</head>\n"
           "<body>\n"
           "  <div class='container'>\n"
           "    <div class='header'>\n"
           "      <h1>Search Results</h1>\n"
           "      <form class='search-form' action='/search' method='get'>\n"
           "        <input type='text' name='q' placeholder='Search torrents...' value='%s'>\n"
           "        <input type='hidden' name='format' value='html'>\n"
           "        <button type='submit' class='btn'>Search</button>\n"
           "      </form>\n"
           "    </div>\n",
           query, query);

    /* Show results info with pagination context */
    int start_result = (page - 1) * HTTP_API_MAX_RESULTS + 1;
    int end_result = start_result + count - 1;
    if (total_pages > 1) {
        APPEND("    <div class='results-info'>Showing %d-%d of %d results (page %d of %d)</div>\n",
               start_result, end_result, total_count, page, total_pages);
    } else {
        APPEND("    <div class='results-info'>Found %d result%s</div>\n",
               count, count == 1 ? "" : "s");
    }

    /* Generate torrent cards */
    for (int i = 0; i < count; i++) {
        char hex[41];
        format_hex(results[i].info_hash, 20, hex);

        char size_str[64];
        format_size(results[i].size_bytes, size_str, sizeof(size_str));

        /* URL encode torrent name for magnet link */
        char *encoded_name = url_encode(results[i].name);
        if (!encoded_name) {
            continue;  /* Skip this torrent if encoding fails */
        }

        /* Determine peer count class */
        const char *peer_class = "low";
        if (results[i].total_peers > 100) peer_class = "";
        else if (results[i].total_peers > 10) peer_class = "medium";

        /* Extract media title for IMDB search */
        char *media_title = extract_media_title(results[i].name);
        char *encoded_imdb_query = media_title ? url_encode(media_title) : NULL;
        free(media_title);

        APPEND("    <div class='torrent-card'>\n"
               "      <div class='torrent-header' onclick='toggleFiles(\"%s\")'>\n"
               "        <div class='torrent-name'>",
               hex);

        /* Add IMDB link if we have a valid encoded query */
        /* Use onclick handler to open in external browser on Android WebView */
        if (encoded_imdb_query) {
            APPEND("<a href='https://www.imdb.com/find/?q=%s' onclick='return openExternal(event, this.href)'>%s</a>",
                   encoded_imdb_query, results[i].name);
            free(encoded_imdb_query);
        } else {
            APPEND("%s", results[i].name);
        }

        APPEND("</div>\n"
               "        <div class='torrent-meta'>\n"
               "          <span class='peer-count %s' id='peers-%s'>%d peer%s</span>\n"
               "          <button class='refresh-btn' onclick='refreshPeers(event, \"%s\")' title='Refresh peer count'>↻</button>\n"
               "          <button class='copy-btn' onclick='copyMagnet(event, \"%s\", \"%s\")' title='Copy magnet link'>\xF0\x9F\xA7\xB2</button>\n"
               "          <span>%s</span>\n"
               "          <span>%d file%s</span>\n"
               "        </div>\n"
               "      </div>\n",
               peer_class, hex,
               results[i].total_peers, results[i].total_peers == 1 ? "" : "s",
               hex, hex, encoded_name, size_str, results[i].num_files, results[i].num_files == 1 ? "" : "s");

        free(encoded_name);

        /* File list */
        APPEND("      <div class='file-list' id='files-%s'>\n", hex);

        if (results[i].num_files > 0 && results[i].file_paths) {
            APPEND("        <ul>\n");
            for (int j = 0; j < results[i].num_files && j < 10; j++) {
                char file_size_str[64];
                format_size(results[i].file_sizes[j], file_size_str, sizeof(file_size_str));

                APPEND("          <li>\n"
                       "            <span class='file-path'>%s</span>\n"
                       "            <span class='file-size'>%s</span>\n"
                       "          </li>\n",
                       results[i].file_paths[j], file_size_str);
            }
            if (results[i].num_files > 10) {
                APPEND("          <li class='no-files'>...and %d more file%s</li>\n",
                       results[i].num_files - 10, results[i].num_files - 10 == 1 ? "" : "s");
            }
            APPEND("        </ul>\n");
        } else {
            APPEND("        <div class='no-files'>No file information available</div>\n");
        }

        APPEND("      </div>\n"
               "    </div>\n");
    }

    /* JavaScript for interactivity */
    APPEND("  </div>\n"
           "  <script>\n"
           "    function openExternal(event, url) {\n"
           "      event.stopPropagation();\n"
           "      // Try to open in external browser for Android WebView\n"
           "      // Using unique target name to ensure each click opens fresh window\n"
           "      var win = window.open(url, '_blank_' + Date.now());\n"
           "      if (!win || win.closed || typeof win.closed === 'undefined') {\n"
           "        // Fallback: use location change which some WebViews intercept\n"
           "        window.location.href = url;\n"
           "      }\n"
           "      return false;\n"
           "    }\n"
           "\n"
           "    function toggleFiles(hash) {\n"
           "      const fileList = document.getElementById('files-' + hash);\n"
           "      if (fileList.classList.contains('show')) {\n"
           "        fileList.classList.remove('show');\n"
           "      } else {\n"
           "        fileList.classList.add('show');\n"
           "      }\n"
           "    }\n"
           "\n"
           "    function refreshPeers(event, hash) {\n"
           "      event.stopPropagation();\n"
           "      const btn = event.target;\n"
           "      const peerSpan = document.getElementById('peers-' + hash);\n"
           "      \n"
           "      if (btn.classList.contains('loading')) return;\n"
           "      \n"
           "      // Store original content for rollback on error\n"
           "      const originalContent = peerSpan.textContent;\n"
           "      const originalClass = peerSpan.className;\n"
           "      \n"
           "      btn.classList.add('loading');\n"
           "      peerSpan.textContent = 'Refreshing...';\n"
           "      peerSpan.className = 'peer-count';\n"
           "      \n"
           "      // Create AbortController with 12 second timeout\n"
           "      const controller = new AbortController();\n"
           "      const timeoutId = setTimeout(() => controller.abort(), 12000);\n"
           "      \n"
           "      fetch('/refresh?hash=' + hash, { signal: controller.signal })\n"
           "        .then(response => {\n"
           "          clearTimeout(timeoutId);\n"
           "          if (!response.ok) {\n"
           "            throw new Error('Server returned ' + response.status);\n"
           "          }\n"
           "          return response.json();\n"
           "        })\n"
           "        .then(data => {\n"
           "          const peers = data.total_peers;\n"
           "          peerSpan.textContent = peers + (peers === 1 ? ' peer' : ' peers');\n"
           "          \n"
           "          // Update color class\n"
           "          peerSpan.className = 'peer-count';\n"
           "          if (peers > 100) peerSpan.className += '';\n"
           "          else if (peers > 10) peerSpan.className += ' medium';\n"
           "          else peerSpan.className += ' low';\n"
           "          \n"
           "          // Show warning if query timed out\n"
           "          if (data.timed_out && peers === 0) {\n"
           "            peerSpan.title = 'Query timed out - no active peers found';\n"
           "          } else if (data.timed_out) {\n"
           "            peerSpan.title = 'Query timed out - partial count shown';\n"
           "          } else {\n"
           "            peerSpan.title = 'Live peer count from DHT network';\n"
           "          }\n"
           "        })\n"
           "        .catch(err => {\n"
           "          clearTimeout(timeoutId);\n"
           "          console.error('Failed to refresh peer count:', err);\n"
           "          \n"
           "          // Restore original content\n"
           "          peerSpan.textContent = originalContent;\n"
           "          peerSpan.className = originalClass;\n"
           "          \n"
           "          // Show specific error message\n"
           "          let errorMsg = 'Failed to refresh peer count';\n"
           "          if (err.name === 'AbortError') {\n"
           "            errorMsg = 'Request timed out (>12s). Try again.';\n"
           "          } else if (err.message.includes('Server returned')) {\n"
           "            errorMsg = 'Server error: ' + err.message;\n"
           "          } else if (err.message.includes('Failed to fetch')) {\n"
           "            errorMsg = 'Network error. Check connection.';\n"
           "          }\n"
           "          \n"
           "          peerSpan.title = errorMsg;\n"
           "          alert(errorMsg);\n"
           "        })\n"
           "        .finally(() => {\n"
           "          btn.classList.remove('loading');\n"
           "        });\n"
           "    }\n"
           "\n"
           "    function copyMagnet(event, hash, encodedName) {\n"
           "      event.stopPropagation();\n"
           "      \n"
           "      const magnetLink = 'magnet:?xt=urn:btih:' + hash + '&dn=' + encodedName;\n"
           "      const btn = event.target;\n"
           "      \n"
           "      // Try modern Clipboard API first (works on localhost/HTTPS)\n"
           "      if (navigator.clipboard && navigator.clipboard.writeText) {\n"
           "        navigator.clipboard.writeText(magnetLink)\n"
           "          .then(() => showCopySuccess(btn))\n"
           "          .catch(() => fallbackCopy(magnetLink, btn));\n"
           "      } else {\n"
           "        // Primary method for HTTP local network\n"
           "        fallbackCopy(magnetLink, btn);\n"
           "      }\n"
           "    }\n"
           "\n"
           "    function fallbackCopy(text, btn) {\n"
           "      // Create temporary textarea for copy\n"
           "      const textarea = document.createElement('textarea');\n"
           "      textarea.value = text;\n"
           "      textarea.style.position = 'fixed';\n"
           "      textarea.style.left = '-9999px';\n"
           "      textarea.style.top = '0';\n"
           "      document.body.appendChild(textarea);\n"
           "      \n"
           "      // Select text\n"
           "      textarea.focus();\n"
           "      textarea.select();\n"
           "      textarea.setSelectionRange(0, 99999);\n"
           "      \n"
           "      let success = false;\n"
           "      try {\n"
           "        success = document.execCommand('copy');\n"
           "      } catch (err) {\n"
           "        console.error('Copy failed:', err);\n"
           "      }\n"
           "      \n"
           "      // Clean up\n"
           "      document.body.removeChild(textarea);\n"
           "      \n"
           "      // Show result\n"
           "      if (success) {\n"
           "        showCopySuccess(btn);\n"
           "      } else {\n"
           "        // Last resort: show magnet link in alert for manual copy\n"
           "        alert('Could not copy automatically. Magnet link:\\n\\n' + text);\n"
           "      }\n"
           "    }\n"
           "\n"
           "    function showCopySuccess(btn) {\n"
           "      const originalContent = btn.textContent;\n"
           "      const originalTitle = btn.title;\n"
           "      \n"
           "      // Visual feedback\n"
           "      btn.textContent = '\\u2713';\n"
           "      btn.title = 'Copied!';\n"
           "      btn.classList.add('copied');\n"
           "      \n"
           "      // Restore after 2 seconds\n"
           "      setTimeout(() => {\n"
           "        btn.textContent = originalContent;\n"
           "        btn.title = originalTitle;\n"
           "        btn.classList.remove('copied');\n"
           "      }, 2000);\n"
           "    }\n"
           "  </script>\n");

    /* Add pagination controls if needed */
    if (total_pages > 1) {
        char *encoded_query = url_encode(query);
        if (encoded_query) {
            APPEND("  <div class='pagination'>\n");

            /* Previous button */
            if (has_prev) {
                APPEND("    <a href='/search?q=%s&format=html&page=%d'>Previous</a>\n",
                       encoded_query, page - 1);
            } else {
                APPEND("    <span class='disabled'>Previous</span>\n");
            }

            /* Page info */
            APPEND("    <span class='page-info'>Page %d of %d</span>\n", page, total_pages);

            /* Next button */
            if (has_next) {
                APPEND("    <a href='/search?q=%s&format=html&page=%d'>Next</a>\n",
                       encoded_query, page + 1);
            } else {
                APPEND("    <span class='disabled'>Next</span>\n");
            }

            APPEND("  </div>\n");
            free(encoded_query);
        }
    }

    APPEND("</body>\n"
           "</html>");

    #undef APPEND

    return html;
}
