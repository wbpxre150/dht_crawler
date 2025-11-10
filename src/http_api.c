#include "http_api.h"
#include "dht_crawler.h"
#include <civetweb.h>
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
static char* generate_search_results_html(search_result_t *results, int count, const char *query);

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
int http_api_init(http_api_t *api, app_context_t *app_ctx, database_t *database, int port) {
    if (!api || !app_ctx || !database) {
        return -1;
    }

    memset(api, 0, sizeof(http_api_t));
    api->app_ctx = app_ctx;
    api->database = database;
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
        "request_timeout_ms", "10000",
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

/* Root handler - Google-like search interface */
static int root_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get database statistics */
    sqlite3_stmt *stmt;
    int torrent_count = 0;
    int file_count = 0;

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

    /* Format numbers with commas */
    char torrent_count_str[32];
    char file_count_str[32];

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
        "    </div>\n"
        "  </div>\n"
        "</body>\n"
        "</html>",
        torrent_count_str, file_count_str);

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

    /* Build JSON response */
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "version", DHT_CRAWLER_VERSION);
    cJSON_AddNumberToObject(root, "torrents_indexed", torrent_count);
    cJSON_AddNumberToObject(root, "files_indexed", file_count);
    cJSON_AddNumberToObject(root, "timestamp", time(NULL));

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

/* Refresh handler - Get updated peer count for a specific torrent */
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

    /* Query database for peer count */
    const char *sql = "SELECT total_peers, last_seen FROM torrents WHERE info_hash = ?";
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(api->database->db, sql, -1, &stmt, NULL);

    if (rc != SQLITE_OK) {
        const char *error = "{\"error\":\"Database query failed\"}";
        mg_printf(conn,
                  "HTTP/1.1 500 Internal Server Error\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 500;
    }

    sqlite3_bind_blob(stmt, 1, info_hash, 20, SQLITE_STATIC);

    cJSON *root = NULL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        int total_peers = sqlite3_column_int(stmt, 0);
        int64_t last_seen = sqlite3_column_int64(stmt, 1);

        root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "info_hash", hash_buf);
        cJSON_AddNumberToObject(root, "total_peers", total_peers);
        cJSON_AddNumberToObject(root, "last_seen", last_seen);
    } else {
        sqlite3_finalize(stmt);
        const char *error = "{\"error\":\"Torrent not found\"}";
        mg_printf(conn,
                  "HTTP/1.1 404 Not Found\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 404;
    }

    sqlite3_finalize(stmt);

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
    const struct mg_request_info *ri = mg_get_request_info(conn);

    int query_len = mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
                                "q", query_buf, sizeof(query_buf));

    /* Get format parameter (default: json) */
    mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
               "format", format_buf, sizeof(format_buf));

    int want_html = (strcmp(format_buf, "html") == 0);

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
    log_msg(LOG_DEBUG, "Search query: %s (format: %s)", query, want_html ? "html" : "json");

    /* Search database */
    search_result_t *results = NULL;
    int count = 0;
    int rc = search_torrents(api->database, query, &results, &count);
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
                "  <a href='/'>← Back to Search</a>\n"
                "</body>\n"
                "</html>", query_buf);

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

        char *html = generate_search_results_html(results, count, query_buf);
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
int search_torrents(database_t *db, const char *query, search_result_t **results, int *count) {
    if (!db || !query || !results || !count) {
        return -1;
    }

    *results = NULL;
    *count = 0;

    /* Build FTS5 search query - searches both torrent names and file paths
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
        "    WHERE tf.id IN (SELECT rowid FROM file_search WHERE path MATCH ?)"
        ") "
        "GROUP BY t.id "
        "ORDER BY t.total_peers DESC, t.added_timestamp DESC "
        "LIMIT ?";

    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare search query: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    /* Bind query parameter twice (once for torrent names, once for file paths) */
    sqlite3_bind_text(stmt, 1, query, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, query, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, HTTP_API_MAX_RESULTS);

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

        /* Get file listings for this torrent */
        if (res[i].num_files > 0) {
            const char *files_sql =
                "SELECT path, size_bytes FROM torrent_files WHERE torrent_id = "
                "(SELECT id FROM torrents WHERE info_hash = ?) ORDER BY file_index LIMIT 10";

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

    log_msg(LOG_DEBUG, "Search found %d results for query: %s", i, query);
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

/* Generate HTML page for search results */
static char* generate_search_results_html(search_result_t *results, int count, const char *query) {
    if (!results || count <= 0) {
        return NULL;
    }

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
           "    </div>\n"
           "    <div class='results-info'>Found %d result%s</div>\n",
           query, query, count, count == 1 ? "" : "s");

    /* Generate torrent cards */
    for (int i = 0; i < count; i++) {
        char hex[41];
        format_hex(results[i].info_hash, 20, hex);

        char size_str[64];
        format_size(results[i].size_bytes, size_str, sizeof(size_str));

        /* Determine peer count class */
        const char *peer_class = "low";
        if (results[i].total_peers > 100) peer_class = "";
        else if (results[i].total_peers > 10) peer_class = "medium";

        APPEND("    <div class='torrent-card'>\n"
               "      <div class='torrent-header' onclick='toggleFiles(\"%s\")'>\n"
               "        <div class='torrent-name'>%s</div>\n"
               "        <div class='torrent-meta'>\n"
               "          <span class='peer-count %s' id='peers-%s'>%d peer%s</span>\n"
               "          <button class='refresh-btn' onclick='refreshPeers(event, \"%s\")' title='Refresh peer count'>↻</button>\n"
               "          <span>%s</span>\n"
               "          <span>%d file%s</span>\n"
               "        </div>\n"
               "      </div>\n",
               hex, results[i].name, peer_class, hex,
               results[i].total_peers, results[i].total_peers == 1 ? "" : "s",
               hex, size_str, results[i].num_files, results[i].num_files == 1 ? "" : "s");

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
           "      btn.classList.add('loading');\n"
           "      \n"
           "      fetch('/refresh?hash=' + hash)\n"
           "        .then(response => response.json())\n"
           "        .then(data => {\n"
           "          const peers = data.total_peers;\n"
           "          peerSpan.textContent = peers + (peers === 1 ? ' peer' : ' peers');\n"
           "          \n"
           "          // Update color class\n"
           "          peerSpan.className = 'peer-count';\n"
           "          if (peers > 100) peerSpan.className += '';\n"
           "          else if (peers > 10) peerSpan.className += ' medium';\n"
           "          else peerSpan.className += ' low';\n"
           "        })\n"
           "        .catch(err => {\n"
           "          console.error('Failed to refresh peer count:', err);\n"
           "          alert('Failed to refresh peer count');\n"
           "        })\n"
           "        .finally(() => {\n"
           "          btn.classList.remove('loading');\n"
           "        });\n"
           "    }\n"
           "  </script>\n"
           "</body>\n"
           "</html>");

    #undef APPEND

    return html;
}
