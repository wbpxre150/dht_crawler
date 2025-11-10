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
static char* url_decode(const char *str);

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

/* Root handler - Simple HTML interface */
static int root_handler(struct mg_connection *conn, void *cbdata) {
    (void)cbdata;

    const char *html =
        "<!DOCTYPE html>\n"
        "<html>\n"
        "<head>\n"
        "  <title>DHT Crawler API</title>\n"
        "  <style>\n"
        "    body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }\n"
        "    h1 { color: #333; }\n"
        "    .endpoint { background: #f4f4f4; padding: 15px; margin: 10px 0; border-radius: 5px; }\n"
        "    code { background: #e0e0e0; padding: 2px 6px; border-radius: 3px; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <h1>DHT Crawler API v0.1.0</h1>\n"
        "  <p>Welcome to the DHT Crawler HTTP API!</p>\n"
        "  \n"
        "  <h2>Available Endpoints:</h2>\n"
        "  \n"
        "  <div class='endpoint'>\n"
        "    <h3>GET /search?q=&lt;query&gt;</h3>\n"
        "    <p>Search for torrents by name or file path.</p>\n"
        "    <p>Example: <code>/search?q=ubuntu</code></p>\n"
        "  </div>\n"
        "  \n"
        "  <div class='endpoint'>\n"
        "    <h3>GET /stats</h3>\n"
        "    <p>Get crawler statistics and database metrics.</p>\n"
        "    <p>Example: <code>/stats</code></p>\n"
        "  </div>\n"
        "  \n"
        "  <h2>Search Form:</h2>\n"
        "  <form action='/search' method='get'>\n"
        "    <input type='text' name='q' placeholder='Enter search query...' size='50'>\n"
        "    <input type='submit' value='Search'>\n"
        "  </form>\n"
        "</body>\n"
        "</html>";

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

/* Search handler - Perform FTS5 search and return JSON */
static int search_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get query parameter */
    char query_buf[256] = {0};
    const struct mg_request_info *ri = mg_get_request_info(conn);

    int query_len = mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
                                "q", query_buf, sizeof(query_buf));

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
    log_msg(LOG_DEBUG, "Search query: %s", query);

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
        "ORDER BY t.added_timestamp DESC "
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
