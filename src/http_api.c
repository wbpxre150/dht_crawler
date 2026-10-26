#include "http_api.h"
#include "dht_crawler.h"
#include "refresh_query.h"
#include "refresh_thread.h"
#include "batch_writer.h"
#include "language_filter.h"
#include "classifier.h"
#include "category_filter_cleanup.h"
#include "torrent_search.h"
#include "supervisor.h"
#include "udp_rate_limit.h"
#include "thread_tree.h"
#include "bep51_cache.h"
#include "http_torrent_page.h"
#include "tree_dispatcher.h"  /* shared dispatcher counters in /stats */
#include <civetweb.h>

/* Forward declarations for tree queue functions to avoid type conflicts */
int tree_routing_get_count(void *rt);
int tree_infohash_queue_count(void *q);
void tree_infohash_queue_get_stats(void *q, uint64_t *out_total, uint64_t *out_duplicates);
/* tree_peers_queue_count() now comes in via thread_tree.h -> tree_peers_queue.h */
#include <cJSON.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <stdarg.h>

/* ---------------------------------------------------------------------------
 * Stage 7: torrent_class-powered filters (/search, /browse, /random-*).
 *
 * The classifier (stage 5) writes one torrent_class row per classified
 * torrent, whose codes are the tparse taxonomy (tp_category_t / tp_tv_kind_t
 * in dht_crawler-android/tparse/taxonomy.h). That header only exists on the
 * Android wrapper's include path while this file also compiles for the
 * crawler's own host `make`, so the values are mirrored here. They are frozen
 * by the data already on the phone; the category *names* come from
 * classifier_category_name(), so the JSON keys keep matching /stats.
 * ------------------------------------------------------------------------ */
typedef enum {
    CLS_CAT_UNKNOWN = 0,
    CLS_CAT_MOVIE,
    CLS_CAT_TV,
    CLS_CAT_ANIME,
    CLS_CAT_MUSIC,
    CLS_CAT_AUDIOBOOK,
    CLS_CAT_EBOOK,
    CLS_CAT_COMIC,
    CLS_CAT_SOFTWARE,
    CLS_CAT_GAME,
    CLS_CAT_IMAGE,
    CLS_CAT_OTHER,
    CLS_NUM_CATEGORIES
} cls_category_t;

/* tp_tv_kind_t display names; 0 (none) is stored as SQL NULL in torrent_class
 * (see database.h) and therefore has no filter name of its own. */
static const char *const CLS_TV_KIND_NAMES[] = {
    "none", "episode", "multi_episode", "season_pack", "multi_season",
    "complete_series", "daily"
};
#define CLS_NUM_TV_KINDS ((int)(sizeof(CLS_TV_KIND_NAMES) / sizeof(CLS_TV_KIND_NAMES[0])))

typedef enum {
    CLS_ADULT_HIDE,  /* classified non-adult rows only: c.adult = 0 */
    CLS_ADULT_SHOW,  /* no adult condition at all */
    CLS_ADULT_ONLY   /* classified adult rows only: c.adult = 1 */
} cls_adult_mode_t;

/* torrent_class filter parsed from a query string. */
typedef struct {
    int present;            /* at least one stage-7 param was supplied */
    cls_adult_mode_t adult; /* CLS_ADULT_HIDE unless the caller says otherwise */
    int has_category;
    int category;           /* tp_category_t */
    int has_tv_kind;
    int tv_kind;            /* tp_tv_kind_t */
    int has_min_conf;
    double min_conf;
} cls_filter_t;

/* Bind parameters a fully populated filter consumes; see
 * append_class_conditions() and bind_class_filter(). */
#define CLS_FILTER_MAX_BINDS 3

/* Forward declarations */
static int search_handler(struct mg_connection *conn, void *cbdata);
static int stats_handler(struct mg_connection *conn, void *cbdata);
static int refresh_handler(struct mg_connection *conn, void *cbdata);
static int random_handler(struct mg_connection *conn, void *cbdata);
static void format_hex(const uint8_t *data, size_t len, char *out);
static void format_size(int64_t bytes, char *out, size_t out_len);
static char* url_decode(const char *str);
static int search_torrents_filtered(database_t *db, const char *query,
                                    const cls_filter_t *filter, int offset,
                                    search_result_t **results, int *count, int *total_count);
static int browse_handler(struct mg_connection *conn, void *cbdata);
static int sb_append(char *buf, size_t bufsize, size_t *pos, const char *fmt, ...);

static int get_random_torrents(database_t *db, const cls_filter_t *filter, long min_peers, search_result_t **results, int *count);

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

/* --- Stage 7: torrent_class filter parsing and rendering helpers ---------- */

/* tp_category_t value for a taxonomy name, or -1 when it is not one of them.
 * The names are the /stats by_category keys, so the two cannot drift. */
static int class_category_from_name(const char *name) {
    for (int i = 0; i < CLASSIFIER_NUM_CATEGORIES; i++) {
        if (strcmp(name, classifier_category_name(i)) == 0) return i;
    }
    return -1;
}

/* tp_tv_kind_t value for a name, or -1 when it is not one of them. The names
 * start at 1: "none" (0) is stored as SQL NULL and is only reachable through
 * the integer form, which matches the stage-7 parameter list. */
static int class_tv_kind_from_name(const char *name) {
    for (int i = 1; i < CLS_NUM_TV_KINDS; i++) {
        if (strcmp(name, CLS_TV_KIND_NAMES[i]) == 0) return i;
    }
    return -1;
}

/* Display name for a stored tv_kind, or NULL when out of range. */
static const char *class_tv_kind_name(int tv_kind) {
    if (tv_kind < 0 || tv_kind >= CLS_NUM_TV_KINDS) return NULL;
    return CLS_TV_KIND_NAMES[tv_kind];
}

/* Strict integer parse: the whole string has to be one integer in [min, max].
 * Overflow saturates to LONG_MAX/LONG_MIN and is rejected by the range check,
 * so errno does not have to be consulted. */
static int parse_long_param(const char *s, long min, long max, long *out) {
    if (!s || !*s) return -1;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (end == s || *end != '\0' || v < min || v > max) return -1;
    *out = v;
    return 0;
}

/* Strict float parse: whole string, finite, in [min, max]. */
static int parse_double_param(const char *s, double min, double max, double *out) {
    if (!s || !*s) return -1;
    char *end = NULL;
    double v = strtod(s, &end);
    if (end == s || *end != '\0' || !(v >= min && v <= max)) return -1;
    *out = v;
    return 0;
}

/* Parse the four stage-7 query parameters shared by /search and /browse:
 *   category=<0-11|name>  tv_kind=<0-6|name>  adult=hide|show|only  min_conf=<0..1>
 * All four are optional and case-sensitive. Only parsed numbers and taxonomy
 * codes leave this function: the names are mapped to their enum value and
 * bound as ints, and no user-supplied text ever reaches the SQL.
 *
 * A malformed value returns -1 and points *bad_param at the offending
 * parameter name (a static string). On success out->present is 1 only when at
 * least one of the four was supplied, which is what turns the adult=hide
 * default on; /search keeps its pre-stage-7 behaviour when none of them is
 * present (see search_handler). */
static int parse_class_filter(const char *query_string, cls_filter_t *out, const char **bad_param) {
    memset(out, 0, sizeof(*out));
    out->adult = CLS_ADULT_HIDE;
    if (bad_param) *bad_param = NULL;

    const char *qs = query_string ? query_string : "";
    size_t qs_len = strlen(qs);
    char buf[64];
    int n;
    long value;

    n = mg_get_var(qs, qs_len, "adult", buf, sizeof(buf));
    if (n >= 0) {
        out->present = 1;
        if (strcmp(buf, "hide") == 0) {
            out->adult = CLS_ADULT_HIDE;
        } else if (strcmp(buf, "show") == 0) {
            out->adult = CLS_ADULT_SHOW;
        } else if (strcmp(buf, "only") == 0) {
            out->adult = CLS_ADULT_ONLY;
        } else {
            if (bad_param) *bad_param = "adult";
            return -1;
        }
    }

    n = mg_get_var(qs, qs_len, "category", buf, sizeof(buf));
    if (n >= 0) {
        out->present = 1;
        if (parse_long_param(buf, 0, CLS_NUM_CATEGORIES - 1, &value) != 0) {
            value = class_category_from_name(buf);
            if (value < 0) {
                if (bad_param) *bad_param = "category";
                return -1;
            }
        }
        out->has_category = 1;
        out->category = (int)value;
    }

    n = mg_get_var(qs, qs_len, "tv_kind", buf, sizeof(buf));
    if (n >= 0) {
        out->present = 1;
        if (parse_long_param(buf, 0, CLS_NUM_TV_KINDS - 1, &value) != 0) {
            value = class_tv_kind_from_name(buf);
            if (value < 0) {
                if (bad_param) *bad_param = "tv_kind";
                return -1;
            }
        }
        out->has_tv_kind = 1;
        out->tv_kind = (int)value;
    }

    n = mg_get_var(qs, qs_len, "min_conf", buf, sizeof(buf));
    if (n >= 0) {
        out->present = 1;
        if (parse_double_param(buf, 0.0, 1.0, &out->min_conf) != 0) {
            if (bad_param) *bad_param = "min_conf";
            return -1;
        }
        out->has_min_conf = 1;
    }

    return 0;
}

/* 1 when the filter actually restricts the row set, i.e. when the caller has to
 * join torrent_class. A NULL filter, and adult=show without any column
 * condition, leave the legacy behaviour (unclassified rows included) alone. */
static int class_filter_needs_join(const cls_filter_t *f) {
    if (!f) return 0;
    return f->adult != CLS_ADULT_SHOW || f->has_category || f->has_tv_kind || f->has_min_conf;
}

/* Append the conditions of *f* as " AND ..." fragments, numbering bind
 * parameters from first_bind on, and return how many were used. The caller
 * supplies what the first fragment attaches to: "JOIN torrent_class c ON
 * c.torrent_id = t.id" (SQLite can then drive the join off the partial index
 * idx_class_cat when the filter names a category) or "WHERE 1" for a
 * torrent_class-driven query. The condition order must stay in sync with
 * bind_class_filter(). */
static int append_class_conditions(char *buf, size_t bufsize, int *pos,
                                   const cls_filter_t *f, int first_bind) {
    if (!f || !pos || *pos < 0) return 0;

    /* sb_append() bounds-checks every fragment and never advances the offset on
     * a failed write, so the caller cannot end up with a negative remaining
     * size for the fragment that follows. */
    size_t sp = (size_t)*pos;
    int binds = 0;
    int rc = 0;

    if (f->adult == CLS_ADULT_HIDE) {
        rc = sb_append(buf, bufsize, &sp, " AND c.adult = 0");
    } else if (f->adult == CLS_ADULT_ONLY) {
        rc = sb_append(buf, bufsize, &sp, " AND c.adult = 1");
    }

    if (rc == 0 && f->has_category) {
        rc = sb_append(buf, bufsize, &sp, " AND c.category = ?%d", first_bind + binds);
        if (rc == 0) binds++;
    }

    if (rc == 0 && f->has_tv_kind) {
        if (f->tv_kind <= 0) {
            /* tp_tv_kind_t 0 (none) is stored as SQL NULL. */
            rc = sb_append(buf, bufsize, &sp, " AND c.tv_kind IS NULL");
        } else {
            rc = sb_append(buf, bufsize, &sp, " AND c.tv_kind = ?%d", first_bind + binds);
            if (rc == 0) binds++;
        }
    }

    if (rc == 0 && f->has_min_conf) {
        rc = sb_append(buf, bufsize, &sp, " AND c.confidence >= ?%d", first_bind + binds);
        if (rc == 0) binds++;
    }

    *pos = (int)sp;
    return rc == 0 ? binds : 0;
}

/* Bind the parameters appended by append_class_conditions(), same order. */
static void bind_class_filter(sqlite3_stmt *stmt, int first_bind, const cls_filter_t *f) {
    int idx = first_bind;

    if (f->has_category) {
        sqlite3_bind_int(stmt, idx++, f->category);
    }
    if (f->has_tv_kind && f->tv_kind > 0) {
        sqlite3_bind_int(stmt, idx++, f->tv_kind);
    }
    if (f->has_min_conf) {
        sqlite3_bind_double(stmt, idx++, f->min_conf);
    }
}

/* Write a {"error":"..."} response with the given HTTP status. */
static int send_json_error(struct mg_connection *conn, int status, const char *message) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "error", message);
    char *json = cJSON_Print(root);
    cJSON_Delete(root);

    if (!json) return 500;

    mg_printf(conn,
              "HTTP/1.1 %d %s\r\n"
              "Content-Type: application/json\r\n"
              "Content-Length: %d\r\n"
              "\r\n"
              "%s",
              status, status == 400 ? "Bad Request" : "Internal Server Error",
              (int)strlen(json), json);
    free(json);
    return status;
}

/* Add the stage-7 torrent_class fields to one result object; absent entirely
 * when the torrent has no class row. */
static void add_class_fields_json(cJSON *item, const search_result_t *r) {
    if (!r->has_class) return;

    cJSON_AddStringToObject(item, "category", classifier_category_name(r->category));

    const char *tv_kind = r->has_tv_kind ? class_tv_kind_name(r->tv_kind) : NULL;
    if (tv_kind) {
        cJSON_AddStringToObject(item, "tv_kind", tv_kind);
    } else {
        cJSON_AddNullToObject(item, "tv_kind");
    }

    cJSON_AddNumberToObject(item, "adult", r->adult);

    if (r->has_confidence) {
        cJSON_AddNumberToObject(item, "confidence", r->confidence);
    } else {
        cJSON_AddNullToObject(item, "confidence");
    }
}

/* Load up to 10 file rows (prefix-qualified path + size) for one result. Only
 * called when the result has files, like the inline version it replaces. */
static void fetch_result_files(database_t *db, search_result_t *r) {
    /* Runs once per result row inside the long search/browse/random loops, so
     * it must stay on the calling thread's own read connection (see
     * database_read_db) instead of the process-wide db->db. */
    sqlite3 *conn = database_read_db(db);
    if (!conn) {
        return;
    }

    const char *files_sql =
        "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename) as path, "
        "       tf.size_bytes "
        "FROM torrent_files tf "
        "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
        "WHERE tf.torrent_id = (SELECT id FROM torrents WHERE info_hash = ?) "
        "ORDER BY tf.file_index LIMIT 10";

    sqlite3_stmt *files_stmt;
    if (sqlite3_prepare_v2(conn, files_sql, -1, &files_stmt, NULL) != SQLITE_OK) {
        return;
    }
    sqlite3_bind_blob(files_stmt, 1, r->info_hash, 20, SQLITE_STATIC);

    int file_count = 0;
    r->file_paths = (char **)calloc(10, sizeof(char *));
    r->file_sizes = (int64_t *)calloc(10, sizeof(int64_t));

    while (sqlite3_step(files_stmt) == SQLITE_ROW && file_count < 10) {
        r->file_paths[file_count] = strdup((const char *)sqlite3_column_text(files_stmt, 0));
        r->file_sizes[file_count] = sqlite3_column_int64(files_stmt, 1);
        file_count++;
    }

    sqlite3_finalize(files_stmt);
}

/* Serialize the /search and /browse result envelope and write the 200
 * response; frees results. */
static int send_json_results(struct mg_connection *conn, search_result_t *results,
                             int count, int total_count, int page) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "count", count);
    cJSON_AddNumberToObject(root, "total_count", total_count);
    cJSON_AddNumberToObject(root, "page", page);
    cJSON_AddNumberToObject(root, "limit", HTTP_API_MAX_RESULTS);
    cJSON_AddBoolToObject(root, "has_more", (page - 1) * HTTP_API_MAX_RESULTS + count < total_count);
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

        /* Stage 7: torrent_class fields, when the torrent has a class row. */
        add_class_fields_json(item, &results[i]);

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

    if (!json) {
        free_search_results(results, count);
        return 500;
    }

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

/* Initialize HTTP API */
int http_api_init(http_api_t *api, app_context_t *app_ctx, database_t *database,
                  struct batch_writer *batch_writer, int port, const char *bind) {
    if (!api || !app_ctx || !database) {
        return -1;
    }
    memset(api, 0, sizeof(http_api_t));
    api->app_ctx = app_ctx;
    api->database = database;
    api->batch_writer = batch_writer;
    api->port = port;
    if (bind && bind[0]) {
        strncpy(api->bind, bind, sizeof(api->bind) - 1);
        api->bind[sizeof(api->bind) - 1] = '\0';
    }
    api->running = 0;
    log_msg(LOG_DEBUG, "HTTP API initialized (port %d, bind %s)", port, api->bind[0] ? api->bind : "0.0.0.0");
    return 0;
}

/* Start HTTP API server */
int http_api_start(http_api_t *api) {
    if (!api || api->running) {
        return -1;
    }

    /* Build the CivetWeb listening_ports value: "<bind>:<port>" when a specific
     * bind address is configured, otherwise just "<port>" (bind all interfaces, as before). */
    char listening_ports[80];
    if (api->bind[0] != '\0' && strcmp(api->bind, "0.0.0.0") != 0) {
        snprintf(listening_ports, sizeof(listening_ports), "%s:%d", api->bind, api->port);
    } else {
        snprintf(listening_ports, sizeof(listening_ports), "%d", api->port);
    }

    /* CivetWeb options */
    const char *options[] = {
        "listening_ports", listening_ports,
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
    mg_set_request_handler(api->mg_ctx, "/search", search_handler, api);
    mg_set_request_handler(api->mg_ctx, "/browse", browse_handler, api);
    mg_set_request_handler(api->mg_ctx, "/stats", stats_handler, api);
    mg_set_request_handler(api->mg_ctx, "/refresh", refresh_handler, api);
    mg_set_request_handler(api->mg_ctx, "/random",        random_handler, api);
    mg_set_request_handler(api->mg_ctx, "/torrent/files", http_torrent_files_handler, api);
    mg_set_request_handler(api->mg_ctx, "/torrent/info", http_torrent_info_handler, api);

    api->running = 1;
    log_msg(LOG_DEBUG, "HTTP API server started on %s", listening_ports);
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

void http_api_set_refresh_thread(http_api_t *api, struct refresh_thread *refresh_thread) {
    if (api) {
        api->refresh_thread = refresh_thread;
    }
}

void http_api_set_refresh_query_store(http_api_t *api, refresh_query_store_t *query_store) {
    if (api) {
        api->refresh_query_store = query_store;
    }
}

/* Stats handler - Return JSON statistics */
static int stats_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get cached database statistics (fast - no COUNT(*) queries) */
    uint64_t torrent_count = 0;
    uint64_t file_count = 0;
    size_t hourly_count = 0;

    if (api->batch_writer) {
        torrent_count = batch_writer_get_torrent_count(api->batch_writer);
        file_count = batch_writer_get_file_count(api->batch_writer);
        hourly_count = batch_writer_get_hourly_count(api->batch_writer);
    }


    /* Build JSON response */
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "version", DHT_CRAWLER_VERSION);
    cJSON_AddNumberToObject(root, "torrents_indexed", torrent_count);
    cJSON_AddNumberToObject(root, "files_indexed", file_count);
    cJSON_AddNumberToObject(root, "torrents_last_hour", (double)hourly_count);
    cJSON_AddNumberToObject(root, "timestamp", time(NULL));
    /* Outbound UDP rate limiting (Stage 2) */
    cJSON_AddNumberToObject(root, "udp_rate_limit_pps", (double)udp_rate_limit_pps());
    cJSON_AddNumberToObject(root, "udp_sends_total", (double)udp_rate_limit_sends_total());
    cJSON_AddNumberToObject(root, "udp_throttled_sends_total", (double)udp_rate_limit_throttled_total());

    /* Calculate and format uptime */
    time_t uptime_seconds = time(NULL) - api->app_ctx->start_time;
    int uptime_hours = (int)(uptime_seconds / 3600);
    int uptime_minutes = (int)((uptime_seconds % 3600) / 60);
    char uptime_str[32];
    snprintf(uptime_str, sizeof(uptime_str), "%d:%02d", uptime_hours, uptime_minutes);
    cJSON_AddStringToObject(root, "uptime", uptime_str);

    /* Effective get_peers knobs the supervisor actually runs with (after
     * defaulting), so a probe can tell "configured" from "built in". */
    if (api->supervisor) {
        cJSON *config_json = cJSON_CreateObject();
        cJSON_AddNumberToObject(config_json, "tree_get_peers_workers",
                                api->supervisor->num_get_peers_workers);
        cJSON_AddNumberToObject(config_json, "tree_get_peers_timeout_ms",
                                api->supervisor->get_peers_timeout_ms);
        cJSON_AddItemToObject(root, "config", config_json);
    }

    /* Add language filter statistics */
    language_filter_stats_t lf_stats;
    language_filter_get_stats(&lf_stats);
    if (lf_stats.total_checked > 0) {
        cJSON *language_filter = cJSON_CreateObject();
        cJSON_AddNumberToObject(language_filter, "total_checked", (double)lf_stats.total_checked);
        cJSON_AddNumberToObject(language_filter, "total_filtered", (double)lf_stats.total_filtered);
        cJSON_AddNumberToObject(language_filter, "filtered_by_non_latin", (double)lf_stats.filtered_by_non_latin);

        double filter_rate = (lf_stats.total_filtered * 100.0) / lf_stats.total_checked;
        cJSON_AddNumberToObject(language_filter, "filter_rate_percent", filter_rate);

        cJSON_AddItemToObject(root, "language_filter", language_filter);
    }

    /* Stage 5: ingest-time classifier counters (present even at zero, so a
     * dashboard can tell "no data yet" from "not built in"). */
    {
        classifier_stats_t clf_stats;
        classifier_get_stats(&clf_stats);

        cJSON *classifier = cJSON_CreateObject();
        cJSON_AddBoolToObject(classifier, "enabled", clf_stats.enabled ? 1 : 0);
        /* Active ingest adult mode and the boundary it is applied on: drop mode
         * discards every adult-tagged row, i.e. every row with adult_p >= this
         * threshold. Required to tell "drop mode is on" from "all rows happen
         * to score below the tag boundary". */
        cJSON_AddStringToObject(classifier, "adult_mode",
                                clf_stats.adult_mode[0] ? clf_stats.adult_mode : "tag");
        cJSON_AddNumberToObject(classifier, "adult_hide_threshold",
                                clf_stats.adult_hide_threshold);
        cJSON_AddNumberToObject(classifier, "total_classified", (double)clf_stats.total_classified);

        double mean_us = 0.0;
        if (clf_stats.total_classified > 0) {
            mean_us = (double)clf_stats.total_us / (double)clf_stats.total_classified;
        }
        cJSON_AddNumberToObject(classifier, "mean_us_per_item", mean_us);
        cJSON_AddNumberToObject(classifier, "adult_tagged", (double)clf_stats.adult_tagged);

        cJSON_AddNumberToObject(classifier, "adult_dropped", (double)clf_stats.adult_dropped);
        cJSON_AddNumberToObject(classifier, "hard_block_dropped",
                                (double)clf_stats.hard_block_dropped);
        cJSON_AddNumberToObject(classifier, "parser_only_fallback",
                                (double)clf_stats.parser_only_fallback);
        cJSON_AddNumberToObject(classifier, "model_load_failures",
                                (double)clf_stats.model_load_failures);
        cJSON_AddNumberToObject(classifier, "tv_kind_model_used",
                                (double)clf_stats.tv_kind_model_used);

        /* Only the categories that actually occurred, keyed by taxonomy name. */
        cJSON *by_category = cJSON_CreateObject();
        for (int i = 0; i < CLASSIFIER_NUM_CATEGORIES; i++) {
            if (clf_stats.by_category[i] > 0) {
                cJSON_AddNumberToObject(by_category, classifier_category_name(i),
                                        (double)clf_stats.by_category[i]);
            }
        }
        cJSON_AddItemToObject(classifier, "by_category", by_category);

        cJSON_AddItemToObject(root, "classifier", classifier);
    }

    /* "Categories to save": ingest drops caused by save_categories /
     * save_tv_kinds. Only present once something has been dropped, so an
     * unfiltered crawl reports exactly the /stats it always did. */
    {
        category_filter_stats_t cf_stats;
        category_filter_get_stats(&cf_stats);

        if (cf_stats.total_dropped > 0) {
            cJSON *category_filter = cJSON_CreateObject();
            cJSON_AddNumberToObject(category_filter, "total_dropped",
                                    (double)cf_stats.total_dropped);
            cJSON_AddNumberToObject(category_filter, "dropped_by_category",
                                    (double)cf_stats.dropped_by_category);
            cJSON_AddNumberToObject(category_filter, "dropped_by_tv_kind",
                                    (double)cf_stats.dropped_by_tv_kind);
            cJSON_AddItemToObject(root, "category_filter", category_filter);
        }
    }

    /* Stage 6: Add supervisor/thread tree statistics */
    if (api->supervisor) {
        int active_trees = 0;
        uint64_t total_metadata = 0;
        uint64_t first_strike_failures = 0;
        uint64_t second_strike_failures = 0;
        uint64_t fresh_peer_relookups = 0;
        uint64_t fresh_peer_rescues = 0;
        uint64_t metadata_piece_reasks = 0;
        supervisor_stats(api->supervisor, &active_trees, &total_metadata,
                        &first_strike_failures, &second_strike_failures,
                        &fresh_peer_relookups, &fresh_peer_rescues,
                        &metadata_piece_reasks);

        cJSON *supervisor_json = cJSON_CreateObject();
        cJSON_AddNumberToObject(supervisor_json, "active_trees", active_trees);
        cJSON_AddNumberToObject(supervisor_json, "max_trees", api->supervisor->max_trees);
        cJSON_AddNumberToObject(supervisor_json, "total_trees_spawned", (double)api->supervisor->next_tree_id - 1);
        cJSON_AddNumberToObject(supervisor_json, "total_metadata_fetched", (double)total_metadata);
        cJSON_AddNumberToObject(supervisor_json, "first_strike_failures", (double)first_strike_failures);
        cJSON_AddNumberToObject(supervisor_json, "second_strike_failures", (double)second_strike_failures);
        cJSON_AddNumberToObject(supervisor_json, "fresh_peer_relookups_total", (double)fresh_peer_relookups);
        cJSON_AddNumberToObject(supervisor_json, "fresh_peer_rescues_total", (double)fresh_peer_rescues);
        cJSON_AddNumberToObject(supervisor_json, "metadata_piece_reasks_total", (double)metadata_piece_reasks);

        /* Add draining tree statistics */
        int draining_count = 0, draining_max = 0, draining_connections = 0;
        supervisor_get_draining_stats(api->supervisor, &draining_count, &draining_max, &draining_connections);
        cJSON_AddNumberToObject(supervisor_json, "draining_trees_count", draining_count);
        cJSON_AddNumberToObject(supervisor_json, "draining_trees_max", draining_max);
        cJSON_AddNumberToObject(supervisor_json, "draining_trees_connections", draining_connections);

        /* get_peers lookup totals (cumulative respawned trees + live trees) */
        uint64_t sup_gp_lookups = 0;
        uint64_t sup_gp_with_peers = 0;
        uint64_t sup_gp_from_retry = 0;
        uint64_t sup_gp_packets = 0;
        uint64_t sup_gp_answers = 0;
        uint64_t sup_gp_lookup_us = 0;
        supervisor_getpeers_stats(api->supervisor, &sup_gp_lookups, &sup_gp_with_peers,
                                  &sup_gp_from_retry, &sup_gp_packets, &sup_gp_answers,
                                  &sup_gp_lookup_us);

        cJSON *supervisor_get_peers = cJSON_CreateObject();
        cJSON_AddNumberToObject(supervisor_get_peers, "lookups_total", (double)sup_gp_lookups);
        cJSON_AddNumberToObject(supervisor_get_peers, "lookups_with_peers", (double)sup_gp_with_peers);

        double sup_gp_yield_percent = 0.0;
        if (sup_gp_lookups > 0) {
            sup_gp_yield_percent = (sup_gp_with_peers * 100.0) / (double)sup_gp_lookups;
        }
        cJSON_AddNumberToObject(supervisor_get_peers, "yield_percent", sup_gp_yield_percent);

        cJSON_AddNumberToObject(supervisor_get_peers, "packets_total", (double)sup_gp_packets);

        double sup_gp_packets_per_lookup = 0.0;
        if (sup_gp_lookups > 0) {
            sup_gp_packets_per_lookup = (double)sup_gp_packets / (double)sup_gp_lookups;
        }
        cJSON_AddNumberToObject(supervisor_get_peers, "packets_per_lookup", sup_gp_packets_per_lookup);

        double sup_gp_mean_lookup_ms = 0.0;
        if (sup_gp_lookups > 0) {
            sup_gp_mean_lookup_ms = ((double)sup_gp_lookup_us / (double)sup_gp_lookups) / 1000.0;
        }
        cJSON_AddNumberToObject(supervisor_get_peers, "mean_lookup_ms", sup_gp_mean_lookup_ms);

        cJSON_AddNumberToObject(supervisor_get_peers, "answers_total", (double)sup_gp_answers);
        cJSON_AddNumberToObject(supervisor_get_peers, "from_retry_total", (double)sup_gp_from_retry);
        cJSON_AddItemToObject(supervisor_json, "get_peers", supervisor_get_peers);

        /* UDP dispatcher counters. With a fixed dht_port every tree aliases the
         * supervisor's shared dispatcher, so read it ONCE - summing per tree
         * would multiply the counts by the tree count. Private dispatchers are
         * summed under trees_lock. */
        uint64_t disp_total_responses = 0;
        uint64_t disp_routed_responses = 0;
        uint64_t disp_dropped_responses = 0;
        uint64_t disp_queue_full_drops = 0;
        if (api->supervisor->shared_dispatcher) {
            disp_total_responses = atomic_load(&api->supervisor->shared_dispatcher->total_responses);
            disp_routed_responses = atomic_load(&api->supervisor->shared_dispatcher->routed_responses);
            disp_dropped_responses = atomic_load(&api->supervisor->shared_dispatcher->dropped_responses);
            disp_queue_full_drops = atomic_load(&api->supervisor->shared_dispatcher->queue_full_drops);
        } else {
            pthread_mutex_lock(&api->supervisor->trees_lock);
            for (int i = 0; i < api->supervisor->max_trees; i++) {
                thread_tree_t *disp_tree = api->supervisor->trees[i];
                if (disp_tree && disp_tree->dispatcher) {
                    disp_total_responses += atomic_load(&disp_tree->dispatcher->total_responses);
                    disp_routed_responses += atomic_load(&disp_tree->dispatcher->routed_responses);
                    disp_dropped_responses += atomic_load(&disp_tree->dispatcher->dropped_responses);
                    disp_queue_full_drops += atomic_load(&disp_tree->dispatcher->queue_full_drops);
                }
            }
            pthread_mutex_unlock(&api->supervisor->trees_lock);
        }

        cJSON *dispatcher_json = cJSON_CreateObject();
        cJSON_AddNumberToObject(dispatcher_json, "total_responses", (double)disp_total_responses);
        cJSON_AddNumberToObject(dispatcher_json, "routed_responses", (double)disp_routed_responses);
        cJSON_AddNumberToObject(dispatcher_json, "dropped_responses", (double)disp_dropped_responses);
        cJSON_AddNumberToObject(dispatcher_json, "queue_full_drops", (double)disp_queue_full_drops);
        cJSON_AddItemToObject(supervisor_json, "dispatcher", dispatcher_json);

        cJSON_AddItemToObject(root, "supervisor", supervisor_json);

        /* BEP51 cache global stats */
        cJSON *cache_obj = cJSON_CreateObject();
        if (api->supervisor->bep51_cache) {
            size_t cache_count = bep51_cache_get_count(api->supervisor->bep51_cache);
            cJSON_AddNumberToObject(cache_obj, "cached_nodes", (double)cache_count);
            cJSON_AddNumberToObject(cache_obj, "capacity", api->supervisor->bep51_cache_capacity);
            cJSON_AddNumberToObject(cache_obj, "submit_percent", api->supervisor->bep51_cache_submit_percent);
        }
        cJSON_AddItemToObject(root, "bep51_cache", cache_obj);

        /* Add per-tree stats */
        cJSON *trees_array = cJSON_CreateArray();
        pthread_mutex_lock(&api->supervisor->trees_lock);
        for (int i = 0; i < api->supervisor->max_trees; i++) {
            thread_tree_t *tree = api->supervisor->trees[i];
            if (tree) {
                cJSON *tree_json = cJSON_CreateObject();
                cJSON_AddNumberToObject(tree_json, "tree_id", tree->tree_id);
                cJSON_AddNumberToObject(tree_json, "partition_index", tree->partition_index);
                cJSON_AddStringToObject(tree_json, "phase", thread_tree_phase_name(atomic_load(&tree->current_phase)));
                cJSON_AddNumberToObject(tree_json, "metadata_count", (double)atomic_load(&tree->metadata_count));
                cJSON_AddNumberToObject(tree_json, "metadata_piece_reasks", (double)atomic_load(&tree->metadata_piece_reasks));
                cJSON_AddNumberToObject(tree_json, "filtered_count", (double)atomic_load(&tree->filtered_count));
                cJSON_AddNumberToObject(tree_json, "metadata_rate", tree->metadata_rate);
                cJSON_AddNumberToObject(tree_json, "active_connections", (int)atomic_load(&tree->active_connections));
                cJSON_AddBoolToObject(tree_json, "discovery_paused", atomic_load(&tree->discovery_paused));
                cJSON_AddBoolToObject(tree_json, "get_peers_paused", atomic_load(&tree->get_peers_paused));

                /* BEP51 cache statistics */
                cJSON_AddNumberToObject(tree_json, "bep51_nodes_cached", (double)atomic_load(&tree->bep51_nodes_cached));

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

                /* infohash queue push accounting. tree_infohash_queue_get_stats()
                 * leaves the out-params untouched when the queue is NULL, so zero
                 * the locals first. */
                uint64_t infohash_push_attempts = 0;
                uint64_t infohash_duplicates_rejected = 0;
                if (tree->infohash_queue) {
                    tree_infohash_queue_get_stats(tree->infohash_queue,
                                                  &infohash_push_attempts,
                                                  &infohash_duplicates_rejected);
                }
                cJSON_AddNumberToObject(queues, "infohash_push_attempts", (double)infohash_push_attempts);
                cJSON_AddNumberToObject(queues, "infohash_duplicates_rejected", (double)infohash_duplicates_rejected);

                cJSON_AddItemToObject(tree_json, "queues", queues);

                /* get_peers lookup instrumentation. Every ratio is guarded on a
                 * non-zero divisor: cJSON prints invalid JSON for NaN/Inf. */
                uint64_t gp_lookups = atomic_load(&tree->gp_lookups);
                uint64_t gp_with_peers = atomic_load(&tree->gp_lookups_with_peers);
                uint64_t gp_from_retry = atomic_load(&tree->gp_lookups_from_retry);
                uint64_t gp_packets = atomic_load(&tree->gp_packets_sent);
                uint64_t gp_answers = atomic_load(&tree->gp_answers);
                uint64_t gp_lookup_us = atomic_load(&tree->gp_lookup_us_total);

                cJSON *get_peers = cJSON_CreateObject();
                cJSON_AddNumberToObject(get_peers, "lookups_total", (double)gp_lookups);
                cJSON_AddNumberToObject(get_peers, "lookups_with_peers", (double)gp_with_peers);

                double gp_yield_percent = 0.0;
                if (gp_lookups > 0) {
                    gp_yield_percent = (gp_with_peers * 100.0) / (double)gp_lookups;
                }
                cJSON_AddNumberToObject(get_peers, "yield_percent", gp_yield_percent);

                cJSON_AddNumberToObject(get_peers, "packets_total", (double)gp_packets);

                double gp_packets_per_lookup = 0.0;
                if (gp_lookups > 0) {
                    gp_packets_per_lookup = (double)gp_packets / (double)gp_lookups;
                }
                cJSON_AddNumberToObject(get_peers, "packets_per_lookup", gp_packets_per_lookup);

                double gp_mean_lookup_ms = 0.0;
                if (gp_lookups > 0) {
                    gp_mean_lookup_ms = ((double)gp_lookup_us / (double)gp_lookups) / 1000.0;
                }
                cJSON_AddNumberToObject(get_peers, "mean_lookup_ms", gp_mean_lookup_ms);

                cJSON_AddNumberToObject(get_peers, "answers_total", (double)gp_answers);
                cJSON_AddNumberToObject(get_peers, "from_retry_total", (double)gp_from_retry);
                cJSON_AddItemToObject(tree_json, "get_peers", get_peers);
                cJSON_AddItemToArray(trees_array, tree_json);
            }
        }
        pthread_mutex_unlock(&api->supervisor->trees_lock);
        cJSON_AddItemToObject(root, "trees", trees_array);

        /* Add partition stats */
        if (api->supervisor->partition_stats) {
            cJSON *partitions_array = cJSON_CreateArray();
            pthread_mutex_lock(&api->supervisor->trees_lock);
            /* Compute average metadata rate per partition from active trees */
            double *partition_rates = calloc(api->supervisor->max_trees, sizeof(double));
            if (partition_rates) {
                for (int s = 0; s < api->supervisor->max_trees; s++) {
                    if (api->supervisor->trees[s]) {
                        uint32_t p = api->supervisor->trees[s]->partition_index;
                        partition_rates[p] += api->supervisor->trees[s]->metadata_rate;
                    }
                }
                for (int i = 0; i < api->supervisor->max_trees; i++) {
                    int count = api->supervisor->partition_stats[i].current_tree_count;
                    if (count > 0) {
                        partition_rates[i] /= count;
                    }
                }
            }
            for (int i = 0; i < api->supervisor->max_trees; i++) {
                cJSON *part = cJSON_CreateObject();
                cJSON_AddNumberToObject(part, "partition", i);
                cJSON_AddNumberToObject(part, "avg_metadata_rate",
                    partition_rates ? partition_rates[i] : 0.0);
                cJSON_AddNumberToObject(part, "dead_consecutive",
                    api->supervisor->partition_stats[i].dead_consecutive);
                cJSON_AddNumberToObject(part, "current_tree_count",
                    api->supervisor->partition_stats[i].current_tree_count);
                cJSON_AddItemToArray(partitions_array, part);
            }
            free(partition_rates);
            pthread_mutex_unlock(&api->supervisor->trees_lock);
            cJSON_AddItemToObject(root, "partitions", partitions_array);
        }

        /* Add aggregate stats */
        cJSON *aggregate = cJSON_CreateObject();
        cJSON_AddNumberToObject(aggregate, "total_metadata_fetched", (double)total_metadata);

        /* Get total active connections across all trees */
        int total_connections = supervisor_get_total_connections(api->supervisor);
        cJSON_AddNumberToObject(aggregate, "active_connections", total_connections);

        /* Calculate metadata fetch success rate */
        uint64_t total_attempts = atomic_load(&api->supervisor->cumulative_metadata_attempts);
        pthread_mutex_lock(&api->supervisor->trees_lock);
        for (int i = 0; i < api->supervisor->max_trees; i++) {
            if (api->supervisor->trees[i]) {
                total_attempts += atomic_load(&api->supervisor->trees[i]->metadata_attempts);
            }
        }
        pthread_mutex_unlock(&api->supervisor->trees_lock);

        double success_rate = 0.0;
        if (total_attempts > 0) {
            success_rate = (total_metadata * 100.0) / total_attempts;
        }
        cJSON_AddNumberToObject(aggregate, "metadata_success_rate_percent", success_rate);

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
    database_read_lock(api->database);
    if (sqlite3_prepare_v2(api->database->db, check_sql, -1, &check_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_blob(check_stmt, 1, info_hash, 20, SQLITE_STATIC);
        if (sqlite3_step(check_stmt) == SQLITE_ROW) {
            exists = 1;
        }
        sqlite3_finalize(check_stmt);
    }
    database_read_unlock(api->database);

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

    /* Check refresh thread availability */
    if (!api->refresh_thread) {
        const char *error = "{\"error\":\"Refresh thread not available\"}";
        mg_printf(conn,
                  "HTTP/1.1 503 Service Unavailable\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 503;
    }

    /* Check refresh query store availability */
    if (!api->refresh_query_store) {
        const char *error = "{\"error\":\"Refresh query system not available\"}";
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
    refresh_query_t *query = refresh_query_create(api->refresh_query_store, info_hash);
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

    /* Submit to refresh thread */
    int rc = refresh_thread_submit_request(api->refresh_thread, info_hash);
    if (rc != 0) {
        log_msg(LOG_WARN, "Failed to submit refresh request, queue may be full");
        refresh_query_complete(api->refresh_query_store, info_hash);
        refresh_query_remove_ptr(api->refresh_query_store, query);
        const char *error = "{\"error\":\"Refresh queue full, try again later\"}";
        mg_printf(conn,
                  "HTTP/1.1 503 Service Unavailable\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %d\r\n"
                  "\r\n"
                  "%s",
                  (int)strlen(error), error);
        return 503;
    }

    /* Wait for results (8 seconds, matching old behavior) */
    int timed_out = 0;
    int peer_count = refresh_query_wait(query, 8, &timed_out);
    int retry_attempted = 0;

    /* Retry logic: retry up to refresh_retry_count times when zero peers found */
    int max_retries = api->refresh_retry_count > 0 ? api->refresh_retry_count : 1;
    for (int retry = 0; retry < max_retries && peer_count == 0 && !timed_out; retry++) {
        log_msg(LOG_DEBUG, "Query returned 0 peers for %s, retrying (%d/%d)...",
                hash_buf, retry + 1, max_retries);

        rc = refresh_thread_submit_request(api->refresh_thread, info_hash);
        if (rc != 0) break;

        peer_count = refresh_query_wait(query, 8, &timed_out);
        retry_attempted = 1;

        if (peer_count > 0) {
            log_msg(LOG_DEBUG, "Retry %d successful: found %d peers for %s",
                    retry + 1, peer_count, hash_buf);
        } else {
            log_msg(LOG_DEBUG, "Retry %d also returned 0 peers for %s", retry + 1, hash_buf);
        }
    }

    /* Remove query from store (this also unrefs) */
    refresh_query_remove_ptr(api->refresh_query_store, query);

    /* Build JSON response (matching old format) */
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "info_hash", hash_buf);
    cJSON_AddNumberToObject(root, "total_peers", peer_count);
    cJSON_AddBoolToObject(root, "live_query", 1);
    cJSON_AddBoolToObject(root, "timed_out", timed_out);
    cJSON_AddBoolToObject(root, "retry_attempted", retry_attempted);
    cJSON_AddNumberToObject(root, "timestamp", time(NULL));

    if (timed_out && peer_count == 0) {
        cJSON_AddStringToObject(root, "warning",
            "Query timed out with no peers - torrent may have no active peers");
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

/* General-purpose JSON /random handler: up to 50 random torrents with
 * total_peers >= min_peers, optionally filtered by category/tv_kind/adult.
 * Same JSON envelope as /search and /browse. */
static int random_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    const struct mg_request_info *ri = mg_get_request_info(conn);
    const char *qs = ri->query_string ? ri->query_string : "";
    size_t qs_len = strlen(qs);

    cls_filter_t filter;
    memset(&filter, 0, sizeof(filter));
    filter.adult = CLS_ADULT_HIDE;   /* default hide */

    long min_peers = 10;
    char buf[256];

    if (mg_get_var(qs, qs_len, "category", buf, sizeof(buf)) > 0) {
        long v;
        if (parse_long_param(buf, 0, CLS_NUM_CATEGORIES - 1, &v) == 0) {
            filter.category = (int)v;
        } else {
            int c = class_category_from_name(buf);
            if (c < 0) {
                return send_json_error(conn, 400, "Invalid value for 'category'");
            }
            filter.category = c;
        }
        filter.has_category = 1;
    }

    if (mg_get_var(qs, qs_len, "tv_kind", buf, sizeof(buf)) > 0) {
        long v;
        if (parse_long_param(buf, 0, CLS_NUM_TV_KINDS - 1, &v) == 0) {
            filter.tv_kind = (int)v;
        } else {
            int t = class_tv_kind_from_name(buf);
            if (t < 0) {
                return send_json_error(conn, 400, "Invalid value for 'tv_kind'");
            }
            filter.tv_kind = t;
        }
        filter.has_tv_kind = 1;
    }

    if (mg_get_var(qs, qs_len, "adult", buf, sizeof(buf)) > 0) {
        if (strcmp(buf, "hide") == 0) {
            filter.adult = CLS_ADULT_HIDE;
        } else if (strcmp(buf, "show") == 0) {
            filter.adult = CLS_ADULT_SHOW;
        } else if (strcmp(buf, "only") == 0) {
            filter.adult = CLS_ADULT_ONLY;
        } else {
            return send_json_error(conn, 400, "Invalid value for 'adult'");
        }
    }

    if (mg_get_var(qs, qs_len, "min_peers", buf, sizeof(buf)) > 0) {
        long v;
        if (parse_long_param(buf, 0, 100000, &v) != 0) {
            return send_json_error(conn, 400, "Invalid value for 'min_peers'");
        }
        min_peers = v;
    }

    search_result_t *results = NULL;
    int count = 0;
    if (get_random_torrents(api->database, &filter, min_peers, &results, &count) != 0) {
        return send_json_error(conn, 500, "Database query failed");
    }

    return send_json_results(conn, results, count, count, 1);
}

static int search_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    /* Get query parameter */
    char query_buf[256] = {0};
    char page_buf[16] = {0};
    const struct mg_request_info *ri = mg_get_request_info(conn);

    int query_len = mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
                                "q", query_buf, sizeof(query_buf));

    /* Get page parameter (default: 1) */
    mg_get_var(ri->query_string, strlen(ri->query_string ? ri->query_string : ""),
               "page", page_buf, sizeof(page_buf));

    /* Parse page number (1-indexed, default to 1) */
    int page = 1;
    if (strlen(page_buf) > 0) {
        page = atoi(page_buf);
        if (page < 1) page = 1;
    }
    int offset = (page - 1) * HTTP_API_MAX_RESULTS;

    if (query_len <= 0 || strlen(query_buf) == 0) {
        return send_json_error(conn, 400, "Missing query parameter 'q'");
    }

    /* Stage 7 filter parameters. All four are optional; a malformed value is a
     * 400. The adult=hide default only turns on when at least one of them is
     * supplied, so a plain /search?q= keeps the pre-stage-7 behaviour (adult
     * rows still visible, no class join) — see parse_class_filter(). */
    cls_filter_t class_filter;
    const char *bad_param = NULL;
    if (parse_class_filter(ri->query_string, &class_filter, &bad_param) != 0) {
        char error_msg[96];
        snprintf(error_msg, sizeof(error_msg), "Invalid value for '%s'", bad_param);
        return send_json_error(conn, 400, error_msg);
    }

    char *query = url_decode(query_buf);
    log_msg(LOG_DEBUG, "Search query: %s (page: %d)", query, page);

    /* Search database */
    search_result_t *results = NULL;
    int count = 0;
    int total_count = 0;
    int rc = search_torrents_filtered(api->database, query,
                                      class_filter.present ? &class_filter : NULL,
                                      offset, &results, &count, &total_count);
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

    /* Build JSON response (shared with /browse) */
    return send_json_results(conn, results, count, total_count, page);
}

/* --- Quoted search query parsing --- */

typedef struct {
    char *torrent_fts_query;  /* full query with quotes (porter handles natively) */
} parsed_query_t;

static void free_parsed_query(parsed_query_t *pq) {
    free(pq->torrent_fts_query);
    memset(pq, 0, sizeof(*pq));
}

/* Build the torrent_search FTS5 query string from the raw user query.
 *
 * FTS5 query syntax treats an unquoted '-' as an operator, so a plain title
 * query such as `Spider-Man Into The Spider Verse 2018` is parsed as
 * `Spider - Man ...` and the MATCH expression fails with "no such column:
 * Man". The handler swallows the statement error and reports zero results.
 * The index side has no such trouble: the porter tokenizer already treats '-'
 * as a separator inside torrent names, so `spider man into ...` matches exactly
 * the rows the hyphenated query should.
 * Normalizing unquoted '-' to a space therefore aligns the query tokens with
 * the tokens that were actually indexed.
 *
 * Quoted phrases are copied verbatim: quoting is how a user asks for an exact
 * phrase and the porter tokenizer handles it natively, so a '-' inside quotes
 * must stay untouched. Quote state is tracked with the same rule
 * parse_search_query() applies below (a '"' toggles, there are no escapes).
 * The mapping is 1:1 in length, so one strlen()+1 allocation is enough.
 * Returns NULL on allocation failure; the caller treats that as a parse error. */
static char *build_torrent_fts_query(const char *raw) {
    size_t len = strlen(raw);
    char *out = (char *)malloc(len + 1);
    if (!out) return NULL;

    int in_quotes = 0;
    for (size_t i = 0; i < len; i++) {
        char c = raw[i];
        if (c == '"') {
            in_quotes = !in_quotes;
            out[i] = c;
        } else if (c == '-' && !in_quotes) {
            out[i] = ' ';
        } else {
            out[i] = c;
        }
    }
    out[len] = '\0';
    return out;
}

static int parse_search_query(const char *raw, parsed_query_t *pq) {
    memset(pq, 0, sizeof(*pq));

    /* Build torrent FTS query: keep quotes (porter handles them natively) but
     * normalize unquoted hyphens, see build_torrent_fts_query(). */
    pq->torrent_fts_query = build_torrent_fts_query(raw);
    return pq->torrent_fts_query ? 0 : -1;
}

/* Bounded string append for the dynamically built SQL fragments. Unlike
 * `pos += snprintf(...)`, which keeps counting the characters that were cut
 * off, this never advances *pos on a failed write and reports it, so the caller
 * can bail out instead of computing a negative remaining size and writing past
 * the buffer. buf is always NUL-terminated. Returns 0 on success, -1 when fmt
 * did not fit (or on an encoding error). */
static int sb_append(char *buf, size_t bufsize, size_t *pos, const char *fmt, ...) {
    if (!buf || bufsize == 0 || !pos || *pos >= bufsize) return -1;

    va_list ap;
    va_start(ap, fmt);
    int written = vsnprintf(buf + *pos, bufsize - *pos, fmt, ap);
    va_end(ap);

    if (written < 0 || (size_t)written >= bufsize - *pos) {
        buf[bufsize - 1] = '\0';
        return -1;
    }
    *pos += (size_t)written;
    return 0;
}

/* Emit one accumulated term of build_docs_fts_query() into the FTS5
 * expression: verbatim when the term is already quoted (the quotes are the
 * phrase syntax) or when it is a plain [A-Za-z0-9] token, wrapped in double
 * quotes otherwise. A single space separates terms — FTS5's implicit AND.
 * Returns 0, or -1 when the append does not fit (see sb_append). */
static int docs_append_term(char *out, size_t out_len, size_t *pos, const char *term,
                            int quoted) {
    if (*pos > 0 && sb_append(out, out_len, pos, " ") != 0) return -1;
    if (quoted) return sb_append(out, out_len, pos, "%s", term);

    int plain = (term[0] != '\0');
    for (const char *p = term; *p; p++) {
        if (!((*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') ||
              (*p >= '0' && *p <= '9'))) {
            plain = 0;
            break;
        }
    }
    return plain ? sb_append(out, out_len, pos, "%s", term)
                 : sb_append(out, out_len, pos, "\"%s\"", term);
}

/* Build the FTS5 expression for the torrent-level unicode61 index
 * (torrent_docs).
 *
 * torrent_docs holds each torrent's name plus all of its filenames in a single
 * document, so one MATCH over it yields the torrent rowids directly.
 *
 * The input is the RAW user query, not parsed_query_t: that parser normalizes
 * the unquoted '-' for the porter index, while an FTS5 phrase here has to keep
 * its quotes. One pass, same quote rule as build_torrent_fts_query():
 * a '"' toggles, there are no escapes.
 *
 *  - a '"' is copied through: it is what turns the term into an FTS5 phrase,
 *    and keeping it verbatim preserves the input's quote parity;
 *  - inside quotes every byte is copied verbatim (a quoted region cannot hold a
 *    '"' — the toggle is the only way out, so the check order below is the
 *    guard that keeps it that way);
 *  - outside quotes '-' becomes a separator, exactly as
 *    build_torrent_fts_query() does: FTS5 reads a bare '-' as NOT, and the
 *    porter tokenizer already splits on it, so `spider-man` must match the same
 *    rows as `spider man`;
 *  - an unquoted token of pure [A-Za-z0-9] is emitted verbatim, anything else
 *    is wrapped in double quotes, so no FTS5 syntax character ever reaches the
 *    unicode61 parser unquoted (a bare '*' would be a prefix query, '(' a
 *    syntax error);
 *  - terms and phrases are joined by single spaces (FTS5's implicit AND).
 *
 * A query that ends inside a quote therefore produces an unterminated phrase,
 * just as build_torrent_fts_query() does today: the MATCH step fails, the
 * caller empties the match set, and /search answers HTTP 200 with count=0 (see
 * the step-failure handling in search_torrents_filtered). Quote parity is what
 * keeps that contract mechanical instead of a special case.
 *
 * Every append goes through sb_append(); returns 0 on success, -1 when the
 * expression does not fit out (the caller then fails the request). */
static int build_docs_fts_query(const char *raw, char *out, size_t out_len) {
    if (!raw || !out || out_len == 0) return -1;
    out[0] = '\0';

    /* One term or one quoted phrase, verbatim. search_handler caps q at 255
     * bytes, so this cannot fill up in practice; the bound check below keeps
     * the function total on its own. */
    char term[512];
    size_t term_len = 0;
    int term_quoted = 0;
    size_t pos = 0;
    int in_quotes = 0;

    term[0] = '\0';
    for (size_t i = 0;; i++) {
        char c = raw[i];

        if (c == '"') {
            in_quotes = !in_quotes;
            term_quoted = 1;
        } else if (c == '\0' || (!in_quotes && (c == ' ' || c == '-'))) {
            /* Term boundary: end of input, a space, or an unquoted '-'. */
            if (term_len > 0 &&
                docs_append_term(out, out_len, &pos, term, term_quoted) != 0) {
                return -1;
            }
            term_len = 0;
            term_quoted = 0;
            term[0] = '\0';
            if (c == '\0') return 0;
            continue;
        }
        /* Everything else belongs to the current term. */
        if (term_len + 1 >= sizeof(term)) return -1;
        term[term_len++] = c;
        term[term_len] = '\0';
    }
}


/* --- Stage 4: per-thread /search match-set cache ---------------------------
 *
 * Stage 3 builds the FTS match set once per request, but every /search page
 * and every repeat of the same search still rebuilt it: an FTS scan over the
 * whole corpus (seconds of random I/O on the phone's USB volume) for each of
 * the 50-row pages. The match set is cached per thread instead. Each request
 * thread owns one read connection (database_read_db, stage 1) and therefore
 * its own TEMP schema, so a per-thread cache can never leak between concurrent
 * searches.
 *
 * Two slots per thread: paging forward keeps page N-1's entry, and a second
 * search interleaved with the first (a filter toggle, say) still hits on the
 * way back. Eviction across the two slots is LRU. Entries expire after
 * SEARCH_MATCH_CACHE_TTL_SECS; staleness is by design (the crawler only
 * flushes new torrents every flush_interval=300 s, and the match set is a
 * superset question — a missing brand-new torrent for a few minutes is the
 * same window /stats already reports). There is no ingest invalidation.
 *
 * Everything below is metadata only: the SQL itself is unchanged stage-3 SQL
 * with the slot's table name substituted, and the table name comes from a
 * compile-time constant array, never from user input.
 * ---------------------------------------------------------------------- */

/* 300 s: matches the crawler's flush_interval, so an entry cannot outlive the
 * window in which the index it was built from is known to be unchanged. */
#define SEARCH_MATCH_CACHE_TTL_SECS 300

/* Slots per thread (and therefore TEMP tables per read connection). */
#define SEARCH_MATCH_CACHE_SLOTS 2

/* A pathological query (a single common letter, say) matches most of the
 * corpus. Serving it from the match set is fine, but pinning megabytes of
 * temp storage in both slots for 5 minutes is not: above this many rows the
 * set is used for this request and then dropped, never cached. */
#define SEARCH_MATCH_CACHE_MAX_ROWS 2000000

/* Temp table per slot. The names are compile-time constants: they are
 * substituted into the SQL below, so no request data ever reaches the SQL
 * text (see the literal-SQL rule in database.h). */
static const char *const SEARCH_MATCH_TABLES[SEARCH_MATCH_CACHE_SLOTS] = {
    "search_match_0",
    "search_match_1"
};

typedef struct {
    int used;                   /* slot holds a usable entry */
    char *key;                  /* malloc'd cache key (NULL while unused) */
    int total_count;            /* exact row count for that match set */
    time_t built_at;            /* time(NULL) when the set was built */
    unsigned long long last_used; /* LRU stamp (cache->tick value) */
} search_match_slot_t;

typedef struct {
    search_match_slot_t slot[SEARCH_MATCH_CACHE_SLOTS];
    unsigned long long tick;    /* monotonic LRU counter for this thread */
} search_match_cache_t;

/* TLS slot holding one search_match_cache_t per thread. Created once; the
 * destructor only frees heap metadata and must NOT issue SQL: pthread
 * destructor order across keys is unspecified, so the read connection (and
 * with it the TEMP tables) may already be gone by the time this runs — the
 * tables die with the connection either way (stage 1's destructor). */
static pthread_key_t g_search_match_cache_key;
static pthread_once_t g_search_match_cache_key_once = PTHREAD_ONCE_INIT;
static int g_search_match_cache_key_valid = 0;

static void search_match_cache_destructor(void *value) {
    search_match_cache_t *cache = (search_match_cache_t *)value;
    if (!cache) return;
    for (int i = 0; i < SEARCH_MATCH_CACHE_SLOTS; i++) {
        free(cache->slot[i].key);
    }
    free(cache);
}

static void search_match_cache_key_init(void) {
    if (pthread_key_create(&g_search_match_cache_key, search_match_cache_destructor) == 0) {
        g_search_match_cache_key_valid = 1;
    } else {
        /* Degrade to the stage-3 behaviour: rebuild per request. */
        log_msg(LOG_WARN, "Search match cache: cannot create TLS key, caching disabled");
    }
}

/* This thread's cache metadata, allocated on first use. NULL disables the
 * cache for the caller (every request still works, it just rebuilds). */
static search_match_cache_t *search_match_cache_get(void) {
    pthread_once(&g_search_match_cache_key_once, search_match_cache_key_init);
    if (!g_search_match_cache_key_valid) return NULL;

    search_match_cache_t *cache =
        (search_match_cache_t *)pthread_getspecific(g_search_match_cache_key);
    if (cache) return cache;

    cache = (search_match_cache_t *)calloc(1, sizeof(*cache));
    if (!cache) return NULL;
    if (pthread_setspecific(g_search_match_cache_key, cache) != 0) {
        free(cache);
        return NULL;
    }
    return cache;
}

/* The cache key: every input that determines the entry. The match set itself
 * only depends on the query, but the cached total_count also depends on the
 * class filter (the count query carries its conditions), so the whole filter
 * rides along. Nothing is normalized or lowercased here — the key holds the
 * same strings the SQL binds, and a forgotten component would silently serve
 * another query's rows. */
static char *search_match_cache_key_build(const char *query, const cls_filter_t *filter) {
    size_t need = strlen(query) + 128;
    char *key = (char *)malloc(need);
    if (!key) return NULL;

    if (filter) {
        (void)snprintf(key, need, "f|%d|%d|%d|%d|%d|%d|%.17g|%s",
                       (int)filter->adult,
                       filter->has_category, filter->category,
                       filter->has_tv_kind, filter->tv_kind,
                       filter->has_min_conf, filter->min_conf, query);
    } else {
        (void)snprintf(key, need, "nofilter|%s", query);
    }
    return key;
}

/* Drop the metadata of one slot; the temp table content stays until the slot
 * is rebuilt (that is what makes a hit cheap). */
static void search_match_slot_clear(search_match_slot_t *slot) {
    free(slot->key);
    slot->key = NULL;
    slot->used = 0;
    slot->total_count = 0;
    slot->built_at = 0;
    slot->last_used = 0;
}

/* Slot holding *key* and younger than the TTL, or -1. A hit is stamped for
 * LRU on the way out. */
static int search_match_cache_lookup(search_match_cache_t *cache, const char *key) {
    if (!cache || !key) return -1;

    time_t now = time(NULL);
    for (int i = 0; i < SEARCH_MATCH_CACHE_SLOTS; i++) {
        search_match_slot_t *slot = &cache->slot[i];
        if (!slot->used || !slot->key) continue;
        if (strcmp(slot->key, key) != 0) continue;
        if (now - slot->built_at >= SEARCH_MATCH_CACHE_TTL_SECS) {
            /* Expired: report a miss so the tester can see the rebuild. */
            log_msg(LOG_DEBUG, "Search match cache: EXPIRED slot %d (age %lds) for query: %s",
                    i, (long)(now - slot->built_at), key);
            search_match_slot_clear(slot);
            return -1;
        }
        slot->last_used = ++cache->tick;
        return i;
    }
    return -1;
}

/* Slot to rebuild into: a free one, else the least recently used. */
static int search_match_cache_pick_slot(search_match_cache_t *cache) {
    int victim = 0;
    unsigned long long oldest = 0;
    for (int i = 0; i < SEARCH_MATCH_CACHE_SLOTS; i++) {
        if (!cache->slot[i].used) return i;
        if (i == 0 || cache->slot[i].last_used < oldest) {
            oldest = cache->slot[i].last_used;
            victim = i;
        }
    }
    return victim;
}

/* Store a built entry. total_count is the exact count already computed for
 * this set (an empty set is a valid, cheap entry). Takes ownership of *key*. */
static void search_match_cache_store(search_match_cache_t *cache, int slot_idx, char *key,
                                     int total_count) {
    if (!cache || !key || slot_idx < 0 || slot_idx >= SEARCH_MATCH_CACHE_SLOTS) {
        free(key);
        return;
    }
    search_match_slot_t *slot = &cache->slot[slot_idx];
    free(slot->key);
    slot->key = key;
    slot->used = 1;
    slot->total_count = total_count;
    slot->built_at = time(NULL);
    slot->last_used = ++cache->tick;
}

/* Drop one slot table. Only used for a set that was built but deliberately not
 * cached (see SEARCH_MATCH_CACHE_MAX_ROWS): it has been served, so the temp
 * storage must go instead of waiting for the slot's next rebuild. */
static void search_match_drop_table(sqlite3 *conn, const char *table) {
    char drop_sql[128];
    (void)snprintf(drop_sql, sizeof(drop_sql), "DROP TABLE IF EXISTS %s;", table);
    if (sqlite3_exec(conn, drop_sql, NULL, NULL, NULL) != SQLITE_OK) {
        log_msg(LOG_WARN, "Search match cache: cannot drop %s: %s", table, sqlite3_errmsg(conn));
    }
}

/* --- Stage 6: degraded-mode serialization on the shared connection ---
 *
 * database_read_db() falls back to the process-wide db->db when it cannot open
 * a per-thread read connection (see database.h). Every request that lands on
 * that shared handle shares one TEMP schema, so two concurrent searches would
 * rebuild the same slot table (both pick slot 0, since neither can look the
 * cache up) while the other is still paging from it — silently serving the
 * wrong rows and a wrong total_count.
 *
 * This mutex makes a degraded search hold the shared connection for its whole
 * rebuild/count/page sequence, which is exactly the documented degrade contract
 * in database.h: fall back to the pre-stage-1 shared-connection behaviour,
 * correct but slow. Healthy requests never touch it: they own their connection
 * and its TEMP schema, so they neither need nor contend for this lock.
 *
 * Lock order: this is a leaf, always taken *before* database_read_lock() and
 * released *after* database_read_unlock(), and nothing else ever acquires it,
 * so it cannot deadlock against the rwlock or any other lock. */
static pthread_mutex_t shared_conn_search_mtx = PTHREAD_MUTEX_INITIALIZER;

static void shared_conn_search_lock(int degraded) {
    if (degraded) {
        pthread_mutex_lock(&shared_conn_search_mtx);
    }
}

static void shared_conn_search_unlock(int degraded) {
    if (degraded) {
        pthread_mutex_unlock(&shared_conn_search_mtx);
    }
}

/* ---------------------------------------------------------------------------
 * Stage 7 diagnostics: per-phase /search timings (log only).
 *
 * The cold-search exit criterion failed at 31.64 s against a 16 s bar while the
 * warm re-run of the same query took 0.063 s (total_count 11), so the cost sits
 * in first-touch I/O over the 78 GB USB-backed database, not in the match-set
 * build the earlier stages optimised. These timings exist so a tester can
 * attribute that time per request instead of guessing.
 *
 * Phases, all measured with CLOCK_MONOTONIC:
 *   conn       = database_read_db() (may open the per-thread connection)
 *   build      = match-set rebuild: CREATE/DELETE reset through the last
 *                INSERT finalize
 *   count      = the count query (prepare, bind, step, finalize)
 *   page       = the page query (prepare, bind, step loop, finalize) minus rows
 *   rows       = the per-row num_files count + fetch_result_files() work
 *   total      = function entry to just before database_read_unlock()
 * A phase that did not run (a cache HIT skips build and count) stays -1 and is
 * printed as "-". No SQL, bind index, lock or control-flow change is involved:
 * the only added calls are clock reads plus the existing sqlite3_changes().
 * Emitted at DEBUG always and again at INFO when the request took >= 1 s,
 * because the device default log level is INFO (config.c).
 * ------------------------------------------------------------------------ */
typedef struct {
    double conn;        /* -1 = phase did not run */
    double build;
    double count;
    double page;
    double rows;
    double total;
} search_timing_t;

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

/* Duration for the timing line; a skipped phase prints as "-". */
static const char *timing_ms_str(char *buf, size_t len, double ms) {
    if (ms < 0.0) {
        return "-";
    }
    snprintf(buf, len, "%.1f", ms);
    return buf;
}

/* Match-set size for the timing line; -1 (no rebuild this request) prints "-". */
static const char *timing_count_str(char *buf, size_t len, int n) {
    if (n < 0) {
        return "-";
    }
    snprintf(buf, len, "%d", n);
    return buf;
}

/* One line per request, so `adb logcat -d | grep "search timing"` is enough. */
static void search_timing_log(const char *query, const char *cache_state, int match_rows,
                              const search_timing_t *tm) {
    char b_conn[24], b_build[24];
    char b_count[24], b_page[24], b_rows[24], b_total[24];
    char b_match_rows[24];
    const char *conn = timing_ms_str(b_conn, sizeof(b_conn), tm->conn);
    const char *build = timing_ms_str(b_build, sizeof(b_build), tm->build);
    const char *count = timing_ms_str(b_count, sizeof(b_count), tm->count);
    const char *page = timing_ms_str(b_page, sizeof(b_page), tm->page);
    const char *rows = timing_ms_str(b_rows, sizeof(b_rows), tm->rows);
    const char *total = timing_ms_str(b_total, sizeof(b_total), tm->total);
    const char *set_rows = timing_count_str(b_match_rows, sizeof(b_match_rows), match_rows);

    log_msg(LOG_DEBUG,
            "search timing: q='%s' cache=%s match_rows=%s conn=%s build=%s "
            "count=%s page=%s rows=%s total=%s",
            query, cache_state, set_rows, conn, build, count, page, rows, total);
    if (tm->total >= 1000.0) {
        /* Slow request: repeat at INFO so the device default level shows it. */
        log_msg(LOG_INFO,
                "search timing: q='%s' cache=%s match_rows=%s conn=%s build=%s "
                "count=%s page=%s rows=%s total=%s",
                query, cache_state, set_rows, conn, build, count, page, rows, total);
    }
}

/* Search torrents using FTS5, with support for quoted exact-word matching.
 * The optional stage-7 class filter rides along on the FTS rowid set: it only
 * adds the torrent_class join when it actually restricts rows, so a plain
 * search keeps its previous SQL and result set (see search_handler). */
static int search_torrents_filtered(database_t *db, const char *query,
                                    const cls_filter_t *filter, int offset,
                                    search_result_t **results, int *count, int *total_count) {
    /* Stage 7 timing: entry stamp for `total`, and one slot per phase (-1 =
     * phase did not run). Declared here so every unlock exit can print it. */
    double t_entry = now_ms();
    search_timing_t tm = {-1.0, -1.0, -1.0, -1.0, -1.0, -1.0};
    /* Stage 7: rows the match-set rebuild stored; -1 when this request reused a
     * cached set (no rebuild ran). */
    int match_rows = -1;

    if (!db || !query || !results || !count || !total_count) {
        return -1;
    }

    *results = NULL;
    *count = 0;
    *total_count = 0;

    /* Everything this function prepares runs on the calling thread's own read
     * connection: an FTS search over the 78 GB database takes seconds and must
     * not block the millisecond queries that stay on db->db. SQL is unchanged
     * and every statement is finalized before database_read_unlock(). */
    double t_conn = now_ms();
    sqlite3 *conn = database_read_db(db);
    tm.conn = now_ms() - t_conn;
    if (!conn) {
        return -1;
    }

    /* Parse the query into the torrent FTS expression */
    parsed_query_t pq;
    if (parse_search_query(query, &pq) != 0) {
        return -1;
    }

    /* --- Stage 4: match-set cache lookup (metadata only: no SQL, no lock) --- */
    search_match_cache_t *cache = search_match_cache_get();
    char *cache_key = search_match_cache_key_build(query, filter);
    /* Degraded mode: database_read_db() falls back to the process-wide handle
     * when it cannot open a per-thread connection. The TEMP tables are then
     * shared by every request thread, so a hit could serve another search's
     * rows — bypass the cache there entirely and rebuild per request (exactly
     * the stage-3 behaviour). sqlite3_open_v2() never hands out db->db, so this
     * comparison is exact. */
    int cache_degraded = (conn == db->db);
    int hit_slot = cache_degraded ? -1 : search_match_cache_lookup(cache, cache_key);
    /* The slot this request works with: the hit, or the one a miss rebuilds
     * into (a free slot, else the least recently used). */
    int slot_idx = (hit_slot >= 0) ? hit_slot
                                   : (cache ? search_match_cache_pick_slot(cache) : 0);
    const char *match_table = SEARCH_MATCH_TABLES[slot_idx];

    if (!cache) {
        log_msg(LOG_DEBUG, "Search match cache: DISABLED for query: %s", query);
    } else if (cache_degraded) {
        log_msg(LOG_DEBUG, "Search match cache: shared connection, bypassing for query: %s",
                query);
    } else if (hit_slot >= 0) {
        log_msg(LOG_DEBUG, "Search match cache: HIT slot %d (total %d) for query: %s",
                slot_idx, cache->slot[slot_idx].total_count, query);
    } else {
        log_msg(LOG_DEBUG, "Search match cache: MISS slot %d for query: %s", slot_idx, query);
    }

    /* Stage 7: the cache state for the timing line, taken from the same values
     * the DEBUG lines above just printed (an EXPIRED slot is a MISS here; the
     * lookup already logged the expiry). */
    const char *cache_state = !cache            ? "OFF"
                              : cache_degraded  ? "DEGRADED"
                              : (hit_slot >= 0) ? "HIT"
                                                : "MISS";

    /* Stage 7: stamps `total` and prints the one-line summary. Called right
     * before database_read_unlock() on every exit path, so no lock is held
     * longer than before and the unlock order is untouched. */
    #define SEARCH_TIMING_EMIT() do { \
        tm.total = now_ms() - t_entry; \
        search_timing_log(query, cache_state, match_rows, &tm); \
    } while (0)

    /* Degraded mode: hold the shared connection for the whole rebuild/count/page
     * sequence (see shared_conn_search_mtx above). Healthy requests skip this. */
    shared_conn_search_lock(cache_degraded);
    database_read_lock(db);

    char match_sql[4096];
    char count_sql[4096];
    char main_sql[4096];

    /* Bind indices of the three /search statements:
     *   - match-set INSERT: ?1 = the unicode61 expression built by
     *     build_docs_fts_query() from the raw query;
     *   - count query: ?1... = the class filter, only when the class join is
     *     active (bind_class_filter(..., 1, ...));
     *   - page query: ?1 = the porter expression over torrent_search, used for
     *     the name-matches-first ORDER BY ranking (a DIFFERENT string from the
     *     match-set ?1), ?2 = LIMIT, ?3 = OFFSET, ?4... = the class filter.
     * Only literal SQL is prepared here: the slot table name is a compile-time
     * constant and every piece of user input stays a bound parameter. */

    /* Stage 7: the class join goes into the count query only when the filter
     * restricts rows, so a plain /search?q= keeps the legacy count query. The
     * main query always joins the class row, because the JSON envelope reports
     * its columns whenever the torrent has one. */
    int class_join = class_filter_needs_join(filter);
    char class_count_join[512];
    char class_main_join[512];

    class_count_join[0] = '\0';
    int cpos = 0;
    if (class_join) {
        cpos = snprintf(class_count_join, sizeof(class_count_join),
                        " JOIN torrent_class c ON c.torrent_id = t.id");
        append_class_conditions(class_count_join, sizeof(class_count_join), &cpos, filter, 1);
    }

    cpos = snprintf(class_main_join, sizeof(class_main_join), "%s",
                    class_join
                        ? "JOIN torrent_class c ON c.torrent_id = t.id"
                        : "LEFT JOIN torrent_class c ON c.torrent_id = t.id");
    if (class_join) {
        append_class_conditions(class_main_join, sizeof(class_main_join), &cpos, filter, 4);
    }

    /* Count from the materialized match set: same rows as before, but the
     * FTS match is no longer evaluated a second time. The torrent_class
     * join is on its torrent_id PK, so it cannot multiply rows and
     * COUNT(*) equals the previous COUNT(DISTINCT t.id). */
    (void)snprintf(count_sql, sizeof(count_sql),
        "SELECT COUNT(*) "
        "FROM torrents t%s "
        "WHERE t.id IN (SELECT id FROM %s)", class_count_join, match_table);

    /* No COUNT(f.id) in the SELECT list: aggregating the file count over
     * the whole match set (~79.8k torrents x ~47 files ~= 3.75M index
     * entries) was ~9 s of a 14.6 s query, only to print a per-row file
     * count for the 50 rows on this page. The count is filled per returned
     * row in the row loop instead (see page_files_count_sql below).
     * GROUP BY t.id stays: the torrent_class join is on its torrent_id PK
     * so it cannot multiply rows, and it keeps the page query's shape
     * identical to the pre-stage-3 one. */
    (void)snprintf(main_sql, sizeof(main_sql),
        "SELECT DISTINCT t.info_hash, t.name, t.size_bytes, t.total_peers, "
        "       t.added_timestamp, "
        "       c.category, c.tv_kind, c.adult, c.confidence "
        "FROM torrents t "
        "%s "
        "WHERE t.id IN (SELECT id FROM %s) "
        "GROUP BY t.id "
        "ORDER BY (t.id IN (SELECT rowid FROM torrent_search WHERE name MATCH ?1)) DESC, "
        "t.total_peers DESC, t.added_timestamp DESC "
        "LIMIT ?2 OFFSET ?3", class_main_join, match_table);

    /* --- Build the match set (stage 3) or reuse the cached one (stage 4) --- */
    /* The FTS match over the whole corpus is the expensive part of /search: it
     * used to run once inside the count query and once inside the page query
     * (plus a third time in the ORDER BY ranking expression, which stays). Both
     * now read one TEMP table instead. It is per-connection — stage 1 gives
     * every request thread its own read connection, so the temp schema cannot
     * leak between concurrent searches — and this request owns exactly one of
     * the two slot tables, so the other slot keeps its rows for the next page
     * or the next repeat search. Only literal SQL is prepared here: the slot
     * table name is a compile-time constant and every piece of user input stays
     * a bound parameter. */
    int cache_hit = (hit_slot >= 0);
    int drop_after_page = 0;   /* over-cap set: served once, then dropped */
    int rc = SQLITE_OK;

    if (!cache_hit) {
        double t_build = now_ms();   /* Stage 7: reset through INSERT finalize */
        char reset_sql[256];
        (void)snprintf(reset_sql, sizeof(reset_sql),
                       "CREATE TEMP TABLE IF NOT EXISTS %s(id INTEGER PRIMARY KEY);"
                       "DELETE FROM %s;", match_table, match_table);

        /* A rebuild replaces what the slot held: drop its metadata first, so a
         * failure below cannot leave an entry pointing at rows it does not own. */
        if (cache) search_match_slot_clear(&cache->slot[slot_idx]);

        if (sqlite3_exec(conn, reset_sql, NULL, NULL, NULL) != SQLITE_OK) {
            log_msg(LOG_ERROR, "Failed to reset the search match set: %s", sqlite3_errmsg(conn));
            tm.build = now_ms() - t_build;   /* Stage 7: partial build */
            SEARCH_TIMING_EMIT();
            database_read_unlock(db);
            shared_conn_search_unlock(cache_degraded);
            free(cache_key);
            free_parsed_query(&pq);
            return -1;
        }

        /* Set by the match-set build below; the shared failure handling after
         * it empties the set. */
        int build_failed = 0;

        /* --- One FTS5 expression over torrent_docs ---
         * torrent_docs holds each torrent's name plus all of its filenames in
         * one unicode61 document, so a single MATCH yields the torrent rowids
         * directly. The expression is a bound parameter (?1), never spliced
         * into the SQL text. */
        /* The handler caps q at 255 bytes and expansion is <= 2 x raw + 2
         * per term, so 1024 cannot overflow; the -1 below stays wired as a
         * guard on the same fail path. */
        char docs_q[1024];
        if (build_docs_fts_query(query, docs_q, sizeof(docs_q)) != 0) {
            log_msg(LOG_ERROR, "Docs search does not fit the FTS buffer");
            SEARCH_TIMING_EMIT();
            database_read_unlock(db);
            shared_conn_search_unlock(cache_degraded);
            free(cache_key);
            free_parsed_query(&pq);
            return -1;
        }

        (void)snprintf(match_sql, sizeof(match_sql),
                       "INSERT OR IGNORE INTO %s "
                       "SELECT rowid FROM torrent_docs WHERE torrent_docs MATCH ?1",
                       match_table);

        double t_docs = now_ms();
        sqlite3_stmt *docs_stmt = NULL;
        rc = sqlite3_prepare_v2(conn, match_sql, -1, &docs_stmt, NULL);
        if (rc != SQLITE_OK) {
            log_msg(LOG_ERROR, "Failed to prepare match set query: %s",
                    sqlite3_errmsg(conn));
            tm.build = now_ms() - t_build;   /* Stage 7: partial build */
            SEARCH_TIMING_EMIT();
            database_read_unlock(db);
            shared_conn_search_unlock(cache_degraded);
            free(cache_key);
            free_parsed_query(&pq);
            return -1;
        }

        sqlite3_bind_text(docs_stmt, 1, docs_q, -1, SQLITE_STATIC);
        int docs_rc = sqlite3_step(docs_stmt);
        /* Rows the INSERT actually stored: an O(1) answer to "is this set
         * pathological?" that does not rescan a multi-million-row temp btree. */
        match_rows = sqlite3_changes(conn);
        sqlite3_finalize(docs_stmt);
        tm.build = now_ms() - t_docs;
        build_failed = (docs_rc != SQLITE_DONE);
        if (build_failed) {
            /* Malformed expression (an unbalanced quote, say): the shared reset
             * below empties the set, the count query then returns 0 and the page
             * query is skipped, so the response is HTTP 200 with count=0. Never
             * a 500 from here. */
            log_msg(LOG_DEBUG, "Search docs match set failed for query '%s': %s",
                    query, sqlite3_errmsg(conn));
        }

        tm.build = now_ms() - t_build;   /* Stage 7: build phase done */

        if (build_failed) {
            /* An FTS syntax error (an unbalanced quote, say) only surfaces at step
             * time. /search has always reported zero results for those instead of
             * an error, so keep that contract: empty the match set and let the
             * count query below return 0, which short-circuits the page query.
             * The empty set is then cached like any other: it is exact and cheap.
             * A failed build must never be counted or cached as if it were
             * complete. */
            (void)sqlite3_exec(conn, reset_sql, NULL, NULL, NULL);
            match_rows = 0;
        }

        /* --- Execute count query (exact: same match set as the page query) --- */
        double t_count = now_ms();   /* Stage 7: count phase */
        sqlite3_stmt *count_stmt;
        rc = sqlite3_prepare_v2(conn, count_sql, -1, &count_stmt, NULL);
        if (rc != SQLITE_OK) {
            log_msg(LOG_ERROR, "Failed to prepare count query: %s", sqlite3_errmsg(conn));
            tm.count = now_ms() - t_count;   /* Stage 7: partial count */
            SEARCH_TIMING_EMIT();
            database_read_unlock(db);
            shared_conn_search_unlock(cache_degraded);
            free(cache_key);
            free_parsed_query(&pq);
            return -1;
        }

        if (class_join) {
            bind_class_filter(count_stmt, 1, filter);
        }

        if (sqlite3_step(count_stmt) == SQLITE_ROW) {
            *total_count = sqlite3_column_int(count_stmt, 0);
        }
        sqlite3_finalize(count_stmt);
        tm.count = now_ms() - t_count;   /* Stage 7: count phase done */

        if (match_rows > SEARCH_MATCH_CACHE_MAX_ROWS) {
            /* One pathological query must not pin both slots of temp storage. */
            log_msg(LOG_DEBUG,
                    "Search match cache: not caching %d-row set (slot %d) for query: %s",
                    match_rows, slot_idx, query);
            drop_after_page = 1;
            free(cache_key);
            cache_key = NULL;
        } else if (cache_degraded) {
            /* Shared connection: an entry could not be told apart from another
             * thread's, so serve this set and let the next request rebuild. */
            free(cache_key);
            cache_key = NULL;
        } else {
            /* Empty sets included: exact and cheap. store() takes the key. */
            search_match_cache_store(cache, slot_idx, cache_key, *total_count);
            cache_key = NULL;
        }
    } else {
        /* Hit: the slot's temp table still holds this query's match set, so
         * neither the FTS build nor the count query runs — only the page query
         * below does. total_count stays exact for the life of the entry. */
        *total_count = cache->slot[slot_idx].total_count;
        free(cache_key);
        cache_key = NULL;
    }

    /* Empty result set: the page query cannot return a row either (the class
     * filter already ran in the count query), so skip it — a search with no
     * hits must not pay for the ORDER BY ranking pass. */
    if (*total_count == 0) {
        /* An over-cap set the class filter emptied still has to go: it was not
         * cached, so nothing else will ever reset this slot table. */
        if (drop_after_page) search_match_drop_table(conn, match_table);
        SEARCH_TIMING_EMIT();
        database_read_unlock(db);
        shared_conn_search_unlock(cache_degraded);
        *results = NULL;  /* send_json_results handles a NULL array with count 0 */
        *count = 0;
        free_parsed_query(&pq);
        log_msg(LOG_DEBUG, "Search found 0 results (offset %d) for query: %s", offset, query);
        return 0;
    }

    /* --- Execute main query --- */
    double t_page = now_ms();   /* Stage 7: page query + row loop */
    double rows_ms = 0.0;       /* Stage 7: time spent in the row loop */
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(conn, main_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare search query: %s", sqlite3_errmsg(conn));
        tm.page = now_ms() - t_page;   /* Stage 7: partial page query */
        if (drop_after_page) search_match_drop_table(conn, match_table);
        SEARCH_TIMING_EMIT();
        database_read_unlock(db);
        shared_conn_search_unlock(cache_degraded);
        free_parsed_query(&pq);
        return -1;
    }

    sqlite3_bind_text(stmt, 1, pq.torrent_fts_query, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, HTTP_API_MAX_RESULTS);
    sqlite3_bind_int(stmt, 3, offset);
    if (class_join) {
        bind_class_filter(stmt, 4, filter);
    }

    /* Allocate results array */
    search_result_t *res = (search_result_t *)calloc(HTTP_API_MAX_RESULTS, sizeof(search_result_t));
    if (!res) {
        sqlite3_finalize(stmt);
        tm.page = now_ms() - t_page;   /* Stage 7: page query never ran */
        if (drop_after_page) search_match_drop_table(conn, match_table);
        SEARCH_TIMING_EMIT();
        database_read_unlock(db);
        shared_conn_search_unlock(cache_degraded);
        free_parsed_query(&pq);
        return -1;
    }

    /* Per-page file count: identical subquery shape to fetch_result_files(),
     * one idx_files_torrent range scan per returned row on this same read
     * connection. This replaces the whole-match-set COUNT(f.id) aggregate the
     * main query used to carry. */
    const char *page_files_count_sql =
        "SELECT COUNT(*) FROM torrent_files "
        "WHERE torrent_id = (SELECT id FROM torrents WHERE info_hash = ?)";

    int i = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && i < HTTP_API_MAX_RESULTS) {
        double t_row = now_ms();   /* Stage 7: this row's file count + file list */
        /* Extract torrent data */
        memcpy(res[i].info_hash, sqlite3_column_blob(stmt, 0), 20);
        res[i].name = strdup((const char *)sqlite3_column_text(stmt, 1));
        res[i].size_bytes = sqlite3_column_int64(stmt, 2);
        res[i].total_peers = sqlite3_column_int(stmt, 3);
        res[i].added_timestamp = sqlite3_column_int64(stmt, 4);

        /* Stage 7: torrent_class columns 5..8, filled when the torrent has a
         * class row. c.category is NOT NULL in the table, so it doubles as the
         * "has a class row" flag; tv_kind and confidence are nullable. */
        if (sqlite3_column_type(stmt, 5) != SQLITE_NULL) {
            res[i].has_class = 1;
            res[i].category = sqlite3_column_int(stmt, 5);
            if (sqlite3_column_type(stmt, 6) != SQLITE_NULL) {
                res[i].has_tv_kind = 1;
                res[i].tv_kind = sqlite3_column_int(stmt, 6);
            }
            res[i].adult = sqlite3_column_int(stmt, 7);
            if (sqlite3_column_type(stmt, 8) != SQLITE_NULL) {
                res[i].has_confidence = 1;
                res[i].confidence = sqlite3_column_double(stmt, 8);
            }
        }

        /* num_files for this row only. The statement is finalized on every
         * path: an unfinalized statement pins a WAL snapshot and stalls the
         * next CHECKPOINT_RESTART. */
        sqlite3_stmt *files_count_stmt = NULL;
        if (sqlite3_prepare_v2(conn, page_files_count_sql, -1, &files_count_stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_blob(files_count_stmt, 1, res[i].info_hash, 20, SQLITE_STATIC);
            if (sqlite3_step(files_count_stmt) == SQLITE_ROW) {
                res[i].num_files = sqlite3_column_int(files_count_stmt, 0);
            }
            sqlite3_finalize(files_count_stmt);
        }

        /* Get file listings for this torrent - reconstruct paths from prefix + filename */
        if (res[i].num_files > 0) {
            fetch_result_files(db, &res[i]);
        }

        i++;
        rows_ms += now_ms() - t_row;   /* Stage 7 */
    }

    sqlite3_finalize(stmt);
    /* Stage 7: `page` is the query itself (prepare, step, finalize) and `rows`
     * the per-row work inside its step loop, so the two do not double-count. */
    tm.rows = rows_ms;
    tm.page = now_ms() - t_page - rows_ms;

    /* Over-cap set: the page has been read, drop the temp table instead of
     * leaving it behind for the slot's next rebuild. */
    if (drop_after_page) search_match_drop_table(conn, match_table);

    SEARCH_TIMING_EMIT();
    database_read_unlock(db);
    shared_conn_search_unlock(cache_degraded);

    *results = res;
    *count = i;

    free_parsed_query(&pq);

    log_msg(LOG_DEBUG, "Search found %d results (offset %d, total %d) for query: %s", i, offset, *total_count, query);
    return 0;
}

#undef SEARCH_TIMING_EMIT

/* Legacy entry point without a class filter (see search_torrents_filtered).
 * Nothing in the tree calls it any more. */
int search_torrents(database_t *db, const char *query, int offset, search_result_t **results, int *count, int *total_count) {
    return search_torrents_filtered(db, query, NULL, offset, results, count, total_count);
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

/* ---------------------------------------------------------------------------
 * Stage 7: /browse — torrent_class-driven listing, no text query.
 * ------------------------------------------------------------------------ */

/* Run the /browse query. The row set is driven by torrent_class, so the common
 * adult=hide + category (+ tv_kind) case is served by the partial index
 * idx_class_cat instead of scanning torrents; each torrent row is then reached
 * by its primary key. order_by is one of the fixed fragments chosen by
 * browse_handler(). */
static int browse_torrents(database_t *db, const cls_filter_t *filter, const char *order_by,
                           int offset, search_result_t **results, int *count, int *total_count) {
    if (!db || !filter || !order_by || !results || !count || !total_count) {
        return -1;
    }

    *results = NULL;
    *count = 0;
    *total_count = 0;

    /* Own read connection: see database_read_db (search/browse/random are the
     * long read paths; the millisecond queries stay on db->db). */
    sqlite3 *conn = database_read_db(db);
    if (!conn) {
        return -1;
    }

    /* "WHERE 1" plus the filter's " AND ..." fragments; the class binds come
     * first, so LIMIT/OFFSET follow them. */
    char count_sql[512];
    char main_sql[1024];
    int pos = snprintf(count_sql, sizeof(count_sql),
                       "SELECT COUNT(*) FROM torrent_class c WHERE 1");
    append_class_conditions(count_sql, sizeof(count_sql), &pos, filter, 1);

    pos = snprintf(main_sql, sizeof(main_sql),
        "SELECT t.info_hash, t.name, t.size_bytes, t.total_peers, t.added_timestamp, "
        "       (SELECT COUNT(*) FROM torrent_files f WHERE f.torrent_id = t.id), "
        "       c.category, c.tv_kind, c.adult, c.confidence "
        "FROM torrent_class c "
        "JOIN torrents t ON t.id = c.torrent_id "
        "WHERE 1");
    append_class_conditions(main_sql, sizeof(main_sql), &pos, filter, 1);
    (void)snprintf(main_sql + pos, sizeof(main_sql) - pos,
                   " ORDER BY %s LIMIT ?%d OFFSET ?%d",
                   order_by, CLS_FILTER_MAX_BINDS + 1, CLS_FILTER_MAX_BINDS + 2);

    database_read_lock(db);

    sqlite3_stmt *count_stmt;
    if (sqlite3_prepare_v2(conn, count_sql, -1, &count_stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare browse count query: %s", sqlite3_errmsg(conn));
        database_read_unlock(db);
        return -1;
    }
    bind_class_filter(count_stmt, 1, filter);
    if (sqlite3_step(count_stmt) == SQLITE_ROW) {
        *total_count = sqlite3_column_int(count_stmt, 0);
    }
    sqlite3_finalize(count_stmt);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(conn, main_sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare browse query: %s", sqlite3_errmsg(conn));
        database_read_unlock(db);
        return -1;
    }
    bind_class_filter(stmt, 1, filter);
    sqlite3_bind_int(stmt, CLS_FILTER_MAX_BINDS + 1, HTTP_API_MAX_RESULTS);
    sqlite3_bind_int(stmt, CLS_FILTER_MAX_BINDS + 2, offset);

    search_result_t *res = (search_result_t *)calloc(HTTP_API_MAX_RESULTS, sizeof(search_result_t));
    if (!res) {
        sqlite3_finalize(stmt);
        database_read_unlock(db);
        return -1;
    }

    int i = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && i < HTTP_API_MAX_RESULTS) {
        memcpy(res[i].info_hash, sqlite3_column_blob(stmt, 0), 20);
        res[i].name = strdup((const char *)sqlite3_column_text(stmt, 1));
        res[i].size_bytes = sqlite3_column_int64(stmt, 2);
        res[i].total_peers = sqlite3_column_int(stmt, 3);
        res[i].added_timestamp = sqlite3_column_int64(stmt, 4);
        res[i].num_files = sqlite3_column_int(stmt, 5);

        /* Every row comes from torrent_class, so the class fields are always
         * present here (tv_kind and confidence can still be NULL). */
        res[i].has_class = 1;
        res[i].category = sqlite3_column_int(stmt, 6);
        if (sqlite3_column_type(stmt, 7) != SQLITE_NULL) {
            res[i].has_tv_kind = 1;
            res[i].tv_kind = sqlite3_column_int(stmt, 7);
        }
        res[i].adult = sqlite3_column_int(stmt, 8);
        if (sqlite3_column_type(stmt, 9) != SQLITE_NULL) {
            res[i].has_confidence = 1;
            res[i].confidence = sqlite3_column_double(stmt, 9);
        }

        if (res[i].num_files > 0) {
            fetch_result_files(db, &res[i]);
        }

        i++;
    }

    sqlite3_finalize(stmt);
    database_read_unlock(db);

    *results = res;
    *count = i;

    log_msg(LOG_DEBUG, "Browse returned %d results (offset %d, total %d)", i, offset, *total_count);
    return 0;
}

/* /browse — no text query: the stage-7 filters (adult defaulting to hide), a
 * sort key and the page number. Answers with the /search JSON envelope by
 * default; format=html renders the same results page as /search through the
 * shared renderer, with every link pointing back at /browse because a text
 * search link cannot be built without a q. */
static int browse_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;
    const struct mg_request_info *ri = mg_get_request_info(conn);
    const char *query_string = ri->query_string ? ri->query_string : "";
    size_t qs_len = strlen(query_string);

    cls_filter_t filter;
    const char *bad_param = NULL;
    if (parse_class_filter(query_string, &filter, &bad_param) != 0) {
        char error_msg[96];
        snprintf(error_msg, sizeof(error_msg), "Invalid value for '%s'", bad_param);
        return send_json_error(conn, 400, error_msg);
    }

    /* sort: whitelisted to fixed ORDER BY fragments — no input is interpolated. */
    char sort_buf[16] = {0};
    mg_get_var(query_string, qs_len, "sort", sort_buf, sizeof(sort_buf));
    const char *order_by;
    if (sort_buf[0] == '\0' || strcmp(sort_buf, "peers") == 0) {
        order_by = "t.total_peers DESC, t.added_timestamp DESC";
    } else if (strcmp(sort_buf, "added") == 0) {
        order_by = "t.added_timestamp DESC, t.id DESC";
    } else if (strcmp(sort_buf, "size") == 0) {
        order_by = "t.size_bytes DESC, t.id DESC";
    } else {
        return send_json_error(conn, 400, "Invalid value for 'sort'");
    }

    /* Page number (1-indexed, default 1), parsed like /search does. */
    char page_buf[16] = {0};
    mg_get_var(query_string, qs_len, "page", page_buf, sizeof(page_buf));
    int page = 1;
    if (strlen(page_buf) > 0) {
        page = atoi(page_buf);
        if (page < 1) page = 1;
    }
    int offset = (page - 1) * HTTP_API_MAX_RESULTS;

    search_result_t *results = NULL;
    int count = 0;
    int total_count = 0;
    if (browse_torrents(api->database, &filter, order_by, offset,
                        &results, &count, &total_count) != 0) {
        return send_json_error(conn, 500, "Database query failed");
    }

    log_msg(LOG_DEBUG, "Browse page %d (%d results, %d total)", page, count, total_count);

    return send_json_results(conn, results, count, total_count, page);
}

/* Stage 7: torrent_class probe for the /random candidate loop. tv_kind is
 * nullable (NULL when the torrent has no tv/anime kind). */
#define RANDOM_CLASS_SQL_EX "SELECT category, tv_kind, adult FROM torrent_class WHERE torrent_id = ?"

/* Class-first candidate test for /random. A torrent with a class row is judged
 * by that row alone (adult per adult_mode, then category / tv_kind equality).
 * A torrent with no class row is eligible only when the caller passed no real
 * filter (adult=show, no category, no tv_kind); the legacy extension/title
 * heuristics are intentionally not applied — the phone DB is fully classified.
 *
 * want_category == -1 => any; want_tv_kind == -1 => any (incl. NULL),
 * 0 => require NULL, >0 => equality. The probed class row is written back via
 * the out-params so the caller can populate the JSON class fields.
 * CALLER must hold database_read_lock. */
static int random_candidate_eligible_ex(database_t *db, sqlite3_stmt *class_stmt,
                                        int64_t torrent_id, const char *name,
                                        int want_category, int want_tv_kind,
                                        int adult_mode,
                                        int *has_class, int *category,
                                        int *has_tv_kind, int *tv_kind,
                                        int *adult) {
    (void)db;
    (void)name;

    sqlite3_bind_int64(class_stmt, 1, torrent_id);

    *has_class = 0;
    *category = 0;
    *has_tv_kind = 0;
    *tv_kind = 0;
    *adult = 0;

    if (sqlite3_step(class_stmt) == SQLITE_ROW) {
        *has_class = 1;
        *category = sqlite3_column_int(class_stmt, 0);
        if (sqlite3_column_type(class_stmt, 1) != SQLITE_NULL) {
            *has_tv_kind = 1;
            *tv_kind = sqlite3_column_int(class_stmt, 1);
        }
        *adult = sqlite3_column_int(class_stmt, 2);
    }
    sqlite3_reset(class_stmt);

    if (*has_class) {
        if (adult_mode == CLS_ADULT_HIDE && *adult != 0) return 0;
        if (adult_mode == CLS_ADULT_ONLY && *adult != 1) return 0;

        if (want_category != -1 && *category != want_category) return 0;

        if (want_tv_kind == 0) {
            if (*has_tv_kind) return 0;              /* require NULL */
        } else if (want_tv_kind > 0) {
            if (!*has_tv_kind || *tv_kind != want_tv_kind) return 0;
        }

        return 1;
    }

    return (adult_mode == CLS_ADULT_SHOW && want_category == -1 && want_tv_kind == -1);
}

/* Generic random sampler: up to HTTP_API_MAX_RESULTS torrents with
 * total_peers >= min_peers, filtered via random_candidate_eligible_ex. */
static int get_random_torrents(database_t *db, const cls_filter_t *filter,
                               long min_peers, search_result_t **results,
                               int *count) {
    if (!db || !filter || !results || !count) {
        return -1;
    }

    *results = NULL;
    *count = 0;

    /* Own read connection: see database_read_db (search/browse/random are the
     * long read paths; the millisecond queries stay on db->db). */
    sqlite3 *conn = database_read_db(db);
    if (!conn) {
        return -1;
    }

    int want_category = filter->has_category ? filter->category : -1;
    int want_tv_kind = filter->has_tv_kind ? filter->tv_kind : -1;
    int adult_mode = filter->adult;

    database_read_lock(db);

    int64_t max_id = 0;
    sqlite3_stmt *max_stmt;
    if (sqlite3_prepare_v2(conn, "SELECT MAX(id) FROM torrents", -1, &max_stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(max_stmt) == SQLITE_ROW) {
            max_id = sqlite3_column_int64(max_stmt, 0);
        }
        sqlite3_finalize(max_stmt);
    }

    if (max_id == 0) {
        database_read_unlock(db);
        return 0;
    }

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    unsigned int seed = (unsigned int)(ts.tv_nsec ^ ts.tv_sec ^ (uintptr_t)&seed);
    srand(seed);

    sqlite3_stmt *class_stmt;
    if (sqlite3_prepare_v2(conn, RANDOM_CLASS_SQL_EX, -1, &class_stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare random class query: %s", sqlite3_errmsg(conn));
        database_read_unlock(db);
        return -1;
    }

    const char *torrent_sql =
        "SELECT id, info_hash, name, size_bytes, total_peers, added_timestamp "
        "FROM torrents WHERE id = ? AND total_peers >= ?";
    sqlite3_stmt *torrent_stmt;
    if (sqlite3_prepare_v2(conn, torrent_sql, -1, &torrent_stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare torrent query: %s", sqlite3_errmsg(conn));
        sqlite3_finalize(class_stmt);
        database_read_unlock(db);
        return -1;
    }

    const char *file_count_sql = "SELECT COUNT(*) FROM torrent_files WHERE torrent_id = ?";
    sqlite3_stmt *file_count_stmt;
    if (sqlite3_prepare_v2(conn, file_count_sql, -1, &file_count_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(torrent_stmt);
        sqlite3_finalize(class_stmt);
        database_read_unlock(db);
        return -1;
    }

    search_result_t *res = (search_result_t *)calloc(100, sizeof(search_result_t));
    if (!res) {
        sqlite3_finalize(torrent_stmt);
        sqlite3_finalize(file_count_stmt);
        sqlite3_finalize(class_stmt);
        database_read_unlock(db);
        return -1;
    }

    uint8_t found_hashes[100][20];
    int found = 0;
    int attempts = 0;
    int max_attempts = 50000;

    while (found < HTTP_API_MAX_RESULTS && attempts < max_attempts) {
        attempts++;

        int64_t random_id = ((int64_t)rand() * rand()) % max_id + 1;

        sqlite3_bind_int64(torrent_stmt, 1, random_id);
        sqlite3_bind_int64(torrent_stmt, 2, (int64_t)min_peers);

        if (sqlite3_step(torrent_stmt) == SQLITE_ROW) {
            int64_t torrent_id = sqlite3_column_int64(torrent_stmt, 0);
            const void *info_hash_blob = sqlite3_column_blob(torrent_stmt, 1);
            const char *name = (const char *)sqlite3_column_text(torrent_stmt, 2);

            int is_duplicate = 0;
            for (int j = 0; j < found; j++) {
                if (memcmp(found_hashes[j], info_hash_blob, 20) == 0) {
                    is_duplicate = 1;
                    break;
                }
            }

            int passes = 0;
            int has_class = 0, category = 0, has_tv_kind = 0, tv_kind = 0, adult = 0;
            if (!is_duplicate) {
                passes = random_candidate_eligible_ex(
                    db, class_stmt, torrent_id, name,
                    want_category, want_tv_kind, adult_mode,
                    &has_class, &category, &has_tv_kind, &tv_kind, &adult);
            }

            if (passes) {
                memcpy(res[found].info_hash, info_hash_blob, 20);
                memcpy(found_hashes[found], info_hash_blob, 20);
                res[found].name = name ? strdup(name) : strdup("Unknown");
                res[found].size_bytes = sqlite3_column_int64(torrent_stmt, 3);
                res[found].total_peers = sqlite3_column_int(torrent_stmt, 4);
                res[found].added_timestamp = sqlite3_column_int64(torrent_stmt, 5);

                res[found].has_class = has_class;
                res[found].category = category;
                res[found].has_tv_kind = has_tv_kind;
                res[found].tv_kind = tv_kind;
                res[found].adult = adult;
                /* has_confidence stays 0 => JSON "confidence": null */

                sqlite3_bind_int64(file_count_stmt, 1, torrent_id);
                if (sqlite3_step(file_count_stmt) == SQLITE_ROW) {
                    res[found].num_files = sqlite3_column_int(file_count_stmt, 0);
                }
                sqlite3_reset(file_count_stmt);

                if (res[found].num_files > 0) {
                    fetch_result_files(db, &res[found]);
                }

                found++;
            }
        }

        sqlite3_reset(torrent_stmt);
    }

    sqlite3_finalize(torrent_stmt);
    sqlite3_finalize(file_count_stmt);
    sqlite3_finalize(class_stmt);

    database_read_unlock(db);

    *results = res;
    *count = found;

    return 0;
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

