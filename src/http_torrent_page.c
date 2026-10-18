#include "http_torrent_page.h"
#include "http_api.h"
#include "database.h"
#include "torrent_search.h"
#include "dht_crawler.h"
#include <civetweb.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>

/* ----- small local helpers (duplicated from http_api.c to keep this unit self-contained) ----- */

static void tp_format_size(int64_t bytes, char *out, size_t out_len) {
    const char *units[] = {"B", "KB", "MB", "GB", "TB"};
    int unit = 0;
    double size = (double)bytes;
    while (size >= 1024 && unit < 4) { size /= 1024; unit++; }
    snprintf(out, out_len, "%.2f %s", size, units[unit]);
}

static char *tp_url_decode(const char *str) {
    if (!str) return NULL;
    size_t len = strlen(str);
    char *out = (char *)malloc(len + 1);
    if (!out) return NULL;
    size_t i = 0, j = 0;
    while (i < len) {
        if (str[i] == '%' && i + 2 < len) {
            unsigned int v;
            if (sscanf(str + i + 1, "%2x", &v) == 1) {
                out[j++] = (char)v;
                i += 3;
            } else {
                out[j++] = str[i++];
            }
        } else if (str[i] == '+') {
            out[j++] = ' '; i++;
        } else {
            out[j++] = str[i++];
        }
    }
    out[j] = '\0';
    return out;
}

/* Get a hex query parameter, validate length 40 hex chars. Returns 0/-1. */
static int tp_get_hash_param(struct mg_connection *conn, char hash_out[41]) {
    const struct mg_request_info *ri = mg_get_request_info(conn);
    const char *qs = ri->query_string ? ri->query_string : "";
    char buf[128] = {0};
    int n = mg_get_var(qs, strlen(qs), "hash", buf, sizeof(buf));
    if (n <= 0 || strlen(buf) != 40) return -1;
    for (int i = 0; i < 40; i++) {
        char c = buf[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
            return -1;
        }
        hash_out[i] = (c >= 'A' && c <= 'F') ? (char)(c + 32) : c;
    }
    hash_out[40] = '\0';
    return 0;
}

/* Append helper using realloc-on-overflow buffer */
typedef struct {
    char *buf;
    size_t len, cap;
} sbuf_t;

static int sb_append(sbuf_t *s, const char *fmt, ...) {
    va_list ap;
    while (1) {
        va_start(ap, fmt);
        int needed = vsnprintf(s->buf + s->len, s->cap - s->len, fmt, ap);
        va_end(ap);
        if (needed < 0) return -1;
        if ((size_t)needed < s->cap - s->len) { s->len += needed; return 0; }
        size_t newcap = s->cap * 2;
        while (newcap < s->len + needed + 1) newcap *= 2;
        char *nb = (char *)realloc(s->buf, newcap);
        if (!nb) return -1;
        s->buf = nb; s->cap = newcap;
    }
}

/* Append a JSON-escaped copy of p to s (mirrors the escaping loop used by
 * http_torrent_files_handler). */
static void tp_append_json_string(sbuf_t *s, const char *p) {
    if (!p) return;
    for (; *p; p++) {
        unsigned char c = (unsigned char)*p;
        switch (c) {
            case '"':  sb_append(s, "\\\""); break;
            case '\\': sb_append(s, "\\\\"); break;
            case '\n': sb_append(s, "\\n"); break;
            case '\r': sb_append(s, "\\r"); break;
            case '\t': sb_append(s, "\\t"); break;
            default:
                if (c < 0x20) sb_append(s, "\\u%04x", c);
                else { char tmp[2] = {(char)c, 0}; sb_append(s, "%s", tmp); }
                break;
        }
    }
}

static void send_404(struct mg_connection *conn, const char *msg) {
    const char *body = msg ? msg : "Not found";
    mg_printf(conn,
              "HTTP/1.1 404 Not Found\r\n"
              "Content-Type: text/plain; charset=utf-8\r\n"
              "Content-Length: %d\r\n\r\n%s",
              (int)strlen(body), body);
}

static void send_400(struct mg_connection *conn, const char *msg) {
    const char *body = msg ? msg : "Bad request";
    mg_printf(conn,
              "HTTP/1.1 400 Bad Request\r\n"
              "Content-Type: text/plain; charset=utf-8\r\n"
              "Content-Length: %d\r\n\r\n%s",
              (int)strlen(body), body);
}

static void send_500(struct mg_connection *conn, const char *msg) {
    const char *body = msg ? msg : "Internal server error";
    mg_printf(conn,
              "HTTP/1.1 500 Internal Server Error\r\n"
              "Content-Type: text/plain; charset=utf-8\r\n"
              "Content-Length: %d\r\n\r\n%s",
              (int)strlen(body), body);
}


/* ----- /torrent/files?hash=<hex>&offset=N&limit=50&q=<filter> ----- */
int http_torrent_files_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    char hash[41];
    if (tp_get_hash_param(conn, hash) != 0) {
        send_400(conn, "Invalid hash");
        return 400;
    }

    const struct mg_request_info *ri = mg_get_request_info(conn);
    const char *qs = ri->query_string ? ri->query_string : "";
    char buf[256];

    int offset = 0, limit = 50;
    if (mg_get_var(qs, strlen(qs), "offset", buf, sizeof(buf)) > 0) {
        offset = atoi(buf);
        if (offset < 0) offset = 0;
    }
    if (mg_get_var(qs, strlen(qs), "limit", buf, sizeof(buf)) > 0) {
        limit = atoi(buf);
        if (limit <= 0 || limit > 200) limit = 50;
    }

    char *filter = NULL;
    if (mg_get_var(qs, strlen(qs), "q", buf, sizeof(buf)) > 0) {
        filter = tp_url_decode(buf);
    }

    /* If filter is too short for trigram FTS (<3 chars), drop it. */
    char *fts_filter = NULL;
    if (filter && strlen(filter) >= 3) {
        /* Wrap in quotes to make a phrase match safe for FTS5. */
        size_t fl = strlen(filter);
        fts_filter = (char *)malloc(fl + 8);
        if (fts_filter) {
            /* Replace embedded double-quotes to avoid breaking the FTS literal. */
            char *clean = strdup(filter);
            for (char *p = clean; *p; p++) if (*p == '"') *p = ' ';
            snprintf(fts_filter, fl + 8, "\"%s\"", clean);
            free(clean);
        }
    }

    torrent_file_row_t *rows = NULL;
    int count = 0, total = 0;
    int rc = db_get_torrent_files_paginated(api->database, hash, offset, limit,
                                            fts_filter, &rows, &count, &total);
    free(filter);
    free(fts_filter);

    if (rc != 0) {
        send_404(conn, "{\"error\":\"not found\"}");
        return 404;
    }

    /* Build JSON */
    sbuf_t s = {0};
    s.cap = 8192;
    s.buf = (char *)malloc(s.cap);
    if (!s.buf) {
        db_free_torrent_file_rows(rows, count);
        send_400(conn, "OOM");
        return 500;
    }
    s.buf[0] = '\0';

    sb_append(&s, "{\"files\":[");
    for (int i = 0; i < count; i++) {
        char size_str[64];
        tp_format_size(rows[i].size_bytes, size_str, sizeof(size_str));
        /* Escape path for JSON */
        const char *p = rows[i].path ? rows[i].path : "";
        sb_append(&s, "%s{\"path\":\"", i ? "," : "");
        for (; *p; p++) {
            unsigned char c = (unsigned char)*p;
            switch (c) {
                case '"':  sb_append(&s, "\\\""); break;
                case '\\': sb_append(&s, "\\\\"); break;
                case '\n': sb_append(&s, "\\n"); break;
                case '\r': sb_append(&s, "\\r"); break;
                case '\t': sb_append(&s, "\\t"); break;
                default:
                    if (c < 0x20) sb_append(&s, "\\u%04x", c);
                    else { char tmp[2] = {(char)c, 0}; sb_append(&s, "%s", tmp); }
                    break;
            }
        }
        sb_append(&s, "\",\"size_bytes\":%lld,\"size\":\"%s\"}",
                  (long long)rows[i].size_bytes, size_str);
    }
    int has_more = (offset + count) < total;
    sb_append(&s, "],\"total\":%d,\"has_more\":%s}", total, has_more ? "true" : "false");

    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: application/json\r\n"
              "Content-Length: %d\r\n\r\n",
              (int)s.len);
    mg_write(conn, s.buf, s.len);

    free(s.buf);
    db_free_torrent_file_rows(rows, count);
    return 200;
}

/* ----- /torrent/info?hash=<hex> ----- */
int http_torrent_info_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    char hash[41];
    if (tp_get_hash_param(conn, hash) != 0) {
        send_400(conn, "Missing or invalid hash parameter");
        return 400;
    }

    torrent_summary_t ts;
    if (db_get_torrent_by_hash(api->database, hash, &ts) != 0) {
        send_404(conn, "{\"error\":\"not_found\"}");
        return 404;
    }

    char *filtered = extract_media_title(ts.name);
    if (!filtered) filtered = strdup(ts.name ? ts.name : "");

    sbuf_t s = {0};
    s.cap = 8192;
    s.buf = (char *)malloc(s.cap);
    if (!s.buf) {
        free(filtered);
        free(ts.name);
        send_500(conn, "OOM");
        return 500;
    }
    s.buf[0] = '\0';

    sb_append(&s, "{\"info_hash\":\"%s\",\"name\":\"", ts.info_hash);
    tp_append_json_string(&s, ts.name ? ts.name : "");
    sb_append(&s, "\",\"filtered_name\":\"");
    tp_append_json_string(&s, filtered ? filtered : "");
    sb_append(&s,
              "\",\"size_bytes\":%lld,\"total_peers\":%d,\"added\":%lld,\"file_count\":%d}",
              (long long)ts.size_bytes, ts.total_peers,
              (long long)ts.added_timestamp, ts.file_count);

    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: application/json\r\n"
              "Content-Length: %d\r\n\r\n",
              (int)s.len);
    mg_write(conn, s.buf, s.len);

    free(s.buf);
    free(filtered);
    free(ts.name);
    return 200;
}
