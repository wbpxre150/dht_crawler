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
#include <ctype.h>

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

static char *tp_url_encode(const char *str) {
    if (!str) return NULL;
    size_t len = strlen(str);
    char *out = (char *)malloc(len * 3 + 1);
    if (!out) return NULL;
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)str[i];
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '-' || c == '_' ||
            c == '.' || c == '~') {
            out[j++] = c;
        } else {
            snprintf(out + j, 4, "%%%02X", c);
            j += 3;
        }
    }
    out[j] = '\0';
    return out;
}

/* HTML-escape into caller-allocated buffer; returns out. */
static char *tp_html_escape(const char *in) {
    if (!in) return strdup("");
    size_t len = strlen(in);
    char *out = (char *)malloc(len * 6 + 1);
    if (!out) return NULL;
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        char c = in[i];
        switch (c) {
            case '&':  memcpy(out + j, "&amp;",  5); j += 5; break;
            case '<':  memcpy(out + j, "&lt;",   4); j += 4; break;
            case '>':  memcpy(out + j, "&gt;",   4); j += 4; break;
            case '"':  memcpy(out + j, "&quot;", 6); j += 6; break;
            case '\'': memcpy(out + j, "&#39;",  5); j += 5; break;
            default:   out[j++] = c; break;
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

/* Tokenize torrent name into "words" (after replacing dots/underscores with spaces).
 * Returns array of newly-allocated strings, count via *out_n. */
static char **tp_tokenize_name(const char *name, int *out_n) {
    *out_n = 0;
    if (!name) return NULL;
    char *copy = strdup(name);
    if (!copy) return NULL;
    for (char *p = copy; *p; p++) {
        if (*p == '.' || *p == '_') *p = ' ';
    }
    int cap = 16, n = 0;
    char **words = (char **)calloc(cap, sizeof(char *));
    if (!words) { free(copy); return NULL; }
    char *saveptr = NULL;
    char *tok = strtok_r(copy, " \t\r\n", &saveptr);
    while (tok) {
        if (n == cap) {
            cap *= 2;
            char **nw = (char **)realloc(words, cap * sizeof(char *));
            if (!nw) break;
            words = nw;
        }
        /* Strip special characters that break search queries */
        char *w = strdup(tok);
        if (w) {
            char *dst = w;
            for (char *src = w; *src; src++) {
                char c = *src;
                if (c != '(' && c != ')' && c != '[' && c != ']' &&
                    c != '{' && c != '}')
                    *dst++ = c;
            }
            *dst = '\0';
        }
        if (w && *w)
            words[n++] = w;
        else
            free(w);
        tok = strtok_r(NULL, " \t\r\n", &saveptr);
    }
    free(copy);
    *out_n = n;
    return words;
}

static int tp_word_in_filtered(const char *word, const char *filtered) {
    if (!word || !filtered) return 0;
    /* filtered has dots/underscores already replaced; do a case-insensitive token match. */
    size_t wl = strlen(word);
    if (wl == 0) return 0;
    const char *p = filtered;
    while (*p) {
        while (*p == ' ' || *p == '\t') p++;
        const char *start = p;
        while (*p && *p != ' ' && *p != '\t') p++;
        size_t tl = (size_t)(p - start);
        if (tl == wl) {
            int eq = 1;
            for (size_t i = 0; i < tl; i++) {
                if (tolower((unsigned char)start[i]) != tolower((unsigned char)word[i])) { eq = 0; break; }
            }
            if (eq) return 1;
        }
    }
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

/* ----- /torrent?hash=<hex> ----- */
int http_torrent_page_handler(struct mg_connection *conn, void *cbdata) {
    http_api_t *api = (http_api_t *)cbdata;

    char hash[41];
    if (tp_get_hash_param(conn, hash) != 0) {
        send_400(conn, "Missing or invalid hash parameter");
        return 400;
    }

    torrent_summary_t ts;
    if (db_get_torrent_by_hash(api->database, hash, &ts) != 0) {
        send_404(conn, "Torrent not found");
        return 404;
    }

    char *filtered = extract_media_title(ts.name);
    if (!filtered) filtered = strdup(ts.name ? ts.name : "");

    int word_count = 0;
    char **words = tp_tokenize_name(ts.name, &word_count);

    char size_str[64];
    tp_format_size(ts.size_bytes, size_str, sizeof(size_str));

    char *name_esc = tp_html_escape(ts.name ? ts.name : "");
    char *encoded_name = tp_url_encode(ts.name ? ts.name : "");

    sbuf_t s = {0};
    s.cap = 64 * 1024;
    s.buf = (char *)malloc(s.cap);
    if (!s.buf) {
        free(name_esc); free(encoded_name);
        for (int i = 0; i < word_count; i++) free(words[i]);
        free(words); free(filtered); free(ts.name);
        send_404(conn, "OOM");
        return 500;
    }
    s.buf[0] = '\0';

    sb_append(&s,
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        "<title>%s</title>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<style>"
        "body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:900px;margin:0 auto;padding:16px;background:#f7f7f7;color:#222}"
        "h1{font-size:1.2em;word-break:break-all}"
        ".meta{color:#555;font-size:0.9em;margin-bottom:1em}"
        ".pills{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0}"
        ".pill{display:inline-block;padding:5px 10px;border-radius:14px;border:1px solid #bbb;background:#fff;cursor:pointer;user-select:none;font-size:0.9em}"
        ".pill.on{background:#1976d2;color:#fff;border-color:#1976d2}"
        ".query-preview{background:#eef;padding:6px 10px;border-radius:4px;font-family:monospace;margin:8px 0;word-break:break-all}"
        ".btnrow{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}"
        ".btn{padding:8px 14px;border:none;border-radius:4px;background:#1976d2;color:#fff;cursor:pointer;font-size:0.95em;text-decoration:none;display:inline-block}"
        ".btn.alt{background:#555}"
        ".btn:hover{opacity:0.9}"
        ".file-tools{margin:12px 0;display:flex;gap:8px}"
        "#fileFilter{flex:1;padding:7px;border:1px solid #bbb;border-radius:4px;font-size:0.95em}"
        ".file-list{background:#fff;border:1px solid #ddd;border-radius:4px;padding:0;margin:0;list-style:none}"
        ".file-list li{padding:6px 10px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;gap:8px;font-size:0.9em}"
        ".file-list li:last-child{border-bottom:none}"
        ".file-path{word-break:break-all}"
        ".file-size{color:#777;flex-shrink:0}"
        ".sentinel{padding:8px;text-align:center;color:#888;font-size:0.85em}"
        "a{color:#1976d2}"
        "</style></head><body>",
        name_esc);

    sb_append(&s, "<p><a href='/' onclick='if(history.length>1){event.preventDefault();history.back();}'>&larr; Back</a></p>");
    sb_append(&s, "<h1>%s</h1>", name_esc);
    sb_append(&s,
        "<div class='meta'>%s &middot; %d peer%s &middot; %d file%s &middot; <code>%s</code></div>",
        size_str,
        ts.total_peers, ts.total_peers == 1 ? "" : "s",
        ts.file_count, ts.file_count == 1 ? "" : "s",
        hash);

    /* Word selector */
    sb_append(&s, "<div><strong>Select keywords for search:</strong></div><div class='pills' id='pills'>");
    for (int i = 0; i < word_count; i++) {
        char *we = tp_html_escape(words[i]);
        int on = tp_word_in_filtered(words[i], filtered);
        sb_append(&s, "<span class='pill%s' data-word=\"%s\">%s</span>",
                  on ? " on" : "", we, we);
        free(we);
    }
    sb_append(&s, "</div>");

    sb_append(&s, "<div class='query-preview' id='queryPreview'></div>");
    sb_append(&s,
        "<div class='btnrow'>"
        "<button class='btn' id='imdbBtn' onclick='openImdb()'>Search IMDb</button>"
        "<button class='btn alt' id='dhtBtn' onclick='openDht()'>Search DHT crawler</button>"
        "<button class='btn alt' onclick='refreshPeers()' id='refreshBtn'>\xE2\x86\xBB Refresh peers</button>"
        "<button class='btn alt' onclick='copyMagnet()'>\xF0\x9F\xA7\xB2 Copy magnet</button>"
        "<span id='peerInfo' style='align-self:center;color:#555'>%d peer%s</span>"
        "</div>",
        ts.total_peers, ts.total_peers == 1 ? "" : "s");

    /* File viewer */
    sb_append(&s,
        "<h3>Files</h3>"
        "<div class='file-tools'><input id='fileFilter' placeholder='Filter files...' autocomplete='off'></div>"
        "<ul class='file-list' id='fileList'></ul>"
        "<div class='sentinel' id='sentinel'>Loading...</div>");

    /* JS */
    sb_append(&s,
        "<script>"
        "var INFO_HASH=\"%s\";"
        "var TORRENT_NAME_ENC=\"%s\";"
        "function buildQuery(){"
        "  var sel=document.querySelectorAll('#pills .pill.on');"
        "  var parts=[];sel.forEach(function(p){parts.push(p.dataset.word);});"
        "  return parts.join(' ');"
        "}"
        "function updatePreview(){"
        "  var q=buildQuery();"
        "  document.getElementById('queryPreview').textContent=q||'(no words selected)';"
        "}"
        "document.querySelectorAll('#pills .pill').forEach(function(p){"
        "  p.addEventListener('click',function(){p.classList.toggle('on');updatePreview();});"
        "});"
        "updatePreview();"
        "function openImdb(){"
        "  var q=encodeURIComponent(buildQuery());"
        "  var a=document.createElement('a');"
        "  a.href='https://www.imdb.com/find/?q='+q;"
        "  a.target='_blank';a.rel='noopener noreferrer';"
        "  document.body.appendChild(a);a.click();document.body.removeChild(a);"
        "}"
        "function openDht(){"
        "  window.location.href='/search?q='+encodeURIComponent(buildQuery())+'&format=html';"
        "}"
        "function refreshPeers(){"
        "  var btn=document.getElementById('refreshBtn');"
        "  var info=document.getElementById('peerInfo');"
        "  if(btn.disabled)return;"
        "  btn.disabled=true;info.textContent='Refreshing...';"
        "  var c=new AbortController();var t=setTimeout(function(){c.abort();},25000);"
        "  fetch('/refresh?hash='+INFO_HASH,{signal:c.signal})"
        "    .then(function(r){clearTimeout(t);return r.json();})"
        "    .then(function(j){"
        "      if(j&&typeof j.total_peers==='number'){"
        "        info.textContent=j.total_peers+' peer'+(j.total_peers===1?'':'s');"
        "      }else{info.textContent='Refresh failed';}"
        "    }).catch(function(){info.textContent='Refresh failed';})"
        "    .finally(function(){btn.disabled=false;});"
        "}"
        "function copyMagnet(){"
        "  var m='magnet:?xt=urn:btih:'+INFO_HASH+'&dn='+TORRENT_NAME_ENC;"
        "  if(navigator.clipboard){navigator.clipboard.writeText(m);}else{"
        "    var ta=document.createElement('textarea');ta.value=m;document.body.appendChild(ta);"
        "    ta.select();document.execCommand('copy');document.body.removeChild(ta);}"
        "}"
        "var fileOffset=0;var fileBatch=50;var fileLoading=false;var fileHasMore=true;var fileQuery='';"
        "function resetFiles(){fileOffset=0;fileHasMore=true;"
        "  document.getElementById('fileList').innerHTML='';"
        "  document.getElementById('sentinel').textContent='Loading...';"
        "  loadFiles();}"
        "function loadFiles(){"
        "  if(fileLoading||!fileHasMore)return;fileLoading=true;"
        "  var url='/torrent/files?hash='+INFO_HASH+'&offset='+fileOffset+'&limit='+fileBatch;"
        "  if(fileQuery)url+='&q='+encodeURIComponent(fileQuery);"
        "  fetch(url).then(function(r){return r.json();}).then(function(j){"
        "    var ul=document.getElementById('fileList');"
        "    (j.files||[]).forEach(function(f){"
        "      var li=document.createElement('li');"
        "      var sp=document.createElement('span');sp.className='file-path';sp.textContent=f.path;"
        "      var sz=document.createElement('span');sz.className='file-size';sz.textContent=f.size;"
        "      li.appendChild(sp);li.appendChild(sz);ul.appendChild(li);"
        "    });"
        "    fileOffset+=(j.files||[]).length;"
        "    fileHasMore=!!j.has_more;"
        "    document.getElementById('sentinel').textContent=fileHasMore?'Scroll for more...':('No more files ('+(j.total||fileOffset)+' total)');"
        "  }).catch(function(){"
        "    document.getElementById('sentinel').textContent='Failed to load files';"
        "    fileHasMore=false;"
        "  }).finally(function(){fileLoading=false;});"
        "}"
        "var ff=document.getElementById('fileFilter');"
        "var ffTimer=null;"
        "ff.addEventListener('input',function(){"
        "  clearTimeout(ffTimer);"
        "  ffTimer=setTimeout(function(){fileQuery=ff.value.trim();resetFiles();},250);"
        "});"
        "var io=new IntersectionObserver(function(es){"
        "  es.forEach(function(en){if(en.isIntersecting)loadFiles();});"
        "});"
        "io.observe(document.getElementById('sentinel'));"
        "loadFiles();"
        "</script></body></html>",
        hash,
        encoded_name ? encoded_name : "");

    /* Send response */
    mg_printf(conn,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html; charset=utf-8\r\n"
              "Content-Length: %d\r\n"
              "\r\n",
              (int)s.len);
    mg_write(conn, s.buf, s.len);

    free(s.buf);
    free(name_esc);
    free(encoded_name);
    free(filtered);
    for (int i = 0; i < word_count; i++) free(words[i]);
    free(words);
    free(ts.name);
    return 200;
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
