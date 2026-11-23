/**
 * Minimal HTTP(S) GET-to-file. See include/http_fetch.h for the contract.
 *
 * Shape: no globals, no threads, no connection reuse. Every call owns its
 * socket, its TLS session and its deadline, and unwinds through a single exit
 * path. OpenSSL is already linked by the crawler (src/classifier.c verifies
 * sha256s with it), so HTTPS costs no new dependency.
 *
 * Host-testable: tests/test_http_fetch.c drives the http:// path against a
 * forked local server and the https:// path against a forked OpenSSL server
 * with a committed self-signed fixture, so both the plain and the TLS paths
 * (including "reject an untrusted certificate") are exercised.
 */

#define _GNU_SOURCE

#include "http_fetch.h"

#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#define HF_HEADER_CAP 16384
#define HF_READ_CHUNK 16384

typedef struct {
    char scheme[8];
    char host[256];
    char port[8];
    char path[1024];
} hf_url_t;

typedef struct {
    int fd;
    SSL *ssl;
} hf_conn_t;

/* Buffered reader: header parsing and body reading share the same window, so
 * bytes that arrived with the header block are never lost. */
typedef struct {
    hf_conn_t *conn;
    unsigned char buf[HF_READ_CHUNK];
    size_t len;
    size_t pos;
} hf_reader_t;

static long long hf_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static void hf_err(char *err, size_t err_len, const char *fmt, ...) {
    if (!err || err_len == 0) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, err_len, fmt, ap);
    va_end(ap);
}

/* ------------------------------------------------------------------------- */
/* URL                                                                       */
/* ------------------------------------------------------------------------- */

static int hf_parse_url(const char *url, hf_url_t *out, char *err, size_t err_len) {
    if (!url || !*url) {
        hf_err(err, err_len, "empty url");
        return -1;
    }
    memset(out, 0, sizeof(*out));

    const char *rest;
    if (strncmp(url, "http://", 7) == 0) {
        snprintf(out->scheme, sizeof(out->scheme), "http");
        snprintf(out->port, sizeof(out->port), "80");
        rest = url + 7;
    } else if (strncmp(url, "https://", 8) == 0) {
        snprintf(out->scheme, sizeof(out->scheme), "https");
        snprintf(out->port, sizeof(out->port), "443");
        rest = url + 8;
    } else {
        hf_err(err, err_len, "unsupported scheme (need http:// or https://): %s", url);
        return -1;
    }

    const char *slash = strchr(rest, '/');
    const char *host_end = slash ? slash : rest + strlen(rest);
    const char *colon = memchr(rest, ':', (size_t)(host_end - rest));

    size_t host_len = (size_t)((colon ? colon : host_end) - rest);
    if (host_len == 0 || host_len >= sizeof(out->host)) {
        hf_err(err, err_len, "bad host in url: %s", url);
        return -1;
    }
    memcpy(out->host, rest, host_len);
    out->host[host_len] = '\0';

    if (colon) {
        size_t port_len = (size_t)(host_end - colon - 1);
        if (port_len == 0 || port_len >= sizeof(out->port)) {
            hf_err(err, err_len, "bad port in url: %s", url);
            return -1;
        }
        memcpy(out->port, colon + 1, port_len);
        out->port[port_len] = '\0';
        for (size_t i = 0; i < port_len; i++) {
            if (out->port[i] < '0' || out->port[i] > '9') {
                hf_err(err, err_len, "non-numeric port in url: %s", url);
                return -1;
            }
        }
    }

    if (slash && slash[1] != '\0') {
        if (strlen(slash) >= sizeof(out->path)) {
            hf_err(err, err_len, "path too long in url: %s", url);
            return -1;
        }
        snprintf(out->path, sizeof(out->path), "%s", slash);
    } else {
        snprintf(out->path, sizeof(out->path), "/");
    }
    return 0;
}

/* Build "<scheme>://<host>[:port]<location>" for a redirect. `location` may be
 * absolute (has a scheme), absolute-path, or relative to the current path. */
static int hf_resolve_location(const hf_url_t *cur, const char *location,
                               char *out, size_t out_len, char *err, size_t err_len) {
    if (!location || !*location) {
        hf_err(err, err_len, "empty Location header");
        return -1;
    }
    if (strncmp(location, "http://", 7) == 0 || strncmp(location, "https://", 8) == 0) {
        if (strlen(location) >= out_len) {
            hf_err(err, err_len, "redirect target too long");
            return -1;
        }
        snprintf(out, out_len, "%s", location);
        return 0;
    }

    char base[1200];
    int n;
    if (location[0] == '/') {
        n = snprintf(base, sizeof(base), "%s://%s:%s", cur->scheme, cur->host, cur->port);
    } else {
        /* Relative: drop everything after the last '/' of the current path. */
        char dir[1024];
        snprintf(dir, sizeof(dir), "%s", cur->path);
        char *last = strrchr(dir, '/');
        if (last) last[1] = '\0';
        n = snprintf(base, sizeof(base), "%s://%s:%s%s", cur->scheme, cur->host, cur->port, dir);
    }
    if (n < 0 || (size_t)n >= sizeof(base)) {
        hf_err(err, err_len, "redirect base too long");
        return -1;
    }
    size_t base_len = strlen(base);
    size_t loc_len = strlen(location);
    if (base_len + loc_len >= out_len) {
        hf_err(err, err_len, "redirect target too long");
        return -1;
    }
    memcpy(out, base, base_len);
    memcpy(out + base_len, location, loc_len + 1);
    return 0;
}

/* ------------------------------------------------------------------------- */
/* Connection                                                               */
/* ------------------------------------------------------------------------- */

static int hf_connect(const hf_url_t *url, int timeout_ms, hf_conn_t *conn,
                      char *err, size_t err_len) {
    struct addrinfo hints, *res = NULL, *ai;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int rc = getaddrinfo(url->host, url->port, &hints, &res);
    if (rc != 0) {
        hf_err(err, err_len, "DNS failed for %s: %s", url->host, gai_strerror(rc));
        return -1;
    }

    long long deadline = hf_now_ms() + timeout_ms;
    int fd = -1;
    for (ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0) continue;

        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int cr = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (cr == 0) {
            fcntl(fd, F_SETFL, flags);
            break;
        }
        if (errno == EINPROGRESS) {
            long long left = deadline - hf_now_ms();
            if (left > 0) {
                struct pollfd pfd = {.fd = fd, .events = POLLOUT};
                int pr = poll(&pfd, 1, (int)left);
                if (pr > 0) {
                    int so_err = 0;
                    socklen_t sl = sizeof(so_err);
                    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &sl) == 0 && so_err == 0) {
                        fcntl(fd, F_SETFL, flags);
                        break;
                    }
                }
            }
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        hf_err(err, err_len, "connect failed for %s:%s", url->host, url->port);
        return -1;
    }

    /* Blocking from here on, but every read/write is deadline-bounded by the
     * caller's loop plus these socket timeouts. */
    long long left = deadline - hf_now_ms();
    if (left <= 0) left = 1;
    struct timeval tv;
    tv.tv_sec = (time_t)(left / 1000);
    tv.tv_usec = (suseconds_t)((left % 1000) * 1000);
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    conn->fd = fd;
    conn->ssl = NULL;
    return 0;
}

static int hf_start_tls(hf_conn_t *conn, const hf_url_t *url, const char *ca_file,
                        const char *ca_dir, char *err, size_t err_len) {
    static int ssl_initialised = 0;
    if (!ssl_initialised) {
        SSL_library_init();
        SSL_load_error_strings();
        ssl_initialised = 1;
    }

    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) {
        hf_err(err, err_len, "SSL_CTX_new failed");
        return -1;
    }
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

    if (ca_file || ca_dir) {
        if (SSL_CTX_load_verify_locations(ctx, ca_file, ca_dir) != 1) {
            hf_err(err, err_len, "cannot load CA locations (file=%s dir=%s)",
                   ca_file ? ca_file : "-", ca_dir ? ca_dir : "-");
            SSL_CTX_free(ctx);
            return -1;
        }
    } else if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
        hf_err(err, err_len, "no system CA store available");
        SSL_CTX_free(ctx);
        return -1;
    }

    SSL *ssl = SSL_new(ctx);
    SSL_CTX_free(ctx); /* the SSL object holds its own reference */
    if (!ssl) {
        hf_err(err, err_len, "SSL_new failed");
        return -1;
    }

    SSL_set_fd(ssl, conn->fd);
    SSL_set_tlsext_host_name(ssl, url->host);
    /* Hostname verification, not just chain validation: a valid certificate for
     * another name must not be accepted. */
    X509_VERIFY_PARAM *param = SSL_get0_param(ssl);
    X509_VERIFY_PARAM_set_hostflags(param, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    struct in_addr v4;
    struct in6_addr v6;
    if (inet_pton(AF_INET, url->host, &v4) == 1 || inet_pton(AF_INET6, url->host, &v6) == 1) {
        /* An IP literal must be matched against the certificate's IP SAN; set1_host
         * compares DNS names only and would fail with "hostname mismatch". */
        if (X509_VERIFY_PARAM_set1_ip_asc(param, url->host) != 1) {
            hf_err(err, err_len, "cannot set expected IP %s", url->host);
            SSL_free(ssl);
            return -1;
        }
    } else if (X509_VERIFY_PARAM_set1_host(param, url->host, 0) != 1) {
        hf_err(err, err_len, "cannot set expected hostname %s", url->host);
        SSL_free(ssl);
        return -1;
    }

    if (SSL_connect(ssl) != 1) {
        long vr = SSL_get_verify_result(ssl);
        hf_err(err, err_len, "TLS handshake failed for %s: %s%s%s", url->host,
               vr == X509_V_OK ? "" : "certificate verify failed: ",
               vr == X509_V_OK ? "" : X509_verify_cert_error_string(vr),
               vr == X509_V_OK ? ERR_error_string(ERR_get_error(), NULL) : "");
        SSL_free(ssl);
        return -1;
    }

    conn->ssl = ssl;
    return 0;
}

static void hf_close(hf_conn_t *conn) {
    if (conn->ssl) {
        SSL_shutdown(conn->ssl);
        SSL_free(conn->ssl);
        conn->ssl = NULL;
    }
    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }
}

static ssize_t hf_read(hf_conn_t *conn, void *buf, size_t len) {
    if (conn->ssl) {
        int n = SSL_read(conn->ssl, buf, (int)len);
        if (n > 0) return n;
        int e = SSL_get_error(conn->ssl, n);
        if (e == SSL_ERROR_ZERO_RETURN) return 0;
        return -1;
    }
    return recv(conn->fd, buf, len, 0);
}

static ssize_t hf_write(hf_conn_t *conn, const void *buf, size_t len) {
    if (conn->ssl) {
        int n = SSL_write(conn->ssl, buf, (int)len);
        return n > 0 ? n : -1;
    }
    return send(conn->fd, buf, len, 0);
}

/* ------------------------------------------------------------------------- */
/* Reader                                                                    */
/* ------------------------------------------------------------------------- */

static int hf_fill(hf_reader_t *r) {
    ssize_t n = hf_read(r->conn, r->buf, sizeof(r->buf));
    if (n <= 0) return -1;
    r->len = (size_t)n;
    r->pos = 0;
    return 0;
}

/* Next byte, or -1 at EOF/error. */
static int hf_byte(hf_reader_t *r) {
    if (r->pos >= r->len && hf_fill(r) != 0) return -1;
    return r->buf[r->pos++];
}

/* ------------------------------------------------------------------------- */
/* One request/response round trip                                           */
/* ------------------------------------------------------------------------- */

typedef struct {
    int status;
    long long content_length; /* -1 when absent */
    int chunked;
    char location[1024];
} hf_head_t;

static int hf_read_head(hf_reader_t *r, hf_head_t *head, char *err, size_t err_len) {
    char raw[HF_HEADER_CAP];
    size_t used = 0;
    int matched = 0;

    /* Read until CRLFCRLF, one byte at a time past the first 4: headers are
     * small and this keeps the body bytes in the reader's window. */
    while (used < sizeof(raw) - 1) {
        int c = hf_byte(r);
        if (c < 0) {
            hf_err(err, err_len, "connection closed before the response headers");
            return -1;
        }
        raw[used++] = (char)c;
        if (used >= 4 && memcmp(raw + used - 4, "\r\n\r\n", 4) == 0) {
            matched = 1;
            break;
        }
    }
    if (!matched) {
        hf_err(err, err_len, "response headers exceed %d bytes", HF_HEADER_CAP);
        return -1;
    }
    raw[used] = '\0';

    memset(head, 0, sizeof(*head));
    head->content_length = -1;

    char *line_end = strstr(raw, "\r\n");
    if (!line_end) {
        hf_err(err, err_len, "malformed status line");
        return -1;
    }
    /* "HTTP/1.1 200 OK" */
    if (strncmp(raw, "HTTP/", 5) != 0 || line_end - raw < 12) {
        hf_err(err, err_len, "malformed status line: %.32s", raw);
        return -1;
    }
    char *sp = strchr(raw, ' ');
    if (!sp) {
        hf_err(err, err_len, "malformed status line: %.32s", raw);
        return -1;
    }
    head->status = atoi(sp + 1);
    if (head->status < 100 || head->status > 599) {
        hf_err(err, err_len, "malformed status code in: %.32s", raw);
        return -1;
    }

    char *p = line_end + 2;
    while (p < raw + used) {
        char *eol = strstr(p, "\r\n");
        if (!eol || eol == p) break;
        *eol = '\0';

        char *colon = strchr(p, ':');
        if (colon) {
            *colon = '\0';
            char *value = colon + 1;
            while (*value == ' ' || *value == '\t') value++;

            if (strcasecmp(p, "Content-Length") == 0) {
                head->content_length = atoll(value);
            } else if (strcasecmp(p, "Transfer-Encoding") == 0) {
                if (strcasestr(value, "chunked")) head->chunked = 1;
            } else if (strcasecmp(p, "Location") == 0) {
                snprintf(head->location, sizeof(head->location), "%s", value);
            }
        }
        p = eol + 2;
    }
    return 0;
}

/* Copy exactly `want` bytes (or fewer at EOF) into out, capped. */
static int hf_copy_body(hf_reader_t *r, FILE *out, long long want, size_t *written,
                        size_t max_bytes, long long deadline, char *err, size_t err_len) {
    while (want != 0) {
        if (hf_now_ms() > deadline) {
            hf_err(err, err_len, "transfer deadline exceeded after %zu bytes", *written);
            return -1;
        }
        if (r->pos >= r->len && hf_fill(r) != 0) {
            if (want > 0) {
                hf_err(err, err_len, "connection closed with %lld bytes outstanding", want);
                return -1;
            }
            return 0; /* read-to-EOF mode */
        }
        size_t avail = r->len - r->pos;
        size_t take = avail;
        if (want > 0 && (long long)take > want) take = (size_t)want;
        if (*written + take > max_bytes) {
            hf_err(err, err_len, "response exceeds the %zu byte cap", max_bytes);
            return -1;
        }
        if (fwrite(r->buf + r->pos, 1, take, out) != take) {
            hf_err(err, err_len, "write failed: %s", strerror(errno));
            return -1;
        }
        r->pos += take;
        *written += take;
        if (want > 0) want -= (long long)take;
    }
    return 0;
}

static int hf_read_chunked(hf_reader_t *r, FILE *out, size_t *written, size_t max_bytes,
                           long long deadline, char *err, size_t err_len) {
    for (;;) {
        /* chunk size line (hex, possibly with ";ext") */
        char line[64];
        size_t n = 0;
        for (;;) {
            int c = hf_byte(r);
            if (c < 0) {
                hf_err(err, err_len, "connection closed inside a chunk header");
                return -1;
            }
            if (c == '\n') break;
            if (c != '\r' && n < sizeof(line) - 1) line[n++] = (char)c;
            if (n >= sizeof(line) - 1) {
                hf_err(err, err_len, "chunk size line too long");
                return -1;
            }
        }
        line[n] = '\0';
        char *semi = strchr(line, ';');
        if (semi) *semi = '\0';
        char *endp = NULL;
        unsigned long long size = strtoull(line, &endp, 16);
        if (endp == line) {
            hf_err(err, err_len, "malformed chunk size: %.32s", line);
            return -1;
        }
        if (size == 0) {
            /* trailer section: read until the blank line */
            for (;;) {
                int c = hf_byte(r);
                if (c < 0) return 0;
                if (c == '\n') {
                    int c2 = hf_byte(r);
                    if (c2 == '\n') return 0;
                    if (c2 == '\r') {
                        int c3 = hf_byte(r);
                        if (c3 == '\n' || c3 < 0) return 0;
                    }
                }
            }
        }
        if (hf_copy_body(r, out, (long long)size, written, max_bytes, deadline, err, err_len) != 0) {
            return -1;
        }
        /* trailing CRLF */
        int cr = hf_byte(r);
        int lf = hf_byte(r);
        if (cr < 0 || lf < 0) {
            hf_err(err, err_len, "connection closed after a chunk");
            return -1;
        }
    }
}

static int hf_attempt(const hf_url_t *url, const char *dest_path, size_t max_bytes,
                      long long deadline, const char *ca_file, const char *ca_dir,
                      char *err, size_t err_len);

static int hf_follow_redirect(const hf_url_t *cur, const char *location, int hops,
                              const char *dest_path, size_t max_bytes, long long deadline,
                              const char *ca_file, const char *ca_dir,
                              char *err, size_t err_len) {
    if (hops <= 0) {
        hf_err(err, err_len, "too many redirects (last: %s)", location);
        return -1;
    }
    char next[2048];
    if (hf_resolve_location(cur, location, next, sizeof(next), err, err_len) != 0) return -1;

    hf_url_t resolved;
    if (hf_parse_url(next, &resolved, err, err_len) != 0) return -1;
    return hf_attempt(&resolved, dest_path, max_bytes, deadline, ca_file, ca_dir, err, err_len);
}

static int hf_attempt(const hf_url_t *url, const char *dest_path, size_t max_bytes,
                      long long deadline, const char *ca_file, const char *ca_dir,
                      char *err, size_t err_len) {
    hf_conn_t conn = {.fd = -1, .ssl = NULL};
    hf_reader_t *reader = NULL;
    FILE *out = NULL;
    char *tmp_path = NULL;
    int result = -1;

    long long left = deadline - hf_now_ms();
    if (left <= 0) {
        hf_err(err, err_len, "transfer deadline exceeded before connecting");
        return -1;
    }

    if (hf_connect(url, (int)left, &conn, err, err_len) != 0) goto done;

    if (strcmp(url->scheme, "https") == 0) {
        if (hf_start_tls(&conn, url, ca_file, ca_dir, err, err_len) != 0) goto done;
    }

    char request[2048];
    int host_hdr = (strcmp(url->scheme, "https") == 0 && strcmp(url->port, "443") == 0) ||
                    (strcmp(url->scheme, "http") == 0 && strcmp(url->port, "80") == 0);
    int rn = snprintf(request, sizeof(request),
                      "GET %s HTTP/1.1\r\n"
                      "Host: %s%s%s\r\n"
                      "User-Agent: dht_crawler-http_fetch/1\r\n"
                      "Accept: */*\r\n"
                      "Accept-Encoding: identity\r\n"
                      "Connection: close\r\n"
                      "\r\n",
                      url->path, url->host,
                      host_hdr ? "" : ":", host_hdr ? "" : url->port);
    if (rn < 0 || (size_t)rn >= sizeof(request)) {
        hf_err(err, err_len, "request line too long");
        goto done;
    }
    size_t sent = 0;
    while (sent < (size_t)rn) {
        ssize_t n = hf_write(&conn, request + sent, (size_t)rn - sent);
        if (n <= 0) {
            hf_err(err, err_len, "send failed: %s", strerror(errno));
            goto done;
        }
        sent += (size_t)n;
    }

    reader = calloc(1, sizeof(*reader));
    if (!reader) {
        hf_err(err, err_len, "out of memory");
        goto done;
    }
    reader->conn = &conn;

    hf_head_t head;
    if (hf_read_head(reader, &head, err, err_len) != 0) goto done;

    if (head.status >= 300 && head.status < 400 && head.status != 304) {
        if (head.location[0] == '\0') {
            hf_err(err, err_len, "HTTP %d without a Location header", head.status);
            goto done;
        }
        result = hf_follow_redirect(url, head.location, HTTP_FETCH_MAX_REDIRECTS, dest_path,
                                    max_bytes, deadline, ca_file, ca_dir, err, err_len);
        goto done;
    }
    if (head.status < 200 || head.status >= 300) {
        hf_err(err, err_len, "HTTP %d from %s://%s%s", head.status, url->scheme, url->host, url->path);
        goto done;
    }

    size_t tmp_len = strlen(dest_path) + 8;
    tmp_path = malloc(tmp_len);
    if (!tmp_path) {
        hf_err(err, err_len, "out of memory");
        goto done;
    }
    snprintf(tmp_path, tmp_len, "%s.tmp", dest_path);

    out = fopen(tmp_path, "wb");
    if (!out) {
        hf_err(err, err_len, "cannot create %s: %s", tmp_path, strerror(errno));
        goto done;
    }

    size_t written = 0;
    if (head.chunked) {
        if (hf_read_chunked(reader, out, &written, max_bytes, deadline, err, err_len) != 0) goto done;
    } else {
        long long want = head.content_length >= 0 ? head.content_length : 0;
        if (hf_copy_body(reader, out, want, &written, max_bytes, deadline, err, err_len) != 0) goto done;
        if (head.content_length >= 0 && (long long)written != head.content_length) {
            hf_err(err, err_len, "short body: %zu of %lld bytes", written, head.content_length);
            goto done;
        }
    }

    if (fflush(out) != 0 || fsync(fileno(out)) != 0) {
        hf_err(err, err_len, "flush failed for %s: %s", tmp_path, strerror(errno));
        goto done;
    }
    if (fclose(out) != 0) {
        out = NULL;
        hf_err(err, err_len, "close failed for %s: %s", tmp_path, strerror(errno));
        goto done;
    }
    out = NULL;

    if (rename(tmp_path, dest_path) != 0) {
        hf_err(err, err_len, "cannot move %s into place: %s", tmp_path, strerror(errno));
        goto done;
    }
    result = 0;

done:
    if (out) fclose(out);
    if (result != 0 && tmp_path) unlink(tmp_path);
    free(tmp_path);
    free(reader);
    hf_close(&conn);
    return result;
}

int http_fetch_to_file(const char *url, const char *dest_path,
                       size_t max_bytes, int timeout_sec,
                       const char *ca_file, const char *ca_dir,
                       char *err, size_t err_len) {
    if (err && err_len) err[0] = '\0';
    if (!url || !dest_path) {
        hf_err(err, err_len, "url and destination are required");
        return -1;
    }
    if (max_bytes == 0) max_bytes = HTTP_FETCH_DEFAULT_MAX_BYTES;
    if (timeout_sec <= 0) timeout_sec = HTTP_FETCH_DEFAULT_TIMEOUT_SEC;

    hf_url_t parsed;
    if (hf_parse_url(url, &parsed, err, err_len) != 0) return -1;

    long long deadline = hf_now_ms() + (long long)timeout_sec * 1000LL;
    return hf_attempt(&parsed, dest_path, max_bytes, deadline, ca_file, ca_dir, err, err_len);
}
