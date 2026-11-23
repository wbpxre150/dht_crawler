/**
 * tests/test_http_fetch.c — the HTTP(S) fetcher the BEP51 cache download rides on.
 *
 * Shape: no test framework, no database, no crawler. A scripted server is forked
 * per case and answers exactly the responses the case asks for, so every branch
 * that matters is exercised for real over a socket rather than mocked:
 *
 *   (a) 200 + Content-Length            -> body on disk, byte-exact
 *   (b) 200 + Transfer-Encoding: chunked -> chunk decoding, byte-exact
 *   (c) 404                              -> -1, "404" in the reason, no file
 *   (d) 302 with an absolute-path Location -> followed to the second response
 *   (e) a body larger than max_bytes      -> -1, cap mentioned, no file
 *   (f) ftp:// URL                        -> -1 without touching the network
 *   (g) nothing listening                 -> -1, connect failure
 *   (h) https with an untrusted cert      -> -1, certificate rejected
 *   (i) https with the fixture CA         -> 200, body on disk
 *
 * (h) is the one that must never regress: the fetch installs a file that decides
 * which peers the crawler talks to, so a silent accept of any certificate would
 * be a remote-influence bug. The fixture is a self-signed cert for
 * CN=localhost / SAN DNS:localhost,IP:127.0.0.1 under tests/fixtures/tls.
 *
 * Runs with cwd = the tree root (same convention as the other host tests).
 */

#define _GNU_SOURCE

#include "http_fetch.h"

#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

#define TLS_CERT "tests/fixtures/tls/test-cert.pem"
#define TLS_KEY "tests/fixtures/tls/test-key.pem"
#define OUT_PATH "build/test_http_fetch.out"

static int failures = 0;
static int checks = 0;

#define CHECK(cond, ...)                                              \
    do {                                                              \
        checks++;                                                     \
        if (!(cond)) {                                                \
            failures++;                                               \
            printf("  FAIL %s:%d: ", __FILE__, __LINE__);             \
            printf(__VA_ARGS__);                                      \
            printf("\n");                                             \
        }                                                             \
    } while (0)

typedef struct {
    pid_t pid;
    int port;
} srv_t;

static ssize_t srv_read(SSL *ssl, int fd, void *buf, size_t len) {
    if (ssl) {
        int n = SSL_read(ssl, buf, (int)len);
        return n > 0 ? n : -1;
    }
    return recv(fd, buf, len, 0);
}

static int read_request(SSL *ssl, int fd) {
    char buf[4096];
    size_t used = 0;
    while (used < sizeof(buf) - 1) {
        ssize_t n = srv_read(ssl, fd, buf + used, sizeof(buf) - 1 - used);
        if (n <= 0) return -1;
        used += (size_t)n;
        buf[used] = '\0';
        if (strstr(buf, "\r\n\r\n")) return 0;
    }
    return -1;
}

static int server_start(srv_t *srv, const char **responses, int n, int use_tls) {
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) return -1;
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(ls, (struct sockaddr *)&addr, sizeof(addr)) != 0) { close(ls); return -1; }
    if (listen(ls, 8) != 0) { close(ls); return -1; }

    socklen_t alen = sizeof(addr);
    if (getsockname(ls, (struct sockaddr *)&addr, &alen) != 0) { close(ls); return -1; }
    srv->port = ntohs(addr.sin_port);

    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) { close(ls); return -1; }
    if (pid == 0) {
        /* child: serve n connections then exit */
        SSL_CTX *ctx = NULL;
        if (use_tls) {
            SSL_library_init();
            SSL_load_error_strings();
            ctx = SSL_CTX_new(TLS_server_method());
            if (!ctx) _exit(2);
            if (SSL_CTX_use_certificate_file(ctx, TLS_CERT, SSL_FILETYPE_PEM) != 1) _exit(3);
            if (SSL_CTX_use_PrivateKey_file(ctx, TLS_KEY, SSL_FILETYPE_PEM) != 1) _exit(4);
        }
        for (int i = 0; i < n; i++) {
            int fd = accept(ls, NULL, NULL);
            if (fd < 0) _exit(5);
            SSL *ssl = NULL;
            if (ctx) {
                ssl = SSL_new(ctx);
                if (!ssl) _exit(6);
                SSL_set_fd(ssl, fd);
                if (SSL_accept(ssl) != 1) { SSL_free(ssl); close(fd); _exit(7); }
            }
            if (read_request(ssl, fd) != 0) { if (ssl) SSL_free(ssl); close(fd); _exit(8); }

            const char *resp = responses[i < n ? i : n - 1];
            size_t len = strlen(resp);
            if (ssl) {
                SSL_write(ssl, resp, (int)len);
                SSL_shutdown(ssl);
                SSL_free(ssl);
            } else {
                size_t off = 0;
                while (off < len) {
                    ssize_t w = send(fd, resp + off, len - off, 0);
                    if (w <= 0) break;
                    off += (size_t)w;
                }
            }
            close(fd);
        }
        if (ctx) SSL_CTX_free(ctx);
        _exit(0);
    }

    close(ls);
    srv->pid = pid;
    return 0;
}

static void server_stop(srv_t *srv) {
    if (srv->pid > 0) {
        int status = 0;
        /* Give the child a moment to finish its script, then insist. */
        for (int i = 0; i < 40; i++) {
            pid_t r = waitpid(srv->pid, &status, WNOHANG);
            if (r == srv->pid) { srv->pid = 0; return; }
            usleep(50000);
        }
        kill(srv->pid, SIGKILL);
        waitpid(srv->pid, &status, 0);
        srv->pid = 0;
    }
}

static int file_equals(const char *path, const char *want) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strcmp(buf, want) == 0;
}

static int file_exists(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    fclose(f);
    return 1;
}

static void url_for(const srv_t *srv, int tls, const char *path, char *out, size_t out_len) {
    snprintf(out, out_len, "%s://127.0.0.1:%d%s", tls ? "https" : "http", srv->port, path);
}

int main(void) {
    char url[512];
    char err[256];
    srv_t srv;

    unlink(OUT_PATH);
    printf("test_http_fetch: HTTP(S) GET-to-file\n");

    /* (a) 200 + Content-Length */
    {
        const char *body = "cache-bytes-ok";
        char resp[512];
        snprintf(resp, sizeof(resp),
                 "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
                 strlen(body), body);
        const char *responses[] = {resp};
        CHECK(server_start(&srv, responses, 1, 0) == 0, "server start (a)");
        url_for(&srv, 0, "/cache.dat", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 10, NULL, NULL, err, sizeof(err));
        CHECK(rc == 0, "(a) fetch returned %d (%s)", rc, err);
        CHECK(file_equals(OUT_PATH, body), "(a) body mismatch on disk");
        server_stop(&srv);
        unlink(OUT_PATH);
    }

    /* (b) chunked */
    {
        const char *responses[] = {
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            "5\r\nhello\r\n"
            "6\r\n world\r\n"
            "0\r\n\r\n"
        };
        CHECK(server_start(&srv, responses, 1, 0) == 0, "server start (b)");
        url_for(&srv, 0, "/c", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 10, NULL, NULL, err, sizeof(err));
        CHECK(rc == 0, "(b) fetch returned %d (%s)", rc, err);
        CHECK(file_equals(OUT_PATH, "hello world"), "(b) chunked body mismatch");
        server_stop(&srv);
        unlink(OUT_PATH);
    }

    /* (c) 404 */
    {
        const char *responses[] = {
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        };
        CHECK(server_start(&srv, responses, 1, 0) == 0, "server start (c)");
        url_for(&srv, 0, "/missing", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 10, NULL, NULL, err, sizeof(err));
        CHECK(rc != 0, "(c) a 404 must fail");
        CHECK(strstr(err, "404") != NULL, "(c) reason should name the status: %s", err);
        CHECK(!file_exists(OUT_PATH), "(c) no file may be created for a 404");
        server_stop(&srv);
    }

    /* (d) 302 -> absolute-path Location */
    {
        const char *responses[] = {
            "HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\nredirect!"
        };
        CHECK(server_start(&srv, responses, 2, 0) == 0, "server start (d)");
        url_for(&srv, 0, "/start", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 10, NULL, NULL, err, sizeof(err));
        CHECK(rc == 0, "(d) redirect not followed: %d (%s)", rc, err);
        CHECK(file_equals(OUT_PATH, "redirect!"), "(d) redirected body mismatch");
        server_stop(&srv);
        unlink(OUT_PATH);
    }

    /* (e) body over the cap */
    {
        char body[256];
        memset(body, 'x', sizeof(body) - 1);
        body[sizeof(body) - 1] = '\0';
        char resp[512];
        snprintf(resp, sizeof(resp),
                 "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
                 strlen(body), body);
        const char *responses[] = {resp};
        CHECK(server_start(&srv, responses, 1, 0) == 0, "server start (e)");
        url_for(&srv, 0, "/big", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 16, 10, NULL, NULL, err, sizeof(err));
        CHECK(rc != 0, "(e) a body over max_bytes must fail");
        CHECK(strstr(err, "cap") != NULL, "(e) reason should mention the cap: %s", err);
        CHECK(!file_exists(OUT_PATH), "(e) no file may be left behind");
        server_stop(&srv);
    }

    /* (f) unsupported scheme, no network */
    {
        int rc = http_fetch_to_file("ftp://example.invalid/x", OUT_PATH, 0, 5, NULL, NULL, err, sizeof(err));
        CHECK(rc != 0, "(f) ftp:// must be refused");
        CHECK(strstr(err, "scheme") != NULL, "(f) reason should name the scheme: %s", err);
    }

    /* (g) nothing listening */
    {
        srv_t dead;
        CHECK(server_start(&dead, (const char *[]){"x"}, 0, 0) == 0, "server start (g)");
        int port = dead.port;
        server_stop(&dead); /* closes the listener, port is now free */
        snprintf(url, sizeof(url), "http://127.0.0.1:%d/x", port);
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 5, NULL, NULL, err, sizeof(err));
        CHECK(rc != 0, "(g) a closed port must fail");
        CHECK(strstr(err, "connect") != NULL, "(g) reason should mention connect: %s", err);
    }

    /* (h) TLS with an untrusted certificate must be refused */
    {
        const char *responses[] = {
            "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsecret"
        };
        CHECK(server_start(&srv, responses, 1, 1) == 0, "server start (h)");
        url_for(&srv, 1, "/tls", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 10, NULL, NULL, err, sizeof(err));
        CHECK(rc != 0, "(h) an untrusted certificate must be refused");
        CHECK(strstr(err, "certificate") != NULL, "(h) reason should mention the certificate: %s", err);
        CHECK(!file_exists(OUT_PATH), "(h) nothing may be written from an untrusted peer");
        server_stop(&srv);
    }

    /* (i) TLS with the fixture CA trusted */
    {
        const char *responses[] = {
            "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\ntrusted-tls"
        };
        CHECK(server_start(&srv, responses, 1, 1) == 0, "server start (i)");
        url_for(&srv, 1, "/tls", url, sizeof(url));
        int rc = http_fetch_to_file(url, OUT_PATH, 0, 10, TLS_CERT, NULL, err, sizeof(err));
        CHECK(rc == 0, "(i) trusted TLS fetch failed: %d (%s)", rc, err);
        CHECK(file_equals(OUT_PATH, "trusted-tls"), "(i) TLS body mismatch");
        server_stop(&srv);
        unlink(OUT_PATH);
    }

    printf("test_http_fetch: %d checks, %d failure(s)\n", checks, failures);
    if (failures == 0) printf("test_http_fetch: all checks passed\n");
    return failures == 0 ? 0 : 1;
}
