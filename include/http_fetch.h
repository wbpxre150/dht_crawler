#ifndef HTTP_FETCH_H
#define HTTP_FETCH_H

#include <stddef.h>

/**
 * Minimal HTTP(S) GET-to-file, used to fetch the published BEP51 cache.
 *
 * Deliberately tiny: one GET, one file, no connection reuse, no cookies, no
 * proxies. It exists because the crawler otherwise has no way to bootstrap from
 * the network except by asking routers for nodes, and a cold cache means a slow
 * (or aborted) start.
 *
 * What it does support, because the object it fetches lives behind a CDN:
 *   - http:// and https://, with TLS certificate and hostname verification
 *     against the system store (or an explicit CA file/directory);
 *   - up to `HTTP_FETCH_MAX_REDIRECTS` 3xx hops, absolute or absolute-path
 *     Location headers;
 *   - Content-Length and chunked bodies, with a hard byte cap;
 *   - connect and total deadlines, so a black-holed host cannot wedge startup.
 *
 * Failures are non-fatal by design: the caller keeps its existing cold-start
 * path (router bootstrap + iterative discovery).
 */

/*: Redirect hops followed before giving up. */
#define HTTP_FETCH_MAX_REDIRECTS 3
/*: Default body cap when the caller passes max_bytes 0 (64 MiB). */
#define HTTP_FETCH_DEFAULT_MAX_BYTES (64u * 1024u * 1024u)
/*: Default total deadline when the caller passes timeout_sec 0. */
#define HTTP_FETCH_DEFAULT_TIMEOUT_SEC 30

/**
 * Download @p url into @p dest_path.
 *
 * The write is atomic from the caller's point of view: the body goes to
 * "<dest_path>.tmp" and is renamed only after a complete 200 response, so an
 * interrupted download never leaves a half file where a valid one used to be.
 *
 * @param url         absolute http:// or https:// URL
 * @param dest_path   destination file (its directory must exist)
 * @param max_bytes   body cap; 0 means HTTP_FETCH_DEFAULT_MAX_BYTES
 * @param timeout_sec total deadline for the whole transfer; 0 means the default
 * @param ca_file     PEM bundle to trust instead of the system store (or NULL)
 * @param ca_dir      hashed CA directory to trust (or NULL)
 * @param err         optional buffer for a human-readable failure reason
 * @param err_len     size of @p err (0 when @p err is NULL)
 * @return 0 on a complete 2xx body written to @p dest_path, -1 otherwise
 */
int http_fetch_to_file(const char *url, const char *dest_path,
                       size_t max_bytes, int timeout_sec,
                       const char *ca_file, const char *ca_dir,
                       char *err, size_t err_len);

/**
 * What the last transfer actually did, for callers that publish an outcome.
 *
 * Zeroed before the transfer starts, so a failure that never reached a server
 * reports status 0 ("no response") instead of a code from an earlier call.
 * `bytes` is the body length that reached the destination file, so it is 0
 * unless the transfer completed; a 200 whose body then fails its own
 * caller-side check (a sha256 gate, say) still reports the full byte count.
 */
typedef struct {
    int status;      /*: HTTP status of the response that ended the transfer; 0 = none */
    long long bytes; /*: body bytes written to the destination; 0 unless it completed */
} http_fetch_info_t;

/**
 * Same as http_fetch_to_file(), plus the outcome in @p info (which may be
 * NULL). The extra out-parameter is the only difference: same arguments, same
 * return contract, same TLS backends.
 */
int http_fetch_to_file_ex(const char *url, const char *dest_path,
                          size_t max_bytes, int timeout_sec,
                          const char *ca_file, const char *ca_dir,
                          http_fetch_info_t *info,
                          char *err, size_t err_len);

#endif /* HTTP_FETCH_H */
