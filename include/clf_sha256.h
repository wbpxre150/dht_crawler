#ifndef CLF_SHA256_H
#define CLF_SHA256_H

/*
 * `clf_sha256_file()` — the one file-hash helper both model loaders gate on.
 *
 * Lifted out of `src/classifier.c`'s `#ifdef CLF_ENABLED` region (movie-kind
 * Stage B) so the fastText loader and `src/movie_kind.c` verify a pinned
 * `manifest.json` entry with the *same* code: an unverified file is never
 * loaded, in either engine. Lives in this small header rather than
 * `classifier.h` because the movie-kind module must not pull `database.h`
 * (and therefore sqlite3) into its manifest path.
 *
 * `<openssl/sha.h>` is real OpenSSL on the host and the mbedTLS shim at
 * `dht_crawler-android/compat/openssl/sha.h` on Android; both are linked into
 * the crawler executable in every flavour, so this helper is unconditional.
 */

/*: Lowercase hex length of a SHA-256 digest. */
#define CLF_SHA_HEX_LEN 64

/*
 * Hex-encode the SHA-256 of the whole file at `path` into `out_hex`, which must
 * hold CLF_SHA_HEX_LEN + 1 bytes.
 *
 * @return 0 on success, -1 when the file cannot be opened/read or the digest
 *         fails (in which case `out_hex` is left untouched).
 */
int clf_sha256_file(const char *path, char *out_hex);

#endif /* CLF_SHA256_H */
