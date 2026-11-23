/**
 * src/model_fetch.c — `--fetch-models`: the crawler downloads its own weights.
 *
 * Why this exists: the Android app used to ship ~490 MB of classifier weights
 * inside its APK. They are static objects with published sha256s, so the
 * daemon can fetch them into ./models itself and the APK only has to carry the
 * crawler.
 *
 * The trust chain is two pins, not six. Each generation's manifest.json is
 * pinned in this binary (MODEL_FETCH_PINS) and is the root of trust; every other
 * file is verified against the sha256 the manifest declares for it. A manifest
 * that does not match its pin is refused outright - it never gets to name the
 * files we download.
 *
 * Cost when everything is already present: two manifest sha256s plus one
 * sha256 per model file, no network at all. That is what makes it cheap enough
 * to call on every process start (the Android app does).
 *
 * Nothing here is TLS code: the transfers ride src/http_fetch.c, which already
 * has both backends (OpenSSL on the host, mbedTLS under HTTP_FETCH_MBEDTLS for
 * the NDK build).
 */

#include "model_fetch.h"

#include "http_fetch.h"
#include "cJSON.h"

#include <errno.h>
#include <openssl/sha.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/*: Longest path this module builds (dir + generation + file name). */
#define MF_PATH_CAP 1024
/*: A manifest is a few KB; anything near this is not a manifest. */
#define MF_MANIFEST_CAP (1024 * 1024)

/**
 * The root of trust: two sha256s, one per shipped generation.
 *
 * Both were confirmed against the published objects (and both match the copies
 * committed under tests/fixtures/cram/models/v15 and the app's models/v13), so a
 * manifest edit upstream is a deliberate re-pin here, never a silent change.
 * Publishing a new generation means adding an entry - no other code changes.
 */
const model_fetch_pin_t MODEL_FETCH_PINS[] = {
    { "v15", "59e39d3f85db21e015d94ffd7f5b055d0b593f69c238ac18f39c30bf50d53e55" },
    { "v13", "e047464314e40bb70e7fc16205297878981d4252349d96ef3830f0260757fb57" },
    { NULL, NULL }
};

/* ---- small helpers ------------------------------------------------------ */

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
/* Same primitive the two model loaders gate on (include/clf_sha256.h): the
 * incremental SHA256_* API, which the Android build maps onto mbedTLS. */
static int mf_sha256_file(const char *path, char out[MODEL_FETCH_SHA_CAP]) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    SHA256_CTX ctx;
    if (!SHA256_Init(&ctx)) {
        fclose(f);
        return -1;
    }

    unsigned char buf[65536];
    size_t n;
    int ok = 1;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        if (!SHA256_Update(&ctx, buf, n)) {
            ok = 0;
            break;
        }
    }
    if (ferror(f)) ok = 0;

    unsigned char digest[SHA256_DIGEST_LENGTH];
    if (!SHA256_Final(digest, &ctx)) ok = 0;
    fclose(f);
    if (!ok) return -1;

    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0x0F];
    }
    out[SHA256_DIGEST_LENGTH * 2] = '\0';
    return 0;
}
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

/* 1 when @p path exists and hashes to @p want (a 64-hex sha256). */
static int mf_sha_matches(const char *path, const char *want) {
    if (!path || !want || strlen(want) != 64) return 0;
    char actual[MODEL_FETCH_SHA_CAP];
    if (mf_sha256_file(path, actual) != 0) return 0;
    return strcmp(actual, want) == 0;
}

/* Read a small text file into a malloc'd NUL-terminated buffer, or NULL. */
static char *mf_read_text(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long size = ftell(f);
    if (size < 0 || size > MF_MANIFEST_CAP) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *buf = malloc((size_t)size + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)size, f);
    fclose(f);
    if (got != (size_t)size) {
        free(buf);
        return NULL;
    }
    buf[got] = '\0';
    return buf;
}

/* mkdir -p, because <dir>/<gen> may be two levels deep and neither may exist. */
static int mf_mkdir_p(const char *path) {
    char buf[MF_PATH_CAP];
    if (!path || !*path || strlen(path) >= sizeof(buf)) return -1;
    snprintf(buf, sizeof(buf), "%s", path);
    for (char *p = buf + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(buf, 0777) != 0 && errno != EEXIST) return -1;
        *p = '/';
    }
    if (mkdir(buf, 0777) != 0 && errno != EEXIST) return -1;
    return 0;
}

/* Join "<dir>/<leaf>" into @p out. */
static int mf_join(char *out, size_t out_len, const char *dir, const char *leaf) {
    int n = snprintf(out, out_len, "%s/%s", dir, leaf);
    return (n < 0 || (size_t)n >= out_len) ? -1 : 0;
}

/* Join "<base>/<generation>/<leaf>" into @p out. */
static int mf_join3(char *out, size_t out_len, const char *base, const char *generation,
                    const char *leaf) {
    int n = snprintf(out, out_len, "%s/%s/%s", base, generation, leaf);
    return (n < 0 || (size_t)n >= out_len) ? -1 : 0;
}

static void mf_log(FILE *log, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(log, fmt, ap);
    va_end(ap);
}

/* ---- manifest parsing --------------------------------------------------- */

/* Pull one {file, sha256} spec out of an object, if it carries both. */
static int mf_read_spec(const cJSON *node, char files[][MODEL_FETCH_NAME_CAP],
                        char shas[][MODEL_FETCH_SHA_CAP], int max_files, int count) {
    if (!cJSON_IsObject(node) || count >= max_files) return count;

    const cJSON *file = cJSON_GetObjectItemCaseSensitive(node, "file");
    const cJSON *sha = cJSON_GetObjectItemCaseSensitive(node, "sha256");
    if (!cJSON_IsString(file) || !file->valuestring[0] || strlen(file->valuestring) >= MODEL_FETCH_NAME_CAP)
        return count;
    if (!cJSON_IsString(sha) || strlen(sha->valuestring) != 64)
        return count;

    snprintf(files[count], MODEL_FETCH_NAME_CAP, "%s", file->valuestring);
    snprintf(shas[count], MODEL_FETCH_SHA_CAP, "%s", sha->valuestring);
    return count + 1;
}

int model_fetch_parse_manifest(const char *json,
                               char files[][MODEL_FETCH_NAME_CAP],
                               char shas[][MODEL_FETCH_SHA_CAP],
                               int max_files) {
    if (!json || !files || !shas || max_files <= 0) return -1;

    cJSON *root = cJSON_Parse(json);
    if (!root) return -1;

    int count = 0;

    /* CRAM: models.<name> holds one spec each. */
    const cJSON *models = cJSON_GetObjectItemCaseSensitive(root, "models");
    if (cJSON_IsObject(models)) {
        for (const cJSON *entry = models->child; entry; entry = entry->next)
            count = mf_read_spec(entry, files, shas, max_files, count);
    }

    /* movie-kind: the two specs are top-level objects. */
    count = mf_read_spec(cJSON_GetObjectItemCaseSensitive(root, "backbone"),
                         files, shas, max_files, count);
    count = mf_read_spec(cJSON_GetObjectItemCaseSensitive(root, "head"),
                         files, shas, max_files, count);

    cJSON_Delete(root);
    return count;
}

/* ---- one generation ----------------------------------------------------- */

/* Download @p url to @p dest (which the caller removes on failure). */
static int mf_download(const char *url, const char *dest,
                       const model_fetch_options_t *opts, FILE *log) {
    http_fetch_info_t info;
    char err[256] = {0};
    int timeout = opts->timeout_sec > 0 ? opts->timeout_sec : MODEL_FETCH_DEFAULT_TIMEOUT_SEC;

    if (http_fetch_to_file_ex(url, dest, MODEL_FETCH_MAX_BYTES, timeout,
                              opts->ca_file, opts->ca_dir, &info, err, sizeof(err)) != 0) {
        mf_log(log, "fetch-models: download failed: %s (%s)\n", url,
               err[0] ? err : "no reason given");
        return -1;
    }
    return 0;
}

/**
 * Fetch and verify one generation.
 *
 * Order matters: the manifest is validated first (it is the root of trust and
 * names every other sha), each model file is renamed into place only after it
 * verifies, and the manifest itself is renamed last. So a generation whose
 * manifest is present is a generation whose files all verified, and an
 * interrupted run leaves a tree the next run will notice and repair.
 */
static int mf_fetch_generation(const char *base, const char *dir,
                               const model_fetch_pin_t *pin,
                               const model_fetch_options_t *opts, FILE *log,
                               int *out_downloaded, long long *out_bytes) {
    char gen_dir[MF_PATH_CAP];
    char manifest[MF_PATH_CAP];
    char manifest_tmp[MF_PATH_CAP];
    char url[MF_PATH_CAP];

    if (mf_join(gen_dir, sizeof(gen_dir), dir, pin->generation) != 0) return -1;
    if (mf_join(manifest, sizeof(manifest), gen_dir, "manifest.json") != 0) return -1;
    int n = snprintf(manifest_tmp, sizeof(manifest_tmp), "%s.tmp", manifest);
    if (n < 0 || (size_t)n >= sizeof(manifest_tmp)) return -1;

    if (mf_mkdir_p(gen_dir) != 0) {
        mf_log(log, "fetch-models: %s: cannot create %s: %s\n",
               pin->generation, gen_dir, strerror(errno));
        return -1;
    }

    /* A manifest that already matches its pin is trusted as-is: no network. */
    int have_manifest = 0;
    char *text = NULL;
    if (!opts->force && mf_sha_matches(manifest, pin->manifest_sha256)) {
        text = mf_read_text(manifest);
        if (text) have_manifest = 1;
    }

    if (!have_manifest) {
        if (mf_join3(url, sizeof(url), base, pin->generation, "manifest.json") != 0) return -1;

        mf_log(log, "fetch-models: %s: fetching manifest.json\n", pin->generation);
        if (mf_download(url, manifest_tmp, opts, log) != 0) return -1;

        /* The pin is checked BEFORE the body is parsed: a manifest that is not
         * the one this binary trusts must not get to name any file. */
        if (!mf_sha_matches(manifest_tmp, pin->manifest_sha256)) {
            char actual[MODEL_FETCH_SHA_CAP];
            if (mf_sha256_file(manifest_tmp, actual) != 0)
                snprintf(actual, sizeof(actual), "unreadable");
            mf_log(log,
                   "fetch-models: %s: manifest.json does not match its pinned sha256 "
                   "(expected %s, got %s); refusing to trust it\n",
                   pin->generation, pin->manifest_sha256, actual);
            unlink(manifest_tmp);
            return -1;
        }
        text = mf_read_text(manifest_tmp);
        if (!text) {
            mf_log(log, "fetch-models: %s: cannot read the downloaded manifest\n", pin->generation);
            unlink(manifest_tmp);
            return -1;
        }
    }

    char files[MODEL_FETCH_MAX_FILES][MODEL_FETCH_NAME_CAP];
    char shas[MODEL_FETCH_MAX_FILES][MODEL_FETCH_SHA_CAP];
    int specs = model_fetch_parse_manifest(text, files, shas, MODEL_FETCH_MAX_FILES);
    free(text);
    if (specs <= 0) {
        mf_log(log, "fetch-models: %s: manifest declares no model files\n", pin->generation);
        if (!have_manifest) unlink(manifest_tmp);
        return -1;
    }

    int downloaded = 0;
    long long bytes = 0;
    int failed = 0;

    for (int i = 0; i < specs && !failed; i++) {
        char dest[MF_PATH_CAP];
        char dest_tmp[MF_PATH_CAP];
        if (mf_join(dest, sizeof(dest), gen_dir, files[i]) != 0) {
            failed = 1;
            break;
        }
        if (!opts->force && mf_sha_matches(dest, shas[i])) {
            mf_log(log, "  ok    %s/%s (already verified)\n", pin->generation, files[i]);
            continue;
        }

        int m = snprintf(dest_tmp, sizeof(dest_tmp), "%s.tmp", dest);
        if (m < 0 || (size_t)m >= sizeof(dest_tmp)) {
            failed = 1;
            break;
        }
        mf_log(log, "  get   %s/%s\n", pin->generation, files[i]);

        if (mf_join3(url, sizeof(url), base, pin->generation, files[i]) != 0) {
            failed = 1;
            break;
        }
        if (mf_download(url, dest_tmp, opts, log) != 0) {
            unlink(dest_tmp);
            failed = 1;
            break;
        }
        if (!mf_sha_matches(dest_tmp, shas[i])) {
            char actual[MODEL_FETCH_SHA_CAP];
            if (mf_sha256_file(dest_tmp, actual) != 0)
                snprintf(actual, sizeof(actual), "unreadable");
            mf_log(log,
                   "fetch-models: %s: %s does not match the manifest sha256 "
                   "(expected %s, got %s); not installing it\n",
                   pin->generation, files[i], shas[i], actual);
            unlink(dest_tmp);
            failed = 1;
            break;
        }
        if (rename(dest_tmp, dest) != 0) {
            mf_log(log, "fetch-models: %s: cannot install %s: %s\n",
                   pin->generation, dest, strerror(errno));
            unlink(dest_tmp);
            failed = 1;
            break;
        }
        downloaded++;
        struct stat st;
        if (stat(dest, &st) == 0) bytes += (long long)st.st_size;
    }

    if (failed) {
        if (!have_manifest) unlink(manifest_tmp);
        return -1;
    }

    if (!have_manifest) {
        if (rename(manifest_tmp, manifest) != 0) {
            mf_log(log, "fetch-models: %s: cannot install %s: %s\n",
                   pin->generation, manifest, strerror(errno));
            unlink(manifest_tmp);
            return -1;
        }
    }

    if (out_downloaded) *out_downloaded += downloaded;
    if (out_bytes) *out_bytes += bytes;
    mf_log(log, "fetch-models: %s: %d file(s) verified, %d downloaded, %lld bytes\n",
           pin->generation, specs, downloaded, bytes);
    return 0;
}

/* ---- entry point -------------------------------------------------------- */

static int mf_generation_requested(const model_fetch_options_t *opts, const char *generation) {
    if (!opts->generations || opts->generation_count <= 0) return 1;
    for (int i = 0; i < opts->generation_count; i++) {
        if (opts->generations[i] && strcmp(opts->generations[i], generation) == 0) return 1;
    }
    return 0;
}

int model_fetch_run(const model_fetch_options_t *opts) {
    model_fetch_options_t defaults;
    if (!opts) {
        memset(&defaults, 0, sizeof(defaults));
        opts = &defaults;
    }

    const char *base = (opts->base && *opts->base) ? opts->base : MODEL_FETCH_DEFAULT_BASE;
    const char *dir = (opts->dir && *opts->dir) ? opts->dir : MODEL_FETCH_DEFAULT_DIR;
    FILE *log = opts->log ? opts->log : stderr;

    int downloaded = 0;
    long long bytes = 0;
    int failures = 0;
    int generations = 0;

    for (const model_fetch_pin_t *pin = MODEL_FETCH_PINS; pin->generation; pin++) {
        if (!mf_generation_requested(opts, pin->generation)) continue;
        generations++;
        if (mf_fetch_generation(base, dir, pin, opts, log, &downloaded, &bytes) != 0)
            failures++;
    }

    if (generations == 0) {
        mf_log(log, "fetch-models: no generation selected\n");
        return -1;
    }

    if (failures != 0) {
        mf_log(log, "fetch-models: %d of %d generation(s) failed under %s\n",
               failures, generations, dir);
        return -1;
    }

    mf_log(log, "fetch-models: %d generation(s) verified under %s (%d file(s) downloaded, %lld bytes)\n",
           generations, dir, downloaded, bytes);
    return 0;
}
