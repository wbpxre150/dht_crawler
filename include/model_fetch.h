#ifndef MODEL_FETCH_H
#define MODEL_FETCH_H

#include <stddef.h>
#include <stdio.h>

/**
 * `--fetch-models`: the crawler downloads its own classifier weights.
 *
 * Two generations ship side by side and are fetched into the directories the
 * compiled-in defaults already point at (`classifier_model_dir` models/v15,
 * `movie_kind_model_dir` models/v13), so a plain `./dht_crawler` picks them up
 * with no config change:
 *
 *   v15  CRAM        cram.bin, cram_vocab.bin, manifest.json
 *   v13  movie-kind  movie_kind_backbone_Q4_0.gguf, movie_kind_head.bin,
 *                    manifest.json
 *
 * TRUST CHAIN. Each generation's manifest.json is the root of trust and its
 * sha256 is pinned in this binary (MODEL_FETCH_PINS) - exactly two pins, not
 * six. Everything else is verified against the sha256 values parsed out of that
 * manifest, so publishing a new generation needs a new pin here and nothing
 * else. A manifest whose own sha does not match its pin is refused before a
 * single byte of it is parsed as instructions.
 *
 * A file is downloaded only when it is missing or its sha256 does not match,
 * and it is renamed into place only after it verifies, so an interrupted
 * download never leaves a corrupt model where a valid one used to be - and the
 * manifest is renamed last, so a generation whose manifest is present is a
 * generation whose files all verified.
 *
 * Nothing here is Android-specific and nothing new is TLS: the transfers reuse
 * src/http_fetch.c, which already carries both backends (OpenSSL on the host,
 * mbedTLS under HTTP_FETCH_MBEDTLS for the NDK build).
 */

/*: Object prefix the generations live under: <base>/<gen>/<file>. */
#define MODEL_FETCH_DEFAULT_BASE "https://unjumble.au/bep51/models"
/*: Model root; the compiled-in model dirs are <root>/v15 and <root>/v13. */
#define MODEL_FETCH_DEFAULT_DIR "models"
/*: Per-file transfer deadline. The backbone is ~478 MiB, so the HTTP default
 *: of 30 s would never finish it. */
#define MODEL_FETCH_DEFAULT_TIMEOUT_SEC 1800
/*: Per-file body cap: 2 GiB, well above the largest shipped file (~478 MiB)
 *: and still a hard stop against a mis-served HTML error page. */
#define MODEL_FETCH_MAX_BYTES ((size_t)2048 * 1024 * 1024)

/*: Longest file name accepted from a manifest. */
#define MODEL_FETCH_NAME_CAP 128
/*: A sha256 in hex plus its terminator. */
#define MODEL_FETCH_SHA_CAP 65
/*: Files one generation may pin (2 today; headroom for a manifest change). */
#define MODEL_FETCH_MAX_FILES 8

/** One pinned generation: the manifest is the root of trust. */
typedef struct {
    const char *generation;      /*: "v15" */
    const char *manifest_sha256; /*: sha256 of <base>/<gen>/manifest.json */
} model_fetch_pin_t;

/** The pins, terminated by an entry whose `generation` is NULL. */
extern const model_fetch_pin_t MODEL_FETCH_PINS[];

/** Knobs for one run; every pointer may be NULL/0 for its default. */
typedef struct {
    const char *base;               /*: NULL = MODEL_FETCH_DEFAULT_BASE */
    const char *dir;                /*: NULL = MODEL_FETCH_DEFAULT_DIR */
    int force;                      /*: re-download even when the file verifies */
    int timeout_sec;                /*: per-file deadline; 0 = default */
    const char *ca_file;            /*: NULL = the system trust store */
    const char *ca_dir;             /*: NULL = none */
    FILE *log;                      /*: progress sink; NULL = stderr */
    /* Which generations to fetch: NULL means every pin. The CLI always wants
     * all of them; the host test narrows the set so it never needs 490 MB. */
    const char *const *generations;
    int generation_count;
} model_fetch_options_t;

/**
 * Collect the {file, sha256} specs a manifest declares.
 *
 * The specs live in `models.<name>` (v15/CRAM) or in the top-level `backbone`
 * and `head` objects (v13/movie-kind), so the walk is limited to those: v13's
 * provenance also carries a `tokenizer.file` + `.sha256` pair for a file the
 * generation does not ship, and a blind walk would try to download it.
 *
 * @param json       manifest text
 * @param files      out: file names, MODEL_FETCH_NAME_CAP each
 * @param shas       out: sha256 hex, MODEL_FETCH_SHA_CAP each
 * @param max_files  capacity of both arrays
 * @return the number of specs found, or -1 when the text is not valid JSON
 */
int model_fetch_parse_manifest(const char *json,
                               char files[][MODEL_FETCH_NAME_CAP],
                               char shas[][MODEL_FETCH_SHA_CAP],
                               int max_files);

/**
 * Fetch and verify every requested generation.
 *
 * @return 0 when every generation is present and verified (including the
 *         already-present no-op case), -1 on any failure.
 */
int model_fetch_run(const model_fetch_options_t *opts);

#endif /* MODEL_FETCH_H */
