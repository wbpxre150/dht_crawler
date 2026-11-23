#include "classifier.h"
#include "dht_crawler.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <openssl/sha.h>

#include "clf_sha256.h"

/* ------------------------------------------------------------------------ *
 * Shared between both build flavours (the Android wrapper build defines
 * CLF_ENABLED and links tparse + the CRAM engine; the crawler's own host
 * `make` does not).
 * ------------------------------------------------------------------------ */

/* taxonomy order from the vendored tparse/taxonomy.h: index 0 is
 * TP_CAT_UNKNOWN, so an out-of-range value degrades to "unknown". */
static const char *const CATEGORY_NAMES[CLASSIFIER_NUM_CATEGORIES] = {
    "unknown", "movie", "tv",       "anime", "music", "audiobook",
    "ebook",   "comic", "software", "game",  "image", "other"
};

const char *classifier_category_name(int category) {
    if (category < 0 || category >= CLASSIFIER_NUM_CATEGORIES) {
        return CATEGORY_NAMES[0];
    }
    return CATEGORY_NAMES[category];
}

/* adult_mode is validated by config.c; anything unexpected folds to the default
 * so /stats can never report an unusable mode. */
static const char *clf_normalize_adult_mode(const char *adult_mode) {
    return (adult_mode && strcmp(adult_mode, "drop") == 0) ? "drop" : "tag";
}

/* Shared by both flavours (see include/clf_sha256.h): the CRAM loader verifies
 * cram.bin/cram_vocab.bin through it (the digests are injected as a function
 * pointer, so the primitive stays here, where src/movie_kind.c and the host
 * movie tests already reach it), and src/movie_kind.c verifies the movie
 * backbone and head the same way. Unverified bytes are never loaded.
 *
 * OpenSSL 3 deprecates this incremental SHA256_* API in favour of EVP; the crawler
 * has used it since bep51_cache.c and the Android build maps it onto mbedTLS (where
 * nothing is deprecated), so it stays -- with the warning silenced locally instead of
 * spreading EVP through the tree or a -Wno- flag across the whole build. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
int clf_sha256_file(const char *path, char *out_hex) {
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
        out_hex[i * 2] = hex[digest[i] >> 4];
        out_hex[i * 2 + 1] = hex[digest[i] & 0x0F];
    }
    out_hex[SHA256_DIGEST_LENGTH * 2] = '\0';
    return 0;
}
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#ifdef CLF_ENABLED

#include <cJSON.h>

#include "cram.h"
#include "taxonomy.h"
#include "tparse.h"

/* The plain count in classifier.h must still match the vendored taxonomy: a
 * mismatch fails to compile here (negative array size). CRAM's cat_classes and
 * kind_classes are the same 12-category / 7-kind taxonomy in the same order, so
 * a predicted class index IS a tp_category_t / tp_tv_kind_t value. */
typedef char clf_category_count_matches_taxonomy[(TP_CAT_OTHER + 1 == CLASSIFIER_NUM_CATEGORIES) ? 1 : -1];
typedef char clf_tv_none_is_zero[TP_TV_NONE == 0 ? 1 : -1];
typedef char clf_kind_count_matches_cram[(TP_TV_DAILY + 1 == (int)CRAM_N_KIND) ? 1 : -1];

#define CLF_MODEL_DIR_DEFAULT "models/v15"
#define CLF_FEATURE_CAP 8192
#define CLF_PATH_CAP 1200
/* Evidence cap: tparse sees at most the first 50 files of a torrent, which is
 * the training-time contract (ORDER BY file_index LIMIT 50). The full list
 * still reaches the DB and movie_kind -- the cap is local to this call. */
#define CLF_FILE_CAP 50
/* Only used when neither config nor the manifest names one; the shipped v15
 * manifest always carries the 0.375 adult operating point. */
#define CLF_ADULT_HIDE_DEFAULT 0.5

static struct {
    int enabled;                 /* set by classifier_init() */
    int manifest_ok;             /* manifest.json parsed */
    long long clf_ver;           /* model version string of the manifest */
    double adult_hide_threshold; /* effective tag threshold: config > manifest > default */
    double cfg_hide_threshold;   /* config adult_hide_threshold; 0 = config supplied none */
    double cram_temperature;     /* manifest cram.temperature; category confidence only */
    char model_dir[CLF_PATH_CAP];
    char cram_sha[CLF_SHA_HEX_LEN + 1];
    char cram_vocab_sha[CLF_SHA_HEX_LEN + 1];
    /* The single v15 engine: it scores category, tv_kind AND adult in one pass.
     * NULL means "unavailable" and every classify() degrades to parser-only --
     * there is no second model to fall back to. */
    cram_model_t *cram_model;
    pthread_mutex_t stats_mutex;
    classifier_stats_t stats;
} clf = {
    .adult_hide_threshold = CLF_ADULT_HIDE_DEFAULT,
    .model_dir = CLF_MODEL_DIR_DEFAULT,
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
    /* Overwritten by classifier_set_adult_mode(); "tag" is the config default
     * and keeps /stats sane if it is somehow never called. */
    .stats = { .adult_mode = "tag" },
};

/* ---- small helpers ----------------------------------------------------- */

static uint64_t clf_elapsed_us(const struct timespec *t0, const struct timespec *t1) {
    int64_t ns = (int64_t)(t1->tv_sec - t0->tv_sec) * 1000000000LL +
                 ((int64_t)t1->tv_nsec - (int64_t)t0->tv_nsec);
    if (ns <= 0) return 0;
    return (uint64_t)(ns / 1000);
}

/* Whole manifest as a NUL-terminated string; NULL when unreadable (a missing
 * manifest is the expected state until stage 5.3 ships the models). */
static char *clf_read_text_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long size = ftell(f);
    if (size <= 0 || size > (1024 * 1024)) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *text = (char *)malloc((size_t)size + 1);
    if (!text) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(text, 1, (size_t)size, f);
    fclose(f);
    if (got != (size_t)size) {
        free(text);
        return NULL;
    }
    text[size] = '\0';
    return text;
}

static const cJSON *clf_json_object(const cJSON *parent, const char *key) {
    const cJSON *item = parent ? cJSON_GetObjectItemCaseSensitive(parent, key) : NULL;
    return cJSON_IsObject(item) ? item : NULL;
}

static void clf_manifest_sha(const cJSON *models, const char *model,
                             char *out_hex, size_t cap) {
    out_hex[0] = '\0';
    const cJSON *entry = clf_json_object(models, model);
    const cJSON *sha = entry ? cJSON_GetObjectItemCaseSensitive(entry, "sha256") : NULL;
    if (cJSON_IsString(sha) && strlen(sha->valuestring) == CLF_SHA_HEX_LEN) {
        snprintf(out_hex, cap, "%s", sha->valuestring);
        return;
    }
    log_msg(LOG_WARN, "Classifier: manifest has no sha256 for models.%s", model);
}

/* ---- manifest ---------------------------------------------------------- */

static void clf_load_manifest(void) {
    char path[CLF_PATH_CAP + 32];
    snprintf(path, sizeof(path), "%s/manifest.json", clf.model_dir);

    char *text = clf_read_text_file(path);
    if (!text) {
        log_msg(LOG_WARN, "Classifier: manifest %s not readable; classifying parser-only", path);
        return;
    }

    cJSON *root = cJSON_Parse(text);
    free(text);
    if (!root) {
        log_msg(LOG_WARN, "Classifier: manifest %s is not valid JSON; classifying parser-only", path);
        return;
    }

    clf.manifest_ok = 1;

    /* `version` is the model version string ("v1"): take its digits as the
     * per-row clf_ver recorded on ingest. */
    const cJSON *version = cJSON_GetObjectItemCaseSensitive(root, "version");
    if (cJSON_IsString(version)) {
        const char *p = version->valuestring;
        while (*p && (*p < '0' || *p > '9')) p++;
        if (*p) clf.clf_ver = atoll(p);
    }
    if (clf.clf_ver <= 0) {
        log_msg(LOG_WARN, "Classifier: manifest has no usable version; clf_ver stays 0");
    }

    /* Effective adult hide threshold, most authoritative first: the config key
     * (what the operator wrote in config.ini), then the manifest, then the
     * built-in default. */
    const cJSON *thresholds = clf_json_object(root, "thresholds");
    const cJSON *hide = thresholds
                            ? cJSON_GetObjectItemCaseSensitive(thresholds, "adult_hide_threshold")
                            : NULL;
    double manifest_hide = 0.0;
    if (cJSON_IsNumber(hide) && hide->valuedouble > 0.0 && hide->valuedouble <= 1.0) {
        manifest_hide = hide->valuedouble;
    } else {
        log_msg(LOG_WARN, "Classifier: manifest threshold missing/invalid");
    }

    if (clf.cfg_hide_threshold > 0.0) {
        clf.adult_hide_threshold = clf.cfg_hide_threshold;
        log_msg(LOG_INFO, "Classifier: adult hide threshold %.6f from config (manifest %.6f)",
                clf.adult_hide_threshold, manifest_hide);
    } else if (manifest_hide > 0.0) {
        clf.adult_hide_threshold = manifest_hide;
        log_msg(LOG_INFO, "Classifier: adult hide threshold %.6f from manifest", clf.adult_hide_threshold);
    } else {
        clf.adult_hide_threshold = CLF_ADULT_HIDE_DEFAULT;
        log_msg(LOG_WARN, "Classifier: no usable adult hide threshold; using built-in %.3f",
                clf.adult_hide_threshold);
    }

    /* v15 has ONE adult boundary: the tag threshold resolved above is also the
     * drop boundary, so the manifest's `adult_drop_threshold` is deliberately
     * not read any more. /stats still publishes the key, mirrored from the
     * effective hide threshold (see classifier_init()). */
    const cJSON *cram = clf_json_object(root, "cram");
    const cJSON *temperature = cram ? cJSON_GetObjectItemCaseSensitive(cram, "temperature") : NULL;
    if (cJSON_IsNumber(temperature) && temperature->valuedouble > 0.0) {
        clf.cram_temperature = temperature->valuedouble;
    } else {
        log_msg(LOG_WARN, "Classifier: manifest has no cram.temperature; /stats reports 0");
    }

    const cJSON *models = clf_json_object(root, "models");
    clf_manifest_sha(models, "cram", clf.cram_sha, sizeof(clf.cram_sha));
    clf_manifest_sha(models, "cram_vocab", clf.cram_vocab_sha, sizeof(clf.cram_vocab_sha));

    cJSON_Delete(root);
}

/* ---- model loading ----------------------------------------------------- */

/* Count one unusable-model event. /stats publishes this number twice on
 * purpose: `classifier.model_load_failures` (the operator's counter) and
 * `cram.load_failures` (the engine's own, which also moves inside cram_load()
 * for container-level failures) describe the same single engine now. */
static void clf_note_load_failure(void) {
    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.model_load_failures++;
    pthread_mutex_unlock(&clf.stats_mutex);
}

/* Load <model_dir>/cram.bin + cram_vocab.bin, but only when the manifest pins
 * both sha256s: unverified bytes are never loaded, they only degrade the
 * classification. cram_load() is all-or-nothing and never fatal -- it returns
 * NULL, frees any partial state and moves its own load_failures counter. */
static void clf_load_cram(void) {
    if (!clf.manifest_ok || clf.cram_sha[0] == '\0' || clf.cram_vocab_sha[0] == '\0') {
        log_msg(LOG_WARN, "Classifier: CRAM not loaded (no verified manifest entry); parser-only");
        clf_note_load_failure();
        return;
    }

    /* clf_sha256_file() has exactly the cram_sha256_file_fn signature, so it is
     * passed straight through -- the engine never grows a hashing dependency. */
    const cram_digests_t digests = {
        .sha256_file = clf_sha256_file,
        .cram_sha256 = clf.cram_sha,
        .vocab_sha256 = clf.cram_vocab_sha,
    };
    cram_status_t status = CRAM_OK;
    clf.cram_model = cram_load(clf.model_dir, &digests, &status);
    if (!clf.cram_model) {
        /* One event, published in both places: this counter and cram.load_failures. */
        log_msg(LOG_WARN, "Classifier: CRAM load from %s failed (%s); parser-only",
                clf.model_dir, cram_status_str(status));
        clf_note_load_failure();
        return;
    }

    char cram_sha[CRAM_SHA_CAP], vocab_sha[CRAM_SHA_CAP];
    cram_get_digests(clf.cram_model, cram_sha, vocab_sha);
    log_msg(LOG_INFO, "Classifier: loaded CRAM %s (cram %.16s.., vocab %.16s..)",
            clf.model_dir, cram_sha, vocab_sha);
}

/* ---- public API -------------------------------------------------------- */

void classifier_init(const char *model_dir, double adult_hide_threshold) {
    const char *dir = (model_dir && model_dir[0]) ? model_dir : CLF_MODEL_DIR_DEFAULT;
    snprintf(clf.model_dir, sizeof(clf.model_dir), "%s", dir);

    /* Recorded before the manifest is read: clf_load_manifest() resolves the
     * config > manifest > default precedence. */
    clf.cfg_hide_threshold = (adult_hide_threshold > 0.0 && adult_hide_threshold <= 1.0)
                                 ? adult_hide_threshold
                                 : 0.0;
    /* Provisional: the manifest may replace it, and it is what stands when the
     * manifest never loads (a 0.0 threshold would tag every degraded item). */
    clf.adult_hide_threshold = clf.cfg_hide_threshold > 0.0 ? clf.cfg_hide_threshold
                                                            : CLF_ADULT_HIDE_DEFAULT;

    clf.enabled = 1;
    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.enabled = 1;
    /* Provisional: clf_load_manifest() below may replace the threshold; the
     * resolved value is republished as soon as it returns. */
    clf.stats.adult_hide_threshold = clf.adult_hide_threshold;
    pthread_mutex_unlock(&clf.stats_mutex);

    log_msg(LOG_INFO, "Initializing classifier (model dir: %s)...", clf.model_dir);

    clf_load_manifest();
    /* The manifest may have replaced the threshold (config > manifest >
     * default), and that resolved value is the boundary adult_mode=drop drops
     * on, so /stats has to publish it rather than the config literal.
     *
     * v15 is a single engine, so there is ONE adult boundary: the drop key is
     * published as a mirror of the tag threshold (kept for /stats shape
     * stability; nothing reads it as a second boundary). */
    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.adult_hide_threshold = clf.adult_hide_threshold;
    clf.stats.adult_drop_threshold = clf.adult_hide_threshold;
    /* Provenance: what /stats reports as the live generation and the bytes it
     * was produced from. Copied here, not read live, so a reader never sees a
     * half-populated snapshot. The engine counters are merged in
     * classifier_get_stats(), so one snapshot carries them all. */
    clf.stats.cram_temperature = clf.cram_temperature;
    snprintf(clf.stats.cram_sha256, sizeof clf.stats.cram_sha256, "%s", clf.cram_sha);
    snprintf(clf.stats.cram_vocab_sha256, sizeof clf.stats.cram_vocab_sha256, "%s",
             clf.cram_vocab_sha);
    pthread_mutex_unlock(&clf.stats_mutex);
    if (!clf.manifest_ok) {
        /* Counted once: without the manifest the container can be neither
         * loaded nor verified, and /stats must not report zero failures while
         * every item degrades to parser-only. */
        log_msg(LOG_WARN, "Classifier: no usable model manifest; every torrent is classified parser-only");
        clf_note_load_failure();
        return;
    }

    clf_load_cram();

    log_msg(LOG_INFO, "Classifier ready (clf_ver=%lld, hide=%.3f): CRAM %s",
            clf.clf_ver, clf.adult_hide_threshold,
            clf.cram_model ? "loaded" : "unavailable");
}

/* ---- feature pipeline (shared by both classify entry points) ------------ */

typedef struct {
    tp_result_t parsed;            /* tparse verdict: hints + season/episode/resolution */
    char feature[CLF_FEATURE_CAP]; /* tp_feature_text() output, NUL-terminated */
    int capped;                    /* n_files was clamped to CLF_FILE_CAP */
} clf_features_t;

/*
 * Render the feature text of one torrent: cap the evidence at CLF_FILE_CAP
 * files, run tparse, then tp_feature_text(). This exists ONCE so the ingest
 * path and the adult-only path can never disagree about the cap or the text
 * (a backfilled verdict and an ingest verdict are produced from byte-identical
 * input).
 *
 * @return 0 with *out filled; 1 when tparse rejected the name or the file array
 *         could not be allocated (the caller decides how loud that is).
 */
static int clf_build_features(const torrent_metadata_t *meta, clf_features_t *out) {
    int n_files = (meta->files && meta->num_files > 0) ? meta->num_files : 0;
    memset(&out->parsed, 0, sizeof(out->parsed));
    out->feature[0] = '\0';
    out->capped = 0;
    if (n_files > CLF_FILE_CAP) {
        n_files = CLF_FILE_CAP;
        out->capped = 1;
    }

    /* tparse wants a flat {path, size} view of the file list; it copies
     * nothing, so the array only has to outlive the calls below. */
    tp_file_t *files = NULL;
    if (n_files > 0) {
        files = (tp_file_t *)malloc((size_t)n_files * sizeof(tp_file_t));
        if (!files) {
            log_msg(LOG_WARN, "Classifier: out of memory for %d files", n_files);
            return 1;
        }
        for (int i = 0; i < n_files; i++) {
            files[i].path = meta->files[i].path ? meta->files[i].path : "";
            files[i].size = meta->files[i].size_bytes;
        }
    }

    const char *name = meta->name ? meta->name : "";
    if (tp_parse(name, files, n_files, meta->size_bytes, &out->parsed) != TP_OK) {
        free(files);
        return 1;
    }

    /* Per-call buffer: several metadata workers run this at once, and
     * tp_feature_text() keeps no state of its own. Ask for the length first so
     * only an oversized input is truncated at the cap. */
    size_t need = tp_feature_text(name, files, n_files, &out->parsed, NULL, 0);
    size_t cap = sizeof(out->feature);
    if (need + 1 < cap) cap = need + 1;
    tp_feature_text(name, files, n_files, &out->parsed, out->feature, cap);
    out->feature[cap - 1] = '\0';

    /* Serving-side canonicalisation (plan assumption 13, Revision 1): the frozen
     * v15 model's training corpus carries `__adult_*` on 20 of 270,390 rows while
     * tparse renders them unconditionally, so the CRAM path drops them here --
     * after the render (the text itself is unchanged) and before any consumer, so
     * `classifier_classify()` and `classifier_classify_adult()` cannot disagree
     * about the input. `src/adult_backfill.c` scans the RAW text with its own
     * gate and is deliberately not routed through this. */
    cram_strip_adult_tokens(out->feature);
    free(files);
    return 0;
}

int classifier_classify(const torrent_metadata_t *meta, classification_t *out) {
    if (!clf.enabled || !meta || !out) return 1;

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* Capped, tparse'd feature evidence. Shared with the adult-only entry point
     * so the 50-file cap and the feature contract exist once. */
    clf_features_t feats;
    if (clf_build_features(meta, &feats) != 0) {
        log_msg(LOG_WARN, "Classifier: tparse rejected '%s'; no classification",
                meta->name ? meta->name : "");
        return 1;
    }

    /* One forward pass decides all three fields; the parser only supplies the
     * season/episode/resolution fields and the hints this path falls back on. */
    int category = feats.parsed.category_hint;
    int tv_kind = feats.parsed.tv_kind;
    int adult = 0;
    double adult_p = 0.0;
    double confidence = 0.0;
    long long clf_ver = 0;
    int degraded = 0;
    int kind_head_used = 0;

    if (clf.cram_model) {
        cram_row_t row;
        cram_verdict_t verdict;
        memset(&row, 0, sizeof(row));
        memset(&verdict, 0, sizeof(verdict));
        if (cram_features(clf.cram_model, feats.feature, &row) == CRAM_OK &&
            cram_predict(clf.cram_model, &row, clf.adult_hide_threshold, &verdict) == CRAM_OK) {
            /* The masked 48-state argmin IS the category model (the bare 12-way
             * unary is ~9 pp worse); the engine already applied it, so this is
             * neither the parser hint nor the bare unary. */
            category = verdict.category;
            confidence = verdict.confidence; /* T-tempered category marginal */
            adult_p = verdict.adult_p;       /* RAW marginal: no temperature */
            adult = verdict.adult;           /* adult_p >= the effective threshold */
            /* CRAM fills `kind` on every prediction; "a concrete kind only on
             * tv/anime" is a caller rule, so it is enforced here -- and with no
             * parser gate: the head decides every tv/anime row. */
            tv_kind = (category == TP_CAT_TV || category == TP_CAT_ANIME) ? verdict.kind
                                                                         : TP_TV_NONE;
            kind_head_used = 1;
            clf_ver = clf.clf_ver;
        } else {
            degraded = 1; /* one unusable row: parser-only, not a load failure */
        }
    } else {
        degraded = 1;
    }

    out->category = category;
    out->tv_kind = tv_kind;
    out->adult = adult;
    out->adult_p = adult_p;
    out->season_lo = feats.parsed.season_lo;
    out->season_hi = feats.parsed.season_hi;
    out->episode_lo = feats.parsed.episode_lo;
    out->episode_hi = feats.parsed.episode_hi;
    out->resolution = feats.parsed.resolution;
    out->confidence = confidence;
    /* 0 on the degraded path: the row must never claim a generation the engine
     * did not run (classifier_meta is gated the same way). */
    out->clf_ver = clf_ver;

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.total_classified++;
    clf.stats.total_us += clf_elapsed_us(&t_start, &t_end);
    /* adult_tagged is NOT bumped here: only rows the caller actually stores
     * with adult = 1 count (classifier_note_stored_adult()), so a dropped
     * torrent never inflates it. */
    clf.stats.parser_only_fallback += (uint64_t)(degraded ? 1 : 0);
    /* Same key as the fastText era, new meaning: "the kind head was consulted"
     * (CRAM consults it on every successful prediction; the caller applies the
     * verdict only on tv/anime rows). */
    clf.stats.tv_kind_model_used += (uint64_t)kind_head_used;
    clf.stats.file_cap_hits += (uint64_t)(feats.capped ? 1 : 0);
    if (category >= 0 && category < CLASSIFIER_NUM_CATEGORIES) {
        clf.stats.by_category[category]++;
    }
    pthread_mutex_unlock(&clf.stats_mutex);

    return 0;
}

/* Adult-only scoring, the token-gated backfill's entry point. It runs the SAME
 * capped-50 -> tparse -> feature text -> CRAM forward pass as the ingest path
 * (clf_build_features() is shared), so a backfilled verdict and an ingest
 * verdict are produced from byte-identical input and cannot disagree about the
 * evidence or the boundary; only the fields returned differ. */
int classifier_classify_adult(const torrent_metadata_t *meta, int *adult, double *adult_p) {
    if (adult) *adult = 0;
    if (adult_p) *adult_p = 0.0;
    if (!clf.enabled || !meta) return 1;
    if (!clf.cram_model) return 1; /* no engine: no verdict, leave the row alone */

    clf_features_t feats;
    if (clf_build_features(meta, &feats) != 0) {
        log_msg(LOG_WARN, "Classifier: tparse rejected '%s'; no adult verdict",
                meta->name ? meta->name : "");
        return 1;
    }

    cram_row_t row;
    cram_verdict_t verdict;
    memset(&row, 0, sizeof(row));
    memset(&verdict, 0, sizeof(verdict));
    if (cram_features(clf.cram_model, feats.feature, &row) != CRAM_OK ||
        cram_predict(clf.cram_model, &row, clf.adult_hide_threshold, &verdict) != CRAM_OK) {
        /* No prediction at all: report "no verdict" rather than a fabricated
         * adult = 0, which would silently rewrite a good stored row. */
        return 1;
    }

    double p = verdict.adult_p;
    if (p < 0.0) p = 0.0;
    if (p > 1.0) p = 1.0;
    if (adult_p) *adult_p = p;
    if (adult) *adult = verdict.adult; /* same >= boundary as the ingest path */
    /* Deliberately no stats bump: these are backfill rows, not ingest rows, and
     * folding them into total_classified would distort mean_us_per_item (which
     * measures the full ingest call). */
    return 0;
}
void classifier_set_adult_mode(const char *adult_mode) {
    /* Race-free by contract: main.c calls this once, before any worker thread
     * exists. The stats mutex keeps it on the same publish/copy path as every
     * other field classifier_get_stats() hands out. */
    pthread_mutex_lock(&clf.stats_mutex);
    snprintf(clf.stats.adult_mode, sizeof(clf.stats.adult_mode), "%s",
             clf_normalize_adult_mode(adult_mode));
    pthread_mutex_unlock(&clf.stats_mutex);
}

void classifier_get_stats(classifier_stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&clf.stats_mutex);
    *out = clf.stats;
    pthread_mutex_unlock(&clf.stats_mutex);

    /* The engine counters live in the CRAM module (it owns load_failures, so a
     * failed load is still visible), and they are merged here -- at snapshot
     * time, from one source -- instead of being re-published per item. */
    cram_stats_t cram_stats;
    cram_get_stats(clf.cram_model, &cram_stats);
    out->cram_enabled = clf.cram_model ? 1 : 0;
    out->cram_rows_predicted = cram_stats.rows_predicted;
    out->cram_load_failures = cram_stats.load_failures;
    out->cram_kind_head_used = cram_stats.kind_head_used;
    out->cram_adult_tagged_predictions = cram_stats.adult_tagged_predictions;
    out->cram_feature_cap_hits = cram_stats.feature_cap_hits;
}

int classifier_models_loaded(void) {
    /* Read without the stats mutex: both fields are set once by
     * classifier_init() before any worker thread exists. One engine now, so
     * "loaded" means CRAM is loaded and usable -- it owns adult tagging too. */
    return (clf.enabled && clf.cram_model) ? 1 : 0;
}

/* Set once by classifier_init() before any other thread exists, so it is read
 * without the stats mutex like the fields classifier_models_loaded() uses. */
int classifier_enabled(void) {
    return clf.enabled ? 1 : 0;
}

/* clf.clf_ver is fixed once clf_load_manifest() ran (once, from init). */
long long classifier_current_version(void) {
    return clf.clf_ver;
}

void classifier_note_stored_adult(void) {
    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.adult_tagged++;
    pthread_mutex_unlock(&clf.stats_mutex);
}

void classifier_note_adult_dropped(void) {
    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.adult_dropped++;
    pthread_mutex_unlock(&clf.stats_mutex);
}

void classifier_note_hard_block_dropped(void) {
    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.hard_block_dropped++;
    pthread_mutex_unlock(&clf.stats_mutex);
}

static void clf_meta_put(sqlite3 *db, const char *key, const char *value) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db,
                           "INSERT OR REPLACE INTO classifier_meta (key, value) VALUES (?, ?)",
                           -1, &stmt, NULL) != SQLITE_OK) {
        log_msg(LOG_DEBUG, "Classifier: classifier_meta unavailable for '%s': %s", key,
                sqlite3_errmsg(db));
        return;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        log_msg(LOG_DEBUG, "Classifier: classifier_meta write of '%s' failed: %s", key,
                sqlite3_errmsg(db));
    }
    sqlite3_finalize(stmt);
}

void classifier_write_meta(sqlite3 *db) {
    /* The manifest_ok gate is kept and widened to the engine: with one engine,
     * "manifest parsed but CRAM unusable" is just as degraded, and a meta row
     * claiming generation 15 while every ingest row stamps clf_ver 0 is exactly
     * the inconsistency this gate exists to prevent. */
    if (!db || !clf.enabled || !clf.manifest_ok || !clf.cram_model) return;

    char buf[64];
    snprintf(buf, sizeof(buf), "%lld", clf.clf_ver);
    clf_meta_put(db, "clf_ver", buf);
    /* Single engine: the adult generation IS the category/tv_kind generation.
     * Additive key -- rows written before v15 simply do not carry it. */
    clf_meta_put(db, "adult_clf_ver", buf);
    if (clf.cram_sha[0]) clf_meta_put(db, "cram_sha256", clf.cram_sha);
    if (clf.cram_vocab_sha[0]) clf_meta_put(db, "cram_vocab_sha256", clf.cram_vocab_sha);
    snprintf(buf, sizeof(buf), "%.6f", clf.adult_hide_threshold);
    clf_meta_put(db, "adult_hide_threshold", buf);

    log_msg(LOG_DEBUG, "Classifier: classifier_meta updated (clf_ver=%lld)", clf.clf_ver);
}

#else /* !CLF_ENABLED */

/*
 * Host build (`make`, plain gcc, no tparse/CRAM and no tparse include path):
 * the classifier is compiled out. tree_metadata.c still calls in, gets 1 back
 * and writes no torrent_class rows; /stats renders enabled:false and zeros.
 */
static struct {
    pthread_mutex_t stats_mutex;
    classifier_stats_t stats;
} clf = {
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
    /* Overwritten by classifier_set_adult_mode(); "tag" is the config default
     * and keeps /stats sane if it is somehow never called. */
    .stats = { .adult_mode = "tag" },
};

void classifier_init(const char *model_dir, double adult_hide_threshold) {
    (void)model_dir;
    (void)adult_hide_threshold;
    log_msg(LOG_DEBUG, "Classifier: built without CLF_ENABLED; classification disabled");
}

/* No models exist in this flavour, so no ingest-time classification happens and
 * every ingest-outcome counter stays zero. */
int classifier_models_loaded(void) {
    return 0;
}

/* This flavour never classifies. */
int classifier_enabled(void) {
    return 0;
}

long long classifier_current_version(void) {
    return 0;
}

void classifier_note_stored_adult(void) {}
void classifier_note_adult_dropped(void) {}
void classifier_note_hard_block_dropped(void) {}

int classifier_classify(const torrent_metadata_t *meta, classification_t *out) {
    (void)meta;
    (void)out;
    return 1;
}

/* This flavour has no models and no tparse, so it never produces an adult
 * verdict; every adult-backfill run reports DISABLED before it gets here. */
int classifier_classify_adult(const torrent_metadata_t *meta, int *adult, double *adult_p) {
    (void)meta;
    if (adult) *adult = 0;
    if (adult_p) *adult_p = 0.0;
    return 1;
}
void classifier_set_adult_mode(const char *adult_mode) {
    /* Race-free by contract: main.c calls this once, before any worker thread
     * exists. The stats mutex keeps it on the same publish/copy path as every
     * other field classifier_get_stats() hands out. */
    pthread_mutex_lock(&clf.stats_mutex);
    snprintf(clf.stats.adult_mode, sizeof(clf.stats.adult_mode), "%s",
             clf_normalize_adult_mode(adult_mode));
    pthread_mutex_unlock(&clf.stats_mutex);
}

void classifier_get_stats(classifier_stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&clf.stats_mutex);
    *out = clf.stats;
    pthread_mutex_unlock(&clf.stats_mutex);
}

void classifier_write_meta(sqlite3 *db) {
    (void)db;
}

#endif /* CLF_ENABLED */
