#include "classifier.h"
#include "dht_crawler.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------------ *
 * Shared between both build flavours (the Android wrapper build defines
 * CLF_ENABLED and links tparse/ftc; the crawler's own host `make` does not).
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

#ifdef CLF_ENABLED

#include <cJSON.h>
#include <openssl/sha.h>

#include "ftc.h"
#include "taxonomy.h"
#include "tparse.h"

/* The plain count in classifier.h must still match the vendored taxonomy: a
 * mismatch fails to compile here (negative array size). */
typedef char clf_category_count_matches_taxonomy[(TP_CAT_OTHER + 1 == CLASSIFIER_NUM_CATEGORIES) ? 1 : -1];
typedef char clf_tv_none_is_zero[TP_TV_NONE == 0 ? 1 : -1];

#define CLF_MODEL_DIR_DEFAULT "models/v1"
#define CLF_FEATURE_CAP 8192
#define CLF_PATH_CAP 1200
#define CLF_SHA_HEX_LEN 64
/* Only used when the manifest has no usable value; the shipped manifest always
 * carries the calibrated 0.917039 hide threshold. */
#define CLF_ADULT_HIDE_DEFAULT 0.5

static struct {
    int enabled;                 /* set by classifier_init() */
    int manifest_ok;             /* manifest.json parsed */
    long long clf_ver;           /* model version string of the manifest */
    double adult_hide_threshold; /* effective tag threshold: config > manifest > default */
    double cfg_hide_threshold;   /* config adult_hide_threshold; 0 = config supplied none */
    char model_dir[CLF_PATH_CAP];
    char adult_sha[CLF_SHA_HEX_LEN + 1];
    char category_sha[CLF_SHA_HEX_LEN + 1];
    char tv_kind_sha[CLF_SHA_HEX_LEN + 1];
    ftc_model_t *adult_model;
    ftc_model_t *category_model;
    ftc_model_t *tv_kind_model;
    pthread_once_t tv_kind_once;  /* lazy tv_kind.ftz load (one-shot) */
    pthread_mutex_t stats_mutex;
    classifier_stats_t stats;
} clf = {
    .adult_hide_threshold = CLF_ADULT_HIDE_DEFAULT,
    .model_dir = CLF_MODEL_DIR_DEFAULT,
    .tv_kind_once = PTHREAD_ONCE_INIT,
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
};

/* ---- small helpers ----------------------------------------------------- */

static uint64_t clf_elapsed_us(const struct timespec *t0, const struct timespec *t1) {
    int64_t ns = (int64_t)(t1->tv_sec - t0->tv_sec) * 1000000000LL +
                 ((int64_t)t1->tv_nsec - (int64_t)t0->tv_nsec);
    if (ns <= 0) return 0;
    return (uint64_t)(ns / 1000);
}

static int clf_sha256_file(const char *path, char *out_hex) {
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
    log_msg(LOG_WARN, "Classifier: manifest has no sha256 for %s.ftz", model);
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
     * per-row clf_ver the backfill compares against. */
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

    const cJSON *models = clf_json_object(root, "models");
    clf_manifest_sha(models, "adult", clf.adult_sha, sizeof(clf.adult_sha));
    clf_manifest_sha(models, "category", clf.category_sha, sizeof(clf.category_sha));
    clf_manifest_sha(models, "tv_kind", clf.tv_kind_sha, sizeof(clf.tv_kind_sha));

    cJSON_Delete(root);
}

/* ---- model loading ----------------------------------------------------- */

/* Load <model_dir>/<name>.ftz, but only when the manifest pins its sha256:
 * unverified bytes are never loaded, they only degrade the classification. */
static ftc_model_t *clf_load_model(const char *name, const char *expected_sha) {
    char path[CLF_PATH_CAP + 32];
    snprintf(path, sizeof(path), "%s/%s.ftz", clf.model_dir, name);

    if (!clf.manifest_ok || expected_sha[0] == '\0') {
        log_msg(LOG_WARN, "Classifier: %s not loaded (no verified manifest entry); parser-only", path);
        /* Counted under the stats mutex like every other stats mutation: the
         * lazy tv_kind load below runs clf_load_model() on a tree-worker thread
         * while other workers are updating clf.stats concurrently. */
        pthread_mutex_lock(&clf.stats_mutex);
        clf.stats.model_load_failures++;
        pthread_mutex_unlock(&clf.stats_mutex);
        return NULL;
    }

    char actual_sha[CLF_SHA_HEX_LEN + 1];
    if (clf_sha256_file(path, actual_sha) != 0) {
        log_msg(LOG_WARN, "Classifier: %s unreadable; parser-only for it", path);
        pthread_mutex_lock(&clf.stats_mutex);
        clf.stats.model_load_failures++;
        pthread_mutex_unlock(&clf.stats_mutex);
        return NULL;
    }
    if (strcmp(actual_sha, expected_sha) != 0) {
        log_msg(LOG_WARN, "Classifier: %s sha256 mismatch (manifest %.16s.., file %.16s..); not loading",
                path, expected_sha, actual_sha);
        pthread_mutex_lock(&clf.stats_mutex);
        clf.stats.model_load_failures++;
        pthread_mutex_unlock(&clf.stats_mutex);
        return NULL;
    }

    ftc_model_t *model = ftc_load(path);
    if (!model) {
        log_msg(LOG_WARN, "Classifier: %s failed to load; parser-only for it", path);
        pthread_mutex_lock(&clf.stats_mutex);
        clf.stats.model_load_failures++;
        pthread_mutex_unlock(&clf.stats_mutex);
        return NULL;
    }

    log_msg(LOG_INFO, "Classifier: loaded %s", path);
    return model;
}

static void clf_tv_kind_load_once(void) {
    clf.tv_kind_model = clf_load_model("tv_kind", clf.tv_kind_sha);
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
    pthread_mutex_unlock(&clf.stats_mutex);

    log_msg(LOG_INFO, "Initializing classifier (model dir: %s)...", clf.model_dir);

    clf_load_manifest();
    if (!clf.manifest_ok) {
        /* Counted once: without the manifest none of the three models can be
         * loaded or verified, and /stats must not report zero failures while
         * every item degrades to parser-only. */
        log_msg(LOG_WARN, "Classifier: no usable model manifest; every torrent is classified parser-only");
        pthread_mutex_lock(&clf.stats_mutex);
        clf.stats.model_load_failures++;
        pthread_mutex_unlock(&clf.stats_mutex);
        return;
    }

    clf.adult_model = clf_load_model("adult", clf.adult_sha);
    clf.category_model = clf_load_model("category", clf.category_sha);
    /* tv_kind.ftz is deliberately not loaded here: it is consulted only for
     * undecided tv/anime items, and each resident model costs ~19 MB RSS. */

    log_msg(LOG_INFO, "Classifier ready (clf_ver=%lld, hide=%.3f): adult=%s category=%s tv_kind=lazy",
            clf.clf_ver, clf.adult_hide_threshold,
            clf.adult_model ? "loaded" : "unavailable",
            clf.category_model ? "loaded" : "unavailable");
}

int classifier_classify(const torrent_metadata_t *meta, classification_t *out) {
    if (!clf.enabled || !meta || !out) return 1;

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* tparse wants a flat {path, size} view of the file list; it copies
     * nothing, so the array only has to outlive the calls below. */
    int n_files = (meta->files && meta->num_files > 0) ? meta->num_files : 0;
    tp_file_t *files = NULL;
    if (n_files > 0) {
        files = (tp_file_t *)malloc((size_t)n_files * sizeof(tp_file_t));
        if (!files) {
            log_msg(LOG_WARN, "Classifier: out of memory for %d files; no classification", n_files);
            return 1;
        }
        for (int i = 0; i < n_files; i++) {
            files[i].path = meta->files[i].path ? meta->files[i].path : "";
            files[i].size = meta->files[i].size_bytes;
        }
    }

    const char *name = meta->name ? meta->name : "";
    tp_result_t parsed;
    memset(&parsed, 0, sizeof(parsed));
    if (tp_parse(name, files, n_files, meta->size_bytes, &parsed) != TP_OK) {
        log_msg(LOG_WARN, "Classifier: tparse rejected '%s'; no classification", name);
        free(files);
        return 1;
    }

    /* Per-call feature buffer: several metadata workers run this at once, and
     * tp_feature_text() keeps no state of its own. Ask for the length first so
     * only an oversized input is truncated at the cap. */
    char feature[CLF_FEATURE_CAP];
    size_t need = tp_feature_text(name, files, n_files, &parsed, NULL, 0);
    size_t cap = sizeof(feature);
    if (need + 1 < cap) cap = need + 1;
    tp_feature_text(name, files, n_files, &parsed, feature, cap);
    feature[cap - 1] = '\0';
    free(files);

    /* category: model verdict when available, else the parser hint */
    int category = parsed.category_hint;
    double confidence = 0.0;
    int degraded = 0;
    if (clf.category_model) {
        int label = -1;
        float prob = 0.0f;
        if (ftc_predict(clf.category_model, feature, 1, &label, &prob) > 0 &&
            label >= 0 && label < CLASSIFIER_NUM_CATEGORIES) {
            category = label;
            confidence = (double)prob;
        }
        /* No prediction, or the -1 "unresolvable label" case: keep the hint. */
    } else {
        degraded = 1;
    }

    /* adult: 0/1 softmax, so P(1) is the winning probability for label 1 and
     * its complement when label 0 wins. */
    int adult = 0;
    double adult_p = 0.0;
    if (clf.adult_model) {
        int label = -1;
        float prob = 0.0f;
        if (ftc_predict(clf.adult_model, feature, 1, &label, &prob) > 0) {
            adult_p = (label == 1) ? (double)prob : 1.0 - (double)prob;
            if (adult_p < 0.0) adult_p = 0.0;
            if (adult_p > 1.0) adult_p = 1.0;
        }
        adult = (adult_p >= clf.adult_hide_threshold) ? 1 : 0;
    } else {
        degraded = 1;
    }

    /* tv_kind: parser-first (classifier.md §4) - the student only decides the
     * tv/anime items tparse left undecided. */
    int tv_kind = parsed.tv_kind;
    int tv_kind_model_used = 0;
    if (tv_kind == TP_TV_NONE && (category == TP_CAT_TV || category == TP_CAT_ANIME)) {
        pthread_once(&clf.tv_kind_once, clf_tv_kind_load_once);
        if (clf.tv_kind_model) {
            int label = -1;
            float prob = 0.0f;
            tv_kind_model_used = 1;
            if (ftc_predict(clf.tv_kind_model, feature, 1, &label, &prob) > 0 &&
                label > TP_TV_NONE && label <= TP_TV_DAILY) {
                tv_kind = label;
            }
        }
    }

    out->category = category;
    out->tv_kind = tv_kind;
    out->adult = adult;
    out->adult_p = adult_p;
    out->season_lo = parsed.season_lo;
    out->season_hi = parsed.season_hi;
    out->episode_lo = parsed.episode_lo;
    out->episode_hi = parsed.episode_hi;
    out->resolution = parsed.resolution;
    out->confidence = confidence;
    out->clf_ver = clf.clf_ver;

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    pthread_mutex_lock(&clf.stats_mutex);
    clf.stats.total_classified++;
    clf.stats.total_us += clf_elapsed_us(&t_start, &t_end);
    /* adult_tagged is NOT bumped here: only rows the caller actually stores
     * with adult = 1 count (classifier_note_stored_adult()), so a dropped
     * torrent never inflates it. */
    clf.stats.parser_only_fallback += (uint64_t)(degraded ? 1 : 0);
    clf.stats.tv_kind_model_used += (uint64_t)tv_kind_model_used;
    if (category >= 0 && category < CLASSIFIER_NUM_CATEGORIES) {
        clf.stats.by_category[category]++;
    }
    pthread_mutex_unlock(&clf.stats_mutex);

    return 0;
}

void classifier_get_stats(classifier_stats_t *out) {
    if (!out) return;
    pthread_mutex_lock(&clf.stats_mutex);
    *out = clf.stats;
    pthread_mutex_unlock(&clf.stats_mutex);
}

int classifier_models_loaded(void) {
    /* Read without the stats mutex: both fields are set once by
     * classifier_init() before any worker thread exists, and only the adult
     * model decides adult tagging (the caller uses this to demote the keyword
     * porn filter). */
    return (clf.enabled && clf.adult_model) ? 1 : 0;
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
    if (!db || !clf.enabled || !clf.manifest_ok) return;

    char buf[64];
    snprintf(buf, sizeof(buf), "%lld", clf.clf_ver);
    clf_meta_put(db, "clf_ver", buf);
    if (clf.adult_sha[0]) clf_meta_put(db, "model_sha256_adult", clf.adult_sha);
    if (clf.category_sha[0]) clf_meta_put(db, "model_sha256_category", clf.category_sha);
    if (clf.tv_kind_sha[0]) clf_meta_put(db, "model_sha256_tv_kind", clf.tv_kind_sha);
    snprintf(buf, sizeof(buf), "%.6f", clf.adult_hide_threshold);
    clf_meta_put(db, "adult_hide_threshold", buf);

    log_msg(LOG_DEBUG, "Classifier: classifier_meta updated (clf_ver=%lld)", clf.clf_ver);
}

#else /* !CLF_ENABLED */

/*
 * Host build (`make`, plain gcc, no tparse/ftc and no tparse include path):
 * the classifier is compiled out. tree_metadata.c still calls in, gets 1 back
 * and writes no torrent_class rows; /stats renders enabled:false and zeros.
 */
static struct {
    pthread_mutex_t stats_mutex;
    classifier_stats_t stats;
} clf = {
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
};

void classifier_init(const char *model_dir, double adult_hide_threshold) {
    (void)model_dir;
    (void)adult_hide_threshold;
    log_msg(LOG_DEBUG, "Classifier: built without CLF_ENABLED; classification disabled");
}

/* No models exist in this flavour, so the keyword porn filter always stays
 * active as the fallback and every ingest-outcome counter stays zero. */
int classifier_models_loaded(void) {
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
