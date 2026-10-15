#ifndef CLASSIFIER_H
#define CLASSIFIER_H

#include "database.h"
#include <stdint.h>

/*
 * Ingest-time classification (Stage 5, tag mode).
 *
 * Every torrent that survives porn_filter_check()/language_filter_check() in
 * tree_metadata.c gets a classification_t. The value rides inside
 * torrent_metadata_t through batch_writer_add() and is written as one
 * torrent_class row, in the same transaction as the torrent itself.
 *
 * Decision path (classifier.md §4):
 *   - tparse always runs (release name + file list) and supplies the season /
 *     episode / resolution fields and a category hint;
 *   - the category and adult fastText students override/confirm the hint;
 *   - the tv_kind student is consulted only when tparse left tv_kind undecided
 *     and the effective category is tv or anime, and it is loaded lazily on
 *     first use (~19 MB RSS per resident model; see stage 4 measurements).
 *
 * Model load failure or a missing model directory is never fatal: the item is
 * classified parser-only (category hint, adult 0, clf_ver from the manifest or
 * 0), a warning is logged and the fallback counters move.
 *
 * Build gating: the real implementation needs the vendored tparse/ftc
 * libraries, which only the Android wrapper build provides (it defines
 * CLF_ENABLED). The crawler's own host `make` (plain gcc, no tparse include
 * path) compiles the inert stubs below instead; /stats then reports
 * enabled:false and zeros.
 */

/* Number of tp_category_t values (TP_CAT_UNKNOWN .. TP_CAT_OTHER). Kept as a
 * plain count here so this header and the /stats renderer compile without the
 * tparse include path; src/classifier.c fails to compile if the vendored
 * taxonomy ever stops matching it. */
#define CLASSIFIER_NUM_CATEGORIES 12

typedef struct {
    int enabled;                   /* 1 when classifier_init() ran (feature on) */
    uint64_t total_classified;     /* items that were classified */
    uint64_t total_us;             /* summed classify() wall time, microseconds */
    uint64_t parser_only_fallback; /* items with a model missing/unusable */
    uint64_t model_load_failures;  /* unusable manifest (counted once) plus each
                                    * model that failed to load or verify */
    uint64_t adult_tagged;         /* items with adult = 1 */
    uint64_t tv_kind_model_used;   /* items where the tv_kind student was consulted */
    uint64_t by_category[CLASSIFIER_NUM_CATEGORIES]; /* per category, all items */
} classifier_stats_t;

/*
 * Initialise the classifier. Non-fatal by contract: a missing model directory,
 * manifest or model file only disables the corresponding model and logs a
 * warning. Call once from main.c before the worker threads start; when the
 * feature is off (config classifier_enabled=0) do not call it at all, and
 * classifier_classify() then declines every item.
 *
 * @param model_dir directory holding manifest.json and {adult,category,tv_kind}.ftz
 *                  (NULL/empty falls back to "models/v1", cwd-relative like the
 *                  keyword files).
 */
void classifier_init(const char *model_dir);

/*
 * Classify one torrent. Thread-safe: many tree metadata workers call this
 * concurrently (the ftc models are const on the predict path).
 *
 * @return 0 and *out filled on success; 1 when no classification is available
 *         (feature disabled, not initialised, or built without CLF_ENABLED), in
 *         which case the caller leaves torrent_metadata_t.has_classification 0
 *         and no torrent_class row is written.
 */
int classifier_classify(const torrent_metadata_t *meta, classification_t *out);

/* Copy the current counters into *out (zeroed when classification is off). */
void classifier_get_stats(classifier_stats_t *out);

/* Display name for a tp_category_t value, for the /stats JSON keys. */
const char *classifier_category_name(int category);

/*
 * Write the classifier_meta rows (clf_ver, model sha256s, adult hide
 * threshold) with INSERT OR REPLACE. Called after database_create_schema()
 * because classifier_init() runs before the database is open; a no-op when the
 * classifier is off, when no manifest was read, or when the table is missing.
 */
void classifier_write_meta(sqlite3 *db);

#endif /* CLASSIFIER_H */
