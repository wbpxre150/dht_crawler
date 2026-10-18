#ifndef CLASSIFIER_H
#define CLASSIFIER_H

#include "database.h"
#include <stdint.h>

/*
 * Ingest-time classification (Stage 5, tag mode).
 *
 * Every torrent that survives the ingest filters in tree_metadata.c (the
 * always-on hard block list, the keyword porn filter while it is still in
 * charge, and the language filter) gets a classification_t. The value rides
 * inside torrent_metadata_t through batch_writer_add() and is written as one
 * torrent_class row, in the same transaction as the torrent itself. In
 * adult_mode=drop the same verdict is also the drop decision: items at or
 * above the drop threshold are never stored at all.
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
    uint64_t adult_tagged;         /* stored rows with adult = 1 (reported by the
                                    * ingest call site, not by classify()) */
    uint64_t adult_dropped;        /* adult_mode=drop rows dropped on P(adult) */
    uint64_t hard_block_dropped;   /* rows dropped by the hard block list */
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
 * @param adult_hide_threshold config adult_hide_threshold; > 0 is authoritative
 *                  (it is what the operator wrote in config.ini), <= 0 means
 *                  "config supplied none" and the manifest value is used, with
 *                  the built-in default as the last resort.
 */
void classifier_init(const char *model_dir, double adult_hide_threshold);

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

/*
 * 1 when the adult model is loaded and usable, i.e. the classifier owns adult
 * tagging. Callers use this to demote the keyword porn filter: when it returns
 * 1 that filter must not run at all, and when the classifier is disabled (or
 * built without CLF_ENABLED, or its models are missing) it returns 0 so the
 * keyword fallback stays active.
 */
int classifier_models_loaded(void);

/*
 * 1 when classifier_init() ran, i.e. when classifier_classify() can produce a
 * verdict (the host build without CLF_ENABLED and a config with
 * classifier_enabled=0 both answer 0).
 */
int classifier_enabled(void);

/*
 * Model version this build labels rows with, the same value
 * classifier_write_meta() stores as classifier_meta['clf_ver'] (0 when no
 * manifest was read).
 */
long long classifier_current_version(void);

/*
 * Ingest-outcome counters. classifier_classify() cannot know whether its
 * verdict was written or dropped, so the tree metadata worker reports the
 * outcome here (each bumps exactly one counter under the stats mutex).
 */
void classifier_note_stored_adult(void); /* row stored with adult = 1 */
void classifier_note_adult_dropped(void); /* row dropped: P(adult) >= adult_drop_threshold */
void classifier_note_hard_block_dropped(void); /* row dropped by the hard block list */

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
