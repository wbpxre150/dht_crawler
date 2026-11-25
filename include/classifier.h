#ifndef CLASSIFIER_H
#define CLASSIFIER_H

#include "database.h"
#include <stdint.h>

/*
 * Ingest-time classification (Stage 5, tag mode).
 *
 * Every torrent that survives the ingest filters in tree_metadata.c (the
 * always-on hard block list and the language filter) gets a classification_t. The value rides
 * inside torrent_metadata_t through batch_writer_add() and is written as one
 * torrent_class row, in the same transaction as the torrent itself. In
 * adult_mode=drop the same verdict is also the drop decision: every item the
 * adult model TAGGED adult (adult_p >= the effective adult_hide_threshold) is
 * never stored at all. The tag boundary — not a separate, higher value — is
 * the drop boundary, so drop mode never stores what tag mode would hide.
 *
 * Decision path (CRAM v15 — one engine, one forward pass, no fastText):
 *   - tparse runs first (release name + the FIRST 50 files) and supplies the
 *     season / episode / resolution fields and the hints the degraded path
 *     falls back on;
 *   - one CRAM forward pass over the feature text decides category (masked
 *     48-state argmin — the category model), confidence (T-tempered category
 *     marginal), adult_p (raw adult marginal, no temperature) and adult
 *     (adult_p >= the effective adult_hide_threshold);
 *   - the standalone kind head decides tv_kind for every CRAM-decided tv/anime
 *     row and tv_kind is TP_TV_NONE otherwise. There is no parser gate on
 *     tv_kind any more.
 *
 * Engine load failure or a missing model directory is never fatal and is
 * all-or-nothing (there is no second model): the item is classified
 * parser-only (category hint, parser tv_kind, adult 0, adult_p 0.0, clf_ver 0),
 * a warning is logged and the fallback counters move.
 *
 * Build gating: the real implementation needs the vendored tparse library and
 * the CRAM engine, which only the Android wrapper build provides (it defines
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
    char adult_mode[8];            /* active ingest adult mode: "tag" or "drop" */
    double adult_hide_threshold;   /* effective tag threshold in use (config >
                                    * manifest > default); 0.0 when not
                                    * initialised. It is also the drop boundary
                                    * in adult_mode=drop. */
    uint64_t total_classified;     /* items that were classified */
    uint64_t total_us;             /* summed classify() wall time, microseconds */
    uint64_t parser_only_fallback; /* items with a model missing/unusable */
    uint64_t model_load_failures;  /* unusable manifest (counted once) plus each
                                    * model that failed to load or verify */
    uint64_t adult_tagged;         /* stored rows with adult = 1 (reported by the
                                    * ingest call site, not by classify()) */
    uint64_t adult_dropped;        /* adult_mode=drop rows dropped for being
                                    * adult-tagged */
    uint64_t hard_block_dropped;   /* rows dropped by the hard block list */
    uint64_t tv_kind_model_used;   /* items where the kind head was consulted
                                    * (CRAM fills it on every prediction; the
                                    * caller applies it only on tv/anime) */
    uint64_t by_category[CLASSIFIER_NUM_CATEGORIES]; /* per category, all items */
    uint64_t file_cap_hits;        /* items whose file list was capped at 50 */
    /* Provenance the dashboard reads back (exit criteria 2/3). These are
     * module state rather than counters, but /stats renders them beside the
     * threshold it already takes from here, so they ride the same snapshot.
     * 0.0 / "" mean "the manifest named nothing" (and, for the inert host
     * build, "no manifest was ever read"). */
    double adult_drop_threshold; /* mirrors adult_hide_threshold: v15 has ONE
                                  * adult boundary (tag == drop); the key is
                                  * kept for /stats shape stability */
    int cram_enabled;            /* 1 when the CRAM engine is loaded and usable */
    double cram_temperature;     /* manifest cram.temperature (category
                                  * confidence only; adult is raw) */
    char cram_sha256[65];        /* verified cram.bin digest, lowercase hex */
    char cram_vocab_sha256[65];  /* verified cram_vocab.bin digest */
    /* Engine counters, merged from cram_get_stats() at snapshot time (the
     * module owns them so a failed load is still visible). */
    uint64_t cram_rows_predicted;           /* rows the engine predicted */
    uint64_t cram_load_failures;            /* engine load failures; the same
                                             * events as model_load_failures */
    uint64_t cram_kind_head_used;           /* predictions that filled kind */
    uint64_t cram_adult_tagged_predictions; /* predictions with adult == 1 */
    uint64_t cram_feature_cap_hits;         /* rows truncated at the engine cap */
} classifier_stats_t;

/*
 * Publish the active ingest adult mode ("tag" or "drop"; anything else is
 * treated as "tag", which is also the config default). Call once from main.c
 * before the worker threads start — it is called even when the classifier is
 * off, so /stats can always report the operator's mode — and then read it back
 * through classifier_get_stats().
 */
void classifier_set_adult_mode(const char *adult_mode);

/*
 * Initialise the classifier. Non-fatal by contract: a missing model directory,
 * manifest or model file only disables the corresponding model and logs a
 * warning. Call once from main.c before the worker threads start; when the
 * feature is off (config classifier_enabled=0) do not call it at all, and
 * classifier_classify() then declines every item.
 *
 * @param model_dir directory holding manifest.json, cram.bin and cram_vocab.bin
 *                  (NULL/empty falls back to "models/v15", cwd-relative like the
 *                  keyword files).
 * @param adult_hide_threshold config adult_hide_threshold; > 0 is authoritative
 *                  (it is what the operator wrote in config.ini), <= 0 means
 *                  "config supplied none" and the manifest value is used, with
 *                  the built-in default as the last resort. The resolved value
 *                  is the tag boundary, and therefore the adult_mode=drop
 *                  boundary, and is reported by classifier_get_stats().
 */
void classifier_init(const char *model_dir, double adult_hide_threshold);

/*
 * Classify one torrent. Thread-safe: many tree metadata workers call this
 * concurrently (a loaded CRAM model is immutable on the predict path, and the
 * feature scratch is per call).
 *
 * @return 0 and *out filled on success; 1 when no classification is available
 *         (feature disabled, not initialised, or built without CLF_ENABLED), in
 *         which case the caller leaves torrent_metadata_t.has_classification 0
 *         and no torrent_class row is written.
 */
int classifier_classify(const torrent_metadata_t *meta, classification_t *out);

/*
 * Score the adult verdict for one torrent (the entry point for the adult-
 * scoring pass).
 *
 * It runs the same capped-50 -> tparse -> feature text -> CRAM forward pass as
 * classifier_classify() (the feature helper is shared), so an adult-scored verdict
 * and an ingest verdict are produced from byte-identical input; only the fields
 * returned differ. The verdict is the CRAM adult marginal, tagged with the same
 * rule as the ingest path (adult = adult_p >= the effective adult hide
 * threshold), so an adult-scored row and a freshly ingested one can never disagree
 * about the boundary.
 *
 * @return 0 and both out-parameters filled on success; 1 when no verdict is
 *         available (feature off, built without CLF_ENABLED, CRAM not loaded,
 *         tparse rejected the name, or the engine predicted nothing), in which
 *         case the caller must leave the stored row untouched rather than
 *         downgrade a good one. Either out-parameter may be NULL.
 */
int classifier_classify_adult(const torrent_metadata_t *meta, int *adult, double *adult_p);

/* Copy the current counters into *out (zeroed when classification is off). */
void classifier_get_stats(classifier_stats_t *out);

/*
 * 1 when the CRAM engine is loaded and usable, i.e. the classifier owns
 * category, tv_kind and adult tagging. 0 when the classifier is disabled
 * (feature off, built without CLF_ENABLED, or its container missing/unusable).
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
void classifier_note_adult_dropped(void); /* row dropped: adult-tagged (drop mode) */
void classifier_note_hard_block_dropped(void); /* row dropped by the hard block list */

/* Display name for a tp_category_t value, for the /stats JSON keys. */
const char *classifier_category_name(int category);

/*
 * Write the classifier_meta rows (clf_ver, adult_clf_ver, the two CRAM
 * sha256s, adult hide threshold) with INSERT OR REPLACE. Called after
 * database_create_schema() because classifier_init() runs before the database
 * is open; a no-op when the classifier is off, when no manifest was read, when
 * the engine is not loaded, or when the table is missing.
 */
void classifier_write_meta(sqlite3 *db);

#endif /* CLASSIFIER_H */
