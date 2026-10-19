#ifndef CATEGORY_FILTER_CLEANUP_H
#define CATEGORY_FILTER_CLEANUP_H

#include "database.h"
#include <stdint.h>

/*
 * "Categories to save" (save_categories / save_tv_kinds) — ingest gating and
 * the --category-filter-update maintenance pass.
 *
 * The two config keys hold the selection as bitmasks over the vendored tparse
 * taxonomy (dht_crawler-android/tparse/taxonomy.h, mirrored by
 * src/classifier.c and the CLS_* names in src/http_api.c):
 *
 *   save_categories  bit i = tp_category_t i   (12 bits, 0 = unknown)
 *   save_tv_kinds    bit i = tp_tv_kind_t i    (7 bits, 0 = none)
 *
 * An empty, missing or unparseable list means "save everything", i.e. every bit
 * set. That is also the default, so the filter is inert until an operator asks
 * for a narrower selection — the feature is purely backward compatible.
 *
 * Ingest (src/tree_metadata.c) drops a torrent whose category is unselected,
 * and — only inside the tv and anime categories, where tv_kind is meaningful —
 * one whose tv_kind is unselected. tv_kind 0 (none) is stored as SQL NULL in
 * torrent_class (see database.h), so the purge compares COALESCE(tv_kind, 0).
 */

#define CATEGORY_FILTER_NUM_CATEGORIES 12
#define CATEGORY_FILTER_NUM_TV_KINDS 7

/* tp_category_t codes the purge SQL needs (taxonomy index order). */
#define CATEGORY_FILTER_CAT_TV 2
#define CATEGORY_FILTER_CAT_ANIME 3

#define CATEGORY_FILTER_ALL_CATEGORIES ((uint16_t)((1u << CATEGORY_FILTER_NUM_CATEGORIES) - 1u))
#define CATEGORY_FILTER_ALL_TV_KINDS ((uint16_t)((1u << CATEGORY_FILTER_NUM_TV_KINDS) - 1u))

/* Verdict from category_filter_should_drop(). */
typedef enum {
    CATEGORY_FILTER_KEEP = 0,
    CATEGORY_FILTER_DROP_CATEGORY = 1,  /* category bit clear */
    CATEGORY_FILTER_DROP_TV_KIND = 2    /* tv/anime, tv_kind bit clear */
} category_filter_verdict_t;

/* Counters behind the /stats "category_filter" object. */
typedef struct {
    uint64_t total_dropped;       /* items dropped at ingest by either rule */
    uint64_t dropped_by_category; /* ... and how many of those for the category */
    uint64_t dropped_by_tv_kind;  /* ... and how many for the tv_kind */
} category_filter_stats_t;

/*
 * Parse one comma-separated list entry into a taxonomy index. Accepts the
 * taxonomy names case-insensitively ("anime", "ANIME", "season_pack") and, as
 * a convenience, a decimal index in range. Returns -1 for anything else so the
 * caller can log and ignore it.
 */
int category_filter_category_parse(const char *token);
int category_filter_tv_kind_parse(const char *token);

/* Bitmask with exactly one bit set for the given taxonomy index, or 0 when the
 * index is out of range. */
uint16_t category_filter_category_bit(int category);
uint16_t category_filter_tv_kind_bit(int tv_kind);

/*
 * The ingest drop decision. `category`/`tv_kind` are tp_category_t /
 * tp_tv_kind_t codes (out-of-range values are treated as unknown / none).
 *
 * Returns CATEGORY_FILTER_KEEP when the item may be stored, otherwise the rule
 * that rejected it — the caller uses that to pick the counter to bump.
 */
category_filter_verdict_t category_filter_should_drop(int category, int tv_kind,
                                                      uint16_t category_mask,
                                                      uint16_t tv_kind_mask);

/* Ingest drop counters (thread-safe: many metadata workers call these). */
void category_filter_note_category_dropped(void);
void category_filter_note_tv_kind_dropped(void);
void category_filter_get_stats(category_filter_stats_t *out);

/*
 * Run the --category-filter-update maintenance pass, unattended:
 *
 *   1. DELETE FROM torrents for every classified row whose category bit is
 *      clear (in batches; the FK cascade drops torrent_class/torrent_files and
 *      the FTS delete triggers clean the search index);
 *   2. the same for rows in the tv/anime categories whose tv_kind bit is clear;
 *   3. the db-wide language pass (language_filter_cleanup_run(), no prompt);
 *   4. PRAGMA wal_checkpoint(TRUNCATE).
 *
 * A full mask for both keys means "nothing to purge": steps 1-2 are skipped and
 * only the language pass runs. The caller does not compact afterwards:
 * --category-filter-update leaves the database in place and exits.
 *
 * Aborts — before deleting anything — when the selection would remove every
 * row in the database; the config parser cannot produce that state (an empty
 * list means all bits set) but the guard keeps a hand-edited mask harmless.
 *
 * Returns 0 on success, non-zero on failure (suitable for exit()).
 */
int category_filter_update_run(database_t *db, const char *db_path,
                               uint16_t category_mask, uint16_t tv_kind_mask);

#endif /* CATEGORY_FILTER_CLEANUP_H */
