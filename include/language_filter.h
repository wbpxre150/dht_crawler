#ifndef LANGUAGE_FILTER_H
#define LANGUAGE_FILTER_H

#include "database.h"
#include <stdint.h>

typedef struct {
    /* Effective state, so a consumer can tell "disabled" from "nothing seen
     * yet" without reading the config struct. `threshold` is the clamped value
     * the module is actually applying. */
    int enabled;
    int threshold;
    uint64_t total_checked;
    uint64_t filtered_by_non_latin;
    uint64_t total_filtered;
} language_filter_stats_t;

/**
 * Initialize the language filter.
 *
 * @param non_latin_threshold Drop a torrent whose name or any file path has at
 *                            or above this % of non-Latin script characters
 *                            (0=disabled, clamped to 0..100).
 * @return 0 on success.
 */
int language_filter_init(int non_latin_threshold);

void language_filter_cleanup(void);

/**
 * @return 1 if the torrent name or any of its file paths is at or above the
 *         non-Latin threshold, 0 otherwise. Fail-open: 0 when the filter is
 *         disabled or not initialized.
 */
int language_filter_check(const torrent_metadata_t *metadata);

void language_filter_get_stats(language_filter_stats_t *stats);

#endif /* LANGUAGE_FILTER_H */
