#ifndef LANGUAGE_FILTER_H
#define LANGUAGE_FILTER_H

#include "database.h"
#include <stdint.h>

typedef struct {
    uint64_t total_checked;
    uint64_t filtered_by_non_latin;
    uint64_t total_filtered;
} language_filter_stats_t;

/**
 * Initialize the language filter.
 *
 * @param non_latin_threshold Max % of non-Latin script chars allowed (0=disabled).
 * @return 0 on success.
 */
int language_filter_init(int non_latin_threshold);

void language_filter_cleanup(void);

/**
 * @return 1 if torrent name/files exceed the non-Latin threshold, 0 otherwise.
 */
int language_filter_check(const torrent_metadata_t *metadata);

void language_filter_get_stats(language_filter_stats_t *stats);

#endif /* LANGUAGE_FILTER_H */
