#ifndef PORN_FILTER_CLEANUP_H
#define PORN_FILTER_CLEANUP_H

#include "database.h"

/**
 * Run the --porn-filter-update maintenance pass.
 *
 * Scans the existing torrent database, applies the current porn_filter_check()
 * to candidate rows surfaced by an SQL LIKE prefilter, and DELETEs matches in
 * batches under a single transaction. The caller must have already called
 * porn_filter_init() so the keyword/regex/heuristic state is loaded.
 *
 * Returns 0 on success, non-zero on failure (suitable for exit()).
 */
int porn_filter_cleanup_run(database_t *db, const char *db_path);

#endif /* PORN_FILTER_CLEANUP_H */
