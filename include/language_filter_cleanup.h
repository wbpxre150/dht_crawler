#ifndef LANGUAGE_FILTER_CLEANUP_H
#define LANGUAGE_FILTER_CLEANUP_H

#include "database.h"

/**
 * Run the language-filter maintenance pass.
 *
 * Scans every row of the torrent database, applies language_filter_check() to
 * each one, and DELETEs matches in batches inside a single transaction. The
 * caller must have already called language_filter_init().
 *
 * Nothing calls this at startup: the pass is compiled and kept as the hook for
 * a maintenance flag, so a db-wide language sweep stays available without the
 * keyword machinery it used to share a file with.
 *
 * Returns 0 on success, non-zero on failure (suitable for exit()).
 */
int language_filter_cleanup_run(database_t *db, const char *db_path);

#endif /* LANGUAGE_FILTER_CLEANUP_H */
