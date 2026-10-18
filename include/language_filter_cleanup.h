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
 * Nothing calls this at startup: it is the language half of the
 * --category-filter-update maintenance pass (category_filter_cleanup.c), so a
 * db-wide language sweep stays available without the keyword machinery it used
 * to share a file with.
 *
 * @param allow_prompt 1 to ask on stdin whether to back up first (interactive
 *                     use), 0 to run unattended — the app invokes this with no
 *                     TTY, so it must never read stdin.
 *
 * Returns 0 on success, non-zero on failure (suitable for exit()).
 */
int language_filter_cleanup_run(database_t *db, const char *db_path, int allow_prompt);

#endif /* LANGUAGE_FILTER_CLEANUP_H */
