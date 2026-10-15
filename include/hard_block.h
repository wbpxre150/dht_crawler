#ifndef HARD_BLOCK_H
#define HARD_BLOCK_H

/*
 * Always-on hard block list (Stage 6).
 *
 * A flat list of unambiguous terms that are dropped at ingest in BOTH adult
 * modes and whether or not the classifier is running: the keyword porn filter
 * is a fallback that can be disabled or demoted, this list is not. Terms are
 * matched as plain case-insensitive substrings of the torrent NAME (the file
 * list is not consulted in this stage) and are never stored, so a dropped
 * infohash is also bloom-added by the caller like any other filtered item.
 *
 * File format mirrors porn_filter_keywords.txt: one term per line, blank lines
 * and '#' comment lines ignored; terms are folded to lowercase on load. File
 * path is cwd-relative, like the other keyword files.
 */

/*
 * Load the list. Non-fatal by contract: a missing or unreadable file logs one
 * warning and leaves the list disabled; an empty file is fine. Safe to call
 * once from main.c before the worker threads start.
 *
 * @param file_path path to the block list (NULL/empty falls back to
 *                  "hard_block.txt")
 */
void hard_block_init(const char *file_path);

/* Free the loaded terms (idempotent). */
void hard_block_cleanup(void);

/*
 * @return 1 when any loaded term occurs in name (case-insensitive substring),
 *         0 when the list is disabled/empty, name is NULL, or nothing matched.
 */
int hard_block_check(const char *name);

#endif /* HARD_BLOCK_H */
