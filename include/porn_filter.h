#ifndef PORN_FILTER_H
#define PORN_FILTER_H

#include "database.h"

/**
 * Pornography content filter
 *
 * Analyzes torrent metadata (name and file names) to determine
 * if content is likely pornographic material.
 *
 * This is a placeholder implementation. Actual filtering logic
 * will be implemented in a future phase.
 */

/**
 * Check if torrent metadata contains pornographic content
 *
 * @param metadata Torrent metadata to analyze (name, files, etc.)
 * @return 1 if content is likely pornography (should be filtered)
 *         0 if content appears safe (should be kept)
 */
int porn_filter_check(const torrent_metadata_t *metadata);

#endif /* PORN_FILTER_H */
