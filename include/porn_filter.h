#ifndef PORN_FILTER_H
#define PORN_FILTER_H

#include "database.h"
#include <stdint.h>

/**
 * Pornography content filter
 *
 * Hybrid 3-layer filtering system:
 * - Layer 1: Hash set keyword matching (fast pre-filter)
 * - Layer 2: Regex pattern matching for evasion detection
 * - Layer 3: Heuristic scoring based on multiple signals
 *
 * Expected performance: 2-7ms per torrent
 * Expected accuracy: 85-92%
 */

/**
 * Filter statistics
 */
typedef struct {
    uint64_t total_checked;         // Total torrents checked
    uint64_t filtered_by_keyword;   // Filtered by keyword or xxx co-occurrence
    uint64_t total_filtered;        // Total filtered
} porn_filter_stats_t;

/**
 * Initialize the pornography content filter
 *
 * @param keyword_file_path Path to keyword file (e.g., "porn_filter_keywords.txt")
 * @return 0 on success, -1 on error
 */
int porn_filter_init(const char *keyword_file_path);

/**
 * Cleanup and free all filter resources
 */
void porn_filter_cleanup(void);

/**
 * Check if torrent metadata contains pornographic content
 *
 * @param metadata Torrent metadata to analyze (name, files, etc.)
 * @return 1 if content is likely pornography (should be filtered)
 *         0 if content appears safe (should be kept)
 */
int porn_filter_check(const torrent_metadata_t *metadata);

/**
 * Get current filter statistics
 *
 * @param stats Pointer to stats structure to fill
 */
void porn_filter_get_stats(porn_filter_stats_t *stats);

/**
 * Enumerate currently-loaded keywords (lowercase form).
 * Returns 0 if filter not initialized.
 */
int porn_filter_get_keyword_count(void);
const char *porn_filter_get_keyword(int index);

#endif /* PORN_FILTER_H */
