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
    uint64_t total_checked;          // Total torrents checked
    uint64_t filtered_by_keyword;    // Filtered by Layer 1 (keywords)
    uint64_t filtered_by_regex;      // Filtered by Layer 2 (regex)
    uint64_t filtered_by_heuristic;  // Filtered by Layer 3 (heuristics)
    uint64_t total_filtered;         // Total filtered (all layers)
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
 * Set filter thresholds (for runtime configuration)
 *
 * @param keyword_threshold Minimum weight for keyword match (default: 8)
 * @param regex_threshold Minimum weight for regex match (default: 9)
 * @param heuristic_threshold Minimum score for heuristic match (default: 5)
 */
void porn_filter_set_thresholds(int keyword_threshold, int regex_threshold, int heuristic_threshold);

#endif /* PORN_FILTER_H */
