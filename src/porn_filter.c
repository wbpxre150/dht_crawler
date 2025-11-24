#include "porn_filter.h"
#include "dht_crawler.h"
#include "uthash.h"
#include <string.h>
#include <strings.h>  /* For strcasecmp */
#include <ctype.h>
#include <regex.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

/* ============================================================================
 * Data Structures
 * ============================================================================ */

/**
 * Hash table entry for keywords (using uthash)
 */
typedef struct keyword_entry {
    char *keyword;              // Normalized keyword (lowercase)
    int weight;                 // Severity weight (1-10)
    UT_hash_handle hh;
} keyword_entry_t;

/**
 * Regex pattern for evasion detection
 */
typedef struct {
    regex_t regex;
    char *pattern_desc;
    int weight;
    int compiled;               // 1 if regex compiled successfully
} regex_pattern_t;

/**
 * Global filter state
 */
static struct {
    keyword_entry_t *keyword_hash;     // Hash table of keywords
    regex_pattern_t *regex_patterns;   // Array of compiled regex
    size_t num_regex;
    pthread_mutex_t stats_mutex;       // Protect stats
    porn_filter_stats_t stats;
    int initialized;

    // Configurable thresholds
    int keyword_threshold;
    int regex_threshold;
    int heuristic_threshold;
} filter_state = {
    .keyword_hash = NULL,
    .regex_patterns = NULL,
    .num_regex = 0,
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
    .stats = {0},
    .initialized = 0,
    .keyword_threshold = 8,
    .regex_threshold = 9,
    .heuristic_threshold = 5
};

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * Normalize string to lowercase (in-place)
 */
static void normalize_string(char *str) {
    if (!str) return;
    for (char *p = str; *p; p++) {
        *p = tolower((unsigned char)*p);
    }
}

/**
 * Trim leading and trailing whitespace (in-place)
 */
static void trim_whitespace(char *str) {
    if (!str) return;

    // Trim leading
    char *start = str;
    while (isspace((unsigned char)*start)) start++;

    // Trim trailing
    char *end = start + strlen(start) - 1;
    while (end > start && isspace((unsigned char)*end)) end--;
    *(end + 1) = '\0';

    // Move trimmed string to beginning if needed
    if (start != str) {
        memmove(str, start, strlen(start) + 1);
    }
}

/**
 * Count special characters in string
 */
static float calculate_special_char_ratio(const char *str) {
    if (!str || strlen(str) == 0) return 0.0f;

    int total = 0;
    int special = 0;

    for (const char *p = str; *p; p++) {
        total++;
        if (!isalnum((unsigned char)*p) && !isspace((unsigned char)*p)) {
            special++;
        }
    }

    return total > 0 ? (float)special / total : 0.0f;
}

/* ============================================================================
 * Layer 1: Hash Set Keyword Matching
 * ============================================================================ */

/**
 * Add keyword to hash table
 */
static int add_keyword(const char *keyword, int weight) {
    if (!keyword || strlen(keyword) == 0) return -1;

    // Check if already exists
    keyword_entry_t *entry;
    HASH_FIND_STR(filter_state.keyword_hash, keyword, entry);
    if (entry) {
        // Update weight if higher
        if (weight > entry->weight) {
            entry->weight = weight;
        }
        return 0;
    }

    // Create new entry
    entry = malloc(sizeof(keyword_entry_t));
    if (!entry) return -1;

    entry->keyword = strdup(keyword);
    if (!entry->keyword) {
        free(entry);
        return -1;
    }
    entry->weight = weight;

    HASH_ADD_KEYPTR(hh, filter_state.keyword_hash, entry->keyword, strlen(entry->keyword), entry);
    return 0;
}

/**
 * Load keywords from file
 * Format: keyword[:weight]
 * Lines starting with # are comments
 */
static int load_keywords(const char *file_path) {
    FILE *fp = fopen(file_path, "r");
    if (!fp) {
        log_msg(LOG_ERROR, "Failed to open keyword file: %s", file_path);
        return -1;
    }

    char line[256];
    int line_num = 0;
    int loaded = 0;

    while (fgets(line, sizeof(line), fp)) {
        line_num++;

        // Remove newline
        line[strcspn(line, "\r\n")] = '\0';

        // Trim whitespace
        trim_whitespace(line);

        // Skip empty lines and comments
        if (strlen(line) == 0 || line[0] == '#') {
            continue;
        }

        // Parse keyword:weight
        char *colon = strchr(line, ':');
        int weight = 5;  // Default weight

        if (colon) {
            *colon = '\0';
            weight = atoi(colon + 1);
            if (weight < 1) weight = 1;
            if (weight > 10) weight = 10;
        }

        // Normalize keyword
        normalize_string(line);
        trim_whitespace(line);

        if (strlen(line) > 0) {
            if (add_keyword(line, weight) == 0) {
                loaded++;
            }
        }
    }

    fclose(fp);
    log_msg(LOG_DEBUG, "Loaded %d keywords from %s", loaded, file_path);
    return 0;
}

/**
 * Check if text contains keywords from hash table
 * Returns highest weight found, or 0 if no match
 */
static int check_keywords_in_text(const char *text, int *out_weight) {
    if (!text || !filter_state.keyword_hash) {
        *out_weight = 0;
        return 0;
    }

    // Normalize text for comparison
    char *normalized = strdup(text);
    if (!normalized) {
        *out_weight = 0;
        return 0;
    }
    normalize_string(normalized);

    int max_weight = 0;
    int found = 0;

    // Check each keyword against the normalized text
    keyword_entry_t *entry, *tmp;
    HASH_ITER(hh, filter_state.keyword_hash, entry, tmp) {
        if (strstr(normalized, entry->keyword)) {
            found = 1;
            if (entry->weight > max_weight) {
                max_weight = entry->weight;
            }
        }
    }

    free(normalized);
    *out_weight = max_weight;
    return found;
}

/* ============================================================================
 * Layer 2: Regex Pattern Matching
 * ============================================================================ */

/**
 * Initialize regex patterns for evasion detection
 */
static int init_regex_patterns(void) {
    // Define patterns (limited to 5 for performance)
    const char *patterns[] = {
        "p[o0][r|2]n",                           // L33tspeak: porn, p0rn, po2n, pr0n
        "x+[._-]*x+[._-]*x+",                    // XXX with separators
        "adult[^a-z]{0,5}(video|movie|film)",    // Adult + video combo
        "(pornhub|xvideos|xhamster|xnxx)",       // Common sites
        "[0-9]{2,}\\+",                          // Age indicators: 18+, 21+, etc.
    };

    const char *descriptions[] = {
        "L33tspeak porn",
        "XXX pattern",
        "Adult video combo",
        "Adult site names",
        "Age indicator"
    };

    const int weights[] = {9, 8, 7, 10, 6};

    filter_state.num_regex = sizeof(patterns) / sizeof(patterns[0]);
    filter_state.regex_patterns = calloc(filter_state.num_regex, sizeof(regex_pattern_t));
    if (!filter_state.regex_patterns) {
        log_msg(LOG_ERROR, "Failed to allocate regex patterns");
        return -1;
    }

    int compiled_count = 0;
    for (size_t i = 0; i < filter_state.num_regex; i++) {
        filter_state.regex_patterns[i].pattern_desc = strdup(descriptions[i]);
        filter_state.regex_patterns[i].weight = weights[i];

        int ret = regcomp(&filter_state.regex_patterns[i].regex, patterns[i],
                         REG_EXTENDED | REG_ICASE | REG_NOSUB);
        if (ret == 0) {
            filter_state.regex_patterns[i].compiled = 1;
            compiled_count++;
        } else {
            char errbuf[256];
            regerror(ret, &filter_state.regex_patterns[i].regex, errbuf, sizeof(errbuf));
            log_msg(LOG_WARN, "Failed to compile regex pattern '%s': %s", patterns[i], errbuf);
            filter_state.regex_patterns[i].compiled = 0;
        }
    }

    log_msg(LOG_DEBUG, "Compiled %d/%zu regex patterns", compiled_count, filter_state.num_regex);
    return 0;
}

/**
 * Cleanup regex patterns
 */
static void cleanup_regex_patterns(void) {
    if (!filter_state.regex_patterns) return;

    for (size_t i = 0; i < filter_state.num_regex; i++) {
        if (filter_state.regex_patterns[i].compiled) {
            regfree(&filter_state.regex_patterns[i].regex);
        }
        free(filter_state.regex_patterns[i].pattern_desc);
    }

    free(filter_state.regex_patterns);
    filter_state.regex_patterns = NULL;
    filter_state.num_regex = 0;
}

/**
 * Check text against regex patterns
 * Returns highest weight found, or 0 if no match
 */
static int check_regex_patterns(const char *text, int *out_weight) {
    if (!text || !filter_state.regex_patterns) {
        *out_weight = 0;
        return 0;
    }

    int max_weight = 0;
    int found = 0;

    for (size_t i = 0; i < filter_state.num_regex; i++) {
        if (!filter_state.regex_patterns[i].compiled) continue;

        if (regexec(&filter_state.regex_patterns[i].regex, text, 0, NULL, 0) == 0) {
            found = 1;
            if (filter_state.regex_patterns[i].weight > max_weight) {
                max_weight = filter_state.regex_patterns[i].weight;
            }
        }
    }

    *out_weight = max_weight;
    return found;
}

/* ============================================================================
 * Layer 3: Heuristic Scoring
 * ============================================================================ */

/**
 * Check if file has video extension
 */
static int is_video_file(const char *path) {
    if (!path) return 0;

    const char *ext = strrchr(path, '.');
    if (!ext) return 0;
    ext++; // Skip the dot

    const char *video_exts[] = {"mp4", "avi", "mkv", "wmv", "mov", "flv", "mpg", "mpeg", "m4v", "webm"};
    for (size_t i = 0; i < sizeof(video_exts) / sizeof(video_exts[0]); i++) {
        if (strcasecmp(ext, video_exts[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

/**
 * Check if file has image extension
 */
static int is_image_file(const char *path) {
    if (!path) return 0;

    const char *ext = strrchr(path, '.');
    if (!ext) return 0;
    ext++;

    const char *image_exts[] = {"jpg", "jpeg", "png", "gif", "bmp", "webp"};
    for (size_t i = 0; i < sizeof(image_exts) / sizeof(image_exts[0]); i++) {
        if (strcasecmp(ext, image_exts[i]) == 0) {
            return 1;
        }
    }
    return 0;
}

/**
 * Check if files have sequential numbering pattern
 */
static int has_sequential_files(const torrent_metadata_t *metadata) {
    if (!metadata || metadata->num_files < 10) return 0;

    int sequential_count = 0;

    for (int i = 0; i < metadata->num_files; i++) {
        const char *path = metadata->files[i].path;
        if (!path) continue;

        // Look for patterns like: 01, 02, 03 or file01, file02, etc.
        const char *p = path;
        while (*p) {
            if (isdigit((unsigned char)*p) && isdigit((unsigned char)*(p+1))) {
                sequential_count++;
                break;
            }
            p++;
        }
    }

    // If more than 50% of files have sequential numbering
    return (sequential_count * 2 > metadata->num_files);
}

/**
 * Calculate heuristic score
 */
static int calculate_heuristic_score(const torrent_metadata_t *metadata) {
    if (!metadata) return 0;

    int score = 0;
    int video_count = 0;
    int image_count = 0;
    uint64_t total_size = 0;

    // Analyze files
    for (int i = 0; i < metadata->num_files; i++) {
        const char *path = metadata->files[i].path;
        if (!path) continue;

        if (is_video_file(path)) {
            video_count++;
        }
        if (is_image_file(path)) {
            image_count++;
        }
        total_size += metadata->files[i].size_bytes;

        // Check for suspicious keywords in file paths (lower weight than direct name match)
        int weight;
        if (check_keywords_in_text(path, &weight)) {
            score += (weight >= 5) ? 2 : 1;
        }
    }

    // Heuristic 1: Video file extensions (+2 points)
    if (video_count >= 1) {
        score += 2;
    }

    // Heuristic 2: Many sequential files (+1 point)
    if (has_sequential_files(metadata)) {
        score += 1;
    }

    // Heuristic 3: Long torrent name (+1 point)
    if (metadata->name && strlen(metadata->name) > 100) {
        score += 1;
    }

    // Heuristic 4: High special character density (+1 point)
    if (metadata->name) {
        float ratio = calculate_special_char_ratio(metadata->name);
        if (ratio > 0.20f) {  // More than 20% special characters
            score += 1;
        }
    }

    // Heuristic 5: Large video files (+1 point)
    if (video_count > 0 && total_size > 1073741824ULL) {  // > 1GB
        score += 1;
    }

    // Heuristic 6: Many small images (+2 points)
    if (image_count > 50) {
        score += 2;
    }

    return score;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

int porn_filter_init(const char *keyword_file_path) {
    if (filter_state.initialized) {
        log_msg(LOG_WARN, "Porn filter already initialized");
        return 0;
    }

    if (!keyword_file_path) {
        log_msg(LOG_ERROR, "Keyword file path is NULL");
        return -1;
    }

    // Load keywords
    if (load_keywords(keyword_file_path) < 0) {
        log_msg(LOG_WARN, "Failed to load keywords, filter may be less effective");
        // Continue anyway - regex and heuristics still work
    }

    // Initialize regex patterns
    if (init_regex_patterns() < 0) {
        log_msg(LOG_ERROR, "Failed to initialize regex patterns");
        // Cleanup and return error
        porn_filter_cleanup();
        return -1;
    }

    filter_state.initialized = 1;
    log_msg(LOG_DEBUG, "Porn filter initialized successfully");
    return 0;
}

void porn_filter_cleanup(void) {
    if (!filter_state.initialized) return;

    // Free keyword hash table
    keyword_entry_t *entry, *tmp;
    HASH_ITER(hh, filter_state.keyword_hash, entry, tmp) {
        HASH_DEL(filter_state.keyword_hash, entry);
        free(entry->keyword);
        free(entry);
    }
    filter_state.keyword_hash = NULL;

    // Cleanup regex patterns
    cleanup_regex_patterns();

    filter_state.initialized = 0;
    log_msg(LOG_DEBUG, "Porn filter cleaned up");
}

int porn_filter_check(const torrent_metadata_t *metadata) {
    if (!filter_state.initialized || !metadata) {
        return 0;
    }

    // Update statistics
    pthread_mutex_lock(&filter_state.stats_mutex);
    filter_state.stats.total_checked++;
    pthread_mutex_unlock(&filter_state.stats_mutex);

    int weight = 0;

    // Layer 1: Keyword matching in torrent name
    if (metadata->name && check_keywords_in_text(metadata->name, &weight)) {
        if (weight >= filter_state.keyword_threshold) {
            pthread_mutex_lock(&filter_state.stats_mutex);
            filter_state.stats.filtered_by_keyword++;
            filter_state.stats.total_filtered++;
            pthread_mutex_unlock(&filter_state.stats_mutex);

            log_msg(LOG_DEBUG, "Filtered by keyword (name): %s (weight=%d)",
                    metadata->name, weight);
            return 1;
        }
    }

    // Layer 1: Keyword matching in file paths
    for (int i = 0; i < metadata->num_files; i++) {
        if (metadata->files[i].path && check_keywords_in_text(metadata->files[i].path, &weight)) {
            if (weight >= filter_state.keyword_threshold) {
                pthread_mutex_lock(&filter_state.stats_mutex);
                filter_state.stats.filtered_by_keyword++;
                filter_state.stats.total_filtered++;
                pthread_mutex_unlock(&filter_state.stats_mutex);

                log_msg(LOG_DEBUG, "Filtered by keyword (file): %s (weight=%d)",
                        metadata->name, weight);
                return 1;
            }
        }
    }

    // Layer 2: Regex pattern matching in torrent name
    if (metadata->name && check_regex_patterns(metadata->name, &weight)) {
        if (weight >= filter_state.regex_threshold) {
            pthread_mutex_lock(&filter_state.stats_mutex);
            filter_state.stats.filtered_by_regex++;
            filter_state.stats.total_filtered++;
            pthread_mutex_unlock(&filter_state.stats_mutex);

            log_msg(LOG_DEBUG, "Filtered by regex (name): %s (weight=%d)",
                    metadata->name, weight);
            return 1;
        }
    }

    // Layer 2: Regex pattern matching in file paths
    for (int i = 0; i < metadata->num_files; i++) {
        if (metadata->files[i].path && check_regex_patterns(metadata->files[i].path, &weight)) {
            if (weight >= filter_state.regex_threshold) {
                pthread_mutex_lock(&filter_state.stats_mutex);
                filter_state.stats.filtered_by_regex++;
                filter_state.stats.total_filtered++;
                pthread_mutex_unlock(&filter_state.stats_mutex);

                log_msg(LOG_DEBUG, "Filtered by regex (file): %s (weight=%d)",
                        metadata->name, weight);
                return 1;
            }
        }
    }

    // Layer 3: Heuristic scoring
    int heuristic_score = calculate_heuristic_score(metadata);
    if (heuristic_score >= filter_state.heuristic_threshold) {
        pthread_mutex_lock(&filter_state.stats_mutex);
        filter_state.stats.filtered_by_heuristic++;
        filter_state.stats.total_filtered++;
        pthread_mutex_unlock(&filter_state.stats_mutex);

        log_msg(LOG_DEBUG, "Filtered by heuristic: %s (score=%d)",
                metadata->name ? metadata->name : "(unnamed)", heuristic_score);
        return 1;
    }

    // All checks passed - allow torrent
    return 0;
}

void porn_filter_get_stats(porn_filter_stats_t *stats) {
    if (!stats) return;

    pthread_mutex_lock(&filter_state.stats_mutex);
    *stats = filter_state.stats;
    pthread_mutex_unlock(&filter_state.stats_mutex);
}

void porn_filter_set_thresholds(int keyword_threshold, int regex_threshold, int heuristic_threshold) {
    filter_state.keyword_threshold = keyword_threshold;
    filter_state.regex_threshold = regex_threshold;
    filter_state.heuristic_threshold = heuristic_threshold;

    log_msg(LOG_DEBUG, "Porn filter thresholds updated: keyword=%d, regex=%d, heuristic=%d",
            keyword_threshold, regex_threshold, heuristic_threshold);
}
