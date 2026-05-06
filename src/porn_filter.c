#include "porn_filter.h"
#include "dht_crawler.h"
#include "uthash.h"
#include <string.h>
#include <strings.h>  /* For strcasecmp */
#include <ctype.h>
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
 * Global filter state
 */
static struct {
    keyword_entry_t *keyword_hash;            // Standalone filter keywords
    keyword_entry_t *xxx_cooccurrence_hash;   // Keywords active only when "xxx" is also present
    pthread_mutex_t stats_mutex;              // Protect stats
    porn_filter_stats_t stats;
    int initialized;

    // Configurable thresholds
    int keyword_threshold;
} filter_state = {
    .keyword_hash = NULL,
    .xxx_cooccurrence_hash = NULL,
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
    .stats = {0},
    .initialized = 0,
    .keyword_threshold = 8,
};

/* Snapshot of keyword pointers (built lazily on first enumeration call) */
static const char **filter_kw_snapshot = NULL;
static int filter_kw_snapshot_count = 0;

static void invalidate_keyword_snapshot(void) {
    free(filter_kw_snapshot);
    filter_kw_snapshot = NULL;
    filter_kw_snapshot_count = 0;
}

static void build_keyword_snapshot(void) {
    if (filter_kw_snapshot) return;
    int n = HASH_COUNT(filter_state.keyword_hash);
    if (n <= 0) {
        filter_kw_snapshot_count = 0;
        return;
    }
    filter_kw_snapshot = malloc((size_t)n * sizeof(*filter_kw_snapshot));
    if (!filter_kw_snapshot) { filter_kw_snapshot_count = 0; return; }
    int i = 0;
    keyword_entry_t *e, *t;
    HASH_ITER(hh, filter_state.keyword_hash, e, t) {
        filter_kw_snapshot[i++] = e->keyword;
    }
    filter_kw_snapshot_count = i;
}

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

/* ============================================================================
 * Layer 1: Hash Set Keyword Matching
 * ============================================================================ */

/**
 * Generic helper: add keyword to any hash table
 */
static int add_keyword_to_table(keyword_entry_t **table, const char *keyword, int weight) {
    if (!keyword || strlen(keyword) == 0) return -1;

    keyword_entry_t *entry;
    HASH_FIND_STR(*table, keyword, entry);
    if (entry) {
        if (weight > entry->weight) entry->weight = weight;
        return 0;
    }

    entry = malloc(sizeof(keyword_entry_t));
    if (!entry) return -1;

    entry->keyword = strdup(keyword);
    if (!entry->keyword) { free(entry); return -1; }
    entry->weight = weight;

    HASH_ADD_KEYPTR(hh, *table, entry->keyword, strlen(entry->keyword), entry);
    return 0;
}

static int add_keyword(const char *keyword, int weight) {
    return add_keyword_to_table(&filter_state.keyword_hash, keyword, weight);
}

static int add_xxx_keyword(const char *keyword, int weight) {
    return add_keyword_to_table(&filter_state.xxx_cooccurrence_hash, keyword, weight);
}

/**
 * Free all entries in a keyword hash table
 */
static void free_keyword_table(keyword_entry_t **table) {
    keyword_entry_t *entry, *tmp;
    HASH_ITER(hh, *table, entry, tmp) {
        HASH_DEL(*table, entry);
        free(entry->keyword);
        free(entry);
    }
    *table = NULL;
}

/**
 * Load keywords from file.
 * Format: keyword[:weight]
 * Section headers: [standalone] or [xxx_cooccurrence]
 * Lines starting with # are comments.
 *
 * [standalone]      – keywords that trigger the filter on their own (default)
 * [xxx_cooccurrence] – keywords that only trigger when "xxx" is also in the title
 */
static int load_keywords(const char *file_path) {
    FILE *fp = fopen(file_path, "r");
    if (!fp) {
        log_msg(LOG_ERROR, "Failed to open keyword file: %s", file_path);
        return -1;
    }

    char line[256];
    int loaded_standalone = 0;
    int loaded_cooccurrence = 0;
    int in_xxx_section = 0;  // 0 = standalone (default), 1 = xxx_cooccurrence

    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\r\n")] = '\0';
        trim_whitespace(line);

        if (strlen(line) == 0 || line[0] == '#') continue;

        // Section header detection
        if (line[0] == '[') {
            if (strcasecmp(line, "[xxx_cooccurrence]") == 0) {
                in_xxx_section = 1;
            } else if (strcasecmp(line, "[standalone]") == 0) {
                in_xxx_section = 0;
            }
            continue;
        }

        // Parse keyword:weight
        char *colon = strchr(line, ':');
        int weight = 5;
        if (colon) {
            *colon = '\0';
            weight = atoi(colon + 1);
            if (weight < 1) weight = 1;
            if (weight > 10) weight = 10;
        }

        normalize_string(line);
        trim_whitespace(line);

        if (strlen(line) > 0) {
            if (in_xxx_section) {
                if (add_xxx_keyword(line, weight) == 0) loaded_cooccurrence++;
            } else {
                if (add_keyword(line, weight) == 0) loaded_standalone++;
            }
        }
    }

    fclose(fp);
    log_msg(LOG_DEBUG, "Loaded %d standalone + %d xxx co-occurrence keywords from %s",
            loaded_standalone, loaded_cooccurrence, file_path);
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
 * Layer 1.5: xxx Co-occurrence Check
 * ============================================================================ */

/**
 * Returns 1 if the text contains "xxx" as a whole word AND at least one
 * keyword from the [xxx_cooccurrence] section also appears as a whole word.
 */
static int check_xxx_cooccurrence(const char *text) {
    if (!text || !filter_state.xxx_cooccurrence_hash) return 0;

    char *lower = strdup(text);
    if (!lower) return 0;
    normalize_string(lower);

    /* Whole-word scan for "xxx" */
    int has_xxx = 0;
    const char *p = lower;
    while ((p = strstr(p, "xxx")) != NULL) {
        int before_ok = (p == lower) || !isalnum((unsigned char)*(p - 1));
        int after_ok  = !isalnum((unsigned char)*(p + 3));
        if (before_ok && after_ok) { has_xxx = 1; break; }
        p += 3;
    }

    if (!has_xxx) { free(lower); return 0; }

    /* Whole-word scan for each co-occurrence keyword */
    keyword_entry_t *entry, *tmp;
    HASH_ITER(hh, filter_state.xxx_cooccurrence_hash, entry, tmp) {
        const char *t = entry->keyword;
        size_t tlen = strlen(t);
        const char *q = lower;
        while ((q = strstr(q, t)) != NULL) {
            int b = (q == lower) || !isalnum((unsigned char)*(q - 1));
            int a = !isalnum((unsigned char)*(q + tlen));
            if (b && a) { free(lower); return 1; }
            q += tlen;
        }
    }

    free(lower);
    return 0;
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

    filter_state.initialized = 1;
    log_msg(LOG_DEBUG, "Porn filter initialized successfully");
    return 0;
}

void porn_filter_cleanup(void) {
    if (!filter_state.initialized) return;

    free_keyword_table(&filter_state.keyword_hash);
    free_keyword_table(&filter_state.xxx_cooccurrence_hash);
    invalidate_keyword_snapshot();

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

    // Layer 1.5: xxx co-occurrence check (torrent name only)
    if (metadata->name && check_xxx_cooccurrence(metadata->name)) {
        pthread_mutex_lock(&filter_state.stats_mutex);
        filter_state.stats.filtered_by_keyword++;
        filter_state.stats.total_filtered++;
        pthread_mutex_unlock(&filter_state.stats_mutex);

        log_msg(LOG_DEBUG, "Filtered by xxx co-occurrence: %s", metadata->name);
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

void porn_filter_set_thresholds(int keyword_threshold) {
    filter_state.keyword_threshold = keyword_threshold;

    log_msg(LOG_DEBUG, "Porn filter thresholds updated: keyword=%d", keyword_threshold);
}

int porn_filter_get_keyword_count(void) {
    if (!filter_state.initialized) return 0;
    build_keyword_snapshot();
    return filter_kw_snapshot_count;
}

const char *porn_filter_get_keyword(int index) {
    if (!filter_state.initialized) return NULL;
    build_keyword_snapshot();
    if (index < 0 || index >= filter_kw_snapshot_count) return NULL;
    return filter_kw_snapshot[index];
}
