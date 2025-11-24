#include "torrent_search.h"
#include "dht_crawler.h"
#include "uthash.h"
#include <stdlib.h>
#include <string.h>
#include <strings.h>  /* For strcasecmp */
#include <ctype.h>
#include <stdio.h>
#include <regex.h>

/* ============================================================================
 * Data Structures
 * ============================================================================ */

/**
 * Hash table entry for strip keywords (using uthash)
 */
typedef struct strip_keyword {
    char *keyword;              // Normalized keyword (lowercase)
    size_t length;              // Length of keyword (for performance)
    UT_hash_handle hh;
} strip_keyword_t;

/**
 * Global module state
 */
static struct {
    strip_keyword_t *keyword_hash;     // Hash table of keywords to strip
    regex_t season_regex;              // Regex for season/episode patterns
    int season_regex_compiled;
    int initialized;
} search_state = {
    .keyword_hash = NULL,
    .season_regex_compiled = 0,
    .initialized = 0
};

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * Normalize string to lowercase (in-place)
 */
static void normalize_to_lower(char *str) {
    if (!str) return;
    for (char *p = str; *p; p++) {
        *p = tolower((unsigned char)*p);
    }
}

/**
 * Trim leading and trailing whitespace (in-place)
 */
static void trim_whitespace(char *str) {
    if (!str || *str == '\0') return;

    // Trim leading
    char *start = str;
    while (isspace((unsigned char)*start)) start++;

    // Handle all whitespace case
    if (*start == '\0') {
        *str = '\0';
        return;
    }

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
 * Collapse multiple spaces into single spaces (in-place)
 */
static void collapse_spaces(char *str) {
    if (!str) return;

    char *read = str;
    char *write = str;
    int prev_space = 1;  // Start as if previous was space to trim leading

    while (*read) {
        if (isspace((unsigned char)*read)) {
            if (!prev_space) {
                *write++ = ' ';
                prev_space = 1;
            }
        } else {
            *write++ = *read;
            prev_space = 0;
        }
        read++;
    }
    *write = '\0';
}

/**
 * Check if a string is a year (1900-2099)
 */
static int is_year(const char *str, size_t len) {
    if (len != 4) return 0;

    for (size_t i = 0; i < 4; i++) {
        if (!isdigit((unsigned char)str[i])) return 0;
    }

    int year = atoi(str);
    return (year >= 1900 && year <= 2099);
}

/* ============================================================================
 * Keyword Management
 * ============================================================================ */

/**
 * Add keyword to hash table
 */
static int add_strip_keyword(const char *keyword) {
    if (!keyword || strlen(keyword) == 0) return -1;

    // Check if already exists
    strip_keyword_t *entry;
    HASH_FIND_STR(search_state.keyword_hash, keyword, entry);
    if (entry) {
        return 0;  // Already exists
    }

    // Create new entry
    entry = malloc(sizeof(strip_keyword_t));
    if (!entry) return -1;

    entry->keyword = strdup(keyword);
    if (!entry->keyword) {
        free(entry);
        return -1;
    }
    entry->length = strlen(keyword);

    HASH_ADD_KEYPTR(hh, search_state.keyword_hash, entry->keyword, entry->length, entry);
    return 0;
}

/**
 * Load keywords from file
 * Format: one keyword per line
 * Lines starting with # are comments
 */
static int load_strip_keywords(const char *file_path) {
    FILE *fp = fopen(file_path, "r");
    if (!fp) {
        log_msg(LOG_ERROR, "Failed to open strip keyword file: %s", file_path);
        return -1;
    }

    char line[256];
    int loaded = 0;

    while (fgets(line, sizeof(line), fp)) {
        // Remove newline
        line[strcspn(line, "\r\n")] = '\0';

        // Trim whitespace
        trim_whitespace(line);

        // Skip empty lines and comments
        if (strlen(line) == 0 || line[0] == '#') {
            continue;
        }

        // Normalize keyword to lowercase
        normalize_to_lower(line);

        if (strlen(line) > 0) {
            if (add_strip_keyword(line) == 0) {
                loaded++;
            }
        }
    }

    fclose(fp);
    log_msg(LOG_DEBUG, "Loaded %d strip keywords from %s", loaded, file_path);
    return 0;
}

/* ============================================================================
 * Text Processing Functions
 * ============================================================================ */

/**
 * Replace dots and underscores with spaces
 */
static void replace_separators(char *str) {
    if (!str) return;

    for (char *p = str; *p; p++) {
        if (*p == '.' || *p == '_') {
            *p = ' ';
        }
    }
}

/**
 * Remove content within square brackets [...]
 */
static void remove_brackets(char *str) {
    if (!str) return;

    char *read = str;
    char *write = str;
    int depth = 0;

    while (*read) {
        if (*read == '[') {
            depth++;
        } else if (*read == ']' && depth > 0) {
            depth--;
        } else if (depth == 0) {
            *write++ = *read;
        }
        read++;
    }
    *write = '\0';
}

/**
 * Process parentheses: preserve years, remove other content
 */
static void process_parentheses(char *str) {
    if (!str) return;

    char *result = malloc(strlen(str) + 1);
    if (!result) return;

    char *read = str;
    char *write = result;

    while (*read) {
        if (*read == '(') {
            // Find closing parenthesis
            char *close = strchr(read + 1, ')');
            if (close) {
                size_t content_len = close - read - 1;
                char *content = read + 1;

                // Check if content is a year
                // Trim spaces from content for checking
                while (content_len > 0 && isspace((unsigned char)*content)) {
                    content++;
                    content_len--;
                }
                while (content_len > 0 && isspace((unsigned char)content[content_len - 1])) {
                    content_len--;
                }

                if (is_year(content, content_len)) {
                    // Preserve the year (without parentheses)
                    for (size_t i = 0; i < content_len; i++) {
                        *write++ = content[i];
                    }
                    *write++ = ' ';
                }
                // Skip the parentheses content
                read = close + 1;
                continue;
            }
        }
        *write++ = *read++;
    }
    *write = '\0';

    strcpy(str, result);
    free(result);
}

/**
 * Truncate at season/episode markers
 * Patterns: S01, S01E01, Season 1, Episode 1, etc.
 */
static void truncate_at_season(char *str) {
    if (!str || !search_state.season_regex_compiled) return;

    regmatch_t match;
    if (regexec(&search_state.season_regex, str, 1, &match, 0) == 0) {
        // Found a match - truncate at the start of it
        if (match.rm_so > 0) {
            str[match.rm_so] = '\0';
        }
    }
}

/**
 * Remove a single keyword from string (case-insensitive, whole word)
 * Returns 1 if keyword was found and removed, 0 otherwise
 */
static int remove_keyword_once(char *str, const char *keyword, size_t keyword_len) {
    if (!str || !keyword || keyword_len == 0) return 0;

    char *lower_str = strdup(str);
    if (!lower_str) return 0;
    normalize_to_lower(lower_str);

    char *found = strstr(lower_str, keyword);
    if (!found) {
        free(lower_str);
        return 0;
    }

    // Calculate position in original string
    size_t pos = found - lower_str;

    // Check for word boundary before
    int word_start = (pos == 0) ||
                     !isalnum((unsigned char)str[pos - 1]);

    // Check for word boundary after
    int word_end = (str[pos + keyword_len] == '\0') ||
                   !isalnum((unsigned char)str[pos + keyword_len]);

    free(lower_str);

    if (word_start && word_end) {
        // Remove the keyword by shifting string
        memmove(str + pos, str + pos + keyword_len, strlen(str + pos + keyword_len) + 1);
        return 1;
    }

    return 0;
}

/**
 * Strip all keywords from string
 */
static void strip_keywords(char *str) {
    if (!str || !search_state.keyword_hash) return;

    strip_keyword_t *entry, *tmp;

    // Iterate through all keywords and remove them
    HASH_ITER(hh, search_state.keyword_hash, entry, tmp) {
        // Remove all occurrences of this keyword
        while (remove_keyword_once(str, entry->keyword, entry->length)) {
            // Keep removing until no more found
        }
    }
}

/**
 * Remove common patterns like hyphens at end (release group separator)
 */
static void clean_trailing_separators(char *str) {
    if (!str) return;

    size_t len = strlen(str);
    while (len > 0) {
        char c = str[len - 1];
        if (c == '-' || c == ' ' || c == '(' || c == '[') {
            str[--len] = '\0';
        } else {
            break;
        }
    }
}

/* ============================================================================
 * Public API
 * ============================================================================ */

int torrent_search_init(const char *keyword_file_path) {
    if (search_state.initialized) {
        log_msg(LOG_WARN, "Torrent search module already initialized");
        return 0;
    }

    if (!keyword_file_path) {
        log_msg(LOG_ERROR, "Keyword file path is NULL");
        return -1;
    }

    // Load keywords
    if (load_strip_keywords(keyword_file_path) < 0) {
        log_msg(LOG_WARN, "Failed to load strip keywords, using empty keyword list");
        // Continue anyway - basic processing still works
    }

    // Compile season/episode regex
    // Matches: S01, S01E01, Season 1, Episode 1, Series 1, etc.
    const char *season_pattern =
        "(^|[^a-zA-Z])(S[0-9]+|Season[[:space:]]*[0-9]+|Series[[:space:]]*[0-9]+|Episode[[:space:]]*[0-9]+|Ep[[:space:]]*[0-9]+)";

    int ret = regcomp(&search_state.season_regex, season_pattern,
                     REG_EXTENDED | REG_ICASE);
    if (ret == 0) {
        search_state.season_regex_compiled = 1;
    } else {
        char errbuf[256];
        regerror(ret, &search_state.season_regex, errbuf, sizeof(errbuf));
        log_msg(LOG_WARN, "Failed to compile season regex: %s", errbuf);
        search_state.season_regex_compiled = 0;
    }

    search_state.initialized = 1;
    log_msg(LOG_DEBUG, "Torrent search module initialized");
    return 0;
}

void torrent_search_cleanup(void) {
    if (!search_state.initialized) return;

    // Free keyword hash table
    strip_keyword_t *entry, *tmp;
    HASH_ITER(hh, search_state.keyword_hash, entry, tmp) {
        HASH_DEL(search_state.keyword_hash, entry);
        free(entry->keyword);
        free(entry);
    }
    search_state.keyword_hash = NULL;

    // Free regex
    if (search_state.season_regex_compiled) {
        regfree(&search_state.season_regex);
        search_state.season_regex_compiled = 0;
    }

    search_state.initialized = 0;
    log_msg(LOG_DEBUG, "Torrent search module cleaned up");
}

/**
 * Extract the movie/TV show title from a torrent name.
 *
 * Processing steps:
 * 1. Replace dots and underscores with spaces
 * 2. Remove content in square brackets [...]
 * 3. Preserve years in parentheses (1900-2099), remove other tech info
 * 4. Truncate at season/episode markers (S01E02, Season 1, etc.)
 * 5. Strip all loaded keywords (case-insensitive)
 * 6. Collapse multiple spaces and trim
 *
 * @param torrent_name The raw torrent name
 * @return Newly allocated string with extracted title (caller must free),
 *         or NULL on error
 */
char* extract_media_title(const char *torrent_name) {
    if (!torrent_name) {
        return NULL;
    }

    // Make a working copy
    char *result = strdup(torrent_name);
    if (!result) {
        return NULL;
    }

    // Step 1: Replace dots and underscores with spaces
    replace_separators(result);

    // Step 2: Remove content in square brackets
    remove_brackets(result);

    // Step 3: Process parentheses (preserve years, remove tech info)
    process_parentheses(result);

    // Step 4: Truncate at season/episode markers
    truncate_at_season(result);

    // Step 5: Strip all keywords
    if (search_state.initialized) {
        strip_keywords(result);
    }

    // Step 6: Clean up
    collapse_spaces(result);
    clean_trailing_separators(result);
    trim_whitespace(result);

    return result;
}
