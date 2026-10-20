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

/* ============================================================================
 * Tracker prefix stripping
 * ============================================================================ */

/*
 * Rows scraped from tracker index pages often carry the site's hostname in front
 * of the real release name, separated by a dash/pipe/colon:
 *
 *   "www.UIndex.org    -    Dune.Prophecy.S01E01..."  -> "Dune.Prophecy.S01E01..."
 *   "www.Torrenting.com - The.Pitt.S01E05..."         -> "The.Pitt.S01E05..."
 *   "rarbg.com - Some.Movie.2020..."                  -> "Some.Movie.2020..."
 *
 * Left in place those host tokens become part of the extracted title (they are
 * before the release pivot, so the pivot path keeps them) and poison the IMDb /
 * DHT query built from it.
 */

/* TLDs that tracker hostnames actually use. A bare (non-www) dotted prefix is
 * only treated as a host when its last label is one of these, so ordinary
 * dot-separated titles followed by a dash ("The.Hunger.Games - Mockingjay...")
 * are never mistaken for a hostname. Deliberately absent: TLDs that are also
 * common English words in this position (one, top, live, life, fun, pro) - they
 * would turn "The.One - ..." into "...", and a wrong title is worse than a
 * dirty one. */
static const char *const TRACKER_TLDS[] = {
    "com", "net", "org", "info", "biz", "tv", "cc", "io", "xyz", "ws",
    "eu", "uk", "ru", "su", "nl", "br", "ch", "pl", "mx", "bz", "nz",
    "hk", "kr", "jp", "cn", "tw", "vn", "sg", "dk", "fi", "cz", "sk",
    "hu", "ro", "bg", "gr", "pt", "rs", "ba", "mk", "al", "tr", "il",
    "ae", "sa", "ir", "pk", "bd", "lk", "np", "th", "kh", "mm", "mn",
    "gg", "cd", "club", "site", "online", "link", "download", "stream",
    "torrent", "torrents", "lol", "vip", "to", "re", "se",
};

/** True for a hostname label character (letters and digits). */
static int is_host_label_char(char c) {
    return isalnum((unsigned char)c) != 0;
}

/**
 * Length of the hostname label at `p`, 0 when there is none.
 *
 * Labels may contain inner hyphens ("www.torrent-bay.org"), otherwise the
 * parser would stop at the first hyphen and strip only half of the host. A
 * trailing hyphen is not part of the label - it is the separator that follows
 * the host ("www.site.org- Title").
 */
static size_t host_label_len(const char *p) {
    size_t n = 0;
    while (is_host_label_char(p[n]) || p[n] == '-') n++;

    while (n > 0 && p[n - 1] == '-') n--;

    for (size_t i = 0; i < n; i++) {
        if (is_host_label_char(p[i])) return n;  /* reject an all-hyphen run */
    }
    return 0;
}

/** True when `label` is one of the tracker TLDs above. */
static int is_tracker_tld(const char *label, size_t len) {
    for (size_t i = 0; i < sizeof(TRACKER_TLDS) / sizeof(TRACKER_TLDS[0]); i++) {
        if (strlen(TRACKER_TLDS[i]) == len &&
            strncasecmp(label, TRACKER_TLDS[i], len) == 0) {
            return 1;
        }
    }
    return 0;
}

/**
 * Match a leading tracker hostname prefix.
 *
 * Grammar: 2-6 dot-separated labels (letters, digits, inner hyphens), then
 * optional blanks, then a separator ('-', '|' or ':'), then blanks. The host is
 * accepted when either
 *   - its first label is "www" (never a title word in this position), or
 *   - its last label is a known tracker TLD.
 * Any label that is itself a year (1900-2099) disqualifies the match: that is a
 * title, not a hostname.
 *
 * @param str Raw torrent name
 * @return Pointer to the first character after the prefix, or NULL when the
 *         string does not start with a tracker hostname
 */
static const char *match_tracker_prefix(const char *str) {
    if (!str) return NULL;

    const char *p = str;
    while (*p == ' ' || *p == '\t') p++;

    const char *labels[6];
    size_t lens[6];
    size_t nlabels = 0;

    while (nlabels < sizeof(labels) / sizeof(labels[0])) {
        size_t n = host_label_len(p);
        if (n == 0) break;

        labels[nlabels] = p;
        lens[nlabels] = n;
        nlabels++;
        p += n;

        if (*p != '.') break;
        p++;  /* consume the dot and try to read the next label */
    }

    if (nlabels < 2) return NULL;
    if (p - str > 48) return NULL;  /* a hostname is short; a title is not */

    for (size_t i = 0; i < nlabels; i++) {
        if (is_year(labels[i], lens[i])) return NULL;
    }

    int is_www = lens[0] == 3 && strncasecmp(labels[0], "www", 3) == 0;
    if (!is_www && !is_tracker_tld(labels[nlabels - 1], lens[nlabels - 1])) {
        return NULL;
    }

    while (*p == ' ' || *p == '\t') p++;
    if (*p != '-' && *p != '|' && *p != ':') return NULL;
    p++;

    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0') return NULL;  /* nothing left to keep */

    return p;
}

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
 * Returns 1 if the string was cut, 0 otherwise.
 */
static int truncate_at_season(char *str) {
    if (!str || !search_state.season_regex_compiled) return 0;

    regmatch_t match;
    if (regexec(&search_state.season_regex, str, 1, &match, 0) == 0) {
        // Found a match - truncate at the start of it
        if (match.rm_so > 0) {
            str[match.rm_so] = '\0';
            return 1;
        }
    }

    return 0;
}

/**
 * Check if a token is a quality marker (480p, 720P, 1080p, 2160p, uhd, hdr, ...)
 */
static int is_quality_token(const char *tok, size_t len) {
    // Resolution token: 3-4 digits followed by 'p'
    if (len == 4 || len == 5) {
        int all_digits = 1;
        for (size_t i = 0; i + 1 < len; i++) {
            if (!isdigit((unsigned char)tok[i])) {
                all_digits = 0;
                break;
            }
        }
        if (all_digits && (tok[len - 1] == 'p' || tok[len - 1] == 'P')) {
            return 1;
        }
    }

    // Bare quality tags
    static const char *const tags[] = {"uhd", "hdr", "hdr10", "4k", "fhd", "qhd"};
    for (size_t i = 0; i < sizeof(tags) / sizeof(tags[0]); i++) {
        if (strlen(tags[i]) == len && strncasecmp(tok, tags[i], len) == 0) {
            return 1;
        }
    }

    return 0;
}

/**
 * Cut the processed name at the first scene-release pivot (grammar only).
 *
 * Pivot ladder over the space-split string, first hit wins:
 *   1. Year pivot: cut inclusive after the first year token (1900-2099) at
 *      index > 0. The index guard keeps year-shaped titles intact
 *      ("1917.2019.1080p" -> "1917 2019"); a title word that also looks like
 *      a year ("Blade.Runner.2049.2017") resolves to the FIRST such token.
 *   2. Season pivot (only when no year token at index > 0 fired): cut at the
 *      season/episode marker, exactly like the legacy path.
 *   3. Quality pivot (no year, no season): cut exclusively before the first
 *      quality token ("Some.Movie.1080p.BluRay" -> "Some Movie").
 *
 * Post-pivot junk (codec/source/release-group suffixes) therefore never
 * survives. Returns 1 if a pivot cut the string, 0 otherwise.
 */
static int cut_at_release_pivot(char *str) {
    if (!str || *str == '\0') return 0;

    size_t index = 0;
    char *p = str;

    // Pass 1: year pivot
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        char *tok = p;
        while (*p && !isspace((unsigned char)*p)) p++;
        size_t toklen = (size_t)(p - tok);

        if (index > 0 && is_year(tok, toklen)) {
            // Cut inclusive: the year is part of the title
            *p = '\0';
            return 1;
        }
        index++;
    }

    // Pass 2: season pivot
    if (truncate_at_season(str)) return 1;

    // Pass 3: quality pivot
    p = str;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        char *tok = p;
        while (*p && !isspace((unsigned char)*p)) p++;
        size_t toklen = (size_t)(p - tok);

        if (is_quality_token(tok, toklen)) {
            // Cut exclusive: the quality tag is not part of the title
            *tok = '\0';
            return 1;
        }
    }

    return 0;
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

/**
 * Remove leading separator runs left behind by a removed prefix.
 *
 * A bracketed tracker prefix is dropped by remove_brackets() but the separator
 * that followed it stays:
 *   "[ www.torrenting.com ] - Carlin.At.Carnegie..." -> "- Carlin At Carnegie..."
 * Only separators that are followed by a blank are trimmed, so titles that
 * legitimately start with one ("-30-") are left alone.
 */
static void clean_leading_separators(char *str) {
    if (!str) return;

    char *p = str;
    while (*p == ' ' || *p == '\t') p++;

    while ((*p == '-' || *p == '|' || *p == ':') &&
           (p[1] == ' ' || p[1] == '\t')) {
        p++;
        while (*p == ' ' || *p == '\t') p++;
    }

    if (p != str) {
        memmove(str, p, strlen(p) + 1);
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
 * 0. Strip a leading tracker hostname prefix ("www.UIndex.org    -    ",
 *    "rarbg.com - ") together with its separator
 * 1. Replace dots and underscores with spaces
 * 2. Remove content in square brackets [...]
 * 3. Preserve years in parentheses (1900-2099), remove other tech info
 * 4. Cut at the first scene-release pivot (release grammar only):
 *      year pivot    "Some.Movie.2020.1080p..."  -> "Some Movie 2020"
 *      season pivot  "Show.Name.S01E05.1080p..." -> "Show Name"
 *      quality pivot "Some.Movie.1080p.BluRay..." -> "Some Movie"
 *    On a pivot path no keyword deletion runs - the grammar alone decides
 *    where the title ends.
 * 5. Legacy path (no pivot): truncate at season/episode markers, then strip
 *    all loaded keywords (case-insensitive)
 * 6. Collapse multiple spaces, drop leading/trailing separators and trim
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

    // Step 0: Strip a leading tracker hostname prefix. Runs before the
    // separators are rewritten, while the dots that identify a host are intact.
    const char *body = match_tracker_prefix(result);
    if (body) {
        memmove(result, body, strlen(body) + 1);
    }

    // Step 1: Replace dots and underscores with spaces
    replace_separators(result);

    // Step 2: Remove content in square brackets
    remove_brackets(result);

    // Step 3: Process parentheses (preserve years, remove tech info)
    process_parentheses(result);

    // Step 4: Cut at the first scene-release pivot (year -> season -> quality).
    // Pivot paths trust the release grammar only: strip_keywords() must NOT run
    // there, otherwise a keyword could delete part of the title itself.
    // Accepted edges (deliberately not chased): language tags before the pivot
    // survive ("Movie.Name.FRENCH.1080p" -> "Movie Name FRENCH") and source
    // tags preceding a quality token survive ("...WEB-DL.1080p" -> "...WEB-DL");
    // IMDb and the crawler FTS both tolerate one extra token. Junk after the
    // pivot (codecs, sources, release-group suffixes) never survives.
    if (!cut_at_release_pivot(result)) {
        // Step 5 (legacy path): truncate at season/episode markers, strip keywords
        (void)truncate_at_season(result);
        if (search_state.initialized) {
            strip_keywords(result);
        }
    }

    // Step 6: Clean up (both paths)
    collapse_spaces(result);
    clean_leading_separators(result);
    clean_trailing_separators(result);
    trim_whitespace(result);

    return result;
}
