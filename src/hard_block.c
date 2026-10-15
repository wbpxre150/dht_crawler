#include "hard_block.h"
#include "dht_crawler.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Same shape as the porn filter's keyword loader, minus the hash table and the
 * co-occurrence layers: the list is small, reviewed, and matched on every
 * ingest, so a plain array of strdup'ed terms is enough. */
#define HB_MAX_TERMS 1024
#define HB_LINE_CAP 256
#define HB_FILE_DEFAULT "hard_block.txt"

static struct {
    char *terms[HB_MAX_TERMS];
    int count;
    int loaded;        /* the file was read (possibly empty) */
} hb;

static void hb_trim(char *str) {
    char *start = str;
    while (*start == ' ' || *start == '\t') start++;
    if (start != str) {
        memmove(str, start, strlen(start) + 1);
    }

    size_t len = strlen(str);
    while (len > 0) {
        char c = str[len - 1];
        if (c != ' ' && c != '\t' && c != '\r' && c != '\n') break;
        str[--len] = '\0';
    }
}

/* Terms are compared against the raw torrent name, so they are stored folded
 * to lowercase once at load time. */
static void hb_lowercase(char *str) {
    for (char *p = str; *p; p++) {
        *p = (char)tolower((unsigned char)*p);
    }
}

static int hb_add_term(const char *term) {
    if (hb.count >= HB_MAX_TERMS) return -1;

    char *copy = strdup(term);
    if (!copy) return -1;
    hb_lowercase(copy);
    hb.terms[hb.count++] = copy;
    return 0;
}

void hard_block_init(const char *file_path) {
    if (hb.loaded) {
        log_msg(LOG_DEBUG, "Hard block list already loaded (%d terms); ignoring second init", hb.count);
        return;
    }

    const char *path = (file_path && file_path[0]) ? file_path : HB_FILE_DEFAULT;

    FILE *fp = fopen(path, "r");
    if (!fp) {
        /* One warning only, and never fatal: the list is a safety net, not a
         * startup requirement. */
        log_msg(LOG_WARN, "Hard block list %s is not readable; hard block disabled", path);
        return;
    }

    char line[HB_LINE_CAP];
    while (fgets(line, sizeof(line), fp)) {
        hb_trim(line);
        if (line[0] == '\0' || line[0] == '#') continue;
        if (hb_add_term(line) != 0) {
            log_msg(LOG_WARN, "Hard block list %s exceeds %d terms; ignoring the rest", path, HB_MAX_TERMS);
            break;
        }
    }

    fclose(fp);
    hb.loaded = 1;
    log_msg(LOG_INFO, "Loaded %d hard block terms from %s", hb.count, path);
}

void hard_block_cleanup(void) {
    for (int i = 0; i < hb.count; i++) {
        free(hb.terms[i]);
        hb.terms[i] = NULL;
    }
    hb.count = 0;
    hb.loaded = 0;
}

/* Case-insensitive substring scan; names are arbitrary length (unlike the
 * 256-byte keyword-file lines), so no fixed temporary buffer is used. */
static int hb_contains_ci(const char *haystack, const char *needle) {
    size_t needle_len = strlen(needle);
    if (needle_len == 0) return 0;

    for (const char *p = haystack; *p; p++) {
        size_t i = 0;
        while (i < needle_len && p[i] &&
               tolower((unsigned char)p[i]) == (unsigned char)needle[i]) {
            i++;
        }
        if (i == needle_len) return 1;
    }
    return 0;
}

int hard_block_check(const char *name) {
    if (!name || hb.count == 0) return 0;

    for (int i = 0; i < hb.count; i++) {
        if (hb_contains_ci(name, hb.terms[i])) {
            log_msg(LOG_DEBUG, "Hard block match: term '%s' in '%s'", hb.terms[i], name);
            return 1;
        }
    }
    return 0;
}
