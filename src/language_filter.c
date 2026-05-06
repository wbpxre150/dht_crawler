#include "language_filter.h"
#include "dht_crawler.h"
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static struct {
    int threshold;
    int initialized;
    pthread_mutex_t stats_mutex;
    language_filter_stats_t stats;
} lf_state = {
    .threshold = 0,
    .initialized = 0,
    .stats_mutex = PTHREAD_MUTEX_INITIALIZER,
    .stats = {0},
};

/* ---- UTF-8 decoder ---- */

static uint32_t utf8_next(const unsigned char **p) {
    unsigned char c = **p;
    if (c < 0x80) { (*p)++; return c; }
    if (c < 0xC2) { (*p)++; return 0xFFFD; }
    if (c < 0xE0) {
        if (((*p)[1] & 0xC0) != 0x80) { (*p)++; return 0xFFFD; }
        uint32_t cp = ((uint32_t)(c & 0x1F) << 6) | ((*p)[1] & 0x3F);
        *p += 2; return cp;
    }
    if (c < 0xF0) {
        if (((*p)[1] & 0xC0) != 0x80 || ((*p)[2] & 0xC0) != 0x80) { (*p)++; return 0xFFFD; }
        uint32_t cp = ((uint32_t)(c & 0x0F) << 12) | ((uint32_t)((*p)[1] & 0x3F) << 6) | ((*p)[2] & 0x3F);
        *p += 3; return cp;
    }
    if (c < 0xF8) {
        if (((*p)[1] & 0xC0) != 0x80 || ((*p)[2] & 0xC0) != 0x80 || ((*p)[3] & 0xC0) != 0x80) { (*p)++; return 0xFFFD; }
        uint32_t cp = ((uint32_t)(c & 0x07) << 18) | ((uint32_t)((*p)[1] & 0x3F) << 12) |
                      ((uint32_t)((*p)[2] & 0x3F) << 6) | ((*p)[3] & 0x3F);
        *p += 4; return cp;
    }
    (*p)++; return 0xFFFD;
}

static int is_non_latin(uint32_t cp) {
    if (cp <= 0x036F) return 0;           /* Latin Basic + Extended A/B + combining diacritics */
    if (cp >= 0x1E00 && cp <= 0x1EFF) return 0; /* Latin Extended Additional */
    if (cp >= 0xA720 && cp <= 0xA7FF) return 0; /* Remaining Latin forms */
    if (cp <= 0x0040) return 0;           /* ASCII control + punctuation — neutral */
    if (cp >= 0x2000 && cp <= 0x206F) return 0; /* General punctuation — neutral */
    if (cp >= 0x2100 && cp <= 0x214F) return 0; /* Letterlike symbols — neutral */
    return 1;
}

static void count_script_chars(const char *str, int *latin_out, int *non_latin_out) {
    const unsigned char *p = (const unsigned char *)str;
    int latin = 0, non_latin = 0;
    while (*p) {
        uint32_t cp = utf8_next(&p);
        if (cp == 0xFFFD) continue;
        if (cp <= 0x0040) continue;
        if (cp >= 0x2000 && cp <= 0x214F) continue;
        if (is_non_latin(cp)) non_latin++;
        else latin++;
    }
    *latin_out = latin;
    *non_latin_out = non_latin;
}

/* ---- Public API ---- */

int language_filter_init(int non_latin_threshold) {
    if (non_latin_threshold < 0) non_latin_threshold = 0;
    if (non_latin_threshold > 100) non_latin_threshold = 100;
    lf_state.threshold = non_latin_threshold;
    lf_state.initialized = 1;
    if (non_latin_threshold == 0)
        log_msg(LOG_DEBUG, "Language filter: non-Latin check disabled");
    else
        log_msg(LOG_DEBUG, "Language filter initialized (non-Latin threshold=%d%%)", non_latin_threshold);
    return 0;
}

void language_filter_cleanup(void) {
    lf_state.initialized = 0;
    lf_state.threshold = 0;
}

int language_filter_check(const torrent_metadata_t *metadata) {
    if (!lf_state.initialized || lf_state.threshold <= 0 || !metadata) return 0;

    pthread_mutex_lock(&lf_state.stats_mutex);
    lf_state.stats.total_checked++;
    pthread_mutex_unlock(&lf_state.stats_mutex);

    int total_latin = 0, total_non_latin = 0;

    if (metadata->name) {
        int l, nl;
        count_script_chars(metadata->name, &l, &nl);
        total_latin += l;
        total_non_latin += nl;
    }

    if (metadata->files) {
        for (int i = 0; i < metadata->num_files; i++) {
            if (metadata->files[i].path) {
                int l, nl;
                count_script_chars(metadata->files[i].path, &l, &nl);
                total_latin += l;
                total_non_latin += nl;
            }
        }
    }

    int total = total_latin + total_non_latin;
    if (total == 0) return 0;

    if ((total_non_latin * 100 / total) >= lf_state.threshold) {
        pthread_mutex_lock(&lf_state.stats_mutex);
        lf_state.stats.filtered_by_non_latin++;
        lf_state.stats.total_filtered++;
        pthread_mutex_unlock(&lf_state.stats_mutex);

        log_msg(LOG_DEBUG, "Language filter: filtered non-Latin (>%d%%): %s",
                lf_state.threshold, metadata->name ? metadata->name : "(no name)");
        return 1;
    }
    return 0;
}

void language_filter_get_stats(language_filter_stats_t *stats) {
    if (!stats) return;
    pthread_mutex_lock(&lf_state.stats_mutex);
    *stats = lf_state.stats;
    pthread_mutex_unlock(&lf_state.stats_mutex);
}
