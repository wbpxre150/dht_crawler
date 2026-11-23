/* tparse name matchers (C11, libc only) — Stage 1b.
 *
 * Hand-written ordered scanners over a normalised working copy of the
 * release name. No regex library; port of the release-name priorities from
 * Sonarr's Parser/ParserCommon and guessit's episode/source/screen-size
 * matchers, reduced to the shapes the packet lists.
 *
 * Normalisation: case-folded, '.', '_' and whitespace become ' ', the CJK
 * brackets U+3010/U+3011 become '[' / ']'. Length never grows (a 3-byte
 * bracket maps to 1 byte), so the working copy fits in n+1 bytes.
 *
 * Matcher order (first decisive match wins for tv_kind):
 *   1 SxxEyy / NNxNN cluster (+ multi-episode suffixes)
 *   2 anime (leading [Group] plus absolute " - NN" or parenthesised SxxEyy)
 *   3 season-only ("Season 1", bare "S01")
 *   4 season range ("S01-S05", "Season 1-5", "S01 S05")
 *   5 complete token
 *   6 daily ("2025.01.02", "20250102")
 *   7 multi-episode (several numbers in one cluster)
 *   8 resolution (independent scanner)
 *   9 source (independent scanner, internal-only field)
 *  10 "Episode N" / "Ep N" / "E N" words
 * Flags are OR'ed in from token/byte scans (see tp_name_scan).
 */
#include "tparse_internal.h"

#include <stdlib.h>
#include <string.h>

/* --- ASCII helpers (the working copy is already case-folded) ----------- */

static int a_isdigit(char c) { return c >= '0' && c <= '9'; }
static int a_isalpha(char c) { return c >= 'a' && c <= 'z'; }
static int a_isalnum(char c) { return a_isdigit(c) || a_isalpha(c); }

/* --- normalisation ----------------------------------------------------- */

/* Case-fold and normalise `name` into `w` (capacity cap >= n+1). */
static size_t norm_work(const char *name, size_t n, char *w, size_t cap) {
    size_t o = 0;
    for (size_t i = 0; i < n && o + 1 < cap; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == 0xE3 && i + 2 < n) {
            unsigned char c1 = (unsigned char)name[i + 1];
            unsigned char c2 = (unsigned char)name[i + 2];
            if (c1 == 0x80 && (c2 == 0x90 || c2 == 0x91)) { /* U+3010/U+3011 */
                w[o++] = (c2 == 0x90) ? '[' : ']';
                i += 2;
                continue;
            }
        }
        if (c >= 'A' && c <= 'Z') {
            w[o++] = (char)(c - 'A' + 'a');
        } else if (c == '.' || c == '_' || c == ' ' || c == '\t' || c == '\r' ||
                   c == '\n' || c == '\v' || c == '\f') {
            w[o++] = ' ';
        } else {
            w[o++] = (char)c;
        }
    }
    w[o] = '\0';
    return o;
}

/* --- token / run helpers ---------------------------------------------- */

static int run_digits(const char *w, size_t n, size_t i) {
    int c = 0;
    while (i + (size_t)c < n && a_isdigit(w[i + (size_t)c])) c++;
    return c;
}

static long num_at(const char *w, size_t i, int len) {
    long v = 0;
    for (int k = 0; k < len; k++) v = v * 10 + (w[i + (size_t)k] - '0');
    return v;
}

static int tok_start(const char *w, size_t i) { return i == 0 || !a_isalnum(w[i - 1]); }

static int tok_is(const char *w, size_t n, size_t i, const char *lit) {
    size_t l = strlen(lit);
    if (i + l > n) return 0;
    if (memcmp(w + i, lit, l) != 0) return 0;
    return tok_start(w, i) && (i + l >= n || !a_isalnum(w[i + l]));
}

static int tok_has(const char *w, size_t n, const char *lit) {
    size_t l = strlen(lit);
    for (size_t i = 0; i + l <= n; i++) {
        if (tok_is(w, n, i, lit)) return 1;
    }
    return 0;
}

/* Skip up to `maxsep` separator characters (' ' or '-'). */
static size_t skip_seps(const char *w, size_t n, size_t i, int maxsep) {
    int c = 0;
    while (i < n && (w[i] == ' ' || w[i] == '-') && c < maxsep) {
        i++;
        c++;
    }
    return i;
}

/* True when only separators follow `pos` to the end of the string. */
static int tail_is_end(const char *w, size_t n, size_t pos) {
    while (pos < n && !a_isalnum(w[pos])) pos++;
    return pos >= n;
}

/* An episode number: 1-3 digits, not glued to a digit, and not the suffix of
 * a resolution token ("720p", "1080i"). */
static int ep_digits_ok(const char *w, size_t n, size_t i, int len) {
    if (len < 1 || len > 3) return 0;
    size_t j = i + (size_t)len;
    if (j < n && a_isdigit(w[j])) return 0;
    if (j < n && (w[j] == 'p' || w[j] == 'i')) return 0;
    return 1;
}

/* "x264"/"x265"/"x266" is a codec, never season+episode. */
static int is_codec_ep(int ep) { return ep == 264 || ep == 265 || ep == 266; }

/* The x/× separator: 'x' or UTF-8 U+00D7. */
static size_t match_x_sep(const char *w, size_t n, size_t i, int *is_x) {
    if (i < n && w[i] == 'x') {
        *is_x = 1;
        return i + 1;
    }
    if (i + 1 < n && (unsigned char)w[i] == 0xC3 && (unsigned char)w[i + 1] == 0x97) {
        *is_x = 1;
        return i + 2;
    }
    return i;
}

/* --- matcher 1: SxxEyy / NNxNN cluster --------------------------------- */

#define SE_MAX_EPS 32

typedef struct {
    int season;
    int eps[SE_MAX_EPS];
    int n_eps;
    size_t pos; /* index of the cluster start (for the anime paren check) */
} se_cluster_t;

static int match_se(const char *w, size_t n, size_t i, se_cluster_t *out) {
    size_t j;
    int is_x = 0;
    int season;

    if (w[i] == 's') {
        /* Season digits must not be preceded by a digit, and a second 's'
         * ("SS01") is a season pack, not a season+episode cluster. */
        if (i > 0 && (a_isdigit(w[i - 1]) || w[i - 1] == 's')) return 0;
        int sd = run_digits(w, n, i + 1);
        if (sd < 1 || sd > 4) return 0;
        season = (int)num_at(w, i + 1, sd);
        size_t k = skip_seps(w, n, i + 1 + (size_t)sd, 2);
        if (k < n && w[k] == 'e') {
            k++;
        } else {
            k = match_x_sep(w, n, k, &is_x);
            if (k == skip_seps(w, n, i + 1 + (size_t)sd, 2)) return 0;
        }
        j = skip_seps(w, n, k, 1);
    } else if (a_isdigit(w[i])) {
        /* Bare NNxNN ("1x05"): both sides digit-glued, season 1-2 digits. */
        if (i > 0 && a_isalnum(w[i - 1])) return 0;
        int sd = run_digits(w, n, i);
        if (sd < 1 || sd > 2) return 0;
        season = (int)num_at(w, i, sd);
        size_t k = match_x_sep(w, n, i + (size_t)sd, &is_x);
        if (!is_x) return 0;
        j = k;
    } else {
        return 0;
    }

    int ed = run_digits(w, n, j);
    if (!ep_digits_ok(w, n, j, ed)) return 0;
    int ep = (int)num_at(w, j, ed);
    if (is_x && is_codec_ep(ep)) return 0;

    out->season = season;
    out->pos = i;
    out->n_eps = 0;
    out->eps[out->n_eps++] = ep;
    size_t pos = j + (size_t)ed;

    /* Multi-episode suffixes on the same season: S01E05E06, S01E05-06,
     * S01E05 06 (dot/space within 3 chars), S01E05-06-07. */
    for (;;) {
        size_t m = pos;
        int seps = 0;
        while (m < n && (w[m] == ' ' || w[m] == '-') && seps < 3) {
            m++;
            seps++;
        }
        /* e/x extensions must be glued to the digits ("S01E05E06"); an
         * 'x' behind a separator is a tag/codec ("S01E05.x264"). */
        int via_x = 0;
        size_t after_x = m;
        if (seps == 0) {
            if (m < n && w[m] == 'e') {
                after_x = skip_seps(w, n, m + 1, 1);
            } else {
                size_t p = match_x_sep(w, n, m, &via_x);
                if (via_x) after_x = skip_seps(w, n, p, 1);
            }
        }
        if (after_x == m) { /* plain separated digit continuation */
            if (seps == 0 || m >= n || !a_isdigit(w[m])) break;
        }
        if (after_x >= n) break;
        int d2 = run_digits(w, n, after_x);
        /* Extensions are 2-3 digits, so a fractional episode ("12.5" →
         * "12 5") never turns its fraction into a second episode. */
        if (d2 < 2 || d2 > 3) break;
        if (!ep_digits_ok(w, n, after_x, d2)) break;
        int ep2 = (int)num_at(w, after_x, d2);
        if (via_x && is_codec_ep(ep2)) break;
        size_t e2 = after_x + (size_t)d2;
        /* A spaced continuation must not be another tag ("S01E05 10bit"). */
        if (!via_x && seps > 0 && e2 < n && a_isalpha(w[e2])) break;
        if (out->n_eps >= SE_MAX_EPS) break;
        out->eps[out->n_eps++] = ep2;
        pos = e2;
    }
    return 1;
}

static int scan_se(const char *w, size_t n, se_cluster_t *out) {
    for (size_t i = 0; i < n; i++) {
        if (w[i] == 's' || a_isdigit(w[i])) {
            if (match_se(w, n, i, out)) return 1;
        }
    }
    return 0;
}

/* --- matcher 2: anime --------------------------------------------------- */

/* Leading "[Group]" bracket token; *title gets the title start. */
static int anime_group(const char *w, size_t n, size_t *title) {
    if (n == 0 || w[0] != '[') return 0;
    size_t close = 0;
    for (size_t i = 1; i < n && i <= 40; i++) {
        if (w[i] == ']') {
            close = i;
            break;
        }
        if (w[i] == '[') return 0;
    }
    if (close < 2) return 0; /* "[]" is not a group */
    size_t j = skip_seps(w, n, close + 1, 2);
    if (j >= n) return 0;
    *title = j;
    return 1;
}

/* Absolute episode numbers: " - NN" with 2-3 digits (1 digit accepted when
 * trailing), optional "v2" version suffix. Fractions ("12.5") normalise to
 * "12 5" and contribute their integer part only. */
static int anime_absolute(const char *w, size_t n, size_t from, int *eps, int cap) {
    int found = 0;
    for (size_t i = from; i < n; i++) {
        if (w[i] != '-') continue;
        size_t j = i + 1;
        while (j < n && w[j] == ' ') j++;
        int d = run_digits(w, n, j);
        if (d < 1 || d > 3) continue;
        if (!ep_digits_ok(w, n, j, d)) continue;
        size_t e = j + (size_t)d;
        if (d < 2 && !tail_is_end(w, n, e)) continue; /* short form: trailing only */
        if (e < n && a_isalpha(w[e])) {
            /* " - 12v2" is a version suffix; anything else is not an episode. */
            int vd = (w[e] == 'v') ? run_digits(w, n, e + 1) : 0;
            if (vd < 1 || vd > 2) continue;
        }
        if (found < cap) eps[found++] = (int)num_at(w, j, d);
    }
    return found;
}

/* --- matchers 3/4: season-only and season range ------------------------ */

static const char *const k_season_words[] = {
    "season", "saison", "serie", "series", "stagione", "staffel", "temporada", NULL
};

/* Match a season token at `i`; *ranged is set for "S01-S05"/"Season 1-5". */
static int match_season_at(const char *w, size_t n, size_t i, int *lo, int *hi, int *ranged) {
    size_t j;
    int d = 0;

    if (tok_start(w, i)) {
        for (int k = 0; k_season_words[k] != NULL; k++) {
            size_t l = strlen(k_season_words[k]);
            if (!tok_is(w, n, i, k_season_words[k])) continue;
            j = skip_seps(w, n, i + l, 2);
            d = run_digits(w, n, j);
            if (d >= 1 && d <= 2) goto got;
            return 0;
        }
    }
    if (w[i] != 's' || (i > 0 && a_isalnum(w[i - 1]))) return 0;
    j = i + 1;
    d = run_digits(w, n, j);
    if (d < 1 || d > 2) return 0;

got:
    *lo = (int)num_at(w, j, d);
    *hi = *lo;
    *ranged = 0;
    char sep1 = w[j + (size_t)d];
    size_t p = skip_seps(w, n, j + (size_t)d, 2);
    if (p == j + (size_t)d || p >= n) return 1; /* no separator: not a range */
    if (w[p] == 's' && !a_isalnum(w[p - 1])) { /* "S01 S05" / "S01-S05" */
        int d2 = run_digits(w, n, p + 1);
        size_t e2 = p + 1 + (size_t)d2;
        if (d2 >= 1 && d2 <= 2 && (e2 >= n || !a_isalpha(w[e2]))) {
            int hi2 = (int)num_at(w, p + 1, d2);
            if (hi2 > *lo) {
                *hi = hi2;
                *ranged = 1;
            }
        }
        return 1;
    }
    /* Bare upper bound: only after '-', never "Season 1 10bit". */
    if (a_isdigit(w[p]) && sep1 == '-') {
        int d2 = run_digits(w, n, p);
        size_t e2 = p + (size_t)d2;
        if (d2 >= 1 && d2 <= 2 && (e2 >= n || !a_isalpha(w[e2]))) {
            int hi2 = (int)num_at(w, p, d2);
            if (hi2 > *lo) {
                *hi = hi2;
                *ranged = 1;
            }
        }
    }
    return 1;
}

static int scan_season(const char *w, size_t n, int *lo, int *hi, int *ranged) {
    for (size_t i = 0; i < n; i++) {
        if (match_season_at(w, n, i, lo, hi, ranged)) return 1;
    }
    return 0;
}

/* --- matcher 6: daily --------------------------------------------------- */

/* YYYY.MM.DD, YYYY-MM-DD, YYYY MM DD (separator must repeat) and the glued
 * YYYYMMDD. Strict year 1900-2099, month 01-12, day 01-31. */
static int scan_daily(const char *w, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (!a_isdigit(w[i])) continue;
        if (i > 0 && a_isalnum(w[i - 1])) continue;
        int d = run_digits(w, n, i);
        if (d == 8) {
            long v = num_at(w, i, 8);
            int y = (int)(v / 10000);
            int mo = (int)((v / 100) % 100);
            int da = (int)(v % 100);
            if (y >= 1900 && y <= 2099 && mo >= 1 && mo <= 12 && da >= 1 && da <= 31 &&
                tail_is_end(w, n, i + 8)) {
                return 1;
            }
        } else if (d == 4) {
            int y = (int)num_at(w, i, 4);
            if (y < 1900 || y > 2099) continue;
            size_t p = i + 4;
            if (p >= n) continue;
            char sep1 = w[p];
            if (sep1 != ' ' && sep1 != '-') continue;
            size_t q = p + 1;
            if (run_digits(w, n, q) != 2) continue;
            int mo = (int)num_at(w, q, 2);
            size_t r = q + 2;
            if (r >= n || w[r] != sep1) continue; /* back-reference */
            size_t t = r + 1;
            if (run_digits(w, n, t) != 2) continue;
            int da = (int)num_at(w, t, 2);
            if (mo < 1 || mo > 12 || da < 1 || da > 31) continue;
            if (t + 2 < n && a_isalnum(w[t + 2])) continue; /* "10bit" */
            return 1;
        }
    }
    return 0;
}

/* --- matcher 8: resolution --------------------------------------------- */

/* Documented outputs only: 576→480, 900→720, 1440→1080, 4k/uhd→2160. */
static int map_res(long v) {
    switch (v) {
        case 480: case 576: return 480;
        case 720: case 900: return 720;
        case 1080: case 1440: return 1080;
        case 2160: return 2160;
        default: return 0;
    }
}

static int scan_resolution(const char *w, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (a_isdigit(w[i]) && tok_start(w, i)) {
            int d = run_digits(w, n, i);
            if (d == 3 || d == 4) {
                int r = map_res(num_at(w, i, d));
                if (r != 0) {
                    size_t j = i + (size_t)d;
                    if (j < n && w[j] == ' ') j++; /* "1080 p" */
                    if (j < n && (w[j] == 'p' || w[j] == 'i') &&
                        (j + 1 >= n || !a_isalnum(w[j + 1]))) {
                        return r;
                    }
                }
            }
        }
        if (w[i] == '4' && tok_start(w, i) && i + 1 < n && w[i + 1] == 'k' &&
            (i + 2 >= n || !a_isalnum(w[i + 2]))) {
            return 2160;
        }
        if (tok_is(w, n, i, "uhd")) return 2160;
    }
    return 0;
}

/* --- matcher 9: source -------------------------------------------------- */

static int single_src(const char *w, size_t s, size_t e) {
    static const struct { const char *p; int v; } t[] = {
        {"hdtv", TP_SRC_HDTV},   {"pdtv", TP_SRC_PDTV},     {"webdl", TP_SRC_WEBDL},
        {"webrip", TP_SRC_WEBRIP}, {"webcap", TP_SRC_WEBCAP}, {"web", TP_SRC_WEB},
        {"bluray", TP_SRC_BLURAY}, {"brrip", TP_SRC_BRRIP}, {"bdrip", TP_SRC_BDRIP},
        {"dvdrip", TP_SRC_DVDRIP}, {"dvd", TP_SRC_DVD},     {"hdrip", TP_SRC_HDRIP},
        {"cam", TP_SRC_CAM},     {"ts", TP_SRC_TS},         {"telesync", TP_SRC_TELESYNC},
        {"tc", TP_SRC_TC},       {"vhs", TP_SRC_VHS},       {"vod", TP_SRC_VOD},
        {"dsr", TP_SRC_DSR},     {NULL, 0}
    };
    for (int k = 0; t[k].p != NULL; k++) {
        size_t l = strlen(t[k].p);
        if (e - s == l && memcmp(w + s, t[k].p, l) == 0) return t[k].v;
    }
    return TP_SRC_NONE;
}

/* Whole-word, case-insensitive, hyphen-insensitive: two adjacent tokens are
 * joined ("WEB-DL" / "WEB DL" both become "webdl"). */
static int scan_source(const char *w, size_t n) {
    size_t i = 0;
    while (i < n) {
        while (i < n && !a_isalnum(w[i])) i++;
        if (i >= n) break;
        size_t s1 = i;
        while (i < n && a_isalnum(w[i])) i++;
        size_t e1 = i;
        size_t j = i;
        while (j < n && !a_isalnum(w[j])) j++;
        if (j < n) {
            size_t s2 = j;
            while (j < n && a_isalnum(w[j])) j++;
            char buf[64];
            size_t l1 = e1 - s1;
            size_t l2 = j - s2;
            if (l1 + l2 < sizeof buf) {
                memcpy(buf, w + s1, l1);
                memcpy(buf + l1, w + s2, l2);
                buf[l1 + l2] = '\0';
                int v = single_src(buf, 0, l1 + l2);
                if (v != TP_SRC_NONE) return v;
            }
        }
        int v = single_src(w, s1, e1);
        if (v != TP_SRC_NONE) return v;
    }
    return TP_SRC_NONE;
}

/* --- matcher 10: episode words ----------------------------------------- */

static int scan_ep_words(const char *w, size_t n, int *lo, int *hi) {
    for (size_t i = 0; i < n; i++) {
        size_t l = 0;
        if (tok_start(w, i)) {
            if (tok_is(w, n, i, "episode")) l = 7;
            else if (tok_is(w, n, i, "episodes")) l = 8;
            else if (tok_is(w, n, i, "episodio")) l = 8;
            else if (tok_is(w, n, i, "ep")) l = 2;
            else if (tok_is(w, n, i, "e")) l = 1;
        }
        if (l == 0) continue;
        size_t p = skip_seps(w, n, i + l, 1);
        if (p == i + l && (p >= n || !a_isdigit(w[p]))) continue;
        if (p >= n) continue;
        int d = run_digits(w, n, p);
        if (!ep_digits_ok(w, n, p, d)) continue;
        *lo = (int)num_at(w, p, d);
        *hi = 0;
        size_t q = skip_seps(w, n, p + (size_t)d, 1);
        if (q > p + (size_t)d) {
            int d2 = run_digits(w, n, q);
            if (ep_digits_ok(w, n, q, d2)) *hi = (int)num_at(w, q, d2);
        }
        return 1;
    }
    return 0;
}

/* --- flags -------------------------------------------------------------- */

/* TP_F_JAV_CODE: whole hyphen-separated token ^[A-Za-z]{2,5}-\d{3,5}$. */
static void flag_jav(const char *w, size_t n, unsigned *flags) {
    for (size_t i = 0; i < n; i++) {
        if (!a_isalpha(w[i])) continue;
        if (i > 0 && (a_isalnum(w[i - 1]) || w[i - 1] == '-')) continue;
        int a = 0;
        while (i + (size_t)a < n && a_isalpha(w[i + (size_t)a])) a++;
        if (a < 2 || a > 5) continue;
        size_t p = i + (size_t)a;
        if (p >= n || w[p] != '-') continue;
        int d = run_digits(w, n, p + 1);
        if (d < 3 || d > 5) continue;
        size_t e = p + 1 + (size_t)d;
        if (e < n && (a_isalnum(w[e]) || w[e] == '-')) continue;
        *flags |= TP_F_JAV_CODE;
        return;
    }
}

/* TP_F_XXX_TOKEN: the whole token "xxx"; never a substring. */
static void flag_xxx(const char *w, size_t n, unsigned *flags) {
    for (size_t i = 0; i + 3 <= n; i++) {
        if (memcmp(w + i, "xxx", 3) != 0) continue;
        if (i > 0 && a_isalnum(w[i - 1])) continue;
        if (i + 3 < n && a_isalnum(w[i + 3])) continue;
        *flags |= TP_F_XXX_TOKEN;
        return;
    }
}

static int utf8_decode(const char *s, size_t n, size_t *i, unsigned *cp) {
    unsigned char c = (unsigned char)s[*i];
    if (c < 0x80) {
        *cp = c;
        (*i)++;
        return 1;
    }
    int extra;
    unsigned v;
    if ((c & 0xE0) == 0xC0) {
        extra = 1;
        v = c & 0x1Fu;
    } else if ((c & 0xF0) == 0xE0) {
        extra = 2;
        v = c & 0x0Fu;
    } else if ((c & 0xF8) == 0xF0) {
        extra = 3;
        v = c & 0x07u;
    } else {
        (*i)++;
        return 0;
    }
    if (*i + (size_t)extra >= n) {
        (*i)++;
        return 0;
    }
    for (int k = 1; k <= extra; k++) {
        unsigned char cc = (unsigned char)s[*i + (size_t)k];
        if ((cc & 0xC0) != 0x80) {
            (*i)++;
            return 0;
        }
        v = (v << 6) | (cc & 0x3Fu);
    }
    *i += (size_t)extra + 1;
    *cp = v;
    return 1;
}

/* TP_F_CJK (U+2E80..9FFF, U+F900..FAFF, U+20000..3FFFF) and TP_F_CYRILLIC
 * (U+0400..04FF) are byte/codepoint scans of the raw name. */
static void flag_scripts(const char *s, size_t n, unsigned *flags) {
    size_t i = 0;
    while (i < n) {
        size_t before = i;
        unsigned cp = 0;
        if (!utf8_decode(s, n, &i, &cp)) {
            if (i == before) i++;
            continue;
        }
        if ((cp >= 0x2E80u && cp <= 0x9FFFu) || (cp >= 0xF900u && cp <= 0xFAFFu) ||
            (cp >= 0x20000u && cp <= 0x3FFFFu)) {
            *flags |= TP_F_CJK;
        }
        if (cp >= 0x400u && cp <= 0x4FFu) *flags |= TP_F_CYRILLIC;
    }
}

static int ci_eq_at(const char *s, size_t n, size_t i, const char *lit) {
    size_t l = strlen(lit);
    if (i + l > n) return 0;
    for (size_t k = 0; k < l; k++) {
        unsigned char c = (unsigned char)s[i + k];
        if (c >= 'A' && c <= 'Z') c = (unsigned char)(c - 'A' + 'a');
        if (c != (unsigned char)lit[k]) return 0;
    }
    return 1;
}

static const char *const k_urltail[] = {
    ".com", ".net", ".org", ".ru", ".to", ".xyz", ".fun", ".url", ".top", ".icu",
    ".live", NULL
};

/* TP_F_URL on the RAW name: normalisation would turn '.' into ' '. */
static void flag_url(const char *s, size_t n, unsigned *flags) {
    size_t i = 0;
    while (i < n && !a_isalnum((char)(unsigned char)s[i])) i++;
    if (ci_eq_at(s, n, i, "www.")) {
        *flags |= TP_F_URL;
        return;
    }
    for (size_t k = 0; k + 7 <= n; k++) {
        if (ci_eq_at(s, n, k, "http://")) {
            *flags |= TP_F_URL;
            return;
        }
    }
    for (size_t k = 0; k + 8 <= n; k++) {
        if (ci_eq_at(s, n, k, "https://")) {
            *flags |= TP_F_URL;
            return;
        }
    }
    for (size_t k = 1; k < n; k++) {
        if (s[k] != '.' || !a_isalnum((char)(unsigned char)s[k - 1])) continue;
        for (int t = 0; k_urltail[t] != NULL; t++) {
            size_t l = strlen(k_urltail[t]);
            if (!ci_eq_at(s, n, k, k_urltail[t])) continue;
            size_t e = k + l;
            if (e >= n || !a_isalnum((char)(unsigned char)s[e])) {
                *flags |= TP_F_URL;
                return;
            }
        }
    }
}

/* Sonarr rejects: a bare 24-32 character alphanumeric hash ("parse
 * nothing"). */
static int is_hash_name(const char *w, size_t n) {
    size_t i = 0;
    size_t j = n;
    while (i < n && !a_isalnum(w[i])) i++;
    while (j > i && !a_isalnum(w[j - 1])) j--;
    size_t len = j - i;
    if (len < 24 || len > 32) return 0;
    for (size_t k = i; k < j; k++) {
        char c = w[k];
        if (!a_isdigit(c) && !(c >= 'a' && c <= 'f')) return 0;
    }
    return 1;
}

/* A "[Group]" whose raw text looks like a URL/domain is a release group tag,
 * not an anime fansub group ("[ www.site.com ]"). */
static int group_looks_like_url(const char *raw, size_t n) {
    if (n == 0 || raw[0] != '[') return 0;
    for (size_t i = 1; i < n && i <= 40; i++) {
        if (raw[i] == ']') return 0;
        if (raw[i] == '.') return 1;
        if ((raw[i] == 'w' || raw[i] == 'W') && ci_eq_at(raw, n, i, "www")) return 1;
    }
    return 0;
}

/* --- driver ------------------------------------------------------------- */

static void scan_impl(const char *name, const char *w, size_t n, tp_name_info_t *out) {
    out->cat_hint = TP_CAT_UNKNOWN;

    /* Sonarr-style hashed release: no tv_kind, no season/episode, no flags. */
    if (is_hash_name(w, n)) return;

    se_cluster_t se;
    se.n_eps = 0;
    se.pos = 0;

    /* 1: standard season+episode cluster. */
    int have_se = scan_se(w, n, &se);

    /* 2: anime — leading [Group] plus an absolute " - NN" or a parenthesised
     * SxxEyy. The category hint only flips to anime for those shapes. */
    size_t title = 0;
    int group = anime_group(w, n, &title) && !group_looks_like_url(name, strlen(name));
    int anime = 0;
    int anime_eps[SE_MAX_EPS];
    int n_anime = 0;
    if (group && !have_se) n_anime = anime_absolute(w, n, title, anime_eps, SE_MAX_EPS);
    if (group) {
        int paren_se = have_se && se.pos > 0 && w[se.pos - 1] == '(';
        if (n_anime > 0 || paren_se) anime = 1;
    }
    if (anime) out->cat_hint = TP_CAT_ANIME;

    /* 3/4: season-only and season range. */
    int season_lo = 0, season_hi = 0, ranged = 0;
    int have_season = scan_season(w, n, &season_lo, &season_hi, &ranged);

    /* 5: complete token. */
    int complete = tok_has(w, n, "complete");

    /* 6: daily. */
    int daily = scan_daily(w, n);

    /* 10: "Episode N" / "Ep N" / "E N". */
    int ew_lo = 0, ew_hi = 0;
    int have_ew = scan_ep_words(w, n, &ew_lo, &ew_hi);

    /* fields: first decisive match wins */
    if (have_se) {
        out->season_lo = se.season;
        out->season_hi = se.season;
        out->episode_lo = se.eps[0];
        out->episode_hi = se.eps[se.n_eps - 1];
    } else if (anime && n_anime > 0) {
        out->season_lo = 0;
        out->season_hi = 0;
        out->episode_lo = anime_eps[0];
        out->episode_hi = anime_eps[n_anime - 1];
    } else {
        if (have_season) {
            out->season_lo = season_lo;
            out->season_hi = season_hi;
        }
        if (have_ew) {
            out->episode_lo = ew_lo;
            out->episode_hi = (ew_hi > ew_lo) ? ew_hi : ew_lo;
        }
    }

    /* tv_kind: episode shapes beat season-only shapes; complete+range is the
     * complete-series decision. */
    int kind = TP_TV_NONE;
    if (have_se) {
        kind = (se.n_eps >= 2) ? TP_TV_MULTI_EPISODE : TP_TV_EPISODE;
    } else if (anime && n_anime > 0) {
        kind = (n_anime >= 2) ? TP_TV_MULTI_EPISODE : TP_TV_EPISODE;
    } else if (have_ew) {
        kind = (ew_hi > ew_lo) ? TP_TV_MULTI_EPISODE : TP_TV_EPISODE;
    } else if (complete && ranged) {
        kind = TP_TV_COMPLETE_SERIES;
    } else if (ranged) {
        kind = TP_TV_MULTI_SEASON;
    } else if (have_season) {
        kind = TP_TV_SEASON_PACK;
    } else if (daily) {
        kind = TP_TV_DAILY;
    }
    out->tv_kind = kind;

    /* 8/9: independent scanners. */
    out->resolution = scan_resolution(w, n);
    out->source = scan_source(w, n);

    /* flags: token scans on the normalised copy, byte scans on the raw name. */
    if (complete) out->flags |= TP_F_COMPLETE;
    flag_jav(w, n, &out->flags);
    flag_xxx(w, n, &out->flags);
    flag_scripts(name, strlen(name), &out->flags);
    flag_url(name, strlen(name), &out->flags);
    /* TP_F_INCOMPLETE is Stage 1c. */
}

void tp_name_scan(const char *name, tp_name_info_t *out) {
    memset(out, 0, sizeof(*out));
    out->cat_hint = TP_CAT_UNKNOWN;
    if (name == NULL) return;

    size_t n = strlen(name);
    char stack[1024];
    char *w = stack;
    if (n + 1 > sizeof stack) {
        w = (char *)malloc(n + 1);
        if (w == NULL) return;
    }
    size_t wn = norm_work(name, n, w, n + 1);
    scan_impl(name, w, wn, out);
    if (w != stack) free(w);
}

/* --- file-path helpers (Stage 1c) --------------------------------------- */

/* Whole tokens that mark a sample, trailer or bonus clip. Matched against
 * the normalised path, so "Sample/", "sample.mkv" and "Extras" all hit. */
static const char *const k_exclude_tokens[] = {
    "sample", "extra", "extras", "bonus", "featurette", "featurettes",
    "proof", NULL
};

/* Basename without its trailing extension: the matchers that anchor on the
 * token being last (daily, short anime " - 5") would not fire otherwise. */
void tp_path_scan(const char *path, tp_name_info_t *out) {
    if (path == NULL) {
        tp_name_scan("", out);
        return;
    }
    const char *base = strrchr(path, '/');
    base = (base != NULL) ? base + 1 : path;
    const char *dot = strrchr(base, '.');
    size_t n = (dot != NULL && dot != base) ? (size_t)(dot - base) : strlen(base);

    char stack[1024];
    char *b = stack;
    if (n + 1 > sizeof stack) {
        b = (char *)malloc(n + 1);
        if (b == NULL) {
            tp_name_scan("", out);
            return;
        }
    }
    memcpy(b, base, n);
    b[n] = '\0';
    tp_name_scan(b, out);
    if (b != stack) free(b);
}

int tp_path_is_excluded(const char *path) {
    if (path == NULL) return 0;
    size_t n = strlen(path);
    char stack[1024];
    char *w = stack;
    if (n + 1 > sizeof stack) {
        w = (char *)malloc(n + 1);
        if (w == NULL) return 0;
    }
    size_t wn = norm_work(path, n, w, n + 1);
    int hit = 0;
    for (int k = 0; !hit && k_exclude_tokens[k] != NULL; k++) {
        hit = tok_has(w, wn, k_exclude_tokens[k]);
    }
    if (w != stack) free(w);
    return hit;
}

const char *tp_source_name(int source) {
    switch (source) {
        case TP_SRC_HDTV: return "hdtv";
        case TP_SRC_PDTV: return "pdtv";
        case TP_SRC_WEBDL: return "webdl";
        case TP_SRC_WEBRIP: return "webrip";
        case TP_SRC_WEBCAP: return "webcap";
        case TP_SRC_WEB: return "web";
        case TP_SRC_BLURAY: return "bluray";
        case TP_SRC_BRRIP: return "brrip";
        case TP_SRC_BDRIP: return "bdrip";
        case TP_SRC_DVDRIP: return "dvdrip";
        case TP_SRC_DVD: return "dvd";
        case TP_SRC_HDRIP: return "hdrip";
        case TP_SRC_CAM: return "cam";
        case TP_SRC_TS: return "ts";
        case TP_SRC_TELESYNC: return "telesync";
        case TP_SRC_TC: return "tc";
        case TP_SRC_VHS: return "vhs";
        case TP_SRC_VOD: return "vod";
        case TP_SRC_DSR: return "dsr";
        default: return "none";
    }
}
