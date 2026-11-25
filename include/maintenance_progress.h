#ifndef MAINTENANCE_PROGRESS_H
#define MAINTENANCE_PROGRESS_H

/*
 * Machine-readable maintenance progress ([MPROG]).
 *
 * Every maintenance one-shot (--check-database, --recover-database,
 * --rebuild-bloom-filter, --clear-failure-filter, --movie-kind-bench) reports
 * its progress on ONE line shape:
 *
 *   [MPROG] action=<route> phase=<slug> [done=<n> total=<n> pct=<n>] [k=v ...]
 *
 * The app parses it (MaintenanceProgress.kt), so the rules below are a
 * contract, not a preference:
 *
 *  - `action` is the flag name without "--", matching MaintenanceAction.route;
 *  - the literal [MPROG] token sits mid-line, behind log_msg()'s
 *    "[YYYY-MM-DD HH:MM:SS] [INFO] " prefix;
 *  - done/total are omitted when unknown, and pct is omitted ENTIRELY when
 *    there is no denominator (never pct=-1): a phase-only action reports a
 *    phase and nothing else;
 *  - the LAST line an action emits is "phase=done rc=<n>" or
 *    "phase=failed rc=<n>", so the UI can finalise without waiting for the
 *    process exit code.
 *
 * Channel: log_msg(LOG_INFO, ...) only. It is the one stream that is flushed
 * per line (log_msg() fflush()es stderr unconditionally) and the one the
 * one-shot runner reads. Bare printf() is forbidden here: stdout is fully
 * buffered when it is a pipe, so progress would arrive in 8 KiB bursts.
 *
 * Throttle: at most one line per 1 % AND per 1 second, whichever is later. The
 * app keeps only a 2000-line console ring buffer, and an unthrottled
 * --recover-database (286.7M torrent_files rows) would emit ~2,860 lines and
 * evict everything printed before it.
 *
 * Header-only on purpose: src/main.c and src/database.c
 * all need the emitters, and a new .c file would have to be added to the host
 * Makefile's explicit per-test source lists (and would only reach the Android
 * build through a CMake re-glob). static inline keeps one definition with no
 * build-system change; unused static inline functions do not warn.
 */

#include <time.h>

#include "dht_crawler.h"

/* Per-phase throttle state. Zero-initialised means "nothing emitted yet", so
 * the first call always emits: a phase that finishes quickly must still show
 * up instead of vanishing behind its own throttle. */
typedef struct {
    int have_last;
    long long last_done;
    int last_pct;
    time_t last_time;
} mprog_throttle_t;

/* Arm a throttle so the next call always emits. */
static inline void mprog_throttle_init(mprog_throttle_t *t) {
    t->have_last = 0;
    t->last_done = 0;
    t->last_pct = -1;
    t->last_time = 0;
}

/* done/total as 0..100, clamped; 0 when there is no denominator. */
static inline int mprog_pct(long long done, long long total) {
    long long pct;

    if (total <= 0 || done <= 0) {
        return 0;
    }
    pct = (done * 100) / total;
    if (pct > 100) {
        pct = 100;
    }
    return (int)pct;
}

/* Emit one [MPROG] line. done < 0 omits the counts; total <= 0 means "no
 * denominator", which also omits pct. extra_kvs is a preformatted "k=v k=v"
 * tail (or NULL) appended verbatim. One snprintf plus one log_msg keeps the
 * line atomic: log_msg() takes no lock, so a second call could interleave. */
static inline void mprog_emit(const char *action, const char *phase,
                              long long done, long long total,
                              const char *extra_kvs) {
    char line[256];
    int len;

    if (done >= 0 && total > 0) {
        len = snprintf(line, sizeof(line),
                       "[MPROG] action=%s phase=%s done=%lld total=%lld pct=%d",
                       action, phase, done, total, mprog_pct(done, total));
    } else if (done >= 0) {
        len = snprintf(line, sizeof(line), "[MPROG] action=%s phase=%s done=%lld",
                       action, phase, done);
    } else {
        len = snprintf(line, sizeof(line), "[MPROG] action=%s phase=%s",
                       action, phase);
    }
    if (extra_kvs && extra_kvs[0] && len > 0 && (size_t)len < sizeof(line)) {
        snprintf(line + len, sizeof(line) - (size_t)len, " %s", extra_kvs);
    }
    log_msg(LOG_INFO, "%s", line);
}

/* The throttle gate: 1 when a line is due (and records the emission). A done
 * that went backwards means the work restarted - the bench repeats its pass
 * until it has run long enough - which re-arms the gate instead of freezing it
 * at the last percentage. */
static inline int mprog_due(mprog_throttle_t *t, long long done, long long total) {
    int pct = mprog_pct(done, total);
    time_t now = time(NULL);

    if (t->have_last && done < t->last_done) {
        t->have_last = 0;
    }
    if (t->have_last) {
        if (now <= t->last_time) {
            return 0; /* less than a second since the last line */
        }
        if (total > 0 && pct <= t->last_pct) {
            return 0; /* less than 1 % since the last line */
        }
    }
    t->have_last = 1;
    t->last_done = done;
    t->last_pct = pct;
    t->last_time = now;
    return 1;
}

/* Gate plus emit in one call: all a loop body needs. */
static inline void mprog_progress(mprog_throttle_t *t, const char *action,
                                  const char *phase, long long done,
                                  long long total, const char *extra_kvs) {
    if (mprog_due(t, done, total)) {
        mprog_emit(action, phase, done, total, extra_kvs);
    }
}

/* The one terminal line every action must end with, on every return path. */
static inline void mprog_terminal(const char *action, int ok, int rc,
                                  const char *extra_kvs) {
    char extra[192];

    if (extra_kvs && extra_kvs[0]) {
        snprintf(extra, sizeof(extra), "rc=%d %s", rc, extra_kvs);
    } else {
        snprintf(extra, sizeof(extra), "rc=%d", rc);
    }
    mprog_emit(action, ok ? "done" : "failed", -1, 0, extra);
}

#endif /* MAINTENANCE_PROGRESS_H */
