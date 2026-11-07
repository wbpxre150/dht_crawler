/*
 * Supervised background thread that keeps torrent_class.movie_kind_* at the
 * generation the *loaded* movie manifest describes.
 *
 * The engine (src/movie_backfill.c) knows how to relabel a batch of rows and how
 * to say whether any work is left; it knows nothing about *when* to run. This
 * module owns exactly that: it polls the engine's decision, invokes one pass when
 * there is work, and publishes what it saw for /stats. Nothing here refers to a
 * model generation by number - the target always comes from the manifest through
 * movie_kind_generation() - so shipping a new generation needs no change here.
 *
 * Gating comes first and never touches the database: a build without an encoder,
 * a config with movie_kind_enabled = 0 and a phone without the verified v9
 * assets all publish "disabled" without scanning a single row, so a device
 * without the assets can never crash or loop here.
 *
 * Threading: one thread, one pass at a time. The thread never holds
 * database_t.mutex: the engine takes it per BEGIN ... COMMIT batch and it is not
 * recursive, so wrapping the call would deadlock. Shutdown is cooperative - the
 * stop flag wakes the thread out of its sliced sleep and
 * movie_backfill_request_stop() makes an in-flight pass return after the batch it
 * is in commits.
 */
#include "movie_backfill_daemon.h"

#include "movie_backfill.h"
#include "movie_kind.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------------ *
 * Tunables
 * ------------------------------------------------------------------------ */

/* Re-decision cadence. The first decision is taken at thread start, so a fresh
 * daemon reports within seconds; afterwards one poll per minute. */
#define MBD_POLL_SECS 60

/* Sleep granularity. The poll interval is slept in slices this long, each one
 * checking the stop flag, so request_shutdown() is honoured in <= 250 ms even
 * though the interval is a minute. */
#define MBD_SLEEP_SLICE_MS 250

/* The daemon's pass always writes for real and targets whatever the loaded
 * manifest describes (target 0). The window/batch knobs are the engine's own
 * defaults, spelled out here so the daemon's pass is reproducible. */
#define MBD_CHUNK_IDS 10000
#define MBD_BATCH_ROWS 500

/* movie_backfill_run() reports counters and a state but no message, so a failed
 * pass gets this text (last_error is never empty after a failure). */
#define MBD_ERROR_TEXT "movie backfill failed"

/* ------------------------------------------------------------------------ *
 * Test seams
 *
 * Every call into the engine or the movie-kind module goes through a file-static
 * pointer that defaults to the real function, so a host test can drive the whole
 * thread without a database, models or a 60 s wait. The pointers are file-static
 * on purpose: the daemon build exports no test surface, and the swap-in entry
 * point below only exists in the test build
 * (-DMOVIE_BACKFILL_DAEMON_TEST, set by the tests-movie-backfill-daemon target).
 * ------------------------------------------------------------------------ */

typedef int (*mbd_needed_fn)(database_t *db, long long target_clf_ver, long long *rows_left);
typedef int (*mbd_run_fn)(database_t *db, const movie_backfill_opts_t *opts,
                          movie_backfill_progress_t *out);
typedef void (*mbd_request_stop_fn)(void);
typedef void (*mbd_get_progress_fn)(movie_backfill_progress_t *out);
typedef int (*mbd_flag_fn)(void);
typedef long long (*mbd_version_fn)(void);
typedef void (*mbd_movie_stats_fn)(movie_kind_stats_t *out);
typedef void (*mbd_sleep_fn)(long long ms);

static void mbd_sleep_real(long long ms);

static mbd_needed_fn mbd_needed = movie_backfill_needed;
static mbd_run_fn mbd_run = movie_backfill_run;
static mbd_request_stop_fn mbd_request_stop = movie_backfill_request_stop;
static mbd_get_progress_fn mbd_get_progress = movie_backfill_get_progress;
static mbd_flag_fn mbd_enabled = movie_kind_enabled;
static mbd_version_fn mbd_generation = movie_kind_generation;
static mbd_movie_stats_fn mbd_movie_stats = movie_kind_get_stats;
static mbd_sleep_fn mbd_sleep = mbd_sleep_real;

#ifdef MOVIE_BACKFILL_DAEMON_TEST
void movie_backfill_daemon_set_hooks(const movie_backfill_daemon_hooks_t *hooks) {
    if (!hooks) {
        return;
    }
    if (hooks->needed) {
        mbd_needed = hooks->needed;
    }
    if (hooks->run) {
        mbd_run = hooks->run;
    }
    if (hooks->request_stop) {
        mbd_request_stop = hooks->request_stop;
    }
    if (hooks->get_progress) {
        mbd_get_progress = hooks->get_progress;
    }
    if (hooks->enabled) {
        mbd_enabled = hooks->enabled;
    }
    if (hooks->generation) {
        mbd_generation = hooks->generation;
    }
    if (hooks->get_movie_stats) {
        mbd_movie_stats = hooks->get_movie_stats;
    }
    if (hooks->sleep_ms) {
        mbd_sleep = hooks->sleep_ms;
    }
}
#endif /* MOVIE_BACKFILL_DAEMON_TEST */

/* ------------------------------------------------------------------------ *
 * Module state
 * ------------------------------------------------------------------------ */

/* One decision per poll; the decision log line is emitted when the decision
 * changes, because the poll repeats every minute and an unchanged decision is not
 * news. Written and read by the thread only. */
typedef enum {
    MBD_DECISION_NONE = -1,
    MBD_DECISION_IDLE = 0,
    MBD_DECISION_RAN = 1,
    MBD_DECISION_DISABLED_FEATURE = 2,
    /* No verified manifest and "no generation" are one decision: a verified
     * manifest always names a generation, so a module that is enabled but has no
     * target generation is a broken manifest, and it is reported the same way. */
    MBD_DECISION_DISABLED_NO_MODEL = 3
} mbd_decision_t;

/* mbd_stats is the module-owned half of the published snapshot (timestamps,
 * error text, and the last engine outcome); every read and write of it happens
 * under mbd_mutex. The engine-owned half (live counters of a pass in flight) and
 * the movie-kind lifetime counters are merged in by
 * movie_backfill_daemon_get_stats(). */
static pthread_mutex_t mbd_mutex = PTHREAD_MUTEX_INITIALIZER;
static movie_backfill_daemon_stats_t mbd_stats = { .state = "idle" };
static pthread_t mbd_thread;
static int mbd_started;          /* guarded by mbd_mutex */
static database_t *mbd_db;       /* not owned; outlives the thread by contract */
static int mbd_thermal_backoff_c; /* config.movie_kind_thermal_backoff_c, 0 = no gate */
static atomic_bool mbd_stop;       /* request_shutdown() sets it, the loop reads it */
static atomic_bool mbd_pass_active; /* 1 while mbd_run() is in flight */
static mbd_decision_t mbd_last_decision = MBD_DECISION_NONE;

/* ------------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------------ */

static void mbd_sleep_real(long long ms) {
    struct timespec ts;

    if (ms <= 0) {
        return;
    }
    ts.tv_sec = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((ms % 1000) * 1000000L);
    nanosleep(&ts, NULL);
}

/* Copy a state name into the fixed-width stats field. */
static void mbd_set_state(movie_backfill_daemon_stats_t *stats, int state) {
    snprintf(stats->state, sizeof stats->state, "%s", movie_backfill_state_name(state));
}

/* rows_total is always "what this pass handled plus what is left", so a reader
 * never sees a snapshot whose parts disagree (a pass that was stopped early
 * reports its remaining candidates as rows_left). */
static void mbd_set_counters(movie_backfill_daemon_stats_t *stats, long long processed,
                             long long left) {
    if (processed < 0) {
        processed = 0;
    }
    if (left < 0) {
        left = 0;
    }
    stats->rows_processed = processed;
    stats->rows_left = left;
    stats->rows_total = processed + left;
}

/* Publish "there is nothing to do": rows_left candidates below the target.
 *
 * A process whose last pass already finished keeps that pass's outcome (state and
 * counters) instead of blanking it: rows_left is 0, so nothing about the report
 * is stale, and a reader that polls after the pass still learns what the pass
 * did. A process that never invoked the engine reports "idle" with zero counters
 * and started_at stays 0 - that is how a no-op start is recognised. */
static void mbd_publish_no_work(long long rows_left, long long target_clf_ver) {
    pthread_mutex_lock(&mbd_mutex);
    mbd_stats.target_clf_ver = (int)target_clf_ver;
    if (rows_left == 0 && mbd_stats.started_at > 0) {
        mbd_set_counters(&mbd_stats, mbd_stats.rows_processed, 0);
    } else {
        mbd_set_state(&mbd_stats, MOVIE_BACKFILL_IDLE);
        mbd_set_counters(&mbd_stats, 0, rows_left);
    }
    pthread_mutex_unlock(&mbd_mutex);
}

/* Publish "this build/config cannot label movies". Counters are zeroed: nothing
 * was counted, so nothing is left as far as this module knows. */
static void mbd_publish_disabled(long long target_clf_ver) {
    pthread_mutex_lock(&mbd_mutex);
    mbd_set_state(&mbd_stats, MOVIE_BACKFILL_DISABLED);
    mbd_stats.target_clf_ver = (int)target_clf_ver;
    mbd_set_counters(&mbd_stats, 0, 0);
    pthread_mutex_unlock(&mbd_mutex);
}

/* The decision log line: one per decision change (see mbd_decision_t). */
static void mbd_log_decision(mbd_decision_t decision, log_level_t level, const char *format,
                             long long a, long long b) {
    if (decision == mbd_last_decision) {
        return;
    }
    mbd_last_decision = decision;
    log_msg(level, format, a, b);
}

/* ------------------------------------------------------------------------ *
 * One pass
 * ------------------------------------------------------------------------ */

/* Run one engine pass and publish its outcome. The engine blocks until the pass
 * finishes or a stop request is honoured; no database lock is taken here. */
static void mbd_run_pass(long long rows_left, long long target_clf_ver) {
    movie_backfill_opts_t opts;
    movie_backfill_progress_t progress;
    long long started_at = (long long)time(NULL);
    long long finished_at;
    int rc;

    memset(&opts, 0, sizeof opts);
    opts.target_clf_ver = 0; /* whatever the loaded manifest describes */
    opts.chunk_ids = MBD_CHUNK_IDS;
    opts.batch_rows = MBD_BATCH_ROWS;
    opts.dry_run = 0;
    opts.limit_rows = 0;
    opts.thermal_backoff_c = mbd_thermal_backoff_c;

    /* Announce the pass before entering the engine, so a /stats reader merges the
     * engine's live counters from the first committed batch on and never sees the
     * previous pass's outcome while this one runs. */
    pthread_mutex_lock(&mbd_mutex);
    mbd_set_state(&mbd_stats, MOVIE_BACKFILL_RUNNING);
    mbd_stats.target_clf_ver = (int)target_clf_ver;
    mbd_set_counters(&mbd_stats, 0, rows_left);
    mbd_stats.started_at = started_at;
    mbd_stats.finished_at = 0;
    mbd_stats.last_error[0] = '\0';
    pthread_mutex_unlock(&mbd_mutex);
    atomic_store(&mbd_pass_active, true);

    memset(&progress, 0, sizeof progress);
    rc = mbd_run(mbd_db, &opts, &progress);

    /* Publish the outcome, then clear the in-flight flag: a reader that still
     * sees pass_active merges the engine's own final snapshot, which agrees with
     * these values. */
    finished_at = (long long)time(NULL);
    pthread_mutex_lock(&mbd_mutex);
    mbd_set_state(&mbd_stats, progress.state);
    if (progress.target_clf_ver > 0) {
        mbd_stats.target_clf_ver = (int)progress.target_clf_ver;
    }
    mbd_set_counters(&mbd_stats, progress.rows_done, progress.rows_total - progress.rows_done);
    mbd_stats.finished_at = finished_at;
    if (rc < 0) {
        snprintf(mbd_stats.last_error, sizeof mbd_stats.last_error, "%s", MBD_ERROR_TEXT);
    }
    pthread_mutex_unlock(&mbd_mutex);
    atomic_store(&mbd_pass_active, false);

    log_msg(rc < 0 ? LOG_ERROR : (rc > 0 ? LOG_WARN : LOG_INFO),
            "movie-backfill: pass finished state=%s target_clf_ver=%lld processed=%lld "
            "left=%lld elapsed=%llds",
            movie_backfill_state_name(progress.state), target_clf_ver, progress.rows_done,
            progress.rows_total - progress.rows_done, finished_at - started_at);
}

/* ------------------------------------------------------------------------ *
 * One poll
 * ------------------------------------------------------------------------ */

/* Decide, log the decision, and run a pass when one is worth starting. Called
 * only from the thread, so the module needs no lock around the decision. */
static void mbd_poll_once(void) {
    long long rows_left = 0;
    long long target_clf_ver;
    int needed;

    /* Gate first, and before any database scan: a build without MK_ENABLED, a
     * config with movie_kind_enabled = 0 and a phone without the verified v9
     * assets all have no model at all, and reporting "idle" there would claim the
     * database is up to date. */
    if (!mbd_enabled()) {
        mbd_log_decision(MBD_DECISION_DISABLED_FEATURE, LOG_INFO,
                         "movie-backfill: disabled-feature (movie-kind not enabled); not "
                         "polling the database (rows_left=%lld target_clf_ver=%lld)",
                         0, 0);
        mbd_publish_disabled(0);
        return;
    }

    target_clf_ver = mbd_generation();
    if (target_clf_ver <= 0) {
        mbd_log_decision(MBD_DECISION_DISABLED_NO_MODEL, LOG_WARN,
                         "movie-backfill: disabled-no-model (no verified movie manifest, or "
                         "this build has no encoder) (rows_left=%lld target_clf_ver=%lld)",
                         0, 0);
        mbd_publish_disabled(0);
        return;
    }

    /* target 0 = "take the generation from the loaded manifest", the same
     * semantics the engine's own opts use. */
    needed = mbd_needed(mbd_db, 0, &rows_left);
    if (rows_left < 0) {
        rows_left = 0;
    }

    /* rows_left, not the engine's "needed" flag, is what a pass has to work on:
     * the flag also fires when only the stored meta generation is behind, and
     * starting a pass for zero rows would stamp started_at on a no-op. */
    if (!needed || rows_left <= 0) {
        mbd_log_decision(MBD_DECISION_IDLE, LOG_INFO,
                         "movie-backfill: not-needed (rows_left=%lld target_clf_ver=%lld)",
                         rows_left, target_clf_ver);
        mbd_publish_no_work(rows_left, target_clf_ver);
        return;
    }

    if (atomic_load(&mbd_stop)) {
        return; /* shutdown arrived between the decision and the pass */
    }
    mbd_log_decision(MBD_DECISION_RAN, LOG_INFO,
                     "movie-backfill: needed/starting (rows_left=%lld target_clf_ver=%lld); "
                     "starting a background pass",
                     rows_left, target_clf_ver);
    mbd_run_pass(rows_left, target_clf_ver);
}

/* ------------------------------------------------------------------------ *
 * Thread
 * ------------------------------------------------------------------------ */

static void *mbd_thread_main(void *arg) {
    (void)arg;

    log_msg(LOG_DEBUG, "movie-backfill: background thread started");

    while (!atomic_load(&mbd_stop)) {
        mbd_poll_once();
        if (atomic_load(&mbd_stop)) {
            break;
        }
        /* Sleep the poll interval in slices so a shutdown request never waits
         * more than MBD_SLEEP_SLICE_MS. */
        for (int slept = 0; slept < MBD_POLL_SECS * 1000; slept += MBD_SLEEP_SLICE_MS) {
            if (atomic_load(&mbd_stop)) {
                break;
            }
            mbd_sleep(MBD_SLEEP_SLICE_MS);
        }
    }

    log_msg(LOG_DEBUG, "movie-backfill: background thread exiting");
    return NULL;
}

/* ------------------------------------------------------------------------ *
 * Public API
 * ------------------------------------------------------------------------ */

void movie_backfill_daemon_start(app_context_t *ctx, database_t *db, int thermal_backoff_c) {
    (void)ctx; /* kept in the signature for symmetry with the other supervisors */

    if (!db) {
        log_msg(LOG_ERROR, "movie-backfill: no database handle; background pass not started");
        return;
    }

    pthread_mutex_lock(&mbd_mutex);
    if (mbd_started) {
        pthread_mutex_unlock(&mbd_mutex);
        log_msg(LOG_WARN, "movie-backfill: background thread already started");
        return;
    }
    mbd_db = db;
    mbd_thermal_backoff_c = thermal_backoff_c > 0 ? thermal_backoff_c : 0;
    mbd_started = 1;
    pthread_mutex_unlock(&mbd_mutex);

    atomic_store(&mbd_stop, false);
    atomic_store(&mbd_pass_active, false);

    if (pthread_create(&mbd_thread, NULL, mbd_thread_main, NULL) != 0) {
        log_msg(LOG_ERROR, "movie-backfill: failed to create the background thread");
        pthread_mutex_lock(&mbd_mutex);
        mbd_started = 0;
        pthread_mutex_unlock(&mbd_mutex);
        return;
    }

    /* Non-blocking on purpose: the first decision is taken by the thread, so the
     * caller (main.c, before its run loop) never waits for a database scan. */
    log_msg(LOG_INFO, "movie-backfill: background thread started (poll every %d s, thermal %s)",
            MBD_POLL_SECS, mbd_thermal_backoff_c > 0 ? "gate on" : "gate off");
}

void movie_backfill_daemon_request_shutdown(void) {
    atomic_store(&mbd_stop, true);
    /* Cooperative: an in-flight pass returns after the batch it is in commits (or
     * out of a thermal pause), and the thread is out of its sliced sleep within
     * MBD_SLEEP_SLICE_MS. */
    if (atomic_load(&mbd_pass_active)) {
        mbd_request_stop();
    }
}

void movie_backfill_daemon_destroy(void) {
    int started;

    pthread_mutex_lock(&mbd_mutex);
    started = mbd_started;
    mbd_started = 0;
    pthread_mutex_unlock(&mbd_mutex);

    if (!started) {
        return; /* idempotent: nothing to join */
    }

    atomic_store(&mbd_stop, true);
    if (atomic_load(&mbd_pass_active)) {
        mbd_request_stop();
    }
    pthread_join(mbd_thread, NULL);
    log_msg(LOG_INFO, "movie-backfill: background thread joined");
}

void movie_backfill_daemon_get_stats(movie_backfill_daemon_stats_t *out) {
    movie_backfill_progress_t progress;
    movie_kind_stats_t movie_stats;
    int live = 0;

    if (!out) {
        return;
    }

    /* A pass in flight is not visible in mbd_stats (the thread is blocked inside
     * the engine), so merge the engine's own live snapshot. Both snapshots are
     * taken outside the module mutex: the engine's accessor has its own lock and
     * never touches database_t.mutex, so this cannot deadlock against the pass. */
    if (atomic_load(&mbd_pass_active)) {
        memset(&progress, 0, sizeof progress);
        mbd_get_progress(&progress);
        live = (progress.state == MOVIE_BACKFILL_RUNNING);
    }

    memset(&movie_stats, 0, sizeof movie_stats);
    mbd_movie_stats(&movie_stats);

    pthread_mutex_lock(&mbd_mutex);
    *out = mbd_stats;
    pthread_mutex_unlock(&mbd_mutex);

    if (out->state[0] == '\0') {
        snprintf(out->state, sizeof out->state, "%s",
                 movie_backfill_state_name(MOVIE_BACKFILL_IDLE));
    }

    if (live) {
        mbd_set_state(out, progress.state);
        out->target_clf_ver = (int)progress.target_clf_ver;
        mbd_set_counters(out, progress.rows_done, progress.rows_total - progress.rows_done);
    }

    /* Module-lifetime counters, not per-pass ones: the plan's field list names
     * them, and the movie-kind module is their single owner. */
    out->thermal_pauses = (long long)movie_stats.thermal_pauses;
    out->model_load_failures = (long long)movie_stats.model_load_failures;
}
