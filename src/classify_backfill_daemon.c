/*
 * Supervised background thread that keeps torrent_class rows at the generation
 * the *loaded* model manifest describes.
 *
 * The engine (src/classify_backfill.c) knows how to relabel a batch of rows and
 * how to say whether any work is left; it knows nothing about *when* to run.
 * This module owns exactly that: it polls the engine's decision, invokes one
 * pass when there is work, and publishes what it saw for /stats. Nothing here
 * refers to a model generation by number - the target always comes from the
 * manifest through the classifier - so shipping a new generation needs no
 * change in this file.
 *
 * Threading: one thread, one pass at a time. The thread never holds
 * database_t.mutex: the engine takes it per BEGIN ... COMMIT batch and it is
 * not recursive, so wrapping the call would deadlock. Shutdown is cooperative -
 * the stop flag wakes the thread out of its sliced sleep and
 * classify_backfill_request_stop() makes an in-flight pass return after the
 * batch it is in commits.
 */
#include "classify_backfill_daemon.h"

#include "classifier.h"
#include "classify_backfill.h"

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
#define CLF_BACKFILL_POLL_SECS 60

/* Sleep granularity. The poll interval is slept in slices this long, each one
 * checking the stop flag, so request_shutdown() is honoured in <= 250 ms even
 * though the interval is a minute. */
#define CB_SLEEP_SLICE_MS 250

/* The daemon's pass always writes for real and targets whatever the loaded
 * manifest describes (target 0). The window/batch knobs are the engine's own
 * defaults, spelled out here so the daemon's pass is reproducible. */
#define CB_CHUNK_IDS 10000
#define CB_BATCH_ROWS 500

/* classify_backfill_run() reports counters and a state but no message, so a
 * FAILED pass gets this text (last_error is never empty after a failure). */
#define CB_ERROR_TEXT "classify backfill failed"

/* ------------------------------------------------------------------------ *
 * Test seams
 *
 * Every call into the engine or the classifier goes through a file-static
 * pointer that defaults to the real function, so a host test can drive the
 * whole thread without a database, models or a 60 s wait. The pointers are
 * file-static on purpose: the daemon build exports no test surface, and the
 * swap-in entry point below only exists in the test build
 * (-DCLF_BACKFILL_DAEMON_TEST, set by the tests-backfill-daemon target).
 * ------------------------------------------------------------------------ */

typedef int (*cbd_needed_fn)(database_t *db, long long target_clf_ver,
                             long long *rows_left);
typedef int (*cbd_run_fn)(database_t *db, const classify_backfill_opts_t *opts,
                          classify_backfill_progress_t *out);
typedef void (*cbd_request_stop_fn)(void);
typedef void (*cbd_get_progress_fn)(classify_backfill_progress_t *out);
typedef int (*cbd_flag_fn)(void);
typedef long long (*cbd_version_fn)(void);
typedef void (*cbd_sleep_fn)(long long ms);

static void cbd_sleep_real(long long ms);

static cbd_needed_fn cbd_needed = classify_backfill_needed;
static cbd_run_fn cbd_run = classify_backfill_run;
static cbd_request_stop_fn cbd_request_stop = classify_backfill_request_stop;
static cbd_get_progress_fn cbd_get_progress = classify_backfill_get_progress;
static cbd_flag_fn cbd_enabled = classifier_enabled;
static cbd_flag_fn cbd_models_loaded = classifier_models_loaded;
static cbd_version_fn cbd_current_version = classifier_current_version;
static cbd_sleep_fn cbd_sleep = cbd_sleep_real;

#ifdef CLF_BACKFILL_DAEMON_TEST
void classify_backfill_daemon_set_hooks(const classify_backfill_daemon_hooks_t *hooks) {
    if (!hooks) {
        return;
    }
    if (hooks->needed) {
        cbd_needed = hooks->needed;
    }
    if (hooks->run) {
        cbd_run = hooks->run;
    }
    if (hooks->request_stop) {
        cbd_request_stop = hooks->request_stop;
    }
    if (hooks->get_progress) {
        cbd_get_progress = hooks->get_progress;
    }
    if (hooks->enabled) {
        cbd_enabled = hooks->enabled;
    }
    if (hooks->models_loaded) {
        cbd_models_loaded = hooks->models_loaded;
    }
    if (hooks->current_version) {
        cbd_current_version = hooks->current_version;
    }
    if (hooks->sleep_ms) {
        cbd_sleep = hooks->sleep_ms;
    }
}
#endif /* CLF_BACKFILL_DAEMON_TEST */

/* ------------------------------------------------------------------------ *
 * Module state
 * ------------------------------------------------------------------------ */

/* One decision per poll; the decision log line is emitted when the decision
 * changes, because the poll repeats every minute and an unchanged decision is
 * not news. Written and read by the thread only. */
typedef enum {
    CBD_DECISION_NONE = -1,
    CBD_DECISION_IDLE = 0,
    CBD_DECISION_RAN = 1,
    CBD_DECISION_DISABLED_FEATURE = 2,
    CBD_DECISION_DISABLED_NO_MODELS = 3,
    CBD_DECISION_DISABLED_NO_TARGET = 4
} cbd_decision_t;

/* cbd_stats is the module-owned half of the published snapshot (timestamps,
 * error text, and the last engine outcome); every read and write of it happens
 * under cbd_mutex. The engine-owned half (live counters of a pass in flight) is
 * merged in by classify_backfill_daemon_get_stats(). */
static pthread_mutex_t cbd_mutex = PTHREAD_MUTEX_INITIALIZER;
static classify_backfill_daemon_stats_t cbd_stats = { .state = "idle" };
static pthread_t cbd_thread;
static int cbd_started;          /* guarded by cbd_mutex */
static database_t *cbd_db;       /* not owned; outlives the thread by contract */
static atomic_bool cbd_stop;     /* request_shutdown() sets it, the loop reads it */
static atomic_bool cbd_pass_active; /* 1 while cbd_run() is in flight */
static cbd_decision_t cbd_last_decision = CBD_DECISION_NONE;

/* ------------------------------------------------------------------------ *
 * Small helpers
 * ------------------------------------------------------------------------ */

static void cbd_sleep_real(long long ms) {
    struct timespec ts;

    if (ms <= 0) {
        return;
    }
    ts.tv_sec = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((ms % 1000) * 1000000L);
    nanosleep(&ts, NULL);
}

/* Copy a state name into the fixed-width stats field. */
static void cbd_set_state(classify_backfill_daemon_stats_t *stats, int state) {
    snprintf(stats->state, sizeof stats->state, "%s", classify_backfill_state_name(state));
}

/* rows_total is always "what this pass handled plus what is left", so a reader
 * never sees a snapshot whose parts disagree (a pass that was stopped early
 * reports its remaining candidates as rows_left). */
static void cbd_set_counters(classify_backfill_daemon_stats_t *stats, long long processed,
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
 * A process whose last pass already finished keeps that pass's outcome (state
 * and counters) instead of blanking it: rows_left is 0, so nothing about the
 * report is stale, and a reader that polls after the pass still learns what the
 * pass did. A process that never invoked the engine reports "idle" with zero
 * counters and started_at stays 0 - that is how a no-op start is recognised. */
static void cbd_publish_no_work(long long rows_left, long long target_clf_ver) {
    pthread_mutex_lock(&cbd_mutex);
    cbd_stats.target_clf_ver = (int)target_clf_ver;
    if (rows_left == 0 && cbd_stats.started_at > 0) {
        cbd_set_counters(&cbd_stats, cbd_stats.rows_processed, 0);
    } else {
        cbd_set_state(&cbd_stats, CLASSIFY_BACKFILL_IDLE);
        cbd_set_counters(&cbd_stats, 0, rows_left);
    }
    pthread_mutex_unlock(&cbd_mutex);
}

/* Publish "this build/config cannot classify". Counters are zeroed: nothing was
 * counted, so nothing is left as far as this module knows. */
static void cbd_publish_disabled(long long target_clf_ver) {
    pthread_mutex_lock(&cbd_mutex);
    cbd_set_state(&cbd_stats, CLASSIFY_BACKFILL_DISABLED);
    cbd_stats.target_clf_ver = (int)target_clf_ver;
    cbd_set_counters(&cbd_stats, 0, 0);
    pthread_mutex_unlock(&cbd_mutex);
}

/* The decision log line: one per decision change (see cbd_decision_t). */
static void cbd_log_decision(cbd_decision_t decision, log_level_t level, const char *format,
                             long long a, long long b) {
    if (decision == cbd_last_decision) {
        return;
    }
    cbd_last_decision = decision;
    log_msg(level, format, a, b);
}

/* ------------------------------------------------------------------------ *
 * One pass
 * ------------------------------------------------------------------------ */

/* Run one engine pass and publish its outcome. The engine blocks until the pass
 * finishes or a stop request is honoured; no database lock is taken here. */
static void cbd_run_pass(long long rows_left, long long target_clf_ver) {
    classify_backfill_opts_t opts;
    classify_backfill_progress_t progress;
    long long started_at = (long long)time(NULL);
    long long finished_at;
    int rc;

    memset(&opts, 0, sizeof opts);
    opts.target_clf_ver = 0; /* whatever the loaded manifest describes */
    opts.chunk_ids = CB_CHUNK_IDS;
    opts.batch_rows = CB_BATCH_ROWS;
    opts.dry_run = 0;
    opts.limit_rows = 0;

    /* Announce the pass before entering the engine, so a /stats reader merges
     * the engine's live counters from the first committed batch on and never
     * sees the previous pass's outcome while this one runs. */
    pthread_mutex_lock(&cbd_mutex);
    cbd_set_state(&cbd_stats, CLASSIFY_BACKFILL_RUNNING);
    cbd_stats.target_clf_ver = (int)target_clf_ver;
    cbd_set_counters(&cbd_stats, 0, rows_left);
    cbd_stats.started_at = started_at;
    cbd_stats.finished_at = 0;
    cbd_stats.last_error[0] = '\0';
    pthread_mutex_unlock(&cbd_mutex);
    atomic_store(&cbd_pass_active, true);

    memset(&progress, 0, sizeof progress);
    rc = cbd_run(cbd_db, &opts, &progress);

    /* Publish the outcome, then clear the in-flight flag: a reader that still
     * sees pass_active merges the engine's own final snapshot, which agrees
     * with these values. */
    finished_at = (long long)time(NULL);
    pthread_mutex_lock(&cbd_mutex);
    cbd_set_state(&cbd_stats, progress.state);
    if (progress.target_clf_ver > 0) {
        cbd_stats.target_clf_ver = (int)progress.target_clf_ver;
    }
    cbd_set_counters(&cbd_stats, progress.rows_done, progress.rows_total - progress.rows_done);
    cbd_stats.finished_at = finished_at;
    if (rc < 0) {
        snprintf(cbd_stats.last_error, sizeof cbd_stats.last_error, "%s", CB_ERROR_TEXT);
    }
    pthread_mutex_unlock(&cbd_mutex);
    atomic_store(&cbd_pass_active, false);

    log_msg(rc < 0 ? LOG_ERROR : (rc > 0 ? LOG_WARN : LOG_INFO),
            "classify-backfill: pass finished state=%s target_clf_ver=%lld processed=%lld "
            "left=%lld elapsed=%llds",
            classify_backfill_state_name(progress.state), target_clf_ver, progress.rows_done,
            progress.rows_total - progress.rows_done, finished_at - started_at);
}

/* ------------------------------------------------------------------------ *
 * One poll
 * ------------------------------------------------------------------------ */

/* Decide, log the decision, and run a pass when one is worth starting. Called
 * only from the thread, so the module needs no lock around the decision. */
static void cbd_poll_once(void) {
    long long rows_left = 0;
    long long target_clf_ver;
    int needed;

    /* Gate first: a build without CLF_ENABLED, or a config with
     * classifier_enabled=0, has no models at all, and reporting "idle" there
     * would claim the database is up to date. The engine's decision would be
     * "nothing needed" for the same reason, so this is also the only way the
     * disabled flavour is distinguishable from an up-to-date one. */
    if (!cbd_enabled()) {
        cbd_log_decision(CBD_DECISION_DISABLED_FEATURE, LOG_INFO,
                         "classify-backfill: disabled (classifier not enabled); not polling "
                         "the database (rows_left=%lld target_clf_ver=%lld)",
                         0, 0);
        cbd_publish_disabled(0);
        return;
    }
    if (!cbd_models_loaded()) {
        cbd_log_decision(CBD_DECISION_DISABLED_NO_MODELS, LOG_WARN,
                         "classify-backfill: disabled (no classifier models loaded); not "
                         "polling the database (rows_left=%lld target_clf_ver=%lld)",
                         0, 0);
        cbd_publish_disabled(0);
        return;
    }

    target_clf_ver = cbd_current_version();
    if (target_clf_ver <= 0) {
        cbd_log_decision(CBD_DECISION_DISABLED_NO_TARGET, LOG_WARN,
                         "classify-backfill: disabled (the loaded manifest names no "
                         "generation) (rows_left=%lld target_clf_ver=%lld)",
                         0, 0);
        cbd_publish_disabled(0);
        return;
    }

    /* target 0 = "take the generation from the loaded manifest", the same
     * semantics the engine's own opts use. */
    needed = cbd_needed(cbd_db, 0, &rows_left);
    if (rows_left < 0) {
        rows_left = 0;
    }

    /* rows_left, not the engine's "needed" flag, is what a pass has to work on:
     * the flag also fires when only the stored meta generation is behind, and
     * starting a pass for zero rows would stamp started_at on a no-op. */
    if (!needed || rows_left <= 0) {
        cbd_log_decision(CBD_DECISION_IDLE, LOG_INFO,
                         "classify-backfill: not needed (rows_left=%lld target_clf_ver=%lld)",
                         rows_left, target_clf_ver);
        cbd_publish_no_work(rows_left, target_clf_ver);
        return;
    }

    if (atomic_load(&cbd_stop)) {
        return; /* shutdown arrived between the decision and the pass */
    }
    cbd_log_decision(CBD_DECISION_RAN, LOG_INFO,
                     "classify-backfill: needed (rows_left=%lld target_clf_ver=%lld); "
                     "starting a background pass",
                     rows_left, target_clf_ver);
    cbd_run_pass(rows_left, target_clf_ver);
}

/* ------------------------------------------------------------------------ *
 * Thread
 * ------------------------------------------------------------------------ */

static void *cbd_thread_main(void *arg) {
    (void)arg;

    log_msg(LOG_DEBUG, "classify-backfill: background thread started");

    while (!atomic_load(&cbd_stop)) {
        cbd_poll_once();
        if (atomic_load(&cbd_stop)) {
            break;
        }
        /* Sleep the poll interval in slices so a shutdown request never waits
         * more than CB_SLEEP_SLICE_MS. */
        for (int slept = 0; slept < CLF_BACKFILL_POLL_SECS * 1000; slept += CB_SLEEP_SLICE_MS) {
            if (atomic_load(&cbd_stop)) {
                break;
            }
            cbd_sleep(CB_SLEEP_SLICE_MS);
        }
    }

    log_msg(LOG_DEBUG, "classify-backfill: background thread exiting");
    return NULL;
}

/* ------------------------------------------------------------------------ *
 * Public API
 * ------------------------------------------------------------------------ */

void classify_backfill_daemon_start(app_context_t *ctx, database_t *db) {
    (void)ctx; /* kept in the signature for symmetry with the other supervisors */

    if (!db) {
        log_msg(LOG_ERROR, "classify-backfill: no database handle; background pass not started");
        return;
    }

    pthread_mutex_lock(&cbd_mutex);
    if (cbd_started) {
        pthread_mutex_unlock(&cbd_mutex);
        log_msg(LOG_WARN, "classify-backfill: background thread already started");
        return;
    }
    cbd_db = db;
    cbd_started = 1;
    pthread_mutex_unlock(&cbd_mutex);

    atomic_store(&cbd_stop, false);
    atomic_store(&cbd_pass_active, false);

    if (pthread_create(&cbd_thread, NULL, cbd_thread_main, NULL) != 0) {
        log_msg(LOG_ERROR, "classify-backfill: failed to create the background thread");
        pthread_mutex_lock(&cbd_mutex);
        cbd_started = 0;
        pthread_mutex_unlock(&cbd_mutex);
        return;
    }

    /* Non-blocking on purpose: the first decision is taken by the thread, so the
     * caller (main.c, before its run loop) never waits for a database scan. */
    log_msg(LOG_INFO, "classify-backfill: background thread started (poll every %d s)",
            CLF_BACKFILL_POLL_SECS);
}

void classify_backfill_daemon_request_shutdown(void) {
    atomic_store(&cbd_stop, true);
    /* Cooperative: an in-flight pass returns after the batch it is in commits,
     * and the thread is out of its sliced sleep within CB_SLEEP_SLICE_MS. */
    if (atomic_load(&cbd_pass_active)) {
        cbd_request_stop();
    }
}

void classify_backfill_daemon_destroy(void) {
    int started;

    pthread_mutex_lock(&cbd_mutex);
    started = cbd_started;
    cbd_started = 0;
    pthread_mutex_unlock(&cbd_mutex);

    if (!started) {
        return; /* idempotent: nothing to join */
    }

    atomic_store(&cbd_stop, true);
    if (atomic_load(&cbd_pass_active)) {
        cbd_request_stop();
    }
    pthread_join(cbd_thread, NULL);
    log_msg(LOG_INFO, "classify-backfill: background thread joined");
}

void classify_backfill_daemon_get_stats(classify_backfill_daemon_stats_t *out) {
    classify_backfill_progress_t progress;
    int live = 0;

    if (!out) {
        return;
    }

    /* A pass in flight is not visible in cbd_stats (the thread is blocked inside
     * the engine), so merge the engine's own live snapshot. Both snapshots are
     * taken outside the module mutex: the engine's accessor has its own lock and
     * never touches database_t.mutex, so this cannot deadlock against the pass. */
    if (atomic_load(&cbd_pass_active)) {
        memset(&progress, 0, sizeof progress);
        cbd_get_progress(&progress);
        live = (progress.state == CLASSIFY_BACKFILL_RUNNING);
    }

    pthread_mutex_lock(&cbd_mutex);
    *out = cbd_stats;
    pthread_mutex_unlock(&cbd_mutex);

    if (out->state[0] == '\0') {
        snprintf(out->state, sizeof out->state, "%s", classify_backfill_state_name(CLASSIFY_BACKFILL_IDLE));
    }

    if (live) {
        cbd_set_state(out, progress.state);
        out->target_clf_ver = (int)progress.target_clf_ver;
        cbd_set_counters(out, progress.rows_done, progress.rows_total - progress.rows_done);
    }
}
