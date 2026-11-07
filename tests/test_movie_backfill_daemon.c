/**
 * Host unit test for the supervised movie-kind backfill thread module
 * (include/movie_backfill_daemon.h, src/movie_backfill_daemon.c).
 *
 * The module decides *when* to run the engine and publishes what it saw; the
 * engine and the movie-kind module are therefore not under test here (the engine
 * has its own suite in tests/test_movie_backfill.c). Every call the module makes
 * into them, and the sleep between polls, is a fake installed through the
 * module's test hooks, which exist only in this build
 * (-DMOVIE_BACKFILL_DAEMON_TEST, set by the tests-movie-backfill-daemon target).
 * That is what makes the cases runnable without a database, without model files
 * and without waiting a minute per poll: the fake sleep is ~0.5 ms per slice, so a
 * 60 s poll interval elapses in ~120 ms.
 *
 * Cases:
 *   1 before start          -> "idle", started_at 0, nothing invoked
 *   2 movie-kind disabled   -> "disabled", the database is never scanned
 *   3 no target generation  -> "disabled"
 *   4 nothing needed        -> "idle": no pass, no timestamps
 *   5 work needed           -> the engine is invoked once with the daemon's own
 *                              options, and the stats carry its outcome
 *   6 pass in flight        -> the merged snapshot reports the engine's live
 *                              counters, then the engine's final state
 *   7 stopped pass          -> "stopped", rows_left carries the remainder
 *   8 failed pass           -> last_error is set, the thread survives and the next
 *                              poll retries successfully
 *   9 shutdown during a pass-> request_stop() is called, destroy() joins without
 *                              hanging, and the stopped pass stays resumable
 *  10 module counters       -> thermal_pauses / model_load_failures come from the
 *                              movie-kind module
 *
 *     make tests-movie-backfill-daemon   # or: make tests / make tests-asan-...
 */
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "database.h"
#include "dht_crawler.h"
#include "movie_backfill.h"
#include "movie_backfill_daemon.h"
#include "movie_kind.h"

/* Generation the fake manifest reports. Deliberately not a real generation: the
 * module must publish whatever the manifest says, never a literal. */
#define STUB_GENERATION 11
/* Thermal threshold the daemon is started with, so the pass's options can be
 * checked against what config.ini asked for. */
#define STUB_THERMAL_C 82

/* Bounded wait for anything the module thread does; a case that needs longer than
 * this is hanging, not slow. */
#define WAIT_TIMEOUT_MS 8000

/* ------------------------------------------------------------------------- *
 * Fakes installed through the module's test hooks
 * ------------------------------------------------------------------------- */

static atomic_int fake_enabled;
static atomic_llong fake_generation;
static atomic_llong fake_thermal_pauses;
static atomic_llong fake_model_load_failures;

/* The fakes never touch it: the module only passes the handle through. */
static database_t fake_db;

static atomic_int fake_needed_ret;  /* what movie_backfill_needed() returns */
static atomic_llong fake_rows_left; /* candidates it reports */
static atomic_int fake_needed_calls;
static atomic_llong fake_needed_target; /* the target the module passed in */

static atomic_int fake_run_rc;      /* what movie_backfill_run() returns */
static atomic_int fake_run_calls;   /* completed invocations */
static atomic_int fake_run_entered; /* 1 while a fake pass is inside run() */
static atomic_int fake_run_block;   /* 1: stay in run() until released/stopped */
static atomic_int fake_release;     /* test-side release of a blocked run */
static atomic_int fake_stop_seen;   /* request_stop() was called */
static atomic_int fake_stop_calls;

static atomic_int fake_sleep_calls; /* sliced sleeps the loop asked for */

/* Recorded inputs/outputs, guarded by rec_mutex: the fake pass runs on the
 * module's thread while the assertions run on the main one. */
static pthread_mutex_t rec_mutex = PTHREAD_MUTEX_INITIALIZER;
static movie_backfill_opts_t rec_opts;
static movie_backfill_progress_t rec_result; /* what the fake pass returns */
static movie_backfill_progress_t rec_live;   /* what get_progress() reports */

static int fake_enabled_fn(void) {
    return atomic_load(&fake_enabled);
}

static long long fake_generation_fn(void) {
    return atomic_load(&fake_generation);
}

static void fake_movie_stats(movie_kind_stats_t *out) {
    if (!out) {
        return;
    }
    memset(out, 0, sizeof *out);
    out->generation = atomic_load(&fake_generation);
    out->thermal_pauses = (uint64_t)atomic_load(&fake_thermal_pauses);
    out->model_load_failures = (uint64_t)atomic_load(&fake_model_load_failures);
}

static int fake_needed(database_t *db, long long target_clf_ver, long long *rows_left) {
    (void)db;
    /* The daemon must always let the engine resolve the generation from the loaded
     * manifest, never pass one in. */
    atomic_store(&fake_needed_target, target_clf_ver);
    if (target_clf_ver != 0) {
        fprintf(stderr,
                "test_movie_backfill_daemon: FAILED: needed() was called with "
                "target_clf_ver=%lld, the daemon must pass 0\n",
                target_clf_ver);
        exit(1);
    }
    atomic_fetch_add(&fake_needed_calls, 1);
    if (rows_left) {
        *rows_left = atomic_load(&fake_rows_left);
    }
    return atomic_load(&fake_needed_ret);
}

static void fake_request_stop(void) {
    atomic_fetch_add(&fake_stop_calls, 1);
    atomic_store(&fake_stop_seen, 1);
}

static void fake_get_progress(movie_backfill_progress_t *out) {
    if (!out) {
        return;
    }
    pthread_mutex_lock(&rec_mutex);
    *out = rec_live;
    pthread_mutex_unlock(&rec_mutex);
}

/* One fake pass: records the options it was handed, optionally stays "in flight"
 * publishing live progress until the test releases it or a stop request arrives,
 * then returns the canned result. */
static int fake_run(database_t *db, const movie_backfill_opts_t *opts,
                    movie_backfill_progress_t *out) {
    int rc;

    (void)db;

    pthread_mutex_lock(&rec_mutex);
    if (opts) {
        rec_opts = *opts;
    } else {
        memset(&rec_opts, 0, sizeof rec_opts);
    }
    pthread_mutex_unlock(&rec_mutex);

    atomic_store(&fake_run_entered, 1);

    while (atomic_load(&fake_run_block)) {
        movie_backfill_progress_t live;

        if (atomic_load(&fake_release) || atomic_load(&fake_stop_seen)) {
            break;
        }
        memset(&live, 0, sizeof live);
        live.state = MOVIE_BACKFILL_RUNNING;
        live.target_clf_ver = STUB_GENERATION;
        live.rows_total = 10;
        live.rows_done = 4;
        pthread_mutex_lock(&rec_mutex);
        rec_live = live;
        pthread_mutex_unlock(&rec_mutex);
        struct timespec slice = {0, 1000000L}; /* 1 ms */
        nanosleep(&slice, NULL);
    }

    rc = atomic_load(&fake_run_rc);
    pthread_mutex_lock(&rec_mutex);
    if (out) {
        *out = rec_result;
    }
    pthread_mutex_unlock(&rec_mutex);
    atomic_fetch_add(&fake_run_calls, 1);
    atomic_store(&fake_run_entered, 0);
    return rc;
}

static void fake_sleep(long long ms) {
    struct timespec slice;

    (void)ms;
    atomic_fetch_add(&fake_sleep_calls, 1);
    slice.tv_sec = 0;
    slice.tv_nsec = 500000L; /* 0.5 ms */
    nanosleep(&slice, NULL);
}

static void install_hooks(void) {
    movie_backfill_daemon_hooks_t hooks;

    memset(&hooks, 0, sizeof hooks);
    hooks.needed = fake_needed;
    hooks.run = fake_run;
    hooks.request_stop = fake_request_stop;
    hooks.get_progress = fake_get_progress;
    hooks.enabled = fake_enabled_fn;
    hooks.generation = fake_generation_fn;
    hooks.get_movie_stats = fake_movie_stats;
    hooks.sleep_ms = fake_sleep;
    movie_backfill_daemon_set_hooks(&hooks);
}

/* ------------------------------------------------------------------------- *
 * Small helpers
 * ------------------------------------------------------------------------- */

static void fail(const char *what, const char *detail) {
    fprintf(stderr, "test_movie_backfill_daemon: FAILED: %s", what);
    if (detail && *detail) {
        fprintf(stderr, ": %s", detail);
    }
    fprintf(stderr, "\n");
    exit(1);
}

typedef int (*pred_fn)(void);

/* Poll `pred` until it is true or WAIT_TIMEOUT_MS elapses. */
static void wait_for(pred_fn pred, const char *what) {
    struct timespec start;
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        long long elapsed_ms;

        if (pred()) {
            return;
        }
        clock_gettime(CLOCK_MONOTONIC, &now);
        elapsed_ms = (long long)(now.tv_sec - start.tv_sec) * 1000 +
                     (now.tv_nsec - start.tv_nsec) / 1000000;
        if (elapsed_ms > WAIT_TIMEOUT_MS) {
            fail(what, "timed out waiting for the background thread");
        }
        struct timespec pause = {0, 1000000L};
        nanosleep(&pause, NULL);
    }
}

static movie_backfill_daemon_stats_t snapshot(void) {
    movie_backfill_daemon_stats_t stats;

    memset(&stats, 0, sizeof stats);
    movie_backfill_daemon_get_stats(&stats);
    return stats;
}

static void expect_counters(long long processed, long long left, const char *what) {
    movie_backfill_daemon_stats_t stats = snapshot();
    char detail[256];

    if (stats.rows_processed != processed || stats.rows_left != left ||
        stats.rows_total != processed + left) {
        snprintf(detail, sizeof detail,
                 "processed=%lld left=%lld total=%lld, want processed=%lld left=%lld", 
                 stats.rows_processed, stats.rows_left, stats.rows_total, processed, left);
        fail(what, detail);
    }
}

/* Give the thread a moment to take its first poll (it runs one immediately at
 * start), for the cases whose assertion is "nothing happened". */
static void settle(void) {
    struct timespec pause = {0, 20000000L}; /* 20 ms */

    nanosleep(&pause, NULL);
}

/* Predicates used by the waits. */
static int pred_state_disabled(void) {
    return strcmp(snapshot().state, "disabled") == 0;
}

static int pred_state_idle(void) {
    return strcmp(snapshot().state, "idle") == 0;
}

static int pred_state_done(void) {
    return strcmp(snapshot().state, "done") == 0;
}

static int pred_state_stopped(void) {
    return strcmp(snapshot().state, "stopped") == 0;
}

static int pred_state_running(void) {
    return strcmp(snapshot().state, "running") == 0;
}

static int pred_one_pass(void) {
    return atomic_load(&fake_run_calls) >= 1;
}

/* "a pass finished since the baseline was taken" - the only reliable wait when the
 * published state the case expects is already the current one. */
static atomic_int run_baseline;

static int pred_new_pass(void) {
    return atomic_load(&fake_run_calls) > atomic_load(&run_baseline);
}

/* ------------------------------------------------------------------------- *
 * Cases
 * ------------------------------------------------------------------------- */

static void case_before_start(void) {
    movie_backfill_daemon_stats_t stats = snapshot();

    if (strcmp(stats.state, "idle") != 0 || stats.started_at != 0 ||
        stats.rows_total != 0 || stats.last_error[0] != '\0') {
        fail("case 1 before start", "the pre-start snapshot is not the zeroed idle state");
    }
    if (atomic_load(&fake_needed_calls) != 0 || atomic_load(&fake_run_calls) != 0) {
        fail("case 1 before start", "the module invoked something before start()");
    }
    printf("  case 1 ok: before start() the snapshot is idle with started_at 0\n");
}

static void case_disabled_feature(void) {
    atomic_store(&fake_enabled, 0);
    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_state_disabled, "case 2 disabled feature");
    if (atomic_load(&fake_needed_calls) != 0) {
        fail("case 2 disabled feature", "a disabled module still scanned the database");
    }
    expect_counters(0, 0, "case 2 disabled feature");
    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 2 ok: a disabled movie-kind module reports \"disabled\" without a DB scan\n");
}

static void case_disabled_no_model(void) {
    atomic_store(&fake_enabled, 1);
    atomic_store(&fake_generation, 0);
    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    settle(); /* the state is already "disabled" from case 2, so wait on time */
    if (strcmp(snapshot().state, "disabled") != 0) {
        fail("case 3 disabled no model", "the module did not report \"disabled\"");
    }
    if (atomic_load(&fake_needed_calls) != 0) {
        fail("case 3 disabled no model", "a module without a manifest still scanned the database");
    }
    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 3 ok: no target generation reports \"disabled\"\n");
}

static void case_nothing_needed(void) {
    atomic_store(&fake_generation, STUB_GENERATION);
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 0);
    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_state_idle, "case 4 nothing needed");
    if (atomic_load(&fake_run_calls) != 0) {
        fail("case 4 nothing needed", "a pass was started for zero candidates");
    }
    {
        movie_backfill_daemon_stats_t stats = snapshot();
        if (stats.started_at != 0) {
            fail("case 4 nothing needed", "started_at was stamped on a no-op");
        }
        if (stats.target_clf_ver != STUB_GENERATION) {
            fail("case 4 nothing needed", "target_clf_ver is not the manifest's generation");
        }
    }
    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 4 ok: nothing needed reports \"idle\" and starts no pass\n");
}

static void case_work_needed(void) {
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 3);
    atomic_store(&fake_run_rc, 0);
    atomic_store(&fake_run_block, 0);
    pthread_mutex_lock(&rec_mutex);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = MOVIE_BACKFILL_DONE;
    rec_result.target_clf_ver = STUB_GENERATION;
    rec_result.rows_total = 3;
    rec_result.rows_done = 3;
    rec_result.rows_updated = 3;
    pthread_mutex_unlock(&rec_mutex);

    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_state_done, "case 5 work needed");
    wait_for(pred_one_pass, "case 5 work needed (one pass)");

    /* The daemon's own options: manifest target, its own window/batch knobs, write
     * mode, no row limit, and the configured thermal threshold. */
    pthread_mutex_lock(&rec_mutex);
    if (rec_opts.target_clf_ver != 0 || rec_opts.chunk_ids != 10000 ||
        rec_opts.batch_rows != 500 || rec_opts.dry_run != 0 || rec_opts.limit_rows != 0 ||
        rec_opts.thermal_backoff_c != STUB_THERMAL_C) {
        char detail[256];
        snprintf(detail, sizeof detail,
                 "opts target=%lld chunk=%d batch=%d dry=%d limit=%lld thermal=%d",
                 rec_opts.target_clf_ver, rec_opts.chunk_ids, rec_opts.batch_rows,
                 rec_opts.dry_run, rec_opts.limit_rows, rec_opts.thermal_backoff_c);
        pthread_mutex_unlock(&rec_mutex);
        fail("case 5 work needed (options)", detail);
    }
    pthread_mutex_unlock(&rec_mutex);

    expect_counters(3, 0, "case 5 work needed");
    {
        movie_backfill_daemon_stats_t stats = snapshot();
        if (stats.started_at <= 0 || stats.finished_at < stats.started_at) {
            fail("case 5 work needed", "the timestamps are not ordered");
        }
        if (stats.target_clf_ver != STUB_GENERATION) {
            fail("case 5 work needed", "target_clf_ver is not the manifest's generation");
        }
        if (stats.last_error[0] != '\0') {
            fail("case 5 work needed", "a successful pass published an error");
        }
    }
    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 5 ok: one pass ran with the daemon's own options and its outcome is "
           "published\n");
}

static void case_live_progress(void) {
    atomic_store(&fake_rows_left, 10);
    atomic_store(&fake_run_rc, 0);
    atomic_store(&fake_run_block, 1);
    atomic_store(&fake_release, 0);
    atomic_store(&fake_stop_seen, 0);
    pthread_mutex_lock(&rec_mutex);
    memset(&rec_live, 0, sizeof rec_live);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = MOVIE_BACKFILL_DONE;
    rec_result.target_clf_ver = STUB_GENERATION;
    rec_result.rows_total = 10;
    rec_result.rows_done = 10;
    pthread_mutex_unlock(&rec_mutex);

    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_state_running, "case 6 live progress");
    /* While the pass is in flight the merged snapshot carries the engine's live
     * counters (the module's own copy still says 0/rows_left). */
    expect_counters(4, 6, "case 6 live counters");

    atomic_store(&fake_release, 1);
    atomic_store(&fake_run_block, 0);
    wait_for(pred_state_done, "case 6 live progress (final state)");
    expect_counters(10, 0, "case 6 live progress");

    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 6 ok: a pass in flight publishes the engine's live counters\n");
}

static void case_stopped_pass(void) {
    atomic_store(&fake_run_block, 0);
    atomic_store(&fake_run_rc, 0);
    atomic_store(&fake_rows_left, 10);
    pthread_mutex_lock(&rec_mutex);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = MOVIE_BACKFILL_STOPPED;
    rec_result.target_clf_ver = STUB_GENERATION;
    rec_result.rows_total = 10;
    rec_result.rows_done = 4;
    pthread_mutex_unlock(&rec_mutex);

    atomic_store(&run_baseline, atomic_load(&fake_run_calls));
    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_new_pass, "case 7 stopped pass");
    wait_for(pred_state_stopped, "case 7 stopped pass");
    expect_counters(4, 6, "case 7 stopped pass");
    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 7 ok: a stopped pass publishes \"stopped\" with the remaining rows\n");
}

static void case_failed_then_retry(void) {
    int calls_before;

    atomic_store(&fake_run_block, 0);
    atomic_store(&fake_run_rc, -1);
    atomic_store(&fake_rows_left, 2);
    pthread_mutex_lock(&rec_mutex);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = MOVIE_BACKFILL_STOPPED;
    rec_result.rows_total = 2;
    rec_result.rows_done = 0;
    pthread_mutex_unlock(&rec_mutex);

    /* The published state is already "stopped" from case 7, so wait for the pass
     * itself, not for the state. */
    atomic_store(&run_baseline, atomic_load(&fake_run_calls));
    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_new_pass, "case 8 failed pass");
    wait_for(pred_state_stopped, "case 8 failed pass");
    {
        movie_backfill_daemon_stats_t stats = snapshot();
        if (stats.last_error[0] == '\0') {
            fail("case 8 failed pass", "a failed pass left last_error empty");
        }
    }
    calls_before = atomic_load(&fake_run_calls);

    /* The thread survives and retries: the next poll succeeds and clears the
     * error. */
    atomic_store(&fake_run_rc, 0);
    pthread_mutex_lock(&rec_mutex);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = MOVIE_BACKFILL_DONE;
    rec_result.target_clf_ver = STUB_GENERATION;
    rec_result.rows_total = 2;
    rec_result.rows_done = 2;
    pthread_mutex_unlock(&rec_mutex);

    wait_for(pred_state_done, "case 8 retry");
    if (atomic_load(&fake_run_calls) <= calls_before) {
        fail("case 8 retry", "the thread did not retry after a failed pass");
    }
    if (snapshot().last_error[0] != '\0') {
        fail("case 8 retry", "a successful retry did not clear last_error");
    }
    movie_backfill_daemon_request_shutdown();
    movie_backfill_daemon_destroy();
    printf("  case 8 ok: a failed pass sets last_error, the thread survives and retries\n");
}

static void case_shutdown_during_pass(void) {
    atomic_store(&fake_run_block, 1);
    atomic_store(&fake_release, 0);
    atomic_store(&fake_stop_seen, 0);
    atomic_store(&fake_run_rc, 0);
    atomic_store(&fake_rows_left, 5);
    pthread_mutex_lock(&rec_mutex);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = MOVIE_BACKFILL_STOPPED;
    rec_result.target_clf_ver = STUB_GENERATION;
    rec_result.rows_total = 5;
    rec_result.rows_done = 1;
    pthread_mutex_unlock(&rec_mutex);

    movie_backfill_daemon_start(NULL, &fake_db, STUB_THERMAL_C);
    wait_for(pred_state_running, "case 9 shutdown during a pass");

    /* request_shutdown() must reach the engine (fake_request_stop) and destroy()
     * must join: the fake run() returns as soon as it sees the stop. */
    movie_backfill_daemon_request_shutdown();
    if (atomic_load(&fake_stop_calls) == 0) {
        fail("case 9 shutdown during a pass", "request_stop() was never called");
    }
    movie_backfill_daemon_destroy();
    /* The stopped pass stayed resumable: the engine's remainder is published. */
    if (strcmp(snapshot().state, "stopped") != 0) {
        fail("case 9 shutdown during a pass", "the joined pass did not publish \"stopped\"");
    }
    atomic_store(&fake_run_block, 0);
    printf("  case 9 ok: shutdown stops the pass cooperatively and destroy() joins\n");
}

static void case_module_counters(void) {
    movie_backfill_daemon_stats_t stats;

    atomic_store(&fake_thermal_pauses, 3);
    atomic_store(&fake_model_load_failures, 2);
    stats = snapshot();
    if (stats.thermal_pauses != 3 || stats.model_load_failures != 2) {
        char detail[128];
        snprintf(detail, sizeof detail, "thermal=%lld model_load_failures=%lld",
                 stats.thermal_pauses, stats.model_load_failures);
        fail("case 10 module counters", detail);
    }
    printf("  case 10 ok: thermal_pauses and model_load_failures come from the movie module\n");
}

int main(void) {
    memset(&fake_db, 0, sizeof fake_db);
    install_hooks();

    atomic_store(&fake_enabled, 1);
    atomic_store(&fake_generation, STUB_GENERATION);
    atomic_store(&fake_needed_ret, 0);
    atomic_store(&fake_rows_left, 0);
    atomic_store(&fake_run_rc, 0);

    printf("test_movie_backfill_daemon: supervised movie-kind backfill thread\n");

    case_before_start();
    case_disabled_feature();
    case_disabled_no_model();
    case_nothing_needed();
    case_work_needed();
    case_live_progress();
    case_stopped_pass();
    case_failed_then_retry();
    case_shutdown_during_pass();
    case_module_counters();

    movie_backfill_daemon_destroy(); /* idempotent */
    printf("test_movie_backfill_daemon: all assertions passed\n");
    return 0;
}
