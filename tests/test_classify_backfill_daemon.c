/**
 * Host unit test for the supervised classify-backfill thread module
 * (include/classify_backfill_daemon.h, src/classify_backfill_daemon.c).
 *
 * The module decides *when* to run the engine and publishes what it saw; the
 * engine and the classifier are therefore not under test here (they have their
 * own suite in tests/test_classify_backfill.c). Every call the module makes into
 * them, and the sleep between polls, is a fake installed through the module's
 * test hooks, which exist only in this build (-DCLF_BACKFILL_DAEMON_TEST, set by
 * the tests-backfill-daemon target). That is what makes the cases runnable
 * without a database, without model files and without waiting a minute per poll:
 * the fake sleep is ~0.5 ms per slice, so a 60 s poll interval elapses in ~120 ms.
 *
 * Cases:
 *   1 nothing needed        -> the thread idles: state "idle", started_at 0, and
 *                              it keeps polling (no pass, no timestamps)
 *   2 work needed           -> the engine is invoked once with the daemon's own
 *                              options, and the stats carry its outcome
 *   3 pass in flight        -> the merged snapshot reports the engine's live
 *                              counters, then the engine's final state
 *   4 shutdown during a pass-> request_stop() is called, destroy() joins without
 *                              hanging, and the stopped pass stays resumable
 *   5 failed pass           -> last_error is non-empty, the thread survives and
 *                              the next poll retries successfully
 *   6 get_stats under load  -> a poller hammering the accessor never sees a torn
 *                              snapshot
 *
 *     make tests-backfill-daemon      # or: make tests / make tests-asan-backfill-daemon
 */
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "classifier.h"
#include "classify_backfill.h"
#include "classify_backfill_daemon.h"
#include "database.h"
#include "dht_crawler.h"

/* Generation the fake classifier reports. Deliberately not a real generation:
 * the module must publish whatever the manifest says, never a literal. */
#define STUB_CLF_VER 7

/* Bounded wait for anything the module thread does; a case that needs longer
 * than this is hanging, not slow. */
#define WAIT_TIMEOUT_MS 8000

/* ------------------------------------------------------------------------- *
 * Fakes installed through the module's test hooks
 * ------------------------------------------------------------------------- */

static atomic_int fake_enabled;
static atomic_int fake_models_loaded;
static atomic_llong fake_version;

/* The fakes never touch it: the module only passes the handle through. */
static database_t fake_db;

/* rows_total the case expects the finished pass to report (see
 * pred_pass_finished): the module's stats outlive a single case, so a wait has
 * to name the pass it is waiting for. */
static atomic_llong want_total;

static atomic_int fake_needed_ret;    /* what classify_backfill_needed() returns */
static atomic_llong fake_rows_left;   /* candidates it reports */
static atomic_int fake_needed_calls;

static atomic_int fake_run_rc;        /* what classify_backfill_run() returns */
static atomic_int fake_run_calls;     /* completed invocations */
static atomic_int fake_run_entered;   /* 1 while a fake pass is inside run() */
static atomic_int fake_run_block;     /* 1: stay in run() until released/stopped */
static atomic_int fake_release;       /* test-side release of a blocked run */
static atomic_int fake_stop_seen;     /* request_stop() was called */
static atomic_int fake_stop_calls;

static atomic_int fake_sleep_calls;   /* sliced sleeps the loop asked for */

/* Recorded inputs/outputs, guarded by rec_mutex: the fake pass runs on the
 * module's thread while the assertions run on the main one. */
static pthread_mutex_t rec_mutex = PTHREAD_MUTEX_INITIALIZER;
static classify_backfill_opts_t rec_opts;
static classify_backfill_progress_t rec_result;  /* what the fake pass returns */
static classify_backfill_progress_t rec_live;    /* what get_progress() reports */

static int fake_enabled_fn(void) {
    return atomic_load(&fake_enabled);
}

static int fake_models_loaded_fn(void) {
    return atomic_load(&fake_models_loaded);
}

static long long fake_current_version_fn(void) {
    return atomic_load(&fake_version);
}

static int fake_needed(database_t *db, long long target_clf_ver, long long *rows_left) {
    (void)db;
    /* The daemon must always let the engine resolve the generation from the
     * loaded manifest, never pass one in. */
    if (target_clf_ver != 0) {
        atomic_store(&fake_needed_calls, WAIT_TIMEOUT_MS * 1000); /* poisons the waits */
        fprintf(stderr, "test_classify_backfill_daemon: FAILED: needed() was called with "
                        "target_clf_ver=%lld, the daemon must pass 0\n", target_clf_ver);
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

static void fake_get_progress(classify_backfill_progress_t *out) {
    if (!out) {
        return;
    }
    pthread_mutex_lock(&rec_mutex);
    *out = rec_live;
    pthread_mutex_unlock(&rec_mutex);
}

/* One fake pass: records the options it was handed, optionally stays "in
 * flight" publishing live progress until the test releases it or a stop request
 * arrives, then returns the canned result. */
static int fake_run(database_t *db, const classify_backfill_opts_t *opts,
                    classify_backfill_progress_t *out) {
    long long done = 0;
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
    if (atomic_load(&fake_run_block)) {
        long long total = atomic_load(&fake_rows_left);
        while (!atomic_load(&fake_release) && !atomic_load(&fake_stop_seen)) {
            done++;
            pthread_mutex_lock(&rec_mutex);
            rec_live.state = CLASSIFY_BACKFILL_RUNNING;
            rec_live.target_clf_ver = atomic_load(&fake_version);
            rec_live.rows_total = total;
            rec_live.rows_done = done;
            rec_live.rows_updated = done;
            pthread_mutex_unlock(&rec_mutex);
            usleep(500);
        }
    }

    rc = atomic_load(&fake_run_rc);
    pthread_mutex_lock(&rec_mutex);
    if (out) {
        *out = rec_result;
    }
    pthread_mutex_unlock(&rec_mutex);

    atomic_store(&fake_run_entered, 0);
    atomic_fetch_add(&fake_run_calls, 1);
    return rc;
}

static void fake_sleep(long long ms) {
    (void)ms;
    atomic_fetch_add(&fake_sleep_calls, 1);
    usleep(500); /* the real sleep is 250 ms; the test only needs the slice count */
}

/* ------------------------------------------------------------------------- *
 * Classifier symbols: src/classify_backfill.c is linked for its state names and
 * its progress accessor, and it references these. They answer from the same
 * fakes, so nothing here needs model files. (Same trick tests-backfill plays.)
 * ------------------------------------------------------------------------- */

int classifier_models_loaded(void) {
    return atomic_load(&fake_models_loaded);
}

int classifier_enabled(void) {
    return atomic_load(&fake_enabled);
}

long long classifier_current_version(void) {
    return atomic_load(&fake_version);
}

void classifier_write_meta(sqlite3 *db) {
    (void)db;
}

int classifier_classify(const torrent_metadata_t *meta, classification_t *out) {
    (void)meta;
    (void)out;
    return 1; /* no verdict: this test never classifies anything */
}

/* src/adult_backfill.c is linked too (the daemon's adult mode calls it), and it
 * defaults its verdict pointer to this symbol. No verdict, for the same reason
 * as classifier_classify above. */
int classifier_classify_adult(const torrent_metadata_t *meta, int *adult, double *adult_p) {
    (void)meta;
    if (adult) *adult = 0;
    if (adult_p) *adult_p = 0.0;
    return 1;
}

/* ------------------------------------------------------------------------- *
 * Helpers
 * ------------------------------------------------------------------------- */

/* Failing assert: print it and exit non-zero (no test framework here). */
static void fail(const char *what, const char *detail) {
    fprintf(stderr, "test_classify_backfill_daemon: FAILED: %s", what);
    if (detail && *detail) {
        fprintf(stderr, ": %s", detail);
    }
    fprintf(stderr, "\n");
    exit(1);
}

static void expect_int(int got, int want, const char *what) {
    if (got != want) {
        char detail[128];
        snprintf(detail, sizeof detail, "got %d, want %d", got, want);
        fail(what, detail);
    }
}

static void expect_ll(long long got, long long want, const char *what) {
    if (got != want) {
        char detail[160];
        snprintf(detail, sizeof detail, "got %lld, want %lld", got, want);
        fail(what, detail);
    }
}

static void expect_str(const char *got, const char *want, const char *what) {
    if (strcmp(got, want) != 0) {
        char detail[256];
        snprintf(detail, sizeof detail, "got \"%s\", want \"%s\"", got, want);
        fail(what, detail);
    }
}

/* Bounded wait until *flag reaches `want`. Every observation of the module
 * thread goes through this: no fixed sleeps, no races. */
static void wait_flag(atomic_int *flag, int want, const char *what) {
    int waited = 0;

    while (atomic_load(flag) < want) {
        if (waited >= WAIT_TIMEOUT_MS) {
            fail(what, "timed out waiting for the module thread");
        }
        usleep(1000);
        waited++;
    }
}

typedef int (*stats_pred_fn)(const classify_backfill_daemon_stats_t *stats);

static int pred_running_with_progress(const classify_backfill_daemon_stats_t *stats) {
    return strcmp(stats->state, "running") == 0 && stats->rows_processed > 0;
}

static int pred_done(const classify_backfill_daemon_stats_t *stats) {
    return strcmp(stats->state, "done") == 0;
}
static int pred_idle(const classify_backfill_daemon_stats_t *stats) {
    return strcmp(stats->state, "idle") == 0 && stats->finished_at == 0;
}

/* "the pass this case started has finished": a fresh finished_at is not enough,
 * because the module keeps the previous case's outcome. */
static int pred_pass_finished(const classify_backfill_daemon_stats_t *stats) {
    return stats->finished_at > 0 && stats->rows_total == atomic_load(&want_total);
}

/* Bounded wait until the published snapshot satisfies `pred`; the last snapshot
 * is copied into *out. */
static void wait_stats(stats_pred_fn pred, classify_backfill_daemon_stats_t *out,
                       const char *what) {
    int waited = 0;

    for (;;) {
        classify_backfill_daemon_get_stats(out);
        if (pred(out)) {
            return;
        }
        if (waited >= WAIT_TIMEOUT_MS) {
            char detail[512];
            snprintf(detail, sizeof detail, "state=%s processed=%lld left=%lld started=%lld "
                                            "finished=%lld last_error=\"%s\"",
                     out->state, out->rows_processed, out->rows_left, out->started_at,
                     out->finished_at, out->last_error);
            fail(what, detail);
        }
        usleep(1000);
        waited++;
    }
}

/* Every published snapshot must be internally consistent; a torn copy (a state
 * from one publish with counters from another) breaks one of these. */
static int stats_consistent(const classify_backfill_daemon_stats_t *stats, char *why,
                            size_t why_len) {
    const char *const STATES[] = {"idle", "running", "done", "failed", "disabled", "unknown"};
    int known = 0;

    for (size_t i = 0; i < sizeof STATES / sizeof STATES[0]; i++) {
        if (strcmp(stats->state, STATES[i]) == 0) {
            known = 1;
        }
    }
    if (!known) {
        snprintf(why, why_len, "state \"%s\" is not a classify_backfill state name",
                 stats->state);
        return 0;
    }
    if (stats->rows_processed < 0 || stats->rows_left < 0) {
        snprintf(why, why_len, "negative counter: processed=%lld left=%lld",
                 stats->rows_processed, stats->rows_left);
        return 0;
    }
    if (stats->rows_total != stats->rows_processed + stats->rows_left) {
        snprintf(why, why_len, "rows_total=%lld != processed=%lld + left=%lld",
                 stats->rows_total, stats->rows_processed, stats->rows_left);
        return 0;
    }
    if (stats->finished_at != 0 && stats->finished_at < stats->started_at) {
        snprintf(why, why_len, "finished_at=%lld < started_at=%lld", stats->finished_at,
                 stats->started_at);
        return 0;
    }
    if (strcmp(stats->state, "running") == 0 && stats->started_at == 0) {
        snprintf(why, why_len, "state running with started_at 0");
        return 0;
    }
    if (strcmp(stats->state, "done") == 0 && stats->rows_left != 0) {
        snprintf(why, why_len, "state done with rows_left=%lld", stats->rows_left);
        return 0;
    }
    if (strnlen(stats->last_error, sizeof stats->last_error) == sizeof stats->last_error) {
        snprintf(why, why_len, "last_error is not NUL-terminated");
        return 0;
    }
    return 1;
}

/* Reset every fake to "enabled, models loaded, nothing to do, a pass that
 * finishes with `rows` handled". */
static void reset_fakes(long long rows) {
    atomic_store(&fake_enabled, 1);
    atomic_store(&fake_models_loaded, 1);
    atomic_store(&fake_version, STUB_CLF_VER);
    atomic_store(&fake_needed_ret, 0);
    atomic_store(&fake_rows_left, 0);
    atomic_store(&fake_needed_calls, 0);
    atomic_store(&fake_run_rc, 0);
    atomic_store(&fake_run_calls, 0);
    atomic_store(&fake_run_entered, 0);
    atomic_store(&fake_run_block, 0);
    atomic_store(&fake_release, 0);
    atomic_store(&fake_stop_seen, 0);
    atomic_store(&fake_stop_calls, 0);
    atomic_store(&fake_sleep_calls, 0);

    pthread_mutex_lock(&rec_mutex);
    memset(&rec_opts, 0, sizeof rec_opts);
    memset(&rec_live, 0, sizeof rec_live);
    memset(&rec_result, 0, sizeof rec_result);
    rec_result.state = CLASSIFY_BACKFILL_DONE;
    rec_result.target_clf_ver = STUB_CLF_VER;
    rec_result.rows_total = rows;
    rec_result.rows_done = rows;
    pthread_mutex_unlock(&rec_mutex);
}

static void install_hooks(void) {
    classify_backfill_daemon_hooks_t hooks;

    memset(&hooks, 0, sizeof hooks);
    hooks.needed = fake_needed;
    hooks.run = fake_run;
    hooks.request_stop = fake_request_stop;
    hooks.get_progress = fake_get_progress;
    hooks.enabled = fake_enabled_fn;
    hooks.models_loaded = fake_models_loaded_fn;
    hooks.current_version = fake_current_version_fn;
    hooks.sleep_ms = fake_sleep;
    classify_backfill_daemon_set_hooks(&hooks);
}

/* ------------------------------------------------------------------------- *
 * Cases
 * ------------------------------------------------------------------------- */

/* 0: the accessor is safe before the thread exists and with a NULL out. */
static void case_before_start(void) {
    classify_backfill_daemon_stats_t stats;

    memset(&stats, 0xFF, sizeof stats); /* poisoned: the accessor must fill it */
    classify_backfill_daemon_get_stats(&stats);
    expect_str(stats.state, "idle", "before start: the snapshot must report idle");
    expect_ll(stats.started_at, 0, "before start: started_at");
    expect_ll(stats.finished_at, 0, "before start: finished_at");
    expect_ll(stats.rows_total, 0, "before start: rows_total");
    expect_int(stats.target_clf_ver, 0, "before start: target_clf_ver");
    expect_str(stats.last_error, "", "before start: last_error");
    classify_backfill_daemon_get_stats(NULL); /* must not crash */

    /* Shutdown and destroy without a start are no-ops, not crashes. */
    classify_backfill_daemon_request_shutdown();
    classify_backfill_daemon_destroy();
    classify_backfill_daemon_get_stats(&stats);
    expect_str(stats.state, "idle", "after a startless shutdown: state");

    printf("  case 0 ok: get_stats(NULL)/destroy() without a start are no-ops\n");
}

/* 1: nothing to do -> the thread idles and never stamps a pass. */
static void case_idle(void) {
    classify_backfill_daemon_stats_t stats;

    reset_fakes(0);
    atomic_store(&fake_needed_ret, 0);
    atomic_store(&fake_rows_left, 0);

    classify_backfill_daemon_start(NULL, NULL); /* no database: logged, not fatal */
    classify_backfill_daemon_start(NULL, &fake_db);

    wait_flag(&fake_needed_calls, 1, "idle: the thread must poll immediately at start");
    wait_stats(pred_idle, &stats, "idle: the first poll must publish an idle snapshot");
    expect_str(stats.state, "idle", "idle: state with nothing to do");
    expect_ll(stats.started_at, 0, "idle: started_at (the engine was never invoked)");
    expect_ll(stats.finished_at, 0, "idle: finished_at");
    expect_ll(stats.rows_left, 0, "idle: rows_left");
    expect_ll(stats.rows_total, 0, "idle: rows_total");
    expect_int(stats.target_clf_ver, STUB_CLF_VER, "idle: target_clf_ver from the classifier");
    expect_str(stats.last_error, "", "idle: last_error");
    expect_int(atomic_load(&fake_run_calls), 0, "idle: the engine must not be invoked");

    /* It must keep polling, sleeping in slices, without ever running a pass. */
    wait_flag(&fake_needed_calls, 2, "idle: the thread must poll again after its interval");
    if (atomic_load(&fake_sleep_calls) == 0) {
        fail("idle: the poll interval must be slept in slices", "no sleep was requested");
    }
    expect_int(atomic_load(&fake_run_calls), 0, "idle: still no engine invocation");

    classify_backfill_daemon_destroy();
    printf("  case 1 ok: nothing needed -> state=idle, started_at=0, %d polls, no pass\n",
           atomic_load(&fake_needed_calls));
}

/* 2: work needed -> one pass with the daemon's options, outcome published. */
static void case_run(void) {
    classify_backfill_daemon_stats_t stats;
    classify_backfill_opts_t opts;

    reset_fakes(1000);
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 1000);
    atomic_store(&want_total, 1000);
    pthread_mutex_lock(&rec_mutex);
    rec_result.rows_updated = 900;
    rec_result.rows_inserted = 100;
    pthread_mutex_unlock(&rec_mutex);

    classify_backfill_daemon_start(NULL, &fake_db);
    wait_stats(pred_pass_finished, &stats, "run: the pass must finish and be published");
    wait_flag(&fake_run_calls, 1, "run: the engine must be invoked");

    /* Nothing left to do from here on: later polls must keep this outcome. */
    atomic_store(&fake_needed_ret, 0);
    atomic_store(&fake_rows_left, 0);
    wait_stats(pred_done, &stats, "run: the finished pass must stay reported as done");

    expect_str(stats.state, "done", "run: state from the engine's result");
    expect_ll(stats.rows_processed, 1000, "run: rows_processed");
    expect_ll(stats.rows_left, 0, "run: rows_left");
    expect_ll(stats.rows_total, 1000, "run: rows_total");
    expect_int(stats.target_clf_ver, STUB_CLF_VER, "run: target_clf_ver");
    if (stats.started_at <= 0) {
        fail("run: started_at", "the pass must be stamped with its start time");
    }
    if (stats.finished_at < stats.started_at) {
        fail("run: finished_at", "the pass must not finish before it started");
    }
    expect_str(stats.last_error, "", "run: a successful pass leaves last_error empty");

    pthread_mutex_lock(&rec_mutex);
    opts = rec_opts;
    pthread_mutex_unlock(&rec_mutex);
    expect_ll(opts.target_clf_ver, 0, "run: the engine must resolve the target from the manifest");
    expect_int(opts.chunk_ids, 10000, "run: chunk_ids");
    expect_int(opts.batch_rows, 500, "run: batch_rows");
    expect_int(opts.dry_run, 0, "run: dry_run (the daemon's pass writes)");
    expect_ll(opts.limit_rows, 0, "run: limit_rows (a daemon pass is never truncated)");

    classify_backfill_daemon_destroy();
    printf("  case 2 ok: needed -> one pass, state=done processed=1000 target=%d\n",
           stats.target_clf_ver);
}

/* 3: a pass in flight is visible live, then reports the engine's final state. */
static void case_live_progress(void) {
    classify_backfill_daemon_stats_t stats;

    reset_fakes(100);
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 100);
    atomic_store(&fake_run_block, 1);

    classify_backfill_daemon_start(NULL, &fake_db);
    wait_flag(&fake_run_entered, 1, "live: the fake pass must be in flight");
    wait_stats(pred_running_with_progress, &stats,
               "live: a pass in flight must be published with the engine's counters");

    expect_str(stats.state, "running", "live: state while the pass runs");
    expect_ll(stats.rows_total, 100, "live: rows_total from the engine's live snapshot");
    expect_ll(stats.rows_left, 100 - stats.rows_processed, "live: rows_left = total - processed");
    expect_ll(stats.finished_at, 0, "live: finished_at while the pass runs");
    if (stats.started_at <= 0) {
        fail("live: started_at", "a running pass must carry its start time");
    }

    /* Let it finish: from here on there is nothing left to do. */
    atomic_store(&fake_needed_ret, 0);
    atomic_store(&fake_rows_left, 0);
    atomic_store(&want_total, 100);
    atomic_store(&fake_release, 1);
    wait_flag(&fake_run_calls, 1, "live: the pass must return after the release");
    wait_stats(pred_done, &stats, "live: the finished pass must publish the engine's state");
    expect_ll(stats.rows_processed, 100, "live: rows_processed after the pass");
    expect_ll(stats.rows_left, 0, "live: rows_left after the pass");

    classify_backfill_daemon_destroy();
    printf("  case 3 ok: live snapshot reported running with rows_processed=%lld, then done\n",
           stats.rows_processed);
}

/* 4: shutdown during a pass -> cooperative stop, no hang, resumable state. */
static void case_shutdown_during_pass(void) {
    classify_backfill_daemon_stats_t stats;
    time_t started;

    reset_fakes(50);
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 50);
    atomic_store(&fake_run_block, 1);
    pthread_mutex_lock(&rec_mutex);
    rec_result.state = CLASSIFY_BACKFILL_RUNNING; /* stopped early: resumable */
    rec_result.rows_done = 10;
    pthread_mutex_unlock(&rec_mutex);

    classify_backfill_daemon_start(NULL, &fake_db);
    wait_flag(&fake_run_entered, 1, "shutdown: the fake pass must be in flight");

    started = time(NULL);
    classify_backfill_daemon_request_shutdown();
    wait_flag(&fake_stop_calls, 1, "shutdown: the engine must be asked to stop");
    classify_backfill_daemon_destroy();
    if (time(NULL) - started > 5) {
        fail("shutdown: destroy()", "joining an in-flight pass took more than 5 s");
    }
    expect_int(atomic_load(&fake_run_calls), 1, "shutdown: the pass must return exactly once");

    classify_backfill_daemon_get_stats(&stats);
    expect_str(stats.state, "running", "shutdown: a stopped pass stays resumable");
    expect_ll(stats.rows_processed, 10, "shutdown: rows_processed of the stopped pass");
    expect_ll(stats.rows_left, 40, "shutdown: rows_left of the stopped pass");
    if (stats.finished_at < stats.started_at) {
        fail("shutdown: timestamps", "the stopped pass must be stamped as finished");
    }

    /* Idempotent: a second destroy joins nothing and must not hang either. */
    classify_backfill_daemon_destroy();
    classify_backfill_daemon_get_stats(&stats);
    expect_str(stats.state, "running", "shutdown: a second destroy must not change the snapshot");

    printf("  case 4 ok: request_stop() was called (%d), destroy() joined, state=%s\n",
           atomic_load(&fake_stop_calls), stats.state);
}

/* 5: a failed pass is reported, non-fatal, and retried on the next poll. */
static void case_failed_then_retry(void) {
    classify_backfill_daemon_stats_t stats;
    char error_text[256];
    reset_fakes(500);
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 500);
    atomic_store(&want_total, 500);
    atomic_store(&fake_run_rc, -1);
    pthread_mutex_lock(&rec_mutex);
    rec_result.state = CLASSIFY_BACKFILL_FAILED;
    rec_result.rows_done = 0;
    pthread_mutex_unlock(&rec_mutex);

    classify_backfill_daemon_start(NULL, &fake_db);
    wait_stats(pred_pass_finished, &stats, "failed: the failing pass must still be published");
    wait_flag(&fake_run_calls, 1, "failed: the engine must be invoked");

    expect_str(stats.state, "failed", "failed: state from the engine's result");
    if (stats.last_error[0] == 0) {
        fail("failed: last_error", "a failed pass must publish a non-empty error");
    }
    snprintf(error_text, sizeof error_text, "%s", stats.last_error);
    if (stats.finished_at < stats.started_at) {
        fail("failed: timestamps", "the failed pass must be stamped as finished");
    }

    /* The thread must still be alive: the next poll retries and succeeds. The
     * work is still there (needed=1, rows_left=500), only the engine's verdict
     * changes. */
    pthread_mutex_lock(&rec_mutex);
    rec_result.state = CLASSIFY_BACKFILL_DONE;
    rec_result.rows_done = 500;
    pthread_mutex_unlock(&rec_mutex);
    atomic_store(&fake_run_rc, 0);

    wait_flag(&fake_run_calls, 2, "retry: the thread must poll again after a failure");
    wait_stats(pred_done, &stats, "retry: the retried pass must publish its outcome");
    expect_ll(stats.rows_processed, 500, "retry: rows_processed of the retried pass");
    expect_str(stats.last_error, "", "retry: a successful pass clears last_error");

    /* Nothing left now: later polls must keep reporting the finished pass. */
    atomic_store(&fake_needed_ret, 0);
    atomic_store(&fake_rows_left, 0);
    wait_stats(pred_done, &stats, "retry: the finished pass must stay reported as done");
    expect_ll(stats.rows_processed, 500, "retry: rows_processed after the work ran out");

    classify_backfill_daemon_destroy();
    printf("  case 5 ok: failed pass reported [%s], retried, then state=done\n", error_text);
}

/* 6: get_stats is a consistent copy while a poller hammers it during a pass. */
static atomic_int poller_stop;
static atomic_int poller_reads;
static atomic_int poller_bad;
static char poller_why[256];

static void *poller_main(void *arg) {
    classify_backfill_daemon_stats_t stats;
    char why[256];

    (void)arg;
    while (!atomic_load(&poller_stop)) {
        classify_backfill_daemon_get_stats(&stats);
        if (!stats_consistent(&stats, why, sizeof why)) {
            if (!atomic_exchange(&poller_bad, 1)) {
                snprintf(poller_why, sizeof poller_why, "%s", why);
            }
            break;
        }
        atomic_fetch_add(&poller_reads, 1);
    }
    return NULL;
}

static void case_no_torn_snapshot(void) {
    classify_backfill_daemon_stats_t stats;
    pthread_t poller;
    int waited = 0;

    reset_fakes(200);
    atomic_store(&fake_needed_ret, 1);
    atomic_store(&fake_rows_left, 200);
    atomic_store(&fake_run_block, 1);
    atomic_store(&poller_stop, 0);
    atomic_store(&poller_reads, 0);
    atomic_store(&poller_bad, 0);
    poller_why[0] = '\0';

    classify_backfill_daemon_start(NULL, &fake_db);
    wait_flag(&fake_run_entered, 1, "torn: the fake pass must be in flight");

    if (pthread_create(&poller, NULL, poller_main, NULL) != 0) {
        fail("torn: pthread_create", "could not start the poller");
    }
    /* Hammer for a while: the pass keeps republishing live counters while the
     * poller copies the merged snapshot. */
    while (atomic_load(&poller_reads) < 2000 && waited < WAIT_TIMEOUT_MS) {
        usleep(1000);
        waited++;
    }
    atomic_store(&poller_stop, 1);
    pthread_join(poller, NULL);

    expect_int(atomic_load(&poller_bad), 0, "torn: every snapshot must be consistent");
    if (atomic_load(&poller_reads) < 100) {
        fail("torn: the poller read too few snapshots to prove anything", poller_why);
    }

    /* The in-flight pass is still consistent with the module's own view. */
    classify_backfill_daemon_get_stats(&stats);
    if (!stats_consistent(&stats, poller_why, sizeof poller_why)) {
        fail("torn: the snapshot after the hammering", poller_why);
    }

    atomic_store(&fake_release, 1);
    classify_backfill_daemon_destroy();
    printf("  case 6 ok: %d snapshots read while a pass ran, all consistent\n",
           atomic_load(&poller_reads));
}

int main(void) {
    install_hooks();

    case_before_start();
    case_idle();
    case_run();
    case_live_progress();
    case_shutdown_during_pass();
    case_failed_then_retry();
    case_no_torn_snapshot();

    printf("test_classify_backfill_daemon: all assertions passed\n");
    return 0;
}
