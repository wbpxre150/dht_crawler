#ifndef CLASSIFY_BACKFILL_DAEMON_H
#define CLASSIFY_BACKFILL_DAEMON_H

#include "database.h"
#include "dht_crawler.h"

#ifdef CLF_BACKFILL_DAEMON_TEST
#include "classify_backfill.h"
#endif

/*
 * Supervised background thread that keeps the stored classification generation
 * in step with the loaded model manifest.
 *
 * The engine (include/classify_backfill.h) decides whether rows are behind and
 * relabels a batch of them; this module decides *when* to run it, without being
 * told: it polls once at start and then once a minute, and starts a pass when
 * the engine reports candidates below the target generation. The target is
 * always read from the loaded manifest through the classifier, so a new model
 * generation needs new model files and no change here.
 *
 * Published state: the engine owns the counters and the state of a pass, this
 * module owns the timestamps and the error text, and
 * classify_backfill_daemon_get_stats() merges both into one snapshot - the shape
 * /stats renders. A pass in flight is visible live (the merged snapshot carries
 * the engine's current counters), so a reader never has to wait for a pass to
 * finish to see progress.
 *
 * Threading: every entry point is safe to call from any thread. start() spawns
 * the thread and returns immediately; request_shutdown() only sets the stop flag
 * and asks an in-flight pass to stop cooperatively (the batch it is in commits);
 * destroy() joins and is idempotent. The module never holds database_t.mutex -
 * the engine takes it per BEGIN ... COMMIT batch and it is not recursive.
 *
 * The module is inert, not fatal, in a build or config without models: the
 * classifier reports itself disabled/unloaded and the published state is
 * "disabled" with zero counters, so /stats has an object to render in every
 * flavour.
 */

typedef struct {
    char state[16];           /* classify_backfill_state_name(): idle|running|done|failed|disabled */
    int target_clf_ver;       /* generation this pass writes; 0 = unknown */
    long long rows_left;      /* candidates still below the target; stale by <= the poll interval when idle */
    long long rows_processed; /* rows handled by the current/last pass */
    long long rows_total;     /* rows_processed + rows_left; 0 when unknown */
    long long started_at;     /* epoch secs of the last engine invocation, 0 = engine never invoked */
    long long finished_at;    /* epoch secs the last pass ended, 0 = not finished */
    char last_error[256];     /* "" normally, non-empty after a failed pass */
} classify_backfill_daemon_stats_t;

/*
 * Spawn the background thread. Non-blocking: the first decision is taken by the
 * thread, never by the caller, so this is safe to call right before the daemon's
 * run loop. A second call is a logged no-op.
 *
 * @param ctx application context (log level, start time); may be NULL
 * @param db  open database whose schema exists and whose classifier is
 *            initialised; not owned, must outlive the thread (destroy() first)
 */
void classify_backfill_daemon_start(app_context_t *ctx, database_t *db);

/*
 * Select which pass the thread runs: "all" (the historical full re-classify,
 * also the default when this is never called), "adult" (the token-gated adult
 * pass only) or "off" (never touch the database; publish the disabled state).
 *
 * Call once from main.c before start(); the value is copied, so the caller's
 * buffer need not outlive the call. Anything unrecognised keeps the current
 * mode, mirroring how the config layer keeps the previous value on a bad
 * classify_backfill_mode. Race-free by contract for the same reason as
 * classifier_set_adult_mode(): it runs before the thread exists.
 */
void classify_backfill_daemon_set_mode(const char *mode);

/*
 * Adult-pass surface: the same shape as classify_backfill_daemon_stats_t, but
 * for the token-gated adult engine. Separate because the two passes report
 * different counters (gated_out/scored/updated instead of
 * processed/inserted/skipped) and /stats renders both objects independently.
 * Zero counters plus state "idle" mean the adult pass never ran in this
 * process, which is what the release expects while classify_backfill_mode is
 * pinned to "off".
 */
typedef struct {
    char state[16];                  /* adult_backfill_state_name() */
    long long target_adult_clf_ver;  /* generation the adult pass writes; 0 = unknown */
    long long rows_total;            /* candidates counted at pass start */
    long long rows_gated_out;        /* candidates the token gate rejected */
    long long rows_scored;           /* candidates the adult student scored */
    long long rows_updated;          /* committed UPDATEs that changed a row */
    long long rows_failed;           /* no verdict available: row left untouched */
    long long last_id;               /* highest committed torrent id */
    long long started_at;            /* epoch secs of the last adult pass, 0 = never */
    long long finished_at;           /* epoch secs it ended, 0 = not finished */
    char last_error[256];            /* "" normally, non-empty after a failure */
} classify_backfill_daemon_adult_stats_t;

/*
 * Copy the current adult-pass snapshot into *out; *out may be NULL (no-op).
 * Safe from any thread at any time, including while a pass runs: it merges the
 * module's mutex-guarded stats with the engine's own live snapshot, so the
 * returned struct is always internally consistent.
 */
void classify_backfill_daemon_get_adult_stats(classify_backfill_daemon_adult_stats_t *out);

/*
 * Ask the thread to stop: sets its stop flag (it is out of the sliced sleep
 * within 250 ms) and, when a pass is in flight, asks the engine to stop after
 * the batch it is in commits. Returns immediately; pair it with destroy().
 */
void classify_backfill_daemon_request_shutdown(void);

/*
 * Join the thread. Idempotent, and a no-op when start() never ran. Must be
 * called before the database is closed, so an in-flight pass cannot write to a
 * closed connection.
 */
void classify_backfill_daemon_destroy(void);

/*
 * Copy the current snapshot into *out; *out may be NULL (no-op). Safe from any
 * thread at any time, including while a pass runs: it copies the module's
 * mutex-guarded stats and, when a pass is in flight, merges the engine's own
 * live snapshot (which needs no database lock), so the returned struct is always
 * internally consistent - rows_total == rows_processed + rows_left, and
 * finished_at >= started_at whenever a pass has finished.
 */
void classify_backfill_daemon_get_stats(classify_backfill_daemon_stats_t *out);

#ifdef CLF_BACKFILL_DAEMON_TEST
/*
 * Test-only seam, compiled only when CLF_BACKFILL_DAEMON_TEST is defined (the
 * tests-backfill-daemon Makefile target). The module calls the engine and the
 * classifier through file-static function pointers; this replaces them so a
 * host test needs no database, no models and no 60 s wait. NULL members keep
 * their current value; the daemon build exports none of this.
 */
typedef struct {
    int (*needed)(database_t *db, long long target_clf_ver, long long *rows_left);
    int (*run)(database_t *db, const classify_backfill_opts_t *opts,
               classify_backfill_progress_t *out);
    void (*request_stop)(void);
    void (*get_progress)(classify_backfill_progress_t *out);
    int (*enabled)(void);
    int (*models_loaded)(void);
    long long (*current_version)(void);
    void (*sleep_ms)(long long ms);
} classify_backfill_daemon_hooks_t;

void classify_backfill_daemon_set_hooks(const classify_backfill_daemon_hooks_t *hooks);
#endif /* CLF_BACKFILL_DAEMON_TEST */

#endif /* CLASSIFY_BACKFILL_DAEMON_H */
