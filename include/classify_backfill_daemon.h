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
