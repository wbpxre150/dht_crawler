#ifndef MOVIE_BACKFILL_DAEMON_H
#define MOVIE_BACKFILL_DAEMON_H

#include "database.h"
#include "dht_crawler.h"
#include "movie_kind.h"

#ifdef MOVIE_BACKFILL_DAEMON_TEST
#include "movie_backfill.h"
#endif

/*
 * Supervised background thread that keeps the stored movie-kind generation in
 * step with the loaded v9 manifest.
 *
 * The engine (include/movie_backfill.h) decides whether rows are behind and
 * relabels a batch of them; this module decides *when* to run it, without being
 * told: it polls once at start and then once a minute, and starts a pass when
 * the engine reports candidates below the target generation. The target is
 * always read from the loaded manifest through movie_kind, so a new model
 * generation needs new model files and no change here.
 *
 * Published state strings are fixed to
 *
 *     disabled | idle | running | stopped | done
 *
 * and nothing else (the acceptance criteria quote them). "disabled" means this
 * build or this phone cannot run the pass at all - no encoder in the build, no
 * verified v9 manifest, or movie_kind_enabled = 0 - and is decided before any
 * database scan, so a phone without the v9 assets reports "disabled" and never
 * crashes or loops. "stopped" is a pass that ended without reaching the target:
 * a cooperative stop (resumable) or a SQL error, which the non-empty
 * last_error distinguishes.
 *
 * Published counters: the engine owns the state and the counters of a pass, this
 * module owns the timestamps and the error text, movie_kind owns the
 * module-lifetime thermal/model counters, and movie_backfill_daemon_get_stats()
 * merges all three into one snapshot - the shape /stats renders. A pass in
 * flight is visible live, so a reader never has to wait for it to finish.
 *
 * Threading: every entry point is safe to call from any thread. start() spawns
 * the thread and returns immediately; request_shutdown() only sets the stop flag
 * and asks an in-flight pass to stop cooperatively (the batch it is in commits);
 * destroy() joins and is idempotent. The module never holds database_t.mutex -
 * the engine takes it per BEGIN ... COMMIT batch and it is not recursive.
 *
 * The module is inert, not fatal, in a build or config without models: the
 * published state is "disabled" with zero counters, so /stats has an object to
 * render in every flavour.
 */

typedef struct {
    char state[16];           /* movie_backfill_state_name(): disabled|idle|running|stopped|done */
    int target_clf_ver;       /* generation this pass writes; 0 = unknown */
    long long rows_left;      /* candidates still below the target; stale by <= the poll interval when idle */
    long long rows_processed; /* rows handled by the current/last pass */
    long long rows_total;     /* rows_processed + rows_left; 0 when unknown */
    long long started_at;     /* epoch secs of the last engine invocation, 0 = engine never invoked */
    long long finished_at;    /* epoch secs the last pass ended, 0 = not finished */
    char last_error[256];     /* "" normally, non-empty after a failed pass */
    long long thermal_pauses; /* thermal back-off episodes, from the movie-kind module */
    long long model_load_failures; /* unusable manifest / model loads, from the module */
} movie_backfill_daemon_stats_t;

/*
 * Spawn the background thread. Non-blocking: the first decision is taken by the
 * thread, never by the caller, so this is safe to call right before the daemon's
 * run loop. A second call is a logged no-op.
 *
 * @param ctx                 application context (log level, start time); may be NULL
 * @param db                  open database whose schema exists and whose movie-kind
 *                            module is initialised; not owned, must outlive the
 *                            thread (destroy() first)
 * @param thermal_backoff_c   config.movie_kind_thermal_backoff_c: pause a pass at
 *                            or above this CPU temperature; <= 0 disables the gate
 */
void movie_backfill_daemon_start(app_context_t *ctx, database_t *db, int thermal_backoff_c);

/*
 * Ask the thread to stop: sets its stop flag (it is out of the sliced sleep
 * within 250 ms) and, when a pass is in flight, asks the engine to stop after the
 * batch it is in commits (a thermal pause is interrupted too). Returns
 * immediately; pair it with destroy().
 */
void movie_backfill_daemon_request_shutdown(void);

/*
 * Join the thread. Idempotent, and a no-op when start() never ran. Must be called
 * before the database is closed, so an in-flight pass cannot write to a closed
 * connection.
 */
void movie_backfill_daemon_destroy(void);

/*
 * Copy the current snapshot into *out; *out may be NULL (no-op). Safe from any
 * thread at any time, including while a pass runs: it copies the module's
 * mutex-guarded stats, merges the engine's own live snapshot when a pass is in
 * flight (which needs no database lock) and the movie-kind module's lifetime
 * counters, so the returned struct is always internally consistent -
 * rows_total == rows_processed + rows_left.
 */
void movie_backfill_daemon_get_stats(movie_backfill_daemon_stats_t *out);

#ifdef MOVIE_BACKFILL_DAEMON_TEST
/*
 * Test-only seam, compiled only when MOVIE_BACKFILL_DAEMON_TEST is defined (the
 * tests-movie-backfill-daemon Makefile target). The module calls the engine and
 * the movie-kind module through file-static function pointers; this replaces
 * them so a host test needs no database, no model files and no 60 s wait. NULL
 * members keep their current value; the daemon build exports none of this.
 */
typedef struct {
    int (*needed)(database_t *db, long long target_clf_ver, long long *rows_left);
    int (*run)(database_t *db, const movie_backfill_opts_t *opts,
               movie_backfill_progress_t *out);
    void (*request_stop)(void);
    void (*get_progress)(movie_backfill_progress_t *out);
    int (*enabled)(void);
    long long (*generation)(void);
    void (*get_movie_stats)(movie_kind_stats_t *out);
    void (*sleep_ms)(long long ms);
} movie_backfill_daemon_hooks_t;

void movie_backfill_daemon_set_hooks(const movie_backfill_daemon_hooks_t *hooks);
#endif /* MOVIE_BACKFILL_DAEMON_TEST */

#endif /* MOVIE_BACKFILL_DAEMON_H */
