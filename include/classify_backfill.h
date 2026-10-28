#ifndef CLASSIFY_BACKFILL_H
#define CLASSIFY_BACKFILL_H

#include "database.h"

/*
 * Generic, resumable re-classification of existing torrent_class rows.
 *
 * The engine relabels every torrent whose class row is missing or was written
 * by an older model generation with whatever generation the *loaded* manifest
 * describes, so it is model-agnostic by construction: shipping a new generation
 * needs new model files, never a change here. It only ever upserts on
 * torrent_id (no row is removed) and correctness never depends on the resume
 * key: the "class row missing or below target" predicate alone makes a re-run
 * idempotent, the key only makes a restart cheap.
 *
 * Gating mirrors include/classifier.h: the engine never calls classifier_init()
 * (the caller owns init and shutdown), it only asks classifier_models_loaded()
 * and classifier_current_version(). A build without CLF_ENABLED — the
 * crawler's own host `make` — has no models, so every run reports DISABLED and
 * writes nothing.
 *
 * Threading: one pass per database at a time. The engine takes
 * database_t.mutex around each BEGIN ... COMMIT batch only, never while
 * parsing or inferring, so the batch writer and the HTTP readers are not
 * starved.
 */

/* State of a pass. RUNNING after a stopped pass means "resumable". */
typedef enum {
    CLASSIFY_BACKFILL_IDLE = 0,  /* never ran */
    CLASSIFY_BACKFILL_RUNNING,   /* walking, or stopped early and resumable */
    CLASSIFY_BACKFILL_DONE,      /* every candidate sits at the target generation */
    CLASSIFY_BACKFILL_FAILED,    /* SQL error that survived the busy retries */
    CLASSIFY_BACKFILL_DISABLED   /* no models loaded / no resolvable target */
} classify_backfill_state_t;

/* Short lowercase name for /stats and log lines ("idle", "running", "done",
 * "failed", "disabled"; "unknown" for anything else). */
const char *classify_backfill_state_name(int state);

typedef struct {
    long long target_clf_ver; /* target generation; <= 0 takes the loaded
                               * manifest's version */
    int chunk_ids;            /* torrents per id window; <= 0 -> 10000 */
    int batch_rows;           /* rows per transaction; <= 0 -> 500 */
    int dry_run;              /* count the candidates, write nothing */
    long long limit_rows;     /* stop after this many rows; <= 0 -> unlimited */
} classify_backfill_opts_t;

typedef struct {
    int state;                /* classify_backfill_state_t */
    long long target_clf_ver; /* generation this pass writes */
    long long rows_total;     /* candidates counted at run start */
    long long rows_done;      /* candidates handled (classified + failed) */
    long long rows_updated;   /* committed upserts that replaced a row */
    long long rows_inserted;  /* committed upserts that created a row */
    long long rows_skipped;   /* candidates left unprocessed by an early stop */
    long long rows_failed;    /* no verdict available: stored row untouched */
    long long last_id;        /* highest committed torrent id */
} classify_backfill_progress_t;

/*
 * Run (or resume) one backfill pass over `db`.
 *
 * @param db   open database; the caller owns schema creation and cleanup
 * @param opts knobs; NULL means "all defaults, write mode, unlimited"
 * @param out  progress snapshot, always filled when non-NULL
 * @return 0  finished: state DONE, or RUNNING when stopped early (resumable);
 *         1  DISABLED: models not loaded or no resolvable target — logged,
 *            nothing written;
 *        -1  FAILED: a SQL error that survived the busy retries.
 */
int classify_backfill_run(database_t *db, const classify_backfill_opts_t *opts,
                          classify_backfill_progress_t *out);

/*
 * 1 when a pass is worth starting: models are loaded, the target generation
 * resolves, and either classifier_meta.clf_ver or the torrent_class rows are
 * behind it. *rows_left (optional) receives the number of torrents below the
 * target; it is filled in both directions so a caller can report either state.
 */
int classify_backfill_needed(database_t *db, long long target_clf_ver,
                             long long *rows_left);

/*
 * Ask a running pass to stop. Cooperative and checked between batches: the
 * batch in flight always commits, the state is left RUNNING and the resume key
 * makes the next run continue where this one stopped.
 */
void classify_backfill_request_stop(void);

/*
 * Last published progress of the most recent pass, for anything that has to
 * watch a pass it did not start (/stats, a supervising thread, a test).
 *
 * Safe to call from any thread at any time, including while a pass is running:
 * it copies the module-global snapshot under its own mutex, so reading progress
 * never requires holding database_t.mutex and never blocks the pass. The
 * snapshot carries the state and every counter classify_backfill_run() reports
 * through *out, republished at run start, after each committed batch, after each
 * window and at every exit path. Before the first pass in this process it is the
 * zeroed IDLE state. *out may be NULL (no-op).
 *
 * rows_total is counted from the resume point, so a live pass reports
 * rows_left == rows_total - rows_done rather than a lifetime total; a reader that
 * wants "work still to do" derives it and needs no extra field here.
 */
void classify_backfill_get_progress(classify_backfill_progress_t *out);

#endif /* CLASSIFY_BACKFILL_H */
