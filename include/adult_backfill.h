#ifndef ADULT_BACKFILL_H
#define ADULT_BACKFILL_H

#include "database.h"

/*
 * Token-gated, resumable adult re-scoring of existing torrent_class rows.
 *
 * The generic engine (include/classify_backfill.h) relabels every row below the
 * loaded generation with all three students. That is the wrong tool for a
 * targeted adult refresh: the adult model is only worth running on rows whose
 * text actually carries one of the parser's adult tokens (`__adult_jav`,
 * `__adult_site`, `__adult_set`), which is a fraction of a percent of the
 * table, and the pass must never rewrite a category or a tv_kind verdict.
 *
 * So this engine:
 *   - selects rows whose adult_clf_ver is NULL or below the target generation
 *     (the column is both the candidate predicate and the watermark);
 *   - gates each row on the name-only feature text BEFORE any inference, and
 *     loads the (287 M-row) file table only for rows that pass - then re-checks
 *     on the full text. A row whose `__adult_set` fires only through file paths
 *     is a documented miss: a recall trade-off, never a correctness issue,
 *     because fresh ingest classifies everything;
 *   - scores the gated rows with the adult student alone
 *     (classifier_classify_adult), so the pass pays no category cost and does
 *     not depend on the category model having loaded;
 *   - writes only adult, adult_p and adult_clf_ver, inside one BEGIN ... COMMIT
 *     per batch, and never deletes: in adult_mode=drop a backfilled adult row is
 *     TAGGED, exactly as the ingest path tags it. Deletion stays an ingest-only,
 *     operator-visible decision.
 *
 * Gating mirrors the other engines: this module never calls classifier_init()
 * (the caller owns init and shutdown) and only asks classifier_models_loaded()
 * and classifier_current_version(). A build without CLF_ENABLED - the crawler's
 * own host `make` - has no models and no tparse, so every run reports DISABLED
 * before it can reach the gate.
 *
 * Threading: one pass per database at a time. database_t.mutex is taken around
 * each BEGIN ... COMMIT batch only, never while parsing or inferring.
 */

/* State of a pass. RUNNING after a stopped pass means "resumable". */
typedef enum {
    ADULT_BACKFILL_IDLE = 0,  /* never ran */
    ADULT_BACKFILL_RUNNING,   /* walking, or stopped early and resumable */
    ADULT_BACKFILL_DONE,      /* every candidate sits at the target generation */
    ADULT_BACKFILL_FAILED,    /* SQL error that survived the busy retries */
    ADULT_BACKFILL_DISABLED   /* no models loaded / no resolvable target */
} adult_backfill_state_t;

/* Short lowercase name for /stats and log lines ("idle", "running", "done",
 * "failed", "disabled"; "unknown" for anything else). */
const char *adult_backfill_state_name(int state);

typedef struct {
    long long target_adult_clf_ver; /* target generation; <= 0 takes the loaded
                                     * manifest's version */
    int chunk_ids;                  /* torrents per id window; <= 0 -> 10000 */
    int batch_rows;                 /* rows per transaction; <= 0 -> 500 */
    int dry_run;                    /* count the candidates, write nothing */
    long long limit_rows;           /* stop after this many scored rows; <= 0 ->
                                     * unlimited */
} adult_backfill_opts_t;

typedef struct {
    int state;                      /* adult_backfill_state_t */
    long long target_adult_clf_ver; /* generation this pass writes */
    long long rows_total;           /* candidates counted at run start */
    long long rows_gated_out;       /* candidates the token gate rejected */
    long long rows_scored;          /* candidates the adult student scored */
    long long rows_updated;         /* committed UPDATEs that changed a row */
    long long rows_failed;          /* no verdict available: stored row untouched */
    long long last_id;              /* highest committed torrent id */
} adult_backfill_progress_t;

/*
 * Run (or resume) one adult backfill pass over `db`.
 *
 * @param db   open database; the caller owns schema creation and cleanup
 * @param opts knobs; NULL means "all defaults, write mode, unlimited"
 * @param out  progress snapshot, always filled when non-NULL
 * @return 0  finished: state DONE, or RUNNING when stopped early (resumable);
 *         1  DISABLED: models not loaded or no resolvable target - logged,
 *            nothing written;
 *        -1  FAILED: a SQL error that survived the busy retries.
 */
int adult_backfill_run(database_t *db, const adult_backfill_opts_t *opts,
                       adult_backfill_progress_t *out);

/*
 * 1 when a pass is worth starting: models are loaded, the target generation
 * resolves, and at least one torrent_class row is below it. *rows_left
 * (optional) receives that count in both directions, so a caller can report
 * either state. Purely the row predicate - there is no meta key to consult, so
 * a finished pass answers 0 and a re-run is a no-op by construction.
 */
int adult_backfill_needed(database_t *db, long long target_adult_clf_ver,
                          long long *rows_left);

/*
 * Ask a running pass to stop. Cooperative and checked between batches: the
 * batch in flight always commits, the state is left RUNNING and the resume key
 * makes the next run continue where this one stopped.
 */
void adult_backfill_request_stop(void);

/*
 * Last published progress of the most recent pass, for anything that has to
 * watch a pass it did not start (/stats, a supervising thread, a test). Safe to
 * call from any thread at any time: it copies the module-global snapshot under
 * its own mutex, so reading progress never requires database_t.mutex and never
 * blocks the pass. *out may be NULL (no-op).
 */
void adult_backfill_get_progress(adult_backfill_progress_t *out);

#ifdef ADULT_BACKFILL_TEST
/*
 * Test-only seam (the tests-adult-backfill Makefile target). The engine calls
 * the adult student through a file-static pointer; this replaces it so a
 * model-free host test can drive the gate, the UPDATE and the watermark path
 * with canned verdicts. The daemon build exports none of this.
 */
typedef int (*adult_backfill_verdict_fn)(const torrent_metadata_t *meta, int *adult,
                                         double *adult_p);
void adult_backfill_set_verdict_hook(adult_backfill_verdict_fn fn);

/*
 * The same seam for the token gate. The model-free host build has no tparse, so
 * ab_gate_text() answers "pass" for every row and nothing can be gated out;
 * a test that has to prove the DRY RUN walks the gate needs a gate it can
 * steer. Only compiled into the tests-adult-backfill build.
 */
typedef int (*adult_backfill_gate_fn)(const char *name, const file_info_t *files,
                                      int num_files, long long size_bytes);
void adult_backfill_set_gate_hook(adult_backfill_gate_fn fn);
#endif /* ADULT_BACKFILL_TEST */

#endif /* ADULT_BACKFILL_H */
