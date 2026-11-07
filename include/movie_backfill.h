#ifndef MOVIE_BACKFILL_H
#define MOVIE_BACKFILL_H

#include "database.h"
#include "movie_kind.h"

/*
 * Resumable re-labelling of the movie-kind columns of existing torrent_class
 * rows: a *sibling* of include/classify_backfill.h, not a parameterisation of
 * it. The fastText engine owns clf_ver and the tparse columns; this one owns
 * movie_kind_mask and movie_kind_clf_ver and nothing else, so the two passes
 * can never make each other think a row is up to date.
 *
 * Membership is decided by the stored generation alone: a row is a candidate
 * when
 *
 *     category = TP_CAT_MOVIE AND adult = 0
 *     AND (movie_kind_clf_ver IS NULL OR movie_kind_clf_ver < target)
 *
 * movie_kind_mask = 0 is a *valid* label (the `none` verdict), never a marker,
 * so it takes no part in the predicate. Correctness never depends on the resume
 * key: the predicate alone makes a re-run idempotent, the key
 * (classifier_meta.movie_backfill_last_id, "<generation>:<id>", tombstoned at
 * completion) only makes a restart cheap.
 *
 * The pass writes exactly two columns and two meta keys:
 *   - torrent_class.movie_kind_mask, torrent_class.movie_kind_clf_ver (UPDATE
 *     only - a candidate always has a class row, the predicate joins it);
 *   - classifier_meta.movie_backfill_last_id after every committed batch;
 *   - classifier_meta.movie_kind_clf_ver once, at pass completion, the same
 *     discipline classifier.c uses for clf_ver.
 * clf_ver, category, adult and every other fastText column are never written
 * (tests/test_movie_backfill.c asserts it row by row).
 *
 * Gating mirrors include/movie_kind.h: the caller owns movie_kind_init(); the
 * engine only asks movie_kind_enabled() and movie_kind_generation(). A build
 * without MK_ENABLED, or a phone without the verified v9 assets, reports
 * DISABLED and writes nothing.
 *
 * Threading: one pass per database at a time. The engine takes
 * database_t.mutex around each BEGIN ... COMMIT batch only, never while
 * encoding, so the batch writer and the HTTP readers are not starved.
 */

/* classifier_meta keys. Generation-scoped on purpose: a value left behind by an
 * older generation can never make a newer pass skip rows. */
#define MOVIE_BACKFILL_RESUME_KEY "movie_backfill_last_id"
/* Written only when a pass completes: "every candidate sits at this
 * generation". Same discipline as classifier_meta.clf_ver. */
#define MOVIE_BACKFILL_VER_KEY "movie_kind_clf_ver"

/* State of a pass. The names are fixed - /stats and the acceptance criteria
 * quote them - and there is no "failed" state: a pass that ends early on a stop
 * request and a pass that dies on a SQL error both publish "stopped", and only
 * the daemon's last_error says which (the pass is resumable either way). */
typedef enum {
    MOVIE_BACKFILL_IDLE = 0,  /* never ran */
    MOVIE_BACKFILL_RUNNING,   /* walking */
    MOVIE_BACKFILL_DONE,      /* every candidate sits at the target generation */
    MOVIE_BACKFILL_STOPPED,   /* ended early: stop requested, limit reached, or error */
    MOVIE_BACKFILL_DISABLED   /* feature off / no verified manifest / no target */
} movie_backfill_state_t;

/* Short lowercase name for /stats and log lines ("idle", "running", "done",
 * "stopped", "disabled"; "unknown" for anything else). */
const char *movie_backfill_state_name(int state);

typedef struct {
    long long target_clf_ver; /* target generation; <= 0 takes the loaded
                               * manifest's generation */
    int chunk_ids;            /* torrents per id window; <= 0 -> 10000 */
    int batch_rows;           /* rows per transaction; <= 0 -> 500 */
    int dry_run;              /* count the candidates, write nothing */
    long long limit_rows;     /* stop after this many rows; <= 0 -> unlimited */
    int thermal_backoff_c;    /* pause at/above this CPU temperature;
                               * <= 0 disables the thermal gate */
} movie_backfill_opts_t;

typedef struct {
    int state;                /* movie_backfill_state_t */
    long long target_clf_ver; /* generation this pass writes */
    long long rows_total;     /* candidates counted at run start */
    long long rows_done;      /* candidates handled (labelled + failed) */
    long long rows_updated;   /* committed UPDATEs */
    long long rows_skipped;   /* candidates left unprocessed by an early stop */
    long long rows_failed;    /* no verdict, or the row disappeared mid-pass */
    long long last_id;        /* highest committed torrent id */
    long long thermal_pauses; /* back-off episodes this pass waited through */
} movie_backfill_progress_t;

/*
 * Run (or resume) one pass over `db`.
 *
 * @param db   open database; the caller owns schema creation and cleanup
 * @param opts knobs; NULL means "all defaults, write mode, unlimited, no
 *             thermal gate"
 * @param out  progress snapshot, always filled when non-NULL
 * @return 0  finished: DONE, or STOPPED when it ended early (resumable);
 *         1  DISABLED: no verified manifest or no resolvable target - logged,
 *            nothing written;
 *        -1  the pass failed (SQL error that survived the busy retries); the
 *            state is STOPPED and the error is logged.
 */
int movie_backfill_run(database_t *db, const movie_backfill_opts_t *opts,
                       movie_backfill_progress_t *out);

/*
 * 1 when a pass is worth starting: the module is enabled, the target generation
 * resolves, and either classifier_meta.movie_kind_clf_ver or the torrent_class
 * rows are behind it. *rows_left (optional) receives the number of candidates
 * below the target; it is filled in both directions so a caller can report
 * either state.
 */
int movie_backfill_needed(database_t *db, long long target_clf_ver, long long *rows_left);

/*
 * Ask a running pass to stop. Cooperative and checked between batches and
 * inside a thermal pause: the batch in flight always commits, the state is left
 * STOPPED and the resume key makes the next run continue where this one
 * stopped.
 */
void movie_backfill_request_stop(void);

/*
 * Last published progress of the most recent pass, for anything that has to
 * watch a pass it did not start (/stats, a supervising thread, a test).
 *
 * Safe from any thread at any time, including while a pass runs: it copies the
 * module-global snapshot under its own mutex, so reading progress never
 * requires database_t.mutex and never blocks the pass.
 */
void movie_backfill_get_progress(movie_backfill_progress_t *out);

/*
 * `--movie-kind-bench` one-shot: encode the first `rows` movie+adult=0 names of
 * `db_path` in a loop for at least ten minutes (or once over all of them when
 * `dump_masks` is set) and print one JSON line with the per-name timing,
 * VmHWM and the hottest CPU temperature seen.
 *
 * Read-only by construction: the database is opened with SQLITE_OPEN_READONLY
 * and nothing is ever written. `threads` is the configured encoder thread count
 * (config.movie_kind_threads, clamped to 1..8) and is echoed in the JSON.
 *
 * With `dump_masks` one "<mask> <hex(name)>" line is printed per row - hex of
 * the raw database name bytes, so the dump survives names that are not valid
 * UTF-8 - and the JSON line follows them on stdout.
 *
 * @return 0 on success, 1 when the module is disabled (no verified manifest or
 *         no encoder in this build: nothing is printed), -1 on an I/O error.
 */
int movie_backfill_bench(const char *db_path, int rows, int threads, int dump_masks);

#ifdef MOVIE_BACKFILL_TEST
/*
 * Test-only seam, compiled only when MOVIE_BACKFILL_TEST is defined (the
 * tests-movie-backfill Makefile target). Every call the engine makes into the
 * movie-kind module - and the thermal reader and the sleeper - goes through a
 * file-static function pointer; this replaces them, so a host test can label
 * rows without a 152.8 MiB model and can drive a thermal pause without a hot
 * CPU. NULL members keep their current value. The daemon build exports none of
 * this.
 */
typedef struct {
    int (*classify)(const char *name, uint16_t *mask);
    int (*enabled)(void);
    long long (*generation)(void);
    int (*thermal_max_c)(void);
    void (*sleep_ms)(long long ms);
    void (*note_thermal_pause)(void);
} movie_backfill_hooks_t;

void movie_backfill_set_hooks(const movie_backfill_hooks_t *hooks);
#endif /* MOVIE_BACKFILL_TEST */

#endif /* MOVIE_BACKFILL_H */
