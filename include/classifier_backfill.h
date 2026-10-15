#ifndef CLASSIFIER_BACKFILL_H
#define CLASSIFIER_BACKFILL_H

#include "database.h"

#include <stdint.h>

/*
 * Stage 8: resumable backfill / re-classify job.
 *
 * The ingest path (tree_metadata.c -> classifier_classify()) only labels the
 * torrents discovered while the classifier is on. A database that predates
 * the classifier therefore carries rows with no torrent_class row at all, and
 * one whose model version moved on carries rows with a stale clf_ver. This job
 * closes both gaps from inside the daemon: one always-on, low-priority thread
 * that sweeps the existing `torrents` rows in id order and writes exactly the
 * torrent_class row the ingest path would have written for them.
 *
 * Shape (classifier.md stage 8):
 *   - keyset pagination (id > ckpt ORDER BY id LIMIT 2000) so a page is never
 *     read twice whatever the ingest side inserts meanwhile;
 *   - a row is (re)classified when it has no torrent_class row or when its
 *     clf_ver is below the model version currently loaded;
 *   - per torrent the WHOLE stored file list is read (idx_files_torrent,
 *     torrent_files.id/rowid order, which reconstructs BEP9 file order even
 *     past the int16_t file_index wrap), uncapped exactly like the ingest path,
 *     so the labels
 *     are bit-identical to the ingest-time ones (see src/classifier_backfill.c);
 *   - one short transaction per page: the torrent_class upserts and the
 *     classifier_meta['backfill_ckpt'] update commit together, followed by a
 *     WAL checkpoint exactly as batch_writer_flush() does;
 *   - the checkpoint makes the job resumable across restarts: a restart
 *     resumes at the last committed id, and a sweep that started at id 0 and
 *     found nothing left to classify ends the job (any sweep that still
 *     classified rows is followed by a fresh sweep from id 0, which catches
 *     rows left stale below the old checkpoint - e.g. after a model bump);
 *   - no config key and no CLI flag gates it: a daemon start starts it, and a
 *     model/version bump arrives as a config apply plus restart.
 *
 * Progress is published for /stats under the job's own mutex; it is a snapshot
 * of a running sweep, not a classifier counter.
 *
 * Build flavours: the thread needs classifier_classify() to answer, so on a
 * build without CLF_ENABLED or with classifier_enabled=0 it reports
 * available=false, state=idle and exits without touching the database.
 */

typedef enum {
    CLASSIFIER_BACKFILL_IDLE = 0, /* not sweeping: not started, unavailable, or stopped */
    CLASSIFIER_BACKFILL_RUNNING,  /* a sweep over the id space is in progress */
    CLASSIFIER_BACKFILL_COMPLETE  /* a sweep from id 0 found nothing left to classify */
} classifier_backfill_state_t;

typedef struct {
    int state;          /* classifier_backfill_state_t */
    int available;      /* 1 when the classifier can label rows (built in + enabled) */
    uint64_t done;      /* rows classified by the current sweep */
    uint64_t total;     /* rows the current sweep has to classify (counted once, at its start) */
    double rate_rows_s; /* done / current sweep wall time, 0.0 before the first row */
    long long ckpt;     /* keyset cursor: highest torrents.id committed by this sweep */
    long long clf_ver;  /* model version the sweep classifies against (0 = none loaded) */
} classifier_backfill_stats_t;

/*
 * Start the job. The thread borrows `db` (it must outlive it: stop the job
 * before database_cleanup()) and owns nothing else.
 * @return 0 when the thread was started (or was already running), -1 when it
 *         could not be spawned. Non-fatal for the daemon: it only logs.
 */
int classifier_backfill_start(database_t *db);

/*
 * Ask the thread to stop and join it. Idempotent; called from the shutdown
 * path only. The stop flag is checked between pages and between torrents, so
 * the call blocks for about one torrent classification at most.
 */
void classifier_backfill_stop(void);

/* Copy the current progress into *out (zeroed when the job never ran). */
void classifier_backfill_get_stats(classifier_backfill_stats_t *out);

/* Display name for a classifier_backfill_state_t value, for the /stats JSON. */
const char *classifier_backfill_state_name(int state);

#endif /* CLASSIFIER_BACKFILL_H */
