#ifndef DATABASE_H
#define DATABASE_H

#include "dht_crawler.h"
#include <sqlite3.h>
#include <stdint.h>
#include <pthread.h>

/* File info structure */
typedef struct {
    char *path;
    int64_t size_bytes;
    int16_t file_index;  /* Changed to int16_t (SMALLINT) for space savings */
} file_info_t;

/*
 * Stage 5 ingest-time classification (tag mode).
 *
 * Plain C values only: this struct is embedded in torrent_metadata_t, which
 * batch_writer_add() deep-copies with a single memcpy (src/batch_writer.c), so
 * extending it needs no copy-code changes.
 *
 * category/tv_kind hold tparse taxonomy codes (tp_category_t / tp_tv_kind_t);
 * tv_kind 0 (TP_TV_NONE) is stored as SQL NULL in torrent_class.
 */
typedef struct {
    int category;       /* tp_category_t */
    int tv_kind;        /* tp_tv_kind_t, 0 = none */
    int adult;          /* 1 when adult_p >= the manifest hide threshold */
    double adult_p;     /* P(adult) from the adult model, 0.0 when unavailable */
    int season_lo, season_hi, episode_lo, episode_hi;
    int resolution;     /* 0, 480, 720, 1080, 2160 */
    double confidence;  /* category model probability, 0.0 for parser-only */
    long long clf_ver;  /* model version from the manifest, 0 when unknown */
} classification_t;

/* Torrent metadata structure */
typedef struct {
    uint8_t info_hash[SHA1_DIGEST_LENGTH];
    char *name;
    int64_t size_bytes;
    int32_t total_peers;
    int64_t added_timestamp;
    /* File information (for multi-file torrents) */
    file_info_t *files;
    int32_t num_files;
    /* Stage 5 ingest-time classification (tag mode): set by tree_metadata.c
     * after the filters pass; has_classification gates the torrent_class row. */
    classification_t classification;
    int has_classification;
} torrent_metadata_t;

/* Forward declaration */
struct bloom_filter;

/* Per-thread read connections registry size. The HTTP API runs two civetweb
 * worker threads plus the refresh thread; 32 slots is far more than needed and
 * still bounds the number of open connections if something leaks a thread. */
#define DATABASE_READ_CONN_MAX 32

/* One per-thread read connection (defined in database.c). */
struct database_read_conn;

/* Database manager */
typedef struct {
    sqlite3 *db;
    sqlite3_stmt *insert_torrent_stmt;
    sqlite3_stmt *insert_class_stmt;  /* Stage 5: torrent_class row */
    sqlite3_stmt *insert_doc_stmt;    /* Schema v2: torrent_docs row (rowid = torrents.id) */
    sqlite3_stmt *insert_file_stmt;
    sqlite3_stmt *insert_prefix_stmt;
    sqlite3_stmt *lookup_prefix_stmt;
    sqlite3_stmt *check_exists_stmt;
    app_context_t *app_ctx;
    int batch_count;
    uv_mutex_t mutex;
    pthread_rwlock_t checkpoint_rwlock;  /* Coordinates readers vs WAL checkpoint */
    struct bloom_filter *bloom;  /* Bloom filter for marking successful writes */
    /* --- Per-thread read connections (database_read_db) -------------------
     * The long read paths (/search, /browse, /random) run their statements on
     * a connection owned by the calling thread, so one multi-second
     * sqlite3_step() cannot block the other DB users behind the process-wide
     * `db`. The registry is only a shutdown safety net: each connection is
     * normally closed by its thread's pthread_key destructor. */
    char *db_path;                              /* strdup'd by database_init() */
    pthread_key_t read_conn_key;                /* TLS slot -> struct database_read_conn */
    int read_conn_key_valid;                    /* 0 after pthread_key_delete() */
    pthread_mutex_t read_conn_mutex;            /* guards read_conns[]/read_conns_count */
    struct database_read_conn *read_conns[DATABASE_READ_CONN_MAX];
    int read_conns_count;
} database_t;

/* Acquire/release read lock for database queries (used by HTTP API etc.)
 * Prevents WAL checkpoint from running while readers are active. */
void database_read_lock(database_t *db);
void database_read_unlock(database_t *db);

/* Return the calling thread's own read connection, opening it on first use.
 *
 * ONLY LITERAL SQL STRINGS MAY EVER BE PREPARED ON THE RETURNED CONNECTION.
 * Every piece of user input must reach it as a bound parameter: a read
 * connection is a private, long-lived handle whose statements are prepared
 * outside the shared `db`, so any interpolated text would bypass the single
 * place where this code base keeps SQL construction reviewable. The reason
 * for the separate connection is isolation, not speed: a multi-second
 * sqlite3_step() on it must never block the millisecond queries that stay on
 * db->db.
 *
 * Falls back to db->db when the per-thread connection cannot be created, so
 * callers degrade to the previous shared-connection behaviour instead of
 * failing. The caller must still hold database_read_lock() and must finalize
 * every statement it prepared here before database_read_unlock(). */
sqlite3 *database_read_db(database_t *db);

/* Function declarations */
int database_init(database_t *db, const char *db_path, app_context_t *app_ctx);
int database_create_schema(database_t *db);
/* Write the schema DDL (the CREATE statements, one per line) to `out`.
 *
 * Backs the --print-schema one-shot so external tooling can take the schema
 * from the crawler itself instead of a hand-copied dump. */
void database_print_schema(FILE *out);
void database_set_bloom(database_t *db, struct bloom_filter *bloom);
int database_insert_torrent(database_t *db, const torrent_metadata_t *torrent,
                           const file_info_t *files, int num_files);
int database_insert_batch(database_t *db, torrent_metadata_t **batch, size_t count, size_t *out_written);
int database_check_exists(database_t *db, const uint8_t *info_hash);
int database_has_infohash(database_t *db, const unsigned char *hash);
void database_cleanup(database_t *db);

/* Torrent summary (for detail page) */
typedef struct {
    char info_hash[41];   /* 40 hex chars + NUL */
    char *name;           /* allocated */
    int64_t size_bytes;
    int total_peers;
    int64_t added_timestamp;
    int file_count;
} torrent_summary_t;

typedef struct {
    char *path;           /* allocated: prefix + '/' + filename, or filename */
    int64_t size_bytes;
} torrent_file_row_t;

/* Look up a torrent summary by hex info_hash.
 * Returns 0 on success (caller frees out->name), -1 if not found / error. */
int db_get_torrent_by_hash(database_t *db, const char *hex_hash, torrent_summary_t *out);

/* Get a paginated, optionally substring-filtered, list of files for a torrent.
 * `filter` may be NULL or empty for no filter; otherwise files whose full
 * displayed path (prefix/filename) matches the escaped %term% LIKE pattern
 * are returned.
 * On success returns 0; caller must free each row's `path` and the array via
 * db_free_torrent_file_rows(). */
int db_get_torrent_files_paginated(database_t *db, const char *hex_hash,
                                   int offset, int limit, const char *filter,
                                   torrent_file_row_t **out_files, int *out_count,
                                   int *out_total);

void db_free_torrent_file_rows(torrent_file_row_t *rows, int count);

/* Rebuild bloom filter by scanning all infohashes in the database.
 * Returns the number of hashes added, or -1 on error. */
int database_rebuild_bloom(const char *db_path, struct bloom_filter *bloom);

/* Backfill the schema-v2 torrent-level word index (torrent_docs) of an existing
 * db_path database from its base tables, in bounded id chunks. Idempotent (a
 * re-run replaces each chunk's rowid range); the WAL is checkpointed and
 * truncated so the file is self-contained. Returns 0 on success, -1 on
 * fatal error. */
int database_rebuild_docs(const char *db_path);

/* Maintenance functions */
int database_wal_checkpoint(database_t *db);
int database_vacuum(database_t *db);
int database_vacuum_into(database_t *db, const char *dest_path);
int database_analyze(database_t *db);
int database_optimize(database_t *db);

/* Run PRAGMA integrity_check. Returns 0 if clean, >0 error count, -1 on failure. */
int database_integrity_check(const char *db_path);

/* Copy all readable rows from src_path into a fresh dst_path database.
 * Returns number of torrent rows recovered, or -1 on fatal error. */
int database_recover(const char *src_path, const char *dst_path);

/* Replay a plain (uncompressed) SQL dump into a fresh dst_path database,
 * creating the full schema first. Returns the number of statements
 * replayed, or -1 on fatal error (dst_path is unlinked on failure). */
int database_restore(const char *sql_path, const char *dst_path);

/* Create the FTS5 tables + triggers in an existing db_path database,
 * rebuild the external-content index (torrent_search) and backfill the
 * contentless torrent_docs index with the bundled SQLite. The WAL is
 * checkpointed and truncated so the file is self-contained. Returns 0 on
 * success, -1 on fatal error. */
int database_rebuild_fts(const char *db_path);

#endif /* DATABASE_H */
