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
} torrent_metadata_t;

/* Forward declaration */
struct bloom_filter;

/* Database manager */
typedef struct {
    sqlite3 *db;
    sqlite3_stmt *insert_torrent_stmt;
    sqlite3_stmt *insert_file_stmt;
    sqlite3_stmt *insert_prefix_stmt;
    sqlite3_stmt *lookup_prefix_stmt;
    sqlite3_stmt *check_exists_stmt;
    app_context_t *app_ctx;
    int batch_count;
    uv_mutex_t mutex;
    pthread_rwlock_t checkpoint_rwlock;  /* Coordinates readers vs WAL checkpoint */
    struct bloom_filter *bloom;  /* Bloom filter for marking successful writes */
} database_t;

/* Acquire/release read lock for database queries (used by HTTP API etc.)
 * Prevents WAL checkpoint from running while readers are active. */
void database_read_lock(database_t *db);
void database_read_unlock(database_t *db);

/* Function declarations */
int database_init(database_t *db, const char *db_path, app_context_t *app_ctx);
int database_create_schema(database_t *db);
void database_set_bloom(database_t *db, struct bloom_filter *bloom);
int database_insert_torrent(database_t *db, const torrent_metadata_t *torrent,
                           const file_info_t *files, int num_files);
int database_insert_batch(database_t *db, torrent_metadata_t **batch, size_t count);
int database_check_exists(database_t *db, const uint8_t *info_hash);
int database_has_infohash(database_t *db, const unsigned char *hash);
int database_begin_transaction(database_t *db);
int database_commit_transaction(database_t *db);
int database_rollback_transaction(database_t *db);
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
 * `filter` may be NULL or empty for no filter; otherwise files whose filename
 * matches the trigram FTS query are returned.
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

/* Maintenance functions */
int database_wal_checkpoint(database_t *db);
int database_vacuum(database_t *db);
int database_analyze(database_t *db);
int database_optimize(database_t *db);

#endif /* DATABASE_H */
