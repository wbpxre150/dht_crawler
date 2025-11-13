#ifndef DATABASE_H
#define DATABASE_H

#include "dht_crawler.h"
#include <sqlite3.h>
#include <stdint.h>

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
    struct bloom_filter *bloom;  /* Bloom filter for marking successful writes */
} database_t;

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

/* Maintenance functions */
int database_vacuum(database_t *db);
int database_analyze(database_t *db);
int database_optimize(database_t *db);

#endif /* DATABASE_H */
