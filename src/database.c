#include "database.h"
#include "dht_crawler.h"
#include "bloom_filter.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

/* SQL schema creation statements */
static const char *CREATE_TABLES_SQL =
    "CREATE TABLE IF NOT EXISTS torrents ("
    "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "    info_hash BLOB(20) NOT NULL UNIQUE,"
    "    name TEXT NOT NULL,"
    "    size_bytes INTEGER NOT NULL,"
    "    piece_length INTEGER,"
    "    num_pieces INTEGER,"
    "    total_peers INTEGER DEFAULT 0,"
    "    added_timestamp INTEGER NOT NULL,"
    "    last_seen INTEGER NOT NULL"
    ");"
    ""
    "CREATE INDEX IF NOT EXISTS idx_torrents_added ON torrents(added_timestamp DESC);"
    "CREATE INDEX IF NOT EXISTS idx_torrents_total_peers ON torrents(total_peers DESC);"
    ""
    "CREATE TABLE IF NOT EXISTS torrent_files ("
    "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "    torrent_id INTEGER NOT NULL,"
    "    path TEXT NOT NULL,"
    "    size_bytes INTEGER NOT NULL,"
    "    file_index INTEGER NOT NULL,"
    "    FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE"
    ");"
    ""
    "CREATE INDEX IF NOT EXISTS idx_files_torrent ON torrent_files(torrent_id);";

static const char *CREATE_FTS_SQL =
    "CREATE VIRTUAL TABLE IF NOT EXISTS torrent_search USING fts5("
    "    name,"
    "    tokenize='porter unicode61',"
    "    content='torrents',"
    "    content_rowid='id'"
    ");"
    ""
    "CREATE VIRTUAL TABLE IF NOT EXISTS file_search USING fts5("
    "    path,"
    "    tokenize='trigram',"
    "    content='torrent_files',"
    "    content_rowid='id'"
    ");";

static const char *CREATE_TRIGGERS_SQL =
    "CREATE TRIGGER IF NOT EXISTS torrents_ai AFTER INSERT ON torrents BEGIN"
    "    INSERT INTO torrent_search(rowid, name) VALUES (new.id, new.name);"
    "END;"
    ""
    "CREATE TRIGGER IF NOT EXISTS torrents_ad AFTER DELETE ON torrents BEGIN"
    "    INSERT INTO torrent_search(torrent_search, rowid, name) VALUES('delete', old.id, old.name);"
    "END;"
    ""
    "CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON torrent_files BEGIN"
    "    INSERT INTO file_search(rowid, path) VALUES (new.id, new.path);"
    "END;"
    ""
    "CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON torrent_files BEGIN"
    "    INSERT INTO file_search(file_search, rowid, path) VALUES('delete', old.id, old.path);"
    "END;";

/* Initialize database */
int database_init(database_t *db, const char *db_path, app_context_t *app_ctx) {
    if (!db || !db_path || !app_ctx) {
        return -1;
    }

    memset(db, 0, sizeof(database_t));
    db->app_ctx = app_ctx;
    db->bloom = NULL;  /* Will be set via database_set_bloom() */

    /* Open database */
    int rc = sqlite3_open(db_path, &db->db);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to open database: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    /* Enable WAL mode for better concurrency */
    sqlite3_exec(db->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA cache_size=-64000;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA page_size=4096;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA mmap_size=268435456;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

    /* Initialize mutex */
    if (uv_mutex_init(&db->mutex) != 0) {
        sqlite3_close(db->db);
        return -1;
    }

    return 0;
}

/* Set bloom filter for tracking successful writes */
void database_set_bloom(database_t *db, bloom_filter_t *bloom) {
    if (!db) {
        return;
    }
    db->bloom = bloom;
}

/* Create database schema */
int database_create_schema(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc;

    /* Create tables */
    rc = sqlite3_exec(db->db, CREATE_TABLES_SQL, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to create tables: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    /* Create FTS tables */
    rc = sqlite3_exec(db->db, CREATE_FTS_SQL, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to create FTS tables: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    /* Create triggers */
    rc = sqlite3_exec(db->db, CREATE_TRIGGERS_SQL, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to create triggers: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    /* Prepare statements */
    const char *insert_torrent_sql =
        "INSERT OR REPLACE INTO torrents "
        "(info_hash, name, size_bytes, piece_length, num_pieces, total_peers, added_timestamp, last_seen) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    rc = sqlite3_prepare_v2(db->db, insert_torrent_sql, -1, &db->insert_torrent_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert torrent statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    const char *insert_file_sql =
        "INSERT INTO torrent_files (torrent_id, path, size_bytes, file_index) VALUES (?, ?, ?, ?)";

    rc = sqlite3_prepare_v2(db->db, insert_file_sql, -1, &db->insert_file_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert file statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    const char *check_exists_sql = "SELECT 1 FROM torrents WHERE info_hash = ? LIMIT 1";

    rc = sqlite3_prepare_v2(db->db, check_exists_sql, -1, &db->check_exists_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare check exists statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    return 0;
}

/* Check if torrent exists */
int database_check_exists(database_t *db, const uint8_t *info_hash) {
    if (!db || !info_hash) {
        return 0;
    }

    uv_mutex_lock(&db->mutex);

    sqlite3_reset(db->check_exists_stmt);
    sqlite3_bind_blob(db->check_exists_stmt, 1, info_hash, SHA1_DIGEST_LENGTH, SQLITE_STATIC);

    int exists = (sqlite3_step(db->check_exists_stmt) == SQLITE_ROW);

    uv_mutex_unlock(&db->mutex);
    return exists;
}

/* Begin transaction */
int database_begin_transaction(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to begin transaction: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* Commit transaction */
int database_commit_transaction(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "COMMIT;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to commit transaction: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* Rollback transaction */
int database_rollback_transaction(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "ROLLBACK;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to rollback transaction: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* Check if info_hash exists (for bloom filter integration) */
int database_has_infohash(database_t *db, const unsigned char *hash) {
    if (!db || !hash) {
        return 0;
    }

    return database_check_exists(db, hash);
}

/* Internal function: Insert torrent without mutex (transaction-safe)
 * MUST be called within a transaction or with external mutex held */
static int database_insert_torrent_unsafe(database_t *db, const torrent_metadata_t *torrent,
                                         const file_info_t *files, int num_files) {
    if (!db || !torrent) {
        return -1;
    }

    /* Bind torrent data - use SQLITE_TRANSIENT to force SQLite to make copies */
    sqlite3_reset(db->insert_torrent_stmt);
    sqlite3_bind_blob(db->insert_torrent_stmt, 1, torrent->info_hash, SHA1_DIGEST_LENGTH, SQLITE_TRANSIENT);
    sqlite3_bind_text(db->insert_torrent_stmt, 2, torrent->name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(db->insert_torrent_stmt, 3, torrent->size_bytes);
    sqlite3_bind_int(db->insert_torrent_stmt, 4, torrent->piece_length);
    sqlite3_bind_int(db->insert_torrent_stmt, 5, torrent->num_pieces);
    sqlite3_bind_int(db->insert_torrent_stmt, 6, torrent->total_peers);
    sqlite3_bind_int64(db->insert_torrent_stmt, 7, torrent->added_timestamp);
    sqlite3_bind_int64(db->insert_torrent_stmt, 8, torrent->last_seen);

    int rc = sqlite3_step(db->insert_torrent_stmt);
    if (rc != SQLITE_DONE) {
        /* Enhanced error logging with info_hash */
        char hash_hex[41];
        for (int i = 0; i < 20; i++) {
            sprintf(hash_hex + i * 2, "%02x", torrent->info_hash[i]);
        }
        log_msg(LOG_ERROR, "Failed to insert torrent %s (name: %s): %s",
               hash_hex, torrent->name ? torrent->name : "(null)",
               sqlite3_errmsg(db->db));
        return -1;
    }

    int64_t torrent_id = sqlite3_last_insert_rowid(db->db);

    /* Insert files - use SQLITE_TRANSIENT for paths */
    for (int i = 0; i < num_files; i++) {
        sqlite3_reset(db->insert_file_stmt);
        sqlite3_bind_int64(db->insert_file_stmt, 1, torrent_id);
        sqlite3_bind_text(db->insert_file_stmt, 2, files[i].path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(db->insert_file_stmt, 3, files[i].size_bytes);
        sqlite3_bind_int(db->insert_file_stmt, 4, files[i].file_index);

        rc = sqlite3_step(db->insert_file_stmt);
        if (rc != SQLITE_DONE) {
            log_msg(LOG_ERROR, "Failed to insert file '%s' for torrent_id %lld: %s",
                   files[i].path ? files[i].path : "(null)",
                   (long long)torrent_id, sqlite3_errmsg(db->db));
            /* Continue with other files rather than failing completely */
        }
    }

    return 0;
}

/* Insert torrent with files (thread-safe, with mutex) */
int database_insert_torrent(database_t *db, const torrent_metadata_t *torrent,
                           const file_info_t *files, int num_files) {
    if (!db || !torrent) {
        return -1;
    }

    uv_mutex_lock(&db->mutex);
    int result = database_insert_torrent_unsafe(db, torrent, files, num_files);
    uv_mutex_unlock(&db->mutex);

    return result;
}

/* Batch insert multiple torrents in a single transaction (optimized for batch_writer) */
int database_insert_batch(database_t *db, torrent_metadata_t **batch, size_t count) {
    if (!db || !batch || count == 0) {
        return -1;
    }

    /* Allocate array to track which inserts succeeded (for bloom filter updates) */
    bool *success_flags = (bool *)calloc(count, sizeof(bool));
    if (!success_flags) {
        log_msg(LOG_ERROR, "Failed to allocate success tracking array");
        return -1;
    }

    /* Lock mutex once for entire batch */
    uv_mutex_lock(&db->mutex);

    /* Begin transaction */
    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to begin transaction: %s", err_msg);
        sqlite3_free(err_msg);
        uv_mutex_unlock(&db->mutex);
        free(success_flags);
        return -1;
    }

    /* Insert all torrents and track successes */
    size_t written = 0;
    size_t failed = 0;
    for (size_t i = 0; i < count; i++) {
        if (database_insert_torrent_unsafe(db, batch[i], batch[i]->files, batch[i]->num_files) == 0) {
            success_flags[i] = true;
            written++;
        } else {
            success_flags[i] = false;
            failed++;
            /* Log first few failures with details */
            if (failed <= 3) {
                char hash_hex[41];
                for (int j = 0; j < 20; j++) {
                    sprintf(hash_hex + j * 2, "%02x", batch[i]->info_hash[j]);
                }
                log_msg(LOG_ERROR, "Failed to insert torrent in batch: %s (name: %s)",
                       hash_hex, batch[i]->name ? batch[i]->name : "(null)");
            }
        }
    }

    /* Commit transaction */
    rc = sqlite3_exec(db->db, "COMMIT;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to commit batch transaction: %s", err_msg);
        sqlite3_free(err_msg);
        /* Rollback on commit failure */
        sqlite3_exec(db->db, "ROLLBACK;", NULL, NULL, NULL);
        uv_mutex_unlock(&db->mutex);
        free(success_flags);
        return -1;
    }

    /* CRITICAL: After successful commit, add all successful inserts to bloom filter
     * This ensures bloom filter only contains infohashes that are actually in the database,
     * preventing data loss from failed metadata fetches that can now be retried. */
    if (db->bloom && written > 0) {
        size_t bloom_added = 0;
        for (size_t i = 0; i < count; i++) {
            if (success_flags[i]) {
                bloom_filter_add(db->bloom, batch[i]->info_hash);
                bloom_added++;
            }
        }
        log_msg(LOG_DEBUG, "Added %zu infohashes to bloom filter after successful batch commit", bloom_added);
    }

    uv_mutex_unlock(&db->mutex);
    free(success_flags);

    if (failed > 0) {
        log_msg(LOG_WARN, "Batch insert: %zu/%zu items written, %zu failed",
                written, count, failed);
    } else {
        log_msg(LOG_DEBUG, "Batch insert: %zu/%zu items written", written, count);
    }

    return (written > 0) ? 0 : -1;
}

/* Vacuum database */
int database_vacuum(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "VACUUM;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "VACUUM failed: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* Analyze database */
int database_analyze(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "ANALYZE;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "ANALYZE failed: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* Optimize database */
int database_optimize(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, "PRAGMA optimize;", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "OPTIMIZE failed: %s", err_msg);
        sqlite3_free(err_msg);
        return -1;
    }

    return 0;
}

/* Cleanup database */
void database_cleanup(database_t *db) {
    if (!db) {
        return;
    }

    if (db->insert_torrent_stmt) {
        sqlite3_finalize(db->insert_torrent_stmt);
        db->insert_torrent_stmt = NULL;
    }

    if (db->insert_file_stmt) {
        sqlite3_finalize(db->insert_file_stmt);
        db->insert_file_stmt = NULL;
    }

    if (db->check_exists_stmt) {
        sqlite3_finalize(db->check_exists_stmt);
        db->check_exists_stmt = NULL;
    }

    if (db->db) {
        sqlite3_close(db->db);
        db->db = NULL;
    }

    uv_mutex_destroy(&db->mutex);
}
