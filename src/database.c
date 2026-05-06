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
    "    total_peers INTEGER DEFAULT 0,"
    "    added_timestamp INTEGER NOT NULL"
    ");"
    ""
    "CREATE INDEX IF NOT EXISTS idx_torrents_added ON torrents(added_timestamp DESC);"
    "CREATE INDEX IF NOT EXISTS idx_torrents_total_peers ON torrents(total_peers DESC);"
    ""
    "CREATE TABLE IF NOT EXISTS path_prefixes ("
    "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "    prefix TEXT NOT NULL UNIQUE"
    ");"
    ""
    "CREATE TABLE IF NOT EXISTS torrent_files ("
    "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "    torrent_id INTEGER NOT NULL,"
    "    prefix_id INTEGER,"
    "    filename TEXT NOT NULL,"
    "    size_bytes INTEGER NOT NULL,"
    "    file_index SMALLINT NOT NULL,"
    "    FOREIGN KEY (torrent_id) REFERENCES torrents(id) ON DELETE CASCADE,"
    "    FOREIGN KEY (prefix_id) REFERENCES path_prefixes(id)"
    ");"
    ""
    "CREATE INDEX IF NOT EXISTS idx_files_torrent ON torrent_files(torrent_id);"
    "CREATE INDEX IF NOT EXISTS idx_files_prefix ON torrent_files(prefix_id);";

static const char *CREATE_FTS_SQL =
    "CREATE VIRTUAL TABLE IF NOT EXISTS torrent_search USING fts5("
    "    name,"
    "    tokenize='porter unicode61',"
    "    content='torrents',"
    "    content_rowid='id'"
    ");"
    ""
    "CREATE VIRTUAL TABLE IF NOT EXISTS file_search USING fts5("
    "    filename,"
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
    "    INSERT INTO file_search(rowid, filename) VALUES (new.id, new.filename);"
    "END;"
    ""
    "CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON torrent_files BEGIN"
    "    INSERT INTO file_search(file_search, rowid, filename) VALUES('delete', old.id, old.filename);"
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
    /* NORMAL sync: WAL survives process crashes intact (no fsync on commit).
     * Durability is provided by wal_checkpoint(RESTART) called after every
     * batch flush, which checkpoints all frames and resets the WAL write
     * position so the file does not grow unboundedly. */
    sqlite3_exec(db->db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    /* Disable automatic PASSIVE checkpoints - they never complete when there
     * are concurrent readers (which is always the case here with many threads).
     * Our manual RESTART checkpoint after each batch flush handles this. */
    sqlite3_exec(db->db, "PRAGMA wal_autocheckpoint=0;", NULL, NULL, NULL);
    /* Cap the WAL file at 500 MB. After each RESTART checkpoint, SQLite will
     * truncate the WAL back down to this limit if it has grown larger. Only
     * the overflow above 500 MB is truncated (not the whole file), so the
     * I/O burst is far smaller than CHECKPOINT_TRUNCATE which always shrinks
     * to zero. */
    sqlite3_exec(db->db, "PRAGMA journal_size_limit=524288000;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA cache_size=-64000;", NULL, NULL, NULL);
    /* Larger page size for better compression (32KB) */
    sqlite3_exec(db->db, "PRAGMA page_size=32768;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA mmap_size=268435456;", NULL, NULL, NULL);
    sqlite3_exec(db->db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    /* Enable auto_vacuum for better space management */
    sqlite3_exec(db->db, "PRAGMA auto_vacuum=INCREMENTAL;", NULL, NULL, NULL);

    /* Initialize mutex */
    if (uv_mutex_init(&db->mutex) != 0) {
        sqlite3_close(db->db);
        return -1;
    }

    /* Initialize checkpoint rwlock */
    if (pthread_rwlock_init(&db->checkpoint_rwlock, NULL) != 0) {
        uv_mutex_destroy(&db->mutex);
        sqlite3_close(db->db);
        return -1;
    }

    return 0;
}

/* Acquire read lock - prevents WAL checkpoint while held */
void database_read_lock(database_t *db) {
    if (db) pthread_rwlock_rdlock(&db->checkpoint_rwlock);
}

/* Release read lock */
void database_read_unlock(database_t *db) {
    if (db) pthread_rwlock_unlock(&db->checkpoint_rwlock);
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
        "(info_hash, name, size_bytes, total_peers, added_timestamp) "
        "VALUES (?, ?, ?, ?, ?)";

    rc = sqlite3_prepare_v2(db->db, insert_torrent_sql, -1, &db->insert_torrent_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert torrent statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    const char *insert_file_sql =
        "INSERT INTO torrent_files (torrent_id, prefix_id, filename, size_bytes, file_index) "
        "VALUES (?, ?, ?, ?, ?)";

    rc = sqlite3_prepare_v2(db->db, insert_file_sql, -1, &db->insert_file_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert file statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    const char *insert_prefix_sql =
        "INSERT OR IGNORE INTO path_prefixes (prefix) VALUES (?)";

    rc = sqlite3_prepare_v2(db->db, insert_prefix_sql, -1, &db->insert_prefix_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert prefix statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    const char *lookup_prefix_sql =
        "SELECT id FROM path_prefixes WHERE prefix = ? LIMIT 1";

    rc = sqlite3_prepare_v2(db->db, lookup_prefix_sql, -1, &db->lookup_prefix_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare lookup prefix statement: %s", sqlite3_errmsg(db->db));
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

    pthread_rwlock_rdlock(&db->checkpoint_rwlock);
    uv_mutex_lock(&db->mutex);

    sqlite3_reset(db->check_exists_stmt);
    sqlite3_bind_blob(db->check_exists_stmt, 1, info_hash, SHA1_DIGEST_LENGTH, SQLITE_STATIC);

    int exists = (sqlite3_step(db->check_exists_stmt) == SQLITE_ROW);

    /* CRITICAL: Reset after stepping to release the implicit read transaction.
     * An unreset statement holds a read lock on the WAL, blocking checkpoints. */
    sqlite3_reset(db->check_exists_stmt);

    uv_mutex_unlock(&db->mutex);
    pthread_rwlock_unlock(&db->checkpoint_rwlock);
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

/* Helper: Split path into prefix and filename
 * Returns: 0 on success, -1 on error
 * Note: Caller must free *prefix and *filename if non-NULL */
static int split_path(const char *path, char **prefix, char **filename) {
    if (!path || !prefix || !filename) {
        return -1;
    }

    *prefix = NULL;
    *filename = NULL;

    /* Find last slash */
    const char *last_slash = strrchr(path, '/');

    if (!last_slash) {
        /* No directory component - file is at root */
        *filename = strdup(path);
        return 0;
    }

    /* Split into prefix and filename */
    size_t prefix_len = last_slash - path;
    if (prefix_len > 0) {
        *prefix = strndup(path, prefix_len);
        if (!*prefix) {
            return -1;
        }
    }

    *filename = strdup(last_slash + 1);
    if (!*filename) {
        free(*prefix);
        *prefix = NULL;
        return -1;
    }

    return 0;
}

/* Helper: Get or create path prefix ID
 * Returns: prefix_id on success, -1 on error, 0 if prefix is NULL (root) */
static int64_t get_or_create_prefix(database_t *db, const char *prefix) {
    if (!db) {
        return -1;
    }

    /* NULL prefix means root level file */
    if (!prefix || strlen(prefix) == 0) {
        return 0;
    }

    /* Try to lookup existing prefix */
    sqlite3_reset(db->lookup_prefix_stmt);
    sqlite3_bind_text(db->lookup_prefix_stmt, 1, prefix, -1, SQLITE_STATIC);

    int rc = sqlite3_step(db->lookup_prefix_stmt);
    if (rc == SQLITE_ROW) {
        /* Found existing prefix */
        int64_t result = sqlite3_column_int64(db->lookup_prefix_stmt, 0);
        sqlite3_reset(db->lookup_prefix_stmt);
        return result;
    }
    sqlite3_reset(db->lookup_prefix_stmt);

    /* Insert new prefix */
    sqlite3_reset(db->insert_prefix_stmt);
    sqlite3_bind_text(db->insert_prefix_stmt, 1, prefix, -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(db->insert_prefix_stmt);
    sqlite3_reset(db->insert_prefix_stmt);
    if (rc != SQLITE_DONE) {
        log_msg(LOG_ERROR, "Failed to insert path prefix '%s': %s",
                prefix, sqlite3_errmsg(db->db));
        return -1;
    }

    /* Get the ID we just inserted */
    int64_t prefix_id = sqlite3_last_insert_rowid(db->db);

    /* If prefix_id is 0, it means INSERT OR IGNORE didn't insert (race condition)
     * Try lookup again */
    if (prefix_id == 0) {
        sqlite3_reset(db->lookup_prefix_stmt);
        sqlite3_bind_text(db->lookup_prefix_stmt, 1, prefix, -1, SQLITE_STATIC);

        if (sqlite3_step(db->lookup_prefix_stmt) == SQLITE_ROW) {
            prefix_id = sqlite3_column_int64(db->lookup_prefix_stmt, 0);
        }
        sqlite3_reset(db->lookup_prefix_stmt);
    }

    return prefix_id;
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
    sqlite3_bind_int(db->insert_torrent_stmt, 4, torrent->total_peers);
    sqlite3_bind_int64(db->insert_torrent_stmt, 5, torrent->added_timestamp);

    int rc = sqlite3_step(db->insert_torrent_stmt);
    sqlite3_reset(db->insert_torrent_stmt);
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

    /* Insert files with path normalization */
    for (int i = 0; i < num_files; i++) {
        char *prefix = NULL;
        char *filename = NULL;

        /* Split path into prefix and filename */
        if (split_path(files[i].path, &prefix, &filename) != 0) {
            log_msg(LOG_ERROR, "Failed to split path '%s' for torrent_id %lld",
                   files[i].path ? files[i].path : "(null)", (long long)torrent_id);
            continue;
        }

        /* Get or create prefix_id */
        int64_t prefix_id = 0;
        if (prefix) {
            prefix_id = get_or_create_prefix(db, prefix);
            if (prefix_id < 0) {
                log_msg(LOG_ERROR, "Failed to get/create prefix '%s' for torrent_id %lld",
                       prefix, (long long)torrent_id);
                free(prefix);
                free(filename);
                continue;
            }
        }

        /* Insert file record */
        sqlite3_reset(db->insert_file_stmt);
        sqlite3_bind_int64(db->insert_file_stmt, 1, torrent_id);
        if (prefix_id > 0) {
            sqlite3_bind_int64(db->insert_file_stmt, 2, prefix_id);
        } else {
            sqlite3_bind_null(db->insert_file_stmt, 2);  /* NULL for root files */
        }
        sqlite3_bind_text(db->insert_file_stmt, 3, filename, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(db->insert_file_stmt, 4, files[i].size_bytes);
        sqlite3_bind_int(db->insert_file_stmt, 5, files[i].file_index);

        rc = sqlite3_step(db->insert_file_stmt);
        sqlite3_reset(db->insert_file_stmt);
        if (rc != SQLITE_DONE) {
            log_msg(LOG_ERROR, "Failed to insert file '%s' (prefix: '%s', filename: '%s') for torrent_id %lld: %s",
                   files[i].path ? files[i].path : "(null)",
                   prefix ? prefix : "(null)",
                   filename ? filename : "(null)",
                   (long long)torrent_id, sqlite3_errmsg(db->db));
        }

        free(prefix);
        free(filename);
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
int database_insert_batch(database_t *db, torrent_metadata_t **batch, size_t count, size_t *out_written) {
    if (!db || !batch || count == 0) {
        return -1;
    }
    if (out_written) *out_written = 0;

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

    if (out_written) *out_written = written;

    if (failed > 0) {
        log_msg(LOG_WARN, "Batch insert: %zu/%zu items written, %zu failed",
                written, count, failed);
    } else {
        log_msg(LOG_DEBUG, "Batch insert: %zu/%zu items written", written, count);
    }

    return (written > 0) ? 0 : -1;
}

/* Checkpoint WAL into the main database file (TRUNCATE mode).
 *
 * Acquires the checkpoint_rwlock write lock to block ALL readers (HTTP API,
 * check_exists) until the checkpoint completes.  This guarantees the WAL is
 * fully transferred to the main DB and the WAL file is truncated to zero.
 *
 * Returns 0 on success, -1 on error. */
int database_wal_checkpoint(database_t *db) {
    if (!db || !db->db) {
        return -1;
    }

    /* Write lock: blocks until all readers release their read locks,
     * and prevents new readers from starting. */
    pthread_rwlock_wrlock(&db->checkpoint_rwlock);
    uv_mutex_lock(&db->mutex);

    /* Reset ALL prepared statements to release any implicit read transactions
     * held by this connection.  An unreset statement prevents the checkpoint
     * from completing even on the same connection. */
    if (db->check_exists_stmt)   sqlite3_reset(db->check_exists_stmt);
    if (db->insert_torrent_stmt) sqlite3_reset(db->insert_torrent_stmt);
    if (db->insert_file_stmt)    sqlite3_reset(db->insert_file_stmt);
    if (db->insert_prefix_stmt)  sqlite3_reset(db->insert_prefix_stmt);
    if (db->lookup_prefix_stmt)  sqlite3_reset(db->lookup_prefix_stmt);

    /* Set busy timeout so TRUNCATE waits for any stragglers the rwlock
     * didn't cover (e.g. database_rebuild_bloom's separate connection). */
    sqlite3_busy_timeout(db->db, 30000);

    int nLog = 0, nCkpt = 0;
    int rc = sqlite3_wal_checkpoint_v2(db->db, NULL,
                SQLITE_CHECKPOINT_RESTART, &nLog, &nCkpt);

    /* Restore no-timeout for normal operations */
    sqlite3_busy_timeout(db->db, 0);

    if (rc == SQLITE_BUSY) {
        log_msg(LOG_WARN, "WAL checkpoint BUSY: %d/%d frames checkpointed (readers still active)",
                nCkpt, nLog);
    } else if (rc == SQLITE_OK) {
        if (nLog > 0) {
            log_msg(LOG_DEBUG, "WAL checkpoint OK: %d/%d frames checkpointed, WAL write pointer reset",
                    nCkpt, nLog);
        }
    } else {
        log_msg(LOG_ERROR, "WAL checkpoint failed (rc=%d): %s", rc, sqlite3_errstr(rc));
    }

    uv_mutex_unlock(&db->mutex);
    pthread_rwlock_unlock(&db->checkpoint_rwlock);

    return (rc == SQLITE_OK) ? 0 : -1;
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

/* Rebuild bloom filter from all infohashes in the database.
 * Opens the database read-only, iterates every info_hash in torrents,
 * and adds each to the provided bloom filter.
 * Returns the number of hashes added, or -1 on error. */
int database_rebuild_bloom(const char *db_path, bloom_filter_t *bloom) {
    if (!db_path || !bloom) {
        return -1;
    }

    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild_bloom: failed to open database %s: %s",
                db_path, sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, "SELECT info_hash FROM torrents", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild_bloom: failed to prepare query: %s",
                sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    int count = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const void *blob = sqlite3_column_blob(stmt, 0);
        int blob_len = sqlite3_column_bytes(stmt, 0);
        if (blob && blob_len == SHA1_DIGEST_LENGTH) {
            bloom_filter_add(bloom, (const unsigned char *)blob);
            count++;
            if (count % 100000 == 0) {
                log_msg(LOG_INFO, "rebuild_bloom: %d infohashes processed...", count);
            }
        }
    }

    if (rc != SQLITE_DONE) {
        log_msg(LOG_ERROR, "rebuild_bloom: query error: %s", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return count;
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

    if (db->insert_prefix_stmt) {
        sqlite3_finalize(db->insert_prefix_stmt);
        db->insert_prefix_stmt = NULL;
    }

    if (db->lookup_prefix_stmt) {
        sqlite3_finalize(db->lookup_prefix_stmt);
        db->lookup_prefix_stmt = NULL;
    }

    if (db->check_exists_stmt) {
        sqlite3_finalize(db->check_exists_stmt);
        db->check_exists_stmt = NULL;
    }

    if (db->db) {
        /* Final checkpoint: reset all statements, then truncate WAL.
         * No need for rwlock here since we're shutting down. */
        if (db->check_exists_stmt)   sqlite3_reset(db->check_exists_stmt);
        if (db->insert_torrent_stmt) sqlite3_reset(db->insert_torrent_stmt);
        if (db->insert_file_stmt)    sqlite3_reset(db->insert_file_stmt);
        if (db->insert_prefix_stmt)  sqlite3_reset(db->insert_prefix_stmt);
        if (db->lookup_prefix_stmt)  sqlite3_reset(db->lookup_prefix_stmt);

        sqlite3_busy_timeout(db->db, 30000);
        int nLog = 0, nCkpt = 0;
        int rc = sqlite3_wal_checkpoint_v2(db->db, NULL,
                    SQLITE_CHECKPOINT_TRUNCATE, &nLog, &nCkpt);
        if (rc != SQLITE_OK) {
            log_msg(LOG_WARN, "Final WAL checkpoint returned %d: %s (%d/%d frames)",
                    rc, sqlite3_errstr(rc), nCkpt, nLog);
        }

        sqlite3_close(db->db);
        db->db = NULL;
    }

    pthread_rwlock_destroy(&db->checkpoint_rwlock);
    uv_mutex_destroy(&db->mutex);
}

static int hex_to_bin20(const char *hex, uint8_t *out) {
    if (!hex || strlen(hex) != 40) return -1;
    for (int i = 0; i < 20; i++) {
        unsigned int v;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

int db_get_torrent_by_hash(database_t *db, const char *hex_hash, torrent_summary_t *out) {
    if (!db || !hex_hash || !out) return -1;
    uint8_t bin[20];
    if (hex_to_bin20(hex_hash, bin) != 0) return -1;

    memset(out, 0, sizeof(*out));
    memcpy(out->info_hash, hex_hash, 40);
    out->info_hash[40] = '\0';

    const char *sql =
        "SELECT t.name, t.size_bytes, t.total_peers, t.added_timestamp, "
        "  (SELECT COUNT(*) FROM torrent_files tf WHERE tf.torrent_id = t.id) "
        "FROM torrents t WHERE t.info_hash = ? LIMIT 1";

    sqlite3_stmt *stmt = NULL;
    int rc = -1;
    database_read_lock(db);
    if (sqlite3_prepare_v2(db->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_blob(stmt, 1, bin, 20, SQLITE_STATIC);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char *name = sqlite3_column_text(stmt, 0);
            out->name = name ? strdup((const char *)name) : strdup("");
            out->size_bytes = sqlite3_column_int64(stmt, 1);
            out->total_peers = sqlite3_column_int(stmt, 2);
            out->added_timestamp = sqlite3_column_int64(stmt, 3);
            out->file_count = sqlite3_column_int(stmt, 4);
            rc = 0;
        }
        sqlite3_finalize(stmt);
    }
    database_read_unlock(db);
    return rc;
}

int db_get_torrent_files_paginated(database_t *db, const char *hex_hash,
                                   int offset, int limit, const char *filter,
                                   torrent_file_row_t **out_files, int *out_count,
                                   int *out_total) {
    if (!db || !hex_hash || !out_files || !out_count) return -1;
    uint8_t bin[20];
    if (hex_to_bin20(hex_hash, bin) != 0) return -1;
    if (limit <= 0 || limit > 500) limit = 50;
    if (offset < 0) offset = 0;

    int has_filter = (filter && filter[0] != '\0');

    *out_files = NULL;
    *out_count = 0;
    if (out_total) *out_total = 0;

    /* Resolve torrent_id once. */
    int64_t torrent_id = -1;
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT id FROM torrents WHERE info_hash = ? LIMIT 1";
        database_read_lock(db);
        if (sqlite3_prepare_v2(db->db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_blob(st, 1, bin, 20, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                torrent_id = sqlite3_column_int64(st, 0);
            }
            sqlite3_finalize(st);
        }
        database_read_unlock(db);
    }
    if (torrent_id < 0) return -1;

    /* Total count */
    {
        sqlite3_stmt *st = NULL;
        const char *count_sql = has_filter
            ? "SELECT COUNT(*) FROM torrent_files tf "
              "JOIN file_search fs ON fs.rowid = tf.id "
              "WHERE tf.torrent_id = ? AND file_search MATCH ?"
            : "SELECT COUNT(*) FROM torrent_files WHERE torrent_id = ?";
        database_read_lock(db);
        if (sqlite3_prepare_v2(db->db, count_sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, torrent_id);
            if (has_filter) sqlite3_bind_text(st, 2, filter, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW && out_total) {
                *out_total = sqlite3_column_int(st, 0);
            }
            sqlite3_finalize(st);
        }
        database_read_unlock(db);
    }

    /* Page query */
    const char *page_sql = has_filter
        ? "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename) AS path, "
          "       tf.size_bytes "
          "FROM torrent_files tf "
          "JOIN file_search fs ON fs.rowid = tf.id "
          "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
          "WHERE tf.torrent_id = ? AND file_search MATCH ? "
          "ORDER BY tf.file_index LIMIT ? OFFSET ?"
        : "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename) AS path, "
          "       tf.size_bytes "
          "FROM torrent_files tf "
          "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
          "WHERE tf.torrent_id = ? "
          "ORDER BY tf.file_index LIMIT ? OFFSET ?";

    torrent_file_row_t *rows = (torrent_file_row_t *)calloc(limit, sizeof(*rows));
    if (!rows) return -1;
    int n = 0;

    sqlite3_stmt *st = NULL;
    database_read_lock(db);
    if (sqlite3_prepare_v2(db->db, page_sql, -1, &st, NULL) == SQLITE_OK) {
        int idx = 1;
        sqlite3_bind_int64(st, idx++, torrent_id);
        if (has_filter) sqlite3_bind_text(st, idx++, filter, -1, SQLITE_STATIC);
        sqlite3_bind_int(st, idx++, limit);
        sqlite3_bind_int(st, idx++, offset);

        while (sqlite3_step(st) == SQLITE_ROW && n < limit) {
            const unsigned char *p = sqlite3_column_text(st, 0);
            rows[n].path = p ? strdup((const char *)p) : strdup("");
            rows[n].size_bytes = sqlite3_column_int64(st, 1);
            n++;
        }
        sqlite3_finalize(st);
    }
    database_read_unlock(db);

    *out_files = rows;
    *out_count = n;
    return 0;
}

void db_free_torrent_file_rows(torrent_file_row_t *rows, int count) {
    if (!rows) return;
    for (int i = 0; i < count; i++) free(rows[i].path);
    free(rows);
}
