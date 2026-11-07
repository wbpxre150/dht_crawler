#include "database.h"
#include "dht_crawler.h"
#include "bloom_filter.h"
#include <time.h>   /* Stage 7: CLOCK_MONOTONIC for the read-connection open timing */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <limits.h>

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
    "CREATE INDEX IF NOT EXISTS idx_files_prefix ON torrent_files(prefix_id);"
    ""
    /* Stage 5 ingest-time classification (classifier.md §7). A separate table
     * leaves the torrents row and its FTS triggers untouched; torrent_id is the
     * primary key, so there is at most one class row per torrent. */
    "CREATE TABLE IF NOT EXISTS torrent_class ("
    "    torrent_id INTEGER PRIMARY KEY REFERENCES torrents(id) ON DELETE CASCADE,"
    "    category INTEGER NOT NULL,"
    "    tv_kind INTEGER,"
    "    adult INTEGER NOT NULL DEFAULT 0,"
    "    adult_p REAL,"
    "    season_lo INTEGER,"
    "    season_hi INTEGER,"
    "    episode_lo INTEGER,"
    "    episode_hi INTEGER,"
    "    resolution INTEGER,"
    "    confidence REAL,"
    "    clf_ver INTEGER NOT NULL,"
    /* Movie-kind (Stage C): the v9 JEV genre mask and the generation that
     * produced it. Additive and nullable on purpose - a database that predates
     * them has neither, and "unclassified" is movie_kind_clf_ver IS NULL (a
     * mask of 0 is a valid `none` verdict, never a marker). Deliberately no
     * index: the candidate walk reuses idx_class_cat below. */
    "    movie_kind_mask INTEGER,"
    "    movie_kind_clf_ver INTEGER"
    ");"
    ""
    "CREATE INDEX IF NOT EXISTS idx_class_cat ON torrent_class(category, tv_kind, torrent_id) WHERE adult = 0;"
    "CREATE INDEX IF NOT EXISTS idx_class_ver ON torrent_class(clf_ver);"
    ""
    "CREATE TABLE IF NOT EXISTS classifier_meta (key TEXT PRIMARY KEY, value TEXT);";

static const char *CREATE_FTS_SQL =
    "CREATE VIRTUAL TABLE IF NOT EXISTS torrent_search USING fts5("
    "    name,"
    "    tokenize='porter unicode61',"
    "    content='torrents',"
    "    content_rowid='id'"
    ");"
    ""
    /* Schema v2 torrent-level word index. Contentless (content='') so only the
     * index is stored, never the body text; contentless_delete=1 (SQLite >=
     * 3.43) lets the delete trigger drop a rowid without that original text. */
    "CREATE VIRTUAL TABLE IF NOT EXISTS torrent_docs USING fts5("
    "    body,"
    "    tokenize='unicode61 remove_diacritics 2',"
    "    content='',"
    "    contentless_delete=1"
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
    /* Schema v2: keep torrent_docs in step with torrent row deletions (torrents
     * are deleted outside the ingest flush path). */
    "CREATE TRIGGER IF NOT EXISTS torrents_docs_ad AFTER DELETE ON torrents BEGIN"
    "    DELETE FROM torrent_docs WHERE rowid = old.id;"
    "END;";
/* Write the schema DDL to `out`, one CREATE statement per line.
 *
 * The three constants above are newline-free concatenated string literals, so
 * the statements are split here instead. The split point is the ';' that
 * completes a whole statement as judged by sqlite3_complete(), not every ';':
 * the inner statements of a CREATE TRIGGER ... BEGIN ... END body end in ';'
 * too, and a trigger must stay on one line to remain one statement. Used by the
 * --print-schema one-shot, so external tooling (the host database rebuild
 * script) takes its schema from this file rather than a hand-copied dump. */
void database_print_schema(FILE *out) {
    const char *parts[] = {
        CREATE_TABLES_SQL, CREATE_FTS_SQL, CREATE_TRIGGERS_SQL
    };
    char stmt[8192];   /* longest statement is a ~200 byte trigger body */
    size_t len = 0;

    if (!out) {
        return;
    }
    stmt[0] = '\0';
    for (size_t p = 0; p < sizeof(parts) / sizeof(parts[0]); p++) {
        for (const char *s = parts[p]; *s; s++) {
            if (len + 2 >= sizeof(stmt)) {
                /* Unreachable with the current DDL; emit rather than truncate. */
                fwrite(stmt, 1, len, out);
                len = 0;
                stmt[0] = '\0';
            }
            stmt[len++] = *s;
            stmt[len] = '\0';
            if (*s == ';' && sqlite3_complete(stmt)) {
                fwrite(stmt, 1, len, out);
                fputc('\n', out);
                len = 0;
                stmt[0] = '\0';
            }
        }
    }
    if (len > 0) {
        fwrite(stmt, 1, len, out);
    }
    fflush(out);
}

/* pthread_key destructor for a thread's read connection (defined below
 * database_read_unlock). Forward-declared because database_init() registers
 * it before the definition. */
static void read_conn_destructor(void *value);

/* Initialize database */
int database_init(database_t *db, const char *db_path, app_context_t *app_ctx) {
    if (!db || !db_path || !app_ctx) {
        return -1;
    }

    memset(db, 0, sizeof(database_t));
    db->app_ctx = app_ctx;
    db->bloom = NULL;  /* Will be set via database_set_bloom() */
    /* Keep the path: database_read_db() reopens it per thread. */
    db->db_path = strdup(db_path);
    if (!db->db_path) {
        return -1;
    }

    /* Open database */
    int rc = sqlite3_open(db_path, &db->db);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to open database: %s", sqlite3_errmsg(db->db));
        free(db->db_path);
        db->db_path = NULL;
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
        free(db->db_path);
        db->db_path = NULL;
        return -1;
    }

    /* Initialize checkpoint rwlock */
    if (pthread_rwlock_init(&db->checkpoint_rwlock, NULL) != 0) {
        uv_mutex_destroy(&db->mutex);
        sqlite3_close(db->db);
        free(db->db_path);
        db->db_path = NULL;
        return -1;
    }

    /* Per-thread read connections: the registry mutex plus the TLS slot whose
     * destructor closes a thread's connection when it exits. */
    if (pthread_mutex_init(&db->read_conn_mutex, NULL) != 0) {
        pthread_rwlock_destroy(&db->checkpoint_rwlock);
        uv_mutex_destroy(&db->mutex);
        sqlite3_close(db->db);
        free(db->db_path);
        db->db_path = NULL;
        return -1;
    }
    db->read_conns_count = 0;
    if (pthread_key_create(&db->read_conn_key, read_conn_destructor) != 0) {
        pthread_mutex_destroy(&db->read_conn_mutex);
        pthread_rwlock_destroy(&db->checkpoint_rwlock);
        uv_mutex_destroy(&db->mutex);
        sqlite3_close(db->db);
        free(db->db_path);
        db->db_path = NULL;
        return -1;
    }
    db->read_conn_key_valid = 1;

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

/* -------------------------------------------------------------------------
 * Per-thread read connections
 *
 * The long read paths (/search, /browse, /random) used to run their
 * statements on the process-wide db->db handle, so a single multi-second
 * sqlite3_step() blocked every other DB user. They now run on a connection
 * owned by the calling thread: database_read_db() opens it lazily and the
 * pthread_key destructor closes it when the thread exits (civetweb worker
 * threads are joined by mg_stop() before database_cleanup() runs).
 *
 * Isolation only: no SQL, no bind order and no result handling changes, and
 * every reader still holds database_read_lock() so a RESTART checkpoint
 * cannot run underneath it.
 * ---------------------------------------------------------------------- */

/* One thread's own read connection. Heap-allocated because the pthread_key
 * destructor only receives the TLS value itself, and it needs both the handle
 * to close and the database_t whose registry entry to drop. */
struct database_read_conn {
    sqlite3 *conn;
    database_t *db;
};

/* Drop [rc] from the registry. Returns 1 when it was there (the caller then
 * owns it and must close/free it), 0 when another owner already claimed it. */
static int read_conn_unregister(database_t *db, struct database_read_conn *rc) {
    int found = 0;
    pthread_mutex_lock(&db->read_conn_mutex);
    for (int i = 0; i < db->read_conns_count; i++) {
        if (db->read_conns[i] == rc) {
            db->read_conns[i] = db->read_conns[db->read_conns_count - 1];
            db->read_conns[db->read_conns_count - 1] = NULL;
            db->read_conns_count--;
            found = 1;
            break;
        }
    }
    pthread_mutex_unlock(&db->read_conn_mutex);
    return found;
}

/* Runs when the owning thread exits (civetweb workers are joined by mg_stop(),
 * so this normally happens long before database_cleanup()). Ownership is
 * decided under the registry mutex by pointer identity: whoever claims the
 * entry owns it, the other side finds nothing and touches nothing. */
static void read_conn_destructor(void *value) {
    struct database_read_conn *rc = (struct database_read_conn *)value;
    if (!rc) {
        return;
    }

    /* Safe even when this runs after database_cleanup(): cleanup drops the TLS
     * key and clears read_conn_key_valid *before* it sweeps the registry, and
     * it deliberately leaves the database_read_conn structs allocated (zombied)
     * instead of freeing them, so this read of rc->db cannot be a use-after-free.
     * The flag being 0 means we return without touching the registry or the
     * connection cleanup already closed. */
    database_t *db = rc->db;
    if (!db || !db->read_conn_key_valid) {
        return;
    }

    if (!read_conn_unregister(db, rc)) {
        return;
    }

    /* close_v2: a stray unfinalized statement must not turn shutdown into a
     * leaked connection (close_v2 zombies the handle instead of failing). */
    if (rc->conn) {
        sqlite3_close_v2(rc->conn);
    }
    free(rc);
}

/* Stage 7 diagnostics: CLOCK_MONOTONIC milliseconds for the read-connection
 * open. Log-only helper; the open path itself is unchanged. */
static double read_open_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

/* Return this thread's read connection, opening it on first use.
 *
 * SQLITE_OPEN_READWRITE (not READONLY) on purpose: a WAL database needs a
 * writable -shm wal-index file, which a read-only connection cannot map. The
 * connection itself never writes anything -- see the literal-SQL rule on the
 * prototype in database.h, which replaces PRAGMA query_only=ON because the
 * bundled SQLite 3.49.1 rejects every write transaction under query_only,
 * TEMP tables included.
 *
 * Any failure degrades to db->db, i.e. exactly today's behaviour. */
sqlite3 *database_read_db(database_t *db) {
    if (!db || !db->db) {
        return NULL;
    }
    if (!db->read_conn_key_valid || !db->db_path) {
        return db->db;
    }

    struct database_read_conn *rc =
        (struct database_read_conn *)pthread_getspecific(db->read_conn_key);
    if (rc && rc->conn) {
        return rc->conn;
    }

    sqlite3 *conn = NULL;
    double t_open = read_open_now_ms();   /* Stage 7: open + pragmas timing */
    if (sqlite3_open_v2(db->db_path, &conn, SQLITE_OPEN_READWRITE, NULL) != SQLITE_OK) {
        log_msg(LOG_WARN, "Read connection: cannot open %s: %s (using shared connection)",
                db->db_path, conn ? sqlite3_errmsg(conn) : "out of memory");
        if (conn) sqlite3_close(conn);
        return db->db;
    }

    /* Mirror the per-connection pragmas of database_init(). journal_mode,
     * page_size and auto_vacuum are database properties, already set there. */
    sqlite3_exec(conn, "PRAGMA cache_size=-64000;", NULL, NULL, NULL);
    sqlite3_exec(conn, "PRAGMA mmap_size=268435456;", NULL, NULL, NULL);
    sqlite3_busy_timeout(conn, 5000);

    /* Stage 7: a cold /search pays for this thread's first connection over the
     * USB-backed database. Fires at most once per worker thread; INFO because
     * that is the device default level (config.c), so it needs no config edit. */
    double open_ms = read_open_now_ms() - t_open;
    if (open_ms >= 1000.0) {
        log_msg(LOG_INFO, "read connection: open+pragmas took %.1fms (first use on this thread)",
                open_ms);
    }

    rc = (struct database_read_conn *)calloc(1, sizeof(*rc));
    if (!rc) {
        sqlite3_close(conn);
        return db->db;
    }
    rc->conn = conn;
    rc->db = db;

    /* Register before publishing the TLS value: an unregistered connection
     * would be invisible to the shutdown safety net, and a TLS value set
     * before a failed registration would dangle into the destructor. */
    pthread_mutex_lock(&db->read_conn_mutex);
    if (db->read_conns_count >= DATABASE_READ_CONN_MAX) {
        pthread_mutex_unlock(&db->read_conn_mutex);
        log_msg(LOG_WARN, "Read connection registry full (%d), using shared connection",
                DATABASE_READ_CONN_MAX);
        sqlite3_close(conn);
        free(rc);
        return db->db;
    }
    db->read_conns[db->read_conns_count++] = rc;
    pthread_mutex_unlock(&db->read_conn_mutex);

    if (pthread_setspecific(db->read_conn_key, rc) != 0) {
        read_conn_unregister(db, rc);
        sqlite3_close(conn);
        free(rc);
        return db->db;
    }

    log_msg(LOG_DEBUG, "Read connection opened for thread %lu", (unsigned long)pthread_self());
    return conn;
}

/* Set bloom filter for tracking successful writes */
void database_set_bloom(database_t *db, bloom_filter_t *bloom) {
    if (!db) {
        return;
    }
    db->bloom = bloom;
}

/* Additive column migration for a schema that already exists.
 *
 * torrent_class gained its two movie-kind columns (Stage C) after databases were
 * already in the field, and this tree has no ALTER TABLE precedent, so the
 * migration is a guarded, idempotent PRAGMA table_info() check: it adds only the
 * columns that are missing and is therefore safe to run on every start
 * (main.c calls database_create_schema() on each launch, against a live
 * multi-gigabyte database). ALTER TABLE ... ADD COLUMN is a metadata-only
 * change - it rewrites no row - and a database created by CREATE TABLE above
 * already has the columns, so the check finds nothing to do there. Nothing here
 * touches an index: the movie-kind walk reuses idx_class_cat.
 *
 * Returns 0 when the column exists afterwards, -1 on a SQL error (logged). */
static int database_ensure_column(sqlite3 *db, const char *table, const char *column,
                                  const char *declaration) {
    sqlite3_stmt *stmt = NULL;
    char pragma_sql[128];
    char alter_sql[256];
    char *err_msg = NULL;
    int found = 0;
    int rc;

    snprintf(pragma_sql, sizeof pragma_sql, "PRAGMA table_info(%s)", table);
    rc = sqlite3_prepare_v2(db, pragma_sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Schema: cannot inspect %s (%d: %s)", table, rc, sqlite3_errmsg(db));
        return -1;
    }
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char *name = sqlite3_column_text(stmt, 1);
        if (name && strcmp((const char *)name, column) == 0) {
            found = 1;
            break;
        }
    }
    sqlite3_finalize(stmt);
    if (!found && rc != SQLITE_DONE) {
        log_msg(LOG_ERROR, "Schema: cannot read the columns of %s (%d: %s)", table, rc,
                sqlite3_errmsg(db));
        return -1;
    }
    if (found) {
        return 0;
    }

    snprintf(alter_sql, sizeof alter_sql, "ALTER TABLE %s ADD COLUMN %s %s", table, column,
             declaration);
    rc = sqlite3_exec(db, alter_sql, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Schema: cannot add %s.%s: %s", table, column,
                err_msg ? err_msg : sqlite3_errmsg(db));
        sqlite3_free(err_msg);
        return -1;
    }
    log_msg(LOG_INFO, "Schema: added the missing column %s.%s", table, column);
    return 0;
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

    /* Additive migration for a database created before the movie-kind columns
     * existed. Idempotent and metadata-only, so it runs on every start. */
    if (database_ensure_column(db->db, "torrent_class", "movie_kind_mask", "INTEGER") != 0 ||
        database_ensure_column(db->db, "torrent_class", "movie_kind_clf_ver", "INTEGER") != 0) {
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

    /* Stage 5: one classification row per torrent, inserted by
     * database_insert_torrent_unsafe() in the same transaction as the torrent.
     * OR IGNORE guards the primary key when an INSERT OR REPLACE on torrents
     * reuses a rowid that still carries an old class row. */
    const char *insert_class_sql =
        "INSERT OR IGNORE INTO torrent_class "
        "(torrent_id, category, tv_kind, adult, adult_p, season_lo, season_hi, "
        " episode_lo, episode_hi, resolution, confidence, clf_ver) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    rc = sqlite3_prepare_v2(db->db, insert_class_sql, -1, &db->insert_class_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert class statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    /* Schema v2: one document per torrent for the torrent-level word index.
     * body = torrents.name plus every leaf filename of that torrent, built in
     * database_insert_torrent_unsafe() from the in-memory batch item. */
    const char *insert_doc_sql =
        "INSERT INTO torrent_docs(rowid, body) VALUES (?, ?)";

    rc = sqlite3_prepare_v2(db->db, insert_doc_sql, -1, &db->insert_doc_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Failed to prepare insert doc statement: %s", sqlite3_errmsg(db->db));
        return -1;
    }

    return 0;
}

/* Rebuild the schema-v2 torrent-level word index (torrent_docs) of an existing
 * database from its base tables. Used by `--rebuild-docs <db>`: the table, its
 * delete trigger and the ingest path all ship with the daemon, but a database
 * that predates them has an empty torrent_docs and has to be backfilled once.
 *
 * The index is contentless (content='', contentless_delete=1), so rows are
 * inserted explicitly with the same body the flush path builds: the torrent
 * name plus every leaf filename of that torrent. The backfill runs in bounded
 * chunks of torrent ids, one transaction per chunk, deleting the chunk's rowid
 * range first so a re-run is idempotent (rowids in a gap are harmless). On a
 * chunk failure the chunk is rolled back; earlier chunks stay committed and a
 * re-run continues idempotently. The WAL is checkpointed and truncated at the
 * end so the file is self-contained for copy/push. Deliberately no
 * database_integrity_check(): that is a full-database scan, and the idempotent
 * re-run plus the row-count checks are the verification here.
 *
 * Returns 0 on success, -1 on failure. */
int database_rebuild_docs(const char *db_path) {
    if (!db_path) return -1;

    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to open %s: %s",
                db_path, sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    sqlite3_stmt *hi_stmt = NULL;
    sqlite3_stmt *del_stmt = NULL;
    sqlite3_stmt *ins_stmt = NULL;

    sqlite3_busy_timeout(db, 30000);
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);

    char *err = NULL;
    log_msg(LOG_INFO, "rebuild-docs: creating torrent_docs and its trigger ...");
    rc = sqlite3_exec(db, CREATE_FTS_SQL, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to create FTS tables: %s", err);
        sqlite3_free(err);
        goto fail;
    }
    rc = sqlite3_exec(db, CREATE_TRIGGERS_SQL, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to create FTS triggers: %s", err);
        sqlite3_free(err);
        goto fail;
    }

    /* Assert that contentless_delete actually took effect. A DELETE on an
     * *empty* contentless table succeeds even without the option, so probe
     * with a real row: insert one at the top of the rowid range, delete it. */
    log_msg(LOG_INFO, "rebuild-docs: probing contentless_delete support ...");
    rc = sqlite3_exec(db,
                      "INSERT INTO torrent_docs(rowid, body) "
                      "VALUES(9223372036854775807, 'rebuild-docs probe');"
                      "DELETE FROM torrent_docs WHERE rowid = 9223372036854775807;",
                      NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: contentless-delete probe failed: %s", err);
        sqlite3_free(err);
        log_msg(LOG_ERROR, "this SQLite build rejects contentless_delete; "
                           "torrent_docs requires SQLite >= 3.43 with FTS5");
        goto fail;
    }

    /* Total is only used for progress reporting. */
    sqlite3_int64 total = 0;
    sqlite3_stmt *count_stmt = NULL;
    rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM torrents", -1, &count_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to count torrents: %s", sqlite3_errmsg(db));
        goto fail;
    }
    if (sqlite3_step(count_stmt) == SQLITE_ROW) {
        total = sqlite3_column_int64(count_stmt, 0);
    }
    sqlite3_finalize(count_stmt);
    count_stmt = NULL;
    log_msg(LOG_INFO, "rebuild-docs: %lld torrents to index", (long long)total);

    /* Chunk bounds: the highest id in the next 10000-id window after ?1. */
    rc = sqlite3_prepare_v2(db,
                            "SELECT MAX(id) FROM (SELECT id FROM torrents "
                            "WHERE id > ?1 ORDER BY id LIMIT 10000)",
                            -1, &hi_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to prepare chunk bound statement: %s",
                sqlite3_errmsg(db));
        goto fail;
    }

    /* Idempotent re-run: drop whatever the previous run (or the live ingest
     * path) left in this rowid range before re-inserting it. */
    rc = sqlite3_prepare_v2(db,
                            "DELETE FROM torrent_docs WHERE rowid > ?1 AND rowid <= ?2;",
                            -1, &del_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to prepare chunk delete statement: %s",
                sqlite3_errmsg(db));
        goto fail;
    }

    /* Same body the flush path builds: torrent name plus its leaf filenames. */
    rc = sqlite3_prepare_v2(db,
                            "INSERT INTO torrent_docs(rowid, body) "
                            "SELECT t.id, t.name || ' ' || COALESCE((SELECT "
                            "group_concat(f.filename, ' ') FROM torrent_files f "
                            "WHERE f.torrent_id = t.id), '') "
                            "FROM torrents t WHERE t.id > ?1 AND t.id <= ?2 "
                            "ORDER BY t.id;",
                            -1, &ins_stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "rebuild-docs: failed to prepare chunk insert statement: %s",
                sqlite3_errmsg(db));
        goto fail;
    }

    sqlite3_int64 last_id = 0;
    sqlite3_int64 done = 0;
    int chunks = 0;

    for (;;) {
        sqlite3_reset(hi_stmt);
        sqlite3_bind_int64(hi_stmt, 1, last_id);
        if (sqlite3_step(hi_stmt) != SQLITE_ROW || sqlite3_column_type(hi_stmt, 0) == SQLITE_NULL) {
            sqlite3_reset(hi_stmt);
            break;
        }
        sqlite3_int64 chunk_lo = last_id;
        sqlite3_int64 chunk_hi = sqlite3_column_int64(hi_stmt, 0);
        sqlite3_reset(hi_stmt);

        rc = sqlite3_exec(db, "BEGIN;", NULL, NULL, &err);
        if (rc != SQLITE_OK) {
            log_msg(LOG_ERROR, "rebuild-docs: BEGIN failed: %s", err);
            sqlite3_free(err);
            goto fail;
        }

        sqlite3_reset(del_stmt);
        sqlite3_bind_int64(del_stmt, 1, chunk_lo);
        sqlite3_bind_int64(del_stmt, 2, chunk_hi);
        int chunk_rc = sqlite3_step(del_stmt);
        sqlite3_reset(del_stmt);
        if (chunk_rc == SQLITE_DONE) {
            sqlite3_reset(ins_stmt);
            sqlite3_bind_int64(ins_stmt, 1, chunk_lo);
            sqlite3_bind_int64(ins_stmt, 2, chunk_hi);
            chunk_rc = sqlite3_step(ins_stmt);
            sqlite3_reset(ins_stmt);
        }

        if (chunk_rc != SQLITE_DONE) {
            log_msg(LOG_ERROR,
                    "rebuild-docs: torrent_docs chunk (%lld, %lld] failed: %s",
                    (long long)chunk_lo, (long long)chunk_hi, sqlite3_errmsg(db));
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            goto fail;
        }

        done += (sqlite3_int64)sqlite3_changes(db);

        rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, &err);
        if (rc != SQLITE_OK) {
            log_msg(LOG_ERROR, "rebuild-docs: COMMIT of chunk (%lld, %lld] failed: %s",
                    (long long)chunk_lo, (long long)chunk_hi, err);
            sqlite3_free(err);
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            goto fail;
        }

        last_id = chunk_hi;
        chunks++;
        if (chunks % 50 == 0) {
            log_msg(LOG_INFO, "torrent_docs: %lld / %lld rows",
                    (long long)done, (long long)total);
        }
    }

    /* The caller may copy or push this file, so fold the WAL back in. */
    {
        int nLog = 0, nCkpt = 0;
        int ckpt_rc = sqlite3_wal_checkpoint_v2(db, NULL, SQLITE_CHECKPOINT_TRUNCATE,
                                                &nLog, &nCkpt);
        if (ckpt_rc != SQLITE_OK) {
            log_msg(LOG_ERROR, "rebuild-docs: WAL checkpoint failed: %d: %s (%d of %d frames)",
                    ckpt_rc, sqlite3_errstr(ckpt_rc), nCkpt, nLog);
            goto fail;
        }
        log_msg(LOG_INFO, "rebuild-docs: WAL checkpointed (%d of %d frames)", nCkpt, nLog);
    }

    sqlite3_finalize(hi_stmt);
    sqlite3_finalize(del_stmt);
    sqlite3_finalize(ins_stmt);
    sqlite3_close(db);

    log_msg(LOG_INFO, "--rebuild-docs: complete (%lld rows)", (long long)done);
    return 0;

fail:
    sqlite3_finalize(hi_stmt);
    sqlite3_finalize(del_stmt);
    sqlite3_finalize(ins_stmt);
    sqlite3_close(db);
    return -1;
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

/* Append `text` to a growable NUL-terminated buffer, doubling the capacity as
 * needed (capacity is the allocated size in bytes, len the current length).
 * Returns 0 on success, -1 when realloc fails; on failure the buffer is left
 * valid and unchanged. Used to build the torrent_docs body in memory. */
static int doc_body_append(char **buf, size_t *len, size_t *cap, const char *text) {
    if (!text) {
        return 0;
    }

    size_t add = strlen(text);
    if (*len + add + 1 > *cap) {
        size_t want = (*cap > 0) ? *cap : 64;
        while (want < *len + add + 1) {
            want *= 2;
        }
        char *grown = realloc(*buf, want);
        if (!grown) {
            return -1;
        }
        *buf = grown;
        *cap = want;
    }

    memcpy(*buf + *len, text, add);
    *len += add;
    (*buf)[*len] = '\0';
    return 0;
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

    /* Schema v2: build this torrent's torrent_docs body — its name plus the leaf
     * filename of every file row that is actually inserted below — from the
     * in-memory batch item. The database is never re-read for it. */
    size_t doc_len = 0;
    size_t doc_cap = 0;
    char *doc_body = NULL;
    int doc_body_ok = 1;
    if (doc_body_append(&doc_body, &doc_len, &doc_cap,
                        torrent->name ? torrent->name : "") != 0) {
        log_msg(LOG_ERROR, "Failed to build torrent_docs body for torrent_id %lld: out of memory",
                (long long)torrent_id);
        return -1;
    }

    /* Stage 5: the classification row rides in the caller's transaction (one
     * BEGIN...COMMIT per batch flush), so it is never a second transaction.
     * A failure here is logged but does not fail the torrent insert: the
     * torrent itself is already written; a missing class row just leaves the
     * torrent unlabelled. */
    if (torrent->has_classification && db->insert_class_stmt) {
        const classification_t *c = &torrent->classification;
        sqlite3_reset(db->insert_class_stmt);
        sqlite3_bind_int64(db->insert_class_stmt, 1, torrent_id);
        sqlite3_bind_int(db->insert_class_stmt, 2, c->category);
        if (c->tv_kind > 0) {
            sqlite3_bind_int(db->insert_class_stmt, 3, c->tv_kind);
        } else {
            sqlite3_bind_null(db->insert_class_stmt, 3);  /* NULL unless tv/anime */
        }
        sqlite3_bind_int(db->insert_class_stmt, 4, c->adult);
        sqlite3_bind_double(db->insert_class_stmt, 5, c->adult_p);
        sqlite3_bind_int(db->insert_class_stmt, 6, c->season_lo);
        sqlite3_bind_int(db->insert_class_stmt, 7, c->season_hi);
        sqlite3_bind_int(db->insert_class_stmt, 8, c->episode_lo);
        sqlite3_bind_int(db->insert_class_stmt, 9, c->episode_hi);
        sqlite3_bind_int(db->insert_class_stmt, 10, c->resolution);
        sqlite3_bind_double(db->insert_class_stmt, 11, c->confidence);
        sqlite3_bind_int64(db->insert_class_stmt, 12, c->clf_ver);

        rc = sqlite3_step(db->insert_class_stmt);
        sqlite3_reset(db->insert_class_stmt);
        if (rc != SQLITE_DONE) {
            log_msg(LOG_WARN, "Failed to insert torrent_class for torrent_id %lld: %s",
                   (long long)torrent_id, sqlite3_errmsg(db->db));
        }
    }

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
        } else if (doc_body_ok &&
                   (doc_body_append(&doc_body, &doc_len, &doc_cap, " ") != 0 ||
                    doc_body_append(&doc_body, &doc_len, &doc_cap, filename) != 0)) {
            /* Only files whose row really made it into torrent_files belong in
             * the document, so this sits on the success path. Keep inserting
             * the remaining files (the torrent row is already written) and fail
             * the item at the end, exactly like a failed doc insert. */
            log_msg(LOG_ERROR, "Failed to build torrent_docs body for torrent_id %lld: out of memory",
                    (long long)torrent_id);
            doc_body_ok = 0;
        }

        free(prefix);
        free(filename);
    }

    if (!doc_body_ok) {
        free(doc_body);
        return -1;
    }

    /* Schema v2: one torrent_docs row per torrent, written in the caller's
     * transaction — the same BEGIN...COMMIT that writes the torrent and its
     * files, so the index can never lag behind the rows it describes.
     *
     * There is deliberately NO insert trigger for torrent_docs: the body is a
     * concatenation of the torrent name plus *all* of that torrent's filenames,
     * which a per-row trigger cannot build (a trigger on torrents fires before
     * the file rows exist, and a trigger on torrent_files would have to keep
     * rewriting an ever-growing document). This explicit insert is therefore the
     * only writer of the table. The delete trigger torrents_docs_ad from the
     * schema is still required, because torrent rows are also deleted outside
     * this flush path — the batched purges in category_filter_cleanup.c:244 and
     * language_filter_cleanup.c:289/302.
     *
     * Orphan note: the torrents insert above is INSERT OR REPLACE and
     * recursive_triggers is off, so re-ingesting an info_hash that already
     * exists REPLACEs the row with a new rowid without firing torrents_docs_ad;
     * the old rowid's document is then left behind. That is rare by construction
     * (the bloom filter keeps already-ingested hashes out of new batches), it is
     * exactly what torrent_search has always done, and a full --rebuild-docs
     * re-run deletes the docs of every id range it processes, which cleans them.
     *
     * A failure here fails the whole item (log + -1): database_insert_batch()
     * records it in success_flags[] and leaves it out of the post-commit bloom
     * update, so the item is re-fetched later. No silent index skip. */
    sqlite3_reset(db->insert_doc_stmt);
    sqlite3_bind_int64(db->insert_doc_stmt, 1, torrent_id);
    sqlite3_bind_text(db->insert_doc_stmt, 2, doc_body, -1, SQLITE_TRANSIENT);

    rc = sqlite3_step(db->insert_doc_stmt);
    sqlite3_reset(db->insert_doc_stmt);
    if (rc != SQLITE_DONE) {
        log_msg(LOG_ERROR, "Failed to insert torrent_docs for torrent_id %lld: %s",
                (long long)torrent_id, sqlite3_errmsg(db->db));
        free(doc_body);
        return -1;
    }

    free(doc_body);

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
    if (db->insert_doc_stmt)     sqlite3_reset(db->insert_doc_stmt);

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

/* Vacuum database into a new file (VACUUM INTO) */
int database_vacuum_into(database_t *db, const char *dest_path) {
    if (!db || !db->db || !dest_path) {
        return -1;
    }

    char sql[PATH_MAX + 32];
    snprintf(sql, sizeof(sql), "VACUUM INTO '%s';", dest_path);

    char *err_msg = NULL;
    int rc = sqlite3_exec(db->db, sql, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "VACUUM INTO failed: %s", err_msg);
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

/* Run integrity_check PRAGMA on the database.
 * Returns 0 if the database is clean, >0 for the number of errors found,
 * or -1 on a fatal open/query failure. */
int database_integrity_check(const char *db_path) {
    if (!db_path) return -1;

    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "integrity_check: failed to open %s: %s",
                db_path, sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "integrity_check: failed to prepare query: %s",
                sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    int errors = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *msg = (const char *)sqlite3_column_text(stmt, 0);
        if (!msg) continue;
        if (strcmp(msg, "ok") == 0) {
            log_msg(LOG_INFO, "integrity_check: ok");
        } else {
            log_msg(LOG_ERROR, "integrity_check: %s", msg);
            errors++;
        }
    }

    if (rc != SQLITE_DONE) {
        log_msg(LOG_ERROR, "integrity_check: query error: %s", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return errors;
}

/* Recover a corrupt database by copying all readable rows into a fresh database.
 * Opens src_path read-only, creates dst_path with the full schema, then copies
 * path_prefixes → torrents → torrent_files in order (preserving original IDs so
 * foreign-key relationships survive). FK checks are disabled during the copy.
 * Returns the number of torrent rows recovered, or -1 on a fatal error. */
int database_recover(const char *src_path, const char *dst_path) {
    if (!src_path || !dst_path) return -1;

    sqlite3 *src = NULL, *dst = NULL;
    int rc;

    rc = sqlite3_open_v2(src_path, &src, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "recover: failed to open source %s: %s",
                src_path, sqlite3_errmsg(src));
        sqlite3_close(src);
        return -1;
    }

    rc = sqlite3_open(dst_path, &dst);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "recover: failed to open destination %s: %s",
                dst_path, sqlite3_errmsg(dst));
        sqlite3_close(src);
        sqlite3_close(dst);
        return -1;
    }

    sqlite3_exec(dst, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(dst, "PRAGMA foreign_keys=OFF;", NULL, NULL, NULL);

    char *err = NULL;
    rc = sqlite3_exec(dst, CREATE_TABLES_SQL, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "recover: failed to create tables: %s", err);
        sqlite3_free(err);
        sqlite3_close(src); sqlite3_close(dst);
        return -1;
    }
    rc = sqlite3_exec(dst, CREATE_FTS_SQL, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "recover: failed to create FTS tables: %s", err);
        sqlite3_free(err);
        sqlite3_close(src); sqlite3_close(dst);
        return -1;
    }
    rc = sqlite3_exec(dst, CREATE_TRIGGERS_SQL, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "recover: failed to create triggers: %s", err);
        sqlite3_free(err);
        sqlite3_close(src); sqlite3_close(dst);
        return -1;
    }

    sqlite3_exec(dst, "BEGIN;", NULL, NULL, NULL);

    sqlite3_stmt *sel = NULL, *ins = NULL;
    int skipped = 0;

    /* --- path_prefixes --- */
    int prefix_count = 0;
    rc = sqlite3_prepare_v2(src, "SELECT id, prefix FROM path_prefixes", -1, &sel, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_prepare_v2(dst,
            "INSERT OR IGNORE INTO path_prefixes(id, prefix) VALUES(?,?)",
            -1, &ins, NULL);
        while ((rc = sqlite3_step(sel)) == SQLITE_ROW) {
            const char *prefix = (const char *)sqlite3_column_text(sel, 1);
            if (!prefix) { skipped++; continue; }
            sqlite3_bind_int64(ins, 1, sqlite3_column_int64(sel, 0));
            sqlite3_bind_text(ins, 2, prefix, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(ins) == SQLITE_DONE) prefix_count++;
            else skipped++;
            sqlite3_reset(ins);
        }
        if (rc != SQLITE_DONE)
            log_msg(LOG_WARN, "recover: path_prefixes scan ended early: %s", sqlite3_errmsg(src));
        sqlite3_finalize(ins); ins = NULL;
    }
    sqlite3_finalize(sel); sel = NULL;
    log_msg(LOG_INFO, "recover: copied %d path prefixes", prefix_count);

    /* --- torrents --- */
    int torrent_count = 0;
    rc = sqlite3_prepare_v2(src,
        "SELECT id, info_hash, name, size_bytes, total_peers, added_timestamp FROM torrents",
        -1, &sel, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_prepare_v2(dst,
            "INSERT OR IGNORE INTO torrents"
            "(id, info_hash, name, size_bytes, total_peers, added_timestamp)"
            " VALUES(?,?,?,?,?,?)",
            -1, &ins, NULL);
        while ((rc = sqlite3_step(sel)) == SQLITE_ROW) {
            const void *hash = sqlite3_column_blob(sel, 1);
            int hash_len = sqlite3_column_bytes(sel, 1);
            const char *name = (const char *)sqlite3_column_text(sel, 2);
            if (!hash || hash_len != SHA1_DIGEST_LENGTH || !name) { skipped++; continue; }

            sqlite3_bind_int64(ins, 1, sqlite3_column_int64(sel, 0));
            sqlite3_bind_blob(ins, 2, hash, hash_len, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 3, name, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(ins, 4, sqlite3_column_int64(sel, 3));
            sqlite3_bind_int(ins, 5, sqlite3_column_int(sel, 4));
            sqlite3_bind_int64(ins, 6, sqlite3_column_int64(sel, 5));

            if (sqlite3_step(ins) == SQLITE_DONE) torrent_count++;
            else skipped++;
            sqlite3_reset(ins);

            if (torrent_count % 100000 == 0 && torrent_count > 0)
                log_msg(LOG_INFO, "recover: %d torrents copied...", torrent_count);
        }
        if (rc != SQLITE_DONE)
            log_msg(LOG_WARN, "recover: torrents scan ended early: %s", sqlite3_errmsg(src));
        sqlite3_finalize(ins); ins = NULL;
    }
    sqlite3_finalize(sel); sel = NULL;

    /* --- torrent_files --- */
    int file_count = 0;
    rc = sqlite3_prepare_v2(src,
        "SELECT id, torrent_id, prefix_id, filename, size_bytes, file_index FROM torrent_files",
        -1, &sel, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_prepare_v2(dst,
            "INSERT OR IGNORE INTO torrent_files"
            "(id, torrent_id, prefix_id, filename, size_bytes, file_index)"
            " VALUES(?,?,?,?,?,?)",
            -1, &ins, NULL);
        while ((rc = sqlite3_step(sel)) == SQLITE_ROW) {
            const char *filename = (const char *)sqlite3_column_text(sel, 3);
            if (!filename) { skipped++; continue; }

            sqlite3_bind_int64(ins, 1, sqlite3_column_int64(sel, 0));
            sqlite3_bind_int64(ins, 2, sqlite3_column_int64(sel, 1));
            if (sqlite3_column_type(sel, 2) == SQLITE_NULL)
                sqlite3_bind_null(ins, 3);
            else
                sqlite3_bind_int64(ins, 3, sqlite3_column_int64(sel, 2));
            sqlite3_bind_text(ins, 4, filename, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(ins, 5, sqlite3_column_int64(sel, 4));
            sqlite3_bind_int(ins, 6, sqlite3_column_int(sel, 5));

            if (sqlite3_step(ins) == SQLITE_DONE) file_count++;
            else skipped++;
            sqlite3_reset(ins);
        }
        if (rc != SQLITE_DONE)
            log_msg(LOG_WARN, "recover: torrent_files scan ended early: %s", sqlite3_errmsg(src));
        sqlite3_finalize(ins); ins = NULL;
    }
    sqlite3_finalize(sel); sel = NULL;
    log_msg(LOG_INFO, "recover: copied %d torrent files", file_count);

    sqlite3_exec(dst, "COMMIT;", NULL, NULL, NULL);
    sqlite3_exec(dst, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

    /* Checkpoint + truncate WAL so the recovered DB is self-contained before rename.
     * Avoids leaving a .recover.tmp-wal sidecar that becomes orphaned after rename. */
    int nLog = 0, nCkpt = 0;
    int ckpt_rc = sqlite3_wal_checkpoint_v2(dst, NULL, SQLITE_CHECKPOINT_TRUNCATE, &nLog, &nCkpt);
    if (ckpt_rc != SQLITE_OK)
        log_msg(LOG_WARN, "recover: WAL checkpoint returned %d: %s (%d/%d frames)",
                ckpt_rc, sqlite3_errstr(ckpt_rc), nCkpt, nLog);

    sqlite3_close(src);
    sqlite3_close(dst);

    if (skipped > 0)
        log_msg(LOG_WARN, "recover: %d rows skipped (corrupt or invalid data)", skipped);
    log_msg(LOG_INFO, "recover: total %d torrents recovered", torrent_count);
    return torrent_count;
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

    if (db->insert_class_stmt) {
        sqlite3_finalize(db->insert_class_stmt);
        db->insert_class_stmt = NULL;
    }

    if (db->insert_doc_stmt) {
        sqlite3_finalize(db->insert_doc_stmt);
        db->insert_doc_stmt = NULL;
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

    /* Per-thread read connections. Normally the pthread_key destructor of each
     * worker thread has already closed and unregistered its connection (mg_stop()
     * joins the workers before cleanup), so this only sweeps a leaked entry.
     * They are closed before the final checkpoint so no read connection can
     * still hold the WAL open.
     *
     * The TLS key is dropped and read_conn_key_valid cleared *before* the sweep,
     * and the database_read_conn structs are deliberately left allocated
     * (zombied) instead of being freed: a destructor still in flight — or one
     * that runs after this returns — then reads live memory, observes
     * read_conn_key_valid == 0 and returns without touching the registry or the
     * connection closed below. That makes read_conn_destructor's guard hold by
     * construction rather than by timing. The leak is bounded by
     * DATABASE_READ_CONN_MAX structs, happens only at shutdown, and is reclaimed
     * at process exit. */
    if (db->read_conn_key_valid) {
        db->read_conn_key_valid = 0;   /* no thread may use the key from here on */
        pthread_key_delete(db->read_conn_key);
    }
    pthread_mutex_lock(&db->read_conn_mutex);
    for (int i = 0; i < db->read_conns_count; i++) {
        struct database_read_conn *rc = db->read_conns[i];
        db->read_conns[i] = NULL;
        if (!rc) continue;
        log_msg(LOG_WARN, "Read connection still registered at cleanup, closing it");
        if (rc->conn) sqlite3_close_v2(rc->conn);
        rc->conn = NULL;   /* a late destructor must never close it twice */
        /* rc itself is intentionally not freed: see the comment above. */
    }
    db->read_conns_count = 0;
    pthread_mutex_unlock(&db->read_conn_mutex);

    if (db->db) {
        /* Final checkpoint: reset all statements, then truncate WAL.
         * No need for rwlock here since we're shutting down. */
        if (db->check_exists_stmt)   sqlite3_reset(db->check_exists_stmt);
        if (db->insert_torrent_stmt) sqlite3_reset(db->insert_torrent_stmt);
        if (db->insert_file_stmt)    sqlite3_reset(db->insert_file_stmt);
        if (db->insert_prefix_stmt)  sqlite3_reset(db->insert_prefix_stmt);
        if (db->lookup_prefix_stmt)  sqlite3_reset(db->lookup_prefix_stmt);
        if (db->insert_doc_stmt)     sqlite3_reset(db->insert_doc_stmt);

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
    pthread_mutex_destroy(&db->read_conn_mutex);
    free(db->db_path);
    db->db_path = NULL;
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

    /* Build a LIKE pattern matched against the FULL displayed path
     * (prefix/filename) so a folder-name term matches too. Escape LIKE
     * wildcards in the user term, then wrap in %term%. This stays scoped
     * to one torrent via the idx_files_torrent index, so it is a fast
     * per-torrent scan. */
    char *like = NULL;
    if (has_filter) {
        size_t n = strlen(filter);
        like = (char *)malloc(n * 2 + 3);   /* worst case: every byte escaped + %..% + NUL */
        if (!like) return -1;
        size_t j = 0;
        like[j++] = '%';
        for (size_t i = 0; i < n; i++) {
            char c = filter[i];
            if (c == '%' || c == '_' || c == '\\') like[j++] = '\\';
            like[j++] = c;
        }
        like[j++] = '%';
        like[j] = '\0';
    }

    /* Total count */
    {
        sqlite3_stmt *st = NULL;
        const char *count_sql = has_filter
            ? "SELECT COUNT(*) FROM torrent_files tf "
              "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
              "WHERE tf.torrent_id = ? AND "
              "COALESCE(pp.prefix || '/' || tf.filename, tf.filename) LIKE ? ESCAPE '\\'"
            : "SELECT COUNT(*) FROM torrent_files WHERE torrent_id = ?";
        database_read_lock(db);
        if (sqlite3_prepare_v2(db->db, count_sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, torrent_id);
            if (has_filter) sqlite3_bind_text(st, 2, like, -1, SQLITE_STATIC);
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
          "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
          "WHERE tf.torrent_id = ? AND "
          "COALESCE(pp.prefix || '/' || tf.filename, tf.filename) LIKE ? ESCAPE '\\' "
          "ORDER BY tf.file_index LIMIT ? OFFSET ?"
        : "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename) AS path, "
          "       tf.size_bytes "
          "FROM torrent_files tf "
          "LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id "
          "WHERE tf.torrent_id = ? "
          "ORDER BY tf.file_index LIMIT ? OFFSET ?";

    torrent_file_row_t *rows = (torrent_file_row_t *)calloc(limit, sizeof(*rows));
    if (!rows) { free(like); return -1; }
    int n = 0;

    sqlite3_stmt *st = NULL;
    database_read_lock(db);
    if (sqlite3_prepare_v2(db->db, page_sql, -1, &st, NULL) == SQLITE_OK) {
        int idx = 1;
        sqlite3_bind_int64(st, idx++, torrent_id);
        if (has_filter) sqlite3_bind_text(st, idx++, like, -1, SQLITE_STATIC);
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

    free(like);
    *out_files = rows;
    *out_count = n;
    return 0;
}

void db_free_torrent_file_rows(torrent_file_row_t *rows, int count) {
    if (!rows) return;
    for (int i = 0; i < count; i++) free(rows[i].path);
    free(rows);
}
