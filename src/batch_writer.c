#include "batch_writer.h"
#include "dht_crawler.h"
#include "bloom_filter.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>

struct batch_writer {
    database_t *db;
    bloom_filter_t *bloom;  /* For persisting bloom filter after batch writes */
    const char *bloom_path; /* Path to save bloom filter */
    bloom_filter_t *failure_bloom;      /* Failure bloom filter for persistence */
    const char *failure_bloom_path;     /* Path to save failure bloom */

    torrent_metadata_t **batch;
    size_t batch_size;
    size_t batch_capacity;

    /* Periodic flush thread (replaces uv_timer which requires uv_run()) */
    pthread_t flush_thread;
    pthread_mutex_t flush_thread_mutex;
    pthread_cond_t flush_thread_cond;
    bool flush_thread_running;
    int flush_interval_sec;

    uv_mutex_t mutex;
    uv_cond_t backup_done;
    uv_cond_t flush_done;    /* Signaled when flush_in_progress clears */
    bool running;
    bool flush_in_progress;  /* Prevents concurrent flush operations */
    bool backup_in_progress; /* Blocks flushes during rsync backup */
    bool backup_inhibited;   /* Set during shutdown to prevent new backups from starting */

    uint64_t total_written;
    uint64_t total_flushes;

    /* Cached database counts for fast HTTP API responses */
    uint64_t cached_torrent_count;
    uint64_t cached_file_count;

    /* Rolling 60-minute window for hourly statistics */
    minute_stat_t hourly_stats[60];

    /* Daily backup */
    char *backup_db_path;       /* Source database file path */
    char *backup_dest_path;     /* Destination backup database file path */
    char *backup_sentinel_path; /* Path for last_backup_date.txt persistence file */
    char  last_backup_date[9];  /* YYYYMMDD of last successful backup, empty = never */

    /* SSH incremental backup */
    char *ssh_host;
    char *ssh_user;
    char *ssh_dest_path;
    char *ssh_key_path;       /* NULL = use SSH default key */
    char *ssh_bookmark_path;
};

/* Background thread for periodic flush.
 * Uses pthread so it works regardless of whether uv_run() is active. */
static void* flush_thread_func(void *arg) {
    batch_writer_t *writer = (batch_writer_t*)arg;

    while (true) {
        /* Sleep for flush_interval_sec, waking early if signaled to stop */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += writer->flush_interval_sec;

        pthread_mutex_lock(&writer->flush_thread_mutex);
        pthread_cond_timedwait(&writer->flush_thread_cond,
                               &writer->flush_thread_mutex, &ts);
        bool should_stop = !writer->flush_thread_running;
        pthread_mutex_unlock(&writer->flush_thread_mutex);

        if (should_stop) break;

        uv_mutex_lock(&writer->mutex);
        if (writer->batch_size > 0) {
            log_msg(LOG_DEBUG, "Auto-flushing batch writer (%zu items)", writer->batch_size);
            uv_mutex_unlock(&writer->mutex);
            batch_writer_flush(writer);
        } else {
            uv_mutex_unlock(&writer->mutex);
            /* Checkpoint even with no pending writes - WAL can grow from
             * rolled-back transactions or FTS5 segment merges without any
             * completed batch flushes. */
            database_wal_checkpoint(writer->db);
        }
    }

    return NULL;
}

batch_writer_t* batch_writer_init(database_t *db, size_t batch_capacity,
                                  int flush_interval_sec, uv_loop_t *loop) {
    if (!db || batch_capacity == 0 || !loop) {
        log_msg(LOG_ERROR, "Invalid batch writer parameters");
        return NULL;
    }

    batch_writer_t *writer = calloc(1, sizeof(batch_writer_t));
    if (!writer) {
        log_msg(LOG_ERROR, "Failed to allocate batch writer");
        return NULL;
    }

    writer->db = db;
    writer->bloom = NULL;
    writer->bloom_path = NULL;
    writer->failure_bloom = NULL;
    writer->failure_bloom_path = NULL;
    writer->batch_capacity = batch_capacity;
    writer->batch_size = 0;
    writer->flush_interval_sec = flush_interval_sec;
    writer->running = true;
    writer->flush_in_progress = false;
    writer->backup_in_progress = false;
    writer->flush_thread_running = false;
    writer->total_written = 0;
    writer->total_flushes = 0;
    writer->backup_db_path = NULL;
    writer->backup_dest_path = NULL;
    writer->backup_sentinel_path = NULL;
    writer->last_backup_date[0] = '\0';
    writer->ssh_host = NULL;
    writer->ssh_user = NULL;
    writer->ssh_dest_path = NULL;
    writer->ssh_key_path = NULL;
    writer->ssh_bookmark_path = NULL;

    /* Allocate batch array */
    writer->batch = calloc(batch_capacity, sizeof(torrent_metadata_t*));
    if (!writer->batch) {
        log_msg(LOG_ERROR, "Failed to allocate batch array");
        free(writer);
        return NULL;
    }

    /* Initialize mutex */
    if (uv_mutex_init(&writer->mutex) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize batch writer mutex");
        free(writer->batch);
        free(writer);
        return NULL;
    }

    if (uv_cond_init(&writer->backup_done) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize backup_done condition variable");
        uv_mutex_destroy(&writer->mutex);
        free(writer->batch);
        free(writer);
        return NULL;
    }

    if (uv_cond_init(&writer->flush_done) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize flush_done condition variable");
        uv_cond_destroy(&writer->backup_done);
        uv_mutex_destroy(&writer->mutex);
        free(writer->batch);
        free(writer);
        return NULL;
    }

    /* Initialize flush thread synchronization primitives */
    if (pthread_mutex_init(&writer->flush_thread_mutex, NULL) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize flush thread mutex");
        uv_mutex_destroy(&writer->mutex);
        free(writer->batch);
        free(writer);
        return NULL;
    }
    if (pthread_cond_init(&writer->flush_thread_cond, NULL) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize flush thread cond");
        pthread_mutex_destroy(&writer->flush_thread_mutex);
        uv_mutex_destroy(&writer->mutex);
        free(writer->batch);
        free(writer);
        return NULL;
    }

    /* Initialize hourly stats array (zero out) */
    memset(writer->hourly_stats, 0, sizeof(writer->hourly_stats));

    /* Initialize cached counts from database (one-time query at startup) */
    writer->cached_torrent_count = 0;
    writer->cached_file_count = 0;
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db->db, "SELECT COUNT(*) FROM torrents", -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            writer->cached_torrent_count = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    if (sqlite3_prepare_v2(db->db, "SELECT COUNT(*) FROM torrent_files", -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            writer->cached_file_count = sqlite3_column_int64(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
    log_msg(LOG_DEBUG, "Cached counts initialized: %llu torrents, %llu files",
            (unsigned long long)writer->cached_torrent_count,
            (unsigned long long)writer->cached_file_count);

    /* Start periodic flush thread */
    if (flush_interval_sec > 0) {
        writer->flush_thread_running = true;
        if (pthread_create(&writer->flush_thread, NULL, flush_thread_func, writer) != 0) {
            log_msg(LOG_ERROR, "Failed to start flush thread");
            writer->flush_thread_running = false;
            pthread_cond_destroy(&writer->flush_thread_cond);
            pthread_mutex_destroy(&writer->flush_thread_mutex);
            uv_mutex_destroy(&writer->mutex);
            free(writer->batch);
            free(writer);
            return NULL;
        }
    }

    log_msg(LOG_DEBUG, "Batch writer initialized: capacity=%zu, flush_interval=%ds",
            batch_capacity, flush_interval_sec);

    return writer;
}

void batch_writer_set_bloom(batch_writer_t *writer, bloom_filter_t *bloom, const char *bloom_path) {
    if (!writer) {
        return;
    }
    writer->bloom = bloom;
    writer->bloom_path = bloom_path;
    log_msg(LOG_DEBUG, "Bloom filter connected to batch writer for persistence (path: %s)",
            bloom_path ? bloom_path : "(null)");
}

void batch_writer_set_failure_bloom(batch_writer_t *writer,
                                     bloom_filter_t *failure_bloom,
                                     const char *failure_bloom_path) {
    if (!writer) {
        return;
    }
    writer->failure_bloom = failure_bloom;
    writer->failure_bloom_path = failure_bloom_path;
    log_msg(LOG_DEBUG, "Failure bloom filter connected to batch writer for persistence (path: %s)",
            failure_bloom_path ? failure_bloom_path : "(null)");
}

void batch_writer_set_backup(batch_writer_t *writer,
                             const char *db_path,
                             const char *backup_dest) {
    if (!writer || !db_path || !backup_dest) {
        return;
    }
    free(writer->backup_db_path);
    free(writer->backup_dest_path);
    free(writer->backup_sentinel_path);
    writer->backup_db_path   = strdup(db_path);
    writer->backup_dest_path = strdup(backup_dest);

    /* Derive sentinel path: <dir of db_path>/last_backup_date.txt */
    char sentinel[2048];
    snprintf(sentinel, sizeof(sentinel), "%s", db_path);
    char *slash = strrchr(sentinel, '/');
    if (slash) {
        slash[1] = '\0';
        strncat(sentinel, "last_backup_date.txt", sizeof(sentinel) - strlen(sentinel) - 1);
    } else {
        snprintf(sentinel, sizeof(sentinel), "last_backup_date.txt");
    }
    writer->backup_sentinel_path = strdup(sentinel);

    /* Restore persisted backup date so a restart doesn't re-trigger the backup */
    writer->last_backup_date[0] = '\0';
    FILE *sf = fopen(writer->backup_sentinel_path, "r");
    if (sf) {
        char buf[16] = {0};
        if (fgets(buf, sizeof(buf), sf)) {
            size_t len = strlen(buf);
            while (len > 0 && (buf[len-1] == '\n' || buf[len-1] == '\r' || buf[len-1] == ' '))
                buf[--len] = '\0';
            if (len == 8) {
                memcpy(writer->last_backup_date, buf, 9);
                log_msg(LOG_INFO, "Backup sentinel restored: last backup was %s",
                        writer->last_backup_date);
            }
        }
        fclose(sf);
    }

    log_msg(LOG_INFO, "Daily incremental backup configured: %s", backup_dest);
}

void batch_writer_set_ssh_backup(batch_writer_t *writer,
                                  const char *host, const char *user,
                                  const char *dest_path, const char *key_path,
                                  const char *bookmark_path) {
    if (!writer || !host || !user || !dest_path || !bookmark_path) return;
    free(writer->ssh_host);          writer->ssh_host          = strdup(host);
    free(writer->ssh_user);          writer->ssh_user          = strdup(user);
    free(writer->ssh_dest_path);     writer->ssh_dest_path     = strdup(dest_path);
    free(writer->ssh_key_path);
    writer->ssh_key_path = (key_path && key_path[0]) ? strdup(key_path) : NULL;
    free(writer->ssh_bookmark_path); writer->ssh_bookmark_path = strdup(bookmark_path);
    log_msg(LOG_INFO, "SSH incremental backup configured: %s@%s:%s", user, host, dest_path);
}

/* Read last_torrent_id and last_prefix_id from bookmark file.
 * Returns 0 on success, -1 if file absent or unparseable. */
static int read_ssh_bookmark(const char *path,
                              int64_t *out_torrent_id, int64_t *out_prefix_id) {
    FILE *f = fopen(path, "r");
    if (!f) return -1;

    int64_t torrent_id = -1, prefix_id = -1;
    char line[128];
    while (fgets(line, sizeof(line), f)) {
        int64_t val;
        if (sscanf(line, "last_torrent_id=%"SCNd64, &val) == 1)
            torrent_id = val;
        else if (sscanf(line, "last_prefix_id=%"SCNd64, &val) == 1)
            prefix_id = val;
    }
    fclose(f);

    if (torrent_id < 0 || prefix_id < 0) return -1;
    *out_torrent_id = torrent_id;
    *out_prefix_id  = prefix_id;
    return 0;
}

/* Write bookmark atomically via .tmp + rename. Returns 0 on success. */
static int write_ssh_bookmark(const char *path,
                               int64_t torrent_id, int64_t prefix_id) {
    char tmp_path[2048];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    FILE *f = fopen(tmp_path, "w");
    if (!f) return -1;
    fprintf(f, "last_torrent_id=%"PRId64"\n", torrent_id);
    fprintf(f, "last_prefix_id=%"PRId64"\n",  prefix_id);
    fclose(f);
    return rename(tmp_path, path);
}

/* Query MAX(id) from a table on an already-open connection.
 * Returns 0 on success. */
static int query_max_id(sqlite3 *db, const char *table, int64_t *out) {
    char sql[128];
    snprintf(sql, sizeof(sql), "SELECT COALESCE(MAX(id), 0) FROM %s", table);
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) *out = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_ROW) ? 0 : -1;
}

typedef struct {
    char src_db_path[1024];
    char backup_dest_path[1024]; /* local backup DB for first-run init */
    char bookmark_path[1024];
    char ssh_host[256];
    char ssh_user[128];
    char ssh_dest_path[1024];
    char ssh_key_path[512];      /* empty string = no -i flag */
} ssh_backup_args_t;

static void *ssh_backup_thread_func(void *arg) {
    ssh_backup_args_t *args = (ssh_backup_args_t *)arg;

    int64_t last_torrent_id, last_prefix_id;

    if (read_ssh_bookmark(args->bookmark_path,
                          &last_torrent_id, &last_prefix_id) != 0) {
        /* First run: initialise bookmark from local backup DB if present,
         * otherwise from the live DB. Skip today's dump either way. */
        sqlite3 *init_db = NULL;
        const char *init_path = args->backup_dest_path[0]
                                ? args->backup_dest_path
                                : args->src_db_path;
        int64_t t_id = 0, p_id = 0;
        if (sqlite3_open_v2(init_path, &init_db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
            query_max_id(init_db, "torrents",      &t_id);
            query_max_id(init_db, "path_prefixes", &p_id);
            sqlite3_close(init_db);
        }
        write_ssh_bookmark(args->bookmark_path, t_id, p_id);
        log_msg(LOG_INFO, "SSH backup: initialised bookmark at torrent_id=%"PRId64
                ", prefix_id=%"PRId64" (skipping today's dump)", t_id, p_id);
        free(args);
        return NULL;
    }

    /* Open fresh read-only connection to source to get current MAX ids */
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(args->src_db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "SSH backup: failed to open source database");
        if (db) sqlite3_close(db);
        free(args);
        return NULL;
    }

    int64_t max_torrent_id = 0, max_prefix_id = 0;
    query_max_id(db, "torrents",      &max_torrent_id);
    query_max_id(db, "path_prefixes", &max_prefix_id);
    sqlite3_close(db);

    if (max_torrent_id <= last_torrent_id) {
        log_msg(LOG_DEBUG, "SSH backup: no new torrents since last dump (max=%"PRId64
                ", last=%"PRId64")", max_torrent_id, last_torrent_id);
        free(args);
        return NULL;
    }

    /* Build dated filename: incremental_YYYYMMDD_START-END.sql.zst */
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y%m%d", t);
    char filename[256];
    snprintf(filename, sizeof(filename), "incremental_%s_%"PRId64"-%"PRId64".sql.zst",
             date_str, last_torrent_id + 1, max_torrent_id);

    /* Build shell pipeline */
    char key_flag[640] = "";
    if (args->ssh_key_path[0])
        snprintf(key_flag, sizeof(key_flag), "-i '%s' ", args->ssh_key_path);

    char cmd[16384];
    snprintf(cmd, sizeof(cmd),
        "{ "
        "sqlite3 '%s' \".mode insert path_prefixes\" "
            "\"SELECT * FROM path_prefixes WHERE id > %"PRId64" AND id <= %"PRId64";\" ; "
        "sqlite3 '%s' \".mode insert torrents\" "
            "\"SELECT * FROM torrents WHERE id > %"PRId64" AND id <= %"PRId64";\" ; "
        "sqlite3 '%s' \".mode insert torrent_files\" "
            "\"SELECT * FROM torrent_files WHERE torrent_id > %"PRId64" AND torrent_id <= %"PRId64";\" ; "
        "} | sed 's/^INSERT INTO /INSERT OR IGNORE INTO /' "
        "| zstd -T0 "
        "| ssh %s'%s@%s' \"mkdir -p '%s' && cat > '%s/%s'\"",
        args->src_db_path, last_prefix_id,  max_prefix_id,
        args->src_db_path, last_torrent_id, max_torrent_id,
        args->src_db_path, last_torrent_id, max_torrent_id,
        key_flag, args->ssh_user, args->ssh_host,
        args->ssh_dest_path, args->ssh_dest_path, filename);

    log_msg(LOG_INFO, "SSH backup: sending %s (%"PRId64" new torrents)",
            filename, max_torrent_id - last_torrent_id);

    int rc = system(cmd);

    if (rc == 0) {
        write_ssh_bookmark(args->bookmark_path, max_torrent_id, max_prefix_id);
        log_msg(LOG_INFO, "SSH backup complete: %s", filename);
    } else {
        log_msg(LOG_ERROR, "SSH backup failed (exit %d); will retry next trigger", rc);
    }

    free(args);
    return NULL;
}

static void do_ssh_backup(batch_writer_t *writer) {
    if (!writer->ssh_host || !writer->ssh_bookmark_path) return;

    ssh_backup_args_t *args = calloc(1, sizeof(ssh_backup_args_t));
    if (!args) return;

    if (writer->backup_db_path)
        snprintf(args->src_db_path, sizeof(args->src_db_path), "%s", writer->backup_db_path);
    if (writer->backup_dest_path)
        snprintf(args->backup_dest_path, sizeof(args->backup_dest_path), "%s", writer->backup_dest_path);
    snprintf(args->bookmark_path,  sizeof(args->bookmark_path),  "%s", writer->ssh_bookmark_path);
    snprintf(args->ssh_host,       sizeof(args->ssh_host),       "%s", writer->ssh_host);
    snprintf(args->ssh_user,       sizeof(args->ssh_user),       "%s", writer->ssh_user);
    snprintf(args->ssh_dest_path,  sizeof(args->ssh_dest_path),  "%s", writer->ssh_dest_path);
    if (writer->ssh_key_path)
        snprintf(args->ssh_key_path, sizeof(args->ssh_key_path), "%s", writer->ssh_key_path);

    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (pthread_create(&tid, &attr, ssh_backup_thread_func, args) != 0) {
        log_msg(LOG_ERROR, "SSH backup: failed to start thread");
        free(args);
    }
    pthread_attr_destroy(&attr);
}

/* Recursively create directories for path (like mkdir -p). */
static void make_dirs(const char *path) {
    char tmp[2048];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

/* Perform the daily incremental backup using SQLite ATTACH.
 * Opens the backup DB as the primary connection, attaches the live DB as a
 * read-only source, then appends only rows with id > MAX(id) already in the
 * backup.  The backup DB is created with the full schema on the first run so
 * it is immediately usable after a restore with no rebuild step. */
static void do_backup(batch_writer_t *writer) {
    /* Ensure destination directory exists */
    char dir_tmp[2048];
    snprintf(dir_tmp, sizeof(dir_tmp), "%s", writer->backup_dest_path);
    char *slash = strrchr(dir_tmp, '/');
    if (slash) {
        *slash = '\0';
        make_dirs(dir_tmp);
    }

    uv_mutex_lock(&writer->mutex);
    writer->backup_in_progress = true;
    uv_mutex_unlock(&writer->mutex);

    log_msg(LOG_INFO, "Starting incremental backup -> %s", writer->backup_dest_path);

    sqlite3 *bak = NULL;
    int rc = sqlite3_open(writer->backup_dest_path, &bak);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Backup: failed to open backup database: %s",
                bak ? sqlite3_errmsg(bak) : "unknown error");
        if (bak) sqlite3_close(bak);
        goto done_fail;
    }

    /* Create schema on first run — mirrors src/database.c but without FTS
     * delete triggers since the backup is append-only. */
    char *err = NULL;
    rc = sqlite3_exec(bak,
        "CREATE TABLE IF NOT EXISTS torrents ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  info_hash BLOB(20) NOT NULL UNIQUE,"
        "  name TEXT NOT NULL,"
        "  size_bytes INTEGER NOT NULL,"
        "  total_peers INTEGER DEFAULT 0,"
        "  added_timestamp INTEGER NOT NULL);"
        "CREATE INDEX IF NOT EXISTS idx_torrents_added"
        "  ON torrents(added_timestamp DESC);"
        "CREATE TABLE IF NOT EXISTS path_prefixes ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  prefix TEXT NOT NULL UNIQUE);"
        "CREATE TABLE IF NOT EXISTS torrent_files ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  torrent_id INTEGER NOT NULL,"
        "  prefix_id INTEGER,"
        "  filename TEXT NOT NULL,"
        "  size_bytes INTEGER NOT NULL,"
        "  file_index SMALLINT NOT NULL);"
        "CREATE INDEX IF NOT EXISTS idx_files_torrent"
        "  ON torrent_files(torrent_id);"
        "CREATE VIRTUAL TABLE IF NOT EXISTS torrent_search USING fts5("
        "  name, tokenize='porter unicode61',"
        "  content='torrents', content_rowid='id');"
        "CREATE VIRTUAL TABLE IF NOT EXISTS file_search USING fts5("
        "  filename, tokenize='trigram',"
        "  content='torrent_files', content_rowid='id');"
        "CREATE TRIGGER IF NOT EXISTS torrents_ai AFTER INSERT ON torrents BEGIN"
        "  INSERT INTO torrent_search(rowid, name) VALUES (new.id, new.name); END;"
        "CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON torrent_files BEGIN"
        "  INSERT INTO file_search(rowid, filename) VALUES (new.id, new.filename); END;",
        NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Backup: schema creation failed: %s", err);
        sqlite3_free(err);
        sqlite3_close(bak);
        goto done_fail;
    }

    /* Attach the live database as source */
    char sql[4096];
    sqlite3_snprintf(sizeof(sql), sql,
        "ATTACH DATABASE %Q AS src", writer->backup_db_path);
    rc = sqlite3_exec(bak, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Backup: failed to attach source database: %s", err);
        sqlite3_free(err);
        sqlite3_close(bak);
        goto done_fail;
    }

    /* Append only new rows in a single atomic transaction.
     * path_prefixes is small; INSERT OR IGNORE handles the full sync safely. */
    rc = sqlite3_exec(bak,
        "BEGIN;"
        "INSERT OR IGNORE INTO path_prefixes SELECT * FROM src.path_prefixes;"
        "INSERT OR IGNORE INTO torrents"
        "  SELECT * FROM src.torrents"
        "  WHERE id > (SELECT COALESCE(MAX(id), 0) FROM torrents);"
        "INSERT OR IGNORE INTO torrent_files"
        "  SELECT * FROM src.torrent_files"
        "  WHERE id > (SELECT COALESCE(MAX(id), 0) FROM torrent_files);"
        "COMMIT;",
        NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_msg(LOG_ERROR, "Backup: incremental insert failed: %s", err);
        sqlite3_free(err);
        sqlite3_exec(bak, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_exec(bak, "DETACH DATABASE src", NULL, NULL, NULL);
        sqlite3_close(bak);
        goto done_fail;
    }

    sqlite3_exec(bak, "DETACH DATABASE src", NULL, NULL, NULL);
    sqlite3_close(bak);
    log_msg(LOG_INFO, "Incremental backup complete: %s", writer->backup_dest_path);

    /* Persist today's date so a restart won't re-trigger the backup */
    if (writer->backup_sentinel_path) {
        FILE *sf = fopen(writer->backup_sentinel_path, "w");
        if (sf) {
            time_t now = time(NULL);
            struct tm *t = localtime(&now);
            char today[9];
            strftime(today, sizeof(today), "%Y%m%d", t);
            fprintf(sf, "%s\n", today);
            fclose(sf);
        } else {
            log_msg(LOG_WARN, "Backup: failed to write sentinel file: %s",
                    writer->backup_sentinel_path);
        }
    }

    uv_mutex_lock(&writer->mutex);
    writer->backup_in_progress = false;
    uv_cond_broadcast(&writer->backup_done);
    uv_mutex_unlock(&writer->mutex);
    return;

done_fail:
    uv_mutex_lock(&writer->mutex);
    writer->backup_in_progress = false;
    writer->last_backup_date[0] = '\0'; /* Allow retry on next flush */
    uv_cond_broadcast(&writer->backup_done);
    uv_mutex_unlock(&writer->mutex);
}

int batch_writer_add(batch_writer_t *writer, const torrent_metadata_t *metadata) {
    if (!writer || !metadata) {
        return -1;
    }

    uv_mutex_lock(&writer->mutex);

    if (!writer->running) {
        uv_mutex_unlock(&writer->mutex);
        return -1;
    }

    /* Make a deep copy of metadata */
    torrent_metadata_t *copy = malloc(sizeof(torrent_metadata_t));
    if (!copy) {
        uv_mutex_unlock(&writer->mutex);
        return -1;
    }

    memcpy(copy, metadata, sizeof(torrent_metadata_t));

    /* Duplicate strings if present */
    if (metadata->name) {
        copy->name = strdup(metadata->name);
        if (!copy->name) {
            free(copy);
            uv_mutex_unlock(&writer->mutex);
            return -1;
        }
    }

    /* Deep copy file information */
    copy->files = NULL;
    copy->num_files = 0;

    if (metadata->files && metadata->num_files > 0) {
        copy->files = malloc(metadata->num_files * sizeof(file_info_t));
        if (!copy->files) {
            if (copy->name) free(copy->name);
            free(copy);
            uv_mutex_unlock(&writer->mutex);
            return -1;
        }

        copy->num_files = metadata->num_files;

        /* Deep copy each file entry */
        for (int32_t i = 0; i < metadata->num_files; i++) {
            copy->files[i].size_bytes = metadata->files[i].size_bytes;
            copy->files[i].file_index = metadata->files[i].file_index;

            if (metadata->files[i].path) {
                copy->files[i].path = strdup(metadata->files[i].path);
                if (!copy->files[i].path) {
                    /* Cleanup on error */
                    for (int32_t j = 0; j < i; j++) {
                        free(copy->files[j].path);
                    }
                    free(copy->files);
                    if (copy->name) free(copy->name);
                    free(copy);
                    uv_mutex_unlock(&writer->mutex);
                    return -1;
                }
            } else {
                copy->files[i].path = NULL;
            }
        }
    }

    /* Flush until space is available. A single flush attempt is not enough:
     * during a long backup hold, hundreds of threads park on backup_done;
     * when it fires they all wake simultaneously, one does the real flush
     * (batch_size -> 0), but the rest return 0 and still need to add.
     * Without a re-check they would all write past batch[capacity-1]. */
    while (writer->batch_size >= writer->batch_capacity) {
        if (!writer->running) {
            if (copy->name) free(copy->name);
            if (copy->files) {
                for (int32_t j = 0; j < copy->num_files; j++) {
                    if (copy->files[j].path) free(copy->files[j].path);
                }
                free(copy->files);
            }
            free(copy);
            uv_mutex_unlock(&writer->mutex);
            return -1;
        }
        log_msg(LOG_DEBUG, "Batch full (%zu items), flushing before add", writer->batch_size);
        uv_mutex_unlock(&writer->mutex);
        batch_writer_flush(writer);
        uv_mutex_lock(&writer->mutex);
    }

    /* Add to batch - we've verified there's space */
    writer->batch[writer->batch_size++] = copy;

    uv_mutex_unlock(&writer->mutex);

    return 0;
}

int batch_writer_flush(batch_writer_t *writer) {
    if (!writer) {
        return -1;
    }

    uv_mutex_lock(&writer->mutex);

    /* Wait for any in-progress backup to finish before touching the db file */
    while (writer->backup_in_progress) {
        uv_cond_wait(&writer->backup_done, &writer->mutex);
    }

    /* Prevent concurrent flushes - wait for the in-progress one to finish.
     * Returning 0 immediately would cause callers (batch_writer_add) to spin
     * in a tight busy-wait loop, burning all CPU cores. */
    while (writer->flush_in_progress) {
        uv_cond_wait(&writer->flush_done, &writer->mutex);
    }

    if (writer->batch_size == 0) {
        uv_mutex_unlock(&writer->mutex);
        return 0;
    }

    /* Mark flush as in progress */
    writer->flush_in_progress = true;

    size_t count = writer->batch_size;
    torrent_metadata_t **batch_copy = malloc(count * sizeof(torrent_metadata_t*));
    if (!batch_copy) {
        writer->flush_in_progress = false;
        uv_mutex_unlock(&writer->mutex);
        return -1;
    }

    memcpy(batch_copy, writer->batch, count * sizeof(torrent_metadata_t*));
    writer->batch_size = 0;

    uv_mutex_unlock(&writer->mutex);

    /* Use optimized batch insert (single transaction, single mutex lock) */
    size_t actual_written = 0;
    int ret = database_insert_batch(writer->db, batch_copy, count, &actual_written);

    /* Checkpoint WAL after every flush attempt, regardless of whether inserts
     * succeeded.  Rolled-back transactions still write frames to the WAL, so
     * we must checkpoint unconditionally to prevent unbounded growth. */
    database_wal_checkpoint(writer->db);

    /* Use actual DB-reported count, not the batch size assumption */
    size_t written = (ret == 0) ? actual_written : 0;

    /* CRITICAL: Update bloom filter in memory and save to disk after successful batch write
     * This ensures the bloom filter stays synchronized with the database.
     * Steps:
     * 1. Add all successfully written infohashes to bloom filter (in-memory)
     * 2. Save bloom filter to disk
     * If the application crashes, the bloom filter will accurately reflect
     * all info_hashes that were successfully written to the database. */
    if (ret == 0 && written > 0 && writer->bloom) {
        /* Add all infohashes to bloom filter in memory */
        for (size_t i = 0; i < count; i++) {
            bloom_filter_add(writer->bloom, batch_copy[i]->info_hash);
        }

        /* Save bloom filter to disk */
        if (writer->bloom_path) {
            if (bloom_filter_save(writer->bloom, writer->bloom_path) != 0) {
                log_msg(LOG_WARN, "Failed to save bloom filter after batch write");
            }
        }

        /* Save failure bloom filter to disk */
        if (writer->failure_bloom && writer->failure_bloom_path) {
            if (bloom_filter_save(writer->failure_bloom, writer->failure_bloom_path) != 0) {
                log_msg(LOG_WARN, "Failed to save failure bloom filter after batch write");
            }
        }
    }

    /* Count total files in successfully written batch for cached count update
     * Must be done BEFORE freeing batch_copy */
    size_t files_written = 0;
    if (ret == 0 && written > 0) {
        for (size_t i = 0; i < count; i++) {
            files_written += batch_copy[i]->num_files;
        }
    }

    /* Free all batch copies */
    for (size_t i = 0; i < count; i++) {
        /* Free metadata copy */
        if (batch_copy[i]->name) {
            free(batch_copy[i]->name);
        }

        /* Free file copies */
        if (batch_copy[i]->files) {
            for (int32_t j = 0; j < batch_copy[i]->num_files; j++) {
                if (batch_copy[i]->files[j].path) {
                    free(batch_copy[i]->files[j].path);
                }
            }
            free(batch_copy[i]->files);
        }

        free(batch_copy[i]);
    }

    free(batch_copy);

    uv_mutex_lock(&writer->mutex);
    writer->total_written += written;
    writer->total_flushes++;

    /* Update cached counts for fast HTTP API responses */
    if (written > 0) {
        writer->cached_torrent_count += written;
        writer->cached_file_count += files_written;
    }

    /* Update hourly statistics - record torrents written in current minute */
    if (written > 0) {
        time_t current_minute = time(NULL) / 60;
        int bucket = current_minute % 60;

        /* If this is a new minute, reset the bucket; otherwise accumulate */
        if (writer->hourly_stats[bucket].minute != current_minute) {
            writer->hourly_stats[bucket].minute = current_minute;
            writer->hourly_stats[bucket].count = written;
        } else {
            writer->hourly_stats[bucket].count += written;
        }
    }

    /* Check if a daily backup is due: new calendar day, data was written,
     * and the writer is not shutting down. */
    bool should_backup = false;
    if (writer->running && !writer->backup_inhibited && writer->backup_dest_path && written > 0) {
        time_t now = time(NULL);
        struct tm *t = localtime(&now);
        char today[9];
        strftime(today, sizeof(today), "%Y%m%d", t);
        if (strcmp(today, writer->last_backup_date) != 0) {
            memcpy(writer->last_backup_date, today, 9);
            should_backup = true;
        }
    }

    /* Clear flush-in-progress flag and wake any threads waiting for space */
    writer->flush_in_progress = false;
    uv_cond_broadcast(&writer->flush_done);

    uv_mutex_unlock(&writer->mutex);

    if (should_backup) {
        do_backup(writer);
        do_ssh_backup(writer);
    }

    return (ret == 0) ? 0 : -1;
}

void batch_writer_stats(batch_writer_t *writer, size_t *out_batch_size,
                       size_t *out_batch_capacity, uint64_t *out_total_written,
                       uint64_t *out_total_flushes) {
    if (!writer) {
        return;
    }

    uv_mutex_lock(&writer->mutex);

    if (out_batch_size) {
        *out_batch_size = writer->batch_size;
    }
    if (out_batch_capacity) {
        *out_batch_capacity = writer->batch_capacity;
    }
    if (out_total_written) {
        *out_total_written = writer->total_written;
    }
    if (out_total_flushes) {
        *out_total_flushes = writer->total_flushes;
    }

    uv_mutex_unlock(&writer->mutex);
}

size_t batch_writer_get_hourly_count(batch_writer_t *writer) {
    if (!writer) {
        return 0;
    }

    uv_mutex_lock(&writer->mutex);

    time_t current_minute = time(NULL) / 60;
    size_t total_count = 0;

    /* Sum all entries in the 60-minute window that are recent */
    for (int i = 0; i < 60; i++) {
        time_t stat_minute = writer->hourly_stats[i].minute;

        /* Only count if this entry is within the last 60 minutes */
        if (stat_minute > 0 && (current_minute - stat_minute) < 60) {
            total_count += writer->hourly_stats[i].count;
        }
    }

    uv_mutex_unlock(&writer->mutex);

    return total_count;
}

uint64_t batch_writer_get_torrent_count(batch_writer_t *writer) {
    if (!writer) {
        return 0;
    }

    uv_mutex_lock(&writer->mutex);
    uint64_t count = writer->cached_torrent_count;
    uv_mutex_unlock(&writer->mutex);

    return count;
}

uint64_t batch_writer_get_file_count(batch_writer_t *writer) {
    if (!writer) {
        return 0;
    }

    uv_mutex_lock(&writer->mutex);
    uint64_t count = writer->cached_file_count;
    uv_mutex_unlock(&writer->mutex);

    return count;
}

void batch_writer_inhibit_backup(batch_writer_t *writer) {
    if (!writer) return;
    uv_mutex_lock(&writer->mutex);
    writer->backup_inhibited = true;
    uv_mutex_unlock(&writer->mutex);
}

void batch_writer_shutdown(batch_writer_t *writer) {
    if (!writer) {
        return;
    }

    log_msg(LOG_DEBUG, "Batch writer shutting down...");

    /* Stop the periodic flush thread first */
    if (writer->flush_thread_running) {
        pthread_mutex_lock(&writer->flush_thread_mutex);
        writer->flush_thread_running = false;
        pthread_cond_signal(&writer->flush_thread_cond);
        pthread_mutex_unlock(&writer->flush_thread_mutex);
        pthread_join(writer->flush_thread, NULL);
    }

    uv_mutex_lock(&writer->mutex);
    writer->running = false;
    uv_mutex_unlock(&writer->mutex);

    /* Flush any pending writes */
    batch_writer_flush(writer);
}

void batch_writer_cleanup(batch_writer_t *writer) {
    if (!writer) {
        return;
    }

    /* Shutdown if not already done */
    if (writer->running) {
        batch_writer_shutdown(writer);
    }

    /* Free any remaining batch items */
    uv_mutex_lock(&writer->mutex);
    for (size_t i = 0; i < writer->batch_size; i++) {
        if (writer->batch[i]) {
            if (writer->batch[i]->name) {
                free(writer->batch[i]->name);
            }
            if (writer->batch[i]->files) {
                for (int32_t j = 0; j < writer->batch[i]->num_files; j++) {
                    if (writer->batch[i]->files[j].path) {
                        free(writer->batch[i]->files[j].path);
                    }
                }
                free(writer->batch[i]->files);
            }
            free(writer->batch[i]);
        }
    }
    uv_mutex_unlock(&writer->mutex);

    log_msg(LOG_DEBUG, "Batch writer cleaned up (%lu total written, %lu flushes)",
            writer->total_written, writer->total_flushes);

    /* Cleanup */
    pthread_cond_destroy(&writer->flush_thread_cond);
    pthread_mutex_destroy(&writer->flush_thread_mutex);
    uv_cond_destroy(&writer->backup_done);
    uv_cond_destroy(&writer->flush_done);
    uv_mutex_destroy(&writer->mutex);
    free(writer->batch);
    free(writer->backup_db_path);
    free(writer->backup_dest_path);
    free(writer->backup_sentinel_path);
    free(writer->ssh_host);
    free(writer->ssh_user);
    free(writer->ssh_dest_path);
    free(writer->ssh_key_path);
    free(writer->ssh_bookmark_path);
    free(writer);
}
