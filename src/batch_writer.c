#include "batch_writer.h"
#include "dht_crawler.h"
#include "bloom_filter.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>

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
    bool running;
    bool flush_in_progress;  /* Prevents concurrent flush operations */

    uint64_t total_written;
    uint64_t total_flushes;

    /* Cached database counts for fast HTTP API responses */
    uint64_t cached_torrent_count;
    uint64_t cached_file_count;

    /* Rolling 60-minute window for hourly statistics */
    minute_stat_t hourly_stats[60];

    /* Daily backup */
    char *backup_db_path;    /* Source database file path */
    char *backup_path_tmpl;  /* Destination template; %DATE% -> YYYY-MM-DD */
    int   last_backup_yday;  /* tm_yday of last successful backup; -1 = never */
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
    writer->flush_thread_running = false;
    writer->total_written = 0;
    writer->total_flushes = 0;
    writer->backup_db_path = NULL;
    writer->backup_path_tmpl = NULL;
    writer->last_backup_yday = -1;

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
                             const char *backup_path_tmpl) {
    if (!writer || !db_path || !backup_path_tmpl) {
        return;
    }
    free(writer->backup_db_path);
    free(writer->backup_path_tmpl);
    writer->backup_db_path   = strdup(db_path);
    writer->backup_path_tmpl = strdup(backup_path_tmpl);
    log_msg(LOG_INFO, "Daily backup configured: %s", backup_path_tmpl);
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

/* Delete backup files in dir whose date suffix is older than 2 days ago. */
static void prune_old_backups(const char *tmpl) {
    /* Extract directory from template */
    const char *last_slash = strrchr(tmpl, '/');
    if (!last_slash) {
        return;
    }

    char dir[1024];
    size_t dir_len = last_slash - tmpl;
    if (dir_len >= sizeof(dir)) {
        return;
    }
    memcpy(dir, tmpl, dir_len);
    dir[dir_len] = '\0';

    /* Extract filename prefix (between last '/' and '%DATE%') */
    const char *fname_start = last_slash + 1;
    const char *date_marker = strstr(fname_start, "%DATE%");
    size_t prefix_len = date_marker ? (size_t)(date_marker - fname_start) : strlen(fname_start);

    /* Cutoff: anything with a date before 2 days ago gets deleted */
    time_t cutoff = time(NULL) - 2 * 86400;

    DIR *dp = opendir(dir);
    if (!dp) {
        return;
    }

    struct dirent *ent;
    while ((ent = readdir(dp)) != NULL) {
        /* Must start with the filename prefix */
        if (strncmp(ent->d_name, fname_start, prefix_len) != 0) {
            continue;
        }

        /* Parse YYYY-MM-DD date suffix after the prefix */
        const char *date_part = ent->d_name + prefix_len;
        int year = 0, month = 0, day = 0;
        if (sscanf(date_part, "%4d-%2d-%2d", &year, &month, &day) != 3) {
            continue;
        }

        struct tm file_tm = {0};
        file_tm.tm_year = year - 1900;
        file_tm.tm_mon  = month - 1;
        file_tm.tm_mday = day;
        time_t file_time = mktime(&file_tm);
        if (file_time < cutoff) {
            char full_path[2048];
            snprintf(full_path, sizeof(full_path), "%s/%s", dir, ent->d_name);
            if (unlink(full_path) == 0) {
                log_msg(LOG_INFO, "Pruned old backup: %s", full_path);
            } else {
                log_msg(LOG_WARN, "Failed to prune old backup %s: %s",
                        full_path, strerror(errno));
            }
        }
    }

    closedir(dp);
}

/* Perform the daily VACUUM INTO backup and prune old backups.
 * Called from batch_writer_flush() outside the mutex. */
static void do_backup(batch_writer_t *writer) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);

    /* Build destination path: replace %DATE% with YYYY-MM-DD */
    char date_str[16];
    strftime(date_str, sizeof(date_str), "%Y-%m-%d", t);

    char dest[2048];
    const char *tmpl = writer->backup_path_tmpl;
    const char *placeholder = strstr(tmpl, "%DATE%");
    if (!placeholder) {
        snprintf(dest, sizeof(dest), "%s", tmpl);
    } else {
        snprintf(dest, sizeof(dest), "%.*s%s%s",
                 (int)(placeholder - tmpl), tmpl,
                 date_str, placeholder + 6);
    }

    /* Ensure destination directory exists */
    char dir_tmp[2048];
    snprintf(dir_tmp, sizeof(dir_tmp), "%s", dest);
    char *last_slash = strrchr(dir_tmp, '/');
    if (last_slash) {
        *last_slash = '\0';
        make_dirs(dir_tmp);
    }

    /* Remove stale file at dest so VACUUM INTO starts clean */
    remove(dest);

    log_msg(LOG_INFO, "Starting daily database backup -> %s", dest);
    int rc = database_vacuum_into(writer->db, dest);
    if (rc != 0) {
        log_msg(LOG_ERROR, "Daily backup failed for %s; will retry next flush", dest);
        /* Reset yday so the next flush retries */
        uv_mutex_lock(&writer->mutex);
        writer->last_backup_yday = -1;
        uv_mutex_unlock(&writer->mutex);
        return;
    }

    log_msg(LOG_INFO, "Daily backup complete: %s", dest);
    prune_old_backups(writer->backup_path_tmpl);
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

    /* Check if batch is full BEFORE writing - prevent buffer overflow */
    if (writer->batch_size >= writer->batch_capacity) {
        /* Batch is full, need to flush first */
        log_msg(LOG_DEBUG, "Batch full, flushing %zu items before adding new item", writer->batch_capacity);
        uv_mutex_unlock(&writer->mutex);

        int flush_result = batch_writer_flush(writer);

        /* Re-acquire lock and add the item after flush */
        uv_mutex_lock(&writer->mutex);

        if (!writer->running) {
            /* Writer was shut down during flush, cleanup and fail */
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

        /* Add to batch after flush (batch_size should now be < capacity) */
        writer->batch[writer->batch_size++] = copy;
        uv_mutex_unlock(&writer->mutex);

        return flush_result;
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

    /* Prevent concurrent flushes - if one is already in progress, skip this one */
    if (writer->flush_in_progress) {
        uv_mutex_unlock(&writer->mutex);
        log_msg(LOG_DEBUG, "Skipping flush - another flush already in progress");
        return 0;
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

    /* Check if a daily backup is due: new calendar day and data was written */
    bool should_backup = false;
    if (writer->backup_path_tmpl && written > 0) {
        time_t now = time(NULL);
        struct tm *t = localtime(&now);
        if (t->tm_yday != writer->last_backup_yday) {
            writer->last_backup_yday = t->tm_yday;
            should_backup = true;
        }
    }

    /* Clear flush-in-progress flag */
    writer->flush_in_progress = false;

    uv_mutex_unlock(&writer->mutex);

    if (should_backup) {
        do_backup(writer);
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
    uv_mutex_destroy(&writer->mutex);
    free(writer->batch);
    free(writer->backup_db_path);
    free(writer->backup_path_tmpl);
    free(writer);
}
