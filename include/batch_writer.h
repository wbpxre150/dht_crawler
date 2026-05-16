#ifndef BATCH_WRITER_H
#define BATCH_WRITER_H

#include <stddef.h>
#include <stdint.h>
#include <uv.h>
#include <time.h>
#include "database.h"
#include "bloom_filter.h"

/**
 * Batched database writer for high-throughput torrent insertion
 *
 * Provides 10-100x improvement in write speed by batching
 * multiple inserts into single transactions.
 */

/**
 * Per-minute statistics for rolling hourly count
 */
typedef struct {
    time_t minute;        /* Unix timestamp / 60 */
    size_t count;         /* Torrents written in this minute */
} minute_stat_t;

typedef struct batch_writer batch_writer_t;

/**
 * Initialize batch writer
 * @param db Database handle
 * @param batch_capacity Maximum batch size before auto-flush
 * @param flush_interval_sec Automatic flush interval in seconds
 * @param loop Event loop for timer
 * @return Pointer to batch writer, or NULL on error
 */
batch_writer_t* batch_writer_init(database_t *db, size_t batch_capacity,
                                  int flush_interval_sec, uv_loop_t *loop);

/**
 * Set bloom filter for persistence after batch writes
 * @param writer Batch writer instance
 * @param bloom Bloom filter instance
 * @param bloom_path Path to save bloom filter to disk
 */
void batch_writer_set_bloom(batch_writer_t *writer, bloom_filter_t *bloom, const char *bloom_path);

/**
 * Set failure bloom filter for persistence after batch writes
 * @param writer Batch writer instance
 * @param failure_bloom Failure bloom filter instance
 * @param failure_bloom_path Path to save failure bloom filter to disk
 */
void batch_writer_set_failure_bloom(batch_writer_t *writer,
                                     bloom_filter_t *failure_bloom,
                                     const char *failure_bloom_path);

/**
 * Configure daily incremental backup (triggered on first flush after midnight each day).
 * Appends only rows not yet present in the backup database; creates it on first run.
 * @param writer Batch writer instance
 * @param db_path Source database file path
 * @param backup_dest Destination backup database file path
 */
void batch_writer_set_backup(batch_writer_t *writer,
                             const char *db_path,
                             const char *backup_dest);

/**
 * Configure SSH incremental backup (runs in detached thread; batch writer is never blocked).
 * Each day's new rows are dumped as INSERT OR IGNORE SQL, compressed with zstd, and streamed
 * to a remote server. Files are named incremental_YYYYMMDD_START-END.sql.zst.
 * @param writer        Batch writer instance
 * @param host          SSH hostname or IP
 * @param user          SSH username
 * @param dest_path     Remote directory path for dump files
 * @param key_path      Path to SSH private key (NULL or empty = use SSH default)
 * @param bookmark_path Local path for watermark file tracking last sent row IDs
 */
void batch_writer_set_ssh_backup(batch_writer_t *writer,
                                  const char *host, const char *user,
                                  const char *dest_path, const char *key_path,
                                  const char *bookmark_path);

/**
 * Configure rclone incremental backup (runs in detached thread; batch writer is never blocked).
 * Each day's new rows are dumped as INSERT OR IGNORE SQL, compressed with zstd, and streamed
 * via rclone rcat to the configured remote. Files are named incremental_YYYYMMDD_START-END.sql.zst.
 * @param writer        Batch writer instance
 * @param remote        rclone remote name (e.g. "r2")
 * @param dest_path     Destination path within the remote (e.g. "dht-backup/dht_crawler_backup")
 * @param bookmark_path Local path for watermark file tracking last sent row IDs
 */
void batch_writer_set_rclone_backup(batch_writer_t *writer,
                                     const char *remote,
                                     const char *dest_path,
                                     const char *bookmark_path);

/**
 * Add torrent metadata to batch
 * @param writer Batch writer instance
 * @param metadata Torrent metadata to write
 * @return 0 on success, -1 on error
 */
int batch_writer_add(batch_writer_t *writer, const torrent_metadata_t *metadata);

/**
 * Flush pending writes to database
 * @param writer Batch writer instance
 * @return 0 on success, -1 on error
 */
int batch_writer_flush(batch_writer_t *writer);

/**
 * Get batch writer statistics
 * @param writer Batch writer instance
 * @param out_batch_size Output: current batch size
 * @param out_batch_capacity Output: maximum batch capacity
 * @param out_total_written Output: total torrents written
 * @param out_total_flushes Output: total flush operations
 */
void batch_writer_stats(batch_writer_t *writer, size_t *out_batch_size,
                       size_t *out_batch_capacity, uint64_t *out_total_written,
                       uint64_t *out_total_flushes);

/**
 * Get torrents discovered in the last hour
 * @param writer Batch writer instance
 * @return Number of torrents written in the last 60 minutes
 */
size_t batch_writer_get_hourly_count(batch_writer_t *writer);

/**
 * Get cached torrent count (for fast HTTP API responses)
 * @param writer Batch writer instance
 * @return Total number of torrents in database (cached, updated on batch writes)
 */
uint64_t batch_writer_get_torrent_count(batch_writer_t *writer);

/**
 * Get cached file count (for fast HTTP API responses)
 * @param writer Batch writer instance
 * @return Total number of files in database (cached, updated on batch writes)
 */
uint64_t batch_writer_get_file_count(batch_writer_t *writer);

/**
 * Prevent any new daily backup from starting (call before supervisor shutdown)
 * @param writer Batch writer instance
 */
void batch_writer_inhibit_backup(batch_writer_t *writer);

/**
 * Shutdown batch writer (flush pending writes)
 * @param writer Batch writer instance
 */
void batch_writer_shutdown(batch_writer_t *writer);

/**
 * Cleanup and free batch writer
 * @param writer Batch writer instance
 */
void batch_writer_cleanup(batch_writer_t *writer);

#endif /* BATCH_WRITER_H */
