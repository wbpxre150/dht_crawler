#ifndef BATCH_WRITER_H
#define BATCH_WRITER_H

#include <stddef.h>
#include <stdint.h>
#include <uv.h>
#include "database.h"
#include "bloom_filter.h"

/**
 * Batched database writer for high-throughput torrent insertion
 * 
 * Provides 10-100x improvement in write speed by batching
 * multiple inserts into single transactions.
 */

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
