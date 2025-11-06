#include "batch_writer.h"
#include "dht_crawler.h"
#include "bloom_filter.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

struct batch_writer {
    database_t *db;
    bloom_filter_t *bloom;  /* For persisting bloom filter after batch writes */
    const char *bloom_path; /* Path to save bloom filter */

    torrent_metadata_t **batch;
    size_t batch_size;
    size_t batch_capacity;

    uv_timer_t flush_timer;
    int flush_interval_sec;

    uv_mutex_t mutex;
    bool running;

    uint64_t total_written;
    uint64_t total_flushes;
};

/* Timer callback for periodic flush */
static void flush_timer_cb(uv_timer_t *timer) {
    batch_writer_t *writer = (batch_writer_t*)timer->data;
    
    uv_mutex_lock(&writer->mutex);
    
    if (writer->batch_size > 0) {
        log_msg(LOG_DEBUG, "Auto-flushing batch writer (%zu items)", writer->batch_size);
        
        /* Unlock during flush to allow other operations */
        uv_mutex_unlock(&writer->mutex);
        batch_writer_flush(writer);
    } else {
        uv_mutex_unlock(&writer->mutex);
    }
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
    writer->batch_capacity = batch_capacity;
    writer->batch_size = 0;
    writer->flush_interval_sec = flush_interval_sec;
    writer->running = true;
    writer->total_written = 0;
    writer->total_flushes = 0;
    
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
    
    /* Initialize flush timer */
    if (uv_timer_init(loop, &writer->flush_timer) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize flush timer");
        uv_mutex_destroy(&writer->mutex);
        free(writer->batch);
        free(writer);
        return NULL;
    }
    
    writer->flush_timer.data = writer;
    
    /* Start periodic flush timer */
    if (flush_interval_sec > 0) {
        uint64_t interval_ms = flush_interval_sec * 1000;
        uv_timer_start(&writer->flush_timer, flush_timer_cb, interval_ms, interval_ms);
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

    /* Add to batch */
    writer->batch[writer->batch_size++] = copy;

    /* Auto-flush if batch is full */
    bool should_flush = (writer->batch_size >= writer->batch_capacity);

    uv_mutex_unlock(&writer->mutex);

    if (should_flush) {
        log_msg(LOG_DEBUG, "Batch full, flushing %zu items", writer->batch_capacity);
        return batch_writer_flush(writer);
    }

    return 0;
}

int batch_writer_flush(batch_writer_t *writer) {
    if (!writer) {
        return -1;
    }
    
    uv_mutex_lock(&writer->mutex);
    
    if (writer->batch_size == 0) {
        uv_mutex_unlock(&writer->mutex);
        return 0;
    }
    
    size_t count = writer->batch_size;
    torrent_metadata_t **batch_copy = malloc(count * sizeof(torrent_metadata_t*));
    if (!batch_copy) {
        uv_mutex_unlock(&writer->mutex);
        return -1;
    }
    
    memcpy(batch_copy, writer->batch, count * sizeof(torrent_metadata_t*));
    writer->batch_size = 0;
    
    uv_mutex_unlock(&writer->mutex);

    /* Use optimized batch insert (single transaction, single mutex lock) */
    int ret = database_insert_batch(writer->db, batch_copy, count);

    /* Count successes (database_insert_batch handles logging) */
    size_t written = (ret == 0) ? count : 0;

    /* CRITICAL: Save bloom filter to disk after successful batch write
     * This ensures the bloom filter on disk stays synchronized with the database.
     * If the application crashes, the bloom filter will accurately reflect
     * all info_hashes that were successfully written to the database. */
    if (ret == 0 && written > 0 && writer->bloom && writer->bloom_path) {
        if (bloom_filter_save(writer->bloom, writer->bloom_path) != 0) {
            log_msg(LOG_WARN, "Failed to save bloom filter after batch write");
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
    size_t total = writer->total_written;
    uv_mutex_unlock(&writer->mutex);

    log_msg(LOG_INFO, "Batch flush: %zu items written (total: %lu)", written, total);

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

void batch_writer_shutdown(batch_writer_t *writer) {
    if (!writer) {
        return;
    }
    
    log_msg(LOG_DEBUG, "Batch writer shutting down...");
    
    uv_mutex_lock(&writer->mutex);
    writer->running = false;
    uv_mutex_unlock(&writer->mutex);
    
    /* Stop flush timer */
    uv_timer_stop(&writer->flush_timer);
    
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
    
    /* Close timer handle */
    uv_close((uv_handle_t*)&writer->flush_timer, NULL);
    
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
    uv_mutex_destroy(&writer->mutex);
    free(writer->batch);
    free(writer);
}
