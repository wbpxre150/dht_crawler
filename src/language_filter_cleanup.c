#include "language_filter_cleanup.h"
#include "language_filter.h"
#include "dht_crawler.h"
#include "database.h"
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <sqlite3.h>

#define DELETE_BATCH_SIZE 5000

static volatile sig_atomic_t cleanup_interrupted = 0;

static void cleanup_sigint_handler(int sig) {
    (void)sig;
    cleanup_interrupted = 1;
}

/* ---------- Backup helper ---------- */

static int copy_file(const char *src, const char *dst) {
    int sfd = open(src, O_RDONLY);
    if (sfd < 0) {
        log_msg(LOG_ERROR, "open(%s) failed: %s", src, strerror(errno));
        return -1;
    }
    struct stat st;
    if (fstat(sfd, &st) != 0) {
        log_msg(LOG_ERROR, "fstat(%s) failed: %s", src, strerror(errno));
        close(sfd);
        return -1;
    }
    int dfd = open(dst, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (dfd < 0) {
        log_msg(LOG_ERROR, "open(%s) failed: %s", dst, strerror(errno));
        close(sfd);
        return -1;
    }

    char buf[1 << 16];
    ssize_t n;
    off_t total = 0;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) {
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = write(dfd, buf + off, (size_t)(n - off));
            if (w < 0) {
                if (errno == EINTR) continue;
                log_msg(LOG_ERROR, "write(%s) failed: %s", dst, strerror(errno));
                close(sfd); close(dfd); unlink(dst);
                return -1;
            }
            off += w;
        }
        total += n;
    }
    if (n < 0) {
        log_msg(LOG_ERROR, "read(%s) failed: %s", src, strerror(errno));
        close(sfd); close(dfd); unlink(dst);
        return -1;
    }

    if (fsync(dfd) != 0) {
        log_msg(LOG_WARN, "fsync(%s) failed: %s", dst, strerror(errno));
    }
    close(sfd);
    close(dfd);

    struct stat dst_st;
    if (stat(dst, &dst_st) != 0 || dst_st.st_size != st.st_size) {
        log_msg(LOG_ERROR, "Backup size mismatch (src=%lld dst=%lld)",
                (long long)st.st_size, (long long)dst_st.st_size);
        unlink(dst);
        return -1;
    }
    log_msg(LOG_INFO, "Backup completed: %s -> %s (%lld bytes)",
            src, dst, (long long)total);
    return 0;
}

static int read_line_trim(char *buf, size_t buflen) {
    if (!fgets(buf, (int)buflen, stdin)) return -1;
    size_t n = strlen(buf);
    while (n > 0 && (buf[n-1] == '\n' || buf[n-1] == '\r')) buf[--n] = '\0';
    return 0;
}

static int prompt_backup(const char *db_path) {
    printf("Back up %s before scanning? [Y/n]: ", db_path);
    fflush(stdout);
    char ans[64];
    if (read_line_trim(ans, sizeof(ans)) != 0) return -1;
    if (ans[0] == 'n' || ans[0] == 'N') {
        printf("Skip backup? Type 'yes' to confirm: ");
        fflush(stdout);
        char confirm[64];
        if (read_line_trim(confirm, sizeof(confirm)) != 0) return -1;
        if (strcmp(confirm, "yes") != 0) {
            printf("Aborted.\n");
            return -1;
        }
        return 0;
    }

    char dst[1024];
    printf("Backup destination [data/torrents-backup.db]: ");
    fflush(stdout);
    if (read_line_trim(dst, sizeof(dst)) != 0) return -1;
    if (dst[0] == '\0') {
        snprintf(dst, sizeof(dst), "data/torrents-backup.db");
    }
    return copy_file(db_path, dst);
}

/* ---------- SQLite helpers ---------- */

static int exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        log_msg(LOG_ERROR, "SQL failed (%s): %s", sql, err ? err : "?");
        sqlite3_free(err);
        return -1;
    }
    return 0;
}

static int apply_session_pragmas(sqlite3 *db) {
    if (exec_sql(db, "PRAGMA cache_size = -1048576;") != 0) return -1;
    if (exec_sql(db, "PRAGMA temp_store = MEMORY;") != 0) return -1;
    if (exec_sql(db, "PRAGMA mmap_size = 2147483648;") != 0) return -1;
    return 0;
}

/* ---------- Row loader ---------- */

static int load_metadata_for_id(sqlite3 *db, int64_t id, torrent_metadata_t *out) {
    memset(out, 0, sizeof(*out));

    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
            "SELECT info_hash, name FROM torrents WHERE id = ?",
            -1, &st, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int64(st, 1, id);
    int rc = sqlite3_step(st);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(st);
        return -1;
    }
    const void *blob = sqlite3_column_blob(st, 0);
    int blen = sqlite3_column_bytes(st, 0);
    if (blob && blen == SHA1_DIGEST_LENGTH) {
        memcpy(out->info_hash, blob, SHA1_DIGEST_LENGTH);
    }
    const unsigned char *nm = sqlite3_column_text(st, 1);
    out->name = nm ? strdup((const char *)nm) : NULL;
    sqlite3_finalize(st);

    /* Load file paths from torrent_files for filename-based language checks */
    sqlite3_stmt *fst = NULL;
    if (sqlite3_prepare_v2(db,
            "SELECT COALESCE(pp.prefix || '/' || tf.filename, tf.filename)"
            " FROM torrent_files tf"
            " LEFT JOIN path_prefixes pp ON tf.prefix_id = pp.id"
            " WHERE tf.torrent_id = ?",
            -1, &fst, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(fst, 1, id);

        int capacity = 0;
        while (sqlite3_step(fst) == SQLITE_ROW) {
            const unsigned char *path = sqlite3_column_text(fst, 0);
            if (!path) continue;
            if (out->num_files >= capacity) {
                int new_cap = capacity == 0 ? 16 : capacity * 2;
                file_info_t *tmp = realloc(out->files, new_cap * sizeof(file_info_t));
                if (!tmp) break;
                out->files = tmp;
                capacity = new_cap;
            }
            out->files[out->num_files].path = strdup((const char *)path);
            out->files[out->num_files].size_bytes = 0;
            out->files[out->num_files].file_index = out->num_files;
            out->num_files++;
        }
        sqlite3_finalize(fst);
    }

    return 0;
}

static void free_metadata(torrent_metadata_t *m) {
    if (!m) return;
    free(m->name);
    if (m->files) {
        for (int i = 0; i < m->num_files; i++)
            free(m->files[i].path);
        free(m->files);
    }
    memset(m, 0, sizeof(*m));
}

/* ---------- Main entry ---------- */

int language_filter_cleanup_run(database_t *db_handle, const char *db_path) {
    if (!db_handle || !db_handle->db) {
        log_msg(LOG_ERROR, "language cleanup: invalid database handle");
        return 1;
    }
    sqlite3 *db = db_handle->db;

    /* Backup prompt */
    if (prompt_backup(db_path) != 0) {
        log_msg(LOG_ERROR, "language cleanup: aborted before any DB modification");
        return 1;
    }

    /* SIGINT handler — flush current batch and exit cleanly */
    struct sigaction sa = {0}, sa_old;
    sa.sa_handler = cleanup_sigint_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, &sa_old);

    if (apply_session_pragmas(db) != 0) {
        sigaction(SIGINT, &sa_old, NULL);
        return 1;
    }

    /* Total count for progress */
    int64_t total = 0;
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM torrents", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    log_msg(LOG_INFO, "language cleanup: torrents in DB = %lld", (long long)total);

    /* ---------------------------------------------------------------
     * Language filter pass: scan ALL rows.
     * --------------------------------------------------------------- */
    int64_t lang_deleted = 0;
    time_t t_start = time(NULL);
    {
        sqlite3_stmt *lang_iter = NULL;
        sqlite3_stmt *lang_del_ins = NULL;

        if (exec_sql(db, "CREATE TEMP TABLE lang_del(id INTEGER PRIMARY KEY);") == 0 &&
            sqlite3_prepare_v2(db, "SELECT id FROM torrents", -1, &lang_iter, NULL) == SQLITE_OK &&
            sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO lang_del(id) VALUES (?)",
                               -1, &lang_del_ins, NULL) == SQLITE_OK) {

            log_msg(LOG_INFO, "language cleanup: starting pass over all %lld rows...", (long long)total);

            if (exec_sql(db, "BEGIN;") == 0) {
                int64_t lang_scanned = 0;
                int lang_batch = 0;

                while (!cleanup_interrupted && sqlite3_step(lang_iter) == SQLITE_ROW) {
                    int64_t id = sqlite3_column_int64(lang_iter, 0);
                    torrent_metadata_t meta;
                    if (load_metadata_for_id(db, id, &meta) != 0) {
                        lang_scanned++;
                        continue;
                    }

                    if (language_filter_check(&meta)) {
                        printf("  [lang] removing: %s\n", meta.name ? meta.name : "(no name)");
                        fflush(stdout);
                        sqlite3_reset(lang_del_ins);
                        sqlite3_bind_int64(lang_del_ins, 1, id);
                        if (sqlite3_step(lang_del_ins) == SQLITE_DONE) {
                            lang_batch++;
                        }
                    }
                    free_metadata(&meta);
                    lang_scanned++;

                    if (lang_batch >= DELETE_BATCH_SIZE) {
                        exec_sql(db, "DELETE FROM torrents WHERE id IN (SELECT id FROM lang_del);");
                        exec_sql(db, "DELETE FROM lang_del;");
                        lang_deleted += lang_batch;
                        lang_batch = 0;
                        log_msg(LOG_INFO, "language cleanup: scanned=%lld deleted=%lld",
                                (long long)lang_scanned, (long long)lang_deleted);
                    } else if (lang_scanned % 10000 == 0) {
                        log_msg(LOG_INFO, "language cleanup: scanned=%lld deleted=%lld (pending=%d)",
                                (long long)lang_scanned, (long long)lang_deleted, lang_batch);
                    }
                }

                if (lang_batch > 0) {
                    exec_sql(db, "DELETE FROM torrents WHERE id IN (SELECT id FROM lang_del);");
                    exec_sql(db, "DELETE FROM lang_del;");
                    lang_deleted += lang_batch;
                }

                exec_sql(db, "COMMIT;");
                log_msg(LOG_INFO, "language cleanup: DONE scanned=%lld deleted=%lld",
                        (long long)lang_scanned, (long long)lang_deleted);
            }
        } else {
            log_msg(LOG_ERROR, "language cleanup: failed to prepare the scan");
        }

        if (lang_iter) sqlite3_finalize(lang_iter);
        if (lang_del_ins) sqlite3_finalize(lang_del_ins);
        sqlite3_exec(db, "DROP TABLE IF EXISTS temp.lang_del;", NULL, NULL, NULL);
    }

    /* Truncate WAL */
    sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE);", NULL, NULL, NULL);

    long elapsed = (long)(time(NULL) - t_start);
    log_msg(LOG_INFO, "language cleanup: DONE total=%lld lang_deleted=%lld elapsed=%lds",
            (long long)total, (long long)lang_deleted, elapsed);
    printf("\nSummary: lang_deleted=%lld elapsed=%lds\n", (long long)lang_deleted, elapsed);

    sigaction(SIGINT, &sa_old, NULL);
    return 0;
}
