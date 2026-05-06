#include "porn_filter_cleanup.h"
#include "porn_filter.h"
#include "dht_crawler.h"
#include "database.h"
#include <ctype.h>
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
#define KW_CHUNK 100

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

/* Build keyword temp table populated as '%' || lower(?) || '%' patterns. */
static int build_keyword_table(sqlite3 *db) {
    if (exec_sql(db, "CREATE TEMP TABLE pf_kw(pat TEXT PRIMARY KEY) WITHOUT ROWID;") != 0) {
        return -1;
    }

    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db,
            "INSERT INTO pf_kw(pat) VALUES ('%' || lower(?) || '%')",
            -1, &st, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "prepare kw insert: %s", sqlite3_errmsg(db));
        return -1;
    }
    int count = porn_filter_get_keyword_count();
    for (int i = 0; i < count; i++) {
        const char *kw = porn_filter_get_keyword(i);
        if (!kw || !*kw) continue;
        sqlite3_reset(st);
        sqlite3_bind_text(st, 1, kw, -1, SQLITE_STATIC);
        if (sqlite3_step(st) != SQLITE_DONE) {
            log_msg(LOG_ERROR, "insert kw failed: %s", sqlite3_errmsg(db));
            sqlite3_finalize(st);
            return -1;
        }
    }
    sqlite3_finalize(st);
    return 0;
}

/* ---------- Confirm + delete ---------- */

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
    return 0;
}

static void free_metadata(torrent_metadata_t *m) {
    if (!m) return;
    free(m->name);
    memset(m, 0, sizeof(*m));
}

static int flush_delete_batch(sqlite3 *db) {
    if (exec_sql(db,
            "DELETE FROM torrents WHERE id IN (SELECT id FROM pf_del);") != 0) {
        return -1;
    }
    if (exec_sql(db, "DELETE FROM pf_del;") != 0) return -1;
    return 0;
}

/* ---------- Main entry ---------- */

int porn_filter_cleanup_run(database_t *db_handle, const char *db_path) {
    if (!db_handle || !db_handle->db) {
        log_msg(LOG_ERROR, "cleanup: invalid database handle");
        return 1;
    }
    sqlite3 *db = db_handle->db;

    if (porn_filter_get_keyword_count() < 0) {
        log_msg(LOG_ERROR, "cleanup: porn filter not initialized");
        return 1;
    }

    /* Backup prompt */
    if (prompt_backup(db_path) != 0) {
        log_msg(LOG_ERROR, "cleanup: aborted before any DB modification");
        return 1;
    }

    /* SIGINT handler — flush current batch and exit cleanly */
    struct sigaction sa = {0}, sa_old;
    sa.sa_handler = cleanup_sigint_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, &sa_old);

    if (apply_session_pragmas(db) != 0) return 1;

    /* Total count for progress */
    int64_t total = 0;
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM torrents", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) total = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    log_msg(LOG_INFO, "cleanup: torrents in DB = %lld", (long long)total);

    int n_kw = porn_filter_get_keyword_count();
    log_msg(LOG_INFO, "cleanup: loaded %d keywords", n_kw);

    /* Candidate temp table */
    if (exec_sql(db, "CREATE TEMP TABLE pf_cand(id INTEGER PRIMARY KEY);") != 0) return 1;
    if (exec_sql(db, "CREATE TEMP TABLE pf_del(id INTEGER PRIMARY KEY);") != 0) return 1;

    if (n_kw == 0) {
        printf("WARNING: no keywords loaded. Prefilter cannot reduce candidate set.\n");
        printf("This will run regex+heuristics against EVERY row in the DB and may take hours.\n");
        printf("Type 'yes' to proceed: ");
        fflush(stdout);
        char ans[64];
        if (read_line_trim(ans, sizeof(ans)) != 0 || strcmp(ans, "yes") != 0) {
            printf("Aborted.\n");
            sigaction(SIGINT, &sa_old, NULL);
            return 1;
        }
        if (exec_sql(db, "INSERT INTO pf_cand(id) SELECT id FROM torrents;") != 0) return 1;
    } else {
        if (build_keyword_table(db) != 0) return 1;

        log_msg(LOG_INFO, "cleanup: scanning torrents.name for keyword hits...");
        time_t t0 = time(NULL);
        if (exec_sql(db,
                "INSERT OR IGNORE INTO pf_cand(id) "
                "SELECT t.id FROM torrents t, pf_kw "
                "WHERE lower(t.name) LIKE pf_kw.pat;") != 0) return 1;
        log_msg(LOG_INFO, "cleanup: name pass done in %lds", (long)(time(NULL) - t0));
    }

    int64_t cand_count = 0;
    {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM pf_cand", -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) cand_count = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    log_msg(LOG_INFO, "cleanup: %lld candidate ids -> running confirm pass", (long long)cand_count);

    /* Single transaction wrapping all deletes */
    if (exec_sql(db, "BEGIN;") != 0) return 1;

    sqlite3_stmt *iter = NULL;
    if (sqlite3_prepare_v2(db, "SELECT id FROM pf_cand", -1, &iter, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "prepare cand iter: %s", sqlite3_errmsg(db));
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return 1;
    }

    sqlite3_stmt *del_ins = NULL;
    if (sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO pf_del(id) VALUES (?)",
            -1, &del_ins, NULL) != SQLITE_OK) {
        log_msg(LOG_ERROR, "prepare pf_del insert: %s", sqlite3_errmsg(db));
        sqlite3_finalize(iter);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        return 1;
    }

    int64_t scanned = 0;
    int64_t deleted = 0;
    int batch_n = 0;
    time_t t_start = time(NULL);

    while (sqlite3_step(iter) == SQLITE_ROW) {
        if (cleanup_interrupted) {
            log_msg(LOG_WARN, "cleanup: SIGINT received, flushing and exiting");
            break;
        }
        int64_t id = sqlite3_column_int64(iter, 0);

        torrent_metadata_t meta;
        if (load_metadata_for_id(db, id, &meta) != 0) {
            scanned++;
            continue;
        }

        if (porn_filter_check(&meta)) {
            sqlite3_reset(del_ins);
            sqlite3_bind_int64(del_ins, 1, id);
            if (sqlite3_step(del_ins) != SQLITE_DONE) {
                log_msg(LOG_ERROR, "pf_del insert failed: %s", sqlite3_errmsg(db));
                free_metadata(&meta);
                break;
            }
            batch_n++;
        }
        free_metadata(&meta);
        scanned++;

        if (batch_n >= DELETE_BATCH_SIZE) {
            if (flush_delete_batch(db) != 0) break;
            deleted += batch_n;
            batch_n = 0;
            log_msg(LOG_INFO, "cleanup: scanned=%lld deleted=%lld",
                    (long long)scanned, (long long)deleted);
        } else if (scanned % 10000 == 0) {
            log_msg(LOG_INFO, "cleanup: scanned=%lld deleted=%lld (pending=%d)",
                    (long long)scanned, (long long)deleted, batch_n);
        }
    }

    /* Flush remaining */
    if (batch_n > 0) {
        if (flush_delete_batch(db) == 0) {
            deleted += batch_n;
        }
    }

    sqlite3_finalize(iter);
    sqlite3_finalize(del_ins);

    if (exec_sql(db, "COMMIT;") != 0) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        sigaction(SIGINT, &sa_old, NULL);
        return 1;
    }

    /* Truncate WAL */
    sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE);", NULL, NULL, NULL);

    sqlite3_exec(db, "DROP TABLE IF EXISTS temp.pf_kw;", NULL, NULL, NULL);
    sqlite3_exec(db, "DROP TABLE IF EXISTS temp.pf_cand;", NULL, NULL, NULL);
    sqlite3_exec(db, "DROP TABLE IF EXISTS temp.pf_del;", NULL, NULL, NULL);

    long elapsed = (long)(time(NULL) - t_start);
    log_msg(LOG_INFO, "cleanup: DONE total=%lld candidates=%lld deleted=%lld elapsed=%lds",
            (long long)total, (long long)cand_count, (long long)deleted, elapsed);
    printf("\nSummary: scanned=%lld candidates=%lld deleted=%lld elapsed=%lds\n",
           (long long)scanned, (long long)cand_count, (long long)deleted, elapsed);

    sigaction(SIGINT, &sa_old, NULL);
    return cleanup_interrupted ? 0 : 0;
}
