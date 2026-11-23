#define _DEFAULT_SOURCE  /* For usleep */

#include "tree_metadata.h"
#include "thread_tree.h"
#include "tree_peers_queue.h"
#include "tree_infohash_queue.h"
#include "batch_writer.h"
#include "bloom_filter.h"
#include "inflight_set.h"
#include "dht_crawler.h"
#include "database.h"
#include "hard_block.h"
#include "language_filter.h"
#include "classifier.h"
#include "movie_kind.h"
#include "category_filter_cleanup.h"
#include "../lib/bencode-c/bencode.h"

#include <openssl/sha.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <poll.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

/* BitTorrent protocol constants */
#define BT_PROTOCOL_LEN 19
#define BT_PROTOCOL_STRING "BitTorrent protocol"
#define BT_HANDSHAKE_SIZE 68
#define BT_METADATA_PIECE_SIZE 16384
#define MAX_METADATA_SIZE_DEFAULT (10 * 1024 * 1024)  /* 10 MB */

/* Message types */
#define BT_MSG_EXTENDED 20
#define BT_EXT_HANDSHAKE 0

/* ut_metadata message types */
#define UT_METADATA_REQUEST 0
#define UT_METADATA_DATA 1
#define UT_METADATA_REJECT 2

/* Our ut_metadata extension ID */
#define OUR_UT_METADATA_ID 1

/* torrent_class.category value for a movie: TP_CAT_MOVIE from the vendored
 * dht_crawler-android/tparse/taxonomy.h. That header only exists on the Android
 * wrapper's include path while this file also compiles for the crawler's own
 * host `make`, so the value is mirrored here - exactly what src/movie_backfill.c
 * (MB_CAT_MOVIE) and src/http_api.c do. It is frozen by the data on the phone. */
#define TM_CAT_MOVIE 1

/* Default config values */
#define DEFAULT_TCP_TIMEOUT_MS 5000
#define DEFAULT_METADATA_TIMEOUT_MS 60000

/* Monotonic milliseconds, used only for the per-piece re-ask cooldown.
 * Kept isolated from the time(NULL) seconds used elsewhere in this file. */
static int64_t now_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000 + (int64_t)ts.tv_nsec / 1000000;
}

/* Connection states */
typedef enum {
    CONN_STATE_CONNECTING,
    CONN_STATE_HANDSHAKING,
    CONN_STATE_EXTENDED_HANDSHAKE,
    CONN_STATE_REQUESTING_METADATA,
    CONN_STATE_COMPLETE,
    CONN_STATE_ERROR
} conn_state_t;

/* Simple bencode encoding helpers */
static void bencode_encode_dict_start(char *buf, size_t *offset, size_t max) {
    if (*offset < max) buf[(*offset)++] = 'd';
}

static void bencode_encode_dict_end(char *buf, size_t *offset, size_t max) {
    if (*offset < max) buf[(*offset)++] = 'e';
}

static void bencode_encode_string(char *buf, size_t *offset, size_t max, const char *s, size_t len) {
    int written = snprintf(buf + *offset, max - *offset, "%zu:", len);
    if (written > 0) *offset += written;
    if (*offset + len <= max) {
        memcpy(buf + *offset, s, len);
        *offset += len;
    }
}

static void bencode_encode_int(char *buf, size_t *offset, size_t max, int64_t val) {
    int written = snprintf(buf + *offset, max - *offset, "i%lde", (long)val);
    if (written > 0) *offset += written;
}

/* Set socket non-blocking */
static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* Connect with timeout */
static int connect_with_timeout(int fd, const struct sockaddr *addr, socklen_t addrlen, int timeout_ms) {
    if (connect(fd, addr, addrlen) == 0) {
        return 0;  /* Connected immediately */
    }

    if (errno != EINPROGRESS) {
        return -1;
    }

    struct pollfd pfd = {
        .fd = fd,
        .events = POLLOUT,
        .revents = 0
    };

    int ret = poll(&pfd, 1, timeout_ms);
    if (ret <= 0) {
        return -1;  /* Timeout or error */
    }

    /* Check if connection succeeded */
    int error;
    socklen_t len = sizeof(error);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
        return -1;
    }

    return 0;
}

/* Read with timeout */
static ssize_t read_with_timeout(int fd, void *buf, size_t len, int timeout_ms) {
    struct pollfd pfd = {
        .fd = fd,
        .events = POLLIN,
        .revents = 0
    };

    int ret = poll(&pfd, 1, timeout_ms);
    if (ret <= 0) {
        return ret == 0 ? -2 : -1;  /* -2 = timeout */
    }

    return read(fd, buf, len);
}

/* Write all data */
static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = buf;
    size_t remaining = len;

    while (remaining > 0) {
        ssize_t written = write(fd, p, remaining);
        if (written <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                struct pollfd pfd = { .fd = fd, .events = POLLOUT };
                poll(&pfd, 1, 1000);
                continue;
            }
            return -1;
        }
        p += written;
        remaining -= written;
    }
    return 0;
}

/* Generate random peer ID */
static void generate_peer_id(uint8_t *peer_id) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        read(fd, peer_id, 20);
        close(fd);
    } else {
        for (int i = 0; i < 20; i++) {
            peer_id[i] = rand() & 0xFF;
        }
    }
}

/* Build and send BitTorrent handshake */
static int send_handshake(int fd, const uint8_t *infohash, const uint8_t *peer_id) {
    uint8_t handshake[BT_HANDSHAKE_SIZE];
    size_t offset = 0;

    handshake[offset++] = BT_PROTOCOL_LEN;
    memcpy(handshake + offset, BT_PROTOCOL_STRING, BT_PROTOCOL_LEN);
    offset += BT_PROTOCOL_LEN;

    /* Reserved bytes - set extension bit */
    memset(handshake + offset, 0, 8);
    handshake[offset + 5] = 0x10;  /* Extension bit (BEP 10) */
    handshake[offset + 7] = 0x01;  /* DHT bit */
    offset += 8;

    memcpy(handshake + offset, infohash, 20);
    offset += 20;
    memcpy(handshake + offset, peer_id, 20);
    offset += 20;

    return write_all(fd, handshake, BT_HANDSHAKE_SIZE);
}

/* Send extended handshake */
static int send_extended_handshake(int fd) {
    char msg[256];
    size_t offset = 4;  /* Leave space for length */

    msg[offset++] = BT_MSG_EXTENDED;
    msg[offset++] = BT_EXT_HANDSHAKE;

    bencode_encode_dict_start(msg, &offset, sizeof(msg));
    bencode_encode_string(msg, &offset, sizeof(msg), "m", 1);
    bencode_encode_dict_start(msg, &offset, sizeof(msg));
    bencode_encode_string(msg, &offset, sizeof(msg), "ut_metadata", 11);
    bencode_encode_int(msg, &offset, sizeof(msg), OUR_UT_METADATA_ID);
    bencode_encode_dict_end(msg, &offset, sizeof(msg));
    bencode_encode_dict_end(msg, &offset, sizeof(msg));

    /* Fill in length */
    uint32_t msg_len = htonl(offset - 4);
    memcpy(msg, &msg_len, 4);

    return write_all(fd, msg, offset);
}

/* Send metadata request */
static int send_metadata_request(int fd, uint8_t ut_metadata_id, int piece) {
    char msg[128];
    size_t offset = 4;

    msg[offset++] = BT_MSG_EXTENDED;
    msg[offset++] = ut_metadata_id;

    bencode_encode_dict_start(msg, &offset, sizeof(msg));
    bencode_encode_string(msg, &offset, sizeof(msg), "msg_type", 8);
    bencode_encode_int(msg, &offset, sizeof(msg), UT_METADATA_REQUEST);
    bencode_encode_string(msg, &offset, sizeof(msg), "piece", 5);
    bencode_encode_int(msg, &offset, sizeof(msg), piece);
    bencode_encode_dict_end(msg, &offset, sizeof(msg));

    uint32_t msg_len = htonl(offset - 4);
    memcpy(msg, &msg_len, 4);

    return write_all(fd, msg, offset);
}

/* Parse extended handshake response */
static int parse_extended_handshake(const uint8_t *data, size_t len,
                                     uint8_t *ut_metadata_id, int *metadata_size) {
    struct bencode b;
    bencode_init(&b, data, len);

    *ut_metadata_id = 0;
    *metadata_size = 0;

    int in_m_dict = 0;
    int depth = 0;
    char last_key[64] = {0};
    size_t last_key_len = 0;
    int expecting_value = 0;

    int token;
    while ((token = bencode_next(&b)) > 0) {
        if (token == BENCODE_DICT_BEGIN) {
            depth++;
            if (expecting_value && depth == 2 && last_key_len == 1 && last_key[0] == 'm') {
                in_m_dict = 1;
            }
            expecting_value = 0;
            last_key_len = 0;
        } else if (token == BENCODE_DICT_END) {
            if (in_m_dict && depth == 2) in_m_dict = 0;
            depth--;
            expecting_value = 0;
        } else if (token == BENCODE_STRING) {
            if (depth > 0 && !expecting_value) {
                last_key_len = b.toklen < sizeof(last_key) ? b.toklen : sizeof(last_key) - 1;
                memcpy(last_key, b.tok, last_key_len);
                last_key[last_key_len] = '\0';
                expecting_value = 1;
            } else {
                expecting_value = 0;
            }
        } else if (token == BENCODE_INTEGER) {
            if (in_m_dict && b.size > 0 && b.stack[b.size - 1].key &&
                b.stack[b.size - 1].keylen == 11 &&
                memcmp(b.stack[b.size - 1].key, "ut_metadata", 11) == 0) {
                *ut_metadata_id = (uint8_t)atoi((const char *)b.tok);
            } else if (depth == 1 && b.size > 0 && b.stack[b.size - 1].key &&
                      b.stack[b.size - 1].keylen == 13 &&
                      memcmp(b.stack[b.size - 1].key, "metadata_size", 13) == 0) {
                *metadata_size = atoi((const char *)b.tok);
            }
            expecting_value = 0;
        } else {
            expecting_value = 0;
        }
    }

    bencode_free(&b);
    return (*ut_metadata_id > 0 && *metadata_size > 0) ? 0 : -1;
}

/* Parse metadata piece response */
static int parse_metadata_piece(const uint8_t *data, size_t len,
                                 int *msg_type, int *piece_index,
                                 const uint8_t **piece_data, size_t *piece_len) {
    struct bencode b;
    bencode_init(&b, data, len);

    *msg_type = -1;
    *piece_index = -1;
    *piece_data = NULL;
    *piece_len = 0;

    int dict_depth = 0;
    size_t dict_end_pos = 0;
    const char *current_key = NULL;
    size_t current_key_len = 0;
    int expecting_value = 0;

    int token;
    while ((token = bencode_next(&b)) > 0) {
        if (token == BENCODE_DICT_BEGIN) {
            dict_depth++;
            expecting_value = 0;
        } else if (token == BENCODE_DICT_END) {
            dict_depth--;
            if (dict_depth == 0) {
                dict_end_pos = (const uint8_t *)b.buf - data;
                break;
            }
            expecting_value = 0;
        } else if (token == BENCODE_STRING && dict_depth == 1 && !expecting_value) {
            current_key = (const char *)b.tok;
            current_key_len = b.toklen;
            expecting_value = 1;
        } else if (token == BENCODE_INTEGER && dict_depth == 1 && expecting_value) {
            int value = atoi((const char *)b.tok);
            if (current_key_len == 8 && memcmp(current_key, "msg_type", 8) == 0) {
                *msg_type = value;
            } else if (current_key_len == 5 && memcmp(current_key, "piece", 5) == 0) {
                *piece_index = value;
            }
            expecting_value = 0;
        } else {
            expecting_value = 0;
        }
    }

    bencode_free(&b);

    if (dict_end_pos < len) {
        *piece_data = data + dict_end_pos;
        *piece_len = len - dict_end_pos;
    }

    return 0;
}

/* Parse info dictionary to extract metadata */
static tree_torrent_metadata_t *parse_info_dict(const uint8_t *data, size_t len,
                                                  const uint8_t *infohash) {
    struct bencode b;
    bencode_init(&b, data, len);

    char *name = NULL;
    int64_t single_file_length = -1;

    /* Multi-file tracking */
    typedef struct { char *path; int64_t size; } temp_file_t;
    temp_file_t *files = NULL;
    int num_files = 0;
    int files_capacity = 0;

    int in_files_list = 0;
    int in_file_dict = 0;
    int in_path_list = 0;
    char path_buffer[1024] = {0};
    int64_t current_file_length = 0;

    int token;
    while ((token = bencode_next(&b)) > 0) {
        if (token == BENCODE_STRING) {
            if (b.size > 0 && b.stack[b.size - 1].key &&
                b.stack[b.size - 1].keylen == 4 &&
                memcmp(b.stack[b.size - 1].key, "name", 4) == 0) {
                if (name) free(name);
                name = malloc(b.toklen + 1);
                if (name) {
                    memcpy(name, b.tok, b.toklen);
                    name[b.toklen] = '\0';
                }
            } else if (in_path_list) {
                if (strlen(path_buffer) > 0) {
                    strncat(path_buffer, "/", sizeof(path_buffer) - strlen(path_buffer) - 1);
                }
                strncat(path_buffer, (const char *)b.tok,
                        b.toklen < sizeof(path_buffer) - strlen(path_buffer) - 1 ?
                        b.toklen : sizeof(path_buffer) - strlen(path_buffer) - 1);
            }
        } else if (token == BENCODE_INTEGER) {
            if (b.size > 0 && b.stack[b.size - 1].key &&
                b.stack[b.size - 1].keylen == 6 &&
                memcmp(b.stack[b.size - 1].key, "length", 6) == 0) {
                if (in_file_dict) {
                    current_file_length = atoll((const char *)b.tok);
                } else {
                    single_file_length = atoll((const char *)b.tok);
                }
            }
        } else if (token == BENCODE_LIST_BEGIN) {
            if (b.size >= 2 && b.stack[b.size - 2].key &&
                b.stack[b.size - 2].keylen == 5 &&
                memcmp(b.stack[b.size - 2].key, "files", 5) == 0) {
                in_files_list = 1;
            } else if (in_file_dict && b.size >= 2 && b.stack[b.size - 2].key &&
                      b.stack[b.size - 2].keylen == 4 &&
                      memcmp(b.stack[b.size - 2].key, "path", 4) == 0) {
                in_path_list = 1;
                path_buffer[0] = '\0';
            }
        } else if (token == BENCODE_LIST_END) {
            if (in_path_list) {
                in_path_list = 0;
            } else if (in_files_list && !in_file_dict) {
                in_files_list = 0;
            }
        } else if (token == BENCODE_DICT_BEGIN) {
            if (in_files_list) {
                in_file_dict = 1;
                path_buffer[0] = '\0';
                current_file_length = 0;
            }
        } else if (token == BENCODE_DICT_END) {
            if (in_file_dict) {
                if (num_files >= files_capacity) {
                    files_capacity = files_capacity == 0 ? 16 : files_capacity * 2;
                    files = realloc(files, files_capacity * sizeof(temp_file_t));
                }
                if (files && strlen(path_buffer) > 0) {
                    files[num_files].path = strdup(path_buffer);
                    files[num_files].size = current_file_length;
                    num_files++;
                }
                in_file_dict = 0;
            }
        }
    }

    bencode_free(&b);

    if (!name) {
        if (files) {
            for (int i = 0; i < num_files; i++) free(files[i].path);
            free(files);
        }
        return NULL;
    }

    /* Build result */
    tree_torrent_metadata_t *meta = calloc(1, sizeof(tree_torrent_metadata_t));
    if (!meta) {
        free(name);
        if (files) {
            for (int i = 0; i < num_files; i++) free(files[i].path);
            free(files);
        }
        return NULL;
    }

    memcpy(meta->infohash, infohash, 20);
    meta->name = name;

    if (single_file_length >= 0) {
        /* Single file */
        meta->total_size = single_file_length;
        meta->file_count = 1;
        meta->files = malloc(sizeof(char *));
        meta->file_sizes = malloc(sizeof(int64_t));
        if (meta->files && meta->file_sizes) {
            meta->files[0] = strdup(name);
            meta->file_sizes[0] = single_file_length;
        }
        if (files) {
            for (int i = 0; i < num_files; i++) free(files[i].path);
            free(files);
        }
    } else if (num_files > 0) {
        /* Multi-file */
        meta->file_count = num_files;
        meta->files = malloc(num_files * sizeof(char *));
        meta->file_sizes = malloc(num_files * sizeof(int64_t));
        meta->total_size = 0;
        if (meta->files && meta->file_sizes) {
            for (int i = 0; i < num_files; i++) {
                meta->files[i] = files[i].path;  /* Transfer ownership */
                meta->file_sizes[i] = files[i].size;
                meta->total_size += files[i].size;
            }
        }
        free(files);  /* Just the array, not the strings */
    } else {
        /* No file info - use name as single file */
        meta->file_count = 1;
        meta->total_size = 0;
        meta->files = malloc(sizeof(char *));
        meta->file_sizes = malloc(sizeof(int64_t));
        if (meta->files && meta->file_sizes) {
            meta->files[0] = strdup(name);
            meta->file_sizes[0] = 0;
        }
    }

    return meta;
}

/* Main fetch function (single peer) */
tree_torrent_metadata_t *tree_fetch_metadata_from_peer(
    const uint8_t *infohash,
    const struct sockaddr_storage *peer,
    tree_metadata_config_t *config
) {
    if (!infohash || !peer) return NULL;

    int tcp_timeout = config ? config->tcp_connect_timeout_ms : DEFAULT_TCP_TIMEOUT_MS;
    int metadata_timeout = config ? config->metadata_timeout_ms : DEFAULT_METADATA_TIMEOUT_MS;
    int max_metadata = config ? config->max_metadata_size : MAX_METADATA_SIZE_DEFAULT;
    thread_tree_t *tree = config ? config->tree : NULL;

    /* Create socket */
    int family = peer->ss_family;
    int fd = socket(family, SOCK_STREAM, 0);
    if (fd < 0) return NULL;

    /* Track active connection */
    if (tree) {
        atomic_fetch_add(&tree->active_connections, 1);
    }

    /* Set non-blocking and options */
    if (set_nonblocking(fd) < 0) {
        if (tree) atomic_fetch_sub(&tree->active_connections, 1);
        close(fd);
        return NULL;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /* Connect */
    socklen_t addrlen = (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
    if (connect_with_timeout(fd, (const struct sockaddr *)peer, addrlen, tcp_timeout) < 0) {
        if (tree) atomic_fetch_sub(&tree->active_connections, 1);
        close(fd);
        return NULL;
    }

    /* Generate peer ID and send handshake */
    uint8_t peer_id[20];
    generate_peer_id(peer_id);

    if (send_handshake(fd, infohash, peer_id) < 0) {
        if (tree) atomic_fetch_sub(&tree->active_connections, 1);
        close(fd);
        return NULL;
    }

    /* State machine */
    conn_state_t state = CONN_STATE_HANDSHAKING;
    uint8_t recv_buf[65536];
    size_t recv_offset = 0;

    uint8_t ut_metadata_id = 0;
    int metadata_size = 0;
    int total_pieces = 0;
    uint8_t *metadata_buffer = NULL;
    uint8_t *pieces_bitmap = NULL;
    int pieces_received = 0;

    time_t start_time = time(NULL);
    tree_torrent_metadata_t *result = NULL;

    while (state != CONN_STATE_COMPLETE && state != CONN_STATE_ERROR) {
        /* Check timeout */
        if (difftime(time(NULL), start_time) > metadata_timeout / 1000) {
            log_msg(LOG_DEBUG, "Metadata fetch timeout");
            break;
        }

        /* Read data */
        int remaining_timeout = metadata_timeout - (int)(difftime(time(NULL), start_time) * 1000);
        if (remaining_timeout < 100) remaining_timeout = 100;

        ssize_t nread = read_with_timeout(fd, recv_buf + recv_offset,
                                           sizeof(recv_buf) - recv_offset,
                                           remaining_timeout < 1000 ? remaining_timeout : 1000);
        if (nread < 0) {
            if (nread == -2) continue;  /* Timeout, retry */
            break;  /* Error */
        }
        if (nread == 0) break;  /* EOF */

        recv_offset += nread;

        /* Process based on state */
        if (state == CONN_STATE_HANDSHAKING) {
            if (recv_offset >= BT_HANDSHAKE_SIZE) {
                /* Verify handshake */
                if (recv_buf[0] != BT_PROTOCOL_LEN ||
                    memcmp(recv_buf + 1, BT_PROTOCOL_STRING, BT_PROTOCOL_LEN) != 0) {
                    state = CONN_STATE_ERROR;
                    break;
                }
                if ((recv_buf[25] & 0x10) == 0) {
                    state = CONN_STATE_ERROR;  /* No extension support */
                    break;
                }
                if (memcmp(recv_buf + 28, infohash, 20) != 0) {
                    state = CONN_STATE_ERROR;  /* Wrong infohash */
                    break;
                }

                /* Send extended handshake */
                if (send_extended_handshake(fd) < 0) {
                    state = CONN_STATE_ERROR;
                    break;
                }

                /* Shift buffer */
                memmove(recv_buf, recv_buf + BT_HANDSHAKE_SIZE, recv_offset - BT_HANDSHAKE_SIZE);
                recv_offset -= BT_HANDSHAKE_SIZE;
                state = CONN_STATE_EXTENDED_HANDSHAKE;
            }
        } else {
            /* Process BitTorrent messages */
            while (recv_offset >= 4 && state != CONN_STATE_ERROR && state != CONN_STATE_COMPLETE) {
                uint32_t msg_len;
                memcpy(&msg_len, recv_buf, 4);
                msg_len = ntohl(msg_len);

                if (msg_len > 1024 * 1024) {  /* Max 1MB message */
                    state = CONN_STATE_ERROR;
                    break;
                }

                if (recv_offset < msg_len + 4) break;  /* Need more data */

                if (msg_len == 0) {
                    /* Keep-alive */
                    memmove(recv_buf, recv_buf + 4, recv_offset - 4);
                    recv_offset -= 4;
                    continue;
                }

                uint8_t msg_type = recv_buf[4];

                if (msg_type == BT_MSG_EXTENDED && msg_len > 1) {
                    uint8_t ext_id = recv_buf[5];

                    if (ext_id == BT_EXT_HANDSHAKE && state == CONN_STATE_EXTENDED_HANDSHAKE) {
                        /* Parse extended handshake */
                        if (parse_extended_handshake(recv_buf + 6, msg_len - 2,
                                                     &ut_metadata_id, &metadata_size) < 0) {
                            state = CONN_STATE_ERROR;
                            break;
                        }

                        if (metadata_size > max_metadata) {
                            state = CONN_STATE_ERROR;
                            break;
                        }

                        total_pieces = (metadata_size + BT_METADATA_PIECE_SIZE - 1) / BT_METADATA_PIECE_SIZE;
                        metadata_buffer = calloc(1, metadata_size);
                        pieces_bitmap = calloc(1, (total_pieces + 7) / 8);
                        pieces_received = 0;

                        if (!metadata_buffer || !pieces_bitmap) {
                            state = CONN_STATE_ERROR;
                            break;
                        }

                        /* Request all pieces */
                        for (int i = 0; i < total_pieces; i++) {
                            send_metadata_request(fd, ut_metadata_id, i);
                        }

                        state = CONN_STATE_REQUESTING_METADATA;

                    } else if (ext_id == OUR_UT_METADATA_ID && state == CONN_STATE_REQUESTING_METADATA) {
                        /* Parse metadata piece */
                        int piece_msg_type, piece_index;
                        const uint8_t *piece_data;
                        size_t piece_len;

                        parse_metadata_piece(recv_buf + 6, msg_len - 2,
                                            &piece_msg_type, &piece_index,
                                            &piece_data, &piece_len);

                        if (piece_msg_type == UT_METADATA_REJECT) {
                            state = CONN_STATE_ERROR;
                            break;
                        }

                        if (piece_msg_type == UT_METADATA_DATA &&
                            piece_index >= 0 && piece_index < total_pieces) {

                            /* Calculate expected size */
                            size_t offset = piece_index * BT_METADATA_PIECE_SIZE;
                            size_t expected = BT_METADATA_PIECE_SIZE;
                            if (offset + expected > (size_t)metadata_size) {
                                expected = metadata_size - offset;
                            }

                            if (piece_len == expected) {
                                int byte_idx = piece_index / 8;
                                int bit_idx = piece_index % 8;

                                if (!(pieces_bitmap[byte_idx] & (1 << bit_idx))) {
                                    memcpy(metadata_buffer + offset, piece_data, piece_len);
                                    pieces_bitmap[byte_idx] |= (1 << bit_idx);
                                    pieces_received++;

                                    if (pieces_received >= total_pieces) {
                                        /* Verify hash */
                                        uint8_t computed_hash[20];
                                        SHA1(metadata_buffer, metadata_size, computed_hash);

                                        if (memcmp(computed_hash, infohash, 20) == 0) {
                                            result = parse_info_dict(metadata_buffer, metadata_size, infohash);
                                            state = CONN_STATE_COMPLETE;
                                        } else {
                                            state = CONN_STATE_ERROR;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                /* Shift buffer */
                memmove(recv_buf, recv_buf + msg_len + 4, recv_offset - msg_len - 4);
                recv_offset -= (msg_len + 4);
            }
        }
    }

    /* Cleanup */
    if (tree) atomic_fetch_sub(&tree->active_connections, 1);
    close(fd);
    free(metadata_buffer);
    free(pieces_bitmap);

    return result;
}

/* ---- Parallel peer connections ---- */

#define MAX_PARALLEL_PEERS 4

/* Max metadata piece requests a single peer may have in flight at once
 * (libtorrent parity: maybe_send_request keeps fewer than 2 outstanding). */
#define METADATA_OUTSTANDING_PER_PEER 2

/* Close a connection that has produced no readable data for this long. */
#define CONN_SILENCE_CLOSE_MS 10000

/* Per-connection state for parallel fetching. The metadata bytes live in the
 * shared metadata_fetch_state_t, not here. */
typedef struct {
    int fd;
    conn_state_t state;
    uint8_t recv_buf[65536];
    size_t recv_offset;
    uint8_t ut_metadata_id;
    int metadata_size;              /* Peer-advertised size (consistency check) */
    time_t connect_start;
    time_t last_activity;           /* Last readable data (silence watchdog) */
    int outstanding_count;          /* Currently in-flight piece requests */
    int outstanding_pieces[METADATA_OUTSTANDING_PER_PEER];
} parallel_conn_t;

/* Shared piece-aggregation state for one parallel metadata fetch. Owned by
 * tree_fetch_metadata_parallel on the worker's stack. No locking is needed:
 * it is driven exclusively by that worker thread's single poll() loop. */
typedef struct {
    int initialized;                /* Set once a peer advertises metadata_size */
    int metadata_size;
    int total_pieces;
    uint8_t *metadata_buffer;       /* total_pieces * 16 KiB (last piece short) */
    uint8_t *pieces_bitmap;         /* ceil(total_pieces / 8) */
    uint8_t *outstanding;           /* 1 byte per piece: request in flight */
    int64_t *last_request_ms;       /* per-piece monotonic ms of last request */
    int pieces_received;
    int reset_count;                /* SHA-1-mismatch resets performed (max 1) */
    int cursor;                     /* Round-robin cursor over pieces */
} metadata_fetch_state_t;

/* One whole parallel fetch: shared state plus the window connection slots. */
typedef struct {
    parallel_conn_t conns[MAX_PARALLEL_PEERS];
    int window_size;
    metadata_fetch_state_t st;
} parallel_fetch_t;

/* Initialize a connection slot with a new peer */
static int parallel_conn_start(parallel_conn_t *conn, const uint8_t *infohash,
                                const struct sockaddr_storage *peer,
                                thread_tree_t *tree) {
    memset(conn, 0, sizeof(*conn));
    conn->fd = -1;
    conn->state = CONN_STATE_ERROR;

    int family = peer->ss_family;
    int fd = socket(family, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    if (tree) atomic_fetch_add(&tree->active_connections, 1);

    if (set_nonblocking(fd) < 0) {
        if (tree) atomic_fetch_sub(&tree->active_connections, 1);
        close(fd);
        return -1;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /* Start non-blocking connect */
    socklen_t addrlen = (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
    int ret = connect(fd, (const struct sockaddr *)peer, addrlen);
    if (ret == 0) {
        /* Connected immediately - send handshake */
        uint8_t peer_id[20];
        generate_peer_id(peer_id);
        if (send_handshake(fd, infohash, peer_id) < 0) {
            if (tree) atomic_fetch_sub(&tree->active_connections, 1);
            close(fd);
            return -1;
        }
        conn->state = CONN_STATE_HANDSHAKING;
    } else if (errno == EINPROGRESS) {
        conn->state = CONN_STATE_CONNECTING;
    } else {
        if (tree) atomic_fetch_sub(&tree->active_connections, 1);
        close(fd);
        return -1;
    }

    conn->fd = fd;
    conn->connect_start = time(NULL);
    conn->last_activity = conn->connect_start;
    return 0;
}

/* Drop one piece from a connection's in-flight list and clear its shared
 * outstanding flag (a request is owned by at most one connection). */
static void meta_conn_clear_outstanding(parallel_conn_t *conn,
                                        metadata_fetch_state_t *st, int piece) {
    for (int i = 0; i < conn->outstanding_count; i++) {
        if (conn->outstanding_pieces[i] == piece) {
            conn->outstanding_pieces[i] =
                conn->outstanding_pieces[conn->outstanding_count - 1];
            conn->outstanding_count--;
            break;
        }
    }
    if (piece >= 0 && piece < st->total_pieces) st->outstanding[piece] = 0;
}

/* Release every piece this connection still had in flight so other window
 * peers can take them over. */
static void meta_conn_release_outstanding(parallel_conn_t *conn,
                                          metadata_fetch_state_t *st) {
    for (int i = 0; i < conn->outstanding_count; i++) {
        int piece = conn->outstanding_pieces[i];
        if (piece >= 0 && piece < st->total_pieces) st->outstanding[piece] = 0;
    }
    conn->outstanding_count = 0;
}

/* One-shot bitfield reset after a SHA-1 mismatch: clear every received piece,
 * in-flight flag and the per-conn lists, then let the scheduler re-request. */
static void metadata_state_reset(parallel_fetch_t *fetch) {
    metadata_fetch_state_t *st = &fetch->st;
    if (!st->initialized) return;
    memset(st->pieces_bitmap, 0, (st->total_pieces + 7) / 8);
    memset(st->outstanding, 0, st->total_pieces);
    memset(st->last_request_ms, 0, st->total_pieces * sizeof(int64_t));
    st->pieces_received = 0;
    st->cursor = 0;
    st->reset_count++;
    for (int i = 0; i < fetch->window_size; i++) {
        fetch->conns[i].outstanding_count = 0;
    }
}

/* Send up to METADATA_OUTSTANDING_PER_PEER requests to one connection, picking
 * missing (bitmap clear) && not-outstanding pieces round-robin from the shared
 * cursor so the window spreads the load. */
static void metadata_schedule_conn(parallel_fetch_t *fetch, int ci, thread_tree_t *tree) {
    parallel_conn_t *conn = &fetch->conns[ci];
    metadata_fetch_state_t *st = &fetch->st;
    if (!st->initialized) return;
    if (conn->state != CONN_STATE_REQUESTING_METADATA || conn->fd < 0) return;

    int pieces = st->total_pieces;
    while (pieces > 0 && conn->outstanding_count < METADATA_OUTSTANDING_PER_PEER) {
        int found = -1;
        int found_was_outstanding = 0;
        for (int n = 0; n < pieces; n++) {
            int p = (st->cursor + n) % pieces;
            if (st->pieces_bitmap[p / 8] & (1 << (p % 8))) continue;  /* have it */

            /* Never re-append a piece this conn already holds: a re-ask must be
             * taken over by a different conn (the original slot keeps its own). */
            int held_by_conn = 0;
            for (int h = 0; h < conn->outstanding_count; h++) {
                if (conn->outstanding_pieces[h] == p) { held_by_conn = 1; break; }
            }
            if (held_by_conn) continue;

            if (st->outstanding[p]) {
                /* In flight elsewhere: requestable again only once the per-piece
                 * 3 s cooldown expires. The original request is never cancelled. */
                if (now_ms() - st->last_request_ms[p] <= 3000) continue;
                found_was_outstanding = 1;
            } else {
                found_was_outstanding = 0;
            }
            found = p;
            st->cursor = (p + 1) % pieces;
            break;
        }
        if (found < 0) break;  /* every remaining piece is in flight/received */

        if (send_metadata_request(conn->fd, conn->ut_metadata_id, found) < 0) {
            conn->state = CONN_STATE_ERROR;  /* caller closes + refills */
            return;
        }
        if (found_was_outstanding && tree) {
            atomic_fetch_add(&tree->metadata_piece_reasks, 1);
        }
        st->outstanding[found] = 1;
        st->last_request_ms[found] = now_ms();  /* restart the 3 s floor on every send */
        conn->outstanding_pieces[conn->outstanding_count++] = found;
    }
}

/* Close and cleanup a connection slot. The shared metadata buffer is owned by
 * the fetch, not the connection, so only per-conn state is touched here. */
static void parallel_conn_close(parallel_conn_t *conn, thread_tree_t *tree,
                                metadata_fetch_state_t *st) {
    if (conn->fd >= 0) {
        if (tree) atomic_fetch_sub(&tree->active_connections, 1);
        close(conn->fd);
        conn->fd = -1;
    }
    if (st) meta_conn_release_outstanding(conn, st);
    conn->outstanding_count = 0;
    conn->state = CONN_STATE_ERROR;
}

/* Process data for a single parallel connection. Returns metadata on success, NULL otherwise. */
static tree_torrent_metadata_t *parallel_conn_process(parallel_fetch_t *fetch, int ci,
                                                       const uint8_t *infohash,
                                                       int max_metadata,
                                                       thread_tree_t *tree) {
    parallel_conn_t *conn = &fetch->conns[ci];
    metadata_fetch_state_t *st = &fetch->st;
    if (conn->state == CONN_STATE_HANDSHAKING) {
        if (conn->recv_offset >= BT_HANDSHAKE_SIZE) {
            if (conn->recv_buf[0] != BT_PROTOCOL_LEN ||
                memcmp(conn->recv_buf + 1, BT_PROTOCOL_STRING, BT_PROTOCOL_LEN) != 0 ||
                (conn->recv_buf[25] & 0x10) == 0 ||
                memcmp(conn->recv_buf + 28, infohash, 20) != 0) {
                conn->state = CONN_STATE_ERROR;
                return NULL;
            }

            if (send_extended_handshake(conn->fd) < 0) {
                conn->state = CONN_STATE_ERROR;
                return NULL;
            }

            memmove(conn->recv_buf, conn->recv_buf + BT_HANDSHAKE_SIZE,
                    conn->recv_offset - BT_HANDSHAKE_SIZE);
            conn->recv_offset -= BT_HANDSHAKE_SIZE;
            conn->state = CONN_STATE_EXTENDED_HANDSHAKE;
        }
    }

    /* Process BT messages */
    while (conn->recv_offset >= 4 &&
           conn->state != CONN_STATE_ERROR &&
           conn->state != CONN_STATE_COMPLETE &&
           conn->state != CONN_STATE_HANDSHAKING &&
           conn->state != CONN_STATE_CONNECTING) {

        uint32_t msg_len;
        memcpy(&msg_len, conn->recv_buf, 4);
        msg_len = ntohl(msg_len);

        if (msg_len > 1024 * 1024) {
            conn->state = CONN_STATE_ERROR;
            return NULL;
        }

        if (conn->recv_offset < msg_len + 4) break;

        if (msg_len == 0) {
            memmove(conn->recv_buf, conn->recv_buf + 4, conn->recv_offset - 4);
            conn->recv_offset -= 4;
            continue;
        }

        uint8_t msg_type = conn->recv_buf[4];

        if (msg_type == BT_MSG_EXTENDED && msg_len > 1) {
            uint8_t ext_id = conn->recv_buf[5];

            if (ext_id == BT_EXT_HANDSHAKE && conn->state == CONN_STATE_EXTENDED_HANDSHAKE) {
                if (parse_extended_handshake(conn->recv_buf + 6, msg_len - 2,
                                             &conn->ut_metadata_id, &conn->metadata_size) < 0) {
                    conn->state = CONN_STATE_ERROR;
                    goto shift;
                }

                if (conn->metadata_size > max_metadata) {
                    /* Advertised size exceeds our cap: drop this peer. */
                    conn->state = CONN_STATE_ERROR;
                    goto shift;
                }

                if (!st->initialized) {
                    /* First peer to speak up defines the fetch's size/shape. */
                    st->metadata_size = conn->metadata_size;
                    st->total_pieces =
                        (st->metadata_size + BT_METADATA_PIECE_SIZE - 1) / BT_METADATA_PIECE_SIZE;
                    st->metadata_buffer = calloc(1, st->metadata_size);
                    st->pieces_bitmap = calloc(1, (st->total_pieces + 7) / 8);
                    st->outstanding = calloc(st->total_pieces, 1);
                    st->last_request_ms = calloc(st->total_pieces, sizeof(int64_t));
                    if (!st->metadata_buffer || !st->pieces_bitmap || !st->outstanding ||
                        !st->last_request_ms) {
                        free(st->metadata_buffer); st->metadata_buffer = NULL;
                        free(st->pieces_bitmap); st->pieces_bitmap = NULL;
                        free(st->outstanding); st->outstanding = NULL;
                        free(st->last_request_ms); st->last_request_ms = NULL;
                        conn->state = CONN_STATE_ERROR;
                        goto shift;
                    }
                    st->initialized = 1;
                } else if (conn->metadata_size != st->metadata_size) {
                    /* Peers disagree on size; at most one is right and the final
                     * SHA-1 arbitrates. Close this conn and refill its slot. */
                    conn->state = CONN_STATE_ERROR;
                    goto shift;
                }

                conn->state = CONN_STATE_REQUESTING_METADATA;
                metadata_schedule_conn(fetch, ci, tree);

            } else if (ext_id == OUR_UT_METADATA_ID && conn->state == CONN_STATE_REQUESTING_METADATA) {
                int piece_msg_type, piece_index;
                const uint8_t *piece_data;
                size_t piece_len;

                parse_metadata_piece(conn->recv_buf + 6, msg_len - 2,
                                    &piece_msg_type, &piece_index,
                                    &piece_data, &piece_len);

                if (piece_msg_type == UT_METADATA_REJECT) {
                    conn->state = CONN_STATE_ERROR;
                    goto shift;
                }

                if (piece_msg_type == UT_METADATA_DATA &&
                    piece_index >= 0 && piece_index < st->total_pieces) {

                    size_t offset = (size_t)piece_index * BT_METADATA_PIECE_SIZE;
                    size_t expected = BT_METADATA_PIECE_SIZE;
                    if (offset + expected > (size_t)st->metadata_size) {
                        expected = st->metadata_size - offset;
                    }

                    /* Whatever happens this request is answered: release it so
                     * another conn can take the piece over if needed. */
                    meta_conn_clear_outstanding(conn, st, piece_index);

                    if (piece_len == expected) {
                        int byte_idx = piece_index / 8;
                        int bit_idx = piece_index % 8;

                        if (!(st->pieces_bitmap[byte_idx] & (1 << bit_idx))) {
                            memcpy(st->metadata_buffer + offset, piece_data, piece_len);
                            st->pieces_bitmap[byte_idx] |= (1 << bit_idx);
                            st->pieces_received++;

                            if (st->pieces_received >= st->total_pieces) {
                                uint8_t computed_hash[20];
                                SHA1(st->metadata_buffer, st->metadata_size, computed_hash);

                                if (memcmp(computed_hash, infohash, 20) == 0) {
                                    tree_torrent_metadata_t *meta = parse_info_dict(
                                        st->metadata_buffer, st->metadata_size, infohash);
                                    conn->state = CONN_STATE_COMPLETE;
                                    return meta;
                                } else if (st->reset_count == 0) {
                                    /* Assembled bytes do not hash to the infohash:
                                     * peers disagreed on content. Reset the piece
                                     * bitfield once and re-request everything. */
                                    metadata_state_reset(fetch);
                                } else {
                                    /* Wrong even after the one allowed reset: fail
                                     * the entry so it falls to the normal path. */
                                    conn->state = CONN_STATE_ERROR;
                                    goto shift;
                                }
                            }
                        }
                    }

                    /* Keep this conn's request pipe full. */
                    metadata_schedule_conn(fetch, ci, tree);
                }
            }
        }

shift:
        memmove(conn->recv_buf, conn->recv_buf + msg_len + 4, conn->recv_offset - msg_len - 4);
        conn->recv_offset -= (msg_len + 4);

        if (conn->state == CONN_STATE_ERROR) return NULL;
    }

    return NULL;
}

/* Fetch metadata using parallel peer connections with sliding window */
tree_torrent_metadata_t *tree_fetch_metadata_parallel(
    const uint8_t *infohash,
    const struct sockaddr_storage *peers,
    int peer_count,
    tree_metadata_config_t *config
) {
    if (!infohash || !peers || peer_count <= 0) return NULL;

    int tcp_timeout = config ? config->tcp_connect_timeout_ms : DEFAULT_TCP_TIMEOUT_MS;
    int metadata_timeout = config ? config->metadata_timeout_ms : DEFAULT_METADATA_TIMEOUT_MS;
    int max_metadata = config ? config->max_metadata_size : MAX_METADATA_SIZE_DEFAULT;
    int window_size = config ? config->parallel_peers : 2;
    thread_tree_t *tree = config ? config->tree : NULL;

    if (window_size < 1) window_size = 1;
    if (window_size > MAX_PARALLEL_PEERS) window_size = MAX_PARALLEL_PEERS;
    if (window_size > peer_count) window_size = peer_count;

    parallel_fetch_t fetch;
    memset(&fetch, 0, sizeof(fetch));
    fetch.window_size = window_size;
    for (int i = 0; i < MAX_PARALLEL_PEERS; i++) {
        fetch.conns[i].fd = -1;
        fetch.conns[i].state = CONN_STATE_ERROR;
    }

    int next_peer = 0;  /* Next untried peer index */
    tree_torrent_metadata_t *result = NULL;
    time_t global_start = time(NULL);

    /* Fill initial window */
    int active_count = 0;
    for (int i = 0; i < window_size && next_peer < peer_count; i++) {
        if (parallel_conn_start(&fetch.conns[i], infohash, &peers[next_peer], tree) == 0) {
            active_count++;
        }
        next_peer++;
    }

    /* Main poll loop */
    while (active_count > 0 && !result) {
        /* Check global timeout */
        if (difftime(time(NULL), global_start) > metadata_timeout / 1000) {
            break;
        }

        /* Build poll array */
        struct pollfd pfds[MAX_PARALLEL_PEERS];
        int pfd_to_conn[MAX_PARALLEL_PEERS];  /* Map pollfd index -> conn index */
        int nfds = 0;

        for (int i = 0; i < window_size; i++) {
            if (fetch.conns[i].fd < 0 || fetch.conns[i].state == CONN_STATE_ERROR ||
                fetch.conns[i].state == CONN_STATE_COMPLETE) continue;

            pfds[nfds].fd = fetch.conns[i].fd;
            pfds[nfds].revents = 0;

            if (fetch.conns[i].state == CONN_STATE_CONNECTING) {
                pfds[nfds].events = POLLOUT;
            } else {
                pfds[nfds].events = POLLIN;
            }

            pfd_to_conn[nfds] = i;
            nfds++;
        }

        if (nfds == 0) break;

        int ret = poll(pfds, nfds, 500);
        if (ret < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* Process each ready connection */
        for (int p = 0; p < nfds && !result; p++) {
            int ci = pfd_to_conn[p];
            parallel_conn_t *conn = &fetch.conns[ci];

            if (pfds[p].revents == 0) {
                /* Check per-connection timeout for connecting state */
                if (conn->state == CONN_STATE_CONNECTING &&
                    difftime(time(NULL), conn->connect_start) > tcp_timeout / 1000) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                }
                continue;
            }

            if (pfds[p].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                parallel_conn_close(conn, tree, &fetch.st);
                active_count--;
                continue;
            }

            if (conn->state == CONN_STATE_CONNECTING && (pfds[p].revents & POLLOUT)) {
                /* Check connect result */
                int error;
                socklen_t len = sizeof(error);
                if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                    continue;
                }

                /* Connected - send handshake */
                uint8_t peer_id[20];
                generate_peer_id(peer_id);
                if (send_handshake(conn->fd, infohash, peer_id) < 0) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                    continue;
                }
                conn->state = CONN_STATE_HANDSHAKING;
                conn->last_activity = time(NULL);
                continue;
            }

            if (pfds[p].revents & POLLIN) {
                ssize_t nread = read(conn->fd, conn->recv_buf + conn->recv_offset,
                                     sizeof(conn->recv_buf) - conn->recv_offset);
                if (nread <= 0) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                    continue;
                }

                conn->recv_offset += nread;
                conn->last_activity = time(NULL);
                result = parallel_conn_process(&fetch, ci, infohash, max_metadata, tree);

                if (conn->state == CONN_STATE_ERROR) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                }
            }
        }

        /* Silence watchdog + refill every live conn's request pipe. Runs once
         * per 500 ms tick, so it also covers conns poll() did not report on. */
        if (!result) {
            time_t now = time(NULL);
            for (int i = 0; i < window_size; i++) {
                parallel_conn_t *conn = &fetch.conns[i];
                if (conn->fd < 0 || conn->state == CONN_STATE_ERROR ||
                    conn->state == CONN_STATE_COMPLETE) continue;

                if (conn->state != CONN_STATE_CONNECTING &&
                    difftime(now, conn->last_activity) > CONN_SILENCE_CLOSE_MS / 1000) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                    continue;
                }

                metadata_schedule_conn(&fetch, i, tree);
                if (conn->state == CONN_STATE_ERROR) {
                    parallel_conn_close(conn, tree, &fetch.st);
                    active_count--;
                }
            }
        }

        /* Fill empty slots from remaining peers */
        if (!result) {
            for (int i = 0; i < window_size && next_peer < peer_count; i++) {
                if (fetch.conns[i].fd >= 0) continue;  /* Slot still active */
                if (fetch.conns[i].state != CONN_STATE_ERROR) continue;

                if (parallel_conn_start(&fetch.conns[i], infohash, &peers[next_peer], tree) == 0) {
                    active_count++;
                }
                next_peer++;
            }
        }
    }

    /* Close every slot, then free the shared buffers exactly once. */
    for (int i = 0; i < window_size; i++) {
        parallel_conn_close(&fetch.conns[i], tree, &fetch.st);
    }
    free(fetch.st.metadata_buffer);
    free(fetch.st.pieces_bitmap);
    free(fetch.st.outstanding);
    free(fetch.st.last_request_ms);

    return result;
}

/**
 * Convert tree metadata format to database format
 * @param tree_meta Tree metadata structure (will NOT be freed)
 * @param db_meta Output database metadata structure (caller must free)
 * @param peer_count Number of peers that had this infohash
 * @return 0 on success, -1 on error
 */
static int tree_metadata_to_database_format(
    const tree_torrent_metadata_t *tree_meta,
    torrent_metadata_t *db_meta,
    int peer_count
) {
    if (!tree_meta || !db_meta) {
        return -1;
    }

    memset(db_meta, 0, sizeof(torrent_metadata_t));

    /* Copy infohash */
    memcpy(db_meta->info_hash, tree_meta->infohash, 20);

    /* Copy name (strdup for ownership) */
    db_meta->name = strdup(tree_meta->name);
    if (!db_meta->name) {
        return -1;
    }

    /* Copy size and metadata */
    db_meta->size_bytes = tree_meta->total_size;
    db_meta->total_peers = peer_count;
    db_meta->added_timestamp = time(NULL);

    /* Convert file arrays to file_info_t array */
    db_meta->num_files = tree_meta->file_count;
    if (tree_meta->file_count > 0) {
        db_meta->files = malloc(tree_meta->file_count * sizeof(file_info_t));
        if (!db_meta->files) {
            free(db_meta->name);
            return -1;
        }

        for (int i = 0; i < tree_meta->file_count; i++) {
            db_meta->files[i].path = strdup(tree_meta->files[i]);
            if (!db_meta->files[i].path) {
                /* Cleanup on failure */
                for (int j = 0; j < i; j++) {
                    free(db_meta->files[j].path);
                }
                free(db_meta->files);
                free(db_meta->name);
                return -1;
            }
            db_meta->files[i].size_bytes = tree_meta->file_sizes[i];
            db_meta->files[i].file_index = i;
        }
    } else {
        db_meta->files = NULL;
    }

    return 0;
}

/**
 * Free database format metadata (after batch_writer_add copies it)
 */
static void free_database_metadata(torrent_metadata_t *db_meta) {
    if (!db_meta) return;

    if (db_meta->name) {
        free(db_meta->name);
    }

    if (db_meta->files) {
        for (int i = 0; i < db_meta->num_files; i++) {
            if (db_meta->files[i].path) {
                free(db_meta->files[i].path);
            }
        }
        free(db_meta->files);
    }
}

/* Free metadata */
void tree_free_metadata(tree_torrent_metadata_t *meta) {
    if (!meta) return;

    if (meta->name) free(meta->name);
    if (meta->files) {
        for (int i = 0; i < meta->file_count; i++) {
            if (meta->files[i]) free(meta->files[i]);
        }
        free(meta->files);
    }
    if (meta->file_sizes) free(meta->file_sizes);
    free(meta);
}

/* Metadata worker thread function */
void *tree_metadata_worker_func(void *arg) {
    metadata_worker_ctx_t *ctx = (metadata_worker_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;
    int worker_id = ctx->worker_id;

    log_msg(LOG_DEBUG, "[tree %u] Metadata worker %d started", tree->tree_id, worker_id);

    tree_peers_queue_t *peers_queue = (tree_peers_queue_t *)tree->peers_queue;
    tree_metadata_config_t config = {
        .tcp_connect_timeout_ms = tree->tcp_connect_timeout_ms,
        .metadata_timeout_ms = tree->metadata_timeout_ms > 0 ? tree->metadata_timeout_ms : DEFAULT_METADATA_TIMEOUT_MS,
        .max_metadata_size = 10 * 1024 * 1024,
        .parallel_peers = tree->parallel_peers,
        .tree = tree
    };

    /* One long-lived response queue for the optional fresh-peer relookup below.
     * Created once here and destroyed once after the loop, instead of being
     * allocated per failed metadata fetch. tree_lookup_peers() drains stale
     * entries and matches responses by TID, so a queue reused across relookups
     * is safe against late responses from a previous lookup. */
    tree_response_queue_t *relookup_queue = tree_response_queue_create(TREE_GP_RESPONSE_QUEUE_CAP);
    if (!relookup_queue) {
        log_msg(LOG_ERROR, "[tree %u] Metadata worker %d failed to create relookup response queue; exiting",
                tree->tree_id, worker_id);
        free(ctx);
        return NULL;
    }

    while (!atomic_load(&tree->shutdown_requested)) {
        /* Pop peer entry from queue */
        peer_entry_t entry;
        if (tree_peers_queue_pop(peers_queue, &entry, 1000) < 0) {
            continue;
        }

        /* Track that we're attempting to fetch this infohash */
        atomic_fetch_add(&tree->metadata_attempts, 1);

        /* Allow immediate exit during program shutdown (Ctrl+C) */
        if (atomic_load(&tree->shutdown_reason) == SHUTDOWN_REASON_SUPERVISOR) {
            break;
        }

        /* Fetch metadata using parallel peer connections */
        tree_torrent_metadata_t *metadata = tree_fetch_metadata_parallel(
            entry.infohash, entry.peers, entry.peer_count, &config);

        /* Exit worker loop immediately on supervisor shutdown */
        if (atomic_load(&tree->shutdown_reason) == SHUTDOWN_REASON_SUPERVISOR) {
            if (metadata) tree_free_metadata(metadata);
            break;
        }

        /* On failure, optionally re-run a get_peers lookup for this infohash to
         * obtain fresh peers and retry the metadata fetch once, BEFORE the
         * failure-bloom strike below is recorded. Gated by
         * tree_metadata_fresh_peer_retries (0 = disabled). The retry is not
         * counted as a separate metadata attempt. */
        if (!metadata && tree->fresh_peer_retries > 0) {
            atomic_fetch_add(&tree->fresh_peer_relookups, 1);
            atomic_fetch_add(&tree->gp_lookups_from_retry, 1);
            peer_entry_t fresh = {0};
            int fresh_count = tree_lookup_peers(tree, entry.infohash, relookup_queue, &fresh);
            if (atomic_load(&tree->shutdown_reason) != SHUTDOWN_REASON_SUPERVISOR && fresh_count > 0) {
                metadata = tree_fetch_metadata_parallel(
                    entry.infohash, fresh.peers, fresh_count, &config);
                if (metadata) atomic_fetch_add(&tree->fresh_peer_rescues, 1);
            }
        }

        /* Handle failure with configurable strike bloom filtering */
        if (!metadata && tree->shared_bloom) {
            if (tree->failure_strike_count <= 1 || !tree->failure_bloom) {
                /* Single-strike: permanently block on first failure */
                bloom_filter_add(tree->shared_bloom, entry.infohash);
                atomic_fetch_add(&tree->second_strike_failures, 1);

                char hex[41];
                for (int i = 0; i < 20; i++) {
                    snprintf(hex + i * 2, 3, "%02x", entry.infohash[i]);
                }
                log_msg(LOG_DEBUG, "[tree %u] Failure for %s - permanently blocked (single-strike)",
                        tree->tree_id, hex);
            } else if (bloom_filter_check(tree->failure_bloom, entry.infohash)) {
                /* Second strike - add to main bloom (permanent block) */
                bloom_filter_add(tree->shared_bloom, entry.infohash);
                atomic_fetch_add(&tree->second_strike_failures, 1);

                char hex[41];
                for (int i = 0; i < 20; i++) {
                    snprintf(hex + i * 2, 3, "%02x", entry.infohash[i]);
                }
                log_msg(LOG_DEBUG, "[tree %u] Second failure for %s - permanently blocked",
                        tree->tree_id, hex);
            } else {
                /* First strike - add to failure bloom (allow retry) */
                bloom_filter_add(tree->failure_bloom, entry.infohash);
                atomic_fetch_add(&tree->first_strike_failures, 1);

                char hex[41];
                for (int i = 0; i < 20; i++) {
                    snprintf(hex + i * 2, 3, "%02x", entry.infohash[i]);
                }
                log_msg(LOG_DEBUG, "[tree %u] First failure for %s - retry allowed",
                        tree->tree_id, hex);
            }
        }

        /* If successful, submit to batch writer */
        if (metadata) {
            /* Update stats */
            atomic_fetch_add(&tree->metadata_count, 1);
            atomic_store(&tree->last_metadata_time, time(NULL));

            /* Record the hash as known NOW, not at batch commit. The commit can
             * be up to batch_size rows / flush_interval seconds away, and every
             * tree sampling this infohash inside that window used to fetch it
             * again - each re-fetch arriving as INSERT OR REPLACE, which the
             * batch writer counted as a new row. The post-commit add in
             * database_insert_batch() is then a no-op. */
            if (tree->shared_bloom) {
                bloom_filter_add(tree->shared_bloom, metadata->infohash);
            }

            log_msg(LOG_DEBUG, "[tree %u] Fetched metadata: %s (%lld bytes, %d files, %d peers)",
                    tree->tree_id, metadata->name,
                    (long long)metadata->total_size, metadata->file_count, entry.peer_count);

            /* Convert to database format */
            torrent_metadata_t db_meta;
            int rc = tree_metadata_to_database_format(metadata, &db_meta, entry.peer_count);

            if (rc == 0) {
                /* Stage 6 filter order, most specific first: the always-on hard
                 * block list, then the language filter, then the classifier's
                 * adult verdict when adult_mode=drop. */
                int filtered = 0;
                if (hard_block_check(db_meta.name)) {
                    filtered = 1;
                    classifier_note_hard_block_dropped();
                    log_msg(LOG_DEBUG, "[tree %u] Filtered torrent (hard block): %s",
                            tree->tree_id, metadata->name);
                } else if (language_filter_check(&db_meta)) {
                    filtered = 1;
                    log_msg(LOG_DEBUG, "[tree %u] Filtered torrent (language): %s", tree->tree_id, metadata->name);
                }

                /* Classify survivors once, before the store/drop split: the
                 * verdict feeds the torrent_class row (tag mode) and the adult
                 * drop decision (drop mode). A failure is not fatal. */
                if (!filtered && tree->shared_batch_writer) {
                    if (classifier_classify(&db_meta, &db_meta.classification) == 0) {
                        db_meta.has_classification = 1;
                    }
                }

                /* Movie-kind (v10): label the same population the backfill
                 * engine's predicate would pick up - a classified movie that is
                 * not adult - but here, before the row is stored, so no pass has
                 * to walk the table afterwards. The mask travels in db_meta like
                 * the fastText verdict: batch_writer_add() deep-copies it and
                 * database.c writes both movie columns inside the same batch
                 * transaction. Deliberately outside db->mutex and outside
                 * batch_writer_add(): movie_kind_classify() takes the module's
                 * own mutex and may load the 478.2 MiB backbone on first use,
                 * which must never happen under the database lock. A failure (1
                 * = no verdict) leaves the row unlabelled, exactly like
                 * classifier_classify(). */
                if (db_meta.has_classification &&
                    db_meta.classification.category == TM_CAT_MOVIE &&
                    db_meta.classification.adult == 0 && movie_kind_enabled()) {
                    uint16_t movie_kind_mask = 0;
                    if (movie_kind_classify(db_meta.name, &movie_kind_mask) == 0) {
                        db_meta.movie_kind_mask = movie_kind_mask;
                        /* Stamped like classification.clf_ver: the generation
                         * the verdict was produced at, never a literal. */
                        db_meta.movie_kind_clf_ver = movie_kind_generation();
                        db_meta.has_movie_kind = 1;
                    }
                }

                /* adult_mode=drop discards exactly what tag mode would hide:
                 * the classifier TAGS a row adult once adult_p reaches the
                 * effective tag threshold (adult_hide_threshold, resolved in
                 * classifier.c), so that tag — not a second, higher threshold —
                 * is the drop boundary. A row in a band below the old value is
                 * therefore dropped here instead of being stored with adult=1
                 * while the operator believed drop mode was active. */
                if (!filtered && db_meta.has_classification &&
                    strcmp(tree->adult_mode, "drop") == 0 &&
                    db_meta.classification.adult == 1) {
                    filtered = 1;
                    classifier_note_adult_dropped();
                    log_msg(LOG_DEBUG, "[tree %u] Filtered torrent (adult %.3f): %s",
                            tree->tree_id, db_meta.classification.adult_p, metadata->name);
                }

                /* "Categories to save": a survivor is dropped when its category
                 * — or, inside the tv/anime categories, its tv_kind — is not in
                 * save_categories/save_tv_kinds. An item the classifier could
                 * not place carries category 0 (unknown) / tv_kind 0 (none),
                 * which are ordinary taxonomy entries here. With the default
                 * all-bits-set masks this branch is inert, so the hot path is
                 * unchanged unless an operator narrowed the selection. */
                if (!filtered &&
                    (tree->save_category_mask != CATEGORY_FILTER_ALL_CATEGORIES ||
                     tree->save_tv_kind_mask != CATEGORY_FILTER_ALL_TV_KINDS)) {
                    int category = db_meta.has_classification ? db_meta.classification.category : 0;
                    int tv_kind = db_meta.has_classification ? db_meta.classification.tv_kind : 0;

                    int verdict = category_filter_should_drop(category, tv_kind,
                                                              tree->save_category_mask,
                                                              tree->save_tv_kind_mask);
                    if (verdict == CATEGORY_FILTER_DROP_CATEGORY) {
                        filtered = 1;
                        category_filter_note_category_dropped();
                        log_msg(LOG_DEBUG, "[tree %u] Filtered torrent (category %d): %s",
                                tree->tree_id, category, metadata->name);
                    } else if (verdict == CATEGORY_FILTER_DROP_TV_KIND) {
                        filtered = 1;
                        category_filter_note_tv_kind_dropped();
                        log_msg(LOG_DEBUG, "[tree %u] Filtered torrent (tv_kind %d): %s",
                                tree->tree_id, tv_kind, metadata->name);
                    }
                }

                if (filtered) {
                    /* Update filtered count statistics */
                    atomic_fetch_add(&tree->filtered_count, 1);

                    /* Add to bloom filter to prevent re-processing */
                    if (tree->shared_bloom) {
                        bloom_filter_add(tree->shared_bloom, metadata->infohash);
                    }

                    /* Free the metadata and skip database insertion */
                    free_database_metadata(&db_meta);
                } else if (tree->shared_batch_writer) {
                    /* The Stage 5 classification already ran above, at the same
                     * point as the filters and only for survivors. The result
                     * travels in db_meta (batch_writer_add deep-copies it) and is
                     * inserted as one torrent_class row inside the same batch
                     * transaction; a classification failure is not fatal. */
                    if (db_meta.has_classification && db_meta.classification.adult) {
                        /* Counted here, not in classifier_classify(): only rows
                         * actually stored with adult=1 are "tagged". */
                        classifier_note_stored_adult();
                    }

                    /* Add to batch writer (batch_writer makes deep copy) */
                    rc = batch_writer_add(tree->shared_batch_writer, &db_meta);
                    if (rc != 0) {
                        char hex[41];
                        for (int i = 0; i < 20; i++) {
                            snprintf(hex + i * 2, 3, "%02x", metadata->infohash[i]);
                        }
                        log_msg(LOG_ERROR, "[tree %u] Failed to add torrent %s to batch writer",
                                tree->tree_id, hex);
                    }

                    /* Free our temporary conversion (batch_writer has its own copy) */
                    free_database_metadata(&db_meta);
                } else {
                    log_msg(LOG_ERROR, "[tree %u] batch_writer unavailable", tree->tree_id);
                    free_database_metadata(&db_meta);
                }
            } else {
                log_msg(LOG_ERROR, "[tree %u] Failed to convert metadata", tree->tree_id);
            }

            /* Free original tree metadata */
            tree_free_metadata(metadata);
        }

        /* The attempt is resolved (stored, filtered, or failed): release the
         * in-flight claim taken at discovery so a later BEP51 sample can retry a
         * hash that was not stored. Hashes that were stored (bloom add above) or
         * dropped (bloom add in the filtered branch) are already in the shared
         * bloom, so this release cannot cause a duplicate fetch. */
        inflight_set_release(tree->shared_inflight, entry.infohash);
    }

    tree_response_queue_destroy(relookup_queue);

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] Metadata worker %d exiting", tree->tree_id, worker_id);
    return NULL;
}

/* Rate monitor thread function
 * Computes and publishes the per-tree EMA metadata rate.
 * Respawn decisions are made by the supervisor monitor thread using the dynamic threshold. */
void *tree_rate_monitor_func(void *arg) {
    rate_monitor_ctx_t *ctx = (rate_monitor_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;

    double alpha = ctx->ema_alpha;
    log_msg(LOG_DEBUG, "[tree %u] Rate monitor started (check_interval=%ds, ema_alpha=%.2f)",
            tree->tree_id, ctx->check_interval_sec, alpha);

    uint64_t last_metadata_count = 0;
    time_t last_check_time = time(NULL);
    double ema_rate = -1.0;  /* Sentinel: -1 means first check (no prior EMA) */

    while (!atomic_load(&tree->shutdown_requested)) {
        /* Sleep in 1-second chunks for responsiveness */
        for (int i = 0; i < ctx->check_interval_sec && !atomic_load(&tree->shutdown_requested); i++) {
            sleep(1);
        }

        if (atomic_load(&tree->shutdown_requested)) break;

        time_t now = time(NULL);

        /* Get current metadata count */
        uint64_t current_metadata_count = atomic_load(&tree->metadata_count);
        double time_delta = difftime(now, last_check_time);

        /* Calculate instantaneous metadata rate */
        uint64_t metadata_delta = current_metadata_count - last_metadata_count;
        double instantaneous_rate = (time_delta > 0) ? (double)metadata_delta / time_delta : 0.0;

        /* Update EMA */
        if (ema_rate < 0.0) {
            ema_rate = instantaneous_rate;  /* First check: seed EMA */
        } else {
            ema_rate = alpha * instantaneous_rate + (1.0 - alpha) * ema_rate;
        }

        /* Publish EMA rate for supervisor and stats endpoint */
        tree->metadata_rate = ema_rate;

        log_msg(LOG_DEBUG, "[tree %u] Metadata rate: instant=%.4f/sec, ema=%.4f/sec (delta=%lu in %.0fs)",
                tree->tree_id, instantaneous_rate, ema_rate,
                (unsigned long)metadata_delta, time_delta);

        last_metadata_count = current_metadata_count;
        last_check_time = now;
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] Rate monitor exiting", tree->tree_id);
    return NULL;
}
