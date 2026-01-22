#define _DEFAULT_SOURCE  /* For usleep */

#include "tree_metadata.h"
#include "thread_tree.h"
#include "tree_peers_queue.h"
#include "tree_infohash_queue.h"
#include "batch_writer.h"
#include "bloom_filter.h"
#include "dht_crawler.h"
#include "database.h"
#include "porn_filter.h"
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

/* Default config values */
#define DEFAULT_TCP_TIMEOUT_MS 5000
#define DEFAULT_METADATA_TIMEOUT_MS 30000

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

/* Main fetch function */
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

                        /* Allocate buffers */
                        total_pieces = (metadata_size + BT_METADATA_PIECE_SIZE - 1) / BT_METADATA_PIECE_SIZE;
                        metadata_buffer = calloc(1, metadata_size);
                        pieces_bitmap = calloc(1, (total_pieces + 7) / 8);

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
                                            /* Parse and build result */
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
    if (metadata_buffer) free(metadata_buffer);
    if (pieces_bitmap) free(pieces_bitmap);

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
        .tcp_connect_timeout_ms = 5000,
        .metadata_timeout_ms = 30000,
        .max_metadata_size = 10 * 1024 * 1024,
        .tree = tree
    };

    while (!atomic_load(&tree->shutdown_requested)) {
        /* Pop peer entry from queue */
        peer_entry_t entry;
        if (tree_peers_queue_pop(peers_queue, &entry, 1000) < 0) {
            continue;
        }

        /* Track that we're attempting to fetch this infohash */
        atomic_fetch_add(&tree->metadata_attempts, 1);

        /* Try each peer until metadata fetched
         * For RATE-BASED shutdowns (respawn): try ALL peers before giving up,
         * so the infohash isn't lost when the tree respawns.
         * For SUPERVISOR shutdowns (program exit): break immediately for fast exit. */
        tree_torrent_metadata_t *metadata = NULL;
        for (int i = 0; i < entry.peer_count && !metadata; i++) {
            /* Allow immediate exit during program shutdown (Ctrl+C) */
            if (tree->shutdown_reason == SHUTDOWN_REASON_SUPERVISOR) {
                break;
            }
            metadata = tree_fetch_metadata_from_peer(entry.infohash, &entry.peers[i], &config);
        }

        /* Exit worker loop immediately on supervisor shutdown */
        if (tree->shutdown_reason == SHUTDOWN_REASON_SUPERVISOR) {
            break;
        }

        /* Handle failure with two-strike bloom filtering */
        if (!metadata && tree->failure_bloom && tree->shared_bloom) {
            /* All peers exhausted - apply two-strike logic */
            if (bloom_filter_check(tree->failure_bloom, entry.infohash)) {
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

            log_msg(LOG_DEBUG, "[tree %u] Fetched metadata: %s (%lld bytes, %d files, %d peers)",
                    tree->tree_id, metadata->name,
                    (long long)metadata->total_size, metadata->file_count, entry.peer_count);

            /* Convert to database format */
            torrent_metadata_t db_meta;
            int rc = tree_metadata_to_database_format(metadata, &db_meta, entry.peer_count);

            if (rc == 0) {
                /* Check porn filter if enabled */
                if (tree->porn_filter_enabled && porn_filter_check(&db_meta)) {
                    /* Update filtered count statistics */
                    atomic_fetch_add(&tree->filtered_count, 1);

                    log_msg(LOG_DEBUG, "[tree %u] Filtered torrent: %s", tree->tree_id, metadata->name);

                    /* Free the metadata and skip database insertion */
                    free_database_metadata(&db_meta);
                } else if (tree->shared_batch_writer) {
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
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] Metadata worker %d exiting", tree->tree_id, worker_id);
    return NULL;
}

/* Rate monitor thread function */
void *tree_rate_monitor_func(void *arg) {
    rate_monitor_ctx_t *ctx = (rate_monitor_ctx_t *)arg;
    thread_tree_t *tree = ctx->tree;

    log_msg(LOG_DEBUG, "[tree %u] Rate monitor started (min_rate=%.4f, check_interval=%ds)",
            tree->tree_id, ctx->min_metadata_rate, ctx->check_interval_sec);

    uint64_t last_metadata_count = 0;
    time_t last_check_time = time(NULL);

    while (!atomic_load(&tree->shutdown_requested)) {
        /* Sleep in 1-second chunks for responsiveness */
        for (int i = 0; i < ctx->check_interval_sec && !atomic_load(&tree->shutdown_requested); i++) {
            sleep(1);
        }

        if (atomic_load(&tree->shutdown_requested)) break;

        time_t now = time(NULL);
        double tree_age = difftime(now, tree->creation_time);

        /* Get current metadata count */
        uint64_t current_metadata_count = atomic_load(&tree->metadata_count);
        double time_delta = difftime(now, last_check_time);

        /* Calculate metadata rate (always, so stats endpoint shows it) */
        uint64_t metadata_delta = current_metadata_count - last_metadata_count;
        double metadata_rate = (time_delta > 0) ? (double)metadata_delta / time_delta : 0.0;
        tree->metadata_rate = metadata_rate;

        /* Check minimum lifetime immunity - still calculate rate but don't act on it */
        if (tree_age < ctx->min_lifetime_sec) {
            log_msg(LOG_DEBUG, "[tree %u] Age %.0fs < min %ds - IMMUNE (rate: %.4f/sec)",
                    tree->tree_id, tree_age, ctx->min_lifetime_sec, metadata_rate);
            last_metadata_count = current_metadata_count;
            last_check_time = now;
            continue;
        }

        log_msg(LOG_DEBUG, "[tree %u] Metadata rate: %.4f/sec (threshold: %.4f/sec, delta=%lu in %.0fs)",
                tree->tree_id, metadata_rate, ctx->min_metadata_rate,
                (unsigned long)metadata_delta, time_delta);

        /* Check if rate is below threshold */
        if (metadata_rate < ctx->min_metadata_rate) {
            /* Check queue requirement */
            if (ctx->require_empty_queue) {
                int queue_size = tree_infohash_queue_count(tree->infohash_queue);
                if (queue_size > 0) {
                    log_msg(LOG_DEBUG, "[tree %u] Metadata rate %.4f < %.4f, but queue not empty (%d) - CONTINUING",
                            tree->tree_id, metadata_rate, ctx->min_metadata_rate, queue_size);
                    last_metadata_count = current_metadata_count;
                    last_check_time = now;
                    continue;
                }
            }

            log_msg(LOG_WARN, "[tree %u] Metadata rate %.4f < threshold %.4f, entering grace period (%ds)",
                    tree->tree_id, metadata_rate, ctx->min_metadata_rate, ctx->grace_period_sec);

            /* Grace period - sleep in 1s chunks */
            for (int i = 0; i < ctx->grace_period_sec && !atomic_load(&tree->shutdown_requested); i++) {
                sleep(1);
            }

            if (atomic_load(&tree->shutdown_requested)) break;

            /* Re-check after grace period */
            time_t now2 = time(NULL);
            uint64_t current_metadata_count2 = atomic_load(&tree->metadata_count);
            double time_delta2 = difftime(now2, last_check_time);
            uint64_t metadata_delta2 = current_metadata_count2 - last_metadata_count;
            double metadata_rate2 = (time_delta2 > 0) ? (double)metadata_delta2 / time_delta2 : 0.0;

            if (metadata_rate2 < ctx->min_metadata_rate) {
                /* Check queue requirement again */
                if (ctx->require_empty_queue) {
                    int queue_size = tree_infohash_queue_count(tree->infohash_queue);
                    if (queue_size > 0) {
                        log_msg(LOG_DEBUG, "[tree %u] Metadata rate STILL %.4f < %.4f after grace period, but queue not empty (%d) - CONTINUING",
                                tree->tree_id, metadata_rate2, ctx->min_metadata_rate, queue_size);
                        last_metadata_count = current_metadata_count2;
                        last_check_time = now2;
                        continue;
                    }
                }

                log_msg(LOG_INFO, "[tree %u] Metadata rate SUSTAINED at %.4f < %.4f after grace period - REQUESTING SHUTDOWN",
                        tree->tree_id, metadata_rate2, ctx->min_metadata_rate);
                thread_tree_request_shutdown(tree, SHUTDOWN_REASON_RATE_BASED);
                break;
            } else {
                log_msg(LOG_DEBUG, "[tree %u] Metadata rate improved to %.4f - CONTINUING",
                        tree->tree_id, metadata_rate2);
            }
        }

        last_metadata_count = current_metadata_count;
        last_check_time = now;
    }

    free(ctx);
    log_msg(LOG_DEBUG, "[tree %u] Rate monitor exiting", tree->tree_id);
    return NULL;
}
