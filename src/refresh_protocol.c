#include "refresh_protocol.h"
#include "protocol_utils.h"
#include "tree_socket.h"
#include "tree_protocol.h"
#include "bencode.h"
#include <stdlib.h>
#include <string.h>

/* ========================================================================== */
/*                          SEND FUNCTIONS                                    */
/* ========================================================================== */

int refresh_send_ping(const uint8_t node_id[20],
                      tree_socket_t *sock,
                      const struct sockaddr_storage *dest) {
    if (!node_id || !sock || !dest) {
        return -1;
    }

    /* Generate TID */
    uint8_t tid[4];
    int tid_len = tree_protocol_gen_tid(tid);

    /* Build query */
    uint8_t buf[256];
    int len = build_ping_query(buf, sizeof(buf), tid, tid_len, node_id);
    if (len <= 0) {
        return -1;
    }

    /* Send */
    return tree_socket_send(sock, buf, len, dest);
}

int refresh_send_find_node(const uint8_t node_id[20],
                           tree_socket_t *sock,
                           const uint8_t *tid, int tid_len,
                           const uint8_t target[20],
                           const struct sockaddr_storage *dest) {
    if (!node_id || !sock || !tid || tid_len <= 0 || !target || !dest) {
        return -1;
    }

    /* Build query */
    uint8_t buf[256];
    int len = build_find_node_query(buf, sizeof(buf), tid, tid_len, node_id, target);
    if (len <= 0) {
        return -1;
    }

    /* Send */
    return tree_socket_send(sock, buf, len, dest);
}

int refresh_send_get_peers(const uint8_t node_id[20],
                          tree_socket_t *sock,
                          const uint8_t *tid, int tid_len,
                          const uint8_t infohash[20],
                          const struct sockaddr_storage *dest) {
    if (!node_id || !sock || !tid || tid_len <= 0 || !infohash || !dest) {
        return -1;
    }

    /* Build query */
    uint8_t buf[256];
    int len = build_get_peers_query(buf, sizeof(buf), tid, tid_len, node_id, infohash);
    if (len <= 0) {
        return -1;
    }

    /* Send */
    return tree_socket_send(sock, buf, len, dest);
}

/* ========================================================================== */
/*                          PARSE FUNCTIONS                                   */
/* ========================================================================== */

int refresh_parse_find_node_response(const uint8_t *data, size_t len,
                                     const struct sockaddr_storage *from,
                                     refresh_find_node_response_t *out) {
    if (!data || len == 0 || !out) {
        return -1;
    }

    memset(out, 0, sizeof(*out));

    struct bencode bc;
    bencode_init(&bc, data, len);

    int type = bencode_next(&bc);
    if (type != BENCODE_DICT_BEGIN) {
        bencode_free(&bc);
        return -1;
    }

    const char *response_type = NULL;
    const uint8_t *nodes_data = NULL;
    size_t nodes_len = 0;
    const uint8_t *sender_id = NULL;

    /* Parse the response dictionary */
    while ((type = bencode_next(&bc)) != BENCODE_DICT_END && type > 0) {
        if (type == BENCODE_STRING) {
            const char *key = bc.tok;
            size_t keylen = bc.toklen;

            type = bencode_next(&bc);
            if (type < 0) break;

            if (keylen == 1 && key[0] == 'y' && type == BENCODE_STRING) {
                response_type = bc.tok;
            } else if (keylen == 1 && key[0] == 'r' && type == BENCODE_DICT_BEGIN) {
                /* Parse response dict */
                while ((type = bencode_next(&bc)) != BENCODE_DICT_END && type > 0) {
                    if (type == BENCODE_STRING) {
                        const char *rkey = bc.tok;
                        size_t rkeylen = bc.toklen;

                        type = bencode_next(&bc);
                        if (type < 0) break;

                        if (rkeylen == 5 && memcmp(rkey, "nodes", 5) == 0 && type == BENCODE_STRING) {
                            nodes_data = bc.tok;
                            nodes_len = bc.toklen;
                        } else if (rkeylen == 2 && memcmp(rkey, "id", 2) == 0 && type == BENCODE_STRING) {
                            if (bc.toklen == 20) {
                                sender_id = bc.tok;
                            }
                        }
                    }
                }
            }
        }
    }

    bencode_free(&bc);

    /* Check response type */
    if (!response_type || response_type[0] != 'r') {
        return -1;
    }

    /* Parse nodes */
    if (nodes_data && nodes_len > 0) {
        out->node_count = parse_compact_nodes(
            nodes_data, nodes_len,
            out->nodes, out->addrs, 256);
    }

    /* Copy sender ID if present */
    if (sender_id) {
        memcpy(out->sender_id, sender_id, 20);
        out->has_sender_id = true;
    }

    return 0;
}

int refresh_parse_get_peers_response(const uint8_t *data, size_t len,
                                     const struct sockaddr_storage *from,
                                     refresh_get_peers_response_t *out) {
    if (!data || len == 0 || !out) {
        return -1;
    }

    memset(out, 0, sizeof(*out));

    struct bencode bc;
    bencode_init(&bc, data, len);

    int type = bencode_next(&bc);
    if (type != BENCODE_DICT_BEGIN) {
        bencode_free(&bc);
        return -1;
    }

    const char *response_type = NULL;
    const uint8_t *nodes_data = NULL;
    size_t nodes_len = 0;
    const uint8_t *token_data = NULL;
    size_t token_len = 0;
    const uint8_t *sender_id = NULL;

    /* Parse the response dictionary */
    while ((type = bencode_next(&bc)) != BENCODE_DICT_END && type > 0) {
        if (type == BENCODE_STRING) {
            const char *key = bc.tok;
            size_t keylen = bc.toklen;

            type = bencode_next(&bc);
            if (type < 0) break;

            if (keylen == 1 && key[0] == 'y' && type == BENCODE_STRING) {
                response_type = bc.tok;
            } else if (keylen == 1 && key[0] == 'r' && type == BENCODE_DICT_BEGIN) {
                /* Parse response dict */
                while ((type = bencode_next(&bc)) != BENCODE_DICT_END && type > 0) {
                    if (type == BENCODE_STRING) {
                        const char *rkey = bc.tok;
                        size_t rkeylen = bc.toklen;

                        type = bencode_next(&bc);
                        if (type < 0) break;

                        if (rkeylen == 6 && memcmp(rkey, "values", 6) == 0 && type == BENCODE_LIST_BEGIN) {
                            /* Parse values list - each item is a compact peer string */
                            while ((type = bencode_next(&bc)) != BENCODE_LIST_END && type > 0) {
                                if (type == BENCODE_STRING && bc.toklen >= 6) {
                                    /* Parse compact peers from this string */
                                    int parsed = parse_compact_peers(
                                        bc.tok, bc.toklen,
                                        &out->peers[out->peer_count],
                                        50 - out->peer_count);
                                    out->peer_count += parsed;
                                }
                            }
                        } else if (rkeylen == 5 && memcmp(rkey, "nodes", 5) == 0 && type == BENCODE_STRING) {
                            nodes_data = bc.tok;
                            nodes_len = bc.toklen;
                        } else if (rkeylen == 5 && memcmp(rkey, "token", 5) == 0 && type == BENCODE_STRING) {
                            token_data = bc.tok;
                            token_len = bc.toklen;
                        } else if (rkeylen == 2 && memcmp(rkey, "id", 2) == 0 && type == BENCODE_STRING) {
                            if (bc.toklen == 20) {
                                sender_id = bc.tok;
                            }
                        }
                    }
                }
            }
        }
    }

    bencode_free(&bc);

    /* Verify it's a response */
    if (!response_type || response_type[0] != 'r') {
        return -1;
    }

    /* Parse nodes (for iterative lookup) */
    if (nodes_data && nodes_len > 0) {
        out->node_count = parse_compact_nodes(
            nodes_data, nodes_len,
            out->nodes, out->node_addrs, 256);
    }

    /* Copy token if present */
    if (token_data && token_len > 0 && token_len <= sizeof(out->token)) {
        memcpy(out->token, token_data, token_len);
        out->token_len = token_len;
    }

    /* Copy sender ID if present */
    if (sender_id) {
        memcpy(out->sender_id, sender_id, 20);
        out->has_sender_id = true;
    }

    return 0;
}
