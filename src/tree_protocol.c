#include "tree_protocol.h"
#include "thread_tree.h"
#include "tree_socket.h"
#include "tree_routing.h"
#include "dht_crawler.h"
#include "bencode.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <arpa/inet.h>
#include <stdatomic.h>

/* Simple transaction ID counter */
static uint32_t g_tid_counter = 0;

int tree_protocol_gen_tid(uint8_t *out) {
    uint32_t tid = __sync_fetch_and_add(&g_tid_counter, 1);
    out[0] = (tid >> 24) & 0xff;
    out[1] = (tid >> 16) & 0xff;
    out[2] = (tid >> 8) & 0xff;
    out[3] = tid & 0xff;
    return 4;
}

/* Build a bencode message manually (simple approach) */
static int build_find_node_query(uint8_t *buf, size_t buflen,
                                  const uint8_t *tid, int tid_len,
                                  const uint8_t *node_id,
                                  const uint8_t *target) {
    /* Format: d1:ad2:id20:<node_id>6:target20:<target>e1:q9:find_node1:t<tid_len>:<tid>1:y1:qe */
    int pos = 0;

    /* Start dict */
    buf[pos++] = 'd';

    /* "a" dict with arguments */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'a';
    buf[pos++] = 'd';
    /* id */
    buf[pos++] = '2'; buf[pos++] = ':'; buf[pos++] = 'i'; buf[pos++] = 'd';
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, node_id, 20); pos += 20;
    /* target */
    buf[pos++] = '6'; buf[pos++] = ':'; buf[pos++] = 't'; buf[pos++] = 'a';
    buf[pos++] = 'r'; buf[pos++] = 'g'; buf[pos++] = 'e'; buf[pos++] = 't';
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, target, 20); pos += 20;
    buf[pos++] = 'e';

    /* "q" = method name */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';
    buf[pos++] = '9'; buf[pos++] = ':';
    memcpy(buf + pos, "find_node", 9); pos += 9;

    /* "t" = transaction id */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 't';
    pos += snprintf((char *)buf + pos, buflen - pos, "%d:", tid_len);
    memcpy(buf + pos, tid, tid_len); pos += tid_len;

    /* "y" = type (q for query) */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'y';
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';

    /* End dict */
    buf[pos++] = 'e';

    return pos;
}

static int build_ping_query(uint8_t *buf, size_t buflen,
                            const uint8_t *tid, int tid_len,
                            const uint8_t *node_id) {
    /* Format: d1:ad2:id20:<node_id>e1:q4:ping1:t<tid_len>:<tid>1:y1:qe */
    int pos = 0;

    buf[pos++] = 'd';

    /* "a" dict */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'a';
    buf[pos++] = 'd';
    buf[pos++] = '2'; buf[pos++] = ':'; buf[pos++] = 'i'; buf[pos++] = 'd';
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, node_id, 20); pos += 20;
    buf[pos++] = 'e';

    /* "q" */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';
    buf[pos++] = '4'; buf[pos++] = ':';
    memcpy(buf + pos, "ping", 4); pos += 4;

    /* "t" */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 't';
    pos += snprintf((char *)buf + pos, buflen - pos, "%d:", tid_len);
    memcpy(buf + pos, tid, tid_len); pos += tid_len;

    /* "y" */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'y';
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';

    buf[pos++] = 'e';

    return pos;
}

int tree_send_ping(struct thread_tree *tree, void *sock,
                   const struct sockaddr_storage *dest) {
    if (!tree || !sock || !dest) {
        return -1;
    }

    uint8_t tid[4];
    int tid_len = tree_protocol_gen_tid(tid);

    uint8_t buf[256];
    int len = build_ping_query(buf, sizeof(buf), tid, tid_len, tree->node_id);
    if (len <= 0) {
        return -1;
    }

    return tree_socket_send((tree_socket_t *)sock, buf, len, dest);
}

int tree_send_find_node(struct thread_tree *tree, void *sock,
                        const uint8_t *tid, int tid_len,
                        const uint8_t *target,
                        const struct sockaddr_storage *dest) {
    if (!tree || !sock || !tid || tid_len <= 0 || !target || !dest) {
        return -1;
    }

    uint8_t buf[256];
    int len = build_find_node_query(buf, sizeof(buf), tid, tid_len,
                                     tree->node_id, target);
    if (len <= 0) {
        return -1;
    }

    return tree_socket_send((tree_socket_t *)sock, buf, len, dest);
}

int tree_parse_compact_nodes(const uint8_t *compact, size_t compact_len,
                              uint8_t out_ids[][20],
                              struct sockaddr_storage *out_addrs,
                              int max_nodes) {
    if (!compact || !out_ids || !out_addrs) {
        return 0;
    }

    int node_count = 0;
    size_t pos = 0;

    /* Each node is 26 bytes: 20 (id) + 4 (ip) + 2 (port) */
    while (pos + 26 <= compact_len && node_count < max_nodes) {
        /* Copy node ID */
        memcpy(out_ids[node_count], compact + pos, 20);
        pos += 20;

        /* Parse IPv4 address */
        struct sockaddr_in *addr = (struct sockaddr_in *)&out_addrs[node_count];
        memset(addr, 0, sizeof(*addr));
        addr->sin_family = AF_INET;
        memcpy(&addr->sin_addr.s_addr, compact + pos, 4);
        pos += 4;
        memcpy(&addr->sin_port, compact + pos, 2);  /* Already in network order */
        pos += 2;

        /* Validate address: skip invalid IPs (0.0.0.0, 255.255.255.255) and port 0 */
        uint32_t ip = ntohl(addr->sin_addr.s_addr);
        uint16_t port = ntohs(addr->sin_port);
        if (ip == 0 || ip == 0xFFFFFFFF || port == 0) {
            /* Invalid address, skip this node */
            continue;
        }

        node_count++;
    }

    return node_count;
}

tree_response_type_t tree_handle_response(struct thread_tree *tree,
                                           const uint8_t *data, size_t len,
                                           const struct sockaddr_storage *from,
                                           tree_find_node_response_t *out_response) {
    if (!tree || !data || len == 0) {
        return TREE_RESP_NONE;
    }

    struct bencode bc;
    bencode_init(&bc, data, len);

    int type = bencode_next(&bc);
    if (type != BENCODE_DICT_BEGIN) {
        bencode_free(&bc);
        return TREE_RESP_NONE;
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
        return TREE_RESP_NONE;
    }

    /* If we got nodes, parse them */
    if (nodes_data && nodes_len > 0 && out_response) {
        out_response->node_count = tree_parse_compact_nodes(
            nodes_data, nodes_len,
            out_response->nodes, out_response->addrs, 256);

        /* Add sender to routing table if we have their ID */
        if (sender_id && tree->routing_table) {
            tree_routing_add_node((tree_routing_table_t *)tree->routing_table,
                                  sender_id, from);
        }

        return TREE_RESP_FIND_NODE;
    }

    /* Ping response (has id but no nodes) */
    if (sender_id) {
        if (tree->routing_table) {
            tree_routing_add_node((tree_routing_table_t *)tree->routing_table,
                                  sender_id, from);
        }
        return TREE_RESP_PONG;
    }

    return TREE_RESP_NONE;
}

/* Build sample_infohashes query (BEP51) */
static int build_sample_infohashes_query(uint8_t *buf, size_t buflen,
                                          const uint8_t *tid, int tid_len,
                                          const uint8_t *node_id,
                                          const uint8_t *target) {
    /* Format: d1:ad2:id20:<node_id>6:target20:<target>e1:q17:sample_infohashes1:t<tid_len>:<tid>1:y1:qe */
    int pos = 0;

    buf[pos++] = 'd';

    /* "a" dict with arguments */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'a';
    buf[pos++] = 'd';
    /* id */
    buf[pos++] = '2'; buf[pos++] = ':'; buf[pos++] = 'i'; buf[pos++] = 'd';
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, node_id, 20); pos += 20;
    /* target */
    buf[pos++] = '6'; buf[pos++] = ':'; buf[pos++] = 't'; buf[pos++] = 'a';
    buf[pos++] = 'r'; buf[pos++] = 'g'; buf[pos++] = 'e'; buf[pos++] = 't';
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, target, 20); pos += 20;
    buf[pos++] = 'e';

    /* "q" = method name */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';
    buf[pos++] = '1'; buf[pos++] = '7'; buf[pos++] = ':';
    memcpy(buf + pos, "sample_infohashes", 17); pos += 17;

    /* "t" = transaction id */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 't';
    pos += snprintf((char *)buf + pos, buflen - pos, "%d:", tid_len);
    memcpy(buf + pos, tid, tid_len); pos += tid_len;

    /* "y" = type (q for query) */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'y';
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';

    buf[pos++] = 'e';

    return pos;
}

int tree_send_sample_infohashes(struct thread_tree *tree, void *sock,
                                 const uint8_t *tid, int tid_len,
                                 const uint8_t *target,
                                 const struct sockaddr_storage *dest) {
    if (!tree || !sock || !tid || tid_len <= 0 || !target || !dest) {
        return -1;
    }

    uint8_t buf[256];
    int len = build_sample_infohashes_query(buf, sizeof(buf), tid, tid_len,
                                             tree->node_id, target);
    if (len <= 0) {
        return -1;
    }

    return tree_socket_send((tree_socket_t *)sock, buf, len, dest);
}

/* Build get_peers query (Stage 4) */
static int build_get_peers_query(uint8_t *buf, size_t buflen,
                                  const uint8_t *tid, int tid_len,
                                  const uint8_t *node_id,
                                  const uint8_t *infohash) {
    /* Format: d1:ad2:id20:<node_id>9:info_hash20:<infohash>e1:q9:get_peers1:t<tid_len>:<tid>1:y1:qe */
    int pos = 0;

    buf[pos++] = 'd';

    /* "a" dict with arguments */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'a';
    buf[pos++] = 'd';
    /* id */
    buf[pos++] = '2'; buf[pos++] = ':'; buf[pos++] = 'i'; buf[pos++] = 'd';
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, node_id, 20); pos += 20;
    /* info_hash */
    buf[pos++] = '9'; buf[pos++] = ':';
    memcpy(buf + pos, "info_hash", 9); pos += 9;
    buf[pos++] = '2'; buf[pos++] = '0'; buf[pos++] = ':';
    memcpy(buf + pos, infohash, 20); pos += 20;
    buf[pos++] = 'e';

    /* "q" = method name */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';
    buf[pos++] = '9'; buf[pos++] = ':';
    memcpy(buf + pos, "get_peers", 9); pos += 9;

    /* "t" = transaction id */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 't';
    pos += snprintf((char *)buf + pos, buflen - pos, "%d:", tid_len);
    memcpy(buf + pos, tid, tid_len); pos += tid_len;

    /* "y" = type (q for query) */
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'y';
    buf[pos++] = '1'; buf[pos++] = ':'; buf[pos++] = 'q';

    buf[pos++] = 'e';

    return pos;
}

int tree_send_get_peers(struct thread_tree *tree, void *sock,
                        const uint8_t *tid, int tid_len,
                        const uint8_t *infohash,
                        const struct sockaddr_storage *dest) {
    if (!tree || !sock || !tid || tid_len <= 0 || !infohash || !dest) {
        return -1;
    }

    uint8_t buf[256];
    int len = build_get_peers_query(buf, sizeof(buf), tid, tid_len,
                                     tree->node_id, infohash);
    if (len <= 0) {
        return -1;
    }

    return tree_socket_send((tree_socket_t *)sock, buf, len, dest);
}

int tree_parse_compact_peers(const uint8_t *compact, size_t compact_len,
                              struct sockaddr_storage *out_addrs,
                              int max_peers) {
    if (!compact || !out_addrs) {
        return 0;
    }

    int peer_count = 0;
    size_t pos = 0;

    /* Each peer is 6 bytes: 4 (ip) + 2 (port) */
    while (pos + 6 <= compact_len && peer_count < max_peers) {
        struct sockaddr_in *addr = (struct sockaddr_in *)&out_addrs[peer_count];
        memset(addr, 0, sizeof(*addr));
        addr->sin_family = AF_INET;
        memcpy(&addr->sin_addr.s_addr, compact + pos, 4);
        pos += 4;
        memcpy(&addr->sin_port, compact + pos, 2);  /* Already in network order */
        pos += 2;

        /* Validate address: skip invalid IPs (0.0.0.0, 255.255.255.255) and port 0 */
        uint32_t ip = ntohl(addr->sin_addr.s_addr);
        uint16_t port = ntohs(addr->sin_port);
        if (ip == 0 || ip == 0xFFFFFFFF || port == 0) {
            /* Invalid address, skip this peer */
            continue;
        }

        peer_count++;
    }

    return peer_count;
}

int tree_handle_get_peers_response(struct thread_tree *tree,
                                   const uint8_t *data, size_t len,
                                   const struct sockaddr_storage *from,
                                   tree_get_peers_response_t *out_response) {
    if (!tree || !data || len == 0 || !out_response) {
        return -1;
    }

    memset(out_response, 0, sizeof(*out_response));

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
                                    int parsed = tree_parse_compact_peers(
                                        bc.tok, bc.toklen,
                                        &out_response->peers[out_response->peer_count],
                                        50 - out_response->peer_count);
                                    out_response->peer_count += parsed;
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
        out_response->node_count = tree_parse_compact_nodes(
            nodes_data, nodes_len,
            out_response->nodes, out_response->node_addrs, 256);
    }

    /* Copy token if present */
    if (token_data && token_len > 0 && token_len <= sizeof(out_response->token)) {
        memcpy(out_response->token, token_data, token_len);
        out_response->token_len = token_len;
    }

    /* Add sender to routing table */
    if (sender_id && tree->routing_table) {
        tree_routing_add_node((tree_routing_table_t *)tree->routing_table,
                              sender_id, from);
    }

    return 0;
}

int tree_handle_sample_infohashes_response(struct thread_tree *tree,
                                            const uint8_t *data, size_t len,
                                            const struct sockaddr_storage *from,
                                            tree_sample_response_t *out_response) {
    if (!tree || !data || len == 0 || !out_response) {
        return -1;
    }

    memset(out_response, 0, sizeof(*out_response));

    struct bencode bc;
    bencode_init(&bc, data, len);

    int type = bencode_next(&bc);
    if (type != BENCODE_DICT_BEGIN) {
        bencode_free(&bc);
        return -1;
    }

    const char *response_type = NULL;
    const uint8_t *samples_data = NULL;
    size_t samples_len = 0;
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

                        if (rkeylen == 7 && memcmp(rkey, "samples", 7) == 0 && type == BENCODE_STRING) {
                            samples_data = bc.tok;
                            samples_len = bc.toklen;
                        } else if (rkeylen == 5 && memcmp(rkey, "nodes", 5) == 0 && type == BENCODE_STRING) {
                            nodes_data = bc.tok;
                            nodes_len = bc.toklen;
                        } else if (rkeylen == 2 && memcmp(rkey, "id", 2) == 0 && type == BENCODE_STRING) {
                            if (bc.toklen == 20) {
                                sender_id = bc.tok;
                                out_response->sender_id = sender_id;  /* NEW: Store in response */
                            }
                        } else if (rkeylen == 8 && memcmp(rkey, "interval", 8) == 0 && type == BENCODE_INTEGER) {
                            /* Parse integer from token */
                            char buf[32];
                            size_t copylen = bc.toklen < 31 ? bc.toklen : 31;
                            memcpy(buf, bc.tok, copylen);
                            buf[copylen] = '\0';
                            out_response->interval = atoi(buf);
                        } else if (rkeylen == 3 && memcmp(rkey, "num", 3) == 0 && type == BENCODE_INTEGER) {
                            char buf[32];
                            size_t copylen = bc.toklen < 31 ? bc.toklen : 31;
                            memcpy(buf, bc.tok, copylen);
                            buf[copylen] = '\0';
                            out_response->num = atoi(buf);
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

    /* Parse samples (20 bytes each) */
    if (samples_data && samples_len > 0) {
        int count = 0;
        for (size_t i = 0; i + 20 <= samples_len && count < 100; i += 20) {
            memcpy(out_response->infohashes[count], samples_data + i, 20);
            count++;
        }
        out_response->infohash_count = count;
    }

    /* Parse nodes */
    if (nodes_data && nodes_len > 0) {
        out_response->node_count = tree_parse_compact_nodes(
            nodes_data, nodes_len,
            out_response->nodes, out_response->addrs, 256);
    }

    /* Add sender to routing table */
    if (sender_id && tree->routing_table) {
        tree_routing_add_node((tree_routing_table_t *)tree->routing_table,
                              sender_id, from);
    }

    return 0;
}

/* Helper function to skip over a bencode value */
static void skip_bencode_value(struct bencode *bc, int value_type) {
    int type;
    if (value_type == BENCODE_DICT_BEGIN) {
        /* Skip entire dictionary */
        int depth = 1;
        while (depth > 0 && (type = bencode_next(bc)) > 0) {
            if (type == BENCODE_DICT_BEGIN || type == BENCODE_LIST_BEGIN) {
                depth++;
            } else if (type == BENCODE_DICT_END || type == BENCODE_LIST_END) {
                depth--;
            }
        }
    } else if (value_type == BENCODE_LIST_BEGIN) {
        /* Skip entire list */
        int depth = 1;
        while (depth > 0 && (type = bencode_next(bc)) > 0) {
            if (type == BENCODE_DICT_BEGIN || type == BENCODE_LIST_BEGIN) {
                depth++;
            } else if (type == BENCODE_DICT_END || type == BENCODE_LIST_END) {
                depth--;
            }
        }
    }
    /* For STRING and INTEGER, they're already consumed by bencode_next() */
}

int tree_protocol_extract_tid(const uint8_t *data, size_t len,
                               uint8_t *out_tid, int *out_tid_len) {
    if (!data || len == 0 || !out_tid || !out_tid_len) {
        return -1;
    }

    *out_tid_len = 0;

    /* Parse bencode to extract transaction ID ("t" key) */
    struct bencode bc;
    bencode_init(&bc, data, len);

    int type = bencode_next(&bc);
    if (type != BENCODE_DICT_BEGIN) {
        /* DEBUG: Not a dict */
        log_msg(LOG_DEBUG, "[tree_protocol] TID extract failed: not a bencode dict (type=%d, first_byte=0x%02x)",
                type, len > 0 ? data[0] : 0);
        bencode_free(&bc);
        return -1;
    }

    /* DEBUG: Log all keys we see in the dict */
    static atomic_int debug_count = 0;
    int count = atomic_fetch_add(&debug_count, 1) + 1;
    bool log_this = (count <= 20);  /* Log first 20 failures */

    if (log_this) {
        log_msg(LOG_DEBUG, "[tree_protocol] Parsing bencode dict (len=%zu), scanning for 't' key...", len);
    }

    /* Scan for "t" key at TOP LEVEL only */
    int keys_seen = 0;
    while ((type = bencode_next(&bc)) != BENCODE_DICT_END && type > 0) {
        if (type == BENCODE_STRING) {
            const char *key = bc.tok;
            size_t keylen = bc.toklen;

            type = bencode_next(&bc);
            if (type < 0) break;

            /* DEBUG: Log keys we find */
            if (log_this && keys_seen < 10) {
                char key_str[32] = {0};
                size_t copy_len = keylen < 31 ? keylen : 31;
                memcpy(key_str, key, copy_len);
                log_msg(LOG_DEBUG, "[tree_protocol]   Found key: '%s' (len=%zu, value_type=%d)",
                        key_str, keylen, type);
            }
            keys_seen++;

            if (keylen == 1 && key[0] == 't' && type == BENCODE_STRING) {
                /* Found transaction ID */
                int tid_len = (int)bc.toklen;
                if (tid_len > 4) {
                    tid_len = 4;  /* Limit to 4 bytes */
                }
                memcpy(out_tid, bc.tok, tid_len);
                *out_tid_len = tid_len;
                if (log_this) {
                    log_msg(LOG_DEBUG, "[tree_protocol] Found 't' key! tid_len=%d", tid_len);
                }
                bencode_free(&bc);
                return 0;
            } else {
                /* Not the 't' key - skip over the value if it's a dict/list */
                skip_bencode_value(&bc, type);
            }
        }
    }

    if (log_this) {
        log_msg(LOG_DEBUG, "[tree_protocol] TID not found after scanning %d keys", keys_seen);
    }

    bencode_free(&bc);
    return -1;  /* TID not found */
}
