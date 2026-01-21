#include "protocol_utils.h"
#include <string.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* Build a bencode ping query manually */
int build_ping_query(uint8_t *buf, size_t buflen,
                    const uint8_t *tid, int tid_len,
                    const uint8_t *node_id) {
    if (!buf || !tid || !node_id || tid_len <= 0) {
        return -1;
    }

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

/* Build a bencode find_node query manually */
int build_find_node_query(uint8_t *buf, size_t buflen,
                         const uint8_t *tid, int tid_len,
                         const uint8_t *node_id,
                         const uint8_t *target) {
    if (!buf || !tid || !node_id || !target || tid_len <= 0) {
        return -1;
    }

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

/* Build a bencode get_peers query manually */
int build_get_peers_query(uint8_t *buf, size_t buflen,
                         const uint8_t *tid, int tid_len,
                         const uint8_t *node_id,
                         const uint8_t *infohash) {
    if (!buf || !tid || !node_id || !infohash || tid_len <= 0) {
        return -1;
    }

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

/* Parse compact node info (26 bytes per node: 20 id + 4 ip + 2 port) */
int parse_compact_nodes(const uint8_t *compact, size_t compact_len,
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

/* Parse compact peer info (6 bytes per peer: 4 ip + 2 port) */
int parse_compact_peers(const uint8_t *compact, size_t compact_len,
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
