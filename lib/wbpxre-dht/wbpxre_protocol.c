/*
 * wbpxre-dht: DHT Protocol Implementation
 * Implements BEP 5 (DHT Protocol) and BEP 51 (sample_infohashes)
 */

#include "wbpxre_dht.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

/* Include bencode library */
#include "bencode.h"

/* Forward declarations */
static int send_query(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                      const wbpxre_message_t *msg);
static wbpxre_message_t *wait_for_response_msg(wbpxre_dht_t *dht,
                                                 const uint8_t *transaction_id,
                                                 int timeout_sec);

/* ============================================================================
 * Bencode Encoding/Decoding
 * ============================================================================ */

int wbpxre_encode_message(const wbpxre_message_t *msg, uint8_t *buf, int buf_len) {
    /* Build bencoded dictionary manually */
    /* Format: d1:t2:XX1:y1:q1:q<method>1:ad2:id20:...ee */

    int pos = 0;

    /* Start dict */
    buf[pos++] = 'd';

    /* Transaction ID */
    pos += snprintf((char *)buf + pos, buf_len - pos, "1:t%d:", WBPXRE_TRANSACTION_ID_LEN);
    memcpy(buf + pos, msg->transaction_id, WBPXRE_TRANSACTION_ID_LEN);
    pos += WBPXRE_TRANSACTION_ID_LEN;

    /* Message type */
    if (msg->type == WBPXRE_MSG_QUERY) {
        buf[pos++] = '1';
        buf[pos++] = ':';
        buf[pos++] = 'y';
        buf[pos++] = '1';
        buf[pos++] = ':';
        buf[pos++] = 'q';

        /* Query method */
        buf[pos++] = '1';
        buf[pos++] = ':';
        buf[pos++] = 'q';

        const char *method_name = NULL;
        switch (msg->method) {
            case WBPXRE_METHOD_PING:
                method_name = "ping";
                break;
            case WBPXRE_METHOD_FIND_NODE:
                method_name = "find_node";
                break;
            case WBPXRE_METHOD_GET_PEERS:
                method_name = "get_peers";
                break;
            case WBPXRE_METHOD_ANNOUNCE_PEER:
                method_name = "announce_peer";
                break;
            case WBPXRE_METHOD_SAMPLE_INFOHASHES:
                method_name = "sample_infohashes";
                break;
        }

        if (method_name) {
            int len = strlen(method_name);
            pos += snprintf((char *)buf + pos, buf_len - pos, "%d:%s", len, method_name);
        }

        /* Arguments dict */
        buf[pos++] = '1';
        buf[pos++] = ':';
        buf[pos++] = 'a';
        buf[pos++] = 'd';

        /* ID */
        pos += snprintf((char *)buf + pos, buf_len - pos, "2:id%d:", WBPXRE_NODE_ID_LEN);
        memcpy(buf + pos, msg->id, WBPXRE_NODE_ID_LEN);
        pos += WBPXRE_NODE_ID_LEN;

        /* Method-specific arguments */
        if (msg->method == WBPXRE_METHOD_FIND_NODE || msg->method == WBPXRE_METHOD_SAMPLE_INFOHASHES) {
            pos += snprintf((char *)buf + pos, buf_len - pos, "6:target%d:", WBPXRE_NODE_ID_LEN);
            memcpy(buf + pos, msg->target, WBPXRE_NODE_ID_LEN);
            pos += WBPXRE_NODE_ID_LEN;
        }

        if (msg->method == WBPXRE_METHOD_GET_PEERS || msg->method == WBPXRE_METHOD_ANNOUNCE_PEER) {
            pos += snprintf((char *)buf + pos, buf_len - pos, "9:info_hash%d:", WBPXRE_INFO_HASH_LEN);
            memcpy(buf + pos, msg->info_hash, WBPXRE_INFO_HASH_LEN);
            pos += WBPXRE_INFO_HASH_LEN;
        }

        /* End arguments dict */
        buf[pos++] = 'e';
    }

    /* End main dict */
    buf[pos++] = 'e';

    return pos;
}

int wbpxre_decode_message(const uint8_t *buf, int buf_len, wbpxre_message_t *msg) {
    /* Parse bencoded message using streaming bencode parser */
    struct bencode parser;
    bencode_init(&parser, buf, buf_len);

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: wbpxre_decode_message: Decoding %d bytes\n", buf_len);
    #endif

    int type;
    const char *current_key = NULL;
    size_t current_key_len = 0;
    int dict_depth = 0;
    bool in_r_dict = false;
    bool in_values_list = false;
    int values_count = 0;
    uint8_t *values_buffer = NULL;
    int values_buffer_capacity = 0;
    /* Track whether we're expecting a key or value in current dict */
    bool expecting_value = false;

    while ((type = bencode_next(&parser)) > 0) {
        switch (type) {
            case BENCODE_DICT_BEGIN:
                dict_depth++;
                /* Check if this is the "r" dict */
                if (dict_depth == 2 && current_key && current_key_len == 1 && current_key[0] == 'r') {
                    in_r_dict = true;
                }
                current_key = NULL;
                expecting_value = false;  /* New dict starts with a key */
                break;

            case BENCODE_DICT_END:
                dict_depth--;
                if (in_r_dict && dict_depth == 1) {
                    in_r_dict = false;
                }
                current_key = NULL;
                expecting_value = false;
                break;

            case BENCODE_LIST_BEGIN:
                /* Check if this is the "values" list */
                if (in_r_dict && current_key && current_key_len == 6 &&
                    memcmp(current_key, "values", 6) == 0) {
                    in_values_list = true;
                    values_count = 0;
                    values_buffer_capacity = 32; /* Initial capacity */
                    values_buffer = malloc(values_buffer_capacity * WBPXRE_COMPACT_PEER_LEN);
                }
                current_key = NULL;
                break;

            case BENCODE_LIST_END:
                if (in_values_list) {
                    in_values_list = false;
                    msg->values = values_buffer;
                    msg->values_len = values_count * WBPXRE_COMPACT_PEER_LEN;
                    values_buffer = NULL;  /* Prevent double-free */
                }
                current_key = NULL;
                break;

            case BENCODE_STRING:
                /* In a dict, strings alternate between keys and values */
                if (dict_depth > 0 && !in_values_list) {
                    if (!expecting_value) {
                        /* This is a key */
                        current_key = (const char *)parser.tok;
                        current_key_len = parser.toklen;
                        expecting_value = true;
                    } else {
                        /* This is a value for the current key */

                        /* Top-level keys */
                        if (dict_depth == 1) {
                            if (current_key_len == 1 && current_key[0] == 't') {
                                /* Transaction ID */
                                size_t copy_len = parser.toklen < WBPXRE_TRANSACTION_ID_LEN ?
                                                parser.toklen : WBPXRE_TRANSACTION_ID_LEN;
                                memcpy(msg->transaction_id, parser.tok, copy_len);
                                #ifdef DEBUG_PROTOCOL
                                fprintf(stderr, "DEBUG: Decoded transaction ID: %02x%02x (len=%zu)\n",
                                       (unsigned char)msg->transaction_id[0],
                                       (unsigned char)msg->transaction_id[1],
                                       copy_len);
                                #endif
                            } else if (current_key_len == 1 && current_key[0] == 'y') {
                                /* Message type */
                                if (parser.toklen > 0) {
                                    msg->type = (wbpxre_msg_type_t)((const char *)parser.tok)[0];
                                    #ifdef DEBUG_PROTOCOL
                                    fprintf(stderr, "DEBUG: Decoded message type: '%c' (%d)\n",
                                           msg->type, msg->type);
                                    #endif
                                }
                            }
                        }
                        /* Keys inside "r" dict */
                        else if (in_r_dict && dict_depth == 2) {
                            if (current_key_len == 2 && memcmp(current_key, "id", 2) == 0) {
                                /* Node ID */
                                size_t copy_len = parser.toklen < WBPXRE_NODE_ID_LEN ?
                                                parser.toklen : WBPXRE_NODE_ID_LEN;
                                memcpy(msg->id, parser.tok, copy_len);
                            } else if (current_key_len == 5 && memcmp(current_key, "nodes", 5) == 0) {
                                /* Compact node info */
                                msg->nodes = malloc(parser.toklen);
                                memcpy(msg->nodes, parser.tok, parser.toklen);
                                msg->nodes_len = parser.toklen;
                            } else if (current_key_len == 7 && memcmp(current_key, "samples", 7) == 0) {
                                /* BEP 51 samples */
                                msg->samples = malloc(parser.toklen);
                                memcpy(msg->samples, parser.tok, parser.toklen);
                                msg->samples_count = parser.toklen / WBPXRE_INFO_HASH_LEN;
                            } else if (current_key_len == 5 && memcmp(current_key, "token", 5) == 0) {
                                /* Token */
                                msg->token = malloc(parser.toklen);
                                memcpy(msg->token, parser.tok, parser.toklen);
                                msg->token_len = parser.toklen;
                            }
                        }

                        current_key = NULL;
                        expecting_value = false;
                    }
                }
                /* Values inside "values" list */
                else if (in_values_list) {
                    /* Add peer to values buffer */
                    if (parser.toklen >= WBPXRE_COMPACT_PEER_LEN) {
                        /* Expand buffer if needed */
                        if (values_count >= values_buffer_capacity) {
                            values_buffer_capacity *= 2;
                            values_buffer = realloc(values_buffer,
                                                   values_buffer_capacity * WBPXRE_COMPACT_PEER_LEN);
                        }
                        memcpy(values_buffer + (values_count * WBPXRE_COMPACT_PEER_LEN),
                               parser.tok, WBPXRE_COMPACT_PEER_LEN);
                        values_count++;
                    }
                }
                break;

            case BENCODE_INTEGER:
                if (current_key && in_r_dict && dict_depth == 2) {
                    /* Parse integer value */
                    long long int_val = 0;
                    const char *int_str = (const char *)parser.tok;
                    size_t int_len = parser.toklen;

                    /* Manual string to integer conversion */
                    bool negative = false;
                    size_t i = 0;
                    if (int_len > 0 && int_str[0] == '-') {
                        negative = true;
                        i = 1;
                    }
                    for (; i < int_len; i++) {
                        if (int_str[i] >= '0' && int_str[i] <= '9') {
                            int_val = int_val * 10 + (int_str[i] - '0');
                        }
                    }
                    if (negative) int_val = -int_val;

                    if (current_key_len == 3 && memcmp(current_key, "num", 3) == 0) {
                        msg->total_num = int_val;
                    } else if (current_key_len == 8 && memcmp(current_key, "interval", 8) == 0) {
                        msg->interval = int_val;
                    }
                }
                current_key = NULL;
                expecting_value = false;
                break;
        }
    }

    /* Check for parsing errors */
    if (type < 0) {
        /* Cleanup any allocated memory on error */
        if (msg->nodes) { free(msg->nodes); msg->nodes = NULL; }
        if (msg->values) { free(msg->values); msg->values = NULL; }
        if (msg->samples) { free(msg->samples); msg->samples = NULL; }
        if (msg->token) { free(msg->token); msg->token = NULL; }
        if (values_buffer && !msg->values) { free(values_buffer); }
        bencode_free(&parser);
        return -1;
    }

    bencode_free(&parser);
    return 0;
}

/* ============================================================================
 * Compact Format Parsing
 * ============================================================================ */

int wbpxre_parse_compact_nodes(const uint8_t *data, int len,
                                wbpxre_routing_node_t **nodes_out) {
    if (len % WBPXRE_COMPACT_NODE_INFO_LEN != 0) {
        return 0;
    }

    int count = len / WBPXRE_COMPACT_NODE_INFO_LEN;
    wbpxre_routing_node_t *nodes = calloc(count, sizeof(wbpxre_routing_node_t));

    for (int i = 0; i < count; i++) {
        const uint8_t *p = data + (i * WBPXRE_COMPACT_NODE_INFO_LEN);

        /* Extract node ID (20 bytes) */
        memcpy(nodes[i].id, p, WBPXRE_NODE_ID_LEN);
        p += WBPXRE_NODE_ID_LEN;

        /* Extract IP address (4 bytes) */
        memcpy(&nodes[i].addr.addr.sin_addr, p, 4);
        p += 4;

        /* Extract port (2 bytes, big-endian) */
        uint16_t port;
        memcpy(&port, p, 2);
        nodes[i].addr.port = ntohs(port);

        /* Setup sockaddr */
        nodes[i].addr.addr.sin_family = AF_INET;
        nodes[i].addr.addr.sin_port = htons(nodes[i].addr.port);

        /* Convert IP to string */
        inet_ntop(AF_INET, &nodes[i].addr.addr.sin_addr,
                  nodes[i].addr.ip, sizeof(nodes[i].addr.ip));

        nodes[i].discovered_at = time(NULL);
    }

    *nodes_out = nodes;
    return count;
}

int wbpxre_parse_compact_peers(const uint8_t *data, int len,
                                wbpxre_peer_t **peers_out) {
    if (len % WBPXRE_COMPACT_PEER_LEN != 0) {
        return 0;
    }

    int count = len / WBPXRE_COMPACT_PEER_LEN;
    wbpxre_peer_t *peers = calloc(count, sizeof(wbpxre_peer_t));

    for (int i = 0; i < count; i++) {
        const uint8_t *p = data + (i * WBPXRE_COMPACT_PEER_LEN);

        /* Extract IP address (4 bytes) */
        memcpy(&peers[i].addr.sin_addr, p, 4);
        p += 4;

        /* Extract port (2 bytes, big-endian) */
        uint16_t port;
        memcpy(&port, p, 2);
        peers[i].port = ntohs(port);

        /* Setup sockaddr */
        peers[i].addr.sin_family = AF_INET;
        peers[i].addr.sin_port = htons(peers[i].port);

        /* Convert IP to string */
        inet_ntop(AF_INET, &peers[i].addr.sin_addr,
                  peers[i].ip, sizeof(peers[i].ip));
    }

    *peers_out = peers;
    return count;
}

/* ============================================================================
 * Query Sending and Response Waiting
 * ============================================================================ */

static int send_query(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                      const wbpxre_message_t *msg) {
    uint8_t buf[WBPXRE_MAX_UDP_PACKET];
    int len = wbpxre_encode_message(msg, buf, sizeof(buf));

    if (len < 0) {
        fprintf(stderr, "ERROR: Failed to encode message\n");
        return -1;
    }

    /* Debug: dump first 100 bytes of encoded message */
    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: Sending query (%d bytes) to %s:%d\n", len,
           inet_ntoa(addr->sin_addr), ntohs(addr->sin_port));
    fprintf(stderr, "  ");
    for (int i = 0; i < len && i < 100; i++) {
        fprintf(stderr, "%02x", buf[i]);
    }
    fprintf(stderr, "\n");
    #endif

    int sent = sendto(dht->udp_socket, buf, len, 0,
                      (struct sockaddr *)addr, sizeof(*addr));

    if (sent < 0) {
        // Silently ignore sendto errors (socket may be closed during shutdown)
        return -1;
    }

    /* Update stats */
    pthread_mutex_lock(&dht->stats_mutex);
    dht->stats.packets_sent++;
    dht->stats.queries_sent++;
    pthread_mutex_unlock(&dht->stats_mutex);

    return 0;
}

static wbpxre_message_t *wait_for_response_msg(wbpxre_dht_t *dht,
                                                 const uint8_t *transaction_id,
                                                 int timeout_sec) {
    /* Create pending query */
    wbpxre_pending_query_t *pq = wbpxre_create_pending_query(transaction_id, timeout_sec);
    if (!pq) return NULL;

    /* Register it */
    wbpxre_register_pending_query(dht, pq);

    /* Wait for response */
    if (wbpxre_wait_for_response(pq) < 0) {
        /* Timeout or error - try to remove from pending queries hash table */
        wbpxre_pending_query_t *removed = wbpxre_find_and_remove_pending_query(dht, transaction_id);
        /* Only free if WE successfully removed it (not already removed by UDP reader) */
        if (removed == pq) {
            /* We own it, we free it */
            wbpxre_free_pending_query(pq);
        }
        /* If removed is NULL, UDP reader already removed and signaled it, but we timed out anyway.
         * In that case, pq is still valid but we shouldn't free it - let it leak rather than crash.
         * This is a race condition edge case that should be rare. */
        return NULL;
    }

    /* Get response data */
    wbpxre_message_t *response = (wbpxre_message_t *)pq->response_data;
    pq->response_data = NULL;  /* Prevent double-free */
    wbpxre_free_pending_query(pq);

    return response;
}

/* ============================================================================
 * DHT Protocol Methods
 * ============================================================================ */

int wbpxre_protocol_ping(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                         uint8_t *node_id_out) {
    /* Build ping message */
    wbpxre_message_t msg;
    memset(&msg, 0, sizeof(msg));

    msg.type = WBPXRE_MSG_QUERY;
    msg.method = WBPXRE_METHOD_PING;
    wbpxre_random_bytes(msg.transaction_id, WBPXRE_TRANSACTION_ID_LEN);

    /* Read node ID with lock protection */
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(msg.id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: Sending ping to %s:%d (transaction_id=%02x%02x)\n",
           inet_ntoa(addr->sin_addr), ntohs(addr->sin_port),
           msg.transaction_id[0], msg.transaction_id[1]);
    #endif

    /* Send query */
    if (send_query(dht, addr, &msg) < 0) {
        #ifdef DEBUG_PROTOCOL
        fprintf(stderr, "DEBUG: send_query failed\n");
        #endif
        return -1;
    }

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: Waiting for ping response (timeout=%ds)...\n",
           dht->config.query_timeout);
    #endif

    /* Wait for response */
    wbpxre_message_t *response = wait_for_response_msg(dht, msg.transaction_id,
                                                         dht->config.query_timeout);
    if (!response) {
        #ifdef DEBUG_PROTOCOL
        fprintf(stderr, "DEBUG: No response received (timeout)\n");
        #endif
        return -1;
    }

    #ifdef DEBUG_PROTOCOL
    fprintf(stderr, "DEBUG: Ping response received!\n");
    #endif

    /* Extract node ID from response */
    if (node_id_out) {
        memcpy(node_id_out, response->id, WBPXRE_NODE_ID_LEN);
    }

    free(response);
    return 0;
}

int wbpxre_protocol_find_node(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                               const uint8_t *target_id,
                               wbpxre_routing_node_t **nodes_out, int *count_out) {
    /* Build find_node message */
    wbpxre_message_t msg;
    memset(&msg, 0, sizeof(msg));

    msg.type = WBPXRE_MSG_QUERY;
    msg.method = WBPXRE_METHOD_FIND_NODE;
    wbpxre_random_bytes(msg.transaction_id, WBPXRE_TRANSACTION_ID_LEN);

    /* Read node ID with lock protection */
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(msg.id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    memcpy(msg.target, target_id, WBPXRE_NODE_ID_LEN);

    /* Send query */
    if (send_query(dht, addr, &msg) < 0) {
        return -1;
    }

    /* Wait for response */
    wbpxre_message_t *response = wait_for_response_msg(dht, msg.transaction_id,
                                                         dht->config.query_timeout);
    if (!response) {
        return -1;
    }

    /* Parse compact node info */
    int count = 0;
    if (response->nodes && response->nodes_len > 0) {
        count = wbpxre_parse_compact_nodes(response->nodes, response->nodes_len, nodes_out);
    }

    if (count_out) *count_out = count;

    if (response->nodes) free(response->nodes);
    free(response);
    return 0;
}

int wbpxre_protocol_get_peers(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                               const uint8_t *info_hash,
                               wbpxre_peer_t **peers_out, int *peer_count,
                               wbpxre_routing_node_t **nodes_out, int *node_count,
                               uint8_t *token_out, int *token_len) {
    /* Build get_peers message */
    wbpxre_message_t msg;
    memset(&msg, 0, sizeof(msg));

    msg.type = WBPXRE_MSG_QUERY;
    msg.method = WBPXRE_METHOD_GET_PEERS;
    wbpxre_random_bytes(msg.transaction_id, WBPXRE_TRANSACTION_ID_LEN);

    /* Read node ID with lock protection */
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(msg.id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    memcpy(msg.info_hash, info_hash, WBPXRE_INFO_HASH_LEN);

    /* Send query */
    if (send_query(dht, addr, &msg) < 0) {
        return -1;
    }

    /* Wait for response */
    wbpxre_message_t *response = wait_for_response_msg(dht, msg.transaction_id,
                                                         dht->config.query_timeout);
    if (!response) {
        return -1;
    }

    /* Parse values (peers) */
    int pcount = 0;
    if (response->values && response->values_len > 0) {
        pcount = wbpxre_parse_compact_peers(response->values, response->values_len, peers_out);
    }
    if (peer_count) *peer_count = pcount;

    /* Parse nodes */
    int ncount = 0;
    if (response->nodes && response->nodes_len > 0) {
        ncount = wbpxre_parse_compact_nodes(response->nodes, response->nodes_len, nodes_out);
    }
    if (node_count) *node_count = ncount;

    /* Copy token */
    if (response->token && response->token_len > 0 && token_out && token_len) {
        memcpy(token_out, response->token, response->token_len);
        *token_len = response->token_len;
    }

    if (response->nodes) free(response->nodes);
    if (response->values) free(response->values);
    if (response->token) free(response->token);
    free(response);
    return 0;
}

int wbpxre_protocol_sample_infohashes(wbpxre_dht_t *dht,
                                       const struct sockaddr_in *addr,
                                       const uint8_t *target_id,
                                       uint8_t **hashes_out, int *hash_count,
                                       int *total_num_out, int *interval_out) {
    /* Build sample_infohashes message */
    wbpxre_message_t msg;
    memset(&msg, 0, sizeof(msg));

    msg.type = WBPXRE_MSG_QUERY;
    msg.method = WBPXRE_METHOD_SAMPLE_INFOHASHES;
    wbpxre_random_bytes(msg.transaction_id, WBPXRE_TRANSACTION_ID_LEN);

    /* Read node ID with lock protection */
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(msg.id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    memcpy(msg.target, target_id, WBPXRE_NODE_ID_LEN);

    /* Send query */
    if (send_query(dht, addr, &msg) < 0) {
        return -1;
    }

    /* Wait for response */
    wbpxre_message_t *response = wait_for_response_msg(dht, msg.transaction_id,
                                                         dht->config.query_timeout);
    if (!response) {
        return -1;
    }

    /* Check if samples field exists (BEP 51 support indicator) */
    if (!response->samples) {
        free(response);
        return -1;
    }

    /* Extract samples */
    if (hashes_out && hash_count) {
        *hash_count = response->samples_count;
        if (response->samples_count > 0) {
            int samples_len = response->samples_count * WBPXRE_INFO_HASH_LEN;
            *hashes_out = malloc(samples_len);
            memcpy(*hashes_out, response->samples, samples_len);
        } else {
            *hashes_out = NULL;
        }
    }

    /* Extract metadata */
    if (total_num_out) *total_num_out = response->total_num;
    if (interval_out) *interval_out = response->interval;

    if (response->samples) free(response->samples);
    free(response);
    return 0;
}

/* ============================================================================
 * Query Response Handlers (NEW - Critical for bootstrap!)
 * ============================================================================ */

/* Encode and send a response message */
static int send_response(wbpxre_dht_t *dht, const struct sockaddr_in *addr,
                        const uint8_t *transaction_id, const uint8_t *node_id,
                        const uint8_t *nodes_compact, int nodes_len) {
    uint8_t buf[WBPXRE_MAX_UDP_PACKET];
    int pos = 0;

    /* Build response: d1:rd2:id20:...e1:t2:XX1:y1:re */
    buf[pos++] = 'd';

    /* Response dict "r" */
    pos += snprintf((char *)buf + pos, sizeof(buf) - pos, "1:rd");

    /* Node ID */
    pos += snprintf((char *)buf + pos, sizeof(buf) - pos, "2:id%d:", WBPXRE_NODE_ID_LEN);
    memcpy(buf + pos, node_id, WBPXRE_NODE_ID_LEN);
    pos += WBPXRE_NODE_ID_LEN;

    /* Nodes (if provided) */
    if (nodes_compact && nodes_len > 0) {
        pos += snprintf((char *)buf + pos, sizeof(buf) - pos, "5:nodes%d:", nodes_len);
        memcpy(buf + pos, nodes_compact, nodes_len);
        pos += nodes_len;
    }

    /* End response dict */
    buf[pos++] = 'e';

    /* Transaction ID */
    pos += snprintf((char *)buf + pos, sizeof(buf) - pos, "1:t%d:", WBPXRE_TRANSACTION_ID_LEN);
    memcpy(buf + pos, transaction_id, WBPXRE_TRANSACTION_ID_LEN);
    pos += WBPXRE_TRANSACTION_ID_LEN;

    /* Message type */
    pos += snprintf((char *)buf + pos, sizeof(buf) - pos, "1:y1:r");

    /* End main dict */
    buf[pos++] = 'e';

    /* Send response */
    int sent = sendto(dht->udp_socket, buf, pos, 0,
                     (struct sockaddr *)addr, sizeof(*addr));

    if (sent < 0) {
        // Silently ignore sendto errors (socket may be closed during shutdown)
        return -1;
    }

    pthread_mutex_lock(&dht->stats_mutex);
    dht->stats.packets_sent++;
    pthread_mutex_unlock(&dht->stats_mutex);

    return 0;
}

/* Encode compact node info */
static int encode_compact_nodes(wbpxre_routing_table_t *table, const uint8_t *target,
                                uint8_t *buf, int buf_size, int max_nodes) {
    wbpxre_routing_node_t *nodes[max_nodes];
    int count = wbpxre_routing_table_get_closest(table, target, nodes, max_nodes);

    if (count == 0) return 0;

    int pos = 0;
    for (int i = 0; i < count && pos + WBPXRE_COMPACT_NODE_INFO_LEN <= buf_size; i++) {
        /* Node ID (20 bytes) */
        memcpy(buf + pos, nodes[i]->id, WBPXRE_NODE_ID_LEN);
        pos += WBPXRE_NODE_ID_LEN;

        /* IP address (4 bytes) */
        memcpy(buf + pos, &nodes[i]->addr.addr.sin_addr, 4);
        pos += 4;

        /* Port (2 bytes, network byte order) */
        memcpy(buf + pos, &nodes[i]->addr.addr.sin_port, 2);
        pos += 2;
    }

    /* Free the node copies returned by get_closest */
    for (int i = 0; i < count; i++) {
        free(nodes[i]);
    }

    return pos;
}

/* Handle incoming ping query */
void wbpxre_handle_ping(wbpxre_dht_t *dht, const wbpxre_message_t *query,
                        const struct sockaddr_in *from) {
    /* Read node ID with lock protection */
    uint8_t local_node_id[WBPXRE_NODE_ID_LEN];
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(local_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    /* Send ping response with our node ID */
    send_response(dht, from, query->transaction_id, local_node_id, NULL, 0);
}

/* Handle incoming find_node query */
void wbpxre_handle_find_node(wbpxre_dht_t *dht, const wbpxre_message_t *query,
                              const struct sockaddr_in *from) {
    /* Get closest nodes to target */
    uint8_t compact_nodes[8 * WBPXRE_COMPACT_NODE_INFO_LEN];
    int nodes_len = encode_compact_nodes(dht->routing_table, query->target,
                                         compact_nodes, sizeof(compact_nodes), 8);

    /* Read node ID with lock protection */
    uint8_t local_node_id[WBPXRE_NODE_ID_LEN];
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(local_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    /* Send find_node response */
    send_response(dht, from, query->transaction_id, local_node_id,
                 compact_nodes, nodes_len);
}

/* Handle incoming get_peers query */
void wbpxre_handle_get_peers(wbpxre_dht_t *dht, const wbpxre_message_t *query,
                              const struct sockaddr_in *from) {
    /* In crawler mode, we don't store peers - just return nodes */
    uint8_t compact_nodes[8 * WBPXRE_COMPACT_NODE_INFO_LEN];
    int nodes_len = encode_compact_nodes(dht->routing_table, query->info_hash,
                                         compact_nodes, sizeof(compact_nodes), 8);

    /* Read node ID with lock protection */
    uint8_t local_node_id[WBPXRE_NODE_ID_LEN];
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(local_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    /* Send get_peers response with nodes */
    send_response(dht, from, query->transaction_id, local_node_id,
                 compact_nodes, nodes_len);
}

/* Handle incoming announce_peer query */
void wbpxre_handle_announce_peer(wbpxre_dht_t *dht, const wbpxre_message_t *query,
                                  const struct sockaddr_in *from) {
    /* Extract info_hash and trigger callback */
    if (dht->config.callback) {
        dht->config.callback(dht->config.callback_closure, WBPXRE_EVENT_VALUES,
                           query->info_hash, NULL, 0);
    }

    /* Read node ID with lock protection */
    uint8_t local_node_id[WBPXRE_NODE_ID_LEN];
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(local_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    /* Send acknowledge response */
    send_response(dht, from, query->transaction_id, local_node_id, NULL, 0);
}

/* Main query dispatcher - called from UDP reader thread */
void wbpxre_handle_incoming_query(wbpxre_dht_t *dht, wbpxre_message_t *query,
                                   const struct sockaddr_in *from) {
    /* Log incoming query for debugging */
    char from_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &from->sin_addr, from_ip, sizeof(from_ip));

    /* Dispatch based on query method */
    /* Note: we need to parse the query method from the "q" field */
    /* For now, infer from presence of fields */

    /* Simple heuristic: check transaction ID and respond to ping */
    /* A proper implementation would parse the "q" field from bencode */
    /* TODO: Enhance decoder to extract query method */

    /* For bootstrap to work, we mainly need to respond to ping and find_node */
    /* Since decoder doesn't extract method yet, respond generically */

    /* Read node ID with lock protection */
    uint8_t local_node_id[WBPXRE_NODE_ID_LEN];
    pthread_rwlock_rdlock(&dht->node_id_lock);
    memcpy(local_node_id, dht->config.node_id, WBPXRE_NODE_ID_LEN);
    pthread_rwlock_unlock(&dht->node_id_lock);

    /* Always send a valid response to keep us in routing tables */
    uint8_t compact_nodes[8 * WBPXRE_COMPACT_NODE_INFO_LEN];
    int nodes_len = encode_compact_nodes(dht->routing_table, local_node_id,
                                         compact_nodes, sizeof(compact_nodes), 8);

    send_response(dht, from, query->transaction_id, local_node_id,
                 compact_nodes, nodes_len);
}
