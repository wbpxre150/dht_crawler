#ifndef REFRESH_PROTOCOL_H
#define REFRESH_PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/socket.h>

/* Forward declaration */
typedef struct tree_socket tree_socket_t;

/**
 * Refresh thread protocol functions
 *
 * These functions are designed specifically for the refresh thread,
 * which is NOT a thread_tree and should not cast itself to one.
 * They take explicit parameters instead of struct pointers to avoid
 * memory alignment issues and type confusion.
 */

/**
 * Send a ping query
 * @param node_id 20-byte node ID (sender)
 * @param sock Socket to send on
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int refresh_send_ping(const uint8_t node_id[20],
                      tree_socket_t *sock,
                      const struct sockaddr_storage *dest);

/**
 * Send a find_node query
 * @param node_id 20-byte node ID (sender)
 * @param sock Socket to send on
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param target 20-byte target node ID to find
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int refresh_send_find_node(const uint8_t node_id[20],
                           tree_socket_t *sock,
                           const uint8_t *tid, int tid_len,
                           const uint8_t target[20],
                           const struct sockaddr_storage *dest);

/**
 * Send a get_peers query
 * @param node_id 20-byte node ID (sender)
 * @param sock Socket to send on
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param infohash 20-byte infohash to query
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int refresh_send_get_peers(const uint8_t node_id[20],
                          tree_socket_t *sock,
                          const uint8_t *tid, int tid_len,
                          const uint8_t infohash[20],
                          const struct sockaddr_storage *dest);

/**
 * Parsed find_node response
 */
typedef struct refresh_find_node_response {
    uint8_t nodes[256][20];              /* Node IDs */
    struct sockaddr_storage addrs[256];  /* Node addresses */
    int node_count;
    uint8_t sender_id[20];               /* Sender's node ID (if present) */
    bool has_sender_id;
} refresh_find_node_response_t;

/**
 * Parse a find_node response
 * @param data Raw response data
 * @param len Length of response data
 * @param from Address of sender
 * @param out Output structure for parsed response
 * @return 0 on success, -1 on error
 */
int refresh_parse_find_node_response(const uint8_t *data, size_t len,
                                     const struct sockaddr_storage *from,
                                     refresh_find_node_response_t *out);

/**
 * Parsed get_peers response
 */
typedef struct refresh_get_peers_response {
    struct sockaddr_storage peers[50];
    int peer_count;
    uint8_t nodes[256][20];
    struct sockaddr_storage node_addrs[256];
    int node_count;
    uint8_t token[20];
    size_t token_len;
    uint8_t sender_id[20];
    bool has_sender_id;
} refresh_get_peers_response_t;

/**
 * Parse a get_peers response
 * @param data Raw response data
 * @param len Length of response data
 * @param from Address of sender
 * @param out Output structure for parsed response
 * @return 0 on success, -1 on error
 */
int refresh_parse_get_peers_response(const uint8_t *data, size_t len,
                                     const struct sockaddr_storage *from,
                                     refresh_get_peers_response_t *out);

#endif /* REFRESH_PROTOCOL_H */
