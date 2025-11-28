#ifndef TREE_PROTOCOL_H
#define TREE_PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <sys/socket.h>

/* Forward declarations */
struct thread_tree;
struct tree_routing_table;

/**
 * DHT protocol message builders and handlers for thread trees
 *
 * Uses bencode for message encoding (reuses lib/bencode-c).
 */

/* Response types from handle_response */
typedef enum {
    TREE_RESP_NONE,             /* Not a response we care about */
    TREE_RESP_PONG,             /* Response to ping */
    TREE_RESP_FIND_NODE,        /* Response to find_node with nodes */
    TREE_RESP_SAMPLE_INFOHASHES,/* Response to sample_infohashes (BEP51) */
    TREE_RESP_GET_PEERS,        /* Response to get_peers (Stage 4) */
    TREE_RESP_ERROR             /* Error response */
} tree_response_type_t;

/* Parsed find_node response */
typedef struct {
    uint8_t nodes[256][20];          /* Node IDs */
    struct sockaddr_storage addrs[256]; /* Node addresses */
    int node_count;
} tree_find_node_response_t;

/* Parsed sample_infohashes response (BEP51) */
typedef struct {
    uint8_t infohashes[100][20];     /* Sampled infohashes */
    int infohash_count;
    uint8_t nodes[256][20];          /* Nodes for further queries */
    struct sockaddr_storage addrs[256];
    int node_count;
    int interval;                     /* Suggested query interval */
    int num;                          /* Total infohashes node claims to have */
    const uint8_t *sender_id;         /* Sender's node ID (20 bytes, NOT owned) */
} tree_sample_response_t;

/* Parsed get_peers response (Stage 4) */
typedef struct {
    struct sockaddr_storage peers[50]; /* Peer addresses (compact format) */
    int peer_count;
    uint8_t nodes[256][20];            /* Closer nodes for iterative lookup */
    struct sockaddr_storage node_addrs[256];
    int node_count;
    uint8_t token[64];                 /* Token for announce_peer (if needed) */
    int token_len;
} tree_get_peers_response_t;

/**
 * Generate a transaction ID for a query
 * @param out Buffer to store transaction ID (at least 4 bytes)
 * @return Length of transaction ID
 */
int tree_protocol_gen_tid(uint8_t *out);

/**
 * Build and send a ping message
 * @param tree Thread tree context
 * @param sock Socket to send on
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int tree_send_ping(struct thread_tree *tree, void *sock,
                   const struct sockaddr_storage *dest);

/**
 * Build and send a find_node message
 * @param tree Thread tree context
 * @param sock Socket to send on
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param target 20-byte target node ID to find
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int tree_send_find_node(struct thread_tree *tree, void *sock,
                        const uint8_t *tid, int tid_len,
                        const uint8_t *target,
                        const struct sockaddr_storage *dest);

/**
 * Handle an incoming DHT response
 * @param tree Thread tree context
 * @param data Raw response data
 * @param len Length of response data
 * @param from Address of sender
 * @param out_response Parsed response (if applicable)
 * @return Response type
 */
tree_response_type_t tree_handle_response(struct thread_tree *tree,
                                           const uint8_t *data, size_t len,
                                           const struct sockaddr_storage *from,
                                           tree_find_node_response_t *out_response);

/**
 * Parse compact node info (26 bytes per node: 20 id + 4 ip + 2 port)
 * @param compact Compact node data
 * @param compact_len Length of compact data
 * @param out_ids Output array for node IDs (caller allocates)
 * @param out_addrs Output array for addresses (caller allocates)
 * @param max_nodes Maximum nodes to parse
 * @return Number of nodes parsed
 */
int tree_parse_compact_nodes(const uint8_t *compact, size_t compact_len,
                              uint8_t out_ids[][20],
                              struct sockaddr_storage *out_addrs,
                              int max_nodes);

/**
 * Build and send a sample_infohashes message (BEP51)
 * @param tree Thread tree context
 * @param sock Socket to send on
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param target 20-byte target to sample around
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int tree_send_sample_infohashes(struct thread_tree *tree, void *sock,
                                 const uint8_t *tid, int tid_len,
                                 const uint8_t *target,
                                 const struct sockaddr_storage *dest);

/**
 * Handle a sample_infohashes response (BEP51)
 * @param tree Thread tree context
 * @param data Raw response data
 * @param len Length of response data
 * @param from Address of sender
 * @param out_response Parsed response
 * @return 0 on success, -1 on error
 */
int tree_handle_sample_infohashes_response(struct thread_tree *tree,
                                            const uint8_t *data, size_t len,
                                            const struct sockaddr_storage *from,
                                            tree_sample_response_t *out_response);

/**
 * Build and send a get_peers message (Stage 4)
 * @param tree Thread tree context
 * @param sock Socket to send on
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param infohash 20-byte infohash to query
 * @param dest Destination address
 * @return 0 on success, -1 on error
 */
int tree_send_get_peers(struct thread_tree *tree, void *sock,
                        const uint8_t *tid, int tid_len,
                        const uint8_t *infohash,
                        const struct sockaddr_storage *dest);

/**
 * Handle a get_peers response (Stage 4)
 * @param tree Thread tree context
 * @param data Raw response data
 * @param len Length of response data
 * @param from Address of sender
 * @param out_response Parsed response
 * @return 0 on success, -1 on error
 */
int tree_handle_get_peers_response(struct thread_tree *tree,
                                   const uint8_t *data, size_t len,
                                   const struct sockaddr_storage *from,
                                   tree_get_peers_response_t *out_response);

/**
 * Parse compact peer info (6 bytes per peer: 4 ip + 2 port)
 * @param compact Compact peer data
 * @param compact_len Length of compact data
 * @param out_addrs Output array for addresses
 * @param max_peers Maximum peers to parse
 * @return Number of peers parsed
 */
int tree_parse_compact_peers(const uint8_t *compact, size_t compact_len,
                              struct sockaddr_storage *out_addrs,
                              int max_peers);

/**
 * Extract transaction ID from a DHT response
 * @param data Raw response data
 * @param len Length of response data
 * @param out_tid Output buffer for TID (at least 4 bytes)
 * @param out_tid_len Pointer to store TID length
 * @return 0 on success, -1 on error
 */
int tree_protocol_extract_tid(const uint8_t *data, size_t len,
                               uint8_t *out_tid, int *out_tid_len);

#endif /* TREE_PROTOCOL_H */
