#ifndef PROTOCOL_UTILS_H
#define PROTOCOL_UTILS_H

#include <stdint.h>
#include <stddef.h>
#include <sys/socket.h>

/**
 * Shared DHT protocol utilities
 *
 * These functions are used by both tree_protocol and refresh_protocol
 * to avoid code duplication. They build bencode queries and parse
 * compact node/peer formats.
 */

/**
 * Build a ping query message
 * @param buf Output buffer for bencode message
 * @param buf_len Size of output buffer
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param node_id 20-byte node ID
 * @return Number of bytes written, or -1 on error
 */
int build_ping_query(uint8_t *buf, size_t buf_len,
                    const uint8_t *tid, int tid_len,
                    const uint8_t node_id[20]);

/**
 * Build a find_node query message
 * @param buf Output buffer for bencode message
 * @param buf_len Size of output buffer
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param node_id 20-byte node ID
 * @param target 20-byte target node ID to find
 * @return Number of bytes written, or -1 on error
 */
int build_find_node_query(uint8_t *buf, size_t buf_len,
                         const uint8_t *tid, int tid_len,
                         const uint8_t node_id[20],
                         const uint8_t target[20]);

/**
 * Build a get_peers query message
 * @param buf Output buffer for bencode message
 * @param buf_len Size of output buffer
 * @param tid Transaction ID bytes
 * @param tid_len Length of transaction ID
 * @param node_id 20-byte node ID
 * @param infohash 20-byte infohash to query
 * @return Number of bytes written, or -1 on error
 */
int build_get_peers_query(uint8_t *buf, size_t buf_len,
                         const uint8_t *tid, int tid_len,
                         const uint8_t node_id[20],
                         const uint8_t infohash[20]);

/**
 * Parse compact node info (26 bytes per node: 20 id + 4 ip + 2 port)
 * @param compact Compact node data
 * @param compact_len Length of compact data
 * @param out_ids Output array for node IDs (caller allocates)
 * @param out_addrs Output array for addresses (caller allocates)
 * @param max_nodes Maximum nodes to parse
 * @return Number of nodes parsed
 */
int parse_compact_nodes(const uint8_t *compact, size_t compact_len,
                        uint8_t out_ids[][20],
                        struct sockaddr_storage *out_addrs,
                        int max_nodes);

/**
 * Parse compact peer info (6 bytes per peer: 4 ip + 2 port)
 * @param compact Compact peer data
 * @param compact_len Length of compact data
 * @param out_addrs Output array for addresses
 * @param max_peers Maximum peers to parse
 * @return Number of peers parsed
 */
int parse_compact_peers(const uint8_t *compact, size_t compact_len,
                        struct sockaddr_storage *out_addrs,
                        int max_peers);

#endif /* PROTOCOL_UTILS_H */
