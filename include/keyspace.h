#ifndef KEYSPACE_H
#define KEYSPACE_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Keyspace Partitioning Module
 *
 * Divides the 160-bit DHT keyspace into equal segments and assigns each
 * thread tree a node ID within its designated segment. This ensures:
 * - Maximum keyspace coverage (no overlap between trees)
 * - Predictable distribution across the DHT
 * - Ability to perturb node IDs while staying within assigned region
 *
 * When trees respawn due to low performance, they receive a new node ID
 * that's slightly perturbed within their keyspace segment to explore
 * different neighborhoods while maintaining coverage.
 */

/**
 * Generate a node ID for a specific keyspace partition
 *
 * The keyspace is divided evenly among num_partitions, and this function
 * generates a node ID at the center of the specified partition.
 *
 * For example, with 16 partitions:
 * - Partition 0: 0x00000000... (0/16 of keyspace)
 * - Partition 1: 0x10000000... (1/16 of keyspace)
 * - Partition 2: 0x20000000... (2/16 of keyspace)
 * - ...
 * - Partition 15: 0xF0000000... (15/16 of keyspace)
 *
 * @param partition_index Which partition (0 to num_partitions-1)
 * @param num_partitions Total number of partitions
 * @param node_id Output buffer (20 bytes)
 */
void keyspace_generate_node_id(uint32_t partition_index, uint32_t num_partitions, uint8_t *node_id);

/**
 * Generate a perturbed node ID within the same keyspace partition
 *
 * This creates a new node ID that's within the same keyspace segment but
 * slightly different from the original. Used when respawning trees to explore
 * different neighborhoods while maintaining keyspace coverage.
 *
 * The perturbation strategy:
 * - Keep the first byte(s) that define the partition boundary
 * - Randomize remaining bytes to move within the partition
 * - Ensures the node ID stays within its assigned keyspace segment
 *
 * @param original_node_id Current node ID
 * @param partition_index Which partition this node belongs to
 * @param num_partitions Total number of partitions
 * @param new_node_id Output buffer (20 bytes)
 */
void keyspace_perturb_node_id(const uint8_t *original_node_id,
                              uint32_t partition_index,
                              uint32_t num_partitions,
                              uint8_t *new_node_id);

/**
 * Calculate which partition a node ID belongs to
 *
 * Given a node ID and the total number of partitions, determine which
 * partition this node ID falls into. Useful for debugging and validation.
 *
 * @param node_id Node ID to check (20 bytes)
 * @param num_partitions Total number of partitions
 * @return Partition index (0 to num_partitions-1)
 */
uint32_t keyspace_get_partition(const uint8_t *node_id, uint32_t num_partitions);

/**
 * Verify that a node ID is within its expected partition
 *
 * Checks that the given node ID falls within the expected partition boundaries.
 * Useful for validation and testing.
 *
 * @param node_id Node ID to verify (20 bytes)
 * @param expected_partition Expected partition index
 * @param num_partitions Total number of partitions
 * @return true if node_id is within expected_partition, false otherwise
 */
bool keyspace_verify_partition(const uint8_t *node_id,
                               uint32_t expected_partition,
                               uint32_t num_partitions);

#endif /* KEYSPACE_H */
