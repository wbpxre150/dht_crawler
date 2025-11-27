#include "keyspace.h"
#include "dht_crawler.h"
#include <string.h>
#include <stdlib.h>
#include <time.h>

/**
 * Calculate the number of bits needed to represent partition boundaries
 * For example:
 * - 2 partitions: 1 bit  (0b0xxx... vs 0b1xxx...)
 * - 4 partitions: 2 bits (0b00xx... 0b01xx... 0b10xx... 0b11xx...)
 * - 8 partitions: 3 bits
 * - 16 partitions: 4 bits
 * - 32 partitions: 5 bits
 */
static int calculate_partition_bits(uint32_t num_partitions) {
    if (num_partitions <= 1) return 0;

    int bits = 0;
    uint32_t n = num_partitions - 1;
    while (n > 0) {
        bits++;
        n >>= 1;
    }
    return bits;
}

void keyspace_generate_node_id(uint32_t partition_index, uint32_t num_partitions, uint8_t *node_id) {
    if (!node_id || num_partitions == 0 || partition_index >= num_partitions) {
        log_msg(LOG_ERROR, "[keyspace] Invalid parameters: partition_index=%u, num_partitions=%u",
                partition_index, num_partitions);
        return;
    }

    /* Clear the node_id buffer */
    memset(node_id, 0, 20);

    /* Strategy: Divide the 160-bit keyspace into num_partitions equal segments
     *
     * The keyspace is 2^160 total values.
     * Each partition gets: 2^160 / num_partitions values
     *
     * For simplicity, we use the high-order bits to determine the partition.
     * For example, with 16 partitions:
     * - Partition 0: 0x00... to 0x0F...  (0/16 to 1/16)
     * - Partition 1: 0x10... to 0x1F...  (1/16 to 2/16)
     * - Partition 2: 0x20... to 0x2F...  (2/16 to 3/16)
     * - ...
     * - Partition 15: 0xF0... to 0xFF... (15/16 to 16/16)
     *
     * We set the node ID to the center of each partition's range.
     */

    /* Calculate partition size as a 160-bit value
     * partition_size = 2^160 / num_partitions
     *
     * Instead of doing arbitrary precision arithmetic, we can take advantage
     * of the fact that we're working with 20 bytes (160 bits) and use
     * a scaling approach based on the first few bytes.
     */

    /* Calculate how many bits we need for the partition prefix */
    int partition_bits = calculate_partition_bits(num_partitions);

    /* For power-of-2 partitions (most common), this is straightforward */
    if ((num_partitions & (num_partitions - 1)) == 0) {
        /* Power of 2: easy bit shifting */
        /* Set the high-order bits to partition_index */

        if (partition_bits <= 8) {
            /* All partition bits fit in first byte */
            int shift = 8 - partition_bits;
            node_id[0] = (uint8_t)(partition_index << shift);

            /* Set middle bits to 0x80 to center within partition */
            if (shift > 0) {
                node_id[0] |= (uint8_t)(1 << (shift - 1));
            }
        } else {
            /* Partition bits span multiple bytes */
            int remaining_bits = partition_bits;
            int byte_idx = 0;

            while (remaining_bits > 0) {
                int bits_in_byte = (remaining_bits > 8) ? 8 : remaining_bits;
                int shift = remaining_bits - bits_in_byte;

                node_id[byte_idx] = (uint8_t)((partition_index >> shift) & 0xFF);

                remaining_bits -= bits_in_byte;
                byte_idx++;
            }
        }
    } else {
        /* Non-power-of-2: use scaled approach
         * scale = (partition_index * 256) / num_partitions
         * This gives us a value 0-255 representing position in keyspace
         */
        uint64_t scale = ((uint64_t)partition_index * 256ULL) / num_partitions;
        node_id[0] = (uint8_t)scale;

        /* Set second byte to 0x80 to center within partition */
        node_id[1] = 0x80;
    }

    /* Fill remaining bytes with 0x80 to center in the partition */
    for (int i = 2; i < 20; i++) {
        node_id[i] = 0x80;
    }

    log_msg(LOG_DEBUG, "[keyspace] Generated node ID for partition %u/%u: %02x%02x%02x%02x...",
            partition_index, num_partitions,
            node_id[0], node_id[1], node_id[2], node_id[3]);
}

void keyspace_perturb_node_id(const uint8_t *original_node_id,
                              uint32_t partition_index,
                              uint32_t num_partitions,
                              uint8_t *new_node_id) {
    if (!original_node_id || !new_node_id || num_partitions == 0 ||
        partition_index >= num_partitions) {
        log_msg(LOG_ERROR, "[keyspace] Invalid parameters for perturbation");
        return;
    }

    /* Strategy: Keep the partition-defining prefix bits, randomize the rest
     *
     * This ensures the new node ID stays within the same partition while
     * exploring a different neighborhood.
     */

    /* Calculate how many bits define the partition boundary */
    int partition_bits = calculate_partition_bits(num_partitions);

    /* Copy original node ID */
    memcpy(new_node_id, original_node_id, 20);

    /* Seed random number generator with time + original node_id */
    unsigned int seed = (unsigned int)time(NULL);
    for (int i = 0; i < 4; i++) {
        seed ^= original_node_id[i] << (i * 8);
    }
    srand(seed);

    /* Calculate which byte the partition boundary falls in */
    int boundary_byte = partition_bits / 8;
    int boundary_bit = partition_bits % 8;

    /* Randomize bytes after the partition boundary */
    for (int i = boundary_byte; i < 20; i++) {
        if (i == boundary_byte && boundary_bit > 0) {
            /* This byte is split: preserve high bits, randomize low bits */
            uint8_t high_mask = (uint8_t)(0xFF << (8 - boundary_bit));
            uint8_t low_mask = ~high_mask;

            uint8_t preserved_bits = new_node_id[i] & high_mask;
            uint8_t random_bits = (uint8_t)(rand() % 256) & low_mask;

            new_node_id[i] = preserved_bits | random_bits;
        } else if (i > boundary_byte) {
            /* Fully randomize this byte */
            new_node_id[i] = (uint8_t)(rand() % 256);
        }
    }

    log_msg(LOG_DEBUG, "[keyspace] Perturbed node ID for partition %u/%u: %02x%02x%02x%02x... -> %02x%02x%02x%02x...",
            partition_index, num_partitions,
            original_node_id[0], original_node_id[1], original_node_id[2], original_node_id[3],
            new_node_id[0], new_node_id[1], new_node_id[2], new_node_id[3]);
}

uint32_t keyspace_get_partition(const uint8_t *node_id, uint32_t num_partitions) {
    if (!node_id || num_partitions == 0) {
        return 0;
    }

    /* Calculate partition bits */
    int partition_bits = calculate_partition_bits(num_partitions);

    /* Extract partition index from high-order bits */
    uint32_t partition = 0;

    if ((num_partitions & (num_partitions - 1)) == 0) {
        /* Power of 2: extract from high bits */
        if (partition_bits <= 8) {
            int shift = 8 - partition_bits;
            partition = node_id[0] >> shift;
        } else {
            /* Multi-byte partition index */
            int remaining_bits = partition_bits;
            int byte_idx = 0;

            while (remaining_bits > 0 && byte_idx < 20) {
                int bits_in_byte = (remaining_bits > 8) ? 8 : remaining_bits;

                partition = (partition << bits_in_byte) | node_id[byte_idx];

                remaining_bits -= bits_in_byte;
                byte_idx++;
            }

            /* Clear any excess bits if we read too many */
            int excess_bits = (byte_idx * 8) - partition_bits;
            if (excess_bits > 0) {
                partition >>= excess_bits;
            }
        }
    } else {
        /* Non-power-of-2: use scaled approach */
        uint64_t scale = ((uint64_t)node_id[0] * num_partitions) / 256ULL;
        partition = (uint32_t)scale;
    }

    /* Ensure we don't exceed bounds due to rounding */
    if (partition >= num_partitions) {
        partition = num_partitions - 1;
    }

    return partition;
}

bool keyspace_verify_partition(const uint8_t *node_id,
                               uint32_t expected_partition,
                               uint32_t num_partitions) {
    if (!node_id || num_partitions == 0 || expected_partition >= num_partitions) {
        return false;
    }

    uint32_t actual_partition = keyspace_get_partition(node_id, num_partitions);

    if (actual_partition != expected_partition) {
        log_msg(LOG_WARN, "[keyspace] Partition mismatch: expected %u, got %u (node_id: %02x%02x%02x%02x...)",
                expected_partition, actual_partition,
                node_id[0], node_id[1], node_id[2], node_id[3]);
        return false;
    }

    return true;
}
