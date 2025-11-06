#ifndef DISCOVERED_NODES_H
#define DISCOVERED_NODES_H

#include <stdint.h>
#include <sys/socket.h>
#include <time.h>
#include <pthread.h>

/* Discovery source types */
typedef enum {
    SOURCE_QUERY_RESPONSE = 0,
    SOURCE_INCOMING_REQUEST,
    SOURCE_BOOTSTRAP
} discovery_source_t;

/* Discovered node entry */
typedef struct {
    unsigned char node_id[20];
    struct sockaddr_storage addr;
    socklen_t addr_len;
    time_t discovered_at;
    discovery_source_t source;
} discovered_node_t;

/* Discovered nodes queue with deduplication */
typedef struct {
    discovered_node_t *nodes;
    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;

    /* Statistics */
    uint64_t total_enqueued;
    uint64_t total_dequeued;
    uint64_t duplicates_filtered;
    uint64_t queue_full_drops;
} discovered_nodes_queue_t;

/* Function declarations */
int discovered_nodes_init(discovered_nodes_queue_t *queue, size_t capacity);
void discovered_nodes_cleanup(discovered_nodes_queue_t *queue);

int discovered_nodes_enqueue(discovered_nodes_queue_t *queue,
                             const unsigned char *node_id,
                             const struct sockaddr *addr,
                             socklen_t addr_len,
                             discovery_source_t source);

int discovered_nodes_dequeue(discovered_nodes_queue_t *queue,
                             discovered_node_t *node,
                             int timeout_ms);

int discovered_nodes_dequeue_batch(discovered_nodes_queue_t *queue,
                                   discovered_node_t *nodes,
                                   size_t max_nodes,
                                   int timeout_ms);

size_t discovered_nodes_count(discovered_nodes_queue_t *queue);
int discovered_nodes_is_empty(discovered_nodes_queue_t *queue);
int discovered_nodes_is_full(discovered_nodes_queue_t *queue);

/* Print statistics */
void discovered_nodes_print_stats(discovered_nodes_queue_t *queue);

#endif /* DISCOVERED_NODES_H */
