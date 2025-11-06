#ifndef CONNECTION_REQUEST_QUEUE_H
#define CONNECTION_REQUEST_QUEUE_H

#include <stdint.h>
#include <sys/socket.h>
#include <uv.h>

#define MAX_PEERS_PER_REQUEST 50

/* Connection request structure */
typedef struct {
    uint8_t info_hash[20];
    struct sockaddr_storage peers[MAX_PEERS_PER_REQUEST];
    socklen_t peer_lens[MAX_PEERS_PER_REQUEST];
    int peer_count;
} connection_request_t;

/* Connection request queue (opaque) */
typedef struct connection_request_queue connection_request_queue_t;

/* Function declarations */
connection_request_queue_t* connection_request_queue_init(size_t capacity);
int connection_request_queue_push(connection_request_queue_t *queue, connection_request_t *req);
connection_request_t* connection_request_queue_try_pop(connection_request_queue_t *queue);
size_t connection_request_queue_size(connection_request_queue_t *queue);
void connection_request_queue_cleanup(connection_request_queue_t *queue);

#endif /* CONNECTION_REQUEST_QUEUE_H */
