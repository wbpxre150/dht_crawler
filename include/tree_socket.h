#ifndef TREE_SOCKET_H
#define TREE_SOCKET_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>

/**
 * Private UDP socket for thread tree DHT operations
 *
 * Each tree has its own UDP socket for sending/receiving DHT messages.
 */

typedef struct tree_socket {
    int fd;
    struct sockaddr_storage local_addr;
    pthread_mutex_t send_lock;
} tree_socket_t;

/**
 * Create a new UDP socket
 * @param port Port to bind to (0 = let OS choose random port)
 * @return Pointer to socket, or NULL on error
 */
tree_socket_t *tree_socket_create(int port);

/**
 * Destroy a socket and free resources
 * @param sock Socket to destroy
 */
void tree_socket_destroy(tree_socket_t *sock);

/**
 * Send data to a destination
 * @param sock Socket to send from
 * @param data Data to send
 * @param len Length of data
 * @param dest Destination address
 * @return Number of bytes sent, or -1 on error
 */
int tree_socket_send(tree_socket_t *sock, const void *data, size_t len,
                     const struct sockaddr_storage *dest);

/**
 * Receive data with timeout
 * @param sock Socket to receive on
 * @param buf Buffer to receive into
 * @param buflen Size of buffer
 * @param from Address of sender (filled in)
 * @param timeout_ms Timeout in milliseconds (-1 = block forever, 0 = non-blocking)
 * @return Number of bytes received, 0 on timeout, -1 on error
 */
int tree_socket_recv(tree_socket_t *sock, void *buf, size_t buflen,
                     struct sockaddr_storage *from, int timeout_ms);

/**
 * Get the local port the socket is bound to
 * @param sock Socket
 * @return Port number, or -1 on error
 */
int tree_socket_get_port(tree_socket_t *sock);

#endif /* TREE_SOCKET_H */
