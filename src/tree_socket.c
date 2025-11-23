#include "tree_socket.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <errno.h>
#include <arpa/inet.h>

tree_socket_t *tree_socket_create(int port) {
    tree_socket_t *sock = calloc(1, sizeof(tree_socket_t));
    if (!sock) {
        return NULL;
    }

    /* Create UDP socket */
    sock->fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock->fd < 0) {
        log_msg(LOG_ERROR, "[tree_socket] Failed to create socket: %s", strerror(errno));
        free(sock);
        return NULL;
    }

    /* Set non-blocking */
    int flags = fcntl(sock->fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(sock->fd, F_SETFL, flags | O_NONBLOCK);
    }

    /* Allow address reuse */
    int opt = 1;
    setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    /* Bind to address */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons((uint16_t)port);

    if (bind(sock->fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        log_msg(LOG_ERROR, "[tree_socket] Failed to bind to port %d: %s", port, strerror(errno));
        close(sock->fd);
        free(sock);
        return NULL;
    }

    /* Get actual bound address (in case port was 0) */
    socklen_t addrlen = sizeof(sock->local_addr);
    if (getsockname(sock->fd, (struct sockaddr *)&sock->local_addr, &addrlen) < 0) {
        log_msg(LOG_WARN, "[tree_socket] Failed to get local address");
    }

    pthread_mutex_init(&sock->send_lock, NULL);

    int bound_port = tree_socket_get_port(sock);
    log_msg(LOG_DEBUG, "[tree_socket] Created socket on port %d", bound_port);

    return sock;
}

void tree_socket_destroy(tree_socket_t *sock) {
    if (!sock) {
        return;
    }

    if (sock->fd >= 0) {
        close(sock->fd);
    }

    pthread_mutex_destroy(&sock->send_lock);
    free(sock);
}

int tree_socket_send(tree_socket_t *sock, const void *data, size_t len,
                     const struct sockaddr_storage *dest) {
    if (!sock || !data || !dest) {
        return -1;
    }

    pthread_mutex_lock(&sock->send_lock);

    socklen_t addrlen;
    if (dest->ss_family == AF_INET) {
        addrlen = sizeof(struct sockaddr_in);
    } else if (dest->ss_family == AF_INET6) {
        addrlen = sizeof(struct sockaddr_in6);
    } else {
        log_msg(LOG_DEBUG, "[tree_socket] Unknown address family: %d", dest->ss_family);
        pthread_mutex_unlock(&sock->send_lock);
        return -1;
    }

    /* IPv4 socket can't send to IPv6 addresses */
    if (dest->ss_family == AF_INET6) {
        log_msg(LOG_DEBUG, "[tree_socket] Cannot send IPv6 through IPv4 socket");
        pthread_mutex_unlock(&sock->send_lock);
        return -1;
    }

    ssize_t sent = sendto(sock->fd, data, len, 0, (const struct sockaddr *)dest, addrlen);

    pthread_mutex_unlock(&sock->send_lock);

    if (sent < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            log_msg(LOG_DEBUG, "[tree_socket] sendto failed: %s", strerror(errno));
        }
        return -1;
    }

    return (int)sent;
}

int tree_socket_recv(tree_socket_t *sock, void *buf, size_t buflen,
                     struct sockaddr_storage *from, int timeout_ms) {
    if (!sock || !buf) {
        return -1;
    }

    /* Use poll for timeout */
    if (timeout_ms >= 0) {
        struct pollfd pfd;
        pfd.fd = sock->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;

        int ret = poll(&pfd, 1, timeout_ms);
        if (ret == 0) {
            return 0;  /* Timeout */
        }
        if (ret < 0) {
            if (errno == EINTR) {
                return 0;  /* Interrupted, treat as timeout */
            }
            return -1;
        }
    }

    socklen_t fromlen = sizeof(struct sockaddr_storage);
    ssize_t received = recvfrom(sock->fd, buf, buflen, 0,
                                 from ? (struct sockaddr *)from : NULL,
                                 from ? &fromlen : NULL);

    if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;  /* No data available */
        }
        return -1;
    }

    return (int)received;
}

int tree_socket_get_port(tree_socket_t *sock) {
    if (!sock) {
        return -1;
    }

    if (sock->local_addr.ss_family == AF_INET) {
        struct sockaddr_in *addr = (struct sockaddr_in *)&sock->local_addr;
        return ntohs(addr->sin_port);
    } else if (sock->local_addr.ss_family == AF_INET6) {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)&sock->local_addr;
        return ntohs(addr->sin6_port);
    }

    return -1;
}
