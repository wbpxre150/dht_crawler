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
        log_msg(LOG_ERROR, "[tree_socket] Failed to allocate socket structure");
        return NULL;
    }

    /* Create UDP socket */
    sock->fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock->fd < 0) {
        log_msg(LOG_ERROR, "[tree_socket] Failed to create socket: %s", strerror(errno));
        free(sock);
        return NULL;
    }

    log_msg(LOG_INFO, "[tree_socket] Created socket with fd=%d", sock->fd);

    /* Set non-blocking */
    int flags = fcntl(sock->fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(sock->fd, F_SETFL, flags | O_NONBLOCK);
        log_msg(LOG_DEBUG, "[tree_socket] Set socket fd=%d to non-blocking mode", sock->fd);
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
        log_msg(LOG_ERROR, "[tree_socket] Failed to bind fd=%d to port %d: %s", sock->fd, port, strerror(errno));
        close(sock->fd);
        free(sock);
        return NULL;
    }

    /* Get actual bound address (in case port was 0) */
    socklen_t addrlen = sizeof(sock->local_addr);
    if (getsockname(sock->fd, (struct sockaddr *)&sock->local_addr, &addrlen) < 0) {
        log_msg(LOG_WARN, "[tree_socket] Failed to get local address for fd=%d", sock->fd);
    }

    pthread_mutex_init(&sock->send_lock, NULL);

    int bound_port = tree_socket_get_port(sock);
    log_msg(LOG_INFO, "[tree_socket] ===== SOCKET CREATED: fd=%d, port=%d =====", sock->fd, bound_port);

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
        log_msg(LOG_ERROR, "[tree_socket] send: NULL parameter (sock=%p, data=%p, dest=%p)",
                (void*)sock, data, (void*)dest);
        return -1;
    }

    pthread_mutex_lock(&sock->send_lock);

    /* Validate address family is initialized */
    if (dest->ss_family == 0) {
        log_msg(LOG_ERROR, "[tree_socket] Address family not initialized (ss_family=0) - this is a bug!");
        pthread_mutex_unlock(&sock->send_lock);
        return -1;
    }

    socklen_t addrlen;
    if (dest->ss_family == AF_INET) {
        addrlen = sizeof(struct sockaddr_in);
    } else if (dest->ss_family == AF_INET6) {
        addrlen = sizeof(struct sockaddr_in6);
    } else {
        log_msg(LOG_ERROR, "[tree_socket] Unknown address family: %d", dest->ss_family);
        pthread_mutex_unlock(&sock->send_lock);
        return -1;
    }

    /* IPv4 socket can't send to IPv6 addresses */
    if (dest->ss_family == AF_INET6) {
        log_msg(LOG_ERROR, "[tree_socket] Cannot send IPv6 through IPv4 socket");
        pthread_mutex_unlock(&sock->send_lock);
        return -1;
    }

    /* DEBUG: Log destination before sending */
    char ip_str[INET6_ADDRSTRLEN] = {0};
    uint16_t port = 0;
    if (dest->ss_family == AF_INET) {
        const struct sockaddr_in *sin = (const struct sockaddr_in *)dest;
        inet_ntop(AF_INET, &sin->sin_addr, ip_str, sizeof(ip_str));
        port = ntohs(sin->sin_port);
    }

    static unsigned long send_count = 0;
    send_count++;

    ssize_t sent = sendto(sock->fd, data, len, 0, (const struct sockaddr *)dest, addrlen);

    pthread_mutex_unlock(&sock->send_lock);

    if (sent < 0) {
        log_msg(LOG_ERROR, "[tree_socket] sendto FAILED: fd=%d, len=%zu, dest=%s:%u, errno=%d (%s)",
                sock->fd, len, ip_str, port, errno, strerror(errno));
        return -1;
    }

    /* DEBUG: Log successful sends (first 10, then every 100) */
    if (send_count <= 10 || send_count % 100 == 0) {
        log_msg(LOG_DEBUG, "[tree_socket] sendto SUCCESS #%lu: fd=%d, sent=%zd bytes to %s:%u",
                send_count, sock->fd, sent, ip_str, port);
    }

    return (int)sent;
}

int tree_socket_recv(tree_socket_t *sock, void *buf, size_t buflen,
                     struct sockaddr_storage *from, int timeout_ms) {
    if (!sock || !buf) {
        log_msg(LOG_ERROR, "[tree_socket] recv: NULL parameter (sock=%p, buf=%p)",
                (void*)sock, buf);
        return -1;
    }

    static unsigned long recv_call_count = 0;
    static unsigned long recv_timeout_count = 0;
    static unsigned long recv_success_count = 0;
    recv_call_count++;

    /* Use poll for timeout */
    if (timeout_ms >= 0) {
        struct pollfd pfd;
        pfd.fd = sock->fd;
        pfd.events = POLLIN;
        pfd.revents = 0;

        int ret = poll(&pfd, 1, timeout_ms);
        if (ret == 0) {
            /* Timeout - this is normal */
            recv_timeout_count++;
            if (recv_call_count <= 10 || (recv_timeout_count % 1000 == 0)) {
                log_msg(LOG_DEBUG, "[tree_socket] poll TIMEOUT: fd=%d, timeout_ms=%d (call #%lu, timeouts=%lu)",
                        sock->fd, timeout_ms, recv_call_count, recv_timeout_count);
            }
            return 0;  /* Timeout */
        }
        if (ret < 0) {
            if (errno == EINTR) {
                log_msg(LOG_DEBUG, "[tree_socket] poll INTERRUPTED: fd=%d", sock->fd);
                return 0;  /* Interrupted, treat as timeout */
            }
            log_msg(LOG_ERROR, "[tree_socket] poll FAILED: fd=%d, errno=%d (%s)",
                    sock->fd, errno, strerror(errno));
            return -1;
        }

        /* DEBUG: Log when poll() indicates data is ready */
        if (recv_call_count <= 10 || recv_success_count % 100 == 0) {
            log_msg(LOG_DEBUG, "[tree_socket] poll says DATA READY: fd=%d, revents=0x%x",
                    sock->fd, pfd.revents);
        }
    }

    socklen_t fromlen = sizeof(struct sockaddr_storage);
    ssize_t received = recvfrom(sock->fd, buf, buflen, 0,
                                 from ? (struct sockaddr *)from : NULL,
                                 from ? &fromlen : NULL);

    if (received < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            log_msg(LOG_DEBUG, "[tree_socket] recvfrom: WOULD BLOCK (fd=%d)", sock->fd);
            return 0;  /* No data available */
        }
        log_msg(LOG_ERROR, "[tree_socket] recvfrom FAILED: fd=%d, errno=%d (%s)",
                sock->fd, errno, strerror(errno));
        return -1;
    }

    /* DEBUG: Log successful receives */
    recv_success_count++;
    char from_ip[INET6_ADDRSTRLEN] = {0};
    uint16_t from_port = 0;
    if (from && from->ss_family == AF_INET) {
        struct sockaddr_in *sin = (struct sockaddr_in *)from;
        inet_ntop(AF_INET, &sin->sin_addr, from_ip, sizeof(from_ip));
        from_port = ntohs(sin->sin_port);
    }

    if (recv_success_count <= 10 || recv_success_count % 100 == 0) {
        log_msg(LOG_INFO, "[tree_socket] recvfrom SUCCESS #%lu: fd=%d, received=%zd bytes from %s:%u",
                recv_success_count, sock->fd, received, from_ip, from_port);
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
