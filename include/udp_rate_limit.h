#ifndef UDP_RATE_LIMIT_H
#define UDP_RATE_LIMIT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Clamp bounds for udp_rate_limit_pps (0 means "disabled" and is not clamped). */
#define UDP_RATE_LIMIT_MIN_PPS 100
#define UDP_RATE_LIMIT_MAX_PPS 20000
#define UDP_RATE_LIMIT_DEFAULT_PPS 1500

/*
 * Process-wide outbound UDP (DHT) token-bucket rate limiter.
 *
 * Every outbound DHT packet goes through udp_rate_limit_acquire() at the top of
 * tree_socket_send(), so a single global bucket bounds the crawler's UDP send
 * rate regardless of how many thread trees/workers are running. TCP metadata
 * traffic uses separate sockets and is not affected.
 *
 * Usage:
 *   - udp_rate_limit_init(pps) once, after config load and before the
 *     supervisor spawns any trees.
 *   - udp_rate_limit_acquire() on the send hot path (atomics only, no mutex).
 *   - udp_rate_limit_shutdown() on the shutdown path; it flips the limiter to
 *     passthrough so threads blocked in acquire() cannot deadlock shutdown.
 */

/* Configure the limiter. pps == 0 disables rate limiting (passthrough).
 * Otherwise pps is clamped to [UDP_RATE_LIMIT_MIN_PPS, UDP_RATE_LIMIT_MAX_PPS]. */
void udp_rate_limit_init(uint32_t pps);

/* Block until one token is available. No-op when disabled or after shutdown.
 * Safe to call from many threads concurrently; uses atomics only. */
void udp_rate_limit_acquire(void);

/* Permanently switch to passthrough (called during process shutdown). */
void udp_rate_limit_shutdown(void);

/* Stats accessors for the /stats endpoint. */
uint32_t udp_rate_limit_pps(void);              /* effective pps, 0 when disabled */
uint64_t udp_rate_limit_sends_total(void);      /* total acquire() calls */
uint64_t udp_rate_limit_throttled_total(void);  /* acquire() calls that slept */

#ifdef __cplusplus
}
#endif

#endif /* UDP_RATE_LIMIT_H */
