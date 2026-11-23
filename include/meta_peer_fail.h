#ifndef META_PEER_FAIL_H
#define META_PEER_FAIL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

/**
 * TTL-aged per-address TCP connect-failure memory for the metadata fetcher.
 *
 * Measured baseline: ~83 % of the metadata failure mass is a TCP connect that
 * never completes (reports/triage.md), and the peer lists handed to the fetch
 * are arrival-ordered with no dedup, so the same unreachable address is retried
 * across infohashes and across trees. This map remembers the addresses whose
 * *connect* failed and lets window selection step over them for a while.
 *
 * It is deliberately the opposite of the existing infohash strike blooms
 * (src/tree_metadata.c): address-keyed rather than infohash-keyed, TTL-aged
 * rather than permanent, and consulted at *selection* rather than at *outcome*.
 *
 * Shape: one supervisor-shared instance (cross-tree learning, and it survives a
 * tree respawn - which fires exactly when the metadata rate is low, i.e. when
 * the memory is worth the most), open addressing with FNV-1a + linear probing,
 * a load-factor clear and lazy TTL ageing on the lookup/insert path only (no
 * thread, no sweep).
 *
 * Fail-open is the hard contract: a NULL handle makes every function a no-op
 * and every lookup answer "do not skip", so a disabled map leaves peer
 * selection byte-identical to legacy. ttl_sec = 0 therefore never creates a
 * map at all, and a creation failure is non-fatal. A cleared table is also
 * fail-open by construction: a clear means "legacy selection".
 *
 * Thread safety: every function takes the map's own mutex. Nothing else is
 * held when it is taken (the fetch path holds no locks at all), so there is no
 * lock-order hazard.
 */

/** Key layout: family (1) + port in network order (2) + address (16).
 * IPv4-mapped IPv6 (::ffff:a.b.c.d) folds to family AF_INET so it collides with
 * the same peer reached as native IPv4; the family byte keeps a native IPv6
 * key distinct from any IPv4 key. 19 bytes - the slot pads to 32. */
#define META_PEER_FAIL_KEY_LEN 19

/** Shipped sizing: 131,072 slots x 32 B = 4 MiB (slot bytes are asserted in
 * src/meta_peer_fail.c). The sizing rule: the 3/4-load clear threshold has to
 * hold the whole TTL working set without a clear, i.e. it must exceed the
 * worst-case number of live entries. Measured ~110-133 NEW distinct connect
 * failure addresses/s x a 600 s TTL = 66,000-79,800 live entries, and
 * 131,072 x 3/4 = 98,304 clear threshold (~23 % margin over that worst case).
 * The original 8192 slots (clear at 6,144) were wiped every ~50-70 s at the
 * measured rate, so entries died by capacity clear instead of by TTL and the
 * knob could not age anything out (reports/w3-sizing-blocker.md). 4 MiB is
 * ~0.2 % of the measured 2.04 GB crawler RSS. */
#define META_PEER_FAIL_CAPACITY 131072

typedef struct meta_peer_fail_set meta_peer_fail_set_t;

/**
 * Create a map with [capacity] slots (rounded up to a power of two) and a
 * [ttl_sec] memory window. [ttl_sec] <= 0 means "disabled" and returns NULL.
 * @return Pointer to the map, or NULL on error/disabled. NULL is the shipped
 *         default and means "legacy selection".
 */
meta_peer_fail_set_t *meta_peer_fail_create(size_t capacity, int ttl_sec);

/** Free the map. Only call after every worker thread has stopped. */
void meta_peer_fail_destroy(meta_peer_fail_set_t *set);

/**
 * Build the normalized map key for [addr].
 * @return 0 on success, -1 for an address the map refuses (unsupported family,
 *         all-zero or broadcast IPv4, all-zero IPv6, port 0).
 */
int meta_peer_fail_key(const struct sockaddr_storage *addr,
                       uint8_t out[META_PEER_FAIL_KEY_LEN]);

/**
 * Should window selection step over [addr]? True when the map holds an
 * unexpired connect failure for it. Always false for a NULL map or an address
 * the map refuses.
 */
bool meta_peer_fail_should_skip(meta_peer_fail_set_t *set,
                                const struct sockaddr_storage *addr,
                                uint64_t now_ms);

/**
 * Record one failed TCP connect to [addr]. Idempotent per address: a repeat
 * bumps the entry's fail_count and refreshes its timestamp.
 * @return 1 when a NEW address entry was admitted, 0 when an existing one was
 *         bumped (or the address was refused / the map is NULL).
 */
int meta_peer_fail_record(meta_peer_fail_set_t *set,
                          const struct sockaddr_storage *addr,
                          uint64_t now_ms);

/**
 * Count one skipped window SLOT. Peers are not deduplicated, so the same
 * address can legitimately be skipped several times inside one attempt: the
 * counter's unit is slots, not distinct addresses.
 */
void meta_peer_fail_note_skip(meta_peer_fail_set_t *set);

/**
 * Snapshot for /stats and the shutdown log.
 *
 * addresses counts NEW distinct addresses admitted since start (not events);
 * bumped counts repeat failures on an address already known; skipped counts
 * skipped slots; expired counts lookups that found an entry past its TTL;
 * clears counts load-factor clears (each one a return to legacy selection).
 */
typedef struct {
    uint64_t capacity;    /* slots (power of two) */
    uint64_t held;        /* unexpired entries right now */
    uint64_t addresses;   /* new address entries admitted since start */
    uint64_t bumped;      /* repeat failures on a known address */
    uint64_t skipped;     /* window slots skipped since start */
    uint64_t expired;     /* lookups that found an expired entry */
    uint64_t clears;      /* load-factor clears */
    int ttl_sec;          /* memory window in seconds (0 = disabled) */
} meta_peer_fail_stats_t;

/** The monotonic clock the map's timestamps are stamped with, so a caller can
 * pass the same time base into the functions above (and tests can inject one). */
uint64_t meta_peer_fail_now_ms(void);

/** Fill [out] at [now_ms] (the [held] count is TTL-aged against it). A NULL map
 * leaves [out] zeroed (ttl_sec 0 = disabled). */
void meta_peer_fail_stats(meta_peer_fail_set_t *set, uint64_t now_ms,
                          meta_peer_fail_stats_t *out);

#endif /* META_PEER_FAIL_H */
