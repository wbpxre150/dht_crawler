#ifndef INFLIGHT_SET_H
#define INFLIGHT_SET_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * Exact, releasable in-flight claim set for infohash deduplication.
 *
 * The shared bloom filter records hashes that are *known*: it is written only
 * after a metadata fetch succeeds (src/tree_metadata.c) or after the batch
 * containing the row is committed (src/database.c, src/batch_writer.c). It can
 * therefore say nothing about a hash that is *being worked on* — the window
 * between discovery (a BEP51 sample), the get_peers lookup, the metadata fetch
 * and the eventual batch commit. Every tree that sampled the same infohash
 * inside that window saw it as unseen and fetched it again, and the batch
 * writer counted each re-fetch as a new row: the torrents insert is
 * INSERT OR REPLACE, which returns SQLITE_DONE for a duplicate.
 *
 * This set closes that window. A worker *claims* an infohash at discovery and
 * *releases* the claim when the attempt resolves, so a second tree cannot start
 * the same work. Unlike a bloom filter a claim can be released, so a hash that
 * is never fetched (full queue, tree shutdown, tree respawn) is not lost.
 *
 * Thread safety: every function takes the set's internal mutex.
 */

typedef struct inflight_set inflight_set_t;

/**
 * Create a claim set with [capacity] slots (rounded up to a power of two) and a
 * stale-claim backstop of [ttl_sec] (0 = no TTL).
 * @return Pointer to the set, or NULL on error. A NULL set means "dedup
 *         disabled": every function fails open and suppresses nothing.
 */
inflight_set_t *inflight_set_create(size_t capacity, int ttl_sec);

/** Free the set. Only call after every worker thread has stopped. */
void inflight_set_destroy(inflight_set_t *set);

/**
 * Atomically claim [hash] on behalf of [tree_id].
 * @return true when the caller now owns the claim (it must release it), false
 *         when another worker already holds it — or when [set] is NULL, which
 *         fails open so a disabled set never suppresses work.
 */
bool inflight_set_claim(inflight_set_t *set, const unsigned char *hash, uint32_t tree_id);

/**
 * Release a claim taken by inflight_set_claim().
 * A no-op when the claim is gone (already released, expired by TTL, or dropped
 * by inflight_set_release_tree), so a late release is always safe.
 */
void inflight_set_release(inflight_set_t *set, const unsigned char *hash);

/**
 * Drop every claim held by [tree_id]. Called from thread_tree_destroy() so a
 * respawned tree can rediscover the hashes its predecessor never resolved
 * instead of waiting for the TTL.
 */
void inflight_set_release_tree(inflight_set_t *set, uint32_t tree_id);

/**
 * Snapshot of the set's counters, for /stats and the shutdown log.
 *
 * refused is the interesting one: it counts claims that hit a live held slot,
 * i.e. the duplicate fetches this set actually prevented. Everything else is
 * bookkeeping, and still_held should hover near the number of in-flight
 * fetches rather than grow.
 */
typedef struct {
    uint64_t capacity;       /* slots (power of two) */
    uint64_t held;           /* claims held right now */
    uint64_t claimed;        /* claims granted since start */
    uint64_t refused;        /* duplicate claims refused since start */
    uint64_t released;       /* claims released by their worker */
    uint64_t expired;        /* stale claims taken over after the TTL */
    uint64_t tree_released;  /* claims dropped by inflight_set_release_tree() */
    uint64_t clears;         /* load-factor clears */
} inflight_stats_t;

/** Fill [out]. A NULL set leaves [out] zeroed. */
void inflight_set_stats(inflight_set_t *set, inflight_stats_t *out);

#endif /* INFLIGHT_SET_H */
