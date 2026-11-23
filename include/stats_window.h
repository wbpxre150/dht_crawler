#ifndef STATS_WINDOW_H
#define STATS_WINDOW_H

#include <stdint.h>

/**
 * Last-interval ("windowed") deltas for the cumulative /stats counters.
 *
 * Every rate counter the crawler publishes is cumulative since process start,
 * so a single /stats snapshot cannot say how fast anything is happening right
 * now: a reader has to take two snapshots and difference them host-side. This
 * sampler keeps that difference inside the process, so one snapshot carries the
 * current window beside the lifetime total.
 *
 * The struct is deliberately plain data and the module is pure C99 with no
 * dependency on the supervisor, civetweb, cJSON, a database or libuv, so it is
 * unit-testable on the host (tests/test_stats_window.c). It is not thread-safe
 * by itself: one owner thread (the supervisor monitor thread) calls
 * stats_window_sample() and publishes the result through atomics.
 *
 * Contract:
 *   - the first sample stores the snapshot and reports all-zero deltas (there
 *     is no previous sample to subtract), so a freshly started crawler
 *     publishes zeros rather than a spike;
 *   - a later sample reports the saturating difference per counter: a counter
 *     that moved backwards (a reset, a re-seeded total, a rebuilt fold) reports
 *     0 for that counter only and never wraps to a huge value;
 *   - the snapshot always advances to the values just sampled, including after
 *     a backwards jump, so the next interval is measured from the new baseline.
 */
typedef struct {
    int has_last;                  /* 0 until the first sample has been stored */
    uint64_t last_attempts;        /* metadata attempts at the previous sample */
    uint64_t last_fetched;         /* metadata fetches at the previous sample */
    uint64_t last_lookups;         /* get_peers lookups at the previous sample */
    uint64_t last_with_peers;      /* lookups that returned peers at the previous sample */
} stats_window_t;

/**
 * Take one sample of the four cumulative counters.
 *
 * @param w          Sampler state. Zero-initialise it before the first call
 *                   ({0}); NULL is a no-op (nothing to store, no deltas).
 * @param attempts   Current cumulative metadata attempts
 * @param fetched    Current cumulative metadata fetches
 * @param lookups    Current cumulative get_peers lookups
 * @param with_peers Current cumulative lookups that returned peers
 * @param out_deltas Receives {attempts, fetched, lookups, with_peers} deltas in
 *                   that order. All zeros on the first call. May be NULL.
 */
void stats_window_sample(stats_window_t *w, uint64_t attempts, uint64_t fetched,
                         uint64_t lookups, uint64_t with_peers,
                         uint64_t out_deltas[4]);

#endif /* STATS_WINDOW_H */
