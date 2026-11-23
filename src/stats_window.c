/**
 * Last-interval ("windowed") deltas for the cumulative /stats counters.
 *
 * Pure arithmetic over four uint64_t counters: no locks, no clocks, no I/O, so
 * the supervisor's 10 s monitor tick owns one stats_window_t and the host test
 * (tests/test_stats_window.c) exercises every branch directly.
 */
#include "stats_window.h"

/**
 * Difference of two monotonic counters, saturating at 0.
 *
 * A counter that moved backwards is not a rate: reporting (now - last) on an
 * unsigned type would wrap to ~1.8e19 and poison the published percentage. The
 * honest value is "no progress observed", i.e. 0.
 */
static uint64_t window_delta(uint64_t now, uint64_t last) {
    return now > last ? now - last : 0;
}

void stats_window_sample(stats_window_t *w, uint64_t attempts, uint64_t fetched,
                         uint64_t lookups, uint64_t with_peers,
                         uint64_t out_deltas[4]) {
    uint64_t delta_attempts = 0;
    uint64_t delta_fetched = 0;
    uint64_t delta_lookups = 0;
    uint64_t delta_with_peers = 0;

    /* First sample: there is no previous snapshot, so every delta stays 0 and
     * the values just sampled become the baseline. */
    if (w && w->has_last) {
        delta_attempts = window_delta(attempts, w->last_attempts);
        delta_fetched = window_delta(fetched, w->last_fetched);
        delta_lookups = window_delta(lookups, w->last_lookups);
        delta_with_peers = window_delta(with_peers, w->last_with_peers);
    }

    /* Advance the snapshot unconditionally: after a backwards jump the next
     * interval must be measured from the values sampled now, not from the old
     * (higher) ones. */
    if (w) {
        w->has_last = 1;
        w->last_attempts = attempts;
        w->last_fetched = fetched;
        w->last_lookups = lookups;
        w->last_with_peers = with_peers;
    }

    if (out_deltas) {
        out_deltas[0] = delta_attempts;
        out_deltas[1] = delta_fetched;
        out_deltas[2] = delta_lookups;
        out_deltas[3] = delta_with_peers;
    }
}
