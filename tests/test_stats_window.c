/**
 * Host unit tests for the last-interval stats sampler (include/stats_window.h).
 *
 * The sampler is what lets a single /stats snapshot report a window rate
 * instead of only a lifetime total, so the invariants that matter are:
 *
 *   (a) the first sample stores a baseline and reports all-zero deltas (a
 *       freshly started crawler must not publish a spike),
 *   (b) later samples report the per-counter difference since the previous
 *       sample,
 *   (c) a counter that moved backwards reports 0 for THAT counter only and
 *       never wraps (unsigned subtraction would report ~1.8e19),
 *   (d) the snapshot always advances, including after a backwards jump, so the
 *       next interval is measured from the new baseline,
 *   (e) a NULL state / NULL output array is a no-op rather than a crash.
 *
 * Standalone: no test framework, no supervisor, no civetweb, no database.
 *
 *     make tests-stats-window
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "stats_window.h"

static int failures = 0;

#define CHECK(cond, ...) do {                                       \
        if (!(cond)) {                                              \
            printf("FAIL: ");                                       \
            printf(__VA_ARGS__);                                    \
            printf(" (%s:%d)\n", __FILE__, __LINE__);               \
            failures++;                                             \
        }                                                           \
    } while (0)

/* (a) The first sample is a baseline: zeros out, snapshot stored. */
static void test_first_sample_is_zero(void) {
    stats_window_t w;
    memset(&w, 0, sizeof(w));

    uint64_t deltas[4] = {7, 7, 7, 7};
    stats_window_sample(&w, 100, 10, 1000, 250, deltas);

    CHECK(w.has_last == 1, "first sample marks the window as primed");
    CHECK(w.last_attempts == 100, "attempts baseline stored, got %llu",
          (unsigned long long)w.last_attempts);
    CHECK(w.last_fetched == 10, "fetched baseline stored, got %llu",
          (unsigned long long)w.last_fetched);
    CHECK(w.last_lookups == 1000, "lookups baseline stored, got %llu",
          (unsigned long long)w.last_lookups);
    CHECK(w.last_with_peers == 250, "with_peers baseline stored, got %llu",
          (unsigned long long)w.last_with_peers);

    CHECK(deltas[0] == 0 && deltas[1] == 0 && deltas[2] == 0 && deltas[3] == 0,
          "first sample reports zeros, got %llu/%llu/%llu/%llu",
          (unsigned long long)deltas[0], (unsigned long long)deltas[1],
          (unsigned long long)deltas[2], (unsigned long long)deltas[3]);
}

/* (b) Monotonic counters produce plain differences, and the snapshot advances. */
static void test_monotonic_deltas(void) {
    stats_window_t w;
    memset(&w, 0, sizeof(w));

    uint64_t deltas[4];
    stats_window_sample(&w, 100, 10, 1000, 250, deltas);
    stats_window_sample(&w, 140, 12, 1300, 300, deltas);

    CHECK(deltas[0] == 40, "attempts delta 40, got %llu", (unsigned long long)deltas[0]);
    CHECK(deltas[1] == 2, "fetched delta 2, got %llu", (unsigned long long)deltas[1]);
    CHECK(deltas[2] == 300, "lookups delta 300, got %llu", (unsigned long long)deltas[2]);
    CHECK(deltas[3] == 50, "with_peers delta 50, got %llu", (unsigned long long)deltas[3]);
    CHECK(w.last_attempts == 140 && w.last_fetched == 12 &&
          w.last_lookups == 1300 && w.last_with_peers == 300,
          "snapshot advanced to the second sample");

    /* A third interval starts from the second sample, not from the first. */
    stats_window_sample(&w, 150, 12, 1400, 300, deltas);
    CHECK(deltas[0] == 10, "third-interval attempts delta 10, got %llu",
          (unsigned long long)deltas[0]);
    CHECK(deltas[1] == 0, "third-interval fetched delta 0, got %llu",
          (unsigned long long)deltas[1]);
    CHECK(deltas[2] == 100, "third-interval lookups delta 100, got %llu",
          (unsigned long long)deltas[2]);
    CHECK(deltas[3] == 0, "third-interval with_peers delta 0, got %llu",
          (unsigned long long)deltas[3]);
}

/* (c) A backwards jump saturates at 0 per counter — no unsigned wrap. */
static void test_backwards_jump_saturates(void) {
    stats_window_t w;
    memset(&w, 0, sizeof(w));

    uint64_t deltas[4];
    stats_window_sample(&w, 500, 50, 5000, 500, deltas);

    /* Only the attempts counter moves backwards (e.g. a fold was rebuilt); the
     * other three keep advancing and must still report their real deltas. */
    stats_window_sample(&w, 400, 55, 5100, 520, deltas);
    CHECK(deltas[0] == 0, "backwards attempts saturate at 0, got %llu",
          (unsigned long long)deltas[0]);
    CHECK(deltas[1] == 5, "forward fetched still reported, got %llu",
          (unsigned long long)deltas[1]);
    CHECK(deltas[2] == 100, "forward lookups still reported, got %llu",
          (unsigned long long)deltas[2]);
    CHECK(deltas[3] == 20, "forward with_peers still reported, got %llu",
          (unsigned long long)deltas[3]);

    /* Every counter backwards at once: all zeros, no wrap. */
    stats_window_sample(&w, 1, 1, 1, 1, deltas);
    CHECK(deltas[0] == 0 && deltas[1] == 0 && deltas[2] == 0 && deltas[3] == 0,
          "all-backwards sample reports zeros, got %llu/%llu/%llu/%llu",
          (unsigned long long)deltas[0], (unsigned long long)deltas[1],
          (unsigned long long)deltas[2], (unsigned long long)deltas[3]);

    /* And a counter that merely stays equal is 0, not a wrap of 0. */
    stats_window_sample(&w, 1, 1, 1, 1, deltas);
    CHECK(deltas[0] == 0 && deltas[1] == 0 && deltas[2] == 0 && deltas[3] == 0,
          "unchanged counters report zeros");
}

/* (d) After a backwards jump the snapshot advances to the lower value. */
static void test_snapshot_advances_after_backwards_jump(void) {
    stats_window_t w;
    memset(&w, 0, sizeof(w));

    uint64_t deltas[4];
    stats_window_sample(&w, 900, 90, 9000, 900, deltas);
    stats_window_sample(&w, 100, 10, 1000, 100, deltas);   /* backwards */
    CHECK(w.last_attempts == 100 && w.last_fetched == 10 &&
          w.last_lookups == 1000 && w.last_with_peers == 100,
          "snapshot advanced to the lower sample");

    /* The next interval is measured from 100, not from 900. */
    stats_window_sample(&w, 130, 11, 1100, 140, deltas);
    CHECK(deltas[0] == 30, "post-jump attempts delta 30, got %llu",
          (unsigned long long)deltas[0]);
    CHECK(deltas[1] == 1, "post-jump fetched delta 1, got %llu",
          (unsigned long long)deltas[1]);
    CHECK(deltas[2] == 100, "post-jump lookups delta 100, got %llu",
          (unsigned long long)deltas[2]);
    CHECK(deltas[3] == 40, "post-jump with_peers delta 40, got %llu",
          (unsigned long long)deltas[3]);
}

/* (e) NULL state and NULL output array are no-ops, not crashes. */
static void test_null_inputs(void) {
    uint64_t deltas[4] = {9, 9, 9, 9};

    stats_window_sample(NULL, 1, 2, 3, 4, deltas);
    CHECK(deltas[0] == 0 && deltas[1] == 0 && deltas[2] == 0 && deltas[3] == 0,
          "NULL state still zeroes the output array");

    stats_window_t w;
    memset(&w, 0, sizeof(w));
    stats_window_sample(&w, 10, 1, 100, 10, NULL);
    CHECK(w.has_last == 1 && w.last_attempts == 10,
          "NULL output array still advances the snapshot");
}

int main(void) {
    printf("test_stats_window\n");

    test_first_sample_is_zero();
    test_monotonic_deltas();
    test_backwards_jump_saturates();
    test_snapshot_advances_after_backwards_jump();
    test_null_inputs();

    if (failures != 0) {
        printf("%d check(s) FAILED\n", failures);
        return 1;
    }

    printf("all checks passed\n");
    return 0;
}
