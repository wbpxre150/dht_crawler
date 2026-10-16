#include "udp_rate_limit.h"
#include "dht_crawler.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <time.h>

/*
 * Fixed-point scaling: one token is represented as UDP_RATE_LIMIT_TOKEN_UNITS
 * micro-tokens. This keeps sub-token precision without floating point on the
 * hot path. The refill rate is pps tokens/second, so
 *
 *     units added over `elapsed_ns` = elapsed_ns * pps / 1000
 *
 * (1 ns * 1 pps = 1e-9 tokens = 1e-3 micro-tokens).
 */
#define TOKEN_UNITS 1000000ULL

/*
 * Refill maths never needs more than one second of elapsed time: even the
 * slowest supported rate (100 pps, burst 10) refills the whole bucket in
 * 100 ms. Clamping the elapsed time keeps the multiply well inside uint64.
 */
#define MAX_REFILL_NS 1000000000ULL

/* Backoff bounds for the throttled path (about half a token, clamped). */
#define MIN_SLEEP_NS 50000ULL      /* 50 us  */
#define MAX_SLEEP_NS 50000000ULL   /* 50 ms  */

static atomic_uint_fast64_t g_pps = 0;             /* configured pps, 0 = disabled */
static atomic_bool g_enabled = false;              /* false while disabled or shut down */
static atomic_uint_fast64_t g_token_units = 0;     /* current tokens, scaled by TOKEN_UNITS */
static atomic_uint_fast64_t g_last_refill_ns = 0;  /* CLOCK_MONOTONIC ns of last refill */
static atomic_uint_fast64_t g_burst_units = 0;     /* burst capacity, scaled by TOKEN_UNITS */
static atomic_uint_fast64_t g_sends_total = 0;     /* acquire() calls (UDP send attempts) */
static atomic_uint_fast64_t g_throttled_total = 0; /* acquire() calls that had to sleep */

static uint64_t udp_rate_limit_monotonic_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

void udp_rate_limit_init(uint32_t pps) {
    uint32_t effective = pps;
    uint64_t burst = 0;

    if (effective != 0) {
        if (effective < UDP_RATE_LIMIT_MIN_PPS) {
            effective = UDP_RATE_LIMIT_MIN_PPS;
        } else if (effective > UDP_RATE_LIMIT_MAX_PPS) {
            effective = UDP_RATE_LIMIT_MAX_PPS;
        }

        /* Burst allowance is a tenth of a second's worth of tokens, clamped. */
        burst = (uint64_t)effective / 10ULL;
        if (burst < 8ULL) {
            burst = 8ULL;
        } else if (burst > 128ULL) {
            burst = 128ULL;
        }
    }

    atomic_store_explicit(&g_pps, effective, memory_order_relaxed);
    atomic_store_explicit(&g_burst_units, burst * TOKEN_UNITS, memory_order_relaxed);
    atomic_store_explicit(&g_token_units, burst * TOKEN_UNITS, memory_order_relaxed);
    atomic_store_explicit(&g_last_refill_ns, udp_rate_limit_monotonic_ns(), memory_order_relaxed);
    atomic_store_explicit(&g_enabled, effective != 0, memory_order_release);

    if (effective == 0) {
        log_msg(LOG_DEBUG, "[udp_rate_limit] disabled (UDP rate limiting off)");
    } else {
        log_msg(LOG_DEBUG, "[udp_rate_limit] enabled: %u pps, burst %llu packets",
                (unsigned)effective, (unsigned long long)burst);
    }
}

void udp_rate_limit_shutdown(void) {
    /* Flip to passthrough. Any thread blocked in udp_rate_limit_acquire()
     * notices on its next loop iteration (within MAX_SLEEP_NS) and returns
     * without a token, so process shutdown can never deadlock here. */
    atomic_store_explicit(&g_enabled, false, memory_order_release);
}

/*
 * Consume one token, or sleep until one is available. Lock-free.
 *
 * Correctness relies on separating the two halves of a token bucket:
 *
 *   1. Refill. A CAS on g_last_refill_ns lets exactly one thread claim a
 *      given interval [last, now). The winner credits that interval's tokens
 *      to g_token_units; losers simply re-read the advanced stamp. This is
 *      what stops the same elapsed time from being credited more than once.
 *
 *   2. Consume. Only after refill (so g_token_units is current as of the
 *      stamp we just published) do we CAS one token out, comparing against a
 *      freshly reloaded value. Comparing against a locally refilled value
 *      would spin forever once the bucket empties: the atomic still holds the
 *      un-refilled value, so the exchange could never match.
 *
 * The two steps are deliberately sequenced in the same loop iteration; the
 * only place we jump back to the top is when a CAS loses a race.
 */
void udp_rate_limit_acquire(void) {
    atomic_fetch_add_explicit(&g_sends_total, 1, memory_order_relaxed);

    if (!atomic_load_explicit(&g_enabled, memory_order_acquire)) {
        return;
    }

    const uint64_t pps = atomic_load_explicit(&g_pps, memory_order_relaxed);
    if (pps == 0) {
        return;
    }

    const uint64_t burst_units = atomic_load_explicit(&g_burst_units, memory_order_relaxed);
    bool throttled = false;

    for (;;) {
        if (!atomic_load_explicit(&g_enabled, memory_order_acquire)) {
            return;
        }

        const uint64_t now = udp_rate_limit_monotonic_ns();
        uint64_t last = atomic_load_explicit(&g_last_refill_ns, memory_order_relaxed);

        /* Step 1: claim the refill interval [last, now) and credit its tokens. */
        if (now > last) {
            uint64_t elapsed = now - last;
            if (elapsed > MAX_REFILL_NS) {
                elapsed = MAX_REFILL_NS;
            }
            const uint64_t add = elapsed * pps / 1000ULL;

            if (atomic_compare_exchange_weak_explicit(&g_last_refill_ns, &last, now,
                                                      memory_order_acq_rel,
                                                      memory_order_relaxed)) {
                if (add > 0) {
                    /* Credit the tokens. A short CAS loop keeps the add
                     * atomic w.r.t. concurrent consumers. */
                    uint64_t cur = atomic_load_explicit(&g_token_units, memory_order_relaxed);
                    for (;;) {
                        uint64_t next = cur + add;
                        if (next > burst_units) {
                            next = burst_units;
                        }
                        if (atomic_compare_exchange_weak_explicit(&g_token_units, &cur, next,
                                                                  memory_order_acq_rel,
                                                                  memory_order_relaxed)) {
                            break;
                        }
                    }
                }
            } else {
                /* Another thread already claimed this interval; re-read. */
                continue;
            }
        }

        /* Step 2: the bucket is current as of the published stamp. Take a token. */
        uint64_t units = atomic_load_explicit(&g_token_units, memory_order_relaxed);
        if (units >= TOKEN_UNITS) {
            if (atomic_compare_exchange_weak_explicit(&g_token_units, &units, units - TOKEN_UNITS,
                                                      memory_order_acq_rel,
                                                      memory_order_relaxed)) {
                return;
            }
            /* Lost the race: recompute against fresh state. */
            continue;
        }

        /* Not enough tokens: sleep about half the time needed for the deficit. */
        uint64_t wait_ns = (TOKEN_UNITS - units) * 1000ULL / pps;
        wait_ns /= 2ULL;
        if (wait_ns < MIN_SLEEP_NS) {
            wait_ns = MIN_SLEEP_NS;
        } else if (wait_ns > MAX_SLEEP_NS) {
            wait_ns = MAX_SLEEP_NS;
        }

        if (!throttled) {
            throttled = true;
            atomic_fetch_add_explicit(&g_throttled_total, 1, memory_order_relaxed);
        }

        struct timespec req;
        req.tv_sec = (time_t)(wait_ns / 1000000000ULL);
        req.tv_nsec = (long)(wait_ns % 1000000000ULL);
        nanosleep(&req, NULL);
    }
}

uint32_t udp_rate_limit_pps(void) {
    return (uint32_t)atomic_load_explicit(&g_pps, memory_order_relaxed);
}

uint64_t udp_rate_limit_sends_total(void) {
    return (uint64_t)atomic_load_explicit(&g_sends_total, memory_order_relaxed);
}

uint64_t udp_rate_limit_throttled_total(void) {
    return (uint64_t)atomic_load_explicit(&g_throttled_total, memory_order_relaxed);
}
