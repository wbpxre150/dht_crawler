/**
 * Host unit tests for the in-flight claim set (include/inflight_set.h).
 *
 * The set is the releasable counterpart to the shared bloom filter, and it is
 * what stops several trees from fetching the same infohash at the same time.
 * The invariants that matter:
 *
 *   (a) a held claim is never handed out twice,
 *   (b) a release really frees the key,
 *   (c) a release never makes a *different*, still-held key claimable — that is
 *       what the tombstone state buys, and a small table forces the long
 *       linear-probe chains where a naive "release = mark empty" breaks,
 *   (d) release_tree() frees exactly one tree's claims,
 *   (e) a full table clears itself instead of refusing work,
 *   (f) a NULL set fails open (never suppresses),
 *   (g) exactly one of N threads racing on one key wins.
 *
 * Standalone: no test framework, no database, no bloom filter.
 *
 *     make tests-inflight
 */
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "inflight_set.h"
#include "dht_crawler.h"

static int failures = 0;

#define CHECK(cond, ...) do {                                       \
        if (!(cond)) {                                              \
            printf("FAIL: ");                                       \
            printf(__VA_ARGS__);                                    \
            printf(" (%s:%d)\n", __FILE__, __LINE__);               \
            failures++;                                             \
        }                                                           \
    } while (0)

static void make_hash(unsigned char *hash, uint32_t seed) {
    memset(hash, 0, 20);
    memcpy(hash, &seed, sizeof(seed));
    hash[19] = (unsigned char)(seed >> 24);
}

/* (a), (b) and the stats accounting. */
static void test_claim_release(void) {
    inflight_set_t *set = inflight_set_create(1024, 0);
    CHECK(set != NULL, "create");

    unsigned char h[20];
    make_hash(h, 1);

    CHECK(inflight_set_claim(set, h, 1), "first claim succeeds");
    CHECK(!inflight_set_claim(set, h, 2), "second claim from another tree is refused");

    inflight_set_release(set, h);
    CHECK(inflight_set_claim(set, h, 2), "claim after release succeeds");

    inflight_set_release(set, h);
    inflight_set_release(set, h);   /* double release must be a no-op */

    inflight_stats_t st;
    inflight_set_stats(set, &st);
    CHECK(st.held == 0, "held drains to 0, got %llu", (unsigned long long)st.held);
    CHECK(st.claimed == 2, "claimed counts 2, got %llu", (unsigned long long)st.claimed);
    CHECK(st.released == 2, "released counts 2, got %llu", (unsigned long long)st.released);
    /* The refused counter is what /stats publishes as the number of duplicate
     * fetches prevented: one per claim that hit a live held slot. */
    CHECK(st.refused == 1, "refused counts the one blocked claim, got %llu",
          (unsigned long long)st.refused);

    inflight_set_destroy(set);
}

/* (c): 512 claims in a 1024-slot table, half released, then every key is
 * probed again. Nothing still-held may come back claimable, and nothing
 * released may stay held. */
static void test_release_keeps_other_claims(void) {
    const size_t keys = 512;
    inflight_set_t *set = inflight_set_create(1024, 0);
    CHECK(set != NULL, "create");

    unsigned char (*hashes)[20] = calloc(keys, 20);
    CHECK(hashes != NULL, "hashes alloc");
    if (!hashes) {
        inflight_set_destroy(set);
        return;
    }

    size_t accepted = 0;
    for (size_t i = 0; i < keys; i++) {
        make_hash(hashes[i], (uint32_t)i);
        if (inflight_set_claim(set, hashes[i], 1)) {
            accepted++;
        }
    }
    CHECK(accepted == keys, "every distinct key claimable, got %zu", accepted);

    for (size_t i = 0; i < keys; i += 2) {
        inflight_set_release(set, hashes[i]);
    }

    size_t wrongly_free = 0;
    size_t wrongly_held = 0;
    for (size_t i = 0; i < keys; i++) {
        bool got = inflight_set_claim(set, hashes[i], 2);
        bool expect_free = (i % 2) == 0;
        if (expect_free && !got) {
            wrongly_held++;
        } else if (!expect_free && got) {
            wrongly_free++;
        }
        if (got) {
            inflight_set_release(set, hashes[i]);
        }
    }
    CHECK(wrongly_free == 0, "%zu still-held key(s) became claimable", wrongly_free);
    CHECK(wrongly_held == 0, "%zu released key(s) stayed held", wrongly_held);

    free(hashes);
    inflight_set_destroy(set);
}

/* (d): destroying one tree frees its claims and nobody else's. */
static void test_release_tree(void) {
    inflight_set_t *set = inflight_set_create(1024, 0);
    CHECK(set != NULL, "create");

    unsigned char a[20], b[20], c[20];
    make_hash(a, 11);
    make_hash(b, 12);
    make_hash(c, 13);

    CHECK(inflight_set_claim(set, a, 7), "tree 7 claims a");
    CHECK(inflight_set_claim(set, b, 7), "tree 7 claims b");
    CHECK(inflight_set_claim(set, c, 8), "tree 8 claims c");

    inflight_set_release_tree(set, 7);

    CHECK(inflight_set_claim(set, a, 8), "a is free after its tree was destroyed");
    CHECK(inflight_set_claim(set, b, 8), "b is free after its tree was destroyed");
    CHECK(!inflight_set_claim(set, c, 7), "c (another tree) stays held");

    inflight_set_destroy(set);
}

/* (e): a full table clears itself; no distinct claim is ever refused. */
static void test_full_table_keeps_accepting(void) {
    inflight_set_t *set = inflight_set_create(16, 0);   /* clears at 12 used */
    CHECK(set != NULL, "create");

    unsigned char h[20];
    size_t accepted = 0;
    for (uint32_t i = 0; i < 64; i++) {
        make_hash(h, 1000 + i);
        if (inflight_set_claim(set, h, 1)) {
            accepted++;
        }
    }
    CHECK(accepted == 64, "every distinct claim accepted across clears, got %zu", accepted);

    inflight_set_destroy(set);
}

/* A claim whose worker died without releasing is taken over only after the TTL,
 * so a live worker is never raced by the backstop. */
static void test_ttl_backstop(void) {
    inflight_set_t *set = inflight_set_create(64, 1);
    CHECK(set != NULL, "create");

    unsigned char h[20];
    make_hash(h, 42);
    CHECK(inflight_set_claim(set, h, 1), "claim");
    CHECK(!inflight_set_claim(set, h, 2), "refused inside the TTL");

    sleep(2);
    CHECK(inflight_set_claim(set, h, 2), "taken over after the TTL");

    inflight_set_destroy(set);
}

/* (f): a NULL set is "dedup disabled", never "suppress everything". */
static void test_null_set_fails_open(void) {
    unsigned char h[20];
    make_hash(h, 1);

    CHECK(inflight_set_claim(NULL, h, 1), "NULL set must not suppress work");
    inflight_set_release(NULL, h);
    inflight_set_release_tree(NULL, 1);
    inflight_stats_t st;
    memset(&st, 0xA5, sizeof(st));
    inflight_set_stats(NULL, &st);
    CHECK(st.capacity == 0 && st.claimed == 0 && st.refused == 0,
          "NULL set zeroes the snapshot");
    inflight_set_stats(NULL, NULL);   /* must not crash */
    inflight_set_destroy(NULL);
}

/* (g): N threads, one key, one winner. */
#define RACE_THREADS 8

static int race_winners;

typedef struct {
    inflight_set_t *set;
    unsigned char hash[20];
    pthread_barrier_t *barrier;
} race_ctx_t;

static void *race_claim_once(void *arg) {
    race_ctx_t *ctx = (race_ctx_t *)arg;
    pthread_barrier_wait(ctx->barrier);
    if (inflight_set_claim(ctx->set, ctx->hash, 1)) {
        __atomic_add_fetch(&race_winners, 1, __ATOMIC_SEQ_CST);
    }
    return NULL;
}

static void test_concurrent_claim(void) {
    inflight_set_t *set = inflight_set_create(1024, 0);
    CHECK(set != NULL, "create");

    pthread_barrier_t barrier;
    CHECK(pthread_barrier_init(&barrier, NULL, RACE_THREADS) == 0, "barrier init");

    race_ctx_t ctx[RACE_THREADS];
    pthread_t threads[RACE_THREADS];
    __atomic_store_n(&race_winners, 0, __ATOMIC_SEQ_CST);

    for (int i = 0; i < RACE_THREADS; i++) {
        ctx[i].set = set;
        ctx[i].barrier = &barrier;
        make_hash(ctx[i].hash, 77);   /* the same key for every thread */
        CHECK(pthread_create(&threads[i], NULL, race_claim_once, &ctx[i]) == 0,
              "spawn thread %d", i);
    }
    for (int i = 0; i < RACE_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    int winners = __atomic_load_n(&race_winners, __ATOMIC_SEQ_CST);
    CHECK(winners == 1, "exactly one of %d racing threads wins, got %d", RACE_THREADS, winners);

    inflight_stats_t st;
    inflight_set_stats(set, &st);
    CHECK(st.claimed == 1, "one claim granted, got %llu", (unsigned long long)st.claimed);
    CHECK(st.refused == RACE_THREADS - 1, "%d refusals, got %llu", RACE_THREADS - 1,
          (unsigned long long)st.refused);

    pthread_barrier_destroy(&barrier);
    inflight_set_destroy(set);
}

int main(void) {
    printf("test_inflight_set\n");

    test_claim_release();
    test_release_keeps_other_claims();
    test_release_tree();
    test_full_table_keeps_accepting();
    test_ttl_backstop();
    test_null_set_fails_open();
    test_concurrent_claim();

    if (failures != 0) {
        printf("%d check(s) FAILED\n", failures);
        return 1;
    }

    printf("all checks passed\n");
    return 0;
}
