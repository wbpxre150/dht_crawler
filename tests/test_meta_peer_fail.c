/**
 * Host unit tests for the per-address TCP connect-failure memory
 * (include/meta_peer_fail.h, patch 21).
 *
 * The map is a *selection* hint in front of the metadata fetch, so the
 * invariants that matter are:
 *
 *   (a) fail-open is absolute: a NULL handle answers "do not skip" to every
 *       lookup, records nothing and reports zeroed stats, and a ttl of 0 never
 *       creates a map at all - a disabled map must leave peer selection
 *       byte-identical to legacy,
 *   (b) a recorded failure makes the same address skippable, and the record is
 *       idempotent per address (fail_count bumps, the address is counted once),
 *   (c) the memory ages out after ttl_sec of *injected* time and a lookup that
 *       finds an expired entry counts one expired_total,
 *   (d) the load-factor clear is fail-open: past 3/4 load the table is wiped
 *       (clears_total 1) and previously-known addresses are skipped again no
 *       more,
 *   (e) the key folds IPv4-mapped IPv6 onto native IPv4, so both spellings of
 *       one peer share a single entry, and it refuses addresses that can never
 *       be a peer (port 0, all-zero, broadcast),
 *   (f) metadata_peers_skipped_total counts SLOTS (peers are not deduplicated),
 *       not distinct addresses.
 *
 * Standalone: no test framework, no supervisor, no database, no libuv. The
 * module takes its clock as a parameter, so every timing case is deterministic.
 *
 *     make tests-meta-peer-fail
 */
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include "meta_peer_fail.h"

static int failures = 0;

#define CHECK(cond, ...) do {                                       \
        if (!(cond)) {                                              \
            printf("FAIL: ");                                       \
            printf(__VA_ARGS__);                                    \
            printf(" (%s:%d)\n", __FILE__, __LINE__);               \
            failures++;                                             \
        }                                                           \
    } while (0)

#define TTL_SEC 600
#define TTL_MS ((uint64_t)TTL_SEC * 1000ULL)

/* Build an IPv4 peer address. */
static struct sockaddr_storage v4_peer(uint32_t host_order_ip, uint16_t port) {
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    struct sockaddr_in *sin = (struct sockaddr_in *)&ss;
    sin->sin_family = AF_INET;
    sin->sin_addr.s_addr = htonl(host_order_ip);
    sin->sin_port = htons(port);
    return ss;
}

/* Build the IPv4-mapped IPv6 spelling of the same peer. */
static struct sockaddr_storage v4_mapped_peer(uint32_t host_order_ip, uint16_t port) {
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&ss;
    sin6->sin6_family = AF_INET6;
    sin6->sin6_port = htons(port);
    uint32_t net = htonl(host_order_ip);
    sin6->sin6_addr.s6_addr[10] = 0xFF;
    sin6->sin6_addr.s6_addr[11] = 0xFF;
    memcpy(sin6->sin6_addr.s6_addr + 12, &net, 4);
    return ss;
}

/* Build a native IPv6 peer address. */
static struct sockaddr_storage v6_peer(uint16_t port) {
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&ss;
    sin6->sin6_family = AF_INET6;
    sin6->sin6_port = htons(port);
    sin6->sin6_addr.s6_addr[0] = 0x20;
    sin6->sin6_addr.s6_addr[1] = 0x01;
    sin6->sin6_addr.s6_addr[2] = 0x0d;
    sin6->sin6_addr.s6_addr[3] = 0xb8;
    sin6->sin6_addr.s6_addr[15] = 0x01;
    return ss;
}

/* (a) Fail-open: NULL map, and ttl 0 never creates one. */
static void test_fail_open(void) {
    struct sockaddr_storage peer = v4_peer(0x0A000001u, 6881);

    CHECK(meta_peer_fail_create(META_PEER_FAIL_CAPACITY, 0) == NULL,
          "ttl 0 must not create a map (fail-open default)");
    CHECK(meta_peer_fail_create(META_PEER_FAIL_CAPACITY, -5) == NULL,
          "a negative ttl must not create a map");

    CHECK(!meta_peer_fail_should_skip(NULL, &peer, 1000),
          "a NULL map never skips");
    CHECK(meta_peer_fail_record(NULL, &peer, 1000) == 0,
          "a NULL map records nothing");

    meta_peer_fail_stats_t stats;
    memset(&stats, 0xAB, sizeof(stats));
    meta_peer_fail_stats(NULL, 1000, &stats);
    CHECK(stats.capacity == 0 && stats.held == 0 && stats.addresses == 0 &&
          stats.skipped == 0 && stats.expired == 0 && stats.clears == 0 &&
          stats.ttl_sec == 0,
          "NULL map reports zeroed stats (ttl 0 = disabled)");

    meta_peer_fail_note_skip(NULL);   /* must not crash */
    meta_peer_fail_stats(NULL, 1000, NULL); /* must not crash */
    CHECK(!meta_peer_fail_should_skip(NULL, NULL, 1000),
          "a NULL address never skips");
}

/* (b) A recorded failure makes the address skippable; the record is idempotent. */
static void test_record_and_skip(void) {
    meta_peer_fail_set_t *set = meta_peer_fail_create(64, TTL_SEC);
    CHECK(set != NULL, "map created with ttl %d", TTL_SEC);
    if (!set) return;

    struct sockaddr_storage peer = v4_peer(0x0A000001u, 6881);
    struct sockaddr_storage other = v4_peer(0x0A000002u, 6881);

    CHECK(!meta_peer_fail_should_skip(set, &peer, 1000),
          "an unknown address is not skipped");

    CHECK(meta_peer_fail_record(set, &peer, 1000) == 1,
          "the first failure admits a new address entry");
    CHECK(meta_peer_fail_should_skip(set, &peer, 1001),
          "a remembered address is skipped");
    CHECK(!meta_peer_fail_should_skip(set, &other, 1001),
          "a different address is not skipped");

    CHECK(meta_peer_fail_record(set, &peer, 2000) == 0,
          "a repeat failure is a bump, not a new address");
    CHECK(meta_peer_fail_should_skip(set, &peer, 2001),
          "the bumped entry is still remembered");

    meta_peer_fail_stats_t stats;
    meta_peer_fail_stats(set, 2001, &stats);
    CHECK(stats.addresses == 1, "one distinct address, got %llu",
          (unsigned long long)stats.addresses);
    CHECK(stats.bumped == 1, "one bump, got %llu",
          (unsigned long long)stats.bumped);
    CHECK(stats.held == 1, "one live entry, got %llu",
          (unsigned long long)stats.held);
    CHECK(stats.capacity == 64, "capacity 64, got %llu",
          (unsigned long long)stats.capacity);
    CHECK(stats.ttl_sec == TTL_SEC, "ttl echoed, got %d", stats.ttl_sec);

    meta_peer_fail_destroy(set);
}

/* (c) The TTL ages an entry out; an expired lookup is counted. */
static void test_ttl_expiry(void) {
    meta_peer_fail_set_t *set = meta_peer_fail_create(64, TTL_SEC);
    if (!set) { CHECK(false, "map created"); return; }

    struct sockaddr_storage peer = v4_peer(0x0A000001u, 6881);
    meta_peer_fail_record(set, &peer, 1000);

    CHECK(meta_peer_fail_should_skip(set, &peer, 1000 + TTL_MS),
          "an entry exactly at its TTL is still remembered");
    CHECK(!meta_peer_fail_should_skip(set, &peer, 1000 + TTL_MS + 1),
          "an entry past its TTL is forgotten");

    meta_peer_fail_stats_t stats;
    meta_peer_fail_stats(set, 1000 + TTL_MS + 1, &stats);
    CHECK(stats.expired == 1, "one expired lookup, got %llu",
          (unsigned long long)stats.expired);
    CHECK(stats.held == 0, "the aged entry is no longer held, got %llu",
          (unsigned long long)stats.held);

    /* Failing again after the TTL is a new admission: the map had forgotten it. */
    CHECK(meta_peer_fail_record(set, &peer, 1000 + TTL_MS + 2) == 1,
          "a failure after the TTL re-admits the address");
    CHECK(meta_peer_fail_should_skip(set, &peer, 1000 + TTL_MS + 3),
          "the re-admitted address is skipped again");

    meta_peer_fail_destroy(set);
}

/* (d) The load-factor clear is fail-open: the table is wiped, not truncated. */
static void test_load_factor_clear(void) {
    meta_peer_fail_set_t *set = meta_peer_fail_create(META_PEER_FAIL_CAPACITY, TTL_SEC);
    if (!set) { CHECK(false, "map created"); return; }

    /* 3/4 of the shipped capacity: the clear fires on the insert after that.
     * Derived symbolically, so this case scales with META_PEER_FAIL_CAPACITY
     * (98,304 inserts at the shipped 131,072 slots). */
    const uint32_t load_limit = (uint32_t)(META_PEER_FAIL_CAPACITY * 3 / 4);
    struct sockaddr_storage first = v4_peer(0x0A000001u, 6881);

    for (uint32_t i = 0; i < load_limit; i++) {
        struct sockaddr_storage peer = v4_peer(0x0A000000u + i + 1, 6881);
        meta_peer_fail_record(set, &peer, 1000);
    }

    meta_peer_fail_stats_t stats;
    meta_peer_fail_stats(set, 1001, &stats);
    CHECK(stats.clears == 0, "no clear at exactly 3/4 load, got %llu",
          (unsigned long long)stats.clears);
    CHECK(meta_peer_fail_should_skip(set, &first, 1001),
          "the first address is remembered before the clear");

    /* One more distinct address trips the clear, then lands in the empty table. */
    struct sockaddr_storage overflow = v4_peer(0x0B000001u, 6881);
    CHECK(meta_peer_fail_record(set, &overflow, 1002) == 1,
          "the overflow address is admitted after the clear");

    meta_peer_fail_stats(set, 1003, &stats);
    CHECK(stats.clears == 1, "one clear, got %llu",
          (unsigned long long)stats.clears);
    CHECK(!meta_peer_fail_should_skip(set, &first, 1003),
          "a cleared table skips nothing (fail-open = legacy selection)");
    CHECK(meta_peer_fail_should_skip(set, &overflow, 1003),
          "the address admitted after the clear is remembered");

    meta_peer_fail_destroy(set);
}

/* (e) Key normalization: mapped-v6 folds onto v4, and unusable addresses are refused. */
static void test_key_normalization(void) {
    struct sockaddr_storage v4 = v4_peer(0x0A000001u, 6881);
    struct sockaddr_storage mapped = v4_mapped_peer(0x0A000001u, 6881);
    struct sockaddr_storage v6 = v6_peer(6881);

    uint8_t v4_key[META_PEER_FAIL_KEY_LEN];
    uint8_t mapped_key[META_PEER_FAIL_KEY_LEN];
    uint8_t v6_key[META_PEER_FAIL_KEY_LEN];

    CHECK(meta_peer_fail_key(&v4, v4_key) == 0, "a v4 peer has a key");
    CHECK(meta_peer_fail_key(&mapped, mapped_key) == 0, "a mapped peer has a key");
    CHECK(meta_peer_fail_key(&v6, v6_key) == 0, "a native v6 peer has a key");
    CHECK(memcmp(v4_key, mapped_key, META_PEER_FAIL_KEY_LEN) == 0,
          "IPv4-mapped IPv6 and native IPv4 share one key");
    CHECK(memcmp(v4_key, v6_key, META_PEER_FAIL_KEY_LEN) != 0,
          "a native v6 key is distinct from a v4 key");

    /* The fold is what makes one entry cover both spellings. */
    meta_peer_fail_set_t *set = meta_peer_fail_create(64, TTL_SEC);
    if (!set) { CHECK(false, "map created"); return; }

    meta_peer_fail_record(set, &v4, 1000);
    CHECK(meta_peer_fail_should_skip(set, &mapped, 1001),
          "the mapped spelling of a remembered v4 peer is skipped");

    /* A different port is a different peer. */
    struct sockaddr_storage v4_other_port = v4_peer(0x0A000001u, 6882);
    CHECK(!meta_peer_fail_should_skip(set, &v4_other_port, 1001),
          "the same address on another port is a different key");

    meta_peer_fail_stats_t stats;
    meta_peer_fail_stats(set, 1001, &stats);
    CHECK(stats.addresses == 1, "the mapped spelling is not a second address, got %llu",
          (unsigned long long)stats.addresses);

    /* Refusals: port 0, all-zero, broadcast, unsupported family. */
    uint8_t scratch[META_PEER_FAIL_KEY_LEN];
    struct sockaddr_storage port0 = v4_peer(0x0A000001u, 0);
    struct sockaddr_storage zero = v4_peer(0x00000000u, 6881);
    struct sockaddr_storage broadcast = v4_peer(0xFFFFFFFFu, 6881);
    struct sockaddr_storage v6_port0 = v6_peer(0);

    CHECK(meta_peer_fail_key(&port0, scratch) == -1, "port 0 is refused");
    CHECK(meta_peer_fail_key(&zero, scratch) == -1, "0.0.0.0 is refused");
    CHECK(meta_peer_fail_key(&broadcast, scratch) == -1, "the broadcast address is refused");
    CHECK(meta_peer_fail_key(&v6_port0, scratch) == -1, "an IPv6 port 0 is refused");
    CHECK(meta_peer_fail_key(NULL, scratch) == -1, "a NULL address is refused");

    CHECK(meta_peer_fail_record(set, &port0, 1000) == 0,
          "a refused address is not recorded");
    CHECK(!meta_peer_fail_should_skip(set, &port0, 1000),
          "a refused address is never skipped");

    meta_peer_fail_destroy(set);
}

/* (f) The skip counter counts SLOTS, not distinct addresses. */
static void test_skip_counter_counts_slots(void) {
    meta_peer_fail_set_t *set = meta_peer_fail_create(64, TTL_SEC);
    if (!set) { CHECK(false, "map created"); return; }

    struct sockaddr_storage peer = v4_peer(0x0A000001u, 6881);
    meta_peer_fail_record(set, &peer, 1000);

    /* Peers are not deduplicated upstream, so one attempt can skip the same
     * address several times: three skipped slots, one distinct address. */
    for (int i = 0; i < 3; i++) {
        CHECK(meta_peer_fail_should_skip(set, &peer, 1001),
              "skip decision %d", i);
        meta_peer_fail_note_skip(set);
    }

    meta_peer_fail_stats_t stats;
    meta_peer_fail_stats(set, 1001, &stats);
    CHECK(stats.skipped == 3, "three skipped slots, got %llu",
          (unsigned long long)stats.skipped);
    CHECK(stats.addresses == 1, "still one distinct address, got %llu",
          (unsigned long long)stats.addresses);

    meta_peer_fail_destroy(set);
}

int main(void) {
    test_fail_open();
    test_record_and_skip();
    test_ttl_expiry();
    test_load_factor_clear();
    test_key_normalization();
    test_skip_counter_counts_slots();

    if (failures == 0) {
        printf("test_meta_peer_fail: all checks passed\n");
        return 0;
    }
    printf("test_meta_peer_fail: %d check(s) FAILED\n", failures);
    return 1;
}
