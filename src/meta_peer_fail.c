#include "meta_peer_fail.h"
#include "dht_crawler.h"

#include <arpa/inet.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Clear the whole table once it is this full. A clear costs one window of
 * legacy selection (the behaviour the crawler ships with anyway); an
 * over-full table would cost probe chains that never terminate cheaply. For a
 * *failure* map a clear is the safe failure mode, so no LRU is worth the
 * bookkeeping at this size. */
#define META_PEER_FAIL_LOAD_NUM 3
#define META_PEER_FAIL_LOAD_DEN 4

/* Slot states. There is no TOMBSTONE: an entry is never released, only aged
 * out, and an expired slot is reused in place (it is logically absent). */
#define META_PEER_FAIL_EMPTY 0
#define META_PEER_FAIL_USED 1

/* Key layout: see include/meta_peer_fail.h. family (1) + port, network order
 * (2) + address (16). */
#define META_PEER_FAIL_FAMILY_OFF 0
#define META_PEER_FAIL_PORT_OFF 1
#define META_PEER_FAIL_ADDR_OFF 3
#define META_PEER_FAIL_ADDR_LEN 16

typedef struct {
    uint8_t key[META_PEER_FAIL_KEY_LEN];  /* normalized address key */
    uint8_t fail_count;                   /* failures seen, saturated (observability) */
    uint8_t state;                        /* META_PEER_FAIL_EMPTY / _USED */
    uint64_t last_fail_ms;                /* monotonic ms of the last failure (TTL) */
} meta_peer_fail_slot_t;

/* 8192 slots x 32 B = 256 KiB: +0.013 % of the measured 1,995 MB crawler RSS.
 * The static assertion is the budget's guard rail - a field added to the slot
 * must be paid for here, not silently. */
typedef char meta_peer_fail_slot_size_check[(sizeof(meta_peer_fail_slot_t) == 32) ? 1 : -1];

struct meta_peer_fail_set {
    meta_peer_fail_slot_t *slots;
    size_t capacity;      /* power of two */
    size_t mask;
    size_t used;          /* slots in META_PEER_FAIL_USED (expired ones included) */
    int ttl_sec;
    pthread_mutex_t mutex;

    uint64_t addresses_total;   /* NEW address entries admitted */
    uint64_t bumped_total;      /* repeat failures on a known address */
    uint64_t skipped_total;     /* window slots skipped */
    uint64_t expired_total;     /* lookups that found an expired entry */
    uint64_t clears_total;      /* load-factor clears */
};

/* FNV-1a over the normalized key. Keys are already uniformly distributed
 * (addresses + ports), so a cheap hash only has to spread them. */
static uint64_t meta_peer_fail_hash(const uint8_t *key) {
    uint64_t h = 1469598103934665603ULL;
    for (int i = 0; i < META_PEER_FAIL_KEY_LEN; i++) {
        h ^= (uint64_t)key[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static size_t meta_peer_fail_next_pow2(size_t n) {
    size_t p = 1;
    while (p < n) {
        p <<= 1;
    }
    return p;
}

int meta_peer_fail_key(const struct sockaddr_storage *addr,
                       uint8_t out[META_PEER_FAIL_KEY_LEN]) {
    if (!addr || !out) {
        return -1;
    }

    /* Build the key in a zeroed local so no stale byte ever reaches a memcmp
     * probe (the IPv4 form leaves addr[4..15] zeroed by construction). */
    uint8_t key[META_PEER_FAIL_KEY_LEN];
    memset(key, 0, sizeof(key));

    if (addr->ss_family == AF_INET) {
        const struct sockaddr_in *sin = (const struct sockaddr_in *)addr;
        uint32_t ip = ntohl(sin->sin_addr.s_addr);
        uint16_t port = ntohs(sin->sin_port);

        if (ip == 0 || ip == 0xFFFFFFFFu || port == 0) {
            return -1;
        }

        key[META_PEER_FAIL_FAMILY_OFF] = AF_INET;
        key[META_PEER_FAIL_PORT_OFF] = (uint8_t)(port >> 8);
        key[META_PEER_FAIL_PORT_OFF + 1] = (uint8_t)(port & 0xFF);
        memcpy(key + META_PEER_FAIL_ADDR_OFF, &sin->sin_addr.s_addr, 4);
        memcpy(out, key, sizeof(key));
        return 0;
    }

    if (addr->ss_family == AF_INET6) {
        const struct sockaddr_in6 *sin6 = (const struct sockaddr_in6 *)addr;
        uint16_t port = ntohs(sin6->sin6_port);
        const uint8_t *s6 = sin6->sin6_addr.s6_addr;

        if (port == 0) {
            return -1;
        }

        /* Reject the all-zero IPv6 address (::). */
        int all_zero = 1;
        for (int i = 0; i < META_PEER_FAIL_ADDR_LEN; i++) {
            if (s6[i] != 0) {
                all_zero = 0;
                break;
            }
        }
        if (all_zero) {
            return -1;
        }

        /* IPv4-mapped IPv6 folds to native IPv4 so both spellings of the same
         * peer share one entry (the same fold bep51_cache applies on insert). */
        if (IN6_IS_ADDR_V4MAPPED(&sin6->sin6_addr)) {
            uint32_t mapped = 0;
            memcpy(&mapped, s6 + 12, 4);
            uint32_t ip = ntohl(mapped);
            if (ip == 0 || ip == 0xFFFFFFFFu) {
                return -1;
            }

            key[META_PEER_FAIL_FAMILY_OFF] = AF_INET;
            key[META_PEER_FAIL_PORT_OFF] = (uint8_t)(port >> 8);
            key[META_PEER_FAIL_PORT_OFF + 1] = (uint8_t)(port & 0xFF);
            memcpy(key + META_PEER_FAIL_ADDR_OFF, s6 + 12, 4);
            memcpy(out, key, sizeof(key));
            return 0;
        }

        key[META_PEER_FAIL_FAMILY_OFF] = AF_INET6;
        key[META_PEER_FAIL_PORT_OFF] = (uint8_t)(port >> 8);
        key[META_PEER_FAIL_PORT_OFF + 1] = (uint8_t)(port & 0xFF);
        memcpy(key + META_PEER_FAIL_ADDR_OFF, s6, META_PEER_FAIL_ADDR_LEN);
        memcpy(out, key, sizeof(key));
        return 0;
    }

    return -1;
}

meta_peer_fail_set_t *meta_peer_fail_create(size_t capacity, int ttl_sec) {
    if (ttl_sec <= 0) {
        /* Disabled: the caller keeps a NULL handle and selection stays legacy.
         * No allocation, so a 0 TTL can never change behaviour. */
        return NULL;
    }
    if (capacity == 0) {
        capacity = 1;
    }

    meta_peer_fail_set_t *set = calloc(1, sizeof(meta_peer_fail_set_t));
    if (!set) {
        log_msg(LOG_ERROR, "Metadata peer-fail map: failed to allocate the map");
        return NULL;
    }

    set->capacity = meta_peer_fail_next_pow2(capacity);
    set->mask = set->capacity - 1;
    set->ttl_sec = ttl_sec;
    set->slots = calloc(set->capacity, sizeof(meta_peer_fail_slot_t));
    if (!set->slots) {
        log_msg(LOG_ERROR, "Metadata peer-fail map: failed to allocate %zu slots",
                set->capacity);
        free(set);
        return NULL;
    }

    if (pthread_mutex_init(&set->mutex, NULL) != 0) {
        log_msg(LOG_ERROR, "Metadata peer-fail map: failed to initialize the mutex");
        free(set->slots);
        free(set);
        return NULL;
    }

    log_msg(LOG_INFO, "Metadata peer-fail map initialized: %zu slots (%.0f KiB), ttl=%ds",
            set->capacity,
            (double)(set->capacity * sizeof(meta_peer_fail_slot_t)) / 1024.0,
            set->ttl_sec);
    return set;
}

void meta_peer_fail_destroy(meta_peer_fail_set_t *set) {
    if (!set) {
        return;
    }
    pthread_mutex_destroy(&set->mutex);
    free(set->slots);
    free(set);
}

/* Is [slot]'s entry past the TTL at [now_ms]? An entry is expired strictly
 * after ttl_sec, so a just-recorded failure is never instantly stale. */
static bool meta_peer_fail_slot_expired(const meta_peer_fail_set_t *set,
                                        const meta_peer_fail_slot_t *slot,
                                        uint64_t now_ms) {
    uint64_t ttl_ms = (uint64_t)set->ttl_sec * 1000ULL;
    return now_ms > slot->last_fail_ms && now_ms - slot->last_fail_ms > ttl_ms;
}

bool meta_peer_fail_should_skip(meta_peer_fail_set_t *set,
                                const struct sockaddr_storage *addr,
                                uint64_t now_ms) {
    if (!set || !addr) {
        return false;   /* fail open: a disabled map never suppresses a peer */
    }

    uint8_t key[META_PEER_FAIL_KEY_LEN];
    if (meta_peer_fail_key(addr, key) != 0) {
        return false;
    }

    bool skip = false;
    bool saw_expired = false;

    pthread_mutex_lock(&set->mutex);

    size_t index = (size_t)meta_peer_fail_hash(key) & set->mask;
    for (size_t probes = 0; probes < set->capacity; probes++) {
        meta_peer_fail_slot_t *slot = &set->slots[index];

        if (slot->state == META_PEER_FAIL_EMPTY) {
            break;
        }

        if (memcmp(slot->key, key, META_PEER_FAIL_KEY_LEN) == 0) {
            if (meta_peer_fail_slot_expired(set, slot, now_ms)) {
                /* Aged out: logically absent. Keep probing - the entry may have
                 * been re-admitted further along its own probe chain. */
                saw_expired = true;
            } else {
                skip = true;
                break;
            }
        }

        index = (index + 1) & set->mask;
    }

    if (saw_expired) {
        set->expired_total++;
    }

    pthread_mutex_unlock(&set->mutex);
    return skip;
}

int meta_peer_fail_record(meta_peer_fail_set_t *set,
                          const struct sockaddr_storage *addr,
                          uint64_t now_ms) {
    if (!set || !addr) {
        return 0;
    }

    uint8_t key[META_PEER_FAIL_KEY_LEN];
    if (meta_peer_fail_key(addr, key) != 0) {
        /* Unusable address (port 0, all-zero, unsupported family): it can never
         * be a real peer, so remembering it would only be noise. */
        return 0;
    }

    int admitted = 0;

    pthread_mutex_lock(&set->mutex);

    if (set->used * META_PEER_FAIL_LOAD_DEN >= set->capacity * META_PEER_FAIL_LOAD_NUM) {
        memset(set->slots, 0, set->capacity * sizeof(meta_peer_fail_slot_t));
        set->used = 0;
        set->clears_total++;
        log_msg(LOG_DEBUG, "Metadata peer-fail map %zu/%zu full: cleared",
                set->capacity, set->capacity);
    }

    size_t index = (size_t)meta_peer_fail_hash(key) & set->mask;
    size_t reusable = (size_t)-1;   /* first expired slot on this probe path */
    int found = 0;

    /* Progress is guaranteed: the load-factor clear above leaves more than a
     * quarter of the slots EMPTY, so this loop always reaches one. */
    for (size_t probes = 0; probes < set->capacity; probes++) {
        meta_peer_fail_slot_t *slot = &set->slots[index];

        if (slot->state == META_PEER_FAIL_EMPTY) {
            break;
        }

        if (memcmp(slot->key, key, META_PEER_FAIL_KEY_LEN) == 0) {
            found = 1;
            if (meta_peer_fail_slot_expired(set, slot, now_ms)) {
                /* Aged out, then failed again: a fresh entry in the same slot,
                 * so it is a new admission (the TTL had forgotten it). */
                slot->fail_count = 1;
                slot->last_fail_ms = now_ms;
                set->addresses_total++;
                admitted = 1;
            } else {
                if (slot->fail_count < 255) {
                    slot->fail_count++;
                }
                slot->last_fail_ms = now_ms;
                set->bumped_total++;
            }
            break;
        }

        if (reusable == (size_t)-1 && meta_peer_fail_slot_expired(set, slot, now_ms)) {
            reusable = index;
        }

        index = (index + 1) & set->mask;
    }

    if (!found) {
        /* Reusing an expired slot on the probe path keeps the table from
         * filling up with dead weight and is probe-correct: the replacement
         * lands between its own hash index and the first EMPTY slot. */
        size_t target = reusable != (size_t)-1 ? reusable : index;
        meta_peer_fail_slot_t *dst = &set->slots[target];

        if (dst->state == META_PEER_FAIL_EMPTY) {
            set->used++;
        }
        memcpy(dst->key, key, META_PEER_FAIL_KEY_LEN);
        dst->fail_count = 1;
        dst->last_fail_ms = now_ms;
        dst->state = META_PEER_FAIL_USED;
        set->addresses_total++;
        admitted = 1;
    }

    pthread_mutex_unlock(&set->mutex);
    return admitted;
}

void meta_peer_fail_note_skip(meta_peer_fail_set_t *set) {
    if (!set) {
        return;
    }
    pthread_mutex_lock(&set->mutex);
    set->skipped_total++;
    pthread_mutex_unlock(&set->mutex);
}

uint64_t meta_peer_fail_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

void meta_peer_fail_stats(meta_peer_fail_set_t *set, uint64_t now_ms,
                          meta_peer_fail_stats_t *out) {
    if (!out) {
        return;
    }

    memset(out, 0, sizeof(*out));
    if (!set) {
        return;
    }

    /* held is the map-consultation evidence the Stage-3 verdict reads, so it is
     * counted live rather than derived from used (which keeps expired slots). */
    pthread_mutex_lock(&set->mutex);
    out->capacity = (uint64_t)set->capacity;
    for (size_t i = 0; i < set->capacity; i++) {
        const meta_peer_fail_slot_t *slot = &set->slots[i];
        if (slot->state == META_PEER_FAIL_USED &&
            !meta_peer_fail_slot_expired(set, slot, now_ms)) {
            out->held++;
        }
    }
    out->addresses = set->addresses_total;
    out->bumped = set->bumped_total;
    out->skipped = set->skipped_total;
    out->expired = set->expired_total;
    out->clears = set->clears_total;
    out->ttl_sec = set->ttl_sec;
    pthread_mutex_unlock(&set->mutex);
}
