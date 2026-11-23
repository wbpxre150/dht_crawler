#include "inflight_set.h"
#include "dht_crawler.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uv.h>

#define INFLIGHT_HASH_LEN 20

/* Clear the whole table once it is this full. A cleared table costs one brief
 * dedup gap (the state the crawler is in today anyway); a full table would cost
 * lost work, so the load factor is kept well below 1. */
#define INFLIGHT_LOAD_NUM 3
#define INFLIGHT_LOAD_DEN 4

/* Slot states. TOMBSTONE is what keeps linear probing correct after a release:
 * an EMPTY slot would truncate the probe chain, so a key inserted later in that
 * chain could be claimed a second time while its first claim is still held. */
#define INFLIGHT_EMPTY 0
#define INFLIGHT_HELD 1
#define INFLIGHT_TOMBSTONE 2

typedef struct {
    unsigned char key[INFLIGHT_HASH_LEN];
    uint64_t stamp_ms;   /* CLOCK_MONOTONIC ms at claim time (TTL backstop) */
    uint32_t tree_id;    /* owner, for inflight_set_release_tree() */
    uint8_t state;
} inflight_slot_t;

struct inflight_set {
    inflight_slot_t *slots;
    size_t capacity;     /* power of two */
    size_t mask;
    size_t held;         /* slots in INFLIGHT_HELD */
    size_t used;         /* slots in INFLIGHT_HELD or INFLIGHT_TOMBSTONE */
    int ttl_sec;
    uv_mutex_t mutex;

    uint64_t claimed_total;
    uint64_t refused_total;
    uint64_t released_total;
    uint64_t expired_total;
    uint64_t dropped_total;
    uint64_t clears_total;
};

static uint64_t inflight_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* FNV-1a over the 20-byte infohash. The keys are already uniformly distributed,
 * so a cheap hash is enough; probing only needs a stable spread. */
static uint64_t inflight_hash(const unsigned char *key) {
    uint64_t h = 1469598103934665603ULL;
    for (int i = 0; i < INFLIGHT_HASH_LEN; i++) {
        h ^= (uint64_t)key[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static size_t inflight_next_pow2(size_t n) {
    size_t p = 1;
    while (p < n) {
        p <<= 1;
    }
    return p;
}

inflight_set_t *inflight_set_create(size_t capacity, int ttl_sec) {
    if (capacity == 0) {
        capacity = 1;
    }

    inflight_set_t *set = calloc(1, sizeof(inflight_set_t));
    if (!set) {
        log_msg(LOG_ERROR, "In-flight set: failed to allocate the set");
        return NULL;
    }

    set->capacity = inflight_next_pow2(capacity);
    set->mask = set->capacity - 1;
    set->ttl_sec = ttl_sec > 0 ? ttl_sec : 0;
    set->slots = calloc(set->capacity, sizeof(inflight_slot_t));
    if (!set->slots) {
        log_msg(LOG_ERROR, "In-flight set: failed to allocate %zu slots", set->capacity);
        free(set);
        return NULL;
    }

    if (uv_mutex_init(&set->mutex) != 0) {
        log_msg(LOG_ERROR, "In-flight set: failed to initialize the mutex");
        free(set->slots);
        free(set);
        return NULL;
    }

    log_msg(LOG_DEBUG, "In-flight set initialized: %zu slots (%.2f MB), ttl=%ds",
            set->capacity,
            (double)(set->capacity * sizeof(inflight_slot_t)) / (1024.0 * 1024.0),
            set->ttl_sec);
    return set;
}

void inflight_set_destroy(inflight_set_t *set) {
    if (!set) {
        return;
    }
    uv_mutex_destroy(&set->mutex);
    free(set->slots);
    free(set);
}

bool inflight_set_claim(inflight_set_t *set, const unsigned char *hash, uint32_t tree_id) {
    if (!set || !hash) {
        return true;   /* fail open: a disabled set never suppresses work */
    }

    uint64_t now = inflight_now_ms();
    uint64_t ttl_ms = set->ttl_sec > 0 ? (uint64_t)set->ttl_sec * 1000ULL : 0;

    uv_mutex_lock(&set->mutex);

    if (set->used * INFLIGHT_LOAD_DEN >= set->capacity * INFLIGHT_LOAD_NUM) {
        memset(set->slots, 0, set->capacity * sizeof(inflight_slot_t));
        set->held = 0;
        set->used = 0;
        set->clears_total++;
        log_msg(LOG_DEBUG, "In-flight set %zu/%zu full: cleared", set->capacity, set->capacity);
    }

    size_t index = (size_t)inflight_hash(hash) & set->mask;
    size_t tombstone = (size_t)-1;
    bool claimed = false;

    /* Progress is guaranteed: the load-factor clear above leaves more than a
     * quarter of the slots EMPTY, so this loop always reaches one. */
    for (size_t probes = 0; probes < set->capacity; probes++) {
        inflight_slot_t *slot = &set->slots[index];

        if (slot->state == INFLIGHT_EMPTY) {
            size_t target = tombstone != (size_t)-1 ? tombstone : index;
            inflight_slot_t *dst = &set->slots[target];
            if (dst->state == INFLIGHT_TOMBSTONE) {
                set->used--;   /* reusing a tombstone: net zero */
            }
            memcpy(dst->key, hash, INFLIGHT_HASH_LEN);
            dst->stamp_ms = now;
            dst->tree_id = tree_id;
            dst->state = INFLIGHT_HELD;
            set->held++;
            set->used++;
            set->claimed_total++;
            claimed = true;
            break;
        }

        if (slot->state == INFLIGHT_TOMBSTONE) {
            if (tombstone == (size_t)-1) {
                tombstone = index;
            }
        } else if (memcmp(slot->key, hash, INFLIGHT_HASH_LEN) == 0) {
            /* Already held. Only a claim whose worker died without releasing it
             * (or a tree that was destroyed mid-flight) is taken over, and only
             * after the TTL, so a live worker is never raced. */
            if (ttl_ms > 0 && now > slot->stamp_ms && now - slot->stamp_ms > ttl_ms) {
                slot->stamp_ms = now;
                slot->tree_id = tree_id;
                set->expired_total++;
                set->claimed_total++;
                claimed = true;
            } else {
                /* The whole point: somebody else is already fetching this hash. */
                set->refused_total++;
            }
            break;
        }

        index = (index + 1) & set->mask;
    }

    uv_mutex_unlock(&set->mutex);
    return claimed;
}

void inflight_set_release(inflight_set_t *set, const unsigned char *hash) {
    if (!set || !hash) {
        return;
    }

    uv_mutex_lock(&set->mutex);

    size_t index = (size_t)inflight_hash(hash) & set->mask;
    for (size_t probes = 0; probes < set->capacity; probes++) {
        inflight_slot_t *slot = &set->slots[index];
        if (slot->state == INFLIGHT_EMPTY) {
            break;
        }
        if (slot->state == INFLIGHT_HELD &&
            memcmp(slot->key, hash, INFLIGHT_HASH_LEN) == 0) {
            memset(slot->key, 0, INFLIGHT_HASH_LEN);
            slot->state = INFLIGHT_TOMBSTONE;
            slot->tree_id = 0;
            slot->stamp_ms = 0;
            if (set->held > 0) {
                set->held--;
            }
            set->released_total++;
            break;
        }
        index = (index + 1) & set->mask;
    }

    uv_mutex_unlock(&set->mutex);
}

void inflight_set_release_tree(inflight_set_t *set, uint32_t tree_id) {
    if (!set) {
        return;
    }

    size_t dropped = 0;

    uv_mutex_lock(&set->mutex);
    for (size_t i = 0; i < set->capacity; i++) {
        inflight_slot_t *slot = &set->slots[i];
        if (slot->state == INFLIGHT_HELD && slot->tree_id == tree_id) {
            memset(slot->key, 0, INFLIGHT_HASH_LEN);
            slot->state = INFLIGHT_TOMBSTONE;
            slot->tree_id = 0;
            slot->stamp_ms = 0;
            if (set->held > 0) {
                set->held--;
            }
            dropped++;
        }
    }
    set->dropped_total += dropped;
    uv_mutex_unlock(&set->mutex);

    if (dropped > 0) {
        log_msg(LOG_DEBUG, "In-flight set: released %zu claim(s) held by tree %u",
                dropped, tree_id);
    }
}

void inflight_set_stats(inflight_set_t *set, inflight_stats_t *out) {
    if (!out) {
        return;
    }

    memset(out, 0, sizeof(*out));
    if (!set) {
        return;
    }

    uv_mutex_lock(&set->mutex);
    out->capacity = (uint64_t)set->capacity;
    out->held = (uint64_t)set->held;
    out->claimed = set->claimed_total;
    out->refused = set->refused_total;
    out->released = set->released_total;
    out->expired = set->expired_total;
    out->tree_released = set->dropped_total;
    out->clears = set->clears_total;
    uv_mutex_unlock(&set->mutex);
}
