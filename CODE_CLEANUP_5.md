# Stage 5 — Tree-native bootstrap (replaces the wbpxre shared-pool bootstrap)

Goal: replace the global, wbpxre-based bootstrap in `supervisor.c`
with a tree-native bootstrap. Each tree performs its own URL
bootstrap using a private `tree_socket` and a small `find_node`
loop, then transitions to BEP51. After this stage, the supervisor
has no `shared_node_pool`, no `global_bootstrap_*` config fields,
and no `bep51_cache_populate_shared_pool` flow.

Files in scope:
- `include/shared_node_pool.h` and `src/shared_node_pool.c` — delete
- `src/bep51_cache.c` — remove the `bep51_cache_populate_shared_pool`
  function (and the `#include "shared_node_pool.h"` /
  `struct shared_node_pool` forward declaration in `bep51_cache.h`)
- `include/bep51_cache.h` — drop `bep51_cache_populate_shared_pool`
  declaration
- `include/supervisor.h` — drop `shared_node_pool_t *`,
  `global_bootstrap_*`, `per_tree_sample_size` fields
- `include/refresh_thread.h` — drop `shared_node_pool_t` field,
  drop the `shared_pool` parameter on `refresh_thread_create`
- `src/refresh_thread.c` — replace the `bootstrap_thread_func` with
  a tree-native bootstrap
- `src/supervisor.c` — drop the `global_bootstrap` call site
  (already done in stage 3), drop `shared_node_pool_create`,
  `bep51_cache_populate_shared_pool`, `bep51_cache_load_from_file`-
  into-pool flow; keep `bep51_cache` for in-tree bootstrap
  fallback
- `include/thread_tree.h` + `src/thread_tree.c` — replace
  `bootstrap_thread_func` with the new tree-native bootstrap;
  remove `tree->supervisor->shared_node_pool` and
  `tree->supervisor->per_tree_sample_size` references
- `src/main.c` — drop the `g_supervisor->shared_node_pool`
  argument to `refresh_thread_create`
- `src/config.c` — drop the four `global_bootstrap_*` and
  `per_tree_sample_size` config keys (and the `triple_routing_*`
  defaults which were already removed in stage 1)

The `tree_bootstrap.h` helper is small and not worth a separate
translation unit (PLAN §5). Inline it in `thread_tree.c`.

---

## 1. New tree-native bootstrap (in `src/thread_tree.c`)

### 1.1 Bootstrap hostnames constant

Add to `src/thread_tree.c`, near `generate_random_node_id`:

```c
/* Built-in DHT bootstrap routers. Same list previously used in
 * dht_manager.c. Hostnames are resolved via getaddrinfo() and
 * pinged once each. */
static const char *TREE_BOOTSTRAP_HOSTS[] = {
    "router.bittorrent.com",
    "dht.transmissionbt.com",
    "dht.libtorrent.org",
    "dht.aelitis.com",
    "router.bitcomet.com",
    "dht.anacrolix.link",
    NULL
};
static const int TREE_BOOTSTRAP_PORTS[] = { 6881, 6881, 25401, 6881, 6881, 42069 };
```

Verify the count matches the `NULL` terminator; we will assert
with a `static_assert` or runtime check.

### 1.2 DNS resolver

Already present in `supervisor.c:527` (`resolve_hostname`).
After §3 deletes it, port it into `thread_tree.c` as a static
function `tree_resolve_hostname` (with the same signature
`static int tree_resolve_hostname(const char *hostname, int port,
struct sockaddr_storage *addr)`). Keep the `<netdb.h>` include
in `thread_tree.c` (already present at line 22).

### 1.3 `bootstrap_thread_func` rewrite

Replace the entire current `bootstrap_thread_func` (lines 896-1028
in `src/thread_tree.c`) with a tree-native version. Sketch:

```c
static void *bootstrap_thread_func(void *arg) {
    thread_tree_t *tree = (thread_tree_t *)arg;
    tree_routing_table_t *rt = (tree_routing_table_t *)tree->routing_table;
    tree_socket_t *sock = (tree_socket_t *)tree->socket;
    tree_dispatcher_t *dispatcher = tree->dispatcher;

    log_msg(LOG_DEBUG, "[tree %u] Tree-native bootstrap starting (timeout=%ds)",
            tree->tree_id, tree->tree_bootstrap_timeout_sec);

    time_t deadline = time(NULL) + tree->tree_bootstrap_timeout_sec;
    int have_minimum = 0;

    /* Phase A: query every URL-bootstrap host once with a random
     * find_node target. Each response populates the routing table. */
    for (int i = 0; TREE_BOOTSTRAP_HOSTS[i] != NULL; i++) {
        struct sockaddr_storage addr;
        if (tree_resolve_hostname(TREE_BOOTSTRAP_HOSTS[i],
                                  TREE_BOOTSTRAP_PORTS[i], &addr) != 0) {
            log_msg(LOG_WARN, "[tree %u] DNS resolution failed for %s",
                    tree->tree_id, TREE_BOOTSTRAP_HOSTS[i]);
            continue;
        }

        uint8_t target[20];
        generate_random_target(target);
        uint8_t tid[4];
        int tid_len = tree_protocol_gen_tid(tid);
        tree_response_queue_t *q = tree->bootstrap_response_queue;
        tree_dispatcher_register_tid(dispatcher, tid, tid_len, q);

        int rc = tree_send_find_node(tree, sock, tid, tid_len, target, &addr);
        if (rc == 0) {
            tree_response_t response_pkt;
            if (tree_response_queue_pop(q, &response_pkt, 1000) == 0) {
                /* Parse compact node info from response. Existing
                 * parser is tree_parse_find_node_response or
                 * equivalent; reuse the live one in tree_protocol.c. */
                ...
                /* Add discovered nodes to routing table */
            }
        }
        tree_dispatcher_unregister_tid(dispatcher, tid, tid_len);

        if (tree_routing_get_count(rt) >= 100) {
            have_minimum = 1;
            break;
        }
        if (time(NULL) >= deadline) {
            break;
        }
    }

    /* Phase B: if we still have fewer than 100 nodes, ask the
     * BEP51 cache for a few warm starts (the persistent bootstrap). */
    if (!have_minimum && tree->supervisor && tree->supervisor->bep51_cache) {
        bep51_cache_t *cache = tree->supervisor->bep51_cache;
        if (bep51_cache_get_count(cache) >= 100) {
            tree_node_t *warm = malloc(tree->find_node_target_nodes
                                       * sizeof(tree_node_t));
            int got = bep51_cache_get_random(cache, warm,
                                             tree->find_node_target_nodes);
            for (int i = 0; i < got; i++) {
                tree_routing_add_node(rt, warm[i].node_id, &warm[i].addr);
            }
            free(warm);
            if (tree_routing_get_count(rt) >= 100) {
                have_minimum = 1;
            }
        }
    }

    if (!have_minimum) {
        log_msg(LOG_ERROR, "[tree %u] Bootstrap failed: routing table has %d nodes after %ds",
                tree->tree_id, tree_routing_get_count(rt),
                tree->tree_bootstrap_timeout_sec);
        goto shutdown;
    }

    log_msg(LOG_INFO, "[tree %u] Bootstrap complete: %d nodes in routing table",
            tree->tree_id, tree_routing_get_count(rt));

    /* Start find_node workers, throttle monitor, BEP51 phase. Unchanged. */
    tree_start_find_node_workers(tree);
    tree_start_throttle_monitor(tree);
    atomic_store(&tree->current_phase, TREE_PHASE_BEP51);
    tree_start_bep51_workers(tree);

    while (!atomic_load(&tree->shutdown_requested)) {
        struct timespec ts = {1, 0};
        nanosleep(&ts, NULL);
    }

shutdown:
    if (atomic_load(&tree->shutdown_reason) == SHUTDOWN_REASON_RATE_BASED) {
        atomic_store(&tree->needs_respawn, true);
    }
    return NULL;
}
```

The exact API names (`tree_parse_find_node_response`,
`tree_send_find_node`) and response-queue field location need to
be verified during implementation; the skeleton above is the
intent. The bootstrap response queue is a new per-tree private
field; see §1.4.

### 1.4 New per-tree field: `bootstrap_response_queue`

Add to `thread_tree_t` (after the existing socket/dispatcher
fields):

```c
tree_response_queue_t *bootstrap_response_queue;
```

Allocation in `thread_tree_create` (near where the routing table
is created, around line 1198):

```c
tree->bootstrap_response_queue = tree_response_queue_create(8);
if (!tree->bootstrap_response_queue) {
    log_msg(LOG_ERROR, "[tree %u] Failed to create bootstrap response queue", tree_id);
    thread_tree_destroy(tree);
    return NULL;
}
```

Destruction in `thread_tree_destroy` (after socket close, before
routing table destroy):

```c
if (tree->bootstrap_response_queue) {
    tree_response_queue_destroy(tree->bootstrap_response_queue);
}
```

### 1.5 New per-tree field: `tree_bootstrap_timeout_sec`

Add to `tree_config_t`:

```c
int tree_bootstrap_timeout_sec;   /* Tree-native bootstrap deadline (default: 30) */
```

Add to `thread_tree_t`:

```c
int tree_bootstrap_timeout_sec;
```

`thread_tree_create` populates from `config->tree_bootstrap_timeout_sec`
(default 30 if non-positive).

### 1.6 Remove bootstrap dependencies on the supervisor pool/sample

Drop from `thread_tree.c`:
- `tree->supervisor->shared_node_pool` references
- `tree->supervisor->bep51_cache` is kept but used only in
  Phase B fallback (above)
- `tree->supervisor->per_tree_sample_size` reference
- `bep51_cache_t *bep51_cache` is no longer pulled from the
  supervisor in §1.3's Phase A
- `shared_node_pool_get_count`, `shared_node_pool_get_random`
  references
- `bep51_cache_get_count(bep51_cache)` is kept (used in Phase B)
- `bep51_cache_get_random(bep51_cache, ...)` is kept
- The `bep51_cache->capacity` reference in the primary-source
  branch (line 932) goes away — the new bootstrap doesn't
  pre-check the cache as primary
- `tree->num_bootstrap_workers` already gone (stage 4)
- `tree->bootstrap_workers` already gone (stage 4)

### 1.7 Add new `tree_bootstrap.h` (PLAN §5, optional)

PLAN §5 says "Add the new bootstrap to a new helper file
`src/tree_bootstrap.c` (or inline in `thread_tree.c` — keep it
inline to avoid a new translation unit)." Inline per PLAN.

If the bootstrap grows beyond ~150 lines during implementation,
promote to `src/tree_bootstrap.c` with a corresponding
`include/tree_bootstrap.h`. Update the `Makefile` `OBJS` list
(which already does `wildcard $(SRC_DIR)/*.c`, so no edit needed).

---

## 2. `src/bep51_cache.c` — drop the pool-population function

### 2.1 Delete `bep51_cache_populate_shared_pool`

In `src/bep51_cache.c:404-440` (the function added in the
`bep51_cache.c:413` `size_t bep51_cache_get_count(bep51_cache_t
*cache)` area). Delete the entire function definition.

The function takes a `struct shared_node_pool *` and a `max_nodes`
int. After this stage the only caller (`supervisor.c:872` in
stage 4 code) is being removed in §3, so the function is dead.

### 2.2 Drop the include

`src/bep51_cache.c` includes `shared_node_pool.h`? Check the
existing file. If it does, drop. (Likely not — `bep51_cache.c`
uses `struct shared_node_pool` only as a forward-declared
opaque type in the function signature.)

### 2.3 Drop the include in `include/bep51_cache.h`

`#include "uthash.h"` stays. `struct shared_node_pool;` forward
declaration at line 11 is the only reference; delete.

---

## 3. `src/supervisor.c` — drop shared_node_pool plumbing

### 3.1 Delete the `global_bootstrap` call site (already done in stage 3)

Already removed in stage 3 §3.5. Verify.

### 3.2 Drop the `shared_node_pool_create` and `bep51_cache_populate_shared_pool` calls

Current `supervisor.c:855-879`:
```c
sup->shared_node_pool = shared_node_pool_create(sup->global_bootstrap_target);
if (!sup->shared_node_pool) { ... }
int cache_loaded = bep51_cache_load_from_file(sup->bep51_cache, sup->bep51_cache_path);
if (cache_loaded == 0) {
    int populated = bep51_cache_populate_shared_pool(...);
}
size_t nodes_from_cache = shared_node_pool_get_count(sup->shared_node_pool);
log_msg(...);
if (nodes_from_cache < 1000) { ... global_bootstrap ... }
size_t nodes_collected = shared_node_pool_get_count(sup->shared_node_pool);
log_msg(...);
```

Replace with:
```c
/* Load BEP51 cache from disk. The cache is now read directly
 * by each tree's bootstrap routine as a warm-start fallback. */
log_msg(LOG_DEBUG, "[supervisor] Loading BEP51 cache from %s",
        sup->bep51_cache_path);
if (bep51_cache_load_from_file(sup->bep51_cache, sup->bep51_cache_path) == 0) {
    log_msg(LOG_INFO, "[supervisor] BEP51 cache loaded: %zu nodes",
            bep51_cache_get_count(sup->bep51_cache));
} else {
    log_msg(LOG_WARN, "[supervisor] No BEP51 cache loaded (will rely on URL bootstrap)");
}
```

### 3.3 Drop the `shared_node_pool_destroy` call sites in error paths

The four `shared_node_pool_destroy(sup->shared_node_pool)` calls
at lines 908, 919, 931, 1105 are now dangling: there is no
`shared_node_pool` field. Delete each.

### 3.4 Drop the supervisor→tree backlink comment for shared_node_pool

The `tree->supervisor` field stays because:
- The tree's bootstrap Phase B reads `tree->supervisor->bep51_cache` (§1.3).
- The tree stores `bep51_cache_submit_percent` for the BEP51 node caching stats counter (kept in stage 3).

The `shared_node_pool` field on `supervisor_t` is removed.

### 3.5 `resolve_hostname` in supervisor

`supervisor.c:527-544` becomes unused after the
`global_bootstrap` deletion. It is a small helper with one
caller, but the caller (the deleted `global_bootstrap`) is gone.
Delete it and the `<netdb.h>` include.

### 3.6 `bep51_cache_save_to_file` still called

`supervisor.c:1087-1093` saves the BEP51 cache to disk on
shutdown. KEEP. The cache is still live (used as a tree
warm-start in §1.3 Phase B and as the BEP51 node submission
sink in `thread_tree.c:556`).

### 3.7 `supervisor.h` field removals

Drop:
- `struct shared_node_pool *shared_node_pool;` (line 129)
- `int global_bootstrap_target;` (line 147)
- `int global_bootstrap_timeout_sec;` (line 148)
- `int global_bootstrap_workers;` (line 149)
- `int per_tree_sample_size;` (line 150)

Drop the same fields from `supervisor_config_t` (lines 51-55).

Drop the `struct shared_node_pool;` forward declaration (line 12).

The supervisor's `bep51_cache_path`, `bep51_cache_capacity`,
`bep51_cache_submit_percent` stay.

---

## 4. `include/refresh_thread.h` + `src/refresh_thread.c`

### 4.1 Drop `shared_node_pool_t *shared_node_pool` field

`refresh_thread.h:43` — delete.
`refresh_thread.h:11` — delete forward decl.
`refresh_thread.c:94` — delete `thread->shared_node_pool = shared_pool;`.
`refresh_thread.c:328, 343` — drop the two `shared_node_pool_get_count` and `shared_node_pool_get_random` calls.
`refresh_thread.c:11` — drop `#include "shared_node_pool.h"`.
`refresh_thread.c:81` — drop the `!shared_pool` check from the validation block.
`refresh_thread.c:79` — drop the `shared_pool` parameter from `refresh_thread_create`.

### 4.2 Rewrite `bootstrap_thread_func`

Current at `src/refresh_thread.c:319-406` waits for the shared
pool, samples 1000 nodes from it, populates the routing table,
and starts the find_node/ping workers.

Replace with the same tree-native pattern from §1.3, but with
these tweaks:
- Uses `refresh_send_find_node` instead of `tree_send_find_node` (the refresh thread has its own protocol).
- Uses `refresh_dispatcher_register_tid` / `refresh_dispatcher_unregister_tid` instead of `tree_dispatcher_*`.
- Uses `refresh_parse_find_node_response` to extract the nodes.
- The BEP51 cache is not available (refresh thread is a sibling
  of the supervisor; the supervisor can pass the BEP51 cache
  reference if it has one).

Concretely: add a new `bep51_cache_t *bep51_cache;` field to
`refresh_thread_t`, pass it as a new `bep51_cache` parameter to
`refresh_thread_create` (the supervisor has it at
`sup->bep51_cache`), and use it in Phase B.

The call from `main.c:776-778` becomes:
```c
g_refresh_thread = refresh_thread_create(&refresh_config,
                                         g_refresh_query_store,
                                         g_supervisor->bep51_cache);
```

`refresh_thread_create` signature change in
`include/refresh_thread.h:65-67`:
```c
refresh_thread_t *refresh_thread_create(const refresh_thread_config_t *config,
                                       refresh_query_store_t *query_store,
                                       bep51_cache_t *bep51_cache);
```

If the bootstrap timeout is needed, add a
`bootstrap_timeout_sec` to `refresh_thread_config_t` (default 30)
sourced from a new `refresh_bootstrap_timeout_sec` config field
or a constant. PLAN §5 only renames `global_bootstrap_timeout_sec`
to `tree_bootstrap_timeout_sec` (for trees). The refresh thread's
bootstrap timeout can stay as a hardcoded default (30s) or be
added as a new key in stage 6. The simplest: hardcode 30s in
`refresh_thread_config_t` defaults; the config struct has no
field for it.

### 4.3 `refresh_thread.c` start sequence

Currently after bootstrap:
```c
for (i = 0; i < thread->config.find_node_worker_count; i++) {
    /* start find_node worker */
}
for (i = 0; i < thread->config.ping_worker_count; i++) {
    /* start ping worker */
}
atomic_store(&thread->initialized, true);
```

Keep. The refresh thread's worker fan-out is unchanged.

### 4.4 Remove the `if (got < 100) ... return NULL` failure

The tree-native bootstrap logs a failure and `goto shutdown`.
The refresh thread's bootstrap should log and call
`atomic_store(&thread->initialized, true)` even if it failed —
otherwise the HTTP API's `/refresh` handler will block forever
on a query that cannot complete. The refresh thread's
`get_peers_worker_func` (line 646) checks `!atomic_load(&thread->initialized)` and marks the query complete with an error. With a failed bootstrap, the get_peers worker
correctly bounces every request. Document this in a comment.

---

## 5. `include/shared_node_pool.h` + `src/shared_node_pool.c` — delete

PLAN §5: "`shared_node_pool.h` and `shared_node_pool.c` go away."

Delete both files entirely.

---

## 6. `src/main.c` — update `refresh_thread_create` call site

`main.c:776-778`:
```c
g_refresh_thread = refresh_thread_create(&refresh_config,
                                         g_supervisor->shared_node_pool,
                                         g_refresh_query_store);
```

After this stage, `g_supervisor->shared_node_pool` is gone. The
new call (per §4.2):
```c
g_refresh_thread = refresh_thread_create(&refresh_config,
                                         g_refresh_query_store,
                                         g_supervisor->bep51_cache);
```

---

## 7. `src/config.c` — drop `global_bootstrap_*` and `per_tree_sample_size`

Remove the four config keys and the per-tree-sample key from
`config_init_defaults` and `config_load_file`:

| Key | Default line | Parser line |
|---|---|---|
| `global_bootstrap_target` | 108 | 380-383 |
| `global_bootstrap_timeout_sec` | 109 | 384-387 |
| `global_bootstrap_workers` | 110 | 388-391 |
| `per_tree_sample_size` | 111 | 392-395 |

`global_bootstrap_timeout_sec` is renamed to
`tree_bootstrap_timeout_sec` per PLAN §5. So the deletion is
followed by an addition:

- Add `tree_bootstrap_timeout_sec` to `crawler_config_t` (default
  30, parser line `else if (strcmp(key, "tree_bootstrap_timeout_sec") == 0)`,
  clamp `[10, 600]`).
- Add the new field to the `supervisor_config_t` initializer
  in `main.c` (line 714 area, as `.tree_bootstrap_timeout_sec = config.tree_bootstrap_timeout_sec,`).
- Add the field to `supervisor_t` and `supervisor_config_t` in
  `include/supervisor.h`.

`global_bootstrap_target`, `global_bootstrap_workers`,
`per_tree_sample_size` are removed with no replacement.

PLAN §6 says stage 6 strips the config.ini keys. But removing
`config.c` defaults and parser here is necessary for the build
to pass and to keep the live code path consistent. Stage 6
re-confirms the config.ini file is updated to match.

---

## 8. `include/supervisor.h` — bootstrap field removals + new field

Delete (already listed in §3.7):
- `struct shared_node_pool *shared_node_pool;`
- `int global_bootstrap_target;`
- `int global_bootstrap_timeout_sec;`
- `int global_bootstrap_workers;`
- `int per_tree_sample_size;`
- `struct shared_node_pool;` forward declaration

Add:
- `int tree_bootstrap_timeout_sec;` to both `supervisor_config_t`
  and `supervisor_t`.

Set in `supervisor.c:supervisor_create`:
```c
sup->tree_bootstrap_timeout_sec = config->tree_bootstrap_timeout_sec > 0
                                   ? config->tree_bootstrap_timeout_sec : 30;
```

Forwarded to each tree in `spawn_tree`:
```c
.tree_bootstrap_timeout_sec = sup->tree_bootstrap_timeout_sec,
```

---

## 9. `src/thread_tree.c` — remove supervisor pool/sample references

In addition to §1.6 (the new bootstrap), remove:
- The line `if (!tree->supervisor || !tree->supervisor->shared_node_pool) { ... }` (line 910).
- The `int sample_size = tree->supervisor->per_tree_sample_size;` (line 917).
- The `shared_node_pool_t *pool = ...` (line 915).
- All `shared_node_pool_get_count`/`shared_node_pool_get_random` calls.
- The `bep51_cache_t *bep51_cache = tree->supervisor->bep51_cache;` line moves to the new §1.3 Phase B and is only fetched when actually entering Phase B.
- The `tree_node_t *sampled_nodes = malloc(sample_size * sizeof(tree_node_t));` allocation (line 920).
- The `free(sampled_nodes);` call (line 982).
- The "Sample random nodes from the shared pool" log line (line 919) and the various "Sampled %d nodes from shared pool" / "Sampled %d nodes from BEP51 cache" debug log lines are replaced by new ones.

The `bep51_cache_t *bep51_cache` reference at line 916 is moved into a conditional block in §1.3.

---

## 10. `src/thread_tree.c` — keep BEP51 cache submission

The keep-set reference at `thread_tree.c:550-557` (sample-and-add
BEP51 responses to the supervisor's BEP51 cache) stays. It is
not bootstrap-related; it is the BEP51 node submission logic.

The keep-set reference at `thread_tree.c:916` (`bep51_cache_t *bep51_cache = tree->supervisor->bep51_cache;`) is gone in §1.6.

---

## 11. `Makefile` and other build artifacts

`Makefile` line 16: `SRCS = $(wildcard $(SRC_DIR)/*.c)`. Deleting
`src/shared_node_pool.c` removes the file from the glob; no
Makefile edit needed.

---

## 12. Out of scope (deferred to later stages)

- `config.ini` keys (`global_bootstrap_*`, `per_tree_sample_size`,
  `triple_routing_threshold`, etc.) — stage 6.
- `CONFIG_INI.md` rewrite — stage 6.
- Dead-code grep — stage 6.

---

## 13. Acceptance

- `grep -RIn 'shared_node_pool\|per_tree_sample_size\|global_bootstrap_target\|global_bootstrap_workers\|global_bootstrap_timeout_sec\|bep51_cache_populate_shared_pool' include/ src/` returns no hits.
- `make` succeeds.
- The supervisor starts; each tree's `bootstrap_thread_func` enters the URL bootstrap phase.
- In a network-enabled run, each tree's bootstrap phase exits within `tree_bootstrap_timeout_sec` (default 30s) with at least 100 routing-table nodes, then transitions to BEP51 within seconds.
- In a network-disabled test, the URL bootstrap times out and each tree falls back to Phase B; if the BEP51 cache is non-empty (from a prior run), the tree still completes bootstrap; if the cache is empty, the tree logs the failure and exits the bootstrap loop without blocking the supervisor.
- `HTTP /stats` reports active_trees=0 (no trees get past bootstrap without a cache or network) or N (with network or warm cache).
- `HTTP /refresh?hash=<known>` completes within `refresh_get_peers_timeout_ms` and returns 200; the refresh thread's bootstrap succeeds via URL or warm cache.
- Clean shutdown via SIGTERM.
