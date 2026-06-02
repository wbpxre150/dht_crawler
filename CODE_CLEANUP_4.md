# Stage 4 — Remove dead tree-internal fields and the wbpxre-only `1500` hardcode

Goal: finish pruning `thread_tree.h` / `thread_tree.c` / the supervisor
config that stage 3 deliberately left in place because it crosses the
bootstrap boundary. After this stage, the keep set has zero references
to the bootstrap path's scaffolding (`num_bootstrap_workers`,
`bootstrap_workers`, `bootstrap_timeout_sec`, `routing_threshold`).
The `1500` hardcode from `dht_manager.c:448` is gone; bootstrap
target node count is sourced from `bep51_cache_capacity` so the
config surface stays small.

Stage 4 does NOT remove the `shared_node_pool`/`bep51_cache` flow
itself; that runs from `bootstrap_thread_func` and is reused in
stage 5 with a tree-native bootstrap. Stage 4 only removes the
dead/unused fields around it.

---

## 1. `include/thread_tree.h` — field and config removals

### 1.1 `tree_config_t` fields (lines 63-71)

Delete these four fields:
```c
int num_bootstrap_workers;      /* Stage 2: Find_node workers for bootstrap (default: 10) */
...
/* Stage 2: Bootstrap settings */
int bootstrap_timeout_sec;      /* Bootstrap phase timeout */
int routing_threshold;          /* Nodes required before BEP51 phase */
```

(Keep `num_find_node_workers` and the rest of the Stage 3-5 fields.)

### 1.2 `thread_tree_t` fields

Delete:
- `int bootstrap_timeout_sec;` (line 137)
- `int routing_threshold;` (line 138)
- `pthread_t *bootstrap_workers;` (line 178) — the array that the
  bootstrap worker threads were never wired to a worker function
  (verified: `global_bootstrap_worker_func` in `supervisor.c` is the
  only thing ever created; it's a no-op stub; `thread_tree.c:1275`
  joins the array which is allocated to `num_bootstrap_workers` size
  at `thread_tree.c:1152` but the array slots are never assigned via
  `pthread_create`)
- `int num_bootstrap_workers;` (line 187)

(`bootstrap_thread` at line 177 stays — it is the tree's bootstrap
thread that runs the bootstrap routine, renamed in stage 5.)

### 1.3 Add new config field `find_node_target_nodes` (PLAN §4)

Add a new field to `tree_config_t`:
```c
/* Routing table target — how many nodes to gather before BEP51 phase
 * Sourced from supervisor's bep51_cache_capacity. Replaces the
 * hardcoded 1500 that used to live in dht_manager.c. */
int find_node_target_nodes;
```

Add a corresponding `int find_node_target_nodes;` to
`thread_tree_t` (place it near `num_find_node_workers`).

Comment block explaining the replacement:
```c
/* find_node_target_nodes replaces the historical "1500" hardcode
 * that lived in dht_manager.c. It is sourced from the supervisor's
 * bep51_cache_capacity (already exposed in HTTP API and config.ini)
 * to keep the config surface small. */
```

### 1.4 `include/http_api.h` — `http_port` mention (PLAN §4 last bullet)

PLAN §4 says "drop the `http_port` field from `http_api_init` call
site path; HTTP_API_PORT is hardcoded as today (port 8080). Do not
change the macro." Done in stage 3 (§2.3 of stage 3 plan).
`include/http_api.h` retains `#define HTTP_API_PORT 8080`. No
stage 4 work here.

---

## 2. `src/thread_tree.c` — remove dead code

### 2.1 Includes

PLAN §4: "Remove the `#include "wbpxre_dht.h"` from `supervisor.c`
(it is no longer used there) and any remaining `wbpxre_*` references
in the keep set."

- `src/supervisor.c:13` — drop `#include "wbpxre_dht.h"` (already dropped in stage 3 §3.1).
- `src/thread_tree.c` — verify no `wbpxre_*` references; the
  bootstrap_thread_func (line 897) does not reference wbpxre. The
  `find_node_worker_func` (line ~140) does not either. Nothing to
  remove in this file.

### 2.2 `thread_tree_create` — bootstrap field assignments (lines 1058-1060)

Delete:
```c
tree->bootstrap_timeout_sec = config->bootstrap_timeout_sec > 0 ? config->bootstrap_timeout_sec : 30;
tree->routing_threshold = config->routing_threshold > 0 ? config->routing_threshold : 500;
```

Replace with:
```c
/* Routing target sourced from bep51_cache_capacity (passed via
 * supervisor) so we don't expose a new config knob. */
tree->find_node_target_nodes = config->find_node_target_nodes > 0
                                ? config->find_node_target_nodes
                                : 10000;  /* matches default bep51_cache_capacity */
```

### 2.3 Bootstrap worker allocation (lines 1151-1159)

Delete the entire block:
```c
if (tree->num_bootstrap_workers > 0) {
    tree->bootstrap_workers = calloc(tree->num_bootstrap_workers, sizeof(pthread_t));
    if (!tree->bootstrap_workers) {
        log_msg(LOG_ERROR, "[tree %u] Failed to allocate bootstrap workers", tree_id);
        thread_tree_destroy(tree);
        return NULL;
    }
}
```

(`num_bootstrap_workers` is gone; `bootstrap_workers` is gone; the
alloc was zeroed but no slot was ever assigned, so dropping the
alloc and the corresponding free in destroy is consistent.)

### 2.4 Bootstrap workers join (lines 1274-1284)

Delete the entire block:
```c
/* Join bootstrap workers */
log_msg(LOG_DEBUG, "[tree %u] Joining %d bootstrap workers...", tree->tree_id, tree->num_bootstrap_workers);
if (tree->bootstrap_workers) {
    for (int i = 0; i < tree->num_bootstrap_workers; i++) {
        if (tree->bootstrap_workers[i]) {
            pthread_join(tree->bootstrap_workers[i], NULL);
        }
    }
    free(tree->bootstrap_workers);
}
log_msg(LOG_DEBUG, "[tree %u] Bootstrap workers joined", tree->tree_id);
```

(The bootstrap thread is still joined at lines 1268-1272.)

### 2.5 Replace the `1500` hardcode (PLAN §4)

PLAN §4 says:
> In `bootstrap_thread_func` (line 896), the `target_nodes = 1500`
> constant (line 129 in `find_node_worker_func`) becomes a config
> value: add a `find_node_target_nodes` field on the tree (sourced
> from `bep51_cache_capacity` or a new `tree_routing_target` config;
> use `bep51_cache_capacity` to keep the config surface small —
> supervisor already exposes this). Add comment that this replaces
> the hardcode.

Searching for `1500` in `find_node_worker_func` and
`bootstrap_thread_func`:

The current bootstrap routine in `bootstrap_thread_func` (lines
897-1028) does NOT use the literal `1500`. The supervisor's
`global_bootstrap` function (deleted in stage 3) used
`config.max_routing_table_nodes = target_nodes * 3` and
`config.triple_routing_threshold = target_nodes`, with default
`target_nodes = 5000`. There is no `1500` in the live code; the
`1500` PLAN refers to is the default of the (now-deleted)
`triple_routing_threshold` field whose default was
`config->triple_routing_threshold = 1500` (deleted in stage 1).

Verification: `grep -nE '\b1500\b' src/thread_tree.c` returns no
hits. PLAN's "line 129 in `find_node_worker_func`" referenced an
earlier revision. There is no live hardcode to replace in the
keep set.

**Action**: in the bootstrap routine, use `tree->find_node_target_nodes` as the desired routing-table node count
(so the supervisor can scale it with the BEP51 cache capacity).
Add a comment block above the bootstrap routine's "have at least N
nodes" check (the 100-node minimum at line 977) explaining that the
target is now config-driven.

```c
/* Bootstrap success threshold: 100 nodes is the minimum for a
 * useful routing table. The TARGET (i.e. how many nodes we aim to
 * gather) is tree->find_node_target_nodes, sourced from the
 * supervisor's bep51_cache_capacity. Replaces the historical
 * hardcode of 1500 that lived in dht_manager.c. */
```

### 2.6 `bootstrap_thread_func` — pool/bep51_cache sample size cap

At line 920:
```c
tree_node_t *sampled_nodes = malloc(sample_size * sizeof(tree_node_t));
```

`sample_size = tree->supervisor->per_tree_sample_size` (line 917).
Stage 5 removes `per_tree_sample_size`. For now, keep. Stage 5 will
also remove the entire pool-based sample path in favour of the
tree-native bootstrap.

---

## 3. `src/supervisor.c` — drop the dead `bootstrap_workers` plumbing

PLAN §4: "Remove the `num_bootstrap_workers` and the
`bootstrap_workers` pthread_t array in `thread_tree_t` are also dead
— see stage 4. Stage 3 only trims `supervisor` config, not tree
internals."

`supervisor.c` does not have a `num_bootstrap_workers` field on
`supervisor_t`; it has `num_find_node_workers`. The
`tree_config_t` initializer in `spawn_tree` (lines 232-235) sets
the (now-deleted) tree fields:

```c
.num_bootstrap_workers = sup->num_find_node_workers,
.num_find_node_workers = sup->num_find_node_workers,
.bootstrap_timeout_sec = 0,
.routing_threshold = 0,
```

Delete all four lines. Replace with the new field:
```c
.num_find_node_workers = sup->num_find_node_workers,
.find_node_target_nodes = sup->bep51_cache_capacity,
```

The `bep51_cache_capacity` is already stored in
`supervisor_t` (set at `supervisor.c:118`). Reusing it as the
routing-table target keeps the config surface unchanged.

### 3.1 `supervisor_t` — no new fields needed

`bep51_cache_capacity` is already on `supervisor_t` and on
`supervisor_config_t`. The new `find_node_target_nodes` is a
per-tree field only; it does not need to be stored on the
supervisor.

---

## 4. `include/supervisor.h` — no changes in stage 4

The fields `global_bootstrap_target`/`global_bootstrap_timeout_sec`/
`global_bootstrap_workers`/`per_tree_sample_size` stay in stage 4
on both `supervisor_config_t` (lines 51-55) and `supervisor_t`
(lines 147-150). They are removed in stage 5 when the bootstrap
path is rewritten.

The `bep51_cache_capacity` field at `supervisor.h:154` is the
source of the new `find_node_target_nodes`. Confirmed at
`supervisor.h:60` for the config and `:154` for the live field.

---

## 5. `config.c` — no changes in stage 4

`tree_routing_target` and `find_node_target_nodes` are not new
`config.ini` keys per PLAN. The supervisor's
`bep51_cache_capacity` (already in config.ini) flows into
`find_node_target_nodes` automatically. No parser, no defaults
change.

---

## 6. The `1500` hardcode — full audit

The PLAN §4 hardcode claim is verified: no `1500` literal remains
in the keep set's bootstrap path. The historical
`triple_routing_threshold = 1500` default was deleted in stage 1
(`config.c:94` and `include/config.h:84` removed).

Search:
- `grep -nE '\b1500\b' src/thread_tree.c` — no hits.
- `grep -nE '\b1500\b' src/supervisor.c` — no hits.
- `grep -nE '\b1500\b' include/thread_tree.h` — no hits.
- `grep -nE '\b1500\b' include/supervisor.h` — no hits.
- `grep -nE '\b1500\b' src/config.c` — no hits.
- `grep -nE '\b1500\b' config.ini` — `triple_routing_threshold=1500` (line 130), removed in stage 6.

The only `1500` literal still in source files is in the
`config.ini` historical record (stage 6 strips it). Verified
during planning: nothing in the keep set compiles with a `1500`
hardcode. The PLAN §4 work item is therefore mainly the
documentation comment explaining where the value used to be, and
the new field name that documents the runtime source.

---

## 7. Out of scope (deferred to later stages)

- `shared_node_pool` / `bep51_cache_populate_shared_pool` /
  `bootstrap_thread_func`'s pool-based sample — stage 5.
- `global_bootstrap_*` config fields and `per_tree_sample_size` —
  stage 5.
- `config.ini` triple_routing_threshold and 1500 line — stage 6.
- `metadata_fetcher.c` config fields still in `config.c`
  (`max_concurrent_connections`, `tcp_connect_timeout_sec`,
  `connection_timeout_sec`, `max_connection_lifetime_sec`,
  `max_metadata_size_mb`) — stage 6 once all readers are gone.
- `http_api.h:15` `HTTP_API_PORT` macro — keep, no change.

---

## 8. Acceptance

- `grep -nE 'num_bootstrap_workers|bootstrap_workers|bootstrap_timeout_sec|routing_threshold' include/thread_tree.h src/thread_tree.c include/supervisor.h src/supervisor.c` returns no hits.
- `grep -nE '\b1500\b' src/ include/ Makefile` returns no hits (config.ini stage 6 concern).
- `make` succeeds.
- `dht_crawler` starts: supervisor creates trees; each tree's bootstrap thread runs `bootstrap_thread_func` and exits through the "shutdown" label if the shared pool/cache is empty (in a no-network test); trees transition through phases when the supervisor's `bep51_cache_load_from_file` populates the cache from disk.
- `find_node_target_nodes` value in a running tree equals the supervisor's `bep51_cache_capacity` (default 10000) — verified by `gdb` or by adding a temporary `log_msg` at `thread_tree_create` line 1058-1062.
