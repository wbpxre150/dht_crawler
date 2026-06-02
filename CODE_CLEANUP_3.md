# Stage 3 — Wire `main.c`, `http_api.c`, and `supervisor.c` to thread-tree-only

Goal: fix the unresolved symbols produced by stage 2 by deleting every
old-architecture code path that is still referenced by the keep set.
After this stage, `make` builds clean and the binary runs the
supervisor/tree/refresh path exclusively.

Files in scope:
- `src/main.c` (largest diff in this stage)
- `src/http_api.c` + `include/http_api.h`
- `src/supervisor.c` (drop the wbpxre-based `global_bootstrap` and its
  call site; keep the rest)
- `include/supervisor.h` (drop now-dead bootstrap config fields, drop
  `bep51_cache_capacity`/`bep51_cache_submit_percent` only if the
  supervisor really no longer needs them — see §5.2)
- `include/dht_crawler.h` (drop `MAX_WORKERS`/`INFOHASH_QUEUE_SIZE`/
  `BATCH_SIZE` macros)

Stage 4 trims tree-internal fields. Stage 5 removes the bootstrap
entirely. This stage removes only the obvious dead code in the
keep set; it does NOT touch `shared_node_pool` or the
`bootstrap_thread_func` inside `thread_tree.c`.

---

## 1. `src/main.c` — old-architecture branch deletion

### 1.1 Globals (lines 27-40)

Delete:
```c
static dht_manager_t g_dht_mgr;  /* Single DHT instance (old architecture) */
static infohash_queue_t g_queue;
static metadata_fetcher_t g_fetcher;
```

Keep `app_context_t g_app_ctx`, `database_t g_database`, `http_api_t g_http_api`, `bloom_filter_t *g_bloom`, plus the supervisor / batch_writer / refresh_thread / refresh_query_store globals.

### 1.2 Includes (lines 1-25)

Delete:
```c
#include "dht_manager.h"
#include "infohash_queue.h"
#include "metadata_fetcher.h"
```

Add (already needed by the keep set but worth confirming):
- `tree_socket.h`, `tree_dispatcher.h` are NOT needed in `main.c` (only the supervisor touches them).
- The `#include <libgen.h>` is required by `ensure_parent_directory_exists` (`dirname`) — keep.

### 1.3 `infohash_queue_init` (lines 393-401)

Delete the entire block:
```c
log_msg(LOG_DEBUG, "Initializing infohash queue (capacity: %d)...", INFOHASH_QUEUE_SIZE);
rc = infohash_queue_init(&g_queue, INFOHASH_QUEUE_SIZE);
if (rc != 0) { ... return 1; }
```

The bloom filter init that follows (lines 414-446) and database init
(lines 521-540) currently call `infohash_queue_cleanup(&g_queue)` on
the error paths. Strip those calls from each error path: search for
`infohash_queue_cleanup` in `main.c` and remove every line. The
expected remaining call sites (in `main.c`):
- `bloom_filter_init` failure: line 418
- `database_init` failure: line 528
- `database_create_schema` failure: line 538
- `--porn-filter-update` blocks: lines 558, 565, 577
- `--compact` blocks: lines 596, 612

After removal, the error paths in those `if` blocks are still
correct because the queue is gone.

### 1.4 Maintenance modes (`--porn-filter-update`, `--compact`, etc.)

These are documented in PLAN §3 to be left intact because they only
need `g_database`. Verify after the infohash_queue_cleanup removal:
- `--rebuild-bloom-filter` (lines 264-304): no `g_queue` reference. Keep.
- `--check-database` (lines 309-323): no `g_queue` reference. Keep.
- `--recover-database` (lines 329-380): no `g_queue` reference. Keep.
- `--porn-filter-update` (lines 549-581): no `g_queue` reference except the `infohash_queue_cleanup` lines being removed. Keep.
- `--compact` (lines 583-631): same. Keep.

The `--porn-filter-update` and `--compact` blocks both call
`cleanup_app_context(&g_app_ctx)` before returning — keep, that's
correct for the new keep set.

### 1.5 Delete the entire `if (config.use_thread_trees)` branch wrapper (lines 633-902)

The wrapper itself goes away. The body that runs the supervisor is
the new entire post-deletion flow. Concretely:
- Delete the `if (config.use_thread_trees) {` ... `}` and the comment block above (lines 633-902).
- Promote the inside of the if to top-level code.
- Delete the `OLD ARCHITECTURE` block (lines 904-1062) entirely.

The new flow inside `main()` after the maintenance-mode early
returns is:

```c
/* === Thread Tree architecture (only architecture) === */
g_batch_writer = batch_writer_init(...);
... [all the supervisor/batch_writer/refresh/HTTP setup] ...
supervisor_start(g_supervisor);
... [wait for shutdown] ...
... [shutdown sequence] ...
return 0;
```

### 1.6 `use_thread_trees` removal

- The `use_thread_trees` config field was deleted in stage 1; the
  struct initializer and the `if` check are gone in §1.5.
- Update `config.c` comment blocks that mention
  `use_thread_trees` (stage 1 also removed those — double check).
- The `config.use_thread_trees = 1;` default (deleted in stage 1) and
  the corresponding `config_load_file` branch (deleted in stage 1) are
  already gone.

### 1.7 `dht_port` initializer in `init_app_context` (line 170)

`ctx->dht_port = DHT_PORT;` is the only `DHT_PORT` reference in the
keep set. Keep it. `DHT_PORT` is a `#define` in
`include/dht_crawler.h:15`; PLAN §"Config settings: keep vs remove"
explicitly says keep `DHT_PORT`. The macro is also a documented
constant — no need to remove it.

### 1.8 `signal_handler` (lines 43-51)

Keep. The `uv_stop(g_app_ctx.loop)` call is still correct for the
keep set: the batch writer and refresh thread run timers on this
loop, and civetweb uses it indirectly. (The supervisor uses libuv
via the shared socket — verify the shared socket uses `loop`; if so,
this is load-bearing.)

### 1.9 Argv parsing of `--help` (lines 201-228)

PLAN §3 does not list this. The `--help` text already lists
`--rebuild-bloom-filter`, `--porn-filter-update`, `--compact`,
`--check-database`, `--recover-database`, `--dont-check-database`,
`--help`. All of those are still live. No change.

The CLI options from `config_parse_args` (lines 561-624) lose `-h`
in stage 1. The `--help` argv loop should not be confused: it
matches `--help` and `-h` literally, so the user can still pass
`--help` to print the maintenance-mode help. The `config_parse_args`
change in stage 1 only removed the `-h` short option, not the
`--help` argv recognition here.

### 1.10 `INFOHASH_QUEUE_SIZE` macro (line 394, deleted in §1.3)

`#define INFOHASH_QUEUE_SIZE 1000` in `dht_crawler.h:17` is now
dead. Delete in §6.

---

## 2. `src/http_api.c` + `include/http_api.h` — JSON pruning

### 2.1 Includes

`src/http_api.c:3` and `:7`:
```c
#include "dht_manager.h"
#include "metadata_fetcher.h"
```
Delete both.

`include/http_api.h:6-7`:
```c
#include "dht_manager.h"
#include "metadata_fetcher.h"
```
Delete both.

### 2.2 `http_api_t` struct

`include/http_api.h:24-26`:
```c
dht_manager_t *dht_manager;
struct batch_writer *batch_writer;
metadata_fetcher_t *metadata_fetcher;
```
Drop the `dht_manager` and `metadata_fetcher` fields. Keep
`batch_writer` (used by the keep-set JSON for torrent counts at
`http_api.c:384-388`).

### 2.3 `http_api_init` signature

Current (in `include/http_api.h:47-49` and
`src/http_api.c:78-80`):
```c
int http_api_init(http_api_t *api, app_context_t *app_ctx, database_t *database,
                  dht_manager_t *dht_manager, batch_writer_t *batch_writer,
                  metadata_fetcher_t *metadata_fetcher, int port);
```

New:
```c
int http_api_init(http_api_t *api, app_context_t *app_ctx, database_t *database,
                  batch_writer_t *batch_writer, int port);
```

Body change (`src/http_api.c:78-93`):
- Drop the `dht_manager`, `metadata_fetcher` parameters.
- Drop `api->dht_manager = dht_manager;` and `api->metadata_fetcher = metadata_fetcher;`.
- Keep the `dht_manager can be NULL` comment removal since the field is gone.

### 2.4 Caller in `src/main.c:803-804`

Current:
```c
rc = http_api_init(&g_http_api, &g_app_ctx, &g_database, NULL,
                   g_batch_writer, NULL, HTTP_API_PORT);
```

New:
```c
rc = http_api_init(&g_http_api, &g_app_ctx, &g_database, g_batch_writer, HTTP_API_PORT);
```

### 2.5 JSON output blocks

`src/http_api.c`:
- **Lines 390-394** — drop the entire `if (api->metadata_fetcher)` block reading metadata fetcher stats.
- **Lines 412-418** — drop the entire `if (api->dht_manager)` block emitting the `"dht"` sub-object.
- **Lines 420-460** — drop the entire `if (api->metadata_fetcher)` block emitting the `"metadata_fetcher"` sub-object. The block also computes success_rate, filter_rate, timeout_rate against `metadata_stats`; remove those.
- `metadata_fetcher_stats_t` is now unused in `http_api.c`; remove the variable at line 391.

The porn filter, language filter, supervisor, BEP51 cache, and
per-tree stats blocks (lines 462-625) stay — they are the
supervisor-driven JSON.

### 2.6 `app_context_t` reference

`http_api_init` keeps `app_context_t *app_ctx` for `start_time` and
`log_level` lookups in the JSON. The start_time field stays; no
change.

---

## 3. `src/supervisor.c` — drop wbpxre bootstrap

### 3.1 Includes (lines 1-21)

Drop:
```c
#include "shared_node_pool.h"   /* removed in stage 5 — keep until then */
#include "wbpxre_dht.h"         /* removed in stage 2 */
```
Wait — `shared_node_pool.h` is consumed until stage 5 (the bootstrap
path in `supervisor.c` and `thread_tree.c` still uses
`shared_node_pool_t`, and `refresh_thread.c` keeps a
`shared_node_pool *` field). KEEP `shared_node_pool.h` for now.
Stage 5 removes it.

Drop only `wbpxre_dht.h`. The `getaddrinfo`/`freeaddrinfo` calls in
`resolve_hostname` need `<netdb.h>`, which is already in the
includes. Keep.

### 3.2 Constants (lines 23-30)

Delete:
```c
static const char *BOOTSTRAP_NODES[] = { ... };
static const int BOOTSTRAP_PORT = 6881;
```

The new tree-native bootstrap (stage 5) brings its own list and
runs from `thread_tree.c`.

### 3.3 Forward declarations (lines 35-36)

Delete:
```c
static int global_bootstrap(supervisor_t *sup, int target_nodes, int timeout_sec, int num_workers);
```

The `monitor_thread_func` forward stays.

### 3.4 `global_bootstrap` and `global_bootstrap_worker_func`

Delete in their entirety:
- `typedef struct global_bootstrap_ctx` (lines 561-569)
- `static void *global_bootstrap_worker_func` (lines 571-610) — already a no-op stub (the comment "let's skip this for now" at line 598 confirms it does nothing); even easier to remove
- `static int global_bootstrap(supervisor_t *, int, int, int)` (lines 612-816) — uses `wbpxre_dht_*`, `tribuf_*`, `wbpxre_routing_*`, all going away

### 3.5 `supervisor_start` call site (lines 884-887)

Current:
```c
int rc = global_bootstrap(sup, sup->global_bootstrap_target,
                         sup->global_bootstrap_timeout_sec,
                         sup->global_bootstrap_workers);
if (rc != 0) { log_msg(...); cleanup ...; return; }
```

Delete the call and its error path. Stage 5 will move bootstrap into
each tree's own bootstrap phase; for stages 3 and 4, the supervisor
just creates the shared socket/dispatcher (already done) and
spawns trees (already done).

### 3.6 `tribuf_*` includes/uses

`tribuf_*` is referenced only in `global_bootstrap`. After §3.4
removes that function, all `tribuf_*` references are gone. The
`<netdb.h>` for `getaddrinfo` stays for §3.4's deletion to compile,
even though `resolve_hostname` becomes unused: stage 5 will delete
`resolve_hostname` too. For now (stages 3-4), leave both the
function and `<netdb.h>` in place — they don't break anything.

PLAN §3 says "remove the `tribuf_*` includes/uses". `tribuf_*` is
not an include; the include that brings in tribuf is
`<urcu/wfqueue.h>` or similar via `wbpxre_dht.h`. Dropping
`wbpxre_dht.h` is enough to remove the symbol surface. No separate
tribuf header to remove.

### 3.7 `supervisor_create` — config init lines

Lines 78-82 set `global_bootstrap_target`, `global_bootstrap_timeout_sec`, `global_bootstrap_workers`, `per_tree_sample_size`. The first three are passed to `global_bootstrap` which is gone; `per_tree_sample_size` is read by `thread_tree.c:917` for the bootstrap sample. The sample is read until stage 5, so `per_tree_sample_size` must stay on the supervisor and the struct.

PLAN §3 says "Stage 3 only trims supervisor config, not tree
internals." Concretely: KEEP the four bootstrap-related fields on
`supervisor_t` and in `supervisor_config_t` through stages 3-4.
Stage 5 will remove them when the bootstrap path is gone.

PLAN §3 also says "Drop `BATCH_SIZE`/`INFOHASH_QUEUE_SIZE` from
`dht_crawler.h`". `BATCH_SIZE` is the macro for the `infohash_queue.h` const (used in old `dht_manager.c`); `INFOHASH_QUEUE_SIZE` is the macro for `g_queue`'s init capacity. Both go away when `dht_manager.h` is deleted (stage 2) and `g_queue` is removed (stage 3, §1.3). So `BATCH_SIZE` macro removal is implicit in stage 2's deletions; `INFOHASH_QUEUE_SIZE` removal is part of §6.

### 3.8 `supervisor_start` shared_node_pool + bep51_cache bootstrap

Lines 856-920 contain: `bep51_cache_create`, `bep51_cache_load_from_file`, `bep51_cache_populate_shared_pool`, `shared_node_pool_create`. The `bep51_cache_*` calls are still needed — they populate the BEP51 cache used by the in-tree bootstrap fallback. The `shared_node_pool_create` and `bep51_cache_populate_shared_pool` calls produce the per-tree sample which `thread_tree.c:917` consumes. KEEP them.

This is consistent with PLAN §3's "keep the supervisor→tree backlink".

---

## 4. `include/supervisor.h` — config fields

### 4.1 `supervisor_config_t` bootstrap fields (lines 51-55)

PLAN §3 says "Stage 3 only trims supervisor config, not tree
internals." KEEP `global_bootstrap_target`, `global_bootstrap_timeout_sec`,
`global_bootstrap_workers`, `per_tree_sample_size` in
`supervisor_config_t` and `supervisor_t` through stage 4. Stage 5
removes them.

### 4.2 `num_bootstrap_workers` and the `bootstrap_workers` array (stage 4 concern)

PLAN §3 defers tree-internal fields to stage 4. `include/thread_tree.h:178` and `:187` define `num_bootstrap_workers` and `bootstrap_workers`. They are populated at `supervisor.c:232` (`.num_bootstrap_workers = sup->num_find_node_workers`) and the array is allocated/joined in `thread_tree.c`. Stage 4 will delete them. Keep in stage 3.

### 4.3 `BATCH_SIZE` / `INFOHASH_QUEUE_SIZE` from `dht_crawler.h` (stage 3 concern per PLAN)

Both are unused after §1.3 and stage 2. Delete. See §6.

---

## 5. Verifying no other consumer depends on the deleted fields

### 5.1 `metadata_fetcher_stats_t` in keep set

After §2 deletes the `metadata_fetcher_t *` field and the JSON
blocks, `metadata_fetcher_stats_t` is still defined in
`include/metadata_fetcher.h`. The struct itself is harmless dead
code in the keep set, but it references `peer_connection_t` and
`connection_request_queue_t` from deleted headers. Either:

(a) Delete `include/metadata_fetcher.h` and `src/metadata_fetcher.c` (already done in stage 2), or
(b) Leave the headers deleted and the type definitions gone. The struct cannot be referenced from `http_api.c` after §2.

Stage 2 already deletes `metadata_fetcher.h` and `.c`. §2 is
instructed to delete the `metadata_fetcher_t *` field and the
metadata_fetcher_stats_t JSON usage. With both source files gone, the
type is no longer in the include set. Verified.

### 5.2 `bep51_cache_capacity` and `bep51_cache_submit_percent`

`http_api.c:528-529` reads `api->supervisor->bep51_cache_capacity`
and `bep51_cache_submit_percent` to emit JSON. These are the
supervisor struct fields. KEEP them — they are part of the
supervisor's runtime BEP51 cache settings, not the deleted
`global_bootstrap_*` family. PLAN §"Config settings: keep vs remove"
keeps `bep51_cache_*`. The supervisor keeps
`bep51_cache_capacity` and `bep51_cache_submit_percent` in
`supervisor_t` and `supervisor_config_t`.

### 5.3 `bep51_cache_populate_shared_pool`

This function lives in `bep51_cache.c`. It takes a
`shared_node_pool *` (deleted in stage 5). For stages 3-4 the call
from `supervisor.c:872` is still made; the function compiles because
`shared_node_pool.h` is still present. Stage 5 will remove both.

---

## 6. `include/dht_crawler.h` — dead constants

PLAN §1 names `MAX_WORKERS`, `INFOHASH_QUEUE_SIZE`, `BATCH_SIZE`.
After this stage:
- `INFOHASH_QUEUE_SIZE` — only used by deleted `g_queue` init at
  `main.c:394` (§1.3 removed it). Macro at `dht_crawler.h:17` is now
  dead. **Delete**.
- `BATCH_SIZE` — only used inside `dht_manager.c` (deleted in stage
  2). The `dht_manager.c:108` reference is gone. **Delete** at
  `dht_crawler.h:18`.
- `MAX_WORKERS` — was never referenced in the keep set (verified by
  grep). **Delete** at `dht_crawler.h:16`.

The keep set reads:
- `DHT_PORT` (line 15) — used by `init_app_context` at `main.c:170`. **Keep**.
- `UDP_RECV_BUFFER_SIZE` (line 19) — used by `tree_socket.c` and `tree_dispatcher.c`. **Keep**.
- `SHA1_DIGEST_LENGTH` (line 20) — used by `tree_protocol.c`, `tree_metadata.c`, `keyspace.c`. **Keep**.
- `NODE_ID_LENGTH` (line 21) — used by `app_context_t.node_id`, `tree_node_t.node_id`, `tree_protocol.c`. **Keep**.

`app_context_t` (lines 28-40) keeps `dht_port`, `log_level`, `db_path`,
`start_time`, `node_id`. All still live.

---

## 7. Out of scope (deferred to later stages)

- `tree_config_t` `num_bootstrap_workers`, `bootstrap_timeout_sec`,
  `routing_threshold`, `bootstrap_workers` array — stage 4.
- `shared_node_pool`, `global_bootstrap_*` config fields, BEP51
  cache→shared pool flow — stage 5.
- `config.ini` cleanup — stage 6.
- `metadata_fetcher.c`'s `connection_timeout_ms`,
  `tcp_connect_timeout_ms`, `max_connection_lifetime_ms`,
  `max_metadata_size`, `max_concurrent_peer_queries` — these live in
  `metadata_fetcher.h` which is deleted in stage 2. The `config.c`
  fields they read from (`max_concurrent_connections`,
  `tcp_connect_timeout_sec`, `connection_timeout_sec`,
  `max_connection_lifetime_sec`, `max_metadata_size_mb`) lose all
  readers. Stage 6 must delete these 5 fields from `crawler_config_t`
  and the parser. Document in stage 6 plan.

---

## 8. Acceptance

- `grep -RIn 'dht_manager\|metadata_fetcher\|infohash_queue\|worker_pool\|peer_retry\|peer_store\|connection_request\|discovered_nodes\|find_node_worker\.c\|wbpxre\|tribuf\|urcu' include/ src/` returns no hits (the keep set is fully purged of old-architecture names).
- `grep -n 'MAX_WORKERS\|INFOHASH_QUEUE_SIZE\|BATCH_SIZE' include/dht_crawler.h` returns no hits.
- `make` succeeds.
- `dht_crawler` starts up: the supervisor path runs to the point
  where stage 4 will move bootstrap into the trees, and the binary
  transitions through BOOTSTRAP/BEP51/GET_PEERS/METADATA phases
  (in a network-disabled test, supervisor creates the trees and the
  shared socket; the trees' bootstrap fails because there is no real
  bootstrap in stage 3 — they fall back to BEP51 cache and exit when
  the cache is empty).
- `config.ini` parses cleanly.
- A SIGTERM still produces a clean shutdown.
