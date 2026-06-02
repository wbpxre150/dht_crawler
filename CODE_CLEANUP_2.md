# Stage 2 — Delete old architecture files + Makefile trim

Goal: delete every file that supports the
`dht_manager`/`metadata_fetcher`/`infohash_queue`/`worker_pool`/
`peer_retry_tracker`/`peer_store`/`connection_request_queue`/
`discovered_nodes`/`find_node_worker`/`shared_node_pool` path and the
`lib/wbpxre-dht/` library, then trim `Makefile` to drop the
`wbpxre_*.o` objects, `-lurcu`, and the `lib/wbpxre-dht` include
path. `make` is EXPECTED TO FAIL at the end of this stage because the
keep set still references deleted symbols; stage 3 wires main.c and
http_api.c to thread-tree-only and brings the build back.

---

## 1. Files to delete (entire files, both header and source)

| File | Reason |
|---|---|
| `include/dht_manager.h` | Old-architecture single-DHT path |
| `src/dht_manager.c` | Same — uses `<urcu.h>`, `wbpxre_dht.h` |
| `include/metadata_fetcher.h` | Old-architecture metadata worker pool |
| `src/metadata_fetcher.c` | Same — uses `infohash_queue.h`, `connection_request_queue.h`, `peer_store.h` |
| `include/infohash_queue.h` | Old-architecture queue |
| `src/infohash_queue.c` | Same — only consumed by `dht_manager.c` + `metadata_fetcher.c` |
| `include/worker_pool.h` | Old-architecture worker pool |
| `src/worker_pool.c` | Same — only included by `dht_manager.c` |
| `include/peer_retry_tracker.h` | Old-architecture retry tracker (never wired in) |
| `src/peer_retry_tracker.c` | Same — only `dht_manager.c` includes it |
| `include/peer_store.h` | Old-architecture peer store (never wired in) |
| `src/peer_store.c` | Same — only `metadata_fetcher.c` includes it |
| `include/connection_request_queue.h` | Old-architecture conn queue |
| `src/connection_request_queue.c` | Same — only `metadata_fetcher.c` includes it |
| `include/discovered_nodes.h` | Already disabled (`.c.disabled`) |
| `src/discovered_nodes.c.disabled` | Fully remove |
| `include/find_node_worker.h` | Already disabled (`.c.disabled`) |
| `src/find_node_worker.c.disabled` | Fully remove |
| `include/shared_node_pool.h` | Will be deleted in stage 5, NOT here. (Stage 5 only — the bootstrap path still uses it from `supervisor.c` and `thread_tree.c` until then.) |
| `src/shared_node_pool.c` | Same |
| `lib/wbpxre-dht/` (entire directory: `wbpxre_dht.c`, `wbpxre_dht.h`, `wbpxre_routing.c`, `wbpxre_protocol.c`, `wbpxre_worker.c`, `README.md`) | Old-architecture DHT library — only used by `dht_manager.c` and the bootstrap path in `supervisor.c` |

NOTE: `shared_node_pool.h` and `shared_node_pool.c` are listed under
"Delete" in PLAN §"Files: keep vs delete" but they are USED by the
live `supervisor.c` bootstrap path AND by `refresh_thread.c`. They
survive stages 2-4 and are deleted in stage 5 along with the bootstrap
path that consumes them. Listing them here would break the keep set.

`patches/bencode-init-fix.patch` and `patches/bencode-cap-fix.patch`:
the patched `lib/bencode-c/bencode.c` is consumed by `tree_protocol.c`
and `tree_metadata.c` (verified at `tree_protocol.c:6` and
`tree_metadata.c:13`). Keep both patches.

`patches/civetweb-stop-flag-atomic.patch`: civetweb stays (used by
`http_api.c`). Keep.

---

## 2. `Makefile` edits

### 2.1 `INCLUDES` line (line 5)

Current:
```
INCLUDES = -Iinclude -Ilib/wbpxre-dht -Ilib/bencode-c -Ilib/cJSON -Ilib/civetweb/include -Ilib/libbloom -Ilib/uthash/src
```

Drop `-Ilib/wbpxre-dht`. Result:
```
INCLUDES = -Iinclude -Ilib/bencode-c -Ilib/cJSON -Ilib/civetweb/include -Ilib/libbloom -Ilib/uthash/src
```

### 2.2 `LDFLAGS` line (line 6)

Current:
```
LDFLAGS = -luv -lsqlite3 -lpthread -lssl -lcrypto -ldl -lm -lurcu lib/libbloom/build/libbloom.a
```

Drop `-lurcu`. The only consumer is `src/dht_manager.c` (verified via
`#include <urcu.h>` search; only hit is `dht_manager.c:15` which is
deleted in this stage). Result:
```
LDFLAGS = -luv -lsqlite3 -lpthread -lssl -lcrypto -ldl -lm lib/libbloom/build/libbloom.a
```

### 2.3 `LIB_OBJS` block (lines 20-26)

Current:
```
LIB_OBJS = $(BUILD_DIR)/wbpxre_dht.o \
           $(BUILD_DIR)/wbpxre_routing.o \
           $(BUILD_DIR)/wbpxre_protocol.o \
           $(BUILD_DIR)/wbpxre_worker.o \
           $(BUILD_DIR)/bencode.o \
           $(BUILD_DIR)/cJSON.o \
           $(BUILD_DIR)/civetweb.o
```

Drop the four `wbpxre_*.o` entries. Result:
```
LIB_OBJS = $(BUILD_DIR)/bencode.o \
           $(BUILD_DIR)/cJSON.o \
           $(BUILD_DIR)/civetweb.o
```

### 2.4 wbpxre build rules (lines 65-75)

Delete the four explicit `$(BUILD_DIR)/wbpxre_*.o:` rules. The generic
`$(BUILD_DIR)/%.o:` rule at line 62 already handles `bencode.o`,
`cJSON.o`, `civetweb.o`, so no replacement is needed.

### 2.5 `patch-bencode` target (lines 36-44)

PLAN §"Stage 2" says "drop `patch-bencode` if `bencode-c` is being
patched (we keep bencode-c — confirm in stage 3)". We KEEP
`bencode-c` (used by `tree_protocol.c` and `tree_metadata.c`).
KEEP `patch-bencode` as-is.

### 2.6 `install-deps` hint (line 102)

Current text says: `"  - liburcu (userspace-rcu on Arch, liburcu-dev on Debian/Ubuntu)"`. Drop this line.

---

## 3. Build artifacts that linger

`make clean` (line 90-93) already does `rm -rf $(BUILD_DIR)`. The
stale `*.o` files for `dht_manager.o`, `metadata_fetcher.o`, etc.
will be removed on next `make clean`. The user should run
`make clean && make` to verify stage 2.

`build/wbpxre_*.o` and `lib/wbpxre-dht/wbpxre_*.o` are stranded
on disk. Not a correctness concern. They will be created on next
`make` attempt if the `wbpxre_*.o` rules remain — they don't, so
no further artifacts.

---

## 4. Expected build failures after stage 2

`make` is expected to fail with unresolved symbols in:

- `src/main.c` (refs `dht_manager_*`, `metadata_fetcher_*`,
  `infohash_queue_*`, `g_dht_mgr`, `g_fetcher`, `g_queue`,
  `http_api_init`'s old signature)
- `src/http_api.c` (refs `dht_manager.h`, `metadata_fetcher.h`,
  the old `http_api_init` parameters)
- `src/supervisor.c` (refs `wbpxre_dht.h` for `global_bootstrap`)

Document these in the stage 2 commit message. Do NOT attempt to fix
them — that is stage 3's job.

---

## 5. Verification at end of stage 2

- `ls lib/wbpxre-dht/ 2>&1` returns "No such file or directory".
- `grep -RE 'wbpxre_dht\.h|wbpxre_routing|wbpxre_protocol|wbpxre_worker|tribuf|urcu' include/ src/ Makefile` returns no hits.
- `grep -RE 'dht_manager|metadata_fetcher|infohash_queue|worker_pool|peer_retry_tracker|peer_store|connection_request_queue' include/ src/` returns no hits in remaining files (it will return hits in `main.c` and `http_api.c` because those still reference the old types — that is expected; stage 3 cleans it up).
- `find include src -name '*.h' -o -name '*.c' | xargs grep -l 'discovered_nodes\|find_node_worker' 2>/dev/null` returns no hits.
- `make` FAILS with the expected unresolved symbols listed in §4.

---

## 6. Out of scope (intentional)

- `Makefile` `patch-bencode` target — keep (bencode-c is live).
- `Makefile` `patch-civetweb` target — keep (civetweb is live).
- `include/bep51_cache.h` `bep51_cache_populate_shared_pool` — keep
  (function takes `struct shared_node_pool *`; when stage 5 removes
  `shared_node_pool`, this function goes too).
- `include/refresh_thread.h` `shared_node_pool_t` typedef — keep
  (consumed until stage 5).
- `src/refresh_thread.c` `shared_node_pool` field — keep.
- `src/supervisor.c` `shared_node_pool` field and bootstrap calls —
  keep (consumed until stage 5).
- `src/thread_tree.c` `bootstrap_thread_func` that calls
  `shared_node_pool_get_random` — keep (consumed until stage 5).
- `include/dht_crawler.h` `MAX_WORKERS` / `INFOHASH_QUEUE_SIZE` /
  `BATCH_SIZE` macros — keep (used by `main.c` until stage 3
  deletes the old-architecture branch).
