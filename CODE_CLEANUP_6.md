# Stage 6 — Final pass: documentation, `config.ini` cleanup, dead-code check

Goal: clean up `config.ini`, refresh `CONFIG_INI.md` to a one-pager
listing only the live keys, and run a final dead-code grep to
verify zero references to deleted symbols remain in the keep set.

After this stage, the project is single-architecture (thread trees
only), the only knobs the operator touches are documented in
`CONFIG_INI.md`, and the build + smoke tests still pass.

---

## 1. `config.ini` — strip dead keys, rename bootstrap keys

### 1.1 Section header cleanup

Remove these section headers (their bodies are gone):

| Line | Header to delete |
|---|---|
| 29 | `# ==== PHASE 4: Worker Pool Settings ====` |
| 34 | `# ==== PHASE 5: Batched Database Writes ====` |
| 96 | `# ==== wbpxre-dht Settings ====` |
| 104 | `# ==== Peer Discovery Retry Settings ====` |
| 116 | `# ==== Triple Routing Table Settings ====` |

The "PHASE 5" header is partially live (it groups `batch_writes_enabled`,
`batch_size`, `flush_interval`); keep the header but trim the dead
keys (`batch_writes_enabled` at line 36).

### 1.2 Dead keys to remove

In source order, delete the following lines (key + comment lines
above and below as appropriate):

| Line | Key | Reason |
|---|---|---|
| 9 | `http_port=8080` | Replaced by `HTTP_API_PORT` macro |
| 31 | `scaling_factor=10` | Old worker pool (stage 1) |
| 32 | `metadata_workers=200` | Old worker pool |
| 36 | `batch_writes_enabled=1` | No code branch checks this |
| 66-71 | `# Meta Data Fetcher` block, `max_concurrent_connections` | Only consumed by `metadata_fetcher.c` (deleted) |
| 78-81 | `tcp_connect_timeout_sec = 2` (and comment block above) | Only consumed by `metadata_fetcher.c` |
| 83-86 | `connection_timeout_sec = 8` (and comment block above) | Same |
| 88-91 | `max_connection_lifetime_sec = 15` (and comment block above) | Same |
| 93-94 | `max_metadata_size_mb = 100` (and comment above) | Never implemented |
| 98-102 | All 5 `wbpxre_*` keys (and comment block above) | `lib/wbpxre-dht/` deleted |
| 109-114 | All 6 `peer_retry_*` keys (and comment block above) | Never wired in |
| 130 | `triple_routing_threshold=1500` (and comment block above) | Design abandoned |
| 138 | `triple_routing_rotation_time=30` (and comment block above) | Same |
| 165 | `use_thread_trees = 1` (and comment block above) | Stage 3 removed the branch; only one path remains |
| 171-179 | `# Global bootstrap settings` block (`global_bootstrap_target`, `global_bootstrap_timeout_sec`, `global_bootstrap_workers`, `per_tree_sample_size`) | Stage 5 removed the shared-pool bootstrap |

### 1.3 Rename `global_bootstrap_*` to `tree_bootstrap_*`

PLAN §6: "rename `global_bootstrap_*` to `tree_bootstrap_*`".

Per stage 5, only `tree_bootstrap_timeout_sec` survives; the
others (`target`, `workers`) are removed outright. So the rename
is just the timeout key:

- Replace `global_bootstrap_timeout_sec = 120` (line 175) with
  `tree_bootstrap_timeout_sec = 30`. Default in `config.c`
  (stage 5 §7) is 30. Keep the comment block above explaining
  "Maximum time for tree bootstrap in seconds (default: 30)".

### 1.4 New section ordering

The cleanest post-stage-6 `config.ini` order:

1. DHT (`dht_port`)
2. Database (`db_path`, `log_level`)
3. Bloom filter (`bloom_*`, `failure_bloom_capacity`)
4. Batched writes (`batch_size`, `flush_interval`)
5. Daily backup (`backup_*`, `ssh_*`, `rclone_*`)
6. Porn filter (`porn_filter_*`)
7. Language filter (`language_filter_non_latin_threshold`)
8. Tree-native bootstrap (`tree_bootstrap_timeout_sec`)
9. Thread tree architecture (`num_trees`, `use_keyspace_partitioning`, `dead_partition_threshold`, `max_trees_per_partition`)
10. Per-tree worker counts (`tree_find_node_workers`, `tree_bep51_workers`, `tree_get_peers_workers`, `tree_metadata_workers`, `tree_infohash_queue_capacity`, `tree_peers_queue_capacity`, `tree_parallel_peers`, `tree_bep51_query_interval_ms`)
11. Throttling thresholds (`tree_infohash_pause/resume_threshold`, `tree_peers_pause/resume_threshold`)
12. Rate-based respawn (`min_metadata_rate`, `dynamic_rate_margin`, `tree_rate_*`, `tree_min_lifetime_minutes`, `tree_require_empty_queue`)
13. Respawn overlapping (`respawn_*`)
14. BEP51 cache (`bep51_cache_*`, `bep51_node_cooldown_sec`)
15. Refresh thread (`refresh_*`)

The author should reorder, not just delete-and-rename. Operators
are not going to compare to a diff; they see the file from scratch.

### 1.5 Add a `tree_bootstrap_timeout_sec` key

Already in §1.3. Place it under a new section header
`# ==== Tree-Native Bootstrap ====` with a comment explaining
that each tree performs its own URL bootstrap; the timeout
bounds how long a single tree waits before giving up.

### 1.6 New comment header at top of file

Replace the existing top comment (lines 1-3):
```
# DHT Crawler Configuration File - ANDROID OPTIMIZED
# Optimized for Termux/Android thread limits while maintaining throughput
# Lines starting with # are comments
```

With:
```
# DHT Crawler Configuration File
# Single architecture: thread trees (no legacy worker pool, no
# wbpxre-dht). Each tree performs its own URL bootstrap from
# built-in DHT routers; the BEP51 cache is a warm-start fallback.
# Lines starting with # are comments.
```

### 1.7 New section: tree-native bootstrap example

Add a worked example block (operator-facing):
```
# ==== Tree-Native Bootstrap ====
# Each tree independently contacts these built-in DHT routers at
# startup to populate its routing table. BEP51 cache is used as
# a warm-start fallback when these routers are unreachable.
# tree_bootstrap_timeout_sec bounds how long a single tree waits
# before giving up and falling back to the cache (or shutting
# its bootstrap loop down on an empty cache).
tree_bootstrap_timeout_sec = 30
```

---

## 2. `CONFIG_INI.md` — rewrite as a one-pager

PLAN §6: "`CONFIG_INI.md` becomes a one-pager listing only the live
keys and their consumers."

### 2.1 Section structure

Replace the entire 205-line `CONFIG_INI.md` with:

```
# DHT Crawler config.ini Reference

Every key in config.ini, in order, with the consumer source file
and the line at which it is read. Anything not on this list is
ignored.

## Network
| Key | Default | Consumer |
| dht_port | 6881 | supervisor.c:57, refresh_thread.c:109, thread_tree.c:1214 |

## Database & Logging
| db_path | data/torrents.db | main.c:522 |
| log_level | INFO | main.c:258 |

## Bloom Filter
| bloom_enabled | 1 | main.c:266, batch_writer.c, refresh_thread.c |
| bloom_capacity | 30000000 | main.c:280, bloom_filter.c |
| failure_bloom_capacity | 30000000 | supervisor.c:69 |
| bloom_error_rate | 0.001 | main.c:280, supervisor.c:70 |
| bloom_persist | 1 | main.c:270, 423 |
| bloom_path | data/bloom.dat | main.c:274, 295 |

## Batched Writes
| batch_size | 500 | main.c:643, supervisor.c:118 (defaults) |
| flush_interval | 300 | main.c:643, supervisor.c:118 (defaults) |

## Backup
| backup_enabled | 1 | main.c:658 |
| backup_path | /home/... | main.c:659 |
| ssh_backup_* | 0 | main.c:663-668 |
| rclone_backup_* | 1 | main.c:671-676 |

## Porn Filter
| porn_filter_enabled | 1 | main.c:554, supervisor.c:115 |
| porn_filter_keyword_file | porn_filter_keywords.txt | main.c:451 |

## Language Filter
| language_filter_non_latin_threshold | 33 | main.c:462 |

## Tree-Native Bootstrap
| tree_bootstrap_timeout_sec | 30 | config.c default; supervisor.c set; thread_tree.c read |

## Thread Tree Architecture
| num_trees | 32 | main.c:680, supervisor.c:140 |
| use_keyspace_partitioning | 1 | supervisor.c:56, thread_tree.c |
| dead_partition_threshold | 3 | supervisor.c:146 |
| max_trees_per_partition | 4 | supervisor.c:147 |

## Per-Tree Worker Counts (multiplied by num_trees)
| tree_find_node_workers | 3 | main.c:688, supervisor.c:73 |
| tree_bep51_workers | 10 | main.c:689 |
| tree_get_peers_workers | 50 | main.c:690 |
| tree_metadata_workers | 75 | main.c:691 |
| tree_infohash_queue_capacity | 3000 | main.c:701 |
| tree_bep51_query_interval_ms | 5 | main.c:702 |
| tree_bep51_node_cooldown_sec | 3 | main.c:703 |
| tree_peers_queue_capacity | 3000 | main.c:705 |
| tree_get_peers_timeout_ms | 3000 | main.c:706 (default in config) |
| tree_parallel_peers | 4 | main.c:715 |
| tree_tcp_connect_timeout_ms | 2000 | main.c:714 |

## Throttling
| tree_infohash_pause_threshold | 2500 | main.c:708 |
| tree_infohash_resume_threshold | 1000 | main.c:709 |
| tree_peers_pause_threshold | 2500 | main.c:711 |
| tree_peers_resume_threshold | 1000 | main.c:712 |

## Rate-Based Respawn
| min_metadata_rate | 0.005 | main.c:717 |
| dynamic_rate_margin | 0.01 | main.c:718 |
| tree_rate_check_interval_sec | 120 | main.c:719 |
| tree_rate_grace_period_sec | 30 | main.c:720 |
| tree_min_lifetime_minutes | 8 | main.c:721 |
| tree_require_empty_queue | 0 | main.c:722 |
| tree_rate_ema_alpha | 0.3 | main.c:723 |

## Respawn Overlapping
| respawn_spawn_threshold | 20 | main.c:725 |
| respawn_drain_timeout_sec | 180 | main.c:726 |
| max_draining_trees | 16 | main.c:727 |

## BEP51 Cache
| bep51_cache_path | data/bep51_cache.dat | main.c:735, supervisor.c:120 |
| bep51_cache_capacity | 50000 | main.c:698, supervisor.c:118 |
| bep51_cache_submit_percent | 17 | main.c:699, supervisor.c:119 |
| bep51_node_cooldown_sec | 3 | main.c:703 |

## Refresh Thread (HTTP /refresh endpoint)
| refresh_bootstrap_sample_size | 1000 | main.c:766 |
| refresh_routing_table_target | 500 | main.c:767 |
| refresh_ping_workers | 1 | main.c:768 |
| refresh_find_node_workers | 1 | main.c:769 |
| refresh_get_peers_workers | 1 | main.c:770 |
| refresh_request_queue_capacity | 100 | main.c:771 |
| refresh_get_peers_timeout_ms | 500 | main.c:772 |
| refresh_max_iterations | 3 | main.c:773 |

---

## Keys intentionally NOT in config.ini

These existed before stage 1/2/3/4/5/6 cleanup and are no longer
read by any source file. They are not parsed; if present in
config.ini they are silently ignored:

- `targeted_search_percentage`, `http_port`, `scaling_factor`,
  `metadata_workers`, `max_retry_attempts`, `retry_delay_sec`,
  `max_metadata_size_mb`, `batch_writes_enabled`, all `wbpxre_*`,
  all `peer_retry_*`, `triple_routing_threshold`,
  `triple_routing_rotation_time`, `use_thread_trees`,
  `global_bootstrap_target`, `global_bootstrap_workers`,
  `global_bootstrap_timeout_sec`, `per_tree_sample_size`,
  `max_concurrent_connections`, `tcp_connect_timeout_sec`,
  `connection_timeout_sec`, `max_connection_lifetime_sec`.

This list mirrors the deletion surface in
`include/config.h` / `src/config.c` after stage 1.

---

## Updates

- 2026-06: rewrote for thread-tree-only build (PLAN.md stage 6).
- Earlier revisions documented the dual wbpxre + thread-tree
  era. See git history.
```

(Line numbers above are post-stages 1-5. They will need to be
verified during implementation against the actual final
`src/main.c`, `src/supervisor.c`, `src/config.c` content.)

---

## 3. Dead-code verification

PLAN §6: "Run a final dead-code check: ripgrep for `wbpxre`,
`discovered_nodes`, `find_node_worker`, `peer_retry`,
`infohash_queue` (with `tree_infohash_queue` allowed),
`worker_pool`, `metadata_fetcher`, `dht_manager`, `peer_store`,
`connection_request_queue`, `tribuf`, `urcu`. All hits must be
inside test or `git` historical contexts."

### 3.1 Grep targets (negative patterns)

Run each of these and confirm zero hits in source/build files
(config.ini, *.md, *.disabled are excluded):

```bash
rg -n 'wbpxre' --type c --type h src/ include/ Makefile
rg -n 'discovered_nodes' --type c --type h src/ include/
rg -n 'find_node_worker\.c|find_node_worker_func' --type c --type h src/ include/
rg -n 'peer_retry' --type c --type h src/ include/
rg -n 'infohash_queue_(init|cleanup|size|capacity|is_full|is_empty|set_bloom|set_database|get_duplicates|push|try_pop|signal_shutdown)' --type c --type h src/ include/
rg -n 'worker_pool' --type c --type h src/ include/
rg -n 'metadata_fetcher' --type c --type h src/ include/
rg -n 'dht_manager' --type c --type h src/ include/
rg -n 'peer_store' --type c --type h src/ include/
rg -n 'connection_request_queue' --type c --type h src/ include/
rg -n 'tribuf' --type c --type h src/ include/
rg -n 'urcu|URCU' --type c --type h src/ include/
```

`tree_infohash_queue` is allowed. `tree_find_node_worker` is
allowed (this is `tree_start_find_node_workers`, a live symbol).

For positive confirmation that the new live code is in place:
```bash
rg -n 'tree_bootstrap_timeout_sec|TREE_BOOTSTRAP_HOSTS|bootstrap_response_queue' --type c src/thread_tree.c
rg -n 'tree_bootstrap_timeout_sec' --type c src/config.c src/supervisor.c src/main.c
rg -n 'bep51_cache_get_count' --type c src/ include/
```

### 3.2 Linker-level verification

After all source changes:

```bash
make clean && make 2>&1 | tee build.log
```

Confirm no unresolved symbols. The linker output should contain
no `undefined reference to` lines for any of the deleted files.

```bash
nm -u build/dht_crawler | grep -E 'wbpxre|dht_manager|metadata_fetcher|infohash_queue|worker_pool|peer_retry|peer_store|connection_request_queue|shared_node_pool|tribuf|urcu'
```

This must return zero hits.

### 3.3 Header-only references (acceptable)

`bep51_cache.h` (post stage 5 §2.3) no longer has a forward
declaration of `struct shared_node_pool`. Verify with:
```bash
grep -n 'shared_node_pool' include/bep51_cache.h
```
Expected: no hits.

`supervisor.h` and `thread_tree.h` likewise have no
`shared_node_pool` references after stage 5 §3.7 and §1.6.

---

## 4. `config.c` — final structural pass

Stage 1 removed the 22 dead fields, but the parser was kept
"silent" on unknown keys (no warning, no error). This is the
right behavior for forward-compat, but for the post-stage-6
project, an optional refinement is to add a debug log when an
unknown key is encountered. PLAN §6 does not require this; skip
unless requested.

Stage 5 §7 already added the new `tree_bootstrap_timeout_sec`
key. Verify the field is in `crawler_config_t` and the parser
clamp is `[10, 600]` (matching the old
`global_bootstrap_timeout_sec` clamp; per-tree bootstrap can
shorter on average because each tree runs in parallel).

---

## 5. README and CLAUDE.md

### 5.1 `README.md`

Search for references to deleted concepts:
- "wbpxre"
- "worker pool"
- "global bootstrap"
- "dht_manager"
- "metadata_fetcher"
- "infohash queue"
- "peer_retry"
- "triple routing"

If any of these terms appear in a description of "what the
crawler is" or "how to configure", update to the new model.
Most references should be in CHANGELOG-style "previously we had
X" sections; leave those alone. Only edit forward-looking
documentation.

### 5.2 `CLAUDE.md`

`CLAUDE.md` is a 4-month-old developer-guide document. Search
for the same terms; same edit rule as README. If the document
is a snapshot of the project state at that point in time,
leave it untouched (CHANGELOG-style) and add a one-line
"Status: see PLAN.md and CODE_CLEANUP_*.md for the cleanup
roadmap" at the top.

---

## 6. `PLAN.md` post-completion stamp

Add a closing section to `PLAN.md`:
```
## Implementation status

See CODE_CLEANUP_1.md ... CODE_CLEANUP_6.md for the per-stage
implementation plans. Stages 1-6 are prerequisites; each must
build clean before the next starts. Stage 2 is the only one
allowed to leave the build broken (it sets up the deletions
that stage 3 wires around).
```

---

## 7. Verification

### 7.1 Headless smoke run

```bash
# Build
make clean && make -j8

# Run with a tmpfs data directory and a short tree bootstrap timeout
# so the test finishes in seconds, not minutes.
mkdir -p /tmp/dht_crawler_test
cat > /tmp/dht_crawler_test/config.ini <<'EOF'
dht_port=0
db_path=/tmp/dht_crawler_test/torrents.db
log_level=INFO
bloom_enabled=1
bloom_capacity=100000
failure_bloom_capacity=100000
bloom_error_rate=0.01
bloom_persist=0
bloom_path=/tmp/dht_crawler_test/bloom.dat
batch_size=10
flush_interval=10
backup_enabled=0
backup_path=
ssh_backup_enabled=0
rclone_backup_enabled=0
porn_filter_enabled=0
language_filter_non_latin_threshold=0
tree_bootstrap_timeout_sec=10
num_trees=2
use_keyspace_partitioning=1
dead_partition_threshold=3
max_trees_per_partition=2
tree_find_node_workers=1
tree_bep51_workers=1
tree_get_peers_workers=1
tree_metadata_workers=1
tree_infohash_queue_capacity=100
tree_bep51_query_interval_ms=10
tree_bep51_node_cooldown_sec=30
tree_peers_queue_capacity=100
tree_get_peers_timeout_ms=1000
tree_parallel_peers=1
tree_tcp_connect_timeout_ms=2000
tree_infohash_pause_threshold=80
tree_infohash_resume_threshold=40
tree_peers_pause_threshold=80
tree_peers_resume_threshold=40
min_metadata_rate=0.0
dynamic_rate_margin=0.0
tree_rate_check_interval_sec=60
tree_rate_grace_period_sec=30
tree_min_lifetime_minutes=1
tree_require_empty_queue=0
tree_rate_ema_alpha=0.3
respawn_spawn_threshold=0
respawn_drain_timeout_sec=30
max_draining_trees=2
bep51_cache_path=/tmp/dht_crawler_test/bep51.dat
bep51_cache_capacity=100
bep51_cache_submit_percent=5
bep51_node_cooldown_sec=30
refresh_bootstrap_sample_size=50
refresh_routing_table_target=50
refresh_ping_workers=0
refresh_find_node_workers=1
refresh_get_peers_workers=1
refresh_request_queue_capacity=10
refresh_get_peers_timeout_ms=500
refresh_max_iterations=1
EOF

# Launch
./dht_crawler -c /tmp/dht_crawler_test/config.ini &
CRAWLER_PID=$!
sleep 15

# Verify HTTP /stats returns 200 with the new JSON shape
curl -fsS http://127.0.0.1:8080/stats | head -c 4096
echo

# Send SIGTERM, verify clean shutdown
kill -TERM $CRAWLER_PID
wait $CRAWLER_PID || true

# Cleanup
rm -rf /tmp/dht_crawler_test
```

Expected: HTTP/1.1 200 OK with a JSON body containing
`{"version", "torrents_indexed", "files_indexed",
"torrents_last_hour", "uptime", "supervisor", "bep51_cache",
"trees", "partitions", "aggregate"}` keys. The "dht" and
"metadata_fetcher" sub-objects must NOT be present (they were
removed in stage 3).

### 7.2 Bootstrap smoke run

```bash
# Same setup as 7.1 but with a generous tree_bootstrap_timeout_sec
# and a non-zero num_trees. With real internet access, each tree
# should reach 100+ nodes within the timeout and transition to
# BEP51 within seconds.
cat > /tmp/dht_crawler_test/config.ini <<'EOF'
... (same as 7.1 with tree_bootstrap_timeout_sec=60, num_trees=4)
EOF

# Run and watch logs for "Tree-native bootstrap complete" + "Transitioning to BEP51"
./dht_crawler -c /tmp/dht_crawler_test/config.ini 2>&1 | grep -E 'Tree-native bootstrap|Transitioning to BEP51'
```

Expected within 60s of startup: 4 "Tree-native bootstrap complete"
log lines, one per tree, each reporting ≥100 routing-table nodes,
followed by 4 "Transitioning to BEP51" lines.

### 7.3 Final checks

- `make` succeeds.
- `nm -u build/dht_crawler` shows no undefined references to
  deleted symbols.
- `grep -RE 'wbpxre|discovered_nodes|find_node_worker\.c|peer_retry|infohash_queue_(init|cleanup|size|capacity|set_bloom|set_database)|worker_pool|metadata_fetcher|dht_manager|peer_store|connection_request_queue|tribuf|urcu' src/ include/ Makefile` returns no hits.
- `config.ini` parses with the new key set and the binary
  reaches the HTTP server state.
- `HTTP /stats` returns the new shape (no `dht` or
  `metadata_fetcher` sub-objects).
- SIGTERM produces a clean shutdown with no zombie threads.
- The smoke run with real network connectivity reaches
  BEP51 in seconds, not minutes.

---

## 8. Out of scope

- Performance tuning of the bootstrap. The 30s default
  `tree_bootstrap_timeout_sec` is conservative; operators can
  tune per deployment.
- HTTP API additions (e.g. `/bootstrap/stats` showing the new
  tree-native bootstrap state). Not in PLAN.
- Replacing civetweb. The keep set uses civetweb; no change.
- Per-host bootstrap retry policy. The current design does
  one attempt per host; PLAN does not call for retries.
- IPv6 bootstrap. Bootstrap is IPv4 only; tree_socket already
  supports IPv6 but bootstrap is hardcoded to `AF_INET` via
  `getaddrinfo` hints. PLAN does not call for IPv6 in this
  stage.

---

## 9. Acceptance

- `config.ini` has been stripped of all 22 dead keys from
  stage 1, the 5 `metadata_fetcher.c`-only keys, the 4
  `global_bootstrap_*` keys from stage 5, the
  `use_thread_trees` toggle from stage 3, and any remaining
  `targeted_search_percentage` / `http_port` /
  `triple_routing_*` / `batch_writes_enabled` references.
- `tree_bootstrap_timeout_sec` exists with default 30.
- `CONFIG_INI.md` is a one-pager with the live keys, their
  defaults, and their consumers.
- The dead-code grep returns zero hits in the keep set.
- The build is clean.
- The smoke runs in §7 pass.
