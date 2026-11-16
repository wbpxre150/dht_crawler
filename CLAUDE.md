# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
make              # Build with -O2 (output: ./dht_crawler)
make debug        # Build with -g -DDEBUG
make asan         # Build with AddressSanitizer + UBSan
make tsan         # Build with ThreadSanitizer
make valgrind     # Debug build + valgrind wrapper
make clean        # Remove build/ and ./dht_crawler
```

System dependencies: `libuv`, `libsqlite3`, `libssl`, `libcrypto`, `libpthread`.

The first `make` automatically applies patches to `lib/bencode-c` and `lib/civetweb`, and builds `lib/libbloom`. No other setup required.

### CLI Flags

```
./dht_crawler [--rebuild-bloom-filter] [--compact]
              [--category-filter-update] [--check-database]
              [--recover-database]
```

These are maintenance modes that exit after completing their task rather than running the crawler.

`--category-filter-update` purges the rows outside `save_categories`/`save_tv_kinds`
and every adult-tagged row (`torrent_class.adult = 1`).

## Architecture

### Supervisor Pattern

The crawler runs N independent **thread trees** (default: 32), each targeting a different slice of the DHT keyspace. A supervisor thread monitors their metadata rates and respawns underperformers. Shared state is minimal:

```
Supervisor (supervisor.c)
├── Thread Tree 0 … Thread Tree 31  (each: thread_tree.c)
│   └── Private: routing table, queues, UDP socket handle
│
└── Shared resources:
    ├── Batch Writer    (batch_writer.c)   — buffered SQLite inserts
    ├── Bloom Filters   (bloom_filter.c)   — dedup + failure tracking
    ├── BEP51 Cache     (bep51_cache.c)    — persistent bootstrap nodes
    └── Shared UDP socket + Dispatcher    — SO_REUSEPORT, routes by txn ID
```

### Five-Stage Pipeline (per tree)

Each tree runs these stages concurrently with independent worker thread pools:

1. **Bootstrap** — contacts hardcoded DHT routers + samples BEP51 cache (cold-start: supervisor first runs a parallel URL bootstrap to populate the cache)
2. **Find_Node workers** (`tree_protocol.c`) — discovers DHT nodes, builds routing table (`tree_routing.c`)
3. **BEP51 workers** (`tree_protocol.c`) — queries `sample_infohashes`, pushes infohashes into `tree_infohash_queue`
4. **Get_Peers workers** (`tree_protocol.c`) — queries peers per infohash, pushes peer addrs into `tree_peers_queue`
5. **Metadata workers** (`tree_metadata.c`) — TCP-connects to peers, fetches torrent info dict via BEP9/10

Throttling: upstream stages pause (condition variable) when downstream queues exceed configurable thresholds; resume when queues drain below a lower threshold.

### get_peers / metadata tuning (patch 19)

Four work-elimination changes, all fail-open: every one restores the pre-patch behaviour at `0` (or at the
old default).

- **Round-idle get_peers exit** — `tree_get_peers_round_idle_ms` (default 1500, `0` = off),
  `thread_tree.c:tree_lookup_peers`. A round ends after that much silence **since the last answer** (the
  timer is refreshed by every matched answer, so a round still producing responses is never cut short), and
  a round that produced **zero** answers terminates the whole lookup instead of walking the remaining
  candidate set. Dead lookups therefore stop burning the full `tree_get_peers_deadline_ms` (6 s) and stop
  sending further rounds. The terminal cause is recorded as a `GP_EXIT_*` reason; only `GP_EXIT_IDLE` and
  `GP_EXIT_DEADLINE` have `/stats` counters.
- **Silent-node penalties** — `tree_get_peers_penalize_misses` (default 1). The round-end sweep charges
  every unanswered fan-out slot against its node through the pre-existing `tree_routing_mark_failed()`
  (`tree_routing.c`), which bumps `fail_count` and evicts the node at `MAX_FAIL_COUNT` (3); silent nodes stop
  being re-selected and the find_node workers refill the table. The sweep is the only place that charges
  misses — answered slots are zeroed in the drain loop and failed sends never get a transaction ID.
- **Piece-idle metadata eviction** — `tree_metadata_piece_idle_ms` (default 15000, `0` = off),
  `tree_metadata.c`. `last_piece_time` is refreshed **only** when a new metadata data piece is copied into
  the shared buffer, so a peer that keeps the socket warm with keep-alive/have/bitfield traffic but delivers
  no pieces is evicted instead of holding a fetch-window slot until the global deadline. The pre-existing
  10 s silence watchdog (`CONN_SILENCE_CLOSE_MS`) still catches fully silent peers and fires first, so no
  event is counted twice; the refill loop then advances the window to the next untried peer. Window and
  deadline defaults moved with it: `tree_parallel_peers` 2 → 4 (clamp narrowed to 1..4 so the accepted value
  equals the effective `MAX_PARALLEL_PEERS`) and `tree_metadata_timeout_ms` 60000 → 30000 (the deadline is
  now a backstop, not the common case).
- **Failure-counter taxonomy** — `/stats` `supervisor.metadata_failures`. `connect`, `handshake`, `stall`,
  `ut_reject`, `size`, `hash` and `other` are **per-connection** outcome events, each counted exactly once at
  the site that first detects the failure; `deadline` is **one event per attempt**, taken at the
  global-timeout break where every live slot is abandoned together. That asymmetry is why
  Σ counters ≥ failed attempts — do not "fix" it. Never count in `parallel_conn_close()`, in the
  post-process close (that reason was already counted inside `parallel_conn_process`) or in the final cleanup
  loop, or a single failure is counted twice. The four get_peers exit/sweep counters
  (`idle_exits`, `deadline_exits`, `unanswered_slots`, `miss_penalties`) are per-tree atomics folded into the
  supervisor at respawn like the other `gp_*` counters and published as `*_total`.

### Respawn Logic

`supervisor.c:monitor_thread_func()` checks each tree's metadata rate (EMA-smoothed) every `rate_check_interval_sec`. Trees below `min_metadata_rate - dynamic_rate_margin` for longer than `rate_grace_period_sec` are respawned. During respawn, the old tree is moved to a "draining" list and destroyed once its active connections drop below `respawn_spawn_threshold` (or after `respawn_drain_timeout_sec`).

Cumulative stats (metadata count, filtered count, failures) are carried across respawns via `atomic_uint_fast64_t` fields on the supervisor.

### Key Files

| File | Role |
|------|------|
| `src/main.c` | Entry point, signal handling, initialises all subsystems in order |
| `src/supervisor.c` | Tree lifecycle, monitor thread, global bootstrap |
| `src/thread_tree.c` | Single tree: phase transitions, worker thread spawning |
| `src/tree_protocol.c` | DHT message encode/decode (find_node, BEP51, get_peers) |
| `src/tree_routing.c` | Kademlia k-bucket routing table |
| `src/tree_dispatcher.c` | Routes UDP responses to waiting workers by transaction ID |
| `src/tree_metadata.c` | BEP9/10 TCP metadata fetcher |
| `src/batch_writer.c` | Buffered SQLite inserts, bloom persistence, backup triggers |
| `src/bep51_cache.c` | Persistent FIFO cache of BEP51-capable nodes |
| `src/database.c` | SQLite schema (`torrents` + `files` tables), WAL mode |
| `src/http_api.c` | CivetWeb REST server (`/stats`, `/search`, `/refresh`) |
| `src/refresh_thread.c` | Dedicated thread for `/refresh?infohash=` endpoint |
| `src/config.c` | INI parser → `crawler_config_t` (~130 keys) |

### Shared vs. Per-Tree State

Per-tree: routing table, infohash queue, peers queue, response queue, socket handle (when using ephemeral port), worker thread handles, rate counters.

Shared (protected): batch writer lock, bloom filter (read-write via supervisor-coordinated access), BEP51 cache mutex, shared dispatcher hash table (mutex per bucket).

### Bloom Filter Design

Two separate bloom filters:
- **`data/bloom.dat`** — infohash deduplication; updated only after successful DB write (avoids blocking retries on transient failures)
- **`data/failure_bloom.dat`** — two-strike failure policy; infohashes that fail metadata fetch twice are suppressed

Both are persisted to disk on each batch flush and reloaded on startup.

## Configuration

`config.ini` is the single configuration source. Key groups:

- `num_trees`, `use_keyspace_partitioning` — tree count and partitioning
- `tree_*_workers` — worker counts per tree (total threads ≈ num_trees × ~140)
- `bloom_capacity`, `failure_bloom_capacity` — filter sizes (~500 MB total at 30M)
- `batch_size`, `flush_interval` — write batching
- `language_filter_non_latin_threshold`, `hard_block_file` — content filtering
- `bep51_cache_*` — bootstrap cache settings
- `min_metadata_rate`, `dynamic_rate_margin`, `rate_*` — respawn thresholds

Bootstrap router list is hardcoded in `src/supervisor.c` (search for `BOOTSTRAP_URLS`).

## HTTP API

```bash
curl http://localhost:8080/stats
curl "http://localhost:8080/search?q=ubuntu"
curl "http://localhost:8080/refresh?infohash=<40-hex>"
```

## Database

```bash
sqlite3 data/torrents.db "SELECT name FROM torrents ORDER BY id DESC LIMIT 10"
```

Schema: `torrents(id, info_hash UNIQUE, name, discovered_at, file_count, file_info JSON)`.
