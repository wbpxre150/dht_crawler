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
./dht_crawler [--rebuild-bloom-filter] [--porn-filter-update] [--compact]
              [--check-database] [--recover-database] [--dont-check-database]
```

These are maintenance modes that exit after completing their task rather than running the crawler.

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
- `porn_filter_enabled`, `language_filter_non_latin_threshold` — content filtering
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
