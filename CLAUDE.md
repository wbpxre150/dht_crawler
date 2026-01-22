# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A high-performance BitTorrent DHT crawler written in C that discovers and collects torrent metadata from the DHT network. The project implements a multi-threaded architecture optimized for Android/Termux environments with thread limits, capable of discovering thousands of torrents per hour.

## Build Commands

```bash
# Build the project
make

# Clean build artifacts
make clean

# Build with debug symbols
make debug

# Build with AddressSanitizer (memory safety)
make asan

# Build with ThreadSanitizer (thread safety)
make tsan

# Run with Valgrind memory checker
make valgrind
```

## Dependencies

Required system packages:
- libuv-dev (async I/O)
- libsqlite3-dev (database)
- libssl-dev (OpenSSL for crypto)
- build-essential (compiler toolchain)
- liburcu (userspace-rcu on Arch, liburcu-dev on Debian/Ubuntu)

The Makefile automatically builds submodule dependencies (libbloom) and applies patches (bencode-cap-fix.patch).

## High-Level Architecture

### Thread Tree Supervisor Model

The crawler uses a **supervisor pattern** that manages multiple independent "thread trees" (isolated DHT crawler units). This architecture solves Android's thread limit constraints while maintaining high throughput.

**Key concept**: Instead of one massive multi-threaded crawler, the supervisor spawns 16-32 small thread trees, each with ~300 threads. Trees are isolated with private state but share critical resources.

- **Supervisor** (`supervisor.h`, `supervisor.c`): Manages lifecycle of all thread trees
  - Monitors metadata fetch rates per tree
  - Respawns underperforming trees (exhausted keyspace regions)
  - Provides shared resources: `batch_writer`, `bloom_filter`, `failure_bloom`, `shared_node_pool`, `bep51_cache`

- **Thread Tree** (`thread_tree.h`, `thread_tree.c`): Isolated DHT crawler unit
  - Private state: node_id, routing table, infohash queue, peers queue, UDP socket
  - Shares: batch writer, bloom filters, node pool
  - Lifecycle phases: BOOTSTRAP → BEP51 → GET_PEERS → METADATA → SHUTTING_DOWN
  - Keyspace partitioning: Each tree claims a partition of the DHT keyspace for comprehensive coverage

### Five-Stage Pipeline (Per Thread Tree)

Each thread tree implements a concurrent pipeline:

1. **Bootstrap Stage** (`thread_tree.c:bootstrap_thread_func`)
   - **Intelligent bootstrap prioritization**:
     - When BEP51 cache is FULL (count >= capacity): Uses cache as PRIMARY source
     - When BEP51 cache is NOT full: Uses shared pool as primary, cache as fallback
   - Global bootstrap pool: Shared static pool populated once at startup (5000 nodes)
   - BEP51 cache: Dynamic cache of verified BEP51-capable nodes (5000 capacity)
     - Continuously updated by BEP51 workers during operation
     - Persisted to disk for instant bootstrap across restarts
     - Prioritized over static pool once full for better node quality

2. **Find_Node Workers** (`thread_tree.c:find_node_worker_func`)
   - Continuously discover DHT nodes to populate routing table
   - Adaptive throttling: Slows down when routing table reaches target size (1500 nodes)
   - Pauses when infohash queue is full to reduce mutex contention

3. **BEP51 Workers** (`thread_tree.c:bep51_worker_func`)
   - Query DHT nodes with `sample_infohashes` (BEP 51) to discover infohashes
   - Populates infohash queue for stage 4
   - Submit high-quality BEP51 nodes to shared cache (5% sample rate)
   - Throttling: Pauses with find_node workers when queue full

4. **Get_Peers Workers** (`thread_tree.c:get_peers_worker_func`)
   - Consume infohashes from queue
   - Query DHT for peers that have each infohash
   - Populates peers queue for metadata fetching
   - Separate throttling: Pauses when peers queue full

5. **Metadata Workers** (`tree_metadata.c`)
   - Connect to peers via TCP and fetch torrent metadata (BEP 9/10)
   - Handles BitTorrent extension protocol (ut_metadata)
   - Connection pooling with configurable limits
   - Writes to database via shared batch writer

### Shared Resources (Cross-Tree)

- **Batch Writer** (`batch_writer.h`, `batch_writer.c`)
  - Thread-safe batched database writes
  - Flushes every 60s or when batch reaches 500 torrents
  - 10-100x performance improvement over individual INSERTs
  - Tracks hourly statistics for monitoring

- **Bloom Filters** (`bloom_filter.h`, `bloom_filter.c`)
  - Main bloom: Tracks seen infohashes (30M capacity, 0.1% error rate)
  - Failure bloom: Tracks failed infohashes with two-strike policy
    - First failure: Marked in failure bloom, retry allowed
    - Second failure: Permanently blocked from retry
  - Persisted to disk for restarts

- **Shared Node Pool** (`shared_node_pool.h`)
  - Global bootstrap pool shared across all trees
  - Populated once at startup with 5000 nodes
  - Trees sample 1000 nodes for fast bootstrap

- **BEP51 Cache** (`bep51_cache.h`)
  - Persists BEP51-capable nodes to disk
  - 5000 node capacity
  - Trees submit 5% of BEP51 responses
  - Enables instant bootstrap on restarts

### Supporting Components

- **Tree Routing Table** (`tree_routing.h`, `tree_routing.c`)
  - Per-tree DHT routing table (Kademlia structure)
  - Target: 1500 nodes per tree
  - Supports keyspace partitioning for comprehensive coverage

- **Tree Dispatcher** (`tree_dispatcher.h`)
  - Maps UDP response transaction IDs (TIDs) to per-worker response queues
  - Enables concurrent DHT queries without blocking

- **Infohash Queue** (`tree_infohash_queue.h`)
  - Lock-free queue (3000-5000 capacity per tree)
  - Feeds get_peers workers

- **Peers Queue** (`tree_peers_queue.h`)
  - Lock-free queue (3000 capacity per tree)
  - Feeds metadata workers

- **Porn Filter** (`porn_filter.h`, `porn_filter.c`)
  - Three-layer filtering: keyword hash set, regex patterns, heuristics
  - Filters discovered torrents before database insertion
  - Configurable thresholds in config.ini

- **HTTP API** (`http_api.h`, `http_api.c`)
  - REST API for statistics and on-demand queries
  - `/stats` - Overall crawler statistics
  - `/refresh?infohash=<hex>` - On-demand get_peers query via refresh thread

- **Refresh Thread** (`refresh_thread.h`)
  - Lightweight singleton thread for on-demand queries
  - Separate from main crawler trees
  - Maintains small routing table (500 nodes)
  - Processes `/refresh` HTTP endpoint requests

## Configuration

All settings are in `config.ini`. Key parameters:

- **Thread counts** (per tree, multiplied by num_trees):
  - `tree_find_node_workers=5` (160 total for 32 trees)
  - `tree_bep51_workers=10` (320 total)
  - `tree_get_peers_workers=125` (4000 total)
  - `tree_metadata_workers=110` (3520 total)

- **Queue capacities** (per tree):
  - `tree_infohash_queue_capacity=3000`
  - `tree_peers_queue_capacity=3000`

- **Throttling thresholds**:
  - `tree_infohash_pause_threshold=2500` (pause find_node+BEP51)
  - `tree_infohash_resume_threshold=1000`
  - `tree_peers_pause_threshold=2500` (pause get_peers)
  - `tree_peers_resume_threshold=1000`

- **Tree respawn settings**:
  - `min_metadata_rate=0.08` (metadata/sec before respawn)
  - `tree_min_lifetime_minutes=20` (grace period before rate checks)

## Code Organization

```
include/          - Public header files
src/              - Implementation files
lib/              - Third-party libraries (git submodules)
  ├── bencode-c/  - Bencode parser (patched)
  ├── cJSON/      - JSON library
  ├── libbloom/   - Bloom filter
  ├── uthash/     - Hash table macros
  └── civetweb/   - HTTP server
patches/          - Submodule patches
data/             - Runtime data (database, bloom filters, caches)
build/            - Build artifacts
```

## Important Implementation Details

### Keyspace Partitioning

Each thread tree is assigned a partition of the DHT keyspace:
- `partition_index`: Tree's partition number (0 to num_partitions-1)
- `num_partitions`: Total partitions (equal to num_trees)
- Node IDs are generated to place trees in specific keyspace regions
- Prevents trees from competing for the same infohashes

### Throttling Mechanisms

Two independent throttling systems prevent queue overflow:

1. **Discovery throttling** (find_node + BEP51 workers):
   - Controlled by `discovery_paused` atomic flag
   - Uses `throttle_lock` mutex + `throttle_resume` condition variable
   - Triggered when infohash queue ≥ pause threshold

2. **Get_peers throttling**:
   - Controlled by `get_peers_paused` atomic flag
   - Uses `get_peers_throttle_lock` mutex + `get_peers_throttle_resume` condition variable
   - Triggered when peers queue ≥ pause threshold

### Two-Strike Failure Policy

Infohashes get two chances before permanent blocking:
- First failure: Marked in `failure_bloom`, can be retried by different tree
- Second failure: Permanently blocked (bloom filter check rejects)
- Prevents wasting resources on consistently failing infohashes

### Metadata Fetching Flow

1. Worker dequeues (infohash, peers[]) from peers queue
2. Creates `infohash_attempt_t` tracking structure
3. Attempts 2 peers in parallel (configurable)
4. TCP handshake → Extended handshake → Request metadata pieces
5. On success: Write to batch writer, mark in main bloom
6. On all peers fail: Mark in failure bloom (first strike) or permanently block (second strike)

## Database Schema

SQLite database with single table:

```sql
CREATE TABLE torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash TEXT UNIQUE NOT NULL,
    name TEXT,
    discovered_at INTEGER NOT NULL,
    file_count INTEGER,
    file_info TEXT
)
```

`file_info` column stores JSON array of file objects.

## Common Development Tasks

### Adding a new configuration parameter

1. Add to `config.ini` with comment
2. Add field to `app_config_t` in `include/config.h`
3. Parse in `config_load()` in `src/config.c`
4. Pass to relevant component (supervisor_config_t, tree_config_t, etc.)

### Modifying thread tree behavior

Thread tree lifecycle is in `src/thread_tree.c`:
- Bootstrap: `bootstrap_main_thread()`
- Worker functions: `find_node_worker_func()`, `bep51_worker_func()`, `get_peers_worker_func()`
- Phase transitions: Check `current_phase` and `shutdown_requested`

### Debugging thread issues

Use sanitizers:
```bash
make tsan  # ThreadSanitizer
./dht_crawler
```

Enable debug logging in config.ini:
```ini
log_level=DEBUG
```

### Testing protocol changes

The DHT protocol layer is in:
- `tree_protocol.h/c` - Packet construction/parsing
- `tree_socket.h/c` - UDP I/O
- `tree_dispatcher.h/c` - Response routing

Metadata protocol (BEP 9/10) is in `tree_metadata.c`.

## External Libraries

- **libuv**: Event loop for async I/O (metadata fetcher)
- **wbpxre-dht**: Custom DHT library (in lib/, not external)
- **bencode-c**: Requires patch (`patches/bencode-cap-fix.patch`) for capacity bug
- **cJSON**: JSON parsing for file_info
- **libbloom**: Probabilistic duplicate detection
- **uthash**: Hash table macros for lookup tables
- **civetweb**: Embedded HTTP server for API

## Performance Characteristics

Typical performance on Android (32 trees, config.ini settings):
- 10-50 torrents/minute sustained
- 6,000-9,000 total threads
- 5,000 concurrent TCP connections
- 30M infohash bloom capacity

Bottlenecks:
- Metadata fetching (TCP latency, peer availability)
- SQLite write performance (mitigated by batch writer)
- Bloom filter size (memory)
