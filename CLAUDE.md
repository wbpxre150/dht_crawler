# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A high-performance BitTorrent DHT crawler written in C that discovers and catalogs torrents from the BitTorrent DHT network. The crawler passively monitors DHT traffic, actively fetches metadata from peers, and stores results in an SQLite database with full-text search capabilities.

## Build Commands

```bash
# Standard build
make

# Clean build artifacts
make clean

# Build with debug symbols
make debug

# Build with AddressSanitizer (recommended for development)
make asan

# Build with ThreadSanitizer (for concurrency debugging)
make tsan

# Run with Valgrind memory checker
make valgrind

# Run the crawler
./dht_crawler
```

## System Dependencies

Required libraries (use package manager to install):
- `libuv` - Async I/O event loop
- `libsqlite3` - Embedded database
- `libssl/libcrypto` - SHA-1 hashing (OpenSSL)
- `liburcu` - Read-Copy-Update synchronization (userspace-rcu)
- `libpthread` - POSIX threads

## Core Architecture

### Thread Tree Architecture (Current)

The crawler uses a **supervisor-managed thread tree** architecture where multiple isolated DHT crawler units run concurrently:

```
Supervisor
├── Thread Tree 1 (Node ID A)
│   ├── Routing Table
│   ├── Infohash Queue
│   ├── Peers Queue
│   ├── Worker Threads (find_node, BEP51, get_peers, metadata)
│   └── UDP Socket
├── Thread Tree 2 (Node ID B)
│   └── [same structure]
└── Thread Tree N...

Shared Resources (supervisor-owned):
├── Batch Writer (SQLite transaction batching)
├── Bloom Filter (duplicate detection)
└── Shared Node Pool (global bootstrap)
```

**Key Files:**
- `src/supervisor.c` / `include/supervisor.h` - Manages multiple thread trees
- `src/thread_tree.c` / `include/thread_tree.h` - Isolated DHT crawler unit
- `src/shared_node_pool.c` / `include/shared_node_pool.h` - Global bootstrap node pool

**Thread Tree Lifecycle:**
1. **Bootstrap Phase** (`TREE_PHASE_BOOTSTRAP`): Samples nodes from shared pool, runs find_node workers to populate routing table
2. **BEP51 Phase** (`TREE_PHASE_BEP51`): Sends `sample_infohashes` queries to discover infohashes
3. **Get Peers Phase** (`TREE_PHASE_GET_PEERS`): Queries DHT for peer addresses for each infohash
4. **Metadata Phase** (`TREE_PHASE_METADATA`): Connects to peers via BitTorrent protocol to fetch torrent metadata
5. **Shutdown Phase** (`TREE_PHASE_SHUTTING_DOWN`): Graceful shutdown if metadata rate falls below threshold

The supervisor monitors tree performance and replaces underperforming trees automatically.

### Data Flow Pipeline

```
DHT Network → Thread Tree → Infohash Queue → Get Peers Workers → Peers Queue
                                                                      ↓
                                                            Metadata Workers
                                                                      ↓
                                                        Bloom Filter Check
                                                                      ↓
                                                              Batch Writer
                                                                      ↓
                                                               SQLite DB
```

### Threading Model

**Main Thread:**
- Runs libuv event loop
- Handles UDP socket I/O for all thread trees
- Manages HTTP API server (CivetWeb)
- Owns batch writer timer for periodic flushes

**Per Thread Tree:**
- Bootstrap workers: find_node queries to populate routing table
- Continuous find_node workers: maintain routing table
- BEP51 workers: `sample_infohashes` queries for infohash discovery
- Get peers workers: Query DHT for peer addresses
- Metadata workers: Connect to BitTorrent peers and fetch metadata
- Rate monitor thread: Monitors metadata fetch rate, triggers shutdown if too low
- Throttle monitor thread: Pauses/resumes discovery workers based on queue size

**Synchronization:**
- Routing tables use RCU (Read-Copy-Update) for lock-free reads
- Queues use mutexes and condition variables (MPSC pattern)
- Bloom filter is thread-safe with internal locking
- Batch writer uses mutex for thread-safe metadata submission

### Key Components

**DHT Implementation (wbpxre-dht):**
- Custom DHT library in `lib/wbpxre-dht/`
- Multi-threaded worker pools for concurrent DHT queries
- Supports BEP 5 (Kademlia), BEP 51 (`sample_infohashes`)
- RCU-based routing table for lock-free lookups
- Files: `wbpxre_dht.c`, `wbpxre_routing.c`, `wbpxre_protocol.c`, `wbpxre_worker.c`

**Metadata Fetcher:**
- Implements BitTorrent protocol (BEP 3, BEP 9, BEP 10)
- Connects to peers via TCP, performs extended handshake
- Uses `ut_metadata` extension to request metadata pieces
- Assembles multi-piece metadata, verifies SHA-1 hash
- Files: `src/metadata_fetcher.c`, `src/tree_metadata.c`

**Batch Writer:**
- Batches multiple torrent inserts into single SQLite transactions
- Provides 10-100x write throughput improvement
- Auto-flushes based on batch size (default: 1000) or time interval (default: 60s)
- Persists bloom filter to disk after each flush
- Files: `src/batch_writer.c`

**Bloom Filter:**
- Probabilistic duplicate detection (30M capacity, 0.1% error rate)
- Reduces database queries by ~90%
- Persisted to disk for resume across restarts
- Files: `src/bloom_filter.c`, `lib/libbloom/`

**Database:**
- SQLite with FTS5 full-text search
- Three-table normalized schema: `torrents`, `torrent_files`, `path_prefixes`
- Path deduplication: common directory prefixes stored once
- 32KB page size for better compression
- 60-70% size reduction vs naive schema (~9-12GB per million torrents)
- Files: `src/database.c`

**HTTP API:**
- CivetWeb embedded HTTP server (port 8080)
- Endpoints: `/` (search UI), `/search?q=<query>`, `/stats`, `/refresh`
- JSON statistics with DHT metrics, connection rates, database info
- Files: `src/http_api.c`

**Porn Filter:**
- Three-layer hybrid filtering: hash set keywords, regex patterns, heuristic scoring
- Configurable thresholds in `config.ini`
- Keyword file: `porn_filter_keywords.txt`
- Files: `src/porn_filter.c`

### Protocol Implementation Details

**BEP 51 (sample_infohashes):**
- Extension for efficiently discovering active infohashes
- Queries return random samples from a node's routing table
- Each thread tree sends queries at configurable interval (default: 5ms)
- Dramatically more efficient than traditional DHT scraping

**BitTorrent Metadata Exchange (BEP 9/10):**
1. TCP connect to peer
2. Send BitTorrent handshake with extension protocol flag
3. Receive handshake, parse reserved bytes for extension support
4. Send extended handshake with `ut_metadata` support
5. Receive extended handshake, get peer's `ut_metadata` extension ID and metadata size
6. Request metadata pieces (16KB each) sequentially
7. Assemble pieces, verify SHA-1 hash against infohash
8. Parse bencode metadata to extract torrent name, files, sizes

**Connection Management:**
- Global connection limit across all trees (default: 6000)
- Per-tree active connection tracking
- TCP connect timeout: 2s (default)
- Idle timeout: 8s (resets on data reception)
- Max lifetime: 15s (hard limit to prevent indefinite holds)

## Configuration

All settings in `config.ini`. Critical settings:

**Thread Tree Settings:**
```ini
use_thread_trees=1              # Enable thread tree architecture
num_trees=16                    # Number of concurrent thread trees
```

**Global Bootstrap Settings:**
```ini
global_bootstrap_target=5000         # Target nodes for shared pool
global_bootstrap_timeout_sec=120     # Bootstrap timeout
global_bootstrap_workers=50          # Bootstrap worker threads
per_tree_sample_size=1000            # Nodes each tree samples
```

**Worker Pool Settings (per tree):**
```ini
tree_find_node_workers=15       # Continuous routing table maintenance
tree_bep51_workers=50           # BEP51 sample_infohashes queries
tree_get_peers_workers=1000     # Get peers for infohashes
tree_metadata_workers=300       # Metadata fetch from peers
```

**Performance Tuning:**
```ini
max_concurrent_connections=6000      # Global TCP connection limit
batch_size=1000                      # Torrents per database batch
bloom_capacity=30000000              # 30M infohash bloom filter
tree_infohash_queue_capacity=20000   # Per-tree infohash queue size
```

**Rate-Based Tree Restart:**
```ini
min_metadata_rate=0.01               # Metadata/sec threshold for tree restart
tree_rate_check_interval_sec=60      # How often to check rate
tree_rate_grace_period_sec=30        # Wait time before restart
tree_min_lifetime_minutes=10         # Immunity period for new trees
tree_require_empty_queue=1           # Only restart if queue empty
```

## Adding New Features

### Adding DHT Statistics

1. Add counter to `dht_stats_t` in `include/dht_manager.h` or thread tree stats
2. Increment counter in appropriate callback/worker
3. Expose in `/stats` endpoint in `src/http_api.c` (search for `cJSON_AddNumberToObject`)

### Adding New Configuration Options

1. Add field to `crawler_config_t` in `include/config.h`
2. Add parser in `src/config.c` (`config_parse_ini`)
3. Add default value in `src/config.c` (`set_default_config`)
4. Document in `config.ini` with comment

### Extending HTTP API

1. Add route handler in `src/http_api.c` (`http_api_request_handler`)
2. Parse query parameters using `mg_get_var`
3. Return JSON using `cJSON` library or HTML string

### Modifying Thread Tree Lifecycle

1. Thread tree phases are in `include/thread_tree.h` (`tree_phase_t` enum)
2. Phase transitions happen in worker threads in `src/thread_tree.c`
3. Each phase has dedicated worker threads that spawn/join on phase entry/exit
4. Supervisor monitors trees in `src/supervisor.c` monitor thread

## Common Debugging Patterns

**Enable Debug Logging:**
```ini
# In config.ini
log_level=DEBUG  # or 0
```

**Check Thread Tree Health:**
- Access `/stats` endpoint to see active trees, metadata rates, connection counts
- Look for trees with low metadata rates (may indicate stuck workers)
- Check `active_connections` vs `max_concurrent_connections`

**Memory Leaks:**
```bash
make valgrind
# Check for "definitely lost" and "indirectly lost" blocks
# All libuv handles must be properly closed before uv_loop_close
```

**Data Race Detection:**
```bash
make tsan
# ThreadSanitizer will report data races
# Common issues: unprotected access to shared state, missing mutexes
```

**Worker Thread Hangs:**
- Check for deadlocks: mutexes acquired in different order
- Check for condition variable misuse: missing signals, spurious wakeups
- Enable debug logging to see worker progress
- Use `gdb` to attach and inspect thread backtraces: `thread apply all bt`

## Database Schema

**Normalized Three-Table Design:**

```sql
-- Path prefixes (deduplication)
CREATE TABLE path_prefixes (
    prefix_id INTEGER PRIMARY KEY,
    prefix TEXT NOT NULL UNIQUE
);

-- Torrents
CREATE TABLE torrents (
    info_hash BLOB(20) PRIMARY KEY,
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL,
    discovered_at INTEGER NOT NULL
);

-- Files
CREATE TABLE torrent_files (
    info_hash BLOB(20),
    file_index SMALLINT,        -- 50% smaller than INTEGER
    prefix_id INTEGER,          -- Reference to path_prefixes
    filename TEXT,              -- Just the filename, not full path
    file_size INTEGER,
    FOREIGN KEY(info_hash) REFERENCES torrents(info_hash),
    FOREIGN KEY(prefix_id) REFERENCES path_prefixes(prefix_id)
);

-- FTS5 full-text search (trigram tokenizer)
CREATE VIRTUAL TABLE torrents_fts USING fts5(
    info_hash UNINDEXED,
    name,
    tokenize='trigram'
);
```

**Path Normalization:**
- Directory paths extracted and deduplicated in `path_prefixes` table
- `torrent_files.filename` stores only the filename, not full path
- Reconstructed at query time: `path_prefixes.prefix || '/' || filename`
- Reduces redundancy for torrents with many files in same directories

## Vendored Libraries

- `lib/wbpxre-dht/` - Custom DHT implementation
- `lib/cJSON/` - JSON parsing/generation
- `lib/bencode-c/` - Bencode encoding/decoding (patched via `patches/bencode-cap-fix.patch`)
- `lib/civetweb/` - Embedded HTTP server
- `lib/libbloom/` - Bloom filter implementation
- `lib/uthash/` - Hash table macros for C

**Important:** `bencode-c` is patched during build. The patch is automatically applied by the Makefile target `patch-bencode`.

## Code Standards

- **C99 standard** with POSIX extensions (`-std=c99 -D_POSIX_C_SOURCE=200809L`)
- **Thread-safe:** All shared data protected by mutexes, atomics, or RCU
- **POSIX-compliant:** Use `localtime_r`, `strtok_r`, etc. (thread-safe variants)
- **Error handling:** Check all syscall return values, log errors before returning
- **Memory management:** Free all allocations, close all file descriptors/handles
- **Naming conventions:**
  - Functions: `module_action()` (e.g., `thread_tree_create`)
  - Structs: `module_name_t` (e.g., `thread_tree_t`)
  - Files: `module_name.c/h` (e.g., `thread_tree.c`)

## Performance Characteristics

- **Discovery Rate:** 500-2000 infohashes/minute (varies by DHT neighborhood and number of trees)
- **Metadata Fetch Rate:** 50-200 torrents/minute per tree (depends on peer availability)
- **Database Write Throughput:** 1000+ torrents/minute (with batch writing)
- **Memory Usage:** ~500MB-2GB (varies with routing table size, connection count, number of trees)
- **Disk Usage:** ~9-12GB per million torrents (optimized schema with path deduplication)

**Optimization Features:**
- Lock-free MPSC queues for cross-thread communication
- RCU-based routing table for lock-free DHT lookups
- Async I/O via libuv event loop
- Connection pooling and reuse (within metadata workers)
- Bloom filter reduces database queries by 90%
- Batch writer provides 10-100x write speedup
- Multiple thread trees explore different DHT neighborhoods concurrently
