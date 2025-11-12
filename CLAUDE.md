# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a BitTorrent DHT crawler written in C that discovers torrent metadata by:
1. Participating in the BitTorrent DHT network to discover info_hashes
2. Fetching metadata from peers using the BitTorrent Extension Protocol (BEP 9/10)
3. Storing torrent metadata in an SQLite database
4. Providing an HTTP API for querying discovered torrents

The crawler uses the `wbpxre-dht` library (a modern DHT implementation supporting BEP 51 for sample_infohashes) for DHT operations and libuv for async I/O.

## Build Commands

```bash
# Build the project
make

# Clean build artifacts
make clean

# Debug build with symbols
make debug

# Build with AddressSanitizer and UndefinedBehaviorSanitizer
make asan

# Run with Valgrind
make valgrind

# Run the crawler
./dht_crawler

# Or use make run
make run
```

## Dependencies

Required system packages:
- libuv-dev
- libsqlite3-dev
- libssl-dev
- build-essential
- liburcu (userspace-rcu on Arch, liburcu-dev on Debian/Ubuntu)

The project uses git submodules in `lib/` for third-party libraries:
- `wbpxre-dht`: Custom DHT implementation with BEP 51 support
- `bencode-c`: Bencode parsing
- `cJSON`: JSON handling for HTTP API
- `civetweb`: Embedded HTTP server
- `libbloom`: Bloom filter for duplicate detection

## Architecture

### Core Components

**Main Event Loop** (`src/main.c`):
- Initializes all subsystems in order: queue → bloom filter → database → DHT manager → metadata fetcher → HTTP API
- Uses libuv's default event loop (`uv_default_loop()`)
- Handles graceful shutdown on SIGINT/SIGTERM

**DHT Manager** (`src/dht_manager.c`, `include/dht_manager.h`):
- Wraps `wbpxre-dht` library and manages DHT network participation
- Discovers info_hashes via BEP 51 `sample_infohashes` queries
- Maintains a shadow routing table with BEP 51 metadata (node capabilities, sample intervals)
- Stores discovered peers in a peer_store for metadata fetching
- Configuration loaded from `config.ini` (worker counts, query rates, intervals)

**Metadata Fetcher** (`src/metadata_fetcher.c`, `include/metadata_fetcher.h`):
- Multi-threaded worker pool for concurrent metadata fetching (configurable: 10-100 workers)
- Implements BitTorrent Extension Protocol (BEP 9/10) for ut_metadata
- Connection lifecycle: TCP connect → handshake → extended handshake → request metadata pieces → reassemble → verify SHA-1
- Retry logic with exponential backoff for failed fetches
- Tracks per-infohash attempt statistics to make smart retry decisions
- Uses connection_request_queue for cross-thread communication with libuv event loop

**Database** (`src/database.c`, `include/database.h`):
- SQLite backend for storing torrent metadata (name, files, size, piece info)
- Prepared statements for efficient inserts
- Batch writer support for high-throughput writes (10-100x speedup)
- Updates bloom filter ONLY after successful writes (critical for correctness)

**Infohash Queue** (`src/infohash_queue.c`, `include/infohash_queue.h`):
- Thread-safe bounded queue (default: 10,000 capacity)
- Pure queue operations only - no deduplication logic
- Deduplication happens upstream in DHT manager before get_peers queries

**Worker Pool** (`src/worker_pool.c`, `include/worker_pool.h`):
- Generic concurrent task processing framework
- Used by metadata fetcher for 10-20x throughput improvement
- Configurable workers and queue capacity

**Bloom Filter** (`src/bloom_filter.c`, `include/bloom_filter.h`):
- Thread-safe probabilistic duplicate detection (capacity: 10M, error rate: 0.1%)
- Persisted to `data/bloom.dat` on shutdown and loaded on startup
- **Critical invariant**: Only updated AFTER successful database writes to prevent data loss

**HTTP API** (`src/http_api.c`, `include/http_api.h`):
- CivetWeb-based HTTP server (default port: 8080)
- Provides REST endpoints for querying torrents and statistics

### Threading Model

The application uses multiple threading models:

1. **Main thread**: Runs libuv event loop for DHT network I/O
2. **wbpxre-dht threads**: Multiple worker pipelines (ping, find_node, sample_infohashes, get_peers)
3. **Metadata fetcher worker threads**: Pool of workers that attempt TCP connections to peers
4. **Metadata fetcher feeder thread**: Dequeues info_hashes and submits to worker pool
5. **Retry thread**: Handles exponential backoff retries for failed metadata fetches

Cross-thread communication uses:
- `uv_async_t` for triggering libuv callbacks from worker threads
- `pthread_mutex_t` and `pthread_cond_t` for work queues
- `connection_request_queue_t` for requesting TCP connections from worker threads

## Configuration

All runtime configuration is in `config.ini`:
- DHT settings: port, worker counts, query rates
- Bloom filter: capacity, error rate, persistence
- Metadata fetcher: worker count, connection limits, timeouts, retry policy
- Database: batch writes, flush intervals
- HTTP API: port

Log level can be set in config.ini (DEBUG, INFO, WARN, ERROR).

## Key Invariants and Gotchas

1. **Bloom filter deduplication**: Bloom filter + database check happens ONCE in `dht_manager_query_peers()` (src/dht_manager.c:944) BEFORE info_hashes enter the get_peers queue. This prevents duplicate info_hashes from saturating the get_peers pipeline. The bloom filter is updated by the database module AFTER successful writes to prevent data loss from failed metadata fetches. Priority queries (HTTP /refresh) bypass the bloom filter check.

2. **wbpxre-dht node management**: The wbpxre-dht library handles node verification internally. Do NOT manually ping dubious nodes as this interferes with the library's state machine.

3. **libuv handle cleanup**: All libuv handles must be closed with `uv_close()` before calling `uv_loop_close()`. Use close trackers to ensure safe shutdown.

4. **Peer connection state machine**: Each TCP connection goes through states: CONNECTING → HANDSHAKING → EXTENDED_HANDSHAKE → REQUESTING_METADATA → RECEIVING_METADATA → COMPLETE/FAILED. Failure at any stage should clean up the connection properly.

5. **BitTorrent protocol**: The crawler implements BEP 3 (handshake), BEP 9 (ut_metadata), BEP 10 (extension protocol), and BEP 51 (sample_infohashes for DHT).

6. **Metadata piece reassembly**: Metadata is split into 16 KiB pieces. Track received pieces with a bitmap and verify SHA-1 hash after reassembly.

## Data Flow

```
DHT Network
    ↓ (BEP 51 sample_infohashes)
wbpxre-dht library
    ↓ (callback with info_hashes)
DHT Manager (bloom filter dedup in dht_manager_query_peers)
    ↓ (duplicates filtered before get_peers)
get_peers queue → get_peers workers
    ↓ (peers discovered)
Infohash Queue
    ↓
Metadata Fetcher Worker Pool
    ↓ (BEP 9/10 metadata fetch from peers)
Batch Writer → Database (SQLite)
    ↓ (updates bloom filter after successful write)
HTTP API (query interface)
```

## Testing and Debugging

- Use `make debug` for debug symbols
- Use `make asan` to detect memory errors and undefined behavior
- Use `make valgrind` for leak checking
- Set `log_level=DEBUG` in config.ini for verbose output
- HTTP API at `http://localhost:8080/` provides runtime statistics
- Database is stored at `data/torrents.db` (can query with sqlite3)

## Code Style

- C99 standard with POSIX extensions
- Header guards use `#ifndef FILENAME_H` / `#define FILENAME_H` / `#endif`
- Struct typedefs end with `_t` suffix
- Error handling: return 0 for success, -1 or non-zero for errors
- Thread-safety: document thread-safety guarantees in header comments
- Logging: use `log_msg()` with appropriate log levels
