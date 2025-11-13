# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A high-performance BitTorrent DHT crawler written in C that discovers and catalogs torrents from the BitTorrent DHT network. The crawler passively monitors DHT traffic and actively fetches torrent metadata from peers. Built with a focus on concurrency, throughput optimization, and efficient resource management.

## Build System

**Build the project:**
```bash
make
```

**Clean build artifacts:**
```bash
make clean
```

**Build with debug symbols:**
```bash
make debug
```

**Build with AddressSanitizer (for memory debugging):**
```bash
make asan
```

**Run with Valgrind:**
```bash
make valgrind
```

**Run the crawler:**
```bash
./dht_crawler
```

**Dependencies required:**
- `libuv-dev` - Async I/O event loop
- `libsqlite3-dev` - Database storage
- `libssl-dev` - OpenSSL for SHA-1 hashing
- `build-essential` - GCC compiler toolchain
- `liburcu-dev` (Debian/Ubuntu) or `userspace-rcu` (Arch) - Read-Copy-Update synchronization

## Code Architecture

### High-Level Data Flow

```
DHT Network → DHT Manager → Infohash Queue → Metadata Fetcher → Batch Writer → SQLite Database
                   ↓              ↑                    ↓
              Peer Store ←────────┴──────────────> Bloom Filter
```

1. **DHT Manager** (`dht_manager.c`) receives infohashes from DHT network via `wbpxre-dht`
2. Infohashes are queued in **Infohash Queue** (`infohash_queue.c`)
3. **Metadata Fetcher** (`metadata_fetcher.c`) pulls infohashes, queries **Peer Store** for peers
4. Worker threads connect to peers via BitTorrent protocol (BEP 9/10) to fetch metadata
5. Successfully fetched metadata goes to **Batch Writer** (`batch_writer.c`)
6. **Bloom Filter** (`bloom_filter.c`) prevents duplicate processing (90% reduction in DB queries)
7. Metadata is flushed to **SQLite Database** in batched transactions (10-100x write speedup)

### Core Components

#### DHT Layer (`lib/wbpxre-dht/`)
Custom DHT implementation based on BitTorrent BEP 5 protocol with multi-pipeline concurrent architecture:
- **wbpxre_dht.c/h**: Core DHT engine with RCU-based lock-free routing table
- **wbpxre_routing.c/h**: Kademlia routing table management (k-buckets, node expiration)
- **wbpxre_protocol.c/h**: DHT message encoding/decoding (bencode format)
- **wbpxre_worker.c/h**: Multi-threaded worker pools for parallel query processing
- Supports BEP 51 `sample_infohashes` extension for efficient infohash discovery
- Implements node ID rotation to maximize DHT neighborhood coverage

#### DHT Manager (`src/dht_manager.c`, `include/dht_manager.h`)
Orchestrates DHT operations and manages discovered data:
- Initializes and manages `wbpxre-dht` instance
- Handles DHT events via `wbpxre_callback_wrapper()` callback
- Manages **Peer Store** for discovered peers (infohash → peer IP:port mappings)
- Implements periodic node ID rotation to explore different DHT neighborhoods
- Tracks comprehensive statistics (packets, queries, discoveries, BEP 51 samples)
- Manages async pruning of routing table nodes via worker pool
- Coordinates with HTTP API for `/refresh` endpoint (priority peer queries)

#### Metadata Fetcher (`src/metadata_fetcher.c`, `include/metadata_fetcher.h`)
Concurrent BitTorrent peer protocol client (BEP 9/10):
- **Worker Pool**: Configurable thread pool (default 50-100 workers) processes infohashes
- **Connection Management**: libuv-based async TCP connections with timeout tracking
- **Protocol Implementation**:
  - BitTorrent handshake with extension support (BEP 10)
  - Extended handshake negotiation (`ut_metadata` extension)
  - BEP 9 metadata piece requests/assembly
- **Sequential Peer Fallback**: Tries all available peers until success or exhaustion
- **Infohash Attempt Tracking**: Aggregates per-connection failures to classify retriable vs permanent failures
- **Activity-Based Timeouts**: Connection timeout resets on data reception (allows slow transfers)
- **Failure Classification**: Routes failed attempts to retry tracker or discard queue
- **Connection Request Queue**: Lock-free MPSC queue for cross-thread coordination

#### Batch Writer (`src/batch_writer.c`, `include/batch_writer.h`)
High-throughput database writer with transaction batching:
- Batches multiple torrent inserts into single SQLite transactions (10-100x speedup)
- Auto-flush triggers: configurable batch size (default 1000) or time interval (default 60s)
- Timer-based periodic flush via libuv event loop
- Thread-safe: mutex-protected for concurrent worker access
- Tracks rolling hourly statistics (per-minute buckets for last 60 minutes)
- Integrates with Bloom Filter: persists filter after successful writes

#### Infohash Queue (`src/infohash_queue.c`, `include/infohash_queue.h`)
Thread-safe queue for discovered infohashes:
- Fixed-size circular buffer with mutex protection
- Configurable capacity (default 10,000)
- Supports non-blocking `try_push` and blocking `pop` operations

#### Peer Store (`src/peer_store.c`, `include/peer_store.h`)
Hash table mapping infohashes to discovered peer addresses:
- Uses `uthash` for O(1) lookups
- Thread-safe with mutex protection
- Stores peer addresses as `sockaddr_storage` (IPv4/IPv6 compatible)
- Supports adding peers and fetching all peers for an infohash
- Eviction policy: removes entries after peer fetch to save memory

#### Bloom Filter (`src/bloom_filter.c`, `include/bloom_filter.h`)
Probabilistic duplicate detection:
- Uses `libbloom` library for space-efficient membership testing
- Configurable capacity (default 30M elements) and error rate (0.001)
- Persistent: saves to disk (`data/bloom.dat`) for restart continuity
- Reduces database queries by ~90% by filtering seen infohashes

#### Database (`src/database.c`, `include/database.h`)
SQLite storage layer:
- **Schema**: `torrents` table (info_hash, name, size, timestamps) and `torrent_files` table (file paths/sizes)
- **Indexes**: Optimized for search queries (added_timestamp, total_peers, name FTS)
- **Search**: Full-text search via `LIKE` queries on torrent/file names
- **API**: Insert, search, statistics (total torrents, total size)

#### HTTP API (`src/http_api.c`, `include/http_api.h`)
Web interface powered by CivetWeb:
- **Endpoints**:
  - `GET /` - HTML landing page with search form
  - `GET /search?q=...` - Search torrents by name, returns HTML results
  - `GET /stats` - JSON statistics (DHT, metadata fetcher, database)
  - `POST /refresh?info_hash=...` - Priority peer query for specific infohash
- **Port**: Configurable (default 8080)
- **Threading**: 2 CivetWeb threads handle requests

#### Worker Pool (`src/worker_pool.c`, `include/worker_pool.h`)
Generic thread pool for concurrent task processing:
- Used by Metadata Fetcher (50-100 workers) and DHT Manager pruning (10 workers)
- Lock-free MPSC queue for task submission
- Blocking and non-blocking submit modes
- Graceful shutdown: waits for in-flight tasks

#### Configuration (`src/config.c`, `include/config.h`)
Loads `config.ini` file with settings:
- DHT parameters (port, BEP 51 settings, node rotation)
- Worker pool sizes (metadata workers, DHT workers)
- Connection limits (concurrent connections, timeouts)
- Batch writer settings (batch size, flush interval)
- Bloom filter configuration (capacity, error rate, persistence)
- Peer retry settings (max attempts, delay)

### Threading Model

**Main Thread (libuv event loop):**
- DHT packet processing (via `wbpxre-dht` callbacks)
- TCP connection management (metadata fetcher)
- Timer callbacks (batch flush, node rotation, pruning)
- HTTP API request handling (CivetWeb threads)

**Worker Threads:**
- **Metadata Workers** (50-100 threads): Process infohash queue, initiate peer connections
- **DHT Workers** (4 pools via `wbpxre-dht`):
  - Ping workers (10): Handle `ping` queries
  - Find_node workers (20): Handle `find_node` queries
  - Sample_infohashes workers (50): Handle BEP 51 queries
  - Get_peers workers (100): Handle `get_peers` queries
- **Pruning Workers** (10 threads): Asynchronous routing table node pruning

**Synchronization:**
- **Mutexes**: Protect shared data structures (peer store, infohash queue, batch writer)
- **Condition Variables**: Worker threads wait for tasks
- **RCU (Read-Copy-Update)**: Lock-free routing table reads in `wbpxre-dht` (via `liburcu`)
- **Atomics**: Lock-free counters (statistics, pruning coordination)
- **libuv Async Handles**: Cross-thread communication (worker → event loop)

### Key Algorithms

#### Node ID Rotation (Hot Rotation)
To maximize DHT coverage, node ID rotates periodically:
1. **STABLE** state: Normal operation with current node ID
2. **ANNOUNCING** phase (30s): Announces new ID to network while keeping old routing table
3. **TRANSITIONING** phase (30s): Uses new ID, drains sample queue
4. Returns to **STABLE**: Full transition complete, continue with new ID

#### Sequential Peer Fallback
When fetching metadata, try all peers sequentially until success:
1. Fetch up to N peers from peer store
2. Start M concurrent connections (configurable, default 5)
3. On connection failure/timeout, immediately try next peer in queue
4. Continue until metadata fetched OR all peers exhausted
5. Classify failure: retriable (no peers yet) vs permanent (all peers failed)

#### Async Node Pruning
Routing table size management without blocking event loop:
1. Timer triggers pruning (every 2 minutes)
2. Collect candidate node IDs to prune (select old/stale nodes)
3. Submit pruning tasks to worker pool (10 threads)
4. Workers call `wbpxre_dht_drop_node()` in parallel
5. Track completion via atomic counter
6. Periodically rebuild hash table (every 30 pruning cycles) for compaction

## Configuration

Primary configuration via `config.ini` in project root:
- DHT settings (port, BEP 51 query rate, node rotation interval)
- Worker pool sizes (metadata_workers, wbpxre workers)
- Connection limits (max_concurrent_connections, connection_timeout_sec)
- Batch writer (batch_size, flush_interval)
- Bloom filter (bloom_capacity, bloom_error_rate)
- Logging (log_level: DEBUG=0, INFO=1, WARN=2, ERROR=3)

## Common Patterns

### Adding New Statistics
1. Add field to `dht_stats_t` in `include/dht_manager.h`
2. Increment counter in relevant callback/function in `src/dht_manager.c`
3. Print statistic in `dht_manager_print_stats()`
4. Expose via HTTP `/stats` endpoint in `src/http_api.c` (optional)

### Adding New DHT Query Type
1. Implement query function in `lib/wbpxre-dht/wbpxre_protocol.c`
2. Add worker pool in `lib/wbpxre-dht/wbpxre_dht.c` (if concurrent processing needed)
3. Handle response in `wbpxre_callback_wrapper()` in `src/dht_manager.c`
4. Update statistics in `dht_stats_t`

### Debugging Connection Issues
- Enable debug logging: set `log_level=DEBUG` in `config.ini` or `log_level=0`
- Build with `make asan` to detect memory errors
- Use `make valgrind` to check for leaks
- Check connection statistics in `/stats` endpoint
- Look for "close_reason" logs in metadata fetcher (timeout, handshake failure, etc.)

## Important Constraints

- **C99 Standard**: Code uses C99 features (designated initializers, `//` comments)
- **POSIX Compliance**: Requires `_POSIX_C_SOURCE=200809L` for POSIX APIs
- **No C++**: Pure C codebase, C++ in vendored libraries only (civetweb)
- **Thread Safety**: All shared data structures must be protected (mutex/atomic/RCU)
- **libuv Integration**: Long-running operations must be async (use worker threads + uv_async)
- **Memory Management**: Manual malloc/free, check for leaks with Valgrind
- **Error Handling**: Functions return -1 on error, use `log_msg()` for diagnostics

## File Naming Conventions

- **Source**: `src/*.c` (implementation files)
- **Headers**: `include/*.h` (public headers), `src/*.h` (private headers rare)
- **Libraries**: `lib/*/` (vendored dependencies: cJSON, bencode-c, civetweb, libbloom, uthash, wbpxre-dht)
- **Build Output**: `build/*.o` (object files), `dht_crawler` (executable)
- **Data**: `data/torrents.db` (SQLite), `data/bloom.dat` (Bloom filter)

## Testing

No formal test suite currently. Testing strategy:
1. **Build Verification**: `make` with `-Wall -Wextra` catches warnings
2. **Manual Testing**: Run `./dht_crawler` and monitor logs/stats
3. **Memory Safety**: `make asan` and `make valgrind`
4. **HTTP API**: Test endpoints with `curl` or browser
5. **Statistics Monitoring**: Check `/stats` endpoint or logs for anomalies

## Performance Characteristics

**Throughput Optimizations:**
- Worker pool: 10-20x metadata fetch rate (50-100 concurrent workers)
- Batch writer: 10-100x database write speed (transaction batching)
- Bloom filter: 90% reduction in database queries
- Lock-free queues: MPSC queues avoid contention in hot paths
- RCU routing table: Lock-free reads for DHT lookups

**Resource Limits:**
- Max concurrent TCP connections: Configurable (default 2000-5000)
- Routing table nodes: Configurable max (default 60,000 after pruning)
- Infohash queue size: 10,000 infohashes
- Bloom filter capacity: 30 million infohashes (default)

## Dependencies

**System Libraries:**
- `libuv`: Async I/O event loop
- `libsqlite3`: Embedded database
- `libssl/libcrypto`: SHA-1 hashing (OpenSSL)
- `liburcu`: Read-Copy-Update synchronization
- `libpthread`: POSIX threads
- `libdl`: Dynamic linking
- `libm`: Math library

**Vendored Libraries (in `lib/`):**
- `cJSON`: JSON parsing (HTTP API responses)
- `bencode-c`: Bencode encoding/decoding (DHT/BitTorrent protocol)
- `civetweb`: Embedded HTTP server
- `libbloom`: Bloom filter implementation
- `uthash`: Hash table macros (peer store)
- `wbpxre-dht`: Custom DHT implementation (core component)
