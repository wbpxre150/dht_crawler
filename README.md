# DHT Crawler

A high-performance BitTorrent DHT crawler written in C that discovers and catalogs torrents from the BitTorrent DHT network.

## Features

- **Passive DHT Monitoring**: Listens to DHT network traffic to discover active torrents
- **Active Metadata Fetching**: Connects to BitTorrent peers to retrieve torrent metadata (BEP 9/10)
- **BEP 51 Support**: Implements `sample_infohashes` extension for efficient infohash discovery
- **High Throughput**:
  - Worker pool: 10-20x metadata fetch rate with 50-100 concurrent workers
  - Batch writer: 10-100x database write speed via transaction batching
  - Bloom filter: 90% reduction in duplicate processing
- **Node ID Rotation**: Periodically rotates DHT node ID to explore different network neighborhoods
- **Web Interface**: Built-in HTTP API for searching and statistics (port 8080)
- **Persistent Storage**: SQLite database with full-text search and optimized schema (60-70% size reduction)

## Architecture

```
DHT Network → DHT Manager → Infohash Queue → Metadata Fetcher → Batch Writer → SQLite DB
                   ↓              ↑                    ↓
              Peer Store ←────────┴──────────────> Bloom Filter
```

The crawler uses a multi-stage pipeline:
1. Custom DHT implementation (`wbpxre-dht`) discovers infohashes from the network
2. Discovered peers are stored in an in-memory peer store
3. Worker threads fetch metadata from peers via BitTorrent protocol
4. Bloom filter prevents duplicate processing
5. Metadata is batch-written to SQLite for high throughput

## Requirements

### System Dependencies

**Debian/Ubuntu:**
```bash
sudo apt-get install build-essential libuv1-dev libsqlite3-dev libssl-dev liburcu-dev
```

**Arch Linux:**
```bash
sudo pacman -S base-devel libuv sqlite openssl userspace-rcu
```

**macOS (Homebrew):**
```bash
brew install libuv sqlite openssl liburcu
```

### Required Libraries
- `libuv` - Async I/O event loop
- `libsqlite3` - Embedded database
- `libssl/libcrypto` - SHA-1 hashing (OpenSSL)
- `liburcu` - Read-Copy-Update synchronization
- `libpthread` - POSIX threads

## Building

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

**Build with AddressSanitizer (for development):**
```bash
make asan
```

**Run with Valgrind:**
```bash
make valgrind
```

## Running

**Start the crawler:**
```bash
./dht_crawler
```

The crawler will:
1. Bootstrap into the DHT network using well-known nodes
2. Start discovering infohashes from DHT traffic
3. Fetch metadata from peers in parallel
4. Store results in `data/torrents.db`
5. Serve HTTP API on port 8080

**Access the web interface:**
```
http://localhost:8080
```

## Configuration

Configuration is loaded from `config.ini` in the project root. Key settings:

```ini
# DHT Settings
dht_port=6881

# HTTP API Settings
http_port=8080

# Database Settings
db_path=data/torrents.db

# Logging (DEBUG=0, INFO=1, WARN=2, ERROR=3)
log_level=INFO

# Worker Pool Settings
metadata_workers=50              # Concurrent metadata fetcher threads
scaling_factor=10                # DHT worker pool scaling

# Connection Limits
concurrent_peers_per_torrent=5   # Max connections per infohash
max_concurrent_connections=5000  # Global connection limit
connection_timeout_sec=30        # Idle timeout (resets on activity)

# Batch Writer Settings
batch_writes_enabled=1
batch_size=1000                  # Torrents per batch
flush_interval=60                # Auto-flush interval (seconds)

# Bloom Filter Settings
bloom_enabled=1
bloom_capacity=30000000          # 30 million infohashes
bloom_error_rate=0.001           # 0.1% false positive rate
bloom_persist=1                  # Save to disk on shutdown
bloom_path=data/bloom.dat

# Node ID Rotation
node_rotation_enabled=1
node_rotation_interval_sec=300   # Rotate every 5 minutes
```

See `config.ini` for all available options with detailed comments.

## HTTP API Endpoints

### `GET /`
Landing page with search interface.

### `GET /search?q=<query>`
Search torrents by name. Returns HTML results with torrent details and file listings.

**Example:**
```bash
curl "http://localhost:8080/search?q=ubuntu"
```

### `GET /stats`
JSON statistics including DHT metrics, metadata fetcher status, and database info.

**Example:**
```bash
curl http://localhost:8080/stats | jq
```

**Response:**
```json
{
  "dht": {
    "packets_received": 1234567,
    "infohashes_discovered": 45678,
    "nodes_in_routing_table": 8234,
    ...
  },
  "metadata_fetcher": {
    "total_fetched": 12345,
    "active_connections": 234,
    "connection_success_rate": 23.45,
    ...
  },
  "database": {
    "total_torrents": 98765,
    "total_size_bytes": 123456789012
  }
}
```

### `POST /refresh?info_hash=<hex>`
Trigger priority peer query for a specific infohash. Useful for refreshing peer lists.

**Example:**
```bash
curl -X POST "http://localhost:8080/refresh?info_hash=abcdef1234567890abcdef1234567890abcdef12"
```

## Project Structure

```
dht_crawler/
├── src/                    # Source files
│   ├── main.c             # Entry point
│   ├── dht_manager.c      # DHT orchestration
│   ├── metadata_fetcher.c # BitTorrent peer client
│   ├── batch_writer.c     # Batched database writes
│   ├── database.c         # SQLite operations
│   ├── http_api.c         # Web interface
│   ├── peer_store.c       # Peer address storage
│   ├── infohash_queue.c   # Infohash queuing
│   ├── bloom_filter.c     # Duplicate detection
│   ├── worker_pool.c      # Thread pool
│   └── config.c           # Configuration loading
├── include/               # Header files
├── lib/                   # Vendored libraries
│   ├── wbpxre-dht/       # Custom DHT implementation
│   ├── cJSON/            # JSON parsing
│   ├── bencode-c/        # Bencode encoding
│   ├── civetweb/         # HTTP server
│   ├── libbloom/         # Bloom filter
│   └── uthash/           # Hash table macros
├── build/                 # Build artifacts (generated)
├── data/                  # Runtime data (generated)
│   ├── torrents.db       # SQLite database
│   └── bloom.dat         # Bloom filter persistence
├── config.ini            # Configuration file
├── Makefile              # Build system
└── README.md             # This file
```

## Performance

**Typical Performance Metrics:**
- **Discovery Rate**: 500-2000 infohashes/minute (varies by DHT neighborhood)
- **Metadata Fetch Rate**: 50-200 torrents/minute (depends on peer availability)
- **Database Write Throughput**: 1000+ torrents/minute (with batch writing)
- **Memory Usage**: ~500MB-2GB (varies with routing table size and connection count)
- **Disk Usage**: ~9-12GB per million torrents (optimized schema)

**Optimization Features:**
- Lock-free MPSC queues for cross-thread communication
- RCU-based routing table for lock-free DHT lookups
- Async I/O via libuv event loop
- Connection pooling and reuse
- Bloom filter reduces database queries by 90%
- **Database schema optimizations (60-70% size reduction)**:
  - Path normalization with prefix deduplication
  - Removed redundant fields (piece_length, num_pieces, last_seen)
  - SMALLINT file_index (50% smaller than INTEGER)
  - 32KB page size for better compression
  - Incremental auto_vacuum for space management

## Development

**Code Style:**
- C99 standard
- POSIX-compliant
- Thread-safe: all shared data protected by mutexes/atomics/RCU

**Debugging:**
```bash
# Enable debug logging
# Edit config.ini: log_level=DEBUG (or 0)

# Build with sanitizers
make asan
./dht_crawler

# Check for memory leaks
make valgrind
```

**Adding Features:**
- See `CLAUDE.md` for detailed architecture documentation
- Common patterns for extending DHT queries, statistics, etc.

## Troubleshooting

**Crawler not discovering torrents:**
- Check firewall allows UDP on port 6881
- Wait 5-10 minutes for DHT bootstrap to complete
- Enable debug logging to see DHT traffic
- Check `/stats` endpoint for `packets_received` counter

**High memory usage:**
- Reduce `max_routing_table_nodes` in config.ini
- Enable `async_pruning_enabled` (default: on)
- Reduce `max_concurrent_connections`

**Database growing too large:**
- Optimized schema provides 60-70% size reduction (9-12GB per million torrents)
- Incremental auto_vacuum automatically reclaims space
- Consider implementing retention policies for old torrents
- Use `PRAGMA incremental_vacuum;` to manually reclaim space if needed

**Low metadata fetch rate:**
- Increase `metadata_workers` (50-100 recommended)
- Increase `max_concurrent_connections` (2000-5000)
- Check connection statistics in `/stats` for timeout rates
- Ensure sufficient network bandwidth

## License

MIT License - See LICENSE file for details.

## Technical Details

**DHT Implementation:**
- Based on Kademlia protocol (BEP 5)
- Supports BEP 51 `sample_infohashes` extension
- Node ID rotation for neighborhood exploration
- Multi-threaded worker pools for query processing

**BitTorrent Protocol:**
- BEP 3: BitTorrent protocol specification
- BEP 9: Extension for Peers to Send Metadata Files (ut_metadata)
- BEP 10: Extension Protocol (extended handshake)

**Database Schema:**
- Three-table normalized design: `torrents`, `torrent_files`, `path_prefixes`
- Path deduplication: Common directory prefixes stored once, referenced by ID
- FTS5 full-text search with trigram tokenization for fuzzy matching
- 32KB page size for improved compression ratio
- Expected storage: 135-180GB for 15M torrents (vs 450-600GB with old schema)

**Threading Model:**
- Main thread: libuv event loop (DHT, TCP, timers)
- Worker threads: Metadata fetching, DHT query processing, node pruning
- Synchronization: Mutexes, condition variables, RCU, atomics

## Acknowledgments

- `wbpxre-dht`: Custom DHT implementation inspired by bitmagnet's architecture
- `libuv`: Cross-platform async I/O library
- `cJSON`, `bencode-c`, `civetweb`, `libbloom`, `uthash`: Excellent open-source libraries

## Contributing

Contributions welcome! Areas for improvement:
- IPv6 support
- Torrent quality metrics (seeders, leechers tracking)
- Advanced search features (filters, sorting)
- Database cleanup/retention policies
- Metrics export (Prometheus, etc.)
- Docker containerization
