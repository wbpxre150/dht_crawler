# DHT Crawler

A high-performance BitTorrent DHT crawler written in C that discovers and indexes torrent metadata from the BitTorrent network.

## Features

- **Automatic Torrent Discovery**: Crawls the BitTorrent DHT network to discover info_hashes using BEP 51 (sample_infohashes)
- **Metadata Fetching**: Retrieves torrent metadata from peers using BitTorrent Extension Protocol (BEP 9/10)
- **High Performance**:
  - Multi-threaded metadata fetching with configurable worker pools (10-100 workers)
  - Batched database writes for 10-100x write performance improvement
  - Bloom filter for duplicate detection (90% reduction in database queries)
  - Concurrent DHT operations with multiple worker pipelines
- **Full-Text Search**: SQLite FTS5 for searching torrents by name and file paths
- **HTTP API**: Query and search discovered torrents via REST endpoints
- **Persistent Storage**: SQLite database with automatic schema management
- **Production Ready**: Memory leak-free, tested with Valgrind and AddressSanitizer

## Architecture

The crawler uses a multi-threaded pipeline architecture:

```
DHT Network (UDP)
    ↓ sample_infohashes queries (BEP 51)
wbpxre-dht Library (multi-threaded DHT implementation)
    ↓ discovered info_hashes
Infohash Queue (thread-safe, bloom filter dedup)
    ↓
Metadata Fetcher (30+ worker threads)
    ↓ TCP connections to peers (BEP 9/10)
Batch Writer (batched transactions)
    ↓
SQLite Database (FTS5 search)
    ↑
HTTP API (CivetWeb)
```

## Quick Start

### Prerequisites

Install required dependencies:

**Debian/Ubuntu:**
```bash
sudo apt-get install build-essential libuv1-dev libsqlite3-dev libssl-dev liburcu-dev
```

**Arch Linux:**
```bash
sudo pacman -S base-devel libuv sqlite openssl userspace-rcu
```

**macOS:**
```bash
brew install libuv sqlite openssl userspace-rcu
```

### Building

```bash
# Clone the repository with submodules
git clone --recursive https://github.com/yourusername/dht_crawler.git
cd dht_crawler

# Build the project
make

# Run the crawler
./dht_crawler
```

The crawler will:
1. Start listening on UDP port 6881 for DHT traffic
2. Bootstrap from known DHT nodes
3. Begin discovering torrents
4. Store metadata in `data/torrents.db`
5. Start HTTP API on port 8080

## Configuration

All settings are in `config.ini`. Key configuration options:

```ini
# DHT Settings
dht_port=6881                              # DHT UDP port

# Metadata Fetcher
metadata_workers=30                        # Worker threads for fetching metadata
max_concurrent_connections=2000            # Max concurrent TCP connections
concurrent_peers_per_torrent=5             # Peers to try per torrent
connection_timeout_sec=4                   # Connection timeout

# Database
batch_writes_enabled=1                     # Enable batched writes
batch_size=1000                            # Torrents per batch
flush_interval=60                          # Auto-flush interval (seconds)

# Bloom Filter
bloom_enabled=1                            # Enable duplicate detection
bloom_capacity=30000000                    # Expected unique torrents
bloom_error_rate=0.001                     # False positive rate (0.1%)
bloom_persist=1                            # Save/load from disk

# wbpxre-dht Workers
wbpxre_ping_workers=20                     # Ping worker threads
wbpxre_find_node_workers=20                # find_node worker threads
wbpxre_sample_infohashes_workers=20        # sample_infohashes workers
wbpxre_get_peers_workers=777               # get_peers worker threads

# Node ID Rotation
node_rotation_enabled=1                    # Rotate node ID to explore keyspace
node_rotation_interval_sec=60              # Rotation interval

# Logging
log_level=INFO                             # DEBUG, INFO, WARN, ERROR
```

## HTTP API

The HTTP API runs on port 8080 (configurable in `config.ini`).

### Endpoints

#### `GET /`
Landing page with crawler statistics and search interface.

#### `GET /search?q=<query>`
Search torrents by name or file path.

**Example:**
```bash
curl "http://localhost:8080/search?q=ubuntu"
```

**Response:** HTML page with search results including torrent names, sizes, file counts, and magnet links.

#### `GET /stats`
Get crawler statistics in JSON format.

**Example:**
```bash
curl http://localhost:8080/stats
```

**Response:**
```json
{
  "dht": {
    "good_nodes": 6543,
    "routing_table_nodes": 8234,
    "infohashes_discovered": 152341,
    "packets_sent": 523421,
    "packets_received": 498234,
    "uptime_hours": 12.5
  },
  "metadata": {
    "total_attempts": 152341,
    "total_fetched": 38543,
    "success_rate": 25.3,
    "active_connections": 342,
    "torrents_per_hour": 3084
  },
  "database": {
    "total_torrents": 38543,
    "total_files": 524234,
    "database_size_mb": 234.5
  }
}
```

#### `GET /refresh?info_hash=<hex>`
Manually trigger peer discovery for a specific info_hash (for testing/debugging).

**Example:**
```bash
curl "http://localhost:8080/refresh?info_hash=0123456789abcdef0123456789abcdef01234567"
```

## Database Schema

The crawler uses SQLite with the following schema:

### `torrents` table
- `id`: Primary key
- `info_hash`: 20-byte SHA-1 hash (unique)
- `name`: Torrent name
- `size_bytes`: Total size in bytes
- `piece_length`: BitTorrent piece length
- `num_pieces`: Number of pieces
- `total_peers`: Peer count
- `added_timestamp`: Discovery time (Unix timestamp)
- `last_seen`: Last seen time

### `torrent_files` table
- `id`: Primary key
- `torrent_id`: Foreign key to torrents
- `path`: File path within torrent
- `size_bytes`: File size
- `file_index`: Index within torrent

### Full-Text Search (FTS5)
- `torrent_search`: Porter-stemmed name search
- `file_search`: Trigram-based file path search

### Querying the Database

```bash
# Count total torrents
sqlite3 data/torrents.db "SELECT COUNT(*) FROM torrents"

# List recent torrents
sqlite3 data/torrents.db "SELECT name, size_bytes FROM torrents ORDER BY added_timestamp DESC LIMIT 10"

# Search by name
sqlite3 data/torrents.db "SELECT name FROM torrent_search WHERE name MATCH 'ubuntu'"

# Search by file path
sqlite3 data/torrents.db "SELECT DISTINCT t.name FROM file_search fs JOIN torrent_files tf ON fs.rowid = tf.id JOIN torrents t ON tf.torrent_id = t.id WHERE fs.path MATCH 'mkv'"
```

## Development

### Build Variants

```bash
# Debug build with symbols
make debug

# Build with sanitizers (detect memory errors, undefined behavior)
make asan

# Run with Valgrind
make valgrind

# Clean build artifacts
make clean
```

### Debug Logging

Set `log_level=DEBUG` in `config.ini` for verbose output:

```ini
log_level=DEBUG
```

This will show detailed information about:
- DHT packet parsing
- Routing table changes
- Metadata fetch attempts
- Connection state machines
- Database operations

## Performance Tuning

### For Maximum Discovery Rate

```ini
# Increase DHT workers
wbpxre_sample_infohashes_workers=50
wbpxre_get_peers_workers=1000

# Increase metadata workers
metadata_workers=50
max_concurrent_connections=5000

# Faster rotation for more keyspace coverage
node_rotation_interval_sec=30
```

### For Lower Resource Usage

```ini
# Reduce workers
wbpxre_sample_infohashes_workers=10
wbpxre_get_peers_workers=200
metadata_workers=10
max_concurrent_connections=500

# Slower rotation
node_rotation_interval_sec=300
```

## BitTorrent Enhancement Proposals (BEPs)

This crawler implements:

- **BEP 3**: The BitTorrent Protocol Specification (handshake)
- **BEP 5**: DHT Protocol (Mainline DHT)
- **BEP 9**: Extension for Peers to Send Metadata Files (ut_metadata)
- **BEP 10**: Extension Protocol (extended handshake)
- **BEP 51**: DHT Infohash Indexing (sample_infohashes queries)

## Troubleshooting

### No torrents being discovered

1. Check that UDP port 6881 is not blocked by firewall:
   ```bash
   sudo ufw allow 6881/udp  # Ubuntu/Debian
   sudo firewall-cmd --add-port=6881/udp  # CentOS/RHEL
   ```

2. Check routing table health in logs:
   ```
   [INFO] Routing table: 6543 good nodes, 234 dubious, 12 bad
   ```
   Should have 100+ good nodes for healthy operation.

3. Verify DHT statistics via HTTP API:
   ```bash
   curl http://localhost:8080/stats | jq '.dht'
   ```

### Low metadata fetch success rate

1. Increase concurrent connections:
   ```ini
   max_concurrent_connections=5000
   concurrent_peers_per_torrent=10
   ```

2. Enable peer retry:
   ```ini
   peer_retry_enabled=1
   peer_retry_max_attempts=3
   ```

3. Check peer store statistics in logs for peer discovery issues.

### High memory usage

1. Reduce bloom filter capacity:
   ```ini
   bloom_capacity=10000000  # 10M instead of 30M
   ```

2. Reduce routing table size:
   ```ini
   max_routing_table_nodes=20000  # Default: 40000
   ```

3. Reduce worker counts as shown in "Lower Resource Usage" above.

### Database growing too large

Run periodic maintenance:

```bash
sqlite3 data/torrents.db "VACUUM"
sqlite3 data/torrents.db "ANALYZE"
```

Or clear old data:

```bash
# Delete torrents older than 30 days
sqlite3 data/torrents.db "DELETE FROM torrents WHERE added_timestamp < strftime('%s', 'now', '-30 days')"
```

## Architecture Details

### Threading Model

1. **Main Thread**: libuv event loop for DHT network I/O
2. **wbpxre-dht Threads**: Separate worker pipelines for ping, find_node, sample_infohashes, get_peers
3. **Metadata Fetcher Workers**: Pool of 30+ threads attempting TCP connections
4. **Feeder Thread**: Dequeues info_hashes from queue and submits to worker pool

### Critical Invariants

- **Bloom filter timing**: Updated AFTER successful database writes, not at discovery time
- **libuv handle cleanup**: All handles must be closed before `uv_loop_close()`
- **Peer connection states**: Proper cleanup required at each state transition
- **Thread safety**: Most data structures use mutexes for thread-safe access

See [CLAUDE.md](CLAUDE.md) for detailed architecture documentation.

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Test with `make asan` and `make valgrind`
4. Submit a pull request

## References

- [BitTorrent DHT Protocol (BEP 5)](http://www.bittorrent.org/beps/bep_0005.html)
- [DHT Infohash Indexing (BEP 51)](http://www.bittorrent.org/beps/bep_0051.html)
- [Extension for Peers to Send Metadata (BEP 9)](http://www.bittorrent.org/beps/bep_0009.html)
- [Extension Protocol (BEP 10)](http://www.bittorrent.org/beps/bep_0010.html)
- [wbpxre-dht Library](lib/wbpxre-dht/)

## Acknowledgments

- Inspired by [bitmagnet](https://github.com/bitmagnet-io/bitmagnet)'s multi-pipeline architecture
- Uses [jech/dht](https://github.com/jech/dht) concepts for routing table management
- Built with [libuv](https://libuv.org/), [SQLite](https://www.sqlite.org/), and [CivetWeb](https://github.com/civetweb/civetweb)
