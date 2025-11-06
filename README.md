# DHT Crawler

A high-performance BitTorrent DHT crawler written in C that discovers and indexes torrent metadata from the BitTorrent network.

## Features

- **Modern DHT Implementation**: Uses `wbpxre-dht` library with full BEP 51 support for efficient info_hash discovery
- **High-Throughput Metadata Fetching**: Multi-threaded worker pool (10-100 workers) for concurrent peer connections
- **Intelligent Deduplication**: Bloom filter provides 90% reduction in database queries
- **Batched Database Writes**: Transaction batching improves write performance by 10-100x
- **Retry Logic**: Exponential backoff for failed metadata fetches
- **HTTP API**: Query discovered torrents via REST endpoints
- **Persistent State**: Bloom filter and shadow routing table persistence across restarts

## Quick Start

### Prerequisites

Install required dependencies:

```bash
# Ubuntu/Debian
sudo apt-get install libuv1-dev libsqlite3-dev libssl-dev build-essential

# Fedora/RHEL
sudo dnf install libuv-devel sqlite-devel openssl-devel gcc make

# macOS (Homebrew)
brew install libuv sqlite openssl
```

### Building

```bash
# Clone the repository with submodules
git clone --recursive https://github.com/yourusername/dht_crawler.git
cd dht_crawler

# Build
make

# Run
./dht_crawler
```

The crawler will:
1. Bootstrap into the DHT network
2. Discover info_hashes via BEP 51 `sample_infohashes` queries
3. Fetch metadata from peers using the BitTorrent Extension Protocol
4. Store torrent metadata in SQLite database at `data/torrents.db`
5. Serve HTTP API on port 8080

## Configuration

Edit `config.ini` to customize behavior:

```ini
# DHT Settings
dht_port=6881

# Worker Pool (more workers = higher throughput)
metadata_workers=100
wbpxre_sample_infohashes_workers=50
wbpxre_get_peers_workers=100

# Connection Limits
max_concurrent_connections=2000
concurrent_peers_per_torrent=5
connection_timeout_sec=10

# Bloom Filter (duplicate detection)
bloom_enabled=1
bloom_capacity=10000000
bloom_error_rate=0.001

# Batch Writes (performance optimization)
batch_writes_enabled=1
batch_size=1000
flush_interval=60

# Logging (DEBUG, INFO, WARN, ERROR)
log_level=INFO

# HTTP API
http_port=8080

# Database
db_path=data/torrents.db
```

## HTTP API

Once running, the HTTP API is available at `http://localhost:8080/`

Example endpoints:
- `GET /stats` - View crawler statistics
- `GET /torrents` - Query discovered torrents
- `GET /torrent/:infohash` - Get metadata for specific torrent

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     BitTorrent DHT Network                   │
└───────────────────────┬─────────────────────────────────────┘
                        │ BEP 51 sample_infohashes
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                    wbpxre-dht Library                        │
│  • Multi-threaded worker pipelines                           │
│  • Routing table with BEP 51 metadata                        │
│  • Ping, find_node, sample_infohashes, get_peers workers    │
└───────────────────────┬─────────────────────────────────────┘
                        │ info_hash callbacks
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                      DHT Manager                             │
│  • Coordinates DHT operations                                │
│  • Maintains peer store for discovered peers                 │
│  • Shadow routing table for node capability tracking         │
└───────────────────────┬─────────────────────────────────────┘
                        │ enqueue info_hashes
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                    Infohash Queue                            │
│  • Thread-safe bounded queue (10K capacity)                  │
│  • Bloom filter deduplication (90% query reduction)          │
└───────────────────────┬─────────────────────────────────────┘
                        │ dequeue for fetching
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                  Metadata Fetcher                            │
│  • Worker pool (10-100 threads)                              │
│  • BEP 9/10 BitTorrent Extension Protocol                    │
│  • TCP connections to peers                                  │
│  • Metadata piece reassembly and SHA-1 verification          │
│  • Retry logic with exponential backoff                      │
└───────────────────────┬─────────────────────────────────────┘
                        │ metadata results
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                   Batch Writer                               │
│  • Transaction batching (10-100x speedup)                    │
│  • Configurable batch size and flush interval                │
└───────────────────────┬─────────────────────────────────────┘
                        │ batched writes
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                  SQLite Database                             │
│  • Torrent metadata (name, files, size, pieces)             │
│  • Updates bloom filter after successful writes              │
└─────────────────────────────────────────────────────────────┘
                        ↑ query
┌─────────────────────────────────────────────────────────────┐
│                      HTTP API                                │
│  • CivetWeb embedded server                                  │
│  • REST endpoints for querying torrents                      │
│  • Statistics and monitoring                                 │
└─────────────────────────────────────────────────────────────┘
```

## BitTorrent Protocol Support

This crawler implements:
- **BEP 3**: The BitTorrent Protocol (handshake)
- **BEP 5**: DHT Protocol (via wbpxre-dht)
- **BEP 9**: Extension for Peers to Send Metadata Files (ut_metadata)
- **BEP 10**: Extension Protocol (extended handshake)
- **BEP 51**: DHT Infohash Indexing (sample_infohashes for efficient discovery)

## Performance

Expected performance on modern hardware:
- **Discovery rate**: 1,000-10,000 info_hashes/hour (depends on DHT network conditions)
- **Metadata fetch rate**: 10-100 torrents/minute (depends on worker count and peer availability)
- **Memory usage**: ~100-500 MB (bloom filter + routing tables + worker threads)
- **Database growth**: ~1-10 KB per torrent (varies with number of files)

Performance tuning tips:
1. Increase `metadata_workers` for higher throughput (more concurrent connections)
2. Increase `wbpxre_sample_infohashes_workers` for faster discovery
3. Enable `batch_writes_enabled` with larger `batch_size` for write-heavy workloads
4. Adjust `max_concurrent_connections` based on system limits (check `ulimit -n`)

## Development

### Build Options

```bash
# Standard build
make

# Debug build with symbols
make debug

# AddressSanitizer build (detect memory errors)
make asan

# Run with Valgrind
make valgrind

# Clean build artifacts
make clean
```

### Project Structure

```
dht_crawler/
├── src/              # Source files (.c)
├── include/          # Header files (.h)
├── lib/              # Third-party libraries (git submodules)
│   ├── wbpxre-dht/   # Custom DHT implementation
│   ├── bencode-c/    # Bencode parser
│   ├── cJSON/        # JSON library
│   ├── civetweb/     # HTTP server
│   └── libbloom/     # Bloom filter
├── data/             # Runtime data (database, bloom filter)
├── config.ini        # Configuration file
├── Makefile          # Build configuration
└── README.md         # This file
```

## Troubleshooting

### No info_hashes being discovered

- Check that UDP port 6881 is not blocked by firewall
- Wait 5-10 minutes for DHT bootstrap to complete
- Verify routing table has good nodes: check HTTP API `/stats` endpoint
- Increase `wbpxre_sample_infohashes_workers` in config.ini

### Metadata fetches failing

- Check `connection_timeout_sec` (increase if peers are slow)
- Verify outbound TCP connections are not blocked
- Increase `max_retry_attempts` for more persistence
- Check logs for specific error patterns

### High memory usage

- Reduce `bloom_capacity` (at cost of higher false positive rate)
- Reduce `metadata_workers` (fewer concurrent connections)
- Reduce `discovered_queue_capacity` (smaller discovery queue)

### Database locked errors

- Ensure only one instance is running
- Check that `data/` directory is writable
- Enable `batch_writes_enabled` to reduce lock contention

## License

[Specify your license here]

## Contributing

[Specify contribution guidelines here]

## Acknowledgments

- [wbpxre-dht](lib/wbpxre-dht): Modern DHT implementation inspired by bitmagnet
- [CivetWeb](https://github.com/civetweb/civetweb): Embedded HTTP server
- [cJSON](https://github.com/DaveGamble/cJSON): JSON parser
- [libbloom](https://github.com/jvirkki/libbloom): Bloom filter implementation
- [bencode-c](https://github.com/alanxz/bencode-c): Bencode parser

## See Also

- [CLAUDE.md](CLAUDE.md) - Detailed architecture and development guide for AI assistants
- [config.ini](config.ini) - Full configuration reference with inline documentation
