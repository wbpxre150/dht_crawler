# DHT Crawler

A high-performance BitTorrent DHT crawler written in C that discovers and collects torrent metadata from the DHT network. Optimized for resource-constrained environments including Android/Termux.

## Features

- **Multi-threaded architecture** with supervisor pattern managing multiple isolated crawler units
- **Full BEP support**: BEP 5 (DHT), BEP 9 (ut_metadata), BEP 10 (Extension Protocol), BEP 51 (sample_infohashes)
- **High throughput**: 10-50 torrents/minute sustained with configurable parallelism
- **Intelligent filtering**: Three-layer pornography filter with keyword, regex, and heuristic detection
- **Efficient deduplication**: Bloom filters with 30M capacity and two-strike failure policy
- **Batched database writes**: 10-100x performance improvement via transaction batching
- **Persistent bootstrap**: BEP51 node cache for instant startup after restarts
- **HTTP API**: REST endpoints for statistics and on-demand infohash queries
- **Android optimized**: Thread limits and resource constraints handled gracefully

## Quick Start

### Dependencies

Install required system packages:

**Arch Linux:**
```bash
sudo pacman -S base-devel libuv sqlite openssl userspace-rcu
```

**Debian/Ubuntu:**
```bash
sudo apt install build-essential libuv1-dev libsqlite3-dev libssl-dev liburcu-dev
```

**Termux (Android):**
```bash
pkg install clang libuv sqlite openssl liburcu
```

### Build

```bash
# Clone with submodules
git clone --recursive https://github.com/yourusername/dht_crawler.git
cd dht_crawler

# Build
make

# Run
./dht_crawler
```

The first build automatically:
- Applies patches to submodules (bencode-c)
- Builds libbloom dependency
- Creates data/ directory for database and caches

### Configuration

Edit `config.ini` to customize behavior. Key settings:

```ini
# Number of thread trees (crawler units)
num_trees = 32

# Worker counts per tree (multiplied by num_trees)
tree_find_node_workers = 5
tree_bep51_workers = 10
tree_get_peers_workers = 125
tree_metadata_workers = 110

# Enable/disable porn filter
porn_filter_enabled = 1

# Bloom filter settings
bloom_capacity = 30000000
bloom_error_rate = 0.001

# HTTP API port
http_port = 8080
```

## Architecture Overview

### Thread Tree Supervisor

The crawler uses a **supervisor pattern** that manages multiple independent "thread trees":

```
Supervisor
├── Thread Tree 1 (keyspace partition 0/32)
│   ├── Bootstrap → Find_Node → BEP51 → Get_Peers → Metadata
│   └── Private: routing table, queues, socket
├── Thread Tree 2 (keyspace partition 1/32)
│   └── ...
└── ...

Shared Resources:
├── Batch Writer (SQLite transactions)
├── Bloom Filters (deduplication + failure tracking)
├── Shared Node Pool (global bootstrap)
└── BEP51 Cache (persistent high-quality nodes)
```

**Benefits:**
- Scales to 10,000+ threads efficiently
- Isolated failures (one tree crash doesn't affect others)
- Automatic respawning of underperforming trees
- Comprehensive DHT keyspace coverage via partitioning

### Five-Stage Pipeline (Per Tree)

Each thread tree implements a concurrent pipeline:

1. **Bootstrap**: Sample nodes from shared pool + BEP51 cache
2. **Find_Node Workers**: Discover DHT nodes to populate routing table
3. **BEP51 Workers**: Query `sample_infohashes` to discover infohashes
4. **Get_Peers Workers**: Query DHT for peers that have each infohash
5. **Metadata Workers**: Connect to peers via TCP and fetch torrent metadata

## HTTP API

Access crawler statistics and perform on-demand queries:

### Get Statistics
```bash
curl http://localhost:8080/stats
```

Returns JSON with:
- Active trees and total metadata fetched
- Hourly torrent discovery rate
- Queue sizes and active connections
- Bloom filter and failure tracking stats

### Refresh Infohash
```bash
curl "http://localhost:8080/refresh?infohash=<40-char-hex>"
```

Performs on-demand get_peers query via dedicated refresh thread.

## Database

Torrents are stored in SQLite database (`data/torrents.db`):

```sql
CREATE TABLE torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash TEXT UNIQUE NOT NULL,
    name TEXT,
    discovered_at INTEGER NOT NULL,
    file_count INTEGER,
    file_info TEXT  -- JSON array of files
);
```

Query discovered torrents:
```bash
sqlite3 data/torrents.db "SELECT name, discovered_at FROM torrents ORDER BY id DESC LIMIT 10"
```

## Advanced Usage

### Debug Build

```bash
make debug
./dht_crawler
```

Enables `-g -DDEBUG` flags for debugging symbols.

### Memory Safety Testing

```bash
# AddressSanitizer (memory errors)
make asan
./dht_crawler

# ThreadSanitizer (race conditions)
make tsan
./dht_crawler

# Valgrind (memory leaks)
make valgrind
```

### Tuning for Android/Termux

Android has stricter thread limits (~10,000). The default config uses:
- 32 trees × ~300 threads/tree = ~9,600 threads

To reduce further:
```ini
num_trees = 16                    # Fewer trees
tree_get_peers_workers = 100      # Fewer workers per tree
tree_metadata_workers = 75
```

### Disabling Porn Filter

To collect all torrents without filtering:
```ini
porn_filter_enabled = 0
```

### Custom Bootstrap Nodes

The shared node pool uses hardcoded bootstrap nodes. To add custom nodes, edit `shared_node_pool.c:bootstrap_from_well_known_nodes()`.

## Monitoring

### Log Levels

Set in `config.ini`:
```ini
log_level = INFO  # DEBUG, INFO, WARN, ERROR
```

DEBUG mode shows per-worker statistics and phase transitions.

### Runtime Statistics

Trees log statistics every 60 seconds:
```
[INFO] [tree 5] ===== STATISTICS =====
[INFO] Phase: METADATA, Uptime: 612s
[INFO] Routing table: 1487 nodes
[INFO] Infohash queue: 1245/3000
[INFO] Peers queue: 892/3000
[INFO] Metadata: 1834 fetched, 245 filtered
[INFO] Rate: 3.0 metadata/min
```

### Bloom Filter Persistence

Bloom filters are persisted to disk:
- `data/bloom.dat` - Main infohash deduplication filter
- `data/failure_bloom.dat` - Failure tracking filter (two-strike policy)

Persisted every 60 seconds during batch flushes.

## Performance Characteristics

Typical performance on Android (32 trees, default config):
- **Throughput**: 10-50 torrents/minute sustained
- **Threads**: 6,000-9,000 total
- **Connections**: Up to 5,000 concurrent TCP
- **Memory**: ~500MB (dominated by bloom filters)
- **Disk**: Minimal (batched writes every 60s)

Bottlenecks:
- Peer availability (many peers don't respond or support ut_metadata)
- TCP connection latency
- Metadata fetch timeouts

## Troubleshooting

### Build Failures

**Error**: `fatal error: uv.h: No such file or directory`
- Install libuv-dev: `sudo apt install libuv1-dev`

**Error**: `undefined reference to 'urcu_*'`
- Install userspace-rcu: `sudo apt install liburcu-dev`

**Error**: Bencode parsing crashes
- The Makefile should auto-apply patches. Manually apply: `patch -p1 < patches/bencode-cap-fix.patch`

### Runtime Issues

**No torrents being discovered:**
- Check firewall allows UDP 6881
- Verify bootstrap works: logs should show "Global bootstrap complete"
- Increase `tree_bep51_workers` if BEP51 queries are slow

**High CPU usage:**
- Reduce worker counts in config.ini
- Increase throttling thresholds to pause workers sooner
- Enable `tree_require_empty_queue=1` to reduce respawning

**Database locked errors:**
- Increase `flush_interval` to reduce write frequency
- Increase `batch_size` for larger transactions

**Thread creation failures:**
- Reduce `num_trees` and worker counts
- Check system limits: `ulimit -u`

## Project Structure

```
dht_crawler/
├── src/              Implementation files
│   ├── main.c        Entry point, signal handling
│   ├── supervisor.c  Thread tree lifecycle management
│   ├── thread_tree.c Thread tree implementation
│   ├── tree_*.c      DHT protocol, routing, queues
│   ├── metadata_fetcher.c  BEP 9/10 metadata fetching
│   ├── batch_writer.c      Batched database writes
│   └── http_api.c    REST API endpoints
├── include/          Header files
├── lib/              Third-party libraries (submodules)
│   ├── bencode-c/    Bencode parser
│   ├── cJSON/        JSON library
│   ├── libbloom/     Bloom filter
│   ├── uthash/       Hash table macros
│   └── civetweb/     HTTP server
├── patches/          Submodule patches
├── data/             Runtime data (created on first run)
├── Makefile          Build configuration
└── config.ini        Runtime configuration
```

## License

MIT License - see LICENSE file

## Contributing

Contributions welcome! Areas for improvement:
- Better DHT bootstrap strategies
- IPv6 support
- Magnet link generation
- Web UI for browsing discovered torrents
- Improved peer selection heuristics

## Acknowledgments

- [wbpxre-dht](lib/wbpxre-dht/): Custom DHT library implementing BEP 51
- [bitmagnet](https://github.com/bitmagnetio/bitmagnet): Inspiration for multi-pipeline architecture
- BitTorrent BEPs: [5](http://www.bittorrent.org/beps/bep_0005.html), [9](http://www.bittorrent.org/beps/bep_0009.html), [10](http://www.bittorrent.org/beps/bep_0010.html), [51](http://www.bittorrent.org/beps/bep_0051.html)
