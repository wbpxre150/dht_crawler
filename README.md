# DHT Crawler

A high-performance BitTorrent DHT crawler written in C that discovers and collects torrent metadata from the DHT network. Optimized for resource-constrained environments including Android/Termux.

## Features

- **Multi-threaded architecture** with supervisor pattern managing multiple isolated crawler units
- **Full BEP support**: BEP 5 (DHT), BEP 9 (ut_metadata), BEP 10 (Extension Protocol), BEP 51 (sample_infohashes)
- **IPv4 and IPv6**: dual-stack UDP socket with IPv4-mapped address support
- **High throughput**: 10-50 torrents/minute sustained with configurable parallelism
- **Intelligent filtering**: Three-layer pornography filter with keyword, regex, and heuristic detection; non-Latin language filter
- **Efficient deduplication**: Two bloom filters — infohash dedup (30M capacity) and two-strike failure tracking
- **Batched database writes**: 10-100x performance improvement via transaction batching
- **Persistent bootstrap**: BEP51 node cache for instant warm-restart (no cold-start penalty after first run)
- **HTTP API + Web UI**: Search interface and REST endpoints for statistics and on-demand queries
- **Backup support**: Daily file copy, SSH incremental export, and rclone (S3-compatible) incremental export
- **Android optimized**: Thread limits and resource constraints handled gracefully

## Quick Start

### Dependencies

**Arch Linux:**
```bash
sudo pacman -S base-devel libuv sqlite openssl
```

**Debian/Ubuntu:**
```bash
sudo apt install build-essential libuv1-dev libsqlite3-dev libssl-dev
```

**Termux (Android):**
```bash
pkg install clang libuv sqlite openssl
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
- Applies patches to `lib/bencode-c` (capacity fix, zero-init fix) and `lib/civetweb` (atomic stop_flag fix)
- Builds `lib/libbloom` as a static library
- Creates `data/` directory for the database and caches

### Configuration

Edit `config.ini` to customize behavior. Key settings:

```ini
# Number of thread trees (crawler units)
num_trees = 32

# Worker counts per tree (multiplied by num_trees)
tree_find_node_workers = 3
tree_bep51_workers = 10
tree_get_peers_workers = 50
tree_metadata_workers = 75

# Enable/disable porn filter
porn_filter_enabled = 1

# Bloom filter settings
bloom_capacity = 30000000
bloom_error_rate = 0.001

# HTTP API port is hardcoded to 8080 (see include/http_api.h to change)
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
├── Bloom Filters (dedup + failure tracking)
└── BEP51 Cache (persistent high-quality nodes)
```

**Benefits:**
- Scales to thousands of threads efficiently
- Isolated failures (one tree crash doesn't affect others)
- Automatic respawning of underperforming trees
- Comprehensive DHT keyspace coverage via partitioning

### Five-Stage Pipeline (Per Tree)

Each thread tree implements a concurrent pipeline:

1. **Bootstrap**: Contact built-in DHT routers + BEP51 cache as fallback
2. **Find_Node Workers**: Discover DHT nodes to populate routing table
3. **BEP51 Workers**: Query `sample_infohashes` to discover infohashes
4. **Get_Peers Workers**: Query DHT for peers that have each infohash
5. **Metadata Workers**: Connect to peers via TCP and fetch torrent metadata

## HTTP API

The HTTP API listens on port 8080 and provides both a web UI and REST endpoints.

### Web Interface
```
http://localhost:8080/           # Google-style search home page
http://localhost:8080/search?q=ubuntu   # Search results page
http://localhost:8080/torrent?hash=...  # Torrent detail page
```

### REST Endpoints
```bash
# Crawler statistics
curl http://localhost:8080/stats

# Search torrents (JSON)
curl "http://localhost:8080/search?q=ubuntu"

# On-demand get_peers query for a specific infohash
curl "http://localhost:8080/refresh?infohash=<40-char-hex>"

# Random torrent discovery
curl http://localhost:8080/random-tv
curl http://localhost:8080/random-movies
curl http://localhost:8080/random-music
```

`/stats` returns JSON with active trees, total metadata fetched, hourly discovery rate, queue sizes, and bloom filter stats.

## Database

Torrents are stored in a SQLite database (`data/torrents.db`) with FTS5 full-text search:

```sql
CREATE TABLE torrents (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash        BLOB(20) NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    size_bytes       INTEGER NOT NULL,
    total_peers      INTEGER DEFAULT 0,
    added_timestamp  INTEGER NOT NULL
);

CREATE TABLE torrent_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    torrent_id  INTEGER NOT NULL REFERENCES torrents(id) ON DELETE CASCADE,
    prefix_id   INTEGER REFERENCES path_prefixes(id),
    filename    TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    file_index  SMALLINT NOT NULL
);

CREATE TABLE path_prefixes (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    prefix  TEXT NOT NULL UNIQUE
);
```

Query discovered torrents:
```bash
sqlite3 data/torrents.db "SELECT name, added_timestamp FROM torrents ORDER BY id DESC LIMIT 10"
```

## Advanced Usage

### Debug Build

```bash
make debug
./dht_crawler
```

Enables `-g -DDEBUG` flags for debugging symbols and more verbose logging.

### Memory Safety Testing

```bash
# AddressSanitizer (memory errors + undefined behaviour)
make asan
./dht_crawler

# ThreadSanitizer (race conditions)
make tsan
./dht_crawler

# Valgrind (memory leaks)
make valgrind
```

### CLI Maintenance Flags

```
./dht_crawler --rebuild-bloom-filter   # Rebuild bloom.dat from the existing database
./dht_crawler --porn-filter-update     # Re-scan database and remove porn-filtered entries
./dht_crawler --compact                # VACUUM database into a fresh file and replace original
./dht_crawler --check-database         # Run SQLite integrity_check and report errors
./dht_crawler --recover-database       # Copy all readable rows to a new database; replace original
./dht_crawler --dont-check-database    # Skip the automatic integrity check at startup
```

These modes exit after completing their task without starting the crawler.

### Tuning for Android/Termux

Android limits total threads to ~10,000. With default config: 32 trees × ~140 threads/tree ≈ 4,500 threads. To reduce further:

```ini
num_trees = 16                    # Fewer trees
tree_get_peers_workers = 30       # Fewer workers per tree
tree_metadata_workers = 50
```

### Custom Bootstrap Nodes

Bootstrap nodes are hardcoded in `src/supervisor.c` in the `BOOTSTRAP_HOSTS` array. Edit that array to add or replace DHT bootstrap routers.

### Disabling Porn Filter

```ini
porn_filter_enabled = 0
```

### Backup Configuration

Three backup mechanisms are available in `config.ini`:

```ini
# Daily file copy
backup_enabled = 1
backup_path = /path/to/backup.db

# SSH incremental (exports new rows as compressed SQL daily)
ssh_backup_enabled = 1
ssh_host = your.server.com
ssh_user = username
ssh_dest_path = /home/username/backups/
ssh_key_path = /path/to/key  # optional, leave "" for default

# rclone incremental (S3-compatible, e.g. Cloudflare R2)
rclone_backup_enabled = 1
rclone_remote = r2
rclone_dest_path = bucket-name/dht_crawler_backup
```

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
- `data/bloom.dat` — main infohash deduplication filter
- `data/failure_bloom.dat` — two-strike failure tracking filter

Both are saved every `flush_interval` seconds during batch flushes and reloaded on startup.

## Performance Characteristics

Typical performance (32 trees, default config):
- **Throughput**: 10-50 torrents/minute sustained
- **Threads**: ~4,500 total
- **Connections**: Up to 3,000+ concurrent TCP
- **Memory**: ~500MB (dominated by the two 30M-capacity bloom filters)
- **Disk**: Minimal (batched writes every 300s by default)

Bottlenecks:
- Peer availability (many peers don't respond or don't support ut_metadata)
- TCP connection latency
- Metadata fetch timeouts

## Troubleshooting

### Build Failures

**Error**: `fatal error: uv.h: No such file or directory`
- Install libuv: `sudo apt install libuv1-dev` / `sudo pacman -S libuv`

**Error**: Bencode parsing crashes
- The Makefile auto-applies patches. Manually: `patch -p1 < patches/bencode-cap-fix.patch`

### Runtime Issues

**No torrents being discovered:**
- Check firewall allows UDP 6881
- Verify bootstrap works: logs should show nodes being discovered within ~30s of cold start

**High CPU usage:**
- Reduce worker counts in `config.ini`
- Increase throttling thresholds to pause workers sooner

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
│   ├── main.c        Entry point, signal handling, maintenance CLI modes
│   ├── supervisor.c  Thread tree lifecycle, rate-based respawn, global bootstrap
│   ├── thread_tree.c Single tree: phase transitions, worker spawning
│   ├── tree_*.c      DHT protocol, routing, queues, metadata fetching
│   ├── batch_writer.c      Batched SQLite writes, bloom/backup triggers
│   ├── bep51_cache.c       Persistent BEP51 node cache
│   ├── http_api.c          REST API + web search UI
│   └── refresh_thread.c    Dedicated thread for /refresh endpoint
├── include/          Header files
├── lib/              Third-party libraries (git submodules)
│   ├── bencode-c/    Bencode parser (auto-patched)
│   ├── cJSON/        JSON library
│   ├── libbloom/     Bloom filter (auto-built)
│   ├── uthash/       Hash table macros
│   └── civetweb/     HTTP server (auto-patched)
├── patches/          Patches applied to submodules during build
├── data/             Runtime data (created on first run)
├── Makefile          Build configuration
└── config.ini        Runtime configuration (~130 keys)
```

## License

MIT License - see LICENSE file

## Contributing

Contributions welcome! Areas for improvement:
- Better DHT bootstrap strategies
- Magnet link generation
- Improved peer selection heuristics

## Acknowledgments

- [bitmagnet](https://github.com/bitmagnetio/bitmagnet): Inspiration for multi-pipeline architecture
- BitTorrent BEPs: [5](http://www.bittorrent.org/beps/bep_0005.html), [9](http://www.bittorrent.org/beps/bep_0009.html), [10](http://www.bittorrent.org/beps/bep_0010.html), [51](http://www.bittorrent.org/beps/bep_0051.html)
