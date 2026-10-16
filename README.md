# DHT Crawler

A high-performance BitTorrent DHT crawler written in C that discovers and collects torrent metadata from the DHT network. Optimized for resource-constrained environments including Android/Termux.

## Features

- **Multi-threaded architecture** with supervisor pattern managing multiple isolated crawler units
- **Full BEP support**: BEP 5 (DHT), BEP 9 (ut_metadata), BEP 10 (Extension Protocol), BEP 51 (sample_infohashes)
- **IPv4 and IPv6**: dual-stack UDP socket with IPv4-mapped address support
- **High throughput**: 10-50 torrents/minute sustained with configurable parallelism
- **Intelligent filtering**: fastText adult/category classifier (`adult_mode = tag|drop`), an always-on hard-block list, and a non-Latin language filter
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

### Classifiers (optional, but they are the point)

A plain `make` crawls and classifies nothing: `src/classifier.c` and
`src/movie_kind.c` compile inert unless the build defines `CLF_ENABLED` /
`MK_ENABLED`. Two extra flavours turn the engines on, and one command fetches the
weights they serve with.

```bash
make models     # ~490 MB: v15 CRAM + v13 movie-kind, sha256-verified into ./models
make clf        # CRAM only  -> build/clf/dht_crawler   (category, tv_kind, adult)
make llama      # builds the pinned llama.cpp (18ab62a8) into build/llama-install
make full       # CRAM + movie-kind -> build/full/dht_crawler
```

`make models` pulls from `https://unjumble.au/bep51/models/<gen>/` and verifies
every file against the sha256 pinned in `tools/fetch-models.sh` before installing
it; the crawler re-verifies at load. `./models/v13` and `./models/v15` are the
compiled-in defaults (`movie_kind_model_dir`, `classifier_model_dir`), so a plain
`./build/full/dht_crawler` picks them up with no config change. Override the
source with `--base URL` and the generation with `--gen v13|v15`.

The crawler can also fetch them itself, with no shell scripts involved:

```bash
./dht_crawler --fetch-models                        # -> ./models/v15 and ./models/v13
./dht_crawler --fetch-models --models-dir /srv/models --force
```

Same objects, same gates, same directories — which is what lets a packaged
build stop shipping 490 MB of weights and pull them on first run instead. The
trust chain is **two pins, not six**: each generation's `manifest.json` is
sha256-pinned in `src/model_fetch.c` and is the root of trust, and every other
file is verified against the sha256 that manifest declares. A file is
downloaded only when it is missing or does not match, and it is renamed into
place only after it verifies (the manifest last), so an interrupted download
never leaves a corrupt model behind. `--force` re-downloads anyway,
`--models-base URL` retargets the object prefix, and the command exits 0 only
when every requested generation is present and verified.

It is cheap to call on every process start: a tree that already verifies costs
a handful of sha256s and no network at all (measured: 1.3 s for the full
512,972,090 B tree, versus 49.9 s and 512,953,487 B of transfers for the cold
run). Progress goes to stderr and nothing to stdout.

Without the weights, or without the flavours, the crawler still crawls: the
classifier degrades all-or-nothing (parser hint, `clf_ver` 0,
`parser_only_fallback`) and `movie_kind` reports `enabled: false`. `/stats`
publishes both states, so a silent degradation is visible.

### BEP51 cache

The crawler starts instantly when it already knows BEP51-capable nodes, and
aborts when a cold cache cannot discover `global_bootstrap_target` nodes in time.
So on a cold start it first fetches the published cache, and only falls back to
router discovery if that fails:

```bash
./dht_crawler --fetch-bep51-cache     # refresh data/bep51_cache.dat and exit
```

| key | default | meaning |
|---|---|---|
| `bep51_cache_url` | `https://unjumble.au/bep51/bep51_cache.dat` | published cache; empty disables the fetch |
| `bep51_cache_fetch_timeout_sec` | `30` | total deadline for the transfer (1..600) |
| `bep51_cache_ca_file` / `_dir` | empty | CA overrides; empty = the system trust store |

The download is validated with the loader's own magic + sha256 checks against a
scratch cache, and the file is renamed into place only on success — a failed,
truncated or corrupt download leaves the existing cache untouched and the crawler
falls through to the router bootstrap exactly as before. HTTPS is verified with
chain and peer-name (or IP SAN) checking — OpenSSL on the host, mbedTLS on the
Android build, which links no OpenSSL — against the system store, or against
`bep51_cache_ca_file`/`_dir` when set (on Android the default is the per-cert
`/system/etc/security/cacerts` directory). There is no "accept anything" mode.

Every fetch publishes its outcome, so a cold start that fell back to router
discovery is visible from `/stats` instead of only in the log:

```json
"bep51_cache_fetch": {
  "url": "https://unjumble.au/bep51/bep51_cache.dat",
  "enabled": true, "attempts": 1, "successes": 1, "failures": 0,
  "last_result": "ok", "last_http_status": 200,
  "last_bytes": 1932165, "last_nodes_loaded": 50000,
  "last_duration_ms": 812, "last_at_epoch": 1764000000, "last_error": ""
}
```

`last_result` is the verdict: `ok`, `disabled` (empty `bep51_cache_url`),
`not_attempted` (nothing has run in this process yet — a warm cache skips the
fetch entirely), `http_<code>` (the server answered and refused), `sha_mismatch`
(the body failed the loader's magic + sha256 gate), `load_failed` (validated but
not installable) or `network_error` (DNS, connect, TLS or the transfer
deadline). The block is published unconditionally — a disabled fetch still
reports `enabled: false` with `last_result: "disabled"` — so a probe never sees
a blank column. The counters are supervisor-owned and reset only by
`supervisor_bep51_fetch_reset()`; `--fetch-bep51-cache` runs without a
supervisor and logs the same verdict as a single-shot line instead.

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

# Bloom filter settings
bloom_capacity = 30000000
bloom_error_rate = 0.001

# Categories to save (comma-separated taxonomy names; unset/empty = save everything)
#   save_categories = unknown,movie,tv,anime,music,audiobook,ebook,comic,software,game,image,other
#   save_tv_kinds = none,episode,multi_episode,season_pack,multi_season,complete_series,daily
# --category-filter-update also purges every adult-tagged row (no config key)

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
./dht_crawler --compact                # VACUUM database into a fresh file and replace original
./dht_crawler --check-database         # Run SQLite integrity_check and report errors
./dht_crawler --recover-database       # Copy all readable rows to a new database; replace original
./dht_crawler --category-filter-update  # Delete rows outside save_categories/save_tv_kinds and adult-tagged rows
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

## Offline Classification Audit (`/random-sample`)

The read-only `GET /random-sample` endpoint serves randomly sampled torrents
that already carry a `torrent_class` row at a requested classifier version, so
a teacher-vs-student accuracy audit can run against a classified database copy
on a Linux host. The handler is stateless and never writes to the database;
deduplicating across calls is the client's job.

Build (the Makefile auto-applies the `lib/` submodule patches on the first run):

```bash
make                # produces ./dht_crawler
```

Create a sanitized `config.ini` beside the binary and point it at the
classified copy:

```ini
db_path=/path/to/classified-copy.db
http_bind=127.0.0.1
http_port=8080
# optional: keep the audit host responsive
nice_level=10
```

Start the crawler and query the endpoint:

```bash
./dht_crawler
curl "http://127.0.0.1:8080/random-sample?count=100&clf_ver=1"
```

Query parameters:

- `count` — items wanted, clamped to 1..500 (default 20). Fewer may be returned
  when the sample pool is small; the client re-rolls to accumulate more.
- `clf_ver` — classifier version to sample; only rows with that `clf_ver` are
  returned. Omit it (or pass `current`) to use the latest version recorded in
  `classifier_meta`. A torrent with no `torrent_class` row, or a row at a
  different version, is never returned.

Response shape (one array element per sampled torrent):

```json
{
  "clf_ver": 1,
  "count": 100,
  "items": [
    {
      "id": 25,
      "infohash": "a44e96d30cfbff6d5a80a3608a2a68e64c977b63",
      "name": "example release name",
      "files": ["Season 1/episode-25.mkv", "sample-25.nfo"],
      "student": {
        "category": "tv",
        "adult": 0,
        "tv_kind": "multi_season",
        "confidence": 0.91
      }
    }
  ]
}
```

`tv_kind` and `confidence` are `null` when the classifier stored no value.

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
