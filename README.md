# DHT Crawler

A BitTorrent DHT crawler in C99. It discovers infohashes with BEP 51
(`sample_infohashes`), resolves peers with BEP 5 `get_peers`, fetches torrent
metadata over TCP with BEP 9/10, and writes rows to SQLite with FTS5 search.
An optional classifier labels every row at ingest time.

Runs on Linux and on Android/Termux (the same tree builds the binary the
Android app ships).

## What it does

- **Supervisor + thread trees.** `num_trees` (default 4) independent trees, each
  owning a Kademlia routing table, queues and a UDP socket; the supervisor
  respawns trees whose metadata rate collapses.
- **Dual-stack DHT.** One IPv6 socket with `IPV6_V6ONLY` off, so IPv4 and IPv4-
  mapped peers share it. BEP 5, 9, 10 and 51.
- **Five-stage pipeline per tree:** bootstrap → `find_node` → BEP 51 → `get_peers`
  → metadata. Downstream queue pressure pauses the upstream stage.
- **Deduplication in three layers:** a shared infohash bloom, a two-strike
  failure bloom, and an in-flight claim set (262,144 slots, 900 s TTL) that stops
  two workers fetching the same infohash.
- **Batched writes.** Rows are buffered and committed in transactions
  (`batch_size` 1000, `flush_interval` 60 s) with a `wal_checkpoint(RESTART)`
  after each flush.
- **Warm start.** The BEP 51 node cache is persisted, and a published copy is
  fetched when the local cache is too small (see below).
- **Ingest-time classification (optional build flavours):** CRAM v15 gives
  category / `tv_kind` / adult, a llama.cpp + JEVH head gives a movie-genre mask.
  A plain `make` compiles both engines inert.
- **JSON HTTP API** on port 8080 (`http_port`, `http_bind`): search, browse,
  random, per-torrent files/info, on-demand `get_peers`, and `/stats`.
  The crawler serves no HTML — the UI lives in the Android app.
- **Backups:** daily file copy, SSH incremental export, rclone (S3-compatible)
  incremental export, all optional.

## Build

```bash
sudo pacman -S base-devel libuv sqlite openssl     # Arch
sudo apt install build-essential libuv1-dev libsqlite3-dev libssl-dev   # Debian/Ubuntu
pkg install clang libuv sqlite openssl             # Termux
```

```bash
git clone --recursive https://github.com/yourusername/dht_crawler.git
cd dht_crawler
make            # -> ./dht_crawler
./dht_crawler   # creates data/ and starts crawling
```

The first `make` applies three patches to the vendored submodules
(`lib/bencode-c` capacity + zero-init, `lib/civetweb` atomic `stop_flag`), builds
`lib/libbloom`, and creates `data/`. Only those three fixes live in `patches/` —
everything else is in the tree.

`config.ini` is optional: absent or unreadable, the crawler logs a warning and
runs on the compiled defaults (`src/config.c:config_init_defaults`). It is
gitignored because it may hold backup hosts and credentials.

### Classifier flavours and weights

A plain `make` crawls and classifies nothing: `src/classifier.c` needs
`-DCLF_ENABLED` and `src/movie_kind.c` needs `-DMK_ENABLED`.

```bash
make models   # ~490 MB: v15 CRAM + v13 movie-kind, sha256-verified into ./models
make clf      # CRAM only        -> build/clf/dht_crawler   (category, tv_kind, adult)
make llama    # pinned llama.cpp (18ab62a8) -> build/llama-install
make full     # CRAM + movie-kind -> build/full/dht_crawler
```

`make models` runs `tools/fetch-models.sh`, which pulls from
`https://unjumble.au/bep51/models/<gen>/` and verifies every file against the
sha256 pinned in the script. `./models/v15` and `./models/v13` are the compiled-in
defaults (`classifier_model_dir`, `movie_kind_model_dir`), so the flavours pick
them up with no config change.

The binary can fetch the same objects itself — same directories, same gates:

```bash
./dht_crawler --fetch-models                                  # -> ./models/{v15,v13}
./dht_crawler --fetch-models --models-dir /srv/models --force
```

The trust chain is two pins, not six: each generation's `manifest.json` sha256 is
pinned in `src/model_fetch.c` and is the root of trust; every other file is
verified against the sha256 that manifest declares. Only missing or mismatched
files are downloaded, each is renamed into place only after it verifies (manifest
last), and an already-valid tree costs a handful of sha256s and no network — so
it is cheap to call on every process start. `--models-base <url>` retargets the
object prefix; progress goes to stderr.

Without weights, or without a flavour that enables them, the crawler still crawls:
the classifier degrades all-or-nothing (`clf_ver` 0, `parser_only_fallback`) and
`movie_kind` reports `enabled: false`. `/stats` publishes both states, so a silent
degradation is visible.

### BEP 51 cache

A cold start first fetches the published node cache and only falls back to router
discovery if that fails:

```bash
./dht_crawler --fetch-bep51-cache   # refresh data/bep51_cache.dat and exit
```

| key | default | meaning |
|---|---|---|
| `bep51_cache_url` | `https://unjumble.au/bep51/bep51_cache.dat` | published cache; empty disables the fetch |
| `bep51_cache_fetch_timeout_sec` | `30` | total transfer deadline (1..600) |
| `bep51_cache_ca_file` / `_dir` | empty | CA overrides; empty = the system trust store |

The download is validated with the loader's own magic + sha256 checks against a
scratch file and renamed into place only on success, so a failed, truncated or
corrupt download leaves the existing cache untouched. HTTPS is verified with
chain and peer-name (or IP SAN) checking — OpenSSL on the host, mbedTLS on the
Android build, which links no OpenSSL. There is no "accept anything" mode.

Every fetch publishes its outcome to `/stats` as `bep51_cache_fetch`, so a cold
start that fell back to router discovery is visible: `last_result` is `ok`,
`disabled`, `not_attempted` (a warm cache skipped the fetch), `http_<code>`,
`sha_mismatch`, `load_failed` or `network_error`, with `last_http_status`,
`last_bytes`, `last_nodes_loaded`, `last_duration_ms` and `last_error` beside it.

## Configuration

`config.ini` is read from the working directory. Keys are `key = value`, parsed
with `atoi` (so `= true` is `0`), and out-of-range values are clamped with a
warning rather than rejected. Defaults worth knowing:

| group | keys (default) |
|---|---|
| DHT | `dht_port` (6881), `udp_rate_limit_pps` (2500, 0 = off) |
| Database | `db_path` (`data/torrents.db`) |
| HTTP | `http_port` (8080), `http_bind` (`0.0.0.0`), `nice_level` (0) |
| Trees | `num_trees` (4), `tree_find_node_workers` (10), `tree_bep51_workers` (10), `tree_get_peers_workers` (8), `tree_metadata_workers` (2) |
| Dedup | `bloom_capacity` (10000000), `failure_bloom_capacity` (30000000), `failure_strike_count` (2), `bloom_error_rate` (0.001) |
| Writes | `batch_size` (1000), `flush_interval` (60) |
| get_peers | `tree_get_peers_deadline_ms` (6000), `_fanout` (8), `_max_queries` (16), `_timeout_ms` (1500) |
| metadata | `tree_tcp_connect_timeout_ms` (2000), `tree_parallel_peers` (2), `tree_metadata_timeout_ms` (60000), `tree_metadata_fresh_peer_retries` (1) |
| Respawn | `min_metadata_rate` (0.01), `dynamic_rate_margin` (0.02), `tree_rate_check_interval_sec` (60), `respawn_drain_timeout_sec` (120) |
| Filtering | `adult_mode` (`tag`), `adult_hide_threshold` (0.375), `hard_block_file` (`hard_block.txt`), `language_filter_non_latin_threshold` (33, 0 = off), `save_categories`, `save_tv_kinds` (unset = save everything) |
| Models | `classifier_model_dir` (`models/v15`), `movie_kind_model_dir` (`models/v13`), `movie_kind_threads` (4), `movie_kind_thermal_backoff_c` (82), `classify_backfill_mode` (`all`) |

Two groups are documented separately because they are opt-in and were measured
net-negative on device:

- **get_peers / metadata tuning** (`tree_get_peers_round_idle_ms`,
  `_penalize_misses`, `_slot_budget_ms`, `_finish_on_peers`,
  `tree_metadata_piece_idle_ms`, `tree_metadata_peer_blacklist_ttl_sec`) — every
  one ships at `0` (off). See `CONFIG_INI.md` for semantics, clamps and the
  measured cost of each.
- **Bootstrap routers** are hardcoded in `src/supervisor.c` (`BOOTSTRAP_HOSTS`);
  the `/refresh` thread has its own list in `src/refresh_thread.c`.

The full key inventory is `include/config.h` plus the parser in
`src/config.c:config_load_file`.

## HTTP API

JSON only — the crawler serves no HTML (`/search`, `/random` and `/stats` also send
`Access-Control-Allow-Origin: *`). CivetWeb runs with 2 threads and a 15 s request
timeout; results are capped at 50 rows per page.

| endpoint | parameters |
|---|---|
| `GET /search` | `q` (required), `page`, plus any filter below |
| `GET /browse` | filter-only listing over classified rows (no `q`), `sort` = `peers` (default) \| `added` \| `size`, `page` |
| `GET /random` | `category`, `tv_kind`, `movie_kind`, `adult`, `min_peers` (default 10) |
| `GET /random-sample` | `count` (1..500, default 20), `clf_ver` (or `current`) |
| `GET /torrent/info` | `hash` (40 hex) |
| `GET /torrent/files` | `hash`, `offset`, `limit` (max 200), `q` (min 3 chars) |
| `GET /refresh` | `hash` (40 hex) — on-demand `get_peers`, torrent must already exist |
| `GET /stats` | — |

Filters (all optional; a malformed value is a 400): `category` (name or index),
`tv_kind`, `adult` (one of `hide`, which is the default once any filter is
supplied, `show` or `only`), `min_conf`, and `movie_kind` = comma-separated
genres (all must match) or `none`. A plain `/search?q=` keeps the pre-filter behaviour: no class join, adult
rows included.

`/search` matches the FTS5 `torrent_search` index (name, porter stemmer) and the
`torrent_docs` index (name + filenames, unicode61), ranking title matches above
file-name matches. Envelope:

```json
{"count":50,"total_count":1234,"page":1,"limit":50,"has_more":true,
 "timestamp":1764000000,
 "results":[{"info_hash":"…","name":"…","filtered_name":"…","size":"1.4 GB",
             "size_bytes":1500000000,"total_peers":12,"added":1764000000,
             "num_files":3,"category":"tv","tv_kind":"episode","adult":0,
             "confidence":0.91,"movie_kind":["drama"],"files":[…]}]}
```

`/stats` is the operational surface. Root blocks: `config`, `language_filter`,
`classifier`, `classifier_backfill`, `adult_backfill`, `movie_kind`,
`movie_kind_backfill`, `category_filter`, `supervisor`, `inflight`,
`metadata_peer_fail`, `bep51_cache`, `bep51_cache_fetch`, `trees`, `partitions`,
`aggregate`. Lifetime counters are cumulative since start; `aggregate` also
carries `*_last_interval` siblings plus `stats_interval_sec` (measured on the
monitor thread's 10 s tick), so `rate = value / stats_interval_sec` is the
current rate rather than the since-start average.

## Database

SQLite in WAL mode (`journal_size_limit` 500 MB, `synchronous=NORMAL`,
`mmap_size` 256 MB, 32 KB pages, incremental auto-vacuum).

| table | contents |
|---|---|
| `torrents` | `id`, `info_hash` (20-byte blob, unique), `name`, `size_bytes`, `total_peers`, `added_timestamp` |
| `torrent_files` | `torrent_id`, `prefix_id`, `filename`, `size_bytes`, `file_index` |
| `path_prefixes` | interned directory prefixes |
| `torrent_class` | one row per torrent: `category`, `tv_kind`, `adult`, `adult_p`, season/episode range, `resolution`, `confidence`, `clf_ver`, `movie_kind_mask`, `movie_kind_clf_ver`, `adult_clf_ver` |
| `classifier_meta` | key/value state (backfill resume keys, versions) |
| `torrent_search` | FTS5 over `torrents.name` (porter/unicode61, external content) |
| `torrent_docs` | contentless FTS5 word index over name + filenames (unicode61) |

```bash
sqlite3 data/torrents.db "SELECT name FROM torrents ORDER BY id DESC LIMIT 10"
./dht_crawler --print-schema     # the DDL, one statement per line
```

## Classification

Ingest-time, optional per build flavour:

- **CRAM v15** — one forward pass per row → `category` (unknown, movie, tv, anime,
  music, audiobook, ebook, comic, software, game, image, other), `tv_kind` (none,
  episode, multi_episode, season_pack, multi_season, complete_series, daily) and
  an adult probability. `adult_hide_threshold` 0.375 is v15's recall-0.90 point;
  `0` takes the threshold from the manifest. `adult_mode = tag` keeps the row,
  `drop` refuses it at ingest.
- **Movie-kind v13** — llama.cpp decoder backbone + JEVH head → a 16-bit genre
  mask (action, comedy, drama, horror, thriller, scifi, fantasy, animation,
  documentary, romance, crime, adventure, family, mystery, war, western). The
  `movie_kind` filter is conjunctive: a row must carry every requested genre.
- **Non-Latin filter (on by default):** a torrent whose name — or any of its file
  paths — is at or above `language_filter_non_latin_threshold` percent non-Latin
  script characters (default 33, `0` disables) is dropped at ingest. ASCII digits,
  punctuation and Latin-with-diacritics count as neutral or Latin. The ratio is
  per string over the whole path, so a CJK directory with an ASCII filename can
  stay under the threshold. Rows stored while the filter was off (or below a
  later-lowered threshold) need `--language-filter-cleanup`.
- **Always-on filter:** `hard_block.txt`, one term per line.
- **Ingest gating:** `save_categories` / `save_tv_kinds` narrow what is stored;
  unset means everything. `--category-filter-update` applies the same selection to
  an existing database and also purges adult-tagged rows.
- **Backfills** re-classify an existing database (see the CLI table below).

## Maintenance CLI

These modes do their work and exit without starting the crawler.

| flag | effect |
|---|---|
| `--check-database` | `PRAGMA integrity_check`, report errors |
| `--recover-database` | copy all readable rows into a fresh database, replace the original |
| `--compact` | `VACUUM` into a fresh file, replace the original |
| `--restore-database <dump.sql>` | replay an uncompressed SQL dump into a fresh database |
| `--rebuild-bloom-filter` | rebuild `bloom.dat` from the existing database |
| `--clear-failure-filter` | delete the persisted failure bloom |
| `--rebuild-fts <db>` | create/rebuild the FTS5 index in an existing database |
| `--rebuild-docs <db>` | backfill the `torrent_docs` word index from the base tables |
| `--print-schema` | print the schema DDL and exit |
| `--category-filter-update` | purge rows outside `save_categories`/`save_tv_kinds`, plus adult-tagged rows |
| `--language-filter-cleanup` | purge stored torrents at or above `language_filter_non_latin_threshold`; refuses when the threshold is 0 |
| `--classify-backfill <db>` | re-classify with the loaded generation; dry run unless `--write`, `--classify-backfill-limit <n>` |
| `--adult-backfill <db>` | token-gated adult pass only; dry run unless `--write`, `--adult-backfill-limit <n>` |
| `--movie-kind-backfill <db>` | genre masks; `--dry-run`, `--limit <n>`, `--reset`, `--dump-masks` |
| `--movie-kind-bench <db> [<rows>]` | throughput measurement for the encoder |
| `--fetch-bep51-cache` | download `bep51_cache_url` into `bep51_cache_path` |
| `--fetch-models` | download and verify the weights (`--models-base`, `--models-dir`, `--force`) |

## Tests

```bash
make tests      # 17 host binaries: search, docs, cram, backfills, movie-kind,
                # model fetch, http fetch, inflight set, language filter,
                # stats window, meta-peer-fail
make tests-asan # the same under ASan/UBSan (tests-asan-cram, -backfill, …)
make asan       # build the crawler with ASan + UBSan
make tsan       # build the crawler with ThreadSanitizer
make valgrind   # debug build under valgrind
```

The suite is hermetic: the CRAM fixture and utf8proc are vendored, no network.
`thread_tree.c` and `http_api.c` have no host test (the accounting identities were
validated on device), so run the crawler itself for those paths.

## Monitoring

`log_level` = `DEBUG` | `INFO` | `WARN` | `ERROR` (default INFO). Trees log a
statistics block every 60 s (phase, uptime, routing-table size, queue depths,
fetched/filtered counts, rate). Bloom filters persist to `data/bloom.dat` and
`data/failure_bloom.dat` on each flush (at most every `bloom_persist_interval_sec`,
default 300) and are reloaded at startup.

`/stats` `supervisor.metadata_failures` splits failures into `connect`,
`handshake`, `stall`, `ut_reject`, `size`, `hash`, `other` (per-connection
outcomes, counted once) and `deadline` (one event per attempt), so the counters
can sum to more than the failed-attempt count by design.

## Performance notes

- get_peers is **latency-bound**, not discovery-bound: per-tree drain tracks
  `1000 × tree_get_peers_workers / mean_lookup_ms`, and raising BEP 51 offer rate
  did not raise lookups/s. Raising `tree_bep51_workers` 3 → 12 lifted queries/s
  per tree 44 % and left lookups/s and metadata/s flat (the extra samples were
  refused by the bloom and the in-flight set).
- Metadata is **peer-liveness-bound**: on the reference device ~83 % of the
  failure mass was a TCP connect that never completed, ~15 % handshake, <1 %
  stall. That is why the connect-failure memory exists and why the piece-idle and
  deadline knobs ship off.
- `tree_parallel_peers` ships at 2 because 4 measured −6.2 % metadata/s in a
  same-hour A/B.
- Reference-device steady state (32-bit ARM, classification on): ~250 get_peers
  lookups/s, ~7 metadata fetches/s, ~2 GB RSS. These numbers come from the
  patch-19…23 measurement run summarised in `CONFIG_INI.md`; the raw run reports
  are not part of this repository.

## Android / Termux

- Android caps a process at roughly 10,000 threads; with the default
  `num_trees` 4 the crawler stays far below that. Raising `num_trees` multiplies
  every per-tree worker count.
- The Android build links mbedTLS instead of OpenSSL and uses
  `/system/etc/security/cacerts` as the default CA directory.
- `nice_level` lets the crawler yield to the rest of the system.

## Project layout

```
src/            implementation (main, supervisor, thread_tree, tree_*, batch_writer,
                database, http_api, http_torrent_page, classifier/cram, movie_kind,
                model_fetch, meta_peer_fail, stats_window, …)
include/        headers, one per module
lib/            submodules: bencode-c, cJSON, civetweb, libbloom, uthash
vendor/utf8proc vendored, for the movie-kind flavour
tparse/         vendored release-name / file-list parser (CRAM flavour)
patches/        the three third-party submodule fixes `make` applies
tools/          fetch-models.sh, build-llama.sh
tests/          host unit tests and fixtures
data/           runtime state (created on first run, gitignored)
```

## License

MIT — see `LICENSE`.

## Acknowledgments

- [bitmagnet](https://github.com/bitmagnetio/bitmagnet) — inspiration for the
  multi-pipeline architecture.
- BitTorrent BEPs [5](http://www.bittorrent.org/beps/bep_0005.html),
  [9](http://www.bittorrent.org/beps/bep_0009.html),
  [10](http://www.bittorrent.org/beps/bep_0010.html),
  [51](http://www.bittorrent.org/beps/bep_0051.html).
