# AGENTS.md

Guidance for coding agents working in this repository. `README.md` is the user
documentation; this file is the working map: build targets, invariants, and the
things that are easy to break.

## Build and test

```bash
make              # -O2, C99, output: ./dht_crawler
make debug        # -g -DDEBUG
make asan         # AddressSanitizer + UBSan
make tsan         # ThreadSanitizer
make valgrind     # debug build, run under valgrind
make clean

make models       # fetch + verify v13/v15 weights into ./models (~490 MB)
make clf          # CRAM only        -> build/clf/dht_crawler
make llama        # pinned llama.cpp 18ab62a8 -> build/llama-install
make full         # CRAM + movie-kind -> build/full/dht_crawler

make tests        # 16 host test binaries
make tests-asan   # the same under ASan (tests-asan-cram, -backfill, …)
```

Dependencies: `libuv`, `libsqlite3`, `libssl`/`libcrypto`, `libpthread`, `patch`.
Submodules (`lib/bencode-c`, `lib/cJSON`, `lib/civetweb`, `lib/libbloom`,
`lib/uthash`) must be checked out. The first `make` applies the three patches in
`patches/` (bencode-c capacity + zero-init, civetweb atomic `stop_flag`), builds
`lib/libbloom`, and creates `data/`.

Provenance: most features used to ship as `.patch` files applied at build time by
an external (Android) build. They are now committed to the tree, one commit per
former patch, so the crawler builds and runs standalone. `patches/` keeps only
the three third-party submodule fixes. Docs that say "patch 19/20/21/22/23" mean
the historical series number, not a file to apply — nothing in `patches/` matches
those names.

## CLI flags

```
./dht_crawler [--rebuild-bloom-filter] [--compact] [--check-database]
              [--recover-database] [--restore-database <dump.sql>]
              [--rebuild-fts <db>] [--rebuild-docs <db>] [--print-schema]
              [--clear-failure-filter] [--category-filter-update]
              [--language-filter-cleanup]
              [--fetch-bep51-cache] [--fetch-models]
              [--classify-backfill <db> [--write] [--classify-backfill-limit <n>]]
              [--adult-backfill <db> [--write] [--adult-backfill-limit <n>]]
              [--movie-kind-backfill <db> [--dry-run] [--limit <n>] [--reset]
                                      [--dump-masks]]
              [--movie-kind-bench <db> [<rows>]]
```

All of these do their work and exit instead of starting the crawler.
`--help` only lists the first group; the backfill and schema modes are
dispatched from a second argument loop in `main.c` and documented in the comments
there. `--fetch-models` takes `--models-base <url>`, `--models-dir <dir>` and
`--force`.

`--fetch-models` is dispatched before any config or database work: it needs
neither, and it must be cheap to call on every process start (a tree that already
verifies costs a handful of sha256s and no network). The trust chain is two pins,
not six: each generation's `manifest.json` sha256 is pinned in
`src/model_fetch.c` and is the root of truth; every other file is verified
against the sha256 that manifest declares. Files are renamed into place only after
they verify (manifest last). Progress on stderr, nothing on stdout, exit 0 only
when every requested generation is present and verified. See
`include/model_fetch.h`.

`--fetch-bep51-cache` downloads `bep51_cache_url` (default the published
`https://unjumble.au/bep51/bep51_cache.dat`) into `bep51_cache_path` and exits.
The supervisor runs the same fetch automatically when its cache is below
`global_bootstrap_target`, and falls back to router bootstrap if it fails.

`--category-filter-update` purges rows outside `save_categories`/`save_tv_kinds`
and every adult-tagged row (`torrent_class.adult = 1`).

`--language-filter-cleanup` runs `language_filter_cleanup_run()` over the
configured database: it backs up on request, scans every `torrents` row, applies
`language_filter_check()` and deletes the matches in 5000-row batches inside one
transaction. It reads the threshold from the already-initialized module, so it
refuses (exit 1, database untouched) when
`language_filter_non_latin_threshold` is 0, and it never prompts — the app runs
it with no TTY.

## Architecture

### Supervisor pattern

```
Supervisor (supervisor.c)
├── Thread Tree 0 … Thread Tree N-1   (thread_tree.c)
│   └── Private: routing table, queues, UDP socket handle
│
└── Shared:
    ├── Batch Writer  (batch_writer.c)  — buffered SQLite inserts, bloom + backup triggers
    ├── Bloom filters (bloom_filter.c)  — infohash dedup + two-strike failure tracking
    ├── In-flight set (inflight_set.c)  — one claim per in-flight infohash
    ├── Peer-fail map (meta_peer_fail.c) — opt-in per-address connect-failure memory
    ├── BEP51 cache   (bep51_cache.c)   — persistent bootstrap nodes
    └── UDP socket + dispatcher         — SO_REUSEPORT, routes responses by txn id
```

`num_trees` defaults to **4**, not 32: `config_init_defaults()` in
`src/config.c` is the single source of truth for every default (workers 10
find_node / 10 BEP51 / 8 get_peers / 2 metadata per tree).

### Five-stage pipeline (per tree)

1. **Bootstrap** — hardcoded routers (`BOOTSTRAP_HOSTS` in `src/supervisor.c`) plus
   a sample of the BEP51 cache; a cold cache is first filled by a parallel URL
   bootstrap.
2. **find_node** (`tree_protocol.c`) — discovers nodes, fills `tree_routing.c`.
3. **BEP51** (`tree_protocol.c`) — `sample_infohashes`, pushes into
   `tree_infohash_queue` through the dedup funnel.
4. **get_peers** (`tree_protocol.c`) — peers per infohash into `tree_peers_queue`.
5. **metadata** (`tree_metadata.c`) — TCP connect, BEP 9/10 fetch.

Upstream stages pause on a condition variable when downstream queues exceed their
threshold and resume when they drain below the lower one.

### Respawn

`supervisor.c:monitor_thread_func()` checks each tree's EMA-smoothed metadata
rate every `tree_rate_check_interval_sec`. A tree below
`min_metadata_rate - dynamic_rate_margin` for longer than
`tree_rate_grace_period_sec` (and past `tree_min_lifetime_minutes`) is respawned;
the old tree moves to a draining list and is destroyed once its connections drop
below `respawn_spawn_threshold` or `respawn_drain_timeout_sec` elapses. Cumulative
counters are carried across respawns in supervisor atomics — any new per-tree
counter that must survive a respawn has to be folded there, like the `gp_*` and
`bep51.*` counters.

### Key files

| File | Role |
|---|---|
| `src/main.c` | entry point, signal handling, maintenance dispatch, shared singletons |
| `src/supervisor.c` | tree lifecycle, monitor thread, global bootstrap |
| `src/thread_tree.c` | one tree: phase transitions, worker spawning, get_peers lookup |
| `src/tree_protocol.c` | DHT message encode/decode (find_node, BEP51, get_peers) |
| `src/tree_routing.c` | Kademlia k-buckets |
| `src/tree_dispatcher.c` | routes UDP responses to waiting workers by transaction id |
| `src/tree_metadata.c` | BEP 9/10 fetcher, parallel peer connections |
| `src/batch_writer.c` | buffered inserts, bloom persistence, backup triggers |
| `src/database.c` | schema, WAL settings, per-thread read connections |
| `src/http_api.c` | CivetWeb server: search/browse/random/stats/refresh |
| `src/http_torrent_page.c` | `/torrent/info`, `/torrent/files` JSON |
| `src/classifier.c`, `src/cram.c` | CRAM v15 (category, tv_kind, adult) |
| `src/movie_kind.c`, `src/jevhead.c` | movie-kind genre mask |
| `src/model_fetch.c` | `--fetch-models` |
| `src/http_fetch.c` | HTTPS fetch (OpenSSL on host, mbedTLS on Android) |
| `src/meta_peer_fail.c`, `src/inflight_set.c`, `src/stats_window.c`, `src/language_filter.c` | pure, host-tested helpers |
| `src/language_filter_cleanup.c` | `--language-filter-cleanup` sweep over stored rows |
| `src/config.c` | defaults (`config_init_defaults`) + INI parser (`config_load_file`) |

Per-tree state: routing table, infohash/peers/response queues, socket handle,
worker handles, rate counters. Shared and locked: batch writer, bloom filters,
BEP51 cache, dispatcher hash table, in-flight set, peer-fail map.

## Invariants that are easy to break

### get_peers / metadata tuning ships disabled

Every mechanism below is **fail-open at `0`** (or at its legacy value) and ships
that way because the measurement said so: TUNED vs LEGACY on the same build in the
same hour gave lookups/s −1.5 %, yield −3.4 pp, packets/lookup +6 %, metadata/s
−13 %. What ships enabled is the **instrumentation** (failure taxonomy, exit/slot
counters, the `/stats` split). Do not flip a default without a same-hour A/B.

- **get_peers is latency-bound, not discovery-bound.** Per-tree drain equals
  `1000 × tree_get_peers_workers / mean_lookup_ms` to within ~1 % across every
  measured window, `idle_exits_total` is 0, and every lookup burns the 6 s
  deadline. Queue-pressure readings that look like a discovery cap are arithmetic
  with the same `mean_lookup_ms`. Doubling BEP51 fill (3.73 → 7.14 /s/tree) did
  not move the drain (3.13 → 3.14).
- **Metadata is peer-liveness-bound.** Failure mass: connect 82.8–84.1 %,
  handshake 14.8–16.1 %, stall 0.8 %, deadline 0.1–0.2 %. Piece-idle eviction and
  a shorter deadline address ≤1 % of it.
- `tree_get_peers_round_idle_ms` (0 = off, 250..6000) — end a round after that
  much silence since the last answer; a round with zero answers terminates the
  lookup. Measured inert (245 idle exits vs 246,380 deadline exits).
- `tree_get_peers_slot_budget_ms` (0 = off, 500..6000) — each sent slot carries
  its own `sent_us`; the drain breaks once no slot is inside budget
  (`GP_EXIT_SLOT_BUDGET`). `gp_slot_expired_total` is a subset of
  `gp_unanswered_slots_total`. Measured net-negative at 2000.
- `tree_get_peers_finish_on_peers` (0 = off) — drains already-queued packets with
  timeout-0 pops and ends; `gp_early_finish_total` counts it. Costs peer-list width
  (one responder instead of up to eight).
- `tree_get_peers_penalize_misses` (0 = off) — charges unanswered slots against
  the node via `tree_routing_mark_failed()`; the slot stores the queried node's
  20-byte id (captured at send), not an array index, so a merge cannot misdirect
  the penalty. At 1 it churned ~1,000 penalties/s and cost 10.8 pp yield.
- `tree_metadata_piece_idle_ms` (0 = off, 3000..60000) — `last_piece_time` is
  refreshed only when a new metadata piece is copied, so a peer that keeps the
  socket warm without delivering pieces is evicted. The 10 s
  `CONN_SILENCE_CLOSE_MS` watchdog fires first for silent peers; no event is
  counted twice.
- `tree_parallel_peers` ships at **2** (clamp 1..4 = `MAX_PARALLEL_PEERS`; the
  narrower clamp is a correctness fix — the fetcher used to clamp deeper, so the
  accepted value was not the effective one). pp=4 measured −6.2 % metadata/s.
- `tree_metadata_timeout_ms` ships at **60000**; the deadline is ≤0.2 % of the
  failure mass, so shortening it is not justified by measurement.
- **Fan-out deadline check is always on** — a defect fix, not tuning: the fan-out
  send loop stops once `now_us() >= deadline_us`, so rate-limit sleeps can no
  longer push `mean_lookup_ms` systematically past `tree_get_peers_deadline_ms`
  (measured 7,044 ms against a 6,000 ms deadline before the fix).

### Failure counters count once

`/stats` `supervisor.metadata_failures`: `connect`, `handshake`, `stall`,
`ut_reject`, `size`, `hash`, `other` are **per-connection** outcome events counted
exactly once at the site that first detects them; `deadline` is **one event per
attempt**, taken where every live slot is abandoned together. Σ counters ≥ failed
attempts is therefore expected — do not "fix" it. Never count in
`parallel_conn_close()`, in the post-process close (already counted inside
`parallel_conn_process`) or in the final cleanup loop, or one failure is counted
twice.

### Windowed rates (`stats_window.c`)

`aggregate.*_last_interval` plus `stats_interval_sec` are sampled by
`monitor_thread_func()` on its existing hard-coded 10 s tick (no config key).
`src/stats_window.c` is pure C99, host-tested by `make tests-stats-window`: it
keeps the previous sample and reports a saturating difference, so the first tick
publishes zeros with `stats_interval_sec` 0 and a counter that moves backwards
reports 0 instead of wrapping. Fold the same totals `stats_handler()` publishes
(cumulative respawn buckets + live trees) and publish through supervisor atomics —
do **not** copy the non-atomic per-tree `metadata_rate` double, which another
thread writes. `rate = value / stats_interval_sec`.

### Per-address connect-failure memory (`meta_peer_fail.c`)

`tree_metadata_peer_blacklist_ttl_sec` (default **0 = off**, clamp 60..3600) keys
one supervisor-shared map: 131,072 slots × 32 B = 4 MiB, open addressing with
FNV-1a + linear probing, one mutex, lazy TTL ageing on lookup/insert only (no
thread, no sweep), load-factor clear at 3/4 = 98,304 slots.

- **Fail-open is absolute.** `ttl_sec = 0` never creates the map; every lookup
  answers "do not skip" and every record is a no-op, so peer selection is
  byte-identical to legacy. A creation failure lands in the same state.
- **Additive recording only.** Every site that counts
  `metadata_failures.connect_total` also records the address. Handshake, stall,
  hash and deadline outcomes are not recorded; the counters keep their count-once
  meaning.
- **Skip-to-empty guard is mandatory.** A zero-connection attempt returns NULL and
  feeds the irreversible infohash strike bloom, which would block an infohash
  because of a *peer address's* history. The selection cursor never passes the
  last candidate, and an attempt whose whole window was skipped refills once with
  the map ignored (without resetting the cursor — retrying a failed address would
  inflate `connect_total` and make `R = connect_total/attempts` unreadable).
- Capacity is sized by the TTL working set: 131,072 × 3/4 = 98,304 > the
  ~66,000–79,800 entries a 600 s TTL accumulates at ~110–133 new distinct fail
  addresses/s. At the original 8192 slots the table was wiped every ~50–70 s and
  `expired_total` stayed 0 — entries died by capacity clear, never by TTL.
- Counter units differ on purpose: `metadata_peers_skipped_total` counts skipped
  **slots** (peers are not deduplicated), `metadata_connect_fail_addresses_total`
  counts **new distinct addresses admitted**.

### BEP 51 query and dedup funnel (instrumentation only)

Eight per-tree atomics, published as `trees[].bep51.*` and summed as
`supervisor.bep51.*`: `queries_sent_total`, `responses_received_total`,
`timeouts_total` (the two branches of the 1000 ms wait), and the four mutually
exclusive gates — `samples_offered_total`, `bloom_rejected_total`,
`inflight_refused_total`, `queue_full_total`, `samples_pushed_total`. Per window
`offered = bloom_rejected + inflight_refused + queue_full + pushed` (±1 % for
in-flight boundary effects); check that identity first when a window looks odd.
The three per-worker locals that used to hold these numbers are kept as
log-throttle cursors, so the DEBUG log sequence is unchanged. There is no host
test for `thread_tree.c` / `http_api.c`, so this was validated on device.

## Classification

Both engines are optional at build time: `src/classifier.c` needs `-DCLF_ENABLED`,
`src/movie_kind.c` needs `-DMK_ENABLED` (and `MK_NFC_ENABLED` for the shipped
Android feature set). A plain `make` compiles both inert.

- **CRAM v15** (`models/v15`, `classifier_model_dir`) — one forward pass per row →
  category, `tv_kind`, adult marginal. Categories: unknown, movie, tv, anime,
  music, audiobook, ebook, comic, software, game, image, other. `tv_kind`: none,
  episode, multi_episode, season_pack, multi_season, complete_series, daily.
  `adult_hide_threshold` 0.375 is v15's recall-0.90 point; `0` means "take it from
  the manifest"; the precedence is config.ini > manifest > C literal.
  `adult_mode` is `tag` (keep the row) or `drop` (refuse at ingest).
- **Movie-kind v13** (`models/v13`, `movie_kind_model_dir`) — llama.cpp Q4_0
  backbone + JEVH head → 16-bit genre mask in v9 manifest order (action, comedy,
  drama, horror, thriller, scifi, fantasy, animation, documentary, romance, crime,
  adventure, family, mystery, war, western). The array index **is** the bit index;
  `http_api.c` mirrors the names by hand so the host build needs no model headers.
  `movie_kind` filter is conjunctive (a row must carry every selected genre) with
  `none` as an OR arm covering mask NULL or 0. `movie_kind_clf_ver` is the
  watermark for "unclassified" — a mask of 0 is a valid `none` verdict, never a
  marker.
- **Adult generation watermark:** `torrent_class.adult_clf_ver` records which
  generation scored the adult columns. NULL is the backfill engine's candidate
  predicate; the generation, never the label, is the watermark.
- **Ingest gating:** `save_categories` / `save_tv_kinds` are bitmasks over the
  tparse taxonomy, default every bit set (= save everything, filter inert).
- **Backfill daemon:** `classify_backfill_mode` = `all` (default in C), `adult`
  (token-gated adult pass only) or `off`. The Android app pins `off` and drives the
  pass from the maintenance action instead. Upsert-only, resumable from
  `classifier_meta`.
- Degradation is all-or-nothing and visible: no weights → parser hint,
  `clf_ver` 0, `parser_only_fallback`; `movie_kind` reports `enabled: false`.
  `/stats` publishes both.

## Database

Schema in `src/database.c` (`database_print_schema()` backs `--print-schema`):
`torrents`, `torrent_files`, `path_prefixes`, `torrent_class`, `classifier_meta`,
plus FTS5 `torrent_search` (external content over `torrents.name`, porter) and
`torrent_docs` (contentless, `contentless_delete=1`, unicode61, name + filenames).
Triggers keep both indexes in step with inserts and deletes.

WAL mode with `synchronous=NORMAL`, `wal_autocheckpoint=0` and a manual
`wal_checkpoint(RESTART)` after every batch flush; `journal_size_limit` 500 MB,
32 KB pages, `mmap_size` 256 MB, incremental auto-vacuum. Long read paths
(search/browse/random) use a per-thread read connection from `database_read_db()`;
millisecond queries stay on `db->db`.

## HTTP API

JSON only — the crawler serves no HTML. Routes registered in
`http_api_start()`: `/search`, `/browse`, `/stats`, `/refresh`, `/random`,
`/random-sample`, `/torrent/files`, `/torrent/info`. Responses are capped at
`HTTP_API_MAX_RESULTS` (50). `/search` requires `q` and accepts the class filters
(`category`, `tv_kind`, `adult`, `min_conf`, `movie_kind`) parsed by
`parse_class_filter()`; a plain `/search?q=` deliberately keeps the pre-filter
behaviour (no class join, adult rows visible). `/browse` is the filter-only
listing over `torrent_class` rows with `sort` = `peers` (default) | `added` |
`size` (a whitelist of fixed ORDER BY fragments — never interpolate input).
`/refresh` requires a 40-hex `hash` that is
already in the database and enqueues a get_peers query on the refresh thread.
`/stats` root blocks: `config`, `language_filter`, `classifier`,
`classifier_backfill`, `adult_backfill`, `movie_kind`, `movie_kind_backfill`,
`category_filter`, `supervisor`, `inflight`, `metadata_peer_fail`,
`bep51_cache`, `bep51_cache_fetch`, `trees`, `partitions`, `aggregate`.

## Configuration gotchas

`config.ini` is optional and gitignored; `main.c` loads `config.ini` from the
working directory and warns when it is missing. Values are parsed with `atoi`
(`= true` is 0) and out-of-range values are clamped with `LOG_WARN`, not rejected.

- Every default lives in `config_init_defaults()`; the struct comments in
  `include/config.h` are not always in step with it (e.g. it claims
  `tree_tcp_connect_timeout_ms` 5000, the code sets 2000).
- `language_filter_non_latin_threshold` defaults to **33** and the comparison is
  **at or above** (`nl * 100 >= threshold * total`, exact, no truncation), so a
  torrent that is exactly 33 % non-Latin is dropped. It used to default to 0 by
  accident — the assignment sat inside a comment block that was never closed —
  which silently disabled the filter; the field is now set outside any comment
  and `tests-language-filter` pins the boundary. `0` disables the filter and
  counts nothing, and `/stats` publishes `language_filter.enabled` /
  `threshold_percent` unconditionally so a disabled filter is visible.
- `config_parse_args()` exists but `main()` never calls it, so `-c/--config` is
  not a working flag. Do not document it as one.
- `use_thread_trees` / `use_keyspace_partitioning` are 1 and there is no second
  architecture behind them.

## Testing notes

`make tests` builds 17 standalone host binaries (no network, no submodule patches;
the CRAM fixture and utf8proc are vendored). `make tests-asan` repeats them under
ASan/UBSan. `tests-language-filter` covers the UTF-8 script classification, the
at-or-above boundary, the clamps and the fail-open/disabled paths.
`tests/test_movie_kind_filter.c` compiles `src/http_api.c` into its
translation unit on purpose — the predicate fragments are file-local there, and
`-ffunction-sections`/`--gc-sections` keep the rest out of the link. Tests that
touch `src/database.c` also compile `src/bloom_filter.c` and link libbloom +
SQLite + libuv because `database_t` holds a `uv_mutex_t`.

`thread_tree.c` and `http_api.c` have no host test. Run the crawler itself when
changing them, and prefer `/stats` counters over log lines as evidence.

## Documentation honesty

- `CONFIG_INI.md` documents the opt-in tuning keys. It cites run reports
  (`measurements.md`, `reports/w3-sizing-blocker.md`, `plan/…`) and
  `tools/crawler/gp_probe.py` that are **not** in this repository — the Android
  app repo has them. Do not quote numbers from memory; if a figure is not in this
  tree or in `CONFIG_INI.md`, say it is unverified.
- The measured figures that are quotable live in `CONFIG_INI.md` and the comments
  in `src/config.c` / `include/config.h` / `include/meta_peer_fail.h`.
- Feature claims must match the code: the crawler serves JSON, not a web UI; the
  defaults are the ones in `config_init_defaults()`, not the ones in the older
  README prose.

## Community index

<!-- gortex:communities:start -->
## Community Skills

| Area | Description | Explore |
|------|-------------|---------|
| Src Third Party Assert | 1907 symbols | `analyze(operation:"communities", id:"community-1193")` |
| Src Third Party Lual Error | 795 symbols | `analyze(operation:"communities", id:"community-1016")` |
| Duktape 1 5 2 Src Separate Duk Get Tval | 795 symbols | `analyze(operation:"communities", id:"community-303")` |
| Duktape 1 8 0 Src Separate Duk Get Tval | 785 symbols | `analyze(operation:"communities", id:"community-667")` |
| Lua 5 5 0 Beta Src Lual Error | 642 symbols | `analyze(operation:"communities", id:"community-1071")` |
| Lua 5 3 6 Src Lual Error | 516 symbols | `analyze(operation:"communities", id:"community-972")` |
| Lua 5 2 4 Src Lua Pushvalue | 482 symbols | `analyze(operation:"communities", id:"community-959")` |
| Duktape 1 8 0 Src Duk Match Regexp | 459 symbols | `analyze(operation:"communities", id:"community-845")` |
| Duktape 1 8 0 Src Noline Duk Match Regexp | 459 symbols | `analyze(operation:"communities", id:"community-617")` |
| Duktape 1 5 2 Src Noline Duk Match Regexp | 458 symbols | `analyze(operation:"communities", id:"community-252")` |
| Duktape 1 5 2 Src Duk Match Regexp | 458 symbols | `analyze(operation:"communities", id:"community-479")` |
| Src Third Party Sqlite3 Free | 450 symbols | `analyze(operation:"communities", id:"community-1177")` |
| Third Party Lua 5 1 5 Lua Pushvalue | 398 symbols | `analyze(operation:"communities", id:"community-920")` |
| Src Third Party Sqlite3 Value Text | 377 symbols | `analyze(operation:"communities", id:"community-1183")` |
| 9 Dirs | 375 symbols | `analyze(operation:"communities", id:"community-525")` |
| Civetweb Src 5 Dirs | 350 symbols | `analyze(operation:"communities", id:"community-152")` |
| Src Third Party Sqlite3 Reset | 303 symbols | `analyze(operation:"communities", id:"community-1180")` |
| Src Log Msg | 301 symbols | `analyze(operation:"communities", id:"community-1287")` |
| Src Third Party Sqlite3fts5getvarint32 | 284 symbols | `analyze(operation:"communities", id:"community-1130")` |
| Lua 5 5 0 Beta Src Luak Exp2anyreg | 194 symbols | `analyze(operation:"communities", id:"community-1101")` |

<!-- gortex:communities:end -->
