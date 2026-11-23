# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
make              # Build with -O2 (output: ./dht_crawler)
make debug        # Build with -g -DDEBUG
make asan         # Build with AddressSanitizer + UBSan
make tsan         # Build with ThreadSanitizer
make valgrind     # Debug build + valgrind wrapper
make clean        # Remove build/ and ./dht_crawler

# Classifier flavours (the plain build compiles both engines inert)
make models       # fetch + verify the v13/v15 weights into ./models (~490 MB)
make clf          # CRAM only      -> build/clf/dht_crawler
make llama        # build pinned llama.cpp 18ab62a8 -> build/llama-install
make full         # CRAM + movie-kind -> build/full/dht_crawler
```

System dependencies: `libuv`, `libsqlite3`, `libssl`, `libcrypto`, `libpthread`.

The first `make` automatically applies patches to `lib/bencode-c` and `lib/civetweb`, and builds `lib/libbloom`. No other setup required.
Provenance: this repository used to ship most of its features as `.patch` files applied at build time by
an external (Android) build. They are now committed directly to the tree, one commit per former patch, so
the crawler builds and runs standalone. `patches/` keeps only the three fixes that patch third-party
submodules (`lib/bencode-c`, `lib/civetweb`); `make` applies those automatically. Docs below still label
the features "patch NN" — that is the historical series number, not a file to apply.

### CLI Flags

```
./dht_crawler [--rebuild-bloom-filter] [--compact]
              [--category-filter-update] [--check-database]
              [--recover-database] [--fetch-bep51-cache] [--fetch-models]
```

These are maintenance modes that exit after completing their task rather than running the crawler.
`--fetch-bep51-cache` downloads `bep51_cache_url` (default the published
`https://unjumble.au/bep51/bep51_cache.dat`) into `bep51_cache_path` and exits; the
supervisor runs the same fetch automatically when its cache is below
`global_bootstrap_target`, and falls back to router bootstrap if it fails.

`--fetch-models [--models-base <url>] [--models-dir <dir>] [--force]` downloads the
classifier weights (`v15` CRAM + `v13` movie-kind) into `<dir>/<gen>` (default `./models`,
the compiled-in model dirs) and exits. The trust chain is two pins, not six: each
generation's `manifest.json` sha256 is pinned in `src/model_fetch.c`, and every other file
is verified against the sha256 that manifest declares. Only missing or mismatched files are
downloaded, each is renamed into place only after it verifies (manifest last), and an
already-correct tree is a no-op with no network — so it is cheap to call on every start.
`--force` re-downloads, `--models-base` retargets the object prefix; progress is on stderr,
nothing on stdout. See `include/model_fetch.h`.

`--category-filter-update` purges the rows outside `save_categories`/`save_tv_kinds`
and every adult-tagged row (`torrent_class.adult = 1`).

## Architecture

### Supervisor Pattern

The crawler runs N independent **thread trees** (default: 32), each targeting a different slice of the DHT keyspace. A supervisor thread monitors their metadata rates and respawns underperformers. Shared state is minimal:

```
Supervisor (supervisor.c)
├── Thread Tree 0 … Thread Tree 31  (each: thread_tree.c)
│   └── Private: routing table, queues, UDP socket handle
│
└── Shared resources:
    ├── Batch Writer    (batch_writer.c)   — buffered SQLite inserts
    ├── Bloom Filters   (bloom_filter.c)   — dedup + failure tracking
    ├── BEP51 Cache     (bep51_cache.c)    — persistent bootstrap nodes
    └── Shared UDP socket + Dispatcher    — SO_REUSEPORT, routes by txn ID
```

### Five-Stage Pipeline (per tree)

Each tree runs these stages concurrently with independent worker thread pools:

1. **Bootstrap** — contacts hardcoded DHT routers + samples BEP51 cache (cold-start: supervisor first runs a parallel URL bootstrap to populate the cache)
2. **Find_Node workers** (`tree_protocol.c`) — discovers DHT nodes, builds routing table (`tree_routing.c`)
3. **BEP51 workers** (`tree_protocol.c`) — queries `sample_infohashes`, pushes infohashes into `tree_infohash_queue`
4. **Get_Peers workers** (`tree_protocol.c`) — queries peers per infohash, pushes peer addrs into `tree_peers_queue`
5. **Metadata workers** (`tree_metadata.c`) — TCP-connects to peers, fetches torrent info dict via BEP9/10

Throttling: upstream stages pause (condition variable) when downstream queues exceed configurable thresholds; resume when queues drain below a lower threshold.

### get_peers / metadata tuning (patch 19)

Every mechanism below exists in the shipped patch and every one is **fail-open** at `0` (or at the old
default). **They ship disabled**, because the measured result is negative: the compiled defaults are the
legacy ones — `tree_get_peers_round_idle_ms` 0, `tree_get_peers_penalize_misses` 0,
`tree_get_peers_slot_budget_ms` 0, `tree_get_peers_finish_on_peers` 0, `tree_metadata_piece_idle_ms` 0,
`tree_parallel_peers` 2, `tree_metadata_timeout_ms` 60000. The **instrumentation** is what ships enabled
(failure taxonomy + exit/slot counters + the `/stats` split); the behaviour knobs stay present, documented
and opt-in.

Two measured bounds explain the negative result. The only valid contemporaneous comparison is TUNED vs
LEGACY on the same build in the same hour: lookups/s −1.5 %, yield −3.4 pp, packets/lookup +6 %,
metadata/s −13 %. (The overnight baseline is not a control — LEGACY on the new build measured 33.1 % yield
against the baseline's 36.9 %, i.e. the DHT thinned overnight.)

- **get_peers is latency-bound, not discovery-bound** — corrected by the seven-window run recorded in
  `plan/getpeers-metadata-results.md`. Per-tree worker drain equals `1000 × tree_get_peers_workers /
  mean_lookup_ms` to within 1.1 % in every one of the seven windows (98.9–99.6 %), `idle_exits_total` is 0,
  and every lookup burns the 6 s deadline. The patch-19 reading (drain 2.5–3.0 lookups/s against a BEP51 fill
  of 3.0–3.5/s, `discovery_paused` 11–14 %) was queue pressure, not a discovery cap: the same arithmetic with
  patch-19's own `mean_lookup_ms` of 7,044 ms gives 1000 × 18 / 7044 = 2.56 /s, inside that band. W5 later
  doubled fill (3.73 → 7.14 /s/tree) and the drain did not move (3.13 → 3.14).
- **Metadata is peer-liveness-bound.** Across all windows the failure mass is connect 82.8–84.1 %,
  handshake 14.8–16.1 %, stall 0.8 %, deadline 0.1–0.2 %. Piece-idle eviction and the deadline knob address
  ≤1 % of it, so neither is justified by measurement.
- **The round-idle premise was falsified.** Answers trickle in faster than 1.5 s, so over a full window the
  idle exit fired 245 times against 246,380 deadline exits — inert, not harmful. The per-slot budget and
  finish-on-peers mechanisms were built to replace it and measured net-negative too.

- **Round-idle get_peers exit** — `tree_get_peers_round_idle_ms` (default 0 = off, opt-in 250..6000),
  `thread_tree.c:tree_lookup_peers`. A round ends after that much silence **since the last answer** (the
  timer is refreshed by every matched answer, so a round still producing responses is never cut short), and
  a round that produced **zero** answers terminates the whole lookup instead of walking the remaining
  candidate set. The terminal cause is recorded as a `GP_EXIT_*` reason; only `GP_EXIT_IDLE` and
  `GP_EXIT_DEADLINE` have `/stats` counters.
- **Per-slot answer budget** — `tree_get_peers_slot_budget_ms` (default 0 = off, opt-in 500..6000). Each
  sent slot carries its own `sent_us`; when every remaining slot has either answered or run out its budget
  the drain breaks (`GP_EXIT_SLOT_BUDGET`), so one dead peer cannot pin the round through another slot's
  trickled answers. The sweep counts the expired subset into `gp_slot_expired_total` (a subset of
  `gp_unanswered_slots`).
- **Finish-on-peers** — `tree_get_peers_finish_on_peers` (default 0 = off, 0/1). When on, a lookup that
  already has peers drains the packets already queued (timeout-0 pops, so answers arriving together are not
  lost) and ends, instead of waiting for all fan-out slots or the full deadline. `gp_early_finish_total`
  counts the lookups terminated this way. The trade-off is real: entries then carry one responder's peer
  list instead of up to eight.
- **Silent-node penalties** — `tree_get_peers_penalize_misses` (default 0, opt-in 0/1). The round-end sweep
  charges every unanswered fan-out slot against its node through the pre-existing
  `tree_routing_mark_failed()` (`tree_routing.c`), which bumps `fail_count` and evicts the node at
  `MAX_FAIL_COUNT` (3). The slot stores the queried node's **20-byte id**, captured at send time, so a
  candidate-array merge cannot misdirect the penalty; a matched answer re-adds the node via
  `tree_routing_add_node()`, resetting `fail_count` (eviction means "repeatedly silent and never answering").
  Measured cost when enabled: ~1,000 penalties/s churned the tables and yield fell 10.8 pp, hence the
  default of 0.
- **Piece-idle metadata eviction** — `tree_metadata_piece_idle_ms` (default 0 = off, opt-in 3000..60000),
  `tree_metadata.c`. `last_piece_time` is refreshed **only** when a new metadata data piece is copied into
  the shared buffer, so a peer that keeps the socket warm with keep-alive/have/bitfield traffic but delivers
  no pieces is evicted instead of holding a fetch-window slot until the global deadline. The pre-existing
  10 s silence watchdog (`CONN_SILENCE_CLOSE_MS`) still catches fully silent peers and fires first, so no
  event is counted twice; the refill loop then advances the window to the next untried peer.
  `tree_parallel_peers` (default 2, clamp 1..4 = `MAX_PARALLEL_PEERS`, so the accepted value is the
  effective one) and `tree_metadata_timeout_ms` (default 60000, 5000..600000) ship at their legacy values:
  the wider window was A/B'd against 2 in this run (same hour, same build: pp=2 6.834 vs pp=4 6.408
  metadata/s, −6.2 %, with more connects and no more fetches at pp=4), which is why 2 ships; the shorter
  deadline was never justified by the measured failure mass.
- **Fan-out deadline check (always on — a defect fix, not tuning).** The fan-out send loop stops sending the
  remaining picked slots once `now_us() >= deadline_us`, so `udp_rate_limit_acquire` sleeps (up to 50 ms
  each, more when the bucket throttles) can no longer push `mean_lookup_ms` systematically above
  `tree_get_peers_deadline_ms` (measured 7,044 ms against a 6,000 ms deadline before the fix). It applies at
  legacy settings too, which is why it ships on.
- **Failure-counter taxonomy** — `/stats` `supervisor.metadata_failures`. `connect`, `handshake`, `stall`,
  `ut_reject`, `size`, `hash` and `other` are **per-connection** outcome events, each counted exactly once at
  the site that first detects the failure; `deadline` is **one event per attempt**, taken at the
  global-timeout break where every live slot is abandoned together. That asymmetry is why
  Σ counters ≥ failed attempts — do not "fix" it. Never count in `parallel_conn_close()`, in the
  post-process close (that reason was already counted inside `parallel_conn_process`) or in the final cleanup
  loop, or a single failure is counted twice. The get_peers exit/sweep counters
  (`idle_exits`, `deadline_exits`, `unanswered_slots`, `miss_penalties`, `slot_expired`, `early_finish`) are
  per-tree atomics folded into the supervisor at respawn like the other `gp_*` counters and published as
  `*_total`.

### Last-interval (windowed) rates (patch 20)

Every rate counter `/stats` publishes is cumulative since process start, so one snapshot cannot say how fast
anything is happening right now. Patch 20 adds seven **additive** keys to `aggregate` — the six
`*_last_interval` siblings of the lifetime counters plus `stats_interval_sec` — sampled by
`monitor_thread_func()` on its existing hard-coded 10 s tick (no new config key). `src/stats_window.c` is a
pure C99 module (`include/stats_window.h`, host-tested by `make tests-stats-window`) that keeps the previous
sample and reports the saturating per-counter difference: the first tick only records the baseline, so a
freshly started crawler publishes zeros with `stats_interval_sec` 0 instead of a spike, and a counter that
moves backwards reports 0 for that counter rather than wrapping. The sampler folds the same totals
`stats_handler()` publishes (cumulative respawn buckets + live trees) and publishes them through atomics on
`supervisor_t` — do not copy the non-atomic per-tree `metadata_rate` double, which is written by another
thread. `stats_interval_sec` is the *measured* elapsed time of the last completed interval, so
`rate = value / stats_interval_sec`. Lifetime keys, their percentages, `supervisor.get_peers.*` and the
eight `metadata_failures` counters are untouched; a respawn landing inside an interval can still make one
window spike, which is exactly why the lifetime totals stay beside the windowed ones.

### Per-address connect-failure memory (patch 21)

Measured baseline: ~83 % of the metadata failure mass is a TCP connect that never completes, and the peer
list handed to a fetch is arrival-ordered with **no address dedup**, so the same unreachable peer is
retried across infohashes and across trees. Patch 21 adds `tree_metadata_peer_blacklist_ttl_sec`
(default **0 = off**, opt-in clamp 60..3600) and `src/meta_peer_fail.c`
(`include/meta_peer_fail.h`, host-tested by `make tests-meta-peer-fail`): ONE supervisor-shared map —
131,072 slots × 32 B = 4 MiB, open addressing with FNV-1a + linear probing, a single `pthread_mutex_t`, and
lazy TTL ageing on the lookup/insert path only (no thread, no sweep; the load-factor clear at 3/4 = 98,304
slots is fail-open). The capacity is sized by the TTL working set — 131,072 × 3/4 = 98,304 clear threshold >
the ~66,000–79,800 entries a 600 s TTL accumulates at the measured ~110–133 NEW distinct fail addresses/s
(~23 % margin) — because at 8192 slots the table was wiped every ~50–70 s and `expired_total` stayed 0, i.e.
entries died by capacity clear and never by TTL (patch 22, in-tree as `META_PEER_FAIL_CAPACITY` in `include/meta_peer_fail.h`). It is deliberately the opposite of the infohash strike blooms: address-keyed, TTL-aged and
consulted at *selection* rather than at *outcome*.

- **Fail-open is absolute.** `ttl_sec = 0` never creates the map, so every lookup answers "do not skip" and
  every record is a no-op — peer selection is byte-identical to legacy. A creation failure is non-fatal and
  lands in the same disabled state. A cleared table is legacy selection too.
- **Additive recording only.** Every site that counts `metadata_failures.connect_total` (the seven direct
  `meta_count_fail(tree, META_FAIL_CONNECT)` sites plus `meta_count_conn_fail()` when it classifies CONNECT)
  also records the address. Handshake/stall/hash/deadline outcomes are NOT recorded, and the eight counters,
  the exit counters and the count-once discipline are untouched. `parallel_conn_t` carries the address
  (`peer`) because the poll-loop sites have no other handle on it.
- **Skip-to-empty guard (mandatory).** A zero-connection attempt returns NULL and feeds the irreversible
  infohash strike bloom, which would block an infohash for a *peer address's* history. Two guards: the
  selection cursor never passes the last candidate of an attempt, and an attempt whose whole window was
  skipped refills once with the map ignored (the cursor is NOT reset — re-trying an address already failed
  would inflate `connect_total` and make `R = connect_total/attempts` unreadable). With the map disabled
  neither guard can fire.
- **Counter units differ on purpose.** `metadata_peer_fail.metadata_peers_skipped_total` counts skipped
  **slots** (peers are not deduplicated, so one address can be skipped several times per attempt);
  `metadata_connect_fail_addresses_total` counts **NEW distinct addresses admitted**, not events.
- `/stats` publishes `metadata_peer_fail` (sibling of `inflight`): `enabled`, `capacity`, `held`, `ttl_sec`,
  `clears_total`, `expired_total` and the two counters. The map is supervisor-owned so it survives a tree
  respawn — which fires exactly when the metadata rate is low, i.e. when the memory matters most.

### BEP51 query + dedup funnel (patch 23)

Patch 23 (the last of the merged series) is **instrumentation only** — no config
key, no behaviour change. Eight per-tree atomics are published as `trees[].bep51.*` and summed as
`supervisor.bep51.*` (cumulative-folded at respawn like the `gp_*` counters):
`queries_sent_total` (the `sample_infohashes` send), `responses_received_total` and `timeouts_total` (the
two branches of the 1000 ms response wait), and the four mutually exclusive gates every examined sample
leaves through — `samples_offered_total`, `bloom_rejected_total` (shared-bloom hit), `inflight_refused_total`
(`inflight_set_claim()` refusal), `queue_full_total` (`tree_infohash_queue_try_push()` refused, claim
released) and `samples_pushed_total`. Per window
`offered = bloom_rejected + inflight_refused + queue_full + pushed` (±1 % for in-flight boundary effects),
which is the accounting identity to check first when reading a window: `tools/crawler/gp_probe.py` derives
`queries/s/tree`, `offered/s/tree`, the accepted/bloom/in-flight/queue-full shares and the identity gap from
both blocks. The three per-worker locals that used to hold these numbers are kept as log-throttle cursors,
so the DEBUG log sequence is unchanged.

Measured (`plan/getpeers-metadata-results.md`, W4/W5, same hour, one variable — `tree_bep51_workers` 3 vs
12): queries/s/tree 3.51 → 5.04 (+44 %) and offered/s/tree 44.34 → 65.78 (+48 %), but pushed fell
362,156 → 356,005 (−1.7 %) and lookups/s (249.1 → 246.4) and metadata/s (7.24 → 7.29) stayed flat, because
the accepted share fell 7.1 % → 4.7 % — bloom 65.9 → 62.1 % and in-flight 27.0 → 33.1 % refuse the extra
duplicates. Fill 3.73 → 7.14 /s/tree did not move the drain (3.13 → 3.14), so **BEP51 offer rate is not a
lookups/s lever**; the ceiling is the deadline-dominated drain, and `discovery_paused` rises with fill as the
90 %/33 % hysteresis predicts ((fill − drain)/fill ≈ 56 % predicted vs 68.1 % measured). The counters ship
as the durable win; no host test exists for `thread_tree.c`/`http_api.c`, so the identity was validated on
device.

### Respawn Logic

`supervisor.c:monitor_thread_func()` checks each tree's metadata rate (EMA-smoothed) every `rate_check_interval_sec`. Trees below `min_metadata_rate - dynamic_rate_margin` for longer than `rate_grace_period_sec` are respawned. During respawn, the old tree is moved to a "draining" list and destroyed once its active connections drop below `respawn_spawn_threshold` (or after `respawn_drain_timeout_sec`).

Cumulative stats (metadata count, filtered count, failures) are carried across respawns via `atomic_uint_fast64_t` fields on the supervisor.

### Key Files

| File | Role |
|------|------|
| `src/main.c` | Entry point, signal handling, initialises all subsystems in order |
| `src/supervisor.c` | Tree lifecycle, monitor thread, global bootstrap |
| `src/thread_tree.c` | Single tree: phase transitions, worker thread spawning |
| `src/tree_protocol.c` | DHT message encode/decode (find_node, BEP51, get_peers) |
| `src/tree_routing.c` | Kademlia k-bucket routing table |
| `src/tree_dispatcher.c` | Routes UDP responses to waiting workers by transaction ID |
| `src/tree_metadata.c` | BEP9/10 TCP metadata fetcher |
| `src/batch_writer.c` | Buffered SQLite inserts, bloom persistence, backup triggers |
| `src/bep51_cache.c` | Persistent FIFO cache of BEP51-capable nodes |
| `src/database.c` | SQLite schema (`torrents` + `files` tables), WAL mode |
| `src/http_api.c` | CivetWeb REST server (`/stats`, `/search`, `/refresh`) |
| `src/refresh_thread.c` | Dedicated thread for `/refresh?infohash=` endpoint |
| `src/config.c` | INI parser → `crawler_config_t` (~130 keys) |

### Shared vs. Per-Tree State

Per-tree: routing table, infohash queue, peers queue, response queue, socket handle (when using ephemeral port), worker thread handles, rate counters.

Shared (protected): batch writer lock, bloom filter (read-write via supervisor-coordinated access), BEP51 cache mutex, shared dispatcher hash table (mutex per bucket).

### Bloom Filter Design

Two separate bloom filters:
- **`data/bloom.dat`** — infohash deduplication; updated only after successful DB write (avoids blocking retries on transient failures)
- **`data/failure_bloom.dat`** — two-strike failure policy; infohashes that fail metadata fetch twice are suppressed

Both are persisted to disk on each batch flush and reloaded on startup.

## Configuration

`config.ini` is the single configuration source. Key groups:

- `num_trees`, `use_keyspace_partitioning` — tree count and partitioning
- `tree_*_workers` — worker counts per tree (total threads ≈ num_trees × ~140)
- `bloom_capacity`, `failure_bloom_capacity` — filter sizes (~500 MB total at 30M)
- `batch_size`, `flush_interval` — write batching
- `language_filter_non_latin_threshold`, `hard_block_file` — content filtering
- `bep51_cache_*` — bootstrap cache settings
- `min_metadata_rate`, `dynamic_rate_margin`, `rate_*` — respawn thresholds

Bootstrap router list is hardcoded in `src/supervisor.c` (search for `BOOTSTRAP_URLS`).

## HTTP API

```bash
curl http://localhost:8080/stats
curl "http://localhost:8080/search?q=ubuntu"
curl "http://localhost:8080/refresh?infohash=<40-hex>"
```

## Database

```bash
sqlite3 data/torrents.db "SELECT name FROM torrents ORDER BY id DESC LIMIT 10"
```

Schema: `torrents(id, info_hash UNIQUE, name, discovered_at, file_count, file_info JSON)`.
