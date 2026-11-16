# config.ini — get_peers & metadata-fetch tuning (patch 19)

Scope: this page documents the `config.ini` keys that crawler **patch 19**
(`patches/getpeers-metadata-tuning.patch`) added or changed, with their defaults, accepted ranges and
semantics. It is not a full `config.ini` reference — for the complete key inventory read
`src/config.c` (`config_load_file`) and the sanitized app-side template
`app/src/main/assets/dht_crawler/config.ini.default`, which carries these five keys with comments.

All values are parsed with `atoi`, so `= true` parses as `0`. For the three new keys, **`0` means
"disabled / pre-patch behaviour"**; every one of them is fail-open to the old code path.

| key | default | accepted | effect |
|---|---:|---|---|
| `tree_get_peers_round_idle_ms` | `1500` | `0`, or `250..6000` | end a get_peers round after this much silence since the last answer |
| `tree_get_peers_penalize_misses` | `1` | `0`/`1` (any non-zero value is coerced to `1`) | charge unanswered round slots against the silent node |
| `tree_metadata_piece_idle_ms` | `15000` | `0`, or `3000..60000` | evict a metadata connection that copies no new piece for this long |
| `tree_parallel_peers` | `4` (was `2`) | `1..4` (was `1..10`) | parallel peer connections per infohash |
| `tree_metadata_timeout_ms` | `30000` (was `60000`) | `5000..600000` (unchanged) | per-tree metadata fetch deadline |

The clamps live in `src/config.c` (`config_load_file`); out-of-range values are clamped and logged at
`LOG_WARN` rather than rejected.

## tree_get_peers_round_idle_ms

Per-round silence cap for `tree_lookup_peers()` (`src/thread_tree.c`). The timer starts with the fan-out
and is refreshed by **every matched answer**, so it measures silence since the last answer, not the age of
the round: a round that is still producing responses is never cut short. When a round ends on this cap
with **zero** answers, the whole lookup terminates instead of walking the remaining candidate set — dead
lookups stop burning the full `tree_get_peers_deadline_ms` (default 6000 ms) and stop spending further
packets.

- `0` disables it: a round is bounded only by the per-infohash deadline (pre-patch behaviour).
- Any other value is clamped into `250..6000` (a negative value becomes `0`).
- Echoed by `/stats` as `supervisor.get_peers.round_idle_ms` and `trees[].get_peers.round_idle_ms`.

## tree_get_peers_penalize_misses

When enabled, the round-end sweep charges every unanswered fan-out slot against its node through
`tree_routing_mark_failed()` (`src/tree_routing.c`): the node's `fail_count` is incremented and the node is
evicted at `MAX_FAIL_COUNT` (`3`). Silent nodes therefore stop being re-selected, and the find_node workers
refill the routing table. Answered slots are zeroed in the drain loop and failed sends never receive a
transaction ID, so only genuinely unanswered queries are charged.

- `0` disables the penalty (pre-patch behaviour); any positive value is coerced to `1`.
- Echoed by `/stats` as `supervisor.get_peers.penalize_misses` and `trees[].get_peers.penalize_misses`.

## tree_metadata_piece_idle_ms

Piece-progress eviction for the BEP9/10 fetcher (`src/tree_metadata.c`). `last_piece_time` is refreshed
**only** when a new metadata data piece is copied into the shared buffer, so a peer that keeps the socket
warm with keep-alive/have/bitfield traffic but delivers no pieces is closed once this much time passes
without new data. The pre-existing 10 s total-silence watchdog (`CONN_SILENCE_CLOSE_MS`) still catches
fully silent peers and fires first, so no event is counted twice; the refill loop then advances the fetch
window to the next untried peer.

- `0` disables the eviction (pre-patch behaviour).
- Any non-zero value is clamped into `3000..60000`; a negative value therefore becomes `3000`, not `0`.
- Echoed by `/stats` as `supervisor.metadata.piece_idle_ms`.

## Changed defaults

`tree_parallel_peers` now defaults to **4** and its clamp narrowed from `1..10` to **`1..4`**, matching
`MAX_PARALLEL_PEERS` in `src/tree_metadata.c` — the accepted value is now the effective one instead of being
silently clamped deeper in the fetcher. This adds TCP connections inside the existing metadata workers'
single poll loop; it does **not** add threads.

`tree_metadata_timeout_ms` now defaults to **30000** (was `60000`). With piece-idle eviction in place the
global deadline is a backstop rather than the common exit; `/stats` `supervisor.metadata_failures.deadline_total`
shows how often it actually fires.

Both changed defaults govern fresh installs and absent keys only: `config.ini` is written by the app's
settings form, and an existing file keeps whatever it already pins.

## Where these show up in `/stats`

`supervisor.metadata` echoes the effective metadata knobs of the first live tree — `parallel_peers`,
`metadata_timeout_ms`, `metadata_workers`, `tcp_connect_timeout_ms`, `fresh_peer_retries`, `piece_idle_ms` —
so a snapshot always carries the settings it was measured under. The get_peers knobs are echoed per tree
(`trees[].get_peers.deadline_ms/fanout/max_queries/round_idle_ms/penalize_misses`).
