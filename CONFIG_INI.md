# config.ini — get_peers & metadata-fetch tuning (patch 19)

Scope: this page documents the `config.ini` keys that crawler **patch 19**
added or changed, with their shipped defaults, accepted ranges
and semantics. It is not a full `config.ini` reference — for the complete key inventory read
`src/config.c` (`config_load_file`) and the sanitized app-side template
`app/src/main/assets/dht_crawler/config.ini.default`, which carries these seven keys with comments.

Patch 21 (in-tree) adds one key to this page,
`tree_metadata_peer_blacklist_ttl_sec` — the per-address TCP connect-failure memory — under the same
fail-open contract: `0` is the shipped value and restores legacy peer selection exactly. The two later
patches of this run add **no key**: patch 22 only resizes that
map's capacity, and patch 23 only adds `/stats` counters — see the
last section for both.

Not covered here: the published-cache download keys (`bep51_cache_url`,
`bep51_cache_fetch_timeout_sec`, `bep51_cache_ca_file`, `bep51_cache_ca_dir`) — see
the "BEP51 cache" section of `README.md`.

All values are parsed with `atoi`, so `= true` parses as `0`. **Every one of these keys ships at its legacy
value — the tuning is opt-in.** Five of the seven treat `0` as "disabled / pre-patch behaviour"; all of them
are fail-open to the old code path. The shipped defaults are the measured result: five 30-minute on-device
windows showed the tuning to be net-negative (TUNED vs LEGACY on the same build in the same hour: lookups/s
−1.5 %, yield −3.4 pp, packets/lookup +6 %, metadata/s −13 %), while the instrumentation that produced that
finding is what patch 19 actually ships enabled. See the run report `measurements.md` for the numbers.

| key | shipped default | accepted | effect |
|---|---:|---|---|
| `tree_get_peers_round_idle_ms` | `0` (off) | `0`, or `250..6000` | end a get_peers round after this much silence since the last answer |
| `tree_get_peers_penalize_misses` | `0` (off) | `0`/`1` (any non-zero value is coerced to `1`) | charge unanswered round slots against the silent node |
| `tree_get_peers_slot_budget_ms` | `0` (off) | `0`, or `500..6000` | stop waiting on a slot once its own answer budget has run out |
| `tree_get_peers_finish_on_peers` | `0` (off) | `0`/`1` (any non-zero value is coerced to `1`) | end a lookup as soon as it has peers |
| `tree_metadata_piece_idle_ms` | `0` (off) | `0`, or `3000..60000` | evict a metadata connection that copies no new piece for this long |
| `tree_metadata_peer_blacklist_ttl_sec` | `0` (off) | `0`, or `60..3600` | remember a peer address whose TCP connect failed and step over it when picking metadata peers (patch 21) |
| `tree_parallel_peers` | `2` (legacy) | `1..4` (clamp narrowed from `1..10`) | parallel peer connections per infohash |
| `tree_metadata_timeout_ms` | `60000` (legacy) | `5000..600000` (unchanged) | per-tree metadata fetch deadline |

The clamps live in `src/config.c` (`config_load_file`); out-of-range values are clamped and logged at
`LOG_WARN` rather than rejected. The `tree_parallel_peers` clamp was narrowed to `1..4` as a **correctness
fix** — `MAX_PARALLEL_PEERS` in `src/tree_metadata.c` silently clamped deeper in the fetcher, so the accepted
value was not the effective one. That fix stays regardless of the default.

## tree_get_peers_round_idle_ms

Per-round silence cap for `tree_lookup_peers()` (`src/thread_tree.c`). The timer starts with the fan-out
and is refreshed by **every matched answer**, so it measures silence since the last answer, not the age of
the round: a round that is still producing responses is never cut short. When a round ends on this cap
with **zero** answers, the whole lookup terminates instead of walking the remaining candidate set.

- `0` (shipped) disables it: a round is bounded only by the per-infohash deadline (pre-patch behaviour).
- Any other value is clamped into `250..6000` (a negative value becomes `0`).
- Echoed by `/stats` as `supervisor.get_peers.round_idle_ms` and `trees[].get_peers.round_idle_ms`.
- Measured on device: with `1500` the mechanism was **inert** — 245 idle exits against 246,380 deadline
  exits over a full window, because answers trickle in faster than 1.5 s. This is why it ships off.

## tree_get_peers_penalize_misses

When enabled, the round-end sweep charges every unanswered fan-out slot against its node through
`tree_routing_mark_failed()` (`src/tree_routing.c`): the node's `fail_count` is incremented and the node is
evicted at `MAX_FAIL_COUNT` (`3`). Silent nodes therefore stop being re-selected, and the find_node workers
refill the routing table. The slot carries the queried node's 20-byte id captured at send time (not a
candidate-array index), so a same-round merge cannot misdirect the penalty. A matched answer re-adds the node
via `tree_routing_add_node()`, which resets `fail_count` — with the knob on, eviction means "repeatedly
silent and never answering". Answered slots are zeroed in the drain loop and failed sends never receive a
transaction ID, so only genuinely unanswered queries are charged.

- `0` (shipped) disables the penalty (pre-patch behaviour); any positive value is coerced to `1`.
- Echoed by `/stats` as `supervisor.get_peers.penalize_misses` and `trees[].get_peers.penalize_misses`.
- Measured on device: at `1` the ~1,000 penalties/s recycled the routing tables several times per window and
  yield fell **10.8 pp** (36.9 % → 26.1 %) without collapsing the tables. Hence the shipped `0`.

## tree_get_peers_slot_budget_ms

Per-slot answer budget in `tree_lookup_peers()`. Every sent slot records its own `sent_us`; the drain loop
stops once no remaining slot is still inside its budget (all answered or all expired), which ends the round
even when another slot keeps trickling answers. If the round ends with zero answers the lookup terminates.
The sweep counts the expired slots into `gp_slot_expired_total`, a subset of `gp_unanswered_slots_total`, so
`slot_expired_total ≤ unanswered_slots_total` always holds.

- `0` (shipped) disables it: slots are bounded only by the per-infohash deadline (pre-patch behaviour).
- Any other value is clamped into `500..6000` (a negative value becomes `0`).
- Echoed by `/stats` as `supervisor.get_peers.slot_budget_ms` and `trees[].get_peers.slot_budget_ms`;
  `slot_expired_total` is published per tree and aggregated by the supervisor.
- Built to replace the falsified round-idle premise; measured net-negative at `2000` (TUNED vs LEGACY), so it
  ships off.

## tree_get_peers_finish_on_peers

When on, a lookup that already has peers drains the packets already queued (timeout-0 pops, so concurrent
answers are not lost) and ends instead of waiting for every fan-out slot or the full deadline.
`gp_early_finish_total` counts the lookups terminated this way.

- `0` (shipped) keeps the legacy wait-for-all behaviour; any positive value is coerced to `1`.
- Echoed by `/stats` as `supervisor.get_peers.finish_on_peers` and `trees[].get_peers.finish_on_peers`.
- The trade-off is real and was measured: an entry then carries one responder's peer list instead of up to
  eight, which costs peer-list width for latency. Ships off.

## tree_metadata_piece_idle_ms

Piece-progress eviction for the BEP9/10 fetcher (`src/tree_metadata.c`). `last_piece_time` is refreshed
**only** when a new metadata data piece is copied into the shared buffer, so a peer that keeps the socket
warm with keep-alive/have/bitfield traffic but delivers no pieces is closed once this much time passes
without new data. The pre-existing 10 s total-silence watchdog (`CONN_SILENCE_CLOSE_MS`) still catches
fully silent peers and fires first, so no event is counted twice; the refill loop then advances the fetch
window to the next untried peer.

- `0` (shipped) disables the eviction (pre-patch behaviour).
- Any non-zero value is clamped into `3000..60000`; a negative value therefore becomes `3000`, not `0`.
- Echoed by `/stats` as `supervisor.metadata.piece_idle_ms`.
- Measured: `stall_total` is 0.8 % of the metadata failure mass and `deadline_total` 0.1–0.2 % across every
  window, i.e. this knob addresses ≤1 % of the failures. Ships off.

## tree_metadata_peer_blacklist_ttl_sec

Per-address TCP connect-failure memory for the BEP9/10 fetcher (patch 21,
Measured baseline: ~83 % of the metadata failure mass is a TCP
connect that never completes, and the peer list handed to a fetch is arrival-ordered with no dedup, so the
same unreachable address is retried across infohashes and across trees. Every site that counts
`metadata_failures.connect_total` also records the *address* into one supervisor-shared map
(`src/meta_peer_fail.c`: 131,072 slots × 32 B = 4 MiB, open addressing, one `pthread_mutex_t`, lazy TTL
ageing, no thread). Window selection then steps over an address the map still remembers, counting each
skipped **slot** in `metadata_peer_fail.metadata_peers_skipped_total` (peers are not deduplicated, so one
address can be skipped several times per attempt).

- `0` (shipped) does not even create the map: every lookup answers "do not skip" and every record is a
  no-op, so peer selection is byte-identical to legacy. A creation failure is non-fatal and lands in the
  same disabled state.
- Any non-zero value is clamped into `60..3600` and logged at `LOG_WARN`; `0` is never clamped.
- Only **connect** failures are remembered — handshake, stall, hash and deadline outcomes say nothing about
  reachability, and the eight `metadata_failures` counters keep their exact count-once meaning (the record
  is additive).
- **Skip-to-empty guard (mandatory):** the selection cursor never passes the last candidate of an attempt,
  and an attempt whose whole window was skipped refills once with the map ignored. A zero-connection
  attempt would reach the irreversible infohash strike bloom and block the infohash because of a *peer
  address's* history. With the map disabled the guard cannot fire.
- Load-factor clear at 3/4 (98,304 live addresses) is fail-open: a cleared table means legacy selection.
  The capacity is sized so that threshold exceeds the whole 600 s TTL working set — measured ~110–133 NEW
  distinct fail addresses/s × 600 s = 66,000–79,800 live entries, ~23 % under 98,304 — so entries age out by
  TTL instead of by a table wipe. (The original 8192 slots cleared every ~50–70 s at that rate, leaving
  `expired_total` at 0; see `reports/w3-sizing-blocker.md`.) 4 MiB is ~0.2 % of the measured 2.04 GB
  crawler RSS.
- Echoed by `/stats` as `metadata_peer_fail`: `enabled`, `capacity`, `held`, `ttl_sec`, `clears_total`,
  `expired_total`, `metadata_peers_skipped_total`, `metadata_connect_fail_addresses_total` (the last one
  counts NEW distinct addresses admitted, not events). A disabled map publishes `enabled: false`,
  `ttl_sec: 0` and zero counters.

## Changed defaults

The two pre-existing keys keep their **legacy** values in the shipped patch, and their clamps are unchanged
except for the `tree_parallel_peers` correctness fix noted above:

- `tree_parallel_peers` ships at **2**, and that is now a **measured** choice, not an untested default: the
  same-hour, same-build A/B (`plan/getpeers-metadata-results.md`, W0/W1) put pp=2 at 6.834 metadata/s against
  pp=4 at 6.408 (−6.2 %, outside the <5 % tie band) with more connect events and more attempts for pp=4 and
  no more fetches. The `1..4` clamp stays (accepted == effective).
- `tree_metadata_timeout_ms` ships at **60000**. The `30000` experiment was confounded with the get_peers
  knobs in the only contemporaneous comparison, and the deadline accounts for ≤0.2 % of the failure mass, so
  it is not justified by measurement either.

Both govern fresh installs and absent keys only: `config.ini` is written by the app's settings form, and an
existing file keeps whatever it already pins. Any of these keys can be re-enabled as an advanced
override in the app's crawler settings (they are on the Kotlin allow-list) — re-measure, do not assume.

## Where these show up in `/stats`

`supervisor.metadata` echoes the effective metadata knobs of the first live tree — `parallel_peers`,
`metadata_timeout_ms`, `metadata_workers`, `tcp_connect_timeout_ms`, `fresh_peer_retries`, `piece_idle_ms` —
so a snapshot always carries the settings it was measured under. The get_peers knobs are echoed per tree
(`trees[].get_peers.deadline_ms/fanout/max_queries/round_idle_ms/penalize_misses/slot_budget_ms/finish_on_peers`).

### `/stats` blocks from patches 20–23 (no config keys involved)

**`aggregate.*_last_interval` (patch 20).** Seven additive keys beside the lifetime counters —
`metadata_attempts_last_interval`, `metadata_fetched_last_interval`,
`metadata_fetch_success_percent_last_interval`, `get_peers_lookups_last_interval`,
`get_peers_lookups_with_peers_last_interval`, `get_peers_yield_percent_last_interval` and
`stats_interval_sec` — sampled on the supervisor monitor thread's existing hard-coded 10 s tick (there is no
config key). `stats_interval_sec` is the *measured* elapsed time of the last completed interval and is 0
before the first sample, when every delta is also 0, so `rate = value / stats_interval_sec`. The lifetime
keys and their percentages are untouched; a tree respawn landing inside an interval can still spike one
window, which is exactly why both forms are published.

**`metadata_peer_fail` (patches 21/22).** Sibling of `inflight`: `enabled`, `capacity`, `held`, `ttl_sec`,
`clears_total`, `expired_total`, `metadata_peers_skipped_total`, `metadata_connect_fail_addresses_total`.
`capacity` echoes the compiled constant (**131,072**), so patch 22's resize needed no schema change; a
disabled map publishes `enabled: false`, `ttl_sec: 0` and zero counters.

**`trees[].bep51` + `supervisor.bep51` (patch 23).** `queries_sent_total`, `responses_received_total`,
`timeouts_total`, `samples_offered_total`, `bloom_rejected_total`, `inflight_refused_total`,
`queue_full_total`, `samples_pushed_total`, per tree and summed. The four sample gates are mutually
exclusive, so per window `offered = bloom_rejected + inflight_refused + queue_full + pushed` (±1 %);
`tools/crawler/gp_probe.py` derives `queries/s/tree`, `offered/s/tree`, the accepted / bloom / in-flight /
queue-full shares of offered and the identity gap from the two blocks.
