# Stage 1 — Strip dead `crawler_config_t` fields

Goal: remove the 22 fields that `use_thread_trees=1` already bypasses from
`crawler_config_t` and from `config_init_defaults` + `config_load_file`. No
other source file changes. `make` must build clean.

Files in scope: `include/config.h`, `src/config.c`. Nothing else.

---

## 1. `include/config.h` — struct edit

Drop these 22 fields from `crawler_config_t` (current line numbers in
parentheses):

| # | Field | Line | Notes |
|---|---|---|---|
| 1 | `targeted_search_percentage` | 12 | never read |
| 2 | `http_port` | 15 | overridden by `HTTP_API_PORT` |
| 3 | `scaling_factor` | 32 | old worker pool |
| 4 | `metadata_workers` | 33 | only `metadata_fetcher.c:214` |
| 5 | `max_retry_attempts` | 41 | never read |
| 6 | `retry_delay_sec` | 42 | never read |
| 7 | `max_metadata_size_mb` | 40 | never read |
| 8 | `batch_writes_enabled` | 45 | never read |
| 9 | `wbpxre_ping_workers` | 68 | never read |
| 10 | `wbpxre_find_node_workers` | 69 | never read |
| 11 | `wbpxre_sample_infohashes_workers` | 70 | never read |
| 12 | `wbpxre_get_peers_workers` | 71 | never read |
| 13 | `wbpxre_query_timeout` | 72 | never read |
| 14 | `max_routing_table_nodes` | 73 | never read |
| 15 | `peer_retry_enabled` | 76 | never read |
| 16 | `peer_retry_max_attempts` | 77 | never read |
| 17 | `peer_retry_min_threshold` | 78 | never read |
| 18 | `peer_retry_delay_ms` | 79 | never read |
| 19 | `peer_retry_cleanup_interval_sec` | 80 | never read |
| 20 | `peer_retry_max_entries` | 81 | never read |
| 21 | `triple_routing_threshold` | 84 | never read |
| 22 | `triple_routing_rotation_time` | 85 | never read |
| 23 | `use_thread_trees` | 144 | toggle becomes unnecessary once only one path remains |

Field #23 (`use_thread_trees`) is removed as a struct member; stage 3 will
delete the `if (config.use_thread_trees)` branch from `main.c`. Keep the
field temporarily if needed to compile, then delete with stage 3.

Strip the comment headers that group these fields ("Phase 2", "Phase 4",
"Phase 5", "Worker Pool", "Metadata fetcher settings", "wbpxre-dht settings",
"Peer discovery retry settings", "Triple routing table settings",
"Thread tree mode toggle") when they are now empty.

After the edit, add one block comment above the kept struct fields:

```c
/*
 * Live fields. The 22 dead fields removed in Stage 1 are documented in
 * CONFIG_INI.md (one-pager post-Stage 6). When auditing this struct, the
 * consumers are:
 *   - main.c (dht_port, db_path, log_level, bloom_*, batch_size, backup_*,
 *     ssh_*, rclone_*, porn_filter_*, language_filter_*)
 *   - supervisor.c (dht_port, num_trees, tree_*, bep51_*, refresh_*,
 *     respawn_*, min_metadata_rate, dynamic_rate_margin, etc.)
 *   - tree_*.c (tree_* via supervisor config)
 *   - tree_metadata.c (tree_tcp_connect_timeout_ms, tree_parallel_peers)
 *   - batch_writer.c (batch_size, flush_interval, backup_path)
 *   - refresh_thread.c (refresh_*)
 *   - language_filter.c (language_filter_non_latin_threshold)
 *   - porn_filter.c (porn_filter_enabled, porn_filter_keyword_file)
 *   - failure_bloom filter (failure_bloom_capacity)
 */
```

This comment is the cross-reference `PLAN.md` asks for so future audits
do not need to re-run `CONFIG_INI.md` generation.

---

## 2. `src/config.c` — defaults edit (`config_init_defaults`)

Remove the defaulting blocks for the 22 fields. The current grouping
labels (and their line ranges) to delete:

| Block | Lines | Removes |
|---|---|---|
| Discovery defaults | 19-20 | `targeted_search_percentage` |
| HTTP defaults | 22-23 | `http_port` |
| Phase 4 worker pool | 39-41 | `scaling_factor`, `metadata_workers` |
| Metadata fetcher | 43-50 | `max_concurrent_connections`, `tcp_connect_timeout_sec`, `connection_timeout_sec`, `max_connection_lifetime_sec`, `max_metadata_size_mb`, `max_retry_attempts`, `retry_delay_sec` |
| Phase 5 batch writer | 52-55 | `batch_writes_enabled` (keep `batch_size`, `flush_interval`) |
| wbpxre-dht | 77-83 | `wbpxre_ping_workers`, `wbpxre_find_node_workers`, `wbpxre_sample_infohashes_workers`, `wbpxre_get_peers_workers`, `wbpxre_query_timeout`, `max_routing_table_nodes` |
| Peer retry | 85-91 | `peer_retry_*` (6 fields) |
| Triple routing | 93-95 | `triple_routing_threshold`, `triple_routing_rotation_time` |
| Thread tree mode toggle | 153-154 | `use_thread_trees` |

`config_print` (lines 640-641) prints `targeted_search_percentage` and
`http_port` — remove those two `log_msg` lines. Update the comment header
above the function if it still references "Discovery Settings" or "HTTP
API Settings" sections that are now empty.

Note: the four `metadata_fetcher.c`-only fields
(`max_concurrent_connections`, `tcp_connect_timeout_sec`,
`connection_timeout_sec`, `max_connection_lifetime_sec`) are NOT in the
PLAN §1 list of 22 — leave them in `crawler_config_t` through stages 1-2
so `metadata_fetcher.c` still compiles. Stage 2 will delete
`metadata_fetcher.c`; stage 6 cleanup is responsible for removing the
resulting-dead config struct fields. Flag this in stage 6.

---

## 3. `src/config.c` — parser edit (`config_load_file`)

Remove the corresponding `else if (strcmp(key, "...") == 0)` blocks.
Line ranges to delete:

| Block | Lines | Removes |
|---|---|---|
| `targeted_search_percentage` | 234-235 | |
| `http_port` | 236-237 | |
| `scaling_factor` / `metadata_workers` | 266-269 | |
| `batch_writes_enabled` | 272-273 | |
| `max_concurrent_connections` / `tcp_connect_timeout_sec` / `connection_timeout_sec` / `max_connection_lifetime_sec` / `max_metadata_size_mb` / `max_retry_attempts` / `retry_delay_sec` | 304-317 | (KEEP per note in §2) |
| `wbpxre_ping_workers` / `wbpxre_find_node_workers` / `wbpxre_sample_infohashes_workers` / `wbpxre_get_peers_workers` / `wbpxre_query_timeout` / `max_routing_table_nodes` | 320-331 | |
| `peer_retry_*` (6 fields) | 333-347 | |
| `triple_routing_threshold` / `triple_routing_rotation_time` | 349-358 | |
| `use_thread_trees` | 490-492 | (KEEP until stage 3 deletes the branch in `main.c`) |

Comments above each block (e.g. `/* wbpxre-dht settings */`) should be
removed when their bodies are removed.

---

## 4. `src/config.c` — CLI parser edit (`config_parse_args`)

- `case 'h':` at line 585-587 sets `config->http_port`; remove this case
  AND remove the `{"http-port", required_argument, 0, 'h'}` long-option
  entry at line 568 AND the usage text at line 616
  (`"  -h, --http-port PORT     HTTP API port (default: 8080)\n"`).
- `case 'a':` ("aggressive") at lines 572, 607-610 is documented as
  "deprecated, no longer used"; delete the option, the case, and the
  usage line 620.
- `static struct option long_options[]` (lines 566-575) keeps
  `{"port", 'p'}`, `{"db-path", 'd'}`, `{"log-level", 'l'}`,
  `{"config", 'c'}`, `{"help", '?'}`.

---

## 5. `include/dht_crawler.h` — constants

PLAN §1 names these as removed in this stage. The macros
`MAX_WORKERS`, `INFOHASH_QUEUE_SIZE`, `BATCH_SIZE` are read at:

- `src/main.c:394,395` (`INFOHASH_QUEUE_SIZE`) — used in the old
  architecture branch which stage 1 does not yet touch. Removing the
  macro in stage 1 will break the build until stage 3.
- `src/dht_manager.c:108` (`BATCH_SIZE` in `MAX_BATCH_SIZE` constant) —
  file deleted in stage 2, so safe to remove macro then.
- `MAX_WORKERS` is not referenced anywhere in the tree.

Therefore: keep the three macros in `dht_crawler.h` through stage 1.
Stage 3 will delete them as part of dropping the old-architecture
branch in `main.c`. (This deviates from PLAN §1's literal wording but
preserves the "stage 1 builds clean" invariant the plan demands.)

---

## 6. `config.ini` (out of scope for stage 1; flag for stage 6)

The 22 keys listed in `config.ini` continue to parse and store
silently after stage 1. This is by design — the user has a running
install. Stage 6 strips the keys and renames `global_bootstrap_*` to
`tree_bootstrap_*`. Do not touch `config.ini` here.

---

## Acceptance

- `grep -nE 'targeted_search_percentage|http_port|scaling_factor|metadata_workers|max_retry_attempts|retry_delay_sec|max_metadata_size_mb|batch_writes_enabled|wbpxre_|peer_retry_|triple_routing_|use_thread_trees' include/config.h` returns no hits.
- `make` succeeds.
- `dht_crawler` still starts and reads the live config keys.
- No source file outside `include/config.h` and `src/config.c` is
  modified.
