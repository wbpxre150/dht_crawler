# Unused config.ini Settings Report

Fields in `config.ini` that are parsed but **never read** in any source file outside `config.c`.
All values below are confirmed by AST search of `src/` and `include/`.

---

## 1. `targeted_search_percentage`

- **INI line:** (not present — default only, set in `config_init_defaults` to 30)
- **Status:** NEVER USED. No source file reads `config->targeted_search_percentage`.
- **Impact:** Zero. This field does nothing.

---

## 2. `http_port`

- **INI line:** `http_port=8080`
- **Config struct:** `int http_port;` (line 15 of config.h)
- **Config parsing:** `config_load_file` line 237.
- **Status:** NEVER READ. The HTTP API uses a hardcoded `#define HTTP_API_PORT 8080` in `include/http_api.h:16`.
- **Impact:** The config value is stored but never applied. HTTP API always listens on port 8080.

---

## 3. `scaling_factor`

- **INI line:** `scaling_factor=10`
- **Config struct:** `int scaling_factor;` (line 32 of config.h)
- **Config parsing:** `config_load_file` line 267.
- **Status:** NEVER READ. Phase 4 worker pool architecture was replaced by thread trees.
- **Impact:** Zero. This is legacy from the old worker pool design.

---

## 4. `metadata_workers`

- **INI line:** `metadata_workers=200`
- **Config struct:** `int metadata_workers;` (line 33 of config.h)
- **Config parsing:** `config_load_file` line 269.
- **Status:** READ in `src/metadata_fetcher.c:214` only. Used in the **old architecture** (`use_thread_trees=0`).
- **Impact:** No effect when `use_thread_trees=1`. Replaced by `tree_metadata_workers=75` in thread tree mode.

---

## 5. `max_retry_attempts`

- **INI line:** (not present — default only, set in `config_init_defaults` to 3)
- **Config struct:** `int max_retry_attempts;` (line 41 of config.h)
- **Config parsing:** `config_load_file` line 315.
- **Status:** NEVER READ. No source file reads `config->max_retry_attempts`.
- **Impact:** Zero.

---

## 6. `retry_delay_sec`

- **INI line:** (not present — default only, set in `config_init_defaults` to 120)
- **Config struct:** `int retry_delay_sec;` (line 42 of config.h)
- **Config parsing:** `config_load_file` line 317.
- **Status:** NEVER READ. No source file reads `config->retry_delay_sec`.
- **Impact:** Zero.

---

## 7. `max_metadata_size_mb`

- **INI line:** `max_metadata_size_mb = 100`
- **Config struct:** `int max_metadata_size_mb;` (line 40 of config.h)
- **Config parsing:** `config_load_file` line 313.
- **Status:** NEVER READ. No source file reads `config->max_metadata_size_mb`.
- **Impact:** Zero. This was likely intended for metadata size validation in the metadata fetcher but was never implemented.

---

## 8. `batch_writes_enabled`

- **INI line:** `batch_writes_enabled=1`
- **Config struct:** `int batch_writes_enabled;` (line 45 of config.h)
- **Config parsing:** `config_load_file` line 273.
- **Status:** NEVER READ. No source file reads `config->batch_writes_enabled`.
- **Impact:** Zero. The batch writer is always enabled when used; this toggle has no code branch.

---

## 9. `ssh_backup_enabled` through `ssh_bookmark_path` (SSH backup)

- **INI lines:** `ssh_backup_enabled=0` through `ssh_bookmark_path=data/ssh_backup_ids.txt`
- **Config struct:** `ssh_backup_enabled`, `ssh_host`, `ssh_user`, `ssh_dest_path`, `ssh_key_path`, `ssh_bookmark_path` (lines 54-59 of config.h)
- **Config parsing:** `config_load_file` lines 283-293.
- **Status:** READ in `src/main.c:663-668` — but only when `use_thread_trees=1`.
- **Impact:** Config is parsed and used correctly in thread tree mode. No issue here.

---

## 10. `rclone_backup_enabled` through `rclone_bookmark_path` (rclone backup)

- **INI lines:** `rclone_backup_enabled=1` through `rclone_bookmark_path=data/rclone_backup_ids.txt`
- **Config struct:** `rclone_backup_enabled`, `rclone_remote`, `rclone_dest_path`, `rclone_bookmark_path` (lines 62-65 of config.h)
- **Config parsing:** `config_load_file` lines 295-301.
- **Status:** READ in `src/main.c:672-675` — but only when `use_thread_trees=1`.
- **Impact:** Config is parsed and used correctly in thread tree mode. No issue here.

---

## 11. wbpxre-dht settings (ALL 6 fields)

| Setting | INI line | Status |
|---------|----------|--------|
| `wbpxre_ping_workers` | 98 | NEVER READ |
| `wbpxre_find_node_workers` | 99 | NEVER READ |
| `wbpxre_sample_infohashes_workers` | 100 | NEVER READ |
| `wbpxre_get_peers_workers` | 101 | NEVER READ |
| `wbpxre_query_timeout` | 102 | NEVER READ |
| `max_routing_table_nodes` | (not in ini, default only) | NEVER READ |

- **Config struct:** `wbpxre_ping_workers`, `wbpxre_find_node_workers`, `wbpxre_sample_infohashes_workers`, `wbpxre_get_peers_workers`, `wbpxre_query_timeout`, `max_routing_table_nodes` (lines 68-73 of config.h)
- **Config parsing:** `config_load_file` lines 321-331.
- **Status:** ALL NEVER READ. No source file reads any of these.
- **Impact:** Zero. The wbpxre-dht library was completely replaced by the thread tree architecture. These are dead code.

---

## 12. Peer discovery retry settings (ALL 6 fields)

| Setting | INI line | Status |
|---------|----------|--------|
| `peer_retry_enabled` | 109 | NEVER READ |
| `peer_retry_max_attempts` | 110 | NEVER READ |
| `peer_retry_min_threshold` | 111 | NEVER READ |
| `peer_retry_delay_ms` | 112 | NEVER READ |
| `peer_retry_cleanup_interval_sec` | 113 | NEVER READ |
| `peer_retry_max_entries` | 114 | NEVER READ |

- **Config struct:** `peer_retry_enabled` through `peer_retry_max_entries` (lines 76-81 of config.h)
- **Config parsing:** `config_load_file` lines 335-347.
- **Status:** ALL NEVER READ. No source file reads any of these.
- **Impact:** Zero. This feature was implemented in the config but never wired into the crawler logic.

---

## 13. Triple routing table settings (ALL 2 fields)

| Setting | INI line | Status |
|---------|----------|--------|
| `triple_routing_threshold` | 130 | NEVER READ |
| `triple_routing_rotation_time` | 138 | NEVER READ |

- **Config struct:** `triple_routing_threshold`, `triple_routing_rotation_time` (lines 84-85 of config.h)
- **Config parsing:** `config_load_file` lines 350-358.
- **Status:** ALL NEVER READ. No source file reads these.
- **Impact:** Zero. The triple routing table design was abandoned in favor of the thread tree approach.

---

## Summary of Completely Unused Settings

These settings are parsed from config.ini but have **zero effect** on program behavior:

### Legacy / Old Architecture (old worker pool, wbpxre-dht, retry)
| Setting | INI line | Note |
|---------|----------|------|
| `targeted_search_percentage` | (not in ini) | Default-only, never used |
| `http_port` | 9 | Hardcoded HTTP_API_PORT used instead |
| `scaling_factor` | 31 | Old worker pool, replaced by thread trees |
| `metadata_workers` | 32 | Old worker pool only (old architecture) |
| `max_retry_attempts` | (not in ini) | Default-only, never used |
| `retry_delay_sec` | (not in ini) | Default-only, never used |
| `max_metadata_size_mb` | 94 | Never implemented |
| `batch_writes_enabled` | 36 | No code branch checks this |
| `wbpxre_ping_workers` | 98 | wbpxre-dht library removed |
| `wbpxre_find_node_workers` | 99 | wbpxre-dht library removed |
| `wbpxre_sample_infohashes_workers` | 100 | wbpxre-dht library removed |
| `wbpxre_get_peers_workers` | 101 | wbpxre-dht library removed |
| `wbpxre_query_timeout` | 102 | wbpxre-dht library removed |
| `max_routing_table_nodes` | (not in ini) | wbpxre-dht library removed |
| `peer_retry_enabled` | 109 | Feature never wired in |
| `peer_retry_max_attempts` | 110 | Feature never wired in |
| `peer_retry_min_threshold` | 111 | Feature never wired in |
| `peer_retry_delay_ms` | 112 | Feature never wired in |
| `peer_retry_cleanup_interval_sec` | 113 | Feature never wired in |
| `peer_retry_max_entries` | 114 | Feature never wired in |
| `triple_routing_threshold` | 130 | Design abandoned |
| `triple_routing_rotation_time` | 138 | Design abandoned |

**Total unused fields: 22** (18 with no code path at all, 4 only used in old architecture)

### Settings That ARE Used (for reference)
- `dht_port` — used in supervisor, thread_tree, refresh_thread
- `log_level` — used in main.c for g_app_ctx.log_level
- `bloom_enabled`, `bloom_capacity`, `bloom_error_rate`, `bloom_persist`, `bloom_path` — used for bloom filter
- `failure_bloom_capacity` — passed to supervisor
- `batch_size`, `flush_interval` — used in batch writer and metadata fetcher
- `backup_enabled`, `backup_path`, `db_path` — used in thread tree batch writer backup
- `ssh_backup_*`, `rclone_backup_*` — used in thread tree batch writer
- `porn_filter_enabled`, `porn_filter_keyword_file` — used in main.c and supervisor
- `language_filter_non_latin_threshold` — used in main.c
- All `tree_*` settings — used in supervisor config
- All `global_bootstrap_*` settings — used in supervisor config
- All `bep51_*` settings — used in supervisor config
- All `respawn_*` settings — used in supervisor config
- All `use_keyspace_partitioning`, `dead_partition_threshold`, `max_trees_per_partition` — used in supervisor/thread_tree
- All `refresh_*` settings — used in refresh_thread config
- `min_metadata_rate`, `dynamic_rate_margin`, `tree_rate_*`, `tree_min_lifetime_minutes`, `tree_require_empty_queue` — used in supervisor config
