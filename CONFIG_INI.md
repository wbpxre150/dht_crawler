# DHT Crawler config.ini Reference

Every key in config.ini, in order, with the consumer source file
and the line at which it is read. Anything not on this list is
ignored.

## Network

| Key | Default | Consumer |
|---|---|---|
| dht_port | 6881 | supervisor.c:57, refresh_thread.c:109, thread_tree.c:1214 |

## Database & Logging

| db_path | data/torrents.db | main.c:522 |
| log_level | INFO | main.c:258 |

## Bloom Filter

| bloom_enabled | 1 | main.c:266, batch_writer.c, refresh_thread.c |
| bloom_capacity | 30000000 | main.c:280, bloom_filter.c |
| failure_bloom_capacity | 30000000 | supervisor.c:69 |
| bloom_error_rate | 0.001 | main.c:280, supervisor.c:70 |
| bloom_persist | 1 | main.c:270, 423 |
| bloom_path | data/bloom.dat | main.c:274, 295 |

## Batched Writes

| batch_size | 500 | main.c:643, supervisor.c:118 (defaults) |
| flush_interval | 300 | main.c:643, supervisor.c:118 (defaults) |

## Backup

| backup_enabled | 1 | main.c:658 |
| backup_path | /home/... | main.c:659 |
| ssh_backup_* | 0 | main.c:663-668 |
| rclone_backup_* | 1 | main.c:671-676 |

## Porn Filter

| porn_filter_enabled | 1 | main.c:554, supervisor.c:115 |
| porn_filter_keyword_file | porn_filter_keywords.txt | main.c:451 |

## Language Filter

| language_filter_non_latin_threshold | 33 | main.c:462 |

## Tree-Native Bootstrap

| tree_bootstrap_timeout_sec | 30 | config.c default; supervisor.c set; thread_tree.c read |
| global_bootstrap_target | 1000 | config.c default; supervisor.c read |
| global_bootstrap_timeout_sec | 60 | config.c default; supervisor.c read |
| global_bootstrap_workers | 20 | config.c default; supervisor.c read |

## Thread Tree Architecture

| num_trees | 32 | main.c:680, supervisor.c:140 |
| use_keyspace_partitioning | 1 | supervisor.c:56, thread_tree.c |
| dead_partition_threshold | 3 | supervisor.c:146 |
| max_trees_per_partition | 4 | supervisor.c:147 |

## Per-Tree Worker Counts (multiplied by num_trees)

| tree_find_node_workers | 3 | main.c:688, supervisor.c:73 |
| tree_bep51_workers | 10 | main.c:689 |
| tree_get_peers_workers | 50 | main.c:690 |
| tree_metadata_workers | 75 | main.c:691 |
| tree_infohash_queue_capacity | 3000 | main.c:701 |
| tree_bep51_query_interval_ms | 5 | main.c:702 |
| tree_bep51_node_cooldown_sec | 3 | main.c:703 |
| tree_peers_queue_capacity | 3000 | main.c:705 |
| tree_get_peers_timeout_ms | 3000 | main.c:706 (default in config) |
| tree_parallel_peers | 4 | main.c:715 |
| tree_tcp_connect_timeout_ms | 2000 | main.c:714 |

## Throttling

| tree_infohash_pause_threshold | 2500 | main.c:708 |
| tree_infohash_resume_threshold | 1000 | main.c:709 |
| tree_peers_pause_threshold | 2500 | main.c:711 |
| tree_peers_resume_threshold | 1000 | main.c:712 |

## Rate-Based Respawn

| min_metadata_rate | 0.005 | main.c:717 |
| dynamic_rate_margin | 0.01 | main.c:718 |
| tree_rate_check_interval_sec | 120 | main.c:719 |
| tree_rate_grace_period_sec | 30 | main.c:720 |
| tree_min_lifetime_minutes | 8 | main.c:721 |
| tree_require_empty_queue | 0 | main.c:722 |
| tree_rate_ema_alpha | 0.3 | main.c:723 |

## Respawn Overlapping

| respawn_spawn_threshold | 20 | main.c:725 |
| respawn_drain_timeout_sec | 180 | main.c:726 |
| max_draining_trees | 16 | main.c:727 |

## BEP51 Cache

| bep51_cache_path | data/bep51_cache.dat | main.c:735, supervisor.c:120 |
| bep51_cache_capacity | 50000 | main.c:698, supervisor.c:118 |
| bep51_cache_submit_percent | 17 | main.c:699, supervisor.c:119 |
| bep51_node_cooldown_sec | 3 | main.c:703 |

## Refresh Thread (HTTP /refresh endpoint)

| refresh_bootstrap_sample_size | 1000 | main.c:766 |
| refresh_routing_table_target | 500 | main.c:767 |
| refresh_ping_workers | 1 | main.c:768 |
| refresh_find_node_workers | 1 | main.c:769 |
| refresh_get_peers_workers | 1 | main.c:770 |
| refresh_request_queue_capacity | 100 | main.c:771 |
| refresh_get_peers_timeout_ms | 500 | main.c:772 |
| refresh_max_iterations | 3 | main.c:773 |

---

## Keys intentionally NOT in config.ini

These existed before stage 1/2/3/4/5/6 cleanup and are no longer
read by any source file. They are not parsed; if present in
config.ini they are silently ignored:

- `targeted_search_percentage`, `http_port`, `scaling_factor`,
  `metadata_workers`, `max_retry_attempts`, `retry_delay_sec`,
  `max_metadata_size_mb`, `batch_writes_enabled`, all `wbpxre_*`,
  all `peer_retry_*`, `triple_routing_threshold`,
  `triple_routing_rotation_time`, `use_thread_trees`,
  `per_tree_sample_size`,
  `max_concurrent_connections`, `tcp_connect_timeout_sec`,
  `connection_timeout_sec`, `max_connection_lifetime_sec`.

This list mirrors the deletion surface in
`include/config.h` / `src/config.c` after stage 1.

---

## Updates

- 2026-06: rewrote for thread-tree-only build (PLAN.md stage 6).
- Earlier revisions documented the dual wbpxre + thread-tree
  era. See git history.
