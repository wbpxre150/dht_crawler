# CRAM v15 container — test fixture

A byte-identical copy of the shipped CRAM v15 classifier, vendored here so `make tests` is hermetic:
`tests-cram` drives the real engine (container loader with sha verification, the whole forward pass, the
masked 48-state argmin) over the real weights instead of a synthetic container, and it needs them on a
machine that has never seen the Android app repo.

| file | bytes | sha256 |
|---|---:|---|
| `cram.bin` | 9,388,835 | `834159b642d67da728bbdc3411badf83b639742743c95d414fc62ddd81e45bec` |
| `cram_vocab.bin` | 2,046,524 | `6e24c4b27e3bd546a7dad0cb9a929dc7f354e664139604f0bb75f3ca1cf21cee` |
| `manifest.json` | 831 | `59e39d3f85db21e015d94ffd7f5b055d0b593f69c238ac18f39c30bf50d53e55` |

`cram_load()` verifies all three sha256s against `manifest.json`, so a drifted fixture fails the test
rather than silently changing the arithmetic it is measuring.

## This is a fixture, not the runtime model

The crawler reads its model from `classifier_model_dir` (default `models/v15`, cwd-relative) — that is a
deployment asset the operator supplies. This copy exists only for the host test suite; nothing in
`src/` refers to it.

Override the test's copy with `make tests-cram CRAM_MODEL_DIR=<dir>` when you are testing a different
generation.

## Provenance

Built from `~/apps/cram/tvkind/data/clean_v3/heads_per_field_v2.npz` (4,909,615 B, sha256
`1cad6d9f9ddfd4afd72338288d12dda6855dceda4cb531d5417b9ee22406fff2`) via `scripts/cram_export.py` at
cram workspace commit `e6ce723`. The export converts and proves bit-identity; it never refits.
