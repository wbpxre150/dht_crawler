# utf8proc 2.12.0 (vendored)

Upstream: <https://github.com/JuliaStrings/utf8proc>, tag `v2.12.0`. Licence: MIT — see `LICENSE.md`,
which must travel with these files.

| file | role |
|---|---|
| `utf8proc.c` | the library; `#include`s `utf8proc_data.c` |
| `utf8proc.h` | public header, `#include <utf8proc.h>` in `src/movie_kind_render.c` |
| `utf8proc_data.c` | generated Unicode tables (2.4 MB) |
| `LICENSE.md` | upstream MIT licence |

## What uses it

`src/movie_kind_render.c` has two flavours: a plain passthrough build, and the `-DMK_NFC_ENABLED` build
that NFC-composes a decomposed torrent name before it becomes the movie-kind prompt. The NFC half is what
the shipped binary compiles, and the stage-11 P0 was a broken NFC length *inside* that half — so both the
host test and the real build must compile it with a real utf8proc.

- `make full` compiles it into the crawler (`-DMK_NFC_ENABLED`, beside `MK_ENABLED`).
- `make tests-movie-kind-render` compiles it into `build/test_movie_kind_render_nfc` only.

The Android wrapper build (`dht_crawler-android/`) has its own copy for the arm64 binary; this one keeps
the standalone build and the host suite free of that checkout.

Do not edit these files. A version bump means replacing all four from the upstream tag, then re-running
`make tests-movie-kind-render` and `make full`.
