# utf8proc 2.12.0 (test-only vendored copy)

Upstream: <https://github.com/JuliaStrings/utf8proc>, tag `v2.12.0`. Licence: MIT — see `LICENSE.md`,
which must travel with these files.

| file | role |
|---|---|
| `utf8proc.c` | the library; `#include`s `utf8proc_data.c` |
| `utf8proc.h` | public header, `#include <utf8proc.h>` in `src/movie_kind_render.c` |
| `utf8proc_data.c` | generated Unicode tables (2.4 MB) |
| `LICENSE.md` | upstream MIT licence |

## Why it is vendored here

`src/movie_kind_render.c` has two flavours: a plain passthrough build, and the `-DMK_NFC_ENABLED` build
that NFC-composes a decomposed torrent name before it becomes the movie-kind prompt. The NFC half is
what the shipped NDK binary compiles, and the stage-11 P0 was a broken NFC length *inside* that half —
so the host suite must compile it too, with a real utf8proc.

The crawler itself never links utf8proc: the Android wrapper (`dht_crawler-android/`) supplies it for the
device build. This copy is compiled into exactly one binary, `build/test_movie_kind_render_nfc`, by the
`tests-movie-kind-render` target.

Do not edit these files. A version bump means replacing all four from the upstream tag and re-running
`make tests-movie-kind-render`.
