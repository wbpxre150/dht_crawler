# tparse — release-name / file-list parser (vendored)

Copied **verbatim** from the classifier-model repository's `native/tparse/`. It is the parser that turns a
torrent's name and file list into the feature text the classifiers consume, so it is part of the model's
serving contract, not a convenience library: a different parser produces different features and therefore
different verdicts.

| file | role |
|---|---|
| `tparse.c` | the parser |
| `name.c` | release-name normalisation |
| `tparse.h` | public API (`tp_parse`, `tp_feature_text`, …) |
| `tparse_internal.h` | internal declarations |
| `adult_terms.h` | **generated** adult-term tables — never hand-edit (`uv run cm gen-header` in the model repo) |
| `taxonomy.h` | the 48-state category/tv_kind taxonomy |

Compiled only into the classifier flavours: `make clf` and `make full` pass `-DCLF_ENABLED -Itparse`.
A plain `make` never sees it.

## Serving-side property to preserve

`tparse` emits `__adult_jav`, `__adult_site` and `__adult_set` **unconditionally**, while the frozen model
was trained on a corpus that carries those tokens on ~20 of 270,390 rows. `cram_strip_adult_tokens()`
(`src/cram.c`) removes them after `tp_feature_text()` and before `cram_features()`. If you re-sync this
directory, re-run `make tests-cram` — the strip behaviour is asserted there.

## Re-syncing

```
cp <classifier-model>/native/tparse/{tparse.c,name.c,tparse.h,tparse_internal.h,adult_terms.h,taxonomy.h} tparse/
make tests && make clf
```
