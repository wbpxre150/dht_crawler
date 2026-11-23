#!/usr/bin/env bash
#
# tools/fetch-models.sh — download the classifier weights the crawler serves with.
#
# The two engines are optional at runtime: without their directories the crawler
# still crawls, `classifier` degrades to the parser hint (all-or-nothing, see
# include/cram.h) and `movie_kind` reports enabled=false. With them present and a
# build that defines CLF_ENABLED / MK_ENABLED (`make clf`, `make full`), the
# crawler classifies at ingest.
#
#   v15  CRAM: one forward pass per row -> category, tv_kind, adult
#   v13  movie-kind: Q4_0 decoder backbone + JEVH head -> genre mask
#
# Layout is cwd-relative and matches the compiled-in defaults, so a plain
# `./dht_crawler` picks them up with no config change:
#
#   models/v15/{cram.bin,cram_vocab.bin,manifest.json}          (classifier_model_dir)
#   models/v13/{movie_kind_backbone_Q4_0.gguf,movie_kind_head.bin,manifest.json}
#                                                               (movie_kind_model_dir)
#
# Every file is verified against the sha256 pinned here AND re-verified by the
# crawler at load time, so a corrupted or substituted download cannot silently
# change a verdict.
#
# USAGE
#   tools/fetch-models.sh [--gen v13|v15|all] [--dest DIR] [--base URL]
#                         [--force] [--dry-run]
#
#   --gen GEN    which generation to fetch (default all)
#   --dest DIR   model root, i.e. DIR/v13 and DIR/v15 (default ./models)
#   --base URL   object prefix (default https://unjumble.au/bep51/models)
#   --force      re-download even when the local copy already verifies
#   --dry-run    print what would happen, download nothing
#
# EXIT
#   0 all requested files present and verified; 1 otherwise.

set -euo pipefail

BASE="${MODELS_BASE_URL:-https://unjumble.au/bep51/models}"
DEST="./models"
GEN="all"
FORCE=0
DRY=0

# name generation sha256
CATALOG=(
  "v15 cram.bin                            834159b642d67da728bbdc3411badf83b639742743c95d414fc62ddd81e45bec"
  "v15 cram_vocab.bin                      6e24c4b27e3bd546a7dad0cb9a929dc7f354e664139604f0bb75f3ca1cf21cee"
  "v15 manifest.json                       59e39d3f85db21e015d94ffd7f5b055d0b593f69c238ac18f39c30bf50d53e55"
  "v13 movie_kind_backbone_Q4_0.gguf       28270fa01100687969b541282814371062ab9e1a560bb7ea755243c822cf0d4c"
  "v13 movie_kind_head.bin                 2f994cbe792441be182717cc6faa46f3ff245db6d17af30d72cfc47bbefb7ae3"
  "v13 manifest.json                       e047464314e40bb70e7fc16205297878981d4252349d96ef3830f0260757fb57"
)

fail() { printf 'fetch-models: %s\n' "$*" >&2; exit 1; }
say()  { printf '%s\n' "$*"; }

usage() { sed -n '2,44p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

while [ $# -gt 0 ]; do
  case "$1" in
    --gen)      GEN="${2:?--gen needs a value}"; shift 2 ;;
    --dest)     DEST="${2:?--dest needs a value}"; shift 2 ;;
    --base)     BASE="${2:?--base needs a value}"; shift 2 ;;
    --force)    FORCE=1; shift ;;
    --dry-run)  DRY=1; shift ;;
    -h|--help)  usage ;;
    *)          fail "unknown argument: $1 (try --help)" ;;
  esac
done

case "$GEN" in
  all|v13|v15) ;;
  *) fail "--gen must be v13, v15 or all (got '$GEN')" ;;
esac

for cmd in curl sha256sum; do
  command -v "$cmd" >/dev/null 2>&1 || fail "'$cmd' is required but not installed"
done

BASE="${BASE%/}"
sha_of() { sha256sum "$1" | cut -d' ' -f1; }

# verify <file> <sha> -> 0 when the file exists and matches
verify() {
  [ -f "$1" ] || return 1
  [ "$(sha_of "$1")" = "$2" ]
}

downloaded=0
skipped=0
bytes=0

for entry in "${CATALOG[@]}"; do
  read -r gen name sha <<<"$entry"
  [ "$GEN" = "all" ] || [ "$GEN" = "$gen" ] || continue

  dir="$DEST/$gen"
  target="$dir/$name"
  url="$BASE/$gen/$name"

  if [ "$FORCE" -eq 0 ] && verify "$target" "$sha"; then
    say "  ok    $gen/$name (already verified)"
    skipped=$((skipped + 1))
    continue
  fi

  if [ "$DRY" -eq 1 ]; then
    say "  would fetch $url -> $target"
    continue
  fi

  mkdir -p "$dir"
  part="$target.part"
  say "  get   $gen/$name"
  # -L: the CDN may redirect; --fail: no HTML error page written as a model.
  # Progress only on a TTY, so CI logs stay readable.
  quiet=(-sS); [ -t 2 ] && quiet=(--progress-bar)
  if ! curl -fL --retry 3 --retry-delay 2 --connect-timeout 20 \
             "${quiet[@]}" -o "$part" "$url"; then
    rm -f "$part"
    fail "download failed: $url"
  fi

  actual="$(sha_of "$part")"
  if [ "$actual" != "$sha" ]; then
    rm -f "$part"
    fail "sha256 mismatch for $gen/$name: expected $sha, got $actual ($url)"
  fi

  mv -f "$part" "$target"
  downloaded=$((downloaded + 1))
  bytes=$((bytes + $(stat -c%s "$target")))
done

if [ "$DRY" -eq 1 ]; then
  say "dry run: nothing downloaded"
  exit 0
fi

# The crawler re-verifies at load, but a bad manifest here means a half-installed
# generation, so say so now rather than after a daemon start.
say ""
say "models under $DEST:"
for gen in v13 v15; do
  [ -d "$DEST/$gen" ] || continue
  printf '  %s: %s\n' "$gen" "$(ls "$DEST/$gen" | tr '\n' ' ')"
done
say "fetched $downloaded file(s) ($(numfmt --to=iec "$bytes" 2>/dev/null || echo "$bytes B")), $skipped already present"
say ""
say "next: make clf   (CRAM only)   |   make llama && make full   (both engines)"
