#!/usr/bin/env bash
#
# tools/build-llama.sh — build the pinned llama.cpp the movie-kind classifier links.
#
# `make full` needs a static, CPU-only llama.cpp install prefix (libllama.a +
# libggml*.a + include/llama.h). This script fetches the exact commit the shipped
# model was converted against, verifies the tarball sha256, and installs it under
# build/llama-install. Nothing here is Android-specific: the same pin is what the
# app's Gradle buildLlama task compiles for arm64.
#
# The pin matters. A GGUF is only portable across llama.cpp versions to the extent
# the file format and the CPU kernels agree, and the v13 manifest records
# `llama_cpp_commit`; building another commit is how you get silently different
# logits. Do not "just use latest".
#
# USAGE
#   tools/build-llama.sh [--prefix DIR] [--jobs N] [--force] [--dry-run]
#
#   --prefix DIR  install prefix (default build/llama-install)
#   --jobs N      parallel compile jobs (default: nproc)
#   --force       rebuild even when the prefix already carries this pin
#   --dry-run     print the plan, build nothing
#
# EXIT
#   0 the prefix holds this pin's static libs; 1 otherwise.

set -euo pipefail

LLAMA_COMMIT="18ab62a8ec5144ae5d2f65e78374ba69652d75dd"
LLAMA_TARBALL_SHA="34bd70e229bf0439575b7865e986209d23584bd35274d95c256f9ccf398ab46a"
LLAMA_URL="https://github.com/ggml-org/llama.cpp/archive/${LLAMA_COMMIT}.tar.gz"

PREFIX="build/llama-install"
JOBS="$(nproc 2>/dev/null || echo 4)"
FORCE=0
DRY=0

fail() { printf 'build-llama: %s\n' "$*" >&2; exit 1; }
say()  { printf '%s\n' "$*"; }
usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0; }

while [ $# -gt 0 ]; do
  case "$1" in
    --prefix)  PREFIX="${2:?--prefix needs a value}"; shift 2 ;;
    --jobs)    JOBS="${2:?--jobs needs a value}"; shift 2 ;;
    --force)   FORCE=1; shift ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) usage ;;
    *)         fail "unknown argument: $1 (try --help)" ;;
  esac
done

for cmd in curl sha256sum tar cmake; do
  command -v "$cmd" >/dev/null 2>&1 || fail "'$cmd' is required but not installed"
done
command -v c++ >/dev/null 2>&1 || command -v g++ >/dev/null 2>&1 \
  || fail "a C++ compiler is required (llama.cpp is C++)"

PREFIX="${PREFIX%/}"
CACHE="build/llama"
TARBALL="$CACHE/llama-${LLAMA_COMMIT:0:8}.tar.gz"
SRCDIR="$CACHE/llama-${LLAMA_COMMIT:0:8}"
STAMP="$PREFIX/.llama-pin"

sha_of() { sha256sum "$1" | cut -d' ' -f1; }

if [ "$FORCE" -eq 0 ] && [ -f "$STAMP" ] && [ "$(cat "$STAMP")" = "$LLAMA_TARBALL_SHA" ] \
   && [ -f "$PREFIX/lib/libllama.a" ]; then
  say "llama.cpp already installed at $PREFIX (pin ${LLAMA_COMMIT:0:8})"
  exit 0
fi

say "llama.cpp pin ${LLAMA_COMMIT:0:8} -> $PREFIX (jobs $JOBS)"
if [ "$DRY" -eq 1 ]; then
  say "  would fetch  $LLAMA_URL"
  say "  would verify sha256 $LLAMA_TARBALL_SHA"
  say "  would cmake -DBUILD_SHARED_LIBS=OFF ... && cmake --build --target install"
  exit 0
fi

mkdir -p "$CACHE"

# 1. tarball: cached, but re-fetched when its bytes are not the pin
if [ -f "$TARBALL" ] && [ "$(sha_of "$TARBALL")" != "$LLAMA_TARBALL_SHA" ]; then
  say "  cached tarball is not the pin, discarding"
  rm -f "$TARBALL"
fi
if [ ! -f "$TARBALL" ]; then
  say "  fetching $LLAMA_URL"
  quiet=(-sS); [ -t 2 ] && quiet=(--progress-bar)
  curl -fL --retry 3 --connect-timeout 20 "${quiet[@]}" -o "$TARBALL.part" "$LLAMA_URL"
  actual="$(sha_of "$TARBALL.part")"
  [ "$actual" = "$LLAMA_TARBALL_SHA" ] \
    || { rm -f "$TARBALL.part"; fail "tarball sha256 mismatch: expected $LLAMA_TARBALL_SHA, got $actual"; }
  mv -f "$TARBALL.part" "$TARBALL"
fi

# 2. extract once
if [ ! -f "$SRCDIR/CMakeLists.txt" ]; then
  say "  extracting"
  tmp="$(mktemp -d "$CACHE/extract.XXXXXX")"
  tar -xzf "$TARBALL" -C "$tmp"
  root="$(find "$tmp" -mindepth 1 -maxdepth 1 -type d | head -1)"
  [ -n "$root" ] || fail "tarball has no top-level directory"
  rm -rf "$SRCDIR"
  mv "$root" "$SRCDIR"
  rm -rf "$tmp"
fi

# 3. configure + install. CPU-only and static, matching the shipped model:
#    every accelerator backend is off so the binary stays a single portable set of
#    archives, and GGML_OPENMP is off because the crawler already owns its threads.
#    The CLI/tools/examples/server/app targets are all off on purpose: at this pin
#    they default ON for a standalone configure, they need llama-common, and the
#    unified `llama` binary fails to link here for reasons that have nothing to do
#    with the four archives the crawler links. Only libs + headers are installed.
BUILDDIR="$SRCDIR/build-host"
say "  configuring"
cmake -S "$SRCDIR" -B "$BUILDDIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$(pwd)/$PREFIX" \
  -DBUILD_SHARED_LIBS=OFF \
  -DLLAMA_CURL=OFF \
  -DLLAMA_BUILD_TESTS=OFF \
  -DLLAMA_BUILD_EXAMPLES=OFF \
  -DLLAMA_BUILD_SERVER=OFF \
  -DLLAMA_BUILD_APP=OFF \
  -DLLAMA_BUILD_TOOLS=OFF \
  -DLLAMA_BUILD_COMMON=OFF \
  -DLLAMA_TOOLS_INSTALL=OFF \
  -DLLAMA_TESTS_INSTALL=OFF \
  -DGGML_BUILD_TESTS=OFF \
  -DGGML_BUILD_EXAMPLES=OFF \
  -DGGML_OPENMP=OFF \
  -DGGML_BLAS=OFF \
  -DGGML_CUDA=OFF \
  -DGGML_VULKAN=OFF \
  -DGGML_OPENCL=OFF \
  >"$CACHE/configure.log" 2>&1 \
  || { tail -30 "$CACHE/configure.log" >&2; fail "cmake configure failed (log: $CACHE/configure.log)"; }

say "  building (-j$JOBS, log: $CACHE/build.log)"
cmake --build "$BUILDDIR" --target install -j "$JOBS" >"$CACHE/build.log" 2>&1 \
  || { tail -30 "$CACHE/build.log" >&2; fail "cmake build failed (log: $CACHE/build.log)"; }

printf '%s\n' "$LLAMA_TARBALL_SHA" > "$STAMP"

say ""
say "installed:"
for lib in libllama.a libggml.a libggml-cpu.a libggml-base.a; do
  [ -f "$PREFIX/lib/$lib" ] && say "  $PREFIX/lib/$lib" || fail "missing $PREFIX/lib/$lib after install"
done
say "next: make full"
