# DHT Crawler Makefile

CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c99 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -D__BSD_VISIBLE=1 $(EXTRA_CFLAGS)
INCLUDES = -Iinclude -Ilib/bencode-c -Ilib/cJSON -Ilib/civetweb/include -Ilib/libbloom -Ilib/uthash/src
LDFLAGS = -luv -lsqlite3 -lpthread -lssl -lcrypto -ldl -lm lib/libbloom/build/libbloom.a

# Directories
SRC_DIR = src
INC_DIR = include
LIB_DIR = lib
BUILD_DIR = build
DATA_DIR = data

# Source files (wildcard picks up every module under src/)
SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

LIB_OBJS = $(BUILD_DIR)/bencode.o \
           $(BUILD_DIR)/cJSON.o \
           $(BUILD_DIR)/civetweb.o

# Target executable
TARGET = dht_crawler

.PHONY: all clean dirs install-deps libbloom patch-bencode patch-civetweb clf clf-backfill full check-llama models llama

all: dirs patch-bencode patch-civetweb libbloom $(TARGET)

# Apply patches to submodules
patch-bencode:
	@if ! grep -q "ctx->cap = newcap" $(LIB_DIR)/bencode-c/bencode.c 2>/dev/null; then \
		echo "Applying bencode-c capacity fix patch..."; \
		patch -p1 -d . < patches/bencode-cap-fix.patch || true; \
	fi
	@if ! grep -q "Zero initialize the newly allocated portion" $(LIB_DIR)/bencode-c/bencode.c 2>/dev/null; then \
		echo "Applying bencode-c initialization fix patch..."; \
		patch -p1 -d . < patches/bencode-init-fix.patch || true; \
	fi

patch-civetweb:
	@if ! grep -q "atomic_load_explicit" $(LIB_DIR)/civetweb/src/civetweb.c 2>/dev/null; then \
		echo "Applying civetweb stop_flag atomic fix patch..."; \
		patch -p1 -d . < patches/civetweb-stop-flag-atomic.patch || true; \
	fi

dirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(DATA_DIR)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(TARGET): $(OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@


$(BUILD_DIR)/bencode.o: $(LIB_DIR)/bencode-c/bencode.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/cJSON.o: $(LIB_DIR)/cJSON/cJSON.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/civetweb.o: $(LIB_DIR)/civetweb/src/civetweb.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -DNO_SSL -c $< -o $@

libbloom:
	@echo "Building libbloom..."
	@cd $(LIB_DIR)/libbloom && $(MAKE) LDFLAGS=""

clean:
	rm -rf $(BUILD_DIR)
	rm -f $(TARGET)
	@cd $(LIB_DIR)/libbloom && $(MAKE) clean

install-deps:
	@echo "Installing dependencies..."
	@echo "Please ensure the following packages are installed:"
	@echo "  - libuv-dev"
	@echo "  - libsqlite3-dev"
	@echo "  - libssl-dev"
	@echo "  - build-essential"

# Development helpers
run: $(TARGET)
	./$(TARGET)

debug:
	$(MAKE) clean
	$(MAKE) all EXTRA_CFLAGS="-g -DDEBUG"

asan:
	$(MAKE) clean
	$(MAKE) all EXTRA_CFLAGS="-g -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer" LDFLAGS="$(LDFLAGS) -fsanitize=address -fsanitize=undefined"

tsan:
	$(MAKE) clean
	$(MAKE) all EXTRA_CFLAGS="-g -fsanitize=thread -fno-omit-frame-pointer" LDFLAGS="$(LDFLAGS) -fsanitize=thread"

valgrind: debug
	valgrind --leak-check=full --show-leak-kinds=all ./$(TARGET)

# Host unit tests (standalone: no dependency on all/$(TARGET) and no patch
# targets). Run from the repo root so the test binary finds
# torrent_search_keywords.txt relative to cwd.
#
# tests-search needs nothing but libc; tests-docs links src/database.c, which
# references bloom_filter_add() from src/bloom_filter.c (itself a wrapper over
# libbloom), so the test also compiles that module and links the libbloom
# archive plus SQLite and libuv (database_t holds a uv_mutex_t). The bloom is
# never set on the database handle, so nothing is ever added to it.
TEST_SRCS = tests/test_torrent_search.c src/torrent_search.c tests/stub_log.c
TEST_BIN = $(BUILD_DIR)/test_torrent_search
TEST_CFLAGS =
TEST_LDFLAGS =

DOCS_TEST_SRCS = tests/test_torrent_docs.c src/database.c src/bloom_filter.c \
                 tests/stub_log.c
DOCS_TEST_BIN = $(BUILD_DIR)/test_torrent_docs
DOCS_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm lib/libbloom/build/libbloom.a

# The in-flight claim set is pure (a mutex over a fixed slot table): no database,
# no bloom filter, no uv loop, so this test needs only the module plus the log
# stub. It covers the tombstone/release invariant and the one-winner race.
INFLIGHT_TEST_SRCS = tests/test_inflight_set.c src/inflight_set.c tests/stub_log.c
INFLIGHT_TEST_BIN = $(BUILD_DIR)/test_inflight_set
INFLIGHT_TEST_LDFLAGS = -luv -lpthread

# The last-interval stats sampler is pure arithmetic over four counters: no
# database, no bloom filter, no uv loop, no civetweb, no logging, so this test
# needs only the module and the test itself. It covers the first-tick zeros, the
# monotonic deltas, the saturating backwards jump per counter and the snapshot
# advance.
STATS_WINDOW_TEST_SRCS = tests/test_stats_window.c src/stats_window.c
STATS_WINDOW_TEST_BIN = $(BUILD_DIR)/test_stats_window
STATS_WINDOW_TEST_CFLAGS =
STATS_WINDOW_TEST_LDFLAGS =

# The per-address connect-failure memory is pure too: a mutex over a fixed slot
# table, a normalized 19-byte key and an injected clock. It logs (creation, the
# load-factor clear), so the log stub is linked in place of src/main.c; no
# database, bloom filter, uv loop or civetweb. Covers the NULL/ttl-0 fail-open
# contract, the idempotent record, the TTL expiry, the load-factor clear, the
# v4-mapped key fold, the refused addresses and the slot-vs-address counter
# semantics.
META_PEER_FAIL_TEST_SRCS = tests/test_meta_peer_fail.c src/meta_peer_fail.c \
                           tests/stub_log.c
META_PEER_FAIL_TEST_BIN = $(BUILD_DIR)/test_meta_peer_fail
META_PEER_FAIL_TEST_CFLAGS =
META_PEER_FAIL_TEST_LDFLAGS = -lpthread

# The CRAM engine is pure C99 + libm: the container/vocab loaders, the crc32
# char tail and the whole forward pass, all over bytes the export wrote, with
# the fixtures parsed by the vendored cJSON. No database, no libuv, no civetweb,
# no tparse/ftc and no src/classifier.c -- the real classifier half only exists
# under CLF_ENABLED, so the engine is linked directly (the meta_peer_fail
# shape).
#
# The model is the shipped v15 container, vendored under tests/fixtures/cram so
# this target is hermetic: `make tests` needs no Android repo, no network and no
# operator-supplied model. cram_load() verifies the pinned sha256s, so a drifted
# fixture fails loudly instead of silently changing the arithmetic under test.
# Point CRAM_MODEL_DIR at another generation to test that one instead. The path
# is cwd-relative like FIXTURE_DIR, which is how the test already resolves its
# other fixtures -- and it therefore also works from the Gradle-prepared copy.
# The HTTP(S) fetcher behind the BEP51 cache download. Forked scripted servers
# answer each case over a real socket: plain HTTP for the framing/redirect/cap
# paths, and TLS against the committed self-signed fixture for both "reject an
# untrusted certificate" and "accept it when the CA is trusted".
HTTP_FETCH_TEST_SRCS = tests/test_http_fetch.c src/http_fetch.c
HTTP_FETCH_TEST_BIN = $(BUILD_DIR)/test_http_fetch
HTTP_FETCH_TEST_CFLAGS =
HTTP_FETCH_TEST_LDFLAGS = -lssl -lcrypto -lpthread

CRAM_MODEL_DIR ?= tests/fixtures/cram/models/v15
CRAM_TEST_SRCS = tests/test_cram.c src/cram.c tests/stub_log.c lib/cJSON/cJSON.c
CRAM_TEST_BIN = $(BUILD_DIR)/test_cram
CRAM_TEST_CFLAGS = -DCRAM_TEST_MODEL_DIR='"$(CRAM_MODEL_DIR)"'
CRAM_TEST_LDFLAGS = -lm -lpthread

# The backfill test defines its own stub classifier symbols (models_loaded,
# current_version, write_meta, classify) so it never links src/classifier.c:
# the host build compiles that module inert, which could never insert a row.
BACKFILL_TEST_SRCS = tests/test_classify_backfill.c src/classify_backfill.c \
                     src/database.c src/bloom_filter.c tests/stub_log.c
BACKFILL_TEST_BIN = $(BUILD_DIR)/test_classify_backfill
BACKFILL_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm lib/libbloom/build/libbloom.a

# The daemon-thread test drives src/classify_backfill_daemon.c through its test
# hooks (-DCLF_BACKFILL_DAEMON_TEST), so the engine and the classifier are fakes
# and no database, model files or 60 s poll interval are involved.
# src/classify_backfill.c is still linked, for the real state names and the
# progress accessor; its classifier dependencies are answered by the test's own
# stubs, exactly like the backfill test above.
BACKFILL_DAEMON_TEST_SRCS = tests/test_classify_backfill_daemon.c \
                            src/classify_backfill_daemon.c \
                            src/classify_backfill.c src/adult_backfill.c \
                            src/database.c src/bloom_filter.c tests/stub_log.c
BACKFILL_DAEMON_TEST_BIN = $(BUILD_DIR)/test_classify_backfill_daemon
BACKFILL_DAEMON_TEST_CFLAGS = -DCLF_BACKFILL_DAEMON_TEST
BACKFILL_DAEMON_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm lib/libbloom/build/libbloom.a

# The movie-kind test links the real src/movie_kind.c and src/jevhead.c. Both
# compile without MK_ENABLED: the manifest/sha256 half is plain C99 + cJSON +
# OpenSSL, and jevhead.c is pure arithmetic, so the host test exercises the
# genuine refusal paths. The encoder half (llama.cpp) and the NFC cases only
# exist under -DMK_ENABLED, which this Makefile never defines -- utf8proc is
# vendored in the outer repo only (movie-kind Stage A). cJSON is compiled in
# directly and -lcrypto backs the pinned sha256s.
MOVIE_KIND_TEST_SRCS = tests/test_movie_kind.c src/movie_kind.c src/movie_kind_render.c \
                       src/jevhead.c src/classifier.c src/database.c \
                       src/bloom_filter.c lib/cJSON/cJSON.c tests/stub_log.c
MOVIE_KIND_TEST_BIN = $(BUILD_DIR)/test_movie_kind
# The embedding-provider seam (include/movie_kind.h): lets the host test drive
# the real guarded classify path with synthetic 1024-float vectors instead of a
# 478.2 MiB GGUF. The daemon build defines nothing here and exports no seam.
MOVIE_KIND_TEST_CFLAGS = -DMOVIE_KIND_TEST
# src/classifier.c is here for clf_sha256_file() alone (include/clf_sha256.h): the
# manifest gate and the fastText loader must verify with the same code. It links
# inert on the host (no CLF_ENABLED), so no ftc/tparse is needed.
# src/database.c + src/bloom_filter.c are here for the inline-ingest case: it
# runs a metadata row through database_insert_batch(), the same write path the
# daemon uses, and reads the torrent_class row back.
MOVIE_KIND_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm -lcrypto lib/libbloom/build/libbloom.a

# The renderer test compiles src/movie_kind_render.c, which is deliberately
# ungated: the stage-11 P0 was a broken NFC length *inside* the MK_ENABLED half,
# where the host build only ever saw a stub, so the one broken function had no
# coverage. Built twice -- plain (passthrough NFC) and with -DMK_NFC_ENABLED plus
# a real utf8proc, which is what the shipped NDK binary compiles. utf8proc
# 2.12.0 is vendored under tests/vendor/utf8proc for exactly this binary (the
# device build gets it from the dht_crawler-android wrapper), so the target is
# hermetic: no Android repo, no network.
RENDER_TEST_SRCS = tests/test_movie_kind_render.c src/movie_kind_render.c
RENDER_TEST_BIN = $(BUILD_DIR)/test_movie_kind_render
RENDER_TEST_NFC_BIN = $(BUILD_DIR)/test_movie_kind_render_nfc
UTF8PROC_DIR = vendor/utf8proc
RENDER_NFC_CFLAGS = -DMK_NFC_ENABLED -I$(UTF8PROC_DIR)
RENDER_NFC_SRCS = $(RENDER_TEST_SRCS) $(UTF8PROC_DIR)/utf8proc.c

# The movie-kind backfill engine test links the real src/movie_backfill.c and the
# real src/movie_kind.c (host flavour: manifest/sha256 only), and drives the
# engine's seams with -DMOVIE_BACKFILL_TEST so a pass can label rows without a
# model and pause for heat without a hot CPU. src/database.c builds the fixture
# through the ingest path, exactly like the fastText backfill test.
MOVIE_BACKFILL_TEST_SRCS = tests/test_movie_backfill.c src/movie_backfill.c \
                           src/movie_kind.c src/movie_kind_render.c src/jevhead.c \
                           src/classifier.c src/database.c src/bloom_filter.c \
                           lib/cJSON/cJSON.c tests/stub_log.c
MOVIE_BACKFILL_TEST_BIN = $(BUILD_DIR)/test_movie_backfill
MOVIE_BACKFILL_TEST_CFLAGS = -DMOVIE_BACKFILL_TEST
MOVIE_BACKFILL_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm -lcrypto lib/libbloom/build/libbloom.a

# The daemon-thread test drives src/movie_backfill_daemon.c through its test hooks
# (-DMOVIE_BACKFILL_DAEMON_TEST): the engine and the movie-kind module are fakes,
# so no database, model files or 60 s poll interval are involved. The real engine
# and movie-kind modules are still linked, for the state names and the defaults.
MOVIE_BACKFILL_DAEMON_TEST_SRCS = tests/test_movie_backfill_daemon.c \
                                  src/movie_backfill_daemon.c src/movie_backfill.c \
                                  src/movie_kind.c src/movie_kind_render.c \
                                  src/jevhead.c src/classifier.c src/database.c \
                                  src/bloom_filter.c lib/cJSON/cJSON.c \
                                  tests/stub_log.c
MOVIE_BACKFILL_DAEMON_TEST_BIN = $(BUILD_DIR)/test_movie_backfill_daemon
MOVIE_BACKFILL_DAEMON_TEST_CFLAGS = -DMOVIE_BACKFILL_DAEMON_TEST
MOVIE_BACKFILL_DAEMON_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm -lcrypto lib/libbloom/build/libbloom.a

# The adult-backfill engine test links the real src/adult_backfill.c and drives
# it with -DADULT_BACKFILL_TEST, which exposes the verdict seam AND the gate
# seam. The token gate needs tparse, which this build does not have, so the real
# gate answers "nothing gated out"; the test installs its own gate instead and
# drives the SQL, the gate, the UPDATE and the watermark path with canned
# verdicts - exactly what the model-free host build can verify (the real gate is
# covered by the clf-backfill smoke run and by G1's parser parity). It defines
# its own classifier symbols instead of linking src/classifier.c, which compiles
# inert on the host (same trick tests-backfill plays).
ADULT_BACKFILL_TEST_SRCS = tests/test_adult_backfill.c src/adult_backfill.c \
                           src/database.c src/bloom_filter.c tests/stub_log.c
ADULT_BACKFILL_TEST_BIN = $(BUILD_DIR)/test_adult_backfill
ADULT_BACKFILL_TEST_CFLAGS = -DADULT_BACKFILL_TEST
ADULT_BACKFILL_TEST_LDFLAGS = -lsqlite3 -luv -lpthread -lm lib/libbloom/build/libbloom.a

# The movie-kind filter test compiles src/http_api.c INTO its translation unit
# (#include "../src/http_api.c"): cls_filter_t, append_class_conditions() and
# bind_class_filter() are file-local there, and going through the real fragment
# builder is the point of the test -- a copy of the predicate could not catch a
# regression. -ffunction-sections plus --gc-sections then drop everything in
# http_api.c the test does not reach, so the civetweb / movie_kind / supervisor
# modules never enter the link and the binary needs only SQLite, libuv and the
# bloom archive that src/database.c pulls in.
MOVIE_KIND_FILTER_TEST_SRCS = tests/test_movie_kind_filter.c src/database.c \
                              src/bloom_filter.c tests/stub_log.c
MOVIE_KIND_FILTER_TEST_BIN = $(BUILD_DIR)/test_movie_kind_filter
MOVIE_KIND_FILTER_TEST_CFLAGS = -ffunction-sections -fdata-sections
MOVIE_KIND_FILTER_TEST_LDFLAGS = -Wl,--gc-sections -lsqlite3 -luv -lpthread -lm lib/libbloom/build/libbloom.a

tests: tests-http-fetch tests-search tests-docs tests-inflight tests-stats-window tests-meta-peer-fail tests-cram tests-backfill tests-backfill-daemon tests-adult-backfill \
       tests-movie-kind tests-movie-kind-render tests-movie-backfill \
       tests-movie-backfill-daemon tests-movie-kind-filter

tests-http-fetch:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(INCLUDES) $(HTTP_FETCH_TEST_SRCS) -o $(HTTP_FETCH_TEST_BIN) $(TEST_LDFLAGS) $(HTTP_FETCH_TEST_LDFLAGS)
	./$(HTTP_FETCH_TEST_BIN)

tests-search:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(INCLUDES) $(TEST_SRCS) -o $(TEST_BIN) $(TEST_LDFLAGS)
	./$(TEST_BIN)

tests-inflight:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(INCLUDES) $(INFLIGHT_TEST_SRCS) -o $(INFLIGHT_TEST_BIN) $(TEST_LDFLAGS) $(INFLIGHT_TEST_LDFLAGS)
	./$(INFLIGHT_TEST_BIN)

tests-stats-window:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(STATS_WINDOW_TEST_CFLAGS) $(INCLUDES) $(STATS_WINDOW_TEST_SRCS) -o $(STATS_WINDOW_TEST_BIN) $(TEST_LDFLAGS) $(STATS_WINDOW_TEST_LDFLAGS)
	./$(STATS_WINDOW_TEST_BIN)

tests-meta-peer-fail:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(META_PEER_FAIL_TEST_CFLAGS) $(INCLUDES) $(META_PEER_FAIL_TEST_SRCS) -o $(META_PEER_FAIL_TEST_BIN) $(TEST_LDFLAGS) $(META_PEER_FAIL_TEST_LDFLAGS)
	./$(META_PEER_FAIL_TEST_BIN)

tests-cram:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(CRAM_TEST_CFLAGS) $(INCLUDES) $(CRAM_TEST_SRCS) -o $(CRAM_TEST_BIN) $(TEST_LDFLAGS) $(CRAM_TEST_LDFLAGS)
	./$(CRAM_TEST_BIN)

tests-docs: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(INCLUDES) $(DOCS_TEST_SRCS) -o $(DOCS_TEST_BIN) $(TEST_LDFLAGS) $(DOCS_TEST_LDFLAGS)
	./$(DOCS_TEST_BIN)

tests-backfill: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(INCLUDES) $(BACKFILL_TEST_SRCS) -o $(BACKFILL_TEST_BIN) $(TEST_LDFLAGS) $(BACKFILL_TEST_LDFLAGS)
	./$(BACKFILL_TEST_BIN)

tests-backfill-daemon: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(BACKFILL_DAEMON_TEST_CFLAGS) $(INCLUDES) $(BACKFILL_DAEMON_TEST_SRCS) -o $(BACKFILL_DAEMON_TEST_BIN) $(TEST_LDFLAGS) $(BACKFILL_DAEMON_TEST_LDFLAGS)
	./$(BACKFILL_DAEMON_TEST_BIN)

tests-movie-kind: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(MOVIE_KIND_TEST_CFLAGS) $(INCLUDES) $(MOVIE_KIND_TEST_SRCS) -o $(MOVIE_KIND_TEST_BIN) $(TEST_LDFLAGS) $(MOVIE_KIND_TEST_LDFLAGS)
	./$(MOVIE_KIND_TEST_BIN)

tests-movie-kind-render:
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(INCLUDES) $(RENDER_TEST_SRCS) -o $(RENDER_TEST_BIN) $(TEST_LDFLAGS)
	./$(RENDER_TEST_BIN)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(RENDER_NFC_CFLAGS) $(INCLUDES) $(RENDER_NFC_SRCS) -o $(RENDER_TEST_NFC_BIN) $(TEST_LDFLAGS)
	./$(RENDER_TEST_NFC_BIN)

tests-movie-backfill: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(MOVIE_BACKFILL_TEST_CFLAGS) $(INCLUDES) $(MOVIE_BACKFILL_TEST_SRCS) -o $(MOVIE_BACKFILL_TEST_BIN) $(TEST_LDFLAGS) $(MOVIE_BACKFILL_TEST_LDFLAGS)
	./$(MOVIE_BACKFILL_TEST_BIN)

tests-movie-backfill-daemon: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(MOVIE_BACKFILL_DAEMON_TEST_CFLAGS) $(INCLUDES) $(MOVIE_BACKFILL_DAEMON_TEST_SRCS) -o $(MOVIE_BACKFILL_DAEMON_TEST_BIN) $(TEST_LDFLAGS) $(MOVIE_BACKFILL_DAEMON_TEST_LDFLAGS)
	./$(MOVIE_BACKFILL_DAEMON_TEST_BIN)

tests-movie-kind-filter: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(MOVIE_KIND_FILTER_TEST_CFLAGS) $(INCLUDES) $(MOVIE_KIND_FILTER_TEST_SRCS) -o $(MOVIE_KIND_FILTER_TEST_BIN) $(TEST_LDFLAGS) $(MOVIE_KIND_FILTER_TEST_LDFLAGS)
	./$(MOVIE_KIND_FILTER_TEST_BIN)

tests-adult-backfill: libbloom
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) $(TEST_CFLAGS) $(ADULT_BACKFILL_TEST_CFLAGS) $(INCLUDES) $(ADULT_BACKFILL_TEST_SRCS) -o $(ADULT_BACKFILL_TEST_BIN) $(TEST_LDFLAGS) $(ADULT_BACKFILL_TEST_LDFLAGS)
	./$(ADULT_BACKFILL_TEST_BIN)

tests-asan:
	$(MAKE) tests TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

# Scoped sanitizer gate for the CRAM engine: the loader mallocs a whole file and
# cram_features() mallocs its raw-hit scratch per call, so leak/UB coverage of
# this binary alone is the point.
tests-asan-cram:
	$(MAKE) tests-cram TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

# Scoped sanitizer gate for the backfill engine: builds and runs only that test
# binary, so its leak/UB verdict never depends on the other test binaries.
tests-asan-backfill:
	$(MAKE) tests-backfill TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

# Same scoped gate for the daemon-thread test (threads + sanitizers).
tests-asan-backfill-daemon:
	$(MAKE) tests-backfill-daemon TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

# Scoped sanitizer gates for the two movie-kind backfill test binaries.
tests-asan-movie-kind:
	$(MAKE) tests-movie-kind TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

tests-asan-movie-backfill:
	$(MAKE) tests-movie-backfill TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

tests-asan-movie-backfill-daemon:
	$(MAKE) tests-movie-backfill-daemon TEST_CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer" TEST_LDFLAGS="-fsanitize=address,undefined"

# ---------------------------------------------------------------------------
# Optional classifier flavours
#
# Plain `make` compiles both classifiers INERT: src/classifier.c needs
# -DCLF_ENABLED and src/movie_kind.c needs -DMK_ENABLED, and neither is defined
# above. That is deliberate -- it keeps the default build free of tparse and
# llama.cpp -- but it means a plain binary only crawls. Two flavours turn the
# engines on:
#
#   make clf    CRAM (category + tv_kind + adult) only. Links the vendored
#               tparse; no llama.cpp, no weights.
#   make full   CRAM + movie-kind. Needs `make llama` first (pinned llama.cpp
#               under build/llama-install, or LLAMA_INSTALL=<prefix>) and
#               compiles MK_NFC_ENABLED against the vendored utf8proc, i.e.
#               exactly the feature set the shipped Android binary has.
#
# Neither flavour embeds weights: `make models` fetches the pinned v13/v15
# files into ./models, which is where the compiled-in defaults look.
# ---------------------------------------------------------------------------

# tparse: libc-only C11 release-name / file-list parser, vendored verbatim from
# the classifier-model repo (native/tparse). See tparse/README.md.
TPARSE_DIR = tparse
TPARSE_SRCS = $(TPARSE_DIR)/tparse.c $(TPARSE_DIR)/name.c
CLF_INCLUDES = -I$(TPARSE_DIR)

# llama.cpp install prefix: static, CPU-only, pinned 18ab62a8. `make llama`
# produces it; point LLAMA_INSTALL elsewhere to reuse another build of the pin.
LLAMA_INSTALL ?= build/llama-install
LLAMA_LIBS = $(LLAMA_INSTALL)/lib/libllama.a $(LLAMA_INSTALL)/lib/libggml.a \
             $(LLAMA_INSTALL)/lib/libggml-cpu.a $(LLAMA_INSTALL)/lib/libggml-base.a

CLF_DIR = build/clf
CLF_TARGET = $(CLF_DIR)/dht_crawler
CLF_OBJS = $(SRCS:$(SRC_DIR)/%.c=$(CLF_DIR)/%.o)

FULL_DIR = build/full
FULL_TARGET = $(FULL_DIR)/dht_crawler
FULL_OBJS = $(SRCS:$(SRC_DIR)/%.c=$(FULL_DIR)/%.o)
FULL_DEFS = -DCLF_ENABLED -DMK_ENABLED -DMK_NFC_ENABLED
FULL_INCLUDES = $(CLF_INCLUDES) -I$(UTF8PROC_DIR) -I$(LLAMA_INSTALL)/include
FULL_LIBS = $(TPARSE_SRCS) $(UTF8PROC_DIR)/utf8proc.c $(LLAMA_LIBS)

$(CLF_DIR):
	mkdir -p $(CLF_DIR)

$(CLF_DIR)/%.o: $(SRC_DIR)/%.c | $(CLF_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -DCLF_ENABLED $(CLF_INCLUDES) -c $< -o $@

$(CLF_TARGET): $(CLF_OBJS) $(LIB_OBJS) $(TPARSE_SRCS)
	$(CC) $(CFLAGS) -o $@ $(CLF_OBJS) $(LIB_OBJS) $(TPARSE_SRCS) $(LDFLAGS)

clf: dirs patch-bencode patch-civetweb libbloom $(CLF_TARGET)
	@echo "built $(CLF_TARGET) (CRAM enabled; run 'make models' if ./models is empty)"

# Historical name: the same flavour was used for --classify-backfill runs.
clf-backfill: clf

# Fail with a readable message instead of a wall of "llama.h: No such file".
check-llama:
	@for l in $(LLAMA_LIBS); do \
		[ -f "$$l" ] || { \
			echo "full: missing $$l"; \
			echo "  run 'make llama' (builds the pinned 18ab62a8), or set"; \
			echo "  LLAMA_INSTALL=<prefix> for an existing build of that pin."; \
			exit 1; }; \
	done

$(FULL_DIR):
	mkdir -p $(FULL_DIR)

$(FULL_DIR)/%.o: $(SRC_DIR)/%.c | $(FULL_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) $(FULL_DEFS) $(FULL_INCLUDES) -c $< -o $@

$(FULL_TARGET): check-llama $(FULL_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) -o $@ $(FULL_OBJS) $(LIB_OBJS) $(FULL_LIBS) $(LDFLAGS) -lstdc++

full: dirs patch-bencode patch-civetweb libbloom $(FULL_TARGET)
	@echo "built $(FULL_TARGET) (CRAM + movie-kind; run 'make models' if ./models is empty)"

# Weights and the llama.cpp toolchain. Both are cached and sha-pinned.
models:
	tools/fetch-models.sh

llama:
	tools/build-llama.sh --prefix $(LLAMA_INSTALL)

.PHONY: run debug asan tsan valgrind tests tests-http-fetch tests-search tests-docs tests-inflight tests-stats-window tests-meta-peer-fail tests-cram tests-backfill tests-backfill-daemon tests-adult-backfill tests-movie-kind tests-movie-kind-render tests-movie-backfill tests-movie-backfill-daemon tests-movie-kind-filter tests-asan tests-asan-cram tests-asan-backfill tests-asan-backfill-daemon tests-asan-movie-backfill tests-asan-movie-backfill-daemon
