/**
 * Host unit test for the movie-kind module: the JEV head
 * (include/jevhead.h, src/jevhead.c) and the manifest half of
 * include/movie_kind.h (src/movie_kind.c).
 *
 * Exercises the real product code, not a copy of it:
 *   - `jevhead_load` / `jevhead_logits` / `jevhead_predict` against the
 *     committed reference fixtures (tests/fixtures/head_8x3.bin and
 *     vectors.txt, imported from classifier-model `native/jevhead/fixtures/`
 *     with their shas recorded in the header comments). Masks and bit counts
 *     are compared exactly, probabilities to 1e-5 (float logits + double
 *     sigmoid in C versus the Python reference).
 *   - the `>=` threshold boundary: a probability of exactly 0.5 against a 0.5
 *     cut is a hit (the rule `jev.apply_thresholds` uses), 0.5 against a
 *     slightly higher cut is not.
 *   - the manifest gate: generation resolution from `manifest.version`
 *     ("v9" -> 9), genre order ("bit j = genres[j]"), and the refusal paths —
 *     missing manifest, unusable version, genre/label disagreement, missing
 *     file, wrong file size, wrong sha256 — each of which must leave the
 *     module disabled and bump `model_load_failures`.
 *
 * The encoder half (llama.cpp + utf8proc) is MK_ENABLED-gated and CANNOT link
 * here: utf8proc is vendored in the outer repo only (movie-kind Stage A), so
 * the NFC cases below are compiled only in an MK_ENABLED host build. The
 * on-device spot check covers NFC in the shipped binary.
 *
 * Standalone: no test framework, no dependency on the daemon build (the test
 * links src/movie_kind.c, src/jevhead.c and lib/cJSON/cJSON.c, with
 * tests/stub_log.c for log_msg()).
 *
 *     make tests        # or: make tests-movie-kind
 */

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "clf_sha256.h"
#include "jevhead.h"
#include "movie_kind.h"

#define FIXTURE_DIR "tests/fixtures"
#define FIXTURE_HEAD FIXTURE_DIR "/head_8x3.bin"
#define FIXTURE_VECTORS FIXTURE_DIR "/vectors.txt"

#define SCRATCH_DIR "/tmp/movie-kind-test"
#define BACKBONE_BYTES 4096
#define HEAD_BYTES 168
#define BACKBONE_FILE "movie_kind_backbone.gguf"
#define HEAD_FILE "movie_kind_head.bin"

static int failures = 0;

static void fail(const char *what, const char *detail) {
    fprintf(stderr, "FAIL: %s (%s)\n", what, detail != NULL ? detail : "");
    failures++;
}

static void fail_int(const char *what, long long got, long long expected) {
    fprintf(stderr, "FAIL: %s (got %lld, expected %lld)\n", what, got, expected);
    failures++;
}

/* ---- fixtures ---------------------------------------------------------- */

/* The v9 taxonomy in manifest order. Bit j of a mask is GENRES[j]. */
static const char *const GENRES[MOVIE_KIND_MAX_GENRES] = {
    "action", "comedy",  "drama",    "horror", "thriller", "scifi",
    "fantasy", "animation", "documentary", "romance", "crime", "adventure",
    "family", "mystery", "war",      "western",
};

static int write_file(const char *path, const void *data, size_t size) {
    FILE *fh = fopen(path, "wb");
    if (fh == NULL) return -1;
    size_t written = fwrite(data, 1, size, fh);
    fclose(fh);
    return written == size ? 0 : -1;
}

static void fill_pattern(unsigned char *buffer, size_t size, unsigned seed) {
    for (size_t index = 0; index < size; index++) {
        buffer[index] = (unsigned char)((index * 31u + seed) & 0xFFu);
    }
}

/* ---- JEV head ---------------------------------------------------------- */

static void case_head_errors(void) {
    char err[256];
    jevhead head;

    if (jevhead_load("/nonexistent/movie-kind-head.bin", &head, err, sizeof(err)) != JEVHEAD_ERR_IO) {
        fail("a missing head file is JEVHEAD_ERR_IO", err);
    }

    /* A 40-byte prefix: shorter than the 48-byte JEVH header. */
    unsigned char truncated[JEVHEAD_HEADER_SIZE];
    memset(truncated, 0, sizeof(truncated));
    memcpy(truncated, JEVHEAD_MAGIC, 4);
    char path[512];
    snprintf(path, sizeof(path), "%s/head-truncated.bin", SCRATCH_DIR);
    if (write_file(path, truncated, 40) != 0) {
        fail("the truncated head is writable", path);
        return;
    }
    if (jevhead_load(path, &head, err, sizeof(err)) != JEVHEAD_ERR_SHAPE) {
        fail("a 40-byte head is JEVHEAD_ERR_SHAPE", err);
    }

    /* Right size, wrong magic. */
    unsigned char wrong_magic[48];
    memset(wrong_magic, 0, sizeof(wrong_magic));
    memcpy(wrong_magic, "JEVX", 4);
    snprintf(path, sizeof(path), "%s/head-wrong-magic.bin", SCRATCH_DIR);
    if (write_file(path, wrong_magic, sizeof(wrong_magic)) != 0) {
        fail("the wrong-magic head is writable", path);
        return;
    }
    if (jevhead_load(path, &head, err, sizeof(err)) != JEVHEAD_ERR_MAGIC) {
        fail("a non-JEVH magic is JEVHEAD_ERR_MAGIC", err);
    }

    remove(path);
    printf("  case 1 ok: jevhead_load refuses a missing/truncated/non-JEVH head\n");
}

static void case_head_fixture(void) {
    char err[256];
    jevhead head;

    int status = jevhead_load(FIXTURE_HEAD, &head, err, sizeof(err));
    if (status != JEVHEAD_OK) {
        fail("the fixture head loads", err);
        return;
    }
    if (head.version != JEVHEAD_VERSION || head.hidden != 8u || head.n_labels != 3u) {
        fail("the fixture head is JEVH v1 with 8 hidden and 3 labels", "header");
    }
    if (jevhead_expected_bytes(&head) != (size_t)JEVHEAD_HEADER_SIZE + 4u * (3u * 8u + 2u * 3u)) {
        fail("jevhead_expected_bytes is the header plus the payload", "size");
    }
    /* thresholds=[0.5, 0.4, 0.6], per the fixture header comment. */
    if (fabs((double)head.thresholds[0] - 0.5) > 1e-6 ||
        fabs((double)head.thresholds[1] - 0.4) > 1e-6 ||
        fabs((double)head.thresholds[2] - 0.6) > 1e-6) {
        fail("the fixture head carries the calibrated thresholds", "thresholds");
    }

    FILE *fh = fopen(FIXTURE_VECTORS, "r");
    if (fh == NULL) {
        fail("vectors.txt opens", FIXTURE_VECTORS);
        jevhead_free(&head);
        return;
    }

    char line[8192];
    unsigned long row = 0;
    while (fgets(line, sizeof(line), fh) != NULL) {
        char *cursor = line;
        while (*cursor == ' ' || *cursor == '\t') cursor++;
        if (*cursor == '#' || *cursor == '\n' || *cursor == '\0') continue;

        float embedding[8];
        int expected_bits[3];
        double expected_probability[3];
        for (unsigned index = 0; index < head.hidden; index++) {
            char *end = NULL;
            embedding[index] = strtof(cursor, &end);
            if (end == cursor) {
                fail("a vector row has all its embedding values", line);
                goto next_row;
            }
            cursor = end;
        }
        while (*cursor == ' ' || *cursor == '\t') cursor++;
        if (*cursor != '|') {
            fail("a vector row has its bit separator", line);
            goto next_row;
        }
        cursor++;
        for (unsigned label = 0; label < head.n_labels; label++) {
            char *end = NULL;
            expected_bits[label] = (int)strtol(cursor, &end, 10);
            if (end == cursor) {
                fail("a vector row has all its expected bits", line);
                goto next_row;
            }
            cursor = end;
        }
        while (*cursor == ' ' || *cursor == '\t') cursor++;
        if (*cursor != '|') {
            fail("a vector row has its probability separator", line);
            goto next_row;
        }
        cursor++;
        for (unsigned label = 0; label < head.n_labels; label++) {
            char *end = NULL;
            expected_probability[label] = strtod(cursor, &end);
            if (end == cursor) {
                fail("a vector row has all its expected probabilities", line);
                goto next_row;
            }
            cursor = end;
        }

        {
            uint32_t mask = 0;
            uint32_t count = 0;
            float logits[3];
            uint32_t expected_mask = 0;
            uint32_t expected_count = 0;
            for (unsigned label = 0; label < head.n_labels; label++) {
                if (expected_bits[label]) {
                    expected_mask |= (uint32_t)1u << label;
                    expected_count++;
                }
            }
            if (jevhead_predict(&head, embedding, &mask, &count) != JEVHEAD_OK) {
                fail("jevhead_predict succeeds", line);
                goto next_row;
            }
            if (mask != expected_mask) {
                fail_int("the fixture row's mask matches the reference", mask, expected_mask);
            }
            if (count != expected_count) {
                fail_int("the fixture row's bit count matches the reference", count,
                         expected_count);
            }
            /* The probabilities pin the *arithmetic*, not just the decisions: a
             * row whose probability sits next to its threshold would otherwise
             * pass on the mask alone and drift on the next re-fit. */
            if (jevhead_logits(&head, embedding, logits) != JEVHEAD_OK) {
                fail("jevhead_logits succeeds", line);
                goto next_row;
            }
            for (unsigned label = 0; label < head.n_labels; label++) {
                double probability = 1.0 / (1.0 + exp(-(double)logits[label]));
                if (fabs(probability - expected_probability[label]) > 1e-5) {
                    fprintf(stderr, "FAIL: row %lu label %u probability %.6f != %.6f\n", row,
                            label, probability, expected_probability[label]);
                    failures++;
                }
            }
            row++;
        }
next_row:
        continue;
    }
    fclose(fh);
    jevhead_free(&head);

    if (row != 6) {
        fail_int("vectors.txt holds the six committed vectors", (long long)row, 6);
    }
    printf("  case 2 ok: jevhead matches all 6 reference vectors (masks exact, probabilities 1e-5)\n");
}

/* A one-label head whose only logit is 0.0: sigmoid(0) == 0.5 exactly, which is
 * the boundary the `>=` rule exists for. */
static void case_threshold_boundary(void) {
    float weights[1] = {0.0f};
    float bias[1] = {0.0f};
    float thresholds[1] = {0.5f};
    float embedding[1] = {123.0f}; /* weight 0, so the dot product is 0 whatever this is */
    jevhead head;
    memset(&head, 0, sizeof(head));
    head.version = JEVHEAD_VERSION;
    head.hidden = 1;
    head.n_labels = 1;
    head.weights = weights;
    head.bias = bias;
    head.thresholds = thresholds;

    uint32_t mask = 0;
    uint32_t count = 0;
    if (jevhead_predict(&head, embedding, &mask, &count) != JEVHEAD_OK) {
        fail("the boundary head predicts", "predict");
        return;
    }
    if (mask != 1u || count != 1u) {
        fail("sigmoid(0) >= 0.5 is a hit (the `>=` boundary)", "mask");
    }

    thresholds[0] = 0.5000001f;
    if (jevhead_predict(&head, embedding, &mask, &count) != JEVHEAD_OK) {
        fail("the boundary head predicts", "predict");
        return;
    }
    if (mask != 0u || count != 0u) {
        fail("sigmoid(0) < 0.5000001 is a miss", "mask");
    }
    printf("  case 3 ok: a probability exactly at its threshold is a hit (`>=`, not `>`)\n");
}

/* ---- manifest gate ----------------------------------------------------- */

typedef struct {
    const char *version;      /* NULL omits the key entirely */
    long long backbone_bytes;
    const char *backbone_sha;
    long long head_bytes;
    const char *head_sha;
    long long n_labels;
    long long head_hidden;    /* 0 omits the key */
} manifest_spec_t;

static int write_manifest(const char *dir, const manifest_spec_t *spec) {
    char path[512];
    char text[4096];
    size_t used = 0;

    used += (size_t)snprintf(text + used, sizeof(text) - used, "{\n  \"genres\": [\n");
    for (int index = 0; index < MOVIE_KIND_MAX_GENRES; index++) {
        used += (size_t)snprintf(text + used, sizeof(text) - used, "    \"%s\"%s\n", GENRES[index],
                                 index + 1 < MOVIE_KIND_MAX_GENRES ? "," : "");
    }
    used += (size_t)snprintf(text + used, sizeof(text) - used,
                             "  ],\n  \"backbone\": {\"file\": \"%s\", \"bytes\": %lld, "
                             "\"sha256\": \"%s\", \"hidden\": 768},\n",
                             BACKBONE_FILE, spec->backbone_bytes, spec->backbone_sha);
    used += (size_t)snprintf(text + used, sizeof(text) - used,
                             "  \"head\": {\"file\": \"%s\", \"bytes\": %lld, \"sha256\": \"%s\", "
                             "\"n_labels\": %lld",
                             HEAD_FILE, spec->head_bytes, spec->head_sha, spec->n_labels);
    if (spec->head_hidden > 0) {
        used += (size_t)snprintf(text + used, sizeof(text) - used, ", \"hidden\": %lld",
                                 spec->head_hidden);
    }
    used += (size_t)snprintf(text + used, sizeof(text) - used, "}");
    if (spec->version != NULL) {
        used += (size_t)snprintf(text + used, sizeof(text) - used, ",\n  \"version\": \"%s\"",
                                 spec->version);
    }
    used += (size_t)snprintf(text + used, sizeof(text) - used, "\n}\n");

    snprintf(path, sizeof(path), "%s/manifest.json", dir);
    return write_file(path, text, used);
}

/* Stage a model directory whose pinned sizes/shas match the files it writes,
 * then let each case corrupt exactly one thing. */
static int stage_model_dir(char *backbone_sha, char *head_sha) {
    mkdir(SCRATCH_DIR, 0755);

    unsigned char backbone[BACKBONE_BYTES];
    unsigned char head[HEAD_BYTES];
    char path[512];

    fill_pattern(backbone, sizeof(backbone), 7u);
    fill_pattern(head, sizeof(head), 11u);

    snprintf(path, sizeof(path), "%s/%s", SCRATCH_DIR, BACKBONE_FILE);
    if (write_file(path, backbone, sizeof(backbone)) != 0) return -1;
    if (clf_sha256_file(path, backbone_sha) != 0) return -1;

    snprintf(path, sizeof(path), "%s/%s", SCRATCH_DIR, HEAD_FILE);
    if (write_file(path, head, sizeof(head)) != 0) return -1;
    if (clf_sha256_file(path, head_sha) != 0) return -1;

    return 0;
}

static void case_manifest_ok(void) {
    char backbone_sha[CLF_SHA_HEX_LEN + 1];
    char head_sha[CLF_SHA_HEX_LEN + 1];
    if (stage_model_dir(backbone_sha, head_sha) != 0) {
        fail("the model directory stages", SCRATCH_DIR);
        return;
    }
    manifest_spec_t spec = {
        .version = "v9",
        .backbone_bytes = BACKBONE_BYTES,
        .backbone_sha = backbone_sha,
        .head_bytes = HEAD_BYTES,
        .head_sha = head_sha,
        .n_labels = MOVIE_KIND_MAX_GENRES,
        .head_hidden = 768,
    };
    if (write_manifest(SCRATCH_DIR, &spec) != 0) {
        fail("the manifest is writable", SCRATCH_DIR);
        return;
    }

    if (movie_kind_init(SCRATCH_DIR) != 0) {
        fail("a pinned, verified manifest initialises the module", SCRATCH_DIR);
        return;
    }
    if (movie_kind_manifest_ok() != 1) {
        fail("the verified manifest reports manifest_ok", "manifest_ok");
    }
    /* "v9" -> 9, resolved from the string: the generation is never a literal. */
    if (movie_kind_generation() != 9) {
        fail_int("manifest.version \"v9\" resolves to generation 9", movie_kind_generation(), 9);
    }
    if (movie_kind_genre_count() != MOVIE_KIND_MAX_GENRES) {
        fail_int("the genre count follows the manifest", movie_kind_genre_count(),
                 MOVIE_KIND_MAX_GENRES);
    }
    /* Bit j is genres[j], in manifest order — never re-sorted, never remapped. */
    const char *expectations[] = {"action", "scifi", "western"};
    const int indices[] = {0, 5, 15};
    for (int index = 0; index < 3; index++) {
        const char *name = movie_kind_genre_name(indices[index]);
        if (name == NULL || strcmp(name, expectations[index]) != 0) {
            fail("genre bit -> manifest order", expectations[index]);
        }
    }
    if (movie_kind_genre_name(MOVIE_KIND_MAX_GENRES) != NULL ||
        movie_kind_genre_name(-1) != NULL) {
        fail("an out-of-range genre index answers NULL", "genre_name");
    }

    movie_kind_stats_t stats;
    movie_kind_get_stats(&stats);
    if (stats.generation != 9 || stats.genre_count != MOVIE_KIND_MAX_GENRES ||
        stats.n_labels != MOVIE_KIND_MAX_GENRES || stats.hidden != 768) {
        fail("the stats snapshot publishes the manifest shape", "get_stats");
    }
    if (strcmp(stats.backbone_sha, backbone_sha) != 0 || strcmp(stats.head_sha, head_sha) != 0) {
        fail("the stats snapshot publishes the verified shas", "get_stats");
    }
    if (stats.model_load_failures != 0) {
        fail_int("a verified manifest counts no load failure", (long long)stats.model_load_failures, 0);
    }

    /* The encoder half is compiled out of the host build: never a verdict. */
#ifndef MK_ENABLED
    if (movie_kind_enabled() != 0) {
        fail("the host build cannot classify (no MK_ENABLED)", "enabled");
    }
    if (movie_kind_model_ready() != 0) {
        fail("the host build has no model resident", "model_ready");
    }
    uint16_t mask = 0xFFFFu;
    if (movie_kind_classify("Some.2024.1080p.mkv", &mask) != 1) {
        fail("the host build declines every classify", "classify");
    }
    if (mask != 0xFFFFu) {
        fail("a declined classify leaves the mask untouched", "classify");
    }
    if (movie_kind_prompt_text("x", (char[64]){0}, 64) != -1) {
        fail("the host build has no NFC renderer", "prompt_text");
    }
#endif

    movie_kind_note_thermal_pause();
    movie_kind_note_thermal_pause();
    movie_kind_get_stats(&stats);
    if (stats.thermal_pauses != 2) {
        fail_int("thermal pauses are counted", (long long)stats.thermal_pauses, 2);
    }
    printf("  case 4 ok: verified manifest, generation 9, genre order, stats snapshot\n");
}

/* Every refusal path must leave the module disabled and count a load failure. */
static void expect_refusal(const char *what, const char *detail) {
    if (movie_kind_init(SCRATCH_DIR) != -1) {
        fail(what, "init returned 0");
        return;
    }
    if (movie_kind_manifest_ok() != 0) {
        fail(what, "manifest_ok stayed 1");
        return;
    }
    movie_kind_stats_t stats;
    movie_kind_get_stats(&stats);
    if (stats.model_load_failures == 0) {
        fail(what, "model_load_failures did not move");
        return;
    }
    printf("  ok: %s (%s)\n", what, detail);
}

static void case_refusals(void) {
    char backbone_sha[CLF_SHA_HEX_LEN + 1];
    char head_sha[CLF_SHA_HEX_LEN + 1];
    if (stage_model_dir(backbone_sha, head_sha) != 0) {
        fail("the model directory stages", SCRATCH_DIR);
        return;
    }

    manifest_spec_t good = {
        .version = "v9",
        .backbone_bytes = BACKBONE_BYTES,
        .backbone_sha = backbone_sha,
        .head_bytes = HEAD_BYTES,
        .head_sha = head_sha,
        .n_labels = MOVIE_KIND_MAX_GENRES,
        .head_hidden = 768,
    };

    /* Missing manifest. */
    {
        char path[512];
        snprintf(path, sizeof(path), "%s/manifest.json", SCRATCH_DIR);
        remove(path);
    }
    expect_refusal("a missing manifest is refused", "no manifest.json");

    /* A manifest with no usable version. */
    manifest_spec_t no_version = good;
    no_version.version = NULL;
    if (write_manifest(SCRATCH_DIR, &no_version) != 0) {
        fail("the version-less manifest is writable", SCRATCH_DIR);
        return;
    }
    expect_refusal("a manifest without a version is refused", "\"version\" absent");

    /* head.n_labels disagreeing with the genre list makes bit->genre undefined. */
    manifest_spec_t bad_labels = good;
    bad_labels.n_labels = MOVIE_KIND_MAX_GENRES - 1;
    if (write_manifest(SCRATCH_DIR, &bad_labels) != 0) {
        fail("the mismatched manifest is writable", SCRATCH_DIR);
        return;
    }
    expect_refusal("genres/labels disagreement is refused", "n_labels 15 vs 16 genres");

    /* A backbone sha that does not match the bytes on disk. */
    manifest_spec_t bad_sha = good;
    bad_sha.backbone_sha = "0000000000000000000000000000000000000000000000000000000000000000";
    if (write_manifest(SCRATCH_DIR, &bad_sha) != 0) {
        fail("the bad-sha manifest is writable", SCRATCH_DIR);
        return;
    }
    expect_refusal("a sha256 mismatch is refused", "backbone sha256");

    /* Wrong pinned size (the file is 168 bytes, the manifest says 167). */
    manifest_spec_t bad_size = good;
    bad_size.head_bytes = HEAD_BYTES - 1;
    if (write_manifest(SCRATCH_DIR, &bad_size) != 0) {
        fail("the bad-size manifest is writable", SCRATCH_DIR);
        return;
    }
    expect_refusal("a pinned-size mismatch is refused", "head 167 vs 168 bytes");

    /* A truncated head: the manifest is right, the file on disk is not. */
    if (write_manifest(SCRATCH_DIR, &good) != 0) {
        fail("the good manifest is writable", SCRATCH_DIR);
        return;
    }
    unsigned char short_head[100];
    fill_pattern(short_head, sizeof(short_head), 11u);
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", SCRATCH_DIR, HEAD_FILE);
    if (write_file(path, short_head, sizeof(short_head)) != 0) {
        fail("the truncated head is writable", path);
        return;
    }
    expect_refusal("a truncated head file is refused", "100 bytes on disk");

    /* A missing pinned file. */
    remove(path);
    expect_refusal("a missing pinned file is refused", "head file absent");

    printf("  case 5 ok: every manifest refusal path disables the module and counts a failure\n");
}

/* ---- NFC (MK_ENABLED only) --------------------------------------------- */

#ifdef MK_ENABLED
static void case_nfc(void) {
    char prompt[256];

    /* "Amélie" with a decomposed e + U+0301 must reach the model composed. */
    if (movie_kind_prompt_text("Am\u0065\u0301lie", prompt, sizeof(prompt)) != 0) {
        fail("the NFC renderer succeeds", "prompt_text");
        return;
    }
    if (strcmp(prompt, "Torrent: Am\u00e9lie\nFilm genres:") != 0) {
        fail("a decomposed name is NFC-composed in the serving text", prompt);
    }

    /* Invalid UTF-8 passes through untouched: DB names are not always valid, and
     * utf8proc_map() refuses such input rather than rewriting it. */
    const char invalid[] = "Bad\xFFname\xC3";
    if (movie_kind_prompt_text(invalid, prompt, sizeof(prompt)) != 0) {
        fail("the renderer accepts invalid UTF-8", "prompt_text");
        return;
    }
    if (strcmp(prompt, "Torrent: Bad\xFFname\xC3\nFilm genres:") != 0) {
        fail("invalid UTF-8 passes through untouched", "prompt_text");
    }
    printf("  case 6 ok: NFC composes, invalid UTF-8 passes through\n");
}
#endif

int main(void) {
    mkdir(SCRATCH_DIR, 0755);

    case_head_errors();
    case_head_fixture();
    case_threshold_boundary();
    case_manifest_ok();
    case_refusals();
#ifdef MK_ENABLED
    case_nfc();
#else
    printf("  (NFC cases skipped: this host build has no MK_ENABLED/utf8proc;\n"
           "   the on-device spot check covers them)\n");
#endif

    /* Leave nothing behind. */
    char path[512];
    snprintf(path, sizeof(path), "%s/manifest.json", SCRATCH_DIR);
    remove(path);
    snprintf(path, sizeof(path), "%s/%s", SCRATCH_DIR, BACKBONE_FILE);
    remove(path);
    snprintf(path, sizeof(path), "%s/%s", SCRATCH_DIR, HEAD_FILE);
    remove(path);
    rmdir(SCRATCH_DIR);

    if (failures != 0) {
        fprintf(stderr, "test_movie_kind: %d assertion(s) failed\n", failures);
        return 1;
    }
    printf("test_movie_kind: all assertions passed\n");
    return 0;
}
