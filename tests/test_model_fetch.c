/**
 * tests/test_model_fetch.c — `--fetch-models`, the weights download.
 *
 * Shape: no test framework, no database, no crawler, and no real network. A
 * scripted server is forked for the case that must answer an HTTP request
 * (a manifest that is then rejected); every other case either parses a fixture
 * or points the fetcher at a closed port, which is what proves the no-op path
 * never dials out at all.
 *
 *   (a) a manifest whose sha256 does not match its pin is refused, and nothing
 *       is installed — the root of trust is the manifest, so it must not get to
 *       name any file
 *   (b) manifest parsing yields the right per-file sha256s from both real
 *       shapes: the committed v15 manifest (models.<name>) and a v13-shaped one
 *       (backbone + head), and it does NOT pick up v13's `tokenizer` pair,
 *       which names a file the generation does not ship
 *   (c) an already-present, correct tree is a no-op: exit 0, no network, no
 *       .tmp files left behind, and the files untouched
 *   (d) --force really re-downloads: the same correct tree against a closed
 *       port must fail
 *
 * Runs with cwd = the tree root (same convention as the other host tests), so
 * the v15 fixture is resolved exactly like tests-cram resolves it.
 */

#define _GNU_SOURCE

#include "model_fetch.h"

#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define V15_FIXTURE_DIR "tests/fixtures/cram/models/v15"
#define V15_MANIFEST V15_FIXTURE_DIR "/manifest.json"
/* Distinct from the binary's own path (build/test_model_fetch). */
#define WORK_DIR "build/test_model_fetch_work"
#define WORK_MODELS WORK_DIR "/models"

static int failures = 0;
static int checks = 0;

#define CHECK(cond, ...)                                              \
    do {                                                              \
        checks++;                                                     \
        if (!(cond)) {                                                \
            failures++;                                               \
            printf("  FAIL %s:%d: ", __FILE__, __LINE__);             \
            printf(__VA_ARGS__);                                      \
            printf("\n");                                             \
        }                                                             \
    } while (0)

/* ---- scripted server (the shape tests/test_http_fetch.c uses) ----------- */

typedef struct {
    pid_t pid;
    int port;
} srv_t;

static int server_start(srv_t *srv, const char *body, int connections) {
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) return -1;
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(ls, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(ls);
        return -1;
    }
    if (listen(ls, 8) != 0) {
        close(ls);
        return -1;
    }
    socklen_t alen = sizeof(addr);
    if (getsockname(ls, (struct sockaddr *)&addr, &alen) != 0) {
        close(ls);
        return -1;
    }
    srv->port = ntohs(addr.sin_port);

    fflush(NULL);
    pid_t pid = fork();
    if (pid < 0) {
        close(ls);
        return -1;
    }
    if (pid == 0) {
        /* child: answer `connections` requests with the same 200 + body */
        char resp[4096];
        int n = snprintf(resp, sizeof(resp),
                         "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
                         strlen(body), body);
        if (n < 0 || (size_t)n >= sizeof(resp)) _exit(2);
        for (int i = 0; i < connections; i++) {
            int fd = accept(ls, NULL, NULL);
            if (fd < 0) _exit(3);
            char buf[2048];
            if (recv(fd, buf, sizeof(buf), 0) <= 0) {
                close(fd);
                _exit(4);
            }
            size_t off = 0;
            while (off < (size_t)n) {
                ssize_t w = send(fd, resp + off, (size_t)n - off, 0);
                if (w <= 0) break;
                off += (size_t)w;
            }
            close(fd);
        }
        _exit(0);
    }

    close(ls);
    srv->pid = pid;
    return 0;
}

static void server_stop(srv_t *srv) {
    if (srv->pid <= 0) return;
    int status = 0;
    for (int i = 0; i < 40; i++) {
        pid_t r = waitpid(srv->pid, &status, WNOHANG);
        if (r == srv->pid) {
            srv->pid = 0;
            return;
        }
        usleep(50000);
    }
    kill(srv->pid, SIGKILL);
    waitpid(srv->pid, &status, 0);
    srv->pid = 0;
}

/* ---- small helpers ------------------------------------------------------ */

static int file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static int copy_file(const char *from, const char *to) {
    FILE *in = fopen(from, "rb");
    if (!in) return -1;
    FILE *out = fopen(to, "wb");
    if (!out) {
        fclose(in);
        return -1;
    }
    char buf[65536];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, n, out) != n) {
            fclose(in);
            fclose(out);
            return -1;
        }
    }
    fclose(in);
    return fclose(out) == 0 ? 0 : -1;
}

static char *read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long size = ftell(f);
    if (size < 0 || size > (4 << 20)) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    char *buf = malloc((size_t)size + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t got = fread(buf, 1, (size_t)size, f);
    fclose(f);
    buf[got] = '\0';
    return buf;
}

/* Copy the committed v15 fixture tree into <WORK_MODELS>/v15. */
static int install_v15_fixture(void) {
    const char *names[] = {"manifest.json", "cram.bin", "cram_vocab.bin", NULL};
    char dir[256];
    snprintf(dir, sizeof(dir), "%s/v15", WORK_MODELS);
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "mkdir -p '%s'", dir);
    if (system(cmd) != 0) return -1;
    for (int i = 0; names[i]; i++) {
        char from[1024], to[1024];
        snprintf(from, sizeof(from), "%s/%s", V15_FIXTURE_DIR, names[i]);
        snprintf(to, sizeof(to), "%s/%s", dir, names[i]);
        if (copy_file(from, to) != 0) return -1;
    }
    return 0;
}

/* ---- (a) a manifest that does not match its pin is refused -------------- */

static void case_manifest_pin_mismatch(void) {
    const char *body = "{\"manifest_version\": 1, \"version\": \"v15\"}";
    const char *gens[] = {"v15"};
    model_fetch_options_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.dir = WORK_DIR "/pin";
    opts.generations = gens;
    opts.generation_count = 1;
    opts.log = stderr;

    char base[128];
    srv_t srv;
    CHECK(server_start(&srv, body, 1) == 0, "(a) server start");
    snprintf(base, sizeof(base), "http://127.0.0.1:%d", srv.port);
    opts.base = base;

    int rc = model_fetch_run(&opts);
    server_stop(&srv);

    CHECK(rc != 0, "(a) a manifest that does not match its pin must fail the run");
    CHECK(!file_exists(WORK_DIR "/pin/v15/manifest.json"),
          "(a) the rejected manifest must not be installed");
    CHECK(!file_exists(WORK_DIR "/pin/v15/manifest.json.tmp"),
          "(a) the rejected manifest must not be left behind as a .tmp");
}

/* ---- (b) manifest parsing ----------------------------------------------- */

static void case_parse_manifest(void) {
    char files[MODEL_FETCH_MAX_FILES][MODEL_FETCH_NAME_CAP];
    char shas[MODEL_FETCH_MAX_FILES][MODEL_FETCH_SHA_CAP];

    /* The committed v15 manifest is the real shape: models.<name>. */
    char *text = read_file(V15_MANIFEST);
    CHECK(text != NULL, "(b) cannot read %s", V15_MANIFEST);
    if (text) {
        int n = model_fetch_parse_manifest(text, files, shas, MODEL_FETCH_MAX_FILES);
        free(text);
        CHECK(n == 2, "(b) v15 manifest should declare 2 files, got %d", n);
        if (n == 2) {
            CHECK(strcmp(files[0], "cram.bin") == 0 && strcmp(files[1], "cram_vocab.bin") == 0,
                  "(b) v15 file names: got '%s' and '%s'", files[0], files[1]);
            CHECK(strcmp(shas[0], "834159b642d67da728bbdc3411badf83b639742743c95d414fc62ddd81e45bec") == 0,
                  "(b) cram.bin sha: got %s", shas[0]);
            CHECK(strcmp(shas[1], "6e24c4b27e3bd546a7dad0cb9a929dc7f354e664139604f0bb75f3ca1cf21cee") == 0,
                  "(b) cram_vocab.bin sha: got %s", shas[1]);
        }
    }

    /* The v13 shape, trimmed to the parts the parser cares about - including a
     * `tokenizer` pair, which names a file the generation does not ship and
     * therefore must NOT be collected. */
    static const char v13[] =
        "{\"manifest_version\": 1, \"version\": \"v13\","
        " \"backbone\": {\"file\": \"movie_kind_backbone_Q4_0.gguf\", \"bytes\": 501452416,"
        "  \"sha256\": \"28270fa01100687969b541282814371062ab9e1a560bb7ea755243c822cf0d4c\"},"
        " \"head\": {\"file\": \"movie_kind_head.bin\", \"bytes\": 65712,"
        "  \"sha256\": \"2f994cbe792441be182717cc6faa46f3ff245db6d17af30d72cfc47bbefb7ae3\"},"
        " \"tokenizer\": {\"file\": \"tokenizer.json\","
        "  \"sha256\": \"19397d6fcb490b1a0df7cf34a470dc806f32f6721ff894e3c726af7555b7f9d4\"}}";

    int n = model_fetch_parse_manifest(v13, files, shas, MODEL_FETCH_MAX_FILES);
    CHECK(n == 2, "(b) v13-shaped manifest should declare 2 files, got %d", n);
    if (n == 2) {
        CHECK(strcmp(files[0], "movie_kind_backbone_Q4_0.gguf") == 0, "(b) backbone file: %s", files[0]);
        CHECK(strcmp(files[1], "movie_kind_head.bin") == 0, "(b) head file: %s", files[1]);
        CHECK(strcmp(shas[0], "28270fa01100687969b541282814371062ab9e1a560bb7ea755243c822cf0d4c") == 0,
              "(b) backbone sha: %s", shas[0]);
        CHECK(strcmp(shas[1], "2f994cbe792441be182717cc6faa46f3ff245db6d17af30d72cfc47bbefb7ae3") == 0,
              "(b) head sha: %s", shas[1]);
        int saw_tokenizer = 0;
        for (int i = 0; i < n; i++)
            if (strcmp(files[i], "tokenizer.json") == 0) saw_tokenizer = 1;
        CHECK(!saw_tokenizer, "(b) a provenance entry is not a model file to download");
    }

    CHECK(model_fetch_parse_manifest("not json at all", files, shas, MODEL_FETCH_MAX_FILES) < 0,
          "(b) invalid JSON must be refused");
    CHECK(model_fetch_parse_manifest("{}", files, shas, MODEL_FETCH_MAX_FILES) == 0,
          "(b) a manifest with no specs yields zero, not an error");
}

/* ---- (c)/(d) no-op and force -------------------------------------------- */

static void case_noop_and_force(void) {
    const char *gens[] = {"v15"};
    model_fetch_options_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.dir = WORK_MODELS;
    opts.generations = gens;
    opts.generation_count = 1;
    opts.log = stderr;

    /* A closed port: if the no-op path dials out at all, it fails. */
    opts.base = "http://127.0.0.1:1";

    struct stat before, after;
    CHECK(stat(WORK_MODELS "/v15/cram.bin", &before) == 0, "(c) fixture tree missing");
    int rc = model_fetch_run(&opts);
    CHECK(rc == 0, "(c) a complete, verified tree must be a no-op (got %d)", rc);
    CHECK(stat(WORK_MODELS "/v15/cram.bin", &after) == 0, "(c) cram.bin vanished");
    CHECK(before.st_size == after.st_size && before.st_mtime == after.st_mtime,
          "(c) the no-op path must not rewrite a verified file");
    CHECK(!file_exists(WORK_MODELS "/v15/cram.bin.tmp"), "(c) no .tmp may be left behind");

    /* --force against the same closed port must actually try. */
    opts.force = 1;
    rc = model_fetch_run(&opts);
    CHECK(rc != 0, "(d) --force must re-download, so a closed port must fail the run");
    CHECK(!file_exists(WORK_MODELS "/v15/manifest.json.tmp"), "(d) no .tmp may survive a failure");
}

int main(void) {
    printf("test_model_fetch: --fetch-models (pinned manifests, sha256, no-op)\n");

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s' && mkdir -p '%s'", WORK_DIR, WORK_MODELS);
    if (system(cmd) != 0) {
        printf("test_model_fetch: cannot prepare %s\n", WORK_DIR);
        return 1;
    }

    case_manifest_pin_mismatch();
    case_parse_manifest();

    if (install_v15_fixture() != 0) {
        printf("  FAIL cannot copy the v15 fixture from %s\n", V15_FIXTURE_DIR);
        failures++;
    } else {
        case_noop_and_force();
    }

    printf("test_model_fetch: %d checks, %d failure(s)\n", checks, failures);
    if (failures == 0) printf("test_model_fetch: all checks passed\n");
    return failures == 0 ? 0 : 1;
}
