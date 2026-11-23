#ifndef TPARSE_TPARSE_H_
#define TPARSE_TPARSE_H_

#include <stddef.h>
#include <stdint.h>

#include "taxonomy.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct { const char *path; int64_t size; } tp_file_t;

typedef struct {
    int category_hint;              /* tp_category_t enum, or TP_CAT_UNKNOWN */
    int tv_kind;                    /* tp_tv_kind_t decision, or TP_TV_NONE */
    int season_lo, season_hi, episode_lo, episode_hi;
    int resolution;                 /* 0, 480, 720, 1080, 2160 */
    int n_video, n_audio, n_image, n_archive, n_exec, n_doc;
    unsigned flags;                 /* TP_F_* bit flags */
} tp_result_t;

/* name-level flag bits (set from Stage 1b on) */
enum {
    TP_F_JAV_CODE   = 1u << 0,
    TP_F_XXX_TOKEN  = 1u << 1,
    TP_F_CJK        = 1u << 2,
    TP_F_CYRILLIC   = 1u << 3,
    TP_F_URL        = 1u << 4,
    TP_F_COMPLETE   = 1u << 5,
    TP_F_INCOMPLETE = 1u << 6
};

/* tp_parse return codes */
#define TP_OK     0
#define TP_EINVAL 1

/*
 * Parse a release name plus its file list into tp_result_t.
 * `name` and `out` must be non-NULL. `files` may be NULL and `n_files` 0.
 * If `total_size` is 0 and files are given, the file sizes are summed.
 * Returns TP_OK on success, TP_EINVAL on invalid arguments.
 */
int    tp_parse(const char *name, const tp_file_t *files, int n_files, int64_t total_size, tp_result_t *out);

/*
 * Render space-separated fastText features into `buf`:
 * normalised name tokens (split on non-alphanumeric, lowercased) followed by
 * `__n<bucket>_<n>` count tokens and `__ncat_<category>` . When a file list is
 * given, file-derived tokens follow: `__nfiles_`/`__sep_`/`__ep_`/`__fres_`/
 * `__ftv_` and the most frequent file-extension tokens `__ext_<ext>` (at most
 * 8, each an alnum extension of <= 8 chars).
 * Returns the number of characters needed (excluding NUL). If `cap` > 0,
 * writes at most `cap - 1` characters plus a NUL (truncating if needed);
 * if `cap` == 0 or `buf` is NULL, writes nothing.
 */
size_t tp_feature_text(const char *name, const tp_file_t *files, int n_files, const tp_result_t *r, char *buf, size_t cap);

#ifdef __cplusplus
}
#endif

#endif /* TPARSE_TPARSE_H_ */
