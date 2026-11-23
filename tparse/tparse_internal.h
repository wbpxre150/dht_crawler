#ifndef TPARSE_TPARSE_INTERNAL_H_
#define TPARSE_TPARSE_INTERNAL_H_

#include "tparse.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Name-level parse results (Stage 1b). Internal only: the public
 * tp_result_t has no source field, so the source token is exposed
 * through tp_feature_text instead.
 */
typedef enum {
    TP_SRC_NONE = 0,
    TP_SRC_HDTV, TP_SRC_PDTV, TP_SRC_WEBDL, TP_SRC_WEBRIP, TP_SRC_WEBCAP,
    TP_SRC_WEB, TP_SRC_BLURAY, TP_SRC_BRRIP, TP_SRC_BDRIP, TP_SRC_DVDRIP,
    TP_SRC_DVD, TP_SRC_HDRIP, TP_SRC_CAM, TP_SRC_TS, TP_SRC_TELESYNC,
    TP_SRC_TC, TP_SRC_VHS, TP_SRC_VOD, TP_SRC_DSR
} tp_source_t;

typedef struct {
    int tv_kind;            /* tp_tv_kind_t decision from the name alone */
    int season_lo, season_hi;
    int episode_lo, episode_hi;
    int resolution;         /* 0, 480, 720, 1080, 2160 */
    int source;             /* tp_source_t */
    unsigned flags;         /* TP_F_* found in the name */
    int cat_hint;           /* TP_CAT_* found in the name, or TP_CAT_UNKNOWN */
} tp_name_info_t;

/* Scan `name` with the hand-written release-name matchers. `name` must be
 * non-NULL; `out` is fully overwritten (zeroed fields, cat_hint unknown). */
void tp_name_scan(const char *name, tp_name_info_t *out);

/* Stage 1c: scan the basename of a file `path` (after the last '/', without
 * its extension) as a release name. `out` is always fully overwritten, even
 * when `path` is NULL. */
void tp_path_scan(const char *path, tp_name_info_t *out);

/* Stage 1c: true when the normalised form of `path` carries a sample/extras
 * token (whole-token, case-insensitive), so the file must not contribute
 * season/episode evidence. */
int tp_path_is_excluded(const char *path);

/* Lowercase feature-token name for a tp_source_t (never NULL). */
const char *tp_source_name(int source);

#ifdef __cplusplus
}
#endif

#endif /* TPARSE_TPARSE_INTERNAL_H_ */
