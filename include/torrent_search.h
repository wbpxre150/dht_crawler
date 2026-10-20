#ifndef TORRENT_SEARCH_H
#define TORRENT_SEARCH_H

/**
 * Initialize the torrent search module.
 *
 * Loads strip keywords from the specified file. Keywords are patterns
 * that will be removed from torrent names when extracting media titles.
 *
 * @param keyword_file_path Path to the keywords file (torrent_search_keywords.txt)
 * @return 0 on success, -1 on error
 */
int torrent_search_init(const char *keyword_file_path);

/**
 * Cleanup the torrent search module.
 *
 * Frees all allocated resources including the keyword hash table.
 */
void torrent_search_cleanup(void);

/**
 * Extract the movie/TV show title from a torrent name.
 *
 * This function filters out common torrent naming patterns to extract
 * just the media title suitable for searching on IMDB or similar services.
 *
 * Processing steps:
 * 0. Strip a leading tracker hostname prefix ("www.UIndex.org    -    ",
 *    "rarbg.com - ") together with its separator
 * 1. Replace dots and underscores with spaces
 * 2. Remove content in square brackets [...]
 * 3. Preserve years in parentheses (1900-2099), remove other tech info
 * 4. Cut at the first scene-release pivot (release grammar only):
 *      year pivot    "Some.Movie.2020.1080p..."  -> "Some Movie 2020"
 *      season pivot  "Show.Name.S01E05.1080p..." -> "Show Name"
 *      quality pivot "Some.Movie.1080p.BluRay..." -> "Some Movie"
 *    Pivot paths do not strip keywords - the grammar alone decides where
 *    the title ends.
 * 5. Legacy path (no pivot): truncate at season/episode markers, then strip
 *    all loaded keywords (case-insensitive)
 * 6. Collapse multiple spaces, drop leading/trailing separators and trim
 *
 * @param torrent_name The raw torrent name
 * @return Newly allocated string with extracted title (caller must free),
 *         or NULL on error
 */
char* extract_media_title(const char *torrent_name);

#endif /* TORRENT_SEARCH_H */
