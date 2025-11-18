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
 * - Replace dots and underscores with spaces
 * - Remove content in square brackets [...]
 * - Preserve years in parentheses (1900-2099), remove other tech info
 * - Truncate at season/episode markers (S01E02, Season 1, etc.)
 * - Strip all loaded keywords (case-insensitive)
 * - Collapse multiple spaces and trim
 *
 * @param torrent_name The raw torrent name
 * @return Newly allocated string with extracted title (caller must free),
 *         or NULL on error
 */
char* extract_media_title(const char *torrent_name);

#endif /* TORRENT_SEARCH_H */
