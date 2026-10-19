/**
 * No-op log_msg() stub for host unit tests.
 *
 * src/torrent_search.c calls log_msg(), which is normally defined in
 * src/main.c. The unit tests link src/torrent_search.c without src/main.c,
 * so this stub satisfies the symbol declared in include/dht_crawler.h.
 */
#include "dht_crawler.h"

void log_msg(log_level_t level, const char *format, ...) {
    (void)level;
    (void)format;
}
