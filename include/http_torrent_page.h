#ifndef HTTP_TORRENT_PAGE_H
#define HTTP_TORRENT_PAGE_H

struct mg_connection;

/* CivetWeb request handlers. cbdata is http_api_t*. */
int http_torrent_page_handler(struct mg_connection *conn, void *cbdata);
int http_torrent_files_handler(struct mg_connection *conn, void *cbdata);

#endif /* HTTP_TORRENT_PAGE_H */
