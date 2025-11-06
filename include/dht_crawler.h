#ifndef DHT_CRAWLER_H
#define DHT_CRAWLER_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uv.h>

/* Version information */
#define DHT_CRAWLER_VERSION "0.1.0"

/* Configuration constants */
#define DHT_PORT 6881
#define MAX_WORKERS 10
#define INFOHASH_QUEUE_SIZE 10000
#define BATCH_SIZE 500
#define UDP_RECV_BUFFER_SIZE 65536
#define SHA1_DIGEST_LENGTH 20
#define NODE_ID_LENGTH 20

/* Log levels */
typedef enum {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
} log_level_t;

/* Global application context */
typedef struct {
    uv_loop_t *loop;
    int running;
    char node_id[NODE_ID_LENGTH];  /* DHT node ID */
    const char *db_path;
    int dht_port;                   /* DHT port (6881) */
    log_level_t log_level;
} app_context_t;

/* Function declarations */
void log_msg(log_level_t level, const char *format, ...);
void init_app_context(app_context_t *ctx);
void cleanup_app_context(app_context_t *ctx);

#endif /* DHT_CRAWLER_H */
