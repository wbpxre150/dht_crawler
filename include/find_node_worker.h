#ifndef FIND_NODE_WORKER_H
#define FIND_NODE_WORKER_H

#include "dht_manager.h"
#include "discovered_nodes.h"
#include "worker_pool.h"

/**
 * Find Node Worker Pool
 *
 * Continuously pulls nodes from the discovered nodes queue and sends
 * find_node queries to expand the routing table. Each successful response
 * typically returns 8 new nodes, creating exponential growth.
 */

/* Task data for find_node worker */
typedef struct {
    dht_manager_t *mgr;
    discovered_node_t node;
} find_node_task_t;

/**
 * Initialize find_node worker pool
 * @param mgr DHT manager context
 * @param num_workers Number of worker threads
 * @return 0 on success, -1 on error
 */
int find_node_worker_init(dht_manager_t *mgr, int num_workers);

/**
 * Shutdown find_node worker pool
 * @param mgr DHT manager context
 */
void find_node_worker_shutdown(dht_manager_t *mgr);

/**
 * Worker function that processes discovered nodes
 * This runs continuously, pulling from the discovered_nodes queue
 */
void find_node_worker_func(void *arg);

#endif /* FIND_NODE_WORKER_H */
