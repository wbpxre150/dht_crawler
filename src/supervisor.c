#include "supervisor.h"
#include "dht_crawler.h"
#include "batch_writer.h"
#include "bloom_filter.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Forward declaration of internal helpers */
static thread_tree_t *spawn_tree(supervisor_t *sup);
static void *monitor_thread_func(void *arg);

supervisor_t *supervisor_create(supervisor_config_t *config) {
    if (!config || config->max_trees <= 0) {
        log_msg(LOG_ERROR, "[supervisor] Invalid config");
        return NULL;
    }

    supervisor_t *sup = calloc(1, sizeof(supervisor_t));
    if (!sup) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate supervisor");
        return NULL;
    }

    sup->max_trees = config->max_trees;
    sup->active_trees = 0;
    sup->next_tree_id = 1;
    sup->monitor_running = 0;

    /* Store shared resources */
    sup->batch_writer = config->batch_writer;
    sup->bloom_filter = config->bloom_filter;

    /* Store worker counts */
    sup->min_metadata_rate = config->min_metadata_rate;
    sup->num_bep51_workers = config->num_bep51_workers;
    sup->num_get_peers_workers = config->num_get_peers_workers;
    sup->num_metadata_workers = config->num_metadata_workers;

    /* Stage 2 settings (Bootstrap) */
    sup->bootstrap_timeout_sec = config->bootstrap_timeout_sec > 0 ? config->bootstrap_timeout_sec : 30;
    sup->routing_threshold = config->routing_threshold > 0 ? config->routing_threshold : 200;

    /* Stage 5 settings */
    sup->rate_check_interval_sec = config->rate_check_interval_sec > 0 ? config->rate_check_interval_sec : 10;
    sup->rate_grace_period_sec = config->rate_grace_period_sec > 0 ? config->rate_grace_period_sec : 30;
    sup->tcp_connect_timeout_ms = config->tcp_connect_timeout_ms > 0 ? config->tcp_connect_timeout_ms : 5000;

    /* Initialize mutex */
    if (pthread_mutex_init(&sup->trees_lock, NULL) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to init mutex");
        free(sup);
        return NULL;
    }

    /* Allocate tree array */
    sup->trees = calloc(sup->max_trees, sizeof(thread_tree_t *));
    if (!sup->trees) {
        log_msg(LOG_ERROR, "[supervisor] Failed to allocate trees array");
        pthread_mutex_destroy(&sup->trees_lock);
        free(sup);
        return NULL;
    }

    log_msg(LOG_INFO, "[supervisor] Created with max_trees=%d, min_rate=%.2f, routing_threshold=%d",
            sup->max_trees, sup->min_metadata_rate, sup->routing_threshold);

    return sup;
}

static thread_tree_t *spawn_tree(supervisor_t *sup) {
    tree_config_t config = {
        /* Stage 2 settings (Bootstrap) */
        .num_bootstrap_workers = 10,  /* Default: 10 find_node workers per tree */
        .bootstrap_timeout_sec = sup->bootstrap_timeout_sec,
        .routing_threshold = sup->routing_threshold,
        /* Worker counts */
        .num_bep51_workers = sup->num_bep51_workers,
        .num_get_peers_workers = sup->num_get_peers_workers,
        .num_metadata_workers = sup->num_metadata_workers,
        /* Stage 5 settings */
        .min_metadata_rate = sup->min_metadata_rate,
        .rate_check_interval_sec = sup->rate_check_interval_sec,
        .rate_grace_period_sec = sup->rate_grace_period_sec,
        .tcp_connect_timeout_ms = sup->tcp_connect_timeout_ms,
        /* Shared resources */
        .batch_writer = sup->batch_writer,
        .bloom_filter = sup->bloom_filter,
        .supervisor_ctx = sup,
        .on_shutdown = supervisor_on_tree_shutdown
    };

    uint32_t tree_id = sup->next_tree_id++;
    thread_tree_t *tree = thread_tree_create(tree_id, &config);
    if (!tree) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create tree %u", tree_id);
        return NULL;
    }

    return tree;
}

void supervisor_start(supervisor_t *sup) {
    if (!sup) {
        return;
    }

    log_msg(LOG_INFO, "[supervisor] Starting with %d trees", sup->max_trees);

    pthread_mutex_lock(&sup->trees_lock);

    /* Spawn all trees */
    for (int i = 0; i < sup->max_trees; i++) {
        sup->trees[i] = spawn_tree(sup);
        if (sup->trees[i]) {
            thread_tree_start(sup->trees[i]);
            sup->active_trees++;
        }
    }

    pthread_mutex_unlock(&sup->trees_lock);

    /* Start monitor thread */
    sup->monitor_running = 1;
    if (pthread_create(&sup->monitor_thread, NULL, monitor_thread_func, sup) != 0) {
        log_msg(LOG_ERROR, "[supervisor] Failed to create monitor thread");
        sup->monitor_running = 0;
    }

    log_msg(LOG_INFO, "[supervisor] Started %d trees", sup->active_trees);
}

void supervisor_stop(supervisor_t *sup) {
    if (!sup) {
        return;
    }

    log_msg(LOG_INFO, "[supervisor] Stopping");

    /* Stop monitor thread */
    log_msg(LOG_DEBUG, "[supervisor] Stopping monitor thread...");
    sup->monitor_running = 0;
    if (sup->monitor_thread) {
        pthread_join(sup->monitor_thread, NULL);
        sup->monitor_thread = 0;
    }
    log_msg(LOG_DEBUG, "[supervisor] Monitor thread stopped");

    /* Request shutdown on all trees */
    log_msg(LOG_DEBUG, "[supervisor] Requesting shutdown on all trees...");
    pthread_mutex_lock(&sup->trees_lock);
    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            thread_tree_request_shutdown(sup->trees[i]);
        }
    }
    pthread_mutex_unlock(&sup->trees_lock);
    log_msg(LOG_DEBUG, "[supervisor] Shutdown requested on all trees");

    /* Wait for all trees to finish and destroy them */
    pthread_mutex_lock(&sup->trees_lock);
    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            log_msg(LOG_DEBUG, "[supervisor] Destroying tree %u...", sup->trees[i]->tree_id);
            thread_tree_destroy(sup->trees[i]);
            sup->trees[i] = NULL;
            log_msg(LOG_DEBUG, "[supervisor] Tree destroyed");
        }
    }
    sup->active_trees = 0;
    pthread_mutex_unlock(&sup->trees_lock);

    log_msg(LOG_INFO, "[supervisor] Stopped");
}

void supervisor_destroy(supervisor_t *sup) {
    if (!sup) {
        return;
    }

    /* Ensure stopped */
    supervisor_stop(sup);

    pthread_mutex_destroy(&sup->trees_lock);
    free(sup->trees);
    free(sup);

    log_msg(LOG_INFO, "[supervisor] Destroyed");
}

void supervisor_on_tree_shutdown(thread_tree_t *tree) {
    if (!tree || !tree->supervisor_ctx) {
        return;
    }

    supervisor_t *sup = (supervisor_t *)tree->supervisor_ctx;

    log_msg(LOG_INFO, "[supervisor] Tree %u signaled shutdown", tree->tree_id);

    pthread_mutex_lock(&sup->trees_lock);

    /* Find and remove the tree */
    int slot = -1;
    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i] == tree) {
            slot = i;
            break;
        }
    }

    if (slot >= 0) {
        /* Destroy old tree */
        thread_tree_destroy(sup->trees[slot]);
        sup->trees[slot] = NULL;
        sup->active_trees--;

        /* Spawn replacement if monitor is still running */
        if (sup->monitor_running) {
            log_msg(LOG_INFO, "[supervisor] Spawning replacement tree for slot %d", slot);
            sup->trees[slot] = spawn_tree(sup);
            if (sup->trees[slot]) {
                thread_tree_start(sup->trees[slot]);
                sup->active_trees++;
            }
        }
    }

    pthread_mutex_unlock(&sup->trees_lock);
}

static void *monitor_thread_func(void *arg) {
    supervisor_t *sup = (supervisor_t *)arg;

    log_msg(LOG_INFO, "[supervisor] Monitor thread started");

    while (sup->monitor_running) {
        /* Sleep in small chunks (1 second) to be responsive to shutdown */
        for (int i = 0; i < 10 && sup->monitor_running; i++) {
            struct timespec ts = {1, 0};
            nanosleep(&ts, NULL);
        }

        if (!sup->monitor_running) {
            break;
        }

        /* Check tree performance */
        pthread_mutex_lock(&sup->trees_lock);

        for (int i = 0; i < sup->max_trees; i++) {
            if (!sup->trees[i]) {
                continue;
            }

            thread_tree_t *tree = sup->trees[i];

            /* TODO: Implement rate calculation in Stage 2 */
            /* For now, just log status */
            log_msg(LOG_DEBUG, "[supervisor] Tree %u phase=%s metadata=%lu",
                    tree->tree_id,
                    thread_tree_phase_name(tree->current_phase),
                    (unsigned long)atomic_load(&tree->metadata_count));
        }

        pthread_mutex_unlock(&sup->trees_lock);
    }

    log_msg(LOG_INFO, "[supervisor] Monitor thread exiting");
    return NULL;
}

void supervisor_stats(supervisor_t *sup, int *out_active_trees, uint64_t *out_total_metadata) {
    if (!sup) {
        if (out_active_trees) *out_active_trees = 0;
        if (out_total_metadata) *out_total_metadata = 0;
        return;
    }

    uint64_t total_metadata = 0;

    pthread_mutex_lock(&sup->trees_lock);

    if (out_active_trees) {
        *out_active_trees = sup->active_trees;
    }

    for (int i = 0; i < sup->max_trees; i++) {
        if (sup->trees[i]) {
            total_metadata += atomic_load(&sup->trees[i]->metadata_count);
        }
    }

    pthread_mutex_unlock(&sup->trees_lock);

    if (out_total_metadata) {
        *out_total_metadata = total_metadata;
    }
}
