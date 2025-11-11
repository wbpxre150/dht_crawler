/*
 * wbpxre-dht: Worker Queue Implementation
 * Thread-safe producer-consumer queues for pipeline architecture
 */

#include "wbpxre_dht.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

/* ============================================================================
 * Work Queue Operations
 * ============================================================================ */

wbpxre_work_queue_t *wbpxre_queue_create(int capacity) {
    wbpxre_work_queue_t *queue = calloc(1, sizeof(wbpxre_work_queue_t));
    if (!queue) return NULL;

    queue->capacity = capacity;
    queue->size = 0;
    queue->head = 0;
    queue->tail = 0;
    queue->shutdown = false;

    queue->items = calloc(capacity, sizeof(void *));
    if (!queue->items) {
        free(queue);
        return NULL;
    }

    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_empty, NULL);
    pthread_cond_init(&queue->not_full, NULL);

    return queue;
}

void wbpxre_queue_destroy(wbpxre_work_queue_t *queue) {
    if (!queue) return;

    pthread_mutex_lock(&queue->mutex);
    queue->shutdown = true;
    pthread_cond_broadcast(&queue->not_empty);
    pthread_cond_broadcast(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);

    /* Free any remaining items */
    for (int i = 0; i < queue->size; i++) {
        int idx = (queue->head + i) % queue->capacity;
        if (queue->items[idx]) {
            free(queue->items[idx]);
        }
    }

    free(queue->items);
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_empty);
    pthread_cond_destroy(&queue->not_full);
    free(queue);
}

int wbpxre_queue_push(wbpxre_work_queue_t *queue, void *item) {
    if (!queue || !item) return -1;

    pthread_mutex_lock(&queue->mutex);

    /* Wait while queue is full */
    while (queue->size >= queue->capacity && !queue->shutdown) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }

    if (queue->shutdown) {
        pthread_mutex_unlock(&queue->mutex);
        return -1;
    }

    /* Add item */
    queue->items[queue->tail] = item;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->size++;

    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);

    return 0;
}

bool wbpxre_queue_try_push(wbpxre_work_queue_t *queue, void *item) {
    if (!queue || !item) return false;

    pthread_mutex_lock(&queue->mutex);

    /* Check if queue is full */
    if (queue->size >= queue->capacity || queue->shutdown) {
        pthread_mutex_unlock(&queue->mutex);
        return false;
    }

    /* Add item */
    queue->items[queue->tail] = item;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->size++;

    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);

    return true;
}

/* Push item to front of queue (for priority items) - non-blocking */
bool wbpxre_queue_try_push_front(wbpxre_work_queue_t *queue, void *item) {
    if (!queue || !item) return false;

    pthread_mutex_lock(&queue->mutex);

    /* Check if queue is full */
    if (queue->size >= queue->capacity || queue->shutdown) {
        pthread_mutex_unlock(&queue->mutex);
        return false;
    }

    /* Add item to front by decrementing head */
    queue->head = (queue->head - 1 + queue->capacity) % queue->capacity;
    queue->items[queue->head] = item;
    queue->size++;

    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);

    return true;
}

void *wbpxre_queue_pop(wbpxre_work_queue_t *queue) {
    if (!queue) return NULL;

    pthread_mutex_lock(&queue->mutex);

    /* Wait while queue is empty */
    while (queue->size == 0 && !queue->shutdown) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }

    if (queue->shutdown && queue->size == 0) {
        pthread_mutex_unlock(&queue->mutex);
        return NULL;
    }

    /* Remove item */
    void *item = queue->items[queue->head];
    queue->items[queue->head] = NULL;
    queue->head = (queue->head + 1) % queue->capacity;
    queue->size--;

    pthread_cond_signal(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);

    return item;
}

void wbpxre_queue_shutdown(wbpxre_work_queue_t *queue) {
    if (!queue) return;

    pthread_mutex_lock(&queue->mutex);
    queue->shutdown = true;
    pthread_cond_broadcast(&queue->not_empty);
    pthread_cond_broadcast(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
}

/* Clear all items from queue without shutdown */
void wbpxre_queue_clear(wbpxre_work_queue_t *queue) {
    if (!queue) return;

    pthread_mutex_lock(&queue->mutex);

    /* Free all items in the queue */
    while (queue->size > 0) {
        void *item = queue->items[queue->head];
        free(item);  /* Free the node structure */
        queue->head = (queue->head + 1) % queue->capacity;
        queue->size--;
    }

    /* Reset queue state */
    queue->head = 0;
    queue->tail = 0;

    /* Signal waiting threads that space is available */
    pthread_cond_broadcast(&queue->not_full);

    pthread_mutex_unlock(&queue->mutex);
}
