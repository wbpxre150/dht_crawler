#include "worker_pool.h"
#include "dht_crawler.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    void **items;
    size_t capacity;
    size_t size;
    size_t head;
    size_t tail;
} task_queue_t;

struct worker_pool {
    uv_thread_t *threads;
    size_t num_workers;

    task_queue_t queue;
    uv_mutex_t mutex;
    uv_cond_t task_available;
    uv_cond_t queue_not_full;

    worker_fn_t worker_fn;
    void *closure;

    bool running;
    uint64_t tasks_processed;
    size_t active_workers;  /* Number of workers currently processing tasks */
    size_t idle_workers;    /* Number of workers waiting for tasks */
};

/* Worker thread function */
static void worker_thread(void *arg) {
    worker_pool_t *pool = (worker_pool_t*)arg;

    /* Mark this worker as idle initially */
    uv_mutex_lock(&pool->mutex);
    pool->idle_workers++;
    uv_mutex_unlock(&pool->mutex);

    while (1) {
        uv_mutex_lock(&pool->mutex);

        /* Wait for tasks or shutdown signal while idle */
        while (pool->running && pool->queue.size == 0) {
            uv_cond_wait(&pool->task_available, &pool->mutex);
        }

        /* Check for shutdown */
        if (!pool->running && pool->queue.size == 0) {
            pool->idle_workers--;
            uv_mutex_unlock(&pool->mutex);
            break;
        }

        /* Mark as active when dequeuing task */
        pool->idle_workers--;
        pool->active_workers++;

        /* Dequeue task */
        void *task = pool->queue.items[pool->queue.head];
        pool->queue.head = (pool->queue.head + 1) % pool->queue.capacity;
        pool->queue.size--;

        /* Signal that queue has space */
        uv_cond_signal(&pool->queue_not_full);

        uv_mutex_unlock(&pool->mutex);

        /* Execute task WITHOUT holding mutex - this is where blocking work happens */
        if (task && pool->worker_fn) {
            pool->worker_fn(task, pool->closure);
        }

        /* Mark as idle again after completing task */
        uv_mutex_lock(&pool->mutex);
        pool->tasks_processed++;
        pool->active_workers--;
        pool->idle_workers++;
        uv_mutex_unlock(&pool->mutex);
    }
}

worker_pool_t* worker_pool_init(size_t num_workers, size_t queue_capacity,
                                worker_fn_t worker_fn, void *closure) {
    if (num_workers == 0 || queue_capacity == 0 || !worker_fn) {
        log_msg(LOG_ERROR, "Invalid worker pool parameters");
        return NULL;
    }
    
    worker_pool_t *pool = calloc(1, sizeof(worker_pool_t));
    if (!pool) {
        log_msg(LOG_ERROR, "Failed to allocate worker pool");
        return NULL;
    }
    
    pool->num_workers = num_workers;
    pool->worker_fn = worker_fn;
    pool->closure = closure;
    pool->running = true;
    pool->tasks_processed = 0;
    pool->active_workers = 0;
    pool->idle_workers = 0;
    
    /* Initialize queue */
    pool->queue.capacity = queue_capacity;
    pool->queue.size = 0;
    pool->queue.head = 0;
    pool->queue.tail = 0;
    pool->queue.items = calloc(queue_capacity, sizeof(void*));
    if (!pool->queue.items) {
        log_msg(LOG_ERROR, "Failed to allocate task queue");
        free(pool);
        return NULL;
    }
    
    /* Initialize synchronization primitives */
    if (uv_mutex_init(&pool->mutex) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize worker pool mutex");
        free(pool->queue.items);
        free(pool);
        return NULL;
    }
    
    if (uv_cond_init(&pool->task_available) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize task_available condition");
        uv_mutex_destroy(&pool->mutex);
        free(pool->queue.items);
        free(pool);
        return NULL;
    }
    
    if (uv_cond_init(&pool->queue_not_full) != 0) {
        log_msg(LOG_ERROR, "Failed to initialize queue_not_full condition");
        uv_cond_destroy(&pool->task_available);
        uv_mutex_destroy(&pool->mutex);
        free(pool->queue.items);
        free(pool);
        return NULL;
    }
    
    /* Create worker threads */
    pool->threads = calloc(num_workers, sizeof(uv_thread_t));
    if (!pool->threads) {
        log_msg(LOG_ERROR, "Failed to allocate worker threads array");
        uv_cond_destroy(&pool->queue_not_full);
        uv_cond_destroy(&pool->task_available);
        uv_mutex_destroy(&pool->mutex);
        free(pool->queue.items);
        free(pool);
        return NULL;
    }
    
    for (size_t i = 0; i < num_workers; i++) {
        if (uv_thread_create(&pool->threads[i], worker_thread, pool) != 0) {
            log_msg(LOG_ERROR, "Failed to create worker thread %zu", i);
            /* Cleanup already created threads */
            pool->num_workers = i;
            worker_pool_cleanup(pool);
            return NULL;
        }
    }
    
    log_msg(LOG_DEBUG, "Worker pool initialized: %zu workers, queue capacity %zu",
            num_workers, queue_capacity);
    
    return pool;
}

int worker_pool_submit(worker_pool_t *pool, void *task) {
    if (!pool || !task) {
        return -1;
    }

    uv_mutex_lock(&pool->mutex);

    /* Wait for queue space if full */
    while (pool->running && pool->queue.size >= pool->queue.capacity) {
        uv_cond_wait(&pool->queue_not_full, &pool->mutex);
    }

    if (!pool->running) {
        uv_mutex_unlock(&pool->mutex);
        return -1;
    }

    /* Enqueue task */
    pool->queue.items[pool->queue.tail] = task;
    pool->queue.tail = (pool->queue.tail + 1) % pool->queue.capacity;
    pool->queue.size++;

    /* Signal workers */
    uv_cond_signal(&pool->task_available);

    uv_mutex_unlock(&pool->mutex);

    return 0;
}

int worker_pool_try_submit(worker_pool_t *pool, void *task) {
    if (!pool || !task) {
        return -1;
    }

    uv_mutex_lock(&pool->mutex);

    /* Check if queue is full - don't wait */
    if (!pool->running || pool->queue.size >= pool->queue.capacity) {
        uv_mutex_unlock(&pool->mutex);
        return -1;
    }

    /* Enqueue task */
    pool->queue.items[pool->queue.tail] = task;
    pool->queue.tail = (pool->queue.tail + 1) % pool->queue.capacity;
    pool->queue.size++;

    /* Signal workers */
    uv_cond_signal(&pool->task_available);

    uv_mutex_unlock(&pool->mutex);

    return 0;
}

void worker_pool_stats(worker_pool_t *pool, size_t *out_num_workers,
                      size_t *out_queue_size, size_t *out_queue_capacity,
                      uint64_t *out_tasks_processed, size_t *out_active_workers,
                      size_t *out_idle_workers) {
    if (!pool) {
        return;
    }

    uv_mutex_lock(&pool->mutex);

    if (out_num_workers) {
        *out_num_workers = pool->num_workers;
    }
    if (out_queue_size) {
        *out_queue_size = pool->queue.size;
    }
    if (out_queue_capacity) {
        *out_queue_capacity = pool->queue.capacity;
    }
    if (out_tasks_processed) {
        *out_tasks_processed = pool->tasks_processed;
    }
    if (out_active_workers) {
        *out_active_workers = pool->active_workers;
    }
    if (out_idle_workers) {
        *out_idle_workers = pool->idle_workers;
    }

    uv_mutex_unlock(&pool->mutex);
}

void worker_pool_shutdown(worker_pool_t *pool) {
    if (!pool) {
        return;
    }
    
    uv_mutex_lock(&pool->mutex);
    pool->running = false;
    uv_cond_broadcast(&pool->task_available);
    uv_cond_broadcast(&pool->queue_not_full);
    uv_mutex_unlock(&pool->mutex);
    
    log_msg(LOG_DEBUG, "Worker pool shutting down...");
}

void worker_pool_cleanup(worker_pool_t *pool) {
    if (!pool) {
        return;
    }
    
    /* Shutdown if not already done */
    if (pool->running) {
        worker_pool_shutdown(pool);
    }
    
    /* Wait for all threads to finish */
    if (pool->threads) {
        for (size_t i = 0; i < pool->num_workers; i++) {
            uv_thread_join(&pool->threads[i]);
        }
        free(pool->threads);
    }
    
    log_msg(LOG_DEBUG, "Worker pool cleaned up (%lu tasks processed)", 
            pool->tasks_processed);
    
    /* Cleanup synchronization primitives */
    uv_cond_destroy(&pool->queue_not_full);
    uv_cond_destroy(&pool->task_available);
    uv_mutex_destroy(&pool->mutex);
    
    /* Free queue */
    if (pool->queue.items) {
        free(pool->queue.items);
    }
    
    free(pool);
}
