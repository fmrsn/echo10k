typedef struct Task_Context_ Task_Context;
typedef struct Task_Queue_ Task_Queue;

static void Task_InitQueue(Task_Queue *queue, int workerCount);
static void
Task_Enqueue(Task_Queue *queue, Task_Context *ctx, void (*proc)(void *, Task_Context *), void *arg);
static void Task_WaitAll(Task_Queue *queue);
static void Task_DeinitQueue(Task_Queue *queue);

/* *************************************** */

#define _POSIX_C_SOURCE 200908L

#include <sys/queue.h>

#include <assert.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>

typedef struct TaskWorker_ {
	TAILQ_ENTRY(TaskWorker_) link;
	Task_Queue *const queue;
	const pthread_t t;
} TaskWorker;

typedef TAILQ_HEAD(TaskWorkerList_, TaskWorker_) TaskWorkerList;

struct Task_Context_ {
	STAILQ_ENTRY(Task_Context_) link;
	void (*func)(void *, Task_Context *);
	void *arg;
};

typedef STAILQ_HEAD(TaskContextQueue_, Task_Context_) TaskContextQueue;

struct Task_Queue_ {
	sigset_t signalSet;
	pthread_attr_t workerAttr;
	pthread_cond_t wakeUp;
	pthread_cond_t allDone;
	pthread_cond_t stopped;
	pthread_mutex_t mutex;
	TaskContextQueue pendingTasks;
	TaskWorkerList activeWorkers;
	int workerCount;
	int idleCount;
	bool waiting : 1;
	bool deiniting : 1;
};

static void TaskDone(void *arg);
static void TaskCleanUp(void *arg);

static void *
TaskWork(void *arg)
{
	Task_Queue *queue = arg;
	TaskWorker worker = {.queue = queue, .t = pthread_self()};

	(void)pthread_mutex_lock(&queue->mutex);
	++queue->workerCount;

	pthread_cleanup_push(TaskCleanUp, queue);
	for (;;) {
		/* Reset thread signals and cancellation state. */
		(void)pthread_sigmask(SIG_SETMASK, &queue->signalSet, NULL);
		(void)pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
		(void)pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

		++queue->idleCount;
		while (!queue->deiniting && STAILQ_EMPTY(&queue->pendingTasks)) {
			(void)pthread_cond_wait(&queue->wakeUp, &queue->mutex);
		}
		--queue->idleCount;
		if (queue->deiniting) {
			break;
		}

		Task_Context *ctx;
		if ((ctx = STAILQ_FIRST(&queue->pendingTasks))) {
			STAILQ_REMOVE_HEAD(&queue->pendingTasks, link);
			TAILQ_INSERT_TAIL(&queue->activeWorkers, &worker, link);
			pthread_mutex_unlock(&queue->mutex);

			pthread_cleanup_push(TaskDone, &worker);
			ctx->func(ctx->arg, ctx);
			pthread_cleanup_pop(1);
		}
	}
	pthread_cleanup_pop(1);

	return NULL;
}

static void
TaskDone(void *arg)
{
	TaskWorker *worker = arg;
	Task_Queue *queue = worker->queue;

	pthread_mutex_lock(&queue->mutex);

	TAILQ_REMOVE(&queue->activeWorkers, worker, link);

	if (!STAILQ_EMPTY(&queue->pendingTasks) || !TAILQ_EMPTY(&queue->activeWorkers)) {
		return;
	}
	if (queue->waiting) {
		(void)pthread_cond_broadcast(&queue->allDone);
		queue->waiting = 0;
	}
}

static void
TaskCleanUp(void *arg)
{
	Task_Queue *queue = arg;
	pthread_t t;
	int r;

	--queue->workerCount;

	if (!queue->deiniting) {
		r = pthread_create(&t, &queue->workerAttr, TaskWork, queue);
		assert(r == 0);

	} else if (queue->workerCount == 0) {
		(void)pthread_cond_broadcast(&queue->stopped);
	}

	pthread_mutex_unlock(&queue->mutex);
}

/*
 * TODO(fmrsn):
 *
 * - param: min/max workers available
 * - param: how much time a thread should stay idle before exiting
 * - wait for individual tasks
 * - fork() safety
 */

static void
Task_InitQueue(Task_Queue *queue, int workerCount)
{
	assert(workerCount > 0);

	(void)sigfillset(&queue->signalSet);

	(void)pthread_attr_init(&queue->workerAttr);
	(void)pthread_attr_setdetachstate(&queue->workerAttr, PTHREAD_CREATE_DETACHED);

	(void)pthread_mutex_init(&queue->mutex, NULL);
	(void)pthread_cond_init(&queue->wakeUp, NULL);
	(void)pthread_cond_init(&queue->allDone, NULL);
	(void)pthread_cond_init(&queue->stopped, NULL);

	STAILQ_INIT(&queue->pendingTasks);
	TAILQ_INIT(&queue->activeWorkers);

	queue->idleCount = 0;
	queue->workerCount = 0;

	for (int i = 0; i < workerCount; ++i) {
		pthread_t t;
		int ret = pthread_create(&t, &queue->workerAttr, TaskWork, queue);
		assert(ret == 0);
	}
}

static void
Task_Enqueue(Task_Queue *queue, Task_Context *ctx, void (*func)(void *, Task_Context *), void *arg)
{
	*ctx = (Task_Context){.func = func, .arg = arg};

	(void)pthread_mutex_lock(&queue->mutex);

	STAILQ_INSERT_TAIL(&queue->pendingTasks, ctx, link);
	if (queue->idleCount > 0) {
		pthread_cond_signal(&queue->wakeUp);
	}

	(void)pthread_mutex_unlock(&queue->mutex);
}

static void
PthreadMutexUnlock(void *mutex)
{
	(void)pthread_mutex_unlock(mutex);
}

static void
Task_WaitAll(Task_Queue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(PthreadMutexUnlock, &queue->mutex);

	while (!STAILQ_EMPTY(&queue->pendingTasks) || !TAILQ_EMPTY(&queue->activeWorkers)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->allDone, &queue->mutex);
	}

	pthread_cleanup_pop(1);
}

static void
Task_DeinitQueue(Task_Queue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(PthreadMutexUnlock, &queue->mutex);

	queue->deiniting = 1;
	(void)pthread_cond_broadcast(&queue->wakeUp);

	TaskWorker *active;
	TAILQ_FOREACH (active, &queue->activeWorkers, link) {
		(void)pthread_cancel(active->t);
	}
	while (!TAILQ_EMPTY(&queue->activeWorkers)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->allDone, &queue->mutex);
	}

	while (queue->workerCount > 0) {
		(void)pthread_cond_wait(&queue->stopped, &queue->mutex);
	}

	pthread_cleanup_pop(1);

	while (!STAILQ_EMPTY(&queue->pendingTasks)) {
		STAILQ_REMOVE_HEAD(&queue->pendingTasks, link);
	}

	pthread_attr_destroy(&queue->workerAttr);
	pthread_cond_destroy(&queue->wakeUp);
	pthread_cond_destroy(&queue->allDone);
	pthread_cond_destroy(&queue->stopped);
	pthread_mutex_destroy(&queue->mutex);
}

/***********************************************************************/

#include <stdio.h>

void
Hello(void *arg, Task_Context *ctx)
{
	(void)arg;
	(void)ctx;

	puts("Hello, world!");
}

int
main(void)
{
	Task_Queue q;
	Task_Context ctx;

	Task_InitQueue(&q, 1);
	Task_Enqueue(&q, &ctx, Hello, NULL);
	Task_WaitAll(&q);
	Task_DeinitQueue(&q);
}
