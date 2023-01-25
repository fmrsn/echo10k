typedef struct Task_Queue Task_Queue;
typedef struct Task_Context Task_Context;

static void Task_Init(Task_Queue *queue, int workerCount);
static void
Task_Enqueue(Task_Queue *queue, Task_Context *ctx, void (*proc)(void *, Task_Context *), void *arg);
static void Task_WaitAll(Task_Queue *queue);
static void Task_Deinit(Task_Queue *queue);

#if 0
typedef struct Event_Loop Event_Loop;
typedef struct Event_Completion Event_Completion;

static void Event_InitLoop(Event_Loop *loop);
static void Event_DeinitLoop(Event_Loop *loop);
#endif

/* *************************************** */

#define _POSIX_C_SOURCE 200908L

#include <sys/queue.h>

#include <assert.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <unistd.h>

typedef struct TaskWorker {
	TAILQ_ENTRY(TaskWorker) link;
	Task_Queue *const queue;
	const pthread_t thread;
} TaskWorker;

typedef TAILQ_HEAD(TaskWorkerList, TaskWorker) TaskWorkerList;

struct Task_Context {
	STAILQ_ENTRY(Task_Context) link;
	void (*func)(void *, Task_Context *);
	void *arg;
};

typedef STAILQ_HEAD(TaskContextQueue, Task_Context) TaskContextQueue;

struct Task_Queue {
	sigset_t allSignals;
	pthread_attr_t workerAttr;
	pthread_cond_t workToDo;
	pthread_cond_t workDone;
	pthread_cond_t queueStopped;
	pthread_mutex_t mutex;
	TaskContextQueue pendingTasks;
	TaskWorkerList activeWorkers;
	int spawnedCount;
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
	TaskWorker worker = {.queue = queue, .thread = pthread_self()};

	(void)pthread_mutex_lock(&queue->mutex);
	++queue->spawnedCount;

	pthread_cleanup_push(TaskCleanUp, queue);
	for (;;) {
		/* Reset thread signals and cancellation state. */
		(void)pthread_sigmask(SIG_SETMASK, &queue->allSignals, NULL);
		(void)pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
		(void)pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

		++queue->idleCount;
		while (!queue->deiniting && STAILQ_EMPTY(&queue->pendingTasks)) {
			(void)pthread_cond_wait(&queue->workToDo, &queue->mutex);
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
		(void)pthread_cond_broadcast(&queue->workDone);
		queue->waiting = 0;
	}
}

static void
TaskCleanUp(void *arg)
{
	Task_Queue *queue = arg;
	pthread_t t;
	int r;

	--queue->spawnedCount;

	if (!queue->deiniting) {
		r = pthread_create(&t, &queue->workerAttr, TaskWork, queue);
		assert(r == 0);

	} else if (queue->spawnedCount == 0) {
		(void)pthread_cond_broadcast(&queue->queueStopped);
	}

	pthread_mutex_unlock(&queue->mutex);
}

/*
 * TODO(fmrsn):
 *
 * - wait for individual tasks
 * - fork() safety
 */

static void
Task_Init(Task_Queue *queue, int numWorkers)
{
	assert(numWorkers > 0);

	(void)sigfillset(&queue->allSignals);

	(void)pthread_attr_init(&queue->workerAttr);
	(void)pthread_attr_setdetachstate(&queue->workerAttr, PTHREAD_CREATE_DETACHED);

	(void)pthread_mutex_init(&queue->mutex, NULL);
	(void)pthread_cond_init(&queue->workToDo, NULL);
	(void)pthread_cond_init(&queue->workDone, NULL);
	(void)pthread_cond_init(&queue->queueStopped, NULL);

	STAILQ_INIT(&queue->pendingTasks);
	TAILQ_INIT(&queue->activeWorkers);

	queue->idleCount = 0;
	queue->spawnedCount = 0;

	pthread_t thread;
	int ret;
	for (int i = 0; i < numWorkers; ++i) {
		ret = pthread_create(&thread, &queue->workerAttr, TaskWork, queue);
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
		pthread_cond_signal(&queue->workToDo);
	}

	(void)pthread_mutex_unlock(&queue->mutex);
}

static void
TaskUnlockMutex(void *mutex)
{
	(void)pthread_mutex_unlock(mutex);
}

static void
Task_WaitAll(Task_Queue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(TaskUnlockMutex, &queue->mutex);

	while (!STAILQ_EMPTY(&queue->pendingTasks) || !TAILQ_EMPTY(&queue->activeWorkers)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->workDone, &queue->mutex);
	}

	pthread_cleanup_pop(1);
}

static void
Task_Deinit(Task_Queue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(TaskUnlockMutex, &queue->mutex);

	queue->deiniting = 1;
	(void)pthread_cond_broadcast(&queue->workToDo);

	TaskWorker *active;
	TAILQ_FOREACH (active, &queue->activeWorkers, link) {
		(void)pthread_cancel(active->thread);
	}
	while (!TAILQ_EMPTY(&queue->activeWorkers)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->workDone, &queue->mutex);
	}

	while (queue->spawnedCount > 0) {
		(void)pthread_cond_wait(&queue->queueStopped, &queue->mutex);
	}

	pthread_cleanup_pop(1);

	while (!STAILQ_EMPTY(&queue->pendingTasks)) {
		STAILQ_REMOVE_HEAD(&queue->pendingTasks, link);
	}

	pthread_attr_destroy(&queue->workerAttr);
	pthread_cond_destroy(&queue->workToDo);
	pthread_cond_destroy(&queue->workDone);
	pthread_cond_destroy(&queue->queueStopped);
	pthread_mutex_destroy(&queue->mutex);
}

/***********************************************************************/

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

void
Hello(void *arg, Task_Context *ctx)
{
	(void)ctx;
	printf("Hello from worker %d\n", (int)(intptr_t)arg);
}

int
main(void)
{
	Task_Queue q;

	Task_Context contexts[5];

	Task_Init(&q, 4);
	for (int i = 0; i < (int)(sizeof contexts / sizeof contexts[0]); ++i) {
		Task_Enqueue(&q, &contexts[i], Hello, (void *)(intptr_t)i);
	}
	Task_WaitAll(&q);
	Task_Deinit(&q);
}
