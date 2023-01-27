#define _POSIX_C_SOURCE 200908L // TODO(fmrsn): Move to command line

#include <stddef.h>
#include <stdint.h>

typedef int32_t Int;
typedef int64_t Long;

typedef uint32_t UInt;
typedef uint64_t ULong;

typedef ptrdiff_t Size;
typedef size_t USize;

typedef intptr_t Ptr;
typedef uintptr_t UPtr;

typedef Int Bool;

#define SizeOf(x) (Size)sizeof(x)
#define CountOf(x) (SizeOf(x) / SizeOf((x)[0]))

// TODO(fmrsn): Remove stdlib dependency
#include <assert.h>
#define Assert(x) assert(x)

typedef struct TaskQueue TaskQueue;
typedef void TaskFunc(void *arg);

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void TaskInitQueue(TaskQueue *queue, Int numWorkers);
static void TaskDeinitQueue(TaskQueue *queue);
static void TaskExecute(TaskQueue *queue, TaskFunc *func, void *funcArg);
static void TaskWaitAll(TaskQueue *queue);

typedef struct EventLoop EventLoop;
typedef void EventCallback(void *arg, Ptr ret);

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void EventInitLoop(EventLoop *loop);
static void EventAccept(EventLoop *loop, int sock, EventCallback *callback, void *callbackArg);
static void EventClose(EventLoop *loop, int fd, EventCallback *callback, void *callbackArg);
static void EventRecv(
	EventLoop *loop, int sock, void *buffer, Size bufferSize, EventCallback *callback,
	void *callbackArg);
static void EventSend(
	EventLoop *loop, int sock, const void *buffer, Size bufferSize, EventCallback *callback,
	void *callbackArg);
static void EventTimer(EventLoop *loop, Long ns, EventCallback *callback, void *callbackArg);
static void EventTick(EventLoop *loop);
static void EventLoopForNs(EventLoop *loop, Long ns);
static void EventDeinitLoop(EventLoop *loop);

// TODO(fmrsn): operation: cancel event submission

/* *************************************** */

// TODO(fmrsn): revise usage of assertions throughout the code

#include <sys/types.h>
#include <sys/event.h>
#include <sys/queue.h>
#include <sys/socket.h>

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct taskSubmission {
	TAILQ_ENTRY(taskSubmission) link;
	TaskQueue *queue;
	pthread_t thread;
	TaskFunc *func;
	void *funcArg;
} taskSubmission;

typedef TAILQ_HEAD(taskSubmissionList, taskSubmission) taskSubmissionList;

struct TaskQueue {
	sigset_t sigfillset;
	pthread_attr_t workerAttr;
	pthread_cond_t workToDo;
	pthread_cond_t allDone;
	pthread_cond_t stopped;
	pthread_mutex_t mutex;
	taskSubmissionList pending;
	taskSubmissionList active;
	Int spawned;
	Int idle;
	Bool waiting;
	Bool deiniting;
};

// TODO(fmrsn): (De)allocations using malloc/free here are pretty frequent.
//  It can (and will) cause a bottleneck. Use memory arenas instead.
static taskSubmission *
taskAlloc(void)
{
	taskSubmission *s = malloc(SizeOf(*s));
	Assert(s);
	return s;
}

static void
taskFree(taskSubmission *s)
{
	free(s);
}

static void taskDone(void *arg);
static void taskCleanUp(void *arg);

static void *
taskWork(void *arg)
{
	TaskQueue *queue = arg;
	pthread_t t = pthread_self();

	(void)pthread_mutex_lock(&queue->mutex);
	++queue->spawned;

	pthread_cleanup_push(taskCleanUp, queue);
	for (;;) {
		/* Reset thread signals and cancellation state. */
		(void)pthread_sigmask(SIG_SETMASK, &queue->sigfillset, NULL);
		(void)pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
		(void)pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

		++queue->idle;
		while (!queue->deiniting && TAILQ_EMPTY(&queue->pending)) {
			(void)pthread_cond_wait(&queue->workToDo, &queue->mutex);
		}
		--queue->idle;
		if (queue->deiniting) {
			break;
		}

		taskSubmission *s;
		if ((s = TAILQ_FIRST(&queue->pending))) {
			TAILQ_REMOVE(&queue->pending, s, link);

			s->thread = t;
			s->queue = queue;
			TAILQ_INSERT_TAIL(&queue->active, s, link);

			pthread_mutex_unlock(&queue->mutex);

			pthread_cleanup_push(taskDone, s);
			s->func(s->funcArg);
			pthread_cleanup_pop(1);
		}
	}
	pthread_cleanup_pop(1);

	return NULL;
}

static void
taskDone(void *arg)
{
	taskSubmission *submission = arg;
	TaskQueue *queue = submission->queue;

	(void)pthread_mutex_lock(&queue->mutex);

	TAILQ_REMOVE(&queue->active, submission, link);

	taskFree(submission);

	if (!TAILQ_EMPTY(&queue->pending) || !TAILQ_EMPTY(&queue->active)) {
		return;
	}
	if (queue->waiting) {
		(void)pthread_cond_broadcast(&queue->allDone);
		queue->waiting = 0;
	}
}

static void
taskCleanUp(void *arg)
{
	TaskQueue *queue = arg;
	pthread_t t;
	int ret;

	--queue->spawned;

	if (!queue->deiniting) {
		ret = pthread_create(&t, &queue->workerAttr, taskWork, queue);
		Assert(ret == 0);

	} else if (queue->spawned == 0) {
		(void)pthread_cond_broadcast(&queue->stopped);
	}

	pthread_mutex_unlock(&queue->mutex);
}

static void
taskUnlockMutex(void *mutex)
{
	(void)pthread_mutex_unlock(mutex);
}

/*
 * TODO(fmrsn):
 *
 * - wait for individual tasks
 * - fork() safety
 */

static void
TaskInitQueue(TaskQueue *queue, Int numWorkers)
{
	Assert(numWorkers > 0);

	(void)sigfillset(&queue->sigfillset);

	(void)pthread_attr_init(&queue->workerAttr);
	(void)pthread_attr_setdetachstate(&queue->workerAttr, PTHREAD_CREATE_DETACHED);

	(void)pthread_mutex_init(&queue->mutex, NULL);
	(void)pthread_cond_init(&queue->workToDo, NULL);
	(void)pthread_cond_init(&queue->allDone, NULL);
	(void)pthread_cond_init(&queue->stopped, NULL);

	TAILQ_INIT(&queue->pending);
	TAILQ_INIT(&queue->active);

	queue->idle = 0;
	queue->spawned = 0;

	pthread_t tid;
	int ret;
	for (Int i = 0; i < numWorkers; ++i) {
		ret = pthread_create(&tid, &queue->workerAttr, taskWork, queue);
		Assert(ret == 0);
	}
}

static void
TaskExecute(TaskQueue *queue, TaskFunc *func, void *funcArg)
{
	(void)pthread_mutex_lock(&queue->mutex);

	taskSubmission *s = taskAlloc();
	*s = (taskSubmission){
		.queue = queue,
		.func = func,
		.funcArg = funcArg,
	};

	TAILQ_INSERT_TAIL(&queue->pending, s, link);
	if (queue->idle > 0) {
		(void)pthread_cond_signal(&queue->workToDo);
	}

	(void)pthread_mutex_unlock(&queue->mutex);
}

static void
TaskWaitAll(TaskQueue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(taskUnlockMutex, &queue->mutex);

	while (!TAILQ_EMPTY(&queue->pending) || !TAILQ_EMPTY(&queue->active)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->allDone, &queue->mutex);
	}

	pthread_cleanup_pop(1);
}

static void
TaskDeinitQueue(TaskQueue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(taskUnlockMutex, &queue->mutex);

	queue->deiniting = 1;
	(void)pthread_cond_broadcast(&queue->workToDo);

	taskSubmission *active;
	TAILQ_FOREACH (active, &queue->active, link) {
		(void)pthread_cancel(active->thread);
	}
	while (!TAILQ_EMPTY(&queue->active)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->allDone, &queue->mutex);
	}

	while (queue->spawned > 0) {
		(void)pthread_cond_wait(&queue->stopped, &queue->mutex);
	}

	pthread_cleanup_pop(1);

	pthread_attr_destroy(&queue->workerAttr);
	pthread_cond_destroy(&queue->workToDo);
	pthread_cond_destroy(&queue->allDone);
	pthread_cond_destroy(&queue->stopped);
	pthread_mutex_destroy(&queue->mutex);
}

typedef struct {
	enum {
		eventOpAccept,
		eventOpClose,
		eventOpRecv,
		eventOpSend,
		eventOpTimer,
	} code;

	union {
		struct {
			int sock;
		} accept;

		struct {
			int fd;
		} close;

		struct {
			int sock;
			void *buffer;
			Size bufferSize;
		} recv;

		struct {
			int sock;
			const void *buffer;
			Size bufferSize;
		} send;

		struct {
			Long ns;
		} timer;
	} args;
} eventOp;

typedef struct eventSubmission {
	TAILQ_ENTRY(eventSubmission) link;
	eventOp op;
	Ptr (*onReady)(const eventOp *op);
	void (*callback)(void *arg, Ptr ret);
	void *callbackArg;
	Ptr ret;
} eventSubmission;

typedef TAILQ_HEAD(eventSubmissionQueue, eventSubmission) eventSubmissionQueue;

struct EventLoop {
	int kq;
	int inflight;
	eventSubmissionQueue pending;
	eventSubmissionQueue completed;
};

static eventSubmission *
eventAlloc(void)
{
	eventSubmission *s = malloc(SizeOf(*s));
	Assert(s);
	return s;
}

static void
eventFree(eventSubmission *s)
{
	free(s);
}

static void
eventInit(void)
{
	signal(SIGPIPE, SIG_IGN);
}

static int
eventFlushPending(
	eventSubmissionQueue *pending, int maxEvents, struct kevent eventBuf[static maxEvents])
{
	eventSubmission *s;

	int i;
	for (i = 0; i < maxEvents; ++i) {
		if (!(s = TAILQ_FIRST(pending))) {
			break;
		}
		TAILQ_REMOVE(pending, s, link);

		struct kevent *event = &eventBuf[i];
		*event = (struct kevent){
			.flags = EV_ADD | EV_ENABLE | EV_ONESHOT,
			.udata = s,
		};

		eventOp *op = &s->op;

		switch (op->code) {
		case eventOpAccept:
			event->ident = op->args.accept.sock;
			event->filter = EVFILT_READ;
			break;

		case eventOpRecv:
			event->ident = op->args.recv.sock;
			event->filter = EVFILT_READ;
			break;

		case eventOpSend:
			event->ident = op->args.send.sock;
			event->filter = EVFILT_WRITE;
			break;

		case eventOpTimer:
			event->ident = (UPtr)s;
			event->filter = EVFILT_TIMER;
			event->fflags = NOTE_NSECONDS;
			event->data = op->args.timer.ns;
			break;

		default:
			Assert(0);
		}
	}

	return i;
}

static void
eventFlushAndWait(EventLoop *loop, const struct timespec *timeout)
{
	int kq = loop->kq;
	eventSubmissionQueue pending = loop->pending;
	eventSubmissionQueue completed = loop->completed;

	struct kevent events[256];
	int numChanges = eventFlushPending(&pending, CountOf(events), events);

	eventSubmission *s;

	if (numChanges > 0 || (TAILQ_EMPTY(&completed) && loop->inflight > 0)) {
		int numEvents = kevent(kq, events, numChanges, events, CountOf(events), timeout);
		Assert(numEvents >= 0);

		loop->pending = pending;
		loop->inflight += numChanges - numEvents;

		for (int i = 0; i < numEvents; ++i) {
			struct kevent *event = &events[i];
			s = event->udata;
			s->ret = event->fflags & EV_ERROR ? -event->data : 0;
			TAILQ_INSERT_TAIL(&completed, s, link);
		}
	}

	TAILQ_INIT(&loop->completed);

	while ((s = TAILQ_FIRST(&completed))) {
		TAILQ_REMOVE(&completed, s, link);

		if (s->ret >= 0 && s->onReady) {
			s->ret = s->onReady(&s->op);
		}
		if (s->ret == -EAGAIN || s->ret == -EWOULDBLOCK) {
			TAILQ_INSERT_TAIL(&pending, s, link);
			continue;
		}

		EventCallback *callback = s->callback;
		void *arg = s->callbackArg;
		Ptr ret = s->ret;
		eventFree(s);

		if (callback) {
			callback(arg, ret);
		}
	}
}

static void
EventInitLoop(EventLoop *loop)
{
	static pthread_once_t once = PTHREAD_ONCE_INIT;
	pthread_once(&once, eventInit);

	int kq = kqueue();
	Assert(kq >= 0);

	loop->kq = kq;
	loop->inflight = 0;
	TAILQ_INIT(&loop->pending);
	TAILQ_INIT(&loop->completed);
}

static void
EventTick(EventLoop *loop)
{
	eventFlushAndWait(loop, &(struct timespec){0});
}

static void
eventLoopDone(void *arg, Ptr ret)
{
	(void)ret;

	Bool *done = arg;
	*done = 1;
}

static void
EventLoopForNs(EventLoop *loop, Long ns)
{
	Bool done = 0;

	EventTimer(loop, ns, eventLoopDone, &done);
	while (!done) {
		eventFlushAndWait(loop, NULL);
	}
}

static void
eventSubmit(eventSubmissionQueue *queue, eventSubmission *submission)
{
	eventSubmission *s = eventAlloc();
	*s = *submission;
	TAILQ_INSERT_TAIL(queue, s, link);
}

static Ptr
eventAcceptOnReady(const eventOp *op)
{
	Assert(op->code == eventOpAccept);

	int sock = accept(op->args.accept.sock, NULL, NULL);
	if (sock < 0) {
		return -errno;
	}

	int flags = fcntl(sock, F_GETFL);
	fcntl(sock, F_SETFL, flags | O_NONBLOCK);

	int ret = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &(int){1}, sizeof(int));
	Assert(ret == 0);

	return sock;
}

static void
EventAccept(EventLoop *loop, int sock, EventCallback *callback, void *arg)
{
	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpAccept,
			.op.args.accept.sock = sock,
			.onReady = eventAcceptOnReady,
			.callback = callback,
			.callbackArg = arg,
		});
}

static Ptr
eventCloseOnReady(const eventOp *op)
{
	Assert(op->code == eventOpClose);

	if (close(op->args.close.fd) < 0) {
		return -errno;
	}
	return 0;
}

static void
EventClose(EventLoop *loop, int fd, EventCallback *callback, void *callbackArg)
{
	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpClose,
			.op.args.close.fd = fd,
			.onReady = eventCloseOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static Ptr
eventRecvOnReady(const eventOp *op)
{
	Assert(op->code == eventOpRecv);

	int sock = op->args.recv.sock;
	void *buffer = op->args.recv.buffer;
	Size bufferSize = op->args.recv.bufferSize;

	ssize_t n = recv(sock, buffer, bufferSize, 0);
	return n < 0 ? -errno : n;
}

static void
EventRecv(
	EventLoop *loop, int sock, void *buffer, Size bufferSize, EventCallback *callback,
	void *callbackArg)
{
	Assert(bufferSize >= 0);
	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpRecv,
			.op.args.recv.sock = sock,
			.op.args.recv.buffer = buffer,
			.op.args.recv.bufferSize = bufferSize,
			.onReady = eventRecvOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static Ptr
eventSendOnReady(const eventOp *op)
{
	Assert(op->code == eventOpSend);

	int sock = op->args.recv.sock;
	const void *buffer = op->args.recv.buffer;
	Size bufferSize = op->args.recv.bufferSize;

	ssize_t n = send(sock, buffer, bufferSize, 0);
	return n < 0 ? -errno : n;
}

static void
EventSend(
	EventLoop *loop, int sock, const void *buffer, Size bufferSize, EventCallback *callback,
	void *callbackArg)
{
	Assert(bufferSize >= 0);
	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpSend,
			.op.args.send.sock = sock,
			.op.args.send.buffer = buffer,
			.op.args.send.bufferSize = bufferSize,
			.onReady = eventSendOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static void
EventTimer(EventLoop *loop, Long ns, EventCallback *callback, void *callbackArg)
{
	Assert(ns >= 0);
	eventSubmit(
		&loop->pending,
		&(eventSubmission){
			.op.code = eventOpTimer,
			.op.args.timer.ns = ns,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static void
EventDeinitLoop(EventLoop *loop)
{
	(void)close(loop->kq);
}

/***********************************************************************/

#include <stdio.h>

static TaskQueue taskQueue;
static EventLoop eventLoop;

void
Hello(void *arg)
{
	printf("Hello from worker %d\n", (int)(Ptr)arg);
}

int
main(void)
{
	(void)EventTick;
	(void)EventLoopForNs;
	(void)EventAccept;
	(void)EventClose;
	(void)EventRecv;
	(void)EventSend;

	TaskInitQueue(&taskQueue, 4);
	EventInitLoop(&eventLoop);

	for (int i = 0; i < 5; ++i) {
		TaskExecute(&taskQueue, Hello, (void *)(Ptr)i);
	}

	TaskWaitAll(&taskQueue);

	EventDeinitLoop(&eventLoop);
	TaskDeinitQueue(&taskQueue);
}
