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
// TODO(fmrsn): static void TaskCancel(???);

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

typedef TAILQ_HEAD(, taskSubmission) taskSubmissionQueue;

struct TaskQueue {
	sigset_t sigfillset;
	pthread_attr_t workerAttr;
	pthread_cond_t workToDo;
	pthread_cond_t allDone;
	pthread_cond_t stopped;
	pthread_mutex_t mutex;
	taskSubmissionQueue pending;
	taskSubmissionQueue active;
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

		taskSubmission *submission = TAILQ_FIRST(&queue->pending);
		if (!submission) {
			continue;
		}
		TAILQ_REMOVE(&queue->pending, submission, link);

		submission->thread = t;
		submission->queue = queue;
		TAILQ_INSERT_TAIL(&queue->active, submission, link);

		pthread_mutex_unlock(&queue->mutex);

		pthread_cleanup_push(taskDone, submission);
		submission->func(submission->funcArg);
		pthread_cleanup_pop(1);
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

	for (Int i = 0; i < numWorkers; ++i) {
		pthread_t thread;
		int ret = pthread_create(&thread, &queue->workerAttr, taskWork, queue);
		Assert(ret == 0);
	}
}

static void
TaskExecute(TaskQueue *queue, TaskFunc *func, void *funcArg)
{
	taskSubmission *submission = taskAlloc();
	*submission = (taskSubmission){
		.queue = queue,
		.func = func,
		.funcArg = funcArg,
	};

	(void)pthread_mutex_lock(&queue->mutex);

	TAILQ_INSERT_TAIL(&queue->pending, submission, link);
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

/*****************************/

typedef struct eventSubmission {
	STAILQ_ENTRY(eventSubmission) link;

	eventOp op;
	Ptr (*onReady)(const eventOp *op);
	void (*callback)(void *arg, Ptr ret);
	void *callbackArg;
	Ptr ret;

} eventSubmission;

typedef STAILQ_HEAD(, eventSubmission) eventSubmissionQueue;

struct EventLoop {
	int kq;
	int inflight;
	eventSubmissionQueue pending;
	eventSubmissionQueue completed;
};

static int
eventFlushPending(
	eventSubmissionQueue *pending, int maxEvents, struct kevent eventBuf[static maxEvents])
{
	int i;
	for (i = 0; i < maxEvents && !STAILQ_EMPTY(pending); ++i) {
		eventSubmission *submission = STAILQ_FIRST(pending);
		STAILQ_REMOVE_HEAD(pending, link);

		struct kevent *event = &eventBuf[i];
		*event = (struct kevent){
			.flags = EV_ADD | EV_ENABLE | EV_ONESHOT,
			.udata = submission,
		};

		eventOp *op = &submission->op;

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
			event->ident = (UPtr)submission;
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
	struct kevent events[256];
	int numChanges = eventFlushPending(&loop->pending, CountOf(events), events);

	if (numChanges > 0 || (STAILQ_EMPTY(&loop->completed) && loop->inflight > 0)) {
		int numEvents =
			kevent(loop->kq, events, numChanges, events, CountOf(events), timeout);
		Assert(numEvents >= 0);

		loop->inflight += numChanges - numEvents;

		for (int i = 0; i < numEvents; ++i) {
			struct kevent *event = &events[i];
			eventSubmission *submission = event->udata;
			submission->ret = event->fflags & EV_ERROR ? -event->data : 0;
			STAILQ_INSERT_TAIL(&loop->completed, submission, link);
		}
	}

	eventSubmission *lastCompleted = STAILQ_LAST(&loop->completed, eventSubmission, link);
	eventSubmission *completed = NULL;
	while (completed != lastCompleted && (completed = STAILQ_FIRST(&loop->completed))) {
		STAILQ_REMOVE_HEAD(&loop->completed, link);

		if (completed->ret >= 0 && completed->onReady) {
			completed->ret = completed->onReady(&completed->op);
		}

		Bool tryAgain = completed->ret == -EAGAIN || completed->ret == -EWOULDBLOCK;
		if (tryAgain) {
			STAILQ_INSERT_TAIL(&loop->pending, completed, link);
		} else if (completed->callback) {
			completed->callback(completed->callbackArg, completed->ret);
		}

		if (!tryAgain) {
			free(completed);
		}
	}
}

static void
EventInitLoop(EventLoop *loop)
{
	Assert(loop);

	int kq = kqueue();
	Assert(kq >= 0);

	loop->kq = kq;
	loop->inflight = 0;
	STAILQ_INIT(&loop->pending);
	STAILQ_INIT(&loop->completed);
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
eventSubmit(eventSubmissionQueue *queue, eventSubmission *desc)
{
	eventSubmission *submission = malloc(SizeOf(*submission));
	Assert(submission);
	*submission = *desc;

	STAILQ_INSERT_TAIL(queue, submission, link);
}

static Ptr eventAcceptOnReady(const eventOp *);
static Ptr eventCloseOnReady(const eventOp *);
static Ptr eventRecvOnReady(const eventOp *);
static Ptr eventSendOnReady(const eventOp *);

static void
EventAccept(EventLoop *loop, int sock, EventCallback *callback, void *callbackArg)
{
	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpAccept,
			.op.args.accept = {sock},
			.onReady = eventAcceptOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static Ptr
eventAcceptOnReady(const eventOp *op)
{
	int listenSock = op->args.accept.sock;

	int sock = accept(listenSock, NULL, NULL);
	if (sock < 0) {
		return -errno;
	}

	int flags = fcntl(sock, F_GETFL);
	fcntl(sock, F_SETFL, flags | O_NONBLOCK);

	// TODO: move to echoSetup
	int r = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &(int){1}, sizeof(int));
	Assert(r == 0);

	return sock;
}

static void
EventClose(EventLoop *loop, int fd, EventCallback *callback, void *callbackArg)
{
	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpClose,
			.op.args.close = {fd},
			.onReady = eventCloseOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static Ptr
eventCloseOnReady(const eventOp *op)
{
	int fd = op->args.close.fd;

	int r = close(fd);
	return r < 0 ? -errno : 0;
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
			.op.args.recv = {sock, buffer, bufferSize},
			.onReady = eventRecvOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static Ptr
eventRecvOnReady(const eventOp *op)
{
	int sock = op->args.recv.sock;
	void *buffer = op->args.recv.buffer;
	Size bufferSize = op->args.recv.bufferSize;

	ssize_t n = recv(sock, buffer, bufferSize, 0);
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
			.op.args.send = {sock, buffer, bufferSize},
			.onReady = eventSendOnReady,
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static Ptr
eventSendOnReady(const eventOp *op)
{
	int sock = op->args.send.sock;
	const void *buffer = op->args.send.buffer;
	Size bufferSize = op->args.send.bufferSize;

	ssize_t n = send(sock, buffer, bufferSize, 0);
	return n < 0 ? -errno : n;
}

static void
EventTimer(EventLoop *loop, Long ns, EventCallback *callback, void *callbackArg)
{
	Assert(ns >= 0);

	eventSubmit(
		&loop->completed,
		&(eventSubmission){
			.op.code = eventOpTimer,
			.op.args.timer = {ns},
			.callback = callback,
			.callbackArg = callbackArg,
		});
}

static void
EventDeinitLoop(EventLoop *loop)
{
	// TODO: we're leaking memory alloc'ed for submissions. Fix it.
	(void)close(loop->kq);
}

/***********************************************************************/

#include <netdb.h>

#include <locale.h>
#include <stdio.h>

static TaskQueue taskQueue;
static EventLoop eventLoop;

typedef struct echoContext {
	EventLoop *loop;
	int sock;

	char *content;
	Size contentSize;
	char contentBuf[8192];
} echoContext;

static echoContext *
echoAlloc(void)
{
	echoContext *ctx = malloc(SizeOf(*ctx));
	Assert(ctx);
	return ctx;
}

static void
echoFree(echoContext *ctx)
{
	free(ctx);
}

static void echoOnAccept(void *arg, Ptr ret);
static void echoOnRecv(void *arg, Ptr ret);
static void echoOnSend(void *arg, Ptr ret);
static void echoOnClose(void *arg, Ptr ret);

static void
echoOnAccept(void *arg, Ptr ret)
{
	int r;

	if (ret < 0) {
		// TODO(fmrsn): Should we close the listening connection?
		return;
	}

	echoContext *listenCtx = arg;
	int sock = ret;

	int flags = fcntl(sock, F_GETFL);
	(void)fcntl(sock, F_SETFL, flags | O_NONBLOCK);

	r = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &(int){1}, SizeOf(int));
	Assert(r >= 0);

	echoContext *ctx = echoAlloc();
	*ctx = (echoContext){
		.loop = listenCtx->loop,
		.sock = sock,
	};

	EventRecv(ctx->loop, sock, ctx->contentBuf, CountOf(ctx->contentBuf), echoOnRecv, ctx);

	EventAccept(listenCtx->loop, listenCtx->sock, echoOnAccept, listenCtx);
}

static void
echoOnRecv(void *arg, Ptr ret)
{
	echoContext *ctx = arg;

	if (ret <= 0) {
		// TODO(fmrsn): log error if ret < 0
		EventClose(ctx->loop, ctx->sock, echoOnClose, ctx);
		return;
	}

	ctx->content = ctx->contentBuf;
	ctx->contentSize = ret;

	EventSend(ctx->loop, ctx->sock, ctx->contentBuf, ctx->contentSize, echoOnSend, ctx);
}

static void
echoOnSend(void *arg, Ptr ret)
{
	echoContext *ctx = arg;

	if (ret < 0) {
		// TODO(fmrsn): log error if ret != -EPIPE
		EventClose(ctx->loop, ctx->sock, echoOnClose, ctx);
		return;
	}

	Size numSent = ret;
	ctx->content += numSent;
	ctx->contentSize -= numSent;

	if (ctx->contentSize > 0) {
		EventSend(ctx->loop, ctx->sock, ctx->content, ctx->contentSize, echoOnSend, ctx);
	} else {
		EventRecv(
			ctx->loop, ctx->sock, ctx->contentBuf, CountOf(ctx->contentBuf), echoOnRecv,
			ctx);
	}
}

static void
echoOnClose(void *arg, Ptr ret)
{
	(void)ret;

	echoContext *ctx = arg;
	echoFree(ctx);
}

static void
loop(void *arg)
{
	(void)arg;

	EventLoopForNs(&eventLoop, 10L * 1000L * 1000L); // 10 ms
	TaskExecute(&taskQueue, loop, NULL);
}

static void
hello(void *arg)
{
	(void)arg;
	fprintf(stderr, "hello\n");
}

int
main(void)
{
	// NOTE(fmrsn): We don't use this function (yet).
	(void)EventTick;

	int r;

	srand(time(NULL) ^ getpid());
	setlocale(LC_ALL, "C");
	signal(SIGPIPE, SIG_IGN);

	TaskInitQueue(&taskQueue, 1);
	EventInitLoop(&eventLoop);

	struct addrinfo hints = {
		.ai_family = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
		.ai_flags = AI_PASSIVE,
	};
	struct addrinfo *addrinfo;
	r = getaddrinfo("localhost", "8080", &hints, &addrinfo);
	Assert(r == 0);

	for (struct addrinfo *ai = addrinfo; ai; ai = ai->ai_next) {
		int sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
		Assert(sock != -1);

		int flags = fcntl(sock, F_GETFL);
		fcntl(sock, F_SETFL, flags | O_NONBLOCK);

		r = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, SizeOf(int));
		Assert(r != -1);

		r = bind(sock, ai->ai_addr, ai->ai_addrlen);
		Assert(r != -1);

		r = listen(sock, SOMAXCONN);
		Assert(r != -1);

		echoContext *listenCtx = echoAlloc();
		listenCtx->loop = &eventLoop;
		listenCtx->sock = sock;
		EventAccept(&eventLoop, sock, echoOnAccept, listenCtx);
	}

	freeaddrinfo(addrinfo);

	TaskExecute(&taskQueue, loop, NULL);

	TaskWaitAll(&taskQueue);

	EventDeinitLoop(&eventLoop);
	TaskDeinitQueue(&taskQueue);
}
