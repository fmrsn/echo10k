#define _POSIX_C_SOURCE 200908L // TODO(fmrsn): Move to command line

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define countof(x) (sizeof(x) / sizeof((x)[0]))

// TODO(fmrsn): Remove stdlib dependency
#include <assert.h> // assert(x)

#define unreachable() assert(!"unreachable")

typedef struct TaskQueue TaskQueue;
typedef void TaskFunc(void *arg);

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void task_init_queue(TaskQueue *queue, int nworkers);
static void task_deinit_queue(TaskQueue *queue);
static void task_execute(TaskQueue *queue, TaskFunc *func, void *arg);
static void task_wait_all(TaskQueue *queue);
// TODO(fmrsn): static void task_wait(who?)
// TODO(fmrsn): static void task_cancel(what?);

typedef struct EventLoop EventLoop;
typedef void EventCallback(void *arg, intptr_t ret);

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void event_init_loop(EventLoop *loop);
static void event_deinit_loop(EventLoop *loop);
static void event_accept(EventLoop *loop, int sock, EventCallback *cb, void *arg);
static void event_close(EventLoop *loop, int fd, EventCallback *cb, void *arg);
static void
event_recv(EventLoop *loop, int sock, void *buf, size_t bufsize, EventCallback *cb, void *arg);
static void event_send(
	EventLoop *loop, int sock, const void *buf, size_t bufsize, EventCallback *cb, void *arg);
static void event_timer(EventLoop *loop, int64_t ns, EventCallback *cb, void *arg);
static void event_tick(EventLoop *loop);
static void event_loop_for_ns(EventLoop *loop, int64_t ns);

// TODO(fmrsn): operation: cancel event submission

typedef struct EventLoopGroup EventLoopGroup;

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

typedef struct TaskSubmission {
	TAILQ_ENTRY(TaskSubmission) link;
	TaskQueue *queue;
	pthread_t thread;
	TaskFunc *func;
	void *arg;
} TaskSubmission;

typedef TAILQ_HEAD(, TaskSubmission) TaskSubmissionQueue;

struct TaskQueue {
	sigset_t sigfillset;
	pthread_attr_t workerattr;
	pthread_cond_t newtask;
	pthread_cond_t alldone;
	pthread_cond_t stopped;
	pthread_mutex_t mutex;
	TaskSubmissionQueue pending;
	TaskSubmissionQueue active;
	int nspawned;
	int nidle;
	bool waiting;
	bool deiniting;
};

// TODO(fmrsn): (De)allocations using malloc/free here are pretty frequent.
//  It can (and will) cause a bottleneck. Use memory arenas instead.
static TaskSubmission *
task_alloc_submission(void)
{
	TaskSubmission *s = malloc(sizeof *s);
	assert(s);
	return s;
}

static void
task_free_submission(TaskSubmission *s)
{
	free(s);
}

static void task_done(void *arg);
static void task_cleanup(void *arg);

static void *
task_work(void *arg)
{
	TaskQueue *queue = arg;
	pthread_t thread = pthread_self();

	(void)pthread_mutex_lock(&queue->mutex);
	++queue->nspawned;

	pthread_cleanup_push(task_cleanup, queue);
	for (;;) {
		/* Reset thread signals and cancellation state. */
		(void)pthread_sigmask(SIG_SETMASK, &queue->sigfillset, NULL);
		(void)pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
		(void)pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

		++queue->nidle;
		while (!queue->deiniting && TAILQ_EMPTY(&queue->pending)) {
			(void)pthread_cond_wait(&queue->newtask, &queue->mutex);
		}
		--queue->nidle;
		if (queue->deiniting) {
			break;
		}

		TaskSubmission *submission = TAILQ_FIRST(&queue->pending);
		if (!submission) {
			/*
			 * pthread_cond_wait might have woke up more than one
			 * worker, and one of the others caught the task.
			 * Welp...
			 */
			continue;
		}
		TAILQ_REMOVE(&queue->pending, submission, link);

		submission->thread = thread;
		submission->queue = queue;
		TAILQ_INSERT_TAIL(&queue->active, submission, link);

		pthread_mutex_unlock(&queue->mutex);

		pthread_cleanup_push(task_done, submission);
		submission->func(submission->arg);
		pthread_cleanup_pop(1);
	}
	pthread_cleanup_pop(1);

	return NULL;
}

static void
task_done(void *arg)
{
	TaskSubmission *submission = arg;
	TaskQueue *queue = submission->queue;

	(void)pthread_mutex_lock(&queue->mutex);

	TAILQ_REMOVE(&queue->active, submission, link);

	task_free_submission(submission);

	if (!TAILQ_EMPTY(&queue->pending) || !TAILQ_EMPTY(&queue->active)) {
		return;
	}
	if (queue->waiting) {
		(void)pthread_cond_broadcast(&queue->alldone);
		queue->waiting = 0;
	}
}

static void
task_cleanup(void *arg)
{
	TaskQueue *queue = arg;
	pthread_t t;
	int ret;

	--queue->nspawned;

	if (!queue->deiniting) {
		ret = pthread_create(&t, &queue->workerattr, task_work, queue);
		assert(ret == 0);

	} else if (queue->nspawned == 0) {
		(void)pthread_cond_broadcast(&queue->stopped);
	}

	pthread_mutex_unlock(&queue->mutex);
}

static void
task_unlock_mutex(void *mutex)
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
task_init_queue(TaskQueue *queue, int nworkers)
{
	assert(nworkers > 0);

	(void)sigfillset(&queue->sigfillset);

	(void)pthread_attr_init(&queue->workerattr);
	(void)pthread_attr_setdetachstate(&queue->workerattr, PTHREAD_CREATE_DETACHED);

	(void)pthread_mutex_init(&queue->mutex, NULL);
	(void)pthread_cond_init(&queue->newtask, NULL);
	(void)pthread_cond_init(&queue->alldone, NULL);
	(void)pthread_cond_init(&queue->stopped, NULL);

	TAILQ_INIT(&queue->pending);
	TAILQ_INIT(&queue->active);

	queue->nidle = 0;
	queue->nspawned = 0;

	for (int i = 0; i < nworkers; ++i) {
		pthread_t thread;
		int ret = pthread_create(&thread, &queue->workerattr, task_work, queue);
		assert(ret == 0);
	}
}

static void
task_execute(TaskQueue *queue, TaskFunc *func, void *arg)
{
	TaskSubmission *submission = task_alloc_submission();
	*submission = (TaskSubmission){
		.queue = queue,
		.func = func,
		.arg = arg,
	};

	(void)pthread_mutex_lock(&queue->mutex);

	TAILQ_INSERT_TAIL(&queue->pending, submission, link);
	if (queue->nidle > 0) {
		(void)pthread_cond_signal(&queue->newtask);
	}

	(void)pthread_mutex_unlock(&queue->mutex);
}

static void
task_wait_all(TaskQueue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(task_unlock_mutex, &queue->mutex);
	while (!TAILQ_EMPTY(&queue->pending) || !TAILQ_EMPTY(&queue->active)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->alldone, &queue->mutex);
	}
	pthread_cleanup_pop(1);
}

static void
task_deinit_queue(TaskQueue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);

	queue->deiniting = 1;
	(void)pthread_cond_broadcast(&queue->newtask);

	TaskSubmission *active;
	TAILQ_FOREACH (active, &queue->active, link) {
		(void)pthread_cancel(active->thread);
	}

	pthread_cleanup_push(task_unlock_mutex, &queue->mutex);
	while (!TAILQ_EMPTY(&queue->active)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->alldone, &queue->mutex);
	}
	while (queue->nspawned > 0) {
		(void)pthread_cond_wait(&queue->stopped, &queue->mutex);
	}
	pthread_cleanup_pop(1);

	pthread_attr_destroy(&queue->workerattr);
	pthread_cond_destroy(&queue->newtask);
	pthread_cond_destroy(&queue->alldone);
	pthread_cond_destroy(&queue->stopped);
	pthread_mutex_destroy(&queue->mutex);
}

/*****************************/

typedef struct {
	enum {
		EVENT_OP_ACCEPT,
		EVENT_OP_CLOSE,
		EVENT_OP_RECV,
		EVENT_OP_SEND,
		EVENT_OP_TIMER,
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
			void *buf;
			size_t bufsize;
		} recv;

		struct {
			int sock;
			const void *buf;
			size_t bufsize;
		} send;

		struct {
			int64_t ns;
		} timer;
	} args;
} EventOp;

typedef struct EventCompletion {
	STAILQ_ENTRY(EventCompletion) link;

	EventOp op;
	intptr_t (*onready)(const EventOp *op);
	void (*cb)(void *arg, intptr_t ret);
	void *arg;
	intptr_t ret;

} EventCompletion;

typedef STAILQ_HEAD(, EventCompletion) EventCompletionQueue;

struct EventLoop {
	int kq;
	int inflight;
	EventCompletionQueue pending;
	EventCompletionQueue completed;
};

static EventCompletion *
event_alloc_completion(void)
{
	EventCompletion *completion = malloc(sizeof *completion);
	assert(completion);
	return completion;
}

static void
event_free_completion(EventCompletion *completion)
{
	free(completion);
}

static int
event_flush_pending(EventCompletionQueue *pending, int bufsize, struct kevent buf[static bufsize])
{
	int i;
	EventCompletion *completion;
	for (i = 0; i < bufsize && (completion = STAILQ_FIRST(pending)); ++i) {
		STAILQ_REMOVE_HEAD(pending, link);

		struct kevent *event = &buf[i];
		*event = (struct kevent){
			.flags = EV_ADD | EV_ENABLE | EV_ONESHOT,
			.udata = completion,
		};

		EventOp *op = &completion->op;

		switch (op->code) {
		case EVENT_OP_ACCEPT:
			event->ident = op->args.accept.sock;
			event->filter = EVFILT_READ;
			break;

		case EVENT_OP_RECV:
			event->ident = op->args.recv.sock;
			event->filter = EVFILT_READ;
			break;

		case EVENT_OP_SEND:
			event->ident = op->args.send.sock;
			event->filter = EVFILT_WRITE;
			break;

		case EVENT_OP_TIMER:
			event->ident = (uintptr_t)completion;
			event->filter = EVFILT_TIMER;
			event->fflags = NOTE_NSECONDS;
			event->data = op->args.timer.ns;
			break;

		default:
			unreachable();
		}
	}

	return i;
}

static void
event_flush(EventLoop *loop, const struct timespec *timeout)
{
	struct kevent events[256];
	int nchanges = event_flush_pending(&loop->pending, countof(events), events);

	if (nchanges > 0 || (STAILQ_EMPTY(&loop->completed) && loop->inflight > 0)) {
		int nevents = kevent(loop->kq, events, nchanges, events, countof(events), timeout);
		assert(nevents >= 0);

		loop->inflight += nchanges - nevents;

		for (int i = 0; i < nevents; ++i) {
			struct kevent *event = &events[i];
			EventCompletion *completion = event->udata;
			completion->ret = event->fflags & EV_ERROR ? -event->data : 0;
			STAILQ_INSERT_TAIL(&loop->completed, completion, link);
		}
	}

	EventCompletion *last = STAILQ_LAST(&loop->completed, EventCompletion, link);
	EventCompletion *completed = NULL;
	while (completed != last && (completed = STAILQ_FIRST(&loop->completed))) {
		STAILQ_REMOVE_HEAD(&loop->completed, link);

		if (completed->ret >= 0 && completed->onready) {
			completed->ret = completed->onready(&completed->op);
		}

		bool retry = completed->ret == -EAGAIN || completed->ret == -EWOULDBLOCK;
		if (retry) {
			STAILQ_INSERT_TAIL(&loop->pending, completed, link);
		} else if (completed->cb) {
			completed->cb(completed->arg, completed->ret);
		}

		if (!retry) {
			event_free_completion(completed);
		}
	}
}

static void
event_init_loop(EventLoop *loop)
{
	assert(loop);

	int kq = kqueue();
	assert(kq >= 0);

	loop->kq = kq;
	loop->inflight = 0;
	STAILQ_INIT(&loop->pending);
	STAILQ_INIT(&loop->completed);
}

static void
event_tick(EventLoop *loop)
{
	event_flush(loop, &(struct timespec){0});
}

static void
event_loop_done(void *arg, intptr_t ret)
{
	(void)ret;

	bool *done = arg;
	*done = 1;
}

static void
event_loop_for_ns(EventLoop *loop, int64_t ns)
{
	bool done = 0;

	event_timer(loop, ns, event_loop_done, &done);
	while (!done) {
		event_flush(loop, NULL);
	}
}

static void
event_submit(EventCompletionQueue *queue, EventCompletion *desc)
{
	EventCompletion *completion = event_alloc_completion();
	*completion = *desc;

	STAILQ_INSERT_TAIL(queue, completion, link);
}

static intptr_t event_onready_accept(const EventOp *);
static intptr_t event_onready_close(const EventOp *);
static intptr_t event_onready_recv(const EventOp *);
static intptr_t event_onready_send(const EventOp *);

static void
event_accept(EventLoop *loop, int sock, EventCallback *cb, void *arg)
{
	event_submit(
		&loop->completed,
		&(EventCompletion){
			.op.code = EVENT_OP_ACCEPT,
			.op.args.accept = {sock},
			.onready = event_onready_accept,
			.cb = cb,
			.arg = arg,
		});
}

static intptr_t
event_onready_accept(const EventOp *op)
{
	int listensock = op->args.accept.sock;

	int sock = accept(listensock, NULL, NULL);
	if (sock < 0) {
		return -errno;
	}

	int flags = fcntl(sock, F_GETFL);
	fcntl(sock, F_SETFL, flags | O_NONBLOCK);

	// TODO: move to echoSetup
	int r = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &(int){1}, sizeof(int));
	assert(r == 0);

	return sock;
}

static void
event_close(EventLoop *loop, int fd, EventCallback *cb, void *arg)
{
	event_submit(
		&loop->completed,
		&(EventCompletion){
			.op.code = EVENT_OP_CLOSE,
			.op.args.close = {fd},
			.onready = event_onready_close,
			.cb = cb,
			.arg = arg,
		});
}

static intptr_t
event_onready_close(const EventOp *op)
{
	int fd = op->args.close.fd;

	int r = close(fd);
	return r < 0 ? -errno : 0;
}

static void
event_recv(EventLoop *loop, int sock, void *buf, size_t bufsize, EventCallback *cb, void *arg)
{
	assert(bufsize >= 0);

	event_submit(
		&loop->completed,
		&(EventCompletion){
			.op.code = EVENT_OP_RECV,
			.op.args.recv = {sock, buf, bufsize},
			.onready = event_onready_recv,
			.cb = cb,
			.arg = arg,
		});
}

static intptr_t
event_onready_recv(const EventOp *op)
{
	int sock = op->args.recv.sock;
	void *buf = op->args.recv.buf;
	size_t bufsize = op->args.recv.bufsize;

	ssize_t n = recv(sock, buf, bufsize, 0);
	return n < 0 ? -errno : n;
}

static void
event_send(EventLoop *loop, int sock, const void *buf, size_t bufsize, EventCallback *cb, void *arg)
{
	assert(bufsize >= 0);

	event_submit(
		&loop->completed,
		&(EventCompletion){
			.op.code = EVENT_OP_SEND,
			.op.args.send = {sock, buf, bufsize},
			.onready = event_onready_send,
			.cb = cb,
			.arg = arg,
		});
}

static intptr_t
event_onready_send(const EventOp *op)
{
	int sock = op->args.send.sock;
	const void *buf = op->args.send.buf;
	size_t bufsize = op->args.send.bufsize;

	ssize_t n = send(sock, buf, bufsize, 0);
	return n < 0 ? -errno : n;
}

static void
event_timer(EventLoop *loop, int64_t ns, EventCallback *cb, void *arg)
{
	assert(ns >= 0);

	event_submit(
		&loop->completed,
		&(EventCompletion){
			.op.code = EVENT_OP_TIMER,
			.op.args.timer = {ns},
			.cb = cb,
			.arg = arg,
		});
}

static void
event_deinit_loop(EventLoop *loop)
{
	// TODO: we're leaking memory alloc'ed for submissions. Fix it.
	(void)close(loop->kq);
}

/***********************************************************************/

typedef struct {
	EventLoop *loop;
	int sock;

	char *sendstart;
	size_t sendsize;
	char sendbuf[8192];
} EchoContext;

static EchoContext *
echo_alloc_context(void)
{
	EchoContext *ctx = malloc(sizeof *ctx);
	assert(ctx);
	return ctx;
}

static void
echo_free_context(EchoContext *ctx)
{
	free(ctx);
}

static void echo_on_accept(void *arg, intptr_t ret);
static void echo_on_recv(void *arg, intptr_t ret);
static void echo_on_send(void *arg, intptr_t ret);
static void echo_on_close(void *arg, intptr_t ret);

static void
echo_on_accept(void *arg, intptr_t ret)
{
	int r;

	if (ret < 0) {
		// TODO(fmrsn): Should we close the listening connection?
		return;
	}

	EchoContext *listenctx = arg;
	int sock = ret;

	int flags = fcntl(sock, F_GETFL);
	(void)fcntl(sock, F_SETFL, flags | O_NONBLOCK);

	r = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &(int){1}, sizeof(int));
	assert(r >= 0);

	EchoContext *ctx = echo_alloc_context();
	*ctx = (EchoContext){
		.loop = listenctx->loop,
		.sock = sock,
	};

	event_recv(ctx->loop, sock, ctx->sendbuf, countof(ctx->sendbuf), echo_on_recv, ctx);

	event_accept(listenctx->loop, listenctx->sock, echo_on_accept, listenctx);
}

static void
echo_on_recv(void *arg, intptr_t ret)
{
	EchoContext *ctx = arg;

	if (ret <= 0) {
		// TODO(fmrsn): log error if ret < 0
		event_close(ctx->loop, ctx->sock, echo_on_close, ctx);
		return;
	}

	ctx->sendstart = ctx->sendbuf;
	ctx->sendsize = ret;

	event_send(ctx->loop, ctx->sock, ctx->sendbuf, ctx->sendsize, echo_on_send, ctx);
}

static void
echo_on_send(void *arg, intptr_t ret)
{
	EchoContext *ctx = arg;

	if (ret < 0) {
		// TODO(fmrsn): log error if ret != -EPIPE
		event_close(ctx->loop, ctx->sock, echo_on_close, ctx);
		return;
	}

	size_t nsent = ret;
	ctx->sendstart += nsent;
	ctx->sendsize -= nsent;

	if (ctx->sendsize > 0) {
		event_send(ctx->loop, ctx->sock, ctx->sendstart, ctx->sendsize, echo_on_send, ctx);
	} else {
		event_recv(
			ctx->loop, ctx->sock, ctx->sendbuf, countof(ctx->sendbuf), echo_on_recv,
			ctx);
	}
}

static void
echo_on_close(void *arg, intptr_t ret)
{
	(void)ret;

	EchoContext *ctx = arg;
	echo_free_context(ctx);
}

#include <netdb.h>

#include <locale.h>
#include <stdio.h>

static EventLoop eventloop;
static TaskQueue taskqueue;

static void
loop(void *arg)
{
	(void)arg;

	event_loop_for_ns(&eventloop, 100L * 1000L * 1000L); // 100 ms
	task_execute(&taskqueue, loop, NULL);
}

int
main(void)
{
	// NOTE(fmrsn): We don't use this function (yet).
	(void)event_tick;

	int r;

	srand(time(NULL) ^ getpid());
	setlocale(LC_ALL, "C");
	signal(SIGPIPE, SIG_IGN);

	event_init_loop(&eventloop);
	task_init_queue(&taskqueue, 1);

	struct addrinfo hints = {
		.ai_family = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
		.ai_flags = AI_PASSIVE,
	};
	struct addrinfo *addrinfo;
	r = getaddrinfo("localhost", "8080", &hints, &addrinfo);
	assert(r == 0);

	for (struct addrinfo *ai = addrinfo; ai; ai = ai->ai_next) {
		int sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
		assert(sock != -1);

		int flags = fcntl(sock, F_GETFL);
		fcntl(sock, F_SETFL, flags | O_NONBLOCK);

		r = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int));
		assert(r != -1);

		r = bind(sock, ai->ai_addr, ai->ai_addrlen);
		assert(r != -1);

		r = listen(sock, SOMAXCONN);
		assert(r != -1);

		EchoContext *listenctx = echo_alloc_context();
		listenctx->loop = &eventloop;
		listenctx->sock = sock;
		event_accept(&eventloop, sock, echo_on_accept, listenctx);
	}

	freeaddrinfo(addrinfo);

	task_execute(&taskqueue, loop, NULL);

	task_wait_all(&taskqueue);

	task_deinit_queue(&taskqueue);
	event_deinit_loop(&eventloop);
}
