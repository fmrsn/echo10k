#define _POSIX_C_SOURCE 200908L // TODO(fmrsn): Move to command line

#include <stddef.h>
#include <stdint.h>

#undef bool

typedef int32_t i32;
typedef int64_t i64;

typedef i32 bool;

// TODO(fmrsn): change codebase to use isize
typedef ptrdiff_t isize;
typedef size_t usize;

typedef intptr_t iptr;
typedef uintptr_t uptr;

#define SIZEOF(x) (isize)(sizeof(x))
#define COUNTOF(x) (SIZEOF(x) / SIZEOF((x)[0]))

typedef struct task_queue task_queue;
typedef void task_callback(void *arg);

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void task_initqueue(task_queue *q, int nworkers);
static void task_deinitqueue(task_queue *q);
static void task_submit(task_queue *q, task_callback *cb, void *arg);
static void task_waitall(task_queue *q);

typedef struct event_loop event_loop;
typedef void event_callback(void *arg, iptr retval);

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void event_initloop(event_loop *loop);
static void event_deinitloop(event_loop *loop);
static void event_tick(event_loop *loop);
static void event_loopfor(event_loop *loop, i64 ns);
static void event_accept(event_loop *loop, int sock, event_callback *cb, void *arg);
static void event_close(event_loop *loop, int fd, event_callback *cb, void *arg);
static void
event_recv(event_loop *loop, int sock, void *buf, isize size, event_callback *cb, void *arg);
static void
event_send(event_loop *loop, int sock, const void *buf, isize size, event_callback *cb, void *arg);
static void event_timeout(event_loop *loop, i64 ns, event_callback *cb, void *arg);

// TODO(fmrsn): operation: cancel submission

/* *************************************** */

// TODO(fmrsn): revise usage of assertions throughout the code

#include <sys/queue.h>

#include <assert.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct tasksubmission {
	TAILQ_ENTRY(tasksubmission) link;
	task_queue *queue;
	pthread_t tid;
	task_callback *cb;
	void *arg;
} tasksubmission;

typedef TAILQ_HEAD(tasksubmissionlist, tasksubmission) tasksubmissionlist;

struct task_queue {
	sigset_t fillset;
	pthread_attr_t workerattr;
	pthread_cond_t worktodo;
	pthread_cond_t alldone;
	pthread_cond_t stopped;
	pthread_mutex_t mutex;
	tasksubmissionlist pending;
	tasksubmissionlist active;
	i32 spawned;
	i32 idle;
	bool waiting;
	bool deiniting;
};

// TODO: use memory arenas
static tasksubmission *
taskalloc(void)
{
	tasksubmission *s = malloc(SIZEOF(*s));
	assert(s);
	return s;
}

static void
taskfree(tasksubmission *s)
{
	free(s);
}

static void taskdone(void *arg);
static void taskcleanup(void *arg);

static void *
taskwork(void *arg)
{
	task_queue *queue = arg;
	pthread_t tid = pthread_self();

	(void)pthread_mutex_lock(&queue->mutex);
	++queue->spawned;

	pthread_cleanup_push(taskcleanup, queue);
	for (;;) {
		/* Reset thread signals and cancellation state. */
		(void)pthread_sigmask(SIG_SETMASK, &queue->fillset, NULL);
		(void)pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
		(void)pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

		++queue->idle;
		while (!queue->deiniting && TAILQ_EMPTY(&queue->pending)) {
			(void)pthread_cond_wait(&queue->worktodo, &queue->mutex);
		}
		--queue->idle;
		if (queue->deiniting) {
			break;
		}

		tasksubmission *submission;
		if ((submission = TAILQ_FIRST(&queue->pending))) {
			TAILQ_REMOVE(&queue->pending, submission, link);

			submission->tid = tid;
			submission->queue = queue;
			TAILQ_INSERT_TAIL(&queue->active, submission, link);

			pthread_mutex_unlock(&queue->mutex);

			pthread_cleanup_push(taskdone, submission);
			submission->cb(submission->arg);
			pthread_cleanup_pop(1);
		}
	}
	pthread_cleanup_pop(1);

	return NULL;
}

static void
taskdone(void *arg)
{
	tasksubmission *submission = arg;
	task_queue *queue = submission->queue;

	(void)pthread_mutex_lock(&queue->mutex);

	TAILQ_REMOVE(&queue->active, submission, link);

	taskfree(submission);

	if (!TAILQ_EMPTY(&queue->pending) || !TAILQ_EMPTY(&queue->active)) {
		return;
	}
	if (queue->waiting) {
		(void)pthread_cond_broadcast(&queue->alldone);
		queue->waiting = 0;
	}
}

static void
taskcleanup(void *arg)
{
	task_queue *queue = arg;
	pthread_t tid;
	int ret;

	--queue->spawned;

	if (!queue->deiniting) {
		ret = pthread_create(&tid, &queue->workerattr, taskwork, queue);
		assert(ret == 0);

	} else if (queue->spawned == 0) {
		(void)pthread_cond_broadcast(&queue->stopped);
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
task_initqueue(task_queue *queue, int nworkers)
{
	assert(nworkers > 0);

	(void)sigfillset(&queue->fillset);

	(void)pthread_attr_init(&queue->workerattr);
	(void)pthread_attr_setdetachstate(&queue->workerattr, PTHREAD_CREATE_DETACHED);

	(void)pthread_mutex_init(&queue->mutex, NULL);
	(void)pthread_cond_init(&queue->worktodo, NULL);
	(void)pthread_cond_init(&queue->alldone, NULL);
	(void)pthread_cond_init(&queue->stopped, NULL);

	TAILQ_INIT(&queue->pending);
	TAILQ_INIT(&queue->active);

	queue->idle = 0;
	queue->spawned = 0;

	pthread_t tid;
	int ret;
	for (int i = 0; i < nworkers; ++i) {
		ret = pthread_create(&tid, &queue->workerattr, taskwork, queue);
		assert(ret == 0);
	}
}

static void
task_submit(task_queue *queue, task_callback *cb, void *arg)
{
	(void)pthread_mutex_lock(&queue->mutex);

	tasksubmission *submission = taskalloc();
	*submission = (tasksubmission){.queue = queue, .cb = cb, .arg = arg};

	TAILQ_INSERT_TAIL(&queue->pending, submission, link);
	if (queue->idle > 0) {
		(void)pthread_cond_signal(&queue->worktodo);
	}

	(void)pthread_mutex_unlock(&queue->mutex);
}

static void
taskunlockmutex(void *mutex)
{
	(void)pthread_mutex_unlock(mutex);
}

static void
task_waitall(task_queue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(taskunlockmutex, &queue->mutex);

	while (!TAILQ_EMPTY(&queue->pending) || !TAILQ_EMPTY(&queue->active)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->alldone, &queue->mutex);
	}

	pthread_cleanup_pop(1);
}

static void
task_deinitqueue(task_queue *queue)
{
	(void)pthread_mutex_lock(&queue->mutex);
	pthread_cleanup_push(taskunlockmutex, &queue->mutex);

	queue->deiniting = 1;
	(void)pthread_cond_broadcast(&queue->worktodo);

	tasksubmission *active;
	TAILQ_FOREACH (active, &queue->active, link) {
		(void)pthread_cancel(active->tid);
	}
	while (!TAILQ_EMPTY(&queue->active)) {
		queue->waiting = 1;
		(void)pthread_cond_wait(&queue->alldone, &queue->mutex);
	}

	while (queue->spawned > 0) {
		(void)pthread_cond_wait(&queue->stopped, &queue->mutex);
	}

	pthread_cleanup_pop(1);

	pthread_attr_destroy(&queue->workerattr);
	pthread_cond_destroy(&queue->worktodo);
	pthread_cond_destroy(&queue->alldone);
	pthread_cond_destroy(&queue->stopped);
	pthread_mutex_destroy(&queue->mutex);
}

#include <sys/event.h>

typedef struct {
	enum {
		EVENTOP_ACCEPT,
		EVENTOP_CLOSE,
		EVENTOP_RECV,
		EVENTOP_SEND,
		EVENTOP_TIMEOUT,
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
			isize size;
		} recv;

		struct {
			int sock;
			const void *buf;
			isize size;
		} send;

		struct {
			i64 ns;
		} timeout;
	} args;
} eventop;

// TODO(fmrsn): use memory arena + pool allocator for context objects
static void event_tick(event_loop *loop);
static void event_loopfor(event_loop *loop, i64 ns);

typedef struct eventsubmission {
	eventop op;
	iptr (*onready)(const eventop *op);
	TAILQ_ENTRY(eventsubmission) link;
	event_callback *callback;
	void *arg;
	iptr retval;
} eventsubmission;

typedef TAILQ_HEAD(eventsubmissionqueue, eventsubmission) eventsubmissionqueue;

struct event_loop {
	int kq;
	int inflight;
	eventsubmissionqueue pending;
	eventsubmissionqueue completed;
};

#include <signal.h>

static eventsubmission *
eventalloc(void)
{
	eventsubmission *s = malloc(SIZEOF(*s));
	assert(s);
	return s;
}

static void
eventfree(eventsubmission *s)
{
	free(s);
}

static void
eventinit(void)
{
	signal(SIGPIPE, SIG_IGN);
}

static void
event_initloop(event_loop *loop)
{
	static pthread_once_t once = PTHREAD_ONCE_INIT;
	pthread_once(&once, eventinit);

	int kq = kqueue();
	assert(kq >= 0);

	loop->kq = kq;
	loop->inflight = 0;
	TAILQ_INIT(&loop->pending);
	TAILQ_INIT(&loop->completed);
}

static int
eventflushpending(
	eventsubmissionqueue *pending, isize nevents, struct kevent events[static nevents])
{
	eventsubmission *submission;

	int i;
	for (i = 0; i < nevents; ++i) {
		if (!(submission = TAILQ_FIRST(pending))) {
			break;
		}
		TAILQ_REMOVE(pending, submission, link);

		struct kevent *event = &events[i];
		*event = (struct kevent){
			.flags = EV_ADD | EV_ENABLE | EV_ONESHOT,
			.udata = submission,
		};

		eventop *op = &submission->op;

		switch (op->code) {
		case EVENTOP_ACCEPT:
			event->ident = op->args.accept.sock;
			event->filter = EVFILT_READ;
			break;

		case EVENTOP_RECV:
			event->ident = op->args.recv.sock;
			event->filter = EVFILT_READ;
			break;

		case EVENTOP_SEND:
			event->ident = op->args.send.sock;
			event->filter = EVFILT_WRITE;
			break;

		case EVENTOP_TIMEOUT:
			event->ident = (uptr)submission;
			event->filter = EVFILT_TIMER;
			event->fflags = NOTE_NSECONDS;
			event->data = op->args.timeout.ns;
			break;

		default:
			assert(!"unreachable");
		}
	}

	return i;
}

#include <errno.h>

static void
eventflushandwait(event_loop *loop, const struct timespec *timeout)
{
	int kq = loop->kq;
	eventsubmissionqueue pending = loop->pending;
	eventsubmissionqueue completed = loop->completed;

	struct kevent events[256];
	int nchanges = eventflushpending(&pending, COUNTOF(events), events);

	if (nchanges > 0 || (TAILQ_EMPTY(&completed) && loop->inflight > 0)) {
		int nevents = kevent(kq, events, nchanges, events, COUNTOF(events), timeout);
		assert(nevents >= 0);

		loop->pending = pending;
		loop->inflight += nchanges - nevents;

		for (int i = 0; i < nevents; ++i) {
			struct kevent *event = &events[i];
			eventsubmission *submission = event->udata;

			submission->retval = event->fflags & EV_ERROR ? -event->data : 0;
			TAILQ_INSERT_TAIL(&completed, submission, link);
		}
	}

	TAILQ_INIT(&loop->completed);

	eventsubmission *submission;
	while ((submission = TAILQ_FIRST(&completed))) {
		TAILQ_REMOVE(&completed, submission, link);

		if (submission->retval >= 0 && submission->onready) {
			submission->retval = submission->onready(&submission->op);
		}
		if (submission->retval == -EAGAIN || submission->retval == -EWOULDBLOCK) {
			TAILQ_INSERT_TAIL(&pending, submission, link);
			continue;
		}

		event_callback *cb = submission->callback;
		void *arg = submission->arg;
		iptr retval = submission->retval;
		eventfree(submission);

		if (cb) {
			cb(arg, retval);
		}
	}
}

static void
event_tick(event_loop *loop)
{
	eventflushandwait(loop, &(struct timespec){0});
}

static void
eventloopdone(void *arg, iptr retval)
{
	(void)retval;

	int *done = arg;
	*done = 1;
}

static void
event_loopfor(event_loop *loop, i64 ns)
{
	int done = 0;

	event_timeout(loop, ns, eventloopdone, &done);
	while (!done) {
		eventflushandwait(loop, NULL);
	}
}

#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>

static void
eventsubmit(eventsubmissionqueue *queue, eventsubmission *submission)
{
	eventsubmission *s = eventalloc();
	*s = *submission;
	TAILQ_INSERT_TAIL(queue, s, link);
}

static iptr
eventacceptonready(const eventop *op)
{
	assert(op->code == EVENTOP_ACCEPT);

	int r;

	int sock = accept(op->args.accept.sock, NULL, NULL);
	if (sock < 0) {
		return -errno;
	}

	int flags = fcntl(sock, F_GETFL);
	fcntl(sock, F_SETFL, flags | O_NONBLOCK);

	r = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &(int){1}, sizeof(int));
	assert(r == 0);

	return sock;
}

static void
event_accept(event_loop *loop, int sock, event_callback *cb, void *arg)
{
	eventsubmit(
		&loop->completed,
		&(eventsubmission){
			.op.code = EVENTOP_ACCEPT,
			.op.args.accept.sock = sock,
			.onready = eventacceptonready,
			.callback = cb,
			.arg = arg,
		});
}

static iptr
eventcloseonready(const eventop *op)
{
	assert(op->code == EVENTOP_CLOSE);

	if (close(op->args.close.fd) < 0) {
		return -errno;
	}
	return 0;
}

static void
event_close(event_loop *loop, int fd, event_callback *cb, void *arg)
{
	eventsubmit(
		&loop->completed,
		&(eventsubmission){
			.op.code = EVENTOP_CLOSE,
			.op.args.close.fd = fd,
			.onready = eventcloseonready,
			.callback = cb,
			.arg = arg,
		});
}

static iptr
eventrecvonready(const eventop *op)
{
	assert(op->code == EVENTOP_RECV);

	int sock = op->args.recv.sock;
	void *buffer = op->args.recv.buf;
	isize bufferSize = op->args.recv.size;

	ssize_t n = recv(sock, buffer, bufferSize, 0);
	return n < 0 ? -errno : n;
}

static void
event_recv(event_loop *loop, int sock, void *buf, isize size, event_callback *cb, void *arg)
{
	eventsubmit(
		&loop->completed,
		&(eventsubmission){
			.op.code = EVENTOP_RECV,
			.op.args.recv.sock = sock,
			.op.args.recv.buf = buf,
			.op.args.recv.size = size,
			.onready = eventrecvonready,
			.callback = cb,
			.arg = arg,
		});
}

static iptr
eventsendonready(const eventop *op)
{
	assert(op->code == EVENTOP_SEND);

	int sock = op->args.recv.sock;
	const void *buffer = op->args.recv.buf;
	isize bufferSize = op->args.recv.size;

	ssize_t n = send(sock, buffer, bufferSize, 0);
	return n < 0 ? -errno : n;
}

static void
event_send(event_loop *loop, int sock, const void *buf, isize size, event_callback *cb, void *arg)
{
	eventsubmit(
		&loop->completed,
		&(eventsubmission){
			.op.code = EVENTOP_SEND,
			.op.args.send.sock = sock,
			.op.args.send.buf = buf,
			.op.args.send.size = size,
			.onready = eventsendonready,
			.callback = cb,
			.arg = arg,
		});
}

static void
event_timeout(event_loop *loop, i64 ns, event_callback *cb, void *arg)
{
	eventsubmit(
		&loop->pending,
		&(eventsubmission){
			.op.code = EVENTOP_TIMEOUT,
			.op.args.timeout.ns = ns,
			.callback = cb,
			.arg = arg,
		});
}

static void
event_deinitloop(event_loop *loop)
{
	(void)close(loop->kq);
}

/***********************************************************************/

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>

static task_queue taskqueue;
static event_loop eventloop;

void
hello(void *arg)
{
	printf("Hello from worker %d\n", (int)(intptr_t)arg);
}

int
main(void)
{
	(void)event_tick;
	(void)event_loopfor;
	(void)event_accept;
	(void)event_close;
	(void)event_recv;
	(void)event_send;

	task_initqueue(&taskqueue, 4);
	event_initloop(&eventloop);

	for (int i = 0; i < 5; ++i) {
		task_submit(&taskqueue, hello, (void *)(iptr)i);
	}

	task_waitall(&taskqueue);

	event_deinitloop(&eventloop);
	task_deinitqueue(&taskqueue);
}
