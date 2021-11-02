#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "libaio.h"

/* Actual syscalls */
int io_setup(int maxevents, io_context_t *ctxp) {
    return syscall(__NR_io_setup, maxevents, ctxp);
}

int io_destroy(io_context_t ctx) {
    return syscall(__NR_io_destroy, ctx);
}

int io_submit(io_context_t ctx, long nr, struct iocb *ios[]) {
    return syscall(__NR_io_submit, ctx, nr, ios);
}

int io_cancel(io_context_t ctx, struct iocb *iocb, struct io_event *evt) {
    return syscall(__NR_io_cancel, ctx, iocb, evt);
}

#define AIO_RING_MAGIC                  0xa10a10a1
/* Ben will hate me for this */
struct aio_ring {
    unsigned        id;     /* kernel internal index number */
    unsigned        nr;     /* number of io_events */
    unsigned        head;
    unsigned        tail;

    unsigned        magic;
    unsigned        compat_features;
    unsigned        incompat_features;
    unsigned        header_length;  /* size of aio_ring */
};

int io_getevents(io_context_t ctx, long min_nr, long nr, struct io_event *events, struct timespec *timeout) {
    struct aio_ring *ring;
    ring = (struct aio_ring*)ctx;
    if (ring == NULL || ring->magic != AIO_RING_MAGIC) {
        goto do_syscall;
    }
    if (timeout!=NULL && timeout->tv_sec == 0 && timeout->tv_nsec == 0) {
        if (ring->head == ring->tail) {
            return 0;
        }
    }

do_syscall:
    return syscall(__NR_io_getevents, ctx, min_nr, nr, events, timeout);
}

int io_queue_init(int maxevents, io_context_t *ctxp)
{
    if (maxevents > 0) {
        *ctxp = NULL;
        return io_setup(maxevents, ctxp);
    }
    return -EINVAL;
}

int io_queue_release(io_context_t ctx)
{
    return io_destroy(ctx);
}

int io_queue_run(io_context_t ctx)
{
    static struct timespec timeout = { 0, 0 };
    struct io_event event;
    int ret = 0;

    /* FIXME: batch requests? */
    while (1 == (ret = io_getevents(ctx, 0, 1, &event, &timeout))) {
        io_callback_t cb = (io_callback_t)event.data;
        struct iocb *iocb = event.obj;

        cb(ctx, iocb, event.res, event.res2);
    }

    return ret;
}
