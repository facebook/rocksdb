package org.rocksdb.limiter;

public class ConcurrentTaskLimiterImpl extends ConcurrentTaskLimiter {
    public ConcurrentTaskLimiterImpl(final String name,
                                     final int maxOutstandingTask) {
        super(newConcurrentTaskLimiterImpl0(name, maxOutstandingTask));
    }

    private native static long newConcurrentTaskLimiterImpl0(final String name,
                                                             final int maxOutstandingTask);

    @Override
    protected final native void disposeInternal(final long handle);
}
