package org.rocksdb.limiter;

import org.rocksdb.RocksObject;

public abstract class ConcurrentTaskLimiter extends RocksObject {
    protected ConcurrentTaskLimiter(final long nativeHandle) {
        super(nativeHandle);
    }
}
