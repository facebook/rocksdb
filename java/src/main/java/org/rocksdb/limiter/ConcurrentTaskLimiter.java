package org.rocksdb.limiter;

import org.rocksdb.RocksObject;

public abstract class ConcurrentTaskLimiter extends RocksObject {
  protected ConcurrentTaskLimiter(final long nativeHandle) {
    super(nativeHandle);
  }

  public abstract String name();
  public abstract ConcurrentTaskLimiter setMaxOutstandingTask(final int maxOutstandinsTask);
  public abstract ConcurrentTaskLimiter resetMaxOutstandingTask();
  public abstract int outstandingTask();
}
