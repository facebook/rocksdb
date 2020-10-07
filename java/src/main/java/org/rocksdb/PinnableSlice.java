package org.rocksdb;

public class PinnableSlice extends FastBuffer {

  private long pinnableSliceHandle;

  PinnableSlice(final long pinnableSliceHandle,
                final long dataHandle,
                final int size) {
    super(dataHandle, size);
    this.pinnableSliceHandle = pinnableSliceHandle;
  }

  @SuppressWarnings("unused")
  @Override
  protected void disposeInternal(long handle) {
    deletePinnableSlice(pinnableSliceHandle);
  }

  private static native void deletePinnableSlice(final long handle);
}
