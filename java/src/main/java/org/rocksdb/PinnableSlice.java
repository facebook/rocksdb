package org.rocksdb;

/**
 * Object for using and managing native PinnableSlice object.
 * See {@link FastBuffer} for methods to access its data.
 */
public class PinnableSlice extends FastBuffer {
  private long pinnableSliceHandle;

  PinnableSlice(final long pinnableSliceHandle, final long dataHandle, final int size) {
    super(dataHandle, size);
    this.pinnableSliceHandle = pinnableSliceHandle;
  }

  @SuppressWarnings("unused")
  @Override
  protected void disposeInternal(final long handle) {
    deletePinnableSlice(pinnableSliceHandle);
  }

  private static native void deletePinnableSlice(final long handle);
}
