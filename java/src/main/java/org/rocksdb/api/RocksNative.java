package org.rocksdb.api;

public abstract class RocksNative implements AutoCloseable {

  /**
   * The reference is final, and the fact that it has been closed (and is no longer valid) must be checked
   * in native (C++) code where all synchronization is performed.
   */
  protected final long nativeReference;

  protected RocksNative(long nativeReference) {
    this.nativeReference = nativeReference;
  }

  @Override
  public final void close() {
    nativeClose(nativeReference);
  }

  /**
   * The native method knows about the C++ class of the native reference,
   * and will go about discounting shared_ptrs enclosed in that object.
   *
   * @param nativeReference index to a table containing a reference
   */
  abstract void nativeClose(long nativeReference);
}
