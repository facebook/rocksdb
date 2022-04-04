package org.rocksdb.api;

import org.rocksdb.RocksDBException;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RocksNative implements AutoCloseable {

  /**
   * The reference is final, and the fact that it has been closed (and is no longer valid) must be checked
   * with the atomic flag.
   */
  private final long nativeReference_;
  protected final AtomicBoolean isOpen;

  protected RocksNative(long nativeReference) {
    this(nativeReference, true);
  }

  protected RocksNative(long nativeReference, boolean isOpen) {
    this.nativeReference_ = nativeReference;
    this.isOpen = new AtomicBoolean(isOpen);
  }

  @Override
  public final void close() {

    if (isOpen.getAndSet(false)) {
      nativeClose(nativeReference_);
    }
  }

  public long getNative() {
    if (isOpen.get()) {
      return nativeReference_;
    } else {
      // TODO AP ref-counting-experiments - should we throw a checked exception ?
      throw new RuntimeException("RocksDB native reference was previously closed");
    }
  }

  /**
   * The native method knows about the C++ class of the native reference,
   * and will go about discounting shared_ptrs enclosed in that object.
   *
   * @param nativeReference index to a table containing a reference
   */
  protected abstract void nativeClose(long nativeReference);
}
