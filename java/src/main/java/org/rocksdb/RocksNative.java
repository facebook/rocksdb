// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RocksNative implements AutoCloseable {
  /**
   * The reference is final, and the fact that it has been closed (and is no longer valid) must be
   * checked with the atomic flag.
   */
  private final long nativeAPIReference_;
  protected final AtomicBoolean isOpen;

  protected RocksNative(long nativeReference) {
    this(nativeReference, true);
  }

  protected RocksNative(long nativeReference, boolean isOpen) {
    this.nativeAPIReference_ = nativeReference;
    this.isOpen = new AtomicBoolean(isOpen);
  }

  @Override
  public void close() {
    if (isOpen.getAndSet(false)) {
      nativeClose(nativeAPIReference_);
    }
  }

  public final long getNative() {
    if (isOpen.get()) {
      return nativeAPIReference_;
    } else {
      // TODO AP ref-counting-experiments - should we throw a checked exception ?
      throw new IllegalStateException("RocksDB native reference was previously closed");
    }
  }

  public final long[] getReferenceCounts() {
    return getReferenceCounts(getNative());
  }

  /**
   * The native method knows about the C++ class of the native reference,
   * and will go about discounting shared_ptrs enclosed in that object.
   *
   * @param nativeReference index to a table containing a reference
   */
  protected abstract void nativeClose(long nativeReference);

  /**
   * Test support method to retrieve reference counts of shadow objects
   *
   * @return a long array, with reference counts depending on the type of object/shadow
   */
  protected abstract long[] getReferenceCounts(long nativeReference);
}
