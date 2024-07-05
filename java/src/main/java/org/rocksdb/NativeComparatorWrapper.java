// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * A simple abstraction to allow a Java class to wrap a custom comparator
 * implemented in C++.
 * <p>
 * The native comparator must directly extend rocksdb::Comparator.
 */
public abstract class NativeComparatorWrapper
    extends AbstractComparator {
  static final String NATIVE_CODE_IMPLEMENTATION_SHOULD_NOT_BE_CALLED =
      "This should not be called. Implementation is in Native code";

  @Override
  final ComparatorType getComparatorType() {
    return ComparatorType.JAVA_NATIVE_COMPARATOR_WRAPPER;
  }

  @Override
  public final String name() {
    throw new IllegalStateException(NATIVE_CODE_IMPLEMENTATION_SHOULD_NOT_BE_CALLED);
  }

  @Override
  public final int compare(final ByteBuffer s1, final ByteBuffer s2) {
    throw new IllegalStateException(NATIVE_CODE_IMPLEMENTATION_SHOULD_NOT_BE_CALLED);
  }

  @Override
  public final void findShortestSeparator(final ByteBuffer start, final ByteBuffer limit) {
    throw new IllegalStateException(NATIVE_CODE_IMPLEMENTATION_SHOULD_NOT_BE_CALLED);
  }

  @Override
  public final void findShortSuccessor(final ByteBuffer key) {
    throw new IllegalStateException(NATIVE_CODE_IMPLEMENTATION_SHOULD_NOT_BE_CALLED);
  }

  /**
   * We override {@link RocksCallbackObject#disposeInternal()}
   * as disposing of a native rocksdb::Comparator extension requires
   * a slightly different approach as it is not really a RocksCallbackObject
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private static native void disposeInternal(final long handle);
}
