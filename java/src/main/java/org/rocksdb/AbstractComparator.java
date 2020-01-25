// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * Comparators are used by RocksDB to determine
 * the ordering of keys.
 *
 * Implementations of Comparators in Java should extend this class.
 */
public abstract class AbstractComparator
    extends RocksCallbackObject {

  AbstractComparator() {
    super();
  }

  protected AbstractComparator(final ComparatorOptions copt) {
    super(copt.nativeHandle_);
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
      return createNewComparator(nativeParameterHandles[0]);
  }

  /**
   * Get the type of this comparator.
   *
   * Used for determining the correct C++ cast in native code.
   *
   * @return The type of the comparator.
   */
  ComparatorType getComparatorType() {
    return ComparatorType.JAVA_COMPARATOR;
  }

  /**
   * The name of the comparator.  Used to check for comparator
   * mismatches (i.e., a DB created with one comparator is
   * accessed using a different comparator).
   *
   * A new name should be used whenever
   * the comparator implementation changes in a way that will cause
   * the relative ordering of any two keys to change.
   *
   * Names starting with "rocksdb." are reserved and should not be used.
   *
   * @return The name of this comparator implementation
   */
  public abstract String name();

  /**
   * Three-way key comparison. Implementations should provide a
   * <a href="https://en.wikipedia.org/wiki/Total_order">total order</a>
   * on keys that might be passed to it.
   *
   * The implementation may modify the {@code ByteBuffer}s passed in, though
   * it would be unconventional to modify the "limit" or any of the
   * underlying bytes. As a callback, RocksJava will ensure that {@code a}
   * is a different instance from {@code b}.
   *
   * @param a buffer containing the first key in its "remaining" elements
   * @param b buffer containing the second key in its "remaining" elements
   *
   * @return Should return either:
   *    1) &lt; 0 if "a" &lt; "b"
   *    2) == 0 if "a" == "b"
   *    3) &gt; 0 if "a" &gt; "b"
   */
  public abstract int compare(final ByteBuffer a, final ByteBuffer b);

  /**
   * Called from JNI.
   *
   * @param a buffer access to first key
   * @param aLen the length of a, may be smaller than the buffer {@code a}
   * @param b buffer access to second key
   * @param bLen the length of b, may be smaller than the buffer {@code b}
   *
   * @return the result of the comparison
   */
  private int compareInternal(final ByteBuffer a, final int aLen,
      final ByteBuffer b, final int bLen) {
    if (aLen != -1) {
      a.mark();
      a.limit(aLen);
    }
    if (bLen != -1) {
      b.mark();
      b.limit(bLen);
    }

    final int c = compare(a, b);

    if (aLen != -1) {
      a.reset();
    }
    if (bLen != -1) {
      b.reset();
    }

    return c;
  }

  /**
   * <p>Used to reduce the space requirements
   * for internal data structures like index blocks.</p>
   *
   * <p>If start &lt; limit, you may modify start which is a
   * shorter string in [start, limit).</p>
   *
   * If you modify start, it is expected that you set the byte buffer so that
   * a subsequent read of start.remaining() bytes from start.position()
   * to start.limit() will obtain the new start value.
   *
   * <p>Simple comparator implementations may return with start unchanged.
   * i.e., an implementation of this method that does nothing is correct.</p>
   *
   * @param start the start
   * @param limit the limit
   */
  public void findShortestSeparator(final ByteBuffer start,
      final ByteBuffer limit) {
    // no-op
  }

  private int findShortestSeparatorInternal(
      final ByteBuffer start, final int startLen,
      final ByteBuffer limit, final int limitLen) {
    if (startLen != -1) {
      start.limit(startLen);
    }
    if (limitLen != -1) {
      limit.limit(limitLen);
    }
    findShortestSeparator(start, limit);
    return start.remaining();
  }

  /**
   * <p>Used to reduce the space requirements
   * for internal data structures like index blocks.</p>
   *
   * <p>You may change key to a shorter key (key1) where
   * key1 &ge; key.</p>
   *
   * <p>Simple comparator implementations may return the key unchanged.
   * i.e., an implementation of
   * this method that does nothing is correct.</p>
   *
   * @param key the key
   */
  public void findShortSuccessor(final ByteBuffer key) {
    // no-op
  }

  private int findShortSuccessorInternal(
      final ByteBuffer key, final int keyLen) {
    if (keyLen != -1) {
      key.limit(keyLen);
    }
    findShortSuccessor(key);
    return key.remaining();
  }

  public final boolean usingDirectBuffers() {
    return usingDirectBuffers(nativeHandle_);
  }

  private native boolean usingDirectBuffers(final long nativeHandle);

  private native long createNewComparator(final long comparatorOptionsHandle);
}
