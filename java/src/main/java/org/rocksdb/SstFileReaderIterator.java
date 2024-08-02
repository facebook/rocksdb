// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * <p>An iterator that yields a sequence of key/value pairs from a source.
 * Multiple implementations are provided by this library.
 * In particular, iterators are provided
 * to access the contents of a Table or a DB.</p>
 *
 * <p>Multiple threads can invoke const methods on an RocksIterator without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same RocksIterator must use
 * external synchronization.</p>
 *
 * @see RocksObject
 */
public class SstFileReaderIterator extends AbstractRocksIterator<SstFileReader> {
  protected SstFileReaderIterator(final SstFileReader reader, final long nativeHandle) {
    super(reader, nativeHandle);
  }

  /**
   * <p>Return the key for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @return key for the current entry.
   */
  public byte[] key() {
    assert (isOwningHandle());
    return key0(nativeHandle_);
  }

  /**
   * <p>Return the key for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @param key the out-value to receive the retrieved key.
   *     It is using position and limit. Limit is set according to key size.
   *     Supports direct buffer only.
   * @return The size of the actual key. If the return key is greater than the
   *     length of {@code key}, then it indicates that the size of the
   *     input buffer {@code key} is insufficient and partial result will
   *     be returned.
   */
  public int key(final ByteBuffer key) {
    assert (isOwningHandle());
    final int result;
    if (key.isDirect()) {
      result = keyDirect0(nativeHandle_, key, key.position(), key.remaining());
    } else {
      result = keyByteArray0(
          nativeHandle_, key.array(), key.arrayOffset() + key.position(), key.remaining());
    }
    key.limit(Math.min(key.position() + result, key.limit()));
    return result;
  }

  /**
   * <p>Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: !AtEnd() &amp;&amp; !AtStart()</p>
   * @return value for the current entry.
   */
  public byte[] value() {
    assert (isOwningHandle());
    return value0(nativeHandle_);
  }

  /**
   * <p>Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @param value the out-value to receive the retrieved value.
   *     It is using position and limit. Limit is set according to value size.
   *     Supports direct buffer only.
   * @return The size of the actual value. If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.
   */
  public int value(final ByteBuffer value) {
    assert (isOwningHandle());
    final int result;
    if (value.isDirect()) {
      result = valueDirect0(nativeHandle_, value, value.position(), value.remaining());
    } else {
      result = valueByteArray0(
          nativeHandle_, value.array(), value.arrayOffset() + value.position(), value.remaining());
    }
    value.limit(Math.min(value.position() + result, value.limit()));
    return result;
  }

  @Override final native void refresh1(long handle, long snapshotHandle);
  @Override
  protected final void disposeInternal(final long handle) {
    disposeInternalJni(handle);
  }
  private static native void disposeInternalJni(final long handle);

  @Override
  final boolean isValid0(long handle) {
    return isValid0Jni(handle);
  }
  private static native boolean isValid0Jni(long handle);
  @Override
  final void seekToFirst0(long handle) {
    seekToFirst0Jni(handle);
  }
  private static native void seekToFirst0Jni(long handle);
  @Override
  final void seekToLast0(long handle) {
    seekToLast0Jni(handle);
  }
  private static native void seekToLast0Jni(long handle);
  @Override
  final void next0(long handle) {
    next0Jni(handle);
  }
  private static native void next0Jni(long handle);
  @Override
  final void prev0(long handle) {
    prev0Jni(handle);
  }
  private static native void prev0Jni(long handle);
  @Override
  final void refresh0(long handle) throws RocksDBException {
    refresh0Jni(handle);
  }
  private static native void refresh0Jni(long handle) throws RocksDBException;
  @Override
  final void seek0(long handle, byte[] target, int targetLen) {
    seek0Jni(handle, target, targetLen);
  }
  private static native void seek0Jni(long handle, byte[] target, int targetLen);
  @Override
  final void seekForPrev0(long handle, byte[] target, int targetLen) {
    seekForPrev0Jni(handle, target, targetLen);
  }
  private static native void seekForPrev0Jni(long handle, byte[] target, int targetLen);

  @Override
  final void status0(long handle) throws RocksDBException {
    status0Jni(handle);
  }
  private static native void status0Jni(long handle) throws RocksDBException;
  @Override
  final void seekDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen) {
    seekDirect0Jni(handle, target, targetOffset, targetLen);
  }
  private static native void seekDirect0Jni(
      long handle, ByteBuffer target, int targetOffset, int targetLen);
  @Override
  final void seekForPrevDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen) {
    seekForPrevDirect0Jni(handle, target, targetOffset, targetLen);
  }
  private static native void seekForPrevDirect0Jni(
      long handle, ByteBuffer target, int targetOffset, int targetLen);
  @Override
  final void seekByteArray0(
      final long handle, final byte[] target, final int targetOffset, final int targetLen) {
    seekByteArray0Jni(handle, target, targetOffset, targetLen);
  }
  private static native void seekByteArray0Jni(
      final long handle, final byte[] target, final int targetOffset, final int targetLen);
  @Override
  final void seekForPrevByteArray0(
      final long handle, final byte[] target, final int targetOffset, final int targetLen) {
    seekForPrevByteArray0Jni(handle, target, targetOffset, targetLen);
  }
  private static native void seekForPrevByteArray0Jni(
      final long handle, final byte[] target, final int targetOffset, final int targetLen);

  private static native byte[] key0(long handle);
  private static native byte[] value0(long handle);

  private static native int keyDirect0(
      long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);
  private static native int keyByteArray0(
      long handle, byte[] buffer, int bufferOffset, int bufferLen);
  private static native int valueDirect0(
      long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);
  private static native int valueByteArray0(
      long handle, byte[] buffer, int bufferOffset, int bufferLen);
}
