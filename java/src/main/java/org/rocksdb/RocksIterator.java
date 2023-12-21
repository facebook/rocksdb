// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import static org.rocksdb.util.BufferUtil.CheckBounds;

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
 * @see org.rocksdb.RocksObject
 */
public class RocksIterator extends AbstractRocksIterator<RocksDB> {
  protected RocksIterator(final RocksDB rocksDB, final long nativeHandle) {
    super(rocksDB, nativeHandle);
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
    assert(isOwningHandle());
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
   * @return The size of the actual key. If the return key is greater than
   *     the length of the buffer {@code key}, then it indicates that the size of the
   *     input buffer {@code key} is insufficient and partial result will
   *     be returned.
   */
  public int key(final byte[] key) {
    assert isOwningHandle();
    return keyByteArray0(nativeHandle_, key, 0, key.length);
  }

  /**
   * <p>Return the key for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @param key the out-value to receive the retrieved key.
   * @param offset in {@code key} at which to place the retrieved key
   * @param len limit to length of received key returned
   * @return The size of the actual key. If the return key is greater than
   *     {@code len}, then it indicates that the size of the
   *     input buffer {@code key} is insufficient and partial result will
   *     be returned.
   */
  public int key(final byte[] key, final int offset, final int len) {
    assert isOwningHandle();
    CheckBounds(offset, len, key.length);
    return keyByteArray0(nativeHandle_, key, offset, len);
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
   * @return The size of the actual key. If the return key is greater than the
   *     length of {@code key}, then it indicates that the size of the
   *     input buffer {@code key} is insufficient and partial result will
   *     be returned.
   */
  public int key(final ByteBuffer key) {
    assert isOwningHandle();
    final int result;
    if (key.isDirect()) {
      result = keyDirect0(nativeHandle_, key, key.position(), key.remaining());
    } else {
      assert key.hasArray();
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
    assert(isOwningHandle());
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
   * @return The size of the actual value. If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.
   */
  public int value(final ByteBuffer value) {
    assert isOwningHandle();
    final int result;
    if (value.isDirect()) {
      result = valueDirect0(nativeHandle_, value, value.position(), value.remaining());
    } else {
      assert value.hasArray();
      result = valueByteArray0(
          nativeHandle_, value.array(), value.arrayOffset() + value.position(), value.remaining());
    }
    value.limit(Math.min(value.position() + result, value.limit()));
    return result;
  }

  /**
   * <p>Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value. If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.
   */
  public int value(final byte[] value) {
    assert isOwningHandle();
    return valueByteArray0(nativeHandle_, value, 0, value.length);
  }

  /**
   * <p>Return the value for the current entry.  The underlying storage for
   * the returned slice is valid only until the next modification of
   * the iterator.</p>
   *
   * <p>REQUIRES: {@link #isValid()}</p>
   *
   * @param value the out-value to receive the retrieved value.
   * @param offset the offset within value at which to place the result
   * @param len the length available in value after offset, for placing the result
   * @return The size of the actual value. If the return value is greater than {@code len},
   *     then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.
   */
  public int value(final byte[] value, final int offset, final int len) {
    assert isOwningHandle();
    CheckBounds(offset, len, value.length);
    return valueByteArray0(nativeHandle_, value, offset, len);
  }

  @Override protected final native void disposeInternal(final long handle);
  @Override final native boolean isValid0(long handle);
  @Override final native void seekToFirst0(long handle);
  @Override final native void seekToLast0(long handle);
  @Override final native void next0(long handle);
  @Override final native void prev0(long handle);
  @Override final native void refresh0(long handle);
  @Override final native void refresh1(long handle, long snapshotHandle);
  @Override final native void seek0(long handle, byte[] target, int targetLen);
  @Override final native void seekForPrev0(long handle, byte[] target, int targetLen);
  @Override
  final native void seekDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  @Override
  final native void seekByteArray0(long handle, byte[] target, int targetOffset, int targetLen);
  @Override
  final native void seekForPrevDirect0(
      long handle, ByteBuffer target, int targetOffset, int targetLen);
  @Override
  final native void seekForPrevByteArray0(
      long handle, byte[] target, int targetOffset, int targetLen);
  @Override final native void status0(long handle) throws RocksDBException;

  private native byte[] key0(long handle);
  private native byte[] value0(long handle);
  private native int keyDirect0(long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);
  private native int keyByteArray0(long handle, byte[] array, int arrayOffset, int arrayLen);
  private native int valueDirect0(long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);
  private native int valueByteArray0(long handle, byte[] array, int arrayOffset, int arrayLen);
}
