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
  protected SstFileReaderIterator(SstFileReader reader, long nativeHandle) {
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
  public int key(ByteBuffer key) {
    assert (isOwningHandle() && key.isDirect());
    int result = keyDirect0(nativeHandle_, key, key.position(), key.remaining());
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
  public int value(ByteBuffer value) {
    assert (isOwningHandle() && value.isDirect());
    int result = valueDirect0(nativeHandle_, value, value.position(), value.remaining());
    value.limit(Math.min(value.position() + result, value.limit()));
    return result;
  }

  @Override protected final native void disposeInternal(final long handle);
  @Override final native boolean isValid0(long handle);
  @Override final native void seekToFirst0(long handle);
  @Override final native void seekToLast0(long handle);
  @Override final native void next0(long handle);
  @Override final native void prev0(long handle);
  @Override final native void refresh0(long handle) throws RocksDBException;
  @Override final native void seek0(long handle, byte[] target, int targetLen);
  @Override final native void seekForPrev0(long handle, byte[] target, int targetLen);
  @Override final native void status0(long handle) throws RocksDBException;

  private native byte[] key0(long handle);
  private native byte[] value0(long handle);

  private native int keyDirect0(long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);
  private native int valueDirect0(long handle, ByteBuffer buffer, int bufferOffset, int bufferLen);

  @Override
  final native void seekDirect0(long handle, ByteBuffer target, int targetOffset, int targetLen);
  @Override
  final native void seekForPrevDirect0(
      long handle, ByteBuffer target, int targetOffset, int targetLen);
}
