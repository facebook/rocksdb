// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * Base class for slices which will receive direct
 * ByteBuffer based access to the underlying data.
 *
 * ByteBuffer backed slices typically perform better with
 * larger keys and values. When using smaller keys and
 * values consider using @see org.rocksdb.Slice
 */
public class DirectSlice extends AbstractSlice<ByteBuffer> {
  public final static DirectSlice NONE = new DirectSlice();

  /**
   * Called from JNI to construct a new Java DirectSlice
   * without an underlying C++ object set
   * at creation time.
   *
   * Note: You should be aware that it is intentionally marked as
   * package-private. This is so that developers cannot construct their own
   * default DirectSlice objects (at present). As developers cannot construct
   * their own DirectSlice objects through this, they are not creating
   * underlying C++ DirectSlice objects, and so there is nothing to free
   * (dispose) from Java.
   */
  DirectSlice() {
    super();
  }

  /**
   * Constructs a slice
   * where the data is taken from
   * a String.
   *
   * @param str The string
   */
  public DirectSlice(final String str) {
    super(createNewSliceFromString(str));
  }

  /**
   * Constructs a slice where the data is
   * read from the provided
   * ByteBuffer up to a certain length
   *
   * @param data The buffer containing the data
   * @param length The length of the data to use for the slice
   */
  public DirectSlice(final ByteBuffer data, final int length) {
    super(createNewDirectSlice0(ensureDirect(data), length));
  }

  /**
   * Constructs a slice where the data is
   * read from the provided
   * ByteBuffer
   *
   * @param data The bugger containing the data
   */
  public DirectSlice(final ByteBuffer data) {
    super(createNewDirectSlice1(ensureDirect(data)));
  }

  private static ByteBuffer ensureDirect(final ByteBuffer data) {
    // TODO(AR) consider throwing a checked exception, as if it's not direct
    // this can SIGSEGV
    assert(data.isDirect());
    return data;
  }

  /**
   * Retrieves the byte at a specific offset
   * from the underlying data
   *
   * @param offset The (zero-based) offset of the byte to retrieve
   *
   * @return the requested byte
   */
  public byte get(int offset) {
    return get0(getNativeHandle(), offset);
  }

  /**
   * Clears the backing slice
   */
  public void clear() {
    clear0(getNativeHandle());
  }

  /**
   * Drops the specified {@code n}
   * number of bytes from the start
   * of the backing slice
   *
   * @param n The number of bytes to drop
   */
  public void removePrefix(final int n) {
    removePrefix0(getNativeHandle(), n);
  }

  private native static long createNewDirectSlice0(final ByteBuffer data,
      final int length);
  private native static long createNewDirectSlice1(final ByteBuffer data);
  @Override protected final native ByteBuffer data0(long handle);
  private native byte get0(long handle, int offset);
  private native void clear0(long handle);
  private native void removePrefix0(long handle, int length);
}
